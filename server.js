const express = require('express');
const cors    = require('cors');
const crypto  = require('crypto');
const fetch   = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const FAPI = 'https://fapi.binance.com';

// ── CACHE ─────────────────────────────────────────────────────────────────────
const cache = new Map();
async function cached(key, ttl, fn) {
  const now = Date.now();
  if (cache.has(key)) {
    const { val, exp } = cache.get(key);
    if (now < exp) return val;
  }
  const val = await fn();
  cache.set(key, { val, exp: now + ttl });
  return val;
}

function sign(qs, secret) {
  return crypto.createHmac('sha256', secret).update(qs).digest('hex');
}

async function bReq(apiKey, apiSecret, method, path, params = {}) {
  const ts  = Date.now();
  const obj = { ...params, timestamp: ts, recvWindow: 10000 };
  const qs  = Object.entries(obj).map(([k,v]) => `${k}=${encodeURIComponent(v)}`).join('&');
  const sig = sign(qs, apiSecret);
  const url = `${FAPI}${path}`;
  const fullQs = `${qs}&signature=${sig}`;
  const options = {
    method: method.toUpperCase(),
    headers: { 'X-MBX-APIKEY': apiKey, 'Content-Type': 'application/x-www-form-urlencoded' },
  };
  const finalUrl = (method.toUpperCase() === 'GET' || method.toUpperCase() === 'DELETE')
    ? `${url}?${fullQs}` : url;
  if (method.toUpperCase() === 'POST') options.body = fullQs;
  const res  = await fetch(finalUrl, options);
  const data = await res.json();
  if (data.code && data.code < 0) {
    if (data.code === -1121) throw new Error(`Sembol bulunamadı: ${params.symbol}`);
    throw new Error(`${data.msg} (${data.code})`);
  }
  return data;
}

// ── RATE LIMIT KORUMASI ───────────────────────────────────────────────────────
let reqCount = 0, reqWindow = Date.now();
async function bPub(path, qs = '') {
  // Dakikada max 800 istek (Binance limiti 1200, güvenli alan 800)
  const now = Date.now();
  if (now - reqWindow > 60000) { reqCount = 0; reqWindow = now; }
  reqCount++;
  if (reqCount > 800) {
    const wait = 60000 - (now - reqWindow);
    console.log(`Rate limit koruması: ${wait}ms bekleniyor`);
    await new Promise(r => setTimeout(r, wait + 1000));
    reqCount = 0; reqWindow = Date.now();
  }
  // İstekler arası min 50ms boşluk
  await new Promise(r => setTimeout(r, 200));
  const url = `${FAPI}${path}${qs ? '?' + qs : ''}`;
  const r = await fetch(url, { signal: AbortSignal.timeout(10000) });
  if (r.status === 429 || r.status === 418) {
    const retry = parseInt(r.headers.get('Retry-After') || '30');
    console.log(`429/418 alındı, ${retry}sn bekleniyor...`);
    await new Promise(r => setTimeout(r, retry * 1000));
    return bPub(path, qs); // tekrar dene
  }
  return r.json();
}

app.get('/', (req, res) => res.json({ status: 'ok', time: new Date().toISOString() }));

// ── FUTURES COİN LİSTESİ ─────────────────────────────────────────────────────
app.get('/api/futures-coins', async (req, res) => {
  try {
    const data = await cached('futures_tickers', 15*60*1000, () => bPub('/fapi/v1/ticker/24hr'));
    if (!Array.isArray(data)) return res.json({ coins: [] });
    const EXCL = new Set(['BTCUSDT','ETHUSDT','BNBUSDT','XRPUSDT','SOLUSDT',
      'ADAUSDT','DOGEUSDT','DOTUSDT','MATICUSDT','LTCUSDT','TRXUSDT','AVAXUSDT',
      'LINKUSDT','UNIUSDT','WBTCUSDT','SHIBUSDT']);
    const coins = data
      .filter(t => t.symbol.endsWith('USDT') && !EXCL.has(t.symbol))
      .map(t => ({
        symbol:    t.symbol.replace('USDT',''),
        fullSymbol:t.symbol,
        price:     parseFloat(t.lastPrice)          || 0,
        change24h: parseFloat(t.priceChangePercent) || 0,
        volume:    parseFloat(t.quoteVolume)        || 0,
        high:      parseFloat(t.highPrice)          || 0,
        low:       parseFloat(t.lowPrice)           || 0,
        trades:    parseInt(t.count)               || 0,
      }))
      .filter(c => c.volume > 5000000); // Min 5M USDT/24s hacim
    res.json({ ok: true, count: coins.length, coins });
  } catch(e) { res.status(400).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ── MARKET MAKER & LİKİDİTE ANALİZ ENGİNİ ────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════
app.get('/api/analyze/:symbol', async (req, res) => {
  const sym  = req.params.symbol.toUpperCase();
  const full = sym.endsWith('USDT') ? sym : sym + 'USDT';

  try {
    const [
      r4h, r1h, r15m, r5m, r1m,
      rFunding, rOIHist, rLS_global, rLS_top,
      rTakerRatio, rTrades, rDepth
    ] = await Promise.allSettled([
      cached(`k4h_${full}`,   30*60*1000, () => bPub('/fapi/v1/klines', `symbol=${full}&interval=4h&limit=200`)),
      cached(`k1h_${full}`,   10*60*1000,   () => bPub('/fapi/v1/klines', `symbol=${full}&interval=1h&limit=200`)),
      cached(`k15m_${full}`,  5*60*1000,   () => bPub('/fapi/v1/klines', `symbol=${full}&interval=15m&limit=200`)),
      cached(`k5m_${full}`,   3*60*1000,   () => bPub('/fapi/v1/klines', `symbol=${full}&interval=5m&limit=100`)),
      cached(`k1m_${full}`,   2*60*1000,   () => bPub('/fapi/v1/klines', `symbol=${full}&interval=1m&limit=60`)),
      cached(`fund_${full}`,  30*60*1000, () => bPub('/fapi/v1/fundingRate', `symbol=${full}&limit=10`)),
      cached(`oih_${full}`,   15*60*1000, () => bPub('/futures/data/openInterestHist', `symbol=${full}&period=1h&limit=24`)),
      cached(`lsg_${full}`,   15*60*1000, () => bPub('/futures/data/globalLongShortAccountRatio', `symbol=${full}&period=1h&limit=12`)),
      cached(`lst_${full}`,   15*60*1000, () => bPub('/futures/data/topLongShortPositionRatio', `symbol=${full}&period=1h&limit=12`)),
      cached(`tkr_${full}`,   5*60*1000,   () => bPub('/futures/data/takerlongshortRatio', `symbol=${full}&period=5m&limit=48`)),
      cached(`trd_${full}`,   3*60*1000,   () => bPub('/fapi/v1/aggTrades', `symbol=${full}&limit=1000`)),
      cached(`dep_${full}`,   2*60*1000,   () => bPub('/fapi/v1/depth', `symbol=${full}&limit=100`)),
    ]);

    const k4h  = r4h.status  ==='fulfilled'&&Array.isArray(r4h.value)  ? r4h.value  : [];
    const k1h  = r1h.status  ==='fulfilled'&&Array.isArray(r1h.value)  ? r1h.value  : [];
    const k15m = r15m.status ==='fulfilled'&&Array.isArray(r15m.value) ? r15m.value : [];
    const k5m  = r5m.status  ==='fulfilled'&&Array.isArray(r5m.value)  ? r5m.value  : [];
    const k1m  = r1m.status  ==='fulfilled'&&Array.isArray(r1m.value)  ? r1m.value  : [];
    const fundArr    = rFunding.status   ==='fulfilled'&&Array.isArray(rFunding.value)    ? rFunding.value    : [];
    const oiHist     = rOIHist.status    ==='fulfilled'&&Array.isArray(rOIHist.value)     ? rOIHist.value     : [];
    const lsGlobal   = rLS_global.status ==='fulfilled'&&Array.isArray(rLS_global.value)  ? rLS_global.value  : [];
    const lsTop      = rLS_top.status    ==='fulfilled'&&Array.isArray(rLS_top.value)     ? rLS_top.value     : [];
    const takerRatio = rTakerRatio.status==='fulfilled'&&Array.isArray(rTakerRatio.value) ? rTakerRatio.value : [];
    const aggTrades  = rTrades.status    ==='fulfilled'&&Array.isArray(rTrades.value)     ? rTrades.value     : [];
    const depth      = rDepth.status     ==='fulfilled' ? rDepth.value : { bids:[], asks:[] };

    const lastPrice = k15m.length ? parseFloat(k15m[k15m.length-1][4]) : 0;

    // ═══════════════════════════════════════════════════════════════════════════
    // BÖLÜM 1: KLASİK TEKNİK ANALİZ
    // ═══════════════════════════════════════════════════════════════════════════
    function rsi(kl, p=14) {
      if (kl.length < p+1) return 50;
      const c=kl.map(k=>parseFloat(k[4]));
      let g=0,l=0;
      for(let i=c.length-p;i<c.length;i++){const d=c[i]-c[i-1];d>0?g+=d:l-=d;}
      const ag=g/p,al=l/p;
      return al===0?100:Math.round(100-(100/(1+ag/al)));
    }
    function ema(kl,p){
      if(kl.length<p)return 0;
      const c=kl.map(k=>parseFloat(k[4]));
      const k=2/(p+1);
      let e=c.slice(0,p).reduce((a,b)=>a+b,0)/p;
      for(let i=p;i<c.length;i++)e=c[i]*k+e*(1-k);
      return e;
    }
    function vwap(kl){
      let tv=0,v=0;
      kl.forEach(k=>{const tp=(parseFloat(k[2])+parseFloat(k[3])+parseFloat(k[4]))/3;const vol=parseFloat(k[5]);tv+=tp*vol;v+=vol;});
      return v>0?tv/v:0;
    }
    function atr(kl,p=14){
      if(kl.length<p+1)return 0;
      const trs=kl.slice(-p-1).map((k,i,a)=>{
        if(i===0)return parseFloat(k[2])-parseFloat(k[3]);
        const prev=parseFloat(a[i-1][4]);
        return Math.max(parseFloat(k[2])-parseFloat(k[3]),Math.abs(parseFloat(k[2])-prev),Math.abs(parseFloat(k[3])-prev));
      });
      return trs.slice(1).reduce((a,b)=>a+b,0)/p;
    }

    const rsi4h=rsi(k4h),rsi1h=rsi(k1h),rsi15m=rsi(k15m),rsi5m=rsi(k5m);
    const ema20_4h=ema(k4h,20),ema50_4h=ema(k4h,50),ema200_4h=ema(k4h,200);
    const ema20_1h=ema(k1h,20),ema50_1h=ema(k1h,50),ema200_1h=ema(k1h,200);
    const ema20_15m=ema(k15m,20),ema50_15m=ema(k15m,50);
    const vwap1h=vwap(k1h),vwap4h=vwap(k4h),vwap15m=vwap(k15m);
    const atr4h=atr(k4h),atr1h=atr(k1h),atr15m=atr(k15m);

    // ═══════════════════════════════════════════════════════════════════════════
    // BÖLÜM 2: MARKET MAKER LİKİDİTE ANALİZİ
    // Temel fikir: MM önce likiditeyi toplar, sonra gerçek yönde hareket eder
    // ═══════════════════════════════════════════════════════════════════════════

    // 2a. LİKİDİTE SEVİYELERİ — Equal Highs/Lows (stop avı hedefleri)
    // Birbirine çok yakın (<%0.3) swing high/low = orada stop yığılmış
    function findLiquidityLevels(klines, lookback=50) {
      if (klines.length < lookback) return { buyLiq: [], sellLiq: [] };
      const recent = klines.slice(-lookback);
      const highs = recent.map((k,i) => ({ price: parseFloat(k[2]), idx: i, vol: parseFloat(k[5]) }));
      const lows  = recent.map((k,i) => ({ price: parseFloat(k[3]), idx: i, vol: parseFloat(k[5]) }));

      // Equal highs: direnç üzerinde stop birikimi (MM bunları tarar → kısa düşüş)
      const buyLiq = [];  // Yukarıdaki stop'lar (long stop-loss + short stop-hunt)
      const sellLiq = []; // Aşağıdaki stop'lar (short stop-loss + long stop-hunt)

      // Swing high'ları bul (soldan ve sağdan yüksek)
      for (let i=2; i<recent.length-2; i++) {
        const h = parseFloat(recent[i][2]);
        const isSwingH = h > parseFloat(recent[i-1][2]) && h > parseFloat(recent[i-2][2]) &&
                         h > parseFloat(recent[i+1][2]) && h > parseFloat(recent[i+2][2]);
        if (isSwingH) {
          // Bu seviyeye yakın başka swing high var mı? (equal high)
          const existing = buyLiq.find(l => Math.abs(l.price-h)/h < 0.003);
          if (existing) {
            existing.strength++;
            existing.touches++;
          } else {
            buyLiq.push({ price: h, strength: 1, touches: 1, idx: i,
              distPct: +((h-lastPrice)/lastPrice*100).toFixed(2) });
          }
        }
      }
      // Swing low'ları bul
      for (let i=2; i<recent.length-2; i++) {
        const l = parseFloat(recent[i][3]);
        const isSwingL = l < parseFloat(recent[i-1][3]) && l < parseFloat(recent[i-2][3]) &&
                         l < parseFloat(recent[i+1][3]) && l < parseFloat(recent[i+2][3]);
        if (isSwingL) {
          const existing = sellLiq.find(x => Math.abs(x.price-l)/l < 0.003);
          if (existing) {
            existing.strength++;
            existing.touches++;
          } else {
            sellLiq.push({ price: l, strength: 1, touches: 1, idx: i,
              distPct: +((lastPrice-l)/lastPrice*100).toFixed(2) });
          }
        }
      }

      // Güçe göre sırala, en yakın 3'ünü al
      const topBuy  = buyLiq.filter(l=>l.price>lastPrice).sort((a,b)=>a.price-b.price).slice(0,3);
      const topSell = sellLiq.filter(l=>l.price<lastPrice).sort((a,b)=>b.price-a.price).slice(0,3);

      return { buyLiq: topBuy, sellLiq: topSell };
    }

    const liq4h  = findLiquidityLevels(k4h, 100);
    const liq1h  = findLiquidityLevels(k1h, 100);
    const liq15m = findLiquidityLevels(k15m, 50);

    // 2b. STOP HUNT TESPİTİ — Wick analizi
    // Uzun wick + hızlı geri dönüş = MM stop topladı, gerçek yön tersi
    function detectStopHunt(klines, n=10) {
      if (klines.length < n+1) return { hunted: false, direction: 'NONE', wickPct: 0 };
      const recent = klines.slice(-n);
      const results = [];

      recent.forEach((k,i) => {
        const o=parseFloat(k[1]),h=parseFloat(k[2]),l=parseFloat(k[3]),c=parseFloat(k[4]);
        const body=Math.abs(c-o);
        const upperWick=h-Math.max(o,c);
        const lowerWick=Math.min(o,c)-l;
        const range=h-l;
        if(range===0)return;

        // Uzun üst wick = yukardaki stop'ları taradı, dönüş aşağı
        if(upperWick/range>0.6 && upperWick>body*2) {
          results.push({ direction:'BEAR_HUNT', wickPct:+(upperWick/lastPrice*100).toFixed(2),
            price:h, candleIdx:i, msg:'Üst wick stop hunt — dönüş beklenir ↓' });
        }
        // Uzun alt wick = aşağıdaki stop'ları taradı, dönüş yukarı
        if(lowerWick/range>0.6 && lowerWick>body*2) {
          results.push({ direction:'BULL_HUNT', wickPct:+(lowerWick/lastPrice*100).toFixed(2),
            price:l, candleIdx:i, msg:'Alt wick stop hunt — dönüş beklenir ↑' });
        }
      });

      // Son 3 mumda var mı?
      const recent3 = results.filter(r=>r.candleIdx>=n-3);
      if (recent3.length) return { hunted: true, ...recent3[recent3.length-1], all: recent3 };
      return { hunted: false, direction:'NONE', wickPct:0 };
    }

    const hunt4h  = detectStopHunt(k4h, 20);
    const hunt1h  = detectStopHunt(k1h, 20);
    const hunt15m = detectStopHunt(k15m, 10);

    // 2c. MM YÖN TAHMİNİ — Likidite nerede daha fazla?
    // MM her zaman en büyük likidite havuzuna doğru hareket eder
    function mmDirectionAnalysis(buyLiq, sellLiq, currentPrice, oiHist, lsTop) {
      // Üstteki vs alttaki toplam likidite
      const upLiqStrength   = buyLiq.reduce((s,l)=>s+l.strength*(1/Math.max(l.distPct,0.1)),0);
      const downLiqStrength = sellLiq.reduce((s,l)=>s+l.strength*(1/Math.max(l.distPct,0.1)),0);

      // OI trendi: son 4 periyot
      let oiTrend = 'FLAT';
      if (oiHist.length >= 4) {
        const latest = parseFloat(oiHist[oiHist.length-1].sumOpenInterest||0);
        const old    = parseFloat(oiHist[oiHist.length-4].sumOpenInterest||0);
        oiTrend = latest > old*1.02 ? 'RISING' : latest < old*0.98 ? 'FALLING' : 'FLAT';
      }

      // Top trader pozisyonu (büyük oyuncuların hazırlandığı yön)
      const topLong = lsTop.length ? parseFloat(lsTop[lsTop.length-1].longAccount||0.5)*100 : 50;
      const smartMoneyBias = topLong > 60 ? 'UP' : topLong < 40 ? 'DOWN' : 'NEUTRAL';

      // MM mantığı: perakende nerede sıkışmış? Oraya hareket eder
      // Perakende genellikle trend yönünde açık pozisyon tutar → MM tersi yapar
      const globalLong = lsGlobal.length ? parseFloat(lsGlobal[lsGlobal.length-1].longAccount||0.5)*100 : 50;
      const retailBias = globalLong > 65 ? 'TOO_LONG' : globalLong < 35 ? 'TOO_SHORT' : 'NEUTRAL';

      // Karar: en büyük ağrı nerede?
      let mmTarget = 'UNKNOWN';
      let mmConfidence = 0;
      let mmReasoning = [];

      // Üstte daha çok likidite → MM yukarı çekip short açar veya longs toplar
      if (upLiqStrength > downLiqStrength * 1.5) {
        mmTarget = 'UP_SWEEP';
        mmConfidence += 25;
        mmReasoning.push(`Üstte güçlü likidite havuzu (${buyLiq[0]?.price?.toFixed(4)||'—'})`);
      } else if (downLiqStrength > upLiqStrength * 1.5) {
        mmTarget = 'DOWN_SWEEP';
        mmConfidence += 25;
        mmReasoning.push(`Altta güçlü likidite havuzu (${sellLiq[0]?.price?.toFixed(4)||'—'})`);
      }

      // Perakende çok long → MM düşürür (en fazla acı verir)
      if (retailBias === 'TOO_LONG') {
        if (mmTarget !== 'UP_SWEEP') mmConfidence += 20;
        mmTarget = mmTarget==='UP_SWEEP' ? 'UP_SWEEP' : 'DOWN_SWEEP';
        mmReasoning.push(`Perakende %${globalLong.toFixed(0)} long → MM baskı yapabilir`);
      } else if (retailBias === 'TOO_SHORT') {
        mmTarget = mmTarget==='DOWN_SWEEP' ? 'DOWN_SWEEP' : 'UP_SWEEP';
        mmConfidence += 20;
        mmReasoning.push(`Perakende %${(100-globalLong).toFixed(0)} short → MM squeeze yapabilir`);
      }

      // Smart money (büyük oyuncular) yönüyle OI büyüyorsa → o yön gerçek
      if (smartMoneyBias === 'UP' && oiTrend === 'RISING') {
        mmTarget = 'GENUINE_UP';
        mmConfidence += 30;
        mmReasoning.push('Balina long + OI artışı = gerçek yukarı hareketi');
      } else if (smartMoneyBias === 'DOWN' && oiTrend === 'RISING') {
        mmTarget = 'GENUINE_DOWN';
        mmConfidence += 30;
        mmReasoning.push('Balina short + OI artışı = gerçek aşağı hareketi');
      }

      mmConfidence = Math.min(mmConfidence, 95);

      return {
        target: mmTarget,
        confidence: mmConfidence,
        reasoning: mmReasoning,
        upLiqStrength:   +upLiqStrength.toFixed(2),
        downLiqStrength: +downLiqStrength.toFixed(2),
        retailBias,
        smartMoneyBias,
        oiTrend,
        nextTarget: mmTarget.includes('UP') ? (buyLiq[0]?.price||0) : (sellLiq[0]?.price||0),
      };
    }

    const mmAnalysis = mmDirectionAnalysis(liq1h.buyLiq, liq1h.sellLiq, lastPrice, oiHist, lsTop);

    // 2d. INDUCEMENT TESPİTİ
    // MM önce yanlış yönde küçük hareket yapar (inducement), sonra ters döner
    function detectInducement(klines) {
      if (klines.length < 10) return { detected: false };
      const last5 = klines.slice(-5);
      const closes = last5.map(k=>parseFloat(k[4]));
      const highs  = last5.map(k=>parseFloat(k[2]));
      const lows   = last5.map(k=>parseFloat(k[3]));

      // Son 3 mum yukarı gitti ama hacim azaldı → fake move up (inducement)
      const vols = last5.map(k=>parseFloat(k[5]));
      const priceUp = closes[4] > closes[2];
      const volDecline = vols[4] < vols[2] * 0.7;
      const priceDown = closes[4] < closes[2];
      const volDeclineDown = vols[4] < vols[2] * 0.7;

      if (priceUp && volDecline) return {
        detected: true, type: 'FAKE_UP',
        msg: 'Fiyat çıkıyor ama hacim düşüyor → sahte yukarı hareket, dönüş beklenir ↓'
      };
      if (priceDown && volDeclineDown) return {
        detected: true, type: 'FAKE_DOWN',
        msg: 'Fiyat düşüyor ama hacim düşüyor → sahte aşağı hareket, dönüş beklenir ↑'
      };
      return { detected: false };
    }

    const inducement1h  = detectInducement(k1h);
    const inducement15m = detectInducement(k15m);

    // 2e. ORDER BLOCK TESPİTİ (SMC)
    // Büyük tek mumlu hareket öncesi son karşıt mum = order block (MM pozisyon aldı)
    function findOrderBlocks(klines) {
      if (klines.length < 10) return { bullOB: null, bearOB: null };
      const recent = klines.slice(-20);
      let bullOB = null, bearOB = null;

      for (let i = 1; i < recent.length - 1; i++) {
        const cur   = recent[i];
        const next  = recent[i+1];
        const curO  = parseFloat(cur[1]),  curC  = parseFloat(cur[4]);
        const nextO = parseFloat(next[1]), nextC = parseFloat(next[4]);
        const nextMove = Math.abs(nextC-nextO)/nextO*100;

        // Büyük yukarı mum öncesi son aşağı mum = bullish OB
        if (nextC > nextO * 1.005 && nextMove > 0.5 && curC < curO) {
          bullOB = {
            high:  parseFloat(cur[2]),
            low:   parseFloat(cur[3]),
            mid:   (parseFloat(cur[2])+parseFloat(cur[3]))/2,
            valid: lastPrice > parseFloat(cur[3]),
            distPct: +((lastPrice - parseFloat(cur[3]))/lastPrice*100).toFixed(2)
          };
        }
        // Büyük aşağı mum öncesi son yukarı mum = bearish OB
        if (nextC < nextO * 0.995 && nextMove > 0.5 && curC > curO) {
          bearOB = {
            high:  parseFloat(cur[2]),
            low:   parseFloat(cur[3]),
            mid:   (parseFloat(cur[2])+parseFloat(cur[3]))/2,
            valid: lastPrice < parseFloat(cur[2]),
            distPct: +((parseFloat(cur[2])-lastPrice)/lastPrice*100).toFixed(2)
          };
        }
      }
      return { bullOB, bearOB };
    }

    const ob4h  = findOrderBlocks(k4h);
    const ob1h  = findOrderBlocks(k1h);
    const ob15m = findOrderBlocks(k15m);

    // 2f. FAIR VALUE GAP (FVG) — İmbalance bölgeleri
    // MM fiyatı buraya çeker, boşluğu kapatır
    function findFVG(klines) {
      if (klines.length < 3) return [];
      const fvgs = [];
      const recent = klines.slice(-30);
      for (let i = 1; i < recent.length-1; i++) {
        const prev = recent[i-1], cur = recent[i], next = recent[i+1];
        const prevH=parseFloat(prev[2]),prevL=parseFloat(prev[3]);
        const nextH=parseFloat(next[2]),nextL=parseFloat(next[3]);
        const curC=parseFloat(cur[4]);
        // Bullish FVG: önceki mumun dibi, sonraki mumun tepesinden yukarıda
        if (nextL > prevH) {
          fvgs.push({ type:'BULL_FVG', top:nextL, bottom:prevH,
            mid:(nextL+prevH)/2, filled: lastPrice >= prevH && lastPrice <= nextL,
            distPct: +((lastPrice - prevH)/lastPrice*100).toFixed(2) });
        }
        // Bearish FVG: önceki mumun tepesi, sonraki mumun dibinden aşağıda
        if (nextH < prevL) {
          fvgs.push({ type:'BEAR_FVG', top:prevL, bottom:nextH,
            mid:(prevL+nextH)/2, filled: lastPrice <= prevL && lastPrice >= nextH,
            distPct: +((prevL - lastPrice)/lastPrice*100).toFixed(2) });
        }
      }
      return fvgs.filter(f=>!f.filled).slice(-3);
    }

    const fvg1h  = findFVG(k1h);
    const fvg15m = findFVG(k15m);

    // 2g. LİKİDASYON KASKAS (Cascade Risk)
    // Fiyat bir seviyeye gelince zincirleme likidasyonlar başlar
    function liquidationCascadeZones(currentPrice, atr) {
      if (!currentPrice || !atr) return [];
      return [
        { level: +(currentPrice * 0.95).toFixed(4), lev:'20x Long Liq', risk:'HIGH',  distPct:5 },
        { level: +(currentPrice * 0.90).toFixed(4), lev:'10x Long Liq', risk:'HIGH',  distPct:10 },
        { level: +(currentPrice * 0.80).toFixed(4), lev:'5x Long Liq',  risk:'MED',   distPct:20 },
        { level: +(currentPrice * 1.05).toFixed(4), lev:'20x Short Liq',risk:'HIGH',  distPct:5 },
        { level: +(currentPrice * 1.10).toFixed(4), lev:'10x Short Liq',risk:'HIGH',  distPct:10 },
        { level: +(currentPrice * 1.20).toFixed(4), lev:'5x Short Liq', risk:'MED',   distPct:20 },
        // ATR bazlı yakın seviyeler (gerçekçi)
        { level: +(currentPrice - atr*2).toFixed(4), lev:'2x ATR Stop',  risk:'NEAR', distPct:+(atr*2/currentPrice*100).toFixed(1) },
        { level: +(currentPrice + atr*2).toFixed(4), lev:'2x ATR Stop',  risk:'NEAR', distPct:+(atr*2/currentPrice*100).toFixed(1) },
      ];
    }

    const liqZones = liquidationCascadeZones(lastPrice, atr1h);

    // ═══════════════════════════════════════════════════════════════════════════
    // BÖLÜM 3: CVD — Kümülatif Hacim Delta (GERÇEK)
    // ═══════════════════════════════════════════════════════════════════════════
    let cvdBuy=0, cvdSell=0, cvdRecBuy=0, cvdRecSell=0;
    const mid = Math.floor(aggTrades.length/2);
    aggTrades.forEach((t,i) => {
      const qty = parseFloat(t.q) * parseFloat(t.p);
      if (t.m) { cvdSell+=qty; if(i>=mid)cvdRecSell+=qty; }
      else     { cvdBuy+=qty;  if(i>=mid)cvdRecBuy+=qty; }
    });
    const cvdTotal    = cvdBuy - cvdSell;
    const cvdRatio    = cvdBuy+cvdSell>0 ? cvdBuy/(cvdBuy+cvdSell)*100 : 50;
    const cvdMomentum = cvdRecBuy-cvdRecSell > (cvdBuy-cvdSell-cvdRecBuy+cvdRecSell)*1.3
      ? 'ACCELERATING_BULL'
      : cvdRecBuy-cvdRecSell < (cvdBuy-cvdSell-cvdRecBuy+cvdRecSell)*0.7
      ? 'ACCELERATING_BEAR'
      : cvdTotal>0 ? 'POSITIVE' : 'NEGATIVE';

    // ═══════════════════════════════════════════════════════════════════════════
    // BÖLÜM 4: FUNDING RATE
    // ═══════════════════════════════════════════════════════════════════════════
    const currentFunding = fundArr.length ? parseFloat(fundArr[fundArr.length-1].fundingRate)*100 : 0;
    const avgFunding = fundArr.length ? fundArr.reduce((s,f)=>s+parseFloat(f.fundingRate)*100,0)/fundArr.length : 0;
    const fundingTrend = fundArr.length>=2
      ? (parseFloat(fundArr[fundArr.length-1].fundingRate) > parseFloat(fundArr[fundArr.length-2].fundingRate) ? 'RISING' : 'FALLING')
      : 'FLAT';
    const fundingSignal =
      currentFunding < -0.05 ? 'EXTREME_NEGATIVE' :
      currentFunding < -0.01 ? 'NEGATIVE' :
      currentFunding > 0.1   ? 'EXTREME_POSITIVE' :
      currentFunding > 0.05  ? 'POSITIVE' : 'NEUTRAL';
    const fundingAnnualized = +(currentFunding * 3 * 365).toFixed(1);

    // ═══════════════════════════════════════════════════════════════════════════
    // BÖLÜM 5: OPEN INTEREST
    // ═══════════════════════════════════════════════════════════════════════════
    let oiChange1h=0, oiChange4h=0, oiChange8h=0;
    if (oiHist.length >= 2) {
      const fn = x => parseFloat(x.sumOpenInterestValue||x.sumOpenInterest||0);
      const latest=fn(oiHist[oiHist.length-1]);
      const h1back=fn(oiHist[oiHist.length-2]);
      const h4back=fn(oiHist[Math.max(0,oiHist.length-5)]);
      const h8back=fn(oiHist[Math.max(0,oiHist.length-9)]);
      oiChange1h = h1back>0?(latest-h1back)/h1back*100:0;
      oiChange4h = h4back>0?(latest-h4back)/h4back*100:0;
      oiChange8h = h8back>0?(latest-h8back)/h8back*100:0;
    }
    const priceChange1h = k1h.length>=2
      ? (parseFloat(k1h[k1h.length-1][4])-parseFloat(k1h[k1h.length-2][4]))/parseFloat(k1h[k1h.length-2][4])*100 : 0;
    const oiDivergence =
      oiChange1h>1  && priceChange1h>0.5  ? 'CONFIRMED_BULL'    :
      oiChange1h>1  && priceChange1h<-0.5 ? 'CONFIRMED_BEAR'    :
      oiChange1h<-1 && priceChange1h>0.5  ? 'SHORT_SQUEEZE'     :
      oiChange1h<-1 && priceChange1h<-0.5 ? 'LONG_LIQUIDATION'  : 'NEUTRAL';

    // ═══════════════════════════════════════════════════════════════════════════
    // BÖLÜM 6: SMART MONEY
    // ═══════════════════════════════════════════════════════════════════════════
    const globalLS   = lsGlobal.length ? parseFloat(lsGlobal[lsGlobal.length-1].longShortRatio||1) : 1;
    const topLS      = lsTop.length    ? parseFloat(lsTop[lsTop.length-1].longShortRatio||1) : 1;
    const globalLong = lsGlobal.length ? parseFloat(lsGlobal[lsGlobal.length-1].longAccount||0.5)*100 : 50;
    const topLong    = lsTop.length    ? parseFloat(lsTop[lsTop.length-1].longAccount||0.5)*100 : 50;
    const smDivergence =
      topLong>55 && globalLong<45 ? 'SMART_BULL' :
      topLong<45 && globalLong>55 ? 'SMART_BEAR' :
      topLong>60 ? 'WHALE_LONG' : topLong<40 ? 'WHALE_SHORT' : 'NEUTRAL';
    const lsTrend = lsTop.length>=3
      ? (parseFloat(lsTop[lsTop.length-1].longAccount) > parseFloat(lsTop[lsTop.length-3].longAccount)
        ? 'SMART_ACCUMULATING' : 'SMART_DISTRIBUTING')
      : 'UNKNOWN';

    // ═══════════════════════════════════════════════════════════════════════════
    // BÖLÜM 7: ORDER BOOK
    // ═══════════════════════════════════════════════════════════════════════════
    const bids = Array.isArray(depth.bids) ? depth.bids.slice(0,30) : [];
    const asks = Array.isArray(depth.asks) ? depth.asks.slice(0,30) : [];
    let totalBidUSDT=0, totalAskUSDT=0;
    bids.forEach(([p,q])=>totalBidUSDT+=parseFloat(p)*parseFloat(q));
    asks.forEach(([p,q])=>totalAskUSDT+=parseFloat(p)*parseFloat(q));
    const bookImbalance = totalBidUSDT+totalAskUSDT>0
      ? (totalBidUSDT-totalAskUSDT)/(totalBidUSDT+totalAskUSDT)*100 : 0;
    const maxBidWall = bids.reduce((m,[p,q])=>Math.max(m,parseFloat(p)*parseFloat(q)),0);
    const maxAskWall = asks.reduce((m,[p,q])=>Math.max(m,parseFloat(p)*parseFloat(q)),0);
    const hasStrongBid = maxBidWall > totalBidUSDT*0.25;
    const hasStrongAsk = maxAskWall > totalAskUSDT*0.25;
    const bookSignal =
      bookImbalance > 20 ? 'STRONG_BIDS' : bookImbalance < -20 ? 'STRONG_ASKS' :
      bookImbalance > 10 ? 'SLIGHT_BIDS' : bookImbalance < -10 ? 'SLIGHT_ASKS' : 'BALANCED';

    // Taker flow
    let takerBuyPct=50;
    if (takerRatio.length>=3) {
      const r3=takerRatio.slice(-3);
      const tb=r3.reduce((s,t)=>s+parseFloat(t.buyVol||0),0);
      const ts=r3.reduce((s,t)=>s+parseFloat(t.sellVol||0),0);
      takerBuyPct = tb+ts>0 ? tb/(tb+ts)*100 : 50;
    }
    const takerTrend = takerBuyPct>60?'AGGRESSIVE_BUYING':takerBuyPct<40?'AGGRESSIVE_SELLING':'NEUTRAL';

    // ═══════════════════════════════════════════════════════════════════════════
    // BÖLÜM 8: SKOR HESAPLAMA — MM felsefesiyle ağırlıklandırılmış
    // ═══════════════════════════════════════════════════════════════════════════
    let longScore=0, shortScore=0;
    const signals = { long:[], short:[] };

    // ── MM YÖN (en ağır) ─────────────────────────────────────────────────────
    if (mmAnalysis.target==='GENUINE_UP')    { longScore+=30;  signals.long.push('MM gerçek yukarı'); }
    if (mmAnalysis.target==='GENUINE_DOWN')  { shortScore+=30; signals.short.push('MM gerçek aşağı'); }
    if (mmAnalysis.target==='UP_SWEEP')      { longScore+=15;  signals.long.push('MM üst liq avı → long fırsatı'); }
    if (mmAnalysis.target==='DOWN_SWEEP')    { shortScore+=15; signals.short.push('MM alt liq avı → short fırsatı'); }

    // ── STOP HUNT (ters oyna) ─────────────────────────────────────────────────
    if (hunt1h.hunted && hunt1h.direction==='BULL_HUNT')  { longScore+=15;  signals.long.push('1h Stop Hunt ↑ tespit: '+hunt1h.msg); }
    if (hunt1h.hunted && hunt1h.direction==='BEAR_HUNT')  { shortScore+=15; signals.short.push('1h Stop Hunt ↓ tespit: '+hunt1h.msg); }
    if (hunt15m.hunted && hunt15m.direction==='BULL_HUNT'){ longScore+=10;  signals.long.push('15m Stop Hunt ↑'); }
    if (hunt15m.hunted && hunt15m.direction==='BEAR_HUNT'){ shortScore+=10; signals.short.push('15m Stop Hunt ↓'); }

    // ── ORDER BLOCK ───────────────────────────────────────────────────────────
    if (ob1h.bullOB && lastPrice <= ob1h.bullOB.high*1.005 && lastPrice >= ob1h.bullOB.low*0.995)
      { longScore+=15; signals.long.push('1h Bull OB üzerinde'); }
    if (ob1h.bearOB && lastPrice >= ob1h.bearOB.low*0.995 && lastPrice <= ob1h.bearOB.high*1.005)
      { shortScore+=15; signals.short.push('1h Bear OB altında'); }
    if (ob4h.bullOB && ob4h.bullOB.distPct < 2) { longScore+=10; signals.long.push('4h Bull OB yakın'); }
    if (ob4h.bearOB && ob4h.bearOB.distPct < 2) { shortScore+=10; signals.short.push('4h Bear OB yakın'); }

    // ── FVG (boşluk kapatma) ─────────────────────────────────────────────────
    const bullFVG1h = fvg1h.find(f=>f.type==='BULL_FVG'&&f.distPct<2);
    const bearFVG1h = fvg1h.find(f=>f.type==='BEAR_FVG'&&f.distPct<2);
    if (bullFVG1h) { longScore+=8; signals.long.push('1h Bull FVG boşluğu'); }
    if (bearFVG1h) { shortScore+=8; signals.short.push('1h Bear FVG boşluğu'); }

    // ── INDUCEMENT ────────────────────────────────────────────────────────────
    if (inducement1h.detected && inducement1h.type==='FAKE_UP')   { shortScore+=10; signals.short.push(inducement1h.msg); }
    if (inducement1h.detected && inducement1h.type==='FAKE_DOWN') { longScore+=10;  signals.long.push(inducement1h.msg); }

    // ── CVD ───────────────────────────────────────────────────────────────────
    if (cvdMomentum==='ACCELERATING_BULL') { longScore+=12;  signals.long.push('CVD Acc.Buy'); }
    if (cvdMomentum==='ACCELERATING_BEAR') { shortScore+=12; signals.short.push('CVD Acc.Sell'); }
    if (cvdRatio>60) { longScore+=5; } if (cvdRatio<40) { shortScore+=5; }

    // ── FUNDING ───────────────────────────────────────────────────────────────
    if (fundingSignal==='EXTREME_NEGATIVE') { longScore+=10;  signals.long.push(`Funding ${currentFunding.toFixed(4)}% aşırı negatif`); }
    if (fundingSignal==='NEGATIVE')         { longScore+=5; }
    if (fundingSignal==='EXTREME_POSITIVE') { shortScore+=10; signals.short.push(`Funding ${currentFunding.toFixed(4)}% aşırı pozitif`); }
    if (fundingSignal==='POSITIVE')         { shortScore+=5; }

    // ── OI ────────────────────────────────────────────────────────────────────
    if (oiDivergence==='CONFIRMED_BULL')  { longScore+=10;  signals.long.push('OI+Fiyat ↑'); }
    if (oiDivergence==='SHORT_SQUEEZE')   { longScore+=8;   signals.long.push('Short Squeeze'); }
    if (oiDivergence==='CONFIRMED_BEAR')  { shortScore+=10; signals.short.push('OI+Fiyat ↓'); }
    if (oiDivergence==='LONG_LIQUIDATION'){ shortScore+=8;  signals.short.push('Long Liq'); }

    // ── SMART MONEY ───────────────────────────────────────────────────────────
    if (smDivergence==='SMART_BULL') { longScore+=12;  signals.long.push('Whale Long'); }
    if (smDivergence==='SMART_BEAR') { shortScore+=12; signals.short.push('Whale Short'); }
    if (smDivergence==='WHALE_LONG') { longScore+=6; }
    if (smDivergence==='WHALE_SHORT'){ shortScore+=6; }

    // ── ORDER BOOK ────────────────────────────────────────────────────────────
    if (bookSignal==='STRONG_BIDS') { longScore+=6; signals.long.push('Alım duvarı'); }
    if (bookSignal==='STRONG_ASKS') { shortScore+=6; signals.short.push('Satış duvarı'); }
    if (hasStrongBid) { longScore+=4; signals.long.push('Iceberg alıcı'); }
    if (hasStrongAsk) { shortScore+=4; signals.short.push('Iceberg satıcı'); }

    // ── TAKER ─────────────────────────────────────────────────────────────────
    if (takerTrend==='AGGRESSIVE_BUYING')  { longScore+=6; signals.long.push(`Taker Buy %${takerBuyPct.toFixed(0)}`); }
    if (takerTrend==='AGGRESSIVE_SELLING') { shortScore+=6; signals.short.push(`Taker Sell %${(100-takerBuyPct).toFixed(0)}`); }

    // ── KLASİK TEKNİK (teyit) ────────────────────────────────────────────────
    const trend4hUp = ema20_4h > ema50_4h && ema50_4h > ema200_4h;
    const trend4hDn = ema20_4h < ema50_4h && ema50_4h < ema200_4h;
    if (trend4hUp) { longScore+=8; } if (trend4hDn) { shortScore+=8; }
    if (rsi4h<35)  { longScore+=8; signals.long.push(`4h RSI oversold ${rsi4h}`); }
    if (rsi4h>65)  { shortScore+=8; signals.short.push(`4h RSI overbought ${rsi4h}`); }
    if (lastPrice < ema200_1h) { shortScore+=5; } else { longScore+=5; }
    if (lastPrice > vwap1h)    { longScore+=3; }  else { shortScore+=3; }

    longScore  = Math.min(Math.round(longScore),  100);
    shortScore = Math.min(Math.round(shortScore), 100);
    const recommendation = longScore>shortScore&&longScore>=50?'LONG':shortScore>longScore&&shortScore>=50?'SHORT':'WAIT';

    // ═══════════════════════════════════════════════════════════════════════════
    // YANIT
    // ═══════════════════════════════════════════════════════════════════════════
    res.json({
      ok: true, symbol: full, price: lastPrice,

      // MM ANALİZİ (yeni)
      marketMaker: mmAnalysis,
      liquidityLevels: {
        '4h': liq4h, '1h': liq1h, '15m': liq15m,
      },
      stopHunt: { '4h': hunt4h, '1h': hunt1h, '15m': hunt15m },
      orderBlocks: { '4h': ob4h, '1h': ob1h, '15m': ob15m },
      fvg: { '1h': fvg1h, '15m': fvg15m },
      inducement: { '1h': inducement1h, '15m': inducement15m },
      liquidationZones: liqZones,

      // KLASİK
      timeframes: {
        '4h':  { rsi:rsi4h,  ema20:+ema20_4h.toFixed(4),  ema50:+ema50_4h.toFixed(4),  ema200:+ema200_4h.toFixed(4),  trend:trend4hUp?'UP':trend4hDn?'DOWN':'RANGE', vwap:+vwap4h.toFixed(4), atr:+atr4h.toFixed(4) },
        '1h':  { rsi:rsi1h,  ema20:+ema20_1h.toFixed(4),  ema50:+ema50_1h.toFixed(4),  ema200:+ema200_1h.toFixed(4),  trend:ema20_1h>ema50_1h?'UP':'DOWN', vwap:+vwap1h.toFixed(4), atr:+atr1h.toFixed(4) },
        '15m': { rsi:rsi15m, ema20:+ema20_15m.toFixed(4), ema50:+ema50_15m.toFixed(4), vwap:+vwap15m.toFixed(4), atr:+atr15m.toFixed(4) },
        '5m':  { rsi:rsi5m },
      },
      cvd: { total:+cvdTotal.toFixed(0), buyRatio:+cvdRatio.toFixed(1), momentum:cvdMomentum, buyVol:+cvdBuy.toFixed(0), sellVol:+cvdSell.toFixed(0) },
      funding: { current:+currentFunding.toFixed(4), avg10:+avgFunding.toFixed(4), trend:fundingTrend, signal:fundingSignal, annualized:fundingAnnualized },
      openInterest: { change1h:+oiChange1h.toFixed(2), change4h:+oiChange4h.toFixed(2), change8h:+oiChange8h.toFixed(2), divergence:oiDivergence },
      smartMoney: { topLongPct:+topLong.toFixed(1), topShortPct:+(100-topLong).toFixed(1), globalLongPct:+globalLong.toFixed(1), globalShortPct:+(100-globalLong).toFixed(1), divergence:smDivergence, trend:lsTrend, topLS:+topLS.toFixed(2), globalLS:+globalLS.toFixed(2) },
      orderBook: { imbalance:+bookImbalance.toFixed(1), signal:bookSignal, bidWall:hasStrongBid, askWall:hasStrongAsk, totalBidUSDT:+totalBidUSDT.toFixed(0), totalAskUSDT:+totalAskUSDT.toFixed(0) },
      takerFlow: { buyPct:+takerBuyPct.toFixed(1), trend:takerTrend },

      longScore, shortScore, recommendation,
      signals: recommendation==='LONG' ? signals.long.slice(0,6) : signals.short.slice(0,6),
    });
  } catch(e) {
    res.status(400).json({ error: e.message });
  }
});

// ── HESAP ─────────────────────────────────────────────────────────────────────
app.post('/api/account', async (req, res) => {
  const { apiKey, apiSecret } = req.body;
  if (!apiKey||!apiSecret) return res.status(400).json({ error:'API key gerekli' });
  try {
    let walletBal=0,availBal=0,unrealized=0;
    try {
      const b=await bReq(apiKey,apiSecret,'GET','/fapi/v3/balance');
      const u=Array.isArray(b)?b.find(x=>x.asset==='USDT'):null;
      if(u){walletBal=parseFloat(u.balance)||0;availBal=parseFloat(u.availableBalance)||0;}
    }catch(e){}
    const data=await bReq(apiKey,apiSecret,'GET','/fapi/v2/account');
    if(parseFloat(data.totalWalletBalance)>0)walletBal=parseFloat(data.totalWalletBalance);
    if(parseFloat(data.availableBalance)>0)availBal=parseFloat(data.availableBalance);
    unrealized=parseFloat(data.totalUnrealizedProfit)||0;
    res.json({ ok:true, totalWalletBalance:walletBal, availableBalance:availBal,
      totalUnrealizedProfit:unrealized,
      positions:(data.positions||[]).filter(p=>parseFloat(p.positionAmt)!==0).map(p=>({
        symbol:p.symbol, side:parseFloat(p.positionAmt)>0?'LONG':'SHORT',
        positionAmt:Math.abs(parseFloat(p.positionAmt)), entryPrice:parseFloat(p.entryPrice),
        markPrice:parseFloat(p.markPrice), unrealizedProfit:parseFloat(p.unRealizedProfit),
        leverage:parseInt(p.leverage), liquidationPrice:parseFloat(p.liquidationPrice),
      }))
    });
  }catch(e){res.status(400).json({error:e.message});}
});

// ── EMİR AÇ ──────────────────────────────────────────────────────────────────
app.post('/api/order', async (req, res) => {
  const { apiKey, apiSecret, symbol, side, leverage, marginType, targetPrice, stopPrice, usdtAmount } = req.body;
  if (!apiKey||!apiSecret||!symbol||!side||!leverage||!targetPrice||!stopPrice||!usdtAmount)
    return res.status(400).json({ error:'Eksik parametre' });

  const sym    = symbol.toUpperCase().includes('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  const isLong = side.toUpperCase()==='LONG';
  const oSide  = isLong ? 'BUY'  : 'SELL';
  const cSide  = isLong ? 'SELL' : 'BUY';

  try {
    // 1. Marjin tipi
    if (marginType) {
      try { await bReq(apiKey,apiSecret,'POST','/fapi/v1/marginType',
        {symbol:sym, marginType:marginType.toUpperCase()}); }
      catch(e) { if(!e.message.includes('No need')) console.log('MarginType:',e.message); }
    }

    // 2. Kaldıraç
    await bReq(apiKey,apiSecret,'POST','/fapi/v1/leverage',
      {symbol:sym, leverage:parseInt(leverage)});

    // 3. Sembol bilgisi (stepSize, tickSize)
    let stepSize=0.001, tickSize=0.01, minNot=5;
    try {
      const si = await bPub('/fapi/v1/exchangeInfo','symbol='+sym);
      const s  = Array.isArray(si.symbols) ? si.symbols.find(x=>x.symbol===sym) : null;
      if (s) {
        const lf=s.filters.find(f=>f.filterType==='LOT_SIZE');
        const pf=s.filters.find(f=>f.filterType==='PRICE_FILTER');
        const mf=s.filters.find(f=>f.filterType==='MIN_NOTIONAL');
        if (lf) stepSize=parseFloat(lf.stepSize);
        if (pf) tickSize=parseFloat(pf.tickSize);
        if (mf) minNot=parseFloat(mf.notional||mf.minNotional||5);
      }
    } catch(e) { console.log('ExchangeInfo:', e.message); }

    // 4. Anlık fiyat
    const pr = await bPub('/fapi/v1/ticker/price','symbol='+sym);
    const curPrice = parseFloat(pr.price)||0;
    if (!curPrice) throw new Error('Fiyat alınamadı');

    const qp  = stepSize<1 ? -Math.floor(Math.log10(stepSize)) : 0;
    const pp  = tickSize<1 ? -Math.floor(Math.log10(tickSize)) : 0;
    const qty = parseFloat(((parseFloat(usdtAmount)*parseInt(leverage))/curPrice).toFixed(qp));
    const rnd = p => parseFloat(parseFloat(p).toFixed(pp));

    if (qty*curPrice < minNot) throw new Error(`Min işlem $${minNot}. Miktarı artır.`);

    // 5. MARKET ana emir
    const main = await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',{
      symbol:sym, side:oSide, type:'MARKET', quantity:qty, positionSide:'BOTH'
    });

    // Ana emir açıldıktan sonra gerçek giriş fiyatını al
    await new Promise(r=>setTimeout(r,500)); // Binance'ın pozisyonu kaydetmesi için bekle
    let execPrice = parseFloat(main.avgPrice||main.price||curPrice);
    try {
      const pos = await bReq(apiKey,apiSecret,'GET','/fapi/v2/positionRisk',{symbol:sym});
      const p   = Array.isArray(pos) ? pos.find(x=>x.symbol===sym&&Math.abs(parseFloat(x.positionAmt))>0) : null;
      if (p) execPrice = parseFloat(p.entryPrice)||execPrice;
    } catch(e) {}

    // TP/SL fiyatlarını gerçek giriş fiyatına göre yeniden hesapla
    const ratio = execPrice / curPrice;
    const realTP = rnd(parseFloat(targetPrice) * ratio);
    const realSL = rnd(parseFloat(stopPrice)   * ratio);

    console.log(`${sym} giriş: ${execPrice}, TP: ${realTP}, SL: ${realSL}`);

    // 6. TP/SL — 4 farklı yöntem dene
    let tp={orderId:null}, sl={orderId:null};

    async function placeTPSL(type, price) {
      const base = {
        symbol:sym, side:cSide, type,
        stopPrice:price, closePosition:'true', positionSide:'BOTH'
      };
      const methods = [
        {...base, workingType:'MARK_PRICE'},
        {...base, workingType:'CONTRACT_PRICE'},
        {...base, workingType:'LAST_PRICE'},
        base  // parametresiz
      ];
      for (const params of methods) {
        try {
          const result = await bReq(apiKey,apiSecret,'POST','/fapi/v1/order', params);
          console.log(`${type} başarılı (${params.workingType||'no-type'}): ${result.orderId}`);
          return result;
        } catch(e) {
          console.log(`${type} hata (${params.workingType||'no-type'}): ${e.message}`);
        }
      }
      return {orderId:null};
    }

    tp = await placeTPSL('TAKE_PROFIT_MARKET', realTP);
    await new Promise(r=>setTimeout(r,300));
    sl = await placeTPSL('STOP_MARKET', realSL);

    res.json({
      ok: true,
      message: `${sym} ${side} açıldı ✅${tp.orderId?' TP:'+tp.orderId:'❌ TP hata'}${sl.orderId?' SL:'+sl.orderId:' ❌ SL hata'}`,
      mainOrderId:  main.orderId,
      tpOrderId:    tp.orderId,
      slOrderId:    sl.orderId,
      executedPrice:execPrice,
      details: {symbol:sym, side, quantity:qty, leverage, entry:execPrice, target:realTP, stop:realSL}
    });
  } catch(e) { res.status(400).json({ error:e.message }); }
});

// ── POZİSYONLAR ──────────────────────────────────────────────────────────────
app.post('/api/positions', async (req, res) => {
  const { apiKey, apiSecret } = req.body;
  if(!apiKey||!apiSecret)return res.status(400).json({error:'API key gerekli'});
  try{
    const data=await bReq(apiKey,apiSecret,'GET','/fapi/v2/positionRisk');
    const open=Array.isArray(data)?data.filter(p=>parseFloat(p.positionAmt)!==0).map(p=>{
      const amt=parseFloat(p.positionAmt),ep=parseFloat(p.entryPrice),mp=parseFloat(p.markPrice),lev=parseInt(p.leverage);
      const side=amt>0?'LONG':'SHORT';
      const pct=ep>0?((mp-ep)/ep*100*lev*(side==='SHORT'?-1:1)).toFixed(2):'0';
      return{symbol:p.symbol,side,positionAmt:Math.abs(amt),entryPrice:ep,markPrice:mp,
        unrealizedProfit:parseFloat(p.unRealizedProfit),pnlPct:parseFloat(pct),
        leverage:lev,liquidationPrice:parseFloat(p.liquidationPrice)};
    }):[];
    res.json({ok:true,positions:open});
  }catch(e){res.status(400).json({error:e.message});}
});

// ── KAPAT ─────────────────────────────────────────────────────────────────────
app.post('/api/close', async (req, res) => {
  const { apiKey, apiSecret, symbol } = req.body;
  if(!apiKey||!apiSecret||!symbol)return res.status(400).json({error:'Eksik parametre'});
  const sym=symbol.toUpperCase().includes('USDT')?symbol.toUpperCase():symbol.toUpperCase()+'USDT';
  try{
    try{await bReq(apiKey,apiSecret,'DELETE','/fapi/v1/allOpenOrders',{symbol:sym});}catch(e){}
    const pos=await bReq(apiKey,apiSecret,'GET','/fapi/v2/positionRisk',{symbol:sym});
    const arr=Array.isArray(pos)?pos:[];
    const p=arr.find(x=>Math.abs(parseFloat(x.positionAmt))>0);
    if(!p)return res.json({ok:true,message:'Açık pozisyon yok'});
    const qty=Math.abs(parseFloat(p.positionAmt));
    const order=await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',{
      symbol:sym,side:parseFloat(p.positionAmt)>0?'SELL':'BUY',
      type:'MARKET',quantity:qty,reduceOnly:'true',positionSide:'BOTH'
    });
    res.json({ok:true,message:`${sym} kapatıldı`,orderId:order.orderId});
  }catch(e){res.status(400).json({error:e.message});}
});

app.listen(PORT, ()=>console.log(`✅ Server ${PORT}`));
