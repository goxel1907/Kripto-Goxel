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

// ── İMZA ─────────────────────────────────────────────────────────────────────
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
  if (method.toUpperCase() === 'POST')   options.body = fullQs;
  const res  = await fetch(finalUrl, options);
  const data = await res.json();
  if (data.code && data.code < 0) {
    if (data.code === -1121) throw new Error(`Sembol bulunamadı: ${params.symbol}`);
    throw new Error(`${data.msg} (${data.code})`);
  }
  return data;
}

async function bPub(path, qs = '') {
  const url = `${FAPI}${path}${qs ? '?' + qs : ''}`;
  const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
  return r.json();
}

// ── SAĞLIK ────────────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.json({ status: 'ok', time: new Date().toISOString() }));

// ── FUTURES COİN LİSTESİ (cache 5dk) ─────────────────────────────────────────
app.get('/api/futures-coins', async (req, res) => {
  try {
    const data = await cached('futures_tickers', 5 * 60 * 1000, () =>
      bPub('/fapi/v1/ticker/24hr'));

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
      .filter(c => c.volume > 500000);

    res.json({ ok: true, count: coins.length, coins });
  } catch(e) { res.status(400).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ── PRO ANALİZ ENDPOİNT — tüm sinyal motorları burada ─────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════
app.get('/api/analyze/:symbol', async (req, res) => {
  const sym = req.params.symbol.toUpperCase();
  const full = sym.endsWith('USDT') ? sym : sym + 'USDT';

  try {
    // Tüm verileri paralel çek
    const [
      r4h, r1h, r15m, r5m,
      rFunding, rOI, rOIHist,
      rLS_global, rLS_top,
      rLiqMap, rTrades, rDepth
    ] = await Promise.allSettled([
      cached(`k4h_${full}`,  4*60*1000, () => bPub('/fapi/v1/klines', `symbol=${full}&interval=4h&limit=100`)),
      cached(`k1h_${full}`,  60*1000,   () => bPub('/fapi/v1/klines', `symbol=${full}&interval=1h&limit=100`)),
      cached(`k15m_${full}`, 30*1000,   () => bPub('/fapi/v1/klines', `symbol=${full}&interval=15m&limit=100`)),
      cached(`k5m_${full}`,  15*1000,   () => bPub('/fapi/v1/klines', `symbol=${full}&interval=5m&limit=60`)),
      // Funding rate — şu anki + geçmiş
      cached(`fund_${full}`, 5*60*1000, () => bPub('/fapi/v1/fundingRate', `symbol=${full}&limit=10`)),
      // Anlık OI
      cached(`oi_${full}`,   2*60*1000, () => bPub('/fapi/v1/openInterest', `symbol=${full}`)),
      // OI tarihçesi (1s periyot, son 8 saat)
      cached(`oihist_${full}`, 3*60*1000, () => bPub('/futures/data/openInterestHist', `symbol=${full}&period=1h&limit=8`)),
      // Global L/S oranı (tüm hesaplar)
      cached(`lsg_${full}`,  3*60*1000, () => bPub('/futures/data/globalLongShortAccountRatio', `symbol=${full}&period=1h&limit=6`)),
      // Top Trader L/S pozisyon oranı (büyük oyuncular)
      cached(`lst_${full}`,  3*60*1000, () => bPub('/futures/data/topLongShortPositionRatio', `symbol=${full}&period=1h&limit=6`)),
      // Tasfiye emirleri (liq map için)
      cached(`liq_${full}`,  60*1000,   () => bPub('/futures/data/takerlongshortRatio', `symbol=${full}&period=5m&limit=24`)),
      // Taker alım/satım hacmi (CVD için)
      cached(`trd_${full}`,  30*1000,   () => bPub('/fapi/v1/aggTrades', `symbol=${full}&limit=500`)),
      // Order book derinliği (iceberg/büyük duvar tespiti)
      cached(`dep_${full}`,  30*1000,   () => bPub('/fapi/v1/depth', `symbol=${full}&limit=50`)),
    ]);

    const k4h  = r4h.status   === 'fulfilled' && Array.isArray(r4h.value)   ? r4h.value   : [];
    const k1h  = r1h.status   === 'fulfilled' && Array.isArray(r1h.value)   ? r1h.value   : [];
    const k15m = r15m.status  === 'fulfilled' && Array.isArray(r15m.value)  ? r15m.value  : [];
    const k5m  = r5m.status   === 'fulfilled' && Array.isArray(r5m.value)   ? r5m.value   : [];
    const fundArr   = rFunding.status === 'fulfilled' && Array.isArray(rFunding.value) ? rFunding.value : [];
    const oiNow     = rOI.status      === 'fulfilled' && rOI.value?.openInterest ? parseFloat(rOI.value.openInterest) : 0;
    const oiHist    = rOIHist.status  === 'fulfilled' && Array.isArray(rOIHist.value)  ? rOIHist.value  : [];
    const lsGlobal  = rLS_global.status === 'fulfilled' && Array.isArray(rLS_global.value) ? rLS_global.value : [];
    const lsTop     = rLS_top.status    === 'fulfilled' && Array.isArray(rLS_top.value)    ? rLS_top.value    : [];
    const takerRatio= rLiqMap.status  === 'fulfilled' && Array.isArray(rLiqMap.value)  ? rLiqMap.value  : [];
    const aggTrades = rTrades.status  === 'fulfilled' && Array.isArray(rTrades.value)  ? rTrades.value  : [];
    const depth     = rDepth.status   === 'fulfilled' ? rDepth.value : { bids:[], asks:[] };

    const lastPrice = k15m.length ? parseFloat(k15m[k15m.length-1][4]) : 0;

    // ─────────────────────────────────────────────────────────────────────────
    // 1. KLASİK TEKNIK — RSI, EMA, VWAP, Bollinger
    // ─────────────────────────────────────────────────────────────────────────
    function rsi(klines, p=14) {
      if (klines.length < p+1) return 50;
      const c = klines.map(k=>parseFloat(k[4]));
      let g=0,l=0;
      for(let i=c.length-p; i<c.length; i++){
        const d=c[i]-c[i-1]; d>0?g+=d:l-=d;
      }
      const ag=g/p,al=l/p;
      return al===0?100:Math.round(100-(100/(1+ag/al)));
    }

    function ema(klines, p) {
      if (klines.length < p) return 0;
      const c = klines.map(k=>parseFloat(k[4]));
      const k = 2/(p+1);
      let e = c.slice(0,p).reduce((a,b)=>a+b,0)/p;
      for(let i=p;i<c.length;i++) e=c[i]*k+e*(1-k);
      return e;
    }

    function vwap(klines) {
      if (!klines.length) return 0;
      let tv=0,v=0;
      klines.forEach(k=>{
        const tp=(parseFloat(k[2])+parseFloat(k[3])+parseFloat(k[4]))/3;
        const vol=parseFloat(k[5]);
        tv+=tp*vol; v+=vol;
      });
      return v>0?tv/v:0;
    }

    function bollinger(klines, p=20, mul=2) {
      if (klines.length < p) return {upper:0,lower:0,mid:0,width:0};
      const c = klines.slice(-p).map(k=>parseFloat(k[4]));
      const mid = c.reduce((a,b)=>a+b,0)/p;
      const std = Math.sqrt(c.reduce((s,v)=>s+Math.pow(v-mid,2),0)/p);
      return { upper:mid+mul*std, lower:mid-mul*std, mid, width:(mul*2*std/mid)*100 };
    }

    function atr(klines, p=14) {
      if (klines.length < p+1) return 0;
      const trs=klines.slice(-p-1).map((k,i,arr)=>{
        if(i===0)return parseFloat(k[2])-parseFloat(k[3]);
        const prev=parseFloat(arr[i-1][4]);
        return Math.max(parseFloat(k[2])-parseFloat(k[3]),
          Math.abs(parseFloat(k[2])-prev),
          Math.abs(parseFloat(k[3])-prev));
      });
      return trs.slice(1).reduce((a,b)=>a+b,0)/p;
    }

    // MACD
    function macd(klines) {
      if (klines.length < 26) return {macd:0,signal:0,hist:0,cross:'NONE'};
      const fast=ema(klines,12), slow=ema(klines,26);
      const macdVal=fast-slow;
      // Signal: EMA9 of macd values (approximate with last value)
      const macdArr=[];
      for(let i=26;i<=klines.length;i++){
        const f=ema(klines.slice(0,i),12);
        const s=ema(klines.slice(0,i),26);
        macdArr.push({o:f-s});
      }
      const signalArr=macdArr.map((_,i,a)=>{
        if(i<9)return 0;
        return a.slice(i-9,i).reduce((s,v)=>s+v.o,0)/9;
      });
      const sig=signalArr[signalArr.length-1];
      const hist=macdVal-sig;
      const prevHist=macdArr.length>1?macdArr[macdArr.length-2].o-signalArr[signalArr.length-2]:0;
      const cross=hist>0&&prevHist<=0?'BULL':hist<0&&prevHist>=0?'BEAR':'NONE';
      return {macd:macdVal,signal:sig,hist,cross};
    }

    // Stochastic RSI
    function stochRSI(klines, rsiP=14, stochP=14, kP=3, dP=3) {
      if (klines.length < rsiP+stochP+kP+dP) return {k:50,d:50,zone:'NEUTRAL'};
      const c=klines.map(k=>parseFloat(k[4]));
      const rsiArr=[];
      for(let i=rsiP;i<c.length;i++){
        const sl=c.slice(i-rsiP,i+1);
        let g=0,l=0;
        for(let j=1;j<sl.length;j++){const d=sl[j]-sl[j-1];d>0?g+=d:l-=d;}
        const ag=g/rsiP,al=l/rsiP;
        rsiArr.push(al===0?100:100-(100/(1+ag/al)));
      }
      const stochArr=[];
      for(let i=stochP-1;i<rsiArr.length;i++){
        const w=rsiArr.slice(i-stochP+1,i+1);
        const hi=Math.max(...w),lo=Math.min(...w);
        stochArr.push(hi===lo?50:(rsiArr[i]-lo)/(hi-lo)*100);
      }
      const kArr=[];
      for(let i=kP-1;i<stochArr.length;i++)
        kArr.push(stochArr.slice(i-kP+1,i+1).reduce((a,b)=>a+b,0)/kP);
      const dArr=[];
      for(let i=dP-1;i<kArr.length;i++)
        dArr.push(kArr.slice(i-dP+1,i+1).reduce((a,b)=>a+b,0)/dP);
      const K=kArr[kArr.length-1]||50, D=dArr[dArr.length-1]||50;
      const zone=K<20&&D<20?'OVERSOLD':K>80&&D>80?'OVERBOUGHT':'NEUTRAL';
      return {k:Math.round(K),d:Math.round(D),zone};
    }

    const rsi4h=rsi(k4h), rsi1h=rsi(k1h), rsi15m=rsi(k15m), rsi5m=rsi(k5m);
    const ema20_4h=ema(k4h,20), ema50_4h=ema(k4h,50), ema200_4h=ema(k4h,200);
    const ema20_1h=ema(k1h,20), ema50_1h=ema(k1h,50), ema200_1h=ema(k1h,200);
    const ema20_15m=ema(k15m,20), ema50_15m=ema(k15m,50);
    const vwap1h=vwap(k1h), vwap4h=vwap(k4h);
    const bb4h=bollinger(k4h), bb1h=bollinger(k1h), bb15m_=bollinger(k15m);
    const atr4h=atr(k4h), atr1h=atr(k1h);
    const macd4h=macd(k4h), macd1h=macd(k1h);
    const stoch15m=stochRSI(k15m), stoch1h=stochRSI(k1h);

    // ─────────────────────────────────────────────────────────────────────────
    // 2. CVD — Cumulative Volume Delta
    // Taker buy vol - taker sell vol. Binance aggTrades: m=true → SELL
    // ─────────────────────────────────────────────────────────────────────────
    let cvdBuy=0, cvdSell=0;
    aggTrades.forEach(t=>{
      const qty=parseFloat(t.q)*parseFloat(t.p); // USDT hacim
      if(t.m) cvdSell+=qty; // maker=true → taker SOLD
      else    cvdBuy+=qty;  // maker=false → taker BOUGHT
    });
    const cvdTotal=cvdBuy-cvdSell;
    const cvdRatio=cvdBuy+cvdSell>0?cvdBuy/(cvdBuy+cvdSell)*100:50;
    // CVD trend: son 100 vs önceki 200 trade
    const recent=aggTrades.slice(-100), older=aggTrades.slice(-300,-100);
    let recBuy=0,recSell=0,oldBuy=0,oldSell=0;
    recent.forEach(t=>{const q=parseFloat(t.q)*parseFloat(t.p);t.m?recSell+=q:recBuy+=q;});
    older.forEach(t=>{const q=parseFloat(t.q)*parseFloat(t.p);t.m?oldSell+=q:oldBuy+=q;});
    const cvdMomentum = recBuy-recSell > (oldBuy-oldSell)*1.2 ? 'ACCELERATING_BULL' :
                        recBuy-recSell < (oldBuy-oldSell)*0.8 ? 'ACCELERATING_BEAR' :
                        cvdTotal>0 ? 'POSITIVE' : 'NEGATIVE';

    // ─────────────────────────────────────────────────────────────────────────
    // 3. FUNDING RATE ANALİZİ
    // Negatif funding → long için ödeme yok, short sıkıştırılabilir
    // Çok pozitif funding → short fırsatı, long çok kalabalık
    // ─────────────────────────────────────────────────────────────────────────
    const currentFunding = fundArr.length ? parseFloat(fundArr[fundArr.length-1].fundingRate)*100 : 0;
    const avgFunding = fundArr.length ? fundArr.reduce((s,f)=>s+parseFloat(f.fundingRate)*100,0)/fundArr.length : 0;
    const fundingTrend = fundArr.length>=2 ?
      (parseFloat(fundArr[fundArr.length-1].fundingRate) > parseFloat(fundArr[fundArr.length-2].fundingRate) ? 'RISING' : 'FALLING') : 'FLAT';

    // Funding sinyali (annualized oran)
    const fundingAnnualized = currentFunding * 3 * 365; // 8 saatlik → yıllık
    const fundingSignal =
      currentFunding < -0.03 ? 'EXTREME_NEGATIVE' :  // Güçlü long fırsatı
      currentFunding < -0.01 ? 'NEGATIVE' :           // Long fırsatı
      currentFunding > 0.1   ? 'EXTREME_POSITIVE' :   // Güçlü short fırsatı
      currentFunding > 0.05  ? 'POSITIVE' :           // Short fırsatı
      'NEUTRAL';

    // ─────────────────────────────────────────────────────────────────────────
    // 4. OPEN INTEREST ANALİZİ
    // OI artarken fiyat artıyor → gerçek alım (bullish)
    // OI artarken fiyat düşüyor → gerçek satım (bearish)
    // OI azalırken fiyat artıyor → short squeeze
    // OI azalırken fiyat düşüyor → long liquidation
    // ─────────────────────────────────────────────────────────────────────────
    let oiChange1h=0, oiChange4h=0, oiDelta=0;
    if (oiHist.length >= 2) {
      const latest=parseFloat(oiHist[oiHist.length-1].sumOpenInterestValue||oiHist[oiHist.length-1].sumOpenInterest||0);
      const oneBack=parseFloat(oiHist[oiHist.length-2].sumOpenInterestValue||oiHist[oiHist.length-2].sumOpenInterest||0);
      const fourBack=parseFloat(oiHist[Math.max(0,oiHist.length-5)].sumOpenInterestValue||oiHist[Math.max(0,oiHist.length-5)].sumOpenInterest||0);
      oiChange1h=oneBack>0?(latest-oneBack)/oneBack*100:0;
      oiChange4h=fourBack>0?(latest-fourBack)/fourBack*100:0;
      oiDelta=latest-oneBack;
    }

    // Price-OI divergence (Smart Money tespiti)
    const priceChange1h = k1h.length>=2 ?
      (parseFloat(k1h[k1h.length-1][4])-parseFloat(k1h[k1h.length-2][4]))/parseFloat(k1h[k1h.length-2][4])*100 : 0;

    const oiDivergence =
      oiChange1h>1 && priceChange1h>0.5 ? 'CONFIRMED_BULL' :    // OI+fiyat artıyor → güçlü long
      oiChange1h>1 && priceChange1h<-0.5? 'CONFIRMED_BEAR' :    // OI artıyor fiyat düşüyor → güçlü short
      oiChange1h<-1&& priceChange1h>0.5 ? 'SHORT_SQUEEZE' :     // OI azalıyor fiyat artıyor → short squeeze
      oiChange1h<-1&& priceChange1h<-0.5? 'LONG_LIQUIDATION' :  // OI azalıyor fiyat düşüyor → long liq
      'NEUTRAL';

    // ─────────────────────────────────────────────────────────────────────────
    // 5. SMART MONEY — Büyük oyuncu tespiti
    // topLongShortPositionRatio: büyük oyuncuların L/S pozisyon oranı
    // globalLongShortAccountRatio: tüm hesaplar
    // ─────────────────────────────────────────────────────────────────────────
    const globalLS = lsGlobal.length ? parseFloat(lsGlobal[lsGlobal.length-1].longShortRatio||1) : 1;
    const topLS    = lsTop.length    ? parseFloat(lsTop[lsTop.length-1].longShortRatio||1)    : 1;
    const globalLong=lsGlobal.length?parseFloat(lsGlobal[lsGlobal.length-1].longAccount||50)*100:50;
    const topLong  =lsTop.length?parseFloat(lsTop[lsTop.length-1].longAccount||50)*100:50;

    // Smart Money Divergence: büyük oyuncular bir yönde, perakende ters
    // Büyük oyuncular long > perakende long → smart money bullish
    const smDivergence = topLong>55 && globalLong<45 ? 'SMART_BULL' :   // Büyükler long, perakende short
                         topLong<45 && globalLong>55 ? 'SMART_BEAR' :   // Büyükler short, perakende long
                         topLong>60 ? 'WHALE_LONG' :                     // Büyük oyuncular çok long
                         topLong<40 ? 'WHALE_SHORT' : 'NEUTRAL';

    // L/S trend değişimi (son 3 periyot)
    const lsTrend = lsTop.length>=3 ?
      (parseFloat(lsTop[lsTop.length-1].longAccount) > parseFloat(lsTop[lsTop.length-3].longAccount) ? 'SMART_ACCUMULATING' : 'SMART_DISTRIBUTING') : 'UNKNOWN';

    // ─────────────────────────────────────────────────────────────────────────
    // 6. TAKER BUY/SELL ORANI — Agresif alım/satım tespiti
    // ─────────────────────────────────────────────────────────────────────────
    let takerBuyPct=50, takerTrend='NEUTRAL';
    if (takerRatio.length) {
      const recent3=takerRatio.slice(-3);
      const buyVols=recent3.map(t=>parseFloat(t.buyVol||0));
      const sellVols=recent3.map(t=>parseFloat(t.sellVol||0));
      const totalBuy=buyVols.reduce((a,b)=>a+b,0);
      const totalSell=sellVols.reduce((a,b)=>a+b,0);
      takerBuyPct=totalBuy+totalSell>0?totalBuy/(totalBuy+totalSell)*100:50;
      takerTrend = takerBuyPct>60 ? 'AGGRESSIVE_BUYING' :
                   takerBuyPct<40 ? 'AGGRESSIVE_SELLING' : 'NEUTRAL';
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 7. ORDER BOOK ANALİZİ — Likidite duvarları, iceberg emirleri
    // ─────────────────────────────────────────────────────────────────────────
    const bids = Array.isArray(depth.bids) ? depth.bids.slice(0,20) : [];
    const asks = Array.isArray(depth.asks) ? depth.asks.slice(0,20) : [];

    let totalBidUSDT=0, totalAskUSDT=0;
    bids.forEach(([p,q])=>totalBidUSDT+=parseFloat(p)*parseFloat(q));
    asks.forEach(([p,q])=>totalAskUSDT+=parseFloat(p)*parseFloat(q));

    const bookImbalance = totalBidUSDT+totalAskUSDT>0 ?
      (totalBidUSDT-totalAskUSDT)/(totalBidUSDT+totalAskUSDT)*100 : 0;

    // En büyük bid/ask duvarı (iceberg tespiti)
    const maxBidWall = bids.reduce((m,[p,q])=>Math.max(m,parseFloat(p)*parseFloat(q)),0);
    const maxAskWall = asks.reduce((m,[p,q])=>Math.max(m,parseFloat(p)*parseFloat(q)),0);
    const hasStrongBid = maxBidWall > totalBidUSDT*0.3;   // tek seviye >%30 → büyük alıcı
    const hasStrongAsk = maxAskWall > totalAskUSDT*0.3;   // tek seviye >%30 → büyük satıcı

    const bookSignal = bookImbalance > 20 ? 'STRONG_BIDS' :
                       bookImbalance < -20? 'STRONG_ASKS' :
                       bookImbalance > 10 ? 'SLIGHT_BIDS' :
                       bookImbalance < -10? 'SLIGHT_ASKS' : 'BALANCED';

    // ─────────────────────────────────────────────────────────────────────────
    // 8. HACİM PROFİLİ & MOMENTUM
    // ─────────────────────────────────────────────────────────────────────────
    function volProfile(klines) {
      if (klines.length < 5) return { above: false, ratio: 1, surge: false };
      const vols = klines.map(k=>parseFloat(k[5]));
      const avg20 = vols.slice(-21,-1).reduce((a,b)=>a+b,0)/20;
      const last  = vols[vols.length-1];
      const ratio = avg20>0 ? last/avg20 : 1;
      return { ratio: parseFloat(ratio.toFixed(2)), surge: ratio>1.5, above: ratio>1 };
    }

    const vol4h=volProfile(k4h), vol1h=volProfile(k1h), vol15m_=volProfile(k15m);

    // ─────────────────────────────────────────────────────────────────────────
    // 9. DESTEK/DİRENÇ SEVİYELERİ
    // Pivot noktaları + son yüksek/düşükler
    // ─────────────────────────────────────────────────────────────────────────
    function pivots(klines, n=20) {
      if (klines.length < n) return { sup: [], res: [] };
      const recent = klines.slice(-n);
      const highs = recent.map(k=>parseFloat(k[2]));
      const lows  = recent.map(k=>parseFloat(k[3]));
      const H=Math.max(...highs), L=Math.min(...lows);
      const C=parseFloat(recent[recent.length-1][4]);
      const P=(H+L+C)/3;
      return {
        pivot: P,
        r1: 2*P-L, r2: P+(H-L), r3: H+2*(P-L),
        s1: 2*P-H, s2: P-(H-L), s3: L-2*(H-P),
        rangeH: H, rangeL: L
      };
    }

    const piv4h=pivots(k4h), piv1h=pivots(k1h);
    const nearSupport = lastPrice && piv1h.s1 ? Math.abs(lastPrice-piv1h.s1)/lastPrice*100 < 1 : false;
    const nearResist  = lastPrice && piv1h.r1 ? Math.abs(lastPrice-piv1h.r1)/lastPrice*100 < 1 : false;

    // ─────────────────────────────────────────────────────────────────────────
    // 10. SMART MONEY CONCEPTS (SMC) — Temel tespitler
    // Break of Structure (BOS), Change of Character (ChoCH)
    // ─────────────────────────────────────────────────────────────────────────
    function detectSMC(klines) {
      if (klines.length < 10) return { bos: 'NONE', choch: false, swing: 'NONE' };
      const recent = klines.slice(-10);
      const highs = recent.map(k=>parseFloat(k[2]));
      const lows  = recent.map(k=>parseFloat(k[3]));
      const closes= recent.map(k=>parseFloat(k[4]));

      // Higher highs, lower lows tespiti
      const HH = highs[9]>highs[7] && highs[7]>highs[5];
      const HL = lows[9]>lows[7]   && lows[7]>lows[5];
      const LL = lows[9]<lows[7]   && lows[7]<lows[5];
      const LH = highs[9]<highs[7] && highs[7]<highs[5];

      const bos = HH&&HL?'BULLISH_BOS': LL&&LH?'BEARISH_BOS':'NONE';

      // Change of character: düşüş trendinde yeni yüksek
      const prevTrend = closes[4]<closes[0]?'BEAR':'BULL';
      const curPrice  = closes[9];
      const choch = prevTrend==='BEAR' && curPrice>Math.max(...highs.slice(0,5));

      return { bos, choch, swing: HH&&HL?'UP':LL&&LH?'DOWN':'RANGE' };
    }

    const smc4h=detectSMC(k4h), smc1h=detectSMC(k1h), smc15m_=detectSMC(k15m);

    // ─────────────────────────────────────────────────────────────────────────
    // 11. LİKİDASYON SEVİYELERİ TAHMİNİ
    // Mevcut OI + ortalama kaldıraç varsayımıyla cluster tespiti
    // ─────────────────────────────────────────────────────────────────────────
    const liqLong5x  = lastPrice * 0.80;  // 5x long liq zone
    const liqLong10x = lastPrice * 0.90;  // 10x long liq zone
    const liqLong20x = lastPrice * 0.95;  // 20x long liq zone
    const liqShort5x = lastPrice * 1.20;
    const liqShort10x= lastPrice * 1.10;
    const liqShort20x= lastPrice * 1.05;

    // ─────────────────────────────────────────────────────────────────────────
    // 12. SKOR HESAPLAMA — Pro ağırlıklı sistem
    // ─────────────────────────────────────────────────────────────────────────
    let longScore=0, shortScore=0;
    const signals = { long:[], short:[] };

    // ── TREND (4h ağırlıklı) ─────────────────────────────────────────────────
    const trend4hUp = ema20_4h > ema50_4h && ema50_4h > ema200_4h;
    const trend4hDn = ema20_4h < ema50_4h && ema50_4h < ema200_4h;
    if (trend4hUp)  { longScore+=15; signals.long.push('4h EMA stack ↑'); }
    if (trend4hDn)  { shortScore+=15; signals.short.push('4h EMA stack ↓'); }

    const trend1hUp = ema20_1h > ema50_1h;
    const trend1hDn = ema20_1h < ema50_1h;
    if (trend1hUp)  { longScore+=10; signals.long.push('1h EMA ↑'); }
    if (trend1hDn)  { shortScore+=10; signals.short.push('1h EMA ↓'); }

    // ── RSI DİVERJANS & SEVİYELER ────────────────────────────────────────────
    if (rsi4h<35)        { longScore+=12; signals.long.push(`4h RSI oversold ${rsi4h}`); }
    else if(rsi4h>65)    { shortScore+=12; signals.short.push(`4h RSI overbought ${rsi4h}`); }
    if (rsi1h<40)        { longScore+=8; signals.long.push(`1h RSI ${rsi1h}`); }
    else if(rsi1h>60)    { shortScore+=8; signals.short.push(`1h RSI ${rsi1h}`); }
    if (rsi15m<30)       { longScore+=6; signals.long.push(`15m RSI oversold ${rsi15m}`); }
    else if(rsi15m>70)   { shortScore+=6; signals.short.push(`15m RSI overbought ${rsi15m}`); }

    // ── MACD ─────────────────────────────────────────────────────────────────
    if (macd4h.cross==='BULL'){ longScore+=12; signals.long.push('4h MACD bullish cross'); }
    if (macd4h.cross==='BEAR'){ shortScore+=12; signals.short.push('4h MACD bearish cross'); }
    if (macd4h.hist>0)        { longScore+=5; }
    if (macd4h.hist<0)        { shortScore+=5; }
    if (macd1h.cross==='BULL'){ longScore+=8; signals.long.push('1h MACD cross'); }
    if (macd1h.cross==='BEAR'){ shortScore+=8; signals.short.push('1h MACD cross'); }

    // ── STOCHASTIC RSI ────────────────────────────────────────────────────────
    if (stoch1h.zone==='OVERSOLD')    { longScore+=8; signals.long.push('1h StochRSI oversold'); }
    if (stoch1h.zone==='OVERBOUGHT')  { shortScore+=8; signals.short.push('1h StochRSI overbought'); }
    if (stoch15m.zone==='OVERSOLD')   { longScore+=5; }
    if (stoch15m.zone==='OVERBOUGHT') { shortScore+=5; }

    // ── CVD (en önemli gösterge) ─────────────────────────────────────────────
    if (cvdMomentum==='ACCELERATING_BULL') { longScore+=15; signals.long.push('CVD acc. buy'); }
    if (cvdMomentum==='ACCELERATING_BEAR') { shortScore+=15; signals.short.push('CVD acc. sell'); }
    if (cvdMomentum==='POSITIVE')          { longScore+=6; }
    if (cvdMomentum==='NEGATIVE')          { shortScore+=6; }
    if (cvdRatio>60)                       { longScore+=5; signals.long.push(`CVD buy ${cvdRatio.toFixed(0)}%`); }
    if (cvdRatio<40)                       { shortScore+=5; signals.short.push(`CVD sell ${(100-cvdRatio).toFixed(0)}%`); }

    // ── FUNDING RATE ─────────────────────────────────────────────────────────
    if (fundingSignal==='EXTREME_NEGATIVE') { longScore+=12; signals.long.push('Funding aşırı negatif'); }
    if (fundingSignal==='NEGATIVE')         { longScore+=7;  signals.long.push('Funding negatif'); }
    if (fundingSignal==='EXTREME_POSITIVE') { shortScore+=12; signals.short.push('Funding aşırı pozitif'); }
    if (fundingSignal==='POSITIVE')         { shortScore+=7;  signals.short.push('Funding pozitif'); }

    // ── OPEN INTEREST + FİYAT UYUMU ──────────────────────────────────────────
    if (oiDivergence==='CONFIRMED_BULL')  { longScore+=12; signals.long.push('OI+Fiyat ↑ (gerçek alım)'); }
    if (oiDivergence==='CONFIRMED_BEAR')  { shortScore+=12; signals.short.push('OI+Fiyat ↓ (gerçek satım)'); }
    if (oiDivergence==='SHORT_SQUEEZE')   { longScore+=8;  signals.long.push('Short squeeze başlıyor'); }
    if (oiDivergence==='LONG_LIQUIDATION'){ shortScore+=8; signals.short.push('Long liq. başlıyor'); }
    if (oiChange1h>2)  { longScore+=4; }
    if (oiChange1h<-2) { shortScore+=4; }

    // ── SMART MONEY / WHALE HAREKET ───────────────────────────────────────────
    if (smDivergence==='SMART_BULL') { longScore+=15; signals.long.push('Whale uzun, perakende kısa'); }
    if (smDivergence==='SMART_BEAR') { shortScore+=15; signals.short.push('Whale kısa, perakende uzun'); }
    if (smDivergence==='WHALE_LONG') { longScore+=8; signals.long.push('Büyük oyuncu long'); }
    if (smDivergence==='WHALE_SHORT'){ shortScore+=8; signals.short.push('Büyük oyuncu short'); }
    if (lsTrend==='SMART_ACCUMULATING') { longScore+=5; }
    if (lsTrend==='SMART_DISTRIBUTING') { shortScore+=5; }

    // ── ORDER BOOK ────────────────────────────────────────────────────────────
    if (bookSignal==='STRONG_BIDS')  { longScore+=8; signals.long.push('Büyük alım duvarı'); }
    if (bookSignal==='STRONG_ASKS')  { shortScore+=8; signals.short.push('Büyük satış duvarı'); }
    if (bookSignal==='SLIGHT_BIDS')  { longScore+=3; }
    if (bookSignal==='SLIGHT_ASKS')  { shortScore+=3; }
    if (hasStrongBid)                { longScore+=5; signals.long.push('Iceberg alıcı'); }
    if (hasStrongAsk)                { shortScore+=5; signals.short.push('Iceberg satıcı'); }

    // ── TAKER FLOW ────────────────────────────────────────────────────────────
    if (takerTrend==='AGGRESSIVE_BUYING')  { longScore+=8; signals.long.push(`Agresif alım %${takerBuyPct.toFixed(0)}`); }
    if (takerTrend==='AGGRESSIVE_SELLING') { shortScore+=8; signals.short.push(`Agresif satım %${(100-takerBuyPct).toFixed(0)}`); }

    // ── SMC — BREAK OF STRUCTURE ──────────────────────────────────────────────
    if (smc4h.bos==='BULLISH_BOS')  { longScore+=10; signals.long.push('4h BOS yukarı'); }
    if (smc4h.bos==='BEARISH_BOS')  { shortScore+=10; signals.short.push('4h BOS aşağı'); }
    if (smc1h.bos==='BULLISH_BOS')  { longScore+=6; signals.long.push('1h BOS'); }
    if (smc1h.bos==='BEARISH_BOS')  { shortScore+=6; signals.short.push('1h BOS'); }
    if (smc4h.choch)                { longScore+=8; signals.long.push('ChoCH tespiti'); }

    // ── BOLLINGER BANDS ───────────────────────────────────────────────────────
    if (lastPrice < bb4h.lower*1.01)  { longScore+=8; signals.long.push('4h BB alt bant'); }
    if (lastPrice > bb4h.upper*0.99)  { shortScore+=8; signals.short.push('4h BB üst bant'); }
    if (lastPrice < bb1h.lower*1.005) { longScore+=5; }
    if (lastPrice > bb1h.upper*0.995) { shortScore+=5; }

    // ── VWAP ─────────────────────────────────────────────────────────────────
    if (lastPrice > vwap1h*1.001)     { longScore+=4; }
    if (lastPrice < vwap1h*0.999)     { shortScore+=4; }

    // ── DESTEK/DİRENÇ ────────────────────────────────────────────────────────
    if (nearSupport) { longScore+=8; signals.long.push('Destek seviyesinde'); }
    if (nearResist)  { shortScore+=8; signals.short.push('Direnç seviyesinde'); }

    // ── HACİM ────────────────────────────────────────────────────────────────
    if (vol1h.surge) { longScore+=4; shortScore+=4; } // Her iki yön de güçlenebilir

    // NORMALIZE
    longScore  = Math.min(Math.round(longScore),  100);
    shortScore = Math.min(Math.round(shortScore), 100);

    const recommendation =
      longScore  > shortScore && longScore  >= 50 ? 'LONG'  :
      shortScore > longScore  && shortScore >= 50 ? 'SHORT' : 'WAIT';

    // ─────────────────────────────────────────────────────────────────────────
    // YANIT
    // ─────────────────────────────────────────────────────────────────────────
    res.json({
      ok: true,
      symbol: full,
      price: lastPrice,

      // Klasik teknik
      timeframes: {
        '4h':  { rsi:rsi4h,  ema20:+ema20_4h.toFixed(4),  ema50:+ema50_4h.toFixed(4),  ema200:+ema200_4h.toFixed(4),  trend:trend4hUp?'UP':trend4hDn?'DOWN':'RANGE', macd:macd4h.cross,  bb:bb4h,  atr:+atr4h.toFixed(4), vwap:+vwap4h.toFixed(4) },
        '1h':  { rsi:rsi1h,  ema20:+ema20_1h.toFixed(4),  ema50:+ema50_1h.toFixed(4),  ema200:+ema200_1h.toFixed(4),  trend:trend1hUp?'UP':trend1hDn?'DOWN':'RANGE', macd:macd1h.cross,  bb:bb1h,  atr:+atr1h.toFixed(4), vwap:+vwap1h.toFixed(4), stoch:stoch1h },
        '15m': { rsi:rsi15m, ema20:+ema20_15m.toFixed(4), ema50:+ema50_15m.toFixed(4), bb:bb15m_, vol:vol15m_, stoch:stoch15m, smc:smc15m_ },
        '5m':  { rsi:rsi5m },
      },

      // CVD — pro gösterge
      cvd: {
        total: +cvdTotal.toFixed(0),
        buyRatio: +cvdRatio.toFixed(1),
        momentum: cvdMomentum,
        buyVol: +cvdBuy.toFixed(0),
        sellVol: +cvdSell.toFixed(0),
      },

      // Funding rate
      funding: {
        current: +currentFunding.toFixed(4),
        avg10:   +avgFunding.toFixed(4),
        trend:   fundingTrend,
        signal:  fundingSignal,
        annualized: +fundingAnnualized.toFixed(1),
      },

      // Open Interest
      openInterest: {
        current: oiNow,
        change1h: +oiChange1h.toFixed(2),
        change4h: +oiChange4h.toFixed(2),
        divergence: oiDivergence,
      },

      // Smart Money
      smartMoney: {
        topLongPct:    +topLong.toFixed(1),
        topShortPct:   +(100-topLong).toFixed(1),
        globalLongPct: +globalLong.toFixed(1),
        globalShortPct:+(100-globalLong).toFixed(1),
        divergence:    smDivergence,
        trend:         lsTrend,
        topLS:         +topLS.toFixed(2),
        globalLS:      +globalLS.toFixed(2),
      },

      // Order Book
      orderBook: {
        imbalance: +bookImbalance.toFixed(1),
        signal:    bookSignal,
        bidWall:   hasStrongBid,
        askWall:   hasStrongAsk,
        totalBidUSDT: +totalBidUSDT.toFixed(0),
        totalAskUSDT: +totalAskUSDT.toFixed(0),
      },

      // Taker flow
      takerFlow: {
        buyPct: +takerBuyPct.toFixed(1),
        trend:  takerTrend,
      },

      // SMC
      smc: { '4h': smc4h, '1h': smc1h },

      // Pivotlar
      pivots: { '1h': piv1h, '4h': piv4h },

      // Likidite seviyeleri
      liquidations: {
        longZones:  [+liqLong20x.toFixed(4), +liqLong10x.toFixed(4), +liqLong5x.toFixed(4)],
        shortZones: [+liqShort20x.toFixed(4),+liqShort10x.toFixed(4),+liqShort5x.toFixed(4)],
      },

      // Skorlar
      longScore, shortScore, recommendation,
      signals: recommendation==='LONG' ? signals.long : signals.short,
    });

  } catch(e) {
    res.status(400).json({ error: e.message });
  }
});

// ── HESAP ─────────────────────────────────────────────────────────────────────
app.post('/api/account', async (req, res) => {
  const { apiKey, apiSecret } = req.body;
  if (!apiKey || !apiSecret) return res.status(400).json({ error: 'API key gerekli' });
  try {
    let walletBal=0, availBal=0, unrealized=0;
    try {
      const b = await bReq(apiKey, apiSecret, 'GET', '/fapi/v3/balance');
      const u = Array.isArray(b) ? b.find(x=>x.asset==='USDT') : null;
      if(u){ walletBal=parseFloat(u.balance)||0; availBal=parseFloat(u.availableBalance)||0; }
    } catch(e){}
    const data = await bReq(apiKey, apiSecret, 'GET', '/fapi/v2/account');
    if(parseFloat(data.totalWalletBalance)>0) walletBal=parseFloat(data.totalWalletBalance);
    if(parseFloat(data.availableBalance)>0)   availBal=parseFloat(data.availableBalance);
    unrealized=parseFloat(data.totalUnrealizedProfit)||0;
    res.json({ ok:true, totalWalletBalance:walletBal, availableBalance:availBal,
      totalUnrealizedProfit:unrealized,
      positions:(data.positions||[]).filter(p=>parseFloat(p.positionAmt)!==0).map(p=>({
        symbol:p.symbol, side:parseFloat(p.positionAmt)>0?'LONG':'SHORT',
        positionAmt:Math.abs(parseFloat(p.positionAmt)),
        entryPrice:parseFloat(p.entryPrice), markPrice:parseFloat(p.markPrice),
        unrealizedProfit:parseFloat(p.unRealizedProfit),
        leverage:parseInt(p.leverage), liquidationPrice:parseFloat(p.liquidationPrice),
      }))
    });
  } catch(e){ res.status(400).json({ error:e.message }); }
});

// ── EMİR AÇ (MARKET) ─────────────────────────────────────────────────────────
app.post('/api/order', async (req, res) => {
  const { apiKey, apiSecret, symbol, side, leverage, marginType,
          targetPrice, stopPrice, usdtAmount } = req.body;
  if (!apiKey||!apiSecret||!symbol||!side||!leverage||!targetPrice||!stopPrice||!usdtAmount)
    return res.status(400).json({ error: 'Eksik parametre' });

  const sym    = symbol.toUpperCase().includes('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  const isLong = side.toUpperCase()==='LONG';

  try {
    if(marginType){
      try{ await bReq(apiKey,apiSecret,'POST','/fapi/v1/marginType',{symbol:sym,marginType:marginType.toUpperCase()}); }
      catch(e){ if(!e.message.includes('No need'))console.log('MarginType:',e.message); }
    }
    await bReq(apiKey,apiSecret,'POST','/fapi/v1/leverage',{symbol:sym,leverage:parseInt(leverage)});

    let stepSize=0.001, tickSize=0.01, minNot=5;
    try {
      const si = await bPub('/fapi/v1/exchangeInfo', 'symbol='+sym);
      const s  = Array.isArray(si.symbols) ? si.symbols.find(x=>x.symbol===sym) : null;
      if(s){
        const lf=s.filters.find(f=>f.filterType==='LOT_SIZE');
        const pf=s.filters.find(f=>f.filterType==='PRICE_FILTER');
        const mf=s.filters.find(f=>f.filterType==='MIN_NOTIONAL');
        if(lf)stepSize=parseFloat(lf.stepSize);
        if(pf)tickSize=parseFloat(pf.tickSize);
        if(mf)minNot=parseFloat(mf.notional||mf.minNotional||5);
      }
    } catch(e){}

    const priceRes  = await bPub('/fapi/v1/ticker/price','symbol='+sym);
    const curPrice  = parseFloat(priceRes.price)||0;
    if(!curPrice) throw new Error('Fiyat alınamadı');

    const qtyPrec = stepSize<1?-Math.floor(Math.log10(stepSize)):0;
    const prcPrec = tickSize<1?-Math.floor(Math.log10(tickSize)):0;
    const qty     = parseFloat(((parseFloat(usdtAmount)*parseInt(leverage))/curPrice).toFixed(qtyPrec));
    const rnd     = p => parseFloat(parseFloat(p).toFixed(prcPrec));

    if(qty*curPrice<minNot) throw new Error(`Min işlem $${minNot}. Miktarı artır.`);

    const oSide=isLong?'BUY':'SELL', cSide=isLong?'SELL':'BUY';

    const main = await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',{
      symbol:sym, side:oSide, type:'MARKET', quantity:qty, positionSide:'BOTH'
    });

    let tp={orderId:null}, sl={orderId:null};
    try {
      tp = await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',{
        symbol:sym, side:cSide, type:'TAKE_PROFIT_MARKET',
        stopPrice:rnd(targetPrice), closePosition:'true', positionSide:'BOTH',
        timeInForce:'GTE_GTC', workingType:'MARK_PRICE'
      });
    } catch(e){ console.log('TP:',e.message); }
    try {
      sl = await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',{
        symbol:sym, side:cSide, type:'STOP_MARKET',
        stopPrice:rnd(stopPrice), closePosition:'true', positionSide:'BOTH',
        timeInForce:'GTE_GTC', workingType:'MARK_PRICE'
      });
    } catch(e){ console.log('SL:',e.message); }

    res.json({ ok:true, message:`${sym} ${side} açıldı ✅`,
      mainOrderId:main.orderId, tpOrderId:tp.orderId, slOrderId:sl.orderId,
      executedPrice:parseFloat(main.avgPrice||curPrice),
      details:{symbol:sym,side,quantity:qty,leverage,entry:curPrice,target:rnd(targetPrice),stop:rnd(stopPrice)}
    });
  } catch(e){ res.status(400).json({ error:e.message }); }
});

// ── POZİSYONLAR ──────────────────────────────────────────────────────────────
app.post('/api/positions', async (req, res) => {
  const { apiKey, apiSecret } = req.body;
  if(!apiKey||!apiSecret) return res.status(400).json({ error:'API key gerekli' });
  try {
    const data = await bReq(apiKey,apiSecret,'GET','/fapi/v2/positionRisk');
    const open = Array.isArray(data)?data.filter(p=>parseFloat(p.positionAmt)!==0).map(p=>{
      const amt=parseFloat(p.positionAmt),ep=parseFloat(p.entryPrice),mp=parseFloat(p.markPrice),lev=parseInt(p.leverage);
      const side=amt>0?'LONG':'SHORT';
      const pct=ep>0?((mp-ep)/ep*100*lev*(side==='SHORT'?-1:1)).toFixed(2):'0';
      return{symbol:p.symbol,side,positionAmt:Math.abs(amt),entryPrice:ep,markPrice:mp,
        unrealizedProfit:parseFloat(p.unRealizedProfit),pnlPct:parseFloat(pct),
        leverage:lev,liquidationPrice:parseFloat(p.liquidationPrice)};
    }):[];
    res.json({ ok:true, positions:open });
  } catch(e){ res.status(400).json({ error:e.message }); }
});

// ── KAPAT ─────────────────────────────────────────────────────────────────────
app.post('/api/close', async (req, res) => {
  const { apiKey, apiSecret, symbol } = req.body;
  if(!apiKey||!apiSecret||!symbol) return res.status(400).json({ error:'Eksik parametre' });
  const sym=symbol.toUpperCase().includes('USDT')?symbol.toUpperCase():symbol.toUpperCase()+'USDT';
  try {
    try{ await bReq(apiKey,apiSecret,'DELETE','/fapi/v1/allOpenOrders',{symbol:sym}); }catch(e){}
    const pos = await bReq(apiKey,apiSecret,'GET','/fapi/v2/positionRisk',{symbol:sym});
    const arr = Array.isArray(pos)?pos:[];
    const p   = arr.find(x=>Math.abs(parseFloat(x.positionAmt))>0);
    if(!p) return res.json({ ok:true, message:'Açık pozisyon yok' });
    const qty=Math.abs(parseFloat(p.positionAmt));
    const order=await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',{
      symbol:sym,side:parseFloat(p.positionAmt)>0?'SELL':'BUY',
      type:'MARKET',quantity:qty,reduceOnly:'true',positionSide:'BOTH'
    });
    res.json({ ok:true, message:`${sym} kapatıldı`, orderId:order.orderId });
  } catch(e){ res.status(400).json({ error:e.message }); }
});

app.listen(PORT, ()=>console.log(`✅ Server ${PORT}`));
