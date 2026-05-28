const express = require('express');
const cors    = require('cors');
const crypto  = require('crypto');
const fetch   = require('node-fetch');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());
const PORT = process.env.PORT || 3000;
const FAPI = 'https://fapi.binance.com';
const FAPI_WS = 'wss://fstream.binance.com/stream';

// ── CACHE ─────────────────────────────────────────────────────────────────────
const cache = new Map();
async function cached(key, ttl, fn) {
  const now = Date.now();
  if (cache.has(key)) { const {val,exp}=cache.get(key); if(now<exp)return val; }
  const val = await fn();
  cache.set(key, { val, exp: now+ttl });
  return val;
}

// ── RATE LIMIT ────────────────────────────────────────────────────────────────
let reqCount=0, reqWindow=Date.now();
// ── ALGO ORDER (TP/SL için yeni coinlerde) ───────────────────────────────────
// Binance /fapi/v1/order/algo:
// - Signature ve timestamp URL query string'de
// - Diğer parametreler JSON body'de
async function bAlgo(apiKey, apiSecret, params) {
  const ts  = Date.now();
  const qs  = `timestamp=${ts}&recvWindow=10000&signature=`;
  const sig = sign(`timestamp=${ts}&recvWindow=10000`, apiSecret);
  const url = `${FAPI}/fapi/v1/order/algo?timestamp=${ts}&recvWindow=10000&signature=${sig}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'X-MBX-APIKEY': apiKey, 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
    signal: AbortSignal.timeout(10000),
  });
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch(e) { throw new Error(`Algo JSON hatası: ${text.substring(0,100)}`); }
  if (data.code && data.code < 0) throw new Error(`${data.msg} (${data.code})`);
  return data;
}

// ── ALGO ORDER (yeni coinler için: OPG, PENDLE, HUSDT vb.) ──────────────────
// Signature query string'de, body JSON — Binance algo endpoint zorunluluğu
async function bAlgo(apiKey, apiSecret, params) {
  const ts  = Date.now();
  const sigStr = `timestamp=${ts}&recvWindow=10000`;
  const sig = sign(sigStr, apiSecret);
  const url = `${FAPI}/fapi/v1/order/algo?timestamp=${ts}&recvWindow=10000&signature=${sig}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'X-MBX-APIKEY': apiKey, 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
    signal: AbortSignal.timeout(10000),
  });
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); }
  catch(e) { throw new Error(`Algo JSON hatası: ${text.substring(0,100)}`); }
  if (data.code && data.code < 0) throw new Error(`${data.msg} (${data.code})`);
  return data;
}

async function bPub(path, qs='') {
  const now=Date.now();
  if(now-reqWindow>60000){reqCount=0;reqWindow=now;}
  reqCount++;
  if(reqCount>800){const w=60000-(now-reqWindow);await new Promise(r=>setTimeout(r,w+1000));reqCount=0;reqWindow=Date.now();}
  await new Promise(r=>setTimeout(r,80));
  const url=`${FAPI}${path}${qs?'?'+qs:''}`;
  const r=await fetch(url,{signal:AbortSignal.timeout(10000)});
  if(r.status===429||r.status===418){const retry=parseInt(r.headers.get('Retry-After')||'30');await new Promise(r=>setTimeout(r,retry*1000));return bPub(path,qs);}
  const text=await r.text();
  try{return JSON.parse(text);}catch(e){throw new Error(`JSON hatası: ${text.substring(0,80)}`);}
}

// ── İMZA ─────────────────────────────────────────────────────────────────────
function sign(qs,secret){return crypto.createHmac('sha256',secret).update(qs).digest('hex');}
async function bReq(apiKey,apiSecret,method,path,params={}) {
  const ts=Date.now();
  const obj={...params,timestamp:ts,recvWindow:10000};
  const qs=Object.entries(obj).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join('&');
  const sig=sign(qs,apiSecret);
  const url=`${FAPI}${path}`;
  const fullQs=`${qs}&signature=${sig}`;
  const isGet=method.toUpperCase()==='GET'||method.toUpperCase()==='DELETE';
  const options={method:method.toUpperCase(),headers:{'X-MBX-APIKEY':apiKey,'Content-Type':'application/x-www-form-urlencoded'}};
  const finalUrl=isGet?`${url}?${fullQs}`:url;
  if(!isGet)options.body=fullQs;
  const res=await fetch(finalUrl,options);
  const text=await res.text();
  let data;
  try{data=JSON.parse(text);}catch(e){throw new Error(`JSON hatası: ${text.substring(0,80)}`);}
  if(data.code&&data.code<0)throw new Error(`${data.msg} (${data.code})`);
  return data;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 1. WEBSOCKET CVD — Sürekli bağlı, gerçek zamanlı Cumulative Volume Delta
// ═══════════════════════════════════════════════════════════════════════════════
const cvdStore = new Map(); // symbol → {buy, sell, history:[{ts,buy,sell}], ws}

// ═══════════════════════════════════════════════════════════════════════════════
// LİKİDASYON STREAM — Binance !forceOrder@arr (tüm coinler tek stream)
// Büyük likidasyon gelince cascade tespiti yapılır
// ═══════════════════════════════════════════════════════════════════════════════
const liqStore = new Map(); // symbol → {longLiqs:[], shortLiqs:[], lastCascade:null}
let globalLiqWS = null;

function startGlobalLiqStream() {
  if (globalLiqWS && globalLiqWS.readyState === WebSocket.OPEN) return;
  const ws = new WebSocket('wss://fstream.binance.com/ws/!forceOrder@arr');
  globalLiqWS = ws;

  ws.on('message', (data) => {
    try {
      const d = JSON.parse(data.toString());
      const o = d.o || d; // order objesi
      const sym    = o.s; // BTCUSDT
      const side   = o.S; // BUY (short liq) veya SELL (long liq)
      const price  = parseFloat(o.p);
      const qty    = parseFloat(o.q);
      const usdt   = price * qty;
      const ts     = Date.now();

      if (!liqStore.has(sym)) liqStore.set(sym, { longLiqs:[], shortLiqs:[], lastCascade:null });
      const store = liqStore.get(sym);

      // BUY = short pozisyon likidasyon edildi (fiyat yükseldi, shortlar patladı)
      // SELL = long pozisyon likidasyon edildi (fiyat düştü, longlar patladı)
      if (side === 'BUY')  store.shortLiqs.push({ ts, usdt, price });
      if (side === 'SELL') store.longLiqs.push({ ts, usdt, price });

      // Son 5 dakikayı tut
      const cutoff = ts - 5 * 60 * 1000;
      store.shortLiqs = store.shortLiqs.filter(l => l.ts > cutoff);
      store.longLiqs  = store.longLiqs.filter(l => l.ts > cutoff);

      // Cascade tespiti: 5 dakikada $500K+ tek yönde likidasyon
      const shortTotal = store.shortLiqs.reduce((s, l) => s + l.usdt, 0);
      const longTotal  = store.longLiqs.reduce((s, l) => s + l.usdt, 0);
      if (shortTotal > 500000 || longTotal > 500000) {
        store.lastCascade = {
          ts,
          direction: shortTotal > longTotal ? 'SHORT_CASCADE' : 'LONG_CASCADE',
          amount: Math.max(shortTotal, longTotal),
          signal: shortTotal > longTotal ? 'LONG_FIRSAT' : 'SHORT_FIRSAT',
        };
      }
    } catch(e) {}
  });

  ws.on('close', () => {
    console.log('Liq stream kapandı, yeniden bağlanıyor...');
    setTimeout(startGlobalLiqStream, 3000);
  });
  ws.on('error', () => {});
  console.log('✅ Global likidasyon stream başlatıldı');
  console.log('✅ V3 UI/API/TAB FIX aktif');
}

// Sunucu başladığında likidasyon stream'ini aç
startGlobalLiqStream();

function getLiqData(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase() + 'USDT';
  const store = liqStore.get(full) || { longLiqs:[], shortLiqs:[], lastCascade:null };
  const now = Date.now();
  const cutoff5m = now - 5 * 60 * 1000;
  const cutoff1m = now - 60 * 1000;

  const shortTotal5m = store.shortLiqs.filter(l=>l.ts>cutoff5m).reduce((s,l)=>s+l.usdt,0);
  const longTotal5m  = store.longLiqs.filter(l=>l.ts>cutoff5m).reduce((s,l)=>s+l.usdt,0);
  const shortTotal1m = store.shortLiqs.filter(l=>l.ts>cutoff1m).reduce((s,l)=>s+l.usdt,0);
  const longTotal1m  = store.longLiqs.filter(l=>l.ts>cutoff1m).reduce((s,l)=>s+l.usdt,0);

  const dominance = shortTotal5m > longTotal5m * 1.5 ? 'SHORT_LIQ_DOM' :
                    longTotal5m > shortTotal5m * 1.5 ? 'LONG_LIQ_DOM' : 'BALANCED';

  return {
    shortLiq5m: +shortTotal5m.toFixed(0),
    longLiq5m:  +longTotal5m.toFixed(0),
    shortLiq1m: +shortTotal1m.toFixed(0),
    longLiq1m:  +longTotal1m.toFixed(0),
    dominance,
    cascade: store.lastCascade && (now - store.lastCascade.ts) < 10*60*1000 ? store.lastCascade : null,
  };
}

function startCVDStream(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  if (cvdStore.has(full) && cvdStore.get(full).ws?.readyState === WebSocket.OPEN) return;

  const store = { buy:0, sell:0, history:[], lastReset: Date.now(), ws:null };
  cvdStore.set(full, store);

  const wsUrl = `wss://fstream.binance.com/ws/${full.toLowerCase()}@aggTrade`;
  const ws = new WebSocket(wsUrl);
  store.ws = ws;

  ws.on('message', (data) => {
    try {
      const t = JSON.parse(data.toString());
      const qty = parseFloat(t.q) * parseFloat(t.p); // USDT hacim
      if (t.m) store.sell += qty;  // maker=true → taker SATTI
      else     store.buy  += qty;  // maker=false → taker ALDI

      // Her 5 dakikada bir geçmişe kaydet
      const now = Date.now();
      if (now - store.lastReset > 5 * 60 * 1000) {
        store.history.push({ ts: now, buy: store.buy, sell: store.sell,
          delta: store.buy - store.sell });
        if (store.history.length > 48) store.history.shift(); // 4 saatlik tarih
        store.buy = 0; store.sell = 0;
        store.lastReset = now;
      }
    } catch(e) {}
  });

  ws.on('close', () => {
    console.log(`CVD WS kapandı: ${full}, yeniden bağlanıyor...`);
    setTimeout(() => startCVDStream(full), 3000);
  });
  ws.on('error', () => {});
}

function getCVD(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  startCVDStream(full); // yoksa başlat
  const store = cvdStore.get(full);
  if (!store) return { buy:0, sell:0, ratio:50, momentum:'UNKNOWN', trend:'UNKNOWN', historyLen:0 };

  const buy=store.buy, sell=store.sell;
  const total=buy+sell;
  const ratio=total>0?buy/total*100:50;
  const delta=buy-sell;

  // Tarihsel CVD trendi
  let trend='NEUTRAL', acceleration='NONE';
  if (store.history.length >= 3) {
    const recent = store.history.slice(-3).reduce((s,h)=>s+h.delta,0);
    const older  = store.history.slice(-6,-3).reduce((s,h)=>s+h.delta,0);
    trend = delta > 0 ? 'POSITIVE' : 'NEGATIVE';
    if (recent > older * 1.3)      acceleration = 'ACCELERATING_BULL';
    else if (recent < older * 0.7) acceleration = 'ACCELERATING_BEAR';
  }

  const momentum = acceleration !== 'NONE' ? acceleration : ratio > 60 ? 'POSITIVE' : ratio < 40 ? 'NEGATIVE' : 'NEUTRAL';

  return { buy:+buy.toFixed(0), sell:+sell.toFixed(0), ratio:+ratio.toFixed(1),
    delta:+delta.toFixed(0), momentum, trend, historyLen:store.history.length };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 2. WEBSOCKET ICEBERG TESPİTİ — Order book değişimlerini izle
// ═══════════════════════════════════════════════════════════════════════════════
const icebergStore = new Map(); // symbol → {bids, asks, hiddenBuy, hiddenSell, events}

function startIcebergStream(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  if (icebergStore.has(full) && icebergStore.get(full).ws?.readyState === WebSocket.OPEN) return;

  const store = { bids: new Map(), asks: new Map(), hiddenBuy:0, hiddenSell:0,
    events:[], ws:null, lastUpdate: Date.now() };
  icebergStore.set(full, store);

  // Depth stream: her 100ms'de book güncelleme
  const wsUrl = `wss://fstream.binance.com/ws/${full.toLowerCase()}@depth@100ms`;
  const ws = new WebSocket(wsUrl);
  store.ws = ws;

  ws.on('message', (data) => {
    try {
      const d = JSON.parse(data.toString());
      const now = Date.now();

      // Bid değişimleri
      (d.b||[]).forEach(([price, qty]) => {
        const p = parseFloat(price), q = parseFloat(qty);
        const prev = store.bids.get(p) || 0;
        if (q === 0) {
          // Emir silindi
          if (prev > 0) {
            const usdt = prev * p;
            // Büyük emir aniden silindi = iceberg (gizli alıcı)
            if (usdt > 50000) {
              store.hiddenBuy += usdt;
              store.events.push({ ts:now, side:'BUY', usdt:+usdt.toFixed(0), type:'ICEBERG_REFILL' });
              if (store.events.length > 100) store.events.shift();
            }
          }
          store.bids.delete(p);
        } else {
          store.bids.set(p, q);
        }
      });

      // Ask değişimleri
      (d.a||[]).forEach(([price, qty]) => {
        const p = parseFloat(price), q = parseFloat(qty);
        const prev = store.asks.get(p) || 0;
        if (q === 0) {
          if (prev > 0) {
            const usdt = prev * p;
            if (usdt > 50000) {
              store.hiddenSell += usdt;
              store.events.push({ ts:now, side:'SELL', usdt:+usdt.toFixed(0), type:'ICEBERG_REFILL' });
              if (store.events.length > 100) store.events.shift();
            }
          }
          store.asks.delete(p);
        } else {
          store.asks.set(p, q);
        }
      });

      store.lastUpdate = now;
    } catch(e) {}
  });

  ws.on('close', () => { setTimeout(() => startIcebergStream(full), 3000); });
  ws.on('error', () => {});
}

function getIceberg(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  startIcebergStream(full);
  const store = icebergStore.get(full);
  if (!store) return { hiddenBuy:0, hiddenSell:0, signal:'NEUTRAL', recentEvents:[] };

  const recentEvents = store.events.filter(e => Date.now() - e.ts < 5 * 60 * 1000);
  const recentBuy  = recentEvents.filter(e=>e.side==='BUY').reduce((s,e)=>s+e.usdt,0);
  const recentSell = recentEvents.filter(e=>e.side==='SELL').reduce((s,e)=>s+e.usdt,0);

  const signal = recentBuy > recentSell * 2  ? 'STRONG_HIDDEN_BUY'  :
                 recentSell > recentBuy * 2   ? 'STRONG_HIDDEN_SELL' :
                 recentBuy > recentSell * 1.3 ? 'HIDDEN_BUY'         :
                 recentSell > recentBuy * 1.3 ? 'HIDDEN_SELL'        : 'NEUTRAL';

  return { hiddenBuy:+recentBuy.toFixed(0), hiddenSell:+recentSell.toFixed(0),
    signal, eventCount: recentEvents.length,
    recentEvents: recentEvents.slice(-5) };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. COINGLASS LİKİDİTE HARİTASI — Gerçek liq seviyeleri
// ═══════════════════════════════════════════════════════════════════════════════
async function getCoinglass(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  try {
    // Coinglass public endpoint (API key gerektirmiyor)
    const r = await fetch(
      `https://open-api.coinglass.com/public/v2/liquidation_map?symbol=${full.replace('USDT','')}&range=12`,
      { headers:{ 'accept':'application/json' }, signal: AbortSignal.timeout(8000) }
    );
    const d = await r.json();
    if (!d.data) return null;

    // Liq yoğunluğu en fazla olan seviyeleri bul
    const levels = d.data;
    if (!Array.isArray(levels) || !levels.length) return null;

    const sorted = [...levels].sort((a,b) => (b.buyLiquidationSize||0)+(b.sellLiquidationSize||0) -
      (a.buyLiquidationSize||0)-(a.sellLiquidationSize||0));

    return {
      topLiqLevels: sorted.slice(0,5).map(l=>({
        price: l.price,
        buyLiq:  +(l.buyLiquidationSize||0).toFixed(0),
        sellLiq: +(l.sellLiquidationSize||0).toFixed(0),
        total:   +((l.buyLiquidationSize||0)+(l.sellLiquidationSize||0)).toFixed(0),
      })),
      source: 'coinglass'
    };
  } catch(e) {
    // Coinglass çalışmıyorsa hesaplanmış seviyeleri döndür
    return null;
  }
}


app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    service: 'kripto-sinyal-backend',
    version: 'SIGNAL_QUALITY_FIX_V3_UI_API_TAB_FIX',
    time: new Date().toISOString(),
    port: PORT,
    features: ['account', 'analyze', 'auto', 'positions', 'liquidation-stream']
  });
});

// ── FEAR & GREED INDEX ────────────────────────────────────────────────────────
app.get('/api/market-mood', async (req, res) => {
  try {
    const fg = await cached('fear_greed', 60*60*1000, async () => {
      const r = await fetch('https://api.alternative.me/fng/?limit=3');
      return r.json();
    });
    const val = parseInt(fg.data[0].value);
    const mood = val<=25?'EXTREME_FEAR':val<=45?'FEAR':val<=55?'NEUTRAL':val<=75?'GREED':'EXTREME_GREED';
    const signal = val<=25?'LONG_BIAS':val>=75?'SHORT_BIAS':'NEUTRAL';
    res.json({ ok:true, value:val, label:fg.data[0].value_classification, mood, signal,
      history:fg.data.map(d=>({value:parseInt(d.value),label:d.value_classification})) });
  } catch(e) { res.status(400).json({ error:e.message }); }
});

// ── EKONOMİK TAKVİM (FOMC/CPI/NFP) ──────────────────────────────────────────
app.get('/api/calendar', async (req, res) => {
  try {
    const HIGH_IMPACT = [
      {date:'2026-05-28',event:'NFP',impact:'HIGH'},
      {date:'2026-06-10',event:'CPI',impact:'HIGH'},
      {date:'2026-06-17',event:'FOMC',impact:'HIGH'},
      {date:'2026-07-02',event:'NFP',impact:'HIGH'},
      {date:'2026-07-08',event:'CPI',impact:'HIGH'},
      {date:'2026-07-29',event:'FOMC',impact:'HIGH'},
      {date:'2026-08-07',event:'NFP',impact:'HIGH'},
      {date:'2026-08-12',event:'CPI',impact:'HIGH'},
      {date:'2026-09-04',event:'NFP',impact:'HIGH'},
      {date:'2026-09-09',event:'CPI',impact:'HIGH'},
      {date:'2026-09-16',event:'FOMC',impact:'HIGH'},
      {date:'2026-10-02',event:'NFP',impact:'HIGH'},
      {date:'2026-10-14',event:'CPI',impact:'HIGH'},
      {date:'2026-11-04',event:'FOMC',impact:'HIGH'},
      {date:'2026-11-06',event:'NFP',impact:'HIGH'},
      {date:'2026-11-11',event:'CPI',impact:'HIGH'},
      {date:'2026-12-04',event:'NFP',impact:'HIGH'},
      {date:'2026-12-09',event:'CPI',impact:'HIGH'},
      {date:'2026-12-16',event:'FOMC',impact:'HIGH'},
    ];
    const today = new Date().toISOString().split('T')[0];
    const tomorrow = new Date(Date.now()+86400000).toISOString().split('T')[0];
    const hour = new Date().getHours();
    const todayEvents = HIGH_IMPACT.filter(e=>e.date===today);
    const tomorrowEvents = HIGH_IMPACT.filter(e=>e.date===tomorrow);
    const dangerZone = todayEvents.length>0 && hour>=14 && hour<=17;
    res.json({ ok:true, todayEvents, tomorrowEvents, dangerZone,
      recommendation: dangerZone?'⛔ HABER SAATİ — işlem açma!':
                      tomorrowEvents.length?'⚠️ Yarın yüksek haber':'✅ Temiz' });
  } catch(e) { res.status(400).json({ error:e.message }); }
});

// ── COIN LİSTESİ ──────────────────────────────────────────────────────────────
app.get('/api/futures-coins', async (req, res) => {
  try {
    const data = await cached('futures_tickers', 45*1000, () => bPub('/fapi/v1/ticker/24hr')); // 5m scalping için hızlı havuz
    if (!Array.isArray(data)) return res.json({ coins:[] });
    const EXCL = new Set(['BTCUSDT','ETHUSDT','BNBUSDT','XRPUSDT','SOLUSDT','ADAUSDT',
      'DOGEUSDT','DOTUSDT','MATICUSDT','LTCUSDT','TRXUSDT','AVAXUSDT','LINKUSDT','UNIUSDT','WBTCUSDT','SHIBUSDT']);
    const coins = data.filter(t=>t.symbol.endsWith('USDT')&&!EXCL.has(t.symbol))
      .map(t=>({ symbol:t.symbol.replace('USDT',''), fullSymbol:t.symbol,
        price:parseFloat(t.lastPrice)||0, change24h:parseFloat(t.priceChangePercent)||0,
        volume:parseFloat(t.quoteVolume)||0, high:parseFloat(t.highPrice)||0,
        low:parseFloat(t.lowPrice)||0, trades:parseInt(t.count)||0 }))
      .filter(c=>c.volume>20000000)
      .map(c=>({...c, rangePct:c.low>0?+((c.high-c.low)/c.low*100).toFixed(2):0, hotRankScore:+(Math.abs(c.change24h)*2 + Math.log10(Math.max(c.volume,1))*3 + Math.log10(Math.max(c.trades,1))).toFixed(2)}))
      .sort((a,b)=>b.change24h-a.change24h);
    res.json({ ok:true, count:coins.length, coins });
  } catch(e) { res.status(400).json({ error:e.message }); }
});

// ── PRO ANALİZ ────────────────────────────────────────────────────────────────
app.get('/api/analyze/:symbol', async (req, res) => {
  const sym  = req.params.symbol.toUpperCase();
  const full = sym.endsWith('USDT') ? sym : sym+'USDT';

  try {
    // WS stream'leri başlat
    startCVDStream(full);
    startIcebergStream(full);

    const [r4h,r1h,r15m,r5m,rFunding,rOIHist,rLS_global,rLS_top,rDepth] =
      await Promise.allSettled([
        cached(`k4h_${full}`,  30*60*1000, ()=>bPub('/fapi/v1/klines',`symbol=${full}&interval=4h&limit=200`)),
        cached(`k1h_${full}`,  10*60*1000, ()=>bPub('/fapi/v1/klines',`symbol=${full}&interval=1h&limit=200`)),
        cached(`k15m_${full}`,  5*60*1000, ()=>bPub('/fapi/v1/klines',`symbol=${full}&interval=15m&limit=200`)),
        cached(`k5m_${full}`,   3*60*1000, ()=>bPub('/fapi/v1/klines',`symbol=${full}&interval=5m&limit=100`)),
        cached(`fund_${full}`, 30*60*1000, ()=>bPub('/fapi/v1/fundingRate',`symbol=${full}&limit=10`)),
        cached(`oih_${full}`,  15*60*1000, ()=>bPub('/futures/data/openInterestHist',`symbol=${full}&period=1h&limit=24`)),
        cached(`lsg_${full}`,  15*60*1000, ()=>bPub('/futures/data/globalLongShortAccountRatio',`symbol=${full}&period=1h&limit=12`)),
        cached(`lst_${full}`,  15*60*1000, ()=>bPub('/futures/data/topLongShortPositionRatio',`symbol=${full}&period=1h&limit=12`)),
        cached(`dep_${full}`,   2*60*1000, ()=>bPub('/fapi/v1/depth',`symbol=${full}&limit=100`)),
      ]);

    const k4h  = r4h.status==='fulfilled'&&Array.isArray(r4h.value)   ?r4h.value  :[];
    const k1h  = r1h.status==='fulfilled'&&Array.isArray(r1h.value)   ?r1h.value  :[];
    const k15m = r15m.status==='fulfilled'&&Array.isArray(r15m.value) ?r15m.value :[];
    const k5m  = r5m.status==='fulfilled'&&Array.isArray(r5m.value)   ?r5m.value  :[];
    const fundArr  = rFunding.status==='fulfilled'&&Array.isArray(rFunding.value)   ?rFunding.value  :[];
    const oiHist   = rOIHist.status==='fulfilled'&&Array.isArray(rOIHist.value)     ?rOIHist.value   :[];
    const lsGlobal = rLS_global.status==='fulfilled'&&Array.isArray(rLS_global.value)?rLS_global.value:[];
    const lsTop    = rLS_top.status==='fulfilled'&&Array.isArray(rLS_top.value)      ?rLS_top.value   :[];
    const depth    = rDepth.status==='fulfilled'?rDepth.value:{bids:[],asks:[]};

    let liveTicker=null;
    try { liveTicker = await cached(`px_${full}`, 15*1000, ()=>bPub('/fapi/v1/ticker/price',`symbol=${full}`)); } catch(e) {}
    const livePrice = liveTicker?.price ? parseFloat(liveTicker.price) : 0;
    const lastPrice = livePrice || (k5m.length?parseFloat(k5m[k5m.length-1][4]):(k15m.length?parseFloat(k15m[k15m.length-1][4]):0));
    const lastTime  = k5m.length?parseInt(k5m[k5m.length-1][6]):(k15m.length?parseInt(k15m[k15m.length-1][6]):Date.now());
    const dataQuality = {
      k5mOk:k5m.length>=60, k15mOk:k15m.length>=80, k1hOk:k1h.length>=80,
      depthOk:Array.isArray(depth.bids)&&depth.bids.length>=20&&Array.isArray(depth.asks)&&depth.asks.length>=20,
      priceOk:lastPrice>0,
    };
    dataQuality.ok = dataQuality.k5mOk && dataQuality.k15mOk && dataQuality.k1hOk && dataQuality.depthOk && dataQuality.priceOk;

    // ── TEKNİK FONKSİYONLAR ──────────────────────────────────────────────────
    function rsi(kl,p=14){
      if(kl.length<p+1)return 50;
      const c=kl.map(k=>parseFloat(k[4]));
      let g=0,l=0;for(let i=c.length-p;i<c.length;i++){const d=c[i]-c[i-1];d>0?g+=d:l-=d;}
      const ag=g/p,al=l/p;return al===0?100:Math.round(100-(100/(1+ag/al)));
    }
    function ema(kl,p){
      if(kl.length<p)return 0;
      const c=kl.map(k=>parseFloat(k[4]));
      const k=2/(p+1);let e=c.slice(0,p).reduce((a,b)=>a+b,0)/p;
      for(let i=p;i<c.length;i++)e=c[i]*k+e*(1-k);return e;
    }
    function atr(kl,p=14){
      if(kl.length<p+1)return 0;
      const trs=kl.slice(-p-1).map((k,i,a)=>{
        if(i===0)return parseFloat(k[2])-parseFloat(k[3]);
        const prev=parseFloat(a[i-1][4]);
        return Math.max(parseFloat(k[2])-parseFloat(k[3]),Math.abs(parseFloat(k[2])-prev),Math.abs(parseFloat(k[3])-prev));
      });return trs.slice(1).reduce((a,b)=>a+b,0)/p;
    }
    function vwap(kl){
      let tv=0,v=0;kl.forEach(k=>{const tp=(parseFloat(k[2])+parseFloat(k[3])+parseFloat(k[4]))/3;const vol=parseFloat(k[5]);tv+=tp*vol;v+=vol;});
      return v>0?tv/v:0;
    }
    function bollinger(kl,p=20){
      if(kl.length<p)return{upper:0,lower:0,mid:0,width:0};
      const c=kl.slice(-p).map(k=>parseFloat(k[4]));
      const mid=c.reduce((a,b)=>a+b,0)/p;
      const std=Math.sqrt(c.reduce((s,v)=>s+Math.pow(v-mid,2),0)/p);
      return{upper:mid+2*std,lower:mid-2*std,mid,width:+(2*2*std/mid*100).toFixed(2)};
    }

    const rsi4h=rsi(k4h),rsi1h=rsi(k1h),rsi15m=rsi(k15m),rsi5m=rsi(k5m);
    const ema20_4h=ema(k4h,20),ema50_4h=ema(k4h,50),ema200_4h=ema(k4h,200);
    const ema20_1h=ema(k1h,20),ema50_1h=ema(k1h,50),ema200_1h=ema(k1h,200);
    const atr4h=atr(k4h),atr1h=atr(k1h),atr15m_=atr(k15m),atr5m=atr(k5m);
    const vwap1h=vwap(k1h),vwap4h=vwap(k4h),vwap5m=vwap(k5m);
    const bb1h=bollinger(k1h),bb15m_=bollinger(k15m);
    const ema9_5m=ema(k5m,9), ema21_5m=ema(k5m,21), ema50_5m=ema(k5m,50);
    const last5=k5m[k5m.length-1]||null, prev5=k5m[k5m.length-2]||null;
    const priceChg5m=last5&&prev5?+((parseFloat(last5[4])-parseFloat(prev5[4]))/parseFloat(prev5[4])*100).toFixed(2):0;
    const vol20=k5m.length>=21?k5m.slice(-21,-1).reduce((a,k)=>a+parseFloat(k[5]),0)/20:0;
    const volRatio5m=last5&&vol20>0?+(parseFloat(last5[5])/vol20).toFixed(2):0;
    const takerBuyRatio5m=last5&&parseFloat(last5[5])>0?+(parseFloat(last5[9])/parseFloat(last5[5])*100).toFixed(1):50;
    const candleBody5m=last5?+((parseFloat(last5[4])-parseFloat(last5[1]))/parseFloat(last5[1])*100).toFixed(2):0;

    // ── SİNYAL YAŞI ──────────────────────────────────────────────────────────
    const signalAgeMs=Date.now()-lastTime;
    const signalAgeMin=Math.floor(signalAgeMs/60000);
    const maxValidMin=240;
    const isExpired=signalAgeMin>maxValidMin;
    const freshness=isExpired?'EXPIRED':signalAgeMin<15?'FRESH':signalAgeMin<60?'VALID':'AGING';
    let freshnessMult=freshness==='EXPIRED'?0:freshness==='AGING'?0.75:1;

    // ── LİKİDİTE SEVİYELERİ ──────────────────────────────────────────────────
    function findLiqLevels(klines,lookback=100){
      if(klines.length<lookback)return{buyLiq:[],sellLiq:[]};
      const recent=klines.slice(-lookback);
      const buyLiq=[],sellLiq=[];
      for(let i=2;i<recent.length-2;i++){
        const h=parseFloat(recent[i][2]);
        const isH=h>parseFloat(recent[i-1][2])&&h>parseFloat(recent[i-2][2])&&h>parseFloat(recent[i+1][2])&&h>parseFloat(recent[i+2][2]);
        if(isH){const ex=buyLiq.find(l=>Math.abs(l.price-h)/h<0.003);
          ex?ex.strength++:buyLiq.push({price:h,strength:1,distPct:+((h-lastPrice)/lastPrice*100).toFixed(2)});}
        const l=parseFloat(recent[i][3]);
        const isL=l<parseFloat(recent[i-1][3])&&l<parseFloat(recent[i-2][3])&&l<parseFloat(recent[i+1][3])&&l<parseFloat(recent[i+2][3]);
        if(isL){const ex=sellLiq.find(x=>Math.abs(x.price-l)/l<0.003);
          ex?ex.strength++:sellLiq.push({price:l,strength:1,distPct:+((lastPrice-l)/lastPrice*100).toFixed(2)});}
      }
      return{
        buyLiq:buyLiq.filter(l=>l.price>lastPrice).sort((a,b)=>a.price-b.price).slice(0,3),
        sellLiq:sellLiq.filter(l=>l.price<lastPrice).sort((a,b)=>b.price-a.price).slice(0,3)
      };
    }
    const liq1h=findLiqLevels(k1h,100);
    const liq4h=findLiqLevels(k4h,100);

    // ── LİKİDİTE SWEEP + TEYİT (joshyattridge/smart-money-concepts mantığı) ──
    // Kural: Sweep tek başına yeterli değil. Sweep SONRASI geri dönüş teyidi lazım.
    // Bullish setup: fiyat alt liq'i sweep etti → hemen geri döndü → LONG gir
    // Bearish setup: fiyat üst liq'i sweep etti → hemen geri döndü → SHORT gir

    function detectSweepAndConfirm(klines, liqLevels, n=20) {
      if (klines.length < n) return { swept:false, confirmed:false, direction:'NONE' };
      const recent = klines.slice(-n);

      // Son 5 mumda sweep var mı?
      for (let i = recent.length-5; i < recent.length-1; i++) {
        const sweepCandle = recent[i];
        const confirmCandle = recent[i+1]; // sweep'ten sonraki mum = teyit
        const sh = parseFloat(sweepCandle[2]); // sweep mumu high
        const sl = parseFloat(sweepCandle[3]); // sweep mumu low
        const so = parseFloat(sweepCandle[1]);
        const sc = parseFloat(sweepCandle[4]);
        const co = parseFloat(confirmCandle[1]);
        const cc = parseFloat(confirmCandle[4]);

        // Alt likidite sweep tespiti (BULLISH setup)
        // Fiyat alt liq seviyesinin altına girdi ama kapandı üstünde
        for (const liq of liqLevels.sellLiq || []) {
          if (sl < liq.price && sc > liq.price) {
            // Sweep oldu. Teyit: sonraki mum yukarı kapandı mı?
            const bullConfirm = cc > co; // bullish teyit mumu
            const strongConfirm = cc > Math.max(so, sc); // güçlü teyit
            if (bullConfirm) {
              const wickPct = +((liq.price - sl) / lastPrice * 100).toFixed(2);
              return {
                swept: true, confirmed: true,
                direction: 'BULL_SWEEP',
                sweepLevel: liq.price,
                sweepStrength: liq.strength,
                wickPct,
                strongConfirm,
                msg: `Alt liq sweep ($${liq.price.toFixed(4)}) + teyit ↑ — giriş zamanı`,
                candleAge: recent.length - 1 - (i+1), // kaç mum öncesi
              };
            } else {
              return { swept:true, confirmed:false, direction:'BULL_SWEEP_UNCONFIRMED',
                msg:'Alt sweep var ama teyit bekleniyor' };
            }
          }
        }

        // Üst likidite sweep tespiti (BEARISH setup)
        for (const liq of liqLevels.buyLiq || []) {
          if (sh > liq.price && sc < liq.price) {
            const bearConfirm = cc < co;
            const strongConfirm = cc < Math.min(so, sc);
            if (bearConfirm) {
              const wickPct = +((sh - liq.price) / lastPrice * 100).toFixed(2);
              return {
                swept: true, confirmed: true,
                direction: 'BEAR_SWEEP',
                sweepLevel: liq.price,
                sweepStrength: liq.strength,
                wickPct,
                strongConfirm,
                msg: `Üst liq sweep ($${liq.price.toFixed(4)}) + teyit ↓ — short zamanı`,
                candleAge: recent.length - 1 - (i+1),
              };
            } else {
              return { swept:true, confirmed:false, direction:'BEAR_SWEEP_UNCONFIRMED',
                msg:'Üst sweep var ama teyit bekleniyor' };
            }
          }
        }
      }

      // Sweep yoksa klasik wick analizi (fallback)
      for (let i = recent.length-3; i < recent.length; i++) {
        const o=parseFloat(recent[i][1]),h=parseFloat(recent[i][2]);
        const l=parseFloat(recent[i][3]),c=parseFloat(recent[i][4]);
        const body=Math.abs(c-o),range=h-l;if(range===0)continue;
        const uw=h-Math.max(o,c),lw=Math.min(o,c)-l;
        if(uw/range>0.65&&uw>body*2.5)return{swept:false,hunted:true,direction:'BEAR_HUNT',msg:'Üst wick → dönüş beklenir ↓'};
        if(lw/range>0.65&&lw>body*2.5)return{swept:false,hunted:true,direction:'BULL_HUNT',msg:'Alt wick → dönüş beklenir ↑'};
      }

      return { swept:false, confirmed:false, direction:'NONE' };
    }

    // Backward compat
    function detectStopHunt(klines, n=10) {
      const r = detectSweepAndConfirm(klines, {buyLiq:liq1h.buyLiq, sellLiq:liq1h.sellLiq}, n);
      return { hunted: r.swept||r.hunted||false, direction: r.direction==='BULL_SWEEP'||r.direction==='BULL_HUNT'?'BULL_HUNT':r.direction==='BEAR_SWEEP'||r.direction==='BEAR_HUNT'?'BEAR_HUNT':'NONE', msg: r.msg };
    }

    const sweep1h  = detectSweepAndConfirm(k1h,  {buyLiq:liq1h.buyLiq,  sellLiq:liq1h.sellLiq},  20);
    const sweep4h  = detectSweepAndConfirm(k4h,  {buyLiq:liq4h.buyLiq,  sellLiq:liq4h.sellLiq},  20);
    const sweep15m = detectSweepAndConfirm(k15m, {buyLiq:liq1h.buyLiq,  sellLiq:liq1h.sellLiq},  10);
    const hunt1h   = detectStopHunt(k1h, 15);
    const hunt15m  = detectStopHunt(k15m, 8);

    // ── ORDER BLOCKS ──────────────────────────────────────────────────────────
    function detectWyckoff(klines) {
      if (klines.length < 30) return { phase:'UNKNOWN', signal:'NONE', recentEvents:[], allEvents:[] };
      const recent=klines.slice(-30);
      const closes=recent.map(k=>parseFloat(k[4]));
      const highs=recent.map(k=>parseFloat(k[2]));
      const lows=recent.map(k=>parseFloat(k[3]));
      const vols=recent.map(k=>parseFloat(k[5]));
      const avgVol=vols.slice(0,-5).reduce((a,b)=>a+b,0)/Math.max(vols.length-5,1);
      const rangeHigh=Math.max(...highs.slice(0,20));
      const rangeLow=Math.min(...lows.slice(0,20));
      const rangeSize=Math.max(rangeHigh-rangeLow,0.000001);
      const events=[];
      for(let i=20;i<recent.length-1;i++){
        const l=lows[i],c=closes[i],v=vols[i],nextC=closes[i+1];
        // SPRING: range altına girdi, hacimle geri döndü = MM dip topladı
        if(l<rangeLow*0.999&&c>rangeLow&&nextC>c&&v>avgVol*1.2)
          events.push({type:'SPRING',price:l,candleIdx:i,depth:+((rangeLow-l)/rangeSize*100).toFixed(1),msg:'Spring! MM dip topladı ↑',signal:'STRONG_LONG'});
        // UTAD: range üstüne çıktı ama döndü = sahte kırılım
        if(highs[i]>rangeHigh*1.001&&c<rangeHigh&&nextC<c&&v>avgVol*1.3)
          events.push({type:'UTAD',price:highs[i],candleIdx:i,msg:'UTAD sahte kırılım ↓',signal:'STRONG_SHORT'});
      }
      for(let i=20;i<recent.length;i++){
        // SOS: range üstünde güçlü kapanış = gerçek kırılım
        if(closes[i]>rangeHigh*1.001&&vols[i]>avgVol*1.5)
          events.push({type:'SOS',price:closes[i],candleIdx:i,msg:'SOS kırılım teyidi ↑',signal:'CONFIRM_LONG'});
      }
      const recentEvents=events.filter(e=>e.candleIdx>=recent.length-5);
      let phase='RANGE';
      if(recentEvents.some(e=>e.type==='SPRING'))phase='ACCUMULATION_C';
      else if(recentEvents.some(e=>e.type==='SOS'))phase='MARKUP';
      else if(recentEvents.some(e=>e.type==='UTAD'))phase='DISTRIBUTION_C';
      return{phase,rangeHigh:+rangeHigh.toFixed(8),rangeLow:+rangeLow.toFixed(8),
        signal:recentEvents[0]?.signal||'NONE',
        lastEvent:recentEvents[recentEvents.length-1]||null,
        recentEvents,allEvents:events.slice(-5)};
    }
    const wyckoff4h=detectWyckoff(k4h);
    const wyckoff1h=detectWyckoff(k1h);
    const wyckoff15m=detectWyckoff(k15m);

    function findOB(klines){
      if(klines.length<5)return{bullOB:null,bearOB:null};
      const r=klines.slice(-20);let bullOB=null,bearOB=null;
      for(let i=1;i<r.length-1;i++){
        const co=parseFloat(r[i][1]),cc=parseFloat(r[i][4]);
        const no=parseFloat(r[i+1][1]),nc=parseFloat(r[i+1][4]);
        const move=Math.abs(nc-no)/no*100;
        if(nc>no*1.005&&move>0.5&&cc<co)bullOB={high:parseFloat(r[i][2]),low:parseFloat(r[i][3]),mid:(parseFloat(r[i][2])+parseFloat(r[i][3]))/2,distPct:+((lastPrice-parseFloat(r[i][3]))/lastPrice*100).toFixed(2)};
        if(nc<no*0.995&&move>0.5&&cc>co)bearOB={high:parseFloat(r[i][2]),low:parseFloat(r[i][3]),mid:(parseFloat(r[i][2])+parseFloat(r[i][3]))/2,distPct:+((parseFloat(r[i][2])-lastPrice)/lastPrice*100).toFixed(2)};
      }
      return{bullOB,bearOB};
    }
    const ob1h=findOB(k1h),ob4h=findOB(k4h);

    // ── FUNDING ───────────────────────────────────────────────────────────────
    const curFund=fundArr.length?parseFloat(fundArr[fundArr.length-1].fundingRate)*100:0;
    const fundSig=curFund<-0.05?'EXTREME_NEGATIVE':curFund<-0.01?'NEGATIVE':curFund>0.1?'EXTREME_POSITIVE':curFund>0.05?'POSITIVE':'NEUTRAL';

    // ── OI ────────────────────────────────────────────────────────────────────
    let oiChg1h=0,oiChg4h=0;
    if(oiHist.length>=2){
      const fn=x=>parseFloat(x.sumOpenInterestValue||x.sumOpenInterest||0);
      const lat=fn(oiHist[oiHist.length-1]),h1=fn(oiHist[oiHist.length-2]),h4=fn(oiHist[Math.max(0,oiHist.length-5)]);
      oiChg1h=h1>0?(lat-h1)/h1*100:0;oiChg4h=h4>0?(lat-h4)/h4*100:0;
    }
    const p1hChg=k1h.length>=2?(parseFloat(k1h[k1h.length-1][4])-parseFloat(k1h[k1h.length-2][4]))/parseFloat(k1h[k1h.length-2][4])*100:0;
    const oiDiv=oiChg1h>1&&p1hChg>0.5?'CONFIRMED_BULL':oiChg1h>1&&p1hChg<-0.5?'CONFIRMED_BEAR':oiChg1h<-1&&p1hChg>0.5?'SHORT_SQUEEZE':oiChg1h<-1&&p1hChg<-0.5?'LONG_LIQUIDATION':'NEUTRAL';

    // ── SMART MONEY ───────────────────────────────────────────────────────────
    const globalLong=lsGlobal.length?parseFloat(lsGlobal[lsGlobal.length-1].longAccount||0.5)*100:50;
    const topLong   =lsTop.length?parseFloat(lsTop[lsTop.length-1].longAccount||0.5)*100:50;
    const smDiv=topLong>55&&globalLong<45?'SMART_BULL':topLong<45&&globalLong>55?'SMART_BEAR':topLong>60?'WHALE_LONG':topLong<40?'WHALE_SHORT':'NEUTRAL';

    // ── ORDER BOOK IMBALANCE — nkaz001/algotrading-example mantığı ─────────────
    // Fiyata yakın seviyelerdeki bid/ask dengesizliği daha önemli
    // Yakın seviye ağırlıklı imbalance hesabı
    const bids=Array.isArray(depth.bids)?depth.bids.slice(0,50):[];
    const asks=Array.isArray(depth.asks)?depth.asks.slice(0,50):[];
    let totBid=0,totAsk=0;
    let nearBid=0,nearAsk=0; // fiyata %0.5 yakın seviyeler
    let wallBidPrice=0,wallBidSize=0,wallAskPrice=0,wallAskSize=0;

    bids.forEach(([p,q])=>{
      const usdt=parseFloat(p)*parseFloat(q);
      totBid+=usdt;
      if(Math.abs(parseFloat(p)-lastPrice)/lastPrice<0.005)nearBid+=usdt;
      if(usdt>wallBidSize){wallBidSize=usdt;wallBidPrice=parseFloat(p);}
    });
    asks.forEach(([p,q])=>{
      const usdt=parseFloat(p)*parseFloat(q);
      totAsk+=usdt;
      if(Math.abs(parseFloat(p)-lastPrice)/lastPrice<0.005)nearAsk+=usdt;
      if(usdt>wallAskSize){wallAskSize=usdt;wallAskPrice=parseFloat(p);}
    });

    // Ağırlıklı imbalance: yakın seviyelere 3x ağırlık
    const weightedBid = totBid*0.4 + nearBid*0.6;
    const weightedAsk = totAsk*0.4 + nearAsk*0.6;
    const bookImb = weightedBid+weightedAsk>0 ? (weightedBid-weightedAsk)/(weightedBid+weightedAsk)*100 : 0;

    // Büyük duvar tespiti (iceberg)
    const bidWallPct = wallBidSize/totBid*100;
    const askWallPct = wallAskSize/totAsk*100;

    // ── WS VERİLERİ ───────────────────────────────────────────────────────────
    const cvd     = getCVD(full);
    const iceberg = getIceberg(full);

    // ── COINGLASS LİKİDATE (async, cache 15dk) ────────────────────────────────
    const cgData = await cached(`cg_${full}`, 15*60*1000, ()=>getCoinglass(full));

    // ── MM YÖN ────────────────────────────────────────────────────────────────
    const upLiqStr=liq1h.buyLiq.reduce((s,l)=>s+l.strength*(1/Math.max(l.distPct,0.1)),0);
    const dnLiqStr=liq1h.sellLiq.reduce((s,l)=>s+l.strength*(1/Math.max(l.distPct,0.1)),0);
    let oiTrend='FLAT';
    if(oiHist.length>=4){const fn=x=>parseFloat(x.sumOpenInterest||0);const lat=fn(oiHist[oiHist.length-1]),old=fn(oiHist[oiHist.length-4]);oiTrend=lat>old*1.02?'RISING':lat<old*0.98?'FALLING':'FLAT';}
    const retailBias=globalLong>65?'TOO_LONG':globalLong<35?'TOO_SHORT':'NEUTRAL';
    const smBias=topLong>60?'UP':topLong<40?'DOWN':'NEUTRAL';
    let mmTarget='UNKNOWN',mmConf=0,mmReasoning=[];
    if(upLiqStr>dnLiqStr*1.5){mmTarget='UP_SWEEP';mmConf+=25;mmReasoning.push(`Üst liq havuzu $${liq1h.buyLiq[0]?.price?.toFixed(4)||'?'}`);}
    else if(dnLiqStr>upLiqStr*1.5){mmTarget='DOWN_SWEEP';mmConf+=25;mmReasoning.push(`Alt liq havuzu $${liq1h.sellLiq[0]?.price?.toFixed(4)||'?'}`);}
    if(retailBias==='TOO_LONG'){mmConf+=20;if(mmTarget!=='UP_SWEEP')mmTarget='DOWN_SWEEP';mmReasoning.push(`Perakende %${globalLong.toFixed(0)} long`);}
    else if(retailBias==='TOO_SHORT'){mmConf+=20;if(mmTarget!=='DOWN_SWEEP')mmTarget='UP_SWEEP';mmReasoning.push(`Perakende %${(100-globalLong).toFixed(0)} short`);}
    if(smBias==='UP'&&oiTrend==='RISING'){mmTarget='GENUINE_UP';mmConf+=30;mmReasoning.push('Whale long + OI artışı');}
    else if(smBias==='DOWN'&&oiTrend==='RISING'){mmTarget='GENUINE_DOWN';mmConf+=30;mmReasoning.push('Whale short + OI artışı');}
    // WS iceberg MM güvenini artır
    if(iceberg.signal==='STRONG_HIDDEN_BUY')  {mmConf+=15;mmReasoning.push('WS Iceberg gizli alıcı');}
    if(iceberg.signal==='STRONG_HIDDEN_SELL') {mmConf+=15;mmReasoning.push('WS Iceberg gizli satıcı');}
    mmConf=Math.min(mmConf,95);
    const mmNextTarget=mmTarget.includes('UP')?(liq1h.buyLiq[0]?.price||0):(liq1h.sellLiq[0]?.price||0);

    // ── PRO TP/SL ─────────────────────────────────────────────────────────────
    function calcProTPSL(side,price,atr1h,atr4h,liq1h,liq4h,ob1h,ob4h,k1h){
      const isLong=side==='LONG';
      let sl=0,tp=0,tp2=0;const reasons={};
      const recent15=k1h.slice(-15);
      if(isLong){
        const swingLow=Math.min(...recent15.map(k=>parseFloat(k[3])));
        const atrSL=price-1.5*atr1h;
        sl=Math.max(swingLow,atrSL);
        if(ob1h.bullOB&&ob1h.bullOB.low>sl&&ob1h.bullOB.low<price*0.99){sl=ob1h.bullOB.low*0.998;reasons.sl=`1h Bull OB altı`;}
        else reasons.sl=`Swing Low + 1.5xATR`;
        const nearLiq=liq1h.buyLiq.find(l=>l.price>price*1.005);
        if(nearLiq&&nearLiq.price<price*1.3){tp=nearLiq.price*0.998;reasons.tp=`1h Liq havuzu (güç:${nearLiq.strength})`;}
        else{tp=price+2*atr1h;reasons.tp='2xATR1h';}
        const liq4hUp=liq4h.buyLiq.find(l=>l.price>price*1.02);
        if(liq4hUp&&liq4hUp.price<price*1.5){tp2=liq4hUp.price*0.997;reasons.tp2='4h Liq havuzu';}
        else if(ob4h.bearOB&&ob4h.bearOB.low>price*1.02){tp2=ob4h.bearOB.low*0.997;reasons.tp2='4h Bear OB';}
        else{tp2=price+4*atr4h;reasons.tp2='4xATR4h';}
      } else {
        const swingHigh=Math.max(...recent15.map(k=>parseFloat(k[2])));
        const atrSL=price+1.5*atr1h;
        sl=Math.min(swingHigh,atrSL);
        if(ob1h.bearOB&&ob1h.bearOB.high<sl&&ob1h.bearOB.high>price*1.01){sl=ob1h.bearOB.high*1.002;reasons.sl='1h Bear OB üstü';}
        else reasons.sl='Swing High + 1.5xATR';
        const nearLiq=liq1h.sellLiq.find(l=>l.price<price*0.995);
        if(nearLiq&&nearLiq.price>price*0.7){tp=nearLiq.price*1.002;reasons.tp=`1h Liq havuzu (güç:${nearLiq.strength})`;}
        else{tp=price-2*atr1h;reasons.tp='2xATR1h';}
        const liq4hDn=liq4h.sellLiq.find(l=>l.price<price*0.98);
        if(liq4hDn&&liq4hDn.price>price*0.5){tp2=liq4hDn.price*1.003;reasons.tp2='4h Liq havuzu';}
        else if(ob4h.bullOB&&ob4h.bullOB.high<price*0.98){tp2=ob4h.bullOB.high*1.003;reasons.tp2='4h Bull OB';}
        else{tp2=price-4*atr4h;reasons.tp2='4xATR4h';}
      }
      if(isLong){if(sl>=price)sl=price*0.95;if(tp<=price)tp=price*1.05;if(tp2<=tp)tp2=tp*1.05;}
      else{if(sl<=price)sl=price*1.05;if(tp>=price)tp=price*0.95;if(tp2>=tp)tp2=tp*0.95;}
      const riskPct=Math.abs(price-sl)/price*100;
      const reward1=Math.abs(tp-price)/price*100;
      const reward2=Math.abs(tp2-price)/price*100;
      return{sl:+sl.toFixed(8),tp:+tp.toFixed(8),tp2:+tp2.toFixed(8),
        riskPct:+riskPct.toFixed(2),rewardPct:+reward1.toFixed(2),reward2Pct:+reward2.toFixed(2),
        rr1:+(reward1/riskPct).toFixed(2),rr2:+(reward2/riskPct).toFixed(2),reasons};
    }
    let proTPSL=calcProTPSL(
      mmTarget==='GENUINE_DOWN'||mmTarget==='DOWN_SWEEP'?'SHORT':'LONG',
      lastPrice,atr1h,atr4h,liq1h,liq4h,ob1h,ob4h,k1h
    );

    // ── KALDIRAÇ ─────────────────────────────────────────────────────────────
    const t4up=ema20_4h>ema50_4h&&ema50_4h>ema200_4h;
    const t4dn=ema20_4h<ema50_4h&&ema50_4h<ema200_4h;
    const atrPct=lastPrice>0?(atr1h/lastPrice)*100:1;
    let longScore=0,shortScore=0;const signals={long:[],short:[]};

    // ── SKORLAMA (öncelik sırasıyla) ──────────────────────────────────────────
    // 0a. WYCKOFF — MM'nin gerçek dip/zirve tespiti
    // Spring = en güçlü sinyal, ML backtested
    if(wyckoff1h.recentEvents?.some(e=>e.type==='SPRING'))  {longScore+=30;  signals.long.push('🌊 Wyckoff Spring! MM dip topladı');}
    if(wyckoff4h.recentEvents?.some(e=>e.type==='SPRING'))  {longScore+=20;  signals.long.push('🌊 4h Wyckoff Spring');}
    if(wyckoff1h.recentEvents?.some(e=>e.type==='SOS'))     {longScore+=15;  signals.long.push('💪 Wyckoff SOS - kırılım teyidi');}
    if(wyckoff1h.recentEvents?.some(e=>e.type==='UTAD'))    {shortScore+=25; signals.short.push('🚨 Wyckoff UTAD - sahte kırılım ↓');}
    if(wyckoff4h.recentEvents?.some(e=>e.type==='UTAD'))    {shortScore+=20; signals.short.push('🚨 4h UTAD dağıtım');}

    // 0b. LİKİDASYON CASCADE — kaskad başlarken tersine gir
    if(liqData.cascade) {
      if(liqData.cascade.direction==='SHORT_CASCADE') {
        // Short liqler patladı → fiyat yukarı → long fırsatı
        longScore+=20; signals.long.push(`💥 Short Cascade $${(liqData.cascade.amount/1000).toFixed(0)}K`);
      }
      if(liqData.cascade.direction==='LONG_CASCADE') {
        shortScore+=20; signals.short.push(`💥 Long Cascade $${(liqData.cascade.amount/1000).toFixed(0)}K`);
      }
    }
    // 1dk içinde dominant likidasyon yönü
    if(liqData.shortLiq1m > 100000) {longScore+=8;  signals.long.push(`⚡ Short Liq $${(liqData.shortLiq1m/1000).toFixed(0)}K/1dk`);}
    if(liqData.longLiq1m  > 100000) {shortScore+=8; signals.short.push(`⚡ Long Liq $${(liqData.longLiq1m/1000).toFixed(0)}K/1dk`);}

    // 0. SWEEP + TEYİT — en kritik sinyal (MM'nin oyunu tamamlandı, giriş zamanı)
    if(sweep1h.confirmed && sweep1h.direction==='BULL_SWEEP') {
      longScore += sweep1h.strongConfirm?35:25;
      signals.long.push(`✅ 1h Sweep+Teyit ↑ (güç:${sweep1h.sweepStrength})`);
    }
    if(sweep1h.confirmed && sweep1h.direction==='BEAR_SWEEP') {
      shortScore += sweep1h.strongConfirm?35:25;
      signals.short.push(`✅ 1h Sweep+Teyit ↓ (güç:${sweep1h.sweepStrength})`);
    }
    if(sweep4h.confirmed && sweep4h.direction==='BULL_SWEEP') {
      longScore += 20; signals.long.push(`✅ 4h Sweep+Teyit ↑`);
    }
    if(sweep4h.confirmed && sweep4h.direction==='BEAR_SWEEP') {
      shortScore += 20; signals.short.push(`✅ 4h Sweep+Teyit ↓`);
    }
    if(sweep15m.confirmed && sweep15m.direction==='BULL_SWEEP') {
      longScore += 15; signals.long.push(`✅ 15m Sweep+Teyit ↑`);
    }
    if(sweep15m.confirmed && sweep15m.direction==='BEAR_SWEEP') {
      shortScore += 15; signals.short.push(`✅ 15m Sweep+Teyit ↓`);
    }
    // Sweep var ama teyit yok → dikkat uyarısı, skor verme
    if(sweep1h.swept && !sweep1h.confirmed) {
      // Teyit bekleniyor, pozisyon alma
      longScore  = Math.max(0, longScore  - 10);
      shortScore = Math.max(0, shortScore - 10);
    }

    // 1. MM YÖN — en yüksek ağırlık
    if(mmTarget==='GENUINE_UP')    {longScore+=28;signals.long.push('🎯 MM Gerçek ↑');}
    if(mmTarget==='GENUINE_DOWN')  {shortScore+=28;signals.short.push('🎯 MM Gerçek ↓');}
    // Sweep hedefi tek başına giriş yönü değildir; teyit gelmeden puanı şişirme.
    if(mmTarget==='UP_SWEEP')      {longScore+=4;signals.long.push('🪤 Üst liq mıknatıs — teyit bekle');}
    if(mmTarget==='DOWN_SWEEP')    {shortScore+=4;signals.short.push('🪤 Alt liq mıknatıs — teyit bekle');}
    // 2. STOP HUNT
    if(hunt1h.hunted&&hunt1h.direction==='BULL_HUNT'){longScore+=14;signals.long.push('🎣 Stop Hunt ↑');}
    if(hunt1h.hunted&&hunt1h.direction==='BEAR_HUNT'){shortScore+=14;signals.short.push('🎣 Stop Hunt ↓');}
    if(hunt15m.hunted&&hunt15m.direction==='BULL_HUNT'){longScore+=7;}
    if(hunt15m.hunted&&hunt15m.direction==='BEAR_HUNT'){shortScore+=7;}
    // 3. WS ICEBERG (gerçek zamanlı)
    if(iceberg.signal==='STRONG_HIDDEN_BUY') {longScore+=15;signals.long.push('🧊 WS Iceberg Alıcı');}
    if(iceberg.signal==='STRONG_HIDDEN_SELL'){shortScore+=15;signals.short.push('🧊 WS Iceberg Satıcı');}
    if(iceberg.signal==='HIDDEN_BUY')        {longScore+=8;}
    if(iceberg.signal==='HIDDEN_SELL')       {shortScore+=8;}
    // 4. WS CVD (gerçek zamanlı)
    if(cvd.momentum==='ACCELERATING_BULL'){longScore+=14;signals.long.push(`📊 CVD WS Acc.Buy ${cvd.ratio}%`);}
    if(cvd.momentum==='ACCELERATING_BEAR'){shortScore+=14;signals.short.push('📊 CVD WS Acc.Sell');}
    if(cvd.momentum==='POSITIVE')         {longScore+=6;}
    if(cvd.momentum==='NEGATIVE')         {shortScore+=6;}
    // 5. OB
    if(ob1h.bullOB&&lastPrice<=ob1h.bullOB.high*1.005&&lastPrice>=ob1h.bullOB.low*0.995){longScore+=14;signals.long.push('📦 1h Bull OB');}
    if(ob1h.bearOB&&lastPrice>=ob1h.bearOB.low*0.995&&lastPrice<=ob1h.bearOB.high*1.005){shortScore+=14;signals.short.push('📦 1h Bear OB');}
    if(ob4h.bullOB&&ob4h.bullOB.distPct<2){longScore+=8;signals.long.push('📦 4h Bull OB');}
    if(ob4h.bearOB&&ob4h.bearOB.distPct<2){shortScore+=8;signals.short.push('📦 4h Bear OB');}
    // 6. FUNDING
    if(fundSig==='EXTREME_NEGATIVE'){longScore+=10;signals.long.push(`💸 Fund ${curFund.toFixed(4)}%`);}
    if(fundSig==='NEGATIVE')        {longScore+=5;}
    if(fundSig==='EXTREME_POSITIVE'){shortScore+=10;signals.short.push(`💸 Fund ${curFund.toFixed(4)}%`);}
    if(fundSig==='POSITIVE')        {shortScore+=5;}
    // 7. OI
    if(oiDiv==='CONFIRMED_BULL'){longScore+=10;signals.long.push('📈 OI+Fiyat ↑');}
    if(oiDiv==='SHORT_SQUEEZE') {longScore+=8;signals.long.push('💥 Short Squeeze');}
    if(oiDiv==='CONFIRMED_BEAR'){shortScore+=10;signals.short.push('📉 OI+Fiyat ↓');}
    if(oiDiv==='LONG_LIQUIDATION'){shortScore+=8;signals.short.push('💥 Long Liq');}
    // 8. SMART MONEY
    if(smDiv==='SMART_BULL'){longScore+=12;signals.long.push('🐋 Whale Long');}
    if(smDiv==='SMART_BEAR'){shortScore+=12;signals.short.push('🐋 Whale Short');}
    if(smDiv==='WHALE_LONG'){longScore+=6;}if(smDiv==='WHALE_SHORT'){shortScore+=6;}
    // 9. ORDER BOOK snapshot
    if(bookImb>20){longScore+=6;signals.long.push('📗 Alım Duvarı');}
    if(bookImb<-20){shortScore+=6;signals.short.push('📕 Satış Duvarı');}
    // 10. TEKNİK
    if(t4up){longScore+=8;signals.long.push('📈 4h EMA ↑');}
    if(t4dn){shortScore+=8;signals.short.push('📉 4h EMA ↓');}
    if(rsi4h<35){longScore+=8;signals.long.push(`RSI4h ${rsi4h}`);}
    if(rsi4h>65){shortScore+=8;signals.short.push(`RSI4h ${rsi4h}`);}
    if(lastPrice<bb1h.lower*1.005){longScore+=6;signals.long.push('BB alt');}
    if(lastPrice>bb1h.upper*0.995){shortScore+=6;signals.short.push('BB üst');}
    // 11. 5m scalper katmanı — sadece canlı momentum + hacim + EMA9/21 uyumu puan alır
    const emaBull5=ema9_5m>ema21_5m && lastPrice>ema21_5m;
    const emaBear5=ema9_5m<ema21_5m && lastPrice<ema21_5m;
    if(emaBull5 && takerBuyRatio5m>=54 && volRatio5m>=1.15 && priceChg5m>0){longScore+=12;signals.long.push(`⚡ 5m EMA9/21 + CVD ${takerBuyRatio5m}%`);}
    if(emaBear5 && takerBuyRatio5m<=46 && volRatio5m>=1.15 && priceChg5m<0){shortScore+=12;signals.short.push(`⚡ 5m EMA9/21 + satış ${takerBuyRatio5m}%`);}
    if(lastPrice>vwap5m && candleBody5m>0.25 && volRatio5m>=1.3){longScore+=6;signals.long.push('📍 5m VWAP üstü impuls');}
    if(lastPrice<vwap5m && candleBody5m<-0.25 && volRatio5m>=1.3){shortScore+=6;signals.short.push('📍 5m VWAP altı impuls');}

    if(!dataQuality.ok){ longScore=Math.max(0,longScore-18); shortScore=Math.max(0,shortScore-18); }
    if(!dataQuality.depthOk){ longScore=Math.max(0,longScore-6); shortScore=Math.max(0,shortScore-6); }

    longScore =Math.min(Math.round(longScore*freshnessMult),100);
    shortScore=Math.min(Math.round(shortScore*freshnessMult),100);
    // ── İKİ KATMANLI KARAR MEKANİZMASI ─────────────────────────────────────────
      // A-Tier: Yüksek güven → otomatik işlem açılır
      // B-Tier: Orta güven  → sinyal gösterilir, elle karar ver
      const rawRec=longScore>shortScore&&longScore>=50?'LONG':
                   shortScore>longScore&&shortScore>=50?'SHORT':'WAIT';

      function evalDecision(side){
        if(side==='WAIT')return{pass:false,tier:'WAIT',score:0,reasons:[],blocks:[],autoOk:false};
        if(!dataQuality.ok)return{pass:false,tier:'WAIT',score:0,reasons:[],blocks:['Veri eksik/bayat'],autoOk:false,reason:'Veri kalitesi düşük: mum/depth eksik'};
        const isL=side==='LONG';
        const sw1=sweep1h,sw4=sweep4h,wy1=wyckoff1h,wy4=wyckoff4h;
        const cvdD=getCVD(full);
        const liqD=getLiqData(full);
        const sc=isL?longScore:shortScore;

        const sweepOk=isL
          ?(sw1?.confirmed&&sw1.direction==='BULL_SWEEP')||(sw4?.confirmed&&sw4.direction==='BULL_SWEEP')
          :(sw1?.confirmed&&sw1.direction==='BEAR_SWEEP')||(sw4?.confirmed&&sw4.direction==='BEAR_SWEEP');
        const wyckoffOk=isL
          ?wy1?.recentEvents?.some(e=>e.type==='SPRING'||e.type==='SOS')||wy4?.recentEvents?.some(e=>e.type==='SPRING')
          :wy1?.recentEvents?.some(e=>e.type==='UTAD')||wy4?.recentEvents?.some(e=>e.type==='UTAD');
        // UP_SWEEP/DOWN_SWEEP hedefi tersine giriş için ancak sweep+teyit sonrası geçerlidir.
        const mmOk=isL ? (mmTarget==='GENUINE_UP'||(mmTarget==='DOWN_SWEEP'&&sweepOk))
                       : (mmTarget==='GENUINE_DOWN'||(mmTarget==='UP_SWEEP'&&sweepOk));
        const cvdWarm=(cvdD.historyLen||0)>0 || Math.abs(cvdD.delta||0)>1000;
        const cvdOk=cvdWarm && (isL?cvdD.ratio>52:cvdD.ratio<48);
        const fundOk=isL?fundSig!=='EXTREME_POSITIVE':fundSig!=='EXTREME_NEGATIVE';
        const oiOk=isL?oiDiv!=='CONFIRMED_BEAR'&&oiDiv!=='LONG_LIQUIDATION':oiDiv!=='CONFIRMED_BULL'&&oiDiv!=='SHORT_SQUEEZE';
        const cascOk=liqD?.cascade?(isL&&liqD.cascade.direction==='SHORT_CASCADE')||(!isL&&liqD.cascade.direction==='LONG_CASCADE'):true;
        const ob1hOk=isL?(ob1h?.bullOB&&lastPrice<=ob1h.bullOB.high*1.005&&lastPrice>=ob1h.bullOB.low*0.995)
                        :(ob1h?.bearOB&&lastPrice>=ob1h.bearOB.low*0.995&&lastPrice<=ob1h.bearOB.high*1.005);
        const sweepPending=isL
          ?(sw1?.swept&&!sw1.confirmed&&sw1.direction?.includes('BULL'))
          :(sw1?.swept&&!sw1.confirmed&&sw1.direction?.includes('BEAR'));

        // Kaç kural geçti (0-8)
        const passCount=[sweepOk,wyckoffOk,mmOk,cvdOk,fundOk,oiOk,cascOk,ob1hOk].filter(Boolean).length;
        const hasPrimary=sweepOk||wyckoffOk; // Ana setup var mı

        // A-Tier: primary setup + skor≥70 + funding ok + 5+ kural
        const isTierA=hasPrimary&&sc>=70&&fundOk&&passCount>=5;
        // B-Tier: esnek — (primary veya ob veya sweep-pending) + skor≥60 + funding ok + 4+ kural
        const hasSomeSetup=hasPrimary||ob1hOk||sweepPending||mmOk;
        const isTierB=!isTierA&&hasSomeSetup&&sc>=60&&fundOk&&passCount>=4;

        const reasons=[], blocks=[];
        if(sweepOk)    reasons.push('✅ Sweep+Teyit');
        if(wyckoffOk)  reasons.push('🌊 Wyckoff');
        if(mmOk)       reasons.push(`🎯 MM(${mmTarget})`);
        if(cvdOk)      reasons.push(`📊 CVD${cvdD.ratio.toFixed(0)}%`);
        if(sweepPending)reasons.push('⏳ Sweep bekliyor');
        if(ob1hOk)     reasons.push('📦 1h OB');
        if(liqD?.cascade&&cascOk) reasons.push('💥 Cascade');
        if(!sweepOk&&!wyckoffOk)  blocks.push('Setup yok');
        if(!mmOk)  blocks.push(`MM ters(${mmTarget})`);
        if(!cvdOk) blocks.push(`CVD ters`);
        if(!fundOk)blocks.push(`Fund aşırı`);

        const tier=isTierA?'A':isTierB?'B':'WAIT';
        return{ pass:tier!=='WAIT', tier, score:sc, passCount,
          reasons, blocks, autoOk:isTierA,
          reason: tier==='A'?`🔥 A: ${reasons.slice(0,3).join('·')}`:
                  tier==='B'?`📊 B: ${reasons.slice(0,2).join('·')} — elle bak`:
                  `Yetersiz(${passCount}/8): ${blocks.slice(0,2).join(',')}` };
      }

      let recommendation='WAIT', decisionChain=null;
      if(rawRec!=='WAIT'){
        decisionChain=evalDecision(rawRec);
        recommendation=decisionChain.pass?rawRec:'WAIT';
      }
      if(recommendation==='LONG' || recommendation==='SHORT') {
        proTPSL = calcProTPSL(recommendation, lastPrice, atr1h, atr4h, liq1h, liq4h, ob1h, ob4h, k1h);
      }
    const score=recommendation==='LONG'?longScore:shortScore;

    let suggestedLev=3;
    if(score>=85&&atrPct<0.5)suggestedLev=25;
    else if(score>=80&&atrPct<0.8)suggestedLev=20;
    else if(score>=75&&atrPct<1.0)suggestedLev=15;
    else if(score>=70&&atrPct<1.5)suggestedLev=10;
    else if(score>=65&&atrPct<2.0)suggestedLev=7;
    else if(score>=60&&atrPct<2.5)suggestedLev=5;
    else if(score>=55)suggestedLev=4;
    else suggestedLev=3;
    suggestedLev=Math.min(suggestedLev,50);

    res.json({
      ok:true, symbol:full, price:lastPrice,
      freshness, signalAgeMin, isExpired,
      marketMaker:{target:mmTarget,confidence:mmConf,reasoning:mmReasoning,
        nextTarget:mmNextTarget,retailBias,smartMoneyBias:smBias,oiTrend},
      liquidityLevels:{'1h':liq1h,'4h':liq4h},
      stopHunt:{'1h':hunt1h,'15m':hunt15m},
      sweepConfirm:{'1h':sweep1h,'4h':sweep4h,'15m':sweep15m},
      wyckoff:{'1h':wyckoff1h,'4h':wyckoff4h,'15m':wyckoff15m},
      liquidations: liqData,
      orderBlocks:{'1h':ob1h,'4h':ob4h},
      // WS gerçek zamanlı veriler
      cvd, iceberg,
      // Coinglass likidite haritası
      coinglass: cgData,
      proTPSL,
      leverage:{suggested:suggestedLev,min:3,max:50,atrPct:+atrPct.toFixed(2)},
      dataQuality,
      scalp5m:{priceChg5m,volRatio5m,takerBuyRatio5m,ema9:+ema9_5m.toFixed(8),ema21:+ema21_5m.toFixed(8),ema50:+ema50_5m.toFixed(8),vwap:+vwap5m.toFixed(8),atr:+atr5m.toFixed(8)},
      timeframes:{
        '4h':{rsi:rsi4h,ema20:+ema20_4h.toFixed(4),ema50:+ema50_4h.toFixed(4),ema200:+ema200_4h.toFixed(4),trend:t4up?'UP':t4dn?'DOWN':'RANGE',atr:+atr4h.toFixed(4)},
        '1h':{rsi:rsi1h,ema20:+ema20_1h.toFixed(4),ema50:+ema50_1h.toFixed(4),ema200:+ema200_1h.toFixed(4),trend:ema20_1h>ema50_1h?'UP':'DOWN',vwap:+vwap1h.toFixed(4),atr:+atr1h.toFixed(4),bb:bb1h},
        '15m':{rsi:rsi15m,atr:+atr15m_.toFixed(4),bb:bb15m_},
        '5m':{rsi:rsi5m,ema9:+ema9_5m.toFixed(8),ema21:+ema21_5m.toFixed(8),ema50:+ema50_5m.toFixed(8),vwap:+vwap5m.toFixed(8),atr:+atr5m.toFixed(8),volRatio:volRatio5m,takerBuyRatio:takerBuyRatio5m,priceChg:priceChg5m},
      },
      funding:{current:+curFund.toFixed(4),signal:fundSig},
      openInterest:{change1h:+oiChg1h.toFixed(2),change4h:+oiChg4h.toFixed(2),divergence:oiDiv},
      smartMoney:{topLongPct:+topLong.toFixed(1),globalLongPct:+globalLong.toFixed(1),divergence:smDiv},
      orderBook:{imbalance:+bookImb.toFixed(1)},
      longScore, shortScore, recommendation, decisionChain,
      signals:recommendation==='LONG'?signals.long.slice(0,6):signals.short.slice(0,6),
    });
  } catch(e) { res.status(400).json({ error:e.message }); }
});

// ── HESAP ─────────────────────────────────────────────────────────────────────
app.post('/api/account', async (req, res) => {
  const{apiKey,apiSecret}=req.body;
  if(!apiKey||!apiSecret)return res.status(400).json({error:'API key gerekli'});
  try{
    let w=0,a=0,u=0;
    try{const b=await bReq(apiKey,apiSecret,'GET','/fapi/v3/balance');const ub=Array.isArray(b)?b.find(x=>x.asset==='USDT'):null;if(ub){w=parseFloat(ub.balance)||0;a=parseFloat(ub.availableBalance)||0;}}catch(e){}
    const data=await bReq(apiKey,apiSecret,'GET','/fapi/v2/account');
    if(parseFloat(data.totalWalletBalance)>0)w=parseFloat(data.totalWalletBalance);
    if(parseFloat(data.availableBalance)>0)a=parseFloat(data.availableBalance);
    u=parseFloat(data.totalUnrealizedProfit)||0;
    res.json({ok:true,totalWalletBalance:w,availableBalance:a,totalUnrealizedProfit:u,
      positions:(data.positions||[]).filter(p=>parseFloat(p.positionAmt)!==0).map(p=>({
        symbol:p.symbol,side:parseFloat(p.positionAmt)>0?'LONG':'SHORT',
        positionAmt:Math.abs(parseFloat(p.positionAmt)),entryPrice:parseFloat(p.entryPrice),
        markPrice:parseFloat(p.markPrice),unrealizedProfit:parseFloat(p.unRealizedProfit),
        leverage:parseInt(p.leverage),liquidationPrice:parseFloat(p.liquidationPrice),
      }))
    });
  }catch(e){res.status(400).json({error:e.message});}
});

// ── EMİR AÇ ──────────────────────────────────────────────────────────────────
app.post('/api/order', async (req, res) => {
  const{apiKey,apiSecret,symbol,side,leverage,marginType,targetPrice,stopPrice,usdtAmount}=req.body;
  if(!apiKey||!apiSecret||!symbol||!side||!leverage||!targetPrice||!stopPrice||!usdtAmount)
    return res.status(400).json({error:'Eksik parametre'});
  const sym=symbol.toUpperCase().includes('USDT')?symbol.toUpperCase():symbol.toUpperCase()+'USDT';
  const isLong=side.toUpperCase()==='LONG';
  const oSide=isLong?'BUY':'SELL', cSide=isLong?'SELL':'BUY';
  try{
    if(marginType){try{await bReq(apiKey,apiSecret,'POST','/fapi/v1/marginType',{symbol:sym,marginType:marginType.toUpperCase()});}catch(e){if(!e.message.includes('No need'))console.log('MarginType:',e.message);}}
    await bReq(apiKey,apiSecret,'POST','/fapi/v1/leverage',{symbol:sym,leverage:parseInt(leverage)});
    let stepSize=0.001,tickSize=0.01,minNot=5;
    try{
      const si=await bPub('/fapi/v1/exchangeInfo','symbol='+sym);
      const s=Array.isArray(si.symbols)?si.symbols.find(x=>x.symbol===sym):null;
      if(s){
        const lf=s.filters.find(f=>f.filterType==='LOT_SIZE');
        const pf=s.filters.find(f=>f.filterType==='PRICE_FILTER');
        const mf=s.filters.find(f=>f.filterType==='MIN_NOTIONAL');
        if(lf)stepSize=parseFloat(lf.stepSize);
        if(pf)tickSize=parseFloat(pf.tickSize);
        if(mf)minNot=parseFloat(mf.notional||mf.minNotional||5);
      }
    }catch(e){}
    const pr=await bPub('/fapi/v1/ticker/price','symbol='+sym);
    const curPrice=parseFloat(pr.price)||0;
    if(!curPrice)throw new Error('Fiyat alınamadı');
    const qp=stepSize<1?-Math.floor(Math.log10(stepSize)):0;
    const pp=tickSize<1?-Math.floor(Math.log10(tickSize)):0;
    const qty=parseFloat(((parseFloat(usdtAmount)*parseInt(leverage))/curPrice).toFixed(qp));
    const rnd=p=>parseFloat(parseFloat(p).toFixed(pp));
    if(qty*curPrice<minNot)throw new Error(`Min işlem $${minNot}. Miktarı artır.`);
    const main=await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',{
      symbol:sym,side:oSide,type:'MARKET',quantity:qty,positionSide:'BOTH'
    });
    await new Promise(r=>setTimeout(r,800));
    let execPrice=parseFloat(main.avgPrice||curPrice);
    try{
      const pos=await bReq(apiKey,apiSecret,'GET','/fapi/v2/positionRisk',{symbol:sym});
      const p=Array.isArray(pos)?pos.find(x=>x.symbol===sym&&Math.abs(parseFloat(x.positionAmt))>0):null;
      if(p&&parseFloat(p.entryPrice)>0)execPrice=parseFloat(p.entryPrice);
    }catch(e){}
    const ratio=execPrice/curPrice;
    const realTP=rnd(parseFloat(targetPrice)*ratio);
    const realSL=rnd(parseFloat(stopPrice)*ratio);
    const ps=realTP.toString(),ss=realSL.toString(),qs=qty.toString();
    console.log(`${sym} giriş:${execPrice} TP:${realTP} SL:${realSL} lev:${leverage}`);

    // ── TP/SL ────────────────────────────────────────────────────────────────
    // "Target strategy invalid" = fiyat yanlış yönde
    // LONG: TP > execPrice, SL < execPrice
    // SHORT: TP < execPrice, SL > execPrice
    // Fiyatları doğrula ve düzelt
    let finalTP = realTP, finalSL = realSL;
    if (isLong) {
      if (finalTP <= execPrice) finalTP = execPrice * 1.05; // TP en az %5 yukarı
      if (finalSL >= execPrice) finalSL = execPrice * 0.95; // SL en az %5 aşağı
    } else {
      if (finalTP >= execPrice) finalTP = execPrice * 0.95;
      if (finalSL <= execPrice) finalSL = execPrice * 1.05;
    }
    finalTP = rnd(finalTP);
    finalSL = rnd(finalSL);
    console.log(`${sym} lev:${leverage} giriş:${execPrice} TP:${finalTP} SL:${finalSL} (isLong:${isLong})`);

    async function placeSLTP(marketType, price) {
      const p  = price.toString();
      const q  = qty.toString();
      let needsAlgo = false;
      // Binance kuralı:
      // LONG TP: stopPrice > markPrice → TAKE_PROFIT_MARKET SELL
      // LONG SL: stopPrice < markPrice → STOP_MARKET SELL
      // SHORT TP: stopPrice < markPrice → TAKE_PROFIT_MARKET BUY
      // SHORT SL: stopPrice > markPrice → STOP_MARKET BUY
      const formats = [
        // Format 1: closePosition (en standart)
        {type:marketType, stopPrice:p, closePosition:'true', positionSide:'BOTH', workingType:'MARK_PRICE'},
        {type:marketType, stopPrice:p, closePosition:'true', positionSide:'BOTH', workingType:'CONTRACT_PRICE'},
        {type:marketType, stopPrice:p, closePosition:'true', positionSide:'BOTH'},
        // Format 2: quantity + reduceOnly
        {type:marketType, stopPrice:p, quantity:q, reduceOnly:'true', positionSide:'BOTH', workingType:'MARK_PRICE'},
        {type:marketType, stopPrice:p, quantity:q, reduceOnly:'true', positionSide:'BOTH'},
      ];
      for (const params of formats) {
        try {
          const r = await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',
            {symbol:sym, side:cSide, ...params});
          if (r.orderId) {
            console.log(`${marketType} BAŞARILI (${params.workingType||'no-wt'}): ${r.orderId}`);
            return r;
          }
        } catch(e) {
          const m = e.message||'';
          console.log(`${marketType} hata (${params.workingType||'no-wt'}): ${m.substring(0,80)}`);
          // -4120: algo gerekiyor → döngüden çık, algo dene
          if (m.includes('-4120')) { needsAlgo = true; break; }
        }
      }

      // Algo order dene (yeni coinler: OPG, PENDLE, HUSDT vb.)
      if (needsAlgo || true) { // her zaman dene, standart başarısız olursa
        const algoType = marketType==='TAKE_PROFIT_MARKET' ? 'TAKE_PROFIT' : 'STOP';
        const algoFormats = [
          { symbol:sym, side:cSide, orderType:algoType, stopPrice:p,
            quantity:q, positionSide:'BOTH', workingType:'MARK_PRICE', timeInForce:'GTE_GTC' },
          { symbol:sym, side:cSide, orderType:algoType, stopPrice:p,
            quantity:q, positionSide:'BOTH', workingType:'CONTRACT_PRICE', timeInForce:'GTE_GTC' },
          { symbol:sym, side:cSide, orderType:algoType, stopPrice:p,
            quantity:q, positionSide:'BOTH', timeInForce:'GTE_GTC' },
          { symbol:sym, side:cSide, orderType:algoType, stopPrice:p,
            quantity:q, positionSide:'BOTH' },
        ];
        for (const algoParams of algoFormats) {
          try {
            const r = await bAlgo(apiKey, apiSecret, algoParams);
            const id = r.clientAlgoId || r.algoId || r.orderId;
            if (id) {
              console.log(`${marketType} ALGO BAŞARILI (${algoParams.workingType||'no-wt'}): ${id}`);
              return { orderId: id };
            }
          } catch(e) {
            console.log(`${marketType} algo hata (${algoParams.workingType||'no-wt'}): ${(e.message||'').substring(0,60)}`);
          }
        }
      }

      console.log(`${marketType} TÜM YÖNTEMLER BAŞARISIZ`);
      return {orderId:null};
    }

    const tp = await placeSLTP('TAKE_PROFIT_MARKET', finalTP);
    await new Promise(r=>setTimeout(r,500));
    const sl = await placeSLTP('STOP_MARKET', finalSL);
    const tpOk=!!tp.orderId,slOk=!!sl.orderId;

    res.json({ok:true,
      message:`${sym} ${side} açıldı ✅${tpOk?` TP#${tp.orderId}`:'❌ TP manuel'}${slOk?` SL#${sl.orderId}`:'❌ SL manuel'}`,
      mainOrderId:main.orderId,tpOrderId:tp.orderId,slOrderId:sl.orderId,
      tpSuccess:tpOk,slSuccess:slOk,executedPrice:execPrice,
      details:{symbol:sym,side,quantity:qty,leverage,entry:execPrice,target:realTP,stop:realSL}
    });
  }catch(e){res.status(400).json({error:e.message});}
});

// ── POZİSYONLAR ──────────────────────────────────────────────────────────────
app.post('/api/positions', async (req, res) => {
  const{apiKey,apiSecret}=req.body;
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

// ── BREAK-EVEN STOP — Kâra geçince SL'yi giriş fiyatına çek ────────────────
app.post('/api/update-sl', async (req, res) => {
  const { apiKey, apiSecret, symbol, newSL, cancelExisting } = req.body;
  if (!apiKey||!apiSecret||!symbol||!newSL)
    return res.status(400).json({ error:'Eksik parametre' });
  const sym = symbol.toUpperCase().includes('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  try {
    // Mevcut pozisyonu al
    const pos = await bReq(apiKey,apiSecret,'GET','/fapi/v2/positionRisk',{symbol:sym});
    const p = Array.isArray(pos) ? pos.find(x=>x.symbol===sym&&Math.abs(parseFloat(x.positionAmt))>0) : null;
    if (!p) return res.status(400).json({ error:'Açık pozisyon yok' });

    const isLong = parseFloat(p.positionAmt) > 0;
    const cSide  = isLong ? 'SELL' : 'BUY';
    const qty    = Math.abs(parseFloat(p.positionAmt)).toString();

    // Mevcut TP korunur; sadece eski STOP/STOP_MARKET emirleri temizlenir.
    if (cancelExisting) await cancelStopOrdersOnly(apiKey, apiSecret, sym);

    // Yeni SL yerleştir
    const sl = await placeProtectiveStop(apiKey, apiSecret, sym, cSide, qty, newSL);

    res.json({ ok:true, message:`${sym} SL güncellendi → $${newSL}`, orderId:sl.orderId });
  } catch(e) { res.status(400).json({ error:e.message }); }
});

// ── OTOMATİK TRAILING SL ──────────────────────────────────────────────────────
// Pozisyon açıldıktan sonra fiyat hareket ettikçe SL'yi takip ettirir
// TP emrini silmeden sadece eski SL emirlerini yeniler.
const trailingState = new Map(); // symbol → {entryPrice, highWater, slOrderId, config}

async function cancelStopOrdersOnly(apiKey, apiSecret, sym) {
  try {
    const open = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/openOrders', { symbol: sym });
    if (!Array.isArray(open)) return 0;
    let n = 0;
    for (const o of open) {
      const t = String(o.type || '').toUpperCase();
      // TAKE_PROFIT emirlerini koru; sadece STOP / STOP_MARKET / trailing stop iptal edilir.
      const isStopOnly = (t === 'STOP' || t === 'STOP_MARKET' || t === 'TRAILING_STOP_MARKET');
      if (!isStopOnly) continue;
      try {
        await bReq(apiKey, apiSecret, 'DELETE', '/fapi/v1/order', { symbol: sym, orderId: o.orderId });
        n++;
      } catch(e) {}
    }
    return n;
  } catch(e) { return 0; }
}

async function placeProtectiveStop(apiKey, apiSecret, sym, side, qty, newSL) {
  const p = parseFloat(newSL).toString();
  const q = parseFloat(qty).toString();
  try {
    const r = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
      symbol: sym, side, type: 'STOP_MARKET', stopPrice: p,
      quantity: q, reduceOnly: 'true', positionSide: 'BOTH', workingType: 'MARK_PRICE'
    });
    return { ok:true, orderId:r.orderId };
  } catch(e1) {
    // Bazı yeni kontratlarda klasik STOP_MARKET yerine algo endpoint gerekir.
    try {
      const r2 = await bAlgo(apiKey, apiSecret, {
        symbol: sym, side, orderType: 'STOP', stopPrice: p,
        quantity: q, positionSide: 'BOTH', workingType: 'MARK_PRICE', timeInForce: 'GTE_GTC'
      });
      return { ok:true, orderId:r2.clientAlgoId || r2.algoId || r2.orderId };
    } catch(e2) {
      throw new Error(`SL yerleştirilemedi: ${e1.message}; algo: ${e2.message}`);
    }
  }
}

async function checkTrailingSL(apiKey, apiSecret, positions) {
  for (const pos of positions) {
    const sym = pos.symbol;
    const ts = trailingState.get(sym);
    if (!ts || !ts.config?.trailing) continue;

    const { entryPrice, trailPct, breakEvenPct } = ts.config;
    const curPrice = pos.markPrice;
    const isLong = pos.side === 'LONG';
    const pnlPct = Math.abs(curPrice - entryPrice) / entryPrice * 100;
    const inProfit = isLong ? curPrice > entryPrice : curPrice < entryPrice;

    let newSL = null;

    // Break-even: pozisyon breakEvenPct kâra geçince SL giriş fiyatına çek
    if (inProfit && pnlPct >= breakEvenPct && !ts.breakEvenSet) {
      newSL = entryPrice;
      ts.breakEvenSet = true;
      ts.highWater = curPrice;
      console.log(`${sym} Break-even SL: $${newSL}`);
    }

    // Trailing: highWater'ı güncelle, SL'yi takip ettir
    if (inProfit && ts.breakEvenSet) {
      const newHigh = isLong ? Math.max(ts.highWater||curPrice, curPrice)
                             : Math.min(ts.highWater||curPrice, curPrice);
      if (newHigh !== ts.highWater) {
        ts.highWater = newHigh;
        // SL = highWater'dan trailPct% geri
        newSL = isLong
          ? +(ts.highWater * (1 - trailPct/100)).toFixed(8)
          : +(ts.highWater * (1 + trailPct/100)).toFixed(8);
        // Önceki SL'den daha iyi mi?
        if (ts.currentSL) {
          const better = isLong ? newSL > ts.currentSL : newSL < ts.currentSL;
          if (!better) newSL = null;
        }
      }
    }

    if (newSL) {
      try {
        // TP emrini silmeden yalnızca eski SL'leri iptal et ve yeni SL yaz.
        await cancelStopOrdersOnly(apiKey, apiSecret, sym);
        const qty = pos.positionAmt.toString();
        const cSide = isLong ? 'SELL' : 'BUY';
        const r = await placeProtectiveStop(apiKey, apiSecret, sym, cSide, qty, newSL);
        ts.currentSL = newSL;
        ts.slOrderId = r.orderId;
        console.log(`${sym} Trailing/BE SL → $${newSL} (orderId:${r.orderId})`);
      } catch(e) {
        console.log(`${sym} Trailing SL hata: ${e.message}`);
      }
    }
  }
}

// ── KAPAT ─────────────────────────────────────────────────────────────────────
app.post('/api/close', async (req, res) => {
  const{apiKey,apiSecret,symbol}=req.body;
  if(!apiKey||!apiSecret||!symbol)return res.status(400).json({error:'Eksik parametre'});
  const sym=symbol.toUpperCase().includes('USDT')?symbol.toUpperCase():symbol.toUpperCase()+'USDT';
  try{
    try{await bReq(apiKey,apiSecret,'DELETE','/fapi/v1/allOpenOrders',{symbol:sym});}catch(e){}
    const pos=await bReq(apiKey,apiSecret,'GET','/fapi/v2/positionRisk',{symbol:sym});
    const arr=Array.isArray(pos)?pos:[];
    const p=arr.find(x=>Math.abs(parseFloat(x.positionAmt))>0);
    if(!p)return res.json({ok:true,message:'Açık pozisyon yok'});
    const order=await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',{
      symbol:sym,side:parseFloat(p.positionAmt)>0?'SELL':'BUY',
      type:'MARKET',quantity:Math.abs(parseFloat(p.positionAmt)),reduceOnly:'true',positionSide:'BOTH'
    });
    res.json({ok:true,message:`${sym} kapatıldı`,orderId:order.orderId});
  }catch(e){res.status(400).json({error:e.message});}
});

// ── OTOMATİK İŞLEM MOTORU ────────────────────────────────────────────────────
// Kullanıcı ayarları:
// autoTrade: { enabled, apiKey, apiSecret, usdtAmount, leverage, marginType,
//   maxPositions, minScore, allowLong, allowShort,
//   trailingPct, breakEvenPct, symbols:[] }

function nNum(v, def, min, max) {
  const x = Number(v);
  if (!Number.isFinite(x)) return def;
  return Math.max(min, Math.min(max, x));
}
function normalizeAutoConfig(raw={}) {
  return {
    enabled: !!raw.enabled,
    apiKey: raw.apiKey,
    apiSecret: raw.apiSecret,
    usdtAmount: nNum(raw.usdtAmount, 15, 5, 100000),
    leverage: Math.round(nNum(raw.leverage, 15, 1, 50)),
    marginType: String(raw.marginType || 'ISOLATED').toUpperCase() === 'CROSSED' ? 'CROSSED' : 'ISOLATED',
    maxPositions: Math.round(nNum(raw.maxPositions, 1, 1, 10)),
    minScore: Math.round(nNum(raw.minScore, 72, 50, 95)),
    allowLong: raw.allowLong !== false,
    allowShort: raw.allowShort !== false,
    sweepOnly: raw.sweepOnly !== false,
    stopLossPct: nNum(raw.stopLossPct, 2, 0.2, 10),
    takeProfitPct: nNum(raw.takeProfitPct, 10, 0.5, 50),
    trailingPct: nNum(raw.trailingPct, 2, 0.2, 10),
    breakEvenPct: nNum(raw.breakEvenPct, 1, 0.2, 10),
    symbols: Array.isArray(raw.symbols) ? raw.symbols : []
  };
}

let autoConfig = null;
let autoRunning = false;
let autoTimer = null;
let lastAutoEntryScanAt = 0;
const AUTO_ENTRY_SCAN_MS = 3 * 60 * 1000;
const autoLog = []; // Son 50 otomatik işlem logu

app.post('/api/auto/config', (req, res) => {
  autoConfig = normalizeAutoConfig(req.body);
  if (!autoConfig.apiKey || !autoConfig.apiSecret) return res.status(400).json({ error:'API key gerekli' });
  if (autoConfig.enabled) {
    startAutoTrader();
    res.json({ ok:true, message:'Otomatik işlem başlatıldı', config:autoConfig });
  } else {
    stopAutoTrader();
    res.json({ ok:true, message:'Otomatik işlem durduruldu' });
  }
});

app.get('/api/auto/status', (req, res) => {
  res.json({ ok:true, enabled:!!autoConfig?.enabled, running:autoRunning,
    config:autoConfig, recentLogs:autoLog.slice(-20) });
});

function logAuto(msg) {
  const entry = `${new Date().toLocaleTimeString('tr-TR')} ${msg}`;
  autoLog.push(entry);
  if (autoLog.length > 50) autoLog.shift();
  console.log('[AUTO]', entry);
}

function stopAutoTrader() {
  if (autoTimer) { clearInterval(autoTimer); autoTimer=null; }
  autoRunning = false;
  logAuto('Otomatik işlem durduruldu');
}

async function runAutoScan() {
  if (autoRunning || !autoConfig?.enabled) return;
  autoRunning = true;

  try {
    const cfg = autoConfig;
    const { apiKey, apiSecret, usdtAmount, leverage, marginType,
      maxPositions=1, minScore=72, allowLong=true, allowShort=true,
      sweepOnly=true, stopLossPct=2, takeProfitPct=10,
      trailingPct=2, breakEvenPct=1, symbols=[] } = cfg;

    // 1. Mevcut pozisyonları kontrol et
    const posData = await bReq(apiKey,apiSecret,'GET','/fapi/v2/positionRisk');
    const openPos = Array.isArray(posData)
      ? posData.filter(p=>Math.abs(parseFloat(p.positionAmt))>0)
      : [];

    // Trailing SL kontrol
    if (openPos.length > 0) {
      const mapped = openPos.map(p=>({
        symbol:p.symbol, side:parseFloat(p.positionAmt)>0?'LONG':'SHORT',
        positionAmt:Math.abs(parseFloat(p.positionAmt)),
        entryPrice:parseFloat(p.entryPrice), markPrice:parseFloat(p.markPrice),
        unrealizedProfit:parseFloat(p.unRealizedProfit)
      }));
      // Her açık pozisyon için trailing state oluştur
      for (const pos of mapped) {
        if (!trailingState.has(pos.symbol)) {
          trailingState.set(pos.symbol, {
            entryPrice:pos.entryPrice, highWater:pos.markPrice,
            breakEvenSet:false, currentSL:null,
            config:{ trailing:true, trailPct:trailingPct, breakEvenPct, entryPrice:pos.entryPrice }
          });
        }
      }
      await checkTrailingSL(apiKey, apiSecret, mapped);
    }

    // Max pozisyon kontrolü
    if (openPos.length >= maxPositions) {
      logAuto(`Max pozisyon (${maxPositions}) doldu, yeni sinyal taranmıyor`);
      return;
    }

    // Açık pozisyon yönetimi 30 sn'de bir yapılır; yeni giriş taraması 3 dk'da bir yapılır.
    const nowScan = Date.now();
    if (lastAutoEntryScanAt && nowScan - lastAutoEntryScanAt < AUTO_ENTRY_SCAN_MS) return;
    lastAutoEntryScanAt = nowScan;

    // 2. Coin listesi al
    const coinsResp = await fetch(`http://localhost:${PORT}/api/futures-coins`).then(r=>r.json());
    if (!coinsResp.ok || !coinsResp.coins) return;

    // Taranacak coinler: belirlenmişse onlar, yoksa hacimli top 20
    let scanList = symbols.length > 0
      ? coinsResp.coins.filter(c=>symbols.includes(c.symbol))
      : coinsResp.coins.sort((a,b)=>(b.hotRankScore||0)-(a.hotRankScore||0)).slice(0,12); // kalite: sıcak/hacimli ilk 12

    // Haber kontrolü — tehlikeli saatlerde işlem açma
    try {
      const cal = await fetch(`http://localhost:${PORT}/api/calendar`).then(r=>r.json());
      if (cal.dangerZone) {
        logAuto('⛔ Haber saati — tarama durduruldu: ' + cal.todayEvents.map(e=>e.event).join(', '));
        return;
      }
    } catch(e) {}

    // Fear & Greed filtresi
    let fgSignal = 'NEUTRAL';
    try {
      const fg = await fetch(`http://localhost:${PORT}/api/market-mood`).then(r=>r.json());
      fgSignal = fg.signal;
      if (fg.mood === 'EXTREME_GREED' && !allowShort) {
        logAuto('⚠️ Extreme Greed — sadece short izinli, long atlanıyor');
      }
    } catch(e) {}

    logAuto(`Tarama başladı: ${scanList.length} coin, max poz:${maxPositions}, mevcut:${openPos.length}`);

    // 3. Her coini analiz et
    for (const coin of scanList) {
      if (openPos.length + (await getNewPosCount()) >= maxPositions) break;

      // Zaten pozisyon var mı?
      const alreadyOpen = openPos.some(p=>p.symbol===coin.fullSymbol);
      if (alreadyOpen) continue;

      try {
        const analysis = await fetch(`http://localhost:${PORT}/api/analyze/${coin.fullSymbol}`)
          .then(r=>r.json());
        if (!analysis.ok) continue;

        const { longScore, shortScore, recommendation, isExpired, freshness } = analysis;
        if (isExpired || freshness === 'EXPIRED') continue;

        // ── BEYİN KARAR ZİNCİRİ — 5 KURAL, SIRAYLA ──────────────────────────────
        // Hepsi geçmezse işlem açılmaz. Basit. Net.

        const score = recommendation==='LONG'?longScore:shortScore;
        const isLong = recommendation==='LONG';
        const isShort = recommendation==='SHORT';

        // Kural 0: Yön izni
        if (isLong  && !allowLong)  continue;
        if (isShort && !allowShort) continue;
        if (recommendation==='WAIT') continue;
        const decisionChain = analysis.decisionChain || { autoOk:false, tier:'WAIT', reason:'Karar zinciri yok' };

        // Kural 1: Wyckoff Spring VEYA Sweep+Teyit ZORUNLU
        // Bu olmadan giriş yok — MM henüz oyununu bitirmedi
        const sw1h = analysis.sweepConfirm?.['1h'];
        const sw4h = analysis.sweepConfirm?.['4h'];
        const wy1h = analysis.wyckoff?.['1h'];
        const wy4h = analysis.wyckoff?.['4h'];

        const hasBullSetup = (
          (sw1h?.confirmed && sw1h.direction==='BULL_SWEEP') ||
          (sw4h?.confirmed && sw4h.direction==='BULL_SWEEP') ||
          wy1h?.recentEvents?.some(e=>e.type==='SPRING') ||
          wy4h?.recentEvents?.some(e=>e.type==='SPRING') ||
          wy1h?.recentEvents?.some(e=>e.type==='SOS')
        );
        const hasBearSetup = (
          (sw1h?.confirmed && sw1h.direction==='BEAR_SWEEP') ||
          (sw4h?.confirmed && sw4h.direction==='BEAR_SWEEP') ||
          wy1h?.recentEvents?.some(e=>e.type==='UTAD') ||
          wy4h?.recentEvents?.some(e=>e.type==='UTAD')
        );

        if (sweepOnly && isLong  && !hasBullSetup) { logAuto(`${coin.symbol} LONG ama setup yok — atlandı`); continue; }
        if (sweepOnly && isShort && !hasBearSetup) { logAuto(`${coin.symbol} SHORT ama setup yok — atlandı`); continue; }

        // Kural 2: MM yönü uyumlu mu?
        const mm = analysis.marketMaker;
        const mmOk = isLong
          ? mm?.target==='GENUINE_UP' || mm?.target==='UP_SWEEP'
          : mm?.target==='GENUINE_DOWN' || mm?.target==='DOWN_SWEEP';
        if (!mmOk) { logAuto(`${coin.symbol} MM yönü uyumsuz (${mm?.target}) — atlandı`); continue; }

        // Kural 3: CVD teyit ediyor mu?
        const cvd = analysis.cvd;
        const cvdOk = isLong
          ? cvd?.ratio > 45  // Alımlar baskın ya da nötr
          : cvd?.ratio < 55; // Satışlar baskın ya da nötr
        if (!cvdOk) { logAuto(`${coin.symbol} CVD ters (${cvd?.ratio}%) — atlandı`); continue; }

        // Kural 4: Funding aşırı karşı yönde değil mi?
        const fund = analysis.funding;
        const fundOk = isLong
          ? fund?.signal !== 'EXTREME_POSITIVE'  // Aşırı pozitif funding → long tehlikeli
          : fund?.signal !== 'EXTREME_NEGATIVE'; // Aşırı negatif funding → short tehlikeli
        if (!fundOk) { logAuto(`${coin.symbol} Funding aşırı karşı (${fund?.current}) — atlandı`); continue; }

        // Kural 5: Skor yeterli mi?
        if (score < minScore) { logAuto(`${coin.symbol} skor ${score} < ${minScore} — atlandı`); continue; }

        // F&G: Extreme durumlarda ters yön yasak
        if (fgSignal==='EXTREME_GREED' && isLong)  { logAuto(`${coin.symbol} Extreme Greed — long atlandı`); continue; }
        if (fgSignal==='EXTREME_FEAR'  && isShort) { logAuto(`${coin.symbol} Extreme Fear — short atlandı`); continue; }

        // Likidasyon cascade var mı? Varsa ters yönde git (cascade yönünün tersi karlı)
        const liq = analysis.liquidations;
        if (liq?.cascade) {
          const cascadeOk = isLong
            ? liq.cascade.direction==='SHORT_CASCADE'   // Shortlar patladı → long iyi
            : liq.cascade.direction==='LONG_CASCADE';   // Longlar patladı → short iyi
          if (!cascadeOk) { logAuto(`${coin.symbol} Cascade yönü ters — atlandı`); continue; }
        }

        // ── KATMAN KONTROLÜ ──────────────────────────────────────────────────
        // Otomatik işlem sadece A-Tier için açılır
        // B-Tier sinyaller gösterilir ama otomatik açılmaz
        const tierOk = decisionChain?.autoOk; // sadece A-tier
        if (!tierOk) {
          logAuto(`📊 ${coin.symbol} B-Tier sinyal (${decisionChain?.reason}) — otomatik açılmıyor`);
          continue;
        }
        logAuto(`🔥 ${coin.symbol} A-Tier! ${decisionChain?.reason}`);

        // TP/SL: kullanıcının otomatik işlem panelindeki coin-hareketi yüzdeleri önceliklidir.
        const basePrice = Number(analysis.price || coin.price);
        let targetPrice, stopPrice;
        if (Number.isFinite(basePrice) && basePrice > 0) {
          if (recommendation === 'LONG') {
            targetPrice = +(basePrice * (1 + takeProfitPct / 100)).toFixed(8);
            stopPrice   = +(basePrice * (1 - stopLossPct / 100)).toFixed(8);
          } else {
            targetPrice = +(basePrice * (1 - takeProfitPct / 100)).toFixed(8);
            stopPrice   = +(basePrice * (1 + stopLossPct / 100)).toFixed(8);
          }
        } else if (analysis.proTPSL) {
          targetPrice = analysis.proTPSL.tp;
          stopPrice   = analysis.proTPSL.sl;
        } else {
          logAuto(`${coin.symbol} TP/SL hesaplanamadı — atlandı`);
          continue;
        }

        logAuto(`🎯 Sinyal: ${coin.symbol} ${recommendation} skor:${score} marj:${usdtAmount}$ lev:${leverage}x SL:${stopLossPct}% TP:${takeProfitPct}% — emir açılıyor`);

        // İşlemi aç
        const orderResp = await fetch(`http://localhost:${PORT}/api/order`, {
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify({
            apiKey, apiSecret,
            symbol: coin.fullSymbol,
            side: recommendation,
            leverage, marginType,
            targetPrice, stopPrice,
            usdtAmount
          })
        }).then(r=>r.json());

        if (orderResp.ok) {
          logAuto(`✅ ${coin.symbol} ${recommendation} açıldı — ${orderResp.message}`);
          // Trailing state başlat
          trailingState.set(coin.fullSymbol, {
            entryPrice: orderResp.executedPrice||analysis.price,
            highWater: orderResp.executedPrice||analysis.price,
            breakEvenSet:false, currentSL:null,
            config:{ trailing:true, trailPct:trailingPct, breakEvenPct,
              entryPrice:orderResp.executedPrice||analysis.price }
          });
          // Bir sonraki coin için bekle
          await new Promise(r=>setTimeout(r,2000));
        } else {
          logAuto(`❌ ${coin.symbol} hata: ${orderResp.error}`);
        }
      } catch(e) {
        logAuto(`${coin.symbol} analiz hata: ${e.message?.substring(0,50)}`);
      }
    }

  } catch(e) {
    logAuto(`Tarama hatası: ${e.message?.substring(0,80)}`);
  } finally {
    autoRunning = false;
  }
}

async function getNewPosCount() {
  try {
    const cfg = autoConfig;
    if (!cfg?.apiKey) return 0;
    const d = await bReq(cfg.apiKey,cfg.apiSecret,'GET','/fapi/v2/positionRisk');
    return Array.isArray(d) ? d.filter(p=>Math.abs(parseFloat(p.positionAmt))>0).length : 0;
  } catch(e) { return 0; }
}

function startAutoTrader() {
  stopAutoTrader();
  logAuto('Otomatik işlem başlatıldı');
  // Pozisyon yönetimi 30 sn, yeni giriş taraması 3 dk throttled.
  autoTimer = setInterval(runAutoScan, 30*1000);
  runAutoScan(); // Hemen başlat
}

app.listen(PORT, ()=>console.log(`✅ Server ${PORT}`));
