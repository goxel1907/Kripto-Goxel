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

      // Cascade tespiti: hacme göre dinamik eşik
  // BTC: $2M+, büyük altcoin: $500K+, küçük coin: $100K+
      const shortTotal = store.shortLiqs.reduce((s, l) => s + l.usdt, 0);
      const longTotal  = store.longLiqs.reduce((s, l) => s + l.usdt, 0);
      // Dinamik eşik: sembol'e göre
      const cascThreshold = sym.startsWith('BTC')?2000000:sym.startsWith('ETH')?1000000:100000;
      if ((shortTotal > cascThreshold || longTotal > cascThreshold) && shortTotal !== longTotal) {
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
// TICK VERİ MOTORU — focus1691/orderflow + dkay95/CryptoFlow mantığı
// Binance @trade stream (aggTrade değil, raw trade) her tick'i yakalar
// Her fiyat seviyesinde buy/sell hacmini biriktirir = Footprint
// Stacked Imbalance, Delta Flip, Absorption tespiti yapar
// ═══════════════════════════════════════════════════════════════════════════════

const tickStore = new Map(); // symbol → tick engine state

function createTickEngine(tickSize = 0.01, volatilePct = 5) {
  const candleMs = volatilePct > 20 ? 15000 : volatilePct > 8 ? 20000 : volatilePct > 3 ? 30000 : 60000;
  return {
    // Footprint: her fiyat seviyesinde {buy, sell} hacmi
    footprint: new Map(),
    // Sweep detector — tick seviyesinde
    sweepDet: createSweepDetector(), // price_level → {buy, sell, delta}
    // 30 saniyelik mumlar
    candles: [],          // [{ts, open, high, low, close, buy, sell, delta, imbalances}]
    currentCandle: null,
    candleMs,  // Dinamik: 15-60s (volatiliteye göre)
    tickSize,
    // Gerçek zamanlı metrikler
    lastPrice: 0,
    totalBuy: 0,
    totalSell: 0,
    bigTrades: [],        // $10K+ işlemler
    ws: null,
  };
}

// Fiyatı tick size'a yuvarla (footprint seviyeleri için)
function roundToTick(price, tickSize) {
  return Math.round(price / tickSize) * tickSize;
}

// Stacked imbalance tespiti — focus1691/orderflow mantığı
// Birbirini izleyen fiyat seviyelerinde aynı yönde güçlü imbalance = güçlü sinyal
function detectStackedImbalance(footprint, tickSize, threshold = 200, stackCount = 3) {
  if (footprint.size < stackCount) return { bull:false, bear:false };

  const levels = Array.from(footprint.entries())
    .sort(([a],[b]) => parseFloat(a)-parseFloat(b));

  let bullStack = 0, bearStack = 0, maxBull = 0, maxBear = 0;

  for (const [price, {buy, sell}] of levels) {
    if (sell === 0 && buy > 0) { bullStack++; bearStack=0; }
    else if (buy === 0 && sell > 0) { bearStack++; bullStack=0; }
    else if (buy > 0 && sell > 0) {
      const ratio = buy/sell*100;
      if (ratio > threshold) { bullStack++; bearStack=0; }
      else if (100/ratio > threshold) { bearStack++; bullStack=0; }
      else { bullStack=0; bearStack=0; }
    }
    maxBull = Math.max(maxBull, bullStack);
    maxBear = Math.max(maxBear, bearStack);
  }

  return {
    bull: maxBull >= stackCount,
    bear: maxBear >= stackCount,
    bullStrength: maxBull,
    bearStrength: maxBear,
  };
}

// Absorption tespiti — fiyat hareket ediyor ama karşı taraf absorbe ediyor
function detectAbsorption(footprint, currentPrice, tickSize, isLong) {
  const nearLevels = Array.from(footprint.entries())
    .filter(([p]) => Math.abs(parseFloat(p)-currentPrice)/currentPrice < 0.002)
    .map(([,v]) => v);

  if (!nearLevels.length) return { absorbed:false };

  const totalBuy = nearLevels.reduce((s,l)=>s+l.buy,0);
  const totalSell = nearLevels.reduce((s,l)=>s+l.sell,0);

  // Long giderken çok fazla satış absorbe ediliyor = duvar var
  if (isLong && totalSell > totalBuy * 3) {
    return { absorbed:true, side:'SELL_WALL',
      msg:`Satış duvarı: $${(totalSell/1000).toFixed(0)}K absorbe edildi` };
  }
  if (!isLong && totalBuy > totalSell * 3) {
    return { absorbed:true, side:'BUY_WALL',
      msg:`Alım duvarı: $${(totalBuy/1000).toFixed(0)}K absorbe edildi` };
  }
  return { absorbed:false };
}

// Tick işleme — her trade gelince çağrılır
function processTick(engine, trade) {
  const price  = parseFloat(trade.p);
  const qty    = parseFloat(trade.q);
  const usdt   = price * qty;
  const isBuy  = !trade.m; // maker=true → taker SATTI (bear), maker=false → taker ALDI (bull)
  const ts     = trade.T || Date.now();

  engine.lastPrice = price;

  // Footprint güncelle — tickSize'a yuvarla
  const level = roundToTick(price, engine.tickSize).toFixed(8);
  if (!engine.footprint.has(level)) {
    engine.footprint.set(level, { buy:0, sell:0, delta:0 });
  }
  const fp = engine.footprint.get(level);
  if (isBuy) { fp.buy += usdt; fp.delta += usdt; engine.totalBuy += usdt; }
  else        { fp.sell += usdt; fp.delta -= usdt; engine.totalSell += usdt; }

  // 30s mum oluştur
  if (!engine.currentCandle) {
    engine.currentCandle = {
      ts, open:price, high:price, low:price, close:price,
      buy:0, sell:0, delta:0, trades:0
    };
  }
  const c = engine.currentCandle;
  c.high  = Math.max(c.high, price);
  c.low   = Math.min(c.low,  price);
  c.close = price;
  c.trades++;
  if (isBuy) { c.buy += usdt; c.delta += usdt; }
  else        { c.sell += usdt; c.delta -= usdt; }

  // Mum kapat
  if (ts - c.ts >= engine.candleMs) {
    // İmbalance tespiti ekle
    c.imbalance = detectStackedImbalance(engine.footprint, engine.tickSize);
    engine.candles.push(c);
    if (engine.candles.length > 100) engine.candles.shift();
    // Footprint'i temizle (yeni mum için)
    engine.footprint.clear();
    engine.currentCandle = null;
  }

  // Tick seviyesinde sweep güncelle
  updateSweepDetector(engine.sweepDet, price, isBuy, usdt);

  // Büyük işlem tespiti — eşiği $50K'ya çıkar (kaba sinyal önle)
  if (usdt > 50000) {
    engine.bigTrades.push({ ts, price, usdt:+usdt.toFixed(0), side:isBuy?'BUY':'SELL' });
    if (engine.bigTrades.length > 50) engine.bigTrades.shift();
  }
}

// ── TİCK SEVİYESİNDE SWEEP TESPİTİ ──────────────────────────────────────────
// Mum kapanışı beklemeden, her tick'te sweep kontrolü yapar
// Swing high/low'u sürekli günceller, ihlal anında teyit arar

function createSweepDetector() {
  return {
    // Swing seviyeleri 5m MUM FİTİLLERİNDEN hesaplanır (tick'ten değil)
    // Mum fitilleri çok daha temiz swing high/low verir
    swingHighs: [], // 5m mumlardan tespit edilen swing high'lar
    swingLows:  [], // 5m mumlardan tespit edilen swing low'lar
    lastTicks:  [], // Son 200 tick (sadece sweep teyidi için)
    pendingSweep: null,
    confirmed: null,
    tickCount: 0,
  };
}

// Swing seviyeleri 5m mumlardan güncelle — volatiliteye göre dinamik lookback
// GUA gibi çok volatil coinlerde daha fazla mum gerekir
function calcDynamicLookback(klines5m) {
  if (!klines5m || klines5m.length < 10) return 30;
  // Son 20 mumun range'ini hesapla
  const recent = klines5m.slice(-20);
  const highs = recent.map(k => parseFloat(k[2]));
  const lows  = recent.map(k => parseFloat(k[3]));
  const avgPrice = (Math.max(...highs) + Math.min(...lows)) / 2;
  const range = (Math.max(...highs) - Math.min(...lows)) / avgPrice * 100;

  // Volatilite → lookback:
  // range > 15%  → 50 mum (çok volatil, GUA gibi)
  // range 8-15%  → 40 mum (volatil)
  // range 3-8%   → 30 mum (normal)
  // range < 3%   → 20 mum (düşük volatilite)
  if (range > 15) return 50;
  if (range > 8)  return 40;
  if (range > 3)  return 30;
  return 20;
}

function updateSwingLevels(det, klines5m) {
  const lookback = calcDynamicLookback(klines5m);
  if (!klines5m || klines5m.length < lookback+4) return;
  const recent = klines5m.slice(-(lookback+4));

  // Swing gücü: her iki yandan kaç mum bakacağız?
  // Volatil coinlerde 3 mum, düşük volatilitede 2 mum
  const wing = lookback >= 40 ? 3 : 2;
  const newHighs = [], newLows = [];

  for (let i=wing; i<recent.length-wing; i++) {
    const h = parseFloat(recent[i][2]);
    const l = parseFloat(recent[i][3]);

    // Swing High: her iki yandan wing kadar mumdan yüksek olmalı
    let isSwH = true, isSwL = true;
    for (let w=1; w<=wing; w++) {
      if (h <= parseFloat(recent[i-w][2]) || h <= parseFloat(recent[i+w][2])) isSwH = false;
      if (l >= parseFloat(recent[i-w][3]) || l >= parseFloat(recent[i+w][3])) isSwL = false;
    }
    if (isSwH) newHighs.push({ price:h, idx:i });
    if (isSwL) newLows.push({ price:l, idx:i });
  }

  // En güçlü ve en yakın swing seviyeleri sakla
  det.swingHighs = newHighs.slice(-7).map(s => s.price);
  det.swingLows  = newLows.slice(-7).map(s => s.price);
  det.lookback   = lookback; // debug için
}

function updateSweepDetector(det, price, isBuy, usdt) {
  det.tickCount++;
  det.lastTicks.push({ price, isBuy, usdt, ts: Date.now() });
  if (det.lastTicks.length > 200) det.lastTicks.shift();
  // NOT: swing seviyeleri artık tick'ten hesaplanmıyor
  // updateSwingLevels() ile 5m mumlardan besleniyor

  // SWEEP KONTROLÜ — her tick'te
  const nearSwingHigh = det.swingHighs.find(h => price > h * 1.0005); // Üstüne çıktı
  const nearSwingLow  = det.swingLows.find(l => price < l * 0.9995);  // Altına geçti

  // Pending sweep varsa teyit kontrol et
  if (det.pendingSweep) {
    const age = Date.now() - det.pendingSweep.ts;
    if (age > 45000) { det.pendingSweep = null; return; } // Max 45sn teyit penceresi

    const ps = det.pendingSweep;
    // BULL sweep teyidi: fiyat sweep low'un üstüne döndü VE alım baskısı var
    if (ps.type === 'BULL' && price > ps.level && isBuy) {
      const recentBuys  = det.lastTicks.slice(-10).filter(t => t.isBuy).length;
      const recentSells = det.lastTicks.slice(-10).filter(t => !t.isBuy).length;
      if (recentBuys > recentSells * 1.5) { // Alımlar baskın
        det.confirmed = {
          type: 'BULL_SWEEP', level: ps.level, sweepPrice: ps.sweepPrice,
          confirmPrice: price, ts: Date.now(),
          msg: `Tick BULL sweep: $${ps.level.toFixed(4)} altı → geri döndü, alım baskısı`,
          fresh: true,
        };
        det.pendingSweep = null;
      }
    }
    // BEAR sweep teyidi: fiyat sweep high'ın altına döndü VE satış baskısı var
    if (ps.type === 'BEAR' && price < ps.level && !isBuy) {
      const recentBuys  = det.lastTicks.slice(-10).filter(t => t.isBuy).length;
      const recentSells = det.lastTicks.slice(-10).filter(t => !t.isBuy).length;
      if (recentSells > recentBuys * 1.5) {
        det.confirmed = {
          type: 'BEAR_SWEEP', level: ps.level, sweepPrice: ps.sweepPrice,
          confirmPrice: price, ts: Date.now(),
          msg: `Tick BEAR sweep: $${ps.level.toFixed(4)} üstü → geri döndü, satış baskısı`,
          fresh: true,
        };
        det.pendingSweep = null;
      }
    }
  }

  // Yeni sweep başladı mı?
  if (!det.pendingSweep) {
    if (nearSwingLow && !isBuy) { // Alt swing'e geçti, satış var
      det.pendingSweep = { type:'BULL', level:nearSwingLow, sweepPrice:price, ts:Date.now() };
    }
    if (nearSwingHigh && isBuy) { // Üst swing'e geçti, alım var
      det.pendingSweep = { type:'BEAR', level:nearSwingHigh, sweepPrice:price, ts:Date.now() };
    }
  }

  // Confirmed sweep 5 dakika sonra sıfırla
  if (det.confirmed && Date.now() - det.confirmed.ts > 5 * 60 * 1000) {
    det.confirmed = null;
  }
}

// Tick WS başlat — @trade stream (aggTrade'den daha granüler)
async function startTickStream(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  if (tickStore.has(full) && tickStore.get(full).ws?.readyState === WebSocket.OPEN) return;

  // 24s volatilite — mum süresini ve swing lookback'i belirler
  let vol24h = 5;
  try {
    const ticker = await bPub('/fapi/v1/ticker/24hr', 'symbol='+full);
    vol24h = Math.abs(parseFloat(ticker.priceChangePercent)||5);
  } catch(e) {}
  const tickSz = full.startsWith('BTC')?0.1:full.startsWith('ETH')?0.01:
                 full.startsWith('BNB')?0.01:0.0001;
  const engine = createTickEngine(tickSz, vol24h);
  console.log(`${full} tick engine: candleMs=${engine.candleMs}ms vol24h=${vol24h.toFixed(1)}%`);
  tickStore.set(full, engine);

  // @trade yerine @aggTrade kullan (Binance Futures'ta @trade yok)
  const wsUrl = `wss://fstream.binance.com/ws/${full.toLowerCase()}@aggTrade`;
  const ws = new WebSocket(wsUrl);
  engine.ws = ws;

  ws.on('message', data => {
    try { processTick(engine, JSON.parse(data.toString())); } catch(e) {}
  });
  ws.on('close', () => { setTimeout(()=>startTickStream(full), 3000); });
  ws.on('error', () => {});
}

// ── VPIN (Volume Synchronized Probability of Informed Trading) ────────────────
// Kaynak: Easley, de Prado, O'Hara (2011) → SGTYang/VPIN, jheusser/vpin
// Market maker'ların bilgilendirilmiş alım satım olasılığını ölçer
// VPIN yüksek → informed trader var → büyük hareket yakın
// Binance flash crash öncesi VPIN spike olur

function calcVPIN(trades, bucketSize = 50) {
  if (!trades || trades.length < bucketSize * 3) return null;

  // Volume bucket'lara böl (zaman değil hacim bazlı)
  const buckets = [];
  let curBuy = 0, curSell = 0, curVol = 0;

  for (const t of trades) {
    const vol = t.usdt;
    if (t.isBuy) curBuy += vol;
    else          curSell += vol;
    curVol += vol;

    if (curVol >= bucketSize * 1000) { // bucketSize × $1K
      buckets.push({ buy: curBuy, sell: curSell, total: curVol,
        imbalance: Math.abs(curBuy - curSell) / curVol });
      curBuy = 0; curSell = 0; curVol = 0;
    }
  }

  if (buckets.length < 5) return null;

  // Son 10 bucket'ta ortalama imbalance = VPIN
  const recent = buckets.slice(-10);
  const vpin = recent.reduce((s, b) => s + b.imbalance, 0) / recent.length;
  const vpinPct = +(vpin * 100).toFixed(1);

  // VPIN yorumu:
  // > 40%: Çok yüksek toxicity → büyük hareket yakın (yön belirsiz)
  // > 25%: Yüksek → informed trader aktif
  // < 15%: Düşük → normal market making
  const toxicity = vpinPct > 40 ? 'EXTREME' : vpinPct > 25 ? 'HIGH' :
                   vpinPct > 15 ? 'MEDIUM' : 'LOW';

  // Yön tespiti: son bucket'larda hangi yön dominant?
  const lastBuySell = recent.slice(-3).reduce((s,b) => ({
    buy:s.buy+b.buy, sell:s.sell+b.sell }), {buy:0,sell:0});
  const direction = lastBuySell.buy > lastBuySell.sell * 1.3 ? 'BUY_DOMINANT' :
                    lastBuySell.sell > lastBuySell.buy * 1.3 ? 'SELL_DOMINANT' : 'NEUTRAL';

  return { vpin: vpinPct, toxicity, direction, bucketCount: buckets.length };
}

// ── DELTA MICROSTRUCTURE — Exhaustion + Absorption + Trapped Trader ────────
// Kaynak: TradingView "Delta Microstructure Analysis" open source indicator
function calcDeltaMicrostructure(candles) {
  if (!candles || candles.length < 5) return null;

  const recent = candles.slice(-10);
  const deltas = recent.map(c => c.delta);
  const prices = recent.map(c => c.close);

  // Exhaustion tespiti: delta ortalamanın X katına çıktı = yorgunluk
  const avgAbsDelta = deltas.reduce((s,d) => s + Math.abs(d), 0) / deltas.length;
  const lastDelta = deltas[deltas.length-1];
  const exhaustion = Math.abs(lastDelta) > avgAbsDelta * 2.5;

  // Delta divergence: fiyat yeni high ama delta düşük = dağıtım
  const priceHigher = prices[prices.length-1] > prices[0];
  const deltaLower  = lastDelta < deltas[0];
  const bullDivergence = !priceHigher && !deltaLower; // Fiyat düşük ama delta yükseliyor
  const bearDivergence = priceHigher && deltaLower;   // Fiyat yüksek ama delta düşüyor

  // Absorption: büyük hacim ama fiyat hareket etmiyor
  const lastCandle = recent[recent.length-1];
  const priceRange = lastCandle ? Math.abs(lastCandle.high - lastCandle.low) : 0;
  const totalVol = lastCandle ? (lastCandle.buy + lastCandle.sell) : 0;
  const avgVol = recent.reduce((s,c) => s+(c.buy+c.sell), 0) / recent.length;
  const absorption = totalVol > avgVol * 2 && priceRange < (prices[0] * 0.001);

  // Trapped trader: güçlü delta ama fiyat geri geldi
  const deltaStrong = Math.abs(lastDelta) > avgAbsDelta * 1.5;
  const priceReversed = prices.length >= 3 &&
    ((lastDelta > 0 && prices[prices.length-1] < prices[prices.length-2]) ||
     (lastDelta < 0 && prices[prices.length-1] > prices[prices.length-2]));
  const trapped = deltaStrong && priceReversed;

  return {
    exhaustion, exhaustionDir: lastDelta > 0 ? 'BULL_EXHAUST' : 'BEAR_EXHAUST',
    bearDivergence, bullDivergence,
    absorption, trapped,
    avgAbsDelta: +avgAbsDelta.toFixed(0),
    lastDelta: +lastDelta.toFixed(0),
  };
}

// Tick analiz sonucu al
function getTickAnalysis(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  startTickStream(full);
  const engine = tickStore.get(full);
  if (!engine) return null;

  const totalVol = engine.totalBuy + engine.totalSell;
  const deltaRatio = totalVol > 0 ? engine.totalBuy/totalVol*100 : 50;

  // Son 5 mum delta trendi
  const recentCandles = engine.candles.slice(-5);
  const deltaTrend = recentCandles.length >= 3
    ? recentCandles.map(c=>c.delta).reduce((a,b)=>a+b,0) > 0 ? 'BULL' : 'BEAR'
    : 'UNKNOWN';

  // Stacked imbalance
  const imbalance = detectStackedImbalance(engine.footprint, engine.tickSize);

  // Son büyük işlemler (son 2 dk)
  const recent2m = engine.bigTrades.filter(t=>Date.now()-t.ts<2*60*1000);
  const bigBuy  = recent2m.filter(t=>t.side==='BUY').reduce((s,t)=>s+t.usdt,0);
  const bigSell = recent2m.filter(t=>t.side==='SELL').reduce((s,t)=>s+t.usdt,0);
  // Whale bias — sadece büyük işlemler ($50K+ zaten filtrelendi)
  const whaleBias = bigBuy > bigSell * 1.5 ? 'WHALE_BUY' :
                    bigSell > bigBuy * 1.5 ? 'WHALE_SELL' : 'NEUTRAL';

  // Delta momentum — son 3 mum
  let deltaFlip = 'NONE';
  if (recentCandles.length >= 3) {
    const last = recentCandles[recentCandles.length-1];
    const prev = recentCandles[recentCandles.length-2];
    if (prev.delta > 0 && last.delta < -Math.abs(prev.delta)*0.5) deltaFlip='BULL_TO_BEAR';
    if (prev.delta < 0 && last.delta > Math.abs(prev.delta)*0.5)  deltaFlip='BEAR_TO_BULL';
  }

  // Tick sweep
  const tickSweep = engine.sweepDet.confirmed;

  // VPIN hesapla — son 200 tick'ten
  const vpinResult = calcVPIN(engine.bigTrades.length > 0
    ? engine.lastTicks.slice(-500).map(t => ({...t, usdt:t.usdt||0}))
    : [], 30);

  // Delta microstructure
  const microstructure = calcDeltaMicrostructure(engine.candles);

  return {
    deltaRatio: +deltaRatio.toFixed(1),
    deltaTrend,
    deltaFlip,
    imbalance,
    whaleBias,
    bigBuy:  +bigBuy.toFixed(0),
    bigSell: +bigSell.toFixed(0),
    tickSweep: tickSweep || null,
    recentCandles: recentCandles.slice(-3).map(c=>({
      ts:c.ts, buy:+c.buy.toFixed(0), sell:+c.sell.toFixed(0),
      delta:+c.delta.toFixed(0), high:c.high, low:c.low, trades:c.trades
    })),
    candleCount: engine.candles.length,
    vpin: vpinResult,
    microstructure,
  };
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

// ── VOLATİLİTE SCANNER — En çok hareket eden coinler ─────────────────────────
// CyberPunkMetalHead/Binance-volatility-trading-bot mantığı
// 5m ve 15m'de en fazla hareket eden, hacimli, aktif coinleri seç
const volatilityStore = { coins:[], lastUpdate:0 };

async function scanVolatility() {
  try {
    const tickers = await bPub('/fapi/v1/ticker/24hr');
    if (!Array.isArray(tickers)) return;

    const now = Date.now();
    const scored = tickers
      .filter(t => t.symbol.endsWith('USDT') &&
        parseFloat(t.quoteVolume) > 20000000 && // min 20M hacim
        parseFloat(t.lastPrice) > 0)
      .map(t => {
        const vol   = parseFloat(t.quoteVolume);
        const chg   = Math.abs(parseFloat(t.priceChangePercent));
        const count = parseInt(t.count) || 0;
        const high  = parseFloat(t.highPrice);
        const low   = parseFloat(t.lowPrice);
        const last  = parseFloat(t.lastPrice);

        // Volatilite skoru: hareket + hacim + işlem yoğunluğu
        const volScore =
          (chg > 10 ? 40 : chg > 5 ? 30 : chg > 3 ? 20 : chg > 1 ? 10 : 3) +
          (vol > 1e9 ? 30 : vol > 5e8 ? 20 : vol > 1e8 ? 12 : vol > 5e7 ? 6 : 3) +
          (count > 500000 ? 20 : count > 200000 ? 12 : count > 100000 ? 6 : 2) +
          // Range genişliği: günlük range / fiyat
          ((high - low) / last * 100 > 8 ? 10 : (high - low) / last * 100 > 4 ? 5 : 0);

        return {
          symbol: t.symbol.replace('USDT',''),
          fullSymbol: t.symbol,
          price: last,
          change24h: parseFloat(t.priceChangePercent),
          volume: vol,
          trades: count,
          rangePct: +((high - low) / last * 100).toFixed(2),
          volScore: Math.round(volScore),
        };
      })
      .sort((a, b) => b.volScore - a.volScore)
      .slice(0, 30); // Top 30 volatil coin

    volatilityStore.coins = scored;
    volatilityStore.lastUpdate = now;
  } catch(e) {
    console.log('Volatilite scanner hata:', e.message);
  }
}

// Her 3 dakikada bir güncelle
setInterval(scanVolatility, 3 * 60 * 1000);
scanVolatility(); // Hemen başlat

app.get('/api/volatile-coins', (req, res) => {
  res.json({ ok:true, coins: volatilityStore.coins,
    lastUpdate: volatilityStore.lastUpdate });
});

// ── KILL ZONE TESPİTİ ─────────────────────────────────────────────────────────
// ICT Kill Zones — en yüksek olasılıklı işlem pencereleri
function getKillZone() {
  // UTC saati kullan (Binance UTC'de çalışır)
  const now = new Date();
  const utcH = now.getUTCHours();
  const utcM = now.getUTCMinutes();
  const utcTime = utcH * 60 + utcM; // dakika cinsinden

  // Asia Session: 00:00-03:00 UTC
  if (utcTime >= 0   && utcTime < 180)  return { zone:'ASIA',    strength:0.6, label:'🌏 Asya', active:true };
  // London Open Kill Zone: 07:00-10:00 UTC (en güçlü)
  if (utcTime >= 420 && utcTime < 600)  return { zone:'LONDON',  strength:1.0, label:'🇬🇧 London Open', active:true };
  // London Close: 11:00-13:00 UTC
  if (utcTime >= 660 && utcTime < 780)  return { zone:'LONDON_CLOSE', strength:0.7, label:'🇬🇧 London Kapat', active:true };
  // NY Open Kill Zone: 13:00-16:00 UTC (çok güçlü)
  if (utcTime >= 780 && utcTime < 960)  return { zone:'NY_OPEN', strength:1.0, label:'🇺🇸 NY Open', active:true };
  // NY PM: 17:00-20:00 UTC
  if (utcTime >= 1020&& utcTime < 1200) return { zone:'NY_PM',   strength:0.7, label:'🇺🇸 NY PM', active:true };
  // Ölü saatler
  return { zone:'DEAD', strength:0.3, label:'💤 Ölü Saat', active:false };
}

app.get('/api/killzone', (req, res) => {
  res.json({ ok:true, ...getKillZone(), time: new Date().toUTCString() });
});

// ── COIN LİSTESİ ──────────────────────────────────────────────────────────────
app.get('/api/futures-coins', async (req, res) => {
  try {
    const data = await cached('futures_tickers', 15*60*1000, () => bPub('/fapi/v1/ticker/24hr'));
    if (!Array.isArray(data)) return res.json({ coins:[] });
    const EXCL = new Set(['BTCUSDT','ETHUSDT','BNBUSDT','XRPUSDT','SOLUSDT','ADAUSDT',
      'DOGEUSDT','DOTUSDT','MATICUSDT','LTCUSDT','TRXUSDT','AVAXUSDT','LINKUSDT','UNIUSDT','WBTCUSDT','SHIBUSDT']);
    const coins = data.filter(t=>t.symbol.endsWith('USDT')&&!EXCL.has(t.symbol))
      .map(t=>({ symbol:t.symbol.replace('USDT',''), fullSymbol:t.symbol,
        price:parseFloat(t.lastPrice)||0, change24h:parseFloat(t.priceChangePercent)||0,
        volume:parseFloat(t.quoteVolume)||0, high:parseFloat(t.highPrice)||0,
        low:parseFloat(t.lowPrice)||0, trades:parseInt(t.count)||0 }))
      .filter(c=>c.volume>20000000);
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
    // tickStream analyze'da await ile çağrılıyor

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

    const lastPrice = k15m.length?parseFloat(k15m[k15m.length-1][4]):0;
    const lastTime  = k15m.length?parseInt(k15m[k15m.length-1][6]):Date.now();

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
    const atr4h=atr(k4h),atr1h=atr(k1h),atr15m_=atr(k15m);
    const vwap1h=vwap(k1h),vwap4h=vwap(k4h);
    const bb1h=bollinger(k1h),bb15m_=bollinger(k15m);

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

    // ── AMD 5M + TİCK SCALP MODELİ ─────────────────────────────────────────────
    // Tick sweep ile AMD'yi birleştir — mum kapanışı bekleme
    // Tick sweep = AMD manipulation teyidi
    function detectAMD(klines5m, klines15m, tickSweepData) {
      if (klines5m.length < 20 || klines15m.length < 10)
        return { phase:'UNKNOWN', signal:'NONE', entry:null };

      const k5  = klines5m.slice(-20);
      const k15 = klines15m.slice(-10);

      // 15m'de genel yön (bias)
      const bias15m = parseFloat(k15[k15.length-1][4]) >
                      parseFloat(k15[0][4]) ? 'BULLISH' : 'BEARISH';

      // 5m'de accumulation range tespit (son 8 mumda dar range)
      const acc = k5.slice(0, 8);
      const accHighs = acc.map(k=>parseFloat(k[2]));
      const accLows  = acc.map(k=>parseFloat(k[3]));
      const accHigh  = Math.max(...accHighs);
      const accLow   = Math.min(...accLows);
      const accRange = (accHigh - accLow) / lastPrice * 100;

      // Dar range = accumulation (< %1.5)
      const isAccumulating = accRange < 1.5;

      if (!isAccumulating) return { phase:'NO_RANGE', signal:'NONE', bias:bias15m };

      // Son 5 mumda manipulation + MSS tespiti
      const recent = k5.slice(-6);
      let manipSweep = null, mssCandle = null;

      for (let i = 0; i < recent.length - 1; i++) {
        const c = recent[i];
        const h = parseFloat(c[2]), l = parseFloat(c[3]);
        const o = parseFloat(c[1]), cl = parseFloat(c[4]);

        // BULLISH AMD: fiyat accLow altına spike → geri döndü
        if (bias15m === 'BULLISH' && l < accLow * 0.999 && cl > accLow) {
          manipSweep = { type:'BULL_MANIP', sweepLevel: accLow, price: l, idx: i };
          // Sonraki mum MSS teyidi
          const next = recent[i+1];
          if (next && parseFloat(next[4]) > parseFloat(next[1])) {
            mssCandle = { type:'BULL_MSS', price: parseFloat(next[4]) };
          }
        }

        // BEARISH AMD: fiyat accHigh üstüne spike → geri döndü
        if (bias15m === 'BEARISH' && h > accHigh * 1.001 && cl < accHigh) {
          manipSweep = { type:'BEAR_MANIP', sweepLevel: accHigh, price: h, idx: i };
          const next = recent[i+1];
          if (next && parseFloat(next[4]) < parseFloat(next[1])) {
            mssCandle = { type:'BEAR_MSS', price: parseFloat(next[4]) };
          }
        }
      }

      if (!manipSweep) return { phase:'ACCUMULATION', signal:'NONE', bias:bias15m, accHigh, accLow, accRange:+accRange.toFixed(2) };
      if (!mssCandle) return { phase:'MANIPULATION', signal:'WAIT_MSS', bias:bias15m, manipSweep, accHigh, accLow };

      // FVG tespiti (Fair Value Gap) — giriş bölgesi
      const isBull = manipSweep.type === 'BULL_MANIP';
      let fvg = null;
      for (let i = 1; i < recent.length - 1; i++) {
        const prev = recent[i-1], curr = recent[i], next2 = recent[i+1];
        if (!prev || !next2) continue;
        // Bullish FVG: prev.high < next.low
        if (isBull && parseFloat(prev[2]) < parseFloat(next2[3])) {
          fvg = { high: parseFloat(next2[3]), low: parseFloat(prev[2]),
            mid: (parseFloat(next2[3]) + parseFloat(prev[2])) / 2 };
        }
        // Bearish FVG: prev.low > next.high
        if (!isBull && parseFloat(prev[3]) > parseFloat(next2[2])) {
          fvg = { high: parseFloat(prev[3]), low: parseFloat(next2[2]),
            mid: (parseFloat(prev[3]) + parseFloat(next2[2])) / 2 };
        }
      }

      // Giriş bölgesi: FVG varsa FVG mid, yoksa MSS mumu
      const entryZone = fvg ? fvg.mid : mssCandle.price;
      const signal = isBull ? 'AMD_LONG' : 'AMD_SHORT';

      // Tick sweep ile teyit kontrolü
      const tickConfirm = tickSweepData &&
        ((isBull && tickSweepData.type==='BULL_SWEEP') ||
         (!isBull && tickSweepData.type==='BEAR_SWEEP'));

      return {
        phase: 'DISTRIBUTION', signal, bias: bias15m,
        manipSweep, mssCandle, fvg, entryZone,
        accHigh, accLow, accRange: +accRange.toFixed(2),
        tickConfirm, // Tick sweep de aynı yönde onayladı mı?
        msg: isBull
          ? `AMD Long: Range[${accLow.toFixed(4)}-${accHigh.toFixed(4)}] → Sweep Alt → MSS ↑${tickConfirm?' ⚡Tick✅':''}`
          : `AMD Short: Range[${accLow.toFixed(4)}-${accHigh.toFixed(4)}] → Sweep Üst → MSS ↓${tickConfirm?' ⚡Tick✅':''}`
      };
    }

    // Tick sweep detector'ına 5m swing seviyeleri besle
    await startTickStream(full); // await: stream kurulduktan sonra devam et
    const tickEng = tickStore.get(full);
    if (tickEng && k5m.length > 0) {
      updateSwingLevels(tickEng.sweepDet, k5m); // Dinamik lookback, volatiliteye göre
    }
    const tickData = getTickAnalysis(full); // Tek referans, hem AMD hem skor için
    const amd5m = detectAMD(k5m, k15m, tickData?.tickSweep);

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
    const proTPSL=calcProTPSL(
      mmTarget==='GENUINE_DOWN'||mmTarget==='DOWN_SWEEP'?'SHORT':'LONG',
      lastPrice,atr1h,atr4h,liq1h,liq4h,ob1h,ob4h,k1h
    );

    // ── KALDIRAÇ ─────────────────────────────────────────────────────────────
    const t4up=ema20_4h>ema50_4h&&ema50_4h>ema200_4h;
    const t4dn=ema20_4h<ema50_4h&&ema50_4h<ema200_4h;
    const atrPct=lastPrice>0?(atr1h/lastPrice)*100:1;
    let longScore=0,shortScore=0;const signals={long:[],short:[]};

    // ── SKORLAMA (öncelik sırasıyla) ──────────────────────────────────────────
    // 0. AMD 5M SCALP SİNYALİ — en hızlı ve net giriş noktası
    if (amd5m.signal === 'AMD_LONG') {
      const bonus = amd5m.tickConfirm ? 10 : 0; // Tick teyitli AMD daha güçlü
      longScore += 30 + bonus;
      signals.long.push(`⚡ AMD Long${amd5m.tickConfirm?' ⚡Tick':''}: ${amd5m.msg}`);
    }
    if (amd5m.signal === 'AMD_SHORT') {
      const bonus = amd5m.tickConfirm ? 10 : 0;
      shortScore += 30 + bonus;
      signals.short.push(`⚡ AMD Short${amd5m.tickConfirm?' ⚡Tick':''}: ${amd5m.msg}`);
    }
    // Tick sweep tek başına da skor verir
    if (tickData?.tickSweep?.type==='BULL_SWEEP'&&tickData.tickSweep.fresh) {
      longScore += 25; signals.long.push(`⚡ Tick Sweep Bull: ${tickData.tickSweep.msg}`);
    }
    if (tickData?.tickSweep?.type==='BEAR_SWEEP'&&tickData.tickSweep.fresh) {
      shortScore += 25; signals.short.push(`⚡ Tick Sweep Bear: ${tickData.tickSweep.msg}`);
    }
    if (amd5m.signal === 'WAIT_MSS') {
      // MSS bekleniyor — skor verme ama bilgi ver
    }

    // Kill Zone ağırlığı — ölü saatte skor %40 azalt
    const kz = getKillZone();
    if (kz.zone === 'DEAD') {
      longScore  = Math.round(longScore  * 0.6);
      shortScore = Math.round(shortScore * 0.6);
    } else if (kz.strength < 1.0) {
      longScore  = Math.round(longScore  * 0.85);
      shortScore = Math.round(shortScore * 0.85);
    }

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
    if(mmTarget==='UP_SWEEP')      {longScore+=12;signals.long.push('🪤 MM Üst Liq');}
    if(mmTarget==='DOWN_SWEEP')    {shortScore+=12;signals.short.push('🪤 MM Alt Liq');}
    // 2. STOP HUNT
    if(hunt1h.hunted&&hunt1h.direction==='BULL_HUNT'){longScore+=14;signals.long.push('🎣 Stop Hunt ↑');}
    if(hunt1h.hunted&&hunt1h.direction==='BEAR_HUNT'){shortScore+=14;signals.short.push('🎣 Stop Hunt ↓');}
    if(hunt15m.hunted&&hunt15m.direction==='BULL_HUNT'){longScore+=7;}
    if(hunt15m.hunted&&hunt15m.direction==='BEAR_HUNT'){shortScore+=7;}
    // 3. TICK VERİ — Footprint, Stacked Imbalance, Whale, Delta Flip
    if(tickData && typeof tickData === 'object') {
      // Stacked imbalance — en güçlü tick sinyali
      if(tickData.imbalance?.bull&&tickData.imbalance.bullStrength>=4)
        {longScore+=18;signals.long.push(`📊 Stacked Imbalance Bull (${tickData.imbalance.bullStrength})`);}
      if(tickData.imbalance?.bear&&tickData.imbalance.bearStrength>=4)
        {shortScore+=18;signals.short.push(`📊 Stacked Imbalance Bear (${tickData.imbalance.bearStrength})`);}
      // Whale bias (son 2dk büyük işlemler)
      if(tickData.whaleBias==='WHALE_BUY')
        {longScore+=15;signals.long.push(`🐋 Whale Buy $${(tickData.bigBuy/1000).toFixed(0)}K`);}
      if(tickData.whaleBias==='WHALE_SELL')
        {shortScore+=15;signals.short.push(`🐋 Whale Sell $${(tickData.bigSell/1000).toFixed(0)}K`);}
      // Delta flip — momentum değişimi (acil çıkış/giriş sinyali)
      if(tickData.deltaFlip==='BEAR_TO_BULL'){longScore+=12;signals.long.push('⚡ Delta Flip ↑');}
      if(tickData.deltaFlip==='BULL_TO_BEAR'){shortScore+=12;signals.short.push('⚡ Delta Flip ↓');}
      // Genel delta trend
      if(tickData.deltaTrend==='BULL'){longScore+=6;}
      if(tickData.deltaTrend==='BEAR'){shortScore+=6;}
    }

    // VPIN — yüksek toxicity = büyük hareket yakın
    if(tickData?.vpin) {
      const vp = tickData.vpin;
      if(vp.toxicity==='EXTREME') {
        // Extreme VPIN: büyük hareket yakın, yön dominant'a göre gir
        if(vp.direction==='BUY_DOMINANT') {longScore+=15;signals.long.push(`🧬 VPIN Extreme Buy ${vp.vpin}%`);}
        if(vp.direction==='SELL_DOMINANT'){shortScore+=15;signals.short.push(`🧬 VPIN Extreme Sell ${vp.vpin}%`);}
      } else if(vp.toxicity==='HIGH') {
        if(vp.direction==='BUY_DOMINANT') {longScore+=8;}
        if(vp.direction==='SELL_DOMINANT'){shortScore+=8;}
      }
    }

    // Delta Microstructure — exhaustion, absorption, trapped trader
    if(tickData?.microstructure) {
      const ms = tickData.microstructure;
      // Bear exhaustion → long fırsat (satışlar tükendi)
      if(ms.exhaustion&&ms.exhaustionDir==='BEAR_EXHAUST'){longScore+=12;signals.long.push('💥 Bear Exhaustion');}
      // Bull exhaustion → short fırsat
      if(ms.exhaustion&&ms.exhaustionDir==='BULL_EXHAUST'){shortScore+=12;signals.short.push('💥 Bull Exhaustion');}
      // Bearish divergence → short
      if(ms.bearDivergence){shortScore+=10;signals.short.push('📉 Delta Divergence Bear');}
      // Bullish divergence → long
      if(ms.bullDivergence){longScore+=10;signals.long.push('📈 Delta Divergence Bull');}
      // Absorption: büyük hacim ama fiyat hareket etmiyor → tersine dönüş beklenir
      if(ms.absorption){
        // CVD yönüne göre
        const cvdD=getCVD(full);
        if(cvdD?.ratio>55){longScore+=8;signals.long.push('🧲 Absorption+Buy');}
        if(cvdD?.ratio<45){shortScore+=8;signals.short.push('🧲 Absorption+Sell');}
      }
      // Trapped trader → hızlı çıkış beklenir, karşı yön fırsat
      if(ms.trapped){
        if(ms.lastDelta>0){shortScore+=8;signals.short.push('🪤 Bull Trapped');}
        else{longScore+=8;signals.long.push('🪤 Bear Trapped');}
      }
    }

    // WS Iceberg
    if(iceberg.signal==='STRONG_HIDDEN_BUY') {longScore+=10;signals.long.push('🧊 WS Iceberg Alıcı');}
    if(iceberg.signal==='STRONG_HIDDEN_SELL'){shortScore+=10;signals.short.push('🧊 WS Iceberg Satıcı');}
    if(iceberg.signal==='HIDDEN_BUY')        {longScore+=5;}
    if(iceberg.signal==='HIDDEN_SELL')       {shortScore+=5;}
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

    longScore =Math.min(Math.round(longScore*freshnessMult),100);
    shortScore=Math.min(Math.round(shortScore*freshnessMult),100);
    // ── İKİ KATMANLI KARAR MEKANİZMASI ─────────────────────────────────────────
      // A-Tier: Yüksek güven → otomatik işlem açılır
      // B-Tier: Orta güven  → sinyal gösterilir, elle karar ver
      const rawRec=longScore>shortScore&&longScore>=50?'LONG':
                   shortScore>longScore&&shortScore>=50?'SHORT':'WAIT';

      // ── SNIPER KARAR MOTORU — 3 kural ──────────────────────────────────────
      function evalDecision(side){
        if(side==='WAIT')return{pass:false,tier:'WAIT',score:0,reasons:[],blocks:[],autoOk:false};
        const isL=side==='LONG';
        const sw1=sweep1h,wy1=wyckoff1h;
        const cvdD=getCVD(full);
        const liqD=getLiqData(full);
        const sc=isL?longScore:shortScore;

        // KURAL 1: Giriş sinyali (bunlardan biri ZORUNLU)
        const hasEntry=
          // Kline bazlı sweep (1h mum)
          (sw1?.confirmed&&(isL?sw1.direction==='BULL_SWEEP':sw1.direction==='BEAR_SWEEP'))||
          // TİCK SEVİYESİNDE sweep — mum kapanışı beklemiyor ⚡
          (tickData?.tickSweep?.type===(isL?'BULL_SWEEP':'BEAR_SWEEP')&&tickData.tickSweep.fresh)||
          // AMD 5m (tick teyitli ise +ağırlık)
          (amd5m?.signal===(isL?'AMD_LONG':'AMD_SHORT'))||
          // Wyckoff
          (wy1?.recentEvents?.some(e=>isL?(e.type==='SPRING'||e.type==='SOS'):e.type==='UTAD'))||
          // Tick: Stacked imbalance + whale aynı yönde
          (tickData?.imbalance?.bull&&tickData.whaleBias==='WHALE_BUY'&&isL)||
          (tickData?.imbalance?.bear&&tickData.whaleBias==='WHALE_SELL'&&!isL)||
          // Delta flip + whale
          (tickData?.deltaFlip==='BEAR_TO_BULL'&&tickData.whaleBias==='WHALE_BUY'&&isL)||
          (tickData?.deltaFlip==='BULL_TO_BEAR'&&tickData.whaleBias==='WHALE_SELL'&&!isL);

        // KURAL 2: Delta (CVD) ters değil — geniş tolerans
        const cvdRatio=cvdD?.ratio||50;
        // Tick delta da CVD ile birlikte bak
        const tickDeltaOk=tickData
          ?(isL?tickData.deltaTrend==='BULL'||tickData.deltaTrend==='UNKNOWN'
               :tickData.deltaTrend==='BEAR'||tickData.deltaTrend==='UNKNOWN')
          :true;
        // Tick veya CVD'den biri yeterli
        const deltaOk=(isL?cvdRatio>40:cvdRatio<60)||tickDeltaOk;

        // KURAL 3: Funding zehirli değil
        const fundOk=isL?fundSig!=='EXTREME_POSITIVE':fundSig!=='EXTREME_NEGATIVE';

        // A-Tier: net sinyal + delta ok + skor≥68
        const isTierA=hasEntry&&deltaOk&&fundOk&&sc>=68;

        // B-Tier: yumuşak sinyal + skor≥55
        const softEntry=
          (sw1?.swept&&!sw1?.confirmed)||
          (isL?(ob1h?.bullOB&&lastPrice<=ob1h.bullOB.high*1.01):(ob1h?.bearOB&&lastPrice>=ob1h.bearOB.low*0.99))||
          mmTarget===(isL?'GENUINE_UP':'GENUINE_DOWN')||
          mmTarget===(isL?'UP_SWEEP':'DOWN_SWEEP');
        const isTierB=!isTierA&&(softEntry||hasEntry)&&fundOk&&sc>=55;

        const reasons=[], blocks=[];
        if(amd5m?.signal===(isL?'AMD_LONG':'AMD_SHORT')) reasons.push('⚡ AMD');
        if(sw1?.confirmed&&(isL?sw1.direction==='BULL_SWEEP':sw1.direction==='BEAR_SWEEP')) reasons.push('✅ Sweep+Teyit');
        if(wy1?.recentEvents?.some(e=>e.type==='SPRING')) reasons.push('🌊 Spring');
        if(wy1?.recentEvents?.some(e=>e.type==='UTAD'))   reasons.push('🚨 UTAD');
        if(deltaOk) reasons.push(`📊 CVD${cvdRatio.toFixed(0)}%`);
        if(liqD?.cascade) reasons.push(`💥 ${liqD.cascade.signal}`);
        if(softEntry&&!hasEntry) reasons.push('👁 Yumuşak sinyal');
        if(!hasEntry&&!softEntry) blocks.push('Sinyal yok');
        if(!deltaOk) blocks.push(`Delta ters(${cvdRatio.toFixed(0)}%)`);
        if(!fundOk)  blocks.push('Fund zehirli');
        if(sc<55)    blocks.push(`Skor düşük(${sc})`);

        const tier=isTierA?'A':isTierB?'B':'WAIT';
        const passCount=[hasEntry||softEntry,deltaOk,fundOk,sc>=55].filter(Boolean).length;
        return{ pass:tier!=='WAIT', tier, score:sc, passCount,
          reasons, blocks, autoOk:isTierA,
          reason: tier==='A'?`🎯 ${reasons.slice(0,3).join(' + ')}`:
                  tier==='B'?`👁 ${reasons.slice(0,2).join(' + ')} — elle bak`:
                  `❌ ${blocks.slice(0,2).join(', ')}` };
      }
      let recommendation='WAIT', decisionChain=null;
      if(rawRec!=='WAIT'){
        decisionChain=evalDecision(rawRec);
        recommendation=decisionChain.pass?rawRec:'WAIT';
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
      amd5m,
      killZone: getKillZone(),
      orderBlocks:{'1h':ob1h,'4h':ob4h},
      // WS gerçek zamanlı veriler
      cvd, iceberg, tickData,
      vpin: tickData?.vpin, microstructure: tickData?.microstructure,
      // Coinglass likidite haritası
      coinglass: cgData,
      proTPSL,
      leverage:{suggested:suggestedLev,min:3,max:50,atrPct:+atrPct.toFixed(2)},
      timeframes:{
        '4h':{rsi:rsi4h,ema20:+ema20_4h.toFixed(4),ema50:+ema50_4h.toFixed(4),ema200:+ema200_4h.toFixed(4),trend:t4up?'UP':t4dn?'DOWN':'RANGE',atr:+atr4h.toFixed(4)},
        '1h':{rsi:rsi1h,ema20:+ema20_1h.toFixed(4),ema50:+ema50_1h.toFixed(4),ema200:+ema200_1h.toFixed(4),trend:ema20_1h>ema50_1h?'UP':'DOWN',vwap:+vwap1h.toFixed(4),atr:+atr1h.toFixed(4),bb:bb1h},
        '15m':{rsi:rsi15m,atr:+atr15m_.toFixed(4),bb:bb15m_},
        '5m':{rsi:rsi5m},
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

    // Mevcut açık emirleri iptal et
    if (cancelExisting) {
      try { await bReq(apiKey,apiSecret,'DELETE','/fapi/v1/allOpenOrders',{symbol:sym}); }
      catch(e) {}
    }

    // Yeni SL yerleştir
    const sl = await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',{
      symbol:sym, side:cSide, type:'STOP_MARKET',
      stopPrice:parseFloat(newSL).toString(),
      quantity:qty, reduceOnly:'true', positionSide:'BOTH',
      workingType:'MARK_PRICE'
    });

    res.json({ ok:true, message:`${sym} SL güncellendi → $${newSL}`, orderId:sl.orderId });
  } catch(e) { res.status(400).json({ error:e.message }); }
});

// ══════════════════════════════════════════════════════════════════════════════
// CANLI POZİSYON YÖNETİCİSİ — Sniper çıkış sistemi
// Her 30 saniyede çalışır. Şunu izler:
// 1. Trailing SL (kâr takibi)
// 2. Break-even (giriş fiyatına çek)
// 3. CVD Flip — delta tersine döndü = acil çıkış
// 4. Delta Divergence — fiyat gidiyor ama hacim yok = sahte hareket
// 5. Momentum kaybı — CVD yavaşlıyor, TP genişletme iptal
// ══════════════════════════════════════════════════════════════════════════════
const trailingState = new Map(); // symbol → pozisyon durumu

// CVD + Tick delta flip tespiti — en kritik çıkış sinyali
function detectCVDFlip(symbol, side) {
  const cvd = getCVD(symbol);
  if (!cvd || cvd.historyLen < 2) return { flip:false, reason:'Yeterli veri yok' };

  const isLong = side === 'LONG';
  const store  = cvdStore.get(symbol);
  if (!store || store.history.length < 3) return { flip:false };

  // Son 3 periyot delta
  const hist = store.history.slice(-3);
  const deltas = hist.map(h => h.delta);

  // Flip: pozitif deltalar negatife döndü (LONG için tehlike)
  const prevPositive = deltas.slice(0,-1).every(d => d > 0);
  const lastNegative = deltas[deltas.length-1] < -Math.abs(deltas[0]) * 0.5;

  if (isLong && prevPositive && lastNegative) {
    return { flip:true, reason:`CVD flip: Alım baskısı satışa döndü (${cvd.ratio.toFixed(0)}%)`, urgency:'HIGH' };
  }

  const prevNegative = deltas.slice(0,-1).every(d => d < 0);
  const lastPositive = deltas[deltas.length-1] > Math.abs(deltas[0]) * 0.5;

  if (!isLong && prevNegative && lastPositive) {
    return { flip:true, reason:`CVD flip: Satış baskısı alıma döndü (${cvd.ratio.toFixed(0)}%)`, urgency:'HIGH' };
  }

  return { flip:false };
}

// Delta divergence — fiyat gidiyor ama CVD desteklemiyor
function detectDeltaDivergence(symbol, side, entryPrice, currentPrice) {
  const cvd = getCVD(symbol);
  if (!cvd) return { divergence:false };

  const isLong = side === 'LONG';
  const priceMoved = isLong
    ? (currentPrice - entryPrice) / entryPrice * 100
    : (entryPrice - currentPrice) / entryPrice * 100;

  if (priceMoved < 0.3) return { divergence:false }; // Henüz hareket yok

  // Fiyat lehimize gitti ama CVD desteklemiyor
  const cvdSupports = isLong ? cvd.ratio > 45 : cvd.ratio < 55;
  const momentumWeak = cvd.momentum === 'NEGATIVE' || cvd.momentum === 'ACCELERATING_BEAR';
  const momentumWeakShort = cvd.momentum === 'POSITIVE' || cvd.momentum === 'ACCELERATING_BULL';

  if (isLong && priceMoved > 1 && !cvdSupports && momentumWeak) {
    return { divergence:true, reason:`Fiyat ↑ %${priceMoved.toFixed(1)} ama CVD zayıf (${cvd.ratio.toFixed(0)}%) — sahte hareket`, urgency:'MEDIUM' };
  }
  if (!isLong && priceMoved > 1 && cvdSupports && momentumWeakShort) {
    return { divergence:true, reason:`Fiyat ↓ %${priceMoved.toFixed(1)} ama CVD güçlü (${cvd.ratio.toFixed(0)}%) — sahte hareket`, urgency:'MEDIUM' };
  }

  return { divergence:false };
}

// Likidasyon cascade — ani büyük likidasyon ters yönde
function detectAdverseCascade(symbol, side) {
  const liq = getLiqData(symbol);
  if (!liq?.cascade) return { adverse:false };

  const isLong = side === 'LONG';
  const isAdverse = isLong
    ? liq.cascade.direction === 'LONG_CASCADE'   // Long pozisyonlar patladı
    : liq.cascade.direction === 'SHORT_CASCADE';  // Short pozisyonlar patladı

  if (isAdverse && liq.cascade.amount > 300000) { // $300K+
    return { adverse:true, reason:`Ters cascade: $${(liq.cascade.amount/1000).toFixed(0)}K ${liq.cascade.direction}`, urgency:'HIGH' };
  }
  return { adverse:false };
}

// Ana pozisyon yöneticisi
async function managePosition(apiKey, apiSecret, pos) {
  const sym = pos.symbol;
  const side = pos.side;
  const isLong = side === 'LONG';
  const curPrice = pos.markPrice;
  const entryPrice = pos.entryPrice;
  const pnlPct = pos.pnlPct; // Kaldıraçlı PnL %

  // Kaldıraçsız gerçek fiyat hareketi — break-even için bunu kullan
  const leverage = pos.leverage || 1;
  const realPricePct = entryPrice > 0
    ? Math.abs(curPrice - entryPrice) / entryPrice * 100
    : 0;
  const inProfit = pos.side === 'LONG'
    ? curPrice > entryPrice
    : curPrice < entryPrice;
  const realProfitPct = inProfit ? realPricePct : -realPricePct;

  // State al veya oluştur
  if (!trailingState.has(sym)) {
    trailingState.set(sym, {
      entryPrice, highWater:curPrice, currentSL:null,
      breakEvenSet:false, tpExtended:false,
      step1Set:false, step2Set:false, step3Set:false,
      peakPnl:0, peakRealPct:0, lastCheck:Date.now()
    });
  }
  const state = trailingState.get(sym);
  state.peakPnl    = Math.max(state.peakPnl, pnlPct);
  state.peakRealPct= Math.max(state.peakRealPct, realProfitPct);

  const cfg = autoConfig || {};
  // parseFloat + fallback — NaN koruması
  const safe = (v, def) => { const n=parseFloat(v); return isNaN(n)?def:n; };
  const trailPct      = safe(cfg.trailingPct,  2);
  const trailStep     = safe(cfg.trailStep,    0.5); // Min adım boyutu % (fiyat bu kadar ilerlerse SL taşı)
  const breakEvenAt   = safe(cfg.breakEvenPct, 0.5); // Break-even tetik % (kaldıraçsız)
  const karTasima1    = safe(cfg.karTasima1,   1.0); // 1. kâr taşıma: %1 kâr → SL +%0.3 yukarı
  const karTasima2    = safe(cfg.karTasima2,   2.0); // 2. kâr taşıma: %2 kâr → SL +%0.8 yukarı
  const karTasima3    = safe(cfg.karTasima3,   3.5); // 3. kâr taşıma: %3.5 kâr → SL +%1.5 yukarı
  const minRR         = safe(cfg.minRR,        1.0); // Min R/R oranı
  const slPct         = safe(cfg.slPct,        2);
  const tpPct         = safe(cfg.tpPct,        10);

  let action = null; // { type, reason, urgency }

  // ── 1. ACİL ÇIKIŞ — CVD Flip ────────────────────────────────────────────
  const cvdFlip   = detectCVDFlip(sym, side);
  const tickSnap  = getTickAnalysis(sym);

  // Tick delta flip — acil çıkış
  const tickFlip = tickSnap && (
    (isLong  && tickSnap.deltaFlip==='BULL_TO_BEAR') ||
    (!isLong && tickSnap.deltaFlip==='BEAR_TO_BULL')
  );

  // Exhaustion — kârdayken momentum bitti, çık
  const exhaustExit = tickSnap?.microstructure && (
    (isLong  && tickSnap.microstructure.exhaustion &&
     tickSnap.microstructure.exhaustionDir==='BULL_EXHAUST') ||
    (!isLong && tickSnap.microstructure.exhaustion &&
     tickSnap.microstructure.exhaustionDir==='BEAR_EXHAUST')
  );

  // Trapped trader — pozisyon yönünde trapped = çabuk çıkış olacak
  const trappedExit = tickSnap?.microstructure?.trapped && (
    (isLong  && tickSnap.microstructure.lastDelta < 0) ||
    (!isLong && tickSnap.microstructure.lastDelta > 0)
  );
  // CVD flip — kaldıraçsız fiyat hareketi > 0.2% kârdayken (çok hassas)
  if ((cvdFlip.flip || tickFlip || exhaustExit || trappedExit) && realProfitPct > 0.2) {
    if (tickFlip)    cvdFlip.reason = `Tick delta flip: ${tickSnap?.deltaFlip}`;
    if (exhaustExit) cvdFlip.reason = `Exhaustion: momentum bitti`;
    if (trappedExit) cvdFlip.reason = `Trapped trader: hızlı ters dönüş bekleniyor`;
    action = { type:'EMERGENCY_EXIT', ...cvdFlip };
  }

  // ── 2. ACİL ÇIKIŞ — Ters Cascade ────────────────────────────────────────
  if (!action) {
    const cascade = detectAdverseCascade(sym, side);
    if (cascade.adverse && realProfitPct > 0) { // Kârdayken cascade gelirse koru
      action = { type:'EMERGENCY_EXIT', ...cascade };
    }
  }

  // ── 3. DELTA DİVERGENCE — TP Genişletme İptal, SL Sıkıştır ─────────────
  const divergence = detectDeltaDivergence(sym, side, entryPrice, curPrice);
  if (!action && divergence.divergence) {
    // Kaldıraçsız kar varsa koru
    if (realProfitPct > 0.3) {
      action = { type:'TIGHTEN_SL', reason:divergence.reason, urgency:'MEDIUM',
        newSL: isLong
          ? +(curPrice * (1 - trailPct*0.5/100)).toFixed(8)  // Normal trailing'in yarısı
          : +(curPrice * (1 + trailPct*0.5/100)).toFixed(8)
      };
    }
  }

  // ── 4. BREAK-EVEN ─────────────────────────────────────────────────────────
  // Kaldıraçsız fiyat hareketi breakEvenAt % geçince SL giriş fiyatına çek
  // Örn: 20x kaldıraç, breakEvenAt=%0.5 → fiyat %0.5 hareket = kaldıraçlı %10
  if (!action && !state.breakEvenSet && realProfitPct >= breakEvenAt) {
    action = {
      type:'BREAK_EVEN',
      reason:`Fiyat %${realProfitPct.toFixed(2)} hareket etti (kaldıraçsız) → SL giriş fiyatına`,
      urgency:'LOW',
      newSL: +(entryPrice * (isLong ? 1.001 : 0.999)).toFixed(8)
    };
    state.breakEvenSet = true;
    logAuto(`${sym} Break-even: Gerçek hareket %${realProfitPct.toFixed(2)}, Kaldıraçlı PnL: %${pnlPct.toFixed(1)}, Lev:${leverage}x`);
  }

  // ── 4b. KÂR TAŞIMA ADIMLARI ─────────────────────────────────────────────────
  if (!action && state.breakEvenSet) {
    let stepSL = null, stepReason = null;
    if (realProfitPct >= karTasima3 && !state.step3Set) {
      stepSL = isLong ? +(entryPrice*(1+0.015)).toFixed(8) : +(entryPrice*(1-0.015)).toFixed(8);
      stepReason = `Kâr taşıma 3: %${realProfitPct.toFixed(2)} → SL kâr +%1.5`;
      state.step3Set = true;
    } else if (realProfitPct >= karTasima2 && !state.step2Set) {
      stepSL = isLong ? +(entryPrice*(1+0.008)).toFixed(8) : +(entryPrice*(1-0.008)).toFixed(8);
      stepReason = `Kâr taşıma 2: %${realProfitPct.toFixed(2)} → SL kâr +%0.8`;
      state.step2Set = true;
    } else if (realProfitPct >= karTasima1 && !state.step1Set) {
      stepSL = isLong ? +(entryPrice*(1+0.003)).toFixed(8) : +(entryPrice*(1-0.003)).toFixed(8);
      stepReason = `Kâr taşıma 1: %${realProfitPct.toFixed(2)} → SL giriş üstü +%0.3`;
      state.step1Set = true;
    }
    if (stepSL) {
      const better = isLong ? (!state.currentSL||stepSL>state.currentSL) : (!state.currentSL||stepSL<state.currentSL);
      if (better) action = { type:'KAR_TASIMA', reason:stepReason, urgency:'LOW', newSL:stepSL };
    }
  }

  // ── 5. TRAILING SL (adım bazlı) ──────────────────────────────────────────
  if (!action && state.breakEvenSet) {
    const newHW = isLong
      ? Math.max(state.highWater||curPrice, curPrice)
      : Math.min(state.highWater||curPrice, curPrice);
    // Min adım: fiyat trailStep% kadar ilerlemiş olmalı (sürekli güncellemeyi önler)
    const movedPct = state.highWater
      ? Math.abs(newHW-state.highWater)/state.highWater*100 : 0;
    if (movedPct >= trailStep) {
      state.highWater = newHW;
      const newSL = isLong
        ? +(state.highWater*(1-trailPct/100)).toFixed(8)
        : +(state.highWater*(1+trailPct/100)).toFixed(8);
      const better = isLong
        ? (!state.currentSL||newSL>state.currentSL)
        : (!state.currentSL||newSL<state.currentSL);
      if (better) action = {
        type:'TRAIL_SL',
        reason:`Trailing (%${trailStep} adım): HW=$${state.highWater.toFixed(4)} → SL=$${newSL}`,
        urgency:'LOW', newSL
      };
    }
  }

  // ── 6. TP GENİŞLETME — Momentum devam ediyorsa TP'yi yukarı taşı ────────
  // TP genişletme: kaldıraçsız fiyat hedefin %70'ini geçti mi?
  const tpRealTarget = tpPct / leverage; // Kaldıraçsız hedef %
  if (!action && !state.tpExtended && realProfitPct > tpRealTarget * 0.7) {
    const cvd = getCVD(sym);
    const momentumStrong = isLong
      ? cvd?.momentum === 'ACCELERATING_BULL' || cvd?.ratio > 65
      : cvd?.momentum === 'ACCELERATING_BEAR' || cvd?.ratio < 35;
    if (momentumStrong) {
      // TP'yi %50 genişlet
      state.tpExtended = true;
      logAuto(`📈 ${sym} Momentum güçlü — TP genişletildi (yeni hedef: %${(tpPct*1.5).toFixed(0)})`);
      // Bunu frontend'e bildir
    }
  }

  // ── AKSİYON UYGULA ────────────────────────────────────────────────────────
  if (!action) return null;

  logAuto(`[${sym}] ${action.type} (${action.urgency}): ${action.reason}`);

  if (action.type === 'EMERGENCY_EXIT') {
    // Tüm açık emirleri iptal et ve pozisyonu kapat
    try {
      await bReq(apiKey, apiSecret, 'DELETE', '/fapi/v1/allOpenOrders', {symbol:sym});
      const qty = pos.positionAmt.toString();
      const r = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
        symbol:sym, side:isLong?'SELL':'BUY',
        type:'MARKET', quantity:qty,
        reduceOnly:'true', positionSide:'BOTH'
      });
      logAuto(`✅ ${sym} ACİL ÇIKIŞ: PnL %${pnlPct.toFixed(2)} — ${r.orderId}`);
      trailingState.delete(sym);
      return { action:'CLOSED', pnl:pnlPct, reason:action.reason };
    } catch(e) {
      logAuto(`❌ ${sym} Acil çıkış hatası: ${e.message}`);
    }
  }

  if (action.type === 'BREAK_EVEN' || action.type === 'TRAIL_SL' || action.type === 'TIGHTEN_SL') {
    const newSL = action.newSL;
    if (!newSL) return null;
    try {
      await bReq(apiKey, apiSecret, 'DELETE', '/fapi/v1/allOpenOrders', {symbol:sym});
      const r = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
        symbol:sym, side:isLong?'SELL':'BUY',
        type:'STOP_MARKET', stopPrice:newSL.toString(),
        quantity:pos.positionAmt.toString(),
        reduceOnly:'true', positionSide:'BOTH', workingType:'MARK_PRICE'
      });
      state.currentSL = newSL;
      logAuto(`✅ ${sym} SL güncellendi → $${newSL} (${r.orderId})`);
    } catch(e) {
      logAuto(`❌ ${sym} SL güncelleme hatası: ${e.message}`);
    }
  }

  return action;
}

// Tüm pozisyonları yönet
async function checkTrailingSL(apiKey, apiSecret, positions) {
  for (const pos of positions) {
    await managePosition(apiKey, apiSecret, pos).catch(e =>
      logAuto(`${pos.symbol} yönetim hatası: ${e.message}`)
    );
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
        // Mevcut SL iptal et
        await bReq(apiKey,apiSecret,'DELETE','/fapi/v1/allOpenOrders',{symbol:sym});
        // Yeni SL yerleştir
        const qty = pos.positionAmt.toString();
        const cSide = isLong ? 'SELL' : 'BUY';
        const r = await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',{
          symbol:sym, side:cSide, type:'STOP_MARKET',
          stopPrice:newSL.toString(), quantity:qty,
          reduceOnly:'true', positionSide:'BOTH', workingType:'MARK_PRICE'
        });
        ts.currentSL = newSL;
        ts.slOrderId = r.orderId;
        console.log(`${sym} Trailing SL → $${newSL} (orderId:${r.orderId})`);
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

let autoConfig = null;
let autoRunning = false;
let autoTimer = null;
const autoLog = []; // Son 50 otomatik işlem logu

app.post('/api/auto/config', (req, res) => {
  autoConfig = req.body;
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
      maxPositions=3, minScore=70, allowLong=true, allowShort=true,
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
            config:{ trailing:true, trailPct, breakEvenPct, entryPrice:pos.entryPrice }
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

    // 2. Kill Zone kontrolü — ölü saatte tarama kalitesi düşük
    const kz = getKillZone();
    if (kz.zone === 'DEAD') {
      logAuto(`💤 Ölü saat (${kz.label}) — tarama seyreltiyor`);
      // Ölü saatte çok daha sıkı kriter
    }

    // 3. Volatil coinleri kullan — en çok hareket edenler önce
    let scanList;
    if (symbols.length > 0) {
      // Kullanıcı belirlemiş
      const coinsResp = await fetch(`http://localhost:${PORT}/api/futures-coins`).then(r=>r.json());
      scanList = (coinsResp.coins||[]).filter(c=>symbols.includes(c.symbol));
    } else {
      // Volatilite scanner'dan al — en aktif coinler
      if (volatilityStore.coins.length > 0) {
        scanList = volatilityStore.coins.slice(0, kz.zone==='DEAD'?10:20);
        logAuto(`🔥 Volatil top ${scanList.length}: ${scanList.slice(0,5).map(c=>c.symbol).join(', ')}...`);
      } else {
        const coinsResp = await fetch(`http://localhost:${PORT}/api/futures-coins`).then(r=>r.json());
        scanList = (coinsResp.coins||[]).sort((a,b)=>b.volume-a.volume).slice(0,20);
      }
    }
    if (!scanList?.length) return;

    // Kill Zone bazlı min skor ayarı
    const effectiveMinScore = kz.zone === 'DEAD' ? Math.max(minScore + 10, 80) :
                              kz.strength < 1.0   ? minScore + 5 : minScore;
    if (kz.zone === 'DEAD') logAuto(`💤 Ölü saat — min skor ${effectiveMinScore}'e yükseltildi`);

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

        if (isLong  && !hasBullSetup) { logAuto(`${coin.symbol} LONG ama setup yok — atlandı`); continue; }
        if (isShort && !hasBearSetup) { logAuto(`${coin.symbol} SHORT ama setup yok — atlandı`); continue; }

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
        if (score < effectiveMinScore) { logAuto(`${coin.symbol} skor ${score} < ${effectiveMinScore}(kz:${kz.zone}) — atlandı`); continue; }

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

        // ── R/R KONTROLÜ ─────────────────────────────────────────────────────
        // Minimum R/R sağlanmıyorsa işlem açma
        const proTP = analysis.proTPSL;
        if (proTP && proTP.rr1 != null && !isNaN(proTP.rr1) && proTP.rr1 < (cfg.minRR||1.0)) {
          logAuto(`${coin.symbol} R/R ${proTP.rr1?.toFixed(2)} < min ${cfg.minRR||1.0} — atlandı`);
          continue;
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

        // TP/SL
        const proTPSL = analysis.proTPSL;
        if (!proTPSL) continue;
        const targetPrice = recommendation==='LONG' ? proTPSL.tp : proTPSL.tp;
        const stopPrice   = recommendation==='LONG' ? proTPSL.sl : proTPSL.sl;

        logAuto(`🎯 Sinyal: ${coin.symbol} ${recommendation} skor:${score} — emir açılıyor`);

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
            config:{ trailing:true, trailPct, breakEvenPct,
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
  // Her 3 dakikada bir tara
  autoTimer = setInterval(runAutoScan, 3*60*1000);
  runAutoScan(); // Hemen başlat
}

app.listen(PORT, ()=>console.log(`✅ Server ${PORT}`));
