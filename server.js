const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const fetch = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));

const PORT = process.env.PORT || 3000;
const BINANCE_BASES = (process.env.BINANCE_FAPI_BASES || 'https://fapi.binance.com')
  .split(',')
  .map(x => x.trim().replace(/\/$/, ''))
  .filter(Boolean);
let activeBaseIndex = 0;
const VERSION = 'RISK_PROOF_451_REGION_FIX_2026_05_27';

function activeBase() {
  return BINANCE_BASES[activeBaseIndex] || 'https://fapi.binance.com';
}
function isGeoBlockedMessage(msg) {
  return String(msg || '').includes('HTTP 451') ||
         String(msg || '').toLowerCase().includes('restricted location') ||
         String(msg || '').toLowerCase().includes('service unavailable from a restricted location');
}
function geoHelp() {
  return 'Binance USD-M Futures resmi REST endpointi yalnızca https://fapi.binance.com. Bu sunucu IP/bölge Binance tarafından HTTP 451 ile kısıtlanmış görünüyor. Railway Settings > Regions bölümünde US replikalarını 0 yapıp EU West veya Southeast Asia replikasını 1 yap, sonra redeploy et. Bu bir kod bugı değil; hosting bölgesi/IP erişim sorunu.';
}
function makeUrl(base, path, query) {
  return `${base}${path}${query ? '?' + query : ''}`;
}


// =============================================================================
// CACHE + HELPERS
// =============================================================================
const cache = new Map();
const inflight = new Map();
let timeOffsetMs = 0;
let timeOffsetExp = 0;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const n = (v, d = 0) => {
  const x = Number.parseFloat(v);
  return Number.isFinite(x) ? x : d;
};
const i = (v, d = 0) => {
  const x = Number.parseInt(v, 10);
  return Number.isFinite(x) ? x : d;
};
const clean = (obj) => Object.fromEntries(
  Object.entries(obj || {}).filter(([, v]) => v !== undefined && v !== null && v !== '')
);
function qs(obj) {
  return new URLSearchParams(clean(obj)).toString();
}
function sign(query, secret) {
  return crypto.createHmac('sha256', secret).update(query).digest('hex');
}
function errMsg(e) {
  return e && e.message ? e.message : String(e);
}
function normalizeSymbol(symbol) {
  if (!symbol) throw new Error('Sembol gerekli');
  const s = String(symbol).trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
  return s.endsWith('USDT') ? s : `${s}USDT`;
}
function sideToOrderSide(side) {
  const s = String(side || '').toUpperCase();
  if (s === 'LONG') return { isLong: true, openSide: 'BUY', closeSide: 'SELL' };
  if (s === 'SHORT') return { isLong: false, openSide: 'SELL', closeSide: 'BUY' };
  throw new Error('side LONG veya SHORT olmalı');
}
function decimalPlaces(step) {
  const s = String(step);
  if (s.includes('e-')) return Number(s.split('e-')[1]);
  const frac = (s.split('.')[1] || '').replace(/0+$/g, '');
  return frac.length;
}
function fmtByStep(value, step) {
  const dp = decimalPlaces(step);
  return Number(value).toFixed(dp).replace(/\.0+$/,'').replace(/(\.\d*?)0+$/,'$1');
}
function floorToStep(value, step) {
  const st = n(step, 1);
  const dp = decimalPlaces(step);
  const floored = Math.floor((n(value) + 1e-15) / st) * st;
  return Number(floored.toFixed(dp));
}
function roundToTick(value, tick) {
  const tk = n(tick, 0.01);
  const dp = decimalPlaces(tick);
  return Number((Math.round(n(value) / tk) * tk).toFixed(dp));
}
function pct(a, b) {
  a = n(a); b = n(b);
  return b ? ((a - b) / b) * 100 : 0;
}

async function cached(key, ttlMs, fn) {
  const now = Date.now();
  const hit = cache.get(key);
  if (hit && now < hit.exp) return hit.val;
  if (inflight.has(key)) return inflight.get(key);
  const p = (async () => {
    const val = await fn();
    cache.set(key, { val, exp: Date.now() + ttlMs });
    inflight.delete(key);
    return val;
  })().catch(e => { inflight.delete(key); throw e; });
  inflight.set(key, p);
  return p;
}

async function fetchJson(url, options = {}, label = 'Binance') {
  let res, text, data;
  try {
    res = await fetch(url, options);
    text = await res.text();
  } catch (e) {
    const m = errMsg(e);
    if (m.includes('ENOTFOUND') || m.includes('getaddrinfo')) {
      throw new Error(`${label}: DNS çözülemedi. BINANCE_FAPI_BASES içinde geçersiz domain olabilir. Resmi USD-M Futures REST base: https://fapi.binance.com. Orijinal hata: ${m}`);
    }
    throw new Error(`${label}: ağ hatası: ${m}`);
  }
  try {
    data = text ? JSON.parse(text) : null;
  } catch (_) {
    throw new Error(`${label}: JSON dönmedi. HTTP ${res.status}. Cevap: ${text.slice(0, 160)}`);
  }
  const retryAfter = res.headers && res.headers.get ? res.headers.get('retry-after') : null;
  if (res.status === 429 || res.status === 418) {
    throw new Error(`${label}: Binance rate limit / ban riski HTTP ${res.status}${retryAfter ? `, Retry-After=${retryAfter}` : ''}`);
  }
  if (!res.ok) {
    throw new Error(`${label}: HTTP ${res.status}: ${data && (data.msg || data.message) ? (data.msg || data.message) : JSON.stringify(data).slice(0, 200)}`);
  }
  if (data && typeof data === 'object' && data.code && Number(data.code) < 0) {
    const code = Number(data.code);
    const msg = data.msg || 'Binance hatası';
    if (code === -1003) throw new Error(`${label}: Çok istek / 429: ${msg}`);
    if (code === -1021) throw new Error(`${label}: Saat senkron hatası (-1021): ${msg}`);
    if (code === -2015) throw new Error(`${label}: API key/secret geçersiz, IP whitelist veya Futures izni yok (-2015): ${msg}`);
    if (code === -2014) throw new Error(`${label}: API key formatı geçersiz (-2014): ${msg}`);
    if (code === -1121) throw new Error(`${label}: Sembol geçersiz (-1121): ${msg}`);
    throw new Error(`${label}: ${msg} (${code})`);
  }
  return data;
}

async function publicGetNoCache(path, params = {}) {
  const query = qs(params);
  let lastErr = null;
  for (let attempt = 0; attempt < BINANCE_BASES.length; attempt++) {
    const idx = (activeBaseIndex + attempt) % BINANCE_BASES.length;
    const base = BINANCE_BASES[idx];
    try {
      const data = await fetchJson(makeUrl(base, path, query), {}, `${path} @ ${base}`);
      activeBaseIndex = idx;
      return data;
    } catch (e) {
      lastErr = e;
      if (isGeoBlockedMessage(errMsg(e)) && attempt < BINANCE_BASES.length - 1) continue;
      if (isGeoBlockedMessage(errMsg(e))) throw new Error(`${errMsg(e)} | ${geoHelp()}`);
      throw e;
    }
  }
  throw lastErr || new Error(`${path}: Binance endpoint erişilemedi`);
}

async function publicGet(path, params = {}, ttlMs = 0) {
  const query = qs(params);
  if (ttlMs > 0) return cached(`PUB:${path}:${query}:BASES=${BINANCE_BASES.join('|')}`, ttlMs, () => publicGetNoCache(path, params));
  return publicGetNoCache(path, params);
}

async function syncTime() {
  const now = Date.now();
  if (now < timeOffsetExp) return now + timeOffsetMs;
  const data = await publicGet('/fapi/v1/time', {}, 0);
  timeOffsetMs = n(data.serverTime, now) - now;
  timeOffsetExp = now + 30_000;
  return Date.now() + timeOffsetMs;
}

async function bReq(apiKey, apiSecret, method, path, params = {}) {
  if (!apiKey || !apiSecret) throw new Error('API key ve secret gerekli');
  const timestamp = await syncTime();
  const payload = clean({ ...params, timestamp, recvWindow: params.recvWindow || 10000 });
  const query = qs(payload);
  const signature = sign(query, apiSecret);
  const full = `${query}&signature=${signature}`;
  const upper = String(method).toUpperCase();
  const headers = { 'X-MBX-APIKEY': apiKey, 'Content-Type': 'application/x-www-form-urlencoded' };
  const options = { method: upper, headers };
  if (upper !== 'GET' && upper !== 'DELETE') options.body = full;

  let lastErr = null;
  for (let attempt = 0; attempt < BINANCE_BASES.length; attempt++) {
    const idx = (activeBaseIndex + attempt) % BINANCE_BASES.length;
    const base = BINANCE_BASES[idx];
    const finalUrl = (upper === 'GET' || upper === 'DELETE') ? `${base}${path}?${full}` : `${base}${path}`;
    try {
      const data = await fetchJson(finalUrl, options, `${path} @ ${base}`);
      activeBaseIndex = idx;
      return data;
    } catch (e) {
      lastErr = e;
      if (isGeoBlockedMessage(errMsg(e)) && attempt < BINANCE_BASES.length - 1) continue;
      if (isGeoBlockedMessage(errMsg(e))) throw new Error(`${errMsg(e)} | ${geoHelp()}`);
      throw e;
    }
  }
  throw lastErr || new Error(`${path}: Binance signed endpoint erişilemedi`);
}

// =============================================================================
// EXCHANGE INFO / RULES
// =============================================================================
async function getExchangeInfo() {
  return publicGet('/fapi/v1/exchangeInfo', {}, 10 * 60 * 1000);
}
async function getSymbolInfo(symbol) {
  const sym = normalizeSymbol(symbol);
  const ex = await getExchangeInfo();
  const si = (ex.symbols || []).find(s => s.symbol === sym);
  if (!si) throw new Error(`${sym} exchangeInfo içinde yok`);
  if (si.status !== 'TRADING' || si.contractType !== 'PERPETUAL' || si.quoteAsset !== 'USDT') {
    throw new Error(`${sym} TRADING USDT-M PERPETUAL değil. status=${si.status}, contractType=${si.contractType}, quoteAsset=${si.quoteAsset}`);
  }
  return si;
}
function parseRules(si) {
  const f = (type) => (si.filters || []).find(x => x.filterType === type) || {};
  const price = f('PRICE_FILTER');
  const lot = f('LOT_SIZE');
  const marketLot = f('MARKET_LOT_SIZE');
  const minNot = f('MIN_NOTIONAL');
  return {
    tickSize: price.tickSize || '0.01',
    stepSize: (marketLot.stepSize && n(marketLot.stepSize) > 0) ? marketLot.stepSize : (lot.stepSize || '0.001'),
    minQty: (marketLot.minQty && n(marketLot.minQty) > 0) ? n(marketLot.minQty) : n(lot.minQty, 0),
    maxQty: (marketLot.maxQty && n(marketLot.maxQty) > 0) ? n(marketLot.maxQty) : n(lot.maxQty, Number.MAX_SAFE_INTEGER),
    minNotional: n(minNot.notional || minNot.minNotional, 5),
    triggerProtect: n(si.triggerProtect, 0),
  };
}

// =============================================================================
// INDICATORS
// =============================================================================
function closes(kl) { return Array.isArray(kl) ? kl.map(k => n(k[4])) : []; }
function ema(values, period) {
  if (!Array.isArray(values) || values.length < period) return 0;
  const k = 2 / (period + 1);
  let e = values.slice(0, period).reduce((a,b) => a + b, 0) / period;
  for (let idx = period; idx < values.length; idx++) e = values[idx] * k + e * (1 - k);
  return e;
}
function rsi(values, period = 14) {
  if (!Array.isArray(values) || values.length < period + 2) return 50;
  let gains = 0, losses = 0;
  for (let idx = 1; idx <= period; idx++) {
    const diff = values[idx] - values[idx - 1];
    if (diff >= 0) gains += diff; else losses -= diff;
  }
  let avgGain = gains / period, avgLoss = losses / period;
  for (let idx = period + 1; idx < values.length; idx++) {
    const diff = values[idx] - values[idx - 1];
    avgGain = ((avgGain * (period - 1)) + Math.max(diff, 0)) / period;
    avgLoss = ((avgLoss * (period - 1)) + Math.max(-diff, 0)) / period;
  }
  if (avgLoss === 0) return 100;
  return 100 - (100 / (1 + avgGain / avgLoss));
}
function macd(values) {
  if (!values || values.length < 35) return { macd: 0, signal: 0, hist: 0 };
  const fast = ema(values, 12);
  const slow = ema(values, 26);
  const m = fast - slow;
  const series = [];
  for (let end = 35; end <= values.length; end++) {
    const v = values.slice(0, end);
    series.push(ema(v, 12) - ema(v, 26));
  }
  const sig = ema(series, 9);
  return { macd: m, signal: sig, hist: m - sig };
}
function atrPct(kl, period = 14) {
  if (!Array.isArray(kl) || kl.length < period + 1) return 0;
  const trs = [];
  for (let idx = 1; idx < kl.length; idx++) {
    const h = n(kl[idx][2]), l = n(kl[idx][3]), pc = n(kl[idx - 1][4]);
    trs.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)));
  }
  const tail = trs.slice(-period);
  const atr = tail.reduce((a,b) => a + b, 0) / tail.length;
  const last = n(kl[kl.length - 1][4]);
  return last ? (atr / last) * 100 : 0;
}
function vwap(kl) {
  if (!Array.isArray(kl) || !kl.length) return 0;
  let tpv = 0, vol = 0;
  for (const k of kl) {
    const typical = (n(k[2]) + n(k[3]) + n(k[4])) / 3;
    const v = n(k[5]);
    tpv += typical * v; vol += v;
  }
  return vol ? tpv / vol : 0;
}
function bollinger(kl, period = 20) {
  const c = closes(kl).slice(-period);
  if (c.length < period) return { mid: 0, upper: 0, lower: 0, pos: 'NA' };
  const mid = c.reduce((a,b) => a + b, 0) / c.length;
  const std = Math.sqrt(c.reduce((s,x) => s + Math.pow(x - mid, 2), 0) / c.length);
  const upper = mid + 2 * std, lower = mid - 2 * std, last = c[c.length - 1];
  const pos = last >= upper * 0.985 ? 'TOP' : last <= lower * 1.015 ? 'BOTTOM' : 'MIDDLE';
  return { mid, upper, lower, pos };
}
function tfPack(kl) {
  const c = closes(kl);
  const last = c[c.length - 1] || 0;
  const e20 = ema(c, 20), e50 = ema(c, 50), e200 = ema(c, Math.min(200, c.length));
  const vw = vwap(kl.slice(-48));
  const m = macd(c);
  return {
    close: last,
    change: c.length > 2 ? pct(last, c[c.length - 2]) : 0,
    changeLookback: c.length > 12 ? pct(last, c[c.length - 12]) : 0,
    rsi: rsi(c),
    ema20: e20,
    ema50: e50,
    ema200: e200,
    macdHist: m.hist,
    atrPct: atrPct(kl),
    vwap: vw,
    aboveVwap: vw ? last > vw : false,
    trend: e20 > e50 && last > e20 ? 'UP' : e20 < e50 && last < e20 ? 'DOWN' : 'MIXED',
  };
}
function analyzeDepth(depth, mark) {
  const bids = Array.isArray(depth?.bids) ? depth.bids : [];
  const asks = Array.isArray(depth?.asks) ? depth.asks : [];
  const sumNot = (rows, lim) => rows.slice(0, lim).reduce((s, r) => s + n(r[0]) * n(r[1]), 0);
  const bid20 = sumNot(bids, 20), ask20 = sumNot(asks, 20);
  const bid50 = sumNot(bids, 50), ask50 = sumNot(asks, 50);
  const bestBid = bids.length ? n(bids[0][0]) : 0;
  const bestAsk = asks.length ? n(asks[0][0]) : 0;
  const spreadPct = mark ? ((bestAsk - bestBid) / mark) * 100 : 0;
  const imbalance20 = (bid20 + ask20) ? (bid20 - ask20) / (bid20 + ask20) : 0;
  const imbalance50 = (bid50 + ask50) ? (bid50 - ask50) / (bid50 + ask50) : 0;
  return { bestBid, bestAsk, spreadPct, bid20, ask20, bid50, ask50, imbalance20, imbalance50 };
}
function analyzeAggTrades(trades) {
  if (!Array.isArray(trades) || !trades.length) return { cvdQuote: 0, buyQuote: 0, sellQuote: 0, buyRatio: 1, trades: 0 };
  let buy = 0, sell = 0;
  for (const t of trades) {
    const quote = n(t.p) * n(t.q);
    // Binance aggTrade m=true: buyer is maker => aggressive seller. m=false => aggressive buyer.
    if (t.m === true) sell += quote; else buy += quote;
  }
  return { cvdQuote: buy - sell, buyQuote: buy, sellQuote: sell, buyRatio: sell ? buy / sell : 9.99, trades: trades.length };
}
function lastItem(arr) { return Array.isArray(arr) && arr.length ? arr[arr.length - 1] : null; }
function ratioLast(arr, field = 'longShortRatio') { return n(lastItem(arr)?.[field], 1); }
function oiChangePct(hist) {
  if (!Array.isArray(hist) || hist.length < 2) return 0;
  const first = n(hist[0].sumOpenInterest || hist[0].openInterest);
  const last = n(hist[hist.length - 1].sumOpenInterest || hist[hist.length - 1].openInterest);
  return first ? pct(last, first) : 0;
}
function takerRatio(arr) { return n(lastItem(arr)?.buySellRatio, 1); }
function scoreAnalysis(data) {
  const { t4, t1, t15, t5, depth, flow, fundingRate, oiChg, globalLs, topPos, topAcc, taker5, taker15, taker1 } = data;
  let long = 0, short = 0;
  const reasons = [];

  if (t4.trend === 'UP') { long += 16; reasons.push('4h trend UP'); }
  if (t4.trend === 'DOWN') { short += 16; reasons.push('4h trend DOWN'); }
  if (t1.trend === 'UP') long += 14;
  if (t1.trend === 'DOWN') short += 14;
  if (t15.trend === 'UP') long += 10;
  if (t15.trend === 'DOWN') short += 10;
  if (t5.trend === 'UP') long += 8;
  if (t5.trend === 'DOWN') short += 8;

  if (t1.aboveVwap && t15.aboveVwap) long += 8;
  if (!t1.aboveVwap && !t15.aboveVwap) short += 8;
  if (t15.rsi < 38 && t1.trend !== 'DOWN') long += 8;
  if (t15.rsi > 62 && t1.trend !== 'UP') short += 8;
  if (t15.macdHist > 0 && t5.macdHist > 0) long += 8;
  if (t15.macdHist < 0 && t5.macdHist < 0) short += 8;

  if (flow.buyRatio > 1.15 && flow.cvdQuote > 0) long += 12;
  if (flow.buyRatio < 0.87 && flow.cvdQuote < 0) short += 12;
  if (taker5 > 1.12 && taker15 > 1.05) long += 9;
  if (taker5 < 0.89 && taker15 < 0.95) short += 9;
  if (depth.imbalance20 > 0.12 && depth.imbalance50 > 0.05) long += 8;
  if (depth.imbalance20 < -0.12 && depth.imbalance50 < -0.05) short += 8;

  if (oiChg > 1.5 && t15.changeLookback > 0) long += 8;
  if (oiChg > 1.5 && t15.changeLookback < 0) short += 8;
  if (fundingRate < -0.015) long += 5;
  if (fundingRate > 0.03) short += 5;

  // Kalabalık taraf / MM tuzağı: aşırı long kalabalığı + fiyat yükselmiş + funding yüksek => SHORT riskini artır.
  let trap = 'NONE';
  if (globalLs > 1.45 && topPos > 1.45 && fundingRate > 0.025 && t15.changeLookback > 1.2) {
    short += 12; trap = 'LONG_CROWD_PUMP_SHORT_RISK';
  }
  if (globalLs < 0.72 && topPos < 0.72 && fundingRate < -0.015 && t15.changeLookback < -1.2) {
    long += 12; trap = 'SHORT_CROWD_DUMP_LONG_RISK';
  }
  if (topAcc > 1.6 && flow.buyRatio < 0.95 && t5.change < 0) { short += 6; trap = trap === 'NONE' ? 'TOP_ACCOUNT_LONG_CROWD_WEAK_FLOW' : trap; }
  if (topAcc < 0.7 && flow.buyRatio > 1.05 && t5.change > 0) { long += 6; trap = trap === 'NONE' ? 'TOP_ACCOUNT_SHORT_CROWD_STRONG_FLOW' : trap; }

  long = Math.max(0, Math.min(100, Math.round(long)));
  short = Math.max(0, Math.min(100, Math.round(short)));
  const recommendation = long >= 58 && long >= short + 6 ? 'LONG' : short >= 58 && short >= long + 6 ? 'SHORT' : 'WAIT';
  const confidence = Math.max(long, short);
  return { longScore: long, shortScore: short, recommendation, confidence, trap, reasons };
}

async function analyzeOne(symbol) {
  const sym = normalizeSymbol(symbol);
  await getSymbolInfo(sym);
  const warnings = [];
  const safe = async (name, p) => {
    try { return await p; } catch (e) { warnings.push(`${name}: ${errMsg(e)}`); return null; }
  };

  const [k4, k1, k15, k5, premium, funding, oiNow, oiHist, globalLsData, topPosData, topAccData, taker5Data, taker15Data, taker1Data, depthData, tradesData, bookTicker] = await Promise.all([
    publicGet('/fapi/v1/klines', { symbol: sym, interval: '4h', limit: 220 }, 4 * 60 * 1000),
    publicGet('/fapi/v1/klines', { symbol: sym, interval: '1h', limit: 220 }, 90 * 1000),
    publicGet('/fapi/v1/klines', { symbol: sym, interval: '15m', limit: 220 }, 45 * 1000),
    publicGet('/fapi/v1/klines', { symbol: sym, interval: '5m', limit: 220 }, 25 * 1000),
    safe('premiumIndex', publicGet('/fapi/v1/premiumIndex', { symbol: sym }, 20 * 1000)),
    safe('fundingRate', publicGet('/fapi/v1/fundingRate', { symbol: sym, limit: 3 }, 4 * 60 * 1000)),
    safe('openInterest', publicGet('/fapi/v1/openInterest', { symbol: sym }, 20 * 1000)),
    safe('openInterestHist', publicGet('/futures/data/openInterestHist', { symbol: sym, period: '15m', limit: 12 }, 60 * 1000)),
    safe('globalLongShortAccountRatio', publicGet('/futures/data/globalLongShortAccountRatio', { symbol: sym, period: '15m', limit: 8 }, 60 * 1000)),
    safe('topLongShortPositionRatio', publicGet('/futures/data/topLongShortPositionRatio', { symbol: sym, period: '15m', limit: 8 }, 60 * 1000)),
    safe('topLongShortAccountRatio', publicGet('/futures/data/topLongShortAccountRatio', { symbol: sym, period: '15m', limit: 8 }, 60 * 1000)),
    safe('taker5m', publicGet('/futures/data/takerlongshortRatio', { symbol: sym, period: '5m', limit: 12 }, 45 * 1000)),
    safe('taker15m', publicGet('/futures/data/takerlongshortRatio', { symbol: sym, period: '15m', limit: 8 }, 60 * 1000)),
    safe('taker1h', publicGet('/futures/data/takerlongshortRatio', { symbol: sym, period: '1h', limit: 6 }, 2 * 60 * 1000)),
    safe('depth', publicGet('/fapi/v1/depth', { symbol: sym, limit: 50 }, 15 * 1000)),
    safe('aggTrades', publicGet('/fapi/v1/aggTrades', { symbol: sym, limit: 500 }, 15 * 1000)),
    safe('bookTicker', publicGet('/fapi/v1/ticker/bookTicker', { symbol: sym }, 8 * 1000)),
  ]);

  if (!Array.isArray(k4) || !Array.isArray(k1) || !Array.isArray(k15) || !Array.isArray(k5)) {
    throw new Error(`${sym} mum verisi alınamadı; analiz sahte üretilmedi.`);
  }

  const t4 = tfPack(k4), t1 = tfPack(k1), t15 = tfPack(k15), t5 = tfPack(k5);
  const mark = n(premium?.markPrice, t5.close || t15.close);
  const depth = analyzeDepth(depthData, mark);
  if (bookTicker) {
    depth.bestBid = n(bookTicker.bidPrice, depth.bestBid);
    depth.bestAsk = n(bookTicker.askPrice, depth.bestAsk);
    depth.spreadPct = mark ? ((depth.bestAsk - depth.bestBid) / mark) * 100 : depth.spreadPct;
  }
  const flow = analyzeAggTrades(tradesData);
  const fundingRate = n(premium?.lastFundingRate, n(lastItem(funding)?.fundingRate, 0)) * 100;
  const oiChg = oiChangePct(oiHist);
  const globalLs = ratioLast(globalLsData);
  const topPos = ratioLast(topPosData);
  const topAcc = ratioLast(topAccData);
  const taker5 = takerRatio(taker5Data);
  const taker15 = takerRatio(taker15Data);
  const taker1 = takerRatio(taker1Data);
  const score = scoreAnalysis({ t4, t1, t15, t5, depth, flow, fundingRate, oiChg, globalLs, topPos, topAcc, taker5, taker15, taker1 });

  return {
    ok: true,
    version: VERSION,
    symbol: sym,
    price: mark,
    time: new Date().toISOString(),
    timeframes: {
      '4h': { trend: t4.trend, rsi: Number(t4.rsi.toFixed(1)), ema20: t4.ema20, ema50: t4.ema50, macdHist: t4.macdHist, atrPct: Number(t4.atrPct.toFixed(2)) },
      '1h': { trend: t1.trend, rsi: Number(t1.rsi.toFixed(1)), ema20: t1.ema20, ema50: t1.ema50, vwap: t1.vwap, aboveVwap: t1.aboveVwap, macdHist: t1.macdHist, atrPct: Number(t1.atrPct.toFixed(2)) },
      '15m': { trend: t15.trend, rsi: Number(t15.rsi.toFixed(1)), ema20: t15.ema20, ema50: t15.ema50, vwap: t15.vwap, aboveVwap: t15.aboveVwap, macdHist: t15.macdHist, atrPct: Number(t15.atrPct.toFixed(2)), changeLookback: Number(t15.changeLookback.toFixed(2)), bb: bollinger(k15) },
      '5m': { trend: t5.trend, rsi: Number(t5.rsi.toFixed(1)), ema20: t5.ema20, ema50: t5.ema50, vwap: t5.vwap, aboveVwap: t5.aboveVwap, macdHist: t5.macdHist, atrPct: Number(t5.atrPct.toFixed(2)), change: Number(t5.change.toFixed(2)) },
    },
    binanceData: {
      funding: { ratePct: Number(fundingRate.toFixed(4)), raw: premium?.lastFundingRate || lastItem(funding)?.fundingRate || null },
      openInterest: { current: n(oiNow?.openInterest, 0), changePct15mWindow: Number(oiChg.toFixed(2)) },
      longShort: { globalAccountRatio: Number(globalLs.toFixed(3)), topPositionRatio: Number(topPos.toFixed(3)), topAccountRatio: Number(topAcc.toFixed(3)) },
      taker: { ratio5m: Number(taker5.toFixed(3)), ratio15m: Number(taker15.toFixed(3)), ratio1h: Number(taker1.toFixed(3)) },
      orderflow: { cvdQuote: Number(flow.cvdQuote.toFixed(2)), buyQuote: Number(flow.buyQuote.toFixed(2)), sellQuote: Number(flow.sellQuote.toFixed(2)), buyRatio: Number(flow.buyRatio.toFixed(3)), trades: flow.trades },
      orderbook: { imbalance20: Number(depth.imbalance20.toFixed(3)), imbalance50: Number(depth.imbalance50.toFixed(3)), spreadPct: Number(depth.spreadPct.toFixed(4)), bid20: Number(depth.bid20.toFixed(2)), ask20: Number(depth.ask20.toFixed(2)), bestBid: depth.bestBid, bestAsk: depth.bestAsk },
    },
    longScore: score.longScore,
    shortScore: score.shortScore,
    recommendation: score.recommendation,
    confidence: score.confidence,
    mmTrap: score.trap,
    reasons: score.reasons,
    warnings,
  };
}

// =============================================================================
// ROUTES
// =============================================================================
app.get('/', (req, res) => res.json({ ok: true, status: 'ok', version: VERSION, activeBinanceBase: activeBase(), binanceBases: BINANCE_BASES, time: new Date().toISOString() }));
app.get('/api/health', async (req, res) => {
  try {
    const st = await syncTime();
    res.json({ ok: true, version: VERSION, officialBinanceFuturesRestBase: 'https://fapi.binance.com', activeBinanceBase: activeBase(), binanceBases: BINANCE_BASES, localTime: Date.now(), binanceTimeApprox: st, timeOffsetMs });
  } catch (e) {
    res.status(502).json({ ok: false, error: errMsg(e), help: geoHelp(), officialBinanceFuturesRestBase: 'https://fapi.binance.com', version: VERSION, activeBinanceBase: activeBase(), binanceBases: BINANCE_BASES });
  }
});

app.get('/api/futures-coins', async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(i(req.query.limit, 120), 250));
    const [ex, tickers] = await Promise.all([
      getExchangeInfo(),
      publicGet('/fapi/v1/ticker/24hr', {}, 60 * 1000),
    ]);
    const tradable = new Set((ex.symbols || [])
      .filter(s => s.status === 'TRADING' && s.contractType === 'PERPETUAL' && s.quoteAsset === 'USDT' && s.marginAsset === 'USDT')
      .map(s => s.symbol));
    const coins = (Array.isArray(tickers) ? tickers : [])
      .filter(t => tradable.has(t.symbol))
      .map(t => {
        const change24h = n(t.priceChangePercent);
        const volume = n(t.quoteVolume);
        const score = Math.abs(change24h) * Math.log10(Math.max(volume, 10));
        return {
          symbol: t.symbol.replace('USDT',''),
          fullSymbol: t.symbol,
          price: n(t.lastPrice),
          change24h,
          volume,
          high: n(t.highPrice),
          low: n(t.lowPrice),
          trades: i(t.count),
          scanScore: Number(score.toFixed(2)),
          source: 'Binance USD-M Futures PERPETUAL/TRADING',
        };
      })
      .filter(c => c.volume >= 100000)
      .sort((a,b) => b.scanScore - a.scanScore)
      .slice(0, limit);
    res.json({ ok: true, version: VERSION, count: coins.length, coins });
  } catch (e) {
    res.status(502).json({ ok: false, error: errMsg(e), coins: [] });
  }
});

app.get('/api/analyze/:symbol', async (req, res) => {
  try {
    res.json(await analyzeOne(req.params.symbol));
  } catch (e) {
    res.status(502).json({ ok: false, error: errMsg(e), symbol: req.params.symbol });
  }
});

app.post('/api/analyze-batch', async (req, res) => {
  const symbols = Array.isArray(req.body?.symbols) ? req.body.symbols : [];
  const max = Math.max(1, Math.min(i(req.body?.max, 8), 12)); // 429 yememek için bilinçli limit
  const list = symbols.slice(0, max);
  const results = [];
  for (const s of list) {
    try { results.push(await analyzeOne(s)); }
    catch (e) { results.push({ ok: false, symbol: s, error: errMsg(e) }); }
    await sleep(120); // küçük fren: Binance 429 riskini azaltır
  }
  res.json({ ok: true, version: VERSION, count: results.length, maxPerRequest: max, results });
});

async function readAccount(apiKey, apiSecret) {
  const [balance, account, pos, config, mode] = await Promise.all([
    bReq(apiKey, apiSecret, 'GET', '/fapi/v3/balance'),
    bReq(apiKey, apiSecret, 'GET', '/fapi/v3/account'),
    bReq(apiKey, apiSecret, 'GET', '/fapi/v3/positionRisk'),
    bReq(apiKey, apiSecret, 'GET', '/fapi/v1/accountConfig').catch(e => ({ warning: errMsg(e) })),
    bReq(apiKey, apiSecret, 'GET', '/fapi/v1/positionSide/dual').catch(e => ({ dualSidePosition: false, warning: errMsg(e) })),
  ]);
  if (!Array.isArray(balance)) throw new Error('Balance array dönmedi; API yetkisini ve Futures iznini kontrol et.');
  const usdt = balance.find(b => b.asset === 'USDT') || {};
  const positions = Array.isArray(pos) ? pos.filter(p => Math.abs(n(p.positionAmt)) > 0).map(p => {
    const amt = n(p.positionAmt);
    const side = p.positionSide && p.positionSide !== 'BOTH' ? p.positionSide : (amt >= 0 ? 'LONG' : 'SHORT');
    const ep = n(p.entryPrice), mp = n(p.markPrice), lev = i(p.leverage, 1);
    const roi = ep ? ((mp - ep) / ep) * 100 * lev * (side === 'SHORT' ? -1 : 1) : 0;
    return {
      symbol: p.symbol,
      side,
      positionSide: p.positionSide || 'BOTH',
      positionAmt: Math.abs(amt),
      entryPrice: ep,
      markPrice: mp,
      breakEvenPrice: n(p.breakEvenPrice),
      unrealizedProfit: n(p.unRealizedProfit || p.unrealizedProfit),
      pnlPct: Number(roi.toFixed(2)),
      leverage: lev,
      liquidationPrice: n(p.liquidationPrice),
      notional: n(p.notional),
      isolatedMargin: n(p.isolatedMargin),
      updateTime: p.updateTime,
    };
  }) : [];
  return {
    ok: true,
    version: VERSION,
    totalWalletBalance: n(account.totalWalletBalance, n(usdt.balance)),
    availableBalance: n(account.availableBalance, n(usdt.availableBalance)),
    totalUnrealizedProfit: n(account.totalUnrealizedProfit, n(usdt.crossUnPnl)),
    maxWithdrawAmount: n(usdt.maxWithdrawAmount),
    canTrade: account.canTrade !== false,
    multiAssetsMargin: account.multiAssetsMargin,
    accountConfig: config,
    dualSidePosition: mode.dualSidePosition === true || mode.dualSidePosition === 'true',
    positions,
  };
}

app.post('/api/account', async (req, res) => {
  try {
    const { apiKey, apiSecret } = req.body || {};
    res.json(await readAccount(apiKey, apiSecret));
  } catch (e) {
    res.status(400).json({ ok: false, error: errMsg(e), totalWalletBalance: 0, availableBalance: 0, positions: [] });
  }
});

app.post('/api/leverage', async (req, res) => {
  try {
    const { apiKey, apiSecret, symbol, leverage } = req.body || {};
    const sym = normalizeSymbol(symbol);
    await getSymbolInfo(sym);
    const lev = Math.max(1, Math.min(i(leverage, 1), 125));
    const data = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/leverage', { symbol: sym, leverage: lev });
    res.json({ ok: true, symbol: sym, leverage: data.leverage || lev, version: VERSION });
  } catch (e) { res.status(400).json({ ok: false, error: errMsg(e) }); }
});

app.post('/api/margin-type', async (req, res) => {
  try {
    const { apiKey, apiSecret, symbol, marginType } = req.body || {};
    const sym = normalizeSymbol(symbol);
    const mt = String(marginType || 'ISOLATED').toUpperCase();
    if (!['ISOLATED','CROSSED','CROSS'].includes(mt)) throw new Error('marginType ISOLATED veya CROSSED olmalı');
    const binMt = mt === 'CROSS' ? 'CROSSED' : mt;
    try {
      await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/marginType', { symbol: sym, marginType: binMt });
    } catch (e) {
      if (!errMsg(e).includes('No need to change margin type') && !errMsg(e).includes('-4046')) throw e;
    }
    res.json({ ok: true, symbol: sym, marginType: binMt, version: VERSION });
  } catch (e) { res.status(400).json({ ok: false, error: errMsg(e) }); }
});

async function currentPosition(apiKey, apiSecret, sym) {
  const pos = await bReq(apiKey, apiSecret, 'GET', '/fapi/v3/positionRisk', { symbol: sym });
  const arr = Array.isArray(pos) ? pos : [];
  return arr.find(p => Math.abs(n(p.positionAmt)) > 0) || null;
}
async function cancelProtection(apiKey, apiSecret, sym) {
  const warnings = [];
  try { await bReq(apiKey, apiSecret, 'DELETE', '/fapi/v1/algoOpenOrders', { symbol: sym }); }
  catch (e) { warnings.push(`algoOpenOrders iptal: ${errMsg(e)}`); }
  try { await bReq(apiKey, apiSecret, 'DELETE', '/fapi/v1/allOpenOrders', { symbol: sym }); }
  catch (e) { warnings.push(`openOrders iptal: ${errMsg(e)}`); }
  return warnings;
}
async function closePositionMarket(apiKey, apiSecret, sym, pos, dualSidePosition) {
  if (!pos) return null;
  const amt = n(pos.positionAmt);
  if (Math.abs(amt) <= 0) return null;
  const closeSide = amt > 0 ? 'SELL' : 'BUY';
  const sidePos = pos.positionSide && pos.positionSide !== 'BOTH' ? pos.positionSide : (amt > 0 ? 'LONG' : 'SHORT');
  const si = await getSymbolInfo(sym);
  const rules = parseRules(si);
  const qty = floorToStep(Math.abs(amt), rules.stepSize);
  const params = {
    symbol: sym,
    side: closeSide,
    type: 'MARKET',
    quantity: fmtByStep(qty, rules.stepSize),
    newOrderRespType: 'RESULT',
    positionSide: dualSidePosition ? sidePos : 'BOTH',
  };
  if (!dualSidePosition) params.reduceOnly = 'true';
  return bReq(apiKey, apiSecret, 'POST', '/fapi/v1/order', params);
}
async function placeAlgoProtection(apiKey, apiSecret, sym, side, triggerPrice, dualSidePosition, type) {
  const { closeSide } = sideToOrderSide(side);
  const params = {
    algoType: 'CONDITIONAL',
    symbol: sym,
    side: closeSide,
    type,
    triggerPrice,
    closePosition: 'true',
    workingType: 'MARK_PRICE',
    priceProtect: 'false',
    positionSide: dualSidePosition ? String(side).toUpperCase() : 'BOTH',
  };
  return bReq(apiKey, apiSecret, 'POST', '/fapi/v1/algoOrder', params);
}

app.post('/api/order', async (req, res) => {
  const body = req.body || {};
  const { apiKey, apiSecret } = body;
  let sym = 'UNKNOWN';
  let openedPosition = null;
  let dualSidePosition = false;
  try {
    sym = normalizeSymbol(body.symbol);
    const side = String(body.side || '').toUpperCase();
    const { isLong, openSide } = sideToOrderSide(side);
    const leverage = Math.max(1, Math.min(i(body.leverage, 1), 125));
    const marginType = String(body.marginType || 'ISOLATED').toUpperCase() === 'CROSS' ? 'CROSSED' : String(body.marginType || 'ISOLATED').toUpperCase();
    const usdtAmount = n(body.usdtAmount);
    let targetPrice = n(body.targetPrice);
    let stopPrice = n(body.stopPrice);
    if (!apiKey || !apiSecret) throw new Error('API key ve secret gerekli');
    if (usdtAmount <= 0) throw new Error('USDT miktarı 0 olamaz');
    if (!targetPrice || !stopPrice) throw new Error('TP ve SL fiyatı zorunlu; korumasız emir açılmadı');

    const si = await getSymbolInfo(sym);
    const rules = parseRules(si);
    const account = await readAccount(apiKey, apiSecret);
    dualSidePosition = account.dualSidePosition;
    if (!account.canTrade) throw new Error('Binance hesabı canTrade=false döndü; Futures işlemi kapalı olabilir.');
    if (account.availableBalance + 1e-9 < usdtAmount) {
      throw new Error(`Yetersiz Futures USDT bakiye. Kullanılabilir=${account.availableBalance}, istenen marj=${usdtAmount}`);
    }

    try {
      if (marginType === 'ISOLATED' || marginType === 'CROSSED') {
        await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/marginType', { symbol: sym, marginType });
      }
    } catch (e) {
      if (!errMsg(e).includes('No need to change margin type') && !errMsg(e).includes('-4046')) throw e;
    }
    await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/leverage', { symbol: sym, leverage });

    const premium = await publicGet('/fapi/v1/premiumIndex', { symbol: sym }, 8 * 1000).catch(() => null);
    const ticker = await publicGet('/fapi/v1/ticker/price', { symbol: sym }, 5 * 1000).catch(() => null);
    const price = n(premium?.markPrice, n(ticker?.price, n(body.entryPrice)));
    if (!price || price <= 0) throw new Error(`${sym} anlık fiyat alınamadı; emir açılmadı.`);

    let qty = floorToStep((usdtAmount * leverage) / price, rules.stepSize);
    if (qty < rules.minQty) throw new Error(`Miktar minQty altında. qty=${qty}, minQty=${rules.minQty}`);
    if (qty > rules.maxQty) throw new Error(`Miktar maxQty üstünde. qty=${qty}, maxQty=${rules.maxQty}`);
    if (qty * price < rules.minNotional) throw new Error(`Minimum notional altında. ${Number((qty * price).toFixed(4))} < ${rules.minNotional}`);

    targetPrice = roundToTick(targetPrice, rules.tickSize);
    stopPrice = roundToTick(stopPrice, rules.tickSize);
    if (isLong && !(targetPrice > price && stopPrice < price)) throw new Error(`LONG için TP fiyatın üstünde, SL fiyatın altında olmalı. price=${price}, TP=${targetPrice}, SL=${stopPrice}`);
    if (!isLong && !(targetPrice < price && stopPrice > price)) throw new Error(`SHORT için TP fiyatın altında, SL fiyatın üstünde olmalı. price=${price}, TP=${targetPrice}, SL=${stopPrice}`);

    const clientId = `mf_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const mainParams = {
      symbol: sym,
      side: openSide,
      type: 'MARKET',
      quantity: fmtByStep(qty, rules.stepSize),
      newOrderRespType: 'RESULT',
      newClientOrderId: clientId,
      positionSide: dualSidePosition ? side : 'BOTH',
    };
    const mainOrder = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/order', mainParams);
    if (mainOrder.status && mainOrder.status !== 'FILLED') {
      throw new Error(`MARKET emir FILLED dönmedi: status=${mainOrder.status}`);
    }

    await sleep(700);
    openedPosition = await currentPosition(apiKey, apiSecret, sym);
    if (!openedPosition) {
      throw new Error(`Binance positionRisk içinde pozisyon görünmedi. Uygulama içi sahte pozisyon yazılmadı. mainOrderId=${mainOrder.orderId}`);
    }

    let tpOrder = null, slOrder = null;
    try {
      tpOrder = await placeAlgoProtection(apiKey, apiSecret, sym, side, fmtByStep(targetPrice, rules.tickSize), dualSidePosition, 'TAKE_PROFIT_MARKET');
      slOrder = await placeAlgoProtection(apiKey, apiSecret, sym, side, fmtByStep(stopPrice, rules.tickSize), dualSidePosition, 'STOP_MARKET');
    } catch (protectErr) {
      const cancelWarnings = await cancelProtection(apiKey, apiSecret, sym);
      const posNow = await currentPosition(apiKey, apiSecret, sym);
      const closeOrder = await closePositionMarket(apiKey, apiSecret, sym, posNow, dualSidePosition).catch(e => ({ error: errMsg(e) }));
      return res.status(409).json({
        ok: false,
        rolledBack: true,
        error: `TP/SL koruması kurulamadı; pozisyon güvenlik için kapatıldı: ${errMsg(protectErr)}`,
        mainOrderId: mainOrder.orderId,
        closeOrder,
        cancelWarnings,
        version: VERSION,
      });
    }

    await sleep(500);
    const posVerify = await currentPosition(apiKey, apiSecret, sym);
    if (!posVerify) throw new Error('TP/SL sonrası pozisyon doğrulama başarısız');

    res.json({
      ok: true,
      verified: true,
      version: VERSION,
      message: `${sym} ${side} MARKET emir Binance'da doğrulandı ve TP/SL algo koruması kuruldu`,
      symbol: sym,
      side,
      mainOrderId: mainOrder.orderId,
      clientOrderId: clientId,
      tpAlgoOrderId: tpOrder.algoId || tpOrder.orderId || tpOrder.clientAlgoId || null,
      slAlgoOrderId: slOrder.algoId || slOrder.orderId || slOrder.clientAlgoId || null,
      executedQty: n(mainOrder.executedQty, qty),
      avgPrice: n(mainOrder.avgPrice, price),
      position: {
        positionAmt: Math.abs(n(posVerify.positionAmt)),
        entryPrice: n(posVerify.entryPrice),
        markPrice: n(posVerify.markPrice),
        unRealizedProfit: n(posVerify.unRealizedProfit),
        leverage: i(posVerify.leverage, leverage),
        positionSide: posVerify.positionSide,
      },
      protection: { targetPrice, stopPrice, workingType: 'MARK_PRICE', endpoint: '/fapi/v1/algoOrder' },
    });
  } catch (e) {
    // Ana emir açıldıysa ama cevap üretirken hata çıktıysa pozisyonu açık bırakmamak için son güvenlik denemesi.
    let emergency = null;
    try {
      if (apiKey && apiSecret && sym !== 'UNKNOWN') {
        const posNow = await currentPosition(apiKey, apiSecret, sym).catch(() => null);
        if (posNow && !openedPosition) {
          await cancelProtection(apiKey, apiSecret, sym);
          emergency = await closePositionMarket(apiKey, apiSecret, sym, posNow, dualSidePosition).catch(x => ({ error: errMsg(x) }));
        }
      }
    } catch (_) {}
    res.status(400).json({ ok: false, error: errMsg(e), emergencyClose: emergency, version: VERSION });
  }
});

app.post('/api/positions', async (req, res) => {
  try {
    const account = await readAccount(req.body?.apiKey, req.body?.apiSecret);
    res.json({ ok: true, version: VERSION, positions: account.positions, dualSidePosition: account.dualSidePosition });
  } catch (e) { res.status(400).json({ ok: false, error: errMsg(e), positions: [] }); }
});

app.post('/api/open-orders', async (req, res) => {
  try {
    const { apiKey, apiSecret, symbol } = req.body || {};
    const sym = symbol ? normalizeSymbol(symbol) : undefined;
    const normal = sym ? await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/openOrders', { symbol: sym }) : [];
    const algo = sym ? await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/openAlgoOrders', { symbol: sym }) : [];
    res.json({ ok: true, version: VERSION, symbol: sym, normal, algo });
  } catch (e) { res.status(400).json({ ok: false, error: errMsg(e) }); }
});

app.post('/api/close', async (req, res) => {
  try {
    const { apiKey, apiSecret, symbol } = req.body || {};
    const sym = normalizeSymbol(symbol);
    const account = await readAccount(apiKey, apiSecret);
    const cancelWarnings = await cancelProtection(apiKey, apiSecret, sym);
    const pos = await currentPosition(apiKey, apiSecret, sym);
    if (!pos) return res.json({ ok: true, version: VERSION, message: 'Açık pozisyon yok', cancelWarnings });
    const order = await closePositionMarket(apiKey, apiSecret, sym, pos, account.dualSidePosition);
    await sleep(700);
    const after = await currentPosition(apiKey, apiSecret, sym);
    res.json({ ok: true, version: VERSION, message: `${sym} kapatma emri gönderildi`, orderId: order?.orderId, stillOpen: !!after, cancelWarnings });
  } catch (e) { res.status(400).json({ ok: false, error: errMsg(e) }); }
});

app.listen(PORT, () => console.log(`✅ ${VERSION} ${PORT} portunda çalışıyor`));
