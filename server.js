const express = require('express');
const cors    = require('cors');
const crypto  = require('crypto');
const fetch   = globalThis.fetch;
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
const PORT = process.env.PORT || 3000;
const FAPI = 'https://fapi.binance.com';
const FAPI_WS = 'wss://fstream.binance.com/stream';
const FAPI_WS_PUBLIC = 'wss://fstream.binance.com/public';
const FAPI_WS_MARKET = 'wss://fstream.binance.com/market';

const AUTO_STATS_PATH = path.join(process.cwd(), 'lazarus_auto_stats.json');
function loadAutoStats() {
  try {
    const raw = fs.readFileSync(AUTO_STATS_PATH, 'utf8');
    const obj = JSON.parse(raw);
    return {
      totalOpenedAllTime: Number(obj.totalOpenedAllTime || 0),
      sessionOpened: Number(obj.sessionOpened || 0),
      lastOpenedAt: Number(obj.lastOpenedAt || 0),
      lastOpenedSymbol: obj.lastOpenedSymbol || null,
      lastOpenedSide: obj.lastOpenedSide || null,
    };
  } catch (_) {
    return { totalOpenedAllTime:0, sessionOpened:0, lastOpenedAt:0, lastOpenedSymbol:null, lastOpenedSide:null };
  }
}
function saveAutoStats(obj) {
  try { fs.writeFileSync(AUTO_STATS_PATH, JSON.stringify(obj, null, 2)); } catch (_) {}
}
const autoPersistentStats = loadAutoStats();

const criticalEvents = [];
function safeErrMsg(err) {
  const raw = (err && (err.message || String(err))) || 'Bilinmeyen hata';
  return String(raw)
    .replace(/signature=[a-f0-9]+/ig, 'signature=***')
    .replace(/apiSecret[^,&\s]*/ig, 'apiSecret=***')
    .slice(0, 240);
}
function pushCritical(scope, err, meta = {}, level = 'CRITICAL') {
  const ev = {
    ts: Date.now(),
    level,
    scope: String(scope || 'GENEL').slice(0, 60),
    message: safeErrMsg(err),
    meta,
  };
  criticalEvents.push(ev);
  while (criticalEvents.length > 80) criticalEvents.shift();
  try { console.error(`[${level}] ${ev.scope}: ${ev.message}`, meta || ''); } catch (_) {}
  return ev;
}
process.on('uncaughtException', e => pushCritical('UNCAUGHT_EXCEPTION', e));
process.on('unhandledRejection', e => pushCritical('UNHANDLED_REJECTION', e));

const cache = new Map();
async function cached(key, ttl, fn) {
  const now = Date.now();
  if (cache.has(key)) { const {val,exp}=cache.get(key); if(now<exp)return val; }
  const val = await fn();
  cache.set(key, { val, exp: now+ttl });
  return val;
}

const LAZARUS_BUILD = 'R369E_DONUS_TEYIDI'
const R150_MIN_SCAN_GAP_MS = 8 * 1000;
let r150LastScanBeginTs = 0;

const binanceGov = {
  q: Promise.resolve(),
  minuteStart: Date.now(),
  usedWeight: 0,
  usedOrders: 0,
  backoffUntil: 0,
  last429At: 0,
};
const sleep = (ms) => new Promise(r => setTimeout(r, Math.max(0, Number(ms)||0)));
function _resetGovWindowIfNeeded() {
  const now = Date.now();
  if (now - binanceGov.minuteStart >= 60_000) {
    binanceGov.minuteStart = now;
    binanceGov.usedWeight = 0;
    binanceGov.usedOrders = 0;
  }
}
async function binanceThrottle(scope='REST', weight=1, orderWeight=0) {
  const job = async () => {
    _resetGovWindowIfNeeded();
    const now = Date.now();
    if (binanceGov.backoffUntil > now && !String(scope).includes('EMERGENCY')) await sleep(binanceGov.backoffUntil - now + 50);
    _resetGovWindowIfNeeded();
    if (binanceGov.usedWeight + weight > 1800 || binanceGov.usedOrders + orderWeight > 70) {
      const wait = 60_000 - (Date.now() - binanceGov.minuteStart) + 250;
      await sleep(wait);
      _resetGovWindowIfNeeded();
    }
    binanceGov.usedWeight += weight;
    binanceGov.usedOrders += orderWeight;
    const baseDelay = orderWeight ? 120 : (String(scope).includes('PUBLIC') ? 90 : 70);
    await sleep(baseDelay);
  };
  const prev = binanceGov.q.catch(()=>{});
  binanceGov.q = prev.then(job, job);
  return binanceGov.q;
}
function registerBinanceBackoff(reason='rate-limit', seconds=45) {
  const sec = Math.max(5, Math.min(180, Number(seconds)||45));
  binanceGov.backoffUntil = Math.max(binanceGov.backoffUntil || 0, Date.now() + sec*1000);
  binanceGov.last429At = Date.now();
  try { pushCritical('BINANCE_BACKOFF', `${reason}: ${sec}sn istek bekleme`, {seconds:sec, reason}, 'WARNING'); } catch(_) {}
}
function isBinanceBackoffActive() {
  return Date.now() < Number(binanceGov.backoffUntil || 0);
}
function getBinanceBackoffMs() {
  return Math.max(0, Number(binanceGov.backoffUntil || 0) - Date.now());
}
function makeBinanceBackoffError(reason='Binance istek freni', seconds=45, status=null) {
  const e = new Error(`${reason}: ${Math.ceil(Number(seconds)||0)}sn merkezi istek freni`);
  e.code = 'BINANCE_BACKOFF_ACTIVE';
  e.status = status;
  e.retryAfter = Number(seconds)||0;
  return e;
}
function registerHttpBackoffAndThrow(scope, status, retryHeader) {
  const retry = parseInt(retryHeader || (Number(status) === 418 ? '60' : '60'), 10);
  const sec = Math.max(Number(status) === 418 ? 60 : 30, Math.min(120, Number(retry)||60));
  registerBinanceBackoff(`HTTP ${status} ${scope}`, sec);
  throw makeBinanceBackoffError(`HTTP ${status} ${scope}`, sec, status);
}

let reqCount=0, reqWindow=Date.now();

async function bAlgo(apiKey, apiSecret, params, _retry=false) {
  await binanceThrottle('ALGO_ORDER', 2, 1);
  if (!lastTimeSync) await syncBinanceTime(false);
  const ts = Date.now() + binanceTimeOffset;
  const obj = { ...params, timestamp: ts, recvWindow: 10000 };
  const fullQs = signedQueryString(obj, apiSecret);
  const url = `${FAPI}/fapi/v1/algoOrder?${fullQs}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'X-MBX-APIKEY': String(apiKey || '').trim() },
    signal: AbortSignal.timeout(10000),
  });
  if (res.status === 429 || res.status === 418) {
    registerHttpBackoffAndThrow('algoOrder', res.status, res.headers.get('Retry-After'));
  }
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); }
  catch(e) { throw new Error(`Algo JSON hatası: ${text.substring(0,100)}`); }
  if (data.code && data.code < 0) {
    if (Number(data.code) === -1021 && !_retry) {
      await syncBinanceTime(true);
      return bAlgo(apiKey, apiSecret, params, true);
    }
    if (Number(data.code) === -1003) registerBinanceBackoff('Binance -1003 algoOrder', 60);
    throw new Error(formatBinanceError('/fapi/v1/algoOrder', data));
  }
  return data;
}

async function cancelAlgoOrders(apiKey, apiSecret, symbol, emergency=false) {
  const em = emergency ? {__emergency:true} : {};
  try { await bReq(apiKey, apiSecret, 'DELETE', '/fapi/v1/allOpenOrders', {symbol, ...em}); } catch(e) {}
  try { await bReq(apiKey, apiSecret, 'DELETE', '/fapi/v1/algoOpenOrders', {symbol, ...em}); } catch(e) {}
}

async function buildAlgoCloseParams(symbol, closeSide, orderType, triggerPrice, clientAlgoId) {
  return {
    algoType: 'CONDITIONAL',
    symbol,
    side: closeSide,
    type: orderType,
    triggerPrice: triggerPrice.toString(),
    closePosition: 'true',
    workingType: 'MARK_PRICE',
    priceProtect: 'false',
    clientAlgoId: clientAlgoId || `SL_${symbol}_${Date.now()}`,
  };
}

async function placeAlgoSL(apiKey, apiSecret, symbol, closeSide, triggerPrice, _unused) {
  const params = await buildAlgoCloseParams(symbol, closeSide, 'STOP_MARKET', triggerPrice, `SL_${symbol}_${Date.now()}`);
  return bAlgo(apiKey, apiSecret, params);
}

async function placeAlgoTP(apiKey, apiSecret, symbol, closeSide, triggerPrice, _unused) {
  const params = await buildAlgoCloseParams(symbol, closeSide, 'TAKE_PROFIT_MARKET', triggerPrice, `TP_${symbol}_${Date.now()}`);
  return bAlgo(apiKey, apiSecret, params);
}

const bracketOrdersCache = new Map();
function bracketCacheKey(symbol) { return normalizeSymbol(symbol || '').toUpperCase(); }
function getBracketOrdersCached(symbol, ttlMs=45_000) {
  const k = bracketCacheKey(symbol);
  const c = bracketOrdersCache.get(k);
  if (c && Date.now() - Number(c.ts || 0) <= ttlMs) return Array.isArray(c.orders) ? c.orders : [];
  return null;
}
function setBracketOrdersCached(symbol, orders) {
  bracketOrdersCache.set(bracketCacheKey(symbol), { ts: Date.now(), orders: Array.isArray(orders) ? orders : [] });
}

async function liveOpenAlgoOrders(apiKey, apiSecret, symbol) {
  try {
    const rows = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/openAlgoOrders', {symbol});
    return Array.isArray(rows) ? rows : [];
  } catch(e) {
    console.log(`${symbol} openAlgoOrders okunamadı: ${e.message}`);
    return [];
  }
}

async function liveOpenStandardOrders(apiKey, apiSecret, symbol) {
  try {
    const rows = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/openOrders', {symbol});
    return Array.isArray(rows) ? rows : [];
  } catch(e) {
    console.log(`${symbol} openOrders okunamadı: ${e.message}`);
    return [];
  }
}

async function liveOpenBracketOrders(apiKey, apiSecret, symbol, opts={}) {
  const ttlMs = Number(opts.ttlMs ?? 45_000);
  const force = !!opts.force;
  if (!force) {
    const cachedOrders = getBracketOrdersCached(symbol, ttlMs);
    if (cachedOrders) return cachedOrders;
    if (isBinanceBackoffActive()) return [];
  }
  const algo = await liveOpenAlgoOrders(apiKey, apiSecret, symbol);
  await sleep(120);
  const standard = await liveOpenStandardOrders(apiKey, apiSecret, symbol);
  const all = [...algo, ...standard];
  setBracketOrdersCached(symbol, all);
  return all;
}

function orderKind(o) {
  const typ = String(o.orderType || o.type || '').toUpperCase();
  const cid = String(o.clientAlgoId || o.clientOrderId || o.origClientOrderId || '').toUpperCase();
  if (typ.includes('TAKE_PROFIT') || cid.startsWith('TP_') || cid.startsWith('TPSTD_')) return 'TP';
  if (typ.includes('STOP') && !typ.includes('TAKE')) return 'SL';
  return '';
}

function orderTriggerPrice(o) {
  for (const k of ['triggerPrice','stopPrice','activatePrice','price']) {
    const v = parseFloat(o?.[k]);
    if (Number.isFinite(v) && v > 0) return v;
  }
  return 0;
}

function priceCloseEnough(expected, actual) {
  expected = parseFloat(expected); actual = parseFloat(actual);
  if (!Number.isFinite(expected) || !Number.isFinite(actual) || expected <= 0 || actual <= 0) return false;
  return Math.abs(expected - actual) / expected < 0.005;
}

async function verifyAlgoSLTPVisible(apiKey, apiSecret, symbol, expectedSL, expectedTP) {
  try {
    const orders = await liveOpenBracketOrders(apiKey, apiSecret, symbol, {force:true, ttlMs:0});
    let foundSL = false, foundTP = false;
    const preview = [];
    for (const o of orders) {
      const kind = orderKind(o);
      const trig = orderTriggerPrice(o);
      preview.push({kind, type:o.orderType||o.type, trigger:trig, client:o.clientAlgoId||o.clientOrderId||o.origClientOrderId});
      if (kind === 'SL' && priceCloseEnough(expectedSL, trig)) foundSL = true;
      if (kind === 'TP' && priceCloseEnough(expectedTP, trig)) foundTP = true;
    }
    return { ok: foundSL && foundTP, foundSL, foundTP, orderCount: orders.length, ordersPreview: preview.slice(0,8) };
  } catch(e) {
    return { ok: false, error: e.message };
  }
}

const symbolFilterCache = new Map();
function decimalPlacesFromStep(step) {
  const raw = String(step || '').trim();
  if (!raw || raw === '0') return 8;
  if (raw.includes('e-')) return parseInt(raw.split('e-')[1], 10) || 8;
  const dot = raw.indexOf('.');
  if (dot < 0) return 0;
  return raw.slice(dot + 1).replace(/0+$/, '').length;
}
function roundToStepValue(value, step) {
  value = Number(value); step = Number(step);
  if (!Number.isFinite(value) || value <= 0 || !Number.isFinite(step) || step <= 0) return value;
  const dec = decimalPlacesFromStep(step);
  const rounded = Math.round(value / step) * step;
  return Number(rounded.toFixed(Math.min(12, Math.max(0, dec))));
}
function formatStepValue(value, step) {
  const dec = decimalPlacesFromStep(step);
  const n = roundToStepValue(value, step);
  return Number(n).toFixed(Math.max(0, dec));
}
async function getSymbolFilters(symbol) {
  const sym = normalizeSymbol ? normalizeSymbol(symbol) : String(symbol || '').toUpperCase();
  const cached = symbolFilterCache.get(sym);
  if (cached && Date.now() - cached.ts < 6 * 60 * 60 * 1000) return cached.val;
  let val = { tickSize: 0.00000001, stepSize: 0.001, pricePrecision: 8, qtyPrecision: 3 };
  try {
    const si = await bPub('/fapi/v1/exchangeInfo', 'symbol=' + encodeURIComponent(sym));
    const info = Array.isArray(si.symbols) ? si.symbols.find(x => x.symbol === sym) : null;
    if (info) {
      const pf = info.filters?.find(f => f.filterType === 'PRICE_FILTER');
      const lf = info.filters?.find(f => f.filterType === 'LOT_SIZE');
      if (pf && Number(pf.tickSize) > 0) val.tickSize = Number(pf.tickSize);
      if (lf && Number(lf.stepSize) > 0) val.stepSize = Number(lf.stepSize);
      if (Number.isFinite(Number(info.pricePrecision))) val.pricePrecision = Number(info.pricePrecision);
      if (Number.isFinite(Number(info.quantityPrecision))) val.qtyPrecision = Number(info.quantityPrecision);
    }
  } catch(e) {
    pushCritical('EXCHANGE_INFO_FILTER', `${sym}: ${e.message}`, {}, 'WARN');
  }
  symbolFilterCache.set(sym, { ts: Date.now(), val });
  return val;
}

const leverageBracketCache = new Map();
async function getSymbolMaxInitialLeverage(apiKey, apiSecret, symbol, targetNotional=0) {
  const sym = normalizeSymbol(symbol);
  const key = `${keyFingerprint(apiKey)}:${sym}:${Math.round(Number(targetNotional||0)/50)*50}`;
  const now = Date.now();
  const cached = leverageBracketCache.get(key);
  if (cached && now - cached.ts < 10 * 60 * 1000) return cached.value;
  try {
    const raw = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/leverageBracket', { symbol:sym }, 10000);
    const row = Array.isArray(raw) ? (raw.find(x => String(x.symbol).toUpperCase() === sym) || raw[0]) : raw;
    const brackets = Array.isArray(row?.brackets) ? row.brackets : [];
    let best = 0;
    const want = Math.max(0, Number(targetNotional || 0));
    for (const b of brackets) {
      const lev = Number(b.initialLeverage || b.initialLeverageCap || b.maxLeverage || 0);
      const cap = Number(b.notionalCap || b.qtyCap || Infinity);
      const floor = Number(b.notionalFloor || 0);
      if (!Number.isFinite(lev) || lev <= 0) continue;
      if (want > 0 && Number.isFinite(cap) && (want < floor || want > cap)) continue;
      best = Math.max(best, lev);
    }
    if (!best && brackets.length) best = Math.max(...brackets.map(b=>Number(b.initialLeverage||0)).filter(n=>n>0));
    const value = Number.isFinite(best) && best > 0 ? Math.floor(best) : null;
    leverageBracketCache.set(key, {ts:now, value});
    return value;
  } catch(e) {
    return null;
  }
}

function normalizeRequestedLeverage(v, fallback=1) {
  const n = Math.floor(Number(v || fallback));
  return Math.max(1, Math.min(125, Number.isFinite(n) ? n : fallback));
}

async function setSymbolLeverageSafe(apiKey, apiSecret, symbol, requestedLeverage, targetNotional=0) {
  const sym = normalizeSymbol(symbol);
  const requested = normalizeRequestedLeverage(requestedLeverage, 1);
  const maxLev = await getSymbolMaxInitialLeverage(apiKey, apiSecret, sym, targetNotional).catch(()=>null);
  const first = maxLev ? Math.min(requested, normalizeRequestedLeverage(maxLev, requested)) : requested;
  const ladder = [125,100,75,50,40,30,25,20,15,12,10,8,7,6,5,4,3,2,1];
  const candidates = [];
  const add = (x) => {
    const n = normalizeRequestedLeverage(x, 1);
    if (maxLev && n > maxLev) return;
    if (n > requested && !maxLev) return;
    if (!candidates.includes(n)) candidates.push(n);
  };
  add(first);
  for (const x of ladder) add(Math.min(x, first));
  add(1);

  let lastErr = null;
  for (const lev of candidates) {
    try {
      await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/leverage', {symbol:sym, leverage:lev});
      return {
        ok:true,
        leverage:lev,
        requested,
        maxLeverage:maxLev || null,
        adjusted:lev !== requested,
        reason: lev !== requested
          ? `Binance ${sym} için ${requested}x kabul etmedi/izin vermedi; ${lev}x ile devam edildi${maxLev ? ` (izinli max ${maxLev}x)` : ''}`
          : ''
      };
    } catch(e) {
      lastErr = e;
      const msg = String(e?.message || '');
      if (!(msg.includes('-4028') || msg.toLowerCase().includes('leverage') || msg.toLowerCase().includes('not valid'))) {
        throw e;
      }
    }
  }
  throw lastErr || new Error(`${sym}: kaldıraç ayarlanamadı`);
}

async function normalizeSLTPToTick(symbol, slPrice, tpPrice) {
  const filters = await getSymbolFilters(symbol);
  const tickSize = Number(filters.tickSize) || 0.00000001;
  const sl = formatStepValue(slPrice, tickSize);
  const tp = formatStepValue(tpPrice, tickSize);
  return { sl, tp, slNum: Number(sl), tpNum: Number(tp), tickSize };
}

function isNoOpenPositionAlgoError(e) {
  const msg = String(e && (e.message || e) || '');
  return msg.includes('-4509') ||
    msg.includes('GTE can only be used with open positions') ||
    msg.includes('Please ensure that positions are available');
}
async function freshOpenPositionForSymbol(apiKey, apiSecret, symbol, attempts=3) {
  const sym = String(symbol||'').toUpperCase();
  for (let i=0; i<attempts; i++) {
    try {
      const rows = await getPositionRisk(apiKey, apiSecret, {symbol:sym});
      const p = Array.isArray(rows) ? rows.find(x=>String(x.symbol||'').toUpperCase()===sym) : null;
      if (p && Math.abs(parseFloat(p.positionAmt || 0)) > 0) return { open:true, pos:p };
    } catch(e) {
      if (i === attempts-1) return { open:null, error:e };
    }
    if (i < attempts-1) await new Promise(r=>setTimeout(r, i===0 ? 450 : 850));
  }
  return { open:false, pos:null };
}
async function cleanupClosedPositionState(symbol, reason='POSITION_ALREADY_CLOSED', state={}) {
  const sym = String(symbol||'').toUpperCase();
  const st = state || (typeof trailingState !== 'undefined' ? trailingState.get(sym) : null) || (lastKnownPositions && lastKnownPositions[sym]) || {};
  try { trailingState.delete(sym); } catch(_) {}
  try { invalidatePositionRiskCache(reason); } catch(_) {}
  try {
    const cls = { code:'EXTERNAL_OR_MANUAL', label:'Kullanıcı/Binance kapanışı algılandı', emoji:'👁️', closePrice:null, realizedPnl:null, side:st?.side };
    const cdMs = setCloseCooldown(sym, cls, st);
    cls.cooldownMs = cdMs;
    try { recordTradeClose(sym, st, cls); } catch(_) {}
    try { forgetKnownPosition(sym); saveLastKnownPositions(); } catch(_) {}
    logAuto(`👁️ ${sym.replace('USDT','')} pozisyon zaten kapalı göründü; koruma emri yazılmadı. Aynı yön ${Math.ceil(cdMs/60000)}dk beklemede, temiz ters yön açık.`);
  } catch(e) {
    try { setCooldown(sym, CD_MANUAL_MS, 'Pozisyon kapalı algılandı; aynı yön bekleme'); } catch(_) {}
    try { logAuto(`⚠️ ${sym.replace('USDT','')} pozisyon kapanmış — koruma emri atlandı (${reason})`); } catch(_) {}
  }
}

async function installSLTPWithProof(apiKey, apiSecret, symbol, closeSide, slPrice, tpPrice, sym) {
  const freshStart = await freshOpenPositionForSymbol(apiKey, apiSecret, symbol, 3);
  if (freshStart.open === false) {
    await cleanupClosedPositionState(symbol, 'FRESH_POSITION_ZERO_BEFORE_SLTP');
    return { ok:false, skipped:true, skippedClosed:true, error:'POSITION_ALREADY_CLOSED', endpoint:'algoOrder' };
  }
  if (freshStart.open === null) {
    console.log(`${symbol} fresh positionRisk kontrolü yapılamadı; SL/TP yazımı deneniyor: ${freshStart.error?.message || freshStart.error}`);
  }

  const normalized = await normalizeSLTPToTick(symbol, slPrice, tpPrice);
  slPrice = normalized.sl;
  tpPrice = normalized.tp;

  try {
    let _mark = Number(freshStart && freshStart.pos && freshStart.pos.markPrice) || 0;
    if (!(_mark > 0)) {
      try { const _pr = await bPub('/fapi/v1/ticker/price', 'symbol=' + symbol); _mark = Number(_pr && _pr.price) || 0; } catch(_) {}
    }
    if (_mark > 0) {
      const _tick = Number((normalized && normalized.tickSize) || 0) || 0;
      const _minGap = Math.max(_tick > 0 ? _tick * 3 : 0, _mark * 0.0004);
      const _isLong = String(closeSide).toUpperCase() === 'SELL';
      const _sl = Number(slPrice), _tp = Number(tpPrice);
      if (_isLong) {
        if (_sl > 0 && _sl > _mark - _minGap) slPrice = (_mark - _minGap);
        if (_tp > 0 && _tp < _mark + _minGap) tpPrice = (_mark + _minGap);
      } else {
        if (_sl > 0 && _sl < _mark + _minGap) slPrice = (_mark + _minGap);
        if (_tp > 0 && _tp > _mark - _minGap) tpPrice = (_mark - _minGap);
      }
      const _renorm = await normalizeSLTPToTick(symbol, slPrice, tpPrice);
      slPrice = _renorm.sl; tpPrice = _renorm.tp;
    }
  } catch(_) { /* fallback */ }

  let lastErr = null;
  let lastProof = null;

  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      await cancelAlgoOrders(apiKey, apiSecret, symbol);

      const slOrder = await placeAlgoSL(apiKey, apiSecret, symbol, closeSide, slPrice, null);
      const tpOrder = await placeAlgoTP(apiKey, apiSecret, symbol, closeSide, tpPrice, null);

      for (const waitMs of [450, 900]) {
        await new Promise(r => setTimeout(r, waitMs));
        const proof = await verifyAlgoSLTPVisible(apiKey, apiSecret, symbol, normalized.slNum, normalized.tpNum);
        lastProof = proof;
        if (proof.ok) {
          return {
            ok: true, slOrder, tpOrder, proof,
            slPrice: normalized.slNum, tpPrice: normalized.tpNum, tickSize: normalized.tickSize,
            endpoint: 'algoOrder', attempt
          };
        }
      }

      lastErr = new Error(`Algo SL/TP proof başarısız (deneme ${attempt}); SL:${lastProof?.foundSL || false} TP:${lastProof?.foundTP || false} emir:${lastProof?.orderCount || 0}`);
    } catch(e) {
      lastErr = e;
      if (isNoOpenPositionAlgoError(e)) {
        await cleanupClosedPositionState(symbol, 'ALGO_-4509_POSITION_ALREADY_CLOSED');
        return { ok:false, skipped:true, skippedClosed:true, error:'POSITION_ALREADY_CLOSED', endpoint:'algoOrder' };
      }
      if (attempt === 1) await new Promise(r => setTimeout(r, 700));
    }
  }

  const msg = lastErr?.message || 'Algo SL/TP proof başarısız';
  if (isNoOpenPositionAlgoError(lastErr)) {
    await cleanupClosedPositionState(symbol, 'ALGO_-4509_POSITION_ALREADY_CLOSED');
    return { ok:false, skipped:true, skippedClosed:true, error:'POSITION_ALREADY_CLOSED', endpoint:'algoOrder' };
  }
  pushCritical('SLTP_ALGO_PROOF_FAIL', `${symbol}: ${msg}`, {
    tickSize: normalized?.tickSize, slPrice, tpPrice, proof: lastProof, endpoint: 'algoOrder'
  });
  return {
    ok: false, error: msg, proof: lastProof,
    slPrice: normalized?.slNum, tpPrice: normalized?.tpNum, tickSize: normalized?.tickSize,
    endpoint: 'algoOrder'
  };
}

async function bPub(path, qs='') {
  const now=Date.now();
  if(now-reqWindow>60000){reqCount=0;reqWindow=now;}
  reqCount++;
  if(reqCount>800){const w=60000-(now-reqWindow);await sleep(w+1000);reqCount=0;reqWindow=Date.now();}
  const r310qWeight = (function(){
    const p = path || '';
    const q = qs || '';
    const limMatch = /limit=(\d+)/.exec(q);
    const lim = limMatch ? Number(limMatch[1]) : 0;
    if (p.includes('/ticker/24hr')) return q.includes('symbol=') ? 1 : 40;
    if (p.includes('/klines'))      return lim > 100 ? 5 : (lim > 0 ? 2 : 2);
    if (p.includes('/depth'))       return lim > 100 ? 10 : 5;
    if (p.includes('/futures/data')) return 1;
    if (p.includes('/openInterest')) return 1;
    if (p.includes('/fundingRate'))  return 1;
    return 1;
  })();
  await binanceThrottle('PUBLIC_REST', r310qWeight, 0);
  const url=`${FAPI}${path}${qs?'?'+qs:''}`;
  const r=await fetch(url,{signal:AbortSignal.timeout(10000)});
  if(r.status===429||r.status===418){registerHttpBackoffAndThrow(path, r.status, r.headers.get('Retry-After'));}
  const text=await r.text();
  try{
    const data = JSON.parse(text);
    if (data && data.code === -1003) { registerBinanceBackoff('Binance -1003 public', 60); throw new Error(formatBinanceError(path, data)); }
    return data;
  }catch(e){ if (e.message && e.message.includes(path)) throw e; throw new Error(`JSON hatası: ${text.substring(0,80)}`);}
}

function sign(qs,secret){return crypto.createHmac('sha256',String(secret||'').trim()).update(qs).digest('hex');}
function signedQueryString(params, apiSecret) {
  const qs = Object.entries(params || {})
    .filter(([,v]) => v !== undefined && v !== null && v !== '')
    .sort(([a],[b]) => a.localeCompare(b))
    .map(([k,v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
    .join('&');
  const sig = sign(qs, String(apiSecret || '').trim());
  return `${qs}&signature=${sig}`;
}

let binanceTimeOffset = 0;
let lastTimeSync = 0;
async function syncBinanceTime(force=false) {
  const now = Date.now();
  if (!force && now - lastTimeSync < 10*60*1000) return binanceTimeOffset;
  try {
    const r = await fetch(`${FAPI}/fapi/v1/time`, { signal: AbortSignal.timeout(5000) });
    const d = await r.json();
    if (d && d.serverTime) {
      binanceTimeOffset = Number(d.serverTime) - Date.now();
      lastTimeSync = Date.now();
      console.log(`Binance time sync offset=${binanceTimeOffset}ms`);
    }
  } catch(e) {
    console.log('Binance time sync hata:', e.message);
  }
  return binanceTimeOffset;
}

function formatBinanceError(path, data) {
  const code = data && data.code != null ? data.code : 'NO_CODE';
  const msg  = data && data.msg ? data.msg : JSON.stringify(data).slice(0,120);
  return `${path}: ${msg} (${code})`;
}

async function bReq(apiKey,apiSecret,method,path,params={},timeout=10000,_retry=false) {
  const m0 = String(method||'GET').toUpperCase();
  const cleanParams = { ...(params || {}) };
  const emergencyBypass = !!cleanParams.__emergency;
  delete cleanParams.__emergency;
  const orderWeight = (m0 === 'POST' || m0 === 'DELETE') ? 1 : 0;
  const w = path.includes('/positionRisk') ? 5 : path.includes('/openOrders') ? 3 : path.includes('/userTrades') ? 5 : 1;
  await binanceThrottle(`${emergencyBypass ? 'EMERGENCY' : 'SIGNED'}:${path}`, w, orderWeight);
  if (!lastTimeSync) await syncBinanceTime(false);
  const ts = Date.now() + binanceTimeOffset;
  const obj = { ...cleanParams, timestamp: ts, recvWindow: 10000 };
  const qs = Object.entries(obj)
    .filter(([,v]) => v !== undefined && v !== null && v !== '')
    .map(([k,v]) => `${k}=${encodeURIComponent(v)}`)
    .join('&');
  const sig = sign(qs, apiSecret);
  const url = `${FAPI}${path}`;
  const fullQs = `${qs}&signature=${sig}`;
  const m = method.toUpperCase();
  const isGet = m === 'GET' || m === 'DELETE';
  const options = {
    method: m,
    headers: {
      'X-MBX-APIKEY': String(apiKey || '').trim(),
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    signal: AbortSignal.timeout(timeout)
  };
  const finalUrl = isGet ? `${url}?${fullQs}` : url;
  if (!isGet) options.body = fullQs;
  let text, netErr;
  for (let attempt = 0; attempt <= 2; attempt++) {
    try {
      const res = await fetch(finalUrl, options);
      if (res.status === 429 || res.status === 418) {
        registerHttpBackoffAndThrow(path, res.status, res.headers.get('Retry-After'));
      }
      text = await res.text();
      netErr = null;
      break;
    } catch(e) {
      netErr = e;
      const msg = String(e?.message || e).toLowerCase();
      const gecici = msg.includes('premature close') || msg.includes('invalid response body') || msg.includes('econnreset') || msg.includes('socket hang up') || msg.includes('timeout') || msg.includes('network') || msg.includes('fetch failed') || msg.includes('body');
      if (!gecici || attempt === 2) break;
      await new Promise(r => setTimeout(r, 500 * (attempt+1)));
      options.signal = AbortSignal.timeout(timeout);
    }
  }
  if (netErr) throw new Error(`Ağ/gövde hatası (3 deneme): ${String(netErr?.message||netErr).slice(0,90)}`);
  let data;
  try { data = JSON.parse(text); }
  catch(e) { throw new Error(`JSON hatası: ${text.substring(0,120)}`); }
  if (data.code && data.code < 0) {
    if (Number(data.code) === -1021 && !_retry) {
      await syncBinanceTime(true);
      return bReq(apiKey,apiSecret,method,path,params,timeout,true);
    }
    if (Number(data.code) === -1003) registerBinanceBackoff(`Binance -1003 ${path}`, 60);
    throw new Error(formatBinanceError(path, data));
  }
  return data;
}

const posRiskCache = {
  data: null,
  ts: 0,
  fetching: false,
  inflight: null,
  inflightStartedAt: 0,
  rateLimitUntil: 0,
  lastApiKey: null,
  lastSource: null,
  lastError: null,
};
const POS_RISK_TTL_NORMAL = 20000;
const POS_RISK_TTL_ACTIVE = 25000;
const POS_RISK_RATELIMIT_MS = 90000;
const POS_RISK_INFLIGHT_TIMEOUT_MS = 15000;

function keyFingerprint(apiKey) {
  const k = String(apiKey || '').trim();
  return k ? `${k.slice(0,6)}:${k.length}` : 'no-key';
}
function isPositionRiskRateLimitError(err) {
  const m = String(err && (err.message || err) || '');
  return m.includes('-1003') || m.includes('BINANCE_BACKOFF_ACTIVE') || m.includes('HTTP 418') || m.includes('HTTP 429') || /too many requests/i.test(m) || /merkezi istek freni/i.test(m);
}
function isPositionRiskCooldownActive() {
  return Date.now() < Number(posRiskCache.rateLimitUntil || 0);
}
function getPositionRiskCooldownMs() {
  return Math.max(0, Number(posRiskCache.rateLimitUntil || 0) - Date.now());
}
function resetStuckPositionRiskInflight(reason='watchdog') {
  const age = posRiskCache.fetching ? Date.now() - Number(posRiskCache.inflightStartedAt || 0) : 0;
  if (posRiskCache.fetching && age > POS_RISK_INFLIGHT_TIMEOUT_MS) {
    try { pushCritical('POSITION_RISK_INFLIGHT_RESET', `positionRisk isteği ${Math.round(age/1000)}sn takıldı; kilit temizlendi`, {reason, ageMs:age}, 'WARNING'); } catch(_) {}
    posRiskCache.fetching = false;
    posRiskCache.inflight = null;
    posRiskCache.inflightStartedAt = 0;
    posRiskCache.lastError = 'positionRisk isteği takıldı; R95 kilit temizledi';
    return true;
  }
  return false;
}
function positionRowsOpenCount(rows) {
  return Array.isArray(rows) ? rows.filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0).length : 0;
}
function filterPositionRiskRows(rows, params={}) {
  if (!Array.isArray(rows)) return [];
  const sym = params && params.symbol ? String(params.symbol).toUpperCase() : '';
  return sym ? rows.filter(p => String(p.symbol || '').toUpperCase() === sym) : rows;
}
async function fetchPositionRiskRaw(apiKey, apiSecret) {
  try {
    const rows = await bReq(apiKey, apiSecret, 'GET', '/fapi/v3/positionRisk', {});
    posRiskCache.lastSource = 'v3/positionRisk';
    return Array.isArray(rows) ? rows : [];
  } catch(e1) {
    if (isPositionRiskRateLimitError(e1)) throw e1;
    const rows = await bReq(apiKey, apiSecret, 'GET', '/fapi/v2/positionRisk', {});
    posRiskCache.lastSource = 'v2/positionRisk';
    return Array.isArray(rows) ? rows : [];
  }
}

async function getPositionRisk(apiKey, apiSecret, params={}) {
  const rows = await fetchPositionRiskRaw(apiKey, apiSecret);
  return filterPositionRiskRows(rows, params);
}

async function getPositionRiskCached(apiKey, apiSecret, params={}) {
  resetStuckPositionRiskInflight('getPositionRiskCached');
  const now = Date.now();
  const apiFp = keyFingerprint(apiKey);

  if (isBinanceBackoffActive()) {
    if (posRiskCache.data && posRiskCache.lastApiKey === apiFp) return filterPositionRiskRows(posRiskCache.data, params);
    throw makeBinanceBackoffError('Binance geçici istek freni', Math.ceil(getBinanceBackoffMs()/1000), 418);
  }

  if (now < posRiskCache.rateLimitUntil) {
    if (posRiskCache.data && posRiskCache.lastApiKey === apiFp) return filterPositionRiskRows(posRiskCache.data, params);
    throw new Error('positionRisk rate-limit cooldown');
  }

  if (params && params.symbol) {
    const hasOpenSym = posRiskCache.data && Array.isArray(posRiskCache.data) &&
      posRiskCache.data.some(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0);
    const ttlSym = hasOpenSym ? POS_RISK_TTL_ACTIVE : POS_RISK_TTL_NORMAL;
    if (posRiskCache.data && (now - posRiskCache.ts < ttlSym) && posRiskCache.lastApiKey === apiFp) {
      return filterPositionRiskRows(posRiskCache.data, params);
    }
    return getPositionRisk(apiKey, apiSecret, params);
  }

  const hasOpen = posRiskCache.data && Array.isArray(posRiskCache.data) &&
    posRiskCache.data.some(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0);
  const ttl = hasOpen ? POS_RISK_TTL_ACTIVE : POS_RISK_TTL_NORMAL;

  if (posRiskCache.data && now - posRiskCache.ts < ttl &&
      posRiskCache.lastApiKey === apiFp) {
    return posRiskCache.data;
  }

  if (posRiskCache.fetching && posRiskCache.inflight) {
    if (posRiskCache.data && posRiskCache.lastApiKey === apiFp) return posRiskCache.data;
    try {
      const rows = await posRiskCache.inflight;
      return Array.isArray(rows) ? rows : [];
    } catch(_e) {
      return posRiskCache.data || [];
    }
  }

  posRiskCache.fetching = true;
  posRiskCache.inflightStartedAt = Date.now();
  posRiskCache.inflight = (async () => {
    try {
      const data = await getPositionRisk(apiKey, apiSecret, {});
      posRiskCache.data = Array.isArray(data) ? data : [];
      posRiskCache.ts = Date.now();
      posRiskCache.lastApiKey = apiFp;
      posRiskCache.lastError = null;
      return posRiskCache.data;
    } catch(e) {
      const msg = e.message || '';
      posRiskCache.lastError = safeErrMsg(e);
      if (msg.includes('-1003') || msg.includes('Too many requests') || msg.includes('BINANCE_BACKOFF_ACTIVE') || msg.includes('HTTP 418') || msg.includes('HTTP 429')) {
        const extraMs = Math.max(POS_RISK_RATELIMIT_MS, getBinanceBackoffMs());
        posRiskCache.rateLimitUntil = Date.now() + extraMs;
        pushCritical('POSITION_RISK_RATELIMIT', e, {cooldownMs:extraMs}, 'WARNING');
        console.log(`⛔ positionRisk / Binance istek freni: ${Math.ceil(extraMs/1000)}sn bekleniyor`);
      }
      if (posRiskCache.data && posRiskCache.lastApiKey === apiFp) return posRiskCache.data;
      throw e;
    } finally {
      posRiskCache.fetching = false;
      posRiskCache.inflight = null;
      posRiskCache.inflightStartedAt = 0;
    }
  })();

  return posRiskCache.inflight;
}

function invalidatePosRiskCache(reason='manual') {
  posRiskCache.ts = 0;
  posRiskCache.lastInvalidateReason = reason;
}
function invalidatePositionRiskCache(reason='manual') {
  invalidatePosRiskCache(reason);
}

const positionRiskState = {
  get cache() {
    if (!posRiskCache.data) return null;
    const hasOpen = positionRowsOpenCount(posRiskCache.data) > 0;
    const ttl = hasOpen ? POS_RISK_TTL_ACTIVE : POS_RISK_TTL_NORMAL;
    return {
      rows: posRiskCache.data,
      ts: posRiskCache.ts,
      ttl,
      exp: posRiskCache.ts + ttl,
      openCount: positionRowsOpenCount(posRiskCache.data),
      fp: posRiskCache.lastApiKey,
    };
  },
  get inflight() { resetStuckPositionRiskInflight('state getter'); return posRiskCache.fetching; },
  get cooldownUntil() { return posRiskCache.rateLimitUntil; },
  get lastError() { return posRiskCache.lastError; },
  get lastSource() { return posRiskCache.lastSource; },
};

const cvdStore = new Map();
const liqStore = new Map();
let globalLiqWS = null;

function startGlobalLiqStream() {
  if (globalLiqWS && (globalLiqWS.readyState === WebSocket.OPEN || globalLiqWS.readyState === WebSocket.CONNECTING)) return;
  const ws = new WebSocket(`${FAPI_WS_MARKET}/ws/!forceOrder@arr`);
  globalLiqWS = ws;

  ws.on('message', (data) => {
    try {
      const d = JSON.parse(data.toString());
      const o = d.o || d;
      const sym    = o.s;
      const side   = o.S;
      const price  = parseFloat(o.p);
      const qty    = parseFloat(o.q);
      const usdt   = price * qty;
      const ts     = Date.now();

      if (!liqStore.has(sym)) liqStore.set(sym, { longLiqs:[], shortLiqs:[], lastCascade:null });
      const store = liqStore.get(sym);

      if (side === 'BUY')  store.shortLiqs.push({ ts, usdt, price });
      if (side === 'SELL') store.longLiqs.push({ ts, usdt, price });

      const cutoff = ts - 5 * 60 * 1000;
      store.shortLiqs = store.shortLiqs.filter(l => l.ts > cutoff);
      store.longLiqs  = store.longLiqs.filter(l => l.ts > cutoff);

      const shortTotal = store.shortLiqs.reduce((s, l) => s + l.usdt, 0);
      const longTotal  = store.longLiqs.reduce((s, l) => s + l.usdt, 0);
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
  ws.on('error', (e) => { try { r130CombinedTickLastErr = String(e?.message || e || 'ws_error').slice(0,120); } catch(_) {} });
  console.log('✅ Global likidasyon stream başlatıldı');
}

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

const r125BookHistory = new Map();
const r125PriorityWake = new Map();
let r310yLastTop2Wake = 0;
let r125FastWakeTimer = null;

function r140PumpPhase(k5m, atrPct) {
  if (!k5m || k5m.length < 20) return { phase:'UNKNOWN', score:0, label:'bilinmiyor' };
  const rows = k5m.slice(-30).map(c=>({
    o:Number(c[1]),h:Number(c[2]),l:Number(c[3]),c:Number(c[4]),v:Number(c[5]||0)
  })).filter(r=>[r.o,r.h,r.l,r.c].every(Number.isFinite));
  if (rows.length < 15) return { phase:'UNKNOWN', score:0, label:'yetersiz veri' };

  const atr = Math.max(0.3, Number(atrPct)||1);
  const recent8 = rows.slice(-8);
  const recent3  = rows.slice(-3);

  function calcAtr(r) {
    const trs = r.map((c,i,a)=>{
      if(i===0)return c.h-c.l;
      return Math.max(c.h-c.l, Math.abs(c.h-a[i-1].c), Math.abs(c.l-a[i-1].c));
    });
    return trs.reduce((s,v)=>s+v,0)/trs.length;
  }
  const atr8  = calcAtr(rows.slice(-8));
  const atr20 = calcAtr(rows.slice(-20));
  const _atRatio = (atr20 > 1e-9) ? +(atr8/atr20).toFixed(2) : 1.0;
  const lp = rows.at(-1).c;
  const volAvg = rows.slice(-20,-1).reduce((s,r)=>s+r.v,0)/19;
  const lastVol = rows.at(-1).v;
  const volRatio = volAvg > 0 ? lastVol/volAvg : 1;
  const mom6 = rows.length>=7 ? (lp - rows.at(-7).c)/rows.at(-7).c*100 : 0;
  const vol6Avg = rows.slice(-6).reduce((s,r)=>s+r.v,0)/6;
  const vol6Ratio = volAvg>0 ? vol6Avg/volAvg : 1;

  let bullSeq = 0, bearSeq = 0;
  for (let i=rows.length-1; i>=Math.max(0,rows.length-8); i--) {
    if(rows[i].c>=rows[i].o){ if(!bearSeq) bullSeq++; else break; }
    else { if(!bullSeq) bearSeq++; else break; }
  }

  const upperWicks = recent3.map(r=>(r.h-Math.max(r.o,r.c))/lp*100);
  const wickGrowing = upperWicks[2] > upperWicks[0]*1.5 && upperWicks[2] > 0.15;

  if (atr8 < atr20*0.65 && bullSeq < 2 && bearSeq < 2) {
    return { phase:'ACCUMULATION', score:1, label:'sıkışma·birikim', atRatio:_atRatio, bullSeq, bearSeq, volRatio:+volRatio.toFixed(2) };
  }
  if (bullSeq >= 4 && volRatio < 0.7 && wickGrowing) {
    return { phase:'DISTRIBUTION', score:-2, label:'dağıtım·sahte pump', atRatio:_atRatio, bullSeq, wickGrowing, volRatio:+volRatio.toFixed(2) };
  }
  const expandClassic = bullSeq >= 3 && atr8 > atr20*1.2 && volRatio >= 1.2;
  const expandMomentum = (mom6 >= 5.0 && atr8 > atr20*1.1) || (mom6 >= 2.0 && atr8 > atr20*1.1 && vol6Ratio >= 1.0 && bullSeq >= 2);
  if (expandClassic || expandMomentum) {
    return { phase:'EXPANSION', score:2, label:'trend·genişleme'+(expandMomentum&&!expandClassic?' (momentum)':''), atRatio:_atRatio, bullSeq, mom6:+mom6.toFixed(1), volRatio:+volRatio.toFixed(2), vol6Ratio:+vol6Ratio.toFixed(2) };
  }
  if ((mom6 <= -5.0 && atr8 > atr20*1.1) || (mom6 <= -2.0 && atr8 > atr20*1.1 && vol6Ratio >= 1.0 && bearSeq >= 2)) {
    return { phase:'EXPANSION_DOWN', score:-2, label:'düşüş·genişleme (SHORT momentum)', atRatio:_atRatio, bearSeq, mom6:+mom6.toFixed(1), vol6Ratio:+vol6Ratio.toFixed(2) };
  }
  return { phase:'TRANSITION', score:0, label:'geçiş·nötr', atRatio:_atRatio, bullSeq, bearSeq, mom6:+mom6.toFixed(1), volRatio:+volRatio.toFixed(2) };
}

function r140EqualLevels(k5m, k1h, lastPrice) {
  const lp = Number(lastPrice||0);
  if (!k5m || k5m.length < 20 || !lp) return { eqHighs:[], eqLows:[], nearHighTrap:false, nearLowTrap:false };
  const tol = 0.0015;

  function findSwings(klines, lookback=40) {
    const rows = klines.slice(-lookback).map(c=>({h:Number(c[2]),l:Number(c[3])}));
    const highs=[], lows=[];
    for(let i=2;i<rows.length-2;i++){
      if(rows[i].h>rows[i-1].h&&rows[i].h>rows[i-2].h&&rows[i].h>rows[i+1].h&&rows[i].h>rows[i+2].h)
        highs.push(rows[i].h);
      if(rows[i].l<rows[i-1].l&&rows[i].l<rows[i-2].l&&rows[i].l<rows[i+1].l&&rows[i].l<rows[i+2].l)
        lows.push(rows[i].l);
    }
    return {highs,lows};
  }

  function cluster(arr) {
    const groups=[];
    for(const v of arr){
      const g=groups.find(g=>Math.abs(g.price-v)/v<tol);
      if(g){g.count++;g.price=(g.price*(g.count-1)+v)/g.count;}
      else groups.push({price:v,count:1});
    }
    return groups.filter(g=>g.count>=3).map(g=>({price:+g.price.toFixed(8),count:g.count,distPct:+((g.price-lp)/lp*100).toFixed(2)}));
  }

  const s5  = findSwings(k5m, 40);
  const s1h = findSwings(k1h||[], 30);
  const allHighs = [...s5.highs, ...s1h.highs];
  const allLows  = [...s5.lows,  ...s1h.lows];
  const eqHighs  = cluster(allHighs);
  const eqLows   = cluster(allLows);

  const nearHighTrap = eqHighs.some(g=>g.distPct>0 && g.distPct<0.5);
  const nearLowTrap  = eqLows.some(g=>g.distPct<0  && g.distPct>-0.5);
  return { eqHighs, eqLows, nearHighTrap, nearLowTrap,
    summary: `eqHigh:${eqHighs.length} eqLow:${eqLows.length} yakınTuzak:${nearHighTrap||nearLowTrap?'EVET':'YOK'}` };
}

function r316TrendlineBreak(k5m) {
  try {
    if (!k5m || k5m.length < 12) return null;
    const win = Math.min(24, k5m.length);
    const rows = k5m.slice(-win).map((c,i)=>({i, o:+c[1], h:+c[2], l:+c[3], c:+c[4]}));
    const n = rows.length;
    const lp = rows[n-1].c;
    if (!(lp > 0)) return null;
    const lastRed = rows[n-1].c < rows[n-1].o;
    const lastGreen = rows[n-1].c > rows[n-1].o;
    const linReg = (pts) => {
      const m=pts.length; let sx=0,sy=0,sxy=0,sxx=0;
      for(const p of pts){ sx+=p.x; sy+=p.y; sxy+=p.x*p.y; sxx+=p.x*p.x; }
      const d = m*sxx - sx*sx;
      if (Math.abs(d) < 1e-12) return null;
      const slope=(m*sxy - sx*sy)/d, intercept=(sy - slope*sx)/m;
      return { slope, intercept };
    };
    const fit = rows.slice(0, n-2);
    if (fit.length < 6) return null;
    const lowsLine = linReg(fit.map(r=>({x:r.i, y:r.l})));
    const highsLine = linReg(fit.map(r=>({x:r.i, y:r.h})));
    const out = { ok:true, risingBreak:false, fallingBreak:false, risingLine:null, fallingLine:null, slopeUp:false, slopeDown:false, note:'' };
    if (!lowsLine || !highsLine) return null;
    const xNow = rows[n-1].i;
    const lowProj = lowsLine.slope*xNow + lowsLine.intercept;
    const highProj = highsLine.slope*xNow + highsLine.intercept;
    const lowSlopePct = lowsLine.slope / lp * 100;
    const highSlopePct = highsLine.slope / lp * 100;
    if (lowSlopePct > 0.03) {
      out.slopeUp = true;
      out.risingLine = +lowProj.toFixed(8);
      if (lp < lowProj * 0.9985 && lastRed) {
        out.risingBreak = true;
        out.note += `YÜKSELEN TREND KIRILDI↓ (yükselen dip çizgisi delindi, fiyat %${((lowProj-lp)/lp*100).toFixed(2)} altına kapandı = aşağı kırılım, SHORT bölgesi) `;
      }
    }
    if (highSlopePct < -0.03) {
      out.slopeDown = true;
      out.fallingLine = +highProj.toFixed(8);
      if (lp > highProj * 1.0015 && lastGreen) {
        out.fallingBreak = true;
        out.note += `DÜŞEN TREND KIRILDI↑ (düşen tepe çizgisi aşıldı, fiyat %${((lp-highProj)/lp*100).toFixed(2)} üstüne kapandı = yukarı kırılım, LONG bölgesi) `;
      }
    }
    out.note = out.note.trim() || `trend çizgisi sağlam — kırılım yok${out.slopeUp?' (yükselen trend DEVAM ediyor — trend yönü YUKARI, SHORT trende karşıdır dikkat)':out.slopeDown?' (düşen trend DEVAM ediyor — trend yönü AŞAĞI, LONG trende karşıdır dikkat)':' (net diagonal trend yok, yatay)'}`;
    try {
      const bandLow = Math.min(lowProj, highProj), bandHigh = Math.max(lowProj, highProj);
      const bandRange = bandHigh - bandLow;
      if (bandRange > 0) {
        const rawPos = (lp - bandLow) / bandRange;
        const pos = Math.max(0, Math.min(1, rawPos));
        out.kanalKonum = +(pos*100).toFixed(0);
        if (out.risingBreak || out.fallingBreak) {
          out.kanalNote = `fiyat kanalı KIRDI (bant dışına taştı, kanal-içi zamanlama artık geçersiz) — yön için kırılım notunu kullan`;
          out.note += ' | ' + out.kanalNote;
          return out;
        }
        const ust = pos >= 0.70, alt = pos <= 0.30;
        out.kanalNote = '';
        if (out.slopeDown) {
          if (ust) out.kanalNote = `DÜŞEN kanal ÜST bandında (%${out.kanalKonum}) — SHORT için DOĞRU zamanlama (tepeden düşüşe)`;
          else if (alt) out.kanalNote = `DÜŞEN kanal ALT bandında (%${out.kanalKonum}) — SHORT geç/dipte, LONG kanala karşı bıçak; dönüş teyidi olmadan girme`;
          else out.kanalNote = `DÜŞEN kanal ORTASINDA (%${out.kanalKonum}) — net zamanlama yok, üst banda çekilmeyi bekle`;
        } else if (out.slopeUp) {
          if (alt) out.kanalNote = `YÜKSELEN kanal ALT bandında (%${out.kanalKonum}) — LONG için DOĞRU zamanlama (dipten yükselişe)`;
          else if (ust) out.kanalNote = `YÜKSELEN kanal ÜST bandında (%${out.kanalKonum}) — LONG geç/tepede, SHORT kanala karşı; dönüş teyidi olmadan girme`;
          else out.kanalNote = `YÜKSELEN kanal ORTASINDA (%${out.kanalKonum}) — net zamanlama yok, alt banda çekilmeyi bekle`;
        }
        if (out.kanalNote) out.note += ' | ' + out.kanalNote;
      }
    } catch(_kanalE) {}
    return out;
  } catch(_) { return null; }
}

function r140OiVelocity(oiHist5m, lastPrice, prevClose) {
  if (!oiHist5m || oiHist5m.length < 4) return { velocity:0, fakePump:false, summary:'OI veri yok' };
  const fn = x => Number(x?.sumOpenInterestValue||x?.sumOpenInterest||0);
  const vals = oiHist5m.slice(-6).map(fn).filter(v=>v>0);
  if (vals.length < 3) return { velocity:0, fakePump:false, summary:'OI yetersiz' };
  const latest = vals.at(-1);
  const base   = vals[0];
  const velocity = base>0 ? ((latest-base)/base)*100 : 0;
  const priceUp  = Number(lastPrice||0) > Number(prevClose||0)*1.001;
  const fakePump = priceUp && velocity < -0.3;
  const oiConfirmed = priceUp && velocity > 0.2;
  return { velocity:+velocity.toFixed(3), fakePump, oiConfirmed,
    summary:`OI${velocity>0?'+':''}${velocity.toFixed(2)}% ${fakePump?'SAHTE_PUMP':''}${oiConfirmed?'GERCEK_PUMP':''}` };
}

function r140BtcDivergence(btc5mCtx, coinChange15m, coinChange60m) {
  if (!btc5mCtx?.ok) return { divergence:false, strong:false, score:0, label:'' };
  const coinC15 = Number(coinChange15m||0);
  const coinC60 = Number(coinChange60m||0);
  const btcDrop = btc5mCtx.dropping || btc5mCtx.change15m < -0.25;
  const coinHolds = btcDrop && coinC15 > -0.1;
  const strongDiv = btcDrop && coinC15 > 0.2;
  if (strongDiv) return { divergence:true, strong:true, score:20, label:`BTC düşerken coin +${coinC15.toFixed(2)}% = ekstrem güç` };
  if (coinHolds) return { divergence:true, strong:false, score:10, label:`BTC drop ama coin tutuyor = birikim` };
  if (btc5mCtx.bouncing && coinC15 > 0.3) return { divergence:false, strong:false, score:8, label:`BTC toparlanıyor + coin güçlü` };
  return { divergence:false, strong:false, score:0, label:'' };
}

function r140CoinRvol(k5m) {
  if (!k5m || k5m.length < 11) return { rvol:1, signal:'UNKNOWN' };
  const vols = k5m.slice(0, -1).map(c=>Number(c[5]||0)).filter(v=>v>0);
  const avg = vols.slice(0,-1).reduce((s,v)=>s+v,0)/Math.max(1,vols.length-1);
  const cur = vols.at(-1)||0;
  const rvol = avg>0 ? cur/avg : 1;
  const signal = rvol>=2.5?'VERY_HIGH':rvol>=1.5?'HIGH':rvol>=1.0?'NORMAL':rvol>=0.6?'LOW':'VERY_LOW';
  return { rvol:+rvol.toFixed(2), signal, avg:+avg.toFixed(0), cur:+cur.toFixed(0) };
}

function r125NormSymbol(symbol='') {
  const s = String(symbol||'').toUpperCase();
  return s.endsWith('USDT') ? s : s + 'USDT';
}
function r125Num(v, d=0) { const n = Number(v); return Number.isFinite(n) ? n : d; }
function r125RegisterPriorityWake(symbol, reason='flow', score=0) {
  const full = r125NormSymbol(symbol);
  const now = Date.now();
  const prev = r125PriorityWake.get(full);
  if (prev && now - prev.ts < 3000 && r125Num(prev.score) >= r125Num(score)) return;
  r125PriorityWake.set(full, { ts:now, reason:String(reason||'flow').slice(0,80), score:r125Num(score) });
}
function r125BookMetricsFromDepth(symbol, depth, lastPrice) {
  const full = r125NormSymbol(symbol);
  const lp = r125Num(lastPrice);
  const bids = Array.isArray(depth?.bids) ? depth.bids.slice(0,50) : [];
  const asks = Array.isArray(depth?.asks) ? depth.asks.slice(0,50) : [];
  let bidTop=0, askTop=0, nearBid=0, nearAsk=0, bestBid=0, bestAsk=0;
  let bidQty5=0, askQty5=0, vampNum=0, vampDen=0;
  const topBids = bids.slice(0,5).map(([p0,q0]) => [r125Num(p0), r125Num(q0)]).filter(([p,q]) => p>0 && q>0);
  const topAsks = asks.slice(0,5).map(([p0,q0]) => [r125Num(p0), r125Num(q0)]).filter(([p,q]) => p>0 && q>0);
  const topBids10 = bids.slice(0,10).map(([p0,q0]) => [r125Num(p0), r125Num(q0)]).filter(([p,q]) => p>0 && q>0);
  const topAsks10 = asks.slice(0,10).map(([p0,q0]) => [r125Num(p0), r125Num(q0)]).filter(([p,q]) => p>0 && q>0);
  const deepBid10 = topBids10.reduce((sum,[p,q]) => sum + p*q, 0);
  const deepAsk10 = topAsks10.reduce((sum,[p,q]) => sum + p*q, 0);
  const deepDen10 = deepBid10 + deepAsk10;
  const deepImb10 = deepDen10 > 0 ? ((deepBid10 - deepAsk10) / deepDen10) * 100 : 0;
  const deepSide10 = deepImb10 > 18 ? 'LONG' : deepImb10 < -18 ? 'SHORT' : 'NEUTRAL';
  const deepStrength10 = Math.min(100, Math.round(Math.abs(deepImb10) * 1.15 + Math.log10(Math.max(1, deepDen10)) * 5));
  for (const [p0,q0] of bids) {
    const p = r125Num(p0), q = r125Num(q0), usdt = p*q;
    if (!bestBid) bestBid = p;
    bidTop += usdt;
    if (lp>0 && Math.abs(p-lp)/lp <= 0.0025) nearBid += usdt;
  }
  for (const [p0,q0] of asks) {
    const p = r125Num(p0), q = r125Num(q0), usdt = p*q;
    if (!bestAsk) bestAsk = p;
    askTop += usdt;
    if (lp>0 && Math.abs(p-lp)/lp <= 0.0025) nearAsk += usdt;
  }
  for (let i=0; i<Math.min(topBids.length, topAsks.length); i++) {
    const [bp,bq] = topBids[i], [ap,aq] = topAsks[i];
    bidQty5 += bq; askQty5 += aq;
    vampNum += ap*bq + bp*aq;
    vampDen += bq + aq;
  }
  const denom = bidTop + askTop;
  const nearDenom = nearBid + nearAsk;
  const imb = denom > 0 ? ((bidTop - askTop) / denom) * 100 : 0;
  const nearImb = nearDenom > 0 ? ((nearBid - nearAsk) / nearDenom) * 100 : imb;
  const spreadPct = (bestAsk>0 && bestBid>0 && lp>0) ? ((bestAsk-bestBid)/lp)*100 : 0;
  const midPrice = (bestAsk>0 && bestBid>0) ? (bestAsk + bestBid) / 2 : 0;
  const microPrice = (bestAsk>0 && bestBid>0 && (bidQty5+askQty5)>0) ? ((bestAsk*bidQty5 + bestBid*askQty5) / (bidQty5+askQty5)) : 0;
  const vampPrice = (vampDen>0) ? (vampNum / vampDen) : microPrice;
  const microBiasPct = (midPrice>0 && microPrice>0) ? ((microPrice-midPrice)/midPrice)*100 : 0;
  const vampBiasPct = (midPrice>0 && vampPrice>0) ? ((vampPrice-midPrice)/midPrice)*100 : microBiasPct;
  const microSide = vampBiasPct > Math.max(0.003, spreadPct*0.18) ? 'LONG' : vampBiasPct < -Math.max(0.003, spreadPct*0.18) ? 'SHORT' : 'NEUTRAL';
  const now = Date.now();
  const hist = (r125BookHistory.get(full) || []).filter(x => now - x.ts < 3*60*1000);
  const prev = hist.length ? hist[hist.length-1] : null;
  hist.push({ ts:now, imb, nearImb, spread:spreadPct, bid:bidTop, ask:askTop, microBias:vampBiasPct, microSide });
  while (hist.length > 120) hist.shift();
  r125BookHistory.set(full, hist);
  const velocity = prev ? imb - prev.imb : 0;
  const nearVelocity = prev ? nearImb - prev.nearImb : 0;
  const recentMicro = hist.slice(-6);
  const microLongCount = recentMicro.filter(x => x.microSide === 'LONG').length;
  const microShortCount = recentMicro.filter(x => x.microSide === 'SHORT').length;
  const microPersistSide = microLongCount >= 4 ? 'LONG' : microShortCount >= 4 ? 'SHORT' : 'NEUTRAL';
  const side = nearImb > 12 || imb > 18 ? 'LONG' : nearImb < -12 || imb < -18 ? 'SHORT' : 'NEUTRAL';
  const strength = Math.min(100, Math.max(0, Math.abs(nearImb)*0.8 + Math.abs(velocity)*1.2 + Math.abs(nearVelocity) + Math.abs(vampBiasPct)*200));
  return {
    ok: denom > 0,
    side, imb:+imb.toFixed(1), nearImb:+nearImb.toFixed(1), velocity:+velocity.toFixed(1), nearVelocity:+nearVelocity.toFixed(1),
    bidTop:+bidTop.toFixed(0), askTop:+askTop.toFixed(0), nearBid:+nearBid.toFixed(0), nearAsk:+nearAsk.toFixed(0),
    spreadPct:+spreadPct.toFixed(4), strength:+strength.toFixed(0), ageMs:0,
    bestBid:+bestBid.toFixed(8), bestAsk:+bestAsk.toFixed(8), midPrice:+midPrice.toFixed(8),
    microPrice:+microPrice.toFixed(8), vampPrice:+vampPrice.toFixed(8), microBiasPct:+microBiasPct.toFixed(5), vampBiasPct:+vampBiasPct.toFixed(5),
    microSide, microPersistSide, microLongCount, microShortCount,
    deepBid10:+deepBid10.toFixed(0), deepAsk10:+deepAsk10.toFixed(0),
    deepImb10:+deepImb10.toFixed(1), deepSide10, deepStrength10
  };
}
function r125LiqClusters(symbol, lastPrice) {
  const full = r125NormSymbol(symbol);
  const lp = r125Num(lastPrice);
  const st = liqStore.get(full);
  if (!st || lp <= 0) return { above:null, below:null, summary:'liq cluster yok' };
  const now = Date.now();
  const rows = [];
  for (const x of (st.shortLiqs||[])) if (now - x.ts < 10*60*1000) rows.push({ ...x, kind:'SHORT_LIQ', dir:'UP' });
  for (const x of (st.longLiqs||[]))  if (now - x.ts < 10*60*1000) rows.push({ ...x, kind:'LONG_LIQ',  dir:'DOWN' });
  if (!rows.length) return { above:null, below:null, summary:'liq cluster yok' };
  const bucketPct = 0.0035;
  const clusters = [];
  for (const r of rows) {
    let c = clusters.find(c => Math.abs(c.price - r.price)/lp <= bucketPct && c.dir === r.dir);
    if (!c) { c = { dir:r.dir, kind:r.kind, price:r.price, usdt:0, count:0, lastTs:0 }; clusters.push(c); }
    c.usdt += r.usdt; c.count += 1; c.lastTs = Math.max(c.lastTs, r.ts);
    c.price = (c.price*(c.count-1)+r.price)/c.count;
  }
  for (const c of clusters) {
    c.distPct = ((c.price - lp)/lp)*100;
    c.strength = Math.round(Math.min(100, (c.usdt/10000) + c.count*8 + Math.max(0, 120000-(now-c.lastTs))/3000));
    c.price = +c.price.toFixed(8); c.usdt = +c.usdt.toFixed(0); c.distPct = +c.distPct.toFixed(3);
  }
  const above = clusters.filter(c => c.price > lp && c.strength >= 12).sort((a,b)=>Math.abs(a.distPct)-Math.abs(b.distPct) || b.strength-a.strength)[0] || null;
  const below = clusters.filter(c => c.price < lp && c.strength >= 12).sort((a,b)=>Math.abs(a.distPct)-Math.abs(b.distPct) || b.strength-a.strength)[0] || null;
  const summary = [above?`üst:${above.price} ${above.usdt}$ g${above.strength}`:'üst:yok', below?`alt:${below.price} ${below.usdt}$ g${below.strength}`:'alt:yok'].join(' | ');
  return { above, below, summary };
}
function r125BuildOrderflowContext(symbol, lastPrice, depth, tickData, liqData) {
  const book = r125BookMetricsFromDepth(symbol, depth, lastPrice);
  const clusters = r125LiqClusters(symbol, lastPrice);
  const cc = tickData?.currentCandle || {};
  const t30 = tickData?.recent30s || {};

  const cvdFallback = getCVD(symbol);
  let delta = r125Num(t30.delta ?? cc.delta, 0);
  let total = r125Num(t30.total ?? ((cc.buy||0)+(cc.sell||0)), 0);
  let trades = r125Num(t30.trades ?? cc.trades, 0);
  let liveSource = 'tick30s';
  let tickFresh = total > 0 || trades > 0;
  const cvdValid = !!(cvdFallback?.valid && ((cvdFallback.buy||0)+(cvdFallback.sell||0)>0));
  if (!tickFresh && cvdValid) {
    delta = r125Num(cvdFallback.delta, 0);
    total = r125Num(cvdFallback.buy, 0) + r125Num(cvdFallback.sell, 0);
    trades = 0;
    liveSource = 'cvd';
  }
  const liveReady = total > 0;
  const flowWarmup = !liveReady;
  const deltaPct = total > 0 ? (delta / total) * 100 : 0;
  const aggression = Math.min(100, Math.round(Math.abs(deltaPct)*0.65 + Math.min(40, trades/2) + (tickData?.deltaFlip && tickData.deltaFlip !== 'NONE' ? 12 : 0)));
  const liveSide = deltaPct > 12 ? 'LONG' : deltaPct < -12 ? 'SHORT' : 'NEUTRAL';
  const r126Extra = r126OrderflowExtras(symbol, lastPrice, book, tickData || {});

  let longRaw =
    (book.side === 'LONG' ? 6 : book.side === 'SHORT' ? -5 : 0) +
    (book.deepSide10 === 'LONG' ? 3 : book.deepSide10 === 'SHORT' ? -3 : 0) +
    (book.velocity > 8 ? 2 : book.velocity < -8 ? -2 : 0) +
    (liveSide === 'LONG' ? 6 : liveSide === 'SHORT' ? -5 : 0) +
    (tickData?.whaleBias === 'WHALE_BUY' ? 3 : tickData?.whaleBias === 'WHALE_SELL' ? -3 : 0) +
    (tickData?.deltaFlip === 'BEAR_TO_BULL' ? 4 : tickData?.deltaFlip === 'BULL_TO_BEAR' ? -4 : 0) +
    (r126Extra.bidAbsorb ? 8 : r126Extra.askAbsorb ? -6 : 0) +
    (r126Extra.forecast?.side === 'LONG' ? Math.min(5, Math.round(r125Num(r126Extra.forecast.confidence,0)/20)) : r126Extra.forecast?.side === 'SHORT' ? -3 : 0) +
    (r126Extra.aggressionTrend?.side === 'LONG' && r126Extra.aggressionTrend?.phase === 'ACCELERATING' ? Math.min(6, Math.round(r125Num(r126Extra.aggressionTrend.strength,0)/16)) : r126Extra.aggressionTrend?.side === 'SHORT' && r126Extra.aggressionTrend?.phase === 'ACCELERATING' ? -4 : 0) +
    (r126Extra.deltaImprint?.coiled && liveSide === 'LONG' ? 2 : 0) +
    (liqData?.cascade?.signal === 'LONG_FIRSAT' ? 5 : liqData?.cascade?.signal === 'SHORT_FIRSAT' ? -5 : 0);

  let shortRaw =
    (book.side === 'SHORT' ? 6 : book.side === 'LONG' ? -5 : 0) +
    (book.deepSide10 === 'SHORT' ? 3 : book.deepSide10 === 'LONG' ? -3 : 0) +
    (book.velocity < -8 ? 2 : book.velocity > 8 ? -2 : 0) +
    (liveSide === 'SHORT' ? 6 : liveSide === 'LONG' ? -5 : 0) +
    (tickData?.whaleBias === 'WHALE_SELL' ? 3 : tickData?.whaleBias === 'WHALE_BUY' ? -3 : 0) +
    (tickData?.deltaFlip === 'BULL_TO_BEAR' ? 4 : tickData?.deltaFlip === 'BEAR_TO_BULL' ? -4 : 0) +
    (r126Extra.askAbsorb ? 8 : r126Extra.bidAbsorb ? -6 : 0) +
    (r126Extra.forecast?.side === 'SHORT' ? Math.min(5, Math.round(r125Num(r126Extra.forecast.confidence,0)/20)) : r126Extra.forecast?.side === 'LONG' ? -3 : 0) +
    (r126Extra.aggressionTrend?.side === 'SHORT' && r126Extra.aggressionTrend?.phase === 'ACCELERATING' ? Math.min(6, Math.round(r125Num(r126Extra.aggressionTrend.strength,0)/16)) : r126Extra.aggressionTrend?.side === 'LONG' && r126Extra.aggressionTrend?.phase === 'ACCELERATING' ? -4 : 0) +
    (r126Extra.deltaImprint?.coiled && liveSide === 'SHORT' ? 2 : 0) +
    (liqData?.cascade?.signal === 'SHORT_FIRSAT' ? 5 : liqData?.cascade?.signal === 'LONG_FIRSAT' ? -5 : 0);

  if (flowWarmup) {
    longRaw = Math.min(longRaw, 3);
    shortRaw = Math.min(shortRaw, 3);
  }
  const longEdge = Math.max(0, longRaw);
  const shortEdge = Math.max(0, shortRaw);
  const bestSide = longEdge > shortEdge + 2 ? 'LONG' : shortEdge > longEdge + 2 ? 'SHORT' : 'NEUTRAL';
  const tpLong = clusters.above && clusters.above.distPct > 0.18 && clusters.above.distPct < 8 ? clusters.above : null;
  const tpShort = clusters.below && clusters.below.distPct < -0.18 && clusters.below.distPct > -8 ? clusters.below : null;
  const flowState = flowWarmup ? 'ISINIYOR' : (tickFresh ? 'CANLI_TICK' : 'CVD_FALLBACK');
  const r130sum = (typeof r130CombinedSummary === 'function') ? r130CombinedSummary() : '';
  const summary = `${flowState} · ${r130sum} · book:${book.side} imb:${book.nearImb}% v:${book.velocity} · deep:${book.deepSide10||'NEUTRAL'} ${book.deepImb10||0}% · micro:${book.microSide||'NEUTRAL'} ${book.vampBiasPct||0}% p:${book.microPersistSide||'NEUTRAL'} · delta:${liveSide} ${deltaPct.toFixed(1)}% src:${liveSource} tr:${trades} · edge L${longEdge}/S${shortEdge} · ${clusters.summary} · ${r126Extra.summary}`;
  return { ok:true, liveReady, tickFresh, cvdValid, flowWarmup, liveSource, book, clusters, liveSide, delta:+delta.toFixed(0), deltaPct:+deltaPct.toFixed(1), aggression, trades, longEdge:+longEdge.toFixed(1), shortEdge:+shortEdge.toFixed(1), bestSide, tpLong, tpShort, r126:r126Extra, summary };
}
function r125FlowForSide(ctx, side='LONG') {
  if (!ctx || !ctx.ok) return { edge:0, against:0, ok:false, strong:false, liveReady:false, summary:'' };
  const edge = side === 'LONG' ? r125Num(ctx.longEdge) : r125Num(ctx.shortEdge);
  const against = side === 'LONG' ? r125Num(ctx.shortEdge) : r125Num(ctx.longEdge);
  const liveReady = !!ctx.liveReady;
  const ok = liveReady && edge >= 6 && edge >= against + 2;
  const strong = liveReady && edge >= 9 && edge >= against + 4;
  return { edge, against, ok, strong, liveReady, summary:ctx.summary || '' };
}

const R126_PLAYBOOK_STATS_PATH = path.join(process.cwd(), 'lazarus_playbook_stats.json');
let r126PlaybookStats = {};
try {
  const raw = fs.readFileSync(R126_PLAYBOOK_STATS_PATH, 'utf8');
  const obj = JSON.parse(raw || '{}');
  if (obj && typeof obj === 'object' && !Array.isArray(obj)) r126PlaybookStats = obj;
} catch(_) { r126PlaybookStats = {}; }
function r126SavePlaybookStats() {
  try { fs.writeFileSync(R126_PLAYBOOK_STATS_PATH, JSON.stringify(r126PlaybookStats, null, 2)); } catch(_) {}
}
function r126PlaybookKey(v) {
  const text = String(v?.brainMode || v?.playbook || v?.entryPermissionReason || v?.openReason || v?.entryReason?.reason || v?.entryReason || '');
  const m = text.match(/(?:SINGLE_BRAIN_|playbook[:= ]+)?(HTF_REVERSAL|HTF_RECLAIM|SQUEEZE_BREAKOUT|TREND_CONTINUATION|COUNTER_TRAP|MOMENTUM_SCALP|FLOW_SCALP)/i);
  return (m ? m[1] : 'UNKNOWN').toUpperCase();
}
function r126PlaybookAdj(mode) {
  const key = String(mode || 'UNKNOWN').toUpperCase();
  const st = r126PlaybookStats[key];
  if (!st || Number(st.n || 0) < 6) return 0;
  const n = Number(st.n || 0), wins = Number(st.wins || 0), avg = Number(st.avgPnl || 0);
  const wr = n > 0 ? wins / n : 0.5;
  let adj = Math.round((wr - 0.55) * 24 + Math.max(-4, Math.min(4, avg * 1.5)));
  return Math.max(-10, Math.min(10, adj));
}
function r126UpdatePlaybookStats(state={}, cls={}) {
  try {
    const key = r126PlaybookKey(state);
    if (!key || key === 'UNKNOWN') return;
    const pnl = Number(cls.realizedPnl);
    if (!Number.isFinite(pnl)) return;
    const st = r126PlaybookStats[key] || { n:0, wins:0, losses:0, pnl:0, avgPnl:0, last:0 };
    st.n = Number(st.n || 0) + 1;
    if (pnl > 0) st.wins = Number(st.wins || 0) + 1; else if (pnl < 0) st.losses = Number(st.losses || 0) + 1;
    st.pnl = +(Number(st.pnl || 0) + pnl).toFixed(6);
    st.avgPnl = +(st.pnl / Math.max(1, st.n)).toFixed(6);
    st.wr = +(Number(st.wins || 0) / Math.max(1, st.n) * 100).toFixed(1);
    st.last = Date.now();
    r126PlaybookStats[key] = st;
    r126SavePlaybookStats();
  } catch(_) {}
}
function r126PlaybookBase(mode, base) {
  return Number(base || 0) + r126PlaybookAdj(mode);
}
function r142Clamp(n, lo, hi) { n = Number(n); return Number.isFinite(n) ? Math.max(lo, Math.min(hi, n)) : 0; }
function r142SideOpp(side='LONG') { return String(side).toUpperCase()==='LONG' ? 'SHORT' : 'LONG'; }
function r142Text(v='') { return String(v || '').toUpperCase(); }
function r142TradeText(row={}) {
  return [row.entryReason, row.openReason, row.reason, row.resultNote, row.exitReason, row.brainMode, row.entryPermissionReason, row.r126OrderflowSummary].filter(Boolean).join(' ');
}
function r142SetupTags(side='LONG', d={}, mode='NO_EDGE') {
  const tags = new Set([String(mode||'NO_EDGE').toUpperCase(), String(side||'').toUpperCase()]);
  const ict = r142Text((d.ictDashboard||'') + ' ' + (d.siksmaOzet||'') + ' ' + (d.r118CandleOzet||'') + ' ' + (d.r140Summary||''));
  if (d.r117HtfReverseOk || d.r117BodyReclaimOk || d.r110IctKoprusuOk) tags.add('HTF_RECLAIM_SWEEP');
  if (d.r116HtfGuardBlock || /HTF:KARŞI|BSL_ALINDI|SSL_ALINDI|SUPPLY_OB|DEMAND_OB/.test(ict)) tags.add('HTF_COUNTER_ZONE');
  if (d.r140OiVel?.fakePump) tags.add('FAKE_PUMP_OI');
  if (d.r140OiVel?.oiConfirmed) tags.add('OI_CONFIRMED');
  if (d.r140Phase?.phase) tags.add('PHASE_'+String(d.r140Phase.phase).toUpperCase());
  if (d.r140BtcDiv?.score > 0) tags.add('BTC_DIVERGENCE');
  if (d.r140EqHL?.nearHighTrap) tags.add('EQ_HIGH_TRAP');
  if (d.r140EqHL?.nearLowTrap) tags.add('EQ_LOW_TRAP');
  if (d.r118CandleOk || d.r118CandleStrong) tags.add('CANDLE_OK');
  if (d.r37LateChaseBlock || d.r37RetestWait || d.r39TargetNearBlock) tags.add('LATE_OR_TARGET_NEAR');
  if (d.r125Flow?.bestSide && d.r125Flow.bestSide !== 'NEUTRAL') tags.add('FLOW_'+String(d.r125Flow.bestSide).toUpperCase());
  if (Math.abs(Number(d.r125Flow?.deltaPct||0)) >= 25) tags.add(Number(d.r125Flow?.deltaPct||0)>0?'DELTA_LONG':'DELTA_SHORT');
  if (d.r190Edge?.earlyContinuation) tags.add('R190_EARLY_CONTINUATION');
  if (d.r190Edge?.lateTrapRisk) tags.add('R190_LATE_TRAP');
  if (d.r190Edge?.squeeze) tags.add('R190_SQUEEZE_FUEL');
  if (d.atrExtremeBlock || d.atrBlocking) tags.add('ATR_RISK');
  if (d.r93PiyasaTehlikeli || d.r88PiyasaBozuk) tags.add('BAD_GROUND');
  return [...tags];
}
function r142RowTags(row={}) {
  const text = r142Text(r142TradeText(row));
  const tags = new Set();
  const side = String(row.side || '').toUpperCase(); if (side) tags.add(side);
  for (const m of ['HTF_REVERSAL','HTF_RECLAIM','SQUEEZE_BREAKOUT','TREND_CONTINUATION','COUNTER_TRAP','MOMENTUM_SCALP','FLOW_SCALP']) if (text.includes(m) || text.includes(m.replace('_',' '))) tags.add(m);
  if (/HTF_COUNTER|HTF:KARŞI|KARŞI 15M|KARŞI 1H|KARŞI 4H|DIRENÇ|SUPPLY/.test(text)) tags.add('HTF_COUNTER_ZONE');
  if (/LATE_CHASE|GEÇ|CHASE|HEDEF ÇOK YAKIN|TARGET/.test(text)) tags.add('LATE_OR_TARGET_NEAR');
  if (/DELTA_OPPOSITE|DELTA TERS|TERS AKIŞ|AKIŞ TERS/.test(text)) tags.add('DELTA_OPPOSITE');
  if (/FAKE_PUMP|SAHTE PUMP|OI.*AZAL|OI.*ÇÖZ/.test(text)) tags.add('FAKE_PUMP_OI');
  if (/DISTRIBUTION|DAĞITIM/.test(text)) tags.add('PHASE_DISTRIBUTION');
  if (/SWEEP|SÜPÜR|BODY|GÖVDE|RECLAIM/.test(text)) tags.add('HTF_RECLAIM_SWEEP');
  if (/ENGULF|HAMMER|TWEEZER|MORNING|EVENING|DARKCLOUD|GRAVESTONE/.test(text)) tags.add('CANDLE_OK');
  return [...tags];
}
function r142Overlap(a=[], b=[]) { const bs = new Set(b); return a.filter(x=>bs.has(x)).length; }
function r142MemoryStats(side='LONG', d={}, mode='NO_EDGE') {
  const now = Date.now();
  const tags = r142SetupTags(side, d, mode);
  const opp = r142SideOpp(side);
  const symbol = normalizeSymbol(d.symbol || d.fullSymbol || d.r38MarketCtx?.symbol || '');
  const rows = (Array.isArray(tradeLedger) ? tradeLedger : []).filter(r => {
    const t = Number(r.closedAt || 0); if (!t || now - t > 7*24*60*60*1000) return false;
    const pnl = Number(r.pnlUSDT); if (!Number.isFinite(pnl) || pnl === 0) return false;
    const rt = r142RowTags(r); return r142Overlap(tags, rt) >= 2 || (symbol && normalizeSymbol(r.symbol||'') === symbol);
  });
  const sameSide = rows.filter(r => normalizeSide(r.side) === side);
  const oppSide  = rows.filter(r => normalizeSide(r.side) === opp);
  const countStats = arr => {
    let wins=0, losses=0, pnl=0;
    for (const r of arr) { const x=Number(r.pnlUSDT); pnl += Number.isFinite(x)?x:0; if (x>0) wins++; else if (x<0) losses++; }
    const n=wins+losses, wr=n?wins/n:0;
    return {n,wins,losses,pnl:+pnl.toFixed(4),wr};
  };
  return { tags, same:countStats(sameSide), opp:countStats(oppSide) };
}
function r142CalibrateEdge(side='LONG', d={}, mode='NO_EDGE', rawEdge=0, score=0) {
  const notes = [];
  let adj = 0;
  const mem = r142MemoryStats(side, d, mode);
  const pb = r126PlaybookStats[String(mode||'UNKNOWN').toUpperCase()] || null;
  if (pb && Number(pb.n||0) >= 6) {
    const wr = Number(pb.wr||0)/100;
    const a = r142Clamp(Math.round((wr - 0.55) * 22), -12, 10);
    adj += a; if (a) notes.push(`playbookWR:${Number(pb.wr||0).toFixed(0)}% ${a>0?'+':''}${a}`);
  }
  if (mem.same.n >= 3) {
    const a = r142Clamp(Math.round((mem.same.wr - 0.55) * 24), -14, 10);
    adj += a; if (a) notes.push(`benzer:${Math.round(mem.same.wr*100)}% ${a>0?'+':''}${a}`);
  }
  if (mem.same.losses >= 2 && mem.same.wins <= mem.same.losses) {
    adj -= 10; notes.push('loss-memory:-10');
  }
  if (mem.opp.n >= 3 && mem.opp.wr >= 0.6 && mem.opp.pnl > 0) {
    adj += 6; notes.push('ters-senaryo:+6');
  }
  const phase = String(d.r140Phase?.phase || '').toUpperCase();
  const deltaPct = Number(d.r125Flow?.deltaPct || 0);
  const forecastSide = String(d.r125Flow?.r126?.forecast?.side || '').toUpperCase();
  const forecastConf = Number(d.r125Flow?.r126?.forecast?.confidence || 0);
  const lateLong = side==='LONG' && (phase==='DISTRIBUTION' || d.r140OiVel?.fakePump || d.r140EqHL?.nearHighTrap || d.r37LateChaseBlock || d.r39TargetNearBlock);
  const lateShort = side==='SHORT' && (d.r140EqHL?.nearLowTrap || d.r37LateChaseBlock || d.r39TargetNearBlock);
  const late = !!(lateLong || lateShort);
  if (late && !['HTF_REVERSAL','HTF_RECLAIM'].includes(String(mode||''))) { adj -= 8; notes.push('late-chase:-8'); }
  const liveAgainst = (side==='LONG' && deltaPct <= -18) || (side==='SHORT' && deltaPct >= 18) || (forecastSide && forecastSide !== side && forecastConf >= 70);
  if (liveAgainst) { adj -= 10; notes.push('live-opposite:-10'); }
  const liveWith = (side==='LONG' && deltaPct >= 18) || (side==='SHORT' && deltaPct <= -18) || (forecastSide === side && forecastConf >= 70);
  if (liveWith && !late) { adj += 5; notes.push('live-align:+5'); }
  const calibrated = r142Clamp(Math.round(Number(rawEdge||0) + adj), 0, 100);
  return { rawEdge:Math.round(Number(rawEdge||0)), calibratedEdge:calibrated, adj, notes, mem, late, liveAgainst, liveWith };
}
function r126AggressionTrendFromTicks(ticks=[]) {
  const now = Date.now();
  const arr = (Array.isArray(ticks) ? ticks : []).filter(t => now - r125Num(t.ts,0) < 60_000);
  const win = (from,to) => arr.filter(t => now - r125Num(t.ts,0) >= from && now - r125Num(t.ts,0) < to);
  const recent = win(0, 15_000), prev = win(15_000, 45_000);
  function stat(rows) {
    const buy = rows.filter(t=>t.isBuy).reduce((s,t)=>s+r125Num(t.usdt),0);
    const sell= rows.filter(t=>!t.isBuy).reduce((s,t)=>s+r125Num(t.usdt),0);
    const total = buy+sell, delta=buy-sell;
    return { buy, sell, total, delta, abs:Math.abs(delta), trades:rows.length, deltaPct:total>0?delta/total*100:0 };
  }
  const a=stat(recent), b=stat(prev);
  let side='NEUTRAL', phase='FLAT', strength=0;
  if (a.total >= 5000 && (a.abs > b.abs*1.25 || a.trades > b.trades*1.25)) {
    side = a.deltaPct > 10 ? 'LONG' : a.deltaPct < -10 ? 'SHORT' : 'NEUTRAL';
    phase = side === 'NEUTRAL' ? 'ACTIVE_NEUTRAL' : 'ACCELERATING';
    strength = Math.min(100, Math.round(Math.abs(a.deltaPct)*0.8 + Math.min(40,a.trades/2) + Math.min(30, a.total/8000)));
  }
  return { side, phase, strength, recent:a, prev:b };
}
function r126CandleForecastFromTicks(engine) {
  try {
    const c = engine?.currentCandle;
    const ticks = Array.isArray(engine?.lastTicks) ? engine.lastTicks : [];
    if (!c || ticks.length < 4) return { side:'NEUTRAL', confidence:0, summary:'mum tahmin yok' };
    const now = Date.now();
    const recent = ticks.filter(t => now - r125Num(t.ts,0) < 45_000);
    if (recent.length < 4) return { side:'NEUTRAL', confidence:0, summary:'mum tahmin ısınma' };
    const first = recent[0].price, last = recent.at(-1).price;
    const slopePct = first > 0 ? (last-first)/first*100 : 0;
    const range = Math.max(1e-12, r125Num(c.high)-r125Num(c.low));
    const closePos = range > 0 ? (r125Num(c.close)-r125Num(c.low))/range : 0.5;
    const elapsed = Math.min(1, Math.max(0, (now - r125Num(c.ts, now)) / Math.max(15000, r125Num(engine.candleMs, 30000))));
    let side='NEUTRAL', confidence=0;
    if (slopePct > 0.08 && closePos > 0.62) { side='LONG'; confidence=Math.round(35 + Math.min(45, slopePct*180) + elapsed*20); }
    if (slopePct < -0.08 && closePos < 0.38) { side='SHORT'; confidence=Math.round(35 + Math.min(45, Math.abs(slopePct)*180) + elapsed*20); }
    return { side, confidence:Math.min(100, confidence), slopePct:+slopePct.toFixed(3), closePos:+closePos.toFixed(2), elapsed:+elapsed.toFixed(2), summary:`mumTahmin:${side} c${Math.min(100,confidence)} slope:${slopePct.toFixed(2)}% pos:${closePos.toFixed(2)}` };
  } catch(_) { return { side:'NEUTRAL', confidence:0, summary:'mum tahmin hata' }; }
}
function r126DeltaImprintFromTickData(tickData={}) {
  const t = tickData?.recent30s || {};
  const total = r125Num(t.total,0), delta=r125Num(t.delta,0), trades=r125Num(t.trades,0);
  const pct = total>0 ? delta/total*100 : 0;
  const coiled = total >= 8000 && trades >= 10 && Math.abs(pct) <= 8;
  return { coiled, pct:+pct.toFixed(1), total:+total.toFixed(0), trades, summary: coiled ? `delta imprint:denge ${pct.toFixed(1)}% / ${Math.round(total)}$` : 'delta imprint yok' };
}
function r126OrderflowExtras(symbol, lastPrice, book={}, tickData={}) {
  const cc = tickData?.currentCandle || {};
  const t30 = tickData?.recent30s || {};
  const deltaPct = r125Num(t30.total,0)>0 ? r125Num(t30.delta,0)/r125Num(t30.total,0)*100 : 0;
  const cOpen = r125Num(cc.open ?? cc.close, 0), cClose = r125Num(cc.close, 0);
  const priceHeldUp = cOpen>0 && cClose >= cOpen * 0.9995;
  const priceHeldDown = cOpen>0 && cClose <= cOpen * 1.0005;
  const bidAbsorb = deltaPct < -22 && r125Num(book.nearImb,0) > 10 && priceHeldUp;
  const askAbsorb = deltaPct > 22 && r125Num(book.nearImb,0) < -10 && priceHeldDown;
  const forecast = tickData?.candleForecast || { side:'NEUTRAL', confidence:0 };
  const imprint = tickData?.deltaImprint || r126DeltaImprintFromTickData(tickData);
  const aggr = tickData?.aggressionTrend || { side:'NEUTRAL', phase:'FLAT', strength:0 };
  const summary = [
    bidAbsorb ? 'absorpsiyon:BUY_WALL' : askAbsorb ? 'absorpsiyon:SELL_WALL' : 'absorpsiyon:yok',
    forecast?.summary || '',
    imprint?.summary || '',
    aggr?.phase === 'ACCELERATING' ? `aggr:${aggr.side} ${aggr.strength}` : 'aggr:flat'
  ].filter(Boolean).join(' · ');
  return { bidAbsorb, askAbsorb, forecast, imprint, aggressionTrend:aggr, summary };
}

let r130CombinedTickWS = null;
let r130CombinedTickKey = '';
let r130CombinedTickSymbols = new Set();
let r130CombinedTickLastMsgTs = 0;
let r130CombinedTickLastOpenTs = 0;
let r130CombinedTickLastErr = '';
let r130CombinedTickRestartCount = 0;
let r133CombinedLastStartTs = 0;
function r133IsAsciiFuturesSymbol(full='') {
  return /^[A-Z0-9_]+USDT$/.test(String(full||'').toUpperCase());
}

function r130EnsureTickEngine(symbol) {
  const full = normalizeSymbol ? normalizeSymbol(symbol) : r125NormSymbol(symbol);
  if (!full || !full.endsWith('USDT')) return null;
  let engine = tickStore.get(full);
  if (!engine) {
    const tickSz = full.startsWith('BTC')?0.1:full.startsWith('ETH')?0.01:full.startsWith('BNB')?0.01:0.0001;
    engine = createTickEngine(tickSz, 5);
    engine.tickSize = tickSz;
    engine.r130Combined = true;
    tickStore.set(full, engine);
  }
  if (!Array.isArray(engine.lastTicks)) engine.lastTicks = [];
  if (!Array.isArray(engine.candles)) engine.candles = [];
  return engine;
}

function r130StreamName(symbol) {
  const full = normalizeSymbol ? normalizeSymbol(symbol) : r125NormSymbol(symbol);
  if (!full || !full.endsWith('USDT')) return '';
  return `${full.toLowerCase()}@aggTrade`;
}

function r130StartCombinedAggTradeStream(symbols=[], opts={}) {
  try {
    const clean = [];
    for (const s0 of (Array.isArray(symbols)?symbols:[symbols])) {
      const full = normalizeSymbol ? normalizeSymbol(s0) : r125NormSymbol(s0);
      if (!full || !full.endsWith('USDT')) continue;
      r130EnsureTickEngine(full);
      if (!r133IsAsciiFuturesSymbol(full)) {
        const eng = tickStore.get(full);
        if (eng) eng.r133CombinedExcluded = true;
        try { startTickStream(full); } catch(_) {}
        continue;
      }
      if (!clean.includes(full)) clean.push(full);
    }
    if (!clean.length) return;

    let next = opts?.replace ? new Set(clean) : new Set([...(r130CombinedTickSymbols||[]), ...clean]);
    const arr = Array.from(next).slice(0, 30);
    const streams = arr.map(r130StreamName).filter(Boolean).sort();
    const key = streams.join('/');
    const rs = r130CombinedTickWS?.readyState;
    if (r130CombinedTickWS && (rs === WebSocket.OPEN || rs === WebSocket.CONNECTING) && key === r130CombinedTickKey) return;
    if (r130CombinedTickWS && (rs === WebSocket.OPEN || rs === WebSocket.CONNECTING) && r130CombinedTickLastMsgTs && Date.now()-r130CombinedTickLastMsgTs < 6000 && Date.now()-r133CombinedLastStartTs < 6000) return;

    try { if (r130CombinedTickWS) r130CombinedTickWS.terminate?.(); } catch(_) {}
    r130CombinedTickSymbols = new Set(arr);
    r130CombinedTickKey = key;
    if (!key) return;

    const url = `${FAPI_WS_MARKET}/stream?streams=${key}`;
    const ws = new WebSocket(url);
    r133CombinedLastStartTs = Date.now();
    r130CombinedTickWS = ws;
    ws.on('open', () => { r130CombinedTickLastOpenTs = Date.now(); r130CombinedTickLastErr = ''; });
    ws.on('message', data => {
      try {
        const msg = JSON.parse(data.toString());
        const t = msg?.data || msg;
        const full = String(t?.s || '').toUpperCase();
        if (!full) return;
        const eng = r130EnsureTickEngine(full);
        if (!eng) return;
        r130CombinedTickLastMsgTs = Date.now();
        eng.r130LastMsgTs = r130CombinedTickLastMsgTs;
        eng.r130Source = 'combinedAggTrade';
        processTick(eng, t);
      } catch(_) {}
    });
    ws.on('close', (code, reason) => {
      try { r130CombinedTickLastErr = code ? `close:${code}${reason?':' + String(reason).slice(0,80):''}` : r130CombinedTickLastErr; } catch(_) {}
      r130CombinedTickRestartCount++;
      const restart = Array.from(r130CombinedTickSymbols || []);
      setTimeout(() => { try { r130StartCombinedAggTradeStream(restart, {replace:true}); } catch(_) {} }, 2500);
    });
    ws.on('error', (e) => { try { r130CombinedTickLastErr = String(e?.message || e || 'ws_error').slice(0,160); } catch(_) {} });
  } catch(_) {}
}

function r130CombinedSummary() {
  try {
    const age = r130CombinedTickLastMsgTs ? Date.now() - r130CombinedTickLastMsgTs : null;
    const openAge = r130CombinedTickLastOpenTs ? Date.now() - r130CombinedTickLastOpenTs : null;
    const rs = r130CombinedTickWS?.readyState;
    const state = rs === WebSocket.OPEN ? 'OPEN' : rs === WebSocket.CONNECTING ? 'CONNECTING' : 'CLOSED';
    const noTick = (rs === WebSocket.OPEN && !r130CombinedTickLastMsgTs && openAge != null && openAge > 12000) ? ' noTickRestartBekliyor' : '';
    const err = r130CombinedTickLastErr ? ` err:${r130CombinedTickLastErr}` : '';
    return `R135 combined:${state} sembol:${r130CombinedTickSymbols?.size||0} sonTick:${age==null?'yok':Math.round(age/1000)+'sn'}${noTick} restart:${r130CombinedTickRestartCount}${err}`;
  } catch(_) { return 'R135 combined:bilinmiyor'; }
}

function startCVDStream(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  const existing = cvdStore.get(full);
  const rs = existing?.ws?.readyState;
  if (existing?.ws && (rs === WebSocket.OPEN || rs === WebSocket.CONNECTING)) return;

  const store = existing || { buy:0, sell:0, history:[], lastReset: Date.now(), ws:null };
  cvdStore.set(full, store);

  const wsUrl = `${FAPI_WS_MARKET}/ws/${encodeURIComponent(full.toLowerCase())}@aggTrade`;
  const ws = new WebSocket(wsUrl);
  store.ws = ws;

  ws.on('message', (data) => {
    try {
      const t = JSON.parse(data.toString());
      const qty = parseFloat(t.q) * parseFloat(t.p);
      if (t.m) store.sell += qty;
      else     store.buy  += qty;

      const now = Date.now();
      if (now - store.lastReset > 5 * 60 * 1000) {
        store.history.push({ ts: now, buy: store.buy, sell: store.sell,
          delta: store.buy - store.sell });
        if (store.history.length > 48) store.history.shift();
        store.buy  = store.buy  * 0.30;
        store.sell = store.sell * 0.30;
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
  startCVDStream(full);
  const store = cvdStore.get(full);
  if (!store) return { buy:0, sell:0, ratio:50, momentum:'UNKNOWN', trend:'UNKNOWN', historyLen:0, valid:false };

  const buy=store.buy, sell=store.sell;
  const total=buy+sell;
  const ratio=total>0?buy/total*100:50;
  const delta=buy-sell;

  let trend='NEUTRAL', acceleration='NONE';
  if (store.history.length >= 3) {
    const recent = store.history.slice(-3).reduce((s,h)=>s+h.delta,0);
    const older  = store.history.slice(-6,-3).reduce((s,h)=>s+h.delta,0);
    trend = delta > 0 ? 'POSITIVE' : delta < 0 ? 'NEGATIVE' : 'NEUTRAL';
    if (recent > 0 && (older <= 0 || recent > older * 1.30)) {
      acceleration = 'ACCELERATING_BULL';
    } else if (recent < 0 && (older >= 0 || Math.abs(recent) > Math.abs(older) * 1.30)) {
      acceleration = 'ACCELERATING_BEAR';
    }
  }

  const momentum = acceleration !== 'NONE' ? acceleration : ratio > 60 ? 'POSITIVE' : ratio < 40 ? 'NEGATIVE' : 'NEUTRAL';

  return { buy:+buy.toFixed(0), sell:+sell.toFixed(0), ratio:+ratio.toFixed(1),
    delta:+delta.toFixed(0), momentum, trend, historyLen:store.history.length, valid: total > 0 };
}

const tickStore = new Map();

function createTickEngine(tickSize = 0.01, volatilePct = 5) {
  const candleMs = volatilePct > 20 ? 15000 : volatilePct > 8 ? 20000 : volatilePct > 3 ? 30000 : 60000;
  return {
    footprint: new Map(),
    sweepDet: createSweepDetector(),
    candles: [],
    currentCandle: null,
    candleMs,
    tickSize,
    lastPrice: 0,
    totalBuy: 0,
    totalSell: 0,
    bigTrades: [],
    lastTicks: [],
    ws: null,
  };
}

function roundToTick(price, tickSize) {
  return Math.round(price / tickSize) * tickSize;
}

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

function detectAbsorption(footprint, currentPrice, tickSize, isLong) {
  const nearLevels = Array.from(footprint.entries())
    .filter(([p]) => Math.abs(parseFloat(p)-currentPrice)/currentPrice < 0.002)
    .map(([,v]) => v);

  if (!nearLevels.length) return { absorbed:false };

  const totalBuy = nearLevels.reduce((s,l)=>s+l.buy,0);
  const totalSell = nearLevels.reduce((s,l)=>s+l.sell,0);

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

function processTick(engine, trade) {
  try {
    const aid = trade?.a ?? trade?.t;
    if (aid !== undefined && aid !== null) {
      if (!engine._r130SeenAggIds) { engine._r130SeenAggIds = new Set(); engine._r130SeenAggQueue = []; }
      const k = String(aid);
      if (engine._r130SeenAggIds.has(k)) return;
      engine._r130SeenAggIds.add(k); engine._r130SeenAggQueue.push(k);
      while (engine._r130SeenAggQueue.length > 1200) engine._r130SeenAggIds.delete(engine._r130SeenAggQueue.shift());
    }
  } catch(_) {}
  const price  = parseFloat(trade.p);
  const qty    = parseFloat(trade.q);
  const usdt   = price * qty;
  const isBuy  = !trade.m;
  const ts     = trade.T || Date.now();
  engine.lastTickTs = ts;
  engine.lastTickWallTs = Date.now();
  if (trade.s && usdt >= 12000) r125RegisterPriorityWake(trade.s, `aggTrade ${isBuy?'BUY':'SELL'} $${Math.round(usdt)}`, Math.min(100, usdt/1000));

  engine.lastPrice = price;
  if (!Array.isArray(engine.lastTicks)) engine.lastTicks = [];
  engine.lastTicks.push({ price, isBuy, usdt, ts });
  if (engine.lastTicks.length > 500) engine.lastTicks.shift();

  const level = roundToTick(price, engine.tickSize).toFixed(8);
  if (!engine.footprint.has(level)) {
    engine.footprint.set(level, { buy:0, sell:0, delta:0 });
  }
  const fp = engine.footprint.get(level);
  if (isBuy) { fp.buy += usdt; fp.delta += usdt; engine.totalBuy += usdt; }
  else        { fp.sell += usdt; fp.delta -= usdt; engine.totalSell += usdt; }

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
  if (trade.s && c.trades >= 12 && Math.abs(c.delta) > 25000) {
    r125RegisterPriorityWake(trade.s, `liveDelta ${c.delta>0?'BUY':'SELL'} $${Math.round(Math.abs(c.delta))}`, Math.min(100, Math.abs(c.delta)/1000));
  }
  if (trade.s && c.trades % 8 === 0) {
    try {
      const ag = r126AggressionTrendFromTicks(engine.lastTicks);
      if (ag.phase === 'ACCELERATING' && ag.strength >= 35) {
        r125RegisterPriorityWake(trade.s, `aggrTrend ${ag.side} ${ag.strength}`, ag.strength);
      }
    } catch(_) {}
  }

  if (ts - c.ts >= engine.candleMs) {
    c.imbalance = detectStackedImbalance(engine.footprint, engine.tickSize);
    engine.candles.push(c);
    if (engine.candles.length > 100) engine.candles.shift();
    engine.footprint.clear();
    engine.currentCandle = null;
  }

  updateSweepDetector(engine.sweepDet, price, isBuy, usdt);

  if (usdt > 50000) {
    engine.bigTrades.push({ ts, price, usdt:+usdt.toFixed(0), side:isBuy?'BUY':'SELL' });
    if (engine.bigTrades.length > 50) engine.bigTrades.shift();
  }
}

function createSweepDetector() {
  return {
    swingHighs: [],
    swingLows:  [],
    lastTicks:  [],
    pendingSweep: null,
    confirmed: null,
    tickCount: 0,
  };
}

function calcDynamicLookback(klines5m) {
  if (!klines5m || klines5m.length < 10) return 30;
  const recent = klines5m.slice(-20);
  const highs = recent.map(k => parseFloat(k[2]));
  const lows  = recent.map(k => parseFloat(k[3]));
  const avgPrice = (Math.max(...highs) + Math.min(...lows)) / 2;
  const range = (Math.max(...highs) - Math.min(...lows)) / avgPrice * 100;

  if (range > 15) return 50;
  if (range > 8)  return 40;
  if (range > 3)  return 30;
  return 20;
}

function updateSwingLevels(det, klines5m) {
  const lookback = calcDynamicLookback(klines5m);
  if (!klines5m || klines5m.length < lookback+4) return;
  const recent = klines5m.slice(-(lookback+4));

  const wing = lookback >= 40 ? 3 : 2;
  const newHighs = [], newLows = [];

  for (let i=wing; i<recent.length-wing; i++) {
    const h = parseFloat(recent[i][2]);
    const l = parseFloat(recent[i][3]);

    let isSwH = true, isSwL = true;
    for (let w=1; w<=wing; w++) {
      if (h <= parseFloat(recent[i-w][2]) || h <= parseFloat(recent[i+w][2])) isSwH = false;
      if (l >= parseFloat(recent[i-w][3]) || l >= parseFloat(recent[i+w][3])) isSwL = false;
    }
    if (isSwH) newHighs.push({ price:h, idx:i });
    if (isSwL) newLows.push({ price:l, idx:i });
  }

  det.swingHighs = newHighs.slice(-7).map(s => s.price);
  det.swingLows  = newLows.slice(-7).map(s => s.price);
  det.lookback   = lookback;
}

function r111AnalyzeSiksmaFlow(k5m, k1h, k4h, lastPrice, fundingData, lsData, lsTopData, takerData, oiHist5mData, oiNowData) {
  const lp = Number(lastPrice);
  const no = {
    ok: false,
    squeezeBreakout: false, squeezeSkor: 0, siksmaAdet: 0,
    shortSqueeze: false, longSqueeze: false,
    demandOB: null, supplyOB: null, obBaskisi: 'YOK',
    longOk: false, shortOk: false,
    ozet: 'sıkışma yok'
  };
  if (!lp || lp <= 0) return no;

  function rows(klines, lim) {
    return (Array.isArray(klines) ? klines : []).slice(-lim)
      .map(k => ({ o:+k[1], h:+k[2], l:+k[3], c:+k[4], v:+k[5] }))
      .filter(k => k.h > 0 && Number.isFinite(k.h));
  }
  const R = v => +Number(v).toFixed(8);

  const r5  = rows(k5m, 60);
  const r1h = rows(k1h, 50);
  const r4h = rows(k4h, 40);
  if (r5.length < 20) return no;

  function calcTR(a, b) {
    if (!b) return a.h - a.l;
    return Math.max(a.h - a.l, Math.abs(a.h - b.c), Math.abs(a.l - b.c));
  }

  const trs = r5.map((k, i) => i === 0 ? k.h - k.l : calcTR(k, r5[i-1]));
  const last = r5.at(-1);
  const lastTR = trs.at(-1);

  const atr8  = trs.slice(-8).reduce((s, v) => s + v, 0) / 8;
  const atr30 = trs.slice(-30).reduce((s, v) => s + v, 0) / Math.min(30, trs.length);

  let siksmaAdet = 0;
  for (let i = trs.length - 2; i >= 0; i--) {
    const windowAvg = trs.slice(Math.max(0, i-29), i).reduce((s,v)=>s+v,0) /
                      Math.min(30, i);
    if (windowAvg > 0 && trs[i] < windowAvg * 0.70) siksmaAdet++;
    else break;
  }

  const siksmaVarMi = atr8 < atr30 * 0.65 && siksmaAdet >= 8;

  const govde = Math.abs(last.c - last.o);
  const range = last.h - last.l;
  const squeezeBreakout = siksmaVarMi &&
    lastTR > atr30 * 1.8 &&
    range > 0 && govde / range > 0.5;

  let squeezeSkor = 0;
  if (squeezeBreakout) squeezeSkor += 1;
  const prevVolRows = r5.slice(-11, -1);
  const avgVol5 = prevVolRows.length ? prevVolRows.reduce((s,k)=>s+k.v,0)/prevVolRows.length : 0;
  if (avgVol5 > 0 && last.v > avgVol5 * 1.5) squeezeSkor += 1;
  const h1Last = r1h.at(-1), h4Last = r4h.at(-1);
  const htfYon = (h1Last ? (h1Last.c > h1Last.o ? 1 : -1) : 0) +
                 (h4Last ? (h4Last.c > h4Last.o ? 1 : -1) : 0);
  const breakDir = last.c > last.o ? 1 : -1;
  if (htfYon !== 0 && breakDir === Math.sign(htfYon)) squeezeSkor += 1;
  if (siksmaAdet >= 12) squeezeSkor += 1;

  const squeezeOk = squeezeBreakout && squeezeSkor >= 3;
  const squeezeLong  = squeezeOk && last.c > last.o;
  const squeezeShort = squeezeOk && last.c < last.o;

  const fundArr = Array.isArray(fundingData) ? fundingData.slice(-3) : [];
  const fundAvg = fundArr.length
    ? fundArr.reduce((s, f) => s + Number(f.fundingRate || f.lastFundingRate || 0), 0) / fundArr.length
    : 0;
  const fundAsiriNeg = fundAvg < -0.0003;
  const fundAsiriPos = fundAvg > +0.0005;

  function parseLongShortRatio(arr) {
    const x = Array.isArray(arr) && arr.length ? arr.at(-1) : {};
    const la = Number(x.longAccount);
    const sa = Number(x.shortAccount);
    if (Number.isFinite(la) && Number.isFinite(sa) && la > 0 && sa > 0) return { long: la, short: sa };
    const ratio = Number(x.longShortRatio);
    if (Number.isFinite(ratio) && ratio > 0) return { long: ratio / (1 + ratio), short: 1 / (1 + ratio) };
    return { long: 0.5, short: 0.5 };
  }
  const lsGlobalParsed = parseLongShortRatio(lsData);
  const lsTopParsed    = parseLongShortRatio(lsTopData);
  const shortRatio = Math.max(lsGlobalParsed.short, lsTopParsed.short);
  const longRatio  = Math.max(lsGlobalParsed.long,  lsTopParsed.long);
  const shortKalabaligi = shortRatio > 0.60;
  const longKalabaligi  = longRatio  > 0.65;

  const takArr = Array.isArray(takerData) ? takerData.slice(-3) : [];
  const takBuySell = takArr.length
    ? takArr.reduce((s, t) => s + Number(t.buySellRatio || 1), 0) / takArr.length
    : 1;
  const takerBuyBaskin  = takBuySell > 1.15;
  const takerSellBaskin = takBuySell < 0.87;

  function oiVal(x) { return Number(x?.sumOpenInterestValue || x?.sumOpenInterest || x?.openInterest || 0); }
  const oi5 = Array.isArray(oiHist5mData) ? oiHist5mData.slice(-6) : [];
  const oiNow = Number(oiNowData?.openInterest || oiVal(oi5.at(-1)) || 0);
  let oiChgPct = 0;
  if (oi5.length >= 4) {
    const base = oiVal(oi5[oi5.length - 4]);
    const lat  = oiVal(oi5.at(-1));
    oiChgPct = base > 0 ? ((lat - base) / base) * 100 : 0;
  }
  const oiArtis = oiChgPct > 0.20;
  const oiDusuyor = oiChgPct < -0.30;

  const fiyatArti  = last.c > last.o;
  const fiyatDustu = last.c < last.o;

  const shortSqueeze = fundAsiriNeg && shortKalabaligi && oiArtis && fiyatArti && takerBuyBaskin;
  const longSqueeze  = fundAsiriPos && longKalabaligi  && oiArtis && fiyatDustu && takerSellBaskin;

  function findOB(rows, type) {
    if (rows.length < 6) return null;
    const avgBody = rows.slice(-20).reduce((s,k)=>s+Math.abs(k.c-k.o),0)/20;

    for (let i = rows.length - 2; i >= 3; i--) {
      const k = rows[i], kNext = rows[i+1];
      if (type === 'demand') {
        const bearBody = k.o - k.c;
        if (bearBody < avgBody * 1.5) continue;
        const upMove = (rows[i+3]?.h - k.l) / k.l * 100;
        if ((upMove || 0) > 3) {
          return { low: R(k.l), high: R(k.h), mid: R((k.l+k.h)/2), tf: '4H' };
        }
      } else {
        const bullBody = k.c - k.o;
        if (bullBody < avgBody * 1.5) continue;
        const dnMove = (k.h - (rows[i+3]?.l || k.l)) / k.h * 100;
        if ((dnMove || 0) > 3) {
          return { low: R(k.l), high: R(k.h), mid: R((k.l+k.h)/2), tf: '4H' };
        }
      }
    }
    return null;
  }

  const demandOB = findOB(r4h, 'demand');
  const supplyOB = findOB(r4h, 'supply');

  const inDemand = demandOB && lp >= demandOB.low * 0.995 && lp <= demandOB.high * 1.005;
  const inSupply = supplyOB && lp >= supplyOB.low * 0.995 && lp <= supplyOB.high * 1.005;
  const obBaskisi = inDemand ? 'DEMAND_OB' : inSupply ? 'SUPPLY_OB' : 'YOK';

  const r111LongOk = !!(
    (squeezeLong) ||
    (shortSqueeze && (inDemand || takerBuyBaskin) && !inSupply)
  );

  const r111ShortOk = !!(
    (squeezeShort) ||
    (longSqueeze && (inSupply || takerSellBaskin) && !inDemand)
  );

  const ozet = [
    siksmaVarMi ? `sıkışma:${siksmaAdet}mum` : 'sıkışma:yok',
    squeezeBreakout ? `patlama:EVET(skor:${squeezeSkor})` : '',
    shortSqueeze ? 'shortSqueeze:EVET' : '',
    longSqueeze  ? 'longSqueeze:EVET' : '',
    obBaskisi !== 'YOK' ? `OB:${obBaskisi}` : '',
    `funding:${(fundAvg*100).toFixed(3)}%`,
    `OI15m:${oiChgPct.toFixed(2)}%`,
    oiDusuyor ? 'OI:çözülüyor' : '',
  ].filter(Boolean).join(' · ');

  return {
    ok: true,
    squeezeBreakout, squeezeSkor, siksmaAdet, siksmaVarMi,
    squeezeLong, squeezeShort,
    shortSqueeze, longSqueeze,
    fundAvg: R(fundAvg), shortRatio: R(shortRatio), longRatio: R(longRatio),
    globalLongRatio: R(lsGlobalParsed.long), globalShortRatio: R(lsGlobalParsed.short),
    topLongRatio: R(lsTopParsed.long), topShortRatio: R(lsTopParsed.short),
    takBuySell: R(takBuySell), oiNow: R(oiNow), oiChgPct: R(oiChgPct), oiArtis, oiDusuyor,
    demandOB, supplyOB, obBaskisi,
    inDemand, inSupply,
    longOk: r111LongOk, shortOk: r111ShortOk,
    ozet
  };
}

function r190N(v, d=0) { const n = Number(v); return Number.isFinite(n) ? n : d; }
function r190Pct(a,b) { a=r190N(a); b=r190N(b); return b>0 ? ((a-b)/b)*100 : 0; }
function r190Rows5m(k5m=[], limit=8) {
  return (Array.isArray(k5m)?k5m:[]).slice(-limit).map(k=>({
    o:r190N(k?.[1]), h:r190N(k?.[2]), l:r190N(k?.[3]), c:r190N(k?.[4]), v:r190N(k?.[5])
  })).filter(x=>x.o>0 && x.h>0 && x.l>0 && x.c>0);
}
function r190OiVal(x) { return r190N(x?.sumOpenInterestValue || x?.sumOpenInterest || x?.openInterest || 0); }
function r190ParseLS(arr=[]) {
  const x = Array.isArray(arr) && arr.length ? arr.at(-1) : {};
  const la = r190N(x.longAccount, NaN), sa = r190N(x.shortAccount, NaN);
  if (Number.isFinite(la) && Number.isFinite(sa) && la>0 && sa>0) return { long:la, short:sa };
  const ratio = r190N(x.longShortRatio, NaN);
  if (Number.isFinite(ratio) && ratio>0) return { long:ratio/(1+ratio), short:1/(1+ratio) };
  return { long:0.5, short:0.5 };
}
function r190TakerRatio(takerArr=[]) {
  const arr = (Array.isArray(takerArr)?takerArr:[]).slice(-4).map(t => r190N(t.buySellRatio, NaN)).filter(Number.isFinite);
  if (!arr.length) return 1;
  const last2 = arr.slice(-2);
  return +(last2.reduce((s,v)=>s+v,0)/last2.length).toFixed(3);
}
function r190FundingTrend(fundArr=[]) {
  const arr = (Array.isArray(fundArr)?fundArr:[]).slice(-6).map(f=>r190N(f.fundingRate || f.lastFundingRate, NaN)).filter(Number.isFinite);
  if (arr.length < 2) return { ok:false, last:0, delta:0, rising:false, falling:false, label:'fund:yok' };
  const first = arr[0], last = arr.at(-1), prev = arr.at(-2);
  const delta = last - first;
  return {
    ok:true, last:+(last*100).toFixed(4), delta:+(delta*100).toFixed(4),
    rising:last > prev && delta > 0, falling:last < prev && delta < 0,
    label:`fund:${(last*100).toFixed(4)}% Δ${(delta*100).toFixed(4)}%`
  };
}

function r192FootprintFromTickData(tickData={}, side='LONG') {
  side = String(side||'LONG').toUpperCase();
  const isL = side === 'LONG';
  const cc = tickData?.currentCandle || {};
  const t30 = tickData?.recent30s || {};
  const buy = r190N(cc.buy,0) + r190N(t30.buy,0) * 0.45;
  const sell = r190N(cc.sell,0) + r190N(t30.sell,0) * 0.45;
  const total = buy + sell;
  const trades = r190N(cc.trades,0) + r190N(t30.trades,0);
  if (total <= 0) return { ok:false, side, ratio:0.5, deltaPct:0, total:0, aligned:false, against:false, absorption:false, label:'R192FP:yok' };
  const ratio = buy / total;
  const deltaPct = ((buy - sell) / total) * 100;
  const aligned = isL ? ratio >= 0.62 : ratio <= 0.38;
  const strongAligned = isL ? ratio >= 0.68 : ratio <= 0.32;
  const against = isL ? ratio <= 0.42 : ratio >= 0.58;
  const highVolNeutral = total >= 18000 && ratio > 0.46 && ratio < 0.54;
  const rangePct = (r190N(cc.high,0)>0 && r190N(cc.low,0)>0 && r190N(cc.close,0)>0)
    ? ((r190N(cc.high)-r190N(cc.low))/r190N(cc.close))*100 : 0;
  const absorption = !!(highVolNeutral && rangePct < 0.18);
  return {
    ok:true, side, ratio:+ratio.toFixed(3), deltaPct:+deltaPct.toFixed(1), total:+total.toFixed(0), trades,
    aligned, strongAligned, against, absorption,
    label:`R192FP ${ratio>=0.5?'BUY':'SELL'}:${(ratio*100).toFixed(0)}% Δ${deltaPct.toFixed(0)}% $${Math.round(total)}`
  };
}
function r192DeepOfiFromBook(book={}, side='LONG') {
  side = String(side||'LONG').toUpperCase();
  const isL = side === 'LONG';
  const imb = r190N(book.deepImb10, 0);
  const side10 = String(book.deepSide10 || 'NEUTRAL').toUpperCase();
  const aligned = isL ? imb >= 18 : imb <= -18;
  const strongAligned = isL ? imb >= 28 : imb <= -28;
  const against = isL ? imb <= -20 : imb >= 20;
  return {
    ok: !!(book && (book.deepBid10 || book.deepAsk10)), side, imb:+imb.toFixed(1), bookSide:side10,
    aligned, strongAligned, against, strength:r190N(book.deepStrength10,0),
    label:`R192Deep ${side10} ${imb.toFixed(1)}%`
  };
}

function r194SwingBreakDetector(side='LONG', ctx={}) {
  side = String(side||'LONG').toUpperCase();
  const isL = side === 'LONG';
  const rows = (Array.isArray(ctx.rows) && ctx.rows.length ? ctx.rows : r190Rows5m(ctx.k5m, 12)).filter(r=>r && r.c>0);
  if (rows.length < 7) return { ok:false, side, bos:false, volOk:false, flowOk:false, candleOk:false, label:'R194Swing:yetersiz' };
  const last = rows.at(-1);
  const prev = rows.slice(Math.max(0, rows.length-9), -1);
  if (!last || prev.length < 5) return { ok:false, side, bos:false, volOk:false, flowOk:false, candleOk:false, label:'R194Swing:yetersiz' };

  const pivH = [];
  const pivL = [];
  for (let i=1; i<prev.length-1; i++) {
    const a=prev[i-1], b=prev[i], c=prev[i+1];
    if (b.h > a.h && b.h >= c.h) pivH.push({ price:b.h, idx:i });
    if (b.l < a.l && b.l <= c.l) pivL.push({ price:b.l, idx:i });
  }
  const rangeHigh = Math.max(...prev.slice(-7).map(x=>x.h));
  const rangeLow  = Math.min(...prev.slice(-7).map(x=>x.l));
  const swingHigh = (pivH.at(-1)?.price || rangeHigh);
  const swingLow  = (pivL.at(-1)?.price || rangeLow);
  const breakBuf = Math.max(0.00015, r190N(ctx.spreadPct,0) * 0.35 / 100);
  const bosLong = last.c > swingHigh * (1 + breakBuf);
  const bosShort = last.c < swingLow * (1 - breakBuf);
  const bos = isL ? bosLong : bosShort;
  const breakPct = isL ? r190Pct(last.c, swingHigh) : r190Pct(swingLow, last.c);

  const vols = prev.slice(-7).map(x=>x.v).filter(v=>v>0);
  const avgVol = vols.reduce((s,v)=>s+v,0) / Math.max(1, vols.length);
  const volRatio = avgVol > 0 ? last.v / avgVol : 1;
  const fp = ctx.r192Footprint || {};
  const deep = ctx.r192DeepOfi || {};
  const volOk = volRatio >= 1.30 || (volRatio >= 1.15 && fp.aligned && deep.aligned);

  const rng = Math.max(1e-12, last.h-last.l);
  const body = Math.abs(last.c-last.o);
  const bodyShare = body / rng;
  const closePos = (last.c-last.l) / rng;
  const candleOk = isL ? (last.c > last.o && closePos >= 0.60 && bodyShare >= 0.28) : (last.c < last.o && closePos <= 0.40 && bodyShare >= 0.28);

  const taker = r190N(ctx.takerRatio, 1);
  const takerOk = isL ? taker >= 1.06 : taker <= 0.94;
  const flowOk = !!(
    (takerOk || fp.aligned || deep.strongAligned || ctx.vpinAligned || ctx.microAligned) &&
    !fp.against && !deep.against && !ctx.vpinAgainst && !ctx.microAgainst
  );

  const seq = r190N(ctx.seq, 0);
  const price3 = Math.abs(r190N(ctx.price3, 0));
  const atrPct = Math.max(0.25, r190N(ctx.atrPct, 0.8));
  const notLate = seq <= 3 && price3 <= Math.max(1.35, atrPct * 1.25);
  const notTooExtendedFromBreak = Math.abs(breakPct) <= Math.max(0.55, atrPct * 0.70);
  const ok = !!(bos && volOk && flowOk && candleOk && notLate && notTooExtendedFromBreak);
  const strong = !!(ok && volRatio >= 1.55 && (fp.strongAligned || deep.strongAligned || takerOk));
  return {
    ok, strong, side, bos, bosLong, bosShort, volOk, flowOk, candleOk, notLate, notTooExtendedFromBreak,
    swingHigh:+swingHigh.toFixed(8), swingLow:+swingLow.toFixed(8), breakPct:+breakPct.toFixed(3),
    volRatio:+volRatio.toFixed(2), bodyShare:+bodyShare.toFixed(2), closePos:+closePos.toFixed(2), seq,
    label:`R194Swing ${side} ${ok?'BOS✅':'bekle'} br:${breakPct.toFixed(2)}% vol:${volRatio.toFixed(2)}x seq:${seq}`
  };
}

function r196Rows(kl=[], n=24) {
  return (Array.isArray(kl)?kl:[]).slice(-n).map(c=>({
    o:r190N(c?.[1],0), h:r190N(c?.[2],0), l:r190N(c?.[3],0), c:r190N(c?.[4],0), v:r190N(c?.[5],0)
  })).filter(x=>x.o>0 && x.h>0 && x.l>0 && x.c>0);
}
function r196RangePct(lastPrice=0, high=0, low=0) {
  lastPrice = r190N(lastPrice,0); high = r190N(high,0); low = r190N(low,0);
  if (!(lastPrice>0 && high>low)) return null;
  return Math.max(0, Math.min(100, ((lastPrice-low)/(high-low))*100));
}
function r196RecentRejection(rows=[], side='LONG') {
  side = String(side||'LONG').toUpperCase();
  const isL = side === 'LONG';
  const arr = (Array.isArray(rows)?rows:[]).slice(-5).filter(x=>x && x.h>x.l);
  if (arr.length < 2) return { ok:false, score:0, label:'R196Reject:yok' };
  let score = 0, wickHit = 0, redHit = 0, dumpHit = 0;
  const last = arr.at(-1), prev = arr.at(-2);
  const maxH = Math.max(...arr.map(x=>x.h));
  const minL = Math.min(...arr.map(x=>x.l));
  const last3High = Math.max(...arr.slice(-3).map(x=>x.h));
  const last3Low  = Math.min(...arr.slice(-3).map(x=>x.l));
  for (const x of arr.slice(-3)) {
    const rng = Math.max(1e-12, x.h-x.l);
    const bodyHi = Math.max(x.o,x.c), bodyLo = Math.min(x.o,x.c);
    const upper = (x.h-bodyHi)/rng;
    const lower = (bodyLo-x.l)/rng;
    if (isL && upper >= 0.38 && x.h >= maxH*0.995) { wickHit++; score += 2; }
    if (!isL && lower >= 0.38 && x.l <= minL*1.005) { wickHit++; score += 2; }
    if (isL && x.c < x.o) { redHit++; score += 1; }
    if (!isL && x.c > x.o) { redHit++; score += 1; }
  }
  const moveFromExtreme = isL ? r190Pct(last.c, last3High) : r190Pct(last3Low, last.c);
  if (isL && moveFromExtreme <= -0.45) { dumpHit++; score += 2; }
  if (!isL && moveFromExtreme <= -0.45) { dumpHit++; score += 2; }
  const engulf = isL ? (last.c < prev.o && last.o >= prev.c) : (last.c > prev.o && last.o <= prev.c);
  if (engulf) score += 2;
  return { ok:score>=3, score, wickHit, redHit, dumpHit, engulf, moveFromExtreme:+moveFromExtreme.toFixed(2), label:`R196Reject ${side} s:${score} wick:${wickHit} mv:${moveFromExtreme.toFixed(2)}%` };
}
function r196Context(side='LONG', ctx={}) {
  side = String(side||'LONG').toUpperCase();
  const isL = side === 'LONG';
  const lastPrice = r190N(ctx.lastPrice,0);
  const meta = ctx.meta || {};
  const high24 = r190N(meta.high ?? meta.highPrice, 0);
  const low24  = r190N(meta.low ?? meta.lowPrice, 0);
  const chg24  = r190N(meta.change24h ?? meta.change24hRaw ?? meta.priceChangePercent, 0);
  const loc24  = r196RangePct(lastPrice, high24, low24);
  const rows5  = r196Rows(ctx.k5m, 48);
  const rows15 = r196Rows(ctx.k15m, 16);
  const rows1h = r196Rows(ctx.k1h, 30);
  const rows4h = r196Rows(ctx.k4h, 24);
  const loc1h = rows1h.length ? r196RangePct(lastPrice, Math.max(...rows1h.map(x=>x.h)), Math.min(...rows1h.map(x=>x.l))) : null;
  const loc4h = rows4h.length ? r196RangePct(lastPrice, Math.max(...rows4h.map(x=>x.h)), Math.min(...rows4h.map(x=>x.l))) : null;
  const rej5  = r196RecentRejection(rows5, side);
  const rej15 = r196RecentRejection(rows15, side);
  const avgVol = rows5.slice(-8,-1).reduce((s,x)=>s+x.v,0)/Math.max(1, rows5.slice(-8,-1).length);
  const rvol = avgVol>0 && rows5.length ? rows5.at(-1).v/avgVol : 1;
  const price60 = rows5.length>=13 ? r190Pct(rows5.at(-1).c, rows5.at(-13).o) : 0;
  const price180 = rows5.length>=37 ? r190Pct(rows5.at(-1).c, rows5.at(-37).o) : 0;
  const topLoc = Number.isFinite(loc24) && loc24 >= 86;
  const upperLoc = Number.isFinite(loc24) && loc24 >= 76;
  const bottomLoc = Number.isFinite(loc24) && loc24 <= 14;
  const lowerLoc = Number.isFinite(loc24) && loc24 <= 24;
  const htfTop = (Number.isFinite(loc1h) && loc1h >= 82) || (Number.isFinite(loc4h) && loc4h >= 82);
  const htfBottom = (Number.isFinite(loc1h) && loc1h <= 18) || (Number.isFinite(loc4h) && loc4h <= 18);
  const pumpLongRisk = isL && (topLoc || (upperLoc && chg24 >= 8) || (upperLoc && htfTop));
  const dumpShortRisk = !isL && (bottomLoc || (lowerLoc && chg24 <= -8) || (lowerLoc && htfBottom));
  const rejection = isL ? (rej5.ok || rej15.ok) : (rej5.ok || rej15.ok);
  const topExhaustion = !!(isL && pumpLongRisk && (rejection || rvol < 0.35 || price60 > 5.5 || chg24 >= 12));
  const bottomExhaustion = !!(!isL && dumpShortRisk && (rejection || rvol < 0.35 || price60 < -5.5 || chg24 <= -12));
  const hardBlock = !!(
    (isL && ((topLoc && (rejection || chg24 >= 10 || rvol < 0.45)) || (topExhaustion && (rvol < 0.75 || rejection)))) ||
    (!isL && ((bottomLoc && (rejection || chg24 <= -10 || rvol < 0.45)) || (bottomExhaustion && (rvol < 0.75 || rejection))))
  );
  const caution = !!(!hardBlock && ((isL && pumpLongRisk) || (!isL && dumpShortRisk)));
  const rescueOk = !!(
    ctx.strongFuel || ctx.squeeze || ctx.strongSwing ||
    (ctx.footprintAligned && ctx.deepAligned && rvol >= 0.75) ||
    (ctx.bodyReclaim && rvol >= 0.65)
  );
  const block = hardBlock && !rescueOk;
  const sideTxt = isL ? 'LONG' : 'SHORT';
  return {
    ok:true, side:sideTxt, loc24:Number.isFinite(loc24)?+loc24.toFixed(1):null, loc1h:Number.isFinite(loc1h)?+loc1h.toFixed(1):null, loc4h:Number.isFinite(loc4h)?+loc4h.toFixed(1):null,
    high24:+high24.toFixed(8), low24:+low24.toFixed(8), chg24:+chg24.toFixed(2), rvol:+rvol.toFixed(2), price60:+price60.toFixed(2), price180:+price180.toFixed(2),
    pumpLongRisk, dumpShortRisk, topExhaustion, bottomExhaustion, rejection, hardBlock, rescueOk, block, caution,
    label:`R196Range ${sideTxt} loc:${Number.isFinite(loc24)?loc24.toFixed(0):'?'}% 1h:${Number.isFinite(loc1h)?loc1h.toFixed(0):'?'} 4h:${Number.isFinite(loc4h)?loc4h.toFixed(0):'?'} 24h:${chg24.toFixed(1)}% rvol:${rvol.toFixed(2)} rej:${rejection?'Y':'N'}${block?' BLOCK':caution?' dikkat':''}`,
    reason: block
      ? `R196 range lokasyonu: ${sideTxt} ${Number.isFinite(loc24)?loc24.toFixed(0):'?'}% günlük range + pump/dump yorgunluğu; tepeden/dipten kovalamaz`
      : caution
        ? `R196 range dikkat: ${sideTxt} ${Number.isFinite(loc24)?loc24.toFixed(0):'?'}% günlük range; güçlü yakıt ister`
        : `R196 range temiz: ${sideTxt} loc ${Number.isFinite(loc24)?loc24.toFixed(0):'?'}%`
  };
}

function r197RangeReversalScout(side='SHORT', ctx={}) {
  side = String(side||'SHORT').toUpperCase();
  const isL = side === 'LONG';
  const rows = r196Rows(ctx.k5m, 18);
  const none = { ok:false, strong:false, side, score:0, label:`R197${side}:yok`, reason:'range reversal scout yok' };
  if (rows.length < 7) return none;
  const last = rows.at(-1), prev = rows.at(-2);
  if (!last || !prev || !(last.h>last.l) || !(prev.h>prev.l)) return none;
  const r196Long = ctx.r196Long || {};
  const r196Short = ctx.r196Short || {};
  const loc24 = Number.isFinite(Number(r196Long.loc24)) ? Number(r196Long.loc24) : Number(r196Short.loc24);
  const loc1h = Number.isFinite(Number(r196Long.loc1h)) ? Number(r196Long.loc1h) : Number(r196Short.loc1h);
  const loc4h = Number.isFinite(Number(r196Long.loc4h)) ? Number(r196Long.loc4h) : Number(r196Short.loc4h);
  const chg24 = Number.isFinite(Number(r196Long.chg24)) ? Number(r196Long.chg24) : Number(r196Short.chg24 || 0);
  const avgVol = rows.slice(-8,-1).reduce((a,x)=>a+x.v,0) / Math.max(1, rows.slice(-8,-1).length);
  const rvol = avgVol > 0 ? last.v / avgVol : 1;
  const rng = Math.max(1e-12, last.h-last.l);
  const body = Math.abs(last.c-last.o);
  const bodyShare = body/rng;
  const closePos = (last.c-last.l)/rng;
  const taker = r190TakerRatio(ctx.takerArr || []);
  const book = ctx.r125Flow?.book || {};
  const spreadPct = r190N(book.spreadPct,0);
  const fp = r192FootprintFromTickData(ctx.tickData || {}, side);
  const deep = r192DeepOfiFromBook(book, side);
  const microSide = String(book.microSide || 'NEUTRAL').toUpperCase();
  const microPersist = String(book.microPersistSide || 'NEUTRAL').toUpperCase();
  const microAligned = microSide === side || microPersist === side;
  const microAgainst = microSide === (isL?'SHORT':'LONG') && microPersist === (isL?'SHORT':'LONG');
  const rows190 = r190Rows5m(ctx.k5m, 10);
  const price3 = rows190.length >= 4 ? r190Pct(rows190.at(-1).c, rows190.at(-4).o) : 0;
  let seq = 0;
  for (let i=rows190.length-1;i>=0;i--) {
    const x=rows190[i]; if (!x) break;
    const dir = x.c > x.o ? 'LONG' : x.c < x.o ? 'SHORT' : 'FLAT';
    if (dir === side) seq++; else break;
  }
  const swing = r194SwingBreakDetector(side, {
    rows: rows190, takerRatio:taker, rvol, price3, seq, atrPct:ctx.atrPct, spreadPct,
    r192Footprint:fp, r192DeepOfi:deep,
    vpinAligned:false, vpinAgainst:false, microAligned, microAgainst
  });

  const topZone = !!(Number.isFinite(loc24) && loc24 >= 76) || !!(Number.isFinite(loc1h) && loc1h >= 82) || !!(Number.isFinite(loc4h) && loc4h >= 82) || !!r196Long.block || !!r196Long.caution || !!r196Long.topExhaustion;
  const bottomZone = !!(Number.isFinite(loc24) && loc24 <= 24) || !!(Number.isFinite(loc1h) && loc1h <= 18) || !!(Number.isFinite(loc4h) && loc4h <= 18) || !!r196Short.block || !!r196Short.caution || !!r196Short.bottomExhaustion;
  const zoneOk = isL ? bottomZone : topZone;
  const rejection = r196RecentRejection(rows, isL ? 'SHORT' : 'LONG');
  const prev3 = rows.slice(-4,-1);
  const prev3High = Math.max(...prev3.map(x=>x.h));
  const prev3Low = Math.min(...prev3.map(x=>x.l));
  const mss = isL
    ? (last.c > prev.h || last.c > prev3High || (last.c > last.o && closePos >= 0.68 && bodyShare >= 0.42))
    : (last.c < prev.l || last.c < prev3Low || (last.c < last.o && closePos <= 0.32 && bodyShare >= 0.42));
  const failedBreak = !isL
    ? (last.h >= Math.max(...rows.slice(-7).map(x=>x.h))*0.995 && last.c < prev.c && closePos <= 0.45)
    : (last.l <= Math.min(...rows.slice(-7).map(x=>x.l))*1.005 && last.c > prev.c && closePos >= 0.55);
  const takerAligned = isL ? taker >= 1.06 : taker <= 0.94;
  const flowAligned = !!(takerAligned || fp.aligned || deep.aligned || deep.strongAligned || swing.strong || microAligned);
  const flowAgainst = !!(fp.against || deep.against || microAgainst || (isL ? taker <= 0.88 : taker >= 1.14));
  const notLate = seq <= 3 && Math.abs(price3) <= Math.max(2.0, r190N(ctx.atrPct,1)*1.35);
  const locNotExtremeAgainst = isL ? !(Number.isFinite(loc24) && loc24 > 70) : !(Number.isFinite(loc24) && loc24 < 30);
  const setupProof = !!(rejection.ok || mss || failedBreak || swing.ok);
  const ok = !!(zoneOk && setupProof && flowAligned && !flowAgainst && notLate && locNotExtremeAgainst && spreadPct < 0.18);
  const strong = !!(ok && (mss || swing.strong) && (rejection.ok || fp.aligned || deep.strongAligned) && rvol >= 0.35);
  let score = 0;
  if (zoneOk) score += 4;
  if (rejection.ok) score += 5;
  if (mss) score += 6;
  if (failedBreak) score += 4;
  if (flowAligned) score += 5;
  if (fp.aligned) score += 3;
  if (deep.strongAligned) score += 4;
  if (swing.strong) score += 5;
  if (rvol >= 0.75) score += 2;
  if (flowAgainst) score -= 8;
  if (!notLate) score -= 6;
  return {
    ok, strong, side, score, zoneOk, loc24:Number.isFinite(loc24)?+loc24.toFixed(1):null,
    loc1h:Number.isFinite(loc1h)?+loc1h.toFixed(1):null, loc4h:Number.isFinite(loc4h)?+loc4h.toFixed(1):null,
    chg24:+chg24.toFixed(2), rvol:+rvol.toFixed(2), takerRatio:taker,
    rejection, mss, failedBreak, flowAligned, flowAgainst, notLate, seq, price3:+price3.toFixed(2),
    footprint:fp, deepOfi:deep, swing,
    label:`R197RangeReversal ${side} ${ok?'ADAY✅':'bekle'} loc:${Number.isFinite(loc24)?loc24.toFixed(0):'?'}% rej:${rejection.ok?'Y':'N'} mss:${mss?'Y':'N'} flow:${flowAligned?'Y':'N'} rvol:${rvol.toFixed(2)} taker:${taker}`,
    reason: ok
      ? `R197 range ters-yön scout: ${side} için ${isL?'dip/discount':'tepe/premium'} + rejection/MSS + canlı akış`
      : `R197 range ters-yön bekle: ${side} için zone:${zoneOk?'Y':'N'} proof:${setupProof?'Y':'N'} flow:${flowAligned?'Y':'N'} against:${flowAgainst?'Y':'N'}`
  };
}

function r190OiVector(oiHist5m=[], oiNowObj=null) {
  const arr = (Array.isArray(oiHist5m)?oiHist5m:[]).slice(-8).map(r190OiVal).filter(v=>v>0);
  const nowOi = r190OiVal(oiNowObj);
  if (nowOi>0 && (!arr.length || Math.abs(nowOi-arr.at(-1))/Math.max(1,arr.at(-1)) > 0.0001)) arr.push(nowOi);
  if (arr.length < 4) return { ok:false, oiChg15:0, lastVel:0, prevVel:0, accel:0, trend:'FLAT', label:'OI vektör:yok' };
  const a0 = arr.at(-4), a1 = arr.at(-3), a2 = arr.at(-2), a3 = arr.at(-1);
  const v1 = r190Pct(a1,a0), v2 = r190Pct(a2,a1), v3 = r190Pct(a3,a2);
  const oiChg15 = r190Pct(a3,a0);
  const accel = v3 - v2;
  const rising = v1 > -0.05 && v2 > -0.05 && v3 > -0.05 && oiChg15 > 0.15;
  const accelerating = rising && (v3 >= v2 - 0.03) && (v2 >= v1 - 0.10);
  const decel = oiChg15 > 0.25 && accel < -0.18;
  const falling = oiChg15 < -0.25;
  const trend = accelerating ? 'ACCEL_UP' : rising ? 'UP' : decel ? 'DECEL_UP' : falling ? 'DOWN' : 'FLAT';
  return { ok:true, oiChg15:+oiChg15.toFixed(3), lastVel:+v3.toFixed(3), prevVel:+v2.toFixed(3), accel:+accel.toFixed(3), trend, accelerating, rising, decel, falling, label:`OI15:${oiChg15.toFixed(2)}% v:${v3.toFixed(2)} a:${accel.toFixed(2)} ${trend}` };
}
function r190Analyze5mEarlyEdge(side='LONG', ctx={}) {
  side = String(side || 'LONG').toUpperCase();
  const isL = side === 'LONG';
  const opp = isL ? 'SHORT' : 'LONG';
  const rows = r190Rows5m(ctx.k5m, 10);
  const last = rows.at(-1) || null;
  const first3 = rows.at(-4) || rows[0] || null;
  const first5 = rows.at(-6) || rows[0] || null;
  const price3 = (last && first3) ? r190Pct(last.c, first3.o) : 0;
  const price5 = (last && first5) ? r190Pct(last.c, first5.o) : 0;
  const dir3Ok = isL ? price3 > 0.18 : price3 < -0.18;
  const dir5Abs = Math.abs(price5);
  let seq = 0;
  for (let i=rows.length-1; i>=Math.max(0, rows.length-6); i--) {
    const bull = rows[i].c >= rows[i].o;
    if (isL ? bull : !bull) seq++; else break;
  }
  const avgVol = rows.slice(-8,-1).reduce((s,x)=>s+x.v,0) / Math.max(1, rows.slice(-8,-1).length);
  const rvol = avgVol>0 && last ? last.v/avgVol : 1;
  const rangeHi = rows.length ? Math.max(...rows.slice(-8).map(x=>x.h)) : 0;
  const rangeLo = rows.length ? Math.min(...rows.slice(-8).map(x=>x.l)) : 0;
  const rangePos = (last && rangeHi>rangeLo) ? (last.c-rangeLo)/(rangeHi-rangeLo) : 0.5;
  const book = ctx.r125Flow?.book || {};
  const spreadPct = r190N(book.spreadPct,0);
  const vpin = ctx.tickData?.vpin || null;
  const vpinDir = String(vpin?.direction || 'NEUTRAL').toUpperCase();
  const vpinToxic = String(vpin?.toxicity || 'LOW').toUpperCase();
  const vpinHigh = ['MEDIUM','HIGH','EXTREME'].includes(vpinToxic);
  const vpinAligned = !!(vpinHigh && ((isL && vpinDir === 'BUY_DOMINANT') || (!isL && vpinDir === 'SELL_DOMINANT')));
  const vpinAgainst = !!(vpinHigh && ((isL && vpinDir === 'SELL_DOMINANT') || (!isL && vpinDir === 'BUY_DOMINANT')));
  const takerRatio = r190TakerRatio(ctx.takerArr);
  const takerAligned = isL ? takerRatio >= 1.08 : takerRatio <= 0.93;
  const takerStrong = isL ? takerRatio >= 1.25 : takerRatio <= 0.80;
  const takerDivergence = isL ? (price3 > 0.15 && takerRatio < 0.90) : (price3 < -0.15 && takerRatio > 1.10);
  const oi = r190OiVector(ctx.oiHist5m, ctx.oiNowObj);
  const oiAccelAligned = !!(oi.ok && oi.accelerating);
  const oiDecelLate = !!(oi.ok && oi.decel && Math.abs(price5) > 0.7);
  const fund = r190FundingTrend(ctx.fundArr);
  const lsG = r190ParseLS(ctx.lsGlobal);
  const lsT = r190ParseLS(ctx.lsTop);
  const shortCrowded = Math.max(lsG.short, lsT.short) >= 0.58;
  const longCrowded  = Math.max(lsG.long,  lsT.long)  >= 0.62;
  const squeezeLong = !!(isL && price3 > 0.35 && oi.oiChg15 > 0.35 && takerRatio >= 1.22 && shortCrowded);
  const squeezeShort= !!(!isL && price3 < -0.35 && oi.oiChg15 > 0.35 && takerRatio <= 0.82 && longCrowded);
  const squeeze = squeezeLong || squeezeShort;
  const r192Footprint = r192FootprintFromTickData(ctx.tickData || {}, side);
  const r192DeepOfi = r192DeepOfiFromBook(book, side);
  const sqzImminentLong = !!(isL && shortCrowded && oi.rising && fund.last <= 0.02 && takerRatio >= 0.98 && !takerDivergence);
  const sqzImminentShort = !!(!isL && longCrowded && oi.rising && fund.last >= -0.02 && takerRatio <= 1.02 && !takerDivergence);
  const r192SqzImminent = !!((sqzImminentLong || sqzImminentShort) && spreadPct < 0.18);
  const microSide = String(book.microSide || 'NEUTRAL').toUpperCase();
  const microPersist = String(book.microPersistSide || 'NEUTRAL').toUpperCase();
  const microAligned = microSide === side || microPersist === side;
  const microAgainst = microSide === opp && microPersist === opp;
  const r194SwingBreak = r194SwingBreakDetector(side, {
    rows, takerRatio, rvol, price3, seq, atrPct: ctx.atrPct, spreadPct,
    r192Footprint, r192DeepOfi, vpinAligned, vpinAgainst, microAligned, microAgainst
  });
  const tooLate = !!(seq >= 4 || (dir5Abs > Math.max(1.05, r190N(ctx.atrPct,1)*0.90) && (isL ? rangePos > 0.82 : rangePos < 0.18)) || oiDecelLate);
  const momentumWindow = !!(dir3Ok && rvol >= 0.75 && oi.rising && takerAligned && !takerDivergence && !microAgainst && !tooLate);
  const earlyContinuation = !!((momentumWindow && seq <= 3 && (oiAccelAligned || vpinAligned || takerStrong || squeeze || r192Footprint.aligned || r192DeepOfi.strongAligned) && spreadPct <= 0.12) || r194SwingBreak.strong);
  const spreadCost15x = spreadPct * 15;
  const spreadBlock = !!(spreadPct >= 0.18 || spreadCost15x >= 3.2);
  const fakePump = !!(isL && (takerDivergence || (price3 > 0.45 && oi.falling) || (fund.rising && fund.last > 0.06 && longCrowded && !squeeze)));
  const fakeDump = !!(!isL && (takerDivergence || (price3 < -0.45 && oi.falling) || (fund.falling && fund.last < -0.06 && shortCrowded && !squeeze)));
  const r192LiveAgainst = !!((r192Footprint.against && r192Footprint.total >= 2500) || (r192DeepOfi.against && !squeeze));
  const r192FuelScore =
    (earlyContinuation ? 4 : 0) + (squeeze ? 5 : 0) + (r192SqzImminent ? 3 : 0) +
    (r192Footprint.strongAligned ? 4 : r192Footprint.aligned ? 2 : 0) +
    (r192DeepOfi.strongAligned ? 3 : r192DeepOfi.aligned ? 1 : 0) +
    (r194SwingBreak.strong ? 4 : r194SwingBreak.ok ? 3 : 0) +
    (oiAccelAligned ? 2 : 0) + (takerStrong ? 2 : 0) + (microAligned ? 1 : 0) -
    (r192Footprint.against ? 4 : 0) - (r192DeepOfi.against ? 3 : 0) - (takerDivergence ? 4 : 0);
  const r192FuelOk = r192FuelScore >= 6 && !spreadBlock && !takerDivergence && !r192LiveAgainst;
  const lateTrapRisk = !!((tooLate && !squeeze && !earlyContinuation && !r192FuelOk) || fakePump || fakeDump || vpinAgainst || spreadBlock || (r192LiveAgainst && !r192FuelOk));
  let score = 0;
  const tags = [];
  const add=(ok,pts,tag)=>{ if(ok){ score += pts; tags.push(tag); } };
  add(earlyContinuation && !r194SwingBreak.ok, 18, 'earlyContinuation');
  add(r194SwingBreak.ok, r194SwingBreak.strong ? 20 : 8, r194SwingBreak.label);
  add(momentumWindow && !earlyContinuation, 8, 'momentumWindow');
  add(oiAccelAligned, 8, oi.label);
  add(vpinAligned, 7, `VPIN:${vpin?.vpin||0}/${vpinToxic}/${vpinDir}`);
  add(takerStrong, 7, `taker:${takerRatio}`);
  add(takerAligned && !takerStrong, 4, `taker:${takerRatio}`);
  add(squeeze, 16, isL?'shortSqueezeFuel':'longSqueezeFuel');
  add(r192SqzImminent && !squeeze, 5, 'R192_SQZ_IMMINENT');
  add(r192Footprint.aligned, r192Footprint.strongAligned ? 6 : 4, r192Footprint.label);
  add(r192DeepOfi.aligned, r192DeepOfi.strongAligned ? 5 : 3, r192DeepOfi.label);
  add(r192FuelOk && !earlyContinuation && !squeeze, 4, `R192Fuel:${r192FuelScore}`);
  add(microAligned, 5, `micro:${microSide}/${microPersist}`);
  if (r192Footprint.against) { score -= 8; tags.push('R192FootprintAgainst'); }
  if (r192DeepOfi.against) { score -= 6; tags.push('R192DeepAgainst'); }
  if (lateTrapRisk) { score -= 18; tags.push('lateTrapRisk'); }
  if (tooLate) { score -= 10; tags.push(`lateSeq:${seq}`); }
  if (takerDivergence) { score -= 12; tags.push(`takerDivergence:${takerRatio}`); }
  if (spreadBlock) { score -= 16; tags.push(`spreadBlock:${spreadPct}%`); }
  return {
    ok:true, side, score, tags:tags.slice(0,8),
    earlyEntryOk: earlyContinuation,
    momentumWindow, earlyContinuation, lateTrapRisk, tooLate, seq, rvol:+rvol.toFixed(2), price3:+price3.toFixed(2), price5:+price5.toFixed(2), rangePos:+rangePos.toFixed(2),
    oi, takerRatio, takerAligned, takerDivergence, vpinAligned, vpinAgainst, vpin, microAligned, microAgainst,
    squeeze, shortCrowded, longCrowded, funding:fund, spreadPct:+spreadPct.toFixed(4), spreadBlock,
    r192Footprint, r192DeepOfi, r194SwingBreak, r192SqzImminent, r192FuelScore:+r192FuelScore.toFixed(1), r192FuelOk, r192LiveAgainst,
    summary:`R190/R192/R194 ${side} ${earlyContinuation?'ERKEN':'izle'} fuel:${r192FuelScore.toFixed(1)} score:${score} p3:${price3.toFixed(2)}% seq:${seq} rvol:${rvol.toFixed(2)} taker:${takerRatio} ${oi.label} ${vpin?`VPIN:${vpin.vpin}/${vpinToxic}/${vpinDir}`:'VPIN:yok'} ${r192Footprint.label} ${r192DeepOfi.label} ${r194SwingBreak.label} spread:${spreadPct.toFixed(4)}%`
  };
}

function r114Analyze5mShift(k5m) {
  const no = {
    ok:false, bullShift:false, bearShift:false, longReclaim:false, shortReclaim:false,
    net4Pct:0, net9Pct:0, net14Pct:0, red4:0, green4:0, avgRangePct:0,
    fromHighPct:0, fromLowPct:0, lastClosePos:0.5, ozet:'5m veri yok'
  };
  const rows = (Array.isArray(k5m) ? k5m : []).slice(-30)
    .map(k => ({ o:+k[1], h:+k[2], l:+k[3], c:+k[4], v:+k[5] }))
    .filter(k => k.o > 0 && k.h > 0 && k.l > 0 && k.c > 0 && Number.isFinite(k.c));
  if (rows.length < 10) return no;
  const R = (x, d=2) => +Number(x || 0).toFixed(d);
  const pct = (a,b) => b > 0 ? ((a-b)/b)*100 : 0;
  const last = rows.at(-1), prev = rows.at(-2);
  const lastRange = Math.max(last.h - last.l, 0);
  const lastClosePos = lastRange > 0 ? (last.c - last.l) / lastRange : 0.5;
  const avgRangePct = rows.slice(-12).reduce((s,k)=>s+((k.h-k.l)/k.c*100),0) / Math.min(12, rows.length);
  const fastThr = Math.max(0.55, avgRangePct * 0.80);
  const medThr  = Math.max(1.05, avgRangePct * 1.35);
  const net4Pct  = pct(last.c, rows.at(-5)?.c || rows[0].c);
  const net9Pct  = pct(last.c, rows.at(-10)?.c || rows[0].c);
  const net14Pct = pct(last.c, rows.at(-15)?.c || rows[0].c);
  const last4 = rows.slice(-4);
  const red4 = last4.filter(k => k.c < k.o).length;
  const green4 = last4.filter(k => k.c > k.o).length;
  const last2Down = rows.slice(-2).every(k => k.c < k.o) && last.c < prev.c;
  const last2Up   = rows.slice(-2).every(k => k.c > k.o) && last.c > prev.c;
  const prev5 = rows.slice(-7, -1);
  const prevSwingHigh = Math.max(...prev5.map(k=>k.h));
  const prevSwingLow  = Math.min(...prev5.map(k=>k.l));
  const recent14 = rows.slice(-14);
  const recentHigh = Math.max(...recent14.map(k=>k.h));
  const recentLow  = Math.min(...recent14.map(k=>k.l));
  const fromHighPct = recentHigh > 0 ? (recentHigh - last.c) / recentHigh * 100 : 0;
  const fromLowPct  = recentLow  > 0 ? (last.c - recentLow) / recentLow * 100 : 0;

  const bearShift = !!(
    net4Pct <= -fastThr || net9Pct <= -medThr ||
    (red4 >= 3 && fromHighPct >= fastThr) ||
    (last2Down && last.c < prevSwingLow * 1.001)
  );
  const bullShift = !!(
    net4Pct >= fastThr || net9Pct >= medThr ||
    (green4 >= 3 && fromLowPct >= fastThr) ||
    (last2Up && last.c > prevSwingHigh * 0.999)
  );

  const longReclaim = !!(
    (last.c > last.o && lastClosePos >= 0.60 && last.c > prev.h * 0.999) ||
    (last.c > prevSwingHigh * 1.0005 && lastClosePos >= 0.55)
  );
  const shortReclaim = !!(
    (last.c < last.o && lastClosePos <= 0.40 && last.c < prev.l * 1.001) ||
    (last.c < prevSwingLow * 0.9995 && lastClosePos <= 0.45)
  );

  const ozet = `5m shift net4:${R(net4Pct)}% net9:${R(net9Pct)}% red:${red4}/4 green:${green4}/4 pos:${R(lastClosePos*100,0)}% ${bearShift?'BEAR_SHIFT':''}${bullShift?'BULL_SHIFT':''}${longReclaim?' LONG_RECLAIM':''}${shortReclaim?' SHORT_RECLAIM':''}`.trim();
  return { ok:true, bullShift, bearShift, longReclaim, shortReclaim,
    net4Pct:R(net4Pct), net9Pct:R(net9Pct), net14Pct:R(net14Pct), red4, green4,
    avgRangePct:R(avgRangePct), fromHighPct:R(fromHighPct), fromLowPct:R(fromLowPct),
    lastClosePos:R(lastClosePos,3), ozet };
}

function r110AnalyzeICT(k5m, k15m, k1h, k4h, lastPrice) {
  const lp = Number(lastPrice);
  const no = { ok: false, phase: 'BEKLE', direction: null, sslLevel: null, bslLevel: null,
    swept: false, bodyClose: false, choch: false, fvg: null, entryOk: false,
    longOk: false, shortOk: false, dashboardText: '', liquidityMapText: '' };
  if (!lp || lp <= 0) return no;

  function rows(klines, limit) {
    return (Array.isArray(klines) ? klines : []).slice(-limit)
      .map(k => ({ t:+k[0], o:+k[1], h:+k[2], l:+k[3], c:+k[4], v:+k[5] }))
      .filter(k => k.o > 0 && k.h > 0 && k.l > 0 && k.c > 0 && Number.isFinite(k.c));
  }
  const R = (v, d=8) => +Number(v || 0).toFixed(d);
  const pctAbs = (a, b) => b > 0 ? Math.abs(a - b) / b * 100 : 999;
  const pctSigned = (a, b) => b > 0 ? ((a - b) / b) * 100 : 0;

  const r5  = rows(k5m,  80);
  const r15 = rows(k15m, 120);
  const r1h = rows(k1h,  120);
  const r4h = rows(k4h,   80);
  if (r5.length < 12 || r15.length < 12) return no;

  function avgRangePct(rs, n=20) {
    const a = rs.slice(-n);
    if (!a.length) return 0.35;
    return a.reduce((s,k)=>s+((k.h-k.l)/k.c*100),0) / a.length;
  }
  const ar5 = avgRangePct(r5, 20);
  const zonePct = Math.min(0.55, Math.max(0.14, ar5 * 0.22));
  const piercePct = Math.min(0.50, Math.max(0.08, ar5 * 0.18));

  function detectSwings(rs) {
    const lows = [], highs = [];
    for (let i = 2; i < rs.length - 2; i++) {
      const k = rs[i];
      if (k.l <= rs[i-1].l && k.l <= rs[i-2].l && k.l <= rs[i+1].l && k.l <= rs[i+2].l) lows.push({ price:R(k.l), idx:i });
      if (k.h >= rs[i-1].h && k.h >= rs[i-2].h && k.h >= rs[i+1].h && k.h >= rs[i+2].h) highs.push({ price:R(k.h), idx:i });
    }
    return { lows, highs };
  }

  function buildLevels(rs, tf) {
    const tfWeight = tf === '4H' ? 4 : tf === '1H' ? 3 : 2;
    const minClusterPct = tf === '4H' ? 0.45 : tf === '1H' ? 0.35 : 0.28;
    const { lows, highs } = detectSwings(rs);
    function cluster(swings, type) {
      const out = [];
      for (const sw of swings) {
        let c = out.find(x => pctAbs(x.price, sw.price) <= minClusterPct);
        if (!c) {
          out.push({ price: sw.price, touches: 1, lastIdx: sw.idx, type, tf, tfWeight, age: rs.length - 1 - sw.idx });
        } else {
          c.price = R((c.price * c.touches + sw.price) / (c.touches + 1));
          c.touches += 1;
          c.lastIdx = Math.max(c.lastIdx, sw.idx);
          c.age = rs.length - 1 - c.lastIdx;
        }
      }
      return out.map(x => {
        const touchScore = Math.min(4, x.touches);
        const freshScore = x.age <= 12 ? 1 : 0;
        const strength = Math.min(10, x.tfWeight + touchScore + freshScore);
        return { ...x, strength, label: x.tf, dist: R(pctAbs(lp, x.price), 3) };
      });
    }
    return { ssl: cluster(lows, 'SSL'), bsl: cluster(highs, 'BSL') };
  }

  const lev4h = buildLevels(r4h, '4H');
  const lev1h = buildLevels(r1h, '1H');
  const lev15 = buildLevels(r15, '15m');

  function mergeAndRank(list, side) {
    const clusters = [];
    for (const l of list) {
      const m = clusters.find(c => pctAbs(c.price, l.price) <= Math.max(0.18, zonePct));
      if (!m) clusters.push({ ...l, sources:[`${l.tf}:${l.touches}`], score:l.strength });
      else {
        const w1 = m.score || 1, w2 = l.strength || 1;
        m.price = R((m.price*w1 + l.price*w2)/(w1+w2));
        m.score += l.strength;
        m.strength = Math.min(10, Math.max(m.strength, l.strength) + 1);
        m.sources.push(`${l.tf}:${l.touches}`);
        if (l.tfWeight > m.tfWeight) { m.tf = l.tf; m.tfWeight = l.tfWeight; m.label = l.tf; }
      }
    }
    return clusters
      .filter(l => side === 'SSL' ? l.price < lp * 1.006 : l.price > lp * 0.994)
      .filter(l => l.strength >= 4 || l.tf === '1H' || l.tf === '4H')
      .map(l => ({ ...l, dist: R(pctAbs(lp, l.price), 3), zoneLow:R(l.price*(1-zonePct/100)), zoneHigh:R(l.price*(1+zonePct/100)) }))
      .sort((a,b)=> side === 'SSL' ? b.price - a.price : a.price - b.price);
  }

  const allSSL = mergeAndRank([...lev4h.ssl, ...lev1h.ssl, ...lev15.ssl], 'SSL');
  const allBSL = mergeAndRank([...lev4h.bsl, ...lev1h.bsl, ...lev15.bsl], 'BSL');
  const nearSSL = allSSL[0] || null;
  const nearBSL = allBSL[0] || null;

  const nearSSLDist = nearSSL ? nearSSL.dist : 999;
  const nearBSLDist = nearBSL ? nearBSL.dist : 999;
  const approachingSSL = !!(nearSSL && nearSSLDist <= Math.max(0.75, ar5 * 1.15));
  const approachingBSL = !!(nearBSL && nearBSLDist <= Math.max(0.75, ar5 * 1.15));

  const last5 = r5.at(-1), prev5 = r5.at(-2);
  let sslSwept = false, sslSweepLevel = 0, sslSweepMumIdx = -1, sslSweepQuality = 0;
  let bslSwept = false, bslSweepLevel = 0, bslSweepMumIdx = -1, bslSweepQuality = 0;

  function candleStats(m) {
    const range = Math.max(m.h - m.l, 0);
    const body = Math.abs(m.c - m.o);
    const bodyLow = Math.min(m.o, m.c);
    const bodyHigh = Math.max(m.o, m.c);
    const lowerWick = Math.max(0, bodyLow - m.l);
    const upperWick = Math.max(0, m.h - bodyHigh);
    const closePos = range > 0 ? (m.c - m.l) / range : 0.5;
    return { range, body, bodyLow, bodyHigh, lowerWick, upperWick, closePos };
  }

  if (nearSSL && approachingSSL) {
    for (let i = 0; i < Math.min(6, r5.length); i++) {
      const m = r5.at(-(i+1));
      const st = candleStats(m);
      const pierced = m.l < nearSSL.zoneLow * (1 - piercePct/100);
      const reclaimedClose = m.c >= nearSSL.zoneLow;
      const bodyNotBroken = st.bodyHigh >= nearSSL.zoneLow && m.c >= nearSSL.zoneLow;
      const wickDominant = st.range > 0 && st.lowerWick / st.range >= 0.28 && st.lowerWick >= Math.max(st.body * 0.65, nearSSL.price * 0.00025);
      if (pierced && reclaimedClose && bodyNotBroken && wickDominant) {
        sslSwept = true; sslSweepLevel = nearSSL.price; sslSweepMumIdx = i;
        sslSweepQuality = (wickDominant?2:0) + (m.c > m.o ? 1 : 0) + (st.closePos >= 0.55 ? 1 : 0) + Math.min(3, Math.floor(nearSSL.strength/3));
        break;
      }
    }
  }

  if (nearBSL && approachingBSL) {
    for (let i = 0; i < Math.min(6, r5.length); i++) {
      const m = r5.at(-(i+1));
      const st = candleStats(m);
      const pierced = m.h > nearBSL.zoneHigh * (1 + piercePct/100);
      const reclaimedClose = m.c <= nearBSL.zoneHigh;
      const bodyNotBroken = st.bodyLow <= nearBSL.zoneHigh && m.c <= nearBSL.zoneHigh;
      const wickDominant = st.range > 0 && st.upperWick / st.range >= 0.28 && st.upperWick >= Math.max(st.body * 0.65, nearBSL.price * 0.00025);
      if (pierced && reclaimedClose && bodyNotBroken && wickDominant) {
        bslSwept = true; bslSweepLevel = nearBSL.price; bslSweepMumIdx = i;
        bslSweepQuality = (wickDominant?2:0) + (m.c < m.o ? 1 : 0) + (st.closePos <= 0.45 ? 1 : 0) + Math.min(3, Math.floor(nearBSL.strength/3));
        break;
      }
    }
  }

  const lastStats = candleStats(last5);
  const prevSwingHigh = Math.max(...r5.slice(-8, -1).map(k => k.h));
  const prevSwingLow  = Math.min(...r5.slice(-8, -1).map(k => k.l));
  const avgBody = r5.slice(-12).reduce((s,k)=>s+Math.abs(k.c-k.o),0) / Math.min(12, r5.length);

  const bullishChoCH = !!(
    sslSwept && sslSweepMumIdx >= 1 &&
    last5.c > last5.o && lastStats.closePos >= 0.55 &&
    (last5.c > prev5.h * 0.999 || last5.c > prevSwingHigh * 0.999) &&
    last5.c > sslSweepLevel
  );
  const bearishChoCH = !!(
    bslSwept && bslSweepMumIdx >= 1 &&
    last5.c < last5.o && lastStats.closePos <= 0.45 &&
    (last5.c < prev5.l * 1.001 || last5.c < prevSwingLow * 1.001) &&
    last5.c < bslSweepLevel
  );

  let bullishFVG = null, bearishFVG = null;
  for (let i = 1; i < Math.min(7, r5.length - 1); i++) {
    const m1 = r5.at(-(i+2));
    const m3 = r5.at(-i);
    if (!m1 || !m3) continue;
    if (m1.h < m3.l && !bullishFVG) bullishFVG = { low:R(m1.h), high:R(m3.l), midpoint:R((m1.h+m3.l)/2) };
    if (m1.l > m3.h && !bearishFVG) bearishFVG = { low:R(m3.h), high:R(m1.l), midpoint:R((m3.h+m1.l)/2) };
  }
  const inBullishFVG = bullishFVG && lp >= bullishFVG.low && lp <= bullishFVG.high;
  const inBearishFVG = bearishFVG && lp <= bearishFVG.high && lp >= bearishFVG.low;

  const bullMssBody = bullishChoCH && lastStats.body >= avgBody * 1.05 && lastStats.closePos >= 0.60;
  const bearMssBody = bearishChoCH && lastStats.body >= avgBody * 1.05 && lastStats.closePos <= 0.40;
  const longEntryOk = !!(sslSwept && bullishChoCH && (bullishFVG || inBullishFVG || bullMssBody) && sslSweepQuality >= 3);
  const shortEntryOk = !!(bslSwept && bearishChoCH && (bearishFVG || inBearishFVG || bearMssBody) && bslSweepQuality >= 3);

  const phase =
    longEntryOk  ? 'HTF_SSL_SWEEP_BODY_RECLAIM_LONG_HAZIR' :
    shortEntryOk ? 'HTF_BSL_SWEEP_BODY_RECLAIM_SHORT_HAZIR' :
    bullishChoCH ? 'SSL_ALINDI_MSS_VAR_FVG_VEYA_GOVDE_BEKLE' :
    bearishChoCH ? 'BSL_ALINDI_MSS_VAR_FVG_VEYA_GOVDE_BEKLE' :
    sslSwept     ? 'SSL_ALINDI_CHOCH_BEKLENIYOR' :
    bslSwept     ? 'BSL_ALINDI_CHOCH_BEKLENIYOR' :
    approachingSSL ? 'HTF_SSL_SEVIYESINDE_5M_MUM_IZLENIYOR' :
    approachingBSL ? 'HTF_BSL_SEVIYESINDE_5M_MUM_IZLENIYOR' :
    'SEVIYE_UZAK_BEKLE';

  const mapParts = [];
  if (nearSSL) mapParts.push(`SSL:${nearSSL.price}(${nearSSL.tf},g:${nearSSL.strength},u:${nearSSLDist}%,z:${nearSSL.zoneLow}-${nearSSL.zoneHigh})`);
  if (nearBSL) mapParts.push(`BSL:${nearBSL.price}(${nearBSL.tf},g:${nearBSL.strength},u:${nearBSLDist}%,z:${nearBSL.zoneLow}-${nearBSL.zoneHigh})`);
  const dashParts = [`R115:${phase}`];
  if (mapParts.length) dashParts.push(mapParts.join(' | '));
  if (sslSwept) dashParts.push(`✅SSL wick+body-reclaim q${sslSweepQuality}`);
  if (bslSwept) dashParts.push(`✅BSL wick+body-reclaim q${bslSweepQuality}`);
  if (bullishChoCH) dashParts.push('✅BullishMSS');
  if (bearishChoCH) dashParts.push('✅BearishMSS');
  if (bullishFVG) dashParts.push(`FVG_bull:${bullishFVG.low}-${bullishFVG.high}`);
  if (bearishFVG) dashParts.push(`FVG_bear:${bearishFVG.low}-${bearishFVG.high}`);

  return {
    ok: true,
    phase,
    longOk: longEntryOk,
    shortOk: shortEntryOk,
    entryOk: longEntryOk || shortEntryOk,
    direction: longEntryOk ? 'LONG' : shortEntryOk ? 'SHORT' : null,
    nearSSL: nearSSL ? { price: nearSSL.price, dist: R(nearSSLDist,3), strength: nearSSL.strength, label: nearSSL.tf, zoneLow:nearSSL.zoneLow, zoneHigh:nearSSL.zoneHigh } : null,
    nearBSL: nearBSL ? { price: nearBSL.price, dist: R(nearBSLDist,3), strength: nearBSL.strength, label: nearBSL.tf, zoneLow:nearBSL.zoneLow, zoneHigh:nearBSL.zoneHigh } : null,
    sslSwept, bslSwept,
    sslSweepQuality, bslSweepQuality,
    bullishChoCH, bearishChoCH,
    bullishFVG, bearishFVG,
    inBullishFVG, inBearishFVG,
    approachingSSL, approachingBSL,
    bslRejection: (function(){
      try {
        const m = last5; if (!m || !nearBSL) return false;
        const rng = Math.max(m.h - m.l, 1e-12);
        const upperWick = (m.h - Math.max(m.o, m.c)) / rng;
        const touchedBSL = m.h >= (nearBSL.zoneLow || nearBSL.price) * 0.999;
        return !!(touchedBSL && upperWick >= 0.35 && m.c < (nearBSL.zoneLow || nearBSL.price) && nearBSLDist <= 0.6);
      } catch(_) { return false; }
    })(),
    sslRejection: (function(){
      try {
        const m = last5; if (!m || !nearSSL) return false;
        const rng = Math.max(m.h - m.l, 1e-12);
        const lowerWick = (Math.min(m.o, m.c) - m.l) / rng;
        const touchedSSL = m.l <= (nearSSL.zoneHigh || nearSSL.price) * 1.001;
        return !!(touchedSSL && lowerWick >= 0.35 && m.c > (nearSSL.zoneHigh || nearSSL.price) && nearSSLDist <= 0.6);
      } catch(_) { return false; }
    })(),
    oteZone: (function(){
      try {
        if (!Array.isArray(r5) || r5.length < 12) return { inOte:false, side:null, depth:0 };
        const seg = r5.slice(-24);
        let hi = -Infinity, lo = Infinity, hiIdx = -1, loIdx = -1;
        for (let i = 0; i < seg.length; i++) {
          if (seg[i].h > hi) { hi = seg[i].h; hiIdx = i; }
          if (seg[i].l < lo) { lo = seg[i].l; loIdx = i; }
        }
        const legRange = hi - lo;
        if (legRange <= 0) return { inOte:false, side:null, depth:0 };
        const upLeg = loIdx < hiIdx;
        if (upLeg) {
          const oteHigh = hi - legRange * 0.62;
          const oteLow  = hi - legRange * 0.79;
          const inOte = lp <= oteHigh && lp >= oteLow;
          const depth = inOte ? (hi - lp) / legRange : 0;
          return { inOte, side:'LONG', depth:+depth.toFixed(3), oteLow:R(oteLow), oteHigh:R(oteHigh) };
        } else {
          const oteLow  = lo + legRange * 0.62;
          const oteHigh = lo + legRange * 0.79;
          const inOte = lp >= oteLow && lp <= oteHigh;
          const depth = inOte ? (lp - lo) / legRange : 0;
          return { inOte, side:'SHORT', depth:+depth.toFixed(3), oteLow:R(oteLow), oteHigh:R(oteHigh) };
        }
      } catch(_) { return { inOte:false, side:null, depth:0 }; }
    })(),
    dashboardText: dashParts.join(' · '),
    liquidityMapText: mapParts.join(' | '),
    allSSL: allSSL.slice(0, 3),
    allBSL: allBSL.slice(0, 3),
  };
}

function r281ProTraderMap(side='LONG', ctx={}) {
  side = String(side || 'LONG').toUpperCase();
  const isL = side === 'LONG';
  const opp = isL ? 'SHORT' : 'LONG';
  const lp = Number(ctx.lastPrice || 0);
  const atrPct = Math.max(0.2, Number(ctx.atrPct || 1));
  const R = (x,d=4)=>Number.isFinite(Number(x)) ? +Number(x).toFixed(d) : 0;
  const pct = (a,b)=>Number(b)>0 ? ((Number(a)-Number(b))/Number(b))*100 : 999;
  const rows = (kl=[], limit=80)=>Array.isArray(kl) ? kl.slice(-limit).map(k=>({
    o:Number(k?.[1]), h:Number(k?.[2]), l:Number(k?.[3]), c:Number(k?.[4]), v:Number(k?.[5])
  })).filter(k=>k.o>0&&k.h>0&&k.l>0&&k.c>0&&k.h>=k.l) : [];
  const pivots = (rs=[], span=2)=>{
    const highs=[], lows=[];
    for(let i=span;i<rs.length-span;i++){
      const x=rs[i];
      let hi=true, lo=true;
      for(let j=i-span;j<=i+span;j++) if(j!==i){ if(rs[j].h>=x.h) hi=false; if(rs[j].l<=x.l) lo=false; }
      if(hi) highs.push({price:x.h, idx:i, age:rs.length-1-i});
      if(lo) lows.push({price:x.l, idx:i, age:rs.length-1-i});
    }
    return {highs, lows};
  };
  const structure = (rs=[], tf='5m', weight=1)=>{
    const p=pivots(rs,2), hs=p.highs.slice(-3), ls=p.lows.slice(-3);
    const h1=hs.at(-1), h0=hs.at(-2), l1=ls.at(-1), l0=ls.at(-2);
    const hh=!!(h1&&h0&&h1.price>h0.price*1.001);
    const lh=!!(h1&&h0&&h1.price<h0.price*0.999);
    const hl=!!(l1&&l0&&l1.price>l0.price*1.001);
    const ll=!!(l1&&l0&&l1.price<l0.price*0.999);
    const trend = hh&&hl ? 'UP_HH_HL' : lh&&ll ? 'DOWN_LH_LL' : hh&&ll ? 'EXPANSION' : lh&&hl ? 'SQUEEZE' : 'MIXED';
    let fav=0,risk=0,notes=[];
    if(isL){ if(trend==='UP_HH_HL'){fav+=3*weight;notes.push(`${tf} HH/HL`);} if(trend==='DOWN_LH_LL'){risk+=3*weight;notes.push(`${tf} LL/LH karşı`);} }
    else { if(trend==='DOWN_LH_LL'){fav+=3*weight;notes.push(`${tf} LL/LH`);} if(trend==='UP_HH_HL'){risk+=3*weight;notes.push(`${tf} HH/HL karşı`);} }
    return {trend, fav, risk, notes, lastHigh:h1?R(h1.price,8):null, lastLow:l1?R(l1.price,8):null, highs:hs, lows:ls};
  };
  const fvgScan = (rs=[], tf='5m', weight=1)=>{
    const bull=[], bear=[];
    for(let i=0;i<rs.length-2;i++){
      const a=rs[i], c=rs[i+2];
      if(a.h < c.l){
        const low=a.h, high=c.l, mid=(low+high)/2, width=pct(high,low);
        const after=rs.slice(i+3);
        const mitigated=after.some(x=>x.l<=mid);
        const dist=lp>0 ? Math.min(Math.abs(pct(low,lp)), Math.abs(pct(high,lp))) : 999;
        bull.push({tf,low:R(low,8),high:R(high,8),mid:R(mid,8),width:R(width,3),age:rs.length-1-(i+2),mitigated,dist:R(dist,3),inZone:lp>=low&&lp<=high,near:dist<=Math.max(0.35,atrPct*0.55)});
      }
      if(a.l > c.h){
        const low=c.h, high=a.l, mid=(low+high)/2, width=pct(high,low);
        const after=rs.slice(i+3);
        const mitigated=after.some(x=>x.h>=mid);
        const dist=lp>0 ? Math.min(Math.abs(pct(low,lp)), Math.abs(pct(high,lp))) : 999;
        bear.push({tf,low:R(low,8),high:R(high,8),mid:R(mid,8),width:R(width,3),age:rs.length-1-(i+2),mitigated,dist:R(dist,3),inZone:lp>=low&&lp<=high,near:dist<=Math.max(0.35,atrPct*0.55)});
      }
    }
    const liveBull=bull.filter(z=>!z.mitigated || z.inZone || z.near).sort((a,b)=>a.dist-b.dist)[0]||null;
    const liveBear=bear.filter(z=>!z.mitigated || z.inZone || z.near).sort((a,b)=>a.dist-b.dist)[0]||null;
    let fav=0,risk=0,notes=[];
    const same=isL?liveBull:liveBear, against=isL?liveBear:liveBull;
    if(same && (same.inZone||same.near)){ fav += (same.inZone?4:2.5)*weight; notes.push(`${tf} ${isL?'bull':'bear'} FVG ${same.inZone?'içi':'yakın'}`); }
    if(against && (against.inZone||against.near)){ risk += (against.inZone?4:2.5)*weight; notes.push(`${tf} karşı FVG ${against.inZone?'içi':'yakın'}`); }
    return {bull:liveBull,bear:liveBear,fav,risk,notes};
  };
  const wickField = (rs=[], tf='5m', weight=1, nearMult=1)=>{
    let upper=0, lower=0, upperNear=null, lowerNear=null;
    const nearPct=Math.max(0.45, Math.min(6, atrPct*nearMult));
    for(const x of rs.slice(-36)){
      const rng=x.h-x.l; if(!(rng>0)||!(lp>0)) continue;
      const bodyHi=Math.max(x.o,x.c), bodyLo=Math.min(x.o,x.c);
      const uw=(x.h-bodyHi)/rng, lw=(bodyLo-x.l)/rng;
      const du=pct(x.h,lp), dl=pct(lp,x.l);
      if(du>=-0.10 && du<=nearPct && uw>=0.38){ upper += uw*(1+(nearPct-Math.max(0,du))/nearPct)*weight; if(!upperNear||Math.abs(du)<Math.abs(upperNear.dist)) upperNear={price:R(x.h,8),dist:R(du,3),wick:R(uw,2),tf}; }
      if(dl>=-0.10 && dl<=nearPct && lw>=0.38){ lower += lw*(1+(nearPct-Math.max(0,dl))/nearPct)*weight; if(!lowerNear||Math.abs(dl)<Math.abs(lowerNear.dist)) lowerNear={price:R(x.l,8),dist:R(dl,3),wick:R(lw,2),tf}; }
    }
    const upperWall=upper>=2.2*weight, lowerWall=lower>=2.2*weight;
    let fav=0,risk=0,notes=[];
    if(isL){ if(lowerWall){fav+=3*weight;notes.push(`${tf} alt iğne likidite alımı`);} if(upperWall){risk+=3*weight;notes.push(`${tf} üst iğne duvarı SHORT bias`);} }
    else { if(upperWall){fav+=3*weight;notes.push(`${tf} üst iğne likidite alımı`);} if(lowerWall){risk+=3*weight;notes.push(`${tf} alt iğne duvarı LONG bias`);} }
    return {upper:R(upper,2),lower:R(lower,2),upperWall,lowerWall,upperNear,lowerNear,fav,risk,notes};
  };
  const fibOte = (rs=[], tf='5m', weight=1)=>{
    const p=pivots(rs,2); const pts=[];
    for(const h of p.highs.slice(-4)) pts.push({...h,type:'H'});
    for(const l of p.lows.slice(-4)) pts.push({...l,type:'L'});
    pts.sort((a,b)=>a.idx-b.idx);
    if(pts.length<2 || !lp) return {inOte:false,fav:0,risk:0,notes:[]};
    let a=pts.at(-2), b=pts.at(-1);
    if(a.type===b.type && pts.length>=3) a=pts.at(-3);
    const hi=Math.max(a.price,b.price), lo=Math.min(a.price,b.price), rng=hi-lo;
    if(!(rng>0)) return {inOte:false,fav:0,risk:0,notes:[]};
    const upLeg=a.type==='L' && b.type==='H';
    const downLeg=a.type==='H' && b.type==='L';
    let oteLow, oteHigh, oteSide=null, depth=0;
    if(upLeg){ oteHigh=hi-rng*0.618; oteLow=hi-rng*0.786; oteSide='LONG'; depth=(hi-lp)/rng; }
    else if(downLeg){ oteLow=lo+rng*0.618; oteHigh=lo+rng*0.786; oteSide='SHORT'; depth=(lp-lo)/rng; }
    else return {inOte:false,fav:0,risk:0,notes:[]};
    const inOte=lp>=oteLow&&lp<=oteHigh;
    let fav=0,risk=0,notes=[];
    if(inOte && oteSide===side){ fav += 4*weight; notes.push(`${tf} Fibo OTE ${R(depth*100,0)}%`); }
    if(inOte && oteSide===opp){ risk += 3*weight; notes.push(`${tf} karşı OTE`); }
    return {inOte,side:oteSide,depth:R(depth,3),oteLow:R(oteLow,8),oteHigh:R(oteHigh,8),fav,risk,notes};
  };
  const pack = [
    {tf:'5m',  rs:rows(ctx.k5m,80),  w:1.25, near:0.90},
    {tf:'15m', rs:rows(ctx.k15m,90), w:1.55, near:1.35},
    {tf:'1h',  rs:rows(ctx.k1h,90),  w:1.15, near:2.10},
    {tf:'4h',  rs:rows(ctx.k4h,70),  w:0.90, near:3.20},
  ];
  let favorable=0, risk=0; const notes=[]; const detail={};
  for(const p of pack){
    if(p.rs.length<8) continue;
    const st=structure(p.rs,p.tf,p.w), fv=fvgScan(p.rs,p.tf,p.w), wk=wickField(p.rs,p.tf,p.w,p.near), fib=fibOte(p.rs,p.tf,p.w);
    favorable += st.fav+fv.fav+wk.fav+fib.fav; risk += st.risk+fv.risk+wk.risk+fib.risk;
    notes.push(...st.notes,...fv.notes,...wk.notes,...fib.notes);
    detail[p.tf]={structure:st.trend,fvg:{bull:fv.bull,bear:fv.bear},wick:{upper:wk.upper,lower:wk.lower,upperNear:wk.upperNear,lowerNear:wk.lowerNear},fib};
  }
  const ict=ctx.ict||{};
  const nearSSL=ict.nearSSL||null, nearBSL=ict.nearBSL||null;
  const sameSweepOk = isL ? (ict.longOk || (ict.sslSwept && ict.bullishChoCH)) : (ict.shortOk || (ict.bslSwept && ict.bearishChoCH));
  const oppositeLiquidityNear = isL ? (nearBSL && Number(nearBSL.dist)<=0.55 && !ict.bslSwept) : (nearSSL && Number(nearSSL.dist)<=0.55 && !ict.sslSwept);
  const sameLiquiditySwept = isL ? ict.sslSwept : ict.bslSwept;
  const sameFvg = isL ? ict.inBullishFVG : ict.inBearishFVG;
  const againstFvg = isL ? ict.inBearishFVG : ict.inBullishFVG;
  if(sameSweepOk){ favorable += 8; notes.push(`r110 ${isL?'SSL':'BSL'} sweep+MSS`); }
  else if(sameLiquiditySwept){ favorable += 4; notes.push(`r110 ${isL?'SSL':'BSL'} likidite alındı`); }
  if(sameFvg){ favorable += 4; notes.push('r110 FVG mitigasyon lehte'); }
  if(againstFvg){ risk += 4; notes.push('r110 karşı FVG'); }
  if(oppositeLiquidityNear){ risk += 5; notes.push(`karşı ${isL?'BSL':'SSL'} alınmamış iğne ucu yakın`); }
  if(isL && ict.bslRejection){ risk += 6; notes.push('BSL reddi: üst iğne SHORT bias'); }
  if(!isL && ict.sslRejection){ risk += 6; notes.push('SSL reddi: alt iğne LONG bias'); }
  if(isL && ict.sslRejection){ favorable += 5; notes.push('SSL reddi LONG güç toplama'); }
  if(!isL && ict.bslRejection){ favorable += 5; notes.push('BSL reddi SHORT güç toplama'); }
  const ote=ict.oteZone||{};
  if(ote.inOte && ote.side===side){ favorable += Number(ote.depth||0)>=0.70 ? 7 : 5; notes.push(`r110 OTE ${Math.round(Number(ote.depth||0)*100)}%`); }
  if(ote.inOte && ote.side===opp){ risk += 4; notes.push('r110 karşı OTE'); }

  const fuelOk = !!(ctx.r190Edge?.earlyContinuation || ctx.r190Edge?.squeeze || ctx.r190Edge?.r192FuelOk || sameSweepOk || sameFvg || (ote.inOte && ote.side===side));
  const five = detail['5m'] || {};
  const fiveAgainstWick = isL ? (five.wick?.upper >= 2.6) : (five.wick?.lower >= 2.6);
  const htfAgainstWall = (isL
    ? ((detail['15m']?.wick?.upper||0) + (detail['1h']?.wick?.upper||0)*0.75 + (detail['4h']?.wick?.upper||0)*0.55)
    : ((detail['15m']?.wick?.lower||0) + (detail['1h']?.wick?.lower||0)*0.75 + (detail['4h']?.wick?.lower||0)*0.55)) >= 4.2;
  const hardNo = !!(!fuelOk && risk >= 10 && (oppositeLiquidityNear || htfAgainstWall || fiveAgainstWick) && favorable < 7);
  const runner = !!(favorable >= 12 && risk <= 6 && fuelOk);
  const protect = !!(!runner && (risk >= 7 || (oppositeLiquidityNear && !sameSweepOk) || againstFvg));
  const net = favorable - risk;
  const bias = hardNo ? opp : net >= 5 ? side : net <= -6 ? opp : 'NEUTRAL';
  return {
    ok:true, side, bias, favorable:R(favorable,1), risk:R(risk,1), net:R(net,1), fuelOk, runner, protect, hardNo,
    summary:`R281 ProMap ${side} net:${R(net,1)} fav:${R(favorable,1)} risk:${R(risk,1)} bias:${bias}${runner?' RUNNER':''}${protect?' PROTECT':''}${hardNo?' HARD-NO':''} · ${notes.slice(0,6).join(' · ')}`,
    notes:notes.slice(0,12), detail
  };
}

function r300SimpleBrain(side, d, ctx={}) {
  const aiRunner = !!ctx.aiRunner;
  const isL = side === 'LONG';
  const txt = String(d.brainSummary || d.reason || '') + ' ' + String(d.r125OrderflowSummary || '');
  const n = (v,dv=0)=>Number.isFinite(Number(v))?Number(v):dv;

  const rangePos = n(d.r276RangePos ?? d._r276RangePos ?? ctx.rangePos, 0.5);
  const rsi = (()=>{ const dv=Number(d.rsi); if(Number.isFinite(dv)&&dv>0) return dv; const m=txt.match(/RSI4?s?[:\s]+([0-9.]+)/i); return m?Number(m[1]):50; })();
  const score = n(d.score, 0);
  const minScore = n(ctx.minScore, 70);
  const edge = n(d.brainConfidence ?? d.edge, 0);
  const htfDists = [...txt.matchAll(/karşı\s+(?:15m|1H|4H|1h|4h)[^u]*u:%\s*([0-9.]+)/gi)].map(m=>Number(m[1])).filter(Number.isFinite);
  const htfCounterDist = htfDists.length ? Math.min(...htfDists) : 999;
  const strongProof = !!(d.r117TrapSweepTaken
    || /SSL_ALINDI_CHOCH|BSL_ALINDI_CHOCH|wick\+body[- ]?(geri|reclaim)/i.test(txt));
  const realProof = !!(strongProof || d.r117BodyReclaimOk || d.r117MssOk || /body[- ]?reclaim q[0-9]/i.test(txt));
  const delta = n(d.r125LiveDeltaPct, 0);
  const bm = txt.match(/book:\s*(YÜKSELİŞ|DÜŞÜŞ|LONG|SHORT|NEUTRAL)\s*imb:\s*(-?[0-9.]+)%/i);
  const bookSide = bm ? (/YÜKSELİŞ|LONG/i.test(bm[1])?'LONG':/DÜŞÜŞ|SHORT/i.test(bm[1])?'SHORT':'NEUTRAL') : 'NEUTRAL';
  const bookImb = bm ? Math.abs(Number(bm[2])||0) : 0;
  const dmm = txt.match(/deep:\s*(YÜKSELİŞ|DÜŞÜŞ|LONG|SHORT|NEUTRAL)\s*(-?[0-9.]+)%/i);
  const deepSide = dmm ? (/YÜKSELİŞ|LONG/i.test(dmm[1])?'LONG':/DÜŞÜŞ|SHORT/i.test(dmm[1])?'SHORT':'NEUTRAL') : 'NEUTRAL';
  const deepPct = dmm ? Math.abs(Number(dmm[2])||0) : 0;
  const dirOk = isL ? (s)=>s==='LONG' : (s)=>s==='SHORT';
  let flowSignals = 0;
  if (isL ? delta>=14 : delta<=-14) flowSignals++;
  if (dirOk(bookSide) && bookImb>=30) flowSignals++;
  if (dirOk(deepSide) && deepPct>=20) flowSignals++;
  if (isL ? /taker:\s*1\.[2-9]|taker:\s*[2-9]/i.test(txt) : /taker:\s*0\.[0-7]/i.test(txt)) flowSignals++;
  const flowAligned = flowSignals >= 2;
  const flowStrong = flowSignals >= 3;
  const shortSqueeze = !!(d.r111?.shortSqueeze || /shortSqueeze:EVET/i.test(txt));
  const longSqueeze = !!(d.r111?.longSqueeze || /longSqueeze:EVET/i.test(txt));
  const fundMatch = txt.match(/funding:\s*(-?[0-9.]+)%/i);
  const funding = fundMatch ? Number(fundMatch[1]) : 0;
  const watchWeak = /kalite\/edge yetersiz|İZLE:/i.test(txt);
  const atTop = rangePos >= 0.84 || rsi >= 78;
  const atBottom = rangePos <= 0.16 || rsi <= 22;

  const safetySpread = !!(ctx.spreadBlock || d.spreadBlock);
  const safetyFakePump = !!((ctx.lateTrapRisk || d.lateTrapRisk) && !(d.r111?.squeeze || ctx.squeeze));
  const safetyChartTrap = !!((ctx.chartHardNo || d.chartHardNo) && !(ctx.runner || d.runner));
  const safetyAtrExtreme = !!(ctx.atrExtreme || d.atrExtreme);
  const safetyRrLow = !!(ctx.rrLow || d.rrLow);
  if (safetySpread)    return { allow:false, side, reason:`R300-0 GÜVENLİK: spread/kayma riski yüksek (POOR likidite)` };
  if (safetyFakePump)  return { allow:false, side, reason:`R300-0 GÜVENLİK: sahte pump/dump veya geç-giriş tuzağı` };
  if (safetyChartTrap) return { allow:false, side, reason:`R300-0 GÜVENLİK: grafik tuzağı (karşı likidite yakın + HTF duvar)` };
  if (safetyAtrExtreme) return { allow:false, side, reason:`R300-0 GÜVENLİK: ATR aşırı volatilite (düşen bıçak riski)` };
  if (safetyRrLow)     return { allow:false, side, reason:`R300-0 GÜVENLİK: risk/ödül oranı çok düşük` };

  if (watchWeak && !strongProof && !flowStrong && !aiRunner) {
    return { allow:false, side, reason:`R300-1 RED: bot 'yetersiz/İZLE' dedi (skor ${score}, gerçek sweep+reclaim yok, akış ${flowSignals}/4) — mum tek başına yetmez, gerçek kanıt ya da güçlü akış (3+) lazım` };
  }

  if (!aiRunner && isL && atTop && !realProof) {
    return { allow:false, side, reason:`R300-2 RED: tepede LONG (rangePos ${rangePos.toFixed(2)}, RSI ${rsi.toFixed(0)}), gerçek dönüş kanıtı yok (akış tek başına yetmez)` };
  }
  if (!aiRunner && !isL && atBottom && !realProof) {
    return { allow:false, side, reason:`R300-2 RED: dipte SHORT (rangePos ${rangePos.toFixed(2)}, RSI ${rsi.toFixed(0)}), gerçek dönüş kanıtı yok (akış tek başına yetmez)` };
  }

  if (htfCounterDist <= 0.6 && !realProof && !aiRunner) {
    return { allow:false, side, reason:`R300-3 RED: HTF karşı seviye %${htfCounterDist.toFixed(2)} yakın, kırılım yok` };
  }

  if (isL && (longSqueeze || funding >= 0.05) && !aiRunner) {
    return { allow:false, side, reason:`R300-4 RED: LONG ama longSqueeze/funding+ (MM aşağı taşıyabilir)` };
  }
  if (!isL && (shortSqueeze || funding <= -0.05) && !aiRunner) {
    return { allow:false, side, reason:`R300-4 RED: SHORT ama shortSqueeze/funding- (MM yukarı taşıyabilir)` };
  }

  if (!aiRunner && !realProof && !flowAligned && score < minScore) {
    return { allow:false, side, reason:`R300-5 RED: akış teyidi yok (flow ${flowSignals}/2, skor ${score})` };
  }

  return { allow:true, side, reason:`R300 EVET: ${realProof?'kırılım kanıtı':''}${flowAligned?` akış ${flowSignals}/4`:''}${score>=minScore?` skor ${score}`:''} temiz` };
}

function r283TradeRecipe(side='LONG', d={}, opt={}) {
  side = String(side || 'LONG').toUpperCase();
  const isL = side === 'LONG';
  const n = (v,def=0)=>Number.isFinite(Number(v))?Number(v):def;
  const txt = [d.reason, d.brainSummary, d.htfTani, d.mumOzet, d.entryPermissionReason].filter(Boolean).join(' ');
  const map = d.r281ProMap || {};
  const ict = d.r110ICT || d._r278ICT || {};
  const e = d.r190Edge || {};
  const r289 = d.r289Playbook || {};
  const r290 = d.r290Smc5m || {};
  const r291 = d.r291Confluence || {};
  const edge = n(d.brainConfidence ?? d.r142CalibratedEdge ?? opt.edge, 0);
  const score = n(d.score ?? opt.score, 0);
  const minScore = n(opt.minScore ?? opt.effectiveMinScore, 70);
  const flowSide = String(d.r125BestSide || d.r125Flow?.bestSide || '').toUpperCase();
  const delta = n(d.r125LiveDeltaPct ?? d.r125Flow?.deltaPct, 0);
  const flowAligned = (flowSide === side) || (isL ? delta >= 14 : delta <= -14);
  const flowAgainst = (flowSide && flowSide !== 'NEUTRAL' && flowSide !== side) || (isL ? delta <= -22 : delta >= 22) || !!e.liveOpposite;
  const real5mTrigger = !!(
    d.r285TraderSetupOk || d.r291TraderSetupOk || r291.tradeOk || d.r289TraderSetupOk || r289.tradeOk || d.r283RealChartStructure || d.r160Q4Proof || d.r117TrapSweepTaken || d.r117BodyReclaimOk || d.r117MssOk ||
    d.r118CandleOk || d.r118CandleStrong || e?.r194SwingBreak?.strong ||
    (e?.r194SwingBreak?.ok && e?.r192FuelOk) ||
    (isL ? d.r29?.r197?.long?.strong : d.r29?.r197?.short?.strong) ||
    /body-reclaim|MSS|ChoCH|sweep\+|SSL_ALINDI|BSL_ALINDI|EngulfingConfirmed|DarkCloudConfirmed|PiercingConfirmed/i.test(txt)
  );
  const fvgOteOk = !!(
    d.r285FvgOteOk || d.r291FvgOteOk || r291.fvgRetest || r291.oteOk || d.r289FvgOteOk || r289.fvgRetest || (isL ? ict.inBullishFVG : ict.inBearishFVG) ||
    (ict.oteZone?.inOte && String(ict.oteZone?.side||'').toUpperCase() === side) ||
    /FVG.*(içi|yakın|mitigasyon|entry)|OTE/i.test(String(map.summary||'') + ' ' + txt)
  );
  const continuationFuel = !!(d.r285ContinuationFuel || d.r291LiquidityFuel || d.r289ContinuationFuel || r289.continuation || e.earlyContinuation || e.squeeze || e.r192FuelOk || e.r192SqzImminent || e.vpinAligned || e.r192FuelScore >= 6);
  const chartFuel = !!(real5mTrigger || fvgOteOk || continuationFuel || map.runner);
  const weakCandle = !!(!d.mumOnay && !d.mumGuclu && /formasyon yok|teyidi zayıf|puan 0\/12|mum onay yok/i.test(txt));
  const r288P3 = n(e.price3, 0);
  const r288Rvol = n(e.rvol ?? d.r140Rvol?.ratio, 0);
  const r288FuelScore = n(e.r192FuelScore ?? e.score, 0);
  const r288Oi15Abs = Math.abs(n(e.oi?.oiChg15 ?? d.r140OiVel?.velocity, 0));
  const r288FastBypassNoRecipe = !!(
    /R156 TOP10 hızlı bypass|hızlı bypass|ek katmanlı kapı yok/i.test(txt) &&
    score < minScore && weakCandle && !d.r118CandleStrong && !e.squeeze &&
    (r288FuelScore < 8 || r288Rvol < 0.80 || Math.abs(r288P3) >= 1.80 || r288Oi15Abs >= 500)
  );
  const r160NeutralStructureTrap = !!(d.r160TraderDecision && d.r160TrueCount >= 4 && !d.r283RealChartStructure && !d.r160Q4Proof && !fvgOteOk && !continuationFuel);
  const htfAgainst = !!(d.htfBlok || d.r116HtfGuardBlock || /HTF.*karşı|karşı .*BSL|karşı .*SSL|karşı FVG|üst iğne|alt iğne/i.test(txt + ' ' + String(map.summary||'')));
  const fvgInvalid = /FVG invalid|FVG geçersiz|FVG gecersiz/i.test(txt);
  const htfCounterDists = [...String(txt).matchAll(/karşı\s+(?:15m|1H|4H|1h|4h)[^u]*u:%\s*([0-9]+(?:\.[0-9]+)?)/gi)].map(m=>Number(m[1])).filter(Number.isFinite);
  const htfCounterDistPct = htfCounterDists.length ? Math.min(...htfCounterDists) : 999;
  const realBreakProof = !!(d.r117BodyReclaimOk || d.r117MssOk || d.r117TrapSweepTaken || d.r160Q4Proof || e?.r194SwingBreak?.strong || /body-reclaim|MSS|ChoCH|sweep\+|SSL_ALINDI_CHOCH|BSL_ALINDI_CHOCH/i.test(txt));
  const r294HtfCounterHardBlock = !!(htfAgainst && htfCounterDistPct <= 0.6 && !realBreakProof);
  const r294TargetRadarBlock = !!(/hedef\/likidite yakın/i.test(txt) && /ters yön radar/i.test(txt) && !realBreakProof);
  const r294FvgOteEffective = fvgOteOk && !fvgInvalid;
  const mapRisk = n(map.risk,0), mapFav = n(map.favorable,0), mapNet = n(map.net, mapFav-mapRisk);
  const hardNo = !!(map.hardNo || r291.hardNo || r289.hardNo || r288FastBypassNoRecipe || r294HtfCounterHardBlock || r294TargetRadarBlock || (r160NeutralStructureTrap && weakCandle) || (!chartFuel && weakCandle && (htfAgainst || flowAgainst || mapRisk >= 8)) || (flowAgainst && weakCandle && !real5mTrigger && !continuationFuel));
  const runner = !!(!hardNo && (map.runner || (continuationFuel && flowAligned && edge >= 55 && mapRisk <= mapFav + 5) || (real5mTrigger && fvgOteOk && flowAligned && mapNet >= 3)));
  const quality = Math.max(0, Math.min(100,
    (edge*0.32) + (score*0.18) + (real5mTrigger?18:0) + (r294FvgOteEffective?12:0) + (continuationFuel?12:0) + (flowAligned?10:0) + (runner?10:0)
    - (weakCandle?10:0) - (flowAgainst?16:0) - (htfAgainst && !realBreakProof ? 16:0) - (r294HtfCounterHardBlock?20:0) - (Math.max(0,mapRisk-mapFav)*1.8)
  ));
  let tradeOk = !hardNo;
  if (!chartFuel && quality < 62) tradeOk = false;
  if (r288FastBypassNoRecipe) tradeOk = false;
  if (r160NeutralStructureTrap && quality < 70) tradeOk = false;
  if (weakCandle && flowAgainst && !runner) tradeOk = false;
  const mode = !tradeOk ? 'WAIT_RECIPE' : runner ? 'RUNNER' : (map.protect || weakCandle || htfAgainst || quality < 64) ? 'TACTICAL' : 'NORMAL';
  const notes = [];
  if(real5mTrigger) notes.push('5m gerçek tetik var'); else notes.push('5m tetik zayıf');
  if(fvgOteOk) notes.push('FVG/OTE/mitigasyon lehte');
  if(continuationFuel) notes.push('devam yakıtı var');
  if(flowAligned) notes.push('canlı akış aynı yön');
  if(flowAgainst) notes.push('canlı akış ters');
  if(weakCandle) notes.push('mum kanıtı zayıf');
  if(htfAgainst) notes.push('HTF/likidite baskısı var');
  if(r294HtfCounterHardBlock) notes.push(`R294 HTF KARŞI SERT KAPI: ${side} emir yok — 1H/4H karşı seviye %${htfCounterDistPct.toFixed(2)} yakın, gerçek kırılım yok (FVG retest bahanesi geçmez)`);
  if(r294TargetRadarBlock) notes.push(`R294 hedef/radar kapısı: ${side} emir yok — hedef/likidite yakın + ters yön radar, kırılım teyidi yok`);
  if(fvgInvalid && fvgOteOk) notes.push('R294: FVG invalid → FVG/OTE gerçek tetik sayılmadı');
  if(r160NeutralStructureTrap) notes.push('R160 4/4 nötr-yapı yanılgısı');
  if(r289.summary) notes.push(String(r289.summary).slice(0,130));
  if(r288FastBypassNoRecipe) notes.push(`R288 R156 bypass: skor ${score}/${minScore}, mum yok, fuel ${r288FuelScore}, rvol ${r288Rvol.toFixed(2)}, p3 ${r288P3.toFixed(2)}%, OI15 ${r288Oi15Abs.toFixed(0)}%`);
  if(map.summary) notes.push(String(map.summary).slice(0,130));
  return { ok:true, tradeOk, mode, runner, quality:+quality.toFixed(1), hardNo, chartFuel, real5mTrigger, fvgOteOk, continuationFuel, flowAligned, flowAgainst, weakCandle, htfAgainst, r160NeutralStructureTrap, r288FastBypassNoRecipe, mapFav, mapRisk, mapNet, notes:notes.slice(0,8), summary:`R283 ${mode} q:${quality.toFixed(1)} ${tradeOk?'ONAY':'BEKLE'} · ${notes.slice(0,5).join(' · ')}` };
}

function r284ProTraderWaitUpgrade(side='LONG', d={}, opt={}) {
  side = String(side || 'LONG').toUpperCase();
  const isL = side === 'LONG';
  const n = (v,def=0)=>Number.isFinite(Number(v))?Number(v):def;
  const txt = [d.reason, d.brainSummary, d.htfTani, d.mumOzet, d.ictDashboard, d.r125OrderflowSummary, d.r126FlowSummary].filter(Boolean).join(' ');
  const lowTxt = txt.toLowerCase();
  const score = n(opt.score ?? d.score, 0);
  const minScore = n(opt.minScore ?? opt.effectiveMinScore, 70);
  const edge = n(d.brainConfidence ?? d.r142CalibratedEdge, 0);
  const pri = n(d.priorityScore, 0);
  const flowSide = String(d.r125BestSide || d.r125Flow?.bestSide || '').toUpperCase();
  const delta = n(d.r125LiveDeltaPct ?? d.r125Flow?.deltaPct, 0);
  const flowAligned = (flowSide === side) || (isL ? delta >= 18 : delta <= -18) || (isL ? /R125\s*YÜKSELİŞ|R125\s*LONG/i.test(txt) : /R125\s*DÜŞÜŞ|R125\s*SHORT/i.test(txt));
  const flowAgainst = (flowSide && flowSide !== 'NEUTRAL' && flowSide !== side) || (isL ? delta <= -30 : delta >= 30);

  const sweptReclaim = isL
    ? /SSL_ALINDI|SSL wick\+body|HTF_SSL_SEVIYESINDE|SSL.*body[- ]?geri|SSL.*reclaim/i.test(txt)
    : /BSL_ALINDI|BSL wick\+body|HTF_BSL_SEVIYESINDE|BSL.*body[- ]?geri|BSL.*reclaim/i.test(txt);
  const wickReject = isL
    ? /Hammer|alt iğne|lower wick|dip|destek reddi|SSL wick/i.test(txt)
    : /Shooting|DarkCloud|üst iğne|upper wick|tepe|direnç reddi|BSL wick/i.test(txt);
  const fvgOte = isL
    ? /FVG_boğa|FVG_bull|bullish.*FVG|OTE|FVG.*boğa/i.test(txt)
    : /FVG_ayı|FVG_bear|bearish.*FVG|OTE|FVG.*ayı/i.test(txt);
  const mssReclaim = /MSS|ChoCH|body-geri|body reclaim|geri kazanım|EngulfingConfirmed|PiercingConfirmed|DarkCloudConfirmed/i.test(txt);
  const liveTrigger = /mumTahmin:(YÜKSELİŞ|LONG).*c[6-9]|mumTahmin:(DÜŞÜŞ|SHORT).*c[6-9]|delta imprint|CANLI_TICK/i.test(txt);

  const explicitEarlyWrong = !!(
    /DESTEK REDDİ.*DÜŞÜŞ erken|DİRENÇ REDDİ.*YÜKSELİŞ erken|geç giriş kovalamaca riski|late-chase:-?\d+/i.test(txt) &&
    !(sweptReclaim && mssReclaim && flowAligned)
  );
  const fakePumpAgainst = !!(/sahte-pump|fakePump|OI:çözülüyor|OI[- ]?çözülüyor/i.test(txt) && !sweptReclaim && !fvgOte);
  const weakNoChart = !!(/formasyon yok|puan 0\/12|teyidi zayıf/i.test(txt) && !sweptReclaim && !fvgOte && !mssReclaim);
  const htfWallAgainst = !!(isL
    ? /karşı .*BSL.*u:%0\.[0-7]|karşı .*direnç yakın|SUPPLY_OB/i.test(txt)
    : /karşı .*SSL.*u:%0\.[0-7]|karşı .*destek yakın|DEMAND_OB/i.test(txt));

  const chartSetup = !!(
    (sweptReclaim && (mssReclaim || wickReject || fvgOte)) ||
    (fvgOte && flowAligned && (score >= minScore - 18 || edge >= 55)) ||
    (wickReject && mssReclaim && flowAligned && score >= minScore - 16)
  );
  const scoreOk = score >= minScore - 18 || edge >= 58 || (score >= minScore - 24 && sweptReclaim && flowAligned);
  const hardNo = !!(flowAgainst || explicitEarlyWrong || fakePumpAgainst || weakNoChart || (htfWallAgainst && !sweptReclaim));
  const ok = !!(chartSetup && scoreOk && !hardNo);
  const mode = ok ? ((sweptReclaim && fvgOte && flowAligned && score >= minScore - 12) ? 'TACTICAL' : 'SCALP') : 'WAIT';
  const notes = [];
  if(sweptReclaim) notes.push('likidite alınmış/reclaim');
  if(wickReject) notes.push('fitil reddi');
  if(fvgOte) notes.push('FVG/OTE yolu');
  if(mssReclaim) notes.push('MSS/ChoCH/body reclaim');
  if(flowAligned) notes.push('canlı akış destekli');
  if(flowAgainst) notes.push('canlı akış ters');
  if(explicitEarlyWrong) notes.push('erken/yanlış yön uyarısı');
  if(fakePumpAgainst) notes.push('sahte-pump/OI çözülüyor');
  if(weakNoChart) notes.push('grafik tetik yok');
  if(htfWallAgainst) notes.push('yakın HTF duvarı');
  return { ok, mode, side, score, edge, priorityScore:pri, chartSetup, sweptReclaim, wickReject, fvgOte, mssReclaim, flowAligned, flowAgainst, hardNo, notes:notes.slice(0,8), summary:`R284 ${ok?'WAIT→TRADE':'WAIT'} ${side} ${mode} · ${notes.slice(0,5).join(' · ')}` };
}

function r285Pro5mTraderCook(side='LONG', d={}, opt={}) {
  side = String(side || 'LONG').toUpperCase();
  const isL = side === 'LONG';
  const n = (v,def=0)=>Number.isFinite(Number(v))?Number(v):def;
  const txt = [d.reason, d.brainSummary, d.htfTani, d.mumOzet, d.ictDashboard, d.r125OrderflowSummary, d.r126FlowSummary, d.r116HtfGuardReason, d.r117HtfReverseReason].filter(Boolean).join(' ');
  const e = d.r190Edge || {};
  const ict = d._r278ICT || d.r110ICT || {};
  const map = d.r281ProMap || {};
  const r289 = d.r289Playbook || {};
  const r290 = d.r290Smc5m || {};
  const r291 = d.r291Confluence || {};
  const score = n(opt.score ?? d.score, 0);
  const minScore = n(opt.minScore ?? 70, 70);
  const edge = n(d.brainConfidence ?? d.r142CalibratedEdge, 0);
  const pri = n(d.priorityScore, 0);
  const delta = n(d.r125LiveDeltaPct ?? d.r125Flow?.deltaPct, 0);
  const flowSide = String(d.r125BestSide || d.r125Flow?.bestSide || '').toUpperCase();
  const flowAligned = !!(
    flowSide === side ||
    (isL ? delta >= 18 : delta <= -18) ||
    (isL ? /R125\s*(YÜKSELİŞ|LONG).*L\d+\/S0/i.test(txt) : /R125\s*(DÜŞÜŞ|SHORT).*L0\/S\d+/i.test(txt)) ||
    (isL ? e.r192Footprint?.aligned && !e.r192Footprint?.against : e.r192Footprint?.aligned && !e.r192Footprint?.against)
  );
  const flowToxicAgainst = !!(
    (flowSide && flowSide !== 'NEUTRAL' && flowSide !== side) ||
    (isL ? delta <= -28 : delta >= 28) ||
    e.r192LiveAgainst || e.r192Footprint?.against || e.r192DeepOfi?.against || e.takerDivergence || e.vpinAgainst
  );

  const sslSwept = !!(ict.sslSwept || d.r278SslSwept || /SSL_ALINDI|SSL wick\+body|SSL.*body[- ]?(geri|reclaim)|HTF_SSL_SEVIYESINDE/i.test(txt));
  const bslSwept = !!(ict.bslSwept || d.r278BslSwept || /BSL_ALINDI|BSL wick\+body|BSL.*body[- ]?(geri|reclaim)|HTF_BSL_SEVIYESINDE/i.test(txt));
  const directionSweep = isL ? sslSwept : bslSwept;
  const wrongSweep = isL ? bslSwept : sslSwept;
  const bodyReclaim = !!(d.r117BodyReclaimOk || d.r117MssOk || d.r117PrecisionCandleOk || /body[- ]?(geri|reclaim)|MSS|ChoCH|geri kazanım|wick\+body/i.test(txt));
  const candleScore = n(d.r86FormasyonPuan ?? d.r118Candle?.score ?? 0, 0);
  const candleOk = !!(d.r118CandleOk || d.r118CandleStrong || candleScore >= 6 || /Hammer|Dragonfly|EngulfingConfirmed|PiercingConfirmed|DarkCloudConfirmed|Shooting|Doji/i.test(txt));
  const wickReject = !!(isL ? /Hammer|Dragonfly|alt iğne|lower wick|dip|SSL wick/i.test(txt) : /Shooting|üst iğne|upper wick|tepe|BSL wick|DarkCloud/i.test(txt));

  const inDirFvg = !!(isL ? (ict.inBullishFVG || d.r278InBullFvg || /FVG_(boğa|bull)|bullish.*FVG/i.test(txt)) : (ict.inBearishFVG || d.r278InBearFvg || /FVG_(ayı|bear)|bearish.*FVG/i.test(txt)));
  const invalidFvg = /FVG invalid/i.test(txt) && !inDirFvg;
  const oteOk = !!(ict.oteZone?.inOte && String(ict.oteZone?.side||'').toUpperCase() === side);
  const fvgOteOk = !!(r290.fvgRetest || r290.oteOk || r289.fvgRetest || d.r289FvgOteOk || ((inDirFvg || oteOk) && !invalidFvg));

  const rangePos = n(e.rangePos, 0.5);
  const seq = n(e.seq, 0);
  const p3 = n(e.price3, 0);
  const atExtreme = isL ? (rangePos >= 0.86 || /pos:0\.(9|8[8-9])|pos:1\.00/i.test(txt)) : (rangePos <= 0.14 || /pos:0\.(0|1[0-2])/i.test(txt));
  const lateChase = !!((e.lateTrapRisk || e.tooLate || /geç giriş|late-chase/i.test(txt) || seq >= 5 || (isL ? p3 > 2.2 : p3 < -2.2)) && !e.squeeze && !e.r192FuelOk);
  const continuationFuel = !!(r289.continuation || d.r289ContinuationFuel || e.earlyContinuation || e.squeeze || e.r192FuelOk || e.r192SqzImminent || e.r194SwingBreak?.strong || (e.r192FuelScore >= 6 && !lateChase));
  const continuationClean = !!(continuationFuel && flowAligned && !flowToxicAgainst && !lateChase && !atExtreme && score >= minScore - 14 && edge >= 45);

  const sweepReclaimSetup = !!(directionSweep && bodyReclaim && (candleOk || wickReject || flowAligned) && !flowToxicAgainst);
  const fvgRetestSetup = !!(fvgOteOk && flowAligned && !flowToxicAgainst && !lateChase && !atExtreme && (score >= minScore - 20 || edge >= 55));
  const reverseTrapSetup = !!(d.r117HtfReverseOk && directionSweep && bodyReclaim && !flowToxicAgainst);

  const r295Delta = n(d.r125LiveDeltaPct ?? d.r125Flow?.deltaPct, 0);
  const r295BookImb = (()=>{ const m=String(txt).match(/book:\s*(YÜKSELİŞ|DÜŞÜŞ|LONG|SHORT|NEUTRAL)\s*imb:\s*(-?[0-9.]+)%/i); if(!m) return {side:'NEUTRAL',imb:0}; const s=/YÜKSELİŞ|LONG/i.test(m[1])?'LONG':/DÜŞÜŞ|SHORT/i.test(m[1])?'SHORT':'NEUTRAL'; return {side:s, imb:Math.abs(Number(m[2])||0)}; })();
  const r295DeepSide = (()=>{ const m=String(txt).match(/deep:\s*(YÜKSELİŞ|DÜŞÜŞ|LONG|SHORT|NEUTRAL)\s*(-?[0-9.]+)%/i); if(!m) return {side:'NEUTRAL',pct:0}; const s=/YÜKSELİŞ|LONG/i.test(m[1])?'LONG':/DÜŞÜŞ|SHORT/i.test(m[1])?'SHORT':'NEUTRAL'; return {side:s, pct:Math.abs(Number(m[2])||0)}; })();
  const r295FlowDirOk = isL ? (s)=>s==='LONG' : (s)=>s==='SHORT';
  let r295InstantSignals = 0;
  if (isL ? r295Delta >= 14 : r295Delta <= -14) r295InstantSignals++;
  if (r295BookImb.side !== 'NEUTRAL' && r295FlowDirOk(r295BookImb.side) && r295BookImb.imb >= 30) r295InstantSignals++;
  if (r295DeepSide.side !== 'NEUTRAL' && r295FlowDirOk(r295DeepSide.side) && r295DeepSide.pct >= 20) r295InstantSignals++;
  if (isL ? /taker:\s*1\.[2-9]|taker:\s*[2-9]/i.test(txt) : /taker:\s*0\.[0-7]/i.test(txt)) r295InstantSignals++;
  const r295ShortSqueezeActive = !!(d.r111?.shortSqueeze || e.squeeze || /shortSqueeze:EVET|short squeeze/i.test(txt));
  const r295LongSqueezeActive = !!(d.r111?.longSqueeze || /longSqueeze:EVET|long squeeze/i.test(txt));
  const r295SqueezeBlocksReverse = isL ? r295LongSqueezeActive : r295ShortSqueezeActive;
  const r295FundingPct = (()=>{ const m=String(txt).match(/funding:\s*(-?[0-9.]+)%/i); return m?Number(m[1]):0; })();
  const r295FundingBlocksReverse = isL ? (r295FundingPct >= 0.05) : (r295FundingPct <= -0.05);
  const r295EarlyReverseOk = !!(r295InstantSignals >= 2 && !r295SqueezeBlocksReverse && !r295FundingBlocksReverse && !flowToxicAgainst);

  const fakeOi = !!(/sahte-pump|SAHTE_PUMP|OI:çözülüyor|OI[- ]?çözülüyor/i.test(txt) || d.r140OiVel?.fakePump || d.r140OiVel?.fakeDump);
  const weakNoTrigger = !!(/formasyon yok|puan 0\/12|teyidi zayıf/i.test(txt) && !bodyReclaim && !fvgOteOk && !continuationClean && !sweepReclaimSetup);
  const htfWallAgainst = !!(isL
    ? (/karşı .*BSL.*u:%0\.[0-7]|karşı .*direnç yakın|SUPPLY_OB/i.test(txt) && !directionSweep)
    : (/karşı .*SSL.*u:%0\.[0-7]|karşı .*destek yakın|DEMAND_OB/i.test(txt) && !directionSweep));
  const hardNo = !!(
    e.spreadBlock || map.hardNo || r289.hardNo || r290.hardNo || flowToxicAgainst ||
    (lateChase && !directionSweep && !continuationClean) ||
    (atExtreme && !(directionSweep && bodyReclaim) && !r295EarlyReverseOk) ||
    (fakeOi && !directionSweep && !fvgOteOk && !continuationClean) ||
    (invalidFvg && !directionSweep && !continuationClean) ||
    (weakNoTrigger && score < minScore - 4) ||
    (htfWallAgainst && !reverseTrapSetup)
  );

  let setup = 'NONE';
  if (r291.tradeOk && r291.setup && r291.setup !== 'NONE') setup = String(r291.setup);
  else if (r290.tradeOk && r290.setup && r290.setup !== 'NONE') setup = String(r290.setup);
  else if (r289.tradeOk && r289.setup && r289.setup !== 'NONE') setup = String(r289.setup);
  else if (reverseTrapSetup) setup = 'REVERSAL_TRAP';
  else if (sweepReclaimSetup) setup = 'LIQUIDITY_RECLAIM';
  else if (fvgRetestSetup) setup = 'FVG_OTE_RETEST';
  else if (continuationClean) setup = 'EARLY_CONTINUATION';
  else if (r295EarlyReverseOk && atExtreme) setup = 'R295_EARLY_REVERSE';

  const setupBonus = setup === 'REVERSAL_TRAP' ? 34 : setup === 'LIQUIDITY_RECLAIM' ? 30 : setup === 'FVG_OTE_RETEST' ? 26 : setup === 'EARLY_CONTINUATION' ? 24 : setup === 'R295_EARLY_REVERSE' ? 28 : 0;
  const quality = Math.max(0, Math.min(100,
    setupBonus + score*0.22 + edge*0.24 + pri*0.08 + (flowAligned?10:0) + (candleOk?7:0) + (fvgOteOk?8:0) + (continuationFuel?8:0) + (r290.tradeOk?18:0) + (Number(r290.capacity||0)>=78?10:0) + (r291.tradeOk?20:0) + (Number(r291.capacity||0)>=78?10:0)
    - (lateChase?20:0) - (atExtreme?12:0) - (fakeOi?10:0) - (invalidFvg?10:0) - (htfWallAgainst?10:0)
  ));
  const minQuality = setup === 'REVERSAL_TRAP' ? 58 : setup === 'LIQUIDITY_RECLAIM' ? 60 : setup === 'FVG_OTE_RETEST' ? 64 : setup === 'EARLY_CONTINUATION' ? 68 : setup === 'R295_EARLY_REVERSE' ? 62 : 999;
  const ok = !!(!hardNo && setup !== 'NONE' && quality >= minQuality && (score >= minScore - 24 || edge >= 50 || setup === 'REVERSAL_TRAP'));

  const notes = [];
  notes.push(`setup:${setup}`);
  if(directionSweep) notes.push(isL?'SSL alınmış/reclaim':'BSL alınmış/reclaim');
  if(bodyReclaim) notes.push('body/MSS reclaim');
  if(fvgOteOk) notes.push('FVG/OTE retest');
  if(r291.tradeOk) notes.push(`R291 kapasite ${Number(r291.capacity||0).toFixed(0)}`);
  if(r290.tradeOk) notes.push(`R290 kapasite ${Number(r290.capacity||0).toFixed(0)}`);
  if(continuationClean) notes.push('erken continuation temiz');
  if(flowAligned) notes.push('akış aynı yön');
  if(flowToxicAgainst) notes.push('akış toksik ters');
  if(lateChase) notes.push('geç-chase');
  if(atExtreme) notes.push('range ucu');
  if(r295EarlyReverseOk && atExtreme) notes.push(`R295 erken ters teyit: ${r295InstantSignals} anlık sinyal (delta/book/deep/taker), squeeze yok — mum kapanışı beklemeden ters giriş`);
  if(atExtreme && r295SqueezeBlocksReverse) notes.push(`R295 squeeze koruması: ${isL?'longSqueeze':'shortSqueeze'} aktif — ters dönüş YASAK (MM zıt yöne taşıyor)`);
  if(atExtreme && r295FundingBlocksReverse) notes.push(`R295 funding koruması: funding %${r295FundingPct} — ${side} ters dönüş riskli, açılmadı`);
  if(fakeOi) notes.push('OI/fake risk');
  if(invalidFvg) notes.push('FVG invalid');
  if(htfWallAgainst) notes.push('HTF duvar');
  if(weakNoTrigger) notes.push('mum/tetik yok');

  return { ok, side, setup, mode: ok ? (setup === 'EARLY_CONTINUATION' ? 'RUNNER' : setup === 'FVG_OTE_RETEST' ? 'NORMAL' : 'TACTICAL') : 'WAIT', quality:+quality.toFixed(1), score, edge, priorityScore:pri,
    traderSetupOk: ok, fvgOteOk, continuationFuel: continuationClean, sweepReclaimSetup, reverseTrapSetup, flowAligned, flowToxicAgainst, lateChase, atExtreme, fakeOi, invalidFvg, hardNo, notes:notes.slice(0,10),
    summary:`R285 ${ok?'TRADE':'WAIT'} ${side} ${setup} q:${quality.toFixed(1)} · ${notes.slice(0,6).join(' · ')}` };
}

function r287ReverseThesisTrader(blockedSide='LONG', blockedD={}, oppD={}, opt={}) {
  const bSide = String(blockedSide||'LONG').toUpperCase();
  const side = bSide === 'LONG' ? 'SHORT' : 'LONG';
  const isShort = side === 'SHORT';
  const n = (v,def=0)=>Number.isFinite(Number(v))?Number(v):def;
  const bTxt = [blockedD?.reason, blockedD?.brainSummary, blockedD?.htfTani, blockedD?.mumOzet, blockedD?.ictDashboard, blockedD?.r125OrderflowSummary, blockedD?.r126FlowSummary, blockedD?.r191UnifiedBlockReason].filter(Boolean).join(' ');
  const oTxt = [oppD?.reason, oppD?.brainSummary, oppD?.htfTani, oppD?.mumOzet, oppD?.ictDashboard, oppD?.r125OrderflowSummary, oppD?.r126FlowSummary, oppD?.r116HtfGuardReason, oppD?.r117HtfReverseReason].filter(Boolean).join(' ');
  const txt = `${bTxt} ${oTxt}`;
  const eB = blockedD?.r190Edge || {};
  const eO = oppD?.r190Edge || {};
  const score = n(opt.score ?? oppD?.score, 0);
  const minScore = n(opt.minScore ?? 70, 70);
  const edge = n(oppD?.brainConfidence ?? oppD?.r142CalibratedEdge, 0);
  const pri = n(oppD?.priorityScore, 0);
  const rangePos = n(blockedD?.r286RangePos ?? eB.rangePos, 0.5);
  const p3 = n(blockedD?.r286P3 ?? eB.price3, 0);
  const seq = n(blockedD?.r286Seq ?? eB.seq, 0);
  const blockedChart = !!(blockedD?.r286ChartAcceptanceBlock || /R286 5m kabul yok|hedef\/iğne yakın|hedef yakın|üst hedef yakın|alt hedef yakın/i.test(bTxt));
  const topRejectionZone = !!(
    bSide === 'LONG' && blockedChart &&
    (rangePos >= 0.82 || /pos:0\.(8[2-9]|9)|pos:1\.00|range 0\.(8[2-9]|9)/i.test(txt)) &&
    (/üst hedef yakın|5M_BEAR_OB|BEAR_OB|SUPPLY_OB|karşı .*BSL|direnç yakın|üst iğne|BSL/i.test(txt) || blockedD?.r39TargetNearBlock || blockedD?.r39SR?.nearResistance)
  );
  const bottomRejectionZone = !!(
    bSide === 'SHORT' && blockedChart &&
    (rangePos <= 0.18 || /pos:0\.(0|1[0-8])|range 0\.(0|1[0-8])/i.test(txt)) &&
    (/alt hedef yakın|5M_BULL_OB|BULL_OB|DEMAND_OB|karşı .*SSL|destek yakın|alt iğne|SSL/i.test(txt) || blockedD?.r39TargetNearBlock || blockedD?.r39SR?.nearSupport)
  );
  const levelReason = topRejectionZone || bottomRejectionZone;

  const oppDelta = n(oppD?.r125LiveDeltaPct ?? oppD?.r125Flow?.deltaPct, 0);
  const oppFlowSide = String(oppD?.r125BestSide || oppD?.r125Flow?.bestSide || '').toUpperCase();
  const flowFlip = !!(
    oppFlowSide === side ||
    (isShort ? oppDelta <= -12 : oppDelta >= 12) ||
    (isShort ? /R125\s*(DÜŞÜŞ|SHORT).*L0\/S\d+/i.test(oTxt) : /R125\s*(YÜKSELİŞ|LONG).*L\d+\/S0/i.test(oTxt))
  );
  const flowStillAgainst = !!(
    (oppFlowSide && oppFlowSide !== 'NEUTRAL' && oppFlowSide !== side) ||
    (isShort ? oppDelta >= 28 : oppDelta <= -28) ||
    eO?.r192LiveAgainst || eO?.r192Footprint?.against || eO?.r192DeepOfi?.against || eO?.takerDivergence || eO?.vpinAgainst
  );
  const rejectionCandle = !!(
    oppD?.r118CandleOk || oppD?.r118CandleStrong || oppD?.r117BodyReclaimOk || oppD?.r117MssOk ||
    (isShort ? /Shooting|DarkCloud|Bearish|ayı|üst iğne|BSL.*reclaim|BSL wick\+body/i.test(txt) : /Hammer|Dragonfly|Piercing|Bullish|boğa|alt iğne|SSL.*reclaim|SSL wick\+body/i.test(txt))
  );
  const fvgOteOpp = !!(
    (isShort ? (oppD?._r278ICT?.inBearishFVG || oppD?.r110ICT?.inBearishFVG || /FVG_(ayı|bear)|bearish.*FVG/i.test(oTxt)) : (oppD?._r278ICT?.inBullishFVG || oppD?.r110ICT?.inBullishFVG || /FVG_(boğa|bull)|bullish.*FVG/i.test(oTxt))) ||
    ((oppD?._r278ICT?.oteZone?.inOte || oppD?.r110ICT?.oteZone?.inOte) && String(oppD?._r278ICT?.oteZone?.side || oppD?.r110ICT?.oteZone?.side || '').toUpperCase() === side)
  );
  const oiTrap = !!(/OI:çözülüyor|sahte-pump|SAHTE_PUMP|OI spike|ACCEL/i.test(txt) || Math.abs(n(eB?.oi?.oiChg15, n(blockedD?.r140OiVel?.velocity, 0))) >= 180);
  const extendedIntoLevel = !!(seq >= 4 || (bSide === 'LONG' ? p3 >= 1.8 : p3 <= -1.8) || /geç giriş|late-chase/i.test(bTxt));
  const trigger = !!(rejectionCandle || fvgOteOpp || flowFlip);
  const armed = !!(levelReason && (extendedIntoLevel || oiTrap || blockedChart));
  const hardNo = !!(oppD?.poorLiquidity || oppD?.atrExtremeBlock || oppD?.hardVeto || eO?.spreadBlock || flowStillAgainst || !armed);
  const quality = Math.max(0, Math.min(100,
    38 + score*0.18 + edge*0.18 + pri*0.06 + (levelReason?16:0) + (extendedIntoLevel?10:0) + (oiTrap?8:0) + (rejectionCandle?13:0) + (fvgOteOpp?10:0) + (flowFlip?12:0)
    - (flowStillAgainst?30:0) - (!trigger?22:0)
  ));
  const ok = !!(!hardNo && trigger && quality >= 62 && (score >= minScore - 30 || edge >= 35 || rejectionCandle || fvgOteOpp));
  const mode = ok ? (fvgOteOpp ? 'FVG_REVERSE' : rejectionCandle ? 'REJECTION_SCALP' : 'FLOW_FLIP_SCALP') : (armed ? 'ARMED_WAIT' : 'WAIT');
  const notes=[];
  if(levelReason) notes.push(bSide==='LONG'?'üst hedef/fitil alanı':'alt hedef/fitil alanı');
  if(blockedChart) notes.push(`${bSide} kabul yok`);
  if(extendedIntoLevel) notes.push(`hareket koşmuş p3:${p3.toFixed(2)} seq:${seq}`);
  if(oiTrap) notes.push('OI/MM av riski');
  if(rejectionCandle) notes.push('karşı mum/reclaim tetik');
  if(fvgOteOpp) notes.push('karşı FVG/OTE');
  if(flowFlip) notes.push('canlı akış flip');
  if(flowStillAgainst) notes.push('akış hâlâ ters — kör reverse yok');
  if(!trigger) notes.push('reverse tetik bekle');
  return { ok, armed, side, fromBlockedSide:bSide, mode, quality:+quality.toFixed(1), score, edge, priorityScore:pri, levelReason, extendedIntoLevel, oiTrap, rejectionCandle, fvgOteOpp, flowFlip, flowStillAgainst, hardNo, notes:notes.slice(0,10), summary:`R287 ${ok?'REVERSE TRADE':'REVERSE RADAR'} ${side} ${mode} q:${quality.toFixed(1)} · ${notes.slice(0,7).join(' · ')}` };
}

const r289ChartState = new Map();
function r289Rows(kl, limit=120) {
  const arr = Array.isArray(kl) ? kl : [];
  const first = arr.find(x => x != null);
  if (first && !Array.isArray(first) && typeof first === 'object') {
    return arr.slice(-limit)
      .map(x => ({ t:Number(x.t||0), o:Number(x.o), h:Number(x.h), l:Number(x.l), c:Number(x.c), v:Number(x.v||0) }))
      .filter(x=>Number.isFinite(x.o)&&Number.isFinite(x.h)&&Number.isFinite(x.l)&&Number.isFinite(x.c)&&x.h>0&&x.l>0&&x.h>=x.l);
  }
  return arr.slice(-limit).map(k=>({
    t:Number(k[0]||0), o:Number(k[1]), h:Number(k[2]), l:Number(k[3]), c:Number(k[4]), v:Number(k[5]||0)
  })).filter(x=>Number.isFinite(x.o)&&Number.isFinite(x.h)&&Number.isFinite(x.l)&&Number.isFinite(x.c)&&x.h>0&&x.l>0&&x.h>=x.l);
}
function r289Pct(a,b){ a=Number(a); b=Number(b); return b?((a-b)/b)*100:0; }
function r289Clamp(v,a,b){ return Math.max(a, Math.min(b, v)); }
function r289Pivots(rows, left=2, right=2) {
  const highs=[], lows=[];
  for(let i=left;i<rows.length-right;i++){
    const r=rows[i]; let hi=true, lo=true;
    for(let j=i-left;j<=i+right;j++){ if(j===i) continue; if(rows[j].h>=r.h) hi=false; if(rows[j].l<=r.l) lo=false; }
    if(hi) highs.push({i, t:r.t, price:r.h});
    if(lo) lows.push({i, t:r.t, price:r.l});
  }
  return { highs, lows };
}
function r289LastSwing(rows) {
  const p=r289Pivots(rows,2,2);
  const hs=p.highs.slice(-3), ls=p.lows.slice(-3);
  const lastH=hs.at(-1), prevH=hs.at(-2), lastL=ls.at(-1), prevL=ls.at(-2);
  return {
    lastHigh:lastH||null, prevHigh:prevH||null, lastLow:lastL||null, prevLow:prevL||null,
    hhhl:!!(lastH&&prevH&&lastL&&prevL&&lastH.price>prevH.price&&lastL.price>prevL.price),
    lhll:!!(lastH&&prevH&&lastL&&prevL&&lastH.price<prevH.price&&lastL.price<prevL.price),
    highs:hs, lows:ls
  };
}
function r289Fvgs(rows, lookback=70) {
  const out=[]; const st=Math.max(2, rows.length-lookback);
  for(let i=st;i<rows.length;i++){
    const a=rows[i-2], b=rows[i]; if(!a||!b) continue;
    if(b.l>a.h) out.push({side:'LONG', low:a.h, high:b.l, mid:(a.h+b.l)/2, age:rows.length-1-i, i});
    if(b.h<a.l) out.push({side:'SHORT', low:b.h, high:a.l, mid:(b.h+a.l)/2, age:rows.length-1-i, i});
  }
  return out;
}
function r289NearestFvg(rows, side, lastPrice) {
  const list=r289Fvgs(rows,70).filter(f=>f.side===side).sort((a,b)=>a.age-b.age);
  let best=null;
  for(const f of list){
    const inZone=lastPrice>=f.low*0.997 && lastPrice<=f.high*1.003;
    const dist = inZone ? 0 : Math.min(Math.abs(r289Pct(lastPrice,f.low)), Math.abs(r289Pct(lastPrice,f.high)));
    const mitigated = rows.slice(f.i+1).some(r => side==='LONG' ? r.l <= f.high && r.c >= f.low : r.h >= f.low && r.c <= f.high);
    const cand={...f,inZone,dist,near:dist<=1.15,mitigated};
    if(!best || (cand.inZone&&!best.inZone) || cand.dist<best.dist) best=cand;
  }
  return best;
}
function r289TfMap(rows, lastPrice) {
  const swing=r289LastSwing(rows);
  const p=r289Pivots(rows,2,2);
  const hs=p.highs.slice(-8).map(x=>x.price).filter(x=>x>lastPrice).sort((a,b)=>a-b);
  const ls=p.lows.slice(-8).map(x=>x.price).filter(x=>x<lastPrice).sort((a,b)=>b-a);
  const up=hs[0]||null, down=ls[0]||null;
  const bull=r289NearestFvg(rows,'LONG',lastPrice), bear=r289NearestFvg(rows,'SHORT',lastPrice);
  return { swing, up, down, upDist:up?r289Pct(up,lastPrice):999, downDist:down?Math.abs(r289Pct(down,lastPrice)):999, bullFvg:bull, bearFvg:bear };
}
function r289FiveMinutePlaybook(symbol, k5m=[], k15m=[], k1h=[], k4h=[], lastPrice=0) {
  const k5=r289Rows(k5m,100), k15=r289Rows(k15m,80), k1=r289Rows(k1h,80), k4=r289Rows(k4h,80);
  const emptySide=(side)=>({side, tradeOk:false, hardNo:false, radar:false, setup:'NONE', quality:0, notes:['veri yok'], summary:`R289 ${side} veri yok`});
  if(k5.length<20 || !Number.isFinite(Number(lastPrice)) || Number(lastPrice)<=0) return { ok:false, long:emptySide('LONG'), short:emptySide('SHORT'), summary:'R289 veri yok' };
  const lp=Number(lastPrice), last=k5.at(-1), prev=k5.at(-2), prev2=k5.at(-3), prev3=k5.at(-4);
  const hi48=Math.max(...k5.slice(-48).map(x=>x.h)), lo48=Math.min(...k5.slice(-48).map(x=>x.l));
  const rangePos=(hi48>lo48)?(lp-lo48)/(hi48-lo48):0.5;
  let upSeq=0, dnSeq=0; for(let i=k5.length-1;i>=0;i--){ const r=k5[i]; if(r.c>r.o){ if(dnSeq) break; upSeq++; } else if(r.c<r.o){ if(upSeq) break; dnSeq++; } else break; }
  const p3=prev3 ? r289Pct(last.c, prev3.c) : 0;
  const body=Math.abs(last.c-last.o), rng=Math.max(1e-12,last.h-last.l);
  const upperW=(last.h-Math.max(last.c,last.o))/rng, lowerW=(Math.min(last.c,last.o)-last.l)/rng;
  const bullCandle=last.c>last.o && body/rng>=0.35, bearCandle=last.c<last.o && body/rng>=0.35;
  const upperReject=upperW>=0.38 && last.c < last.h - rng*0.45;
  const lowerReject=lowerW>=0.38 && last.c > last.l + rng*0.45;
  const swing5=r289LastSwing(k5);
  const brokeUp=!!(swing5.lastHigh && last.c > swing5.lastHigh.price*1.002 && prev.c <= swing5.lastHigh.price*1.002);
  const brokeDn=!!(swing5.lastLow && last.c < swing5.lastLow.price*0.998 && prev.c >= swing5.lastLow.price*0.998);
  const reclaimedUp=!!(lowerReject && last.c>prev.h*0.998);
  const reclaimedDn=!!(upperReject && last.c<prev.l*1.002);
  const bullFvg5=r289NearestFvg(k5,'LONG',lp), bearFvg5=r289NearestFvg(k5,'SHORT',lp);
  const m15=r289TfMap(k15,lp), m1=r289TfMap(k1,lp), m4=r289TfMap(k4,lp);
  const nearUp = Math.min(m15.upDist, m1.upDist, m4.upDist, bearFvg5?.dist??999, m15.bearFvg?.dist??999);
  const nearDn = Math.min(m15.downDist, m1.downDist, m4.downDist, bullFvg5?.dist??999, m15.bullFvg?.dist??999);
  const htfBull = (m15.swing.hhhl?1:0)+(m1.swing.hhhl?1:0)+(m4.swing.hhhl?1:0);
  const htfBear = (m15.swing.lhll?1:0)+(m1.swing.lhll?1:0)+(m4.swing.lhll?1:0);
  const trendLong = swing5.hhhl || htfBull>=2;
  const trendShort = swing5.lhll || htfBear>=2;
  const longChase = (rangePos>=0.82 && p3>=1.6) || upSeq>=5;
  const shortChase = (rangePos<=0.18 && p3<=-1.6) || dnSeq>=5;
  const longLevelTooClose = nearUp<=0.75 && !brokeUp;
  const shortLevelTooClose = nearDn<=0.75 && !brokeDn;
  const longFvgRetest = !!((bullFvg5&&(bullFvg5.inZone||bullFvg5.near)) || (m15.bullFvg&&(m15.bullFvg.inZone||m15.bullFvg.near)));
  const shortFvgRetest = !!((bearFvg5&&(bearFvg5.inZone||bearFvg5.near)) || (m15.bearFvg&&(m15.bearFvg.inZone||m15.bearFvg.near)));
  const longContinuation = !!(trendLong && brokeUp && !longChase && !longLevelTooClose && p3<2.8);
  const shortContinuation = !!(trendShort && brokeDn && !shortChase && !shortLevelTooClose && p3>-2.8);
  const longReclaim = !!(reclaimedUp || (lowerReject && (longFvgRetest || rangePos<=0.45)));
  const shortReclaim = !!(reclaimedDn || (upperReject && (shortFvgRetest || rangePos>=0.55)));
  const topShortRadar = !!((rangePos>=0.82 || nearUp<=0.75 || upperReject) && (p3>=1.4 || upSeq>=3 || trendLong));
  const bottomLongRadar = !!((rangePos<=0.18 || nearDn<=0.75 || lowerReject) && (p3<=-1.4 || dnSeq>=3 || trendShort));
  const state=r289ChartState.get(symbol)||{};
  const now=Date.now();
  const longSetup = longReclaim ? 'LIQUIDITY_RECLAIM' : longFvgRetest && (bullCandle||brokeUp||trendLong) ? 'FVG_OTE_RETEST' : longContinuation ? 'EARLY_CONTINUATION' : (state.longRadarUntil>now && lowerReject ? 'RADAR_TRIGGER' : 'NONE');
  const shortSetup = shortReclaim ? 'LIQUIDITY_RECLAIM' : shortFvgRetest && (bearCandle||brokeDn||trendShort) ? 'FVG_OTE_RETEST' : shortContinuation ? 'EARLY_CONTINUATION' : (state.shortRadarUntil>now && upperReject ? 'RADAR_TRIGGER' : 'NONE');
  const longHardNo = !!((longChase && !longReclaim && !longFvgRetest && !brokeUp) || (longLevelTooClose && !brokeUp && !longFvgRetest) || (rangePos>=0.90 && !brokeUp));
  const shortHardNo= !!((shortChase && !shortReclaim && !shortFvgRetest && !brokeDn) || (shortLevelTooClose && !brokeDn && !shortFvgRetest) || (rangePos<=0.10 && !brokeDn));
  if(topShortRadar) state.shortRadarUntil=now+9*60*1000;
  if(bottomLongRadar) state.longRadarUntil=now+9*60*1000;
  state.last={rangePos,p3,upSeq,dnSeq,nearUp,nearDn,topShortRadar,bottomLongRadar}; state.ts=now; r289ChartState.set(symbol,state);
  function sideOut(side, setup, hardNo, radar, qualityBase, notesExtra){
    const tradeOk=!!(setup!=='NONE' && !hardNo && qualityBase>=58);
    const notes=[];
    notes.push(`5m:${setup}`); notes.push(`range:${rangePos.toFixed(2)}`); notes.push(`p3:${p3.toFixed(2)}%`); notes.push(`seq:${side==='LONG'?upSeq:dnSeq}`);
    if(side==='LONG' ? longFvgRetest : shortFvgRetest) notes.push('FVG/OTE retest yolu');
    if(side==='LONG' ? brokeUp : brokeDn) notes.push('kırılım-kabul');
    if(side==='LONG' ? longReclaim : shortReclaim) notes.push('iğne/reclaim');
    if(side==='LONG' ? longLevelTooClose : shortLevelTooClose) notes.push('hedef/likidite yakın');
    if(hardNo) notes.push('grafik hard-no');
    if(radar) notes.push('ters yön radar');
    notes.push(...(notesExtra||[]));
    return { side, tradeOk, hardNo, radar, setup, quality:+qualityBase.toFixed(1), rangePos:+rangePos.toFixed(2), p3:+p3.toFixed(2), seq:side==='LONG'?upSeq:dnSeq, nearUp:+nearUp.toFixed(2), nearDn:+nearDn.toFixed(2), fvgRetest: side==='LONG'?longFvgRetest:shortFvgRetest, continuation: side==='LONG'?longContinuation:shortContinuation, reclaim: side==='LONG'?longReclaim:shortReclaim, broke: side==='LONG'?brokeUp:brokeDn, notes:notes.slice(0,10), summary:`R289 ${tradeOk?'TRADE':'WAIT'} ${side} ${setup} q:${qualityBase.toFixed(1)} · ${notes.slice(0,7).join(' · ')}` };
  }
  const qLong=42 + (longReclaim?25:0)+(longFvgRetest?20:0)+(longContinuation?24:0)+(trendLong?8:0)+(bullCandle?6:0)+(brokeUp?10:0)-(longHardNo?35:0)-(longChase?12:0);
  const qShort=42 + (shortReclaim?25:0)+(shortFvgRetest?20:0)+(shortContinuation?24:0)+(trendShort?8:0)+(bearCandle?6:0)+(brokeDn?10:0)-(shortHardNo?35:0)-(shortChase?12:0);
  const long=sideOut('LONG', longSetup, longHardNo, bottomLongRadar, r289Clamp(qLong,0,100), [`15m:${m15.swing.hhhl?'HHHL':m15.swing.lhll?'LHLL':'RANGE'}`, `1h:${m1.swing.hhhl?'HHHL':m1.swing.lhll?'LHLL':'RANGE'}`, `4h:${m4.swing.hhhl?'HHHL':m4.swing.lhll?'LHLL':'RANGE'}`]);
  const short=sideOut('SHORT', shortSetup, shortHardNo, topShortRadar, r289Clamp(qShort,0,100), [`15m:${m15.swing.hhhl?'HHHL':m15.swing.lhll?'LHLL':'RANGE'}`, `1h:${m1.swing.hhhl?'HHHL':m1.swing.lhll?'LHLL':'RANGE'}`, `4h:${m4.swing.hhhl?'HHHL':m4.swing.lhll?'LHLL':'RANGE'}`]);
  return { ok:true, long, short, summary:`R289 5m playbook: L ${long.setup}/${long.quality} S ${short.setup}/${short.quality} · range ${rangePos.toFixed(2)} p3 ${p3.toFixed(2)} nearUp ${nearUp.toFixed(2)} nearDn ${nearDn.toFixed(2)}` };
}

function r290Median(arr){ const a=(arr||[]).map(Number).filter(Number.isFinite).sort((x,y)=>x-y); if(!a.length) return 0; const m=Math.floor(a.length/2); return a.length%2?a[m]:(a[m-1]+a[m])/2; }
function r290Avg(arr){ const a=(arr||[]).map(Number).filter(Number.isFinite); return a.length?a.reduce((x,y)=>x+y,0)/a.length:0; }
function r290AtrPct(rows, n=14){
  rows = Array.isArray(rows)?rows:[]; if(rows.length<3) return 0;
  const trs=[]; for(let i=Math.max(1, rows.length-n); i<rows.length; i++){ const r=rows[i], p=rows[i-1]; trs.push(Math.max(r.h-r.l, Math.abs(r.h-p.c), Math.abs(r.l-p.c))); }
  const atr=r290Avg(trs); const c=rows.at(-1)?.c||0; return c?atr/c*100:0;
}
function r290Clusters(points=[], pct=0.004) {
  const levels=(points||[]).map(x=>({price:Number(x.price), i:Number(x.i||0)})).filter(x=>Number.isFinite(x.price)).sort((a,b)=>a.price-b.price);
  const clusters=[];
  for(const pt of levels){
    let hit=null;
    for(const c of clusters){ if(Math.abs(pt.price-c.level)/Math.max(pt.price,c.level,1e-12) <= pct){ hit=c; break; } }
    if(!hit) clusters.push({level:pt.level, count:1, first:pt.i, last:pt.i, prices:[pt.price]});
    else { hit.prices.push(pt.price); hit.count++; hit.last=Math.max(hit.last,pt.i); hit.level=r290Avg(hit.prices); }
  }
  return clusters.filter(c=>c.count>=2).sort((a,b)=>b.count-a.count);
}
function r290LiquidityMap(rows=[], lastPrice=0) {
  rows=r289Rows(rows,120); const lp=Number(lastPrice||rows.at(-1)?.c||0); if(rows.length<20||!lp) return {up:null,down:null,upDist:999,downDist:999,sweptUp:false,sweptDown:false};
  const p=r289Pivots(rows,2,2); const range=Math.max(...rows.slice(-72).map(x=>x.h))-Math.min(...rows.slice(-72).map(x=>x.l));
  const tol=Math.max(0.0025, Math.min(0.008, range/Math.max(lp,1e-12)*0.10));
  const hi=r290Clusters(p.highs.slice(-12), tol), lo=r290Clusters(p.lows.slice(-12), tol);
  const ups=hi.filter(c=>c.level>lp).sort((a,b)=>a.level-b.level); const dns=lo.filter(c=>c.level<lp).sort((a,b)=>b.level-a.level);
  const up=ups[0]||null, down=dns[0]||null; const last=rows.at(-1);
  const nearHi=hi.sort((a,b)=>Math.abs(a.level-lp)-Math.abs(a.level-lp))[0]||null;
  const nearLo=lo.sort((a,b)=>Math.abs(a.level-lp)-Math.abs(a.level-lp))[0]||null;
  const sweptUp=!!(nearHi && last.h>nearHi.level*(1+tol*0.35) && last.c<nearHi.level);
  const sweptDown=!!(nearLo && last.l<nearLo.level*(1-tol*0.35) && last.c>nearLo.level);
  return { up, down, upDist:up?Math.abs(r289Pct(up.level,lp)):999, downDist:down?Math.abs(r289Pct(down.level,lp)):999, sweptUp, sweptDown, tolPct:tol*100 };
}
function r290BosChoch(rows=[]){
  rows=r289Rows(rows,120); if(rows.length<20) return {bosUp:false,bosDn:false,chochUp:false,chochDn:false};
  const sw=r289LastSwing(rows), last=rows.at(-1), prev=rows.at(-2);
  const bosUp=!!(sw.lastHigh && last.c>sw.lastHigh.price*1.0015 && prev.c<=sw.lastHigh.price*1.0015);
  const bosDn=!!(sw.lastLow && last.c<sw.lastLow.price*0.9985 && prev.c>=sw.lastLow.price*0.9985);
  return { bosUp, bosDn, chochUp:bosUp && sw.lhll, chochDn:bosDn && sw.hhhl, sw };
}
function r290OteZone(rows=[], side='LONG', lastPrice=0){
  rows=r289Rows(rows,90); const lp=Number(lastPrice||rows.at(-1)?.c||0); if(rows.length<18||!lp) return {ok:false,near:false,inZone:false};
  side=String(side||'LONG').toUpperCase();
  const piv=r289Pivots(rows,2,2); const pts=[...piv.highs.map(x=>({...x,type:'H'})), ...piv.lows.map(x=>({...x,type:'L'}))].sort((a,b)=>a.i-b.i).slice(-10);
  let low=null, high=null;
  if(side==='LONG'){
    for(let i=pts.length-1;i>=0;i--){ if(pts[i].type==='H'){ high=pts[i]; break; } }
    if(high){ for(let i=pts.findIndex(x=>x===high)-1;i>=0;i--){ if(pts[i].type==='L'){ low=pts[i]; break; } } }
    if(!low||!high||high.price<=low.price) return {ok:false,near:false,inZone:false};
    const zTop=high.price-(high.price-low.price)*0.62, zBot=high.price-(high.price-low.price)*0.79;
    const inZone=lp>=zBot*0.996 && lp<=zTop*1.004; const near=Math.min(Math.abs(r289Pct(lp,zBot)),Math.abs(r289Pct(lp,zTop)))<=0.75;
    return {ok:true,side,low:zBot,high:zTop,inZone,near,legLow:low.price,legHigh:high.price};
  } else {
    for(let i=pts.length-1;i>=0;i--){ if(pts[i].type==='L'){ low=pts[i]; break; } }
    if(low){ for(let i=pts.findIndex(x=>x===low)-1;i>=0;i--){ if(pts[i].type==='H'){ high=pts[i]; break; } } }
    if(!low||!high||high.price<=low.price) return {ok:false,near:false,inZone:false};
    const zBot=low.price+(high.price-low.price)*0.62, zTop=low.price+(high.price-low.price)*0.79;
    const inZone=lp>=zBot*0.996 && lp<=zTop*1.004; const near=Math.min(Math.abs(r289Pct(lp,zBot)),Math.abs(r289Pct(lp,zTop)))<=0.75;
    return {ok:true,side,low:zBot,high:zTop,inZone,near,legLow:low.price,legHigh:high.price};
  }
}
function r290CandleState(rows=[]){
  rows=r289Rows(rows,20); const last=rows.at(-1), prev=rows.at(-2), prev2=rows.at(-3); if(!last||!prev) return {};
  const rng=Math.max(1e-12,last.h-last.l), body=Math.abs(last.c-last.o);
  const upper=(last.h-Math.max(last.c,last.o))/rng, lower=(Math.min(last.c,last.o)-last.l)/rng;
  const bull=last.c>last.o, bear=last.c<last.o;
  const bullEngulf=!!(bull && prev.c<prev.o && last.c>prev.o && last.o<prev.c);
  const bearEngulf=!!(bear && prev.c>prev.o && last.c<prev.o && last.o>prev.c);
  let upSeq=0,dnSeq=0; for(let i=rows.length-1;i>=0;i--){ const r=rows[i]; if(r.c>r.o){ if(dnSeq) break; upSeq++; } else if(r.c<r.o){ if(upSeq) break; dnSeq++; } else break; }
  const p3=prev2?r289Pct(last.c,prev2.c):0;
  return { bull,bear,bullEngulf,bearEngulf,upperReject:upper>=0.38&&last.c<last.h-rng*0.45,lowerReject:lower>=0.38&&last.c>last.l+rng*0.45,bodyRatio:body/rng,upper,lower,upSeq,dnSeq,p3 };
}
function r290OpenSourceSmc5mCalibrator(symbol, k5m=[], k15m=[], k1h=[], k4h=[], lastPrice=0, longD={}, shortD={}){
  const k5=r289Rows(k5m,120), k15=r289Rows(k15m,90), k1=r289Rows(k1h,90), k4=r289Rows(k4h,90);
  const lp=Number(lastPrice||k5.at(-1)?.c||0); const empty=(side)=>({side,tradeOk:false,hardNo:false,capacity:0,setup:'NONE',notes:['veri yok'],summary:`R290 ${side} veri yok`});
  if(k5.length<28||!lp) return {ok:false,long:empty('LONG'),short:empty('SHORT'),summary:'R290 veri yok'};
  const liq5=r290LiquidityMap(k5,lp), bos5=r290BosChoch(k5), c5=r290CandleState(k5), atr=r290AtrPct(k5,14);
  const m15=r289TfMap(k15,lp), m1=r289TfMap(k1,lp), m4=r289TfMap(k4,lp);
  const hi48=Math.max(...k5.slice(-48).map(x=>x.h)), lo48=Math.min(...k5.slice(-48).map(x=>x.l)); const rangePos=(hi48>lo48)?(lp-lo48)/(hi48-lo48):0.5;
  const fvgL=r289NearestFvg(k5,'LONG',lp), fvgS=r289NearestFvg(k5,'SHORT',lp); const fvg15L=m15.bullFvg, fvg15S=m15.bearFvg;
  const oteL=r290OteZone(k5,'LONG',lp), oteS=r290OteZone(k5,'SHORT',lp);
  const tfBull=(m15.swing.hhhl?1:0)+(m1.swing.hhhl?1:0)+(m4.swing.hhhl?1:0), tfBear=(m15.swing.lhll?1:0)+(m1.swing.lhll?1:0)+(m4.swing.lhll?1:0);
  function sideEval(side,d){
    const isL=side==='LONG'; const txt=[d?.reason,d?.brainSummary,d?.r125OrderflowSummary,d?.r126FlowSummary,d?.htfTani,d?.ictDashboard].filter(Boolean).join(' ');
    const delta=Number(d?.r125LiveDeltaPct ?? d?.r125Flow?.deltaPct ?? 0)||0; const flowSide=String(d?.r125BestSide||d?.r125Flow?.bestSide||'').toUpperCase();
    const flowAligned=flowSide===side || (isL?delta>=18:delta<=-18) || (isL?/R125\s*(LONG|YÜKSELİŞ).*L\d+\/S0/i.test(txt):/R125\s*(SHORT|DÜŞÜŞ).*L0\/S\d+/i.test(txt));
    const flowAgainst=(flowSide && flowSide!=='NEUTRAL' && flowSide!==side) || (isL?delta<=-28:delta>=28);
    const fvgRetest=!!(isL?((fvgL&&(fvgL.inZone||fvgL.near))||(fvg15L&&(fvg15L.inZone||fvg15L.near))):(fvgS&&(fvgS.inZone||fvgS.near))||(fvg15S&&(fvg15S.inZone||fvg15S.near)));
    const oteOk=!!(isL?(oteL.inZone||oteL.near):(oteS.inZone||oteS.near));
    const sweepReclaim=!!(isL?(liq5.sweptDown||c5.lowerReject||/SSL_ALINDI|SSL wick\+body|body-reclaim|body-geri|ChoCH|MSS/i.test(txt)):(liq5.sweptUp||c5.upperReject||/BSL_ALINDI|BSL wick\+body|body-reclaim|body-geri|ChoCH|MSS/i.test(txt)));
    const bos=!!(isL?(bos5.bosUp||bos5.chochUp):(bos5.bosDn||bos5.chochDn));
    const candle=!!(isL?(c5.bull&&c5.bodyRatio>=0.35||c5.bullEngulf||c5.lowerReject):(c5.bear&&c5.bodyRatio>=0.35||c5.bearEngulf||c5.upperReject));
    const tfAlign=isL?tfBull:tfBear; const room=isL?Math.min(liq5.upDist,m15.upDist,m1.upDist,m4.upDist):Math.min(liq5.downDist,m15.downDist,m1.downDist,m4.downDist);
    const chase=!!(isL?(rangePos>=0.82 && c5.p3>=1.45 && !fvgRetest && !oteOk && !sweepReclaim):(rangePos<=0.18 && c5.p3<=-1.45 && !fvgRetest && !oteOk && !sweepReclaim));
    const wall=room<=0.55 && !bos && !sweepReclaim; const oiExtreme=/OI15:\s*\d{3,}|OI15m:\s*[89]\d|ACCEL|UP VPIN|SAHTE_PUMP|OI:çözülüyor/i.test(txt) && !fvgRetest && !sweepReclaim && !bos;
    let setup='NONE';
    if(sweepReclaim && (candle||bos||flowAligned)) setup='LIQUIDITY_RECLAIM';
    else if((fvgRetest||oteOk) && (candle||flowAligned||bos) && !chase) setup='FVG_OTE_RETEST';
    else if(bos && flowAligned && !wall && !chase && atr<8) setup='BOS_CONTINUATION';
    else if((isL?tfBull>=2:tfBear>=2) && flowAligned && !chase && room>0.8 && c5.bodyRatio>=0.25) setup='EARLY_CONTINUATION';
    const capacity=r289Clamp(
      32 + (setup!=='NONE'?24:0) + (sweepReclaim?18:0) + ((fvgRetest||oteOk)?16:0) + (bos?14:0) + (tfAlign*5) + (flowAligned?9:0) + (candle?7:0) + (room>1.2?6:0)
      - (flowAgainst?28:0) - (chase?22:0) - (wall?18:0) - (oiExtreme?12:0) - (atr>9?12:0), 0, 92);
    const hardNo=!!(flowAgainst || chase || wall || (oiExtreme&&setup==='NONE') || (atr>11&&!sweepReclaim));
    const tradeOk=!!(!hardNo && setup!=='NONE' && capacity>=66);
    const notes=[]; notes.push(`setup:${setup}`); if(sweepReclaim)notes.push('liq sweep/reclaim'); if(fvgRetest)notes.push('FVG retest'); if(oteOk)notes.push('OTE zone'); if(bos)notes.push('BOS/CHoCH close break'); if(tfAlign)notes.push(`MTF align ${tfAlign}/3`); if(flowAligned)notes.push('flow aynı yön'); if(candle)notes.push('mum tetik'); if(chase)notes.push('chase/range ucu'); if(wall)notes.push('yakın likidite duvarı'); if(oiExtreme)notes.push('OI tek başına'); if(flowAgainst)notes.push('flow ters');
    return {side,tradeOk,hardNo,capacity:+capacity.toFixed(1),setup,rangePos:+rangePos.toFixed(2),p3:+c5.p3.toFixed(2),room:+room.toFixed(2),atr:+atr.toFixed(2),flowAligned,flowAgainst,fvgRetest,oteOk,sweepReclaim,bos,candle,chase,wall,notes:notes.slice(0,10),summary:`R290 ${tradeOk?'TRADE':'WAIT'} ${side} ${setup} kapasite:${capacity.toFixed(1)} · ${notes.slice(0,7).join(' · ')}`};
  }
  const long=sideEval('LONG',longD||{}), short=sideEval('SHORT',shortD||{});
  return {ok:true,long,short,summary:`R290 SMC 5m: L ${long.setup}/${long.capacity} S ${short.setup}/${short.capacity} · range ${rangePos.toFixed(2)} ATR ${atr.toFixed(2)} liqUp ${liq5.upDist.toFixed(2)} liqDn ${liq5.downDist.toFixed(2)}`};
}

function r291NormSide(x='') {
  x = String(x||'').toUpperCase();
  if (/LONG|YÜKSEL|YUKSEL|BUY|BULL/.test(x)) return 'LONG';
  if (/SHORT|DÜŞ|DUS|SELL|BEAR/.test(x)) return 'SHORT';
  return 'NEUTRAL';
}
function r291ParseOrderBookText(txt='', side='LONG') {
  side = String(side||'LONG').toUpperCase();
  const isL = side === 'LONG';
  const n = (v,d=0)=>Number.isFinite(Number(v))?Number(v):d;
  const out = {bookSide:'NEUTRAL',deepSide:'NEUTRAL',microSide:'NEUTRAL',deltaSide:'NEUTRAL',bookImb:0,deepImb:0,microPct:0,deltaPct:0,upperWall:null,lowerWall:null,spreadRisk:false,thinBook:false,notes:[]};
  const mBook = String(txt).match(/book:\s*([A-ZÇĞİÖŞÜYÜKSELDÜŞ]+)\s+imb:\s*([-\d.]+)%/i);
  if (mBook) { out.bookSide = r291NormSide(mBook[1]); out.bookImb = n(mBook[2]); }
  const mDeep = String(txt).match(/deep:\s*([A-ZÇĞİÖŞÜYÜKSELDÜŞ]+)\s+([-\d.]+)%/i);
  if (mDeep) { out.deepSide = r291NormSide(mDeep[1]); out.deepImb = n(mDeep[2]); }
  const mMicro = String(txt).match(/micro:\s*([A-ZÇĞİÖŞÜYÜKSELDÜŞ]+)\s+([-\d.]+)%/i);
  if (mMicro) { out.microSide = r291NormSide(mMicro[1]); out.microPct = n(mMicro[2]); }
  const mDelta = String(txt).match(/delta:\s*([A-ZÇĞİÖŞÜYÜKSELDÜŞ]+)\s+([-\d.]+)%/i);
  if (mDelta) { out.deltaSide = r291NormSide(mDelta[1]); out.deltaPct = n(mDelta[2]); }
  const wallObj = (m)=> m ? {price:n(m[1]), usd:n(m[2]), gap:n(m[3]), raw:m[0]} : null;
  out.upperWall = wallObj(String(txt).match(/üst:\s*([\d.]+)\s+([\d.]+)\$\s*g(\d+)/i));
  out.lowerWall = wallObj(String(txt).match(/alt:\s*([\d.]+)\s+([\d.]+)\$\s*g(\d+)/i));
  out.spreadRisk = /spreadBlock|poorLiquidity|makas:(?!normal)|defter:(İNCE|INCE|zayıf|zayif)|TEHLİKELİ|tehlikeli/i.test(String(txt));
  out.thinBook = /defter:(İNCE|INCE|zayıf|zayif)|thin book|book thin/i.test(String(txt));
  const same = (x)=>r291NormSide(x)===side;
  const opp  = (x)=>{ const y=r291NormSide(x); return y!==side && y!=='NEUTRAL'; };
  let align = 0, against = 0;
  if (same(out.bookSide) || (isL ? out.bookImb>=35 : out.bookImb<=-35)) align += 1.1; else if (opp(out.bookSide) || (isL ? out.bookImb<=-35 : out.bookImb>=35)) against += 1.1;
  if (same(out.deepSide) || (isL ? out.deepImb>=18 : out.deepImb<=-18)) align += 1.0; else if (opp(out.deepSide) || (isL ? out.deepImb<=-18 : out.deepImb>=18)) against += 1.0;
  if (same(out.microSide) || (isL ? out.microPct>=0.018 : out.microPct<=-0.018)) align += 0.8; else if (opp(out.microSide) || (isL ? out.microPct<=-0.018 : out.microPct>=0.018)) against += 0.8;
  if (same(out.deltaSide) || (isL ? out.deltaPct>=25 : out.deltaPct<=-25)) align += 1.4; else if (opp(out.deltaSide) || (isL ? out.deltaPct<=-25 : out.deltaPct>=25)) against += 1.4;
  const upperNear = !!(out.upperWall && out.upperWall.gap <= 30);
  const lowerNear = !!(out.lowerWall && out.lowerWall.gap <= 30);
  const upperBig = !!(out.upperWall && out.upperWall.usd >= 900);
  const lowerBig = !!(out.lowerWall && out.lowerWall.usd >= 900);
  const sameWall = isL ? (lowerNear && lowerBig) : (upperNear && upperBig);
  const oppWall  = isL ? (upperNear && upperBig) : (lowerNear && lowerBig);
  const bookVacuum = isL ? (!out.upperWall || out.upperWall.gap >= 44) : (!out.lowerWall || out.lowerWall.gap >= 44);
  out.alignScore = +(align-against).toFixed(2);
  out.flowAligned = align >= 2.1 && align > against;
  out.flowAgainst = against >= 2.0 && against > align;
  out.sameWall = sameWall;
  out.oppWall = oppWall;
  out.bookVacuum = bookVacuum;
  if (out.flowAligned) out.notes.push('book/depth/delta aynı hikâye');
  if (out.flowAgainst) out.notes.push('book/depth/delta ters hikâye');
  if (sameWall) out.notes.push(isL?'alt hidden wall destek':'üst hidden wall direnç');
  if (oppWall) out.notes.push(isL?'üst hidden wall risk':'alt hidden wall risk');
  if (bookVacuum && out.flowAligned) out.notes.push('order book gap/vacuum lehte');
  if (out.spreadRisk) out.notes.push('spread/depth risk');
  return out;
}
function r291SmcLiquidityConfluence(symbol, k5m=[], k15m=[], k1h=[], k4h=[], lastPrice=0, longD={}, shortD={}) {
  const k5=r289Rows(k5m,140), k15=r289Rows(k15m,100), k1=r289Rows(k1h,100), k4=r289Rows(k4h,90);
  const lp=Number(lastPrice||k5.at(-1)?.c||0);
  const empty=(side)=>({side,tradeOk:false,hardNo:false,capacity:0,setup:'NONE',notes:['veri yok'],summary:`R291 ${side} veri yok`});
  if(k5.length<30||!lp) return {ok:false,long:empty('LONG'),short:empty('SHORT'),summary:'R291 veri yok'};
  const c5=r290CandleState(k5), liq5=r290LiquidityMap(k5,lp), bos5=r290BosChoch(k5);
  const fvg5L=r289NearestFvg(k5,'LONG',lp), fvg5S=r289NearestFvg(k5,'SHORT',lp);
  const m15=r289TfMap(k15,lp), m1=r289TfMap(k1,lp), m4=r289TfMap(k4,lp);
  const oteL=r290OteZone(k5,'LONG',lp), oteS=r290OteZone(k5,'SHORT',lp);
  const hi=Math.max(...k5.slice(-60).map(x=>x.h)), lo=Math.min(...k5.slice(-60).map(x=>x.l));
  const rangePos=(hi>lo)?(lp-lo)/(hi-lo):0.5;
  const atr=r290AtrPct(k5,14);
  const tfBull=(m15.swing.hhhl?1:0)+(m1.swing.hhhl?1:0)+(m4.swing.hhhl?1:0);
  const tfBear=(m15.swing.lhll?1:0)+(m1.swing.lhll?1:0)+(m4.swing.lhll?1:0);
  function evalSide(side,d){
    const isL=side==='LONG'; const txt=[d?.reason,d?.brainSummary,d?.r125OrderflowSummary,d?.r126FlowSummary,d?.htfTani,d?.ictDashboard,d?.siksmaOzet,d?.r140Summary].filter(Boolean).join(' ');
    const r290=d?.r290Smc5m||{}; const r289=d?.r289Playbook||{}; const book=r291ParseOrderBookText(txt,side);
    const fvgRetest=!!(isL ? ((fvg5L&&(fvg5L.inZone||fvg5L.near)) || (m15.bullFvg&&(m15.bullFvg.inZone||m15.bullFvg.near)) || r290.fvgRetest || r289.fvgRetest) : ((fvg5S&&(fvg5S.inZone||fvg5S.near)) || (m15.bearFvg&&(m15.bearFvg.inZone||m15.bearFvg.near)) || r290.fvgRetest || r289.fvgRetest));
    const oteOk=!!(isL ? (oteL.inZone||oteL.near||r290.oteOk) : (oteS.inZone||oteS.near||r290.oteOk));
    const sweepReclaim=!!(isL ? (liq5.sweptDown || c5.lowerReject || /SSL_ALINDI|SSL.*body|alt iğne|Dragonfly|Hammer|body[- ]?(reclaim|geri)/i.test(txt) || r290.sweepReclaim || r289.reclaim) : (liq5.sweptUp || c5.upperReject || /BSL_ALINDI|BSL.*body|üst iğne|Shooting|DarkCloud|body[- ]?(reclaim|geri)/i.test(txt) || r290.sweepReclaim || r289.reclaim));
    const bos=!!(isL ? (bos5.bosUp||bos5.chochUp||r290.bos||r289.broke) : (bos5.bosDn||bos5.chochDn||r290.bos||r289.broke));
    const candle=!!(isL ? (c5.bullEngulf || c5.lowerReject || (c5.bull&&c5.bodyRatio>=0.38) || /Hammer|Dragonfly|Bull|YÜKSELİŞ mum teyidi/i.test(txt)) : (c5.bearEngulf || c5.upperReject || (c5.bear&&c5.bodyRatio>=0.38) || /Shooting|DarkCloud|Bear|DÜŞÜŞ mum teyidi/i.test(txt)));
    const tfAlign=isL?tfBull:tfBear;
    const room=isL?Math.min(liq5.upDist,m15.upDist,m1.upDist,m4.upDist):Math.min(liq5.downDist,m15.downDist,m1.downDist,m4.downDist);
    const chase=!!(isL ? (rangePos>=0.84 && c5.p3>=1.35 && !fvgRetest && !oteOk && !sweepReclaim) : (rangePos<=0.16 && c5.p3<=-1.35 && !fvgRetest && !oteOk && !sweepReclaim));
    const nearWall=!!(room<=0.55 && !bos && !sweepReclaim);
    const oiExtreme=!!(/OI15:\s*\d{3,}|OI15m:\s*[89]\d|ACCEL|UP VPIN|SAHTE_PUMP|OI:çözülüyor/i.test(txt) && !sweepReclaim && !fvgRetest && !bos);
    const weakNoGraph=!!(/formasyon yok|puan 0\/12|mum kanıtı zayıf|teyidi zayıf/i.test(txt) && !candle && !sweepReclaim && !fvgRetest && !oteOk && !bos);
    const spreadRisk=!!(book.spreadRisk || d?.r190Edge?.spreadBlock || /TEHLİKELİ|poorLiquidity|makas:(?!normal)/i.test(txt));
    let setup='NONE';
    if(sweepReclaim && (candle||book.flowAligned||bos)) setup='LIQUIDITY_SWEEP_RECLAIM';
    else if((fvgRetest||oteOk) && (book.flowAligned||candle||bos) && !chase) setup='FVG_OTE_RETEST';
    else if(bos && book.flowAligned && room>0.65 && !chase) setup='BOS_CHOCH_CONTINUATION';
    else if(tfAlign>=2 && book.flowAligned && room>0.9 && !chase && (candle||r290.capacity>=74)) setup='MTF_CONTINUATION';
    const smcProofs=[sweepReclaim,fvgRetest||oteOk,bos,candle,tfAlign>=2].filter(Boolean).length;
    const liqProofs=[book.flowAligned,book.sameWall,book.bookVacuum && book.flowAligned,room>1.0].filter(Boolean).length;
    const riskCount=[book.flowAgainst,book.oppWall,spreadRisk,chase,nearWall,oiExtreme,weakNoGraph,atr>9].filter(Boolean).length;
    let capacity = 18 + Number(r290.capacity||0)*0.38 + Number(r289.quality||0)*0.12 + smcProofs*8.5 + liqProofs*7.5 + (setup!=='NONE'?12:0) + (tfAlign*3.5) + (room>1.4?5:0) - riskCount*12;
    if (book.flowAligned && (sweepReclaim||fvgRetest||oteOk||bos)) capacity += 8;
    if (book.flowAgainst && !sweepReclaim) capacity -= 18;
    if (weakNoGraph && /R156 TOP10 hızlı bypass|R144 hızlı|edge mikro-scalp|hızlı bypass/i.test(txt)) capacity -= 14;
    capacity = r289Clamp(capacity,0,94);
    const fastBypass=!!(d?.r156FastBypass || /R156 TOP10 hızlı bypass|R144 hızlı|edge mikro-scalp|hızlı bypass/i.test(txt));
    const hardNo=!!(spreadRisk || (book.flowAgainst && chase && !sweepReclaim) || (nearWall && !bos && !sweepReclaim) || (oiExtreme && smcProofs<1 && weakNoGraph) || (atr>16 && !sweepReclaim));
    const need = fastBypass ? 70 : 66;
    const tradeOk=!!(!hardNo && setup!=='NONE' && capacity>=need && smcProofs>=2);
    const radar=!!(!tradeOk && !hardNo && setup!=='NONE' && capacity>=58);
    const notes=[]; notes.push(`setup:${setup}`); notes.push(`SMC:${smcProofs}/5 LIQ:${liqProofs}/4 risk:${riskCount}`); if(sweepReclaim)notes.push('sweep/reclaim'); if(fvgRetest)notes.push('FVG retest'); if(oteOk)notes.push('OTE'); if(bos)notes.push('BOS/CHoCH'); if(candle)notes.push('mum kabul'); if(tfAlign)notes.push(`MTF ${tfAlign}/3`); notes.push(...book.notes.slice(0,3)); if(chase)notes.push('chase/range ucu'); if(nearWall)notes.push('yakın likidite duvarı'); if(oiExtreme)notes.push('OI tek başına'); if(weakNoGraph)notes.push('grafik tetik zayıf');
    return {side,tradeOk,radar,hardNo,capacity:+capacity.toFixed(1),setup,smcProofs,liqProofs,riskCount,rangePos:+rangePos.toFixed(2),p3:+c5.p3.toFixed(2),room:+room.toFixed(2),atr:+atr.toFixed(2),flowAligned:book.flowAligned,flowAgainst:book.flowAgainst,fvgRetest,oteOk,sweepReclaim,bos,candle,chase,nearWall,oiExtreme,weakNoGraph,notes:notes.slice(0,12),summary:`R291 ${tradeOk?'TRADE':radar?'RADAR':'WAIT'} ${side} ${setup} kapasite:${capacity.toFixed(1)} · ${notes.slice(0,8).join(' · ')}`};
  }
  const long=evalSide('LONG',longD||{}), short=evalSide('SHORT',shortD||{});
  return {ok:true,long,short,summary:`R291 SMC+LIQ: L ${long.setup}/${long.capacity} S ${short.setup}/${short.capacity} · ${long.notes?.[1]||''} / ${short.notes?.[1]||''}`};
}

function r109CalcSweepReclaimScore(k5m, side) {
  const rows = Array.isArray(k5m) ? k5m.slice(-8).map(k => ({
    o: +k[1], h: +k[2], l: +k[3], c: +k[4], v: +k[5]
  })).filter(k => k.h > 0) : [];

  if (rows.length < 4) return { score: 0, swept: false, reclaimed: false, cvdAlign: false };

  const isLong = side === 'LONG';
  const last  = rows.at(-1);
  const prev  = rows.at(-2);
  const prev2 = rows.at(-3);
  const prev3 = rows.at(-4);

  let score = 0;
  let swept = false, reclaimed = false, cvdAlign = false;

  if (isLong) {
    const prevMin3 = Math.min(prev2.l, prev3.l, rows.at(-5)?.l || prev3.l);
    const sweepMum = [prev, prev2, prev3].find(k => k.l < prevMin3 * 0.999);
    swept = !!sweepMum;
    if (swept) score += 3;

    const reclaimTarget = sweepMum ? sweepMum.h : prev.h;
    reclaimed = last.c > reclaimTarget * 0.998 && last.c > last.o;
    if (reclaimed) score += 4;

    const body = last.c - last.o;
    const range = last.h - last.l;
    if (body > 0 && range > 0 && body / range > 0.5) score += 2;

    if (last.c > prev.h) score += 1;

  } else {
    const prevMax3 = Math.max(prev2.h, prev3.h, rows.at(-5)?.h || prev3.h);
    const sweepMum = [prev, prev2, prev3].find(k => k.h > prevMax3 * 1.001);
    swept = !!sweepMum;
    if (swept) score += 3;

    const reclaimTarget = sweepMum ? sweepMum.l : prev.l;
    reclaimed = last.c < reclaimTarget * 1.002 && last.c < last.o;
    if (reclaimed) score += 4;

    const body = last.o - last.c;
    const range = last.h - last.l;
    if (body > 0 && range > 0 && body / range > 0.5) score += 2;

    if (last.c < prev.l) score += 1;
  }

  const last2closes = rows.slice(-3);
  cvdAlign = isLong
    ? last2closes.filter(k => k.c > k.o).length >= 2
    : last2closes.filter(k => k.c < k.o).length >= 2;
  if (cvdAlign) score += 1;

  return { score: Math.min(10, score), swept, reclaimed, cvdAlign };
}

function r93AnalyzeStairAndTurn(klines5m) {
  const rows = (Array.isArray(klines5m) ? klines5m : [])
    .filter(k => k && k.length >= 6)
    .map(k => ({
      o: Number(k[1]), h: Number(k[2]), l: Number(k[3]), c: Number(k[4]), v: Number(k[5]), ts: Number(k[0] || 0)
    }))
    .filter(k => Number.isFinite(k.o) && Number.isFinite(k.h) && Number.isFinite(k.l) && Number.isFinite(k.c) && k.o > 0 && k.h > 0 && k.l > 0 && k.c > 0);

  const out = {
    ok:false,
    longMerdiven:false, shortMerdiven:false,
    longDevam:false, shortDevam:false,
    longdanShorta:false, shorttanLonga:false,
    longDonusSkor:0, shortDonusSkor:0,
    longMerdivenSkor:0, shortMerdivenSkor:0,
    sonMum:'NÖTR', sonMumTers:false,
    sonTepeGeriAlindi:false, sonDipGeriAlindi:false,
    notlar:[]
  };
  if (rows.length < 8) return out;
  const r = rows.slice(-12);
  const last = r.at(-1), prev = r.at(-2), prev2 = r.at(-3);
  const first = r[0];
  const avgVol = r.slice(0,-1).reduce((a,k)=>a+(Number(k.v)||0),0) / Math.max(1, r.length-1);
  const chgPct = first.o > 0 ? ((last.c - first.o) / first.o * 100) : 0;
  const bodyPct = k => Math.abs(k.c-k.o) / Math.max(1e-12, k.o) * 100;
  const rangePct = k => (k.h-k.l) / Math.max(1e-12, k.o) * 100;
  const upperWickPct = k => (k.h - Math.max(k.o,k.c)) / Math.max(1e-12, k.o) * 100;
  const lowerWickPct = k => (Math.min(k.o,k.c) - k.l) / Math.max(1e-12, k.o) * 100;

  let higherHigh=0, higherLow=0, higherClose=0, lowerHigh=0, lowerLow=0, lowerClose=0, green=0, red=0, volUp=0, volDn=0;
  for (let i=1;i<r.length;i++) {
    if (r[i].h > r[i-1].h) higherHigh++;
    if (r[i].l >= r[i-1].l) higherLow++;
    if (r[i].c > r[i-1].c) higherClose++;
    if (r[i].h <= r[i-1].h) lowerHigh++;
    if (r[i].l < r[i-1].l) lowerLow++;
    if (r[i].c < r[i-1].c) lowerClose++;
    if (r[i].c >= r[i].o) { green++; volUp += Number(r[i].v)||0; }
    else { red++; volDn += Number(r[i].v)||0; }
  }

  const recentHigh = Math.max(...r.slice(0,-1).map(k=>k.h));
  const recentLow  = Math.min(...r.slice(0,-1).map(k=>k.l));
  const lastRedStrong = last.c < last.o && (bodyPct(last) >= Math.max(0.25, rangePct(last)*0.45) || last.v > avgVol*1.15);
  const lastGreenStrong = last.c > last.o && (bodyPct(last) >= Math.max(0.25, rangePct(last)*0.45) || last.v > avgVol*1.15);
  const twoRed = last.c < last.o && prev.c < prev.o;
  const twoGreen = last.c > last.o && prev.c > prev.o;
  const failedHigh = prev.h >= recentHigh*0.998 && last.c < prev.c && last.c < (prev.o + prev.c)/2;
  const failedLow  = prev.l <= recentLow*1.002 && last.c > prev.c && last.c > (prev.o + prev.c)/2;
  const highNotReclaimed = last.c < Math.max(prev.h, prev2.h) && (lastRedStrong || twoRed);
  const lowNotReclaimed  = last.c > Math.min(prev.l, prev2.l) && (lastGreenStrong || twoGreen);
  const upVolShare = volUp / Math.max(1e-9, volUp + volDn);
  const downVolShare = volDn / Math.max(1e-9, volUp + volDn);

  const ctx = rows.slice(-36);
  const ctxFirst = ctx[0] || first;
  const ctxHigh = Math.max(...ctx.map(k=>k.h));
  const ctxLow  = Math.min(...ctx.map(k=>k.l));
  const ctxRange = Math.max(1e-12, ctxHigh - ctxLow);
  const ctxChgPct = ctxFirst.o > 0 ? ((last.c - ctxFirst.o) / ctxFirst.o * 100) : chgPct;
  const ctxPos = (last.c - ctxLow) / ctxRange;
  let ctxUpClose=0, ctxDownClose=0, ctxHigherLow=0, ctxLowerHigh=0;
  for (let i=1;i<ctx.length;i++) {
    if (ctx[i].c > ctx[i-1].c) ctxUpClose++; else if (ctx[i].c < ctx[i-1].c) ctxDownClose++;
    if (ctx[i].l >= ctx[i-1].l) ctxHigherLow++;
    if (ctx[i].h <= ctx[i-1].h) ctxLowerHigh++;
  }
  const ctxLongAlive = !!(ctx.length >= 18 && ctxChgPct >= 2.2 && ctxPos >= 0.58 && ctxUpClose >= Math.max(5, ctxDownClose * 0.85));
  const ctxShortAlive = !!(ctx.length >= 18 && ctxChgPct <= -2.2 && ctxPos <= 0.42 && ctxDownClose >= Math.max(5, ctxUpClose * 0.85));

  const longStairScore =
    (chgPct > 1.0 ? 2 : chgPct > 0.45 ? 1 : 0) +
    (higherLow >= 6 ? 3 : higherLow >= 4 ? 2 : higherLow >= 3 ? 1 : 0) +
    (higherHigh >= 5 ? 2 : higherHigh >= 3 ? 1 : 0) +
    (higherClose >= 5 ? 2 : higherClose >= 3 ? 1 : 0) +
    (green >= red ? 1 : 0) +
    (upVolShare >= 0.55 ? 1 : 0);
  const shortStairScore =
    (chgPct < -1.0 ? 2 : chgPct < -0.45 ? 1 : 0) +
    (lowerHigh >= 6 ? 3 : lowerHigh >= 4 ? 2 : lowerHigh >= 3 ? 1 : 0) +
    (lowerLow >= 5 ? 2 : lowerLow >= 3 ? 1 : 0) +
    (lowerClose >= 5 ? 2 : lowerClose >= 3 ? 1 : 0) +
    (red >= green ? 1 : 0) +
    (downVolShare >= 0.55 ? 1 : 0);

  let longToShort = 0;
  if (ctxLongAlive && (lastRedStrong || twoRed || failedHigh || highNotReclaimed)) longToShort += 2;
  if (longStairScore >= 7) longToShort += 2;
  if (failedHigh || highNotReclaimed) longToShort += 2;
  if (lastRedStrong) longToShort += 2;
  if (twoRed) longToShort += 1;
  if (last.c < prev.l) longToShort += 2;
  if (upperWickPct(prev) > bodyPct(prev)*0.7 && prev.h >= recentHigh*0.995) longToShort += 1;
  if (last.v > avgVol*1.15 && last.c < last.o) longToShort += 1;
  if (last.c > recentHigh) longToShort -= 3;
  if (lastGreenStrong) longToShort -= 2;

  let shortToLong = 0;
  if (ctxShortAlive && (lastGreenStrong || twoGreen || failedLow || lowNotReclaimed)) shortToLong += 2;
  if (shortStairScore >= 7) shortToLong += 2;
  if (failedLow || lowNotReclaimed) shortToLong += 2;
  if (lastGreenStrong) shortToLong += 2;
  if (twoGreen) shortToLong += 1;
  if (last.c > prev.h) shortToLong += 2;
  if (lowerWickPct(prev) > bodyPct(prev)*0.7 && prev.l <= recentLow*1.005) shortToLong += 1;
  if (last.v > avgVol*1.15 && last.c > last.o) shortToLong += 1;
  if (last.c < recentLow) shortToLong -= 3;
  if (lastRedStrong) shortToLong -= 2;

  out.ok = true;
  out.longMerdivenSkor = longStairScore;
  out.shortMerdivenSkor = shortStairScore;
  out.longMerdiven = longStairScore >= 6 || ctxLongAlive;
  out.shortMerdiven = shortStairScore >= 6 || ctxShortAlive;
  out.longDonusSkor = Math.max(0, longToShort);
  out.shortDonusSkor = Math.max(0, shortToLong);
  out.longdanShorta = out.longDonusSkor >= 6;
  out.shorttanLonga = out.shortDonusSkor >= 6;
  out.longDevam = out.longMerdiven && !out.longdanShorta && !lastRedStrong && !twoRed;
  out.shortDevam = out.shortMerdiven && !out.shorttanLonga && !lastGreenStrong && !twoGreen;
  out.sonMum = lastGreenStrong ? 'GÜÇLÜ YEŞİL' : lastRedStrong ? 'GÜÇLÜ KIRMIZI' : (last.c>=last.o ? 'YEŞİL' : 'KIRMIZI');
  out.sonMumTers = lastRedStrong || lastGreenStrong;
  out.sonTepeGeriAlindi = last.c > recentHigh;
  out.sonDipGeriAlindi = last.c < recentLow;
  if (out.longMerdiven) out.notlar.push(ctxLongAlive ? 'geniş pencerede yükseliş merdiveni' : 'yükseliş merdiveni');
  if (out.shortMerdiven) out.notlar.push(ctxShortAlive ? 'geniş pencerede düşüş merdiveni' : 'düşüş merdiveni');
  if (out.longdanShorta) out.notlar.push('tepe yoruldu, düşüş radarı');
  if (out.shorttanLonga) out.notlar.push('dip yoruldu, yükseliş radarı');
  return out;
}

function updateSweepDetector(det, price, isBuy, usdt) {
  det.tickCount++;
  det.lastTicks.push({ price, isBuy, usdt, ts: Date.now() });
  if (det.lastTicks.length > 200) det.lastTicks.shift();

  const nearSwingHigh = det.swingHighs.find(h => price > h * 1.0005);
  const nearSwingLow  = det.swingLows.find(l => price < l * 0.9995);

  if (det.pendingSweep) {
    const age = Date.now() - det.pendingSweep.ts;
    if (age > 45000) { det.pendingSweep = null; return; }

    const ps = det.pendingSweep;
    if (ps.type === 'BULL' && price > ps.level && isBuy) {
      const recentBuys  = det.lastTicks.slice(-10).filter(t => t.isBuy).length;
      const recentSells = det.lastTicks.slice(-10).filter(t => !t.isBuy).length;
      if (recentBuys > recentSells * 1.5) {
        det.confirmed = {
          type: 'BULL_SWEEP', level: ps.level, sweepPrice: ps.sweepPrice,
          confirmPrice: price, ts: Date.now(),
          msg: `Tick BULL sweep: $${ps.level.toFixed(4)} altı → geri döndü, alım baskısı`,
          fresh: true,
        };
        det.pendingSweep = null;
      }
    }
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

  if (!det.pendingSweep) {
    if (nearSwingLow && !isBuy) {
      det.pendingSweep = { type:'BULL', level:nearSwingLow, sweepPrice:price, ts:Date.now() };
    }
    if (nearSwingHigh && isBuy) {
      det.pendingSweep = { type:'BEAR', level:nearSwingHigh, sweepPrice:price, ts:Date.now() };
    }
  }

  if (det.confirmed && Date.now() - det.confirmed.ts > 5 * 60 * 1000) {
    det.confirmed = null;
  }
}

const tickStarting = new Set();

const marketBreadthStore = {
  signals: [],
  bull: 0, bear: 0, neutral: 0,
  breadthScore: 0,
  ts: 0,
};
function updateMarketBreadth(sym, rec, score) {
  const now = Date.now();
  marketBreadthStore.signals = marketBreadthStore.signals.filter(s => now-s.ts < 20*60*1000);
  const idx = marketBreadthStore.signals.findIndex(s => s.sym === sym);
  if (idx >= 0) marketBreadthStore.signals[idx] = { sym, rec, score, ts:now };
  else marketBreadthStore.signals.push({ sym, rec, score, ts:now });
  if (marketBreadthStore.signals.length > 60) marketBreadthStore.signals.shift();
  const recent = marketBreadthStore.signals.filter(s => now-s.ts < 10*60*1000);
  const bull = recent.filter(s => s.rec==='LONG').length;
  const bear = recent.filter(s => s.rec==='SHORT').length;
  const total = bull + bear;
  marketBreadthStore.bull = bull; marketBreadthStore.bear = bear;
  marketBreadthStore.neutral = recent.length - total;
  marketBreadthStore.breadthScore = total > 0 ? (bull-bear)/total : 0;
  marketBreadthStore.ts = now;
}

let btcChange5mCache = 0;
const btcPriceRef = { p:0, prev:0, ts:0 };
async function refreshBtcChange5m() {
  if (Date.now() - btcPriceRef.ts < 25000) return;
  try {
    const t = await bPub('/fapi/v1/ticker/bookTicker', 'symbol=BTCUSDT');
    const np = parseFloat(t.bidPrice || t.askPrice || 0);
    if (btcPriceRef.p > 0 && np > 0)
      btcChange5mCache = (np - btcPriceRef.p) / btcPriceRef.p * 100;
    btcPriceRef.prev = btcPriceRef.p;
    btcPriceRef.p    = np;
    btcPriceRef.ts   = Date.now();
  } catch(_) {}
}

const r27SpreadHistory = new Map();
function updateSpreadHistory(sym, spreadPct) {
  const now = Date.now();
  const prev = r27SpreadHistory.get(sym);
  if (!prev) { r27SpreadHistory.set(sym, { cur:spreadPct, prev3m:spreadPct, ts:now }); return; }
  if (now - prev.ts > 3*60*1000) {
    r27SpreadHistory.set(sym, { cur:spreadPct, prev3m:prev.cur, ts:now });
  } else {
    prev.cur = spreadPct;
  }
}

async function startTickStream(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  const existing = tickStore.get(full);
  if (existing?.ws && (existing.ws.readyState === WebSocket.OPEN || existing.ws.readyState === WebSocket.CONNECTING)) return;
  if (tickStarting.has(full)) return;
  tickStarting.add(full);

  const tickSz = full.startsWith('BTC')?0.1:full.startsWith('ETH')?0.01:
                 full.startsWith('BNB')?0.01:0.0001;

  const engine = existing || createTickEngine(tickSz, 5);
  engine.tickSize = tickSz;
  if (!Array.isArray(engine.lastTicks)) engine.lastTicks = [];
  if (!Array.isArray(engine.candles)) engine.candles = [];
  tickStore.set(full, engine);

  bPub('/fapi/v1/ticker/24hr', 'symbol='+full).then(ticker => {
    const vol24h = Math.abs(parseFloat(ticker.priceChangePercent)||5);
    const tmp = createTickEngine(tickSz, vol24h);
    engine.candleMs = tmp.candleMs;
    engine.vol24h = +vol24h.toFixed(2);
    console.log(`${full} tick engine: candleMs=${engine.candleMs}ms vol24h=${vol24h.toFixed(1)}%`);
  }).catch(()=>{});

  const wsUrl = `${FAPI_WS_MARKET}/ws/${encodeURIComponent(full.toLowerCase())}@aggTrade`;
  const ws = new WebSocket(wsUrl);
  engine.ws = ws;

  ws.on('message', data => {
    try { processTick(engine, JSON.parse(data.toString())); } catch(e) {}
  });
  ws.on('open',  () => { tickStarting.delete(full); engine.lastOpenTs = Date.now(); });
  ws.on('close', () => { tickStarting.delete(full); setTimeout(()=>startTickStream(full), 3000); });
  ws.on('error', () => { tickStarting.delete(full); });
}

function calcVPIN(trades, bucketSize = 50) {
  if (!trades || trades.length < bucketSize * 3) return null;

  const buckets = [];
  let curBuy = 0, curSell = 0, curVol = 0;

  for (const t of trades) {
    const vol = t.usdt;
    if (t.isBuy) curBuy += vol;
    else          curSell += vol;
    curVol += vol;

    if (curVol >= bucketSize * 1000) {
      buckets.push({ buy: curBuy, sell: curSell, total: curVol,
        imbalance: Math.abs(curBuy - curSell) / curVol });
      curBuy = 0; curSell = 0; curVol = 0;
    }
  }

  if (buckets.length < 5) return null;

  const recent = buckets.slice(-10);
  const vpin = recent.reduce((s, b) => s + b.imbalance, 0) / recent.length;
  const vpinPct = +(vpin * 100).toFixed(1);

  const toxicity = vpinPct > 40 ? 'EXTREME' : vpinPct > 25 ? 'HIGH' :
                   vpinPct > 15 ? 'MEDIUM' : 'LOW';

  const lastBuySell = recent.slice(-3).reduce((s,b) => ({
    buy:s.buy+b.buy, sell:s.sell+b.sell }), {buy:0,sell:0});
  const direction = lastBuySell.buy > lastBuySell.sell * 1.3 ? 'BUY_DOMINANT' :
                    lastBuySell.sell > lastBuySell.buy * 1.3 ? 'SELL_DOMINANT' : 'NEUTRAL';

  return { vpin: vpinPct, toxicity, direction, bucketCount: buckets.length };
}

function calcDeltaMicrostructure(candles) {
  if (!candles || candles.length < 5) return null;

  const recent = candles.slice(-10);
  const deltas = recent.map(c => c.delta);
  const prices = recent.map(c => c.close);

  const avgAbsDelta = deltas.reduce((s,d) => s + Math.abs(d), 0) / deltas.length;
  const lastDelta = deltas[deltas.length-1];
  const exhaustion = Math.abs(lastDelta) > avgAbsDelta * 2.5;

  const priceHigher = prices[prices.length-1] > prices[0];
  const deltaLower  = lastDelta < deltas[0];
  const bullDivergence = !priceHigher && !deltaLower;
  const bearDivergence = priceHigher && deltaLower;

  const lastCandle = recent[recent.length-1];
  const priceRange = lastCandle ? Math.abs(lastCandle.high - lastCandle.low) : 0;
  const totalVol = lastCandle ? (lastCandle.buy + lastCandle.sell) : 0;
  const avgVol = recent.reduce((s,c) => s+(c.buy+c.sell), 0) / recent.length;
  const absorption = totalVol > avgVol * 2 && priceRange < (prices[0] * 0.001);

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

function getTickAnalysis(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  r130StartCombinedAggTradeStream([full]);
  const engine = tickStore.get(full);
  if (!engine) return null;

  const totalVol = engine.totalBuy + engine.totalSell;
  const deltaRatio = totalVol > 0 ? engine.totalBuy/totalVol*100 : 50;

  const recentCandles = engine.candles.slice(-5);
  const deltaTrend = recentCandles.length >= 3
    ? recentCandles.map(c=>c.delta).reduce((a,b)=>a+b,0) > 0 ? 'BULL' : 'BEAR'
    : 'UNKNOWN';

  const imbalance = detectStackedImbalance(engine.footprint, engine.tickSize);

  const nowTick = Date.now();
  const recentRaw30s = (Array.isArray(engine.lastTicks)?engine.lastTicks:[]).filter(t=>nowTick-r125Num(t.ts,0)<30*1000);
  const recent30Buy = recentRaw30s.filter(t=>t.isBuy).reduce((s,t)=>s+r125Num(t.usdt),0);
  const recent30Sell = recentRaw30s.filter(t=>!t.isBuy).reduce((s,t)=>s+r125Num(t.usdt),0);
  const recent2m = engine.bigTrades.filter(t=>Date.now()-t.ts<2*60*1000);
  const bigBuy  = recent2m.filter(t=>t.side==='BUY').reduce((s,t)=>s+t.usdt,0);
  const bigSell = recent2m.filter(t=>t.side==='SELL').reduce((s,t)=>s+t.usdt,0);
  const whaleBias = bigBuy > bigSell * 1.5 ? 'WHALE_BUY' :
                    bigSell > bigBuy * 1.5 ? 'WHALE_SELL' : 'NEUTRAL';

  let deltaFlip = 'NONE';
  if (recentCandles.length >= 3) {
    const last = recentCandles[recentCandles.length-1];
    const prev = recentCandles[recentCandles.length-2];
    if (prev.delta > 0 && last.delta < -Math.abs(prev.delta)*0.5) deltaFlip='BULL_TO_BEAR';
    if (prev.delta < 0 && last.delta > Math.abs(prev.delta)*0.5)  deltaFlip='BEAR_TO_BULL';
  }

  const tickSweep = engine.sweepDet.confirmed;

  const rawTicksForVpin = Array.isArray(engine.lastTicks)
    ? engine.lastTicks
    : (Array.isArray(engine.sweepDet?.lastTicks) ? engine.sweepDet.lastTicks : []);
  const vpinResult = calcVPIN(rawTicksForVpin.length > 0
    ? rawTicksForVpin.slice(-500).map(t => ({...t, usdt:t.usdt||0}))
    : [], 30);

  const microstructure = calcDeltaMicrostructure(engine.candles);
  const aggressionTrend = r126AggressionTrendFromTicks(engine.lastTicks);
  const candleForecast = r126CandleForecastFromTicks(engine);
  const deltaImprint = r126DeltaImprintFromTickData({ recent30s:{ buy:recent30Buy, sell:recent30Sell, delta:recent30Buy-recent30Sell, total:recent30Buy+recent30Sell, trades:recentRaw30s.length } });

  return {
    deltaRatio: +deltaRatio.toFixed(1),
    deltaTrend,
    deltaFlip,
    imbalance,
    whaleBias,
    bigBuy:  +bigBuy.toFixed(0),
    bigSell: +bigSell.toFixed(0),
    tickSweep: tickSweep || null,
    currentCandle: engine.currentCandle ? {
      ts:engine.currentCandle.ts, open:engine.currentCandle.open, buy:+engine.currentCandle.buy.toFixed(0), sell:+engine.currentCandle.sell.toFixed(0),
      delta:+engine.currentCandle.delta.toFixed(0), trades:engine.currentCandle.trades, high:engine.currentCandle.high, low:engine.currentCandle.low, close:engine.currentCandle.close
    } : null,
    recent30s: {
      buy:+recent30Buy.toFixed(0), sell:+recent30Sell.toFixed(0), delta:+(recent30Buy-recent30Sell).toFixed(0),
      total:+(recent30Buy+recent30Sell).toFixed(0), trades:recentRaw30s.length
    },
    recentCandles: recentCandles.slice(-3).map(c=>({
      ts:c.ts, buy:+c.buy.toFixed(0), sell:+c.sell.toFixed(0),
      delta:+c.delta.toFixed(0), high:c.high, low:c.low, trades:c.trades
    })),
    candleCount: engine.candles.length,
    vpin: vpinResult,
    microstructure,
    aggressionTrend,
    candleForecast,
    deltaImprint,
  };
}

const icebergStore = new Map();

function startIcebergStream(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  const existing = icebergStore.get(full);
  const rs = existing?.ws?.readyState;
  if (existing?.ws && (rs === WebSocket.OPEN || rs === WebSocket.CONNECTING)) return;

  const store = existing || { bids: new Map(), asks: new Map(), hiddenBuy:0, hiddenSell:0,
    events:[], ws:null, lastUpdate: Date.now() };
  icebergStore.set(full, store);

  const wsUrl = `${FAPI_WS_PUBLIC}/ws/${full.toLowerCase()}@depth@100ms`;
  const ws = new WebSocket(wsUrl);
  store.ws = ws;

  ws.on('message', (data) => {
    try {
      const d = JSON.parse(data.toString());
      const now = Date.now();

      (d.b||[]).forEach(([price, qty]) => {
        const p = parseFloat(price), q = parseFloat(qty);
        const prev = store.bids.get(p) || 0;
        if (q === 0) {
          if (prev > 0) {
            const usdt = prev * p;
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

async function getCoinglass(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  try {
    const r = await fetch(
      `https://open-api.coinglass.com/public/v2/liquidation_map?symbol=${full.replace('USDT','')}&range=12`,
      { headers:{ 'accept':'application/json' }, signal: AbortSignal.timeout(3000) }
    );
    const d = await r.json();
    if (!d.data) return null;

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
    return null;
  }
}

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

const volatilityStore = { coins:[], lastUpdate:0 };

function trTime(ts = Date.now()) {
  try {
    return new Intl.DateTimeFormat('tr-TR', {
      timeZone: 'Europe/Istanbul',
      day: '2-digit', month: '2-digit', year: 'numeric',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
      hour12: false
    }).format(new Date(ts));
  } catch(_) {
    return new Date(ts + 3*60*60*1000).toISOString().replace('T',' ').substring(0,19) + ' TR';
  }
}

const TG_TOKEN   = process.env.TELEGRAM_BOT_TOKEN || '';
const TG_CHAT_ID = process.env.TELEGRAM_CHAT_ID   || '';
const ANTHROPIC_API_KEY = String(process.env.ANTHROPIC_API_KEY || '').trim();
const ANTHROPIC_MODEL   = String(process.env.ANTHROPIC_MODEL || process.env.AI_BRAIN_MODEL || 'claude-sonnet-4-6').trim();
const AI_MAX_TOKENS = /opus|fable|mythos/i.test(ANTHROPIC_MODEL) ? 2000 : 450;
const AI_BRAIN_ENABLED  = process.env.AI_BRAIN_ENABLED === '1' || process.env.AI_BRAIN_ENABLED === 'true';
const AI_BRAIN_SHADOW   = process.env.AI_BRAIN_SHADOW !== '0';
const AI_BRAIN_B_MODE   = process.env.AI_BRAIN_B_MODE === '1';
const AI_BRAIN_TOP_N    = Math.max(1, Math.min(2, parseInt(process.env.AI_BRAIN_TOP_N || '2', 10) || 2));
const AI_BRAIN_REVIEW_GAP_MS = Math.max(0, (parseInt(process.env.AI_BRAIN_REVIEW_GAP_SEC || '420', 10) || 420) * 1000);
const AI_BRAIN_MAX_DAILY_CALLS = Math.max(1, parseInt(process.env.AI_BRAIN_MAX_DAILY_CALLS || '150', 10) || 150);
let AI_SAVER_MODE = process.env.AI_SAVER_MODE === '1' || process.env.AI_SAVER_MODE === 'true';
const AI_BRAIN_CONF_FLOOR = 64;
const AI_BRAIN_MIN_CONF = AI_BRAIN_CONF_FLOOR;
const AI_BRAIN_MIN_RR = Math.max(0.8, Number(process.env.AI_BRAIN_MIN_RR || 1.5) || 1.5);
const AI_BRAIN_MAX_SL_PCT = Math.max(0.3, Number(process.env.AI_BRAIN_MAX_SL_PCT || 6.0) || 6.0);
const AI_BRAIN_STRICT_GATE = process.env.AI_BRAIN_STRICT_GATE !== '0';
let r308AiSpendDay = new Date().toISOString().slice(0,10);
let r308AiSpendCount = 0;
const r308AiLastBySymbol = new Map();
let r308LastAiDecision = null;
function r308Round(v, d=6) { const n=Number(v); return Number.isFinite(n) ? Number(n.toFixed(d)) : null; }
function r308AiDailyInfo(){
  const day = r308AiDayKey();
  if (r308AiSpendDay !== day) { r308AiSpendDay = day; r308AiSpendCount = 0; r308AiLastBySymbol.clear(); }
  return { day:r308AiSpendDay, count:r308AiSpendCount, limit:AI_BRAIN_MAX_DAILY_CALLS, remaining:Math.max(0, AI_BRAIN_MAX_DAILY_CALLS-r308AiSpendCount) };
}
function r308SetLastAiDecision(p={}) {
  try {
    const ai = p.ai || {}; const q = p.quality || {};
    const entry = Number(ai.entry ?? p.entry); const tp = Number(ai.tp ?? p.tp); const sl = Number(ai.sl ?? p.sl);
    const rr = Number.isFinite(Number(q.rr)) ? Number(q.rr) : ((Number.isFinite(entry)&&Number.isFinite(tp)&&Number.isFinite(sl)&&Math.abs(entry-sl)>0) ? Math.abs(tp-entry)/Math.abs(entry-sl) : null);
    const slPct = Number.isFinite(Number(q.slPct)) ? Number(q.slPct) : ((Number.isFinite(entry)&&Number.isFinite(sl)&&entry>0) ? Math.abs(entry-sl)/entry*100 : null);
    r308LastAiDecision = {
      ts: Date.now(),
      build: LAZARUS_BUILD,
      enabled: !!AI_BRAIN_ENABLED,
      shadow: !!AI_BRAIN_SHADOW,
      mode: AI_BRAIN_SHADOW ? 'GÖLGE' : 'CANLI',
      model: ANTHROPIC_MODEL,
      status: String(p.status || 'AI_İZLEME'),
      symbol: String(p.symbol || '').replace('USDT','').toUpperCase(),
      side: String(ai.side || p.side || 'WAIT').toUpperCase(),
      confidence: Number(ai.confidence || p.confidence || 0),
      karKosma: (String(ai.karKosma || '').toUpperCase() === 'RUNNER') ? 'RUNNER' : 'NORMAL',
      entry: r308Round(entry, 8), tp: r308Round(tp, 8), sl: r308Round(sl, 8),
      rr: rr == null ? null : r308Round(rr, 2),
      slPct: slPct == null ? null : r308Round(slPct, 2),
      reasoning: String(ai.reasoning || p.reasoning || p.reason || '').slice(0,260),
      rejectReason: String(p.rejectReason || q.reason || '').slice(0,180),
      candidate: p.candidate ? {
        symbol:String(p.candidate.symbol||'').replace('USDT','').toUpperCase(),
        rec:String(p.candidate.rec||''), recTR:String(p.candidate.recTR||''),
        tier:String(p.candidate.tier||''), tierTR:String(p.candidate.tierTR||''),
        score:Number(p.candidate.score||0), reason:String(p.candidate.reasonTR || p.candidate.reason || '').slice(0,260)
      } : null,
      order: p.order || null,
      daily: r308AiDailyInfo(),
      limits: { topN:AI_BRAIN_TOP_N, minConf:AI_BRAIN_MIN_CONF, minRR:AI_BRAIN_MIN_RR, maxSlPct:AI_BRAIN_MAX_SL_PCT, reviewGapSec:Math.round(AI_BRAIN_REVIEW_GAP_MS/1000), strictGate:AI_BRAIN_STRICT_GATE }
    };
  } catch(e) {
    r308LastAiDecision = { ts:Date.now(), build:LAZARUS_BUILD, status:'AI_KART_HATA', error:String(e?.message||e).slice(0,180), daily:r308AiDailyInfo() };
  }
}
function r308AiDashboardStatus(){
  return {
    enabled: !!AI_BRAIN_ENABLED, keySet: !!ANTHROPIC_API_KEY, shadow: !!AI_BRAIN_SHADOW, mode: AI_BRAIN_SHADOW ? 'GÖLGE' : 'CANLI',
    model: ANTHROPIC_MODEL, bMode: false, topN: AI_BRAIN_TOP_N, daily: r308AiDailyInfo(),
    limits: { minConf:AI_BRAIN_MIN_CONF, minRR:AI_BRAIN_MIN_RR, maxSlPct:AI_BRAIN_MAX_SL_PCT, reviewGapSec:Math.round(AI_BRAIN_REVIEW_GAP_MS/1000), strictGate:AI_BRAIN_STRICT_GATE },
    last: r308LastAiDecision
  };
}
let   tgLastSent = 0;
let   tgQueue = Promise.resolve();
const TG_MIN_GAP = 1200;
const r185OrderOpenSent = new Set();

function tgEsc(v) {
  return String(v ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}
function tgNum(v, d=2) {
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(d) : '-';
}
function tgEnabled() { return !!(TG_TOKEN && TG_CHAT_ID); }

async function r184DirectTelegramText(text, silent=false) {
  if (!tgEnabled()) return {ok:false, skipped:true, reason:'telegram_env_missing', error:'telegram_env_missing'};
  try {
    const res = await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({
        chat_id: TG_CHAT_ID,
        text: String(text).slice(0,3900),
        disable_notification: !!silent,
      }),
      signal: AbortSignal.timeout(10000),
    });
    const data = await res.json().catch(()=>({}));
    if (!res.ok || data.ok === false) {
      const err = data.description || `HTTP_${res.status}`;
      try { console.log(`[telegram sessiz başarısız] ${err}`); } catch(_) {}
      return {ok:false, error:err, status:res.status, data};
    }
    return {ok:true, messageId:data?.result?.message_id || null};
  } catch(e) {
    const err = String(e?.message || e);
    try { console.log(`[telegram sessiz başarısız] ${err}`); } catch(_) {}
    return {ok:false, error:err};
  }
}

async function tgSendNow(text, silent=false) {
  if (!tgEnabled()) return {ok:false, skipped:true, reason:'telegram_env_missing'};
  const wait = Math.max(0, TG_MIN_GAP - (Date.now() - tgLastSent));
  if (wait > 0) await new Promise(r => setTimeout(r, wait));
  tgLastSent = Date.now();
  try {
    const res = await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        chat_id: TG_CHAT_ID,
        text: String(text).slice(0, 3900),
        parse_mode: 'HTML',
        disable_notification: !!silent,
      }),
      signal: AbortSignal.timeout(8000),
    });
    const data = await res.json().catch(()=>({}));
    if (!res.ok || data.ok === false) {
      const err = data.description || `HTTP_${res.status}`;
      try {
        const plain = String(text)
          .replace(/<[^>]+>/g, '')
          .replace(/&amp;/g, '&')
          .replace(/&lt;/g, '<')
          .replace(/&gt;/g, '>');
        const r2 = await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify({chat_id:TG_CHAT_ID, text:plain.slice(0,3900), disable_notification:!!silent}),
          signal: AbortSignal.timeout(8000),
        });
        const d2 = await r2.json().catch(()=>({}));
        if (r2.ok && d2.ok !== false) return {ok:true, fallback:'plain_text', originalError:err};
      } catch(_fallbackErr) {}
      try { console.log('[telegram sessiz başarısız] TELEGRAM_SEND_FAIL:', err); } catch(_) {}
      console.log('⚠️ Telegram gönderim hatası:', err);
      return {ok:false, error:err};
    }
    return {ok:true};
  } catch(e) {
    const err = String(e?.message || e).slice(0,160);
    try { console.log('[telegram sessiz başarısız] TELEGRAM_SEND_FAIL:', err); } catch(_) {}
    console.log('⚠️ Telegram gönderim exception:', err);
    return {ok:false, error:err};
  }
}

function tgSend(text, silent=false) {
  tgQueue = tgQueue.then(() => tgSendNow(text, silent)).catch(() => {});
  return tgQueue;
}

function tgFmtUsd(v, d=2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return '-';
  return `${n >= 0 ? '+' : ''}${n.toFixed(d)}$`;
}
function tgFmtPct(v, d=2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return '-';
  return `${n >= 0 ? '+' : ''}${n.toFixed(d)}%`;
}
function tgPrice(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return '-';
  if (n < 0.001) return n.toFixed(8);
  if (n < 0.1) return n.toFixed(6);
  if (n < 10) return n.toFixed(5);
  return n.toFixed(4);
}
function tgMovePct(entry, price, side='LONG') {
  const e = Number(entry), p = Number(price);
  if (!(e > 0 && p > 0)) return null;
  const raw = (p - e) / e * 100 * (normalizeSide(side) === 'SHORT' ? -1 : 1);
  return raw;
}
function tgRoiFromPrice(entry, price, side, lev) {
  const m = tgMovePct(entry, price, side);
  const l = Number(lev || 1) || 1;
  return Number.isFinite(m) ? m * l : null;
}
function tgTradeOpen(symbol, side, score, edge, margin, leverage, reason, extra={}) {
  const icon = side === 'LONG' ? '🟢' : '🔴';
  const dirTR = side === 'LONG' ? 'LONG ↑' : 'SHORT ↓';
  const lev = Number(leverage || extra.leverage || 1) || 1;
  const mar = Number(margin || extra.margin || 0) || 0;
  const qty = Number(extra.quantity || extra.qty || 0);
  const pos = mar * lev;
  const entry = Number(extra.entryPrice || extra.entry || 0);
  const sl = Number(extra.slPrice || extra.stopPrice || extra.currentSL || 0);
  const tp = Number(extra.tpPrice || extra.targetPrice || extra.targetTP || 0);
  const slMove = tgMovePct(entry, sl, side);
  const tpMove = tgMovePct(entry, tp, side);
  const slRoi = tgRoiFromPrice(entry, sl, side, lev);
  const tpRoi = tgRoiFromPrice(entry, tp, side, lev);
  const rr = (Number.isFinite(slMove) && Number.isFinite(tpMove) && Math.abs(slMove) > 0)
    ? Math.abs(tpMove / slMove) : null;
  const lines = [
    `${icon} <b>İŞLEM AÇILDI</b> — <b>${tgEsc(symbol)}</b> ${dirTR}`,
    `━━━━━━━━━━━━━━━━`,
    `💰 Marjin: <b>${tgNum(mar,2)} USDT</b> | Pozisyon: <b>${tgNum(pos,2)} USDT</b> | Lev: <b>${tgEsc(lev)}x</b>`,
  ];
  if (qty) lines.push(`📦 Miktar: <b>${tgNum(qty,6)}</b>`);
  if (entry) lines.push(`🎯 Entry: <b>$${tgPrice(entry)}</b>`);
  if (sl || tp) lines.push(`🛡 SL: <b>$${tgPrice(sl)}</b> | 🎯 TP: <b>$${tgPrice(tp)}</b>`);
  if (Number.isFinite(slRoi) || Number.isFinite(tpRoi)) {
    lines.push(`📐 Risk/Ödül: SL <b>${tgFmtPct(slRoi,1)} ROI</b> | TP <b>${tgFmtPct(tpRoi,1)} ROI</b>${Number.isFinite(rr) ? ` | RR <b>${tgNum(rr,2)}</b>` : ''}`);
  } else if (extra.slPct || extra.tpPct) {
    lines.push(`📐 SL: <b>%${tgEsc(extra.slPct)}</b> | TP: <b>%${tgEsc(extra.tpPct)}</b> (kaldıraçsız)`);
  }
  lines.push(`📊 Skor: <b>${tgEsc(score)}</b> | Edge: <b>${tgEsc(edge)}/100</b>${extra.tier ? ` | Tier: <b>${tgEsc(extra.tier)}</b>` : ''}`);
  if (extra.mode || extra.brainMode) lines.push(`🧠 Mod: <b>${tgEsc(extra.mode || extra.brainMode)}</b>`);
  if (reason) lines.push(`📝 Sebep: ${tgEsc(String(reason).slice(0,220))}`);
  if (extra.sltpVerified !== undefined) lines.push(`✅ SL/TP Binance: <b>${extra.sltpVerified ? 'doğrulandı' : 'kontrol et'}</b>`);
  lines.push(`━━━━━━━━━━━━━━━━`);
  lines.push(...tgPerfSummaryLines());
  lines.push(`🏷 Build: ${tgEsc(LAZARUS_BUILD)}`);
  lines.push(`⏰ ${new Date().toLocaleString('tr-TR')}`);
  return tgSend(lines.join('\n'), false);
}

function tgTradeClose(symbol, side, pnl, roi, closeReason, extra={}) {
  const pn = Number(pnl);
  const ro = Number(roi);
  const win = Number.isFinite(pn) ? pn >= 0 : (Number.isFinite(ro) ? ro >= 0 : false);
  const icon = win ? '✅' : '❌';
  const emojiPnl = win ? '🤑' : '😬';
  const dirTR = side === 'LONG' ? 'LONG ↑' : (side === 'SHORT' ? 'SHORT ↓' : tgEsc(side||'?'));
  const entry = Number(extra.entryPrice || extra.entry || 0);
  const close = Number(extra.closePrice || extra.exitPrice || 0);
  const sl = Number(extra.sl || extra.slPrice || extra.currentSL || 0);
  const tp = Number(extra.tp || extra.tpPrice || extra.targetTP || 0);
  const qty = Number(extra.quantity || extra.qty || 0);
  const lev = Number(extra.leverage || 0);
  const margin = Number(extra.marginUSDT || extra.margin || 0);
  const rawMove = tgMovePct(entry, close, side);
  const lines = [
    `${icon} <b>İŞLEM KAPANDI</b> — <b>${tgEsc(symbol)}</b> ${dirTR}`,
    `━━━━━━━━━━━━━━━━`,
    `${emojiPnl} PnL: <b>${Number.isFinite(pn) ? tgFmtUsd(pn,2) : 'bilinmiyor'}</b> | ROI: <b>${Number.isFinite(ro) ? tgFmtPct(ro,1) : 'bilinmiyor'}</b>`,
  ];
  if (entry || close) lines.push(`💹 Entry: <b>$${tgPrice(entry)}</b> → Çıkış: <b>$${tgPrice(close)}</b>${Number.isFinite(rawMove) ? ` | Coin hareketi: <b>${tgFmtPct(rawMove,2)}</b>` : ''}`);
  if (sl || tp) lines.push(`🛡 Açılış SL: <b>$${tgPrice(sl)}</b> | TP: <b>$${tgPrice(tp)}</b>`);
  if (lev || margin || qty) lines.push(`💼 Marjin: <b>${margin ? tgNum(margin,2)+' USDT' : '-'}</b> | Lev: <b>${lev || '-'}x</b> | Miktar: <b>${qty ? tgNum(qty,6) : '-'}</b>`);
  if (extra.duration) lines.push(`⏱ Süre: <b>${tgEsc(extra.duration)}</b>`);
  if (extra.peakRoi !== undefined || extra.peakPnl !== undefined) lines.push(`📈 Zirve ROI: <b>${extra.peakRoi!==undefined ? tgFmtPct(extra.peakRoi,1) : '-'}</b> | Zirve PnL: <b>${extra.peakPnl!==undefined ? tgFmtUsd(extra.peakPnl,2) : '-'}</b>`);
  if (closeReason) lines.push(`📋 Kapanış: ${tgEsc(String(closeReason).slice(0,160))}`);
  if (extra.resultNote) lines.push(`🧾 Sonuç: ${tgEsc(String(extra.resultNote).slice(0,180))}`);
  lines.push(`━━━━━━━━━━━━━━━━`);
  lines.push(...tgPerfSummaryLines());
  lines.push(`🏷 Build: ${tgEsc(LAZARUS_BUILD)}`);
  lines.push(`⏰ ${new Date().toLocaleString('tr-TR')}`);
  return tgSend(lines.join('\n'), win);
}

function tgAlert(text) {
  return tgSend(`⚠️ <b>Lazarus UYARI</b>\n${tgEsc(text)}`, false);
}

async function r181TradeOpenCard(row={}, state={}) {
  try {
    const id = row.id || `${row.symbol}_${row.openedAt||Date.now()}`;
    if (tgOpenNotifiedIds.has(id)) return {ok:true, skipped:true, duplicate:true};
    tgOpenNotifiedIds.add(id);
    const sym = row.symbol || state.symbol || '?';
    const side = row.side || state.side || '?';
    const lev = row.leverage || state.leverage || autoConfig?.leverage || '-';
    const margin = row.marginUSDT || state.usdtAmount || state.marginUSDT || autoConfig?.usdtAmount || '-';
    const qty = row.quantity || state.quantity || state.positionAmt || '-';
    const entry = row.entryPrice || state.entryPrice || '-';
    const sl = row.sl || state.currentSL || state.slPrice || '-';
    const tp = row.tp || state.targetTP || state.tpPrice || '-';
    const reasonObj = state.entryReason || state.openReason || {};
    const reason = typeof reasonObj === 'string' ? reasonObj : (reasonObj.reason || row.entryReason || `${side} açıldı`);
    const msg = [
      `🟢 İŞLEM AÇILDI`,
      `Hey Ben Kripto Goxel Bot İşleme Girdi`,
      `${sym} ${side}`,
      `Marjin: ${margin} USDT | Lev: ${lev}x`,
      `Miktar: ${qty}`,
      `Entry: ${entry}`,
      `SL: ${sl} | TP: ${tp}`,
      `Skor: ${row.score ?? state.score ?? '-'} | Tier: ${row.tier ?? state.tier ?? '-'}`,
      `Sebep: ${String(reason).slice(0,500)}`,
      `Build: ${LAZARUS_BUILD}`,
      `${new Date().toLocaleString('tr-TR')}`
    ].join('\n');
    return await r184DirectTelegramText(msg, false);
  } catch(e) {
    try { console.log('[telegram sessiz başarısız] R181_TG_OPEN_DIRECT_FAIL:', String(e?.message||e)); } catch(_) {}
    return {ok:false,error:String(e?.message||e)};
  }
}

async function r181TradeCloseCard(row={}, state={}, cls={}) {
  try {
    const id = row.id || `${row.symbol}_${row.openedAt||''}_${row.closedAt||Date.now()}`;
    if (tgCloseNotifiedIds.has(id)) return {ok:true, skipped:true, duplicate:true};
    tgCloseNotifiedIds.add(id);
    const pnl = Number(row.pnlUSDT);
    const roi = Number(row.roiPct);
    const win = Number.isFinite(pnl) ? pnl >= 0 : false;
    const goxelPhrase = win
      ? 'Kripto Goxel Tekkeyi Bekleyen Çorbayı İçer'
      : 'Kripto Goxel Ben siz girdiğiniz diye girdim :)';
    const msg = [
      `${win ? '✅' : '❌'} İŞLEM KAPANDI`,
      goxelPhrase,
      `${row.symbol || '?'} ${row.side || '?'}`,
      `PnL: ${Number.isFinite(pnl) ? (pnl>=0?'+':'')+pnl.toFixed(2)+' USDT' : 'bilinmiyor'} | ROI: ${Number.isFinite(roi) ? (roi>=0?'+':'')+roi.toFixed(1)+'%' : 'bilinmiyor'}`,
      `Entry: ${row.entryPrice ?? '-'} → Çıkış: ${row.closePrice ?? cls.closePrice ?? '-'}`,
      `SL: ${row.sl ?? cls.sl ?? state.currentSL ?? '-'} | TP: ${row.tp ?? cls.tp ?? state.targetTP ?? '-'}`,
      `Kapanış: ${cls.label || row.exitLabel || row.exitReason || 'Kapanış algılandı'}`,
      `Build: ${LAZARUS_BUILD}`,
      `${new Date().toLocaleString('tr-TR')}`
    ].join('\n');
    return await r184DirectTelegramText(msg, !win);
  } catch(e) {
    try { console.log('[telegram sessiz başarısız] R181_TG_CLOSE_DIRECT_FAIL:', String(e?.message||e)); } catch(_) {}
    return {ok:false,error:String(e?.message||e)};
  }
}

function tgPerfSummaryLines() {
  try {
    const obj = r170PerfMode();
    const a = obj.account || {};
    const fmt = (p={}) => {
      const closed = Number(p.closed || 0);
      const wr = p.wr !== null && p.wr !== undefined ? `%${(p.wr*100).toFixed(0)}` : '?';
      const net = Number(p.net || 0);
      const pf = Number(p.pf || 0);
      return `${wr} | Net ${net>=0?'+':''}${net.toFixed(2)}$ | PF ${pf.toFixed(2)} | ${closed} işlem`;
    };
    return [
      `🎯 <b>HESAP WR HEDEFİ</b>: %60–85 | Mod: <b>${tgEsc(obj.mode || '-')}</b>`,
      `📅 Günlük: <b>${tgEsc(fmt(a.day))}</b>`,
      `🗓 Haftalık: <b>${tgEsc(fmt(a.week))}</b>`,
      `📆 Aylık: <b>${tgEsc(fmt(a.month))}</b>`,
      `🧪 Son 12: <b>${tgEsc(fmt(a.recent))}</b> | Saatlik frekans: <b>${tgEsc(r170TradeFreq(60*60*1000))}</b>`
    ];
  } catch(e) {
    return [`🎯 <b>HESAP WR HEDEFİ</b>: veri hazırlanıyor`];
  }
}

const tgOpenNotifiedIds = new Set();
const tgCloseNotifiedIds = new Set();

function tgNotifyTradeOpenOnce(row={}, state={}) {
  try {
    const id = row.id || `${row.symbol}_${row.openedAt||Date.now()}`;
    if (tgOpenNotifiedIds.has(id)) return;
    tgOpenNotifiedIds.add(id);
    const openReasonObj = state?.entryReason || state?.openReason || {};
    const reasonText = typeof openReasonObj === 'string'
      ? openReasonObj
      : (openReasonObj.reason || row.entryReason || `${row.side} açıldı`);
    tgTradeOpen(row.symbol || state.symbol || '?', row.side || state.side || '?',
      row.score ?? openReasonObj.score ?? state.score ?? '-',
      state.brainConfidence ?? state.edge ?? openReasonObj.edge ?? '-',
      row.marginUSDT ?? state.usdtAmount ?? state.marginUSDT ?? autoConfig?.usdtAmount,
      row.leverage ?? state.leverage ?? autoConfig?.leverage,
      reasonText,
      {
        entryPrice: row.entryPrice ?? state.entryPrice,
        slPrice: row.sl ?? state.currentSL ?? state.slPrice,
        tpPrice: row.tp ?? state.targetTP ?? state.tpPrice,
        slPct: row.slPct ?? state.slPct,
        tpPct: row.tpPct ?? state.tpPct,
        quantity: row.quantity ?? state.quantity ?? state.positionAmt,
        leverage: row.leverage ?? state.leverage,
        margin: row.marginUSDT ?? state.usdtAmount ?? state.marginUSDT,
        tier: row.tier ?? state.tier ?? openReasonObj.tier,
        mode: state.brainMode ?? state.primaryMode ?? openReasonObj.mode ?? '',
        sltpVerified: state.sltpVerified ?? undefined,
      });
  } catch(e) {
    try { console.log('[telegram sessiz başarısız] TELEGRAM_OPEN_NOTIFY_FAIL:', String(e?.message||e)); } catch(_) {}
  }
}

function tgNotifyTradeCloseOnce(row={}, state={}, cls={}) {
  try {
    const id = row.id || `${row.symbol}_${row.openedAt||''}_${row.closedAt||Date.now()}`;
    if (tgCloseNotifiedIds.has(id)) return;
    tgCloseNotifiedIds.add(id);
    const pnl = Number(row.pnlUSDT);
    const roi = Number(row.roiPct);
    const dur = row.openedAt
      ? (() => { const m = Math.max(0, Math.round((Number(row.closedAt||Date.now())-Number(row.openedAt))/60000)); return m < 60 ? `${m}dk` : `${Math.floor(m/60)}s ${m%60}dk`; })()
      : '';
    tgTradeClose(row.symbol, row.side,
      Number.isFinite(pnl) ? pnl : null,
      Number.isFinite(roi) ? roi : null,
      cls.label || row.exitLabel || 'Kapanış algılandı',
      {
        entryPrice: row.entryPrice ?? state.entryPrice,
        closePrice: cls.closePrice ?? row.closePrice,
        duration: dur,
        sl: row.sl ?? cls.sl ?? state.currentSL,
        tp: row.tp ?? cls.tp ?? state.targetTP,
        leverage: row.leverage ?? state.leverage,
        quantity: row.quantity ?? state.quantity ?? state.positionAmt,
        marginUSDT: row.marginUSDT ?? state.usdtAmount ?? state.marginUSDT,
        peakRoi: state.peakRealPct,
        peakPnl: state.peakPnl,
        resultNote: row.resultNote,
      });
  } catch(e) {
    try { console.log('[telegram sessiz başarısız] TELEGRAM_CLOSE_NOTIFY_FAIL:', String(e?.message||e)); } catch(_) {}
  }
}

const tradeLedgerPath = './trade_ledger_live.json';
let tradeLedger = [];
try {
  tradeLedger = JSON.parse(fs.readFileSync(tradeLedgerPath, 'utf8') || '[]');
  if (!Array.isArray(tradeLedger)) tradeLedger = [];
} catch(_) { tradeLedger = []; }
function saveTradeLedger() {
  try { fs.writeFileSync(tradeLedgerPath, JSON.stringify(tradeLedger.slice(0,250), null, 2)); } catch(_) {}
}

function r179LedgerKey(row={}) {
  const sym = normalizeSymbol(String(row.symbol||'')).replace('USDT','');
  const t = Math.round(Number(row.closedAt || row.openedAt || 0) / (2*60*1000));
  const pnl = Number(row.pnlUSDT||0).toFixed(2);
  return `${sym}_${t}_${pnl}`;
}
function r176DedupeLedger(limit=250) {
  const seen = new Set();
  const out = [];
  for (const r of (Array.isArray(tradeLedger) ? tradeLedger : [])) {
    if (!r || !(r.openedAt || r.closedAt)) continue;
    const key = r.id && !String(r.id).startsWith('RESTORE') && !String(r.id).startsWith('binance_hist_')
      ? String(r.id)
      : r179LedgerKey(r);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(r);
  }
  tradeLedger = out.sort((a,b)=>Number(b.closedAt||b.openedAt||0)-Number(a.closedAt||a.openedAt||0)).slice(0, limit);
  return tradeLedger.length;
}
function r179Stat(rows=[]) {
  const arr = (Array.isArray(rows)?rows:[]).filter(t => t && (t.status==='CLOSED'||t.closedAt) && Number.isFinite(Number(t.pnlUSDT)));
  const wins = arr.filter(t=>Number(t.pnlUSDT)>0).length;
  const losses = arr.filter(t=>Number(t.pnlUSDT)<0).length;
  const closed = wins + losses;
  const profit = arr.filter(t=>Number(t.pnlUSDT)>0).reduce((s,t)=>s+Number(t.pnlUSDT||0),0);
  const lossAbs = Math.abs(arr.filter(t=>Number(t.pnlUSDT)<0).reduce((s,t)=>s+Number(t.pnlUSDT||0),0));
  const net = arr.reduce((s,t)=>s+Number(t.pnlUSDT||0),0);
  return {closed,wins,losses,wr:closed?wins/closed:null,net:safeNum(net,2),profit:safeNum(profit,2),loss:safeNum(lossAbs,2),pf:lossAbs>0?safeNum(profit/lossAbs,2):(profit>0?99:0)};
}
function r179AccountPerf() {
  const now = Date.now();
  const day = now - 24*60*60*1000;
  const week = now - 7*24*60*60*1000;
  const month = now - 30*24*60*60*1000;
  const rows = Array.isArray(tradeLedger) ? tradeLedger : [];
  const byTime = rows.filter(t=>t && (t.closedAt||t.openedAt)).sort((a,b)=>Number(b.closedAt||b.openedAt||0)-Number(a.closedAt||a.openedAt||0));
  return {
    day: r179Stat(byTime.filter(t=>Number(t.closedAt||t.openedAt||0)>=day)),
    week: r179Stat(byTime.filter(t=>Number(t.closedAt||t.openedAt||0)>=week)),
    month: r179Stat(byTime.filter(t=>Number(t.closedAt||t.openedAt||0)>=month)),
    recent: r179Stat(byTime.slice(0,12)),
  };
}
function r170TradeFreq(windowMs=60*60*1000) {
  const since = Date.now() - Number(windowMs||60*60*1000);
  return (Array.isArray(tradeLedger)?tradeLedger:[]).filter(t=>Number(t.openedAt||t.closedAt||0)>=since).length;
}
function r170PerfMode() {
  const account = r179AccountPerf();
  const d = account.day || {};
  let mode = 'BALANCED_FREQ';
  let reason = 'R165_BASE_FREQ';
  if (Number(d.closed||0) >= 20 && d.wr !== null && d.wr >= 0.60 && d.wr <= 0.85 && Number(d.net||0) >= 0 && Number(d.pf||0) >= 1.2) {
    mode = 'TARGET_OK'; reason = 'DAILY_TARGET_OK';
  } else if (Number(d.closed||0) >= 20 && (Number(d.wr||0) < 0.45 || Number(d.pf||0) < 0.75 || Number(d.net||0) < -30)) {
    mode = 'ACCOUNT_DRAWDOWN_INFO'; reason = 'INFO_ONLY_NOT_ENTRY_GATE';
  }
  return {mode, reason, perf:d, account};
}

async function r179BootstrapLedger48h(apiKey, apiSecret, lookbackMs=48*60*60*1000, opts={}) {
  if (!apiKey || !apiSecret) return {ok:false, reason:'api_missing'};
  try {
    const startTime = Date.now() - Math.max(60*60*1000, Number(lookbackMs)||48*60*60*1000);
    const endTime = Date.now() + 5000;
    const inc = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/income', {
      incomeType:'REALIZED_PNL', startTime, endTime, limit:1000
    });
    if (!Array.isArray(inc) || !inc.length) return {ok:true, restored:0, groups:0, source:'income_empty'};
    const groups = new Map();
    for (const x of inc) {
      const pnl = Number(x.income || 0);
      const t = Number(x.time || x.timestamp || 0);
      const symFull = normalizeSymbol(String(x.symbol || ''));
      if (!symFull || !t || !Number.isFinite(pnl) || Math.abs(pnl) < 0.000001) continue;
      const b = Math.round(t / (2*60*1000));
      const key = `${symFull}_${b}`;
      const g = groups.get(key) || {symbol:symFull.replace('USDT',''), pnlUSDT:0, first:t, last:t, count:0};
      g.pnlUSDT += pnl; g.first = Math.min(g.first,t); g.last = Math.max(g.last,t); g.count++;
      groups.set(key,g);
    }
    let base = opts.replace ? [] : (Array.isArray(tradeLedger)?tradeLedger.filter(r=>!String(r.id||'').startsWith('RESTORE48_')):[]);
    const existing = new Set(base.map(r=>r179LedgerKey(r)));
    let restored=0;
    for (const g of groups.values()) {
      if (!Number.isFinite(g.pnlUSDT) || Math.abs(g.pnlUSDT)<0.000001) continue;
      const realNearby = base.find(r => {
        try {
          const rs = normalizeSymbol(String(r.symbol||'')).replace('USDT','');
          const gs = String(g.symbol||'').replace('USDT','');
          const rt = Number(r.closedAt || r.openedAt || 0);
          const gt = Number(g.last || g.first || 0);
          return rs === gs && String(r.side||'UNKNOWN').toUpperCase() !== 'UNKNOWN' && rt > 0 && gt > 0 && Math.abs(rt-gt) <= 10*60*1000;
        } catch(_) { return false; }
      });
      if (realNearby) {
        if (Math.abs(Number(realNearby.pnlUSDT||0)) < 0.05) {
          realNearby.pnlUSDT = safeNum(g.pnlUSDT,4);
          const m = Number(realNearby.marginUSDT || autoConfig?.usdtAmount || 0);
          realNearby.roiPct = safeNum((m>0 ? g.pnlUSDT/m*100 : Number(realNearby.roiPct||0)),2);
          realNearby.exitLabel = realNearby.exitLabel || 'Binance realized PNL ile düzeltildi';
          realNearby.resultNote = `R191: 48s Binance REALIZED_PNL yakın gerçek trade satırına işlendi (${g.count} income)`;
          restored++;
        }
        continue;
      }
      const row = {
        id:`RESTORE48_${g.symbol}_${g.last}_${g.count}_${Math.round(g.pnlUSDT*10000)}`,
        symbol:g.symbol, side:'UNKNOWN', status:'CLOSED',
        openedAt:Math.max(0, Number(g.first||g.last)-60*1000),
        closedAt:Number(g.last||g.first),
        entryPrice:null, closePrice:null, quantity:null,
        leverage:Number(autoConfig?.leverage||1), marginUSDT:Number(autoConfig?.usdtAmount||0),
        pnlUSDT:safeNum(g.pnlUSDT,4),
        roiPct:safeNum((Number(autoConfig?.usdtAmount||0)>0 ? g.pnlUSDT/Number(autoConfig.usdtAmount)*100 : 0),2),
        exitLabel:'Binance 48s gelir bootstrap',
        exitReason:'R179_BINANCE_48H_BOOTSTRAP',
        resultNote:`R179: son 48 saat Binance REALIZED_PNL gruplandı (${g.count} income)`
      };
      const k = r179LedgerKey(row);
      if (existing.has(k)) continue;
      existing.add(k); base.push(row); restored++;
    }
    tradeLedger = base.sort((a,b)=>Number(b.closedAt||b.openedAt||0)-Number(a.closedAt||a.openedAt||0)).slice(0,250);
    r176DedupeLedger(250); saveTradeLedger();
    try { logAuto(`🧾 R179 48s bootstrap: ${restored}/${groups.size} grup eklendi, ledger:${tradeLedger.length}`); } catch(_) {}
    return {ok:true, restored, groups:groups.size, incomeRows:inc.length, after:tradeLedger.length, source:'binance_income_48h_grouped'};
  } catch(e) {
    try { pushCritical('R179_48H_BOOTSTRAP_FAIL', new Error(String(e?.message||e)), {symbol:'PNL'}, 'WARN'); } catch(_) {}
    return {ok:false, error:String(e?.message||e)};
  }
}
let r173LastAutoReconcileAt = 0;
let r173LastAutoReconcileResult = null;
async function r173AutoReconcileTick(force=false) {
  if (!autoConfig?.apiKey || !autoConfig?.apiSecret) return {ok:false, reason:'api_missing'};
  if (!force && Date.now() - Number(r173LastAutoReconcileAt||0) < 15*60*1000) return {ok:true, skipped:true, throttle:'15m', last:r173LastAutoReconcileResult};
  r173LastAutoReconcileAt = Date.now();
  const boot = await r179BootstrapLedger48h(autoConfig.apiKey, autoConfig.apiSecret, 48*60*60*1000, {replace:false});
  r173LastAutoReconcileResult = {ok:!!boot?.ok, bootstrap:boot};
  return r173LastAutoReconcileResult;
}

async function r171MaintenanceTick() {
  try { await r173AutoReconcileTick(false); } catch(e) {
    try { pushCritical('R180_MAINT_RECONCILE_FAIL', new Error(String(e?.message||e)), {symbol:'PNL'}, 'WARN'); } catch(_) {}
  }
  try {
    if (typeof r171TelegramPollLedger === 'function') await r171TelegramPollLedger(false);
  } catch(e) {
    try { console.log('[telegram sessiz başarısız] R180_MAINT_TELEGRAM_POLL_FAIL:', String(e?.message||e)); } catch(_) {}
  }
}

const lastKnownPositionsPath = './last_known_positions.json';
let lastKnownPositions = {};
try {
  const _lkp = JSON.parse(fs.readFileSync(lastKnownPositionsPath, 'utf8') || '{}');
  if (_lkp && typeof _lkp === 'object' && !Array.isArray(_lkp)) lastKnownPositions = _lkp;
} catch(_) { lastKnownPositions = {}; }
function saveLastKnownPositions() {
  try { fs.writeFileSync(lastKnownPositionsPath, JSON.stringify(lastKnownPositions, null, 2)); } catch(_) {}
}
function rememberOpenPositionForReentry(p, state={}) {
  if (!p || !p.symbol) return;
  const sym = normalizeSymbol(p.symbol);
  const amt = parseFloat(p.positionAmt || 0);
  if (!amt) return;
  const side = normalizeSide(state.side) || (amt > 0 ? 'LONG' : 'SHORT');
  const old = lastKnownPositions[sym] || {};
  lastKnownPositions[sym] = {
    symbol:sym, side, positionAmt:Math.abs(amt),
    entryPrice:safeNum(p.entryPrice || state.entryPrice || old.entryPrice, 10),
    leverage:Number(p.leverage || state.leverage || old.leverage || autoConfig?.leverage || 1),
    openedAt:Number(state.openedAt || old.openedAt || Date.now()),
    currentSL:safeNum(state.currentSL || state.slPrice || old.currentSL || old.slPrice, 10),
    targetTP:safeNum(state.targetTP || state.tpPrice || old.targetTP || old.tpPrice, 10),
    sltpVerified:!!(state.sltpVerified || old.sltpVerified),
    usdtAmount:safeNum(state.usdtAmount || autoConfig?.usdtAmount || old.usdtAmount, 2),
    slPct:safeNum(state.slPct || old.slPct, 4),
    tpPct:safeNum(state.tpPct || old.tpPct, 4),
    aiRunner:!!(state.aiRunner || old.aiRunner),
    brainMode: state.brainMode || old.brainMode || null,
    entryReason:state.openReason || old.entryReason || null, lastSeen:Date.now()
  };
}
function forgetKnownPosition(sym) { delete lastKnownPositions[normalizeSymbol(sym)]; }
function safeNum(v, dec=6) { const n=Number(v); return Number.isFinite(n)?+n.toFixed(dec):null; }
function makeTradeId(sym, ts) { return sym.replace('USDT','') + '_' + ts; }
function buildResultNote(cls={}, state={}) {
  const pnl = Number(cls.realizedPnl);
  if (Number.isFinite(pnl) && pnl < 0) {
    const er = JSON.stringify(state?.openReason||'').toLowerCase();
    if (er.includes('fitil')||er.includes('premium')||er.includes('vah'))
      return 'Zarar: fitil/likidite bölgesi ters piyasa yapıcı izi; 60dk aynı yön bekleme, temiz ters yön serbest.';
    return 'Zarar: zarar kes/ters akış; 60dk aynı yön bekleme, temiz ters yön serbest.';
  }
  if (Number.isFinite(pnl) && pnl > 0) return 'Kâr: plan çalıştı.';
  if (cls.code==='EXTERNAL_OR_MANUAL') return 'Kullanıcı/Binance kapanışı: 15dk aynı yön bekleme, temiz ters yön serbest.';
  return cls.label || 'Binance senkronu.';
}
function recordTradeOpen(symbol, side, entryPrice, qty, state={}) {
  const sym = normalizeSymbol(symbol);
  const openedAt = Number(state?.openedAt||Date.now());
  const id = makeTradeId(sym, openedAt);
  const row = {
    id, symbol:sym.replace('USDT',''), side:normalizeSide(side), status:'OPEN',
    openedAt, openedAtTR:trTime(openedAt), entryPrice:safeNum(entryPrice,10),
    leverage:Number(state?.leverage||autoConfig?.leverage||0)||null,
    marginUSDT:safeNum(autoConfig?.usdtAmount,2),
    entryReason:state?.openReason||`${normalizeSide(side)} açıldı`,
    aiSnapshot: state?.aiSnapshot || null,
    score:state?.score||null, tier:state?.tier||null,
    exitReason:null,resultNote:null,pnlUSDT:null,roiPct:null,
    closedAt:null,closedAtTR:null,cooldownMin:null,
  };
  tradeLedger = [row,...tradeLedger.filter(x=>x.id!==id)].slice(0,250);
  saveTradeLedger();
  try { r181TradeOpenCard(row, state).catch(e=>{ try { console.log('[telegram sessiz başarısız] R186_TG_OPEN_PROMISE_FAIL:', String(e?.message||e)); } catch(_) {} }); } catch(_) {}
  return row;
}
let r345SonIslemler = [];
function recordTradeClose(symbol, state={}, cls={}) {
  const sym = normalizeSymbol(symbol);
  const openedAt = Number(state?.openedAt||0);
  const id = openedAt ? makeTradeId(sym,openedAt) : null;
  let row = tradeLedger.find(x=>(id&&x.id===id)||(x.symbol===sym.replace('USDT','')&&x.status==='OPEN'));
  if (!row) row = recordTradeOpen(sym, state?.side||'UNKNOWN', state?.entryPrice||null, null, state);
  const closedAt = Date.now();
  const entry = Number(row.entryPrice||state?.entryPrice||0);
  const close = Number(cls.closePrice||0);
  const rawMove = entry>0&&close>0?((close-entry)/entry*100*(normalizeSide(row.side)==='SHORT'?-1:1)):null;
  const lev = Number(row.leverage||1)||1;
  const cdMs = Number(cls.cooldownMs||0);
  Object.assign(row, {
    status:'CLOSED', closedAt, closedAtTR:trTime(closedAt), closePrice:safeNum(close,10),
    pnlUSDT:Number.isFinite(Number(cls.realizedPnl))?safeNum(cls.realizedPnl,4):null,
    roiPct:Number.isFinite(rawMove)?safeNum(rawMove*lev,2):(Number.isFinite(Number(cls.roiPct))?safeNum(cls.roiPct,2):null),
    exitReason:cls.code||'CLOSED', exitLabel:cls.label||'Binance kapanışı',
    resultNote:buildResultNote(cls,state),
    sl:cls.sl||state?.currentSL||null, tp:cls.tp||state?.targetTP||null,
    cooldownMin:cdMs>0?Math.ceil(cdMs/60000):null,
  });
  tradeLedger = [row,...tradeLedger.filter(x=>x!==row&&x.id!==row.id)].slice(0,250);
  try {
    const mod2 = state?.aiRunner ? 'RUNNER' : 'NORMAL';
    const nedenKisa = String(row.entryReason || state?.openReason || '').replace(/\|.*$/,'').slice(0, 70);
    r345SonIslemler.unshift({
      t: trTime(closedAt), coin: sym.replace('USDT',''), side: normalizeSide(row.side)||'?',
      roi: Number.isFinite(row.roiPct) ? row.roiPct : null, mod: mod2,
      cikis: cls.label || cls.code || '?', neden: nedenKisa
    });
    if (r345SonIslemler.length > 12) r345SonIslemler.length = 12;
  } catch(_) {}
  try { r126UpdatePlaybookStats(state, cls); } catch(_) {}
  try { r181TradeCloseCard(row, state, cls).catch(e=>{ try { console.log('[telegram sessiz başarısız] R186_TG_CLOSE_PROMISE_FAIL:', String(e?.message||e)); } catch(_) {} }); } catch(_tge) {}
  saveTradeLedger(); return row;
}

function r311zCoinGecmisOzeti(symbol, lookbackMs = 6 * 60 * 60 * 1000) {
  try {
    const sym = String(symbol || '').replace('USDT', '').toUpperCase();
    if (!sym) return null;
    const now = Date.now();
    const rows = (Array.isArray(tradeLedger) ? tradeLedger : []).filter(x => {
      if (String(x.symbol || '').replace('USDT','').toUpperCase() !== sym) return false;
      if (x.status !== 'CLOSED') return false;
      const closedAt = Number(x.closedAt || 0);
      return closedAt && (now - closedAt <= lookbackMs);
    });
    if (!rows.length) return null;
    rows.sort((a, b) => Number(b.closedAt || 0) - Number(a.closedAt || 0));
    const son = rows.slice(0, 8);
    let kazanan = 0, kaybeden = 0, netPnl = 0;
    let noSweepLossL = 0, noSweepLossS = 0, longLoss = 0, shortLoss = 0;
    for (const r of son) {
      const pnl = Number(r.pnlUSDT || 0);
      netPnl += pnl;
      const side = normalizeSide(r.side);
      const txt = [r.entryReason, r.resultNote, r.exitReason].filter(Boolean).join(' ').toLowerCase();
      const wasNoSweep = /sweep[✗x]|nosweep|sweep yok|süpürme yok/i.test(txt);
      if (pnl > 0) kazanan++;
      else if (pnl < 0) {
        kaybeden++;
        if (side === 'LONG') { longLoss++; if (wasNoSweep) noSweepLossL++; }
        if (side === 'SHORT') { shortLoss++; if (wasNoSweep) noSweepLossS++; }
      }
    }
    const last = son[0];
    const lastPnl = Number(last.pnlUSDT || 0);
    const lastSide = normalizeSide(last.side);
    const lastTxt = [last.entryReason, last.resultNote].filter(Boolean).join(' ').toLowerCase();
    const lastNoSweep = /sweep[✗x]|nosweep|sweep yok|süpürme yok/i.test(lastTxt);
    const dakikaOnce = Math.round((now - Number(last.closedAt || now)) / 60000);
    let sonIslem = `son işlem ${lastSide} ${lastPnl >= 0 ? '+' : ''}${lastPnl.toFixed(1)}$ (${dakikaOnce}dk önce${lastNoSweep ? ', noSweep' : ''})`;
    let ders = '';
    if (noSweepLossL >= 2) ders = ` ⚠UYARI: bu coinde noSweep LONG ${noSweepLossL} kez yandı — sweep yoksa LONG açma`;
    else if (noSweepLossS >= 2) ders = ` ⚠UYARI: bu coinde noSweep SHORT ${noSweepLossS} kez yandı — sweep yoksa SHORT açma`;
    else if (longLoss >= 3 && shortLoss === 0 && kazanan === 0) ders = ` ⚠UYARI: bu coinde LONG üst üste ${longLoss} kez yandı, SHORT denenmedi — yön körlüğü, KARŞI yönü değerlendir`;
    else if (shortLoss >= 3 && longLoss === 0 && kazanan === 0) ders = ` ⚠UYARI: bu coinde SHORT üst üste ${shortLoss} kez yandı, LONG denenmedi — yön körlüğü, KARŞI yönü değerlendir`;
    return `${sym} son ${lookbackMs / 3600000}sa: ${son.length} işlem (${kazanan}K/${kaybeden}Z, net ${netPnl >= 0 ? '+' : ''}${netPnl.toFixed(1)}$) · ${sonIslem}${ders}`;
  } catch (_) { return null; }
}

const R84_MUM_MS = 5 * 60 * 1000;
const CD_PROFIT_MS  = 10 * 60 * 1000;
const CD_BE_MS      =  8 * 60 * 1000;
const CD_MANUAL_MS  = 30 * 60 * 1000;
const CD_LOSS_MS    = 60 * 60 * 1000;
const CD_HARD_LOSS_MS = 60 * 60 * 1000;
const CD_ERR_MS_R25 = 8 * 60 * 1000;
const CD_AFTER_CLOSE_PAUSE_MS = 45 * 1000;

function entryPatternKeyFromText(txt='') {
  const t = String(txt||'').toLowerCase();
  const parts = [];
  if (t.includes('r134') || t.includes('hızlı edge') || t.includes('fast edge') || t.includes('mikro-scalp')) parts.push('R134_FAST');
  if (t.includes('r160 trader karar') || t.includes('r160:')) parts.push('R160');
  if (t.includes('r159 momentum')) parts.push('R159');
  if (t.includes('trend devam')) parts.push('TREND_CONT');
  if (t.includes('tuzak dönüş') || t.includes('counter_trap')) parts.push('COUNTER_TRAP');
  if (t.includes('5m momentum')) parts.push('MOMENTUM');
  if (t.includes('5m akış') || t.includes('flow_scalp')) parts.push('FLOW');
  if (t.includes('formasyon yok') || t.includes('mum teyidi zayıf')) parts.push('WEAK_5M_CANDLE');
  if (t.includes('late-chase')) parts.push('LATE_CHASE');
  if (t.includes('live-opposite')) parts.push('LIVE_OPPOSITE');
  if (t.includes('rvol:very_low') || t.includes('rvol: very_low') || t.includes('very_low×')) parts.push('VERY_LOW_RVOL');
  if (t.includes('sweep') || t.includes('süpür')) parts.push('SWEEP');
  if (t.includes('body-reclaim') || t.includes('body geri') || t.includes('gövde')) parts.push('BODY_RECLAIM');
  if (t.includes('htf:karşı') || t.includes('karşı 15m') || t.includes('karşı 1h') || t.includes('karşı 4h')) parts.push('HTF_COUNTER');
  if (t.includes('stop hunt') || t.includes('stophunt')) parts.push('STOPHUNT');
  if (t.includes('wyckoff') || t.includes('spring') || t.includes('utad')) parts.push('WYCKOFF');
  if (t.includes('mm genuine_up') || t.includes('mm up_sweep')) parts.push('MM_UP');
  if (t.includes('mm genuine_down') || t.includes('mm down_sweep')) parts.push('MM_DOWN');
  if (t.includes('funding') || t.includes('fonlama')) parts.push('FUNDING');
  if (t.includes('vwap')) parts.push('VWAP');
  if (t.includes('rvol')) parts.push('RVOL');
  return parts.length ? [...new Set(parts)].join('+') : 'GENERIC';
}
function recentLossPatternGuard(symbolOrSide, sideMaybe, decisionChain={}, lookbackMs=24*60*60*1000) {
  let sym = '', side = '';
  if (sideMaybe) { sym = normalizeSymbol(symbolOrSide); side = normalizeSide(sideMaybe); }
  else { side = normalizeSide(symbolOrSide); }
  const now = Date.now();
  const rawText = [decisionChain?.reason, decisionChain?.brainSummary, decisionChain?.entryPermissionReason, decisionChain?.brainMode].filter(Boolean).join(' ');
  let key = entryPatternKeyFromText(rawText);
  if (key === 'GENERIC') {
    const bm = String(decisionChain?.brainMode || '').toUpperCase();
    const ep = String(decisionChain?.entryPermissionReason || '').toUpperCase();
    key = [bm, ep].filter(Boolean).join('+') || 'GENERIC';
  }
  const rows = tradeLedger.filter(x => {
    const closedAt = Number(x.closedAt || 0);
    if (!closedAt || now - closedAt > lookbackMs) return false;
    if (normalizeSide(x.side) !== side) return false;
    const pnl = Number(x.pnlUSDT);
    if (!(Number.isFinite(pnl) && pnl < 0)) return false;
    const tx = [x.entryReason, x.exitReason, x.resultNote].filter(Boolean).join(' ');
    return entryPatternKeyFromText(tx) === key;
  });
  const sameSymLosses = sym ? rows.filter(x => normalizeSymbol(x.symbol || '') === sym || normalizeSymbol((x.symbol||'')+'USDT') === sym) : [];
  if (sameSymLosses.length >= 1) {
    return { block:true, key, count:sameSymLosses.length, scope:'SYMBOL', reason:`${sym.replace('USDT','')} aynı ${side} setup bugün zarar yazdı (${key}); tekrar giriş için yeni yapı/ters akış bekleniyor` };
  }
  if (rows.length >= 3) {
    return { block:true, key, count:rows.length, scope:'GLOBAL', reason:`Son 24s aynı ${side} setup paterni ${rows.length} kez zarar yazdı (${key}); global kalite freni` };
  }
  return { block:false, key, count:rows.length, scope:'NONE' };
}

const FUTURES_TICKERS_CACHE_MS = 45 * 1000;
const R33_TOP_GAINER_LOCK_COUNT = 10;
const R33_TOP_GAINER_MIN_QUOTE_VOL = 1_000_000;
const R54_SCAN_MODES = new Set(['FAST6','TOP10','TOP24']);
function normalizeR54ScanMode(v) {
  const raw = String(v || '').toUpperCase();
  if (raw === '6' || raw === 'FAST' || raw === 'FAST6' || raw === 'TOP3_PLUS3') return 'FAST6';
  if (raw === '10' || raw === 'TOP10') return 'TOP10';
  if (raw === '24' || raw === 'TOP24' || raw === 'FULL24') return 'TOP24';
  return 'TOP24';
}
function r54ScanLimitForMode(mode, fallback=6) {
  const m = normalizeR54ScanMode(mode || fallback);
  if (m === 'FAST6') return 6;
  if (m === 'TOP10') return 10;
  return 24;
}
function r54VolatilityRankScore(c={}) {
  const range = Number(c.rangePct || 0);
  const volScore = Number(c.volScore || 0);
  const chg = Math.abs(Number(c.change24h || c.change24hRaw || 0));
  const vol = Number(c.volume || 0);
  const trades = Number(c.trades || 0);
  return Math.round((range*6) + (chg*3) + Math.min(30, volScore/2) + Math.min(25, Math.log10(Math.max(10, vol))/1.5) + Math.min(12, trades/100000));
}
function r33IsTradeableTicker(t={}) {
  const sym = String(t.symbol || '').toUpperCase();
  if (!sym.endsWith('USDT')) return false;
  if (['BTCUSDT','ETHUSDT','BNBUSDT','XRPUSDT','SOLUSDT','ADAUSDT','DOGEUSDT','DOTUSDT','MATICUSDT','LTCUSDT','TRXUSDT','AVAXUSDT','LINKUSDT','UNIUSDT','WBTCUSDT','SHIBUSDT'].includes(sym)) return false;
  const vol = Number(t.quoteVolume || t.volume || 0);
  const price = Number(t.lastPrice || t.price || 0);
  return vol >= R33_TOP_GAINER_MIN_QUOTE_VOL && price > 0;
}
function r33TopGainersFromTickers(data=[], n=R33_TOP_GAINER_LOCK_COUNT) {
  if (!Array.isArray(data)) return new Map();
  const rows = data
    .filter(r33IsTradeableTicker)
    .map(t => ({
      symbol: String(t.symbol).replace('USDT',''),
      fullSymbol: String(t.symbol).toUpperCase(),
      price: Number(t.lastPrice || 0),
      change24h: Number(t.priceChangePercent || 0),
      change24hRaw: Number(t.priceChangePercent || 0),
      volume: Number(t.quoteVolume || 0),
      high: Number(t.highPrice || 0),
      low: Number(t.lowPrice || 0),
      trades: Number(t.count || 0),
      rangePct: Number(t.lastPrice) > 0 ? +(((Number(t.highPrice||0)-Number(t.lowPrice||0))/Number(t.lastPrice))*100).toFixed(2) : 0,
      source: 'top_gainers_lock'
    }))
    .filter(c => Number.isFinite(c.change24h) && (Math.abs(c.change24h) >= 2 || c.rangePct >= 4))
    .map(c => {
      const _range = Math.max(1e-12, c.high - c.low);
      const _pos = (c.price - c.low) / _range;
      const _freshMult = 0.55 + (Math.abs(_pos - 0.5) * 2 * 0.45);
      const _yonCarpan = c.change24h >= 0 ? 1.0 : 0.35;
      const _chgKatki = c.change24h >= 0 ? c.change24h * 1.2 : 0;
      const _score = ((c.rangePct * 1.6) + _chgKatki +
                      (Math.log10(Math.max(1, c.volume)) * 2) + (Math.log10(Math.max(1, c.trades)) * 1.5)) * _freshMult * _yonCarpan;
      return { ...c, r152Score: _score };
    })
    .sort((a,b) => (b.r152Score - a.r152Score));
  const out = new Map();
  rows.forEach((c, i) => out.set(c.fullSymbol, {...c, topGainerRank:i+1, topGainerLocked:true}));
  return out;
}

const R152_NEW_COIN_AGE_MS = 15 * 24 * 60 * 60 * 1000;
const R152_EXCHANGE_INFO_CACHE_MS = 6 * 60 * 60 * 1000;

let r152OnboardCache = { ts: 0, map: null };

async function r152GetOnboardDateMap() {
  try {
    const now = Date.now();
    if (r152OnboardCache.map && now - r152OnboardCache.ts < R152_EXCHANGE_INFO_CACHE_MS) {
      return r152OnboardCache.map;
    }
    const info = await bPub('/fapi/v1/exchangeInfo', '');
    if (!Array.isArray(info?.symbols)) return r152OnboardCache.map || new Map();
    const map = new Map();
    for (const s of info.symbols) {
      if (s.symbol && Number.isFinite(Number(s.onboardDate))) {
        map.set(String(s.symbol).toUpperCase(), Number(s.onboardDate));
      }
    }
    r152OnboardCache = { ts: now, map };
    return map;
  } catch(e) {
    return r152OnboardCache.map || new Map();
  }
}

function r152FilterAndExtendGainers(data=[], onboardMap=new Map(), n=R33_TOP_GAINER_LOCK_COUNT) {
  if (!Array.isArray(data)) return new Map();
  const now = Date.now();

  const allCandidates = data
    .filter(r33IsTradeableTicker)
    .map(t => ({
      symbol: String(t.symbol).replace('USDT',''),
      fullSymbol: String(t.symbol).toUpperCase(),
      price: Number(t.lastPrice || 0),
      change24h: Number(t.priceChangePercent || 0),
      change24hRaw: Number(t.priceChangePercent || 0),
      volume: Number(t.quoteVolume || 0),
      high: Number(t.highPrice || 0),
      low: Number(t.lowPrice || 0),
      trades: Number(t.count || 0),
      rangePct: Number(t.lastPrice) > 0 ? +(((Number(t.highPrice||0)-Number(t.lowPrice||0))/Number(t.lastPrice))*100).toFixed(2) : 0,
      source: 'top_gainers_lock'
    }))
    .filter(c => Number.isFinite(c.change24h) && Math.abs(c.change24h) >= 2 && Number(c.volume) > 1_000_000)
    .map(c => {
      const _range = Math.max(1e-12, c.high - c.low);
      const _pos = (c.price - c.low) / _range;
      const _fresh = 0.55 + (Math.abs(_pos - 0.5) * 2 * 0.45);
      const _yonCarpan = c.change24h >= 0 ? 1.0 : 0.35;
      const _chgKatki = c.change24h >= 0 ? c.change24h * 1.2 : 0;
      const _score = ((c.rangePct * 1.6) + _chgKatki +
                      (Math.log10(Math.max(1, c.volume)) * 2) + (Math.log10(Math.max(1, c.trades)) * 1.5)) * _fresh * _yonCarpan;
      return { ...c, r152Score: _score };
    })
    .sort((a,b) => (b.r152Score - a.r152Score));

  const hasAgeData = onboardMap.size > 0;
  const selected = [];
  const skipped = [];

  for (const c of allCandidates) {
    if (selected.length >= n) break;
    const onboard = onboardMap.get(c.fullSymbol);
    const isNewCoin = hasAgeData && Number.isFinite(onboard) && (now - onboard) < R152_NEW_COIN_AGE_MS;
    if (isNewCoin) {
      const ageDays = Math.floor((now - onboard) / (24*60*60*1000));
      skipped.push({ sym: c.symbol, ageDays });
      continue;
    }
    selected.push(c);
  }

  if (skipped.length > 0) {
    const skipTxt = skipped.map(x => `${x.sym}(${x.ageDays}g)`).join(', ');
    const replaceTxt = selected.slice(selected.length - skipped.length).map(x => `${x.symbol}(${selected.indexOf(x)+1})`).join(', ');
    const logMsg = `🆕 R152 yeni coin filtresi: ${skipTxt} TOP10'dan çıkarıldı → ${replaceTxt || 'yedek eklendi'}`;
    const now2 = Date.now();
    if (!r152FilterAndExtendGainers._lastLogMsg || r152FilterAndExtendGainers._lastLogMsg !== logMsg || now2 - (r152FilterAndExtendGainers._lastLogTs||0) > 60000) {
      r152FilterAndExtendGainers._lastLogMsg = logMsg;
      r152FilterAndExtendGainers._lastLogTs = now2;
      logAuto(logMsg);
    }
  }

  const out = new Map();
  selected.forEach((c, i) => out.set(c.fullSymbol, {...c, topGainerRank:i+1, topGainerLocked:true}));
  return out;
}

async function scanVolatility() {
  try {

    const now = Date.now();
    const tickers = await cached('futures_tickers', FUTURES_TICKERS_CACHE_MS, () => bPub('/fapi/v1/ticker/24hr'));
    if (!Array.isArray(tickers) || tickers.length === 0) {
      console.log('[VOLATILITE] futures_tickers boş/alınamadı — liste güncellenmedi');
      return;
    }
    const scored = tickers
      .filter(t => t.symbol.endsWith('USDT') &&
        parseFloat(t.quoteVolume) > 20000000 &&
        parseFloat(t.lastPrice) > 0)
      .map(t => {
        const vol   = parseFloat(t.quoteVolume);
        const count = parseInt(t.count) || 0;
        const high  = parseFloat(t.highPrice);
        const low   = parseFloat(t.lowPrice);
        const last  = parseFloat(t.lastPrice);

        const rawChg = parseFloat(t.priceChangePercent);
        const absChg = Math.abs(rawChg);
        const baseVol =
          (absChg > 10 ? 40 : absChg > 5 ? 30 : absChg > 3 ? 20 : absChg > 1 ? 10 : 3) +
          (vol > 1e9 ? 30 : vol > 5e8 ? 20 : vol > 1e8 ? 12 : vol > 5e7 ? 6 : 3) +
          (count > 500000 ? 20 : count > 200000 ? 12 : count > 100000 ? 6 : 2) +
          ((high - low) / last * 100 > 8 ? 10 : (high - low) / last * 100 > 4 ? 5 : 0);
        const dirMult = rawChg > 3 ? 1.25 : rawChg > 0 ? 1.0 : rawChg > -2 ? 0.85 : rawChg > -5 ? 0.55 : 0.30;
        const _range = Math.max(1e-12, high - low);
        const _pos = (last - low) / _range;
        const _freshMult = 0.55 + (Math.abs(_pos - 0.5) * 2 * 0.45);
        const volScore = Math.round(baseVol * dirMult * _freshMult);

        return {
          symbol: t.symbol.replace('USDT',''),
          fullSymbol: t.symbol,
          price: last,
          change24h: parseFloat(t.priceChangePercent),
          change24hRaw: rawChg,
          dirMult,
          volume: vol,
          trades: count,
          rangePct: +((high - low) / last * 100).toFixed(2),
          volScore: Math.round(volScore),
        };
      })
      .sort((a, b) => b.volScore - a.volScore)
      .slice(0, 40);

    volatilityStore.coins = scored;
    volatilityStore.lastUpdate = now;
  } catch(e) {
    console.log('Volatilite scanner hata:', e.message);
  }
}

setInterval(scanVolatility, 3 * 60 * 1000);
scanVolatility();

setInterval(() => {
  const activeSyms = new Set(
    [...(volatilityStore.coins||[]).map(c=>c.fullSymbol),
     ...(autoConfig?.symbols||[]),
    ]
  );
  for (const [sym, eng] of tickStore.entries()) {
    if (!activeSyms.has(sym)) {
      try { eng.ws?.terminate(); } catch(e) {}
      tickStore.delete(sym);
    }
  }
  for (const [sym, store] of cvdStore.entries()) {
    if (!activeSyms.has(sym)) {
      try { store.ws?.terminate(); } catch(e) {}
      cvdStore.delete(sym);
    }
  }
  for (const [sym, store] of icebergStore.entries()) {
    if (!activeSyms.has(sym)) {
      try { store.ws?.terminate(); } catch(e) {}
      icebergStore.delete(sym);
    }
  }
  console.log(`[MEM] WS temizlendi. tick:${tickStore.size} cvd:${cvdStore.size} iceberg:${icebergStore.size}`);
}, 60 * 60 * 1000);

app.get('/api/volatile-coins', (req, res) => {
  res.json({ ok:true, coins: volatilityStore.coins,
    lastUpdate: volatilityStore.lastUpdate });
});

function getKillZone() {
  return { zone:'CRYPTO_24_7', strength:1.0, label:'⚡ Kripto 24/7', active:true, disabled:true };
}

app.get('/api/killzone', (req, res) => {
  res.json({ ok:true, ...getKillZone(), time: trTime(), timeUTC: new Date().toUTCString() });
});

function calcScanInterest(c={}) {
  const vol = Number(c.volume || c.quoteVolume || 0);
  const signed = Number(c.change24h ?? c.priceChangePercent ?? 0);
  const absChg = Math.abs(signed);
  const trades = Number(c.trades || c.count || 0);
  const range = Number(c.rangePct || 0);

  let interest = 0;
  if (vol > 1e9) interest += 42;
  else if (vol > 5e8) interest += 34;
  else if (vol > 1e8) interest += 22;
  else if (vol > 5e7) interest += 14;
  else if (vol > 2e7) interest += 8;
  else interest += 2;

  if (absChg > 12) interest += 28;
  else if (absChg > 8) interest += 22;
  else if (absChg > 5) interest += 16;
  else if (absChg > 3) interest += 10;
  else if (absChg > 1.2) interest += 5;

  if (range > 12) interest += 15;
  else if (range > 8) interest += 10;
  else if (range > 5) interest += 6;
  else if (range > 3) interest += 3;

  if (trades > 500000) interest += 18;
  else if (trades > 200000) interest += 12;
  else if (trades > 100000) interest += 8;
  else if (trades > 50000) interest += 4;

  if (Number(c.volScore || 0) > 0) interest += Math.min(14, Math.round(Number(c.volScore || 0) / 6));

  if (absChg < 1.0 && range < 2.4) interest -= 14;
  if (signed > 3) interest += 6;
  if (signed < -3) interest += 5;

  return Math.max(1, Math.round(interest));
}

function normalizeTickerToCoin(t={}) {
  const full = String(t.fullSymbol || t.symbol || '').toUpperCase().endsWith('USDT')
    ? String(t.fullSymbol || t.symbol).toUpperCase()
    : String(t.fullSymbol || t.symbol || '').toUpperCase() + 'USDT';
  const sym = full.replace('USDT','');
  return {
    symbol: sym,
    fullSymbol: full,
    price: Number(t.price || t.lastPrice || 0),
    change24h: Number(t.change24h ?? t.priceChangePercent ?? 0),
    volume: Number(t.volume || t.quoteVolume || 0),
    high: Number(t.high || t.highPrice || 0),
    low: Number(t.low || t.lowPrice || 0),
    trades: Number(t.trades || t.count || 0),
    rangePct: Number(t.rangePct || 0),
    volScore: Number(t.volScore || 0),
  };
}

const r22AnalysisMemory = new Map();
const R22_MEMORY_MS = 12 * 60 * 1000;

function r22BaseSymbol(symbol) {
  return String(symbol || '').toUpperCase().replace(/USDT$/,'').replace(/^1000/,'');
}
function r22SectorOf(symbol) {
  const s = r22BaseSymbol(symbol);
  const groups = [
    ['AI',      ['FET','RENDER','RNDR','TAO','AIGENSYN','CGPT','NFP','WLD','VVV','VIRTUAL','GRASS','ARKM',
                 'AI','AIOZ','OCEAN','AGIX','NMR','MASA','AIT','PAAL','PALM','HYPER']],
    ['MEME',    ['PEPE','DOGE','SHIB','WIF','BONK','FLOKI','MEME','TURBO','BOME','MYRO','BRETT','GOAT',
                 'MOG','NEIRO','POPCAT','SUNDOG','PNUT','ACT','TRUMP','MELANIA']],
    ['LAYER2',  ['OP','ARB','STRK','MANTA','ZK','SCROLL','LINEA','METIS','IMX','LOOPRING','LRC']],
    ['LAYER1',  ['SUI','APT','NEAR','SEI','TON','INJ','TIA','AVAX','ADA','HBAR','ALGO','ATOM','DOT',
                 'KAVA','OSMO','LUNA','ONE','EGLD','ICX','QTUM']],
    ['PRIVACY', ['ZEC','XMR','SCRT','ROSE','AZERO','BEAM','DUSK']],
    ['DEFI',    ['UNI','AAVE','PENDLE','ENA','LDO','CRV','MKR','COMP','SUSHI','CAKE','JUP','DYDX',
                 'GMX','BAL','SNX','YFI','RUNE','BANANA','BIFI']],
    ['GAMING',  ['GALA','SAND','MANA','PIXEL','MAGIC','PORTAL','YGG','AXS','GMT','BIGTIME',
                 'RONIN','RON','ENJ','BEAM','MAGIC']],
    ['STORAGE', ['FIL','AR','HNT','STORJ','BLUZ']],
    ['RWA',     ['LINK','PYTH','ONDO','OM','TRU','RIO','POND','MPL','CFG','POLYX','TOKEN']],
    ['EXCHANGE',['OKB','LEO','CRO','KCS','GT','MX']],
  ];
  for (const [name, arr] of groups) if (arr.includes(s)) return name;
  return 'ALT';
}
function r22PruneMemory() {
  const now = Date.now();
  for (const [k,v] of r22AnalysisMemory.entries()) {
    if (!v || now - (v.ts||0) > R22_MEMORY_MS) r22AnalysisMemory.delete(k);
  }
}
function r22RememberAnalysis(symbol, summary={}) {
  try {
    const full = String(symbol || '').toUpperCase().endsWith('USDT') ? String(symbol).toUpperCase() : String(symbol).toUpperCase() + 'USDT';
    const side = summary.recommendation === 'LONG' || summary.recommendation === 'SHORT' ? summary.recommendation : 'WAIT';
    const dc = summary.decisionChain || {};
    const score = side === 'LONG' ? Number(summary.longScore||0) : side === 'SHORT' ? Number(summary.shortScore||0) : Math.max(Number(summary.longScore||0), Number(summary.shortScore||0));
    r22AnalysisMemory.set(full, {
      ts: Date.now(), symbol: full, base: r22BaseSymbol(full), sector: r22SectorOf(full),
      side, tier: String(dc.tier || 'WAIT'), autoOk: !!dc.autoOk,
      priorityScore: Number(dc.priorityScore || 0), score,
      signalAgeMin: Number(summary.signalAgeMin || 0),
      r22: dc.r22 || null,
    });
    r22PruneMemory();
  } catch(e) {}
}
function r22RotationBias(symbol, side) {
  r22PruneMemory();
  const full = String(symbol || '').toUpperCase().endsWith('USDT') ? String(symbol).toUpperCase() : String(symbol).toUpperCase() + 'USDT';
  const sector = r22SectorOf(full);
  const rows = [...r22AnalysisMemory.values()].filter(x =>
    x.symbol !== full && x.side === side && ['A','B+'].includes(x.tier) && x.score >= 68 &&
    !(x.r22?.signalDecay?.noAuto)
  );
  const sameSector = rows.filter(x => x.sector === sector);
  const marketSameSide = rows;
  let score = 0, active = false, tag = 'NONE';
  if (sameSector.length >= 3) { active = true; score = 12; tag = 'SECTOR_ROTATION_STRONG'; }
  else if (sameSector.length >= 2) { active = true; score = 8; tag = 'SECTOR_ROTATION'; }
  else if (marketSameSide.length >= 4) { active = true; score = 7; tag = 'MARKET_ROTATION'; }
  return {
    active, score, tag, sector,
    sameSectorCount: sameSector.length,
    marketCount: marketSameSide.length,
    symbols: (sameSector.length ? sameSector : marketSameSide).slice(0,5).map(x => x.base),
  };
}
function r22Clamp(n, lo, hi) { return Math.max(lo, Math.min(hi, Number(n||0))); }

function r25DetectWickTrapMap(k1h=[], k4h=[], price=0, ctx={}) {
  const px = Number(price || 0);
  if (!px) return { upperTrap:false, lowerTrap:false, strength:0, notes:[] };
  const atrPct = Math.max(0.25, Number(ctx.atrPct || 1));
  const nearPct = Math.min(3.2, Math.max(0.65, atrPct * 0.85));
  const notes = [];
  function scan(kl, tf, weight=1) {
    const out = { upper:0, lower:0, nearestUpper:null, nearestLower:null };
    const arr = Array.isArray(kl) ? kl.slice(-(tf==='4h'?55:70)) : [];
    for (const k of arr) {
      const o=Number(k[1]), h=Number(k[2]), l=Number(k[3]), c=Number(k[4]);
      const range=h-l, body=Math.abs(c-o);
      if (!range || !h || !l) continue;
      const up=h-Math.max(o,c), dn=Math.min(o,c)-l;
      const upReject = up/range > 0.42 && up > body*1.15;
      const dnReject = dn/range > 0.42 && dn > body*1.15;
      const du = Math.abs(h-px)/px*100;
      const dl = Math.abs(px-l)/px*100;
      if (upReject && du <= nearPct) {
        out.upper += weight;
        if (!out.nearestUpper || du < out.nearestUpper.distPct)
          out.nearestUpper = { price:h, distPct:+du.toFixed(2), tf };
      }
      if (dnReject && dl <= nearPct) {
        out.lower += weight;
        if (!out.nearestLower || dl < out.nearestLower.distPct)
          out.nearestLower = { price:l, distPct:+dl.toFixed(2), tf };
      }
    }
    return out;
  }
  const a = scan(k1h,'1h',1.0), b = scan(k4h,'4h',1.45);
  let upperStrength = a.upper + b.upper;
  let lowerStrength = a.lower + b.lower;
  const nearestUpper = [a.nearestUpper,b.nearestUpper].filter(Boolean)
    .sort((x,y)=>x.distPct-y.distPct)[0] || null;
  const nearestLower = [a.nearestLower,b.nearestLower].filter(Boolean)
    .sort((x,y)=>x.distPct-y.distPct)[0] || null;
  const pd1 = ctx.pd1h || {}, pd4 = ctx.pd4h || {};
  const vp1 = ctx.vpvr1h || {}, vp4 = ctx.vpvr4h || {};
  const premHigh = pd1.zone==='PREMIUM_HIGH'||pd4.zone==='PREMIUM_HIGH'||
    Number(pd1.pct||0)>78||Number(pd4.pct||0)>82;
  const discLow  = pd1.zone==='DISCOUNT_LOW' ||pd4.zone==='DISCOUNT_LOW' ||
    Number(pd1.pct||0)<22||Number(pd4.pct||0)<18;
  const nearVah = (Number(vp1.vah)>0 && Math.abs(px-Number(vp1.vah))/px*100<=nearPct) ||
    (Number(vp4.vah)>0 && Math.abs(px-Number(vp4.vah))/px*100<=nearPct*1.25);
  const nearVal = (Number(vp1.val)>0 && Math.abs(px-Number(vp1.val))/px*100<=nearPct) ||
    (Number(vp4.val)>0 && Math.abs(px-Number(vp4.val))/px*100<=nearPct*1.25);
  const upLiq = ctx.liq1h?.buyLiq?.[0] || ctx.liq4h?.buyLiq?.[0];
  const dnLiq = ctx.liq1h?.sellLiq?.[0] || ctx.liq4h?.sellLiq?.[0];
  const nearUpperLiq = !!(upLiq && Number(upLiq.distPct)>=-0.05 && Number(upLiq.distPct)<=nearPct*1.25);
  const nearLowerLiq = !!(dnLiq && Number(dnLiq.distPct)>=-0.05 && Number(dnLiq.distPct)<=nearPct*1.25);
  if (premHigh)     { upperStrength+=1.0; notes.push('premium_high'); }
  if (nearVah)      { upperStrength+=0.8; notes.push('VAH_yakın'); }
  if (nearUpperLiq) { upperStrength+=0.9; notes.push('üst_liq_yakın'); }
  if (discLow)      { lowerStrength+=1.0; notes.push('discount_low'); }
  if (nearVal)      { lowerStrength+=0.8; notes.push('VAL_yakın'); }
  if (nearLowerLiq) { lowerStrength+=0.9; notes.push('alt_liq_yakın'); }
  const upperTrap = upperStrength >= 2.8;
  const lowerTrap = lowerStrength >= 2.8;
  const dominant = upperTrap&&upperStrength>=lowerStrength ? 'UPPER_REJECTION_TRAP'
    : lowerTrap ? 'LOWER_REJECTION_TRAP' : 'NONE';
  return {
    upperTrap, lowerTrap, dominant,
    upperStrength:+upperStrength.toFixed(2), lowerStrength:+lowerStrength.toFixed(2),
    strength:+Math.max(upperStrength,lowerStrength).toFixed(2),
    nearestUpper, nearestLower, nearPct:+nearPct.toFixed(2), notes
  };
}

function calcHeikinAshi(klines) {
  if (!Array.isArray(klines) || klines.length < 2) return [];
  const ha = [];
  for (let i = 0; i < klines.length; i++) {
    const o = Number(klines[i][1]), h = Number(klines[i][2]), l = Number(klines[i][3]), c = Number(klines[i][4]);
    if (![o,h,l,c].every(Number.isFinite)) continue;
    const haC = (o + h + l + c) / 4;
    const haO = i === 0 || !ha[i-1] ? (o + c) / 2 : (ha[i-1].o + ha[i-1].c) / 2;
    ha.push({
      o: haO,
      h: Math.max(h, haO, haC),
      l: Math.min(l, haO, haC),
      c: haC,
      bull: haC > haO,
      body: Math.abs(haC - haO),
      upper: Math.max(h, haO, haC) - Math.max(haO, haC),
      lower: Math.min(haO, haC) - Math.min(l, haO, haC)
    });
  }
  return ha;
}

function haSignal(ha) {
  if (!Array.isArray(ha) || ha.length < 3) return { signal:'NEUTRAL', strength:0 };
  const last = ha.slice(-3);
  const bc = last.filter(c => c.bull).length;
  const nc = last.filter(c => !c.bull).length;
  const avgB = last.reduce((s,c) => s + Number(c.body || 0), 0) / 3;
  const weak = last[2].body < avgB * 0.35;
  const sBull = last[2].bull && last[2].lower < last[2].body * 0.10 && last[2].upper < last[2].body * 0.30;
  const sBear = !last[2].bull && last[2].upper < last[2].body * 0.10 && last[2].lower < last[2].body * 0.30;
  if (bc === 3 && sBull) return { signal:'STRONG_BULL', strength:3 };
  if (nc === 3 && sBear) return { signal:'STRONG_BEAR', strength:3 };
  if (bc === 3) return { signal:'BULL', strength:2 };
  if (nc === 3) return { signal:'BEAR', strength:2 };
  if (weak) return { signal:'WEAKENING', strength:1 };
  return { signal:'NEUTRAL', strength:0 };
}

function cdlPatterns(klines) {
  if (!Array.isArray(klines) || klines.length < 5) return { bull:[], bear:[], score:0 };
  const bull = [], bear = [];
  const k = klines.slice(-5).map(c => ({
    o: Number(c[1]), h: Number(c[2]), l: Number(c[3]), c: Number(c[4]),
    body: Math.abs(Number(c[4]) - Number(c[1])),
    range: Number(c[2]) - Number(c[3]),
    upper: Number(c[2]) - Math.max(Number(c[1]), Number(c[4])),
    lower: Math.min(Number(c[1]), Number(c[4])) - Number(c[3]),
    bull: Number(c[4]) > Number(c[1])
  })).filter(x => [x.o,x.h,x.l,x.c,x.body,x.range].every(Number.isFinite));
  if (k.length < 5) return { bull:[], bear:[], score:0 };
  const [p4,p3,p2,p1,p0] = k;
  const safeRange = (x) => Math.max(Number(x?.range || 0), 1e-12);
  const safePrice = (x) => Math.max(Math.abs(Number(x || 0)), 1e-12);

  if (p0.body > 0 && p0.lower > p0.body * 2 && p0.upper < p0.body * 0.5) {
    if (p0.bull) bull.push({name:'Hammer',str:3}); else bear.push({name:'HangingMan',str:2});
  }
  if (p0.body > 0 && p0.upper > p0.body * 2 && p0.lower < p0.body * 0.5) {
    if (!p0.bull) bear.push({name:'ShootingStar',str:3}); else bull.push({name:'InvHammer',str:2});
  }
  if (p0.range > 0 && p0.body / safeRange(p0) < 0.10) { bull.push({name:'Doji',str:1}); bear.push({name:'Doji',str:1}); }
  if (p0.lower > p0.range * 0.60 && p0.body < p0.range * 0.10) bull.push({name:'DragonflyDoji',str:3});
  if (p0.upper > p0.range * 0.60 && p0.body < p0.range * 0.10) bear.push({name:'GravestoneDoji',str:3});
  if (p0.body > 0 && p0.upper < p0.body * 0.05 && p0.lower < p0.body * 0.05) {
    if (p0.bull) bull.push({name:'BullMarubozu',str:3}); else bear.push({name:'BearMarubozu',str:3});
  }

  if (p1 && !p1.bull && p0.bull && p0.o < p1.c && p0.c > p1.o && p0.body > p1.body * 1.1) bull.push({name:'BullEngulfing',str:4});
  if (p1 && p1.bull && !p0.bull && p0.o > p1.c && p0.c < p1.o && p0.body > p1.body * 1.1) bear.push({name:'BearEngulfing',str:4});
  if (p1 && p0.h < p1.h && p0.l > p1.l && p0.body < p1.body * 0.5) {
    if (p0.bull && !p1.bull) bull.push({name:'BullHarami',str:2});
    if (!p0.bull && p1.bull) bear.push({name:'BearHarami',str:2});
  }
  if (p1 && p0.h < p1.h && p0.l > p1.l && p0.body / safeRange(p0) < 0.10) {
    if (!p1.bull) bull.push({name:'BullHaramiCross',str:3}); else bear.push({name:'BearHaramiCross',str:3});
  }
  if (p1 && Math.abs(p0.l - p1.l) / Math.max(safePrice(p0.l), safePrice(p1.l)) < 0.001 && !p1.bull && p0.bull) bull.push({name:'TweezerBottom',str:3});
  if (p1 && Math.abs(p0.h - p1.h) / Math.max(safePrice(p0.h), safePrice(p1.h)) < 0.001 && p1.bull && !p0.bull) bear.push({name:'TweezerTop',str:3});
  if (p1 && !p1.bull && p0.bull && p0.o > p1.h && p0.upper < p0.body * 0.1 && p1.lower < p1.body * 0.1) bull.push({name:'BullKicking',str:5});
  if (p1 && p1.bull && !p0.bull && p0.o < p1.l && p1.upper < p1.body * 0.1 && p0.lower < p0.body * 0.1) bear.push({name:'BearKicking',str:5});
  if (p1 && p1.bull && !p0.bull && p0.o > p1.h && p0.c < (p1.o + p1.c) / 2 && p0.c > p1.o) bear.push({name:'DarkCloudCover',str:4});
  if (p1 && !p1.bull && p0.bull && p0.o < p1.l && p0.c > (p1.o + p1.c) / 2 && p0.c < p1.o) bull.push({name:'PiercingLine',str:4});

  if (p2 && p1 && p0) {
    if (!p2.bull && p2.body > 0 && p1.body < p2.body * 0.3 && p0.bull && p0.c > p2.o + (p2.c - p2.o) * 0.5) bull.push({name:'MorningStar',str:5});
    if (p2.bull && p2.body > 0 && p1.body < p2.body * 0.3 && !p0.bull && p0.c < p2.o + (p2.c - p2.o) * 0.5) bear.push({name:'EveningStar',str:5});
    if (!p2.bull && p2.body > 0 && p1.body / safeRange(p1) < 0.10 && p0.bull && p0.c > p2.o + (p2.c - p2.o) * 0.5) bull.push({name:'MorningDojiStar',str:5});
    if (p2.bull && p2.body > 0 && p1.body / safeRange(p1) < 0.10 && !p0.bull && p0.c < p2.o + (p2.c - p2.o) * 0.5) bear.push({name:'EveningDojiStar',str:5});
    if (p2.bull && p1.bull && p0.bull && p0.c > p1.c && p1.c > p2.c && p0.lower < p0.body * 0.3 && p1.lower < p1.body * 0.3) bull.push({name:'ThreeWhiteSoldiers',str:5});
    if (!p2.bull && !p1.bull && !p0.bull && p0.c < p1.c && p1.c < p2.c && p0.upper < p0.body * 0.3 && p1.upper < p1.body * 0.3) bear.push({name:'ThreeBlackCrows',str:5});
    if (!p2.bull && p1.bull && p1.o < p2.c && p1.c > p2.o && p0.bull && p0.c > p1.c) bull.push({name:'ThreeOutsideUp',str:4});
    if (p2.bull && !p1.bull && p1.o > p2.c && p1.c < p2.o && !p0.bull && p0.c < p1.c) bear.push({name:'ThreeOutsideDown',str:4});
    if (!p2.bull && p1.h < p2.h && p1.l > p2.l && p0.bull && p0.c > p2.c) bull.push({name:'ThreeInsideUp',str:3});
    if (p2.bull && p1.h < p2.h && p1.l > p2.l && !p0.bull && p0.c < p2.c) bear.push({name:'ThreeInsideDown',str:3});
    if (!p2.bull && p2.body > 0 && p1.h < p2.l && p1.body / safeRange(p1) < 0.10 && p0.l > p1.h && p0.bull) bull.push({name:'AbandonedBabyBull',str:5});
    if (p2.bull && p2.body > 0 && p1.l > p2.h && p1.body / safeRange(p1) < 0.10 && p0.h < p1.l && !p0.bull) bear.push({name:'AbandonedBabyBear',str:5});
  }
  return { bull, bear, score: bull.reduce((s,p)=>s+p.str,0) - bear.reduce((s,p)=>s+p.str,0) };
}

function calcZigZag(klines, devPct = 1.5) {
  if (!Array.isArray(klines) || klines.length < 8) return [];
  const pts = [], highs = klines.map(c => Number(c[2])), lows = klines.map(c => Number(c[3]));
  let dir = 0, lastPt = { i:0, v:highs[0], type:'H' };
  for (let i = 1; i < klines.length; i++) {
    if (![highs[i], lows[i], lastPt.v].every(Number.isFinite)) continue;
    if (dir >= 0) {
      if (highs[i] >= lastPt.v) lastPt = { i, v:highs[i], type:'H' };
      else if ((lastPt.v - lows[i]) / Math.max(lastPt.v, 1e-12) * 100 >= devPct) { pts.push(lastPt); lastPt = { i, v:lows[i], type:'L' }; dir = -1; }
    }
    if (dir <= 0) {
      if (lows[i] <= lastPt.v) lastPt = { i, v:lows[i], type:'L' };
      else if ((highs[i] - lastPt.v) / Math.max(lastPt.v, 1e-12) * 100 >= devPct) { pts.push(lastPt); lastPt = { i, v:highs[i], type:'H' }; dir = 1; }
    }
  }
  pts.push(lastPt);
  return pts.slice(-12);
}

function detectChartPatterns(klines, lastPrice, atrPct = 1.5) {
  const tol = Math.max(0.01, Number(atrPct || 1.5) * 0.012);
  const pivots = calcZigZag(klines, Math.max(0.8, Number(atrPct || 1.5) * 0.8));
  if (pivots.length < 4 || !Number.isFinite(Number(lastPrice))) return { patterns:[], bullScore:0, bearScore:0 };
  const patterns = []; let bullScore = 0, bearScore = 0;
  const near = (a,b) => Math.abs(a-b) / Math.max(Math.abs(a), Math.abs(b), 1e-12) < tol;
  const px = pivots;
  for (let i = 0; i < px.length - 3; i++) {
    const [a,b,c,d] = [px[i], px[i+1], px[i+2], px[i+3]];
    if (!a || !b || !c || !d) continue;
    if (a.type==='H' && b.type==='L' && c.type==='H' && d.type==='L' && near(b.v,d.v) && c.v > Math.max(a.v,b.v)) {
      const neck = Math.max(a.v, c.v), bot = Math.min(b.v, d.v), tgt = +(neck + (neck - bot)).toFixed(8);
      if (lastPrice > c.v * 0.995) { patterns.push({name:'DoubleBottom',str:4,dir:'BULL',target:tgt}); bullScore += 16; }
      else { patterns.push({name:'DoubleBottomForming',str:2,dir:'BULL',target:tgt}); bullScore += 6; }
    }
    if (a.type==='L' && b.type==='H' && c.type==='L' && d.type==='H' && near(b.v,d.v) && c.v < Math.min(a.v,d.v)) {
      const neck = Math.min(a.v, c.v), top = Math.max(b.v, d.v), tgt = +(neck - (top - neck)).toFixed(8);
      if (lastPrice < c.v * 1.005) { patterns.push({name:'DoubleTop',str:4,dir:'BEAR',target:tgt}); bearScore += 16; }
      else { patterns.push({name:'DoubleTopForming',str:2,dir:'BEAR',target:tgt}); bearScore += 6; }
    }
  }
  for (let i = 0; i < px.length - 4; i++) {
    const [s0,s1,s2,s3,s4] = [px[i], px[i+1], px[i+2], px[i+3], px[i+4]];
    if (!s0 || !s1 || !s2 || !s3 || !s4) continue;
    if (s0.type==='H' && s1.type==='L' && s2.type==='L' && s3.type==='L' && s4.type==='H' && s1.v > s2.v && s3.v > s2.v && near(s1.v,s3.v)) {
      if (lastPrice > ((s0.v + s4.v) / 2) * 0.998) { patterns.push({name:'InvHeadShoulders',str:5,dir:'BULL'}); bullScore += 20; }
      else { patterns.push({name:'InvHSForming',str:3,dir:'BULL'}); bullScore += 8; }
    }
    if (s0.type==='L' && s1.type==='H' && s2.type==='H' && s3.type==='H' && s4.type==='L' && s1.v < s2.v && s3.v < s2.v && near(s1.v,s3.v)) {
      if (lastPrice < ((s0.v + s4.v) / 2) * 1.002) { patterns.push({name:'HeadShoulders',str:5,dir:'BEAR'}); bearScore += 20; }
      else { patterns.push({name:'HSForming',str:3,dir:'BEAR'}); bearScore += 8; }
    }
  }
  if (px.length >= 4) {
    const l4 = px.slice(-4);
    if (l4[0].type==='L' && l4[1].type==='H' && l4[2].type==='H' && l4[3].type==='L') {
      const pole = (l4[1].v - l4[0].v) / Math.max(l4[0].v, 1e-12) * 100;
      const pullbackPct = Math.abs(l4[1].v - l4[2].v) / Math.max(l4[1].v, 1e-12) * 100;
      if (pole > 3 && pullbackPct > 0.15 && pullbackPct < pole * 0.5) { patterns.push({name:'BullFlag',str:4,dir:'BULL'}); bullScore += 14; }
    }
    if (l4[0].type==='H' && l4[1].type==='L' && l4[2].type==='L' && l4[3].type==='H') {
      const pole = (l4[0].v - l4[1].v) / Math.max(l4[0].v, 1e-12) * 100;
      const pullbackPct = Math.abs(l4[2].v - l4[1].v) / Math.max(l4[1].v, 1e-12) * 100;
      if (pole > 3 && pullbackPct > 0.15 && pullbackPct < pole * 0.5) { patterns.push({name:'BearFlag',str:4,dir:'BEAR'}); bearScore += 14; }
    }
  }
  return { patterns, bullScore, bearScore };
}

function detectHarmonicPatterns(klines, lastPrice) {
  if (!Array.isArray(klines) || klines.length < 20 || !Number.isFinite(Number(lastPrice))) return { patterns:[], bullScore:0, bearScore:0 };
  const pivots = calcZigZag(klines, 2.0);
  if (pivots.length < 5) return { patterns:[], bullScore:0, bearScore:0 };
  const patterns = []; let bullScore = 0, bearScore = 0;
  const fibM = (val,tgt,tol=0.06) => Math.abs(val-tgt) < tgt * tol;
  const [X,A,B,C,D] = pivots.slice(-5);
  if (!X || !A || !B || !C || !D) return { patterns:[], bullScore:0, bearScore:0 };
  const XA = Math.abs(A.v-X.v), AB = Math.abs(B.v-A.v), BC = Math.abs(C.v-B.v), XD = Math.abs(D.v-X.v);
  if (XA < 1e-12 || AB < 1e-12 || BC < 1e-12) return { patterns:[], bullScore:0, bearScore:0 };
  const R_AB = AB / XA, R_BC = BC / AB, R_XD = XD / XA;
  const isBull = D.v < X.v, dir = isBull ? 'BULL' : 'BEAR';
  const addP = (name,str,s) => { patterns.push({ name, str, dir, prz:D.v }); if (isBull) bullScore += s; else bearScore += s; };
  if (fibM(R_AB,0.618) && R_BC > 0.382 && R_BC < 0.886 && fibM(R_XD,0.786)) addP(isBull?'BullGartley':'BearGartley',4,18);
  if (fibM(R_AB,0.786) && (fibM(R_XD,1.27) || fibM(R_XD,1.618,0.08))) addP(isBull?'BullButterfly':'BearButterfly',4,18);
  if (R_AB > 0.382 && R_AB < 0.5 && fibM(R_XD,0.886)) addP(isBull?'BullBat':'BearBat',5,22);
  if (R_AB > 0.382 && R_AB < 0.618 && fibM(R_XD,1.618,0.08)) addP(isBull?'BullCrab':'BearCrab',5,25);
  if (R_BC > 1.13 && R_BC < 1.618 && fibM(R_XD,0.886)) addP(isBull?'BullShark':'BearShark',3,14);
  if (patterns.length > 0) {
    const minD = Math.min(...patterns.map(p => Math.abs(Number(lastPrice) - p.prz) / Math.max(Number(lastPrice), 1e-12) * 100));
    if (minD > 3) { bullScore = Math.round(bullScore / 2); bearScore = Math.round(bearScore / 2); }
  }
  return { patterns, bullScore, bearScore };
}

function buildRenko(klines, brickSize) {
  if (!Array.isArray(klines) || klines.length < 5 || !(brickSize > 0)) return [];
  const bricks = [];
  let refPrice = Number(klines[0][4]);
  if (!Number.isFinite(refPrice) || refPrice <= 0) return [];
  for (const c of klines) {
    const h = Number(c[2]), l = Number(c[3]);
    if (!Number.isFinite(h) || !Number.isFinite(l)) continue;
    while (h >= refPrice + brickSize) { refPrice += brickSize; bricks.push({ open:refPrice-brickSize, close:refPrice, bull:true }); if (bricks.length > 200) break; }
    while (l <= refPrice - brickSize) { refPrice -= brickSize; bricks.push({ open:refPrice+brickSize, close:refPrice, bull:false }); if (bricks.length > 200) break; }
    if (bricks.length > 200) break;
  }
  return bricks.slice(-20);
}

function renkoSignal(klines, lastPrice, atrVal) {
  if (!Array.isArray(klines) || klines.length < 10 || !atrVal || !lastPrice)
    return { signal:'NEUTRAL', spikeTrap:false, ranging:false, trend:'FLAT', brickCount:0, consecutive:0 };
  const brickSize = Math.max(Number(atrVal) * 0.8, Number(lastPrice) * 0.003);
  const bricks = buildRenko(klines, brickSize);
  if (bricks.length < 3) return { signal:'NEUTRAL', spikeTrap:false, ranging:false, trend:'FLAT', brickCount:bricks.length, consecutive:0 };
  const last = bricks.slice(-6);
  const bulls = last.filter(b => b.bull).length, bears = last.filter(b => !b.bull).length;
  let consecutive = 1;
  for (let i = bricks.length - 2; i >= 0; i--) {
    if (bricks[i].bull === bricks[bricks.length-1].bull) consecutive++;
    else break;
    if (consecutive >= 6) break;
  }
  const spikeTrap = bricks.length >= 3 && (
    (bricks.at(-1).bull && !bricks.at(-2).bull && (bricks.at(-3)?.bull ?? true)) ||
    (!bricks.at(-1).bull && bricks.at(-2).bull && !(bricks.at(-3)?.bull ?? false))
  );
  let alternations = 0;
  for (let i = 1; i < last.length; i++) if (last[i].bull !== last[i-1].bull) alternations++;
  const ranging = alternations >= 4;
  const trend = bulls >= 4 ? 'STRONG_UP' : bulls >= 3 ? 'UP' : bears >= 4 ? 'STRONG_DOWN' : bears >= 3 ? 'DOWN' : ranging ? 'RANGING' : 'FLAT';
  const signal = trend === 'STRONG_UP' ? 'STRONG_BULL' : trend === 'UP' ? 'BULL' : trend === 'STRONG_DOWN' ? 'STRONG_BEAR' : trend === 'DOWN' ? 'BEAR' : ranging ? 'RANGING' : 'NEUTRAL';
  return { signal, spikeTrap, ranging, trend, brickCount:bricks.length, consecutive, bulls, bears, brickSize:+brickSize.toFixed(8) };
}

function r118AnalyzeCandlePlaybook(klines, dir='LONG', level=null) {
  const out = { ok:false, dir, score:0, strong:false, names:[], trNames:[], reject:false, rejectReason:'', ozet:'formasyon yok' };
  const rows = (Array.isArray(klines) ? klines : []).slice(-14).map(k => ({
    t:+k[0], o:+k[1], h:+k[2], l:+k[3], c:+k[4], v:+k[5]
  })).filter(k => [k.o,k.h,k.l,k.c].every(Number.isFinite) && k.h > 0 && k.l > 0 && k.c > 0);
  if (rows.length < 5) return out;
  const R = (v,d=3) => +Number(v||0).toFixed(d);
  const pctAbs = (a, b) => b > 0 ? Math.abs(a - b) / b * 100 : 999;
  function st(k) {
    const range = Math.max(k.h-k.l, 1e-12);
    const body = Math.abs(k.c-k.o);
    const bodyLow = Math.min(k.o,k.c), bodyHigh = Math.max(k.o,k.c);
    const upper = Math.max(0, k.h-bodyHigh);
    const lower = Math.max(0, bodyLow-k.l);
    return { range, body, bodyLow, bodyHigh, upper, lower,
      bull:k.c>k.o, bear:k.c<k.o, bodyPct:body/range, upperPct:upper/range, lowerPct:lower/range, closePos:(k.c-k.l)/range };
  }
  const a = rows.map(st);
  const n = rows.length;
  const k0 = rows[n-1], k1 = rows[n-2], k2 = rows[n-3], k3 = rows[n-4];
  const s0 = a[n-1], s1 = a[n-2], s2 = a[n-3], s3 = a[n-4];
  const avgBody = a.slice(-10,-1).reduce((x,y)=>x+y.body,0) / Math.max(1, a.slice(-10,-1).length);
  const avgRange = a.slice(-10,-1).reduce((x,y)=>x+y.range,0) / Math.max(1, a.slice(-10,-1).length);
  const avgVol = rows.slice(-11,-1).reduce((x,y)=>x+Number(y.v||0),0) / Math.max(1, rows.slice(-11,-1).length);
  const volBoost = Number(k0.v||0) > avgVol * 1.25;
  const bigBody0 = s0.body >= avgBody * 0.85 || s0.bodyPct >= 0.48;
  const impulse0 = s0.body >= avgBody * 1.10 || s0.range >= avgRange * 1.15;
  const zH = Number(level?.zoneHigh || level?.price || 0);
  const zL = Number(level?.zoneLow || level?.price || 0);
  const levelPrice = Number(level?.price || 0);
  const touchedLongZone = !levelPrice || !zL || rows.slice(-4).some(k => k.l <= zH && k.c >= zL*0.998);
  const touchedShortZone = !levelPrice || !zH || rows.slice(-4).some(k => k.h >= zL && k.c <= zH*1.002);
  const add = (name, score, why='') => { out.names.push(name); out.score += score; };

  const oppositeBear = (
    (s1.bull && s0.bear && k0.o >= k1.c*0.999 && k0.c <= k1.o*1.001 && s0.body >= s1.body*0.95 && s0.closePos <= 0.40) ||
    (s0.upperPct >= 0.55 && s0.closePos <= 0.45 && s0.bodyPct <= 0.38) ||
    (s2.bull && s1.bodyPct <= 0.34 && s0.bear && k0.c <= k2.o + (k2.c-k2.o)*0.50)
  );
  const oppositeBull = (
    (s1.bear && s0.bull && k0.o <= k1.c*1.001 && k0.c >= k1.o*0.999 && s0.body >= s1.body*0.95 && s0.closePos >= 0.60) ||
    (s0.lowerPct >= 0.55 && s0.closePos >= 0.55 && s0.bodyPct <= 0.38) ||
    (s2.bear && s1.bodyPct <= 0.34 && s0.bull && k0.c >= k2.o + (k2.c-k2.o)*0.50)
  );

  if (dir === 'LONG') {
    if (!touchedLongZone) { out.reject = true; out.rejectReason = 'HTF SSL/demand bölgesi mumla temas etmedi'; return out; }
    const bullEngulf = s1.bear && s0.bull && k0.o <= k1.c*1.001 && k0.c >= k1.o*0.999 && s0.body >= s1.body*0.95 && s0.closePos >= 0.60;
    const piercing = s1.bear && s1.bodyPct >= 0.42 && s0.bull && k0.c > (k1.o+k1.c)/2 && k0.c < k1.o*1.006 && s0.closePos >= 0.58;
    const morningStar = s2.bear && s2.bodyPct >= 0.42 && s1.bodyPct <= 0.36 && s0.bull && k0.c >= k2.o + (k2.c-k2.o)*0.50;
    const hammer = s0.lowerPct >= 0.48 && s0.upperPct <= 0.25 && s0.closePos >= 0.58 && s0.bodyPct <= 0.45;
    const hammerConfirm = s1.lowerPct >= 0.48 && s1.bodyPct <= 0.45 && s0.bull && k0.c > Math.max(k1.o,k1.c);
    const dragonfly = s0.lowerPct >= 0.62 && s0.bodyPct <= 0.20 && s0.closePos >= 0.62;
    const tweezerBottom = pctAbs(k0.l, k1.l) <= 0.16 && (s0.bull || k0.c > Math.max(k1.o,k1.c)) && s1.lowerPct >= 0.25;
    const threeOutsideUp = s2.bear && s1.bull && s1.body >= s2.body*0.85 && k1.c >= k2.o*0.999 && s0.bull && k0.c > k1.c;
    const closeMicroHigh = k0.c > Math.max(k1.h, k2.h) * 0.999 && s0.closePos >= 0.55;
    const strongBullBody = s0.bull && bigBody0 && s0.closePos >= 0.66 && (volBoost || impulse0);

    if (bullEngulf) add('BullEngulfingConfirmed', 5);
    if (piercing) add('PiercingLineConfirmed', 4);
    if (morningStar) add('MorningStarConfirmed', 5);
    if (hammer || hammerConfirm) add(hammerConfirm ? 'HammerConfirmed' : 'HammerReclaim', hammerConfirm ? 4 : 3);
    if (dragonfly) add('DragonflyReclaim', 4);
    if (tweezerBottom) add('TweezerBottomConfirmed', 3);
    if (threeOutsideUp) add('ThreeOutsideUpConfirmed', 4);
    if (strongBullBody && closeMicroHigh) add('BullImpulseReclaim', 3);
    if (oppositeBear && out.score < 6) { out.reject = true; out.rejectReason = 'karşı bearish mum baskın'; }
  } else {
    if (!touchedShortZone) { out.reject = true; out.rejectReason = 'HTF BSL/supply bölgesi mumla temas etmedi'; return out; }
    const bearEngulf = s1.bull && s0.bear && k0.o >= k1.c*0.999 && k0.c <= k1.o*1.001 && s0.body >= s1.body*0.95 && s0.closePos <= 0.40;
    const darkCloud = s1.bull && s1.bodyPct >= 0.42 && s0.bear && k0.c < (k1.o+k1.c)/2 && k0.c > k1.o*0.994 && s0.closePos <= 0.42;
    const eveningStar = s2.bull && s2.bodyPct >= 0.42 && s1.bodyPct <= 0.36 && s0.bear && k0.c <= k2.o + (k2.c-k2.o)*0.50;
    const shooting = s0.upperPct >= 0.48 && s0.lowerPct <= 0.25 && s0.closePos <= 0.42 && s0.bodyPct <= 0.45;
    const shootingConfirm = s1.upperPct >= 0.48 && s1.bodyPct <= 0.45 && s0.bear && k0.c < Math.min(k1.o,k1.c);
    const gravestone = s0.upperPct >= 0.62 && s0.bodyPct <= 0.20 && s0.closePos <= 0.38;
    const tweezerTop = pctAbs(k0.h, k1.h) <= 0.16 && (s0.bear || k0.c < Math.min(k1.o,k1.c)) && s1.upperPct >= 0.25;
    const threeOutsideDown = s2.bull && s1.bear && s1.body >= s2.body*0.85 && k1.c <= k2.o*1.001 && s0.bear && k0.c < k1.c;
    const closeMicroLow = k0.c < Math.min(k1.l, k2.l) * 1.001 && s0.closePos <= 0.45;
    const strongBearBody = s0.bear && bigBody0 && s0.closePos <= 0.34 && (volBoost || impulse0);

    if (bearEngulf) add('BearEngulfingConfirmed', 5);
    if (darkCloud) add('DarkCloudConfirmed', 4);
    if (eveningStar) add('EveningStarConfirmed', 5);
    if (shooting || shootingConfirm) add(shootingConfirm ? 'ShootingStarConfirmed' : 'ShootingStarReclaim', shootingConfirm ? 4 : 3);
    if (gravestone) add('GravestoneReclaim', 4);
    if (tweezerTop) add('TweezerTopConfirmed', 3);
    if (threeOutsideDown) add('ThreeOutsideDownConfirmed', 4);
    if (strongBearBody && closeMicroLow) add('BearImpulseReclaim', 3);
    if (oppositeBull && out.score < 6) { out.reject = true; out.rejectReason = 'karşı bullish mum baskın'; }
  }

  if (volBoost) out.score += 1;
  if (impulse0) out.score += 1;
  out.score = Math.min(12, out.score);
  out.strong = out.score >= 8;
  out.ok = out.score >= 6 && !out.reject;
  out.trNames = out.names.map(n => trPatternName(n));
  out.ozet = out.ok
    ? `${dir} mum teyidi: ${out.trNames.slice(0,3).join(' + ')} · puan ${out.score}/12 · vol:${volBoost?'VAR':'yok'} impuls:${impulse0?'VAR':'yok'}`
    : `${dir} mum teyidi zayıf: ${out.trNames.slice(0,3).join(' + ') || 'net formasyon yok'} · puan ${out.score}/12${out.rejectReason ? ' · '+out.rejectReason : ''}`;
  return out;
}

function trPatternName(name='') {
  const n = String(name || '');
  const map = {
    Hammer:'Çekiç', HangingMan:'Asılı adam', ShootingStar:'Kayan yıldız', InvHammer:'Ters çekiç',
    Doji:'Doji', DragonflyDoji:'Ejder doji', GravestoneDoji:'Mezar taşı doji',
    BullMarubozu:'Güçlü yeşil gövde', BearMarubozu:'Güçlü kırmızı gövde',
    BullEngulfing:'Yükseliş yutan mum', BearEngulfing:'Düşüş yutan mum',
    BullHarami:'Yükseliş harami', BearHarami:'Düşüş harami', BullHaramiCross:'Yükseliş harami doji', BearHaramiCross:'Düşüş harami doji',
    TweezerBottom:'Cımbız dip', TweezerTop:'Cımbız tepe', BullKicking:'Yükseliş tekmesi', BearKicking:'Düşüş tekmesi',
    DarkCloudCover:'Kara bulut', PiercingLine:'Delici çizgi', MorningStar:'Sabah yıldızı', EveningStar:'Akşam yıldızı',
    MorningDojiStar:'Sabah doji yıldızı', EveningDojiStar:'Akşam doji yıldızı',
    ThreeWhiteSoldiers:'Üç beyaz asker', ThreeBlackCrows:'Üç siyah karga',
    ThreeOutsideUp:'Dıştan yükseliş dönüşü', ThreeOutsideDown:'Dıştan düşüş dönüşü',
    ThreeInsideUp:'İçten yükseliş dönüşü', ThreeInsideDown:'İçten düşüş dönüşü',
    AbandonedBabyBull:'Terk edilmiş bebek yükseliş', AbandonedBabyBear:'Terk edilmiş bebek düşüş',
    DoubleBottom:'İkili dip', DoubleBottomForming:'İkili dip oluşuyor', DoubleTop:'İkili tepe', DoubleTopForming:'İkili tepe oluşuyor',
    InvHeadShoulders:'Ters omuz baş omuz', InvHSForming:'Ters OBO oluşuyor', HeadShoulders:'Omuz baş omuz', HSForming:'OBO oluşuyor',
    BullFlag:'Yükseliş bayrağı', BearFlag:'Düşüş bayrağı',
    BullGartley:'Yükseliş Gartley', BearGartley:'Düşüş Gartley', BullButterfly:'Yükseliş kelebek', BearButterfly:'Düşüş kelebek',
    BullBat:'Yükseliş yarasa', BearBat:'Düşüş yarasa', BullCrab:'Yükseliş yengeç', BearCrab:'Düşüş yengeç',
    BullShark:'Yükseliş köpekbalığı', BearShark:'Düşüş köpekbalığı'
  };
  return map[n] || n.replace(/Bull/g,'Yükseliş ').replace(/Bear/g,'Düşüş ');
}
function trPatternList(arr) {
  return (Array.isArray(arr) ? arr : []).map(x => trPatternName(x?.name || x)).filter(Boolean).slice(0,3).join(' + ');
}

function r128SafeForJson(obj) {
  const seen = new WeakSet();
  try {
    return JSON.parse(JSON.stringify(obj, (key, value) => {
      if (key === 'sideDecisions') return undefined;
      if (typeof value === 'bigint') return Number(value);
      if (typeof value === 'function') return undefined;
      if (value && typeof value === 'object') {
        if (seen.has(value)) return undefined;
        seen.add(value);
      }
      return value;
    }));
  } catch (e) {
    return null;
  }
}
function r128SideMini(d={}) {
  return {
    pass: !!d?.pass,
    autoOk: !!d?.autoOk,
    tier: d?.tier || 'WAIT',
    score: Number(d?.score || 0),
    priorityScore: Number(d?.priorityScore || 0),
    brainAction: d?.brainAction || '',
    brainMode: d?.brainMode || '',
    brainConfidence: Number(d?.brainConfidence || 0),
    entryPermissionReason: d?.entryPermissionReason || '',
    reason: String(d?.brainSummary || d?.reason || '').slice(0,260)
  };
}
function r128SideDecisionSummary(longDecision, shortDecision) {
  return { LONG: r128SideMini(longDecision), SHORT: r128SideMini(shortDecision) };
}

function r120BrainNum(v, d=0) { const n=Number(v); return Number.isFinite(n) ? n : d; }
function r120Bool(v) { return !!v; }
function r120BrainModeLabel(mode='') {
  const m = String(mode || 'WAIT');
  const map = {
    HTF_REVERSAL:'HTF ters-köşe',
    HTF_RECLAIM:'HTF likidite reclaim',
    SQUEEZE_BREAKOUT:'Sıkışma patlaması',
    TREND_CONTINUATION:'Trend devamı',
    COUNTER_TRAP:'Tuzak dönüşü',
    MOMENTUM_SCALP:'5m momentum scalp',
    FLOW_SCALP:'5m akış scalp',
    NO_EDGE:'Edge yok',
    WAIT:'Bekle'
  };
  return map[m] || m;
}
function r120BrainSensorSummary(d={}) {
  const bits = [];
  const htf = [];
  if (d.r110NearSSL) htf.push(`SSL ${r119FmtLevel(d.r110NearSSL)}`);
  if (d.r110NearBSL) htf.push(`BSL ${r119FmtLevel(d.r110NearBSL)}`);
  if (d.r116CounterLevel) htf.push(`karşı ${r119FmtLevel(d.r116CounterLevel)}`);
  if (d.r117TrapLevel) htf.push(`ters ${r119FmtLevel(d.r117TrapLevel)}`);
  if (htf.length) bits.push(`HTF:${htf.slice(0,2).join(' | ')}`);
  const candle = String(d.r118CandleOzet || '').replace(/R\d+\s*/g,'').slice(0,120);
  if (candle) bits.push(`Mum:${candle}`);
  const flow = [];
  const cvdFlowFlag = !!(d.cvdBridgePass || d.r53CvdSmartSafe || d.r45CvdAlternativeOk);
  const r129AltLiveFlow = !!(d.r125Flow?.liveReady && d.r125Flow?.bestSide && d.r125Flow.bestSide !== 'NEUTRAL');
  if (cvdFlowFlag && !d.cvdMissing) flow.push('CVD/flow uygun');
  else if (d.cvdMissing && (d.r42FlowGate || d.r111ObBaskisi || d.r45CvdAlternativeOk || d.r117FlowOk || r129AltLiveFlow)) flow.push(r129AltLiveFlow ? 'CVD yok ama canlı orderflow var' : 'CVD yok, alternatif akış var');
  else if (d.r125Flow?.flowWarmup) flow.push('orderflow ısınıyor');
  if (d.r111OiChgPct !== undefined) flow.push(`OI:${r120BrainNum(d.r111OiChgPct).toFixed(2)}%`);
  if (d.r111ShortSqueeze) flow.push('short squeeze yakıtı');
  if (d.r111LongSqueeze) flow.push('long squeeze yakıtı');
  if (d.r42FlowGate) flow.push('akış kapısı temiz');
  if (d.r125Flow?.ok) flow.push(`R125 ${d.r125Flow.bestSide} L${d.r125Flow.longEdge}/S${d.r125Flow.shortEdge}`);
  if (d.r125Flow?.r126?.summary) flow.push(`R126 ${String(d.r125Flow.r126.summary).slice(0,90)}`);
  if (d.r190Edge?.summary) flow.push(String(d.r190Edge.summary).slice(0,120));
  if (flow.length) bits.push(`Akış:${flow.slice(0,6).join(', ')}`);
  if (d.r93PiyasaEtiketi) bits.push(`Zemin:${d.r93PiyasaEtiketi}`);
  return bits.join(' · ');
}
function r120SingleBrainDecision(side, raw={}, sideScore=0, minAutoScore=72) {
  const d = raw && typeof raw === 'object' ? {...raw} : { pass:false, tier:'WAIT', score:0 };
  d.side = side;
  const score = r120BrainNum(sideScore || d.score, 0);
  const minScore = r120BrainNum(minAutoScore, 72);
  const r47 = r120BrainNum(d.r47Readiness, 0);
  const flowPts = r120BrainNum(d.r47FlowPts, 0);
  const timingPts = r120BrainNum(d.r47TimingPts, 0);
  const contextPts = r120BrainNum(d.r47ContextPts, 0);
  const structurePts = r120BrainNum(d.r47StructurePts, 0);
  const priority = r120BrainNum(d.priorityScore || d.r50EffectivePriority || d.r92Terazi, 0);
  const micro = r120BrainNum(d.r88MikroSkor, 0);
  const teyit = r120BrainNum(d.r88AkisTeyidiSayisi, 0);
  const r274Signal = d.r274Signal || {};
  const r274EntryOk = r120Bool(r274Signal.entryOk);
  const r274WatchOk = r120Bool(r274Signal.watchOk);
  const r274Score = r120BrainNum(r274Signal.score, 0);

  const r125SideFlow = r125FlowForSide(d.r125Flow, side);
  const flowOk = r120Bool(
    flowPts >= 1 || d.r42FlowGate || d.cvdBridgePass || d.r53CvdSmartSafe ||
    d.r45CvdAlternativeOk || d.r117FlowOk || d.r111ObBaskisi || d.microConfirm || r125SideFlow.ok || d.r190Edge?.earlyContinuation ||
    (r274EntryOk && !r274Signal.oneZoneRisk && !r274Signal.farChase)
  );
  const liveFlowOk = r120Bool(
    (flowOk && !d.cvdMissing) || d.r117FlowOk || d.r111ObBaskisi ||
    Math.abs(r120BrainNum(d.r111OiChgPct, 0)) >= 0.35 || d.r42FlowGate || d.r45CvdAlternativeOk || r125SideFlow.ok || d.r190Edge?.earlyContinuation || (d.r190Edge?.momentumWindow && d.r190Edge?.score >= 12) ||
    (r274EntryOk && (r125SideFlow.ok || r125SideFlow.strong || Math.abs(r120BrainNum(d.r125LiveDeltaPct,0)) >= 18 || !d.cvdMissing))
  );
  const timingOk = r120Bool(
    timingPts >= 1 || d.r117TrapSweepTaken || d.r117BodyReclaimOk || d.r118CandleOk ||
    d.r111SiksmaBreakout || d.fresh5mImpulseOrRecent || d.r37EarlyOk || d.directSweepOk || d.r190Edge?.earlyContinuation ||
    r274EntryOk || (r274WatchOk && r274Signal.zoneOk)
  );
  const contextOk = r120Bool(contextPts >= 2 || d.r93PiyasaIslemYapilabilir || d.r93DalgaliAmaIslemYapilabilir || !d.r88PiyasaBozuk);
  const r125OpposingFlow = r120Bool(r125SideFlow.against >= r125SideFlow.edge + 6 && r125SideFlow.against >= 8);
  const toxicFlow = r120Bool(d.cvdToxic || d.deltaToxic || d.r22LiqWaterfall?.adverse || d.poorLiquidity || r125OpposingFlow || d.r190Edge?.spreadBlock || (d.r190Edge?.lateTrapRisk && !d.r190Edge?.squeeze));

  const htfReverse = r120Bool(d.r117HtfReverseOk && (d.r117PrecisionCandleOk || d.r118CandleOk) && (d.r117FlowOk || flowOk));
  const htfReclaim = r120Bool((d.r110IctKoprusuOk || d.r110EntryOk) && (d.r110ChoCH || d.r110YonUyumlu || d.r117MssOk || d.r117BodyReclaimOk));
  const squeeze = r120Bool(d.r111KoprusuOk && (d.r111SiksmaBreakout || r120BrainNum(d.r111SqueezeSkor,0) >= 3 || d.r111ShortSqueeze || d.r111LongSqueeze));
  const trend = r120Bool(
    d.r93MerdivenDevamOk && teyit >= 4 && r47 >= 7 && micro >= 8 &&
    contextOk && liveFlowOk && !d.cvdMissing && !d.r116HtfGuardBlock && !d.r114TrapBlock
  );
  const counterTrap = r120Bool(
    (d.r93DonusRadariOk || d.r62CounterTrendTrapBridgeOk || d.r65ScalperCoreCounterTrapOk || d.wickTrapFlip?.favorable) &&
    (d.r118CandleOk || d.r117BodyReclaimOk || d.r117MssOk || d.r42TrapReclaimOk || timingOk) &&
    liveFlowOk
  );
  const momentumScalp = r120Bool(
    (d.r74Top10ProScalperOk || d.r68EntryEventOk || d.r67ScalperCoreHuntEntryOk || d.r37EarlyOk || d.fresh5mImpulseOrRecent || d.r190Edge?.earlyContinuation) &&
    r47 >= 5 && (liveFlowOk || priority >= 70) && timingOk && !d.r116HtfGuardBlock && !d.r114TrapBlock
  );
  const flowScalp = r120Bool(
    ((r47 >= 6 && micro >= 6 && teyit >= 2) || r125SideFlow.strong) && liveFlowOk && timingOk && contextOk &&
    !d.r116HtfGuardBlock && !d.r114TrapBlock
  );

  const htfOpposite = r120Bool(d.r116HtfGuardBlock && !htfReverse && !htfReclaim);
  const ictText = String(d.ictDashboard || d.r110Phase || '') + ' ' + String(d.siksmaOzet || '');
  const counterLevel = side === 'LONG' ? d.r110NearBSL : d.r110NearSSL;
  const counterDist = r120BrainNum(counterLevel?.dist ?? counterLevel?.distPct, 999);
  const htfCounterWait = r120Bool(side === 'LONG'
    ? (/BSL_ALINDI_CHOCH_BEKLENIYOR|BSL\s*wick\+body-reclaim|SUPPLY_OB|supply/i.test(ictText) && counterDist <= 0.95 && !d.r116AcceptedCounterBreak)
    : (/SSL_ALINDI_CHOCH_BEKLENIYOR|SSL\s*wick\+body-reclaim|DEMAND_OB|demand/i.test(ictText) && counterDist <= 0.95 && !d.r116AcceptedCounterBreak)
  );
  const r146R116CounterDist = r120BrainNum(d.r116CounterLevel?.dist ?? d.r116CounterLevel?.distPct, 999);
  const r146CounterDist = Math.min(counterDist, r146R116CounterDist);
  const r146CandleText0 = String(d.r118CandleOzet || '');
  const r146CandleScore0 = r120BrainNum(d.r118Candle?.score, 0);
  const r146No5mPattern = r120Bool(/formasyon yok/i.test(r146CandleText0) || (!d.r118CandleOk && r146CandleScore0 <= 2));
  const r146HtfCounterNear = r120Bool(r146CounterDist <= 0.70 || (d.r116HtfGuardBlock && !d.r117HtfReverseOk));
  const r146CounterWallPressure = r120Bool(r146HtfCounterNear && !d.r116AcceptedCounterBreak && !d.r117HtfReverseOk && !d.r117BodyReclaimOk);
  const r146LateWallNoPattern = r120Bool(r146CounterWallPressure && r146No5mPattern && !squeeze && !htfReclaim);
  const r134LegacySensorVeto = r120Bool(d.r68CriticalHardBlock || d.r65ScalperCoreHardVeto || d.r66WyckoffHardVeto);
  const fatalDanger = r120Bool(
    d.hardVeto || d.r114TrapBlock ||
    (htfOpposite && r120BrainNum(d.r116CounterLevel?.dist ?? d.r116CounterLevel?.distPct, 999) > 0.5) ||
    d.poorLiquidity || d.atrExtremeBlock || d.signalDecayAutoBlock ||
    (side === 'LONG' ? d.r41FallingKnifeBlock : d.r41RisingKnifeBlock)
  );
  const hardDanger = r120Bool(
    fatalDanger || toxicFlow ||
    (d.r88PiyasaBozuk && !d.r93DalgaliAmaIslemYapilabilir && !counterTrap && !htfReverse && !momentumScalp && !flowScalp)
  );

  const candidates = [];
  const add = (mode, ok, base) => { if (ok) candidates.push({mode, base}); };
  add('HTF_REVERSAL', htfReverse, r126PlaybookBase('HTF_REVERSAL', 88));
  add('HTF_RECLAIM', htfReclaim, r126PlaybookBase('HTF_RECLAIM', 82));
  add('SQUEEZE_BREAKOUT', squeeze, r126PlaybookBase('SQUEEZE_BREAKOUT', 78));
  add('TREND_CONTINUATION', trend, r126PlaybookBase('TREND_CONTINUATION', 72));
  add('COUNTER_TRAP', counterTrap, r126PlaybookBase('COUNTER_TRAP', 74));
  add('MOMENTUM_SCALP', momentumScalp, r126PlaybookBase('MOMENTUM_SCALP', 68));
  add('FLOW_SCALP', flowScalp, r126PlaybookBase('FLOW_SCALP', 64));
  add('C20_L20_RSI_RATIO_FVG', r274EntryOk && (liveFlowOk || priority >= 58 || r274Score >= 34), r126PlaybookBase('C20_L20_RSI_RATIO_FVG', 76));
  candidates.sort((a,b)=>b.base-a.base);
  const primaryMode = candidates[0]?.mode || 'NO_EDGE';
  const playbookActive = primaryMode !== 'NO_EDGE';

  const rawEdge = Math.max(0, Math.min(100, Math.round(
    score*0.32 + priority*0.25 + r47*2.2 + flowPts*5 + timingPts*4 + contextPts*2 + structurePts*2 +
    (htfReverse?20:0) + (htfReclaim?16:0) + (squeeze?15:0) + (trend?12:0) +
    (counterTrap?13:0) + (momentumScalp?11:0) + (flowScalp?9:0) + Math.min(8, r125SideFlow.edge) +
    (d.r125Flow?.r126?.bidAbsorb && side==='LONG' ? 7 : 0) + (d.r125Flow?.r126?.askAbsorb && side==='SHORT' ? 7 : 0) +
    (d.r125Flow?.r126?.forecast?.side === side ? Math.min(4, Math.round(r120BrainNum(d.r125Flow?.r126?.forecast?.confidence,0)/25)) : 0) +
    (d.r125Flow?.r126?.aggressionTrend?.side === side && d.r125Flow?.r126?.aggressionTrend?.phase === 'ACCELERATING' ? Math.min(5, Math.round(r120BrainNum(d.r125Flow?.r126?.aggressionTrend?.strength,0)/18)) : 0) +
    (d.r125Flow?.r126?.deltaImprint?.coiled && (squeeze || flowScalp) ? 3 : 0) + r126PlaybookAdj(primaryMode) +
    (d.r140Phase?.phase==='EXPANSION' ? (side==='LONG' ? 8 : -5)
      : d.r140Phase?.phase==='EXPANSION_DOWN' ? (side==='SHORT' ? 8 : -5)
      : d.r140Phase?.phase==='DISTRIBUTION' ? (side==='LONG' ? -15 : 10)
      : d.r140Phase?.phase==='ACCUMULATION' ? 4 : 0) +
    (side==='LONG' ? Math.min(20, Number(d.r140BtcDiv?.score||0))
      : (Number(d.r140BtcDiv?.score||0) > 0 ? -Math.min(10, Math.round(Number(d.r140BtcDiv?.score||0)/2)) : 0)) +
    (d.r140OiVel?.oiConfirmed ? (side==='LONG' ? 5 : -3)
      : d.r140OiVel?.fakePump ? (side==='LONG' ? -10 : 8) : 0) +
    (d.r140EqHL?.nearHighTrap ? (side==='LONG' ? -8 : 6)
      : d.r140EqHL?.nearLowTrap ? (side==='SHORT' ? -8 : 6) : 0) +
    (d.r140Rvol?.signal==='VERY_HIGH' ? 4 : d.r140Rvol?.signal==='HIGH' ? 2
      : d.r140Rvol?.signal==='VERY_LOW' ? -4 : d.r140Rvol?.signal==='LOW' ? -2 : 0) +
    Math.max(-18, Math.min(18, r120BrainNum(d.r190Edge?.score,0))) +
    (r274EntryOk ? Math.min(20, Math.max(10, Math.round(r274Score/2))) : r274WatchOk ? 6 : 0) -
    (r274Signal.oneZoneRisk ? 12 : 0) - (r274Signal.farChase ? 10 : 0) +
    (r146LateWallNoPattern ? -14 : r146CounterWallPressure ? -8 : 0) -
    (hardDanger?55:0) - (d.cvdMissing?5:0) - (r125OpposingFlow?12:0)
  )));
  const r142Cal = r142CalibrateEdge(side, d, primaryMode, rawEdge, score);
  let edge = r142Cal.calibratedEdge;

  const adaptiveFloorBase = playbookActive
    ? (['HTF_REVERSAL','HTF_RECLAIM','SQUEEZE_BREAKOUT'].includes(primaryMode)
        ? Math.max(40, minScore - 32)
        : primaryMode === 'TREND_CONTINUATION'
          ? Math.max(52, minScore - 18)
          : Math.max(50, minScore - 20))
    : Math.max(44, minScore - 16);
  const adaptiveFloor = Math.max(40, adaptiveFloorBase - (d.r190Edge?.earlyContinuation ? 6 : (d.r190Edge?.momentumWindow ? 3 : 0)));
  const needsPremiumProof = ['TREND_CONTINUATION','MOMENTUM_SCALP','FLOW_SCALP'].includes(primaryMode);
  const r158DeltaOk = r120Bool(
    side === 'LONG' ? r120BrainNum(d.r125Flow?.deltaPct, 0) >= 15 : r120BrainNum(d.r125Flow?.deltaPct, 0) <= -15
  );
  const r158CounterTrapFlowOk = r120Bool(r125SideFlow.ok || r125SideFlow.strong || r158DeltaOk);
  const modeQualityBlock = r120Bool(
    (needsPremiumProof && htfCounterWait) ||
    (primaryMode === 'COUNTER_TRAP' && htfCounterWait && counterDist <= 1.0 && !d.r117HtfReverseOk) ||
    (primaryMode === 'COUNTER_TRAP' && !r158CounterTrapFlowOk && !d.r117HtfReverseOk && !htfReclaim) ||
    (primaryMode === 'TREND_CONTINUATION' && (!liveFlowOk || d.cvdMissing || teyit < 4 || r47 < 7)) ||
    (['MOMENTUM_SCALP','FLOW_SCALP'].includes(primaryMode) && (!liveFlowOk || r47 < 6))
  );
  const edgeNeed = primaryMode === 'TREND_CONTINUATION' ? 70 : primaryMode === 'FLOW_SCALP' ? 66 : primaryMode === 'MOMENTUM_SCALP' ? 68 : 60;
  const dataMinimum = r120Bool(playbookActive && (liveFlowOk || htfReverse || htfReclaim || squeeze || r125SideFlow.strong) && r47 >= (needsPremiumProof ? 6 : 4));
  const r133HtfSweepAligned = r120Bool(side === 'LONG'
    ? /SSL_ALINDI|SSL_SWEEP|HTF_SSL|SSL_.*BODY_RECLAIM/i.test(ictText)
    : /BSL_ALINDI|BSL_SWEEP|HTF_BSL|BSL_.*BODY_RECLAIM/i.test(ictText)
  );
  const r133LiveTradeCount = r120BrainNum(d.r125Flow?.trades, 0);
  const r133LiveDeltaPct = r120BrainNum(d.r125Flow?.deltaPct, 0);
  const r133LiveDeltaAbs = Math.abs(r133LiveDeltaPct);
  const r134LiveDeltaAligned = r120Bool(side === 'LONG' ? r133LiveDeltaPct >= 12 : r133LiveDeltaPct <= -12);
  const r134LiveDeltaAgainst = r120Bool(side === 'LONG' ? r133LiveDeltaPct <= -16 : r133LiveDeltaPct >= 16);
  const r134ForecastAligned = r120Bool(d.r125Flow?.r126?.forecast?.side === side && r120BrainNum(d.r125Flow?.r126?.forecast?.confidence,0) >= 60);
  const r134AggressionAligned = r120Bool(d.r125Flow?.r126?.aggressionTrend?.side === side && r120BrainNum(d.r125Flow?.r126?.aggressionTrend?.strength,0) >= 55);
  const r134FlowAligned = r120Bool(
    r125SideFlow.strong ||
    (r125SideFlow.ok && r133LiveTradeCount >= 20) ||
    (r134LiveDeltaAligned && r133LiveTradeCount >= 40) ||
    r134ForecastAligned || r134AggressionAligned
  );
  const r134OpposingLiveFlow = r120Bool(
    r125OpposingFlow ||
    (r134LiveDeltaAgainst && r133LiveTradeCount >= 35 && r125SideFlow.against >= Math.max(3, r125SideFlow.edge)) ||
    (d.r125Flow?.bestSide && d.r125Flow.bestSide !== 'NEUTRAL' && d.r125Flow.bestSide !== side && r125SideFlow.against >= 7 && r133LiveTradeCount >= 80)
  );
  const r134HtfOrCandleProof = r120Bool(
    r133HtfSweepAligned || d.r117BodyReclaimOk || d.r117MssOk || d.r118CandleOk ||
    (d.r118Candle?.score >= 4) || htfReclaim || htfReverse || squeeze
  );
  const r144CandleText = String(d.r118CandleOzet || '');
  const r144NoVolumeConfirm = /vol\s*:\s*yok/i.test(r144CandleText);
  const r144CounterVeryNear = r120Bool(counterDist <= 0.65 || (d.r116HtfGuardBlock && !d.r117HtfReverseOk));
  const r144PumpLateRisk = r120Bool(
    (side === 'LONG' && (d.r140Phase?.phase === 'DISTRIBUTION' || d.r140OiVel?.fakePump || d.r140EqHL?.nearHighTrap)) ||
    (side === 'SHORT' && d.r140EqHL?.nearLowTrap)
  );
  const r145NoVolCounterTrapRisk = r120Bool(
    r144NoVolumeConfirm && r144CounterVeryNear && !squeeze &&
    !(htfReverse && d.r118Candle?.score >= 12 && r134FlowAligned && !r144PumpLateRisk)
  );
  const r144FastNeedsRealProof = r120Bool(
    (r144NoVolumeConfirm && !htfReverse && !htfReclaim && !squeeze &&
      (r144CounterVeryNear || r144PumpLateRisk || d.r93PiyasaDalgali || d.r93PiyasaBozuk)) ||
    r145NoVolCounterTrapRisk || r146LateWallNoPattern
  );

  const r147ForecastAgainst = r120Bool(
    d.r125Flow?.r126?.forecast?.side && d.r125Flow.r126.forecast.side !== 'NEUTRAL' &&
    d.r125Flow.r126.forecast.side !== side &&
    r120BrainNum(d.r125Flow?.r126?.forecast?.confidence, 0) >= 55
  );
  const r147BookFlowAgainst = r120Bool(
    r125SideFlow.against >= Math.max(8, r125SideFlow.edge + 5) ||
    (d.r125Flow?.bestSide && d.r125Flow.bestSide !== 'NEUTRAL' && d.r125Flow.bestSide !== side && r125SideFlow.against >= 6)
  );
  const r147WrongWayCluster = r120Bool(
    (r134OpposingLiveFlow || r147ForecastAgainst || r147BookFlowAgainst) &&
    (d.r93PiyasaBozuk || d.r93PiyasaTehlikeli || r144PumpLateRisk || r144CounterVeryNear || r146CounterWallPressure)
  );
  const r147NoProofCounterTrap = r120Bool(
    ['COUNTER_TRAP','MOMENTUM_SCALP','FLOW_SCALP','TREND_CONTINUATION'].includes(primaryMode) &&
    !htfReverse && !htfReclaim && !squeeze &&
    r146No5mPattern && r146CounterDist <= 0.90 &&
    r147WrongWayCluster
  );
  const r147TrapGuardReason = r147NoProofCounterTrap
    ? `R147 ters-akış tuzak freni: 5m formasyon yok + HTF karşı seviye yakın + canlı akış/mum tahmini karşı; ${side} yerine ters yön/izleme gerekir`
    : '';

  const r148NearHighTrap = r120Bool(d.r140EqHL?.nearHighTrap || (side === 'LONG' && (r144CounterVeryNear || htfCounterWait)) || (side === 'SHORT' && r133HtfSweepAligned));
  const r148NearLowTrap  = r120Bool(d.r140EqHL?.nearLowTrap  || (side === 'SHORT' && (r144CounterVeryNear || htfCounterWait)) || (side === 'LONG' && r133HtfSweepAligned));
  const r148SideWithLive = r120Bool(r134FlowAligned || r134LiveDeltaAligned || r125SideFlow.strong || r134ForecastAligned || r134AggressionAligned);
  const r148SideAgainstLive = r120Bool(r134OpposingLiveFlow || r147BookFlowAgainst || r147ForecastAgainst);
  const r148LongTrapToShort = r120Bool(
    side === 'SHORT' &&
    (d.r140Phase?.phase === 'DISTRIBUTION' || d.r140OiVel?.fakePump || d.r140EqHL?.nearHighTrap || r146CounterWallPressure || r144CounterVeryNear) &&
    r148SideWithLive && !fatalDanger
  );
  const r148ShortTrapToLong = r120Bool(
    side === 'LONG' &&
    (d.r140EqHL?.nearLowTrap || d.r111ShortSqueeze || /SSL_ALINDI|SSL_SWEEP|HTF_SSL|DEMAND_OB|demand/i.test(ictText)) &&
    r148SideWithLive && !fatalDanger
  );
  const r148ReversalSideOk = r120Bool(r148LongTrapToShort || r148ShortTrapToLong);
  const r148WrongSideBlock = r120Bool(
    !htfReverse && !htfReclaim && !squeeze &&
    ((side === 'LONG' && (d.r140Phase?.phase === 'DISTRIBUTION' || d.r140OiVel?.fakePump || d.r140EqHL?.nearHighTrap || r146CounterWallPressure || r144CounterVeryNear) && r148SideAgainstLive) ||
     (side === 'SHORT' && (d.r140EqHL?.nearLowTrap || /SSL_ALINDI|SSL_SWEEP|HTF_SSL|DEMAND_OB|demand/i.test(ictText)) && r148SideAgainstLive))
  );
  const r148BalanceBonus = r148ReversalSideOk ? 12 : (r148SideWithLive && !r148SideAgainstLive ? 4 : 0);
  const r148WrongPenalty = r148WrongSideBlock ? 34 : (r148SideAgainstLive ? 10 : 0);
  if (r148BalanceBonus) edge = Math.min(100, edge + r148BalanceBonus);
  if (r148WrongPenalty) edge = Math.max(0, edge - r148WrongPenalty);
  const r148ScoreOk = r148ReversalSideOk ? score >= Math.max(42, minScore - 30) : score >= adaptiveFloor;
  const r148Note = r148WrongSideBlock
    ? `R148 eşit yön terazisi: ${side} canlı/HTF ters; önce ${r142SideOpp(side)}/izle`
    : r148ReversalSideOk
      ? `R148 eşit yön terazisi: ${side} tarafı trap-inversion fırsatı`
      : '';

  let r133FastScalpOverride = r120Bool(
    !fatalDanger && !r134OpposingLiveFlow && !r148WrongSideBlock && !htfCounterWait && !r144FastNeedsRealProof && r134HtfOrCandleProof && r134FlowAligned &&
    r133LiveTradeCount >= 12 && r133LiveDeltaAbs >= 12 &&
    score >= Math.max(50, minScore - 22) && edge >= 82 &&
    !(d.r116HtfGuardBlock && !d.r116AcceptedCounterBreak && !r133HtfSweepAligned)
  );
  const r144FastBlockReason = r144FastNeedsRealProof
    ? `${r146LateWallNoPattern ? 'R146 karşı-duvar freni: 5m formasyon yok + HTF karşı seviye yakın; live delta tek başına yeterli değil' : `R144 hızlı-edge freni: hacim teyidi yok${r144CounterVeryNear?' + HTF karşı seviye yakın':''}${r144PumpLateRisk?' + geç-pump/dağıtım izi':''}; gerçek HTF/squeeze/reclaim beklenir`}`
    : '';
  const r142FrequencySafeEdge = r120Bool(rawEdge >= edgeNeed + 8 && !r142Cal.late && !r142Cal.liveAgainst && r134FlowAligned && r134HtfOrCandleProof && !fatalDanger && !r147NoProofCounterTrap && !r148WrongSideBlock);
  const passEdge = r142FrequencySafeEdge ? Math.max(edge, edgeNeed) : edge;
  const r156RealHardBlock = r120Bool(
    d.poorLiquidity || d.atrExtremeBlock ||
    (side === 'LONG' ? d.r41FallingKnifeBlock : d.r41RisingKnifeBlock) ||
    d.hardVeto
  );
  const r156CalibrationOk = r120Bool(r142Cal.mem.same.n >= 2);
  const r156FastTop10Bypass = r120Bool(
    !r156RealHardBlock && !r148WrongSideBlock && !r147NoProofCounterTrap &&
    r156CalibrationOk &&
    r125SideFlow.edge >= 10 &&
    edge >= 55 &&
    score >= Math.max(45, minScore - 20) &&
    primaryMode !== 'NO_EDGE' &&
    r133LiveTradeCount >= 25
  );

  const ictTxt = String(d.r110ICT?.ictState || d.r110ICT?.dashboardText || '');

  const r283RealChartStructure = r120Bool(
    htfReverse || htfReclaim || squeeze ||
    d.r117TrapSweepTaken || d.r117BodyReclaimOk || d.r117MssOk ||
    d.r118CandleOk || d.r118CandleStrong ||
    d.r190Edge?.r194SwingBreak?.strong ||
    (d.r190Edge?.r194SwingBreak?.ok && d.r190Edge?.r192FuelOk) ||
    (side === 'LONG' ? d.r29?.r197?.long?.strong : d.r29?.r197?.short?.strong) ||
    (d.r110ICT?.oteZone?.inOte && String(d.r110ICT?.oteZone?.side||'').toUpperCase() === side) ||
    (side === 'LONG' ? d.r110ICT?.inBullishFVG : d.r110ICT?.inBearishFVG) ||
    (side === 'LONG'
      ? /SSL_ALINDI|HTF_SSL|SSL_SWEEP|SSL.*BODY_RECLAIM|bullish.*FVG|OTE/i.test(ictTxt)
      : /BSL_ALINDI|HTF_BSL|BSL_SWEEP|BSL.*BODY_RECLAIM|bearish.*FVG|OTE/i.test(ictTxt))
  );

  const r160Q1Structure = r120Bool(
    (side === 'LONG'
      ? /SSL_ALINDI|HTF_SSL|SSL_SWEEP|SSL.*BODY_RECLAIM/i.test(ictTxt)
      : /BSL_ALINDI|HTF_BSL|BSL_SWEEP|BSL.*BODY_RECLAIM/i.test(ictTxt)) ||
    (side === 'LONG'
      ? /HTF_SSL_SEVIYESINDE/i.test(ictTxt)
      : /HTF_BSL_SEVIYESINDE/i.test(ictTxt)) ||
    r120Bool(d.r117NearTrapHTF) ||
    r283RealChartStructure
  );

  const r160LiveDeltaOk = r120Bool(
    side === 'LONG'
      ? r120BrainNum(d.r125Flow?.deltaPct, 0) >= 15
      : r120BrainNum(d.r125Flow?.deltaPct, 0) <= -15
  );
  const r160FlowNotAgainst = r120Bool(r125SideFlow.against < r125SideFlow.edge + 5);

  const r160Phase = String(d.r140Phase?.phase || '').toUpperCase();
  const r160OiPos = r120Bool(d.r140OiVel && !d.r140OiVel.fakePump && r120BrainNum(d.r140OiVel?.velocity, 0) > 0);
  const r160RvolOk = r120Bool(r120BrainNum(d.r140Rvol?.ratio, 0) >= 0.45);
  const r160Q3Momentum = r120Bool(
    r160Phase === 'EXPANSION' ||
    r160Phase === 'ACCUMULATION' ||
    (r160Phase === 'TRANSITION' && r160OiPos) ||
    (r160RvolOk && r160OiPos) ||
    r160RvolOk
  );

  const r161DeltaConfirm = r120Bool(
    side === 'LONG'
      ? r120BrainNum(d.r125Flow?.deltaPct, 0) >= 10 || r125SideFlow.strong
      : r120BrainNum(d.r125Flow?.deltaPct, 0) <= -10 || r125SideFlow.strong
  );
  const r160ForecastOk = r120Bool(
    d.r125Flow?.r126?.forecast?.side === side ||
    (side === 'LONG' ? r120BrainNum(d.r125Flow?.r126?.forecast?.confidence, 0) >= 65 && r120BrainNum(d.r125Flow?.deltaPct, 0) >= 5
                     : r120BrainNum(d.r125Flow?.r126?.forecast?.confidence, 0) >= 65 && r120BrainNum(d.r125Flow?.deltaPct, 0) <= -5)
  );
  const r160Q4Proof = r120Bool(
    (r120Bool(d.r117TrapSweepTaken) && r161DeltaConfirm) ||
    (r120Bool(d.r118CandleOk) && (r120Bool(d.r114ReclaimOk) || r120Bool(d.r117BodyReclaimOk))) ||
    r120Bool(d.r118CandleStrong) ||
    (r120Bool(d.r117TrapSweepTaken) && r120Bool(d.r117BodyReclaimOk)) ||
    (r120Bool(d.r190Edge?.r192Footprint?.aligned) && r120Bool(d.r190Edge?.r192DeepOfi?.aligned) && r120BrainNum(d.r190Edge?.r192FuelScore,0) >= 5) ||
    r120Bool(d.r190Edge?.r194SwingBreak?.strong || (d.r190Edge?.r194SwingBreak?.ok && d.r190Edge?.r192FuelOk && r120BrainNum(d.r190Edge?.score,0) >= 28)) ||
    r120Bool((side === 'LONG' ? d.r29?.r197?.long?.strong : d.r29?.r197?.short?.strong)) ||
    (r274EntryOk && (r274Signal.zoneOk || r274Signal.holdOk || r274Score >= 32))
  );
  const r191ForecastOnlyProof = r120Bool(r160Q4Proof && !r191Hard5mProof);

  const r160Q2Flow = r120Bool(
    r160FlowNotAgainst && (
      r125SideFlow.strong ||
      (r125SideFlow.edge >= 5 && r161DeltaConfirm) ||
      (r125SideFlow.edge >= 8)
    )
  );

  const r160HardBlock = r120Bool(
    d.poorLiquidity ||
    d.atrExtremeBlock ||
    (side === 'LONG' ? d.r41FallingKnifeBlock : d.r41RisingKnifeBlock) ||
    (d.hardVeto && !d.r117HtfReverseOk) ||
    r148WrongSideBlock
  );

  const r160TrueCount = [r160Q1Structure, r160Q2Flow, r160Q3Momentum, r160Q4Proof].filter(Boolean).length;

  const r187R160ScoreFloor3of4 = Math.max(55, minScore - 15);
  const r187R160ScoreFloor4of4 = Math.max(48, minScore - 25);
  const r187R160HasRealStructure = r120Bool(
    r283RealChartStructure || htfReverse || htfReclaim || squeeze || d.r117BodyReclaimOk || d.r117MssOk || d.r118CandleOk || d.r118CandleStrong
  );

  let r188AccountCaution = false;
  try {
    const _r188Recent = r179AccountPerf()?.recent || {};
    r188AccountCaution = Number(_r188Recent.closed||0) >= 8 && (Number(_r188Recent.wr||0) < 0.45 || Number(_r188Recent.pf||0) < 0.75 || Number(_r188Recent.net||0) < 0);
  } catch(_r188Perf) {}
  const r188CandleText = String(d.mumOzet || d.htfTani || d.brainSummary || d.reason || '');
  const r188WeakCandle = r120Bool(/teyidi zayıf|formasyon yok|puan 0\/12|karşı .*mum baskın|mum onay yok/i.test(r188CandleText));
  const r188HtfCounterPressure = r120Bool(
    htfCounterWait || r144CounterVeryNear ||
    (d.r116HtfGuardBlock && !d.r117HtfReverseOk) ||
    (side === 'LONG' && (d.r140Phase?.phase === 'DISTRIBUTION' || d.r140OiVel?.fakePump || d.r140EqHL?.nearHighTrap)) ||
    (side === 'SHORT' && d.r140EqHL?.nearLowTrap)
  );
  const r189Book = d.r125Flow?.book || {};
  const r189MicroSide = String(r189Book.microSide || 'NEUTRAL').toUpperCase();
  const r189MicroPersistSide = String(r189Book.microPersistSide || 'NEUTRAL').toUpperCase();
  const r189VampBiasAbs = Math.abs(r120BrainNum(r189Book.vampBiasPct, 0));
  const r189MicroAligned = r120Bool(
    r189MicroSide === side &&
    (r189MicroPersistSide === side || r189VampBiasAbs >= Math.max(0.006, r120BrainNum(r189Book.spreadPct,0)*0.25))
  );
  const r189MicroAgainst = r120Bool(
    r189MicroSide === r142SideOpp(side) &&
    (r189MicroPersistSide === r142SideOpp(side) || r189VampBiasAbs >= Math.max(0.008, r120BrainNum(r189Book.spreadPct,0)*0.30)) &&
    !r125SideFlow.strong && !r160Q4Proof
  );
  const r189OiCrowdTrap = r120Bool(
    (side === 'LONG' && (d.r140OiVel?.fakePump || d.r140Phase?.phase === 'DISTRIBUTION') && !r160Q4Proof && !r189MicroAligned) ||
    (side === 'SHORT' && d.r140EqHL?.nearLowTrap && !r160Q4Proof && !r189MicroAligned)
  );

  const r188StrongNoQ4Exception = r120Bool(
    !r160Q4Proof && r160TrueCount >= 3 &&
    r125SideFlow.edge >= 11 && r133LiveTradeCount >= 35 &&
    r161DeltaConfirm && r160ForecastOk && (r189MicroAligned || d.r190Edge?.earlyContinuation) &&
    !r188WeakCandle && !r188HtfCounterPressure && !r148WrongSideBlock
  );
  const r188NoProofGuard = r120Bool(
    r160TrueCount === 3 && !r160Q4Proof && !r188StrongNoQ4Exception &&
    (d.r190Edge?.lateTrapRisk || r188AccountCaution || r188WeakCandle || r188HtfCounterPressure || !r187R160HasRealStructure || r189MicroAgainst || r189OiCrowdTrap)
  );

  const r187R160ScoreOk = r120Bool(
    r160TrueCount >= 4
      ? (score >= r187R160ScoreFloor4of4 || (score >= 42 && edge >= 82 && r187R160HasRealStructure && r161DeltaConfirm))
      : (score >= r187R160ScoreFloor3of4 && (r160Q3Momentum || r187R160HasRealStructure) && r161DeltaConfirm)
  );
  const r187R160LowScoreBlock = r120Bool(!r187R160ScoreOk && r160TrueCount >= 3);

  const r190bSqueezeBoost = !!(d?.r190Edge?.squeeze || d?.r190Edge?.earlyContinuation);
  const r190bEffEdge = r190bSqueezeBoost ? Math.max(28, 35 - 5) : 35;

  const r160TraderDecision = r120Bool(
    !r160HardBlock && !r188NoProofGuard && r187R160ScoreOk && edge >= r190bEffEdge && r133LiveTradeCount >= 10 && (
      (r160TrueCount >= 3 && r125SideFlow.edge >= 4) ||
      (r160TrueCount >= 4)
    )
  );

  const r159Points = (
    (r125SideFlow.edge >= 6 ? 2 : r125SideFlow.edge >= 3 ? 1 : 0) +
    (edge >= 60 ? 2 : edge >= 45 ? 1 : 0) +
    (r120BrainNum(d.r140Rvol?.ratio, 0) >= 0.6 ? 1 : 0) +
    (d.r140OiVel && !d.r140OiVel.fakePump && r120BrainNum(d.r140OiVel?.velocity,0) > 0 ? 1 : 0) +
    (r160ForecastOk ? 1 : 0) +
    (d.r190Edge?.earlyContinuation ? 2 : d.r190Edge?.momentumWindow ? 1 : 0) +
    (d.r190Edge?.vpinAligned ? 1 : 0) +
    (d.r190Edge?.squeeze ? 2 : 0) +
    (score >= minScore - 28 ? 1 : 0) +
    (primaryMode !== 'NO_EDGE' ? 1 : 0) +
    (r133LiveTradeCount >= 20 ? 1 : 0)
  );
  const r187R159ScoreFloor = Math.max(48, minScore - 22);
  const r187R159ScoreOk = r120Bool(score >= r187R159ScoreFloor || (score >= 42 && edge >= 82 && r187R160HasRealStructure && r161DeltaConfirm));
  const r188R159ProofOk = r120Bool(
    r160Q4Proof ||
    (d.r190Edge?.earlyContinuation && !d.r190Edge?.lateTrapRisk && r133LiveTradeCount >= 12) ||
    (r187R160HasRealStructure && r160ForecastOk && r161DeltaConfirm && r125SideFlow.edge >= 8 && r133LiveTradeCount >= 25 && !r188WeakCandle && !r188HtfCounterPressure && !r189MicroAgainst) ||
    (r189MicroAligned && r160ForecastOk && r161DeltaConfirm && r125SideFlow.edge >= 10 && r133LiveTradeCount >= 35 && !r188WeakCandle && !r188HtfCounterPressure)
  );
  const r159MomentumPass = r120Bool(
    r159Points >= 6 &&
    r187R159ScoreOk &&
    r188R159ProofOk &&
    !r156RealHardBlock &&
    !r148WrongSideBlock &&
    !d.r190Edge?.lateTrapRisk &&
    !d.r190Edge?.spreadBlock &&
    r125SideFlow.edge >= (d.r190Edge?.earlyContinuation ? 4 : 5) &&
    edge >= (d.r190Edge?.earlyContinuation ? 36 : 40) &&
    r133LiveTradeCount >= (d.r190Edge?.earlyContinuation ? 10 : 15)
  );

  const r191Hard5mProof = r120Bool(
    (r120Bool(d.r117TrapSweepTaken) && r161DeltaConfirm) ||
    (r120Bool(d.r118CandleOk) && (r120Bool(d.r114ReclaimOk) || r120Bool(d.r117BodyReclaimOk))) ||
    r120Bool(d.r118CandleStrong) ||
    (r120Bool(d.r117TrapSweepTaken) && r120Bool(d.r117BodyReclaimOk)) ||
    (r120Bool(d.r190Edge?.r192Footprint?.aligned) && r120Bool(d.r190Edge?.r192DeepOfi?.aligned) && r120BrainNum(d.r190Edge?.r192FuelScore,0) >= 5) ||
    r120Bool(d.r190Edge?.r194SwingBreak?.strong || (d.r190Edge?.r194SwingBreak?.ok && d.r190Edge?.r192FuelOk && r120BrainNum(d.r190Edge?.score,0) >= 28)) ||
    r120Bool((side === 'LONG' ? d.r29?.r197?.long?.strong : d.r29?.r197?.short?.strong)) ||
    (r274EntryOk && (r274Signal.zoneOk || r274Signal.holdOk || r274Score >= 32))
  );
  let r191PerfCaution = false;
  try {
    const _p = r179AccountPerf()?.recent || {};
    r191PerfCaution = Number(_p.closed||0) >= 8 && (Number(_p.wr||0) < 0.45 || Number(_p.pf||0) < 0.80 || Number(_p.net||0) < 0);
  } catch(_r191p) {}
  const r191EarlyFuel = r120Bool(
    d.r190Edge?.squeeze ||
    d.r190Edge?.r192FuelOk ||
    (d.r190Edge?.earlyContinuation && (d.r190Edge?.r194SwingBreak?.strong || d.r190Edge?.r192FuelOk || d.r190Edge?.squeeze)) ||
    (r274EntryOk && !r274Signal.farChase && !r274Signal.oneZoneRisk)
  );
  const r191LateNoFuel = r120Bool((d.r190Edge?.lateTrapRisk || d.r190Edge?.tooLate) && !r191EarlyFuel);
  const r191SpreadBad = r120Bool(d.r190Edge?.spreadBlock || d.poorLiquidity);
  const r192AgainstFinal = r120Bool(d.r190Edge?.r192LiveAgainst || d.r190Edge?.r192Footprint?.against || d.r190Edge?.r192DeepOfi?.against);
  const r191TakerOrVpinAgainst = r120Bool((d.r190Edge?.takerDivergence || d.r190Edge?.vpinAgainst || r189MicroAgainst || r192AgainstFinal) && !r191EarlyFuel);
  const r193EdgeObj = d.r190Edge || {};
  const r193Rvol = r120BrainNum(r193EdgeObj.rvol, r120BrainNum(d.r140Rvol?.ratio, 0));
  const r193P3 = r120BrainNum(r193EdgeObj.price3, 0);
  const r193FuelScore = r120BrainNum(r193EdgeObj.r192FuelScore, 0);
  const r193EdgeScore = r120BrainNum(r193EdgeObj.score, 0);
  const r193Oi15Abs = Math.abs(r120BrainNum(r193EdgeObj.oi?.oiChg15, r120BrainNum(d.r140OiVel?.velocity, 0)));
  const r193ZeminText = String(d.zemin || d.zeminOzet || d.brainSummary || d.reason || '');
  const r193BrokenGround = /BOZUK|kirli|dirty|risk/i.test(r193ZeminText);
  const r193StrongLiveFuel = r120Bool(
    r193EdgeObj.squeeze ||
    r193EdgeObj.earlyContinuation ||
    r193EdgeObj.r192FuelOk ||
    r193EdgeObj.r194SwingBreak?.strong ||
    (r193EdgeObj.r192Footprint?.strongAligned && r193EdgeObj.r192DeepOfi?.aligned && r193FuelScore >= 7) ||
    (r193EdgeObj.r192DeepOfi?.strongAligned && r193EdgeObj.r192Footprint?.aligned && r193FuelScore >= 7)
  );
  const r193Current5mAgainst = r120Bool((side === 'LONG' && r193P3 <= -0.35) || (side === 'SHORT' && r193P3 >= 0.35));
  const r193WeakZeroCandleDirtyFuel = r120Bool(
    r188WeakCandle &&
    !r193StrongLiveFuel &&
    (r193BrokenGround || r193Rvol < 0.25 || r193Current5mAgainst || r193EdgeScore < 35)
  );
  const r193OiAnomalyNoFuel = r120Bool(
    r193Oi15Abs >= 180 &&
    !r193StrongLiveFuel &&
    (r193Rvol < 0.45 || r188WeakCandle || r193BrokenGround)
  );
  const r193HtfOnlyWeakProof = r120Bool(
    r191Hard5mProof &&
    r188WeakCandle &&
    !r193StrongLiveFuel &&
    !r120Bool(d.r118CandleOk) &&
    !r120Bool(d.r118CandleStrong) &&
    !r120Bool(d.r117BodyReclaimOk)
  );

  const r195SwingWeakOnly = r120Bool(r193EdgeObj.r194SwingBreak?.ok && !r193EdgeObj.r194SwingBreak?.strong);
  const r195LowScoreLowEdgeNoFuel = r120Bool(
    score < Math.max(55, minScore - 15) &&
    edge < 35 &&
    !r193StrongLiveFuel
  );
  const r195WeakSwingLowQuality = r120Bool(
    r195SwingWeakOnly &&
    !r193EdgeObj.r192FuelOk &&
    (score < Math.max(58, minScore - 10) || edge < 35 || r193FuelScore < 6)
  );
  const r195R160LowEdgeFalse4of4 = r120Bool(
    r160TrueCount >= 4 &&
    edge < 35 &&
    score < Math.max(58, minScore - 10) &&
    !r193StrongLiveFuel
  );

  const r286EdgeObj = d.r190Edge || {};
  const r286RangePos = r120BrainNum(r286EdgeObj.rangePos, 0.5);
  const r286Seq = r120BrainNum(r286EdgeObj.seq, 0);
  const r286P3 = r120BrainNum(r286EdgeObj.price3, 0);
  const r286Rvol = r120BrainNum(r286EdgeObj.rvol, r120BrainNum(d.r140Rvol?.ratio, 0));
  const r286Oi15Abs = Math.abs(r120BrainNum(r286EdgeObj.oi?.oiChg15, r120BrainNum(d.r140OiVel?.velocity, 0)));
  const r286AtRangeEdge = r120Bool(side === 'LONG' ? r286RangePos >= 0.86 : r286RangePos <= 0.14);
  const r286ExtendedMove = r120Bool(
    r286Seq >= 4 ||
    (side === 'LONG' ? r286P3 >= Math.max(1.65, r120BrainNum(d.atrPct, 1) * 0.75) : r286P3 <= -Math.max(1.65, r120BrainNum(d.atrPct, 1) * 0.75))
  );
  const r286TargetNear = r120Bool(
    d.r39TargetNearBlock ||
    (d.r39SR?.targetTooNear && !d.r39SR?.breakConfirmed) ||
    (side === 'LONG' ? (r144CounterVeryNear || htfCounterWait || d.r39SR?.nearResistance) : (r144CounterVeryNear || htfCounterWait || d.r39SR?.nearSupport))
  );
  const r286Real5mAcceptance = r120Bool(
    d.r39SR?.breakConfirmed ||
    d.r37EarlyOk ||
    d.r37Timing?.retestOk ||
    d.r117BodyReclaimOk ||
    d.r117MssOk ||
    d.r118CandleOk ||
    d.r118CandleStrong ||
    d.r190Edge?.r194SwingBreak?.strong ||
    (d.r190Edge?.r194SwingBreak?.ok && d.r190Edge?.r192FuelOk && r120BrainNum(d.r190Edge?.score,0) >= 30) ||
    (side === 'LONG' ? (d.r110ICT?.inBullishFVG || d.r278InBullFvg) : (d.r110ICT?.inBearishFVG || d.r278InBearFvg)) ||
    (d.r110ICT?.oteZone?.inOte && String(d.r110ICT?.oteZone?.side||'').toUpperCase() === side)
  );
  const r286PullbackRetestOk = r120Bool(
    r286Real5mAcceptance &&
    (side === 'LONG' ? r286RangePos <= 0.78 : r286RangePos >= 0.22) &&
    !r188WeakCandle
  );
  const r286HtfOnlyChaseIntoNeedle = r120Bool(
    r286TargetNear &&
    (r188WeakCandle || r146No5mPattern) &&
    (r286AtRangeEdge || r286ExtendedMove || d.r37LateChaseBlock) &&
    !r286Real5mAcceptance &&
    !r286PullbackRetestOk &&
    !d.r190Edge?.squeeze
  );
  const r286OiSpikeNoAcceptance = r120Bool(
    r286TargetNear &&
    r286Oi15Abs >= 250 &&
    r286Rvol < 0.85 &&
    (r188WeakCandle || r146No5mPattern) &&
    !r286Real5mAcceptance &&
    !d.r190Edge?.squeeze
  );
  const r286ChartAcceptanceBlock = r120Bool(r286HtfOnlyChaseIntoNeedle || r286OiSpikeNoAcceptance);

  const r191LowScoreNoFuel = r120Bool(score < Math.max(52, minScore - 18) && !r191EarlyFuel && !r191Hard5mProof);
  const r191ForecastOnlyChase = r120Bool(r191ForecastOnlyProof && !r191EarlyFuel && score < Math.max(58, minScore - 12));
  const r191CounterTrapWeak = r120Bool(primaryMode === 'COUNTER_TRAP' && !r191Hard5mProof && !r191EarlyFuel && (r188WeakCandle || r188HtfCounterPressure || r191TakerOrVpinAgainst));
  const r191FastScalpWeak = r120Bool(r133FastScalpOverride && !r191Hard5mProof && !r191EarlyFuel && (r188WeakCandle || r191TakerOrVpinAgainst || r191LateNoFuel));
  const r191PerfCautionWeak = r120Bool(r191PerfCaution && !r191EarlyFuel && !r191Hard5mProof && (r159MomentumPass || r133FastScalpOverride || r191ForecastOnlyProof));
  const r196SideCtxBrain = side === 'LONG' ? d.r29?.r196?.long : d.r29?.r196?.short;
  const r196RangeLocationBlock = r120Bool(r196SideCtxBrain?.block);
  const r196RangeLocationCaution = r120Bool(r196SideCtxBrain?.caution);

  const r288FuelScore = r120BrainNum(r286EdgeObj.r192FuelScore, r120BrainNum(r286EdgeObj.score, 0));
  const r288FastBypassWeakChart = r120Bool(
    r156FastTop10Bypass &&
    score < minScore &&
    (r188WeakCandle || r146No5mPattern) &&
    !r120Bool(d.r118CandleStrong) && !r120Bool(d.r190Edge?.squeeze) &&
    (
      r288FuelScore < 8 ||
      r286Rvol < 0.80 ||
      r286Oi15Abs >= 500 ||
      (side === 'LONG' ? r286P3 >= 1.80 : r286P3 <= -1.80) ||
      r286TargetNear || r286AtRangeEdge
    )
  );
  const r288FastBypassWeakReason = r288FastBypassWeakChart
    ? `R288 R156 TOP10 bypass kapalı: skor ${score}/${minScore}, mum yok/zayıf, fuel ${r288FuelScore}, RVOL ${r286Rvol.toFixed(2)}, p3 ${r286P3.toFixed(2)}%, OI15 ${r286Oi15Abs.toFixed(0)}%; FVG/OTE retest veya gerçek 5m kabul bekle`
    : '';

  const r274SafeEntryPass = r120Bool(
    r274EntryOk && !hardDanger && !modeQualityBlock && !r147NoProofCounterTrap && !r148WrongSideBlock &&
    dataMinimum && !r196RangeLocationBlock && !toxicFlow && score >= Math.max(48, minScore - 22) &&
    (edge >= 40 || r274Score >= 32)
  );
  const r191RawOk = r120Bool(
    (!hardDanger && !modeQualityBlock && !r147NoProofCounterTrap && !r148WrongSideBlock && dataMinimum && r148ScoreOk && passEdge >= edgeNeed) ||
    (r133FastScalpOverride && !r147NoProofCounterTrap && !r148WrongSideBlock) ||
    r156FastTop10Bypass ||
    r159MomentumPass ||
    r160TraderDecision ||
    r274SafeEntryPass
  );
  const r191UnifiedEntryBlock = r120Bool(r191RawOk && (
    r191SpreadBad ||
    r191LateNoFuel ||
    r191TakerOrVpinAgainst ||
    r191LowScoreNoFuel ||
    r191ForecastOnlyChase ||
    r191CounterTrapWeak ||
    r191FastScalpWeak ||
    r191PerfCautionWeak ||
    r193WeakZeroCandleDirtyFuel ||
    r193OiAnomalyNoFuel ||
    r193HtfOnlyWeakProof ||
    r195LowScoreLowEdgeNoFuel ||
    r195WeakSwingLowQuality ||
    r195R160LowEdgeFalse4of4 ||
    r286ChartAcceptanceBlock ||
    r288FastBypassWeakChart ||
    r196RangeLocationBlock
  ));
  // R311C/R325D: AI RUNNER modunda sadece gerçek güvenlik blokları kalır, yakıt/kanıt blokları runner'da açılır.
  if (d?.aiRunner) {
    // Runner'da sadece likidite/spread ve aşırı risk blokları kalsın
    const r191UnifiedEntryBlockTemp = r191RawOk && (
      r191SpreadBad ||
      r191LateNoFuel ||
      r191TakerOrVpinAgainst ||
      r191PerfCautionWeak ||
      r193WeakZeroCandleDirtyFuel ||
      r193OiAnomalyNoFuel ||
      r193HtfOnlyWeakProof ||
      r196RangeLocationBlock
    );
    r191UnifiedEntryBlock = r120Bool(r191UnifiedEntryBlockTemp);
  }
  const r191UnifiedBlockReason = r191UnifiedEntryBlock
    ? `R191 final vali: ${[
        r191SpreadBad?'spread/likidite pahalı':'',
        r191LateNoFuel?'4-5 mum geç/chase yakıt yok':'',
        r191TakerOrVpinAgainst?'taker/VPIN/micro/footprint/deepOFI ters':'',
        r191LowScoreNoFuel?`skor ${score}/${minScore} düşük + erken yakıt yok`:'',
        r191ForecastOnlyChase?'forecast-only 4/4; gerçek 5m kanıt yok':'',
        r191CounterTrapWeak?'counter-trap zayıf kanıt':'',
        r191FastScalpWeak?'hızlı scalp zayıf kanıt':'',
        r191PerfCautionWeak?'son performans kötü; zayıf bypass kapalı':'',
        r193WeakZeroCandleDirtyFuel?'5m mum 0/12/zayıf + zemin/RVOL/yakıt kirli':'',
        r193OiAnomalyNoFuel?`OI spike anomali (${r193Oi15Abs.toFixed(0)}%) ama canlı yakıt yok`:'',
        r193HtfOnlyWeakProof?'HTF/stop-hunt kanıtı var ama gerçek 5m mum/footprint/deepOFI yok':'',
        r195LowScoreLowEdgeNoFuel?`düşük skor ${score}/${minScore} + düşük edge ${edge} + güçlü yakıt yok`:'',
        r195WeakSwingLowQuality?'R194 swing zayıf; TP/yakıt/edge kalite yok':'',
        r195R160LowEdgeFalse4of4?'R160 4/4 görünüyor ama edge düşük; gerçek kaliteli 5m yakıt yok':'',
        r286HtfOnlyChaseIntoNeedle?`R286 5m kabul yok: hedef/iğne yakın + mum yok + range ${r286RangePos.toFixed(2)} + p3 ${r286P3.toFixed(2)}% seq ${r286Seq}; kırılım-kabul/retest veya FVG/OTE bekle`:'',
        r286OiSpikeNoAcceptance?`R286 OI spike (${r286Oi15Abs.toFixed(0)}%) ama RVOL ${r286Rvol.toFixed(2)} ve 5m kabul yok; MM likidite iğnesi riski`:'',
        r288FastBypassWeakReason,
        r196RangeLocationBlock?(r196SideCtxBrain?.reason || 'R196 günlük range tepesi/dibi; işlem yok'):''
      ].filter(Boolean).join(' + ')}`
    : '';
  const r276Gate = r276MmHuntGate(d, side);
  d.r276Gate = r276Gate;
  d.r160Q1Structure = r160Q1Structure; d.r160Q2Flow = r160Q2Flow;
  d.r160Q3Momentum = r160Q3Momentum; d.r160Q4Proof = r160Q4Proof;
  const r278Hunt = r278LiquidityHunter(d, side);
  d.r278Hunt = r278Hunt;
  if (r278Hunt.allow && Number(r278Hunt.edge) > 0) {
    edge = Math.min(100, edge + Number(r278Hunt.edge));
  }
  const ok = r120Bool(r191RawOk && !r191UnifiedEntryBlock && r276Gate.allow && r278Hunt.allow);

  const sensorSummary = r120BrainSensorSummary(d);
  const modeLabel = r120BrainModeLabel(primaryMode);
  const r142Txt = `raw:${rawEdge} kalibre:${edge}${r142Cal.notes.length ? ' · '+r142Cal.notes.slice(0,3).join(',') : ''}`;
  const r133FastScalpWhy = r133FastScalpOverride
    ? `R144 hızlı 5m scalp: HTF/mum kanıtı + canlı tick ${r133LiveTradeCount} trade + delta ${r133LiveDeltaAbs.toFixed(1)}% + edge ${edge}`
    : '';
  const r187R160BlockReason = r187R160LowScoreBlock
    ? `R187 R160 skor freni: ${r160TrueCount}/4 karar var ama skor ${score}/${minScore}; 3/4 için taban ${r187R160ScoreFloor3of4}, 4/4 için taban ${r187R160ScoreFloor4of4}`
    : '';
  const r187R159BlockReason = (!r187R159ScoreOk && r159Points >= 6)
    ? `R187 R159 skor freni: momentum ${r159Points}p ama skor ${score}/${minScore}; taban ${r187R159ScoreFloor}`
    : '';
  const r188NoProofBlockReason = r188NoProofGuard
    ? `R189 5m micro/kanıt freni: R160 ${r160TrueCount}/4 ama Q4 kanıt yok; zayıfMum:${r188WeakCandle?'EVET':'hayır'} HTFkarşı:${r188HtfCounterPressure?'EVET':'hayır'} microTers:${r189MicroAgainst?'EVET':'hayır'} OI/crowdTuzak:${r189OiCrowdTrap?'EVET':'hayır'} hesapDikkat:${r188AccountCaution?'EVET':'hayır'}`
    : '';
  const r188R159BlockReason = (!r188R159ProofOk && r159Points >= 6)
    ? `R188 R159 kanıt freni: momentum ${r159Points}p ama 5m mum/sweep/body-reclaim/forecast kanıtı yetersiz`
    : '';
  const r274ReasonTxt = r274Signal.reason ? `R274 ${r274EntryOk?'ENTRY':'WATCH'}: ${r274Signal.reason}` : '';
  const r278BlockReason = (!r278Hunt.allow) ? r278Hunt.reason : '';
  const r276BlockReason = (!r276Gate.allow) ? r276Gate.reason : '';
  const r144WatchExtraRaw = r278BlockReason || r276BlockReason || r191UnifiedBlockReason || r288FastBypassWeakReason || r188NoProofBlockReason || r188R159BlockReason || r187R160BlockReason || r187R159BlockReason || r274ReasonTxt || r148Note || r147TrapGuardReason || r144FastBlockReason || '';
  const r144WatchExtra = r144WatchExtraRaw ? ` · ${r144WatchExtraRaw}` : '';
  const r156Label = r156FastTop10Bypass ? 'R156 TOP10 hızlı bypass' : '';
  const r159Label = r159MomentumPass ? `R159 momentum geçiş (${r159Points}p)` : '';
  const r160Label = r160TraderDecision ? `R160 trader karar (${r160TrueCount}/4: ${[r160Q1Structure?'yapı':'',r160Q2Flow?'akış':'',r160Q3Momentum?'ivme':'',r160Q4Proof?'kanıt':''].filter(Boolean).join('+')})` : '';
  const core = ok
    ? `🧠 5m Fırsat Beyni ${side}: ${r160TraderDecision ? r160Label : r159MomentumPass ? r159Label : r156FastTop10Bypass ? r156Label : r133FastScalpOverride ? 'R144 hızlı edge mikro-scalp' : modeLabel} · edge ${edge}/100 · ${r142Txt} · skor ${score}/${minScore}${r133FastScalpWhy ? ` · ${r133FastScalpWhy}` : ''} · ${sensorSummary}`
    : `🧠 5m Fırsat Beyni İZLE: ${side} kalite/edge yetersiz · olası oyun:${modeLabel} · edge ${edge}/100 · ${r142Txt} · skor ${score}/${minScore}${hardDanger?' · sert risk aktif':''}${modeQualityBlock?' · kalite duvarı aktif':''}${htfCounterWait?' · HTF karşı duvar/CHOCH bekleniyor':''}${r144WatchExtra}${sensorSummary?' · '+sensorSummary:''} · R160:${r160TrueCount}/4(${[r160Q1Structure?'Y':'',r160Q2Flow?'A':'',r160Q3Momentum?'I':'',r160Q4Proof?'K':''].join('')})`;

  d.brainMode = primaryMode;
  d.brainAction = ok ? 'TRADE' : 'WATCH';
  d.r156FastBypass = r156FastTop10Bypass;
  d.r159MomentumPass = r159MomentumPass;
  d.r159Points = r159Points;
  d.r160TraderDecision = r160TraderDecision;
  d.r160TrueCount = r160TrueCount;
  d.r160Q1Structure = r160Q1Structure;
  d.r283RealChartStructure = r283RealChartStructure;
  d.r286ChartAcceptanceBlock = r286ChartAcceptanceBlock;
  d.r288FastBypassWeakChart = r288FastBypassWeakChart;
  d.r288FastBypassWeakReason = r288FastBypassWeakReason;
  d.r286Real5mAcceptance = r286Real5mAcceptance;
  d.r286RangePos = r286RangePos;
  d.r286Seq = r286Seq;
  d.r286P3 = r286P3;
  d.r286TargetNear = r286TargetNear;
  d.r286HtfOnlyChaseIntoNeedle = r286HtfOnlyChaseIntoNeedle;
  d.r286OiSpikeNoAcceptance = r286OiSpikeNoAcceptance;
  d.r286ChartAcceptanceReason = r286ChartAcceptanceBlock ? (r286HtfOnlyChaseIntoNeedle ? 'HTF_ONLY_CHASE_INTO_NEEDLE' : 'OI_SPIKE_NO_ACCEPTANCE') : '';
  d.r160Q2Flow = r160Q2Flow;
  d.r160Q3Momentum = r160Q3Momentum;
  d.r160Q4Proof = r160Q4Proof;
  if (r278Hunt.allow && r278Hunt.bonus > 0) { edge = Math.min(100, edge + r278Hunt.bonus); }
  d.brainConfidence = edge;
  d.brainRawEdge = rawEdge;
  d.r142CalibratedEdge = edge;
  d.r142CalibrationAdj = r142Cal.adj;
  d.r142CalibrationNotes = r142Cal.notes;
  d.r142MemoryStats = { same:r142Cal.mem.same, opp:r142Cal.mem.opp, tags:r142Cal.mem.tags.slice(0,10) };
  d.r142FrequencySafeEdge = r142FrequencySafeEdge;
  d.brainSummary = core.slice(0,500);
  d.brainSensors = sensorSummary;
  d.brainAdaptiveFloor = adaptiveFloor;
  d.brainDataMinimum = dataMinimum;
  d.brainModeQualityBlock = modeQualityBlock;
  d.brainHtfCounterWait = htfCounterWait;
  d.brainLiveFlowOk = liveFlowOk;
  d.brainR125FlowOk = r125SideFlow.ok;
  d.brainR125OpposingFlow = r125OpposingFlow;
  d.brainR134OpposingLiveFlow = r134OpposingLiveFlow;
  d.brainR134LegacySensorVeto = r134LegacySensorVeto;
  d.brainR134FlowAligned = r134FlowAligned;
  d.brainR134HtfOrCandleProof = r134HtfOrCandleProof;
  d.brainFatalDanger = fatalDanger;
  d.brainHardDanger = hardDanger;
  d.r133FastScalpOverride = r133FastScalpOverride;
  d.r133FastScalpWhy = r133FastScalpWhy;
  d.r134FastScalpOverride = r133FastScalpOverride;
  d.r134FastScalpWhy = d.r133FastScalpWhy;
  d.r144FastNeedsRealProof = r144FastNeedsRealProof;
  d.r144FastBlockReason = r144FastBlockReason;
  d.r144NoVolumeConfirm = r144NoVolumeConfirm;
  d.r144CounterVeryNear = r144CounterVeryNear;
  d.r145NoVolCounterTrapRisk = r145NoVolCounterTrapRisk;
  d.r146CounterWallPressure = r146CounterWallPressure;
  d.r146LateWallNoPattern = r146LateWallNoPattern;
  d.r146CounterDist = r146CounterDist;
  d.r147ForecastAgainst = r147ForecastAgainst;
  d.r147BookFlowAgainst = r147BookFlowAgainst;
  d.r147WrongWayCluster = r147WrongWayCluster;
  d.r147NoProofCounterTrap = r147NoProofCounterTrap;
  d.r147TrapGuardReason = r147TrapGuardReason;
  d.r148ReversalSideOk = r148ReversalSideOk;
  d.r148WrongSideBlock = r148WrongSideBlock;
  d.r148BalanceBonus = r148BalanceBonus;
  d.r148WrongPenalty = r148WrongPenalty;
  d.r148Note = r148Note;
  d.r187R160ScoreOk = r187R160ScoreOk;
  d.r187R160LowScoreBlock = r187R160LowScoreBlock;
  d.r187R160ScoreFloor3of4 = r187R160ScoreFloor3of4;
  d.r187R160ScoreFloor4of4 = r187R160ScoreFloor4of4;
  d.r187R160BlockReason = r187R160BlockReason;
  d.r187R159ScoreOk = r187R159ScoreOk;
  d.r187R159ScoreFloor = r187R159ScoreFloor;
  d.r187R159BlockReason = r187R159BlockReason;
  d.r188AccountCaution = r188AccountCaution;
  d.r188WeakCandle = r188WeakCandle;
  d.r188HtfCounterPressure = r188HtfCounterPressure;
  d.r188StrongNoQ4Exception = r188StrongNoQ4Exception;
  d.r188NoProofGuard = r188NoProofGuard;
  d.r188NoProofBlockReason = r188NoProofBlockReason;
  d.r188R159ProofOk = r188R159ProofOk;
  d.r188R159BlockReason = r188R159BlockReason;
  d.r189MicroSide = r189MicroSide;
  d.r189MicroPersistSide = r189MicroPersistSide;
  d.r189VampBiasAbs = +r189VampBiasAbs.toFixed(5);
  d.r189MicroAligned = r189MicroAligned;
  d.r189MicroAgainst = r189MicroAgainst;
  d.r189OiCrowdTrap = r189OiCrowdTrap;
  d.r191Hard5mProof = r191Hard5mProof;
  d.r191ForecastOnlyProof = r191ForecastOnlyProof;
  d.r191PerfCaution = r191PerfCaution;
  d.r191EarlyFuel = r191EarlyFuel;
  d.r192AgainstFinal = r192AgainstFinal;
  d.r192FuelOk = !!d.r190Edge?.r192FuelOk;
  d.r192Footprint = d.r190Edge?.r192Footprint || null;
  d.r192DeepOfi = d.r190Edge?.r192DeepOfi || null;
  d.r191RawOk = r191RawOk;
  d.r191UnifiedEntryBlock = r191UnifiedEntryBlock;
  d.r191UnifiedBlockReason = r191UnifiedBlockReason;
  d.r193WeakZeroCandleDirtyFuel = r193WeakZeroCandleDirtyFuel;
  d.r193OiAnomalyNoFuel = r193OiAnomalyNoFuel;
  d.r193HtfOnlyWeakProof = r193HtfOnlyWeakProof;
  d.r195LowScoreLowEdgeNoFuel = r195LowScoreLowEdgeNoFuel;
  d.r195WeakSwingLowQuality = r195WeakSwingLowQuality;
  d.r195R160LowEdgeFalse4of4 = r195R160LowEdgeFalse4of4;
  d.r274Signal = r274Signal;
  d.r274SafeEntryPass = r274SafeEntryPass;
  d.entryPermissionReason = ok ? (r274SafeEntryPass ? 'R274_C20_L20_RSI_RATIO_FVG_ENTRY' : (r148ReversalSideOk ? 'R148_BALANCED_TRAP_INVERSION' : (r133FastScalpOverride ? 'R135_FAST_EDGE_PASS' : `R121_SINGLE_BRAIN_${primaryMode}`))) : 'R121_SINGLE_BRAIN_WATCH';
  d.entryPermissionOk = ok;
  d.autoOk = ok;
  d.pass = ok;
  d.tier = ok ? (edge >= 82 ? 'A' : 'B+') : 'WAIT';
  d.reason = core;
  if (!ok) {
    d.blocks = [core];
    d.reasons = [];
  } else {
    d.reasons = [core];
    d.blocks = [];
  }
  return d;
}

function r120AutoReason(decisionChain={}, fallback='Bekle') {
  return String(decisionChain?.brainSummary || fallback || '5m Fırsat Beyni izle').replace(/R\d+[^·,]*/g, '').replace(/\s+/g,' ').trim().slice(0,500);
}

function r274N(v,d=0){ const n=Number(v); return Number.isFinite(n)?n:d; }
function r274Rows(k5m=[], limit=90){
  return (Array.isArray(k5m)?k5m:[]).slice(-limit).map(k=>({
    o:r274N(k?.[1]), h:r274N(k?.[2]), l:r274N(k?.[3]), c:r274N(k?.[4]), v:r274N(k?.[5])
  })).filter(x=>x.o>0&&x.h>0&&x.l>0&&x.c>0);
}
function r274EmaVals(vals=[], len=20){
  const a=[]; const p=Math.max(1, Number(len)||20); const k=2/(p+1); let e=null;
  for(const v0 of vals){ const v=Number(v0); if(!Number.isFinite(v)){ a.push(e); continue; } e=e==null?v:(v*k+e*(1-k)); a.push(e); }
  return a;
}
function r274RsiVals(closes=[], len=14){
  const n=Math.max(2,Number(len)||14); const out=Array(closes.length).fill(null);
  if(closes.length<n+1) return out;
  let gain=0, loss=0;
  for(let i=1;i<=n;i++){ const d=closes[i]-closes[i-1]; if(d>=0) gain+=d; else loss-=d; }
  let avgG=gain/n, avgL=loss/n;
  out[n]=avgL===0?100:100-(100/(1+avgG/avgL));
  for(let i=n+1;i<closes.length;i++){
    const d=closes[i]-closes[i-1]; const g=d>0?d:0, l=d<0?-d:0;
    avgG=(avgG*(n-1)+g)/n; avgL=(avgL*(n-1)+l)/n;
    out[i]=avgL===0?100:100-(100/(1+avgG/avgL));
  }
  return out;
}
function r274Atr(rows=[], len=14){
  if(rows.length<3) return 0;
  const trs=[];
  for(let i=1;i<rows.length;i++){
    const p=rows[i-1], c=rows[i];
    trs.push(Math.max(c.h-c.l, Math.abs(c.h-p.c), Math.abs(c.l-p.c)));
  }
  const tail=trs.slice(-len); return tail.reduce((a,b)=>a+b,0)/Math.max(1,tail.length);
}
function r274LastFvg(rows=[], side='LONG'){
  const isL=String(side).toUpperCase()==='LONG';
  const recent=rows.slice(-24);
  for(let j=recent.length-1;j>=2;j--){
    const a=recent[j-2], c=recent[j];
    if(isL && a.h < c.l){
      const z={side:'LONG', low:a.h, high:c.l, mid:(a.h+c.l)/2, age:recent.length-1-j};
      z.invalid = rows.at(-1)?.c < z.low;
      return z;
    }
    if(!isL && a.l > c.h){
      const z={side:'SHORT', low:c.h, high:a.l, mid:(c.h+a.l)/2, age:recent.length-1-j};
      z.invalid = rows.at(-1)?.c > z.high;
      return z;
    }
  }
  return null;
}
function r274Events(rows=[], side='LONG'){
  const isL=String(side).toUpperCase()==='LONG';
  const closes=rows.map(x=>x.c);
  const rs=r274RsiVals(closes,14);
  const rE20=r274EmaVals(rs.map(x=>Number.isFinite(x)?x:null),20);
  const e20=r274EmaVals(closes,20);
  const ev=[];
  for(let i=22;i<rows.length;i++){
    const r0=rs[i-1], r1=rs[i], re0=rE20[i-1], re1=rE20[i];
    const c0=rows[i-1].c, c1=rows[i].c, pe0=e20[i-1], pe1=e20[i];
    if([r0,r1,re0,re1,c0,c1,pe0,pe1].some(x=>!Number.isFinite(x))) continue;
    if(isL && r0<=re0 && r1>re1) ev.push({i,type:'C20',dir:'LONG',c:c1,rsi:r1});
    if(!isL && r0>=re0 && r1<re1) ev.push({i,type:'C20',dir:'SHORT',c:c1,rsi:r1});
    if(isL && c0<=pe0 && c1>pe1) ev.push({i,type:'L20',dir:'LONG',c:c1,rsi:r1});
    if(!isL && c0>=pe0 && c1<pe1) ev.push({i,type:'L20',dir:'SHORT',c:c1,rsi:r1});
  }
  return {ev, rs, rE20, e20};
}
function r274RatioBetween(a,b){
  const pricePct=Math.abs((b.c-a.c)/Math.max(1e-12,a.c)*100);
  const rsiDelta=Math.abs(b.rsi-a.rsi);
  const ratio=pricePct>0.01 ? rsiDelta/pricePct : 99;
  return {pricePct,rsiDelta,ratio};
}
function r274C20L20RsiRatioFvgEntry(k5m=[], side='LONG', lastPrice=0, atrPct=1){
  try{
    const dir=String(side||'').toUpperCase(); const isL=dir==='LONG';
    const rows=r274Rows(k5m,96); const lp=r274N(lastPrice, rows.at(-1)?.c||0);
    if(rows.length<35 || !lp) return {ok:false, entryOk:false, watchOk:false, side:dir, score:0, reason:'R274 veri<35'};
    const atr=r274Atr(rows,14); const atrSafe=atr>0?atr:lp*Math.max(0.002,r274N(atrPct,1)/100);
    const {ev, rs, e20}=r274Events(rows,dir);
    const lastC20=ev.filter(x=>x.type==='C20').at(-1); const lastL20=ev.filter(x=>x.type==='L20').at(-1);
    const lastEv=[lastC20,lastL20].filter(Boolean).sort((a,b)=>b.i-a.i)[0]||null;
    const age=lastEv? rows.length-1-lastEv.i : 99;
    let ratioNow=null, ratioPrev=null, ratioDown=false, ratioUp=false, ratioLabel='ratio:yok';
    if(lastEv){
      const same=ev.filter(x=>x.type===lastEv.type && x.i<=lastEv.i);
      const b=same.at(-1), a=same.at(-2), p=same.at(-3);
      if(a&&b){ ratioNow=r274RatioBetween(a,b); ratioLabel=`${lastEv.type} ratio ${ratioNow.ratio.toFixed(2)}`; }
      if(p&&a&&b){ ratioPrev=r274RatioBetween(p,a); ratioDown=ratioNow.ratio < ratioPrev.ratio*0.86; ratioUp=ratioNow.ratio > ratioPrev.ratio*1.16; ratioLabel=`${lastEv.type} ratio ${ratioPrev.ratio.toFixed(2)}→${ratioNow.ratio.toFixed(2)}`; }
    }
    const fvg=r274LastFvg(rows,dir);
    const zoneOk=!!(fvg && !fvg.invalid && lp>=fvg.low && lp<=fvg.high);
    const holdOk=!!(fvg && !fvg.invalid && (isL ? (lp>fvg.high && lp-fvg.high<=atrSafe*0.45) : (lp<fvg.low && fvg.low-lp<=atrSafe*0.45)));
    const farChase=!!(fvg && !fvg.invalid && (isL ? lp>fvg.high+atrSafe*1.35 : lp<fvg.low-atrSafe*1.35));
    const rsiNow=r274N(rs.at(-1),50), ema20=r274N(e20.at(-1),lp);
    const l20Side= isL ? lp>=ema20 : lp<=ema20;
    const recentCross=age<=6;
    const oneZoneRisk=!!(ratioNow && ratioNow.ratio>=0.74 && ratioNow.ratio<=1.35 && ratioNow.pricePct>=0.45);
    const noZoneChase=farChase && !zoneOk && !holdOk;
    let score=0; const plus=[], minus=[];
    if(recentCross && lastEv?.type==='C20'){ score+=12; plus.push('C20 taze'); }
    if(recentCross && lastEv?.type==='L20'){ score+=10; plus.push('L20 taze'); }
    if(zoneOk){ score+=16; plus.push('FVG içi'); }
    else if(holdOk){ score+=10; plus.push('FVG hold/retest'); }
    if(ratioDown){ score+=12; plus.push('RSI-ratio kolaylaşma'); }
    if(l20Side){ score+=5; plus.push('L20 yön üstü/altı'); }
    if(isL && rsiNow<68) score+=4; if(!isL && rsiNow>32) score+=4;
    if(ratioUp){ score-=10; minus.push('RSI-ratio zorlaşma'); }
    if(oneZoneRisk){ score-=14; minus.push('1-bölge/doyum'); }
    if(noZoneChase){ score-=18; minus.push('FVG uzağı/chase'); }
    if(fvg?.invalid){ score-=22; minus.push('FVG invalid'); }
    const entryOk=score>=24 && (zoneOk || holdOk || (recentCross && l20Side)) && !fvg?.invalid && !noZoneChase && !oneZoneRisk;
    const watchOk=score>=14 && !fvg?.invalid && !noZoneChase;
    return {
      ok:true, side:dir, entryOk, watchOk, zoneOk, holdOk, fvg, recentCross, c20:!!lastC20, l20:!!lastL20,
      score, priority:entryOk?Math.min(100,score+52):score, ratio:ratioNow?+ratioNow.ratio.toFixed(2):null,
      ratioDown, ratioUp, oneZoneRisk, farChase:noZoneChase, rsi:+rsiNow.toFixed(1), age,
      reason:`R274 ${dir}: ${[...plus,...minus].join(' · ')||'bekle'} · ${ratioLabel} · RSI ${rsiNow.toFixed(1)}${fvg?` · FVG ${fvg.low.toFixed(8)}-${fvg.high.toFixed(8)} age:${fvg.age}`:' · FVG yok'}`
    };
  } catch(e){ return {ok:false, entryOk:false, watchOk:false, side:String(side).toUpperCase(), score:0, reason:`R274 hata:${String(e?.message||e).slice(0,80)}`}; }
}

function r276Rows(k5m, limit){
  limit = limit || 60;
  return (Array.isArray(k5m)?k5m:[]).slice(-limit).map(function(k){
    return { o:+(k&&k[1]), h:+(k&&k[2]), l:+(k&&k[3]), c:+(k&&k[4]), v:+(k&&k[5]) };
  }).filter(function(x){ return x.o>0&&x.h>0&&x.l>0&&x.c>0&&x.h>=x.l; });
}
function r276MmHuntGate(d, side){
  d = d || {};
  try {
    var isL = String(side||'').toUpperCase() !== 'SHORT';
    var rows = r276Rows(d._r276k5m||[], 60);
    var isChase = r120Bool(d.r159MomentumPass || d.r156FastTop10Bypass || /momentum ge\u00e7i\u015f|momentum scalp|trend devam/i.test(String(d.reason||'')));
    if (rows.length < 40) {
      return { allow: !isChase, reason: isChase ? 'R276 veri<40: momentum-chase kapal\u0131 (fail-soft)' : 'R276 veri yok, chase de\u011fil -> ge\u00e7ir', regime:'VERI' };
    }
    var win = rows.slice(-48);
    var his=win.map(function(x){return x.h;}), los=win.map(function(x){return x.l;});
    var hi=Math.max.apply(null,his), lo=Math.min.apply(null,los);
    var lp = +(d._r276LastPrice) || rows[rows.length-1].c;
    var pos = hi>lo ? (lp-lo)/(hi-lo)*100 : 50;
    var bandPct = lo>0 ? (hi-lo)/lo*100 : 99;
    var atr=0; for(var i=rows.length-14;i<rows.length;i++){var c=rows[i],p=rows[i-1];atr+=Math.max(c.h-c.l,Math.abs(c.h-p.c),Math.abs(c.l-p.c));} atr/=14;
    var slopeAtr = atr>0 ? (rows[rows.length-1].c - rows[rows.length-13].c)/atr : 0;
    var isRange = bandPct <= 3.2 && Math.abs(slopeAtr) < 2.0;
    var last=rows[rows.length-1], prev=rows[rows.length-2];
    var midPrev=(prev.o+prev.c)/2;
    var reclaimUp = last.c>last.o && last.c>midPrev;
    var rejectDn  = last.c<last.o && last.c<midPrev;
    var base = { regime:isRange?'RANGE':'TREND', pos:+pos.toFixed(0), bandPct:+bandPct.toFixed(2), slopeAtr:+slopeAtr.toFixed(2) };
    function R(allow,reason){ return Object.assign({allow:allow,reason:reason}, base); }

    if (!isRange) {
      if (isChase && isL && slopeAtr <= -2.2) return R(false, 'R276 trend AŞAĞI iken LONG-chase blok: düşen bıçak (slope '+slopeAtr.toFixed(1)+' ATR)');
      if (isChase && !isL && slopeAtr >= 2.2) return R(false, 'R276 trend YUKARI iken SHORT-chase blok: yükselen bıçak (slope +'+slopeAtr.toFixed(1)+' ATR)');
      if (isChase && isL && pos>=96 && slopeAtr>=4.0) return R(false, 'R276 trend parabolik tepe %'+pos.toFixed(0)+' LONG geç-chase blok');
      if (isChase && !isL && pos<=4 && slopeAtr<=-4.0) return R(false, 'R276 trend parabolik dip %'+pos.toFixed(0)+' SHORT geç-chase blok');
      return R(true, 'R276 trend rejimi: yön-uyumlu giriş serbest (pos %'+pos.toFixed(0)+', slope '+slopeAtr.toFixed(1)+')');
    }
    if (isL) {
      if (pos >= 80) return R(false, 'R276 RANGE tepe %'+pos.toFixed(0)+' LONG blok: dip degil tavan');
      if (pos >= 68 && isChase) return R(false, 'R276 RANGE premium %'+pos.toFixed(0)+' LONG-chase blok: MM burada short-avi yapar');
      if (pos <= 40) return R(true, 'R276 RANGE discount %'+pos.toFixed(0)+' LONG: dogru uc'+(reclaimUp?' (reclaim)':''));
      return R(!isChase || reclaimUp, 'R276 RANGE orta %'+pos.toFixed(0)+' LONG: '+((!isChase||reclaimUp)?'gecir':'chase+reclaim yok, blok'));
    } else {
      if (pos <= 20) return R(false, 'R276 RANGE dip %'+pos.toFixed(0)+' SHORT blok: tavan degil taban');
      if (pos <= 32 && isChase) return R(false, 'R276 RANGE discount %'+pos.toFixed(0)+' SHORT-chase blok: MM burada long-avi yapar');
      if (pos >= 60) return R(true, 'R276 RANGE premium %'+pos.toFixed(0)+' SHORT: dogru uc'+(rejectDn?' (reject)':''));
      return R(!isChase || rejectDn, 'R276 RANGE orta %'+pos.toFixed(0)+' SHORT: '+((!isChase||rejectDn)?'gecir':'chase+reject yok, blok'));
    }
  } catch(e){
    return { allow:true, reason:'R276 hata, gecir (fail-soft): '+String(e&&e.message||e).slice(0,50), regime:'HATA' };
  }
}

function r278LiquidityHunter(d, side){
  d = d || {};
  try {
    const ict = d._r278ICT;
    if (!ict || !ict.ok) return { allow:true, edge:0, reason:'R278 ICT veri yok, fail-soft', tag:'VERI' };
    const isL = String(side||'').toUpperCase() !== 'SHORT';
    const isChase = r120Bool(d.r159MomentumPass || d.r156FastTop10Bypass || /momentum ge\u00e7i\u015f|momentum scalp|trend devam|h\u0131zl\u0131 edge|mikro-scalp/i.test(String(d.reason||'')));
    const r160FullProof = r120Bool(d.r160Q1Structure && d.r160Q2Flow && d.r160Q3Momentum && d.r160Q4Proof);

    const sweepReclaimOk = isL ? r120Bool(ict.longOk) : r120Bool(ict.shortOk);
    const sweepQ = isL ? (ict.sslSweepQuality||0) : (ict.bslSweepQuality||0);
    const inFvg  = isL ? r120Bool(ict.inBullishFVG) : r120Bool(ict.inBearishFVG);

    const counterLiq   = isL ? ict.nearSSL : ict.nearBSL;
    const counterSwept = isL ? r120Bool(ict.sslSwept) : r120Bool(ict.bslSwept);
    const counterDist  = counterLiq ? Number(counterLiq.dist) : 999;

    let edge = 0; const tags = [];

    if (!sweepReclaimOk && !r160FullProof && counterLiq && counterDist <= 0.35 && !counterSwept) {
      return { allow:false, edge:-20,
        reason:`R278 TUZAK BLOK: ${isL?'alt SSL':'ust BSL'} %${counterDist.toFixed(2)} (${counterLiq.label}) supurulmemis — MM oraya cekip avlar`,
        tag:'TUZAK' };
    }
    if (!sweepReclaimOk && isChase && counterLiq && counterDist <= 0.6 && !counterSwept) {
      return { allow:false, edge:-12,
        reason:`R278 chase TUZAK BLOK: ${isL?'alt SSL':'ust BSL'} %${counterDist.toFixed(2)} yakin, momentum oraya kosacak (av)`,
        tag:'CHASE_TUZAK' };
    }

    const bslReject = r120Bool(ict.bslRejection);
    const sslReject = r120Bool(ict.sslRejection);
    if (!sweepReclaimOk && !r160FullProof) {
      if (isL && bslReject) {
        return { allow:false, edge:-15,
          reason:`R279 DİRENÇ REDDİ: ust BSL kirilamadi — MM asagi g\u00fc\u00e7 toplayacak; LONG erken (asagi sweep+reclaim sonrasi gir). Su an MM ile SHORT uyumlu.`,
          tag:'REDDI_LONG_BEKLE', flipHint:'SHORT' };
      }
      if (!isL && sslReject) {
        return { allow:false, edge:-15,
          reason:`R279 DESTEK REDDİ: alt SSL kirilamadi — MM yukari g\u00fc\u00e7 toplayacak; SHORT erken (yukari sweep+reclaim sonrasi gir). Su an MM ile LONG uyumlu.`,
          tag:'REDDI_SHORT_BEKLE', flipHint:'LONG' };
      }
      if (!isL && bslReject) { edge += 8; tags.push('R279 MM-ile-asagi (diren\u00e7 reddi, g\u00fc\u00e7 toplama yon\u00fc)'); }
      if (isL && sslReject)  { edge += 8; tags.push('R279 MM-ile-yukari (destek reddi, g\u00fc\u00e7 toplama yon\u00fc)'); }
    }

    if (sweepReclaimOk) {
      edge += 14; tags.push(`SUPURME+RECLAIM q${sweepQ} (KAT deseni)`);
      if (inFvg) { edge += 6; tags.push('FVG mitigasyon'); }
      return { allow:true, edge, reason:`R278 AV TAMAM: ${isL?'SSL':'BSL'} supuruldu+reclaim, MM avlandi — ${tags.join(' · ')}`, tag:'AV_BINDI' };
    }
    const targetSwept = isL ? r120Bool(ict.sslSwept) : r120Bool(ict.bslSwept);
    if (targetSwept && sweepQ >= 2) { edge += 7; tags.push(`hedef supuruldu q${sweepQ}`); }

    if (inFvg) { edge += 6; tags.push('FVG ici (mitigasyon)'); }

    if (counterDist >= 0.8) { edge += 4; tags.push(`temiz kosu (karsi %${counterDist.toFixed(2)})`); }

    try {
      const ote = ict.oteZone;
      if (ote && ote.inOte && ote.side === side) {
        const oteBonus = ote.depth >= 0.705 ? 10 : 7;
        edge += oteBonus;
        tags.push(`OTE ${(ote.depth*100).toFixed(0)}% geri-cekilme (ICT optimal giris)`);
      }
    } catch(_) {}

    return { allow:true, edge, reason: tags.length ? `R278 serbest: ${tags.join(' · ')}` : 'R278 notr serbest', tag:'SERBEST' };
  } catch(e){
    return { allow:true, edge:0, reason:'R278 hata fail-soft: '+String(e&&e.message||e).slice(0,40), tag:'HATA' };
  }
}

function r37EMA(values, period=9) {
  const arr = (values || []).map(Number).filter(Number.isFinite);
  if (arr.length < 2) return arr.at(-1) || 0;
  const k = 2 / (period + 1);
  let e = arr[0];
  for (let i = 1; i < arr.length; i++) e = arr[i] * k + e * (1-k);
  return e;
}
function r37VWAP(klines) {
  let pv = 0, vol = 0;
  for (const c of (klines || [])) {
    const h=Number(c[2]), l=Number(c[3]), cl=Number(c[4]), v=Number(c[5]||0);
    if (![h,l,cl,v].every(Number.isFinite)) continue;
    const typ = (h+l+cl)/3; pv += typ*v; vol += v;
  }
  return vol > 0 ? pv/vol : 0;
}
function r37MoveTiming(klines5m, klines15m, lastPrice, atrPct=1, vpvr1h=null, liq1h=null) {
  const kl = Array.isArray(klines5m) ? klines5m.filter(Boolean) : [];
  const k15 = Array.isArray(klines15m) ? klines15m.filter(Boolean) : [];
  const lp = Number(lastPrice || (kl.at(-1)||[])[4] || 0);
  const base = { ok:false, long:{}, short:{}, notes:[] };
  if (kl.length < 12 || !(lp > 0)) return base;
  const rows = kl.map(c=>({
    o:Number(c[1]), h:Number(c[2]), l:Number(c[3]), c:Number(c[4]), v:Number(c[5]||0)
  })).filter(c=>[c.o,c.h,c.l,c.c].every(Number.isFinite));
  if (rows.length < 12) return base;
  const last = rows.at(-1);
  const prev = rows.at(-2) || last;
  const chgFrom = n => {
    const r = rows.at(-1-n);
    return r && r.o > 0 ? (lp - r.o) / r.o * 100 : 0;
  };
  const chg3 = chgFrom(3), chg5 = chgFrom(5), chg8 = chgFrom(8);
  let bullSeq = 0, bearSeq = 0;
  for (let i = rows.length - 1; i >= Math.max(0, rows.length - 8); i--) {
    if (rows[i].c >= rows[i].o) {
      if (bearSeq === 0) bullSeq++; else break;
    } else {
      if (bullSeq === 0) bearSeq++; else break;
    }
  }
  const recent8 = rows.slice(-8);
  const recent20 = rows.slice(-20);
  const localHigh = Math.max(...recent20.map(x=>x.h));
  const localLow  = Math.min(...recent20.map(x=>x.l));
  const distHigh = localHigh > 0 ? (localHigh - lp) / lp * 100 : 99;
  const distLow  = localLow > 0 ? (lp - localLow) / lp * 100 : 99;
  const range8Hi = Math.max(...recent8.map(x=>x.h));
  const range8Lo = Math.min(...recent8.map(x=>x.l));
  const rangePos = range8Hi > range8Lo ? (lp - range8Lo) / (range8Hi - range8Lo) : 0.5;
  const closes = rows.map(x=>x.c);
  const ema9 = r37EMA(closes.slice(-30), 9);
  const ema21 = r37EMA(closes.slice(-50), 21);
  const vw = r37VWAP(rows.slice(-72));
  const atr = Math.max(0.25, Number(atrPct)||1);
  const near = (a,b,pct) => a > 0 && b > 0 && Math.abs(a-b)/b*100 <= pct;
  const nearBand = Math.max(0.12, Math.min(0.55, atr * 0.28));
  const lastBody = Math.abs(last.c-last.o) / lp * 100;
  const lowerWick = (Math.min(last.o,last.c)-last.l) / lp * 100;
  const upperWick = (last.h-Math.max(last.o,last.c)) / lp * 100;
  const volAvg = rows.slice(-25,-1).reduce((s,x)=>s+x.v,0) / Math.max(1, rows.slice(-25,-1).length);
  const volBoost = last.v > volAvg * 1.25;
  const emaStackBull = ema9 > ema21 && lp >= ema9 * 0.996;
  const emaStackBear = ema9 < ema21 && lp <= ema9 * 1.004;
  const retestLong = (near(last.l, ema9, nearBand) || near(last.l, ema21, nearBand) || near(last.l, vw, nearBand)) && last.c >= last.o && lowerWick >= lastBody * 0.35;
  const retestShort= (near(last.h, ema9, nearBand) || near(last.h, ema21, nearBand) || near(last.h, vw, nearBand)) && last.c <= last.o && upperWick >= lastBody * 0.35;
  const firstBullImpulse = chg3 > Math.max(0.35, atr*0.35) && chg5 < Math.max(1.6, atr*1.4) && volBoost && emaStackBull;
  const firstBearImpulse = chg3 < -Math.max(0.35, atr*0.35) && chg5 > -Math.max(1.6, atr*1.4) && volBoost && emaStackBear;
  const bullExtended = chg5 > Math.max(1.15, atr*1.15) || chg8 > Math.max(1.8, atr*1.65) || bullSeq >= 4;
  const bearExtended = chg5 < -Math.max(1.15, atr*1.15) || chg8 < -Math.max(1.8, atr*1.65) || bearSeq >= 4;
  const nearUpperTarget = !!(distHigh < Math.max(0.28, atr*0.28) || rangePos > 0.86 || (vpvr1h?.vah && Math.abs(lp-Number(vpvr1h.vah))/lp*100 < Math.max(0.35, atr*0.35)));
  const nearLowerTarget = !!(distLow  < Math.max(0.28, atr*0.28) || rangePos < 0.14 || (vpvr1h?.val && Math.abs(lp-Number(vpvr1h.val))/lp*100 < Math.max(0.35, atr*0.35)));
  const k15Rows = k15.map(c=>({o:Number(c[1]), h:Number(c[2]), l:Number(c[3]), c:Number(c[4])})).filter(c=>[c.o,c.h,c.l,c.c].every(Number.isFinite));
  const k15Last = k15Rows.at(-1) || null;
  const k15Bull = !!(k15Last && k15Last.c > k15Last.o);
  const k15Bear = !!(k15Last && k15Last.c < k15Last.o);
  const longLateChase  = bullExtended && nearUpperTarget && !retestLong && !firstBullImpulse;
  const shortLateChase = bearExtended && nearLowerTarget && !retestShort && !firstBearImpulse;
  const longRetestOnly = bullExtended && !firstBullImpulse && !retestLong;
  const shortRetestOnly= bearExtended && !firstBearImpulse && !retestShort;
  const longEarlyScore = [firstBullImpulse, retestLong, emaStackBull, k15Bull, volBoost, rangePos < 0.72].filter(Boolean).length;
  const shortEarlyScore= [firstBearImpulse, retestShort, emaStackBear, k15Bear, volBoost, rangePos > 0.28].filter(Boolean).length;
  return {
    ok:true, chg3:+chg3.toFixed(2), chg5:+chg5.toFixed(2), chg8:+chg8.toFixed(2),
    bullSeq, bearSeq, distHigh:+distHigh.toFixed(2), distLow:+distLow.toFixed(2), rangePos:+rangePos.toFixed(2), ema9:+ema9.toFixed(8), ema21:+ema21.toFixed(8), vwap:+vw.toFixed(8),
    long:{ lateChase:longLateChase, retestOnly:longRetestOnly, retestOk:retestLong, earlyImpulse:firstBullImpulse, earlyScore:longEarlyScore, targetNear:nearUpperTarget, extended:bullExtended, reason:`5m chg5 ${chg5.toFixed(2)}%, mum:${bullSeq}, hedefUzak:${distHigh.toFixed(2)}%` },
    short:{ lateChase:shortLateChase, retestOnly:shortRetestOnly, retestOk:retestShort, earlyImpulse:firstBearImpulse, earlyScore:shortEarlyScore, targetNear:nearLowerTarget, extended:bearExtended, reason:`5m chg5 ${chg5.toFixed(2)}%, mum:${bearSeq}, hedefUzak:${distLow.toFixed(2)}%` }
  };
}

function r39Round(x, d=8) { const n=Number(x); return Number.isFinite(n) ? +n.toFixed(d) : 0; }
function r39K(klines) {
  return (Array.isArray(klines)?klines:[]).map(k=>({
    t:Number(k[0]||0), o:Number(k[1]), h:Number(k[2]), l:Number(k[3]), c:Number(k[4]), v:Number(k[5]||0)
  })).filter(x=>[x.o,x.h,x.l,x.c].every(Number.isFinite) && x.h>=x.l && x.c>0);
}
function r39DailyPivots(k1h, lastPrice) {
  const rows = r39K(k1h);
  const lp = Number(lastPrice||0);
  const out = { ok:false, pdh:0, pdl:0, pdc:0, pp:0, r1:0, s1:0, r2:0, s2:0, rangePct:0, dayKey:null };
  if (rows.length < 30 || !(lp>0)) return out;
  const byDay = new Map();
  for (const r of rows) {
    const d = new Date(r.t || Date.now()).toISOString().slice(0,10);
    if (!byDay.has(d)) byDay.set(d, []);
    byDay.get(d).push(r);
  }
  const keys = Array.from(byDay.keys()).sort();
  if (keys.length < 2) return out;
  const prevKey = keys[keys.length-2];
  const prev = byDay.get(prevKey) || [];
  if (prev.length < 8) return out;
  const h = Math.max(...prev.map(x=>x.h));
  const l = Math.min(...prev.map(x=>x.l));
  const c = prev[prev.length-1].c;
  const pp = (h+l+c)/3;
  const r1 = 2*pp - l;
  const s1 = 2*pp - h;
  const r2 = pp + (h-l);
  const s2 = pp - (h-l);
  return { ok:true, pdh:r39Round(h), pdl:r39Round(l), pdc:r39Round(c), pp:r39Round(pp), r1:r39Round(r1), s1:r39Round(s1), r2:r39Round(r2), s2:r39Round(s2), rangePct:r39Round((h-l)/lp*100,2), dayKey:prevKey };
}
function r39Find5mOrderBlocks(k5m, lastPrice, atrPct=1) {
  const rows = r39K(k5m).slice(-80);
  const lp = Number(lastPrice||0);
  const out = { ok:false, bull:[], bear:[], nearestBull:null, nearestBear:null };
  if (rows.length < 18 || !(lp>0)) return out;
  const vols = rows.map(x=>x.v).filter(Number.isFinite);
  const vAvg = vols.slice(-40,-1).reduce((a,b)=>a+b,0) / Math.max(1, vols.slice(-40,-1).length);
  const dispThr = Math.max(0.22, Math.min(1.35, Number(atrPct||1)*0.22));
  const zones = { bull:[], bear:[] };
  for (let i=3; i<rows.length-3; i++) {
    const c = rows[i];
    const n1 = rows[i+1], n2 = rows[i+2], n3 = rows[i+3];
    const nextHigh = Math.max(n1.h,n2.h,n3.h), nextLow = Math.min(n1.l,n2.l,n3.l);
    const nextClose = n3.c;
    const volBoost = Math.max(n1.v,n2.v,n3.v) > vAvg * 1.15;
    const bullDisp = (nextHigh - c.o) / c.o * 100;
    const bearDisp = (c.o - nextLow) / c.o * 100;
    const isBearCandle = c.c < c.o;
    const isBullCandle = c.c > c.o;
    if (isBearCandle && bullDisp >= dispThr && nextClose > c.h && volBoost) {
      const z = { type:'BULL_OB_5M', high:r39Round(c.h), low:r39Round(c.l), mid:r39Round((c.h+c.l)/2), age:rows.length-1-i,
        distPct:r39Round((lp-c.l)/lp*100,2), displacement:r39Round(bullDisp,2) };
      const after = rows.slice(i+4);
      const broken = after.some(x => x.c < c.l * 0.998);
      if (!broken) zones.bull.push(z);
    }
    if (isBullCandle && bearDisp >= dispThr && nextClose < c.l && volBoost) {
      const z = { type:'BEAR_OB_5M', high:r39Round(c.h), low:r39Round(c.l), mid:r39Round((c.h+c.l)/2), age:rows.length-1-i,
        distPct:r39Round((c.h-lp)/lp*100,2), displacement:r39Round(bearDisp,2) };
      const after = rows.slice(i+4);
      const broken = after.some(x => x.c > c.h * 1.002);
      if (!broken) zones.bear.push(z);
    }
  }
  zones.bull = zones.bull.sort((a,b)=>Math.abs(a.mid-lp)-Math.abs(b.mid-lp)).slice(0,3);
  zones.bear = zones.bear.sort((a,b)=>Math.abs(a.mid-lp)-Math.abs(b.mid-lp)).slice(0,3);
  out.ok = true; out.bull = zones.bull; out.bear = zones.bear;
  out.nearestBull = zones.bull[0] || null;
  out.nearestBear = zones.bear[0] || null;
  return out;
}
function r39FiveMinuteSR(k5m, k1h, lastPrice, atrPct=1, vpvr1h=null, liq1h=null) {
  const rows = r39K(k5m).slice(-80);
  const lp = Number(lastPrice||0);
  const atr = Math.max(0.25, Number(atrPct)||1);
  const band = Math.max(0.18, Math.min(0.85, atr*0.28));
  const out = { ok:false, bandPct:+band.toFixed(2), pivots:r39DailyPivots(k1h, lp), ob5m:r39Find5mOrderBlocks(k5m, lp, atr), nearestSupport:null, nearestResistance:null, long:{}, short:{}, notes:[] };
  if (rows.length < 18 || !(lp>0)) return out;
  const recent = rows.slice(-36);
  const swingHighs=[], swingLows=[];
  for (let i=2;i<recent.length-2;i++) {
    const r=recent[i];
    if (r.h>recent[i-1].h&&r.h>recent[i-2].h&&r.h>recent[i+1].h&&r.h>recent[i+2].h) swingHighs.push({type:'5M_SWING_HIGH', price:r39Round(r.h), strength:1, age:recent.length-1-i});
    if (r.l<recent[i-1].l&&r.l<recent[i-2].l&&r.l<recent[i+1].l&&r.l<recent[i+2].l) swingLows.push({type:'5M_SWING_LOW', price:r39Round(r.l), strength:1, age:recent.length-1-i});
  }
  const supports=[], resistances=[];
  const addS=(type, price, strength=1, extra={})=>{ price=Number(price); if(price>0&&price<lp*1.003) supports.push({type, price:r39Round(price), distPct:r39Round((lp-price)/lp*100,2), strength, ...extra}); };
  const addR=(type, price, strength=1, extra={})=>{ price=Number(price); if(price>0&&price>lp*0.997) resistances.push({type, price:r39Round(price), distPct:r39Round((price-lp)/lp*100,2), strength, ...extra}); };
  for (const s of swingLows) addS(s.type, s.price, s.strength, {age:s.age});
  for (const r of swingHighs) addR(r.type, r.price, r.strength, {age:r.age});
  const piv = out.pivots;
  if (piv.ok) {
    for (const [type,price] of [['PDL',piv.pdl],['PIVOT_S1',piv.s1],['PIVOT_S2',piv.s2],['PIVOT_PP',piv.pp]]) addS(type, price, type==='PDL'?3:2);
    for (const [type,price] of [['PDH',piv.pdh],['PIVOT_R1',piv.r1],['PIVOT_R2',piv.r2],['PIVOT_PP',piv.pp]]) addR(type, price, type==='PDH'?3:2);
  }
  if (vpvr1h?.val) addS('VP_VAL', Number(vpvr1h.val), 2);
  if (vpvr1h?.poc) { addS('VP_POC', Number(vpvr1h.poc), 1.6); addR('VP_POC', Number(vpvr1h.poc), 1.6); }
  if (vpvr1h?.vah) addR('VP_VAH', Number(vpvr1h.vah), 2);
  if (liq1h?.sellLiq?.[0]?.price) addS('LIQ_SELL_POOL', Number(liq1h.sellLiq[0].price), 1.8, {liq:true});
  if (liq1h?.buyLiq?.[0]?.price) addR('LIQ_BUY_POOL', Number(liq1h.buyLiq[0].price), 1.8, {liq:true});
  for (const z of out.ob5m.bull || []) addS('5M_BULL_OB', z.low, 2.4, {zone:z});
  for (const z of out.ob5m.bear || []) addR('5M_BEAR_OB', z.high, 2.4, {zone:z});
  supports.sort((a,b)=>a.distPct-b.distPct || b.strength-a.strength);
  resistances.sort((a,b)=>a.distPct-b.distPct || b.strength-a.strength);
  const ns=supports[0]||null, nr=resistances[0]||null;
  const last = rows.at(-1), prev=rows.at(-2)||last;
  const body = Math.abs(last.c-last.o)/lp*100;
  const closeNearHigh = (last.h-last.c)/lp*100 < Math.max(0.08, body*0.45);
  const closeNearLow = (last.c-last.l)/lp*100 < Math.max(0.08, body*0.45);
  const breakRes = !!(nr && last.c > nr.price*(1+band/100*0.30) && last.c>last.o && closeNearHigh);
  const breakSup = !!(ns && last.c < ns.price*(1-band/100*0.30) && last.c<last.o && closeNearLow);
  const longSupportConfluence = !!(ns && ns.distPct <= band && ['PDL','PIVOT_S1','PIVOT_S2','VP_VAL','5M_BULL_OB','5M_SWING_LOW','LIQ_SELL_POOL'].includes(ns.type));
  const shortResistanceConfluence = !!(nr && nr.distPct <= band && ['PDH','PIVOT_R1','PIVOT_R2','VP_VAH','5M_BEAR_OB','5M_SWING_HIGH','LIQ_BUY_POOL'].includes(nr.type));
  const longNearResistance = !!(nr && nr.distPct <= band && !breakRes);
  const shortNearSupport = !!(ns && ns.distPct <= band && !breakSup);
  const longTargetTooNear = !!(nr && nr.distPct <= Math.max(0.22, band*0.75) && !breakRes);
  const shortTargetTooNear = !!(ns && ns.distPct <= Math.max(0.22, band*0.75) && !breakSup);
  out.ok=true; out.supports=supports.slice(0,6); out.resistances=resistances.slice(0,6); out.nearestSupport=ns; out.nearestResistance=nr;
  out.long={ supportConfluence:longSupportConfluence, nearResistance:longNearResistance, targetTooNear:longTargetTooNear, breakConfirmed:breakRes, nearestSupport:ns, nearestResistance:nr,
    reason:nr?`üst hedef ${nr.type} ${nr.distPct}%`:ns?`destek ${ns.type} ${ns.distPct}%`:'seviye yok' };
  out.short={ resistanceConfluence:shortResistanceConfluence, nearSupport:shortNearSupport, targetTooNear:shortTargetTooNear, breakConfirmed:breakSup, nearestSupport:ns, nearestResistance:nr,
    reason:ns?`alt hedef ${ns.type} ${ns.distPct}%`:nr?`direnç ${nr.type} ${nr.distPct}%`:'seviye yok' };
  return out;
}

async function getUnifiedScanCandidates(limit=24, mode='TOP24') {
  const scanMode = normalizeR54ScanMode(mode || limit);
  const lim = r54ScanLimitForMode(scanMode, limit);
  const EXCL = new Set(['BTCUSDT','ETHUSDT','BNBUSDT','XRPUSDT','SOLUSDT','ADAUSDT',
    'DOGEUSDT','DOTUSDT','MATICUSDT','LTCUSDT','TRXUSDT','AVAXUSDT','LINKUSDT','UNIUSDT','WBTCUSDT','SHIBUSDT']);

  const merged = new Map();
  let topLocked = new Map();
  try {
    const data = await cached('futures_tickers', FUTURES_TICKERS_CACHE_MS, () => bPub('/fapi/v1/ticker/24hr'));
    const onboardMap = await r152GetOnboardDateMap();
    topLocked = r152FilterAndExtendGainers(data, onboardMap, R33_TOP_GAINER_LOCK_COUNT);
    if (Array.isArray(data)) {
      for (const t of data) {
        if (!String(t.symbol||'').endsWith('USDT') || EXCL.has(String(t.symbol))) continue;
        const c = normalizeTickerToCoin(t);
        if (c.volume <= 20000000 || !c.price) continue;
        c.source = 'futures';
        const tg = topLocked.get(c.fullSymbol);
        merged.set(c.fullSymbol, tg ? {...c, ...tg, source:'futures+top_gainers_lock'} : c);
      }
    }
  } catch(e) {
    console.log('[R33_SCAN] futures_tickers/top_gainers alınamadı:', e.message);
  }

  for (const [sym, c] of topLocked.entries()) {
    const old = merged.get(sym) || {};
    merged.set(sym, { ...old, ...c, source: old.source ? old.source + '+top_gainers_lock' : 'top_gainers_lock' });
  }

  for (const v of (volatilityStore.coins || [])) {
    const c = normalizeTickerToCoin(v);
    if (!c.fullSymbol || !c.price) continue;
    const old = merged.get(c.fullSymbol) || {};
    merged.set(c.fullSymbol, {
      ...old,
      ...c,
      topGainerRank: old.topGainerRank,
      topGainerLocked: old.topGainerLocked,
      volume: Math.max(Number(old.volume||0), Number(c.volume||0)),
      trades: Math.max(Number(old.trades||0), Number(c.trades||0)),
      volScore: Math.max(Number(old.volScore||0), Number(c.volScore||0)),
      rangePct: Math.max(Number(old.rangePct||0), Number(c.rangePct||0)),
      source: old.source ? old.source + '+volatility' : 'volatility'
    });
  }

  const all = [...merged.values()].map(c => ({ ...c, interest: calcScanInterest(c), r54VolRank: r54VolatilityRankScore(c) }));
  const pinned = all
    .filter(c => c.topGainerLocked && Number(c.topGainerRank||999) <= R33_TOP_GAINER_LOCK_COUNT)
    .sort((a,b) => Number(a.topGainerRank||999) - Number(b.topGainerRank||999));
  const rest = all
    .filter(c => !(c.topGainerLocked && Number(c.topGainerRank||999) <= R33_TOP_GAINER_LOCK_COUNT))
    .sort((a,b) => (b.interest-a.interest) || (b.volScore-a.volScore) || (b.volume-a.volume));

  let rawOrdered;
  if (scanMode === 'FAST6') {
    const top3Ultra = pinned
      .slice(0, 10)
      .sort((a,b) => (b.r54VolRank-a.r54VolRank) || (b.rangePct-a.rangePct) || (b.interest-a.interest))
      .slice(0, 3)
      .map(c => ({ ...c, r54Bucket:'TOP3_ULTRA_VOL_IN_TOP10' }));
    const candidate3 = rest
      .filter(c => Number(c.price||0) > 0)
      .slice(0, 3)
      .map(c => ({ ...c, r54Bucket:'TOP10_CANDIDATE_3' }));
    rawOrdered = [...top3Ultra, ...candidate3];
  } else if (scanMode === 'TOP10') {
    rawOrdered = pinned.slice(0, 10).map(c => ({ ...c, r54Bucket:'TOP10_GAINER' }));
  } else {
    rawOrdered = [...pinned.map(c => ({ ...c, r54Bucket:'TOP24_PINNED_TOP10' })), ...rest.map(c => ({ ...c, r54Bucket:'TOP24_INTEREST' }))];
  }

  const seen = new Set();
  const ordered = [];
  for (const c of rawOrdered) {
    const key = c.fullSymbol;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    ordered.push(c);
    if (ordered.length >= lim) break;
  }

  try {
    await r309Add12hChange(ordered);
    const isPinned = (c) => /TOP24_PINNED_TOP10|TOP10_GAINER|TOP3_ULTRA/.test(String(c.r54Bucket||''));
    const r311yYesil = (c) => Number(c.change12h ?? c.change24h ?? 0) > 0;
    const R309V_MIN_12H = 5;
    const pinnedGrp = ordered.filter(c => isPinned(c) && r311yYesil(c))
      .sort((a,b) => (b.change12h ?? b.change24h) - (a.change12h ?? a.change24h));
    const restGrp   = ordered.filter(c => !isPinned(c))
      .filter(c => r311yYesil(c) && Number(c.change12h ?? c.change24h ?? 0) >= R309V_MIN_12H)
      .sort((a,b) => (b.change12h ?? b.change24h) - (a.change12h ?? a.change24h));
    let out = [...pinnedGrp, ...restGrp];
    try {
      const r348Boga  = out.filter(c => Number(c.change24h ?? 0) >= -3);
      const r348Tepki = out.filter(c => Number(c.change24h ?? 0) < -3);
      if (r348Boga.length) out = [...r348Boga, ...r348Tepki];
    } catch(_) {}
    if (out.length < 3) {
      const yedek = ordered
        .filter(c => !out.includes(c))
        .sort((a,b) => (b.change12h ?? b.change24h) - (a.change12h ?? a.change24h));
      for (const c of yedek) { if (out.length >= 3) break; out.push(c); }
    }
    return r326ApplySingleCoinLock(out);
  } catch (_) {
    return ordered;
  }
}

async function r309Add12hChange(coins) {
  if (!Array.isArray(coins) || coins.length === 0) return coins;
  await Promise.all(coins.map(async (c) => {
    const full = c.fullSymbol || (c.symbol ? c.symbol + 'USDT' : null);
    if (!full || !(Number(c.price) > 0)) { c.change12h = c.change24h; return; }
    try {
      const k = await cached(`k1h12_${full}`, 15*60*1000,
        () => bPub('/fapi/v1/klines', `symbol=${full}&interval=1h&limit=13`));
      if (Array.isArray(k) && k.length >= 2) {
        const open12h = Number(k[0][1]);
        const nowPrice = Number(c.price) || Number(k[k.length-1][4]);
        c.change12h = open12h > 0 ? +(((nowPrice - open12h) / open12h) * 100).toFixed(2) : c.change24h;
      } else {
        c.change12h = c.change24h;
      }
    } catch (_) {
      c.change12h = c.change24h;
    }
  }));
  return coins;
}

function r327PatlamaScore(c) {
  const ch = Number(c.change12h ?? c.change24h ?? 0);
  const range = Number(c.rangePct || 0);
  const trades = Number(c.trades || 0);
  let score = 0;
  if (ch > 5 && ch < 35) score += 2;
  if (range >= 10) score += 1;
  if (range >= 20) score += 1;
  if (trades > 50000) score += 1;
  return score;
}
function r326ApplySingleCoinLock(orderedList) {
  if (!Array.isArray(orderedList) || orderedList.length === 0) return orderedList;
  const top2 = orderedList.slice(0, 2);
  top2.sort((a,b) => r327PatlamaScore(b) - r327PatlamaScore(a));
  const tagged = top2.map((c,idx) => {
    const ps = r327PatlamaScore(c);
    return { ...c, r327Top2: true, r327Rank: idx+1, r327PatlamaScore: ps,
             r327Patlama: ps >= 3 };
  });
  const names = tagged.map(c => `${String(c.fullSymbol||'').replace('USDT','')}${c.r327Patlama?'🚀':''}`).join(', ');
  logAuto(`🎯 R327 TOP2 izleniyor: ${names} (patlama adayı 🚀 ile işaretli)`);
  return tagged;
}

app.get('/api/scan-candidates', async (req, res) => {
  try {
    const serverMode = autoConfig?.scanMode || autoScanState?.settings?.scanMode || autoScanState?.scanMode || 'TOP10';
    const mode = normalizeR54ScanMode(req.query.mode || req.query.scanMode || serverMode);
    const limit = r54ScanLimitForMode(mode, req.query.limit || r54ScanLimitForMode(mode, 6));
    const coins = await getUnifiedScanCandidates(limit, mode);
    res.json({ ok:true, count:coins.length, limit, scanMode:mode, coins, source:'R55_SYNCED_SCANMODE_LIST' });
  } catch(e) { res.status(400).json({ ok:false, error:e.message }); }
});

app.get('/api/futures-coins', async (req, res) => {
  try {
    const data = await cached('futures_tickers', FUTURES_TICKERS_CACHE_MS, () => bPub('/fapi/v1/ticker/24hr'));
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

app.get('/api/analyze/:symbol', async (req, res) => {
  const sym  = req.params.symbol.toUpperCase();
  const full = sym.endsWith('USDT') ? sym : sym+'USDT';

  try {
    startCVDStream(full);
    startIcebergStream(full);

    const [r4h,r1h,r15m,r5m,rFunding,rOIHist,rLS_global,rLS_top,rDepth,rTaker,rOIHist5m,rOINow,rBtc5m,r1d,r1m] =
      await Promise.allSettled([
        cached(`k4h_${full}`,  60*60*1000, ()=>bPub('/fapi/v1/klines',`symbol=${full}&interval=4h&limit=200`)),
        cached(`k1h_${full}`,   15*60*1000, ()=>bPub('/fapi/v1/klines',`symbol=${full}&interval=1h&limit=200`)),
        cached(`k15m_${full}`, 90*1000, ()=>bPub('/fapi/v1/klines',`symbol=${full}&interval=15m&limit=200`)),
        cached(`k5m_${full}`,  45*1000, ()=>bPub(`/fapi/v1/klines`,`symbol=${full}&interval=5m&limit=200`)),
        cached(`fund_${full}`, 30*60*1000, ()=>bPub('/fapi/v1/fundingRate',`symbol=${full}&limit=10`)),
        cached(`oih_${full}`,  15*60*1000, ()=>bPub('/futures/data/openInterestHist',`symbol=${full}&period=1h&limit=24`)),
        cached(`lsg_${full}`,  15*60*1000, ()=>bPub('/futures/data/globalLongShortAccountRatio',`symbol=${full}&period=1h&limit=12`)),
        cached(`lst_${full}`,  15*60*1000, ()=>bPub('/futures/data/topLongShortPositionRatio',`symbol=${full}&period=1h&limit=12`)),
        cached(`dep_${full}`,  60*1000, ()=>bPub('/fapi/v1/depth',`symbol=${full}&limit=100`)),
        cached(`tak_${full}`,  5*60*1000, ()=>bPub('/futures/data/takerlongshortRatio',`symbol=${full}&period=5m&limit=6`)),
        cached(`oih5_${full}`, 90*1000, ()=>bPub('/futures/data/openInterestHist',`symbol=${full}&period=5m&limit=12`)),
        cached(`oin_${full}`,  60*1000, ()=>bPub('/fapi/v1/openInterest',`symbol=${full}`)),
        cached('btc5m_r29_ctx', 45*1000, () => bPub('/fapi/v1/klines', `symbol=BTCUSDT&interval=5m&limit=24`)),
        cached(`k1d_${full}`, 4*60*60*1000, ()=>bPub('/fapi/v1/klines',`symbol=${full}&interval=1d&limit=30`)),
        cached(`k1m_${full}`, 20*1000, ()=>bPub('/fapi/v1/klines',`symbol=${full}&interval=1m&limit=60`)),
      ]);

    const k4h  = r4h.status==='fulfilled'&&Array.isArray(r4h.value)   ?r4h.value  :[];
    const k1d  = r1d.status==='fulfilled'&&Array.isArray(r1d.value)   ?r1d.value  :[];
    const k1m  = r1m.status==='fulfilled'&&Array.isArray(r1m.value)   ?r1m.value  :[];
    const k1h  = r1h.status==='fulfilled'&&Array.isArray(r1h.value)   ?r1h.value  :[];
    const k15m = r15m.status==='fulfilled'&&Array.isArray(r15m.value) ?r15m.value :[];
    const k5m  = r5m.status==='fulfilled'&&Array.isArray(r5m.value)   ?r5m.value  :[];
    const fundArr  = rFunding.status==='fulfilled'&&Array.isArray(rFunding.value)   ?rFunding.value  :[];
    const oiHist   = rOIHist.status==='fulfilled'&&Array.isArray(rOIHist.value)     ?rOIHist.value   :[];
    const lsGlobal = rLS_global.status==='fulfilled'&&Array.isArray(rLS_global.value)?rLS_global.value:[];
    const lsTop    = rLS_top.status==='fulfilled'&&Array.isArray(rLS_top.value)      ?rLS_top.value   :[];
    const depth    = rDepth.status==='fulfilled'?rDepth.value:{bids:[],asks:[]};
    const takerArr = rTaker?.status==='fulfilled'&&Array.isArray(rTaker.value)?rTaker.value:[];
    const oiHist5m = rOIHist5m?.status==='fulfilled'&&Array.isArray(rOIHist5m.value)?rOIHist5m.value:[];
    const oiNowObj = rOINow?.status==='fulfilled'&&rOINow.value?rOINow.value:null;

    const lastPrice = k5m.length?parseFloat(k5m[k5m.length-1][4]):(k15m.length?parseFloat(k15m[k15m.length-1][4]):0);
    const lastTime  = k15m.length?parseInt(k15m[k15m.length-1][6]):Date.now();

    let r38MarketCtx = { topMover:false, topRank:null, change24h:0, volume:0, fearSoft:true };
    try {
      const _tickers38 = await cached('futures_tickers', FUTURES_TICKERS_CACHE_MS, () => bPub('/fapi/v1/ticker/24hr'));
      if (Array.isArray(_tickers38)) {
        const _top38 = r33TopGainersFromTickers(_tickers38, R33_TOP_GAINER_LOCK_COUNT);
        const _t38 = _tickers38.find(t => String(t.symbol||'').toUpperCase() === full);
        const _chg38 = Number(_t38?.priceChangePercent || 0);
        const _vol38 = Number(_t38?.quoteVolume || 0);
        const _locked38 = _top38.get(full);
        r38MarketCtx = {
          topMover: !!_locked38 || Math.abs(_chg38) >= 6 || _vol38 >= 100000000,
          topRank: _locked38?.topGainerRank || null,
          change24h: _chg38,
          volume: _vol38,
          fearSoft: true
        };
      }
    } catch(_e) {}
    const r38TopMoverStrong = !!(r38MarketCtx.topMover || Math.abs(Number(r38MarketCtx.change24h||0)) >= 6 || Number(r38MarketCtx.volume||0) >= 100000000);

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
    const atrPct=lastPrice>0?(atr1h/lastPrice)*100:1;
    const vwap1h=vwap(k1h),vwap4h=vwap(k4h);
    const bb1h=bollinger(k1h),bb15m_=bollinger(k15m);
    const bb5m=bollinger(k5m);
    const bb5mPos = (bb5m.upper > bb5m.lower) ? Math.round((lastPrice - bb5m.lower)/(bb5m.upper - bb5m.lower)*100) : 50;
    const bb5mDurum = bb5m.width < 2.5 ? 'SIKIŞMA (patlama yakıtı — yön bekle)' : bb5mPos >= 100 ? 'fiyat ÜST bant DIŞINDA (aşırı uzama, dönüş/geri-çekilme riski)' : bb5mPos <= 0 ? 'fiyat ALT bant DIŞINDA (aşırı uzama, tepki/dönüş riski)' : bb5mPos >= 80 ? 'üst banda yakın (%'+bb5mPos+', premium)' : bb5mPos <= 20 ? 'alt banda yakın (%'+bb5mPos+', discount)' : 'bant ortası (%'+bb5mPos+', net konum yok)';

    const r321MomentumZayiflama = (function(){
      try {
        const closes = k5m.map(k=>parseFloat(k[4]));
        if (closes.length < 35) return null;
        const emaArr = (arr,p)=>{ const k=2/(p+1); let e=arr[0]; const out=[e]; for(let i=1;i<arr.length;i++){e=arr[i]*k+e*(1-k);out.push(e);} return out; };
        const ema12=emaArr(closes,12), ema26=emaArr(closes,26);
        const macdLine=closes.map((_,i)=>ema12[i]-ema26[i]);
        const signalArr=emaArr(macdLine.slice(-20),9);
        const hist=macdLine.slice(-20).map((m,i)=>m-signalArr[i]);
        const n=hist.length;
        if (n<4) return null;
        const h0=hist[n-1], h1=hist[n-2], h2=hist[n-3];
        const bullZayifliyor = h2>0 && h1>0 && h1>h2 && h0<h1 && h0>0;
        const bearZayifliyor = h2<0 && h1<0 && h1<h2 && h0>h1 && h0<0;
        const bullKesti = h1<=0 && h0>0;
        const bearKesti = h1>=0 && h0<0;
        if (bullKesti) return 'momentum YUKARI döndü (MACD hist sıfırı yukarı kesti — yükseliş başlıyor olabilir)';
        if (bearKesti) return 'momentum AŞAĞI döndü (MACD hist sıfırı aşağı kesti — düşüş başlıyor olabilir)';
        if (bullZayifliyor) return 'YÜKSELİŞ momentumu ZAYIFLIYOR (MACD hist tepe yapıp düşüyor — yükselen trende LONG geç olabilir, SHORT dönüş izle)';
        if (bearZayifliyor) return 'DÜŞÜŞ momentumu ZAYIFLIYOR (MACD hist dip yapıp yükseliyor — düşen trende SHORT geç olabilir, LONG dönüş izle)';
        return null;
      } catch(_){ return null; }
    })();

    const r321InvertedFVG = (function(){
      try {
        const win = k5m.slice(-30);
        if (win.length < 10) return null;
        const o=i=>parseFloat(win[i][1]), h=i=>parseFloat(win[i][2]), l=i=>parseFloat(win[i][3]), c=i=>parseFloat(win[i][4]);
        const atrApprox = (h(win.length-1)-l(win.length-1)) || lastPrice*0.005;
        const buf = atrApprox * 0.05;
        for (let i=win.length-3; i>=2; i--){
          if (l(i) > h(i-2) + buf){
            const fvgTop=l(i), fvgBot=h(i-2);
            for (let j=i+1; j<win.length; j++){
              if (c(j) < fvgBot - buf){
                if (lastPrice < fvgBot && lastPrice > fvgBot*0.985)
                  return `inverted FVG DİRENÇ yakın (${fvgBot.toFixed(6)} — eski bull FVG aşağı delindi, şimdi direnç; LONG zorlanabilir)`;
                break;
              }
            }
          }
          if (h(i) < l(i-2) - buf){
            const fvgTop=l(i-2), fvgBot=h(i);
            for (let j=i+1; j<win.length; j++){
              if (c(j) > fvgTop + buf){
                if (lastPrice > fvgTop && lastPrice < fvgTop*1.015)
                  return `inverted FVG DESTEK yakın (${fvgTop.toFixed(6)} — eski bear FVG yukarı delindi, şimdi destek; SHORT zorlanabilir)`;
                break;
              }
            }
          }
        }
        return null;
      } catch(_){ return null; }
    })();

    let btc5mCtx = { ok:false, change15m:0, change60m:0, dropping:false, bouncing:false, redCandles:0 };
    try {
      const btc5m = rBtc5m?.status==='fulfilled'&&Array.isArray(rBtc5m.value)?rBtc5m.value:null;
      if (btc5m && btc5m.length >= 13) {
        const bLast = Number(btc5m.at(-1)[4]);
        const b3 = Number(btc5m.at(-4)[1]);
        const b12 = Number(btc5m.at(-13)[1]);
        let redCount = 0;
        for (let i = btc5m.length-1; i >= Math.max(0,btc5m.length-5); i--) {
          if (Number(btc5m[i][4]) < Number(btc5m[i][1])) redCount++;
          else break;
        }
        const chg15 = b3>0 ? +(((bLast-b3)/b3)*100).toFixed(3) : 0;
        const chg60 = b12>0 ? +(((bLast-b12)/b12)*100).toFixed(3) : 0;
        const lastGreen  = Number(btc5m.at(-1)[4]) > Number(btc5m.at(-1)[1]);
        const prevRed    = Number(btc5m.at(-2)[4]) < Number(btc5m.at(-2)[1]);
        btc5mCtx = {
          ok:true,
          change15m: chg15,
          change60m: chg60,
          redCandles: redCount,
          dropping: redCount >= 3 && chg15 < -0.3,
          bouncing:  lastGreen && prevRed && chg15 > -0.5,
        };
      }
    } catch(_) {}

    const signalAgeMs=Date.now()-lastTime;
    const signalAgeMin=Math.floor(signalAgeMs/60000);
    const maxValidMin=240;
    const isExpired=signalAgeMin>maxValidMin;
    const freshness=isExpired?'EXPIRED':signalAgeMin<15?'FRESH':signalAgeMin<60?'VALID':'AGING';
    let freshnessMult=freshness==='EXPIRED'?0:freshness==='AGING'?0.75:1;

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
    const r110ICT = r110AnalyzeICT(k5m, k15m, k1h, k4h, lastPrice);

    const r140Phase  = r140PumpPhase(k5m, atrPct);
    const r140EqHL   = r140EqualLevels(k5m, k1h, lastPrice);
    const r140OiVel  = r140OiVelocity(oiHist5m, lastPrice, k5m.length>=2?k5m.at(-2)[4]:lastPrice);
    const _coin15m   = (k5m.length>=4&&Number(k5m.at(-4)[1])>0) ? ((lastPrice-Number(k5m.at(-4)[1]))/Number(k5m.at(-4)[1])*100) : 0;
    const _coin60m   = (k5m.length>=13&&Number(k5m.at(-13)[1])>0) ? ((lastPrice-Number(k5m.at(-13)[1]))/Number(k5m.at(-13)[1])*100) : 0;
    const r140BtcDiv = r140BtcDivergence(btc5mCtx, _coin15m, _coin60m);
    const r140Rvol   = r140CoinRvol(k5m);
    const r111Siksma = r111AnalyzeSiksmaFlow(k5m, k1h, k4h, lastPrice, fundArr, lsGlobal, lsTop, takerArr, oiHist5m, oiNowObj);

    function detectAMD(klines5m, klines15m, tickSweepData) {
      if (klines5m.length < 20 || klines15m.length < 10)
        return { phase:'UNKNOWN', signal:'NONE', entry:null };

      const k5  = klines5m.slice(-20);
      const k15 = klines15m.slice(-10);

      const bias15m = parseFloat(k15[k15.length-1][4]) >
                      parseFloat(k15[0][4]) ? 'BULLISH' : 'BEARISH';

      const acc = k5.slice(0, 8);
      const accHighs = acc.map(k=>parseFloat(k[2]));
      const accLows  = acc.map(k=>parseFloat(k[3]));
      const accHigh  = Math.max(...accHighs);
      const accLow   = Math.min(...accLows);
      const accRange = (accHigh - accLow) / lastPrice * 100;

      const isAccumulating = accRange < 1.5;

      if (!isAccumulating) return { phase:'NO_RANGE', signal:'NONE', bias:bias15m };

      const recent = k5.slice(-6);
      let manipSweep = null, mssCandle = null;

      for (let i = 0; i < recent.length - 1; i++) {
        const c = recent[i];
        const h = parseFloat(c[2]), l = parseFloat(c[3]);
        const o = parseFloat(c[1]), cl = parseFloat(c[4]);

        if (bias15m === 'BULLISH' && l < accLow * 0.999 && cl > accLow) {
          manipSweep = { type:'BULL_MANIP', sweepLevel: accLow, price: l, idx: i };
          const next = recent[i+1];
          if (next && parseFloat(next[4]) > parseFloat(next[1])) {
            mssCandle = { type:'BULL_MSS', price: parseFloat(next[4]) };
          }
        }

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

      const isBull = manipSweep.type === 'BULL_MANIP';
      let fvg = null;
      for (let i = 1; i < recent.length - 1; i++) {
        const prev = recent[i-1], curr = recent[i], next2 = recent[i+1];
        if (!prev || !next2) continue;
        if (isBull && parseFloat(prev[2]) < parseFloat(next2[3])) {
          fvg = { high: parseFloat(next2[3]), low: parseFloat(prev[2]),
            mid: (parseFloat(next2[3]) + parseFloat(prev[2])) / 2 };
        }
        if (!isBull && parseFloat(prev[3]) > parseFloat(next2[2])) {
          fvg = { high: parseFloat(prev[3]), low: parseFloat(next2[2]),
            mid: (parseFloat(prev[3]) + parseFloat(next2[2])) / 2 };
        }
      }

      const entryZone = fvg ? fvg.mid : mssCandle.price;
      const signal = isBull ? 'AMD_LONG' : 'AMD_SHORT';

      const tickConfirm = tickSweepData &&
        ((isBull && tickSweepData.type==='BULL_SWEEP') ||
         (!isBull && tickSweepData.type==='BEAR_SWEEP'));

      return {
        phase: 'DISTRIBUTION', signal, bias: bias15m,
        manipSweep, mssCandle, fvg, entryZone,
        accHigh, accLow, accRange: +accRange.toFixed(2),
        tickConfirm,
        msg: isBull
          ? `AMD Long: Range[${accLow.toFixed(4)}-${accHigh.toFixed(4)}] → Sweep Alt → MSS ↑${tickConfirm?' ⚡Tick✅':''}`
          : `AMD Short: Range[${accLow.toFixed(4)}-${accHigh.toFixed(4)}] → Sweep Üst → MSS ↓${tickConfirm?' ⚡Tick✅':''}`
      };
    }

    r130StartCombinedAggTradeStream([full]);
    const tickEng = r130EnsureTickEngine(full);
    if (tickEng && k5m.length > 0) {
      updateSwingLevels(tickEng.sweepDet, k5m);
    }
    const tickData = getTickAnalysis(full);
    const amd5m = detectAMD(k5m, k15m, tickData?.tickSweep);

    const _r32AtrPct = lastPrice > 0 ? (atr1h / lastPrice) * 100 : 1;
    const _cdl5m = cdlPatterns(k5m);
    const _cdl1h = cdlPatterns(k1h);
    const _ha5m  = haSignal(calcHeikinAshi(k5m));
    const _ha1h  = haSignal(calcHeikinAshi(k1h));
    const _chart = detectChartPatterns(k1h, lastPrice, _r32AtrPct);
    const _harm  = detectHarmonicPatterns(k4h, lastPrice);
    const _renko = renkoSignal(k1h, lastPrice, atr1h);

    function detectSweepAndConfirm(klines, liqLevels, n=20) {
      if (klines.length < n) return { swept:false, confirmed:false, direction:'NONE' };
      const recent = klines.slice(-n);

      for (let i = recent.length-5; i < recent.length-1; i++) {
        const sweepCandle = recent[i];
        const confirmCandle = recent[i+1];
        const sh = parseFloat(sweepCandle[2]);
        const sl = parseFloat(sweepCandle[3]);
        const so = parseFloat(sweepCandle[1]);
        const sc = parseFloat(sweepCandle[4]);
        const co = parseFloat(confirmCandle[1]);
        const cc = parseFloat(confirmCandle[4]);

        for (const liq of liqLevels.sellLiq || []) {
          if (sl < liq.price && sc > liq.price) {
            const bullConfirm = cc > co;
            const strongConfirm = cc > Math.max(so, sc);
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
                candleAge: recent.length - 1 - (i+1),
              };
            } else {
              return { swept:true, confirmed:false, direction:'BULL_SWEEP_UNCONFIRMED',
                msg:'Alt sweep var ama teyit bekleniyor' };
            }
          }
        }

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

    function detectStopHunt(klines, n=10) {
      const r = detectSweepAndConfirm(klines, {buyLiq:liq1h.buyLiq, sellLiq:liq1h.sellLiq}, n);
      return { hunted: r.swept||r.hunted||false, direction: r.direction==='BULL_SWEEP'||r.direction==='BULL_HUNT'?'BULL_HUNT':r.direction==='BEAR_SWEEP'||r.direction==='BEAR_HUNT'?'BEAR_HUNT':'NONE', msg: r.msg };
    }

    const sweep1h  = detectSweepAndConfirm(k1h,  {buyLiq:liq1h.buyLiq,  sellLiq:liq1h.sellLiq},  20);
    const sweep4h  = detectSweepAndConfirm(k4h,  {buyLiq:liq4h.buyLiq,  sellLiq:liq4h.sellLiq},  20);
    const sweep15m = detectSweepAndConfirm(k15m, {buyLiq:liq1h.buyLiq,  sellLiq:liq1h.sellLiq},  10);
    const hunt1h   = detectStopHunt(k1h, 15);
    const hunt15m  = detectStopHunt(k15m, 8);

    function r22SideSweepRows(isLong) {
      const dir = isLong ? 'BULL_SWEEP' : 'BEAR_SWEEP';
      const rows = [];
      if (sweep15m?.confirmed && sweep15m.direction === dir) rows.push({ tf:'15m', minutesPerCandle:15, ...sweep15m });
      if (sweep1h?.confirmed  && sweep1h.direction  === dir) rows.push({ tf:'1h',  minutesPerCandle:60, ...sweep1h  });
      if (sweep4h?.confirmed  && sweep4h.direction  === dir) rows.push({ tf:'4h',  minutesPerCandle:240,...sweep4h  });
      return rows;
    }
    function r22SignalDecayForSide(isLong) {
      const tickDir = isLong ? 'BULL_SWEEP' : 'BEAR_SWEEP';
      const tickFresh = !!(tickData?.tickSweep?.fresh && tickData?.tickSweep?.type === tickDir);
      const amdFresh = !!(amd5m?.tickConfirm && amd5m?.signal === (isLong ? 'AMD_LONG' : 'AMD_SHORT'));
      if (tickFresh || amdFresh) return { ageMin:0, mult:1, penalty:0, bonus:6, noAuto:false, label:'0-3dk taze tick/AMD' };
      const rows = r22SideSweepRows(isLong);
      if (!rows.length) return { ageMin:null, mult:1, penalty:0, bonus:0, noAuto:false, label:'sweep yok' };
      const ageMin = Math.min(...rows.map(r => Math.max(0, Number(r.candleAge||0) * Number(r.minutesPerCandle||15))));
      if (ageMin <= 3)  return { ageMin, mult:1.00, penalty:0,  bonus:5, noAuto:false, label:'0-3dk taze' };
      if (ageMin <= 6)  return { ageMin, mult:0.80, penalty:4,  bonus:0, noAuto:false, label:'3-6dk geçerli' };
      if (ageMin <= 10) return { ageMin, mult:0.60, penalty:8,  bonus:0, noAuto:false, label:'6-10dk zayıflıyor' };
      return { ageMin, mult:0.65, penalty:10, bonus:0, noAuto:true, label:'10dk+ bayat: auto yok (R60: skor çarpanı yumuşatıldı)' };
    }
    function r22TestQualityForSide(isLong) {
      const rows = r22SideSweepRows(isLong);
      if (!rows.length) return { count:0, label:'test yok', bonus:0, penalty:0, first:false, second:false, late:false };
      const best = rows.sort((a,b)=>(Number(a.candleAge||99)*Number(a.minutesPerCandle||15))-(Number(b.candleAge||99)*Number(b.minutesPerCandle||15)))[0];
      const count = Math.max(1, Number(best.sweepStrength || 1));
      if (count <= 1) return { count, tf:best.tf, label:'İlk test / taze sweep', bonus:8, penalty:0, first:true, second:false, late:false };
      if (count === 2) return { count, tf:best.tf, label:'2. test hâlâ geçerli', bonus:3, penalty:0, first:false, second:true, late:false };
      return { count, tf:best.tf, label:'3+ test: seviye yıprandı', bonus:0, penalty:8, first:false, second:false, late:true };
    }

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
        if(l<rangeLow*0.999&&c>rangeLow&&nextC>c&&v>avgVol*1.2)
          events.push({type:'SPRING',price:l,candleIdx:i,depth:+((rangeLow-l)/rangeSize*100).toFixed(1),msg:'Spring! MM dip topladı ↑',signal:'STRONG_LONG'});
        if(highs[i]>rangeHigh*1.001&&c<rangeHigh&&nextC<c&&v>avgVol*1.3)
          events.push({type:'UTAD',price:highs[i],candleIdx:i,msg:'UTAD sahte kırılım ↓',signal:'STRONG_SHORT'});
      }
      for(let i=20;i<recent.length;i++){
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

    const curFund=fundArr.length?parseFloat(fundArr[fundArr.length-1].fundingRate)*100:0;
    const fundSig=curFund<-0.05?'EXTREME_NEGATIVE':curFund<-0.01?'NEGATIVE':curFund>0.1?'EXTREME_POSITIVE':curFund>0.05?'POSITIVE':'NEUTRAL';

    let oiChg1h=0,oiChg4h=0;
    if(oiHist.length>=2){
      const fn=x=>parseFloat(x.sumOpenInterestValue||x.sumOpenInterest||0);
      const lat=fn(oiHist[oiHist.length-1]),h1=fn(oiHist[oiHist.length-2]),h4=fn(oiHist[Math.max(0,oiHist.length-5)]);
      oiChg1h=h1>0?(lat-h1)/h1*100:0;oiChg4h=h4>0?(lat-h4)/h4*100:0;
    }
    const p1hChg=k1h.length>=2?(parseFloat(k1h[k1h.length-1][4])-parseFloat(k1h[k1h.length-2][4]))/parseFloat(k1h[k1h.length-2][4])*100:0;
    const oiDiv=oiChg1h>1&&p1hChg>0.5?'CONFIRMED_BULL':oiChg1h>1&&p1hChg<-0.5?'CONFIRMED_BEAR':oiChg1h<-1&&p1hChg>0.5?'SHORT_SQUEEZE':oiChg1h<-1&&p1hChg<-0.5?'LONG_LIQUIDATION':'NEUTRAL';

    const globalLong=lsGlobal.length?parseFloat(lsGlobal[lsGlobal.length-1].longAccount||0.5)*100:50;
    const topLong   =lsTop.length?parseFloat(lsTop[lsTop.length-1].longAccount||0.5)*100:50;
    const smDiv=topLong>55&&globalLong<45?'SMART_BULL':topLong<45&&globalLong>55?'SMART_BEAR':topLong>60?'WHALE_LONG':topLong<40?'WHALE_SHORT':'NEUTRAL';

    const bids=Array.isArray(depth.bids)?depth.bids.slice(0,50):[];
    const asks=Array.isArray(depth.asks)?depth.asks.slice(0,50):[];
    let totBid=0,totAsk=0;
    let nearBid=0,nearAsk=0;
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

    const weightedBid = totBid*0.4 + nearBid*0.6;
    const weightedAsk = totAsk*0.4 + nearAsk*0.6;
    const bookImb = weightedBid+weightedAsk>0 ? (weightedBid-weightedAsk)/(weightedBid+weightedAsk)*100 : 0;

    const bidWallPct = wallBidSize/totBid*100;
    const askWallPct = wallAskSize/totAsk*100;

    const cvd     = getCVD(full);
    const iceberg = getIceberg(full);
    const liqData = getLiqData(full);
    const r125Flow = r125BuildOrderflowContext(full, lastPrice, depth, tickData, liqData);

    const cgData = (cache.has(`cg_${full}`) ? cache.get(`cg_${full}`)?.val : null) ?? null;

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
    if(iceberg.signal==='STRONG_HIDDEN_BUY')  {mmConf+=15;mmReasoning.push('WS Iceberg gizli alıcı');}
    if(iceberg.signal==='STRONG_HIDDEN_SELL') {mmConf+=15;mmReasoning.push('WS Iceberg gizli satıcı');}
    mmConf=Math.min(mmConf,95);
    const mmNextTarget=mmTarget.includes('UP')?(liq1h.buyLiq[0]?.price||0):(liq1h.sellLiq[0]?.price||0);

    function calcProTPSL(side,price,atr1h,atr4h,liq1h,liq4h,ob1h,ob4h,k1h){
      const isLong=side==='LONG';
      let sl=0,tp=0,tp2=0;const reasons={};
      const recent15=k1h.slice(-15);
      if(isLong){
        const swingLow=Math.min(...recent15.map(k=>parseFloat(k[3])));
        const atrSL=price-1.5*atr1h;
        const rawSL=Math.max(swingLow - 0.3*atr1h, atrSL);
        sl=rawSL;
        if(ob1h.bullOB&&ob1h.bullOB.low>sl&&ob1h.bullOB.low<price*0.99){sl=ob1h.bullOB.low*0.996;reasons.sl=`1h Bull OB altı -0.4%`;}
        else reasons.sl=`Swing Low -0.3×ATR (MM buffer)`;
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
        const rawSL=Math.min(swingHigh + 0.3*atr1h, atrSL);
        sl=rawSL;
        if(ob1h.bearOB&&ob1h.bearOB.high<sl&&ob1h.bearOB.high>price*1.01){sl=ob1h.bearOB.high*1.004;reasons.sl='1h Bear OB üstü +0.4%';}
        else reasons.sl='Swing High +0.3×ATR (MM buffer)';
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
    const t4up=ema20_4h>ema50_4h&&ema50_4h>ema200_4h;
    const t4dn=ema20_4h<ema50_4h&&ema50_4h<ema200_4h;

    function calcSqueeze(kl, lenKC=20, multKC=1.5, lenBB=20, multBB=2.0) {
      if(!kl||kl.length<Math.max(lenBB,lenKC)+5)return{ok:false};
      const c=kl.slice(-lenBB).map(k=>parseFloat(k[4]));
      const h=kl.slice(-lenBB).map(k=>parseFloat(k[2]));
      const lo=kl.slice(-lenBB).map(k=>parseFloat(k[3]));
      const midBB=c.reduce((a,b)=>a+b,0)/c.length;
      const stdBB=Math.sqrt(c.reduce((s,v)=>s+Math.pow(v-midBB,2),0)/c.length);
      const trArr=kl.slice(-lenKC).map((k,i,a)=>{
        if(i===0)return parseFloat(k[2])-parseFloat(k[3]);
        const p=parseFloat(a[i-1][4]);
        return Math.max(parseFloat(k[2])-parseFloat(k[3]),Math.abs(parseFloat(k[2])-p),Math.abs(parseFloat(k[3])-p));
      });
      const atrKC=trArr.reduce((a,b)=>a+b,0)/trArr.length;
      const midKC=c.reduce((a,b)=>a+b,0)/c.length;
      const sqzOn=midBB+multBB*stdBB<midKC+multKC*atrKC && midBB-multBB*stdBB>midKC-multKC*atrKC;
      const hh=Math.max(...h),ll=Math.min(...lo);
      const mom=c[c.length-1]-((hh+ll)/2+midBB)/2;
      const prev=c[c.length-2]-((hh+ll)/2+midBB)/2;
      const dir=mom>0?'BULL':'BEAR';
      const accel=mom>prev?'GROWING':'FADING';
      return{ok:true,squeeze:sqzOn,momentum:+mom.toFixed(8),direction:dir,acceleration:accel,
        signal:sqzOn&&accel==='GROWING'?`SQ_${dir}`:`${dir}_FREE`};
    }

    function calcCMF(kl, period=20) {
      if(!kl||kl.length<period)return{ok:false};
      const r=kl.slice(-period);
      let mfv=0,tv=0;
      for(const k of r){
        const h=parseFloat(k[2]),lo=parseFloat(k[3]),c=parseFloat(k[4]),v=parseFloat(k[5]);
        const mfm=h===lo?0:((c-lo)-(h-c))/(h-lo);
        mfv+=mfm*v; tv+=v;
      }
      const cmf=tv>0?mfv/tv:0;
      const sig=cmf>0.15?'STRONG_BUY':cmf>0.07?'BUY':cmf<-0.15?'STRONG_SELL':cmf<-0.07?'SELL':'NEUTRAL';
      return{ok:true,value:+cmf.toFixed(4),signal:sig};
    }

    function calcEWO(kl) {
      if(!kl||kl.length<40)return{ok:false};
      const c=kl.map(k=>parseFloat(k[4]));
      function e(arr,p){const k=2/(p+1);let v=arr.slice(0,p).reduce((a,b)=>a+b,0)/p;for(let i=p;i<arr.length;i++)v=arr[i]*k+v*(1-k);return v;}
      const ewo=(e(c,5)-e(c,35))/c[c.length-1]*100;
      const prev=(e(c.slice(0,-1),5)-e(c.slice(0,-1),35))/c[c.length-2]*100;
      return{ok:true,value:+ewo.toFixed(3),growing:ewo>prev,
        signal:ewo>0.2&&ewo>prev?'BULL_WAVE':ewo<-0.2&&ewo<prev?'BEAR_WAVE':'NEUTRAL'};
    }

    function calcWeisWave(kl) {
      if(!kl||kl.length<15)return{ok:false};
      const r=kl.slice(-20);
      let cur={dir:null,vol:0};const waves=[];
      for(const k of r){
        const o=parseFloat(k[1]),c=parseFloat(k[4]),v=parseFloat(k[5]);
        const d=c>=o?'UP':'DOWN';
        if(cur.dir===d){cur.vol+=v;}
        else{if(cur.dir)waves.push({...cur});cur={dir:d,vol:v};}
      }
      if(cur.dir)waves.push({...cur});
      if(waves.length<3)return{ok:false};
      const lastUp=[...waves].reverse().find(w=>w.dir==='UP');
      const lastDn=[...waves].reverse().find(w=>w.dir==='DOWN');
      const ratio=lastUp&&lastDn?lastUp.vol/Math.max(lastDn.vol,1):1;
      return{ok:true,ratio:+ratio.toFixed(2),upVol:+lastUp?.vol.toFixed(0)||0,dnVol:+lastDn?.vol.toFixed(0)||0,
        signal:ratio>1.5?'BULL_EFFORT':ratio<0.67?'BEAR_EFFORT':'NEUTRAL'};
    }

    function detectChoCH(kl) {
      if(!kl||kl.length<20)return{ok:false};
      const r=kl.slice(-20);
      const sH=[],sL=[];
      for(let i=2;i<r.length-2;i++){
        const h=parseFloat(r[i][2]),lo=parseFloat(r[i][3]);
        if(h>parseFloat(r[i-1][2])&&h>parseFloat(r[i-2][2])&&h>parseFloat(r[i+1][2])&&h>parseFloat(r[i+2][2]))sH.push(h);
        if(lo<parseFloat(r[i-1][3])&&lo<parseFloat(r[i-2][3])&&lo<parseFloat(r[i+1][3])&&lo<parseFloat(r[i+2][3]))sL.push(lo);
      }
      if(sH.length<2||sL.length<2)return{ok:false};
      const bullChoCH=sH[sH.length-1]>sH[sH.length-2]&&sL[sL.length-1]>sL[sL.length-2];
      const bearChoCH=sL[sL.length-1]<sL[sL.length-2]&&sH[sH.length-1]<sH[sH.length-2];
      return{ok:true,bullChoCH,bearChoCH,
        signal:bullChoCH?'BULL_CHOCH':bearChoCH?'BEAR_CHOCH':'NONE'};
    }

    function calcLiqQuality(dep, price) {
      if(!dep||!price)return{ok:false,quality:'UNKNOWN',slippageRisk:false};
      const bids=Array.isArray(dep.bids)?dep.bids.slice(0,10):[];
      const asks=Array.isArray(dep.asks)?dep.asks.slice(0,10):[];
      if(!bids.length||!asks.length)return{ok:false,quality:'UNKNOWN',slippageRisk:false};
      const bestB=parseFloat(bids[0][0]),bestA=parseFloat(asks[0][0]);
      const spread=(bestA-bestB)/price*100;
      const bidD=bids.slice(0,5).reduce((s,[p,q])=>s+parseFloat(p)*parseFloat(q),0);
      const askD=asks.slice(0,5).reduce((s,[p,q])=>s+parseFloat(p)*parseFloat(q),0);
      const depth=bidD+askD;
      const quality=spread<0.02&&depth>50000?'EXCELLENT':spread<0.05&&depth>20000?'GOOD':spread<0.10&&depth>5000?'FAIR':'POOR';
      const slippageRisk=spread>0.08||depth<10000;
      return{ok:true,spread:+spread.toFixed(4),depth:+depth.toFixed(0),quality,slippageRisk};
    }

    function calcFundingMomentum(funArr) {
      if(!funArr||funArr.length<4)return{ok:false};
      const rates=funArr.slice(-8).map(f=>parseFloat(f.fundingRate)*100);
      const trend=rates[rates.length-1]-rates[0];
      const accel=rates[rates.length-1]-rates[rates.length-2];
      return{ok:true,trend:+trend.toFixed(5),acceleration:+accel.toFixed(5),
        signal:trend<-0.015&&accel<0?'STRONG_LONG_BIAS':trend<-0.008?'LONG_BIAS':
               trend>0.015&&accel>0?'STRONG_SHORT_BIAS':trend>0.008?'SHORT_BIAS':'NEUTRAL'};
    }

    const slPctForGate=parseFloat(autoConfig?.slPct||2);
    const atrGateWarn=atrPct>slPctForGate*1.5;
    const atrExtremeBlock = atrPct > Math.max(11, slPctForGate * 6.0);
    const atrBlocking = !!atrExtremeBlock;
    const r190bSpreadPct = Number(liqQual?.spread || 0);
    const r190bSpreadKill = r190bSpreadPct > 0.18;
    const poorLiquidity = !!(r190bSpreadKill || liqQual?.quality==='POOR' || (liqQual?.slippageRisk && r190bSpreadPct > 0.10));
    const r37Side = r37Timing?.ok ? (isL ? r37Timing.long : r37Timing.short) : null;
    const r37LateChaseBlock = !!(r37Side?.lateChase && !r190Edge?.squeeze && !r190Edge?.earlyContinuation);
    const r37RetestWaitBase = !!(r37Side?.retestOnly && !r37Side?.retestOk && !r37Side?.earlyImpulse);
    let r37RetestWait = r37RetestWaitBase && !r190Edge?.earlyContinuation;
    const r37EarlyOk = !!(r37Side?.earlyImpulse || r37Side?.retestOk || Number(r37Side?.earlyScore||0) >= 4);
    const r39Side = r39SR?.ok ? (isL ? r39SR.long : r39SR.short) : null;
    const r39TargetNearBlock = !!(r39Side?.targetTooNear && !r39Side?.breakConfirmed && !r37Side?.earlyImpulse && !r37Side?.retestOk);
    const r39AgainstZone = !!(isL ? r39Side?.nearResistance : r39Side?.nearSupport);
    const r39Confluence = !!(isL ? r39Side?.supportConfluence : r39Side?.resistanceConfluence);

    const r41OppositeWyckoff = !!(isL
      ? (wy1?.recentEvents?.some(e=>e.type==='UTAD') || wy15?.recentEvents?.some(e=>e.type==='UTAD') || wy4?.recentEvents?.some(e=>e.type==='UTAD'))
      : (wy1?.recentEvents?.some(e=>e.type==='SPRING'||e.type==='SOS') || wy15?.recentEvents?.some(e=>e.type==='SPRING'||e.type==='SOS') || wy4?.recentEvents?.some(e=>e.type==='SPRING'||e.type==='SOS'))
    );
    const _r41Rows5 = (Array.isArray(k5m)?k5m:[]).slice(-5).map(k=>({
      o:Number(k?.[1]), h:Number(k?.[2]), l:Number(k?.[3]), c:Number(k?.[4])
    })).filter(x=>[x.o,x.h,x.l,x.c].every(Number.isFinite) && x.c>0);
    const _r41First = _r41Rows5[0] || null;
    const _r41Last  = _r41Rows5.at(-1) || null;
    const _r41Move3 = (_r41First && _r41Last && _r41First.o>0) ? (_r41Last.c - _r41First.o) / _r41First.o * 100 : 0;
    const _r41LastBear = !!(_r41Last && _r41Last.c < _r41Last.o);
    const _r41LastBull = !!(_r41Last && _r41Last.c > _r41Last.o);
    const r41FallingKnifeBlock = !!(isL && _r41Move3 < -0.55 && _r41LastBear && _r35ClosePos < 0.48 && !r39Side?.breakConfirmed && !deltaOkStrict);
    const r41RisingKnifeBlock  = !!(!isL && _r41Move3 > 0.55 && _r41LastBull && _r35ClosePos > 0.52 && !r39Side?.breakConfirmed && !deltaOkStrict);
    const r42TrapReclaimOk = !!(r41OppositeWyckoff && deltaOkStrict && r39Side?.breakConfirmed && r37EarlyOk);
    const r41TrapBlock = !!(r41OppositeWyckoff && !r42TrapReclaimOk);

    const r46Clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, Number(v) || 0));
    const r46SpreadHist = r27SpreadHistory?.get?.(full);
    const r46SpreadVelocity = (r46SpreadHist && Number(r46SpreadHist.prev3m) > 0)
      ? (Number(r46SpreadHist.cur || 0) - Number(r46SpreadHist.prev3m || 0)) / Number(r46SpreadHist.prev3m || 1)
      : 0;
    const r46Rvol = Number(rvol1h?.rvol || 0);
    const r46PerfectAlignCount = [hardSweepForBridge, mtfBridgeOk, fresh5mImpulse, deltaOkStrict, fundBridgeOk].filter(Boolean).length;
    const r46PerfectAlignBonus = r46PerfectAlignCount >= 5 ? 15 : r46PerfectAlignCount === 4 ? 8 : r46PerfectAlignCount === 3 ? 3 : 0;
    const r46CvdGradeBonus = cvdValid ? (isL
      ? (cvdRatio >= 75 ? 14 : cvdRatio >= 65 ? 10 : cvdRatio >= 55 ? 5 : 0)
      : (cvdRatio <= 25 ? 14 : cvdRatio <= 35 ? 10 : cvdRatio <= 45 ? 5 : 0)
    ) : 0;
    const r46SqueezeQualityScore = (r46Rvol > 0 && r46Rvol < 0.30 && fresh5mImpulse)
      ? Math.round(r46Clamp(_r35BodyPct * 8 * (1 - r46Rvol / 0.30), 0, 12))
      : 0;
    const r46HasSpring = !!(isL && wy1?.recentEvents?.some(e=>e.type==='SPRING'||e.type==='SOS'));
    const r46SpringFlowOk = !!(r46HasSpring && (deltaOkStrict || (cvdValid && cvdRatio >= 55) || tickDeltaOk));
    const r46RecoverySpeed = Math.max(0, Number(_r41Move3 || 0));
    const r46SpringQuality = (r46HasSpring && r46SpringFlowOk && (fresh5mImpulse || r46RecoverySpeed > 0.15))
      ? Math.round(r46Clamp(6 + r46RecoverySpeed * 4 + (_r35ClosePos >= 0.65 ? 4 : 0) + (hardSweepForBridge ? 3 : 0), 6, 18))
      : 0;
    const r46ExhaustionShort = !!(!isL && wy1?.recentEvents?.some(e=>e.type==='UTAD') && !cvdSideOk && oiOpposite && r46SpreadVelocity > 0.15);

    let priorityScore=0;
    const priorityTags=[];
    const priorityFamily={macro:0,structure:0};
    const addP=(ok,pts,tag,family=null,cap=999)=>{
      if(!ok)return 0;
      let add=Number(pts||0);
      if(family){
        const left=Math.max(0, Number(cap||0)-Number(priorityFamily[family]||0));
        add=Math.min(add,left);
        priorityFamily[family]=(priorityFamily[family]||0)+add;
      }
      if(add>0){ priorityScore+=add; priorityTags.push(`${tag}+${add}`); }
      else if(family){ priorityTags.push(`${tag}+0(cap)`); }
      return add;
    };
    const subP=(ok,pts,tag)=>{ if(ok){ priorityScore-=pts; priorityTags.push(`${tag}-${pts}`); } };
    addP(hardSweepForBridge,24,'Sweep');
    addP(mmSameSide,mmTarget?.startsWith('GENUINE')?18:12,'MM');
    addP(huntBridgeOk,14,'StopHunt');
    addP(oiBridgeOk,12,'OI');
    addP(obSameSide,10,'Book');
    addP(fundBridgeOk,8,'Funding');
    addP(cmfSameSide,7,'CMF');
    addP(weisSameSide,7,'Weis');
    addP(chochSameSide,6,'ChoCH');
    addP(mtfBridgeOk,6,'MTF');
    addP(ewoSameSide,4,'EWO');
    addP(squeezeSameSide,4,'Squeeze');
    addP(pdSameSide,3,'P/D');
    addP(r46PerfectAlignBonus > 0, r46PerfectAlignBonus, `R46Align${r46PerfectAlignCount}/5`);
    addP(r46CvdGradeBonus > 0, r46CvdGradeBonus, `R46CVD${Math.round(cvdRatio)}%`);
    addP(r46SqueezeQualityScore > 0, r46SqueezeQualityScore, `R46Squeeze${r46Rvol.toFixed(2)}x`);
    addP(r46SpringQuality > 0, r46SpringQuality, 'R46SpringQuality');
    addP(r46ExhaustionShort, 20, 'R46ExhaustionShort');
    addP(r37Side?.earlyImpulse, 10, 'R37Early');
    addP(r37Side?.retestOk, 8, 'R37Retest');
    addP(Number(r37Side?.earlyScore||0) >= 4, 5, 'R37Micro');
    addP(r190Edge?.earlyContinuation && !r190Edge?.r194SwingBreak?.ok, 18, 'R190Early');
    addP(r190Edge?.r194SwingBreak?.ok, r190Edge?.r194SwingBreak?.strong ? 16 : 12, 'R194SwingBreak');
    addP(r190Edge?.momentumWindow && !r190Edge?.earlyContinuation, 8, 'R190Window');
    addP(r190Edge?.vpinAligned, 7, 'R190VPIN');
    addP(r190Edge?.squeeze, 16, 'R190Squeeze');
    addP(r190Edge?.r192Footprint?.aligned, r190Edge?.r192Footprint?.strongAligned ? 8 : 5, 'R192Footprint');
    addP(r190Edge?.r192DeepOfi?.aligned, r190Edge?.r192DeepOfi?.strongAligned ? 6 : 4, 'R192DeepOFI');
    addP(r190Edge?.r192SqzImminent, 5, 'R192SqzFuel');
    subP(r190Edge?.r192LiveAgainst, 16, 'R192LiveAgainst');
    subP(r190Edge?.lateTrapRisk && !r190Edge?.squeeze, 22, 'R190LateTrap');
    subP(r190Edge?.takerDivergence, 14, 'R190TakerDiv');
    subP(r190Edge?.spreadBlock, 20, 'R190Spread');
    subP(r37Side?.lateChase && !r190Edge?.squeeze && !r190Edge?.earlyContinuation, 22, 'R37LateChase');
    subP(r37RetestWaitBase && !r190Edge?.earlyContinuation, (r38TopMoverStrong && Number(r37Side?.earlyScore||0) >= 3) ? 6 : 12, 'R37RetestWait');
    addP(r39Confluence, 8, 'R39_SR_Confluence');
    addP(r39Side?.breakConfirmed, 7, 'R39_Break');
    subP(r39TargetNearBlock, 18, 'R39_TargetNear');
    subP(r39AgainstZone && !r39Side?.breakConfirmed, 7, 'R39_AgainstSR');
    subP(r41TrapBlock, 26, 'R41_WyckoffTrap');
    subP(r41FallingKnifeBlock || r41RisingKnifeBlock, 20, 'R41_Falling/RisingKnife');
    addP(r22Rotation.active, r22Rotation.score, r22Rotation.tag === 'SECTOR_ROTATION_STRONG' ? 'SektörRot' : 'Rotasyon', 'macro', 28);
    addP(r22FundingTrap.detected, r22FundingTrap.psBonus || 16, r22FundingTrap.strength==='STRONG'?'FundTrap🔥':'FundingTrap', 'macro', 28);
    addP(r22LiqWaterfall.favorable && !r22LiqWaterfall.adverse, 8, 'LiqŞelale+', 'macro', 28);
    addP(r22Decay.bonus > 0, r22Decay.bonus, 'Taze', 'structure', 16);
    addP(r22TestQ.bonus > 0, r22TestQ.bonus, r22TestQ.first ? 'İlkTest' : '2.Test', 'structure', 16);
    subP(r22Decay.penalty > 0, r22Decay.penalty, 'Decay');
    subP(r22TestQ.penalty > 0, r22TestQ.penalty, '3.Test');
    subP(r22LiqWaterfall.adverse, 14, 'LiqŞelaleTers');
    subP(rvolVeryLow,4,'RVOL');
    subP(cmfOpposite,7,'CMFters');
    subP(pdOpposite,5,'P/Dters');
    subP(mtfStrongOpposite,10,'MTFters');
    subP(oiOpposite,12,'OIters');
    subP(mmStrongOpposite,18,'MMters');
    if(isL && rsi4h>75) subP(true, rsi4h>82?12:7, 'RSI4h');
    if(!isL && rsi4h<25) subP(true, rsi4h<18?12:7, 'RSI4h');
    if((atrGateWarn||atrWarnForAuto)&&!atrBlocking) subP(true,5,'ATRwarn');

    {
      const _cdlStr5 = (_cdl5m?.bull || []).reduce((s,p)=>s+p.str,0) - (_cdl5m?.bear || []).reduce((s,p)=>s+p.str,0);
      addP(isL && _cdlStr5 >= 5, Math.min(_cdlStr5, 8), 'CDL5m🕯️', 'structure', 16);
      addP(!isL && _cdlStr5 <= -5, Math.min(-_cdlStr5, 8), 'CDL5m🕯️', 'structure', 16);
      addP(isL && _ha5m?.signal === 'STRONG_BULL', 8, 'HA_StrongBull', 'structure', 16);
      addP(!isL && _ha5m?.signal === 'STRONG_BEAR', 8, 'HA_StrongBear', 'structure', 16);
      const _cBull = _chart?.bullScore || 0, _cBear = _chart?.bearScore || 0;
      const _hBull = _harm?.bullScore || 0, _hBear = _harm?.bearScore || 0;
      addP(isL && _cBull > _cBear && _cBull >= 14, 10, 'ChartForm📐', 'structure', 16);
      addP(!isL && _cBear > _cBull && _cBear >= 14, 10, 'ChartForm📐', 'structure', 16);
      addP(isL && _hBull > _hBear && _hBull >= 18, 10, 'HarmonicPRZ🔮', 'macro', 28);
      addP(!isL && _hBear > _hBull && _hBear >= 18, 10, 'HarmonicPRZ🔮', 'macro', 28);
      addP(isL && _renko?.signal === 'STRONG_BULL', 8, 'Renko🧱', 'structure', 16);
      addP(!isL && _renko?.signal === 'STRONG_BEAR', 8, 'Renko🧱', 'structure', 16);
      subP(isL && (_chart?.patterns || []).some(p=>['HeadShoulders','DoubleTop'].includes(p.name)), 12, 'H&S_ters');
      subP(!isL && (_chart?.patterns || []).some(p=>['InvHeadShoulders','DoubleBottom'].includes(p.name)), 12, 'IHS_ters');
      subP(Boolean(_renko?.spikeTrap), 12, 'RenkoSpikeTrap');
      subP(Boolean(_renko?.ranging), 6, 'RenkoRanging');
    }

    {
      const _sh = r27SpreadHistory.get(full);
      const _sv = (_sh && _sh.prev3m > 0) ? (_sh.cur - _sh.prev3m) / _sh.prev3m : 0;
      subP(_sv > 0.45, 20, 'SpreadHızlanma🚨');
      subP(_sv > 0.25 && _sv <= 0.45, 10, 'SpreadGenişliyor');
      const _eng2 = tickStore?.get?.(full);
      const _bigSells = (_eng2?.bigTrades||[]).filter(t=>t.side==='SELL'&&Date.now()-t.ts<5*60*1000).length;
      subP(_bigSells >= 3, 12, 'Absorption🧲');
      if (k5m?.length >= 3 && btcPriceRef.p > 0) {
        const _cChg = (Number(k5m.at(-1)[4]) - Number(k5m.at(-2)[1])) / Number(k5m.at(-2)[1]) * 100;
        const _divR = Math.abs(_cChg) / (Math.abs(btcChange5mCache) + 0.02);
        subP(_divR > 6 && Math.abs(_cChg) > 1.8, 12, 'BTCKopuşu📡');
      }
      if (k5m?.length >= 20) {
        const _pv    = Math.max(...k5m.slice(-20).map(c=>Number(c[5])));
        const _cv    = Number(k5m.at(-1)[5]);
        const _pxChg = k5m.length>20 ? (Number(k5m.at(-1)[4])-Number(k5m.at(-21)?.[1]||k5m[0][1]))/Number(k5m.at(-21)?.[1]||k5m[0][1])*100 : 0;
        const _isDecay = _pv > 0 && (_pv-_cv)/_pv > 0.70 && _pxChg > 7;
        addP(!isL && _isDecay, 12, 'HacimÇürümesi⚡', 'macro', 28);
        subP( isL && _isDecay, 10, 'HacimÇürümesiLong');
      }
    }

    {
      const r29Risk = isL ? Number(r29Context.longRisk||0) : Number(r29Context.shortRisk||0);
      const r29Fav  = isL ? r29Context.preferSide==='LONG'  : r29Context.preferSide==='SHORT';
      const r29Vwap = isL ? r29Context.vwap?.longReclaim : r29Context.vwap?.shortReject;
      const r196SideCtx = isL ? r29Context.r196?.long : r29Context.r196?.short;
      const r197SideCtx = isL ? r29Context.r197?.long : r29Context.r197?.short;
      addP(r197SideCtx?.ok, r197SideCtx?.strong ? 18 : 12, 'R197RangeReversal', 'macro', 28);
      subP(r197SideCtx?.flowAgainst, 10, 'R197TersAkış');
      addP(r29Fav, 10, 'R29BağlamLehte', 'macro', 28);
      addP(r29Vwap, 8, 'VWAPTeyit', 'structure', 16);
      subP(r29Risk >= 45, Math.min(22, Math.round(r29Risk/5)), 'R29BağlamRisk');
      subP(r196SideCtx?.caution, 8, 'R196RangeDikkat');
      subP(r196SideCtx?.block, 28, 'R196TepedenDiptenKovalama');
    }

    addP(r281Map?.favorable >= 7, Math.min(16, Math.round(Number(r281Map?.favorable||0) * 0.75)), 'R281GrafikPro', 'structure', 18);
    subP(r281Map?.risk >= 7 && !r281Map?.runner, Math.min(16, Math.round(Number(r281Map?.risk||0) * 0.75)), 'R281MMAvRisk');
    if (r281Map?.runner) priorityTags.push('R281_RUNNER');
    if (r281Map?.protect) priorityTags.push('R281_PROTECT');
    if (r281Map?.hardNo) priorityTags.push('R281_HARD_NO');

    const bridgeCount=[mtfBridgeOk,fundBridgeOk,oiBridgeOk,huntBridgeOk].filter(Boolean).length;
    const r38RetestBridgeOk = !!(
      r37RetestWaitBase && !r37LateChaseBlock && r38TopMoverStrong &&
      Number(r37Side?.earlyScore||0) >= 3 && priorityScore >= 78 &&
      (hardSweepForBridge || huntBridgeOk) && (mmSameSide || oiBridgeOk || fundBridgeOk || mtfBridgeOk)
    );
    if (r38RetestBridgeOk) {
      r37RetestWait = false;
      priorityTags.push('R38RetestBridge+OK');
    }
    const cvdWarmingBridge=cvdMissing && (hasEntry||hardSweepForBridge) && sc>=Math.min(72,minAutoScore) && bridgeCount>=2;
    const cvdBridgePass = deltaOkStrict || (!cvdValid && priorityScore>=58 && hardSweepForBridge && (huntBridgeOk||mmSameSide||oiBridgeOk||fundBridgeOk));
    const r42FlowGate = !!(
      deltaOkStrict ||
      (tickData?.tickSweep?.fresh && hardSweepForBridge) ||
      (!cvdValid && priorityScore >= 76 && bridgeCount >= 3 && hardSweepForBridge && (fresh5mImpulseOrRecent || r37EarlyOk || r38RetestBridgeOk) && !r41OppositeWyckoff)
    );
    const deltaOk = deltaOkStrict || (cvdBridgePass && r42FlowGate);
    const microConfirm = deltaOkStrict || (hardSweepForBridge && (tickData?.tickSweep?.fresh || huntBridgeOk || obSameSide || oiBridgeOk || fundBridgeOk));
    const microConfirmR35 = microConfirm || (fresh5mImpulseOrRecent && (huntBridgeOk || obSameSide || oiBridgeOk || fundBridgeOk || mmSameSide || mtfBridgeOk));
    const cvdBridgeQualityOk = !cvdWarmingBridge || (cvdBridgePass && r42FlowGate);

    const fundOk=isL?fundSig!=='EXTREME_POSITIVE':fundSig!=='EXTREME_NEGATIVE';
    const rsiOk = isL ? rsi4h < 82 : rsi4h > 18;
    const mmOk = !mmVeryStrongOpposite;
    const r61MtfFullTrendOk = !!(
      isL
        ? (mtfBridgeOk && (Number(mtfBias?.bull || 0) >= Math.min(4, Number(mtfBias?.total || 4)) || mtfBias?.bias === 'STRONG_BULL' || Number(mtfBias?.bullPct || 0) >= 65))
        : (mtfBridgeOk && (Number(mtfBias?.bear || 0) >= Math.min(4, Number(mtfBias?.total || 4)) || mtfBias?.bias === 'STRONG_BEAR' || Number(mtfBias?.bullPct || 100) <= 35))
    );
    const r60StrongTrendContinuation = !!(
      r22Decay.noAuto &&
      r61MtfFullTrendOk &&
      (Number(rvol1h?.rvol||0) >= 1.5) &&
      oiBridgeOk &&
      !r37LateChaseBlock && !r39TargetNearBlock &&
      !r41TrapBlock && !r41OppositeWyckoff &&
      (fresh5mImpulseOrRecent || fresh15mConfirm || r37EarlyOk || r39Confluence || r39Side?.breakConfirmed)
    );
    const r62SideTrapEventOk = !!(isL
      ? (wy1?.recentEvents?.some(e=>e.type==='SPRING'||e.type==='SOS') || wy15?.recentEvents?.some(e=>e.type==='SPRING'||e.type==='SOS') || wy4?.recentEvents?.some(e=>e.type==='SPRING'||e.type==='SOS'))
      : (wy1?.recentEvents?.some(e=>e.type==='UTAD') || wy15?.recentEvents?.some(e=>e.type==='UTAD') || wy4?.recentEvents?.some(e=>e.type==='UTAD'))
    );
    const r62CounterTrendTrapContextOk = !!(
      mtfStrongOpposite &&
      (r62SideTrapEventOk || hardSweepForBridge || huntBridgeOk || tickData?.tickSweep?.fresh || r39Confluence || r39Side?.breakConfirmed)
    );
    const r62CounterTrendTrapFlowOk = !!(
      deltaOkStrict || obSameSide || oiBridgeOk || fundBridgeOk || cmfSameSide || weisSameSide || chochSameSide || ewoSameSide || squeezeSameSide
    );
    const signalDecayAutoBlock = !!(r22Decay.noAuto && (hardSweepForBridge || huntBridgeOk) && !fresh15mConfirm && !fresh5mImpulseOrRecent && !r60StrongTrendContinuation);
    const r29CtxBlock = isL ? !!r29Context.longAutoBlock : !!r29Context.shortAutoBlock;
    const r196SideCtx = isL ? r29Context.r196?.long : r29Context.r196?.short;
    const r196RangeBlock = !!(r196SideCtx?.block);
    const r196RangeCaution = !!(r196SideCtx?.caution);
    const r75LateChaseHard = !!(r37LateChaseBlock && !r37Side?.retestOk);
    const hardVeto = !!(poorLiquidity || atrBlocking || r39TargetNearBlock || r41TrapBlock || r41FallingKnifeBlock || r41RisingKnifeBlock || !fundOk || !rsiOk || !mmOk || r29CtxBlock || r196RangeBlock);
    const hardVetoReasons=[];
    if(poorLiquidity) hardVetoReasons.push(`Likidite kötü ${liqQual?.quality||''} spread:${liqQual?.spread??'?'}`);
    if(atrBlocking) hardVetoReasons.push(`ATR gate %${atrPct.toFixed(2)}`);
    if(r75LateChaseHard) hardVetoReasons.push(`R75 geç chase (retest yok): ${r37Side?.reason||''}`);
    if(r37LateChaseBlock && !r75LateChaseHard) hardVetoReasons.push(`R75 geç chase soft (retest var): ${r37Side?.reason||''}`);

    if(r39TargetNearBlock) hardVetoReasons.push(`R39 hedef çok yakın: ${r39Side?.reason||''}`);
    if(r41TrapBlock) hardVetoReasons.push(`R41 ters Wyckoff tuzağı: ${isL?'LONG üstünde UTAD/dağıtım':'SHORT altında Spring/SOS'}`);
    if(r41FallingKnifeBlock) hardVetoReasons.push(`R41 düşen bıçak LONG yok: 5m ${_r41Move3.toFixed(2)}%, canlı akış teyidi yok`);
    if(r41RisingKnifeBlock) hardVetoReasons.push(`R41 yükselen bıçak SHORT yok: 5m ${_r41Move3.toFixed(2)}%, canlı akış teyidi yok`);
    if(!fundOk) hardVetoReasons.push('Funding aşırı ters');
    if(!rsiOk) hardVetoReasons.push(`RSI4h uç ${rsi4h}`);
    if(!mmOk) hardVetoReasons.push(`MM kesin ters ${mmTarget}(%${mmConf})`);
    if(signalDecayAutoBlock) hardVetoReasons.push(`Signal decay ${r22Decay.label}`);
    if(r29CtxBlock) hardVetoReasons.push(`R29 bağlam riski ${isL ? r29Context.longRisk : r29Context.shortRisk}`);
    if(r196RangeBlock) hardVetoReasons.push(r196SideCtx?.reason || 'R196 günlük range tepesi/dibi risk');

    const sweepRequired = !!(autoConfig?.sweepOnly === true);
    const directSweepOk = !!(hardSweepForBridge || huntBridgeOk);
    const r45Rvol = Number(rvol1h?.rvol || 0);
    const r45RvolUnknown = !(r45Rvol > 0);
    const r45RvolDead = r45Rvol > 0 && r45Rvol < 0.18;
    const r45RvolLowButWatch = r45Rvol >= 0.18 && r45Rvol < 0.45;
    const r45RvolTradable = r45Rvol >= 0.45;
    const r45TopMoverSecondImpulseWatch = !!(
      r45RvolDead && r38TopMoverStrong && (fresh5mImpulseOrRecent || r37EarlyOk) && (oiBridgeOk || obSameSide || mmSameSide) &&
      !r37LateChaseBlock && !r39TargetNearBlock && priorityScore >= 78
    );
    const r45RvolOkForBridge = !!(
      r45RvolTradable ||
      (r45RvolUnknown && priorityScore >= 76 && (fresh5mImpulseOrRecent || r37EarlyOk) && (oiBridgeOk || obSameSide || mmSameSide)) ||
      (r45RvolLowButWatch && (fresh5mImpulseOrRecent || r37EarlyOk) && priorityScore >= 70) ||
      (r45TopMoverSecondImpulseWatch && directSweepOk)
    );
    const r45RvolStatus = r45RvolUnknown ? 'UNKNOWN' : r45RvolDead ? 'DEAD_WATCH' : r45RvolLowButWatch ? 'LOW_WATCH' : r45RvolTradable ? 'TRADABLE' : 'UNKNOWN';
    const r45CvdAlternativeOk = !!(
      cvdMissing && priorityScore >= 76 && (fresh5mImpulseOrRecent || r37EarlyOk) &&
      (oiBridgeOk || obSameSide || (mmSameSide && mmConf >= 45)) &&
      !r41OppositeWyckoff && !r37LateChaseBlock && !r39TargetNearBlock
    );
    const r45CvdOkForBridge = !!(deltaOkStrict || r45CvdAlternativeOk);

    const r47TimingPts = Math.min(4,
      (fresh5mImpulse ? 2 : 0) +
      (fresh5mImpulse2Bridge ? 1 : 0) +
      (r37EarlyOk ? 2 : 0) +
      (!r37EarlyOk && Number(r37Side?.earlyScore||0) >= 3 ? 1 : 0) +
      (r38RetestBridgeOk ? 1 : 0) +
      (r39Side?.breakConfirmed ? 1 : 0)
    );
    const r47FlowPts = Math.min(4,
      (deltaOkStrict ? 3 : 0) +
      (r45CvdAlternativeOk ? 2 : 0) +
      (r47CvdMissingBridge ? 1 : 0) +
      (r47CvdNeutralBridge ? 1 : 0) +
      (tickData?.tickSweep?.fresh ? 1 : 0) +
      (obSameSide ? 1 : 0)
    );
    const r47ContextPts = Math.min(5,
      (oiBridgeOk ? 1 : 0) +
      (mmSameSide ? 1 : 0) +
      (fundBridgeOk ? 1 : 0) +
      (mtfBridgeOk ? 1 : 0) +
      (chochSameSide ? 1 : 0) +
      (weisSameSide ? 1 : 0)
    );
    const r47StructurePts = Math.min(4,
      (r39Confluence ? 2 : 0) +
      (r39Side?.breakConfirmed ? 1 : 0) +
      (!r39TargetNearBlock && !r37LateChaseBlock ? 1 : 0) +
      (r46PerfectAlignCount >= 3 ? 1 : 0)
    );
    const r47RvolPts = r45RvolTradable ? 2
      : r45RvolLowButWatch ? 1
      : (r45RvolUnknown && (r38TopMoverStrong || priorityScore >= 76)) ? 1
      : r45TopMoverSecondImpulseWatch ? 1
      : 0;
    const r47Readiness = r47TimingPts + r47FlowPts + r47ContextPts + r47StructurePts + r47RvolPts;
    const r47Needed = r38TopMoverStrong ? 8 : 10;
    const r47FlowEnough = !!(
      r47FlowPts >= 2 ||
      (r47FlowPts >= 1 && r38TopMoverStrong && priorityScore >= 70 && r47ContextPts >= 2) ||
      (r47FlowPts >= 1 && priorityScore >= 76 && r47ContextPts >= 3)
    );
    const r47CompositeNonSweepOk = !!(
      !directSweepOk && !hardVeto && !signalDecayAutoBlock && !r37RetestWait && !r37LateChaseBlock && !r39TargetNearBlock &&
      !poorLiquidity && (!mtfStrongOpposite || r62CounterTrendTrapContextOk) && !mmVeryStrongOpposite && !r41OppositeWyckoff &&
      sc >= minAutoScore && priorityScore >= (r38TopMoverStrong ? 64 : 68) &&
      r47TimingPts >= 2 && r47ContextPts >= 2 && r47RvolPts >= 1 && r47FlowEnough && r47Readiness >= r47Needed
    );
    const r48CvdNotAgainst = !!(!cvdValid || deltaOkStrict || r47FlowPts >= 2 || (priorityScore >= 50 && r47ContextPts >= 3));
    const r48DirectSweepBalanceOk = !!(
      !sweepRequired && directSweepOk && !hardVeto && !signalDecayAutoBlock && !r37RetestWait &&
      !r37LateChaseBlock && !r39TargetNearBlock && !poorLiquidity && !mtfStrongOpposite &&
      !mmVeryStrongOpposite && !r41OppositeWyckoff && r48CvdNotAgainst &&
      sc >= minAutoScore && priorityScore >= (r38TopMoverStrong ? 24 : 34) &&
      r47StructurePts >= 1 && r47RvolPts >= 1 &&
      (r47TimingPts >= 1 || fresh15mConfirm || r39Confluence || r39Side?.breakConfirmed) &&
      (r47FlowPts >= 1 || r47ContextPts >= 3) &&
      r47Readiness >= (r38TopMoverStrong ? 6 : 7)
    );

    const r49CvdAgainst = !!(cvdValid && !deltaOkStrict);
    const r49CvdSafe = !!(
      !r49CvdAgainst ||
      (tickData?.tickSweep?.fresh && r47ContextPts >= 3 && priorityScore >= 60)
    );
    const r49ContextOk = !!(
      r47ContextPts >= 3 ||
      (priorityScore >= 55 && r47ContextPts >= 2) ||
      (huntBridgeOk && (oiBridgeOk || mmSameSide || fundBridgeOk) && priorityScore >= 45)
    );
    const r49TimingOk = !!(r47TimingPts >= 1 || fresh15mConfirm || r39Confluence || r39Side?.breakConfirmed || r37EarlyOk);
    const r49StructureOk = !!(r47StructurePts >= 1 && r47RvolPts >= 1);
    const r49DirectSweepUnlockOk = !!(
      !sweepRequired && directSweepOk && (hasEntry || huntBridgeOk) &&
      !hardVeto && !signalDecayAutoBlock && !r37RetestWait && !r37LateChaseBlock && !r39TargetNearBlock &&
      !poorLiquidity && !mtfStrongOpposite && !mmVeryStrongOpposite && !r41OppositeWyckoff &&
      sc >= minAutoScore && priorityScore >= (r38TopMoverStrong ? 45 : 55) &&
      r49CvdSafe && r49ContextOk && r49TimingOk && r49StructureOk &&
      r47Readiness >= (r38TopMoverStrong ? 8 : 10)
    );

    const r50PriorityBoost = r47Readiness >= 10 ? 15 : r47Readiness >= 8 ? 8 : 0;
    const r50EffectivePriority = Math.max(0, Number(priorityScore || 0) + r50PriorityBoost);
    const r50HardClean = !!(
      !hardVeto && !signalDecayAutoBlock && !r37RetestWait && !r39TargetNearBlock &&
      !poorLiquidity && !mtfStrongOpposite && !mmVeryStrongOpposite && !r41OppositeWyckoff &&
      !r41TrapBlock && !r41FallingKnifeBlock && !r41RisingKnifeBlock
    );
    const r50MinReadiness = r38TopMoverStrong ? 8 : 10;
    const r50FlowOrContextOk = !!(
      r47FlowPts >= 1 || r47ContextPts >= 3 || deltaOkStrict || obSameSide || oiBridgeOk || mmSameSide || fundBridgeOk
    );
    const r50StructureOrTimingOk = !!(
      r47TimingPts >= 1 || r47StructurePts >= 1 || fresh5mImpulseOrRecent || r37EarlyOk || fresh15mConfirm || r39Confluence || r39Side?.breakConfirmed
    );
    const r50RvolUsable = !!(r47RvolPts >= 1 || r45RvolUnknown || (r38TopMoverStrong && r45RvolLowButWatch));
    const r50DirectSweepMatrixOk = !!(
      !sweepRequired && directSweepOk && (hasEntry || huntBridgeOk || hardSweepForBridge) && r50HardClean &&
      sc >= minAutoScore && r47Readiness >= r50MinReadiness &&
      r50EffectivePriority >= (r38TopMoverStrong ? 32 : 42) &&
      r50FlowOrContextOk && r50StructureOrTimingOk && r50RvolUsable && r49CvdSafe
    );
    const r50NonSweepMatrixOk = !!(
      !sweepRequired && !directSweepOk && r50HardClean && sc >= minAutoScore &&
      r47Readiness >= r47Needed && r50EffectivePriority >= (r38TopMoverStrong ? 64 : 70) &&
      r47TimingPts >= 2 && r47FlowEnough && r47ContextPts >= 2 && r50RvolUsable
    );

    const r51DirectSweepRetestClean = !!((!r75LateChaseHard || r37Side?.retestOk) &&
      !hardVeto && !signalDecayAutoBlock && !r37RetestWait && !r39TargetNearBlock &&
      !poorLiquidity && !r41TrapBlock && !r41FallingKnifeBlock && !r41RisingKnifeBlock
    );
    const r51DirectSweepMinEdgeOk = !!(
      !sweepRequired && directSweepOk && (hasEntry || huntBridgeOk || hardSweepForBridge) && r51DirectSweepRetestClean &&
      sc >= minAutoScore && r47Readiness >= r50MinReadiness &&
      r50EffectivePriority >= (r38TopMoverStrong ? 24 : 34) &&
      r47StructurePts >= 2 && r47RvolPts >= 1 &&
      (r47FlowPts >= 1 || r47ContextPts >= 2 || deltaOkStrict || obSameSide || oiBridgeOk || mmSameSide || fundBridgeOk) &&
      r50StructureOrTimingOk && r49CvdSafe
    );

    const r53SmartEdgeBoost = r47Readiness >= 10 && r50EffectivePriority >= 80 ? 8
      : r47Readiness >= 9 && r50EffectivePriority >= 74 ? 6
      : r47Readiness >= 8 && r50EffectivePriority >= 68 ? 4
      : 0;
    const r53EffectiveScore = Number(sc || 0) + r53SmartEdgeBoost;
    const r53ScoreFloor = Math.max(64, Number(minAutoScore || 72) - 8);
    const r53CvdSmartSafe = !!(
      r49CvdSafe ||
      (directSweepOk && huntBridgeOk && r47FlowPts >= 1 && r47ContextPts >= 3 && r50EffectivePriority >= 84)
    );
    const r53SmartEdgeScoreOk = !!(
      !sweepRequired && r50HardClean &&
      sc >= r53ScoreFloor && r53EffectiveScore >= minAutoScore &&
      r47Readiness >= r50MinReadiness &&
      r50EffectivePriority >= (r38TopMoverStrong ? 78 : 84) &&
      r50FlowOrContextOk && r50StructureOrTimingOk && r50RvolUsable && r53CvdSmartSafe &&
      (
        (directSweepOk && (hasEntry || huntBridgeOk || hardSweepForBridge)) ||
        (!directSweepOk && r47TimingPts >= 2 && r47FlowEnough && r47ContextPts >= 3)
      )
    );
    const r54MicroProbeOk = !!(
      !sweepRequired && r50HardClean &&
      r47Readiness >= 7 &&
      sc >= Math.max(68, minAutoScore - 4) && sc < minAutoScore &&
      r50EffectivePriority >= 70 &&
      r50FlowOrContextOk && r50StructureOrTimingOk && r50RvolUsable && r53CvdSmartSafe &&
      (directSweepOk || huntBridgeOk || hardSweepForBridge || mmSameSide || oiBridgeOk || obSameSide || fresh5mImpulseOrRecent)
    );

    const r57ScalperBTierBridgeOk = !!(
      !sweepRequired && r50HardClean &&
      sc >= minAutoScore &&
      r47Readiness >= r47Needed &&
      r50EffectivePriority >= (r38TopMoverStrong ? 38 : 45) &&
      r50FlowOrContextOk && r50RvolUsable && r53CvdSmartSafe &&
      (r47StructurePts >= 1 || r47TimingPts >= 1 || r50StructureOrTimingOk) &&
      (directSweepOk || huntBridgeOk || hardSweepForBridge || mmSameSide || oiBridgeOk || obSameSide || fresh5mImpulseOrRecent || r37EarlyOk) &&
      !r37LateChaseBlock && !r39TargetNearBlock && !poorLiquidity && !rvolVeryLow &&
      (r47FlowPts >= 1 || r47ContextPts >= 3 || deltaOkStrict || oiBridgeOk || mmSameSide || obSameSide)
    );
    const r61TrendContinuationBoost = r60StrongTrendContinuation
      ? Math.min(34, Math.max(12,
          (r47Readiness >= 8 ? 8 : 4) +
          (r47ContextPts >= 4 ? 8 : r47ContextPts >= 3 ? 5 : 0) +
          (r47RvolPts >= 2 ? 6 : r47RvolPts >= 1 ? 3 : 0) +
          (Number(rvol1h?.rvol || 0) >= 3 ? 6 : 3) +
          (oiBridgeOk ? 4 : 0) +
          (mmSameSide ? 3 : 0)
        ))
      : 0;
    const r61TrendEffectiveScore = r60StrongTrendContinuation
      ? Math.min(100, Math.max(Number(sc || 0) + r61TrendContinuationBoost, Math.round(Number(sc || 0) / Math.max(0.65, Number(r22Decay?.mult || 1)))))
      : Number(sc || 0);
    const r61TrendPriorityOk = !!(
      r50EffectivePriority >= (r38TopMoverStrong ? 34 : 42) ||
      (r60StrongTrendContinuation && r47ContextPts >= 3 && r47RvolPts >= 1 && oiBridgeOk && mtfBridgeOk)
    );
    const r61TrendContinuationBridgeOk = !!(
      !sweepRequired && r50HardClean && r60StrongTrendContinuation &&
      sc >= Math.max(60, minAutoScore - 12) &&
      r61TrendEffectiveScore >= minAutoScore &&
      r47Readiness >= 7 && r61TrendPriorityOk &&
      r50FlowOrContextOk && r50StructureOrTimingOk && r50RvolUsable && r53CvdSmartSafe &&
      (directSweepOk || huntBridgeOk || hardSweepForBridge || mmSameSide || oiBridgeOk || obSameSide || fresh5mImpulseOrRecent || r37EarlyOk || r39Confluence)
    );
    const r62TrapHardClean = !!(
      !hardVeto && !signalDecayAutoBlock && !r37RetestWait && !r37LateChaseBlock && !r39TargetNearBlock &&
      !poorLiquidity && !mmVeryStrongOpposite && !r41TrapBlock && !r41FallingKnifeBlock && !r41RisingKnifeBlock &&
      (!mtfStrongOpposite || r62CounterTrendTrapContextOk) &&
      (!r41OppositeWyckoff || r62CounterTrendTrapContextOk)
    );
    const r62CounterTrendTrapBridgeOk = !!(
      !sweepRequired && r62TrapHardClean && r62CounterTrendTrapContextOk &&
      sc >= minAutoScore &&
      r47Readiness >= Math.max(7, Math.min(10, Number(r47Needed || 8) - 1)) &&
      r50EffectivePriority >= (r38TopMoverStrong ? 44 : 54) &&
      r50FlowOrContextOk && r50StructureOrTimingOk && r50RvolUsable && r53CvdSmartSafe && r62CounterTrendTrapFlowOk &&
      (directSweepOk || hardSweepForBridge || huntBridgeOk || r62SideTrapEventOk || r37EarlyOk || r39Confluence || r39Side?.breakConfirmed) &&
      !r37LateChaseBlock && !r39TargetNearBlock && !poorLiquidity && !rvolVeryLow && !mmVeryStrongOpposite
    );

    const r66WyckoffTrapReclaimOk = !!(
      r41TrapBlock &&
      sc >= minAutoScore &&
      r47Readiness >= 5 &&
      (directSweepOk || hardSweepForBridge || huntBridgeOk) &&
      mtfBridgeOk &&
      (fresh5mImpulseOrRecent || r37EarlyOk || r38TopMoverStrong || r39Confluence || r39Side?.breakConfirmed) &&
      (r53CvdSmartSafe || r47FlowPts >= 1 || mmSameSide || oiBridgeOk || obSameSide || fundBridgeOk)
    );
    const r66WyckoffHardVeto = !!(r41TrapBlock && !r42TrapReclaimOk && !r66WyckoffTrapReclaimOk);
    const r65ScalperCoreHardVeto = !!(
      poorLiquidity || atrExtremeBlock || r39TargetNearBlock ||
      r66WyckoffHardVeto || r41FallingKnifeBlock || r41RisingKnifeBlock
    );
    const r65ScalperCoreTrendOk = !!(
      !sweepRequired &&
      sc >= minAutoScore &&
      (directSweepOk || hardSweepForBridge) &&
      mtfBridgeOk &&
      r47Readiness >= 5 &&
      !r65ScalperCoreHardVeto
    );
    const r65ScalperCoreCounterTrapOk = !!(
      !sweepRequired &&
      sc >= minAutoScore &&
      (directSweepOk || hardSweepForBridge || huntBridgeOk) &&
      r62CounterTrendTrapContextOk &&
      r47Readiness >= 5 &&
      !r65ScalperCoreHardVeto &&
      (r62CounterTrendTrapFlowOk || oiBridgeOk || obSameSide || fundBridgeOk || mmSameSide)
    );
    const r67ScalperCoreHuntEntryOk = !!(
      !sweepRequired &&
      sc >= minAutoScore &&
      (directSweepOk || hardSweepForBridge || huntBridgeOk) &&
      mtfBridgeOk &&
      r47Readiness >= 5 &&
      !poorLiquidity && !r39TargetNearBlock && !atrExtremeBlock &&
      !r41FallingKnifeBlock && !r41RisingKnifeBlock
    );

    const r74ScoreFloor = Math.max(40, Number(minAutoScore || 68) - 25);
    const r74ImpulseEntryOk = !!(
      r38TopMoverStrong &&
      (fresh5mImpulseOrRecent || fresh5mImpulse || r37EarlyOk || r37Side?.retestOk || r39Confluence || r39Side?.breakConfirmed) &&
      !atrExtremeBlock && !poorLiquidity && !r39TargetNearBlock
    );
    const r75R47MinBypass = r38TopMoverStrong ? 5 : 6;
    const r74Top10ContextBypassOk = !!(
      r38TopMoverStrong && Number(r47Readiness || 0) >= r75R47MinBypass && r74ImpulseEntryOk &&
      Number(sc || 0) >= r74ScoreFloor
    );
    const r74Top10ProScalperOk = !!(
      (!sweepRequired || (sweepRequired && directSweepOk)) &&
      r74Top10ContextBypassOk && !r65ScalperCoreHardVeto
    );

    const r68EntryEventOk = !!(directSweepOk || hardSweepForBridge || huntBridgeOk || tickData?.tickSweep?.fresh || r74ImpulseEntryOk);
    const r68TrendContextOk = !!(mtfBridgeOk || r61MtfFullTrendOk);
    const r68CounterTrapContextOk = !!(mtfStrongOpposite && r62CounterTrendTrapContextOk && (r62CounterTrendTrapFlowOk || oiBridgeOk || obSameSide || fundBridgeOk || mmSameSide));
    const r68ReadinessOk = Number(r47Readiness || 0) >= 5;
    const r68ScoreOk = Number(sc || 0) >= Number(minAutoScore || 0);
    const r68CriticalHardBlock = !!(
      poorLiquidity || r39TargetNearBlock || atrExtremeBlock ||
      r41FallingKnifeBlock || r41RisingKnifeBlock
    );
    const r69PriorityContextOverrideOk = !!(
      (Number(r47Readiness || 0) >= 8 && Number(r50EffectivePriority || priorityScore || 0) >= 68) ||
      (Number(r47Readiness || 0) >= 10 && Number(r50EffectivePriority || priorityScore || 0) >= 55) ||
      (Number(r47Readiness || 0) >= 12 && Number(priorityScore || 0) >= 45) ||
      r74Top10ContextBypassOk
    );
    const r69ContextOk = !!(r68TrendContextOk || r68CounterTrapContextOk || r69PriorityContextOverrideOk);
    const r69PriorityExecutionOk = !!(
      !sweepRequired && r68ScoreOk && r68EntryEventOk && r68ReadinessOk &&
      r69ContextOk && !r68CriticalHardBlock
    );
    const r68UnifiedScalperCoreOk = !!(r69PriorityExecutionOk || r74Top10ProScalperOk);

    const r65ScalperCoreOk = !!(r68UnifiedScalperCoreOk || r65ScalperCoreTrendOk || r65ScalperCoreCounterTrapOk || r67ScalperCoreHuntEntryOk);

    const r75RetestBridgeOk = !!(r37Side?.retestOk && r74ImpulseEntryOk && r38TopMoverStrong && !r75LateChaseHard);

    const r86CdlBull5 = (_cdl5m?.bull || []).reduce((s,p)=>s+Number(p.str||0),0);
    const r86CdlBear5 = (_cdl5m?.bear || []).reduce((s,p)=>s+Number(p.str||0),0);
    const r86CdlSame = isL ? r86CdlBull5 : r86CdlBear5;
    const r86CdlOpp  = isL ? r86CdlBear5 : r86CdlBull5;
    const r86ChartSame = isL ? Number(_chart?.bullScore||0) : Number(_chart?.bearScore||0);
    const r86ChartOpp  = isL ? Number(_chart?.bearScore||0) : Number(_chart?.bullScore||0);
    const r86HarmSame  = isL ? Number(_harm?.bullScore||0) : Number(_harm?.bearScore||0);
    const r86HarmOpp   = isL ? Number(_harm?.bearScore||0) : Number(_harm?.bullScore||0);
    const r86HaSame = isL
      ? (_ha5m?.signal === 'STRONG_BULL' ? 3 : _ha5m?.signal === 'BULL' ? 1 : 0)
      : (_ha5m?.signal === 'STRONG_BEAR' ? 3 : _ha5m?.signal === 'BEAR' ? 1 : 0);
    const r86HaOpp = isL
      ? (_ha5m?.signal === 'STRONG_BEAR' ? 3 : _ha5m?.signal === 'BEAR' ? 1 : 0)
      : (_ha5m?.signal === 'STRONG_BULL' ? 3 : _ha5m?.signal === 'BULL' ? 1 : 0);
    const r86RenkoSame = isL
      ? (_renko?.signal === 'STRONG_BULL' ? 3 : _renko?.signal === 'BULL' ? 1 : 0)
      : (_renko?.signal === 'STRONG_BEAR' ? 3 : _renko?.signal === 'BEAR' ? 1 : 0);
    const r86RenkoOpp = isL
      ? (_renko?.signal === 'STRONG_BEAR' ? 3 : _renko?.signal === 'BEAR' ? 1 : 0)
      : (_renko?.signal === 'STRONG_BULL' ? 3 : _renko?.signal === 'BULL' ? 1 : 0);
    const r86FormasyonPuan = Math.min(12,
      (r86CdlSame >= 8 ? 4 : r86CdlSame >= 5 ? 3 : r86CdlSame >= 3 ? 1 : 0) +
      r86HaSame +
      (r86ChartSame >= 16 ? 3 : r86ChartSame >= 8 ? 2 : r86ChartSame > 0 ? 1 : 0) +
      (r86HarmSame >= 18 ? 3 : r86HarmSame >= 10 ? 2 : r86HarmSame > 0 ? 1 : 0) +
      r86RenkoSame
    );
    const r86KarsiFormasyonPuan = Math.min(12,
      (r86CdlOpp >= 8 ? 4 : r86CdlOpp >= 5 ? 3 : r86CdlOpp >= 3 ? 1 : 0) +
      r86HaOpp +
      (r86ChartOpp >= 16 ? 3 : r86ChartOpp >= 8 ? 2 : r86ChartOpp > 0 ? 1 : 0) +
      (r86HarmOpp >= 18 ? 3 : r86HarmOpp >= 10 ? 2 : r86HarmOpp > 0 ? 1 : 0) +
      r86RenkoOpp
    );
    const r86CanliTetikOk = !!(
      directSweepOk || hardSweepForBridge || huntBridgeOk || tickData?.tickSweep?.fresh ||
      fresh5mImpulseOrRecent || r37EarlyOk || r37Side?.retestOk || r39Confluence || r39Side?.breakConfirmed
    );
    const r86VeriTeyitleri = [
      ['canlı akış', deltaOkStrict || r45CvdAlternativeOk || r47FlowPts >= 2],
      ['açık ilgi', oiBridgeOk],
      ['emir defteri', obSameSide],
      ['piyasa yapıcı', mmSameSide],
      ['çoklu zaman', mtfBridgeOk || r61MtfFullTrendOk],
      ['para akışı', fundBridgeOk],
      ['destek/direnç', r39Confluence || r39Side?.breakConfirmed],
      ['canlı tetik', r86CanliTetikOk]
    ];
    const r86VeriTeyitSayisi = r86VeriTeyitleri.filter(([,ok])=>!!ok).length;
    const r86FormasyonAdlari = [
      ...((isL ? (_cdl5m?.bull||[]) : (_cdl5m?.bear||[])).slice().sort((a,b)=>b.str-a.str).map(p=>p.name)),
      ...((isL ? (_chart?.patterns||[]).filter(p=>p.dir==='BULL') : (_chart?.patterns||[]).filter(p=>p.dir==='BEAR')).map(p=>p.name)),
      ...((isL ? (_harm?.patterns||[]).filter(p=>p.dir==='BULL') : (_harm?.patterns||[]).filter(p=>p.dir==='BEAR')).map(p=>p.name))
    ];
    const r86KarsiFormasyonGucu = !!(r86KarsiFormasyonPuan >= r86FormasyonPuan + 4 || _renko?.spikeTrap);
    const r86FormasyonVeriTeyitOk = !!(
      !sweepRequired && !hardVeto && !signalDecayAutoBlock && !r37RetestWait &&
      !poorLiquidity && !atrExtremeBlock && !r39TargetNearBlock && !r41FallingKnifeBlock && !r41RisingKnifeBlock &&
      r86FormasyonPuan >= 5 && !r86KarsiFormasyonGucu &&
      r86CanliTetikOk && r86VeriTeyitSayisi >= 3 &&
      r47Readiness >= 5 && r47ContextPts >= 2 && (r47FlowPts >= 1 || deltaOkStrict || obSameSide || oiBridgeOk) &&
      Number(sc || 0) >= Math.max(40, Number(minAutoScore || 68) - 30) &&
      !mmVeryStrongOpposite && (!mtfStrongOpposite || r62CounterTrendTrapContextOk || r86FormasyonPuan >= 8)
    );

    const r88VurKacEnabled = !!((autoConfig?.vurKacEnabled !== false) || autoConfig?.scalpEngineEnabled);
    const r88Spread = Number(liqQual?.spread || 0);
    const r88Depth  = Number(liqQual?.depth || 0);
    const r88SpreadWide = !!(r88Spread > 0.12 || (r88Spread > 0.08 && r88Depth < 20000));
    const r88DefterInce = !!((r88Depth > 0 && r88Depth < 20000) || liqQual?.quality === 'POOR' || (liqQual?.quality === 'FAIR' && r88Spread > 0.06));
    const r88OynaklikAsiri = !!(atrPct > Math.max(12, slPctForGate * 6));
    const r88PiyasaBozuk = !!(poorLiquidity || r88SpreadWide || r88DefterInce || r88OynaklikAsiri || atrExtremeBlock);
    const r93PiyasaHamTehlikeli = !!(
      atrExtremeBlock ||
      r88Spread > 0.30 ||
      (r88Depth > 0 && r88Depth < 2500 && r88Spread > 0.12) ||
      (r88SpreadWide && r88DefterInce && r88OynaklikAsiri) ||
      atrPct > Math.max(18, slPctForGate * 8)
    );
    let r93PiyasaTehlikeli = !!r93PiyasaHamTehlikeli;
    let r93PiyasaDalgali = !!(r88PiyasaBozuk && !r93PiyasaTehlikeli);
    const r88CanliHamleIzi = !!(
      fresh5mImpulse || fresh5mImpulse2 || fresh5mImpulseOrRecent || r37EarlyOk ||
      Number(r37Side?.earlyScore || 0) >= 3 || directSweepOk || hardSweepForBridge || huntBridgeOk ||
      r39Side?.breakConfirmed || tickData?.tickSweep?.fresh
    );
    const r88AkisTeyidiSayisi = [
      deltaOkStrict || r45CvdAlternativeOk || r47FlowPts >= 1,
      oiBridgeOk,
      obSameSide,
      mmSameSide,
      fundBridgeOk,
      cmfSameSide || weisSameSide || chochSameSide || ewoSameSide,
      r86FormasyonPuan >= 4,
      r39Confluence || r39Side?.breakConfirmed
    ].filter(Boolean).length;
    const r88RiskEksi =
      (r86KarsiFormasyonGucu ? 3 : 0) +
      (mmVeryStrongOpposite ? 4 : 0) +
      (r39TargetNearBlock && !r39Side?.breakConfirmed ? 2 : 0) +
      (mtfStrongOpposite && !r62CounterTrendTrapContextOk ? 2 : 0) +
      (signalDecayAutoBlock && !fresh5mImpulseOrRecent && !r37EarlyOk ? 2 : 0);
    const r89SuperMikroYapiOk = !!(
      Number(r47Readiness || 0) >= 9 && Number(r47TimingPts || 0) >= 2 && Number(r47ContextPts || 0) >= 3 &&
      (Number(r47FlowPts || 0) >= 1 || oiBridgeOk || obSameSide || mmSameSide || fundBridgeOk) &&
      (Number(r47StructurePts || 0) >= 1 || Number(r47RvolPts || 0) >= 1 || r39Confluence || r39Side?.breakConfirmed || Number(r37Side?.earlyScore || 0) >= 3)
    );
    const r93Merdiven = r93AnalyzeStairAndTurn(k5m);
    const r93MerdivenDevamOk = !!(isL ? r93Merdiven.longDevam : r93Merdiven.shortDevam);
    const r93DonusRadariOk = !!(isL ? r93Merdiven.shorttanLonga : r93Merdiven.longdanShorta);
    const r93DonusSkor = Number(isL ? r93Merdiven.shortDonusSkor : r93Merdiven.longDonusSkor);
    const r93MerdivenSkor = Number(isL ? r93Merdiven.longMerdivenSkor : r93Merdiven.shortMerdivenSkor);
    const r93SonMumTers = !!(isL ? String(r93Merdiven.sonMum).includes('KIRMIZI') : String(r93Merdiven.sonMum).includes('YEŞİL'));
    const r88MikroSkor =
      (fresh5mImpulse ? 3 : 0) +
      (fresh5mImpulse2 ? 1 : 0) +
      (r37EarlyOk ? 3 : Number(r37Side?.earlyScore||0) >= 3 ? 2 : 0) +
      (r47TimingPts >= 2 ? 3 : r47TimingPts >= 1 ? 1 : 0) +
      (r47FlowPts >= 2 ? 3 : r47FlowPts >= 1 ? 2 : 0) +
      (r47ContextPts >= 3 ? 3 : r47ContextPts >= 2 ? 2 : 0) +
      (r47StructurePts >= 1 ? 1 : 0) +
      (r47RvolPts >= 1 ? 1 : 0) +
      (oiBridgeOk ? 2 : 0) + (mmSameSide ? 2 : 0) + (obSameSide ? 1 : 0) +
      ((directSweepOk || hardSweepForBridge || huntBridgeOk) ? 2 : 0) +
      (r86FormasyonPuan >= 5 ? 2 : r86FormasyonPuan >= 3 ? 1 : 0) +
      (r93MerdivenDevamOk ? 2 : 0) +
      (r93DonusRadariOk ? 3 : 0) - r88RiskEksi;
    const r93TrendCanliOk = !!(r93MerdivenDevamOk || r93DonusRadariOk || r89SuperMikroYapiOk || (r88CanliHamleIzi && Number(r47Readiness||0) >= 8));
    const r94CanliTrendZeminKurtarmaOk = !!(
      r88VurKacEnabled && !r93PiyasaHamTehlikeli && r88PiyasaBozuk &&
      (r38TopMoverStrong || Math.abs(Number(r38MarketCtx?.change24h||0)) >= 8 || Number(r38MarketCtx?.volume||0) >= 10000000) &&
      Number(r47Readiness || 0) >= 8 && Number(r88AkisTeyidiSayisi || 0) >= 4 &&
      (Number(r88MikroSkor || 0) >= 13 || Number(r50EffectivePriority || priorityScore || 0) >= 45 || r89SuperMikroYapiOk || r93DonusSkor >= 6) &&
      r88Spread <= 0.18 && !r88OynaklikAsiri && !atrExtremeBlock
    );
    if (r94CanliTrendZeminKurtarmaOk) { r93PiyasaTehlikeli = false; r93PiyasaDalgali = true; }
    const r93DalgaliAmaIslemYapilabilir = !!(
      (r93PiyasaDalgali || r94CanliTrendZeminKurtarmaOk) && !r93PiyasaTehlikeli && r93TrendCanliOk &&
      Number(r47Readiness || 0) >= 8 && Number(r88AkisTeyidiSayisi || 0) >= 3 &&
      (Number(r50EffectivePriority || priorityScore || 0) >= 45 || Number(r88MikroSkor || 0) >= 13 || r93DonusSkor >= 6)
    );
    const r93PiyasaIslemYapilabilir = !!(!r88PiyasaBozuk || r93DalgaliAmaIslemYapilabilir);
    const r88ScoreFloor = Math.max(35, Number(minAutoScore || 70) - 35);
    const r89ScoreFloor = r89SuperMikroYapiOk ? Math.max(28, r88ScoreFloor - 12) : r88ScoreFloor;
    const r90CanliKopmaOk = !!(
      r88VurKacEnabled && !sweepRequired && !hardVeto && r93PiyasaIslemYapilabilir &&
      (r88CanliHamleIzi || r93MerdivenDevamOk || r93DonusRadariOk) && Number(r47Readiness || 0) >= 8 && Number(r47TimingPts || 0) >= 2 &&
      Number(r47ContextPts || 0) >= 2 && Number(r47StructurePts || 0) >= 1 &&
      Number(r88MikroSkor || 0) >= 12 && Number(r88AkisTeyidiSayisi || 0) >= 3 &&
      Number(sc || 0) >= Math.max(30, Number(r89ScoreFloor || 35) - 5) &&
      (!r75LateChaseHard || r37Side?.retestOk || r39Side?.breakConfirmed || fresh5mImpulseOrRecent || Number(r47TimingPts || 0) >= 3) &&
      (!r39TargetNearBlock || r39Side?.breakConfirmed || r62CounterTrendTrapContextOk || Number(r47TimingPts || 0) >= 3) &&
      !mmVeryStrongOpposite && !r86KarsiFormasyonGucu
    );
    const r92VurKacAdayOk = !!(
      r90CanliKopmaOk || (
      r88VurKacEnabled && !sweepRequired && (r38TopMoverStrong || r89SuperMikroYapiOk || r93MerdivenDevamOk || r93DonusRadariOk) && !hardVeto && r93PiyasaIslemYapilabilir &&
      (r88CanliHamleIzi || r89SuperMikroYapiOk || r93MerdivenDevamOk || r93DonusRadariOk) && Number(r47Readiness || 0) >= 5 && Number(sc || 0) >= r89ScoreFloor &&
      (r88MikroSkor >= 8 || r89SuperMikroYapiOk) && (r88AkisTeyidiSayisi >= 3 || (r89SuperMikroYapiOk && r88AkisTeyidiSayisi >= 2)) &&
      (!r75LateChaseHard || r37Side?.retestOk || r39Side?.breakConfirmed || fresh5mImpulse || r89SuperMikroYapiOk) &&
      (!r39TargetNearBlock || r39Side?.breakConfirmed || r62CounterTrendTrapContextOk || (r89SuperMikroYapiOk && r88CanliHamleIzi)) &&
      !mmVeryStrongOpposite && !r86KarsiFormasyonGucu)
    );
    const r92Terazi = Number(r50EffectivePriority || priorityScore || 0);

    const r109Reclaim = r109CalcSweepReclaimScore(k5m || [], isL ? 'LONG' : 'SHORT');
    const r109ReclaimOk  = !!(r109Reclaim.score >= 6 && r109Reclaim.swept && r109Reclaim.reclaimed);
    const r109ReclaimSkor = Number(r109Reclaim.score || 0);
    const r109SweepReclaimKoprusuOk = !!(
      r88VurKacEnabled && !sweepRequired && !r93PiyasaHamTehlikeli &&
      r109ReclaimOk &&
      Number(r47Readiness || 0) >= 6 && Number(r88MikroSkor || 0) >= 8 &&
      Number(r88AkisTeyidiSayisi || 0) >= 2 && Number(r92Terazi || 0) >= 25 &&
      !mmVeryStrongOpposite && !r86KarsiFormasyonGucu &&
      !r41FallingKnifeBlock && !r41RisingKnifeBlock
    );
    const r96DalgaliZeminVurKacOk = !!(
      r88VurKacEnabled && !sweepRequired && !r93PiyasaHamTehlikeli && r93DalgaliAmaIslemYapilabilir &&
      (r88CanliHamleIzi || r93MerdivenDevamOk || r93DonusRadariOk || directSweepOk || hardSweepForBridge || huntBridgeOk) &&
      (r93MerdivenDevamOk || r93DonusRadariOk || r90CanliKopmaOk || directSweepOk || hardSweepForBridge || huntBridgeOk) &&
      Number(r47Readiness || 0) >= 7 && Number(r88MikroSkor || 0) >= 14 &&
      Number(r88AkisTeyidiSayisi || 0) >= 3 && Number(r92Terazi || 0) >= 45 &&
      !atrExtremeBlock && !r88OynaklikAsiri && !mmVeryStrongOpposite && !r86KarsiFormasyonGucu &&
      !r41FallingKnifeBlock && !r41RisingKnifeBlock &&
      (!r39TargetNearBlock || r39Side?.breakConfirmed || r62CounterTrendTrapContextOk || r93DonusRadariOk || Number(r47TimingPts || 0) >= 3)
    );
    const r92DefterSaglam = !!(!poorLiquidity && !r88SpreadWide && !r88DefterInce && !r88OynaklikAsiri && !atrExtremeBlock);
    const r93EmirZeminiOk = !!(r92DefterSaglam || r93DalgaliAmaIslemYapilabilir);
    const r93PiyasaEtiketi = r92DefterSaglam ? 'SAĞLAM' : r93DalgaliAmaIslemYapilabilir ? 'DALGALI AMA İŞLEM YAPILABİLİR' : r93PiyasaTehlikeli ? 'TEHLİKELİ' : r88PiyasaBozuk ? 'BOZUK' : 'SAĞLAM';
    const r93SonMumKoru = !!(r93SonMumTers && (r93MerdivenDevamOk || r89SuperMikroYapiOk || (r88CanliHamleIzi && Number(r47Readiness||0) >= 8)) && !r39Side?.breakConfirmed && !fresh5mImpulse && !directSweepOk && !r93DonusRadariOk);
    const r110LongOk    = !!(r110ICT?.longOk);
    const r110ShortOk   = !!(r110ICT?.shortOk);
    const r110EntryOk   = !!(r110ICT?.entryOk);
    const r110YonUyumlu = !!(isL ? r110LongOk : r110ShortOk);
    const r110Phase     = String(r110ICT?.phase || 'BEKLE');
    const r110SSLAlindi = !!(r110ICT?.sslSwept);
    const r110BSLAlindi = !!(r110ICT?.bslSwept);
    const r110ChoCH     = !!(isL ? r110ICT?.bullishChoCH : r110ICT?.bearishChoCH);
    const r110FVG       = isL ? r110ICT?.bullishFVG : r110ICT?.bearishFVG;
    const r110NearSSL   = r110ICT?.nearSSL || null;
    const r110NearBSL   = r110ICT?.nearBSL || null;
    const r110IctKoprusuOk = !!(
      r110YonUyumlu && r110ChoCH && !hardVeto &&
      Number(r47Readiness || 0) >= 6 && Number(r88MikroSkor || 0) >= 8 &&
      Number(r88AkisTeyidiSayisi || 0) >= 2 &&
      !mmVeryStrongOpposite && !r41FallingKnifeBlock && !r41RisingKnifeBlock
    );
    const r111SiksmaBreakout = !!(r111Siksma?.squeezeBreakout);
    const r111SqueezeSkor    = Number(r111Siksma?.squeezeSkor || 0);
    const r111SiksmaAdet     = Number(r111Siksma?.siksmaAdet || 0);
    const r111LongOk         = !!(r111Siksma?.longOk);
    const r111ShortOk        = !!(r111Siksma?.shortOk);
    const r111ShortSqueeze   = !!(r111Siksma?.shortSqueeze);
    const r111LongSqueeze    = !!(r111Siksma?.longSqueeze);
    const r111OiChgPct       = Number(r111Siksma?.oiChgPct || 0);
    const r111ObBaskisi      = String(r111Siksma?.obBaskisi || 'YOK');
    const r111DemandOB       = r111Siksma?.demandOB || null;
    const r111SupplyOB       = r111Siksma?.supplyOB || null;
    const r111YonUyumlu      = !!(isL ? r111LongOk : r111ShortOk);
    const r111BloklandMi     = !!(r93MerdivenDevamOk || r110ChoCH);
    const r111KoprusuOk = !!(
      !r111BloklandMi && r111YonUyumlu &&
      r111SiksmaAdet >= 8 && r111SqueezeSkor >= 3 &&
      !hardVeto && !mmVeryStrongOpposite &&
      Number(r47Readiness || 0) >= 5 &&
      Number(r88MikroSkor || 0) >= 6 &&
      !r41FallingKnifeBlock && !r41RisingKnifeBlock
    );
    const r92GucluVurKacOk = !!(
      (r92VurKacAdayOk || r96DalgaliZeminVurKacOk) && r93EmirZeminiOk && !r93SonMumKoru && Number(r88MikroSkor || 0) >= 15 &&
      Number(r88AkisTeyidiSayisi || 0) >= 4 && Number(r47Readiness || 0) >= 9 &&
      Number(r47TimingPts || 0) >= 2 && r92Terazi >= 45
    );
    const r92NormalVurKacOk = !!(
      (r92VurKacAdayOk || r96DalgaliZeminVurKacOk || r109SweepReclaimKoprusuOk || r110IctKoprusuOk || r111KoprusuOk) && r93EmirZeminiOk && !r93SonMumKoru && !r92GucluVurKacOk &&
      Number(r88MikroSkor || 0) >= 12 && Number(r88AkisTeyidiSayisi || 0) >= 3 &&
      Number(r47Readiness || 0) >= 8 && Number(r47TimingPts || 0) >= 2 &&
      (r92Terazi >= 25 || r89SuperMikroYapiOk || r90CanliKopmaOk || r96DalgaliZeminVurKacOk || r109ReclaimOk || r110IctKoprusuOk)
    );
    const r92SadeceIzleOk = !!(r92VurKacAdayOk && !r92GucluVurKacOk && !r92NormalVurKacOk);
    const r92IslemTipi = r92GucluVurKacOk ? 'GÜÇLÜ VUR-KAÇ' : r92NormalVurKacOk ? 'NORMAL VUR-KAÇ' : r92SadeceIzleOk ? 'SADECE İZLE' : 'YOK';
    const r92RiskDurumu = !r93EmirZeminiOk ? 'PİYASA ZEMİNİ BOZUK' : r92Terazi >= 45 ? 'DÜŞÜK' : r92Terazi >= 25 ? 'ORTA' : 'YÜKSEK';
    const r88VurKacOk = !!(r92GucluVurKacOk || r92NormalVurKacOk || r96DalgaliZeminVurKacOk || r109SweepReclaimKoprusuOk || r110IctKoprusuOk || r111KoprusuOk);

    const r50AutoPermissionOk = !!(r88VurKacOk || r86FormasyonVeriTeyitOk || r75RetestBridgeOk || r74Top10ProScalperOk || r68UnifiedScalperCoreOk || r67ScalperCoreHuntEntryOk || r65ScalperCoreOk || r50DirectSweepMatrixOk || r50NonSweepMatrixOk || r51DirectSweepMinEdgeOk || r53SmartEdgeScoreOk || r54MicroProbeOk || r57ScalperBTierBridgeOk || r61TrendContinuationBridgeOk || r62CounterTrendTrapBridgeOk);

    const nonSweepQualityOk = r88VurKacOk || r86FormasyonVeriTeyitOk || r75RetestBridgeOk || r74Top10ProScalperOk || r68UnifiedScalperCoreOk || r67ScalperCoreHuntEntryOk || r65ScalperCoreOk || r47CompositeNonSweepOk || r48DirectSweepBalanceOk || r49DirectSweepUnlockOk || r50NonSweepMatrixOk || r53SmartEdgeScoreOk || r54MicroProbeOk || r57ScalperBTierBridgeOk || r61TrendContinuationBridgeOk || r62CounterTrendTrapBridgeOk;

    function r116RecentLevelAcceptance(level, dir) {
      const rows = (Array.isArray(k5m) ? k5m : []).slice(-5).map(k => ({o:+k[1], h:+k[2], l:+k[3], c:+k[4]})).filter(k=>k.c>0);
      if (!level || rows.length < 2) return false;
      const last = rows.at(-1), prev = rows.at(-2);
      const zH = Number(level.zoneHigh || level.price || 0);
      const zL = Number(level.zoneLow  || level.price || 0);
      const range = Math.max(last.h-last.l, 0);
      const closePos = range > 0 ? (last.c-last.l)/range : 0.5;
      const bodyLow = Math.min(last.o,last.c), bodyHigh = Math.max(last.o,last.c);
      if (dir === 'LONG') {
        return !!(zH > 0 && last.c > zH * 1.001 && bodyLow > zH * 0.998 && closePos >= 0.55 && prev.c > zH * 0.996);
      }
      return !!(zL > 0 && last.c < zL * 0.999 && bodyHigh < zL * 1.002 && closePos <= 0.45 && prev.c < zL * 1.004);
    }
    const r116CounterLevel = isL ? r110NearBSL : r110NearSSL;
    const r116CounterApproach = !!(isL ? r110ICT?.approachingBSL : r110ICT?.approachingSSL);
    const r116CounterSweepTaken = !!(isL ? r110BSLAlindi : r110SSLAlindi);
    const r116CounterTf = String(r116CounterLevel?.label || r116CounterLevel?.tf || '');
    const r116CounterDist = Number(r116CounterLevel?.dist ?? 999);
    const r116CounterMajor = !!(r116CounterLevel && (['4H','1H'].includes(r116CounterTf) || Number(r116CounterLevel?.strength||0) >= 6));
    const r116NearCounterHTF = !!(r116CounterMajor && (r116CounterApproach || r116CounterDist <= 0.95));
    const r116AcceptedCounterBreak = r116RecentLevelAcceptance(r116CounterLevel, isL ? 'LONG' : 'SHORT');
    const r116CleanStructuralEntry = !!(r110IctKoprusuOk || r111KoprusuOk);
    const r116LegacyRoute = !!(
      (r92VurKacAdayOk || r96DalgaliZeminVurKacOk || r109SweepReclaimKoprusuOk || r88VurKacOk || r50AutoPermissionOk || nonSweepQualityOk) &&
      !r116CleanStructuralEntry
    );
    const r116HtfGuardBlock = !!(
      r116LegacyRoute && r116NearCounterHTF && !r116AcceptedCounterBreak &&
      (r116CounterSweepTaken || r116CounterDist <= 0.75 || String(isL ? pd4h?.zone||'' : pd4h?.zone||'').includes(isL ? 'PREMIUM' : 'DISCOUNT'))
    );
    const r116HtfGuardReason = r116HtfGuardBlock
      ? `R116 HTF likidite amiri: ${isL?'LONG':'SHORT'} eski köprüyle açılamaz; karşı ${isL?'BSL/direnç':'SSL/destek'} yakın (${r116CounterTf||'-'} ${r116CounterLevel?.price||'-'} uzaklık %${Number(r116CounterDist||0).toFixed(2)}). Önce 5m gövde kabulü veya R115 body-reclaim/MSS beklenir.`
      : '';

    const r114Shift = r114Analyze5mShift(k5m);
    const r114OppositeShift = !!(isL ? r114Shift.bearShift : r114Shift.bullShift);
    const r114ReclaimOk = !!(isL ? r114Shift.longReclaim : r114Shift.shortReclaim);
    const r114ExtremeZone = !!(isL
      ? (String(pd1h?.zone||'').includes('PREMIUM') || String(pd4h?.zone||'').includes('PREMIUM') || rsi4h >= 68)
      : (String(pd1h?.zone||'').includes('DISCOUNT') || String(pd4h?.zone||'').includes('DISCOUNT') || rsi4h <= 32));
    const r114SweepTrapFamily = !!(
      directSweepOk || hardSweepForBridge || huntBridgeOk || r88VurKacOk ||
      mmTarget === (isL ? 'UP_SWEEP' : 'DOWN_SWEEP')
    );
    const r114ContinuationProof = !!(
      r110IctKoprusuOk || r111KoprusuOk ||
      (r61TrendContinuationBridgeOk && deltaOkStrict && (oiBridgeOk || obSameSide)) ||
      (r93MerdivenDevamOk && r88AkisTeyidiSayisi >= 4 && r114ReclaimOk && !r93SonMumKoru)
    );
    const r114TrapBlock = !!(
      r114SweepTrapFamily && r114OppositeShift && !r114ReclaimOk && !r114ContinuationProof &&
      (r114ExtremeZone || Math.abs(Number(r114Shift.net9Pct||0)) >= Math.max(1.0, Number(r114Shift.avgRangePct||0)*1.15))
    );
    const r114TrapReason = r114TrapBlock
      ? `R115 5m body-shift tuzağı: ${isL?'LONG':'SHORT'} için ters akış var; sweep/stop-hunt wick sayılır, gövde reclaim beklenir — ${r114Shift.ozet}`
      : '';

    const r117TrapLevel = isL ? r110NearSSL : r110NearBSL;
    const r117TrapApproach = !!(isL ? r110ICT?.approachingSSL : r110ICT?.approachingBSL);
    const r117TrapSweepTaken = !!(isL ? r110SSLAlindi : r110BSLAlindi);
    const r117TrapTf = String(r117TrapLevel?.label || r117TrapLevel?.tf || '');
    const r117TrapDist = Number(r117TrapLevel?.dist ?? 999);
    const r117TrapMajor = !!(r117TrapLevel && (['4H','1H'].includes(r117TrapTf) || Number(r117TrapLevel?.strength||0) >= 6));
    const r117NearTrapHTF = !!(r117TrapMajor && (r117TrapApproach || r117TrapDist <= 0.95));
    const r118Candle = r118AnalyzeCandlePlaybook(k5m, isL ? 'LONG' : 'SHORT', r117TrapLevel);
    const r118CandleOk = !!(r118Candle?.ok);
    const r118CandleStrong = !!(r118Candle?.strong);
    const r118CandleOzet = r118Candle?.ozet || '';
    const r117AcceptedAgainst = r116RecentLevelAcceptance(r117TrapLevel, isL ? 'SHORT' : 'LONG');
    const r117BodyReclaimOk = !!(isL ? r114Shift.longReclaim : r114Shift.shortReclaim);
    const r117BodyShiftOk = !!(isL ? r114Shift.bullShift : r114Shift.bearShift);
    const r117MssOk = !!(isL ? r110ICT?.bullishChoCH : r110ICT?.bearishChoCH);
    const r117IctSameSideOk = !!(isL ? r110ICT?.longOk : r110ICT?.shortOk);
    const r117TrapEvidenceRawOk = !!(
      r117TrapSweepTaken || r117IctSameSideOk || r117MssOk || r117BodyReclaimOk || r118CandleOk ||
      (r117BodyShiftOk && (r117TrapDist <= 0.70 || r117TrapApproach)) ||
      r93DonusRadariOk || r62CounterTrendTrapBridgeOk || r65ScalperCoreCounterTrapOk || r66WyckoffTrapReclaimOk
    );
    const r117PrecisionCandleOk = !!(r117IctSameSideOk || (r117MssOk && r117BodyReclaimOk) || r118CandleOk || (r118CandleStrong && r117TrapSweepTaken));
    const r117TrapEvidenceOk = !!(r117TrapEvidenceRawOk && r117PrecisionCandleOk);
    const r117R125SideFlow = r125FlowForSide(r125Flow, isL ? 'LONG' : 'SHORT');
    const r117LiveFlowBase = r120Bool(deltaOkStrict || r117R125SideFlow.ok || r117R125SideFlow.strong);
    const r117SecondaryTeyit = r120Bool(oiBridgeOk || obSameSide || fundBridgeOk || mmSameSide || cvdBridgePass ||
      (r22FundingTrap.detected && ((isL && r111ShortSqueeze) || (!isL && r111LongSqueeze))));
    const r117FlowOk = r120Bool(
      (r117LiveFlowBase && r117SecondaryTeyit) ||
      (r117LiveFlowBase && Number(r88AkisTeyidiSayisi||0) >= 3) ||
      (deltaOkStrict && r117R125SideFlow.ok)
    );
    const r117HtfReverseOk = !!(
      r117NearTrapHTF && !r117AcceptedAgainst && r117TrapEvidenceOk && r117FlowOk &&
      r117LiveFlowBase &&
      !hardVeto && !signalDecayAutoBlock && !poorLiquidity && !atrExtremeBlock &&
      r93EmirZeminiOk && Number(r47Readiness || 0) >= 6 && Number(r88MikroSkor || 0) >= 8 &&
      Number(r88AkisTeyidiSayisi || 0) >= 2 &&
      !(isL ? r41FallingKnifeBlock : r41RisingKnifeBlock)
    );
    const r117HtfReverseReason = r117HtfReverseOk
      ? `R118 HTF ters-köşe hedefi: ${isL?'SSL/destek/demand → LONG':'BSL/direnç/supply → SHORT'} (${r117TrapTf||'-'} ${r117TrapLevel?.price||'-'} uzaklık %${Number(r117TrapDist||0).toFixed(2)}) · kanıt:${r117TrapSweepTaken?'sweep ':''}${r117MssOk?'MSS ':''}${r117BodyReclaimOk?'body-reclaim ':''}${r117BodyShiftOk?'body-shift ':''}${r118CandleOk?'mum-formasyon ':''}· ${r118CandleOzet}`
      : '';

    const entryPermissionOk = (sweepRequired ? directSweepOk : (directSweepOk || nonSweepQualityOk || r50AutoPermissionOk || r117HtfReverseOk)) && !r114TrapBlock && (!r116HtfGuardBlock || r117HtfReverseOk);
    const entryPermissionReason = r117HtfReverseOk ? 'R118_HTF_CANDLE_REVERSAL_TARGET'
      : r116HtfGuardBlock ? 'R116_HTF_SUPERVISOR_BLOCK'
      : r110IctKoprusuOk ? 'R115_HTF_LIQUIDITY_BODY_RECLAIM'
      : r111KoprusuOk ? 'R111_SIKISMA_PATLAMA'
      : r109SweepReclaimKoprusuOk ? 'R109_SWEEP_RECLAIM_TEMIZ'
      : r96DalgaliZeminVurKacOk ? 'R97_DALGALI_ZEMIN_CANLI_VURKAC'
      : r93DonusRadariOk && r88VurKacOk ? 'R97_MERDIVEN_DONUS_RADARI'
      : r93MerdivenDevamOk && r88VurKacOk ? 'R97_CANLI_MERDIVEN_DEVAM'
      : r90CanliKopmaOk ? 'R97_5M_CANLI_KOPMA'
      : r88VurKacOk ? 'R97_AKILLI_VUR_KAC_TERAZI'
      : r86FormasyonVeriTeyitOk ? 'R86_FORMASYON_VERI_TEYIT'
      : r74Top10ProScalperOk ? 'R74_TOP10_5M_PRO_SCALPER'
      : r75RetestBridgeOk ? 'R75_RETEST_BRIDGE'
      : r69PriorityExecutionOk ? 'R69_PRIORITY_CONTEXT_EXECUTION'
      : r68UnifiedScalperCoreOk ? 'R69_UNIFIED_SCALPER_CORE'
      : r67ScalperCoreHuntEntryOk ? 'R67_SCALPER_CORE_HUNT_ENTRY'
      : (r65ScalperCoreTrendOk && r66WyckoffTrapReclaimOk) ? 'R66_SCALPER_CORE_WYCKOFF_RECLAIM'
      : r65ScalperCoreCounterTrapOk ? 'R65_SCALPER_CORE_COUNTER_TRAP'
      : r65ScalperCoreTrendOk ? 'R65_SCALPER_CORE_TREND'
      : r62CounterTrendTrapBridgeOk ? 'R62_COUNTER_TREND_TRAP_BRIDGE'
      : r61TrendContinuationBridgeOk ? 'R61_TREND_CONTINUATION_BRIDGE'
      : r57ScalperBTierBridgeOk ? 'R57_SCALPER_B_TIER_BRIDGE'
      : r54MicroProbeOk ? 'R54_MICRO_PROBE_BPLUS'
      : r53SmartEdgeScoreOk ? 'R53_SMART_EDGE_SCORE_PRIORITY'
      : r50DirectSweepMatrixOk ? 'R50_DIRECT_SWEEP_MATRIX'
      : r51DirectSweepMinEdgeOk ? 'R51_DIRECT_SWEEP_MIN_EDGE'
      : r50NonSweepMatrixOk ? 'R50_NON_SWEEP_MATRIX'
      : directSweepOk ? (r49DirectSweepUnlockOk ? 'DIRECT_SWEEP_BPLUS_R49' : 'DIRECT_SWEEP')
      : (!sweepRequired && nonSweepQualityOk) ? 'NON_SWEEP_5M_BRIDGE_R47'
      : sweepRequired ? 'SWEEP_REQUIRED_FAIL'
      : 'ENTRY_PERMISSION_FAIL_R68';

    const r35ScalperBridge = !!(
      !hardVeto && !signalDecayAutoBlock && !rvolVeryLow && !poorLiquidity && r42FlowGate &&
      sc >= minAutoScore && priorityScore >= 64 &&
      (hasEntry || nonSweepQualityOk || (softEntry && fresh5mImpulseOrRecent) || (softEntry && r37EarlyOk)) &&
      microConfirmR35 && !r37RetestWait &&
      (bridgeCount >= 2 || priorityScore >= 72 || (r37EarlyOk && priorityScore >= 66)) &&
      !mtfStrongOpposite && !mmVeryStrongOpposite
    );

    const isTierA = !hardVeto && !signalDecayAutoBlock && !r37RetestWait && entryPermissionOk && directSweepOk && hasEntry && deltaOkStrict && microConfirmR35 && sc>=Math.max(68, minAutoScore) && priorityScore>=62;
    const r53TierScoreOk = !!(sc >= minAutoScore || r117HtfReverseOk || r88VurKacOk || r86FormasyonVeriTeyitOk || r75RetestBridgeOk || r74Top10ProScalperOk || r68UnifiedScalperCoreOk || r67ScalperCoreHuntEntryOk || r65ScalperCoreOk || r53SmartEdgeScoreOk || r54MicroProbeOk || r57ScalperBTierBridgeOk || r61TrendContinuationBridgeOk || r62CounterTrendTrapBridgeOk);
    const isTierBPlus = !isTierA && (r117HtfReverseOk || r88VurKacOk || r86FormasyonVeriTeyitOk || r75RetestBridgeOk || r74Top10ProScalperOk || r68UnifiedScalperCoreOk || r67ScalperCoreHuntEntryOk || r65ScalperCoreOk || (!hardVeto && !signalDecayAutoBlock && !r37RetestWait)) && entryPermissionOk && r53TierScoreOk && (
          r117HtfReverseOk ||
          r88VurKacOk ||
          r86FormasyonVeriTeyitOk ||
          r75RetestBridgeOk ||
          r74Top10ProScalperOk ||
          r68UnifiedScalperCoreOk ||
          r67ScalperCoreHuntEntryOk ||
          r65ScalperCoreOk ||
          (directSweepOk && hasEntry && cvdBridgePass && r42FlowGate && microConfirmR35 && priorityScore>=58) ||
          (!sweepRequired && nonSweepQualityOk) ||
          r48DirectSweepBalanceOk ||
          r49DirectSweepUnlockOk ||
          r50AutoPermissionOk ||
          r53SmartEdgeScoreOk ||
          r57ScalperBTierBridgeOk ||
          r61TrendContinuationBridgeOk ||
          r62CounterTrendTrapBridgeOk ||
          r35ScalperBridge
        );
    const isTierB = !isTierA && !isTierBPlus && (softEntry||hasEntry||nonSweepQualityOk) && fundOk && sc>=55;

    const reasons=[], blocks=[];
    if(amd5m?.signal===(isL?'AMD_LONG':'AMD_SHORT')) reasons.push('⚡ AMD');
    if(hardSweepForBridge) reasons.push('✅ Sweep+Teyit');
    if(isL && wy1?.recentEvents?.some(e=>e.type==='SPRING'||e.type==='SOS')) reasons.push('🌊 Wyckoff');
    if(!isL && wy1?.recentEvents?.some(e=>e.type==='UTAD')) reasons.push('🚨 UTAD');
    if(isL && wy1?.recentEvents?.some(e=>e.type==='UTAD')) blocks.push('🚨 UTAD long karşıtı');
    if(!isL && wy1?.recentEvents?.some(e=>e.type==='SPRING'||e.type==='SOS')) blocks.push('🌊 Spring/SOS short karşıtı');
    if(huntBridgeOk) reasons.push('🎣 Stop Hunt');
    if(mmSameSide) reasons.push(`🏦 MM ${mmTarget}`);
    if(oiBridgeOk) reasons.push(`📈 OI ${oiDiv}`);
    if(fundBridgeOk) reasons.push(`💸 Funding ${fundSig}`);
    if(r22FundingTrap.detected) reasons.push('💣 Funding tuzağı');
    if(r22Rotation.active) reasons.push(`🎯 ${r22Rotation.tag} ${r22Rotation.symbols.join('/')}`);
    if(r22Decay.bonus>0) reasons.push(`⏳ ${r22Decay.label}`);
    if(r22TestQ.first||r22TestQ.second) reasons.push(`🔁 ${r22TestQ.label}`);
    if(r22LiqWaterfall.favorable&&!r22LiqWaterfall.adverse) reasons.push('🌊 Liq şelalesi lehte');
    if(r39Confluence) reasons.push(`🧱 R39 ${isL?'destek':'direnç'} ${isL?(r39Side?.nearestSupport?.type||''):(r39Side?.nearestResistance?.type||'')}`);
    if(r39Side?.breakConfirmed) reasons.push(`🚀 R39 ${isL?'breakout':'breakdown'}`);
    if(r46PerfectAlignBonus > 0) reasons.push(`🧠 R46 uyum ${r46PerfectAlignCount}/5`);
    if(r46CvdGradeBonus > 0) reasons.push(`📊 R46 CVD kademeli +${r46CvdGradeBonus}`);
    if(r46SqueezeQualityScore > 0) reasons.push(`⚡ R46 sıkışma kalitesi +${r46SqueezeQualityScore}`);
    if(r46SpringQuality > 0) reasons.push(`🌱 R46 spring kalite +${r46SpringQuality}`);
    if(r46ExhaustionShort) reasons.push('🚨 R46 exhaustion short birleşimi');
    if((isL&&r29Context.preferSide==='LONG')||(!isL&&r29Context.preferSide==='SHORT')) reasons.push(`🧠 R29 bağlam lehte`);
    if(r196RangeCaution && !r196RangeBlock) reasons.push(`⚠️ ${r196SideCtx?.label||'R196 range dikkat'}`);
    if(r281Map?.runner) reasons.push(`🧠 R281 grafik trader RUNNER: ${r281Map.summary}`);
    else if(r281Map?.protect) reasons.push(`🛡️ R281 grafik trader PROTECT: ${r281Map.summary}`);
    else if(Number(r281Map?.favorable||0) >= 7) reasons.push(`🧠 R281 grafik trader: ${r281Map.summary}`);
    if(isL&&r29Context.vwap?.longReclaim) reasons.push('🧭 VWAP reclaim');
    if(!isL&&r29Context.vwap?.shortReject) reasons.push('🧭 VWAP reject');
    if(cmfSameSide) reasons.push('💧 CMF');
    if(weisSameSide) reasons.push('🌊 Weis');
    if(chochSameSide) reasons.push('🔄 ChoCH');
    if(cvdValid&&deltaOkStrict) reasons.push(`📊 CVD${cvdRatio.toFixed(0)}%`);
    else if(!cvdValid && cvdBridgePass) reasons.push(`🟡 CVD yok ama terazi güçlü ${priorityScore}`);
    else if(!cvdValid) reasons.push('📊 CVD ısınma/veri yok');
    if(directSweepOk) reasons.push(`🔐 Sweep kapısı: ${sweepRequired?'AÇIK':'KAPALI'} / DIRECT_SWEEP`);
    else if(!sweepRequired && r47CompositeNonSweepOk) reasons.push(`🟦 R47 non-sweep köprü ${r47Readiness}/${r47Needed} (T${r47TimingPts}/F${r47FlowPts}/C${r47ContextPts}/S${r47StructurePts}/V${r47RvolPts})`);
    if(!sweepRequired && r48DirectSweepBalanceOk) reasons.push(`🟪 R48 direct-sweep denge köprüsü ${r47Readiness}/${r38TopMoverStrong?6:7} (T${r47TimingPts}/F${r47FlowPts}/C${r47ContextPts}/S${r47StructurePts}/V${r47RvolPts})`);
    if(!sweepRequired && r49DirectSweepUnlockOk) reasons.push(`🟣 R49 direct-sweep B+ unlock ${r47Readiness}/${r38TopMoverStrong?8:10} (T${r47TimingPts}/F${r47FlowPts}/C${r47ContextPts}/S${r47StructurePts}/V${r47RvolPts})`);
    if(!sweepRequired && r50DirectSweepMatrixOk) reasons.push(`🧩 R50 direct-sweep matrix ${r47Readiness}/${r50MinReadiness} P${priorityScore}+${r50PriorityBoost}=${r50EffectivePriority}`);
    if(!sweepRequired && r51DirectSweepMinEdgeOk) reasons.push(`🧩 R51 direct-sweep min-edge ${r47Readiness}/${r50MinReadiness} P${priorityScore}+${r50PriorityBoost}=${r50EffectivePriority}`);
    if(!sweepRequired && r50NonSweepMatrixOk) reasons.push(`🧩 R50 non-sweep matrix ${r47Readiness}/${r47Needed} P${priorityScore}+${r50PriorityBoost}=${r50EffectivePriority}`);
    if(!sweepRequired && r53SmartEdgeScoreOk) reasons.push(`🧠 R53 smart-edge score ${sc}+${r53SmartEdgeBoost}=${r53EffectiveScore} / P${r50EffectivePriority}`);
    if(!sweepRequired && r54MicroProbeOk) reasons.push(`🧪 R54 micro-probe B+ score ${sc}/${minAutoScore} R47 ${r47Readiness}/8 P${r50EffectivePriority}`);
    if(!sweepRequired && r57ScalperBTierBridgeOk) reasons.push(`🦅 R57 scalper B→B+ score ${sc}/${minAutoScore} R47 ${r47Readiness}/8 P${r50EffectivePriority}`);
    if(!sweepRequired && r61TrendContinuationBridgeOk) reasons.push(`🚀 R61 trend devamı score ${sc}+${r61TrendContinuationBoost}=${r61TrendEffectiveScore} R47 ${r47Readiness}/8 P${r50EffectivePriority}`);
    if(r90CanliKopmaOk) reasons.push(`🚀 R97 canlı 5dk kopma: mikro ${r88MikroSkor}/8 · teyit ${r88AkisTeyidiSayisi}/8 · taban ${r89ScoreFloor}`);
    if(r93MerdivenDevamOk) reasons.push(`🪜 R97 canlı merdiven devamı: merdiven ${r93MerdivenSkor}/10 · son mum ${r93Merdiven.sonMum}`);
    if(r93DonusRadariOk) reasons.push(`🔁 R97 dönüş radarı: dönüş puanı ${r93DonusSkor}/10 · ${r93Merdiven.notlar.join(' + ')}`);
    if(r111KoprusuOk) reasons.push(`🧨 R115 sıkışma/squeeze köprüsü: ${r111Siksma?.ozet||'sıkışma patlaması'} · skor ${r111SqueezeSkor}/4`);
    if(r117HtfReverseOk) reasons.push(`🎯 ${r117HtfReverseReason}`);
    if(r88VurKacOk) reasons.push(`⚡ R97 akıllı vur-kaç terazisi: skor ${r88MikroSkor}/8 · teyit ${r88AkisTeyidiSayisi}/8 · taban ${r89ScoreFloor}${r89SuperMikroYapiOk?' · süper-mikro':''}`);
    if(r86FormasyonVeriTeyitOk) reasons.push(`🕯️ R86 formasyon+veri teyidi: ${trPatternList(r86FormasyonAdlari)||'formasyon'} · veri ${r86VeriTeyitSayisi}/8`);
    if(!sweepRequired && r69PriorityExecutionOk) reasons.push(`⚡ R69 priority scalper core score ${sc}/${minAutoScore} R47 ${r47Readiness}/8 P${r50EffectivePriority}`);
    else if(!sweepRequired && r74Top10ProScalperOk) reasons.push(`⚡ R74 TOP10 5m impulse core skor ${sc}/${r74ScoreFloor} R47 ${r47Readiness}/${r75R47MinBypass}`);
    else if(r75RetestBridgeOk) reasons.push(`↩ R75 retest köprüsü skor ${sc}/${r74ScoreFloor} earlyScore ${r37Side?.earlyScore||0}`);
    else if(!sweepRequired && r68UnifiedScalperCoreOk) reasons.push(`⚡ R69/R68 unified scalper core score ${sc}/${minAutoScore} R47 ${r47Readiness}/8`);
    if(!sweepRequired && r67ScalperCoreHuntEntryOk) reasons.push(`⚡ R67 scalper core hunt-entry score ${sc}/${minAutoScore} R47 ${r47Readiness}/8`);
    if(!sweepRequired && r65ScalperCoreOk) reasons.push(`⚡ R65 scalper core ${r65ScalperCoreCounterTrapOk?'counter-trap':'trend'} score ${sc}/${minAutoScore} R47 ${r47Readiness}/8`);
    if(!sweepRequired && r62CounterTrendTrapBridgeOk) reasons.push(`🎯 R62 karşı-trend trap ${isL?'LONG':'SHORT'} R47 ${r47Readiness}/8 P${r50EffectivePriority}`);
    if(r45CvdAlternativeOk) reasons.push('🟡 CVD eksik ama OI/Book/5m güçlü köprü');
    if(r45RvolLowButWatch) reasons.push(`📊 RVOL düşük izleme ${r45Rvol.toFixed(2)}x`);
    if(r45RvolDead) reasons.push(`📊 RVOL ölü/ikinci impuls izleme ${r45Rvol.toFixed(2)}x`);
    if(softEntry&&!hasEntry) reasons.push('👁 Yumuşak sinyal');

    if(!entryPermissionOk) {
      const r47Dbg = !sweepRequired ? ` · R47 ${r47Readiness}/${r47Needed} T${r47TimingPts}/F${r47FlowPts}/C${r47ContextPts}/S${r47StructurePts}/V${r47RvolPts}` : '';
      blocks.push(r116HtfGuardBlock ? r116HtfGuardReason : (r114TrapBlock ? r114TrapReason : `5m Fırsat Beyni izle: emir için kanıt yetersiz / ${entryPermissionReason}${r47Dbg}`));
    }
    if(!hasEntry&&!softEntry&&!nonSweepQualityOk&&!r117HtfReverseOk) blocks.push('Sinyal yok');
    if(!deltaOk) blocks.push(cvdValid?`Delta ters(${cvdRatio.toFixed(0)}%)`:'CVD eksik veya gerçek sweep köprüsü zayıf');
    if(cvdWarmingBridge && !cvdBridgeQualityOk) blocks.push(`CVD köprüsü zayıf (${bridgeCount}/4, terazi ${priorityScore})`);
    if(signalDecayAutoBlock) blocks.push(`Bayat sweep: ${r22Decay.label}`);
    if(r22LiqWaterfall.adverse) blocks.push('Likidasyon şelalesi ters risk');
    if(isL && r29Context.longAutoBlock) blocks.push(`R29 bağlam LONG blok ${r29Context.longRisk}: ${r29Context.longNotes.slice(0,2).join(' + ')}`);
    if(!isL && r29Context.shortAutoBlock) blocks.push(`R29 bağlam SHORT blok ${r29Context.shortRisk}: ${r29Context.shortNotes.slice(0,2).join(' + ')}`);
    if(r196RangeBlock) blocks.push(r196SideCtx?.reason || 'R196 günlük range lokasyonu blok');
    if(r190Edge?.spreadBlock) blocks.push(`R190 spread pahalı: ${r190Edge.spreadPct}%`);
    if(r281Map?.hardNo && !r281Map?.runner) blocks.push(`R281 son kale RED: ${r281Map.summary}`);
    if(r190Edge?.lateTrapRisk && !r190Edge?.squeeze) blocks.push(`R190 geç/ters akış riski: ${r190Edge.summary}`);
    if(r37LateChaseBlock) blocks.push(`R37 geç giriş/chase: ${r37Side?.reason||''}`);
    if(r37RetestWait) blocks.push(`R37 hareket kaçmış: retest bekleniyor (${r37Side?.reason||''})`);
    if(r39TargetNearBlock) blocks.push(`R39 hedef çok yakın — market chase yok (${r39Side?.reason||''})`);
    else if(r39AgainstZone && !r39Side?.breakConfirmed) blocks.push(`R39 karşı seviye yakın (${r39Side?.reason||''})`);
    if(r88VurKacEnabled && r88PiyasaBozuk) blocks.push(`R88 piyasa bozuk: makas ${r88Spread||'?'} derinlik ${r88Depth||'?'} ATR ${atrPct.toFixed(2)}`);
    if(priorityScore<50) blocks.push(`Terazi düşük ${priorityScore}`);
    if(hardVetoReasons.length) blocks.push(...hardVetoReasons.slice(0,2));
    if(sc<55) blocks.push(`Skor düşük(${sc})`);

    const tier=isTierA?'A':isTierBPlus?'B+':isTierB?'B':'WAIT';
    if((tier==='B'||tier==='WAIT') && softEntry && !hasEntry && !nonSweepQualityOk) blocks.push('Yumuşak bağlam var ama canlı emir için gerçek giriş izi yok');
    if((isTierA||isTierBPlus) && !microConfirmR35) blocks.push('Mikro teyit eksik');
    const passCount=[entryPermissionOk,hasEntry||softEntry||nonSweepQualityOk,deltaOk,fundOk,rsiOk,mmOk,sc>=55,priorityScore>=50,!hardVeto].filter(Boolean).length;
    const autoOkLegacy=(isTierA||isTierBPlus) && entryPermissionOk && !r190Edge?.spreadBlock && !(r190Edge?.lateTrapRisk && !r190Edge?.squeeze) && !(r281Map?.hardNo && !r281Map?.runner);
    const r305Safety = entryPermissionOk && !r190Edge?.spreadBlock && !(r190Edge?.lateTrapRisk && !r190Edge?.squeeze) && !(r281Map?.hardNo && !r281Map?.runner) && !hardVeto;
    const r300SummaryTxt = [].concat(reasons||[]).join(' · ') + ' ' + String((typeof r274Signal!=='undefined'&&r274Signal?.summary)||'') + ' ' + String((typeof r110ICT!=='undefined'&&r110ICT?.summary)||'');
    const r300RangePos = (typeof r276Pos!=='undefined' && Number.isFinite(r276Pos)) ? r276Pos
                           : (r281Map && Number.isFinite(r281Map.rangePos)) ? r281Map.rangePos : 0.5;
    const r300Rsi = (typeof rsi5m!=='undefined' && Number.isFinite(rsi5m)) ? rsi5m
                      : (typeof rsi4h!=='undefined' && Number.isFinite(rsi4h)) ? rsi4h : 50;
    const r300Gate = r300SimpleBrain(recommendation, {
      brainSummary: r300SummaryTxt, r125OrderflowSummary: r125Flow?.summary||'',
      score: sc, brainConfidence: priorityScore, r125LiveDeltaPct: r125Flow?.deltaPct,
      r117BodyReclaimOk, r117MssOk, r117TrapSweepTaken,
      r111: (typeof r111Sonuc!=='undefined'?r111Sonuc:null),
      _r276RangePos: r300RangePos, rsi: r300Rsi
    }, { minScore: 70, rangePos: r300RangePos, aiRunner: !!decisionChain?.aiRunner });
    const autoOk = r305Safety && r300Gate.allow;

    if (!r300Gate.allow) {
      const isSafetyBlock = /R300-0 GÜVENLİK/i.test(String(r300Gate.reason||''));
      if (isSafetyBlock) {
        logAuto(`⛔ ${coin.fullSymbol} R300 GÜVENLİK RED (AI'ya sorulmaz): ${r300Gate.reason} — emir AÇILMADI`);
        markAutoSkip(coin.symbol, `R300 güvenlik: ${r300Gate.reason}`, {rec:recommendation, score, brainMode:decisionChain?.brainMode, brainSummary:decisionChain?.brainSummary});
        continue;
      }
      logAuto(`🔍 ${coin.symbol} R300 yumuşak red (${r300Gate.reason}) — AI PRO TRADER'a devrediliyor, son kararı AI verecek`);
      decisionChain.r300SoftReject = r300Gate.reason;
    } else {
      logAuto(`✅ ${coin.symbol} R300 SON KAPI ONAY: ${r300Gate.reason} (RSI5m:${r300Rsi.toFixed(0)} range:${r300RangePos.toFixed(2)})`);
    }

    if (AI_BRAIN_ENABLED && ANTHROPIC_API_KEY) {
      try {
        const aiData = {
          lastPrice: Number(coin.lastPrice || analysis?.price || 0),
          gainerRank: coin.gainerRank || null,
          candles: analysis?.r308RawCandles || null,
          parabolik1m: analysis?.r366Parabolik || null,
          rsi5m: analysis?.timeframes?.['5m']?.rsi, rsi15m: analysis?.timeframes?.['15m']?.rsi,
          rsi1h: analysis?.timeframes?.['1h']?.rsi, rsi4h: analysis?.timeframes?.['4h']?.rsi,
          funding: analysis?.funding?.current,
          shortSqueeze: !!(decisionChain?.r111ShortSqueeze || decisionChain?.r111?.shortSqueeze || decisionChain?.r190Edge?.squeeze),
          longSqueeze: !!(decisionChain?.r111LongSqueeze || decisionChain?.r111?.longSqueeze),
          altSupurmeYapildi: !!(decisionChain?.r278SslSwept && Number(decisionChain?.r278SslSweepQ || 0) >= 3),
          ustSupurmeYapildi: !!(decisionChain?.r278BslSwept && Number(decisionChain?.r278BslSweepQ || 0) >= 3),
          altSupurmeSuruyorReclaimYok: !!(decisionChain?.r278SslSwept && Number(decisionChain?.r278SslSweepQ || 0) < 3),
          ustSupurmeSuruyorRedYok: !!(decisionChain?.r278BslSwept && Number(decisionChain?.r278BslSweepQ || 0) < 3),
          altSupurmeKalite: Number(decisionChain?.r278SslSweepQ || 0),
          ustSupurmeKalite: Number(decisionChain?.r278BslSweepQ || 0),
          oiChange1h: analysis?.openInterest?.change1h, oiChange4h: analysis?.openInterest?.change4h,
          orderBookImbalance: analysis?.orderBook?.imbalance,
          cvdDelta: analysis?.r125OrderFlow?.deltaPct ?? null,
          topTraderLongPct: analysis?.smartMoney?.topLongPct,
          globalLongPct: analysis?.smartMoney?.globalLongPct,
          liqLevels: analysis?.liquidityLevels || null,
          atrPct: analysis?.leverage?.atrPct,
          slTabanOneri: (function(){
            const a = analysis?.leverage?.atrPct;
            if (!a || !Number.isFinite(a)) return null;
            const min = Math.min(3, +(a*1.2).toFixed(2));
            return `bu coinin ATR'si %${a.toFixed(2)} — gürültü-altı SL için ~%${min} ve üzeri öneri (daha dar SL bu volatilitede tek ters mumda süpürülebilir; SL'i sweep/yapı ötesine koy). Kararı sen ver.`;
          })(),
          rvol5m: analysis?.rvol?.['5m']?.rvol ?? null,
          botOkumasi: {
            mumFormasyonu: decisionChain?.mumOzet || decisionChain?.r118CandleOzet || null,
            ictDurum: decisionChain?.ictDashboard || null,
            htfTeshis: decisionChain?.htfTani || null,
            botTezi: (function(){
              try {
                const tr = analysis?.r316Trend;
                const d5 = analysis?.rsiDivergence?.['5m'];
                const mom = String(analysis?.momentumZayiflama||'');
                const bbP = Number(analysis?.bb5m?.pos);
                const dlt = Number(decisionChain?.r125LiveDeltaPct||0);
                const ict = String(decisionChain?.ictDashboard||'');
                const sweep = /ALINDI|reclaim|geri.?kazan/i.test(ict);
                let lehLong=0, lehShort=0; const seb=[];
                if (tr?.slopeUp && !tr?.risingBreak){ lehLong++; seb.push('trend yukarı'); }
                if (tr?.slopeDown && !tr?.fallingBreak){ lehShort++; seb.push('trend aşağı'); }
                if (tr?.fallingBreak){ lehLong++; seb.push('düşen trend yukarı kırıldı'); }
                if (tr?.risingBreak){ lehShort++; seb.push('yükselen trend aşağı kırıldı'); }
                if (Number.isFinite(tr?.kanalKonum)){
                  if (tr.slopeUp && tr.kanalKonum<=30){ lehLong++; seb.push('yükselen kanal dibi'); }
                  if (tr.slopeDown && tr.kanalKonum>=70){ lehShort++; seb.push('düşen kanal tepesi'); }
                }
                if (dlt>=20){ lehLong++; seb.push('delta alıcı +'+dlt.toFixed(0)); }
                if (dlt<=-20){ lehShort++; seb.push('delta satıcı '+dlt.toFixed(0)); }
                if (d5?.bullDiv){ lehLong++; seb.push('5m bull divergence'); }
                if (d5?.bearDiv){ lehShort++; seb.push('5m bear divergence'); }
                if (/YUKARI döndü/i.test(mom)){ lehLong++; seb.push('momentum yukarı döndü'); }
                if (/AŞAĞI döndü/i.test(mom)){ lehShort++; seb.push('momentum aşağı döndü'); }
                if (sweep){ const s=lehLong>=lehShort?'LONG':'SHORT'; if(s==='LONG')lehLong+=2; else lehShort+=2; seb.push('★sweep+reclaim VAR (en güçlü teyit — WR%64)'); }
                else { seb.push('⚠sweep YOK (backtest: sweepsiz girişler WR%37/negatif — net trend+delta+yapı yoksa BEKLE daha güvenli)'); }
                if (Number.isFinite(bbP)){
                  if (bbP<=0){ lehLong++; seb.push('BB alt bant dışı (tepki)'); }
                  if (bbP>=100){ lehShort++; seb.push('BB üst bant dışı (tepki)'); }
                }
                const atrP = Number(analysis?.leverage?.atrPct||0);
                const r140ph = String(decisionChain?.r140Phase?.phase || '').toUpperCase();
                const pumped = atrP >= 3 || /EXPANSION/.test(r140ph);
                const dusenGenisleme = r140ph === 'EXPANSION_DOWN';
                const momZayif = /AŞAĞI döndü|zayıfl|tükendi|peak|tepe/i.test(mom);
                if (dusenGenisleme){ lehShort++; seb.push('düşüş genişlemesi (EXPANSION_DOWN — SHORT momentum)'); }
                if (pumped && Number.isFinite(tr?.kanalKonum)){
                  if (tr.kanalKonum>=75){
                    lehLong = Math.max(0, lehLong-1);
                    seb.push('⚠pump+TEPE: LONG tuzak riski (kovalama)');
                    if (momZayif){ lehShort++; seb.push('pump tepesi+momentum zayıfladı → SHORT fırsatı (geniş SL şart)'); }
                  }
                  if (tr.kanalKonum<=35 && tr.slopeUp){
                    lehLong++; seb.push('pump SONRASI çekilme dibi (G2+ en kârlı LONG)');
                  }
                }
                const net = lehLong - lehShort;
                if (lehLong+lehShort === 0) return 'Bot tezi: net kanıt yok, ben olsam BEKLERDİM (sen de teyit et).';
                if (Math.abs(net) <= 1) return `Bot tezi: kanıtlar KARIŞIK (L:${lehLong}/S:${lehShort} — ${seb.join(', ')}). Ben olsam net taraf görene dek BEKLERDİM. Sen karar ver komutanım.`;
                const yon = net>0 ? 'LONG' : 'SHORT';
                return `Bot tezi: ben olsam ${yon} açardım (${net>0?lehLong:lehShort} kanıt: ${seb.join(', ')}). AMA sen bağımsız bak — bu kanıtlar gerçekten hizalı mı, yoksa tuzak mı? Sen karar ver.`;
              } catch(_){ return null; }
            })(),
            botAnalizOzeti: (decisionChain?.brainSummary || '').replace(/\bR\d+\b\s*/g,'').replace(/\s+·\s+/g,' · ').slice(0, 480) || null,
            yapiOkumasi5m: (function(){
              const e = decisionChain?.r190Edge;
              if (!e || !e.ok) return null;
              const parts = [];
              if (e.r194SwingBreak?.bos) parts.push(`5m yapı kırılımı(BOS) ${e.r194SwingBreak.strong?'GÜÇLÜ':'var'} ${e.side}`);
              if (e.earlyContinuation) parts.push('erken trend-devamı (taze, geç değil)');
              if (e.lateTrapRisk) parts.push('GEÇ giriş tuzak riski (hareket olgun)');
              if (e.tooLate) parts.push('çok geç (kovalamaca)');
              if (typeof e.rangePos === 'number') parts.push(`5m range konumu ${(e.rangePos*100).toFixed(0)}% (${e.rangePos>=0.7?'tepe bölge':e.rangePos<=0.3?'dip bölge':'orta'})`);
              if (e.squeeze) parts.push('5m sıkışma (patlama yakıtı)');
              if (typeof e.r192FuelScore === 'number' && e.r192FuelScore >= 5) parts.push(`devam yakıtı ${e.r192FuelScore.toFixed(1)}`);
              if (typeof e.price3 === 'number') parts.push(`son3mum ${e.price3>=0?'+':''}${e.price3.toFixed(2)}%`);
              return parts.length ? parts.join(' · ') : null;
            })(),
            trendCizgisi: (function(){
              const t = analysis?.r316Trend;
              if (!t || !t.ok) return 'trend çizgisi okunamadı (5m geçmiş yetersiz <12 mum ya da net diagonal yok) — trend yönü TEYİTSİZ, dikkatli';
              return t.note || 'trend çizgisi nötr';
            })(),
            mmDurum: (function(){
              const ph = String(decisionChain?.r140Phase?.phase || '').toUpperCase();
              const fake = decisionChain?.r140OiVel?.fakePump;
              const eqHigh = decisionChain?.r140EqHL?.nearHighTrap;
              const eqLow = decisionChain?.r140EqHL?.nearLowTrap;
              const bits = [];
              if (ph === 'DISTRIBUTION') bits.push('FAZ:DAĞITIM (MM tepede satıyor olabilir — SHORT İHTİMALİ, ama trend yukarıysa erken)');
              else if (ph === 'ACCUMULATION') bits.push('FAZ:TOPLAMA (MM dipte biriktiriyor olabilir — LONG İHTİMALİ)');
              else if (ph === 'EXPANSION') bits.push('FAZ:GENİŞLEME (pump/güçlü yukarı momentum AKTİF — trend yönü güçlü, tepede LONG-kovalama riski; çekilme dibi LONG fırsatı)');
              else if (ph === 'EXPANSION_DOWN') bits.push('FAZ:DÜŞÜŞ-GENİŞLEME (sert aşağı momentum AKTİF — SHORT yönü güçlü, dipte SHORT-kovalama riski; sıçrama tepesi SHORT fırsatı)');
              else if (ph === 'EXPANSION') bits.push('FAZ:GENİŞLEME (trend güçlü, trend yönünde devam)');
              if (fake) bits.push('SAHTE PUMP işareti (fiyat yukarı ama gerçek alım zayıf — LONG tuzağı olabilir)');
              if (eqHigh) bits.push('eşit-tepe tuzağı yakın (üst likidite mıknatısı)');
              if (eqLow) bits.push('eşit-dip tuzağı yakın (alt likidite mıknatısı)');
              if (!bits.length) return null;
              return bits.join(' · ') + ' · NOT: bu MM ölçümüdür, YÖN DEĞİL — trendCizgisi + delta + sweep ile ÇAPRAZ DOĞRULA, tek başına SHORT/LONG sebebi sayma.';
            })(),
            gostergeKonumu: (function(){
              const bb = analysis?.bb5m;
              if (!bb || !bb.durum) return null;
              return `5m Bollinger: ${bb.durum} | bant genişlik %${bb.width} (dar=sıkışma/patlama yakın, geniş=trend aktif). NOT: Bollinger tek başına yön değil — trendCizgisi+sweep ile çapraz oku; üst bant aşımı trend YUKARIYSA devam olabilir, dönüş garantisi değil.`;
            })(),
            divergence: (function(){
              const d5 = analysis?.rsiDivergence?.['5m'];
              const d1 = analysis?.rsiDivergence?.['1h'];
              const bits = [];
              if (d5?.bullDiv) bits.push('5m BULLISH divergence (fiyat↓ ama RSI↑ = düşüş zayıflıyor, LONG dönüş İHTİMALİ)');
              if (d5?.bearDiv) bits.push('5m BEARISH divergence (fiyat↑ ama RSI↓ = yükseliş zayıflıyor, SHORT dönüş İHTİMALİ)');
              if (d5?.hiddenBull) bits.push('5m gizli bull divergence (trend DEVAM sinyali, LONG)');
              if (d5?.hiddenBear) bits.push('5m gizli bear divergence (trend DEVAM sinyali, SHORT)');
              if (d1?.bullDiv) bits.push('1h bullish divergence (HTF dönüş zemini, LONG)');
              if (d1?.bearDiv) bits.push('1h bearish divergence (HTF dönüş zemini, SHORT)');
              if (!bits.length) return null;
              return bits.join(' · ') + ' · NOT: divergence dönüş İPUCUDUR, tetik DEĞİL — sweep+reclaim veya trend kırılımı ile teyit et, tek başına girme.';
            })(),
            momentumDurum: analysis?.momentumZayiflama || null,
            invertedFVG: analysis?.invertedFVG || null,
            buCoinGecmis: r311zCoinGecmisOzeti(coin.fullSymbol || (coin.symbol ? coin.symbol + 'USDT' : ''))
          },
          marketCtx: (function(){
            const s = String(decisionChain?.r140Summary||'');
            const btcBit = /BTC[^·]*/.exec(s);
            const div = decisionChain?.r140BtcDiv;
            let txt = btcBit ? btcBit[0].trim() : '';
            if (div?.label) txt += (txt?' · ':'') + div.label;
            return txt || 'BTC ham mumlari candles.btc5m icinde';
          })()
        };
        r309eAiSentCount++;
        logAuto(`🧠 ${coin.symbol} AI PRO TRADER'a gönderiliyor (${r309eAiSentCount}/${R309E_MAX_AI_PER_SCAN} bu taramada)`);
        const ai = await r308AiProTraderBrain(coin.symbol, aiData);
        if (ai && ai.ok) {
          const rrTxt = (ai.tp && ai.sl && ai.entry) ? ` R:R≈${(Math.abs(ai.tp-ai.entry)/Math.abs(ai.entry-ai.sl)||0).toFixed(2)}` : '';
          logAuto(`🤖 ${coin.symbol} AI PRO TRADER: ${ai.side} güven:${ai.confidence}% · giriş:${ai.entry} TP:${ai.tp} SL:${ai.sl}${rrTxt} — ${ai.reasoning}${ai.plan?' · PLAN: '+ai.plan:''}`);
          decisionChain.aiBrain = ai;
          const _q = r308AiPlanQuality(ai);
          try {
            r308SetLastAiDecision({status: ai.side==='WAIT'?'AI_BEKLE':'AI_CANLI_KARAR', symbol:coin.symbol, ai, quality:_q, candidate:{symbol:coin.symbol, rec:recommendation, score, tier:decisionChain?.tier||'AI'}});
          } catch(_setE) {}
          if (AI_BRAIN_SHADOW) {
            logAuto(`👁️ ${coin.symbol} GÖLGE MOD: AI kararı kaydedildi, işlem AÇILMADI (gerçek fiyatla karşılaştır). Bot ${recommendation} açacaktı.`);
            markAutoSkip(coin.symbol, `AI gölge mod: ${ai.side} güven ${ai.confidence}% (işlem açılmadı)`, {rec:recommendation, score, aiBrain:ai});
            continue;
          } else {
            if (ai.side === 'WAIT') {
              global.__aiWaitStreak = (global.__aiWaitStreak || 0) + 1;
              global.__aiTotalDecisions = (global.__aiTotalDecisions || 0) + 1;
              global.__aiTotalWait = (global.__aiTotalWait || 0) + 1;
              const oran = Math.round(global.__aiTotalWait / global.__aiTotalDecisions * 100);
              let streakNot = '';
              if (global.__aiWaitStreak >= 8) streakNot = ` ⚠️ ARDI ARDINA ${global.__aiWaitStreak} WAIT — AI fazla pasif (oran %${oran})`;
              logAuto(`⛔ ${coin.symbol} AI PRO TRADER WAIT dedi — emir AÇILMADI: ${ai.reasoning}${streakNot}`);
              markAutoSkip(coin.symbol, `AI WAIT: ${ai.reasoning}`, {rec:ai.side, score, aiBrain:ai});
              continue;
            }
            global.__aiWaitStreak = 0;
            global.__aiTotalDecisions = (global.__aiTotalDecisions || 0) + 1;
            if (ai.side !== 'LONG' && ai.side !== 'SHORT') {
              logAuto(`⛔ ${coin.symbol} AI geçersiz yön (${ai.side}) — emir AÇILMADI`);
              markAutoSkip(coin.symbol, `AI geçersiz yön: ${ai.side}`, {rec:ai.side, score, aiBrain:ai});
              continue;
            }
            if (!_q.ok) {
              logAuto(`⛔ ${coin.symbol} AI emir reddi: ${_q.reason} — AÇILMADI`);
              markAutoSkip(coin.symbol, `AI kalite/güven reddi: ${_q.reason}`, {rec:ai.side, score, aiBrain:ai});
              continue;
            }
            try {
              const tr = analysis?.r316Trend;
              if (tr && tr.ok) {
                if (tr.slopeUp && !tr.risingBreak && ai.side === 'SHORT') {
                  logAuto(`⚠️ ${coin.symbol} R317 uyarı: yükselen trend devam ederken SHORT — R343: AI kararı VETO EDİLMEZ — uyarı olarak loglandı, emir devam ediyor`);
                }
                if (tr.slopeDown && !tr.fallingBreak && ai.side === 'LONG') {
                  logAuto(`⚠️ ${coin.symbol} R317 uyarı: düşen trend devam ederken LONG — R343: AI kararı VETO EDİLMEZ — uyarı olarak loglandı, emir devam ediyor`);
                }
              }
            } catch(_r317e) {}
            try {
              const ictTxt2 = String(decisionChain?.ictDashboard||decisionChain?.ictDurum||'');
              const sweepKesin2 = /ALINDI|swept.?[✓Y]|LONG_HAZIR|SHORT_HAZIR|süpürme.*BODY.*teyit|reclaim.*(onay|tutuyor|✓)/i.test(ictTxt2);
              const reclaimTeyit = /reclaim✓|reclaim onay|gövde kapan|body.*kapan|sweep✓|swept✓/i.test(ai.reasoning || '');
              if (sweepKesin2 && ai.side === 'LONG') {
                // Sweep varsa, R318 uyarıları atla, emir aç
              } else if (!sweepKesin2) {
                const rPos = Number.isFinite(decisionChain?._r276RangePos) ? decisionChain._r276RangePos : 0.5;
                const dlt = Number(decisionChain?.r125LiveDeltaPct || 0);
                const obI = Number(analysis?.orderBook?.imbalance ?? decisionChain?.orderBookImbalance ?? NaN);
                if (ai.side === 'SHORT' && rPos <= 0.30) {
                  logAuto(`⚠️ ${coin.symbol} R318 uyarı: dipte sweep'siz SHORT (TNSR/M dersi) — R343: AI kararı VETO EDİLMEZ — uyarı olarak loglandı, emir devam ediyor`);
                }
                if (ai.side === 'LONG' && rPos >= 0.70) {
                  logAuto(`⚠️ ${coin.symbol} R318 uyarı: tepede(%${(rPos*100).toFixed(0)}) sweep'siz LONG (G-42 dersi) — R343: AI kararı VETO EDİLMEZ — uyarı olarak loglandı, emir devam ediyor`);
                }
                const dltTers = (ai.side === 'LONG' && dlt < -3) || (ai.side === 'SHORT' && dlt > 3);
                const obTers  = Number.isFinite(obI) && ((ai.side === 'LONG' && obI < -3) || (ai.side === 'SHORT' && obI > 3));
                const dltSiddetliTers = dltTers && Math.abs(dlt) >= 40;
                const obSiddetliTers  = obTers && Number.isFinite(obI) && Math.abs(obI) >= 40;
                if ((dltTers && obTers) || dltSiddetliTers || obSiddetliTers) {
                  const tip = (dltTers && obTers) ? 'çift-ters' : (dltSiddetliTers ? `delta şiddetli(${dlt.toFixed(0)})` : `emirDef şiddetli(${obI.toFixed(0)})`);
                  logAuto(`⚠️ ${coin.symbol} R318 uyarı: akış ${ai.side} yönüne ters [${tip}] (M LONG dersi) — R343: AI kararı VETO EDİLMEZ — uyarı olarak loglandı, emir devam ediyor`);
                }
                const tr2 = analysis?.r316Trend;
                const kirilimVar = tr2 && tr2.ok && (tr2.risingBreak || tr2.fallingBreak);
                if (kirilimVar) {
                  const deltaDestek = (ai.side === 'LONG' && dlt >= 25) || (ai.side === 'SHORT' && dlt <= -25);
                  const oi1h = Number(analysis?.openInterest?.change1h ?? decisionChain?.oiChange1h ?? NaN);
                  const oiCelisik = Number.isFinite(oi1h) && ((ai.side === 'LONG' && oi1h < -2) || (ai.side === 'SHORT' && oi1h < -2));
                  const deltaTersKirilimda = (ai.side === 'LONG' && dlt < -3) || (ai.side === 'SHORT' && dlt > 3);
                  if (!deltaDestek && (oiCelisik || deltaTersKirilimda)) {
                    logAuto(`⚠️ ${coin.symbol} R318 uyarı: kırılım akışsız (delta ${dlt.toFixed(1)}) (RESOLV/BAS dersi) — R343: AI kararı VETO EDİLMEZ — uyarı olarak loglandı, emir devam ediyor`);
                  }
                }
                const r1h = Number(decisionChain?.rsi1h ?? analysis?.timeframes?.['1h']?.rsi ?? NaN);
                if (Number.isFinite(r1h)) {
                  if (ai.side === 'LONG' && r1h >= 78) {
                    logAuto(`⚠️ ${coin.symbol} R318 uyarı: 1hRSI ${r1h.toFixed(0)} aşırı alımda sweep'siz LONG — R343: AI kararı VETO EDİLMEZ — uyarı olarak loglandı, emir devam ediyor`);
                  }
                  if (ai.side === 'SHORT' && r1h <= 22) {
                    logAuto(`⚠️ ${coin.symbol} R318 uyarı: 1hRSI ${r1h.toFixed(0)} aşırı satımda sweep'siz SHORT — R343: AI kararı VETO EDİLMEZ — uyarı olarak loglandı, emir devam ediyor`);
                  }
                }
                const tr3 = analysis?.r316Trend;
                if (tr3 && tr3.ok && Number.isFinite(tr3.kanalKonum)) {
                  const kp = tr3.kanalKonum;
                  if (tr3.slopeDown && ai.side === 'SHORT' && kp <= 30) {
                    logAuto(`⚠️ ${coin.symbol} R320 uyarı: düşen kanal dibinde SHORT (AIN dersi) — R343: AI kararı VETO EDİLMEZ — uyarı olarak loglandı, emir devam ediyor`);
                  }
                  if (tr3.slopeUp && ai.side === 'LONG' && kp >= 70) {
                    logAuto(`⚠️ ${coin.symbol} R320 uyarı: yükselen kanal tepesinde (%${kp}) sweep'siz LONG — R343: AI kararı VETO EDİLMEZ — uyarı olarak loglandı, emir devam ediyor`);
                  }
                  if (tr3.slopeDown && ai.side === 'LONG' && kp <= 50) {
                    logAuto(`⚠️ ${coin.symbol} R320 uyarı: düşen kanalda (%${kp}) sweep'siz LONG — R343: AI kararı VETO EDİLMEZ — uyarı olarak loglandı, emir devam ediyor`);
                  }
                }
              }
            } catch(_r318e) {}
            try {
              const rsn = String(ai.reasoning || ai.reason || '').toLowerCase();
              const amaVar = /(^|[\s_|↓+\-,])ama([\s_|0-9]|$)/i.test(rsn) || /ancak|rağmen|yine de|karşın|çelişk/i.test(rsn);
              const dlt2 = Math.abs(Number(decisionChain?.r125LiveDeltaPct || 0));
              const ictTxt3 = String(decisionChain?.ictDashboard||decisionChain?.ictDurum||'');
              const sweepKesin3 = /ALINDI|swept.?[✓Y]|LONG_HAZIR|SHORT_HAZIR|süpürme.*BODY.*teyit|reclaim.*(onay|tutuyor|✓)/i.test(ictTxt3);
              const reclaimTeyit3 = /reclaim✓|reclaim onay|gövde kapan|body.*kapan|sweep✓|swept✓/i.test(rsn);
              if (amaVar && dlt2 < 50 && !sweepKesin3 && !reclaimTeyit3) {
                logAuto(`🛑 ${coin.symbol} R319 AMA TESTİ: AI gerekçesinde çelişki ('ama/ancak/rağmen') + sweep yok + akış zayıf (Δ${dlt2.toFixed(0)}) = AI emin değil (dev kayıpların ortak paydası) — AÇILMADI`);
                markAutoSkip(coin.symbol, `R319: AI çelişkili gerekçe (ama testi) engellendi`, {rec:ai.side, score, aiBrain:ai});
                continue;
              }
            } catch(_r319e) {}
            let r308AiFlippedDir = false;
            if (ai.side !== recommendation) {
              logAuto(`🔄 ${coin.symbol} AI bağımsız yön: bot ${recommendation} demişti, AI ${ai.side} okudu — AI'nın kararı uygulanıyor`);
              recommendation = ai.side;
              isLong = ai.side === 'LONG'; isShort = ai.side === 'SHORT';
              score = ai.side === 'LONG' ? longScore : shortScore;
              r308AiFlippedDir = true;
            }
            try {
              const aiEntry = Number(ai.entry), aiTp = Number(ai.tp), aiSl = Number(ai.sl);
              if ([aiEntry,aiTp,aiSl].every(x=>Number.isFinite(x)&&x>0)) {
                const dirOk = isLong ? (aiTp>aiEntry && aiSl<aiEntry) : (aiTp<aiEntry && aiSl>aiEntry);
                if (dirOk) {
                  targetPrice = +aiTp.toFixed(8);
                  stopPrice   = +aiSl.toFixed(8);
                  userTPPct = +(Math.abs(aiTp-aiEntry)/aiEntry*100).toFixed(3);
                  userSLPct = +(Math.abs(aiEntry-aiSl)/aiEntry*100).toFixed(3);
                  userRR = userTPPct / Math.max(0.05, userSLPct);
                  logAuto(`🎯 ${coin.symbol} AI planı uygulandı: giriş${aiEntry} TP${aiTp}(%${userTPPct}) SL${aiSl}(%${userSLPct}) R:R≈${userRR.toFixed(2)}`);
                  try {
                    const aiConf = Number(ai.confidence || 0);
                    const panelMax = Math.max(1, parseInt(cfg.vurKacMaxLev || leverage || 20) || 20);
                    const binancePanelCap = Math.max(1, executeLeverage);
                    let aiTargetLev;
                    if (aiConf >= 78)      aiTargetLev = 15;
                    else if (aiConf >= 70) aiTargetLev = 13;
                    else                   aiTargetLev = 10;
                    let r310BinanceMax = null;
                    try {
                      r310BinanceMax = await getSymbolMaxInitialLeverage(apiKey, apiSecret, coin.fullSymbol, Number(usdtAmount||0) * aiTargetLev).catch(()=>null);
                    } catch(_e) {}
                    const r310Ceil = (r310BinanceMax && r310BinanceMax >= 1) ? Math.min(25, r310BinanceMax) : 25;
                    aiTargetLev = Math.max(10, Math.min(15, Math.min(aiTargetLev, r310Ceil)));
                    if (aiTargetLev >= 1 && aiTargetLev !== executeLeverage) {
                      const oldAiLev = executeLeverage;
                      executeLeverage = aiTargetLev;
                      leverageNote += ` · R325D AI güven ${aiConf} → ${oldAiLev}x→${executeLeverage}x (min10x, TAVAN 25x, Binance izin ${r310Ceil}x)`;
                      logAuto(`🎚️ ${coin.symbol} AI güven ${aiConf}% → kaldıraç ${oldAiLev}x→${executeLeverage}x (R325D: min 10x, TAVAN 25x, Binance limit ${r310Ceil}x)`);
                    }
                  } catch(_aiLevE) { logAuto(`⚠️ ${coin.symbol} AI kaldıraç hatası: ${String(_aiLevE?.message||_aiLevE).slice(0,60)}`); }
                  try {
                    const maxRoiRisk = 25;
                    if (userSLPct * executeLeverage > maxRoiRisk) {
                      const oldLev = executeLeverage;
                      const riskLev = Math.max(6, Math.floor(maxRoiRisk / Math.max(0.1, userSLPct)));
                      executeLeverage = Math.min(executeLeverage, riskLev);
                      if (executeLeverage !== oldLev) {
                        leverageNote += ` · R369-B SL×Lev güvenlik ${oldLev}x→${executeLeverage}x (SL%${userSLPct}×lev≤%25, kayıp sığlaştırma)`;
                        logAuto(`🛡️ ${coin.symbol} AI SL %${userSLPct} geniş → kaldıraç ${oldLev}x→${executeLeverage}x (risk %${(userSLPct*executeLeverage).toFixed(0)}, max %25)`);
                      }
                    }
                  } catch(_levSafeE) {}
                } else if (r308AiFlippedDir) {
                  logAuto(`⛔ ${coin.symbol} R308U İPTAL: AI yön çevirdi (${ai.side}) ama TP/SL yönle tutarsız (TP${aiTp} SL${aiSl} entry${aiEntry}) — eski karşı-yön TP/SL ile açmak ölümcül, emir AÇILMADI`);
                  markAutoSkip(coin.symbol, `R308U: AI yön çevirdi ama plan tutarsız — güvenlik iptali`, {rec:ai.side, score, aiBrain:ai});
                  continue;
                }
              } else if (r308AiFlippedDir) {
                logAuto(`⛔ ${coin.symbol} R308U İPTAL: AI yön çevirdi (${ai.side}) ama geçerli TP/SL vermedi (entry${ai.entry} tp${ai.tp} sl${ai.sl}) — emir AÇILMADI`);
                markAutoSkip(coin.symbol, `R308U: AI yön çevirdi ama TP/SL eksik — güvenlik iptali`, {rec:ai.side, score, aiBrain:ai});
                continue;
              }
            } catch(_aiPlanE) { logAuto(`⚠️ ${coin.symbol} AI TP/SL uygulama hatası: ${String(_aiPlanE?.message||_aiPlanE).slice(0,60)}`); }
            if (ai.side === 'LONG' && !allowLong) {
              logAuto(`⛔ ${coin.symbol} AI LONG dedi ama panel LONG KAPALI — emir AÇILMADI`);
              markAutoSkip(coin.symbol, `Panel LONG kapalı (AI LONG dedi)`, {rec:ai.side, score, aiBrain:ai});
              continue;
            }
            if (ai.side === 'SHORT' && !allowShort) {
              logAuto(`⛔ ${coin.symbol} AI SHORT dedi ama panel SHORT KAPALI — emir AÇILMADI`);
              markAutoSkip(coin.symbol, `Panel SHORT kapalı (AI SHORT dedi)`, {rec:ai.side, score, aiBrain:ai});
              continue;
            }
            logAuto(`✅ ${coin.symbol} AI PRO TRADER ONAY: ${ai.side} güven ${ai.confidence}% — emir açılıyor`);
          }
        } else if (AI_BRAIN_SHADOW) {
          logAuto(`⚪ ${coin.symbol} GÖLGE MOD: AI kararı alınamadı (API/parse), işlem yine de AÇILMADI`);
          continue;
        } else if (AI_BRAIN_STRICT_GATE) {
          const why = ai?.reason || ai?.error || 'AI onayı yok / maliyet freni / parse yok';
          logAuto(`⛔ ${coin.symbol} R308E STRICT: AI net onay vermedi (${why}) — eski motor EMİR AÇAMAZ`);
          markAutoSkip(coin.symbol, `R308E STRICT AI onayı yok: ${why}`, {rec:recommendation, score, aiBrain:ai});
          continue;
        }
      } catch(_aiE) {
        logAuto(`⚠️ ${coin.symbol} AI beyin bağlama hatası: ${String(_aiE?.message||_aiE).slice(0,80)}`);
        if (AI_BRAIN_SHADOW || AI_BRAIN_STRICT_GATE) {
          markAutoSkip(coin.symbol, `R308E STRICT AI hata: ${String(_aiE?.message||_aiE).slice(0,80)}`, {rec:recommendation, score});
          continue;
        }
      }

      if (decisionChain.r300SoftReject) {
        const aiSideUp = String(decisionChain.aiBrain?.side || '').toUpperCase();
        const aiApproved = decisionChain.aiBrain && decisionChain.aiBrain.ok &&
                           (aiSideUp === 'LONG') &&
                           !AI_BRAIN_SHADOW;
        if (!aiApproved) {
          logAuto(`⛔ ${coin.symbol} TEK KAPI: R300 yumuşak red (${decisionChain.r300SoftReject}) + AI net onay yok — emir AÇILMADI`);
          markAutoSkip(coin.symbol, `Tek kapı: R300 yetersiz + AI onayı yok`, {rec:recommendation, score, brainMode:decisionChain?.brainMode, brainSummary:decisionChain?.brainSummary});
          continue;
        }
      }

      {
        const aiSideUp = String(decisionChain.aiBrain?.side || '').toUpperCase();
        const aiApproved = decisionChain.aiBrain && decisionChain.aiBrain.ok &&
                           (aiSideUp === 'LONG') &&
                           !AI_BRAIN_SHADOW;
        if (!aiApproved) {
          const why = !AI_BRAIN_ENABLED ? 'AI kapalı' :
                      (!decisionChain.aiBrain ? 'AI çağrılmadı/limit doldu' :
                       (aiSideUp === 'WAIT' ? 'AI WAIT dedi' : 'AI açık LONG/SHORT onayı yok'));
          logAuto(`⛔ ${coin.symbol} R325 TEK BEYİN: eski motor emir AÇAMAZ — ${why} (sadece AI açar)`);
          markAutoSkip(coin.symbol, `R325 tek beyin: ${why}`, {rec:recommendation, score, aiBrain:decisionChain.aiBrain});
          continue;
        }
        recommendation = aiSideUp;
      }

      logAuto(`🎯 Sinyal: ${coin.symbol} ${trSideLabel(recommendation)} skor:${score} — marj:${usdtAmount} USDT ${leverageNote}  zarar-kes:%${userSLPct} kâr-al:%${userTPPct} oran:${userRR.toFixed(2)}${r125TpNote}${r192ExitPlanNote||''} · R283:${r283Recipe.mode}/${r282TradePlan.mode} — emir açılıyor`);
      const orderResp = await fetch(`http://localhost:${PORT}/api/order`, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({
          apiKey, apiSecret,
          symbol: coin.fullSymbol,
          side: recommendation,
          leverage: executeLeverage, marginType,
          targetPrice, stopPrice,
          usdtAmount, maxPositions
        })
      }).then(r=>r.json());

      if (orderResp.ok) {
        if (orderResp?.details?.leverageAdjusted) {
          logAuto(`⚙️ ${coin.symbol} R137 kaldıraç fallback: panel ${orderResp.details.requestedLeverage}x → ${orderResp.details.leverage}x`);
        }
        markAutoOpened(coin.fullSymbol, recommendation);
        invalidatePositionRiskCache('ORDER_OPENED');
        autoScanState.phase = 'EMİR_AÇILDI';
        logAuto(`✅ ${coin.symbol} ${trSideLabel(recommendation)} açıldı — ${toTurkishText(orderResp.message||'Emir açıldı')}`);
        try {
          const _tgKey = `${coin.fullSymbol}_${recommendation}_${Math.round(Date.now()/60000)}`;
          if (!r185OrderOpenSent.has(_tgKey)) {
            r185OrderOpenSent.add(_tgKey);
            const _msg = [
              '🟢 İŞLEM AÇILDI',
              'Hey Ben Kripto Goxel Bot İşleme Girdi',
              `${coin.fullSymbol} ${recommendation}`,
              `Marjin: ${usdtAmount} USDT | Lev: ${parseInt(executeLeverage)||parseInt(leverage)||1}x`,
              `Entry: ${orderResp.executedPrice||analysis.price}`,
              `SL: ${orderResp.details?.stop||stopPrice} | TP: ${orderResp.details?.target||targetPrice}`,
              `Skor: ${score}/100`,
              `Sebep: ${String(decisionChain?.reason || '').slice(0,500)}`,
              `Build: ${LAZARUS_BUILD}`,
              new Date().toLocaleString('tr-TR')
            ].join('\n');
            r184DirectTelegramText(_msg, false).catch(()=>{});
          }
        } catch(_tgOpenE) {}
        trailingState.set(coin.fullSymbol, {
          side: recommendation,
          entryPrice: orderResp.executedPrice||analysis.price,
          highWater: orderResp.executedPrice||analysis.price,
          breakEvenSet:false, currentSL:orderResp.details?.stop||stopPrice,
          targetTP:orderResp.details?.target||targetPrice,
          leverage:parseInt(executeLeverage)||parseInt(leverage)||1,
          slPct:userSLPct, tpPct:userTPPct,
          r190Edge:{
            earlyContinuation:!!decisionChain?.r190Edge?.earlyContinuation, squeeze:!!decisionChain?.r190Edge?.squeeze,
            r192FuelOk:!!decisionChain?.r190Edge?.r192FuelOk, r192FuelScore:Number(decisionChain?.r190Edge?.r192FuelScore||0),
            r192SqzImminent:!!decisionChain?.r190Edge?.r192SqzImminent, r192ExitPlanNote:r192ExitPlanNote||''
          },
          r281ProMap: decisionChain?.r281ProMap ? {
            protect:!!decisionChain.r281ProMap.protect, runner:!!decisionChain.r281ProMap.runner, hardNo:!!decisionChain.r281ProMap.hardNo,
            favorable:Number(decisionChain.r281ProMap.favorable||0), risk:Number(decisionChain.r281ProMap.risk||0), net:Number(decisionChain.r281ProMap.net||0),
            summary:String(decisionChain.r281ProMap.summary||'').slice(0,300), exitPlanNote:r281ExitPlanNote||''
          } : null,
          r282TradePlan: r282TradePlan || {mode:'NORMAL', maxRoiRisk:15, earlyBEroi:11.0, scratchAfterMin:5},
          r283Recipe: r283Recipe ? {
            tradeOk:!!r283Recipe.tradeOk, mode:String(r283Recipe.mode||'NORMAL'), runner:!!r283Recipe.runner,
            quality:Number(r283Recipe.quality||0), summary:String(r283Recipe.summary||'').slice(0,300),
            real5mTrigger:!!r283Recipe.real5mTrigger, fvgOteOk:!!r283Recipe.fvgOteOk, continuationFuel:!!r283Recipe.continuationFuel,
            weakCandle:!!r283Recipe.weakCandle, flowAgainst:!!r283Recipe.flowAgainst, htfAgainst:!!r283Recipe.htfAgainst
          } : null,
          sltpVerified: !!orderResp.slSuccess && !!orderResp.tpSuccess,
          openedAt: Date.now(),
          openReason: (decisionChain?.aiBrain?.ok && decisionChain.aiBrain.reasoning)
            ? `AI ${decisionChain.aiBrain.side} güven ${decisionChain.aiBrain.confidence}%: ${decisionChain.aiBrain.reasoning}${decisionChain.aiBrain.plan?' | PLAN: '+decisionChain.aiBrain.plan:''}`
            : (decisionChain?.reason || ''),
          brainMode: decisionChain?.brainMode || '',
          entryPermissionReason: decisionChain?.entryPermissionReason || '',
          r126OrderflowSummary: decisionChain?.r125Flow?.summary || '',
          tier: decisionChain?.tier || '',
          score: decisionChain?.score || 0,
          step1Set:false, step2Set:false, step3Set:false,
          peakPnl:0, peakRealPct:0,
          r91Exit:{active:true, mode:'İZLE', exitScore:0, reasons:['pozisyon yeni açıldı'], pnlPct:0, realProfitPct:0, peakPnl:0, pullbackPct:0, givebackRoi:0},
          exitMode:'İZLE', profitLockLevel:0,
          config:{ trailing:true, trailingPct, trailStep, breakEvenPct,
            entryPrice:orderResp.executedPrice||analysis.price, targetTP:orderResp.details?.target||targetPrice },
          entryReason:{
            score, longScore, shortScore,
            reason: decisionChain?.reason || `${recommendation} A-Tier`,
            tags: (analysis.signals||[]).slice(0,6),
            mm: analysis.marketMaker?.target,
            cvd: analysis.cvd?.momentum,
            funding: analysis.funding?.current,
            panel:{ usdtAmount, leverage:executeLeverage, panelLeverage:leverage, slPct:userSLPct, tpPct:userTPPct, leverageNote },
          },
          entryDiscipline:{ canliGirisIziOk:r85CanliGirisIziOk, terazi:r85Terazi, r47:r85R47, timing:r85TimingPts, flow:r85FlowPts, sadeceFundingDestek:r85SadeceFundingDestek, bartiDisiplinOk:r85BartiDisiplinOk },
          managerStatus:{ type:'AÇILDI', reason:`${coin.symbol} ${trSideLabel(recommendation)} açıldı — skor:${score} terazi:${r85Terazi}`, urgency:'LOW', lastCheck:Date.now() },
        });
        try {
          const _stOpen = trailingState.get(coin.fullSymbol) || {};
          try {
            const _ai = decisionChain?.aiBrain || {};
            const _tf = analysis?.timeframes || {};
            _stOpen.aiRunner = (_ai.karKosma === 'RUNNER') || r368BotRunnerUygunMu(analysis, coin).runner;
            _stOpen.aiSnapshot = {
              aiSide: _ai.side || recommendation,
              aiConfidence: Number(_ai.confidence || 0) || null,
              aiReasoning: String(_ai.reasoning || '').slice(0, 600),
              aiPlan: String(_ai.plan || '').slice(0, 400),
              aiEntry: _ai.entry ?? null, aiTp: _ai.tp ?? null, aiSl: _ai.sl ?? null,
              aiKarKosma: _ai.karKosma || 'NORMAL',
              aiFlippedBotDir: (_ai.side && _ai.side !== recommendation) ? `bot ${recommendation}→AI ${_ai.side}` : null,
              rsi: { '5m': _tf['5m']?.rsi ?? null, '15m': _tf['15m']?.rsi ?? null, '1h': _tf['1h']?.rsi ?? null, '4h': _tf['4h']?.rsi ?? null },
              funding: analysis?.funding?.current ?? null,
              delta: analysis?.r125OrderFlow?.deltaPct ?? analysis?.cvd?.momentum ?? null,
              oiChange1h: analysis?.openInterest?.change1h ?? null,
              orderBookImbalance: analysis?.orderBook?.imbalance ?? null,
              atrPct: analysis?.leverage?.atrPct ?? null,
              shortSqueeze: !!(decisionChain?.r111ShortSqueeze || decisionChain?.r111?.shortSqueeze || decisionChain?.r190Edge?.squeeze),
              longSqueeze: !!(decisionChain?.r111LongSqueeze || decisionChain?.r111?.longSqueeze),
              liqLevels: analysis?.liquidityLevels ? {
                ust: (analysis.liquidityLevels['1h']?.buyLiq || analysis.liquidityLevels['4h']?.buyLiq || []).slice(0,2),
                alt: (analysis.liquidityLevels['1h']?.sellLiq || analysis.liquidityLevels['4h']?.sellLiq || []).slice(0,2)
              } : null,
              leverage: Number(executeLeverage) || null,
              leverageNote: String(leverageNote || '').slice(0, 200),
              gainerRank: coin.gainerRank || null,
              marketCtx: String(decisionChain?.r140Summary || '').slice(0, 150),
              openPrice: orderResp.executedPrice || analysis.price || null,
              snapshotAt: Date.now()
            };
            trailingState.set(coin.fullSymbol, _stOpen);
          } catch(_snapE) { logAuto(`⚠️ ${coin.symbol} AI snapshot yazılamadı: ${String(_snapE?.message||_snapE).slice(0,60)}`); }
          recordTradeOpen(coin.fullSymbol, recommendation, orderResp.executedPrice||analysis.price, orderResp.details?.quantity||null, _stOpen);
          const _realQtyForKnown = Math.abs(Number(orderResp.details?.quantity || orderResp.quantity || 0)) || 1;
          rememberOpenPositionForReentry({symbol:coin.fullSymbol, positionAmt: recommendation==='LONG' ? _realQtyForKnown : -_realQtyForKnown, entryPrice:orderResp.executedPrice||analysis.price, leverage:parseInt(executeLeverage)||parseInt(leverage)||1}, _stOpen);
          saveLastKnownPositions();
        } catch(_e) { logAuto(`⚠️ Trade ledger açılış kaydı yazılamadı: ${String(_e.message||_e).slice(0,80)}`); }
        openSideCounts[recommendation] = Number(openSideCounts[recommendation]||0) + 1;
        const freshCntAfterOpen = await getNewPosCount();
        autoScanState.positionCount = freshCntAfterOpen;
        autoScanState.livePositions = freshCntAfterOpen;
        if (!orderResp.slSuccess || !orderResp.tpSuccess) {
          autoScanState.lastAction = `${coin.symbol} ${recommendation} açıldı ama SL/TP tam doğrulanmadı; güvenlik için tarama durdu`;
          return;
        }
        if (freshCntAfterOpen >= maxPositions) {
          autoScanState.phase = 'MAX_POZİSYON_DOLU';
          autoScanState.lastAction = `${coin.symbol} ${recommendation} açıldı; max pozisyon doldu (${freshCntAfterOpen}/${maxPositions})`;
          return;
        }
        autoScanState.lastAction = `${coin.symbol} ${recommendation} açıldı; max pozisyon ${freshCntAfterOpen}/${maxPositions}, tarama devam ediyor`;
        await sleep(650);
        continue;
      } else {
        logAuto(`❌ ${coin.symbol} hata: ${orderResp.error}`);
        setCooldown(coin.fullSymbol, COOLDOWN_ERR_MS, `Emir hatası: ${String(orderResp.error||'').slice(0,50)}`);
        markAutoSkip(coin.symbol, `Emir hata: ${orderResp.error}`, {rec:recommendation, score});
      }
    } catch(e) {
      const msg = e.message?.substring(0,120) || 'bilinmeyen';
      pushCritical('AUTO_COIN_' + (coin.fullSymbol || coin.symbol), e, { symbol: coin.fullSymbol || coin.symbol }, 'CRITICAL');
      logAuto(`${coin.symbol} analiz hata: ${msg}`);
      markAutoSkip(coin.symbol, `Analiz hata: ${msg}`, {rec:'ERR', tier:'ERR', reason:msg});
    }
  }

} catch(e) {
  if (String(e?.code||'') === 'BINANCE_BACKOFF_ACTIVE' || String(e?.message||'').includes('merkezi istek freni')) {
    const rem = Math.max(Math.ceil(getBinanceBackoffMs()/1000), Math.ceil(getPositionRiskCooldownMs()/1000));
    autoScanState.phase = 'BINANCE_İSTEK_FRENİ';
    autoScanState.lastAction = `Binance geçici istek freni — ${rem}sn bekleniyor; yeni emir kapalı`;
    logAuto(`⏳ Binance istek freni — ${rem}sn yeni emir yok: ${String(e.message||'').slice(0,120)}`);
  } else if (isPositionRiskRateLimitError(e) || String(e?.message||'').includes('RATE_LIMIT_COOLDOWN')) {
    autoScanState.phase = 'RATE_LIMIT_COOLDOWN';
    autoScanState.lastAction = `positionRisk limit/cooldown — ${Math.ceil(getPositionRiskCooldownMs()/1000)}sn bekleniyor`;
    logAuto(`⏳ positionRisk limit/cooldown — yeni emir bekletiliyor: ${e.message?.substring(0,120)}`);
  } else {
    autoScanState.phase = 'TARAMA_HATA';
    pushCritical('AUTO_SCAN', e, { phase:autoScanState.phase }, 'CRITICAL');
    logAuto(`Tarama hatası: ${e.message?.substring(0,120)}`);
  }
} finally {
  autoRunning = false;
  autoScanState.running = false;
  autoScanState.currentSymbol = null;
  autoScanState.lastScanEnd = Date.now();
  autoScanState.nextScanDue = autoConfig?.enabled ? Date.now() + AUTO_SCAN_INTERVAL_MS : null;

  await r308RunAiCandidateReviewAfterScan();

  if (autoScanState.phase === 'MAX_POZİSYON_DOLU') {
    try {
      const cnt = await getNewPosCount();
      const maxP = normalizeUserMaxPositions(autoConfig?.maxPositions || autoScanState?.settings?.maxPositions || 1, 1);
      autoScanState.positionCount = cnt;
      if (cnt < maxP) autoScanState.phase = autoScanState.opened>0 ? 'EMİR_AÇILDI' : 'BEKLİYOR';
    } catch(e) {}
  }

  if (!['MAX_POZİSYON_DOLU','EMİR_AÇILDI','TARAMA_HATA','RATE_LIMIT_COOLDOWN'].includes(autoScanState.phase)) {
    autoScanState.phase = autoScanState.opened>0 ? 'EMİR_AÇILDI' : 'BEKLİYOR';
  }
}
}

async function getNewPosCount() {
  try {
    const cfg = autoConfig;
    if (!cfg?.apiKey) return 0;
    const d = await getPositionRiskCached(cfg.apiKey,cfg.apiSecret);
    return Array.isArray(d) ? d.filter(p=>Math.abs(parseFloat(p.positionAmt))>0).length : 0;
  } catch(e) { return 0; }
}

function startAutoTrader() {
  logAuto('Otomatik işlem başlatıldı');
  function scheduleNextScan() {
    const now = Date.now();
    const d = new Date(now);
    const min = d.getUTCMinutes();
    const sec = d.getUTCSeconds();
    const marks = [0, 7.5, 15, 22.5, 30, 37.5, 45, 52.5];
    const nowMin = min + sec/60;
    let nextMark = marks.find(m => m > nowMin + 0.05);
    let waitMin;
    if (nextMark === undefined) { nextMark = 60; waitMin = 60 - nowMin; }
    else waitMin = nextMark - nowMin;
    const isCandleClose = (nextMark % 15 === 0);
    const waitMs = waitMin*60*1000 + (isCandleClose ? 8000 : 0);
    autoScanState.nextScanDue = now + waitMs;
    if (autoTimer) clearTimeout(autoTimer);
    autoTimer = setTimeout(async () => {
      try { await runAutoScan(); } catch(_) {}
      scheduleNextScan();
    }, waitMs);
  }
  scheduleNextScan();
  if (!r125FastWakeTimer) {
    r125FastWakeTimer = setInterval(() => {
      try {
        if (!autoConfig?.enabled || autoRunning || isBinanceBackoffActive() || isPositionRiskCooldownActive()) return;
        try {
          const lastList = (autoScanState?.scanList||[]).map(x=>normalizeSymbol(x)).filter(Boolean);
          const rs130 = r130CombinedTickWS?.readyState;
          const noFirstTick = r130CombinedTickWS && rs130 === WebSocket.OPEN && !r130CombinedTickLastMsgTs && r130CombinedTickLastOpenTs && Date.now()-r130CombinedTickLastOpenTs > 12000;
          const stale = r130CombinedTickWS && rs130 === WebSocket.OPEN && r130CombinedTickLastMsgTs && Date.now()-r130CombinedTickLastMsgTs > 20000;
          if (lastList.length && (!r130CombinedTickWS || rs130 > WebSocket.OPEN || stale || noFirstTick)) r130StartCombinedAggTradeStream(lastList, {replace:true});
        } catch(_) {}
        const now = Date.now();
        for (const [sym, ev] of r125PriorityWake.entries()) {
          if (now - ev.ts > 15000) { r125PriorityWake.delete(sym); continue; }
          const top2Syms = (autoScanState?.scanList||[]).slice(0,2).map(x=>normalizeSymbol(x)).filter(Boolean);
          const isTop2Sym = top2Syms.includes(sym);
          const wakeThreshold = isTop2Sym ? 11 : 14;
          const TOP2_WAKE_COOLDOWN_MS = 90 * 1000;
          const lastWakeOk = !r310yLastTop2Wake || (now - r310yLastTop2Wake > TOP2_WAKE_COOLDOWN_MS);
          const scanGapOk = now - Math.max(Number(autoScanState.lastScanStart||0), Number(autoScanState.lastScanEnd||0), r150LastScanBeginTs||0) > R150_MIN_SCAN_GAP_MS;
          if (Number(ev.score||0) >= wakeThreshold && scanGapOk && (!isTop2Sym || lastWakeOk) && !isBinanceBackoffActive()) {
            if (isTop2Sym) r310yLastTop2Wake = now;
            autoScanState.lastAction = `${isTop2Sym?'⚡TOP2 spike (not edildi, mum-senkron tarama bekleniyor)':'R151 orderflow spike (not)'}: ${sym.replace('USDT','')} ${ev.reason}`;
            r125PriorityWake.delete(sym);
            break;
          }
        }
      } catch(_) {}
    }, 5000);
  }
  if (!fastManagerTimer) {
    fastManagerTimer = setInterval(fastManageOpenPositions, 10 * 1000);
  }
  if (!positionSyncTimer) {
    positionSyncTimer = setInterval(syncPositions, 30 * 1000);
  }
  runAutoScan();
  fastManageOpenPositions();
}

async function getRecentUserTradesSafe(apiKey, apiSecret, symbol, state) {
  try {
    const openedAt = Number(state?.openedAt || state?.entryAt || 0);
    const startTime = openedAt > 0 ? Math.max(0, openedAt - 5*60*1000) : Date.now() - 6*60*60*1000;
    let rows = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/userTrades', {
      symbol,
      startTime,
      limit: 80,
    }, 10000);
    if ((!Array.isArray(rows) || rows.length === 0) && openedAt > 0) {
      rows = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/userTrades', {
        symbol,
        startTime: Math.max(0, Date.now() - 2*60*60*1000),
        limit: 100,
      }, 10000).catch(()=>[]);
    }
    return Array.isArray(rows) ? rows : [];
  } catch(e) {
    return [];
  }
}

function inferStateSide(state) {
  const side = String(state?.side || state?.positionSide || '').toUpperCase();
  if (side === 'LONG' || side === 'SHORT') return side;
  const ep = parseFloat(state?.entryPrice || state?.entry || 0);
  const tp = parseFloat(state?.targetTP || state?.tp || 0);
  if (ep > 0 && tp > 0) return tp > ep ? 'LONG' : 'SHORT';
  return '';
}

function pctDiff(a, b) {
  a = parseFloat(a); b = parseFloat(b);
  if (!Number.isFinite(a) || !Number.isFinite(b) || a <= 0 || b <= 0) return Infinity;
  return Math.abs(a - b) / a * 100;
}

function weightedAvgTradePrice(trades) {
  let notional = 0, qty = 0, pnl = 0;
  for (const t of trades || []) {
    const p = parseFloat(t.price || 0);
    const q = Math.abs(parseFloat(t.qty || t.quantity || 0));
    if (p > 0 && q > 0) { notional += p*q; qty += q; }
    const rp = parseFloat(t.realizedPnl || 0);
    if (Number.isFinite(rp)) pnl += rp;
  }
  return { price: qty > 0 ? notional/qty : 0, qty, pnl };
}

async function classifyClosedPosition(apiKey, apiSecret, symbol, state) {
  const ledgerOpenRow = (tradeLedger || []).find(x => x && x.symbol === String(symbol).replace('USDT','') && x.status === 'OPEN');
  const side = inferStateSide(state) || normalizeSide(ledgerOpenRow?.side);
  const isLong = side === 'LONG';
  const closeTradeSide = isLong ? 'SELL' : side === 'SHORT' ? 'BUY' : '';
  const trades = await getRecentUserTradesSafe(apiKey, apiSecret, symbol, state);
  const closeTrades = closeTradeSide
    ? trades.filter(t => String(t.side || '').toUpperCase() === closeTradeSide)
    : trades;
  const wa = weightedAvgTradePrice(closeTrades.length ? closeTrades : trades);
  const closePrice = wa.price;
  const sl = parseFloat(state?.currentSL || state?.stop || state?.sl || 0);
  const tp = parseFloat(state?.targetTP || state?.target || state?.tp || 0);

  const tol = 1.25;
  let code = 'BINANCE_CLOSED_UNKNOWN';
  let label = 'Binance kapanışı';
  let emoji = '🔍';

  const nearTP = closePrice > 0 && tp > 0 && pctDiff(closePrice, tp) <= tol;
  const nearSL = closePrice > 0 && sl > 0 && (
    pctDiff(closePrice, sl) <= tol ||
    (isLong && closePrice <= sl * (1 + tol/100)) ||
    (!isLong && closePrice >= sl * (1 - tol/100))
  );
  if (nearTP) {
    code = 'TAKE_PROFIT'; label = 'TP ile kapandı'; emoji = '🎯';
  } else if (nearSL) {
    if (state?.step3Set || state?.step2Set || state?.step1Set) {
      code = 'KAR_TASIMA_SL'; label = 'Kâr taşıma SL ile kapandı'; emoji = '📈';
    } else if (state?.breakEvenSet) {
      code = 'BREAK_EVEN_SL'; label = 'BE / güvenli SL ile kapandı'; emoji = '🟦';
    } else {
      code = 'STOP_LOSS'; label = 'SL ile kapandı'; emoji = '🛑';
    }
  } else if (closePrice > 0) {
    code = 'EXTERNAL_OR_MANUAL'; label = 'Binance dış/manuel kapanış olabilir'; emoji = '👁️';
  }
  let realPnlFromIncome = null;
  if ((!wa.pnl || wa.pnl === 0) && apiKey && apiSecret) {
    try {
      const openTs = Number(state?.openTs || state?.openedAt || 0);
      const closeTs = Date.now();
      const incomeStart = openTs > 0 ? openTs - 3000 : closeTs - 90*1000;
      const incomeEnd = closeTs + 3000;
      const incomeData = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/income', {
        symbol, incomeType:'REALIZED_PNL', startTime:incomeStart, endTime:incomeEnd, limit:20
      });
      if (Array.isArray(incomeData) && incomeData.length > 0) {
        if (incomeData.length === 1) {
          realPnlFromIncome = parseFloat(incomeData[0].income||0);
        } else {
          const ep = Number(state?.entryPrice || state?.entry || 0);
          const cp = Number(closePrice || 0);
          const qty = Math.abs(Number(state?.positionAmt || state?.qty || state?.quantity || 0));
          const beklenen = (ep>0 && cp>0 && qty>0) ? (cp-ep)*qty*((normalizeSide(state?.side)==='SHORT')?-1:1) : null;
          if (beklenen !== null) {
            let best=incomeData[0], bestDiff=Math.abs(parseFloat(incomeData[0].income||0)-beklenen);
            for (const x of incomeData) { const d=Math.abs(parseFloat(x.income||0)-beklenen); if(d<bestDiff){bestDiff=d;best=x;} }
            realPnlFromIncome = parseFloat(best.income||0);
          } else {
            realPnlFromIncome = incomeData.reduce((sum, x) => sum + parseFloat(x.income||0), 0);
          }
        }
      }
    } catch(_e) {}
  }
  let approxPnlFromPrice = null;
  try {
    const ep = Number(state?.entryPrice || state?.entry || 0);
    const cp = Number(closePrice || 0);
    const qty = Math.abs(Number(state?.positionAmt || state?.qty || state?.quantity || ledgerOpenRow?.quantity || 0));
    const sdir = normalizeSide(state?.side) || (Number(state?.positionAmt||0) < 0 ? 'SHORT' : 'LONG');
    if (ep > 0 && cp > 0 && qty > 0) approxPnlFromPrice = (cp - ep) * qty * (sdir === 'SHORT' ? -1 : 1);
  } catch(_) {}
  let pnlVal = Number(realPnlFromIncome ?? wa.pnl);
  if ((!Number.isFinite(pnlVal) || Math.abs(pnlVal) < 0.000001) && Number.isFinite(approxPnlFromPrice) && Math.abs(approxPnlFromPrice) > 0.000001) pnlVal = approxPnlFromPrice;
  if (code === 'EXTERNAL_OR_MANUAL' && Number.isFinite(pnlVal)) {
    if (pnlVal > 0) { code = 'BINANCE_PROFIT_CLOSE'; label = 'Binance kapanışı kârda'; emoji = '✅'; }
    if (pnlVal < 0) { code = 'BINANCE_LOSS_CLOSE'; label = 'Binance kapanışı zararda'; emoji = '❌'; }
  }
  const margin = Number(state?.usdtAmount || state?.marginUSDT || autoConfig?.usdtAmount || 0);
  const roiByPnl = margin > 0 && Number.isFinite(pnlVal) ? pnlVal / margin * 100 : null;

  return {
    code, label, emoji,
    closePrice: closePrice ? +closePrice.toFixed(8) : null,
    realizedPnl: Number.isFinite(pnlVal) ? +pnlVal.toFixed(6) : null,
    roiPct: Number.isFinite(roiByPnl) ? +roiByPnl.toFixed(2) : null,
    tradeCount: closeTrades.length || trades.length || 0,
    sl: sl || null,
    tp: tp || null,
  };
}

async function syncPositions() {
  if (!autoConfig?.enabled || !autoConfig?.apiKey || !autoConfig?.apiSecret) return;
  try {
    if (isBinanceBackoffActive()) {
      resetStuckPositionRiskInflight('syncPositions-backoff');
      autoScanState.lastAction = `Binance geçici istek freni ${Math.ceil(getBinanceBackoffMs()/1000)}sn — pozisyon senkronu bekliyor`;
      return;
    }
    const posData = await getPositionRiskCached(autoConfig.apiKey, autoConfig.apiSecret);
    const openMap = new Map();
    if (Array.isArray(posData)) {
      for (const p of posData) {
        if (Math.abs(parseFloat(p.positionAmt)) > 0) openMap.set(p.symbol, p);
      }
    }
    autoScanState.livePositions = openMap.size;
    autoScanState.positionCount = openMap.size;

    const r344StillOpen = async (sym) => {
      try {
        const one = await getPositionRisk(autoConfig.apiKey, autoConfig.apiSecret, { symbol: sym });
        const arr = Array.isArray(one) ? one : [];
        return arr.some(x => Math.abs(parseFloat(x.positionAmt)) > 0);
      } catch(_) { return true; }
    };
    for (const [sym, state] of trailingState.entries()) {
      if (!openMap.has(sym)) {
        if (await r344StillOpen(sym)) {
          logAuto(`🛟 R344: ${sym} toplu pozisyon listesinde görünmedi ama tek-sembol sorguda AÇIK — sahte-flat engellendi, yönetim sürüyor`);
          invalidatePositionRiskCache('R344_FALSE_FLAT');
          continue;
        }
        const cls = await classifyClosedPosition(autoConfig.apiKey, autoConfig.apiSecret, sym, state);
        const px = cls.closePrice ? ` fiyat:${cls.closePrice}` : '';
        const pnl = Number.isFinite(cls.realizedPnl) ? ` pnl:${cls.realizedPnl}` : '';

        const isLossClose = ['STOP_LOSS','R14_HARD_LOSS_GUARD'].includes(cls.code);
        const isManualClose = cls.code === 'EXTERNAL_OR_MANUAL';
        const isProfitClose = ['TAKE_PROFIT','KAR_TASIMA_SL','BREAK_EVEN_SL'].includes(cls.code);
        logAuto(`${cls.emoji} ${sym.replace('USDT','')} kapandı → ${cls.label}${px}${pnl}`);
        trailingState.delete(sym);

        const cdMs = setCloseCooldown(sym, cls, state);
        Object.assign(cls, { cooldownMs: cdMs });
        recordTradeClose(sym, state, cls);
        try { forgetKnownPosition(sym); saveLastKnownPositions(); } catch(_) {}
      }
    }

    const closedHandled = new Set([...trailingState.keys()]);
    for (const sym of Object.keys(lastKnownPositions || {})) {
      if (openMap.has(sym) || closedHandled.has(sym)) continue;
      if (await r344StillOpen(sym)) { logAuto(`🛟 R344: ${sym} (last-known) tek-sembol sorguda AÇIK — sahte kapanış engellendi`); continue; }
      const st = lastKnownPositions[sym] || {};
      const cls = await classifyClosedPosition(autoConfig.apiKey, autoConfig.apiSecret, sym, st).catch(()=>({
        code:'EXTERNAL_OR_MANUAL', label:'Binance/manuel kapanış', emoji:'👁️', closePrice:null, realizedPnl:null
      }));
      const px = cls.closePrice ? ` fiyat:${cls.closePrice}` : '';
      const pnl = Number.isFinite(cls.realizedPnl) ? ` pnl:${cls.realizedPnl}` : '';
      const cdMs = setCloseCooldown(sym, cls, st);
      cls.cooldownMs = cdMs;
      try { recordTradeClose(sym, st, cls); } catch(_) {}
      logAuto(`${cls.emoji||'👁️'} ${sym.replace('USDT','')} last-known kapanış → ${cls.label||cls.code}${px}${pnl}; ${Math.ceil(cdMs/60000)}dk cooldown`);
      forgetKnownPosition(sym);
      invalidatePositionRiskCache('LAST_KNOWN_POSITION_CLOSED');
    }
    saveLastKnownPositions();

    for (const [sym, p] of openMap.entries()) {
      if (!trailingState.has(sym)) {
        const ep  = parseFloat(p.entryPrice || 0);
        const lev = parseInt(p.leverage) || parseInt(autoConfig?.leverage) || 1;
        const side = parseFloat(p.positionAmt) > 0 ? 'LONG' : 'SHORT';
        const lastKnown = lastKnownPositions?.[sym] || {};
        trailingState.set(sym, {
          entryPrice:    lastKnown.entryPrice || ep,
          side:          lastKnown.side || side,
          leverage:      lastKnown.leverage || lev,
          usdtAmount:    lastKnown.usdtAmount || parseFloat(autoConfig?.usdtAmount || 10),
          currentSL:      lastKnown.currentSL || lastKnown.slPrice || 0,
          targetTP:      lastKnown.targetTP || lastKnown.tpPrice || 0,
          slPrice:       lastKnown.currentSL || lastKnown.slPrice || 0,
          tpPrice:       lastKnown.targetTP || lastKnown.tpPrice || 0,
          breakEvenSet:  !!(lastKnown.currentSL || lastKnown.slPrice),
          tpExtended:    false,
          trendHoldCount:0,
          sltpVerified:  !!(lastKnown.sltpVerified || lastKnown.currentSL || lastKnown.targetTP),
          slPct:         Number(lastKnown.slPct || autoConfig?.slPct || 0),
          tpPct:         Number(lastKnown.tpPct || autoConfig?.tpPct || 0),
          aiRunner:      !!(lastKnown.aiRunner),
          brainMode:     lastKnown.brainMode || null,
          openedAt:      lastKnown.openedAt || lastKnown.openTs || Date.now(),
          openTs:        lastKnown.openedAt || lastKnown.openTs || Date.now(),
          openReason:    lastKnown.entryReason || 'restart sonrası restore',
          peakPnl:       Number(lastKnown.peakPnl || 0),
          peakRealPct:   Number(lastKnown.peakRealPct || 0),
          _restoredAfterRestart: true,
        });
        logAuto(`🔄 ${sym.replace('USDT','')} trailingState restart sonrası restore edildi (ep:${ep} ${side} ${lev}x)`);
      }
    }

    for (const [sym, p] of openMap.entries()) {
      try {
        const amt = parseFloat(p.positionAmt || 0);
        const ep  = parseFloat(p.entryPrice || 0);
        const mp  = parseFloat(p.markPrice || 0);
        const isLongGuard = amt > 0;
        const stGuard = trailingState.get(sym) || lastKnownPositions?.[sym] || {};
        const lev = [parseInt(stGuard.executeLeverage), parseInt(stGuard.leverage), parseInt(p.leverage), parseInt(autoConfig.leverage)].find(v => Number.isFinite(v) && v > 1) || 1;
        const slFromPriceGuard = (() => {
          const slp = parseFloat(stGuard.currentSL || stGuard.slPrice || 0);
          return (slp > 0 && ep > 0) ? Math.abs(ep - slp) / ep * 100 : 0;
        })();
        const slFromAiGuard = (() => {
          const s = parseFloat(stGuard.aiBrain?.sl || 0);
          return (s > 0 && ep > 0) ? Math.abs(ep - s) / ep * 100 : 0;
        })();
        const slPctGuard = Math.max(0.1, parseFloat(stGuard.slPct || stGuard.entrySLPct || 0) || slFromPriceGuard || slFromAiGuard || parseFloat(autoConfig.slPct || 2));
        const realMoveGuard = ep > 0 && mp > 0
          ? ((mp - ep) / ep * 100 * (isLongGuard ? 1 : -1))
          : 0;
        const roiGuard = realMoveGuard * lev;
        const hardLossRealGuard = -Math.max(slPctGuard + 0.15, slPctGuard * 1.06);
        const hardLossRoiGuard  = -Math.max((slPctGuard * lev) + 2, 16);
        if (ep > 0 && mp > 0 && (realMoveGuard <= hardLossRealGuard || roiGuard <= hardLossRoiGuard)) {
          try {
            pushCritical('R14_HARD_LOSS_GUARD', `${sym}: SL ötesi zarar yakalandı; market reduceOnly kapatılıyor. move=${realMoveGuard.toFixed(2)}% roi=${roiGuard.toFixed(1)}%`, {symbol:sym, entry:ep, mark:mp, leverage:lev});
            const closeR = await safeMarketClosePosition(autoConfig.apiKey, autoConfig.apiSecret, sym, {reason:'R14_HARD_LOSS_GUARD'});
            if (!closeR.ok) throw new Error(closeR.error || 'safe close başarısız');
            logAuto(`🛑 ${sym.replace('USDT','')} acil hasar koruması kapattı: move ${realMoveGuard.toFixed(2)}% ROI ${roiGuard.toFixed(1)}%${closeR.fallback?' · fallback':''}`);
            trailingState.delete(sym);
            setCooldown(sym, COOLDOWN_CLOSE_MS, 'R14_HARD_LOSS_GUARD');
            continue;
          } catch(closeErr) {
            pushCritical('R14_HARD_LOSS_CLOSE_FAIL', `${sym}: hard-loss close başarısız — ${closeErr.message}`, {symbol:sym});
          }
        }

        try { rememberOpenPositionForReentry(p, trailingState.get(sym)||{}); saveLastKnownPositions(); } catch(_) {}
        const freshPosCheck = await freshOpenPositionForSymbol(autoConfig.apiKey, autoConfig.apiSecret, sym, 2);
        if (freshPosCheck.open === false) {
          await cleanupClosedPositionState(sym, 'SYNC_POSITION_ALREADY_CLOSED_BEFORE_SLTP_RESCUE', trailingState.get(sym)||lastKnownPositions?.[sym]||{});
          continue;
        }

        const stForBracket = trailingState.get(sym) || {};
        let hasSL = !!(stForBracket.sltpVerified && stForBracket.currentSL);
        let hasTP = !!(stForBracket.sltpVerified && stForBracket.targetTP);
        if ((!hasSL || !hasTP) && !isBinanceBackoffActive()) {
          const orders = await liveOpenBracketOrders(autoConfig.apiKey, autoConfig.apiSecret, sym, {ttlMs:60_000});
          const liveSLOrder = orders.find(o => orderKind(o) === 'SL');
          const liveTPOrder = orders.find(o => orderKind(o) === 'TP');
          const liveSLPrice = orderTriggerPrice(liveSLOrder);
          const liveTPPrice = orderTriggerPrice(liveTPOrder);
          hasSL = !!liveSLOrder;
          hasTP = !!liveTPOrder;
          if ((liveSLPrice || liveTPPrice) && trailingState.has(sym)) {
            const stHydrate = trailingState.get(sym) || {};
            if (liveSLPrice) stHydrate.currentSL = liveSLPrice;
            if (liveTPPrice) stHydrate.targetTP = liveTPPrice;
            stHydrate.sltpVerified = !!(hasSL && hasTP);
            trailingState.set(sym, stHydrate);
          }
        }
        if (!hasSL || !hasTP) {
          const isLong  = parseFloat(p.positionAmt) > 0;
          const ep      = parseFloat(p.entryPrice) || 0;
          if (!ep) continue;
          const slPct   = Math.max(0.1, parseFloat(autoConfig.slPct || 2));
          const tpPct   = Math.max(0.1, parseFloat(autoConfig.tpPct || 10));
          const slPrice = isLong ? +(ep*(1-slPct/100)).toFixed(8) : +(ep*(1+slPct/100)).toFixed(8);
          const tpPrice = isLong ? +(ep*(1+tpPct/100)).toFixed(8) : +(ep*(1-tpPct/100)).toFixed(8);
          logAuto(`⚠️ ${sym.replace('USDT','')} SL/TP eksik (SL:${hasSL} TP:${hasTP}) → SL=${slPrice} TP=${tpPrice} kuruluyor`);
          const cSide = isLong ? 'SELL' : 'BUY';
          const result = await installSLTPWithProof(autoConfig.apiKey, autoConfig.apiSecret, sym, cSide, slPrice, tpPrice, sym);
          if (result.ok) {
            logAuto(`✅ ${sym.replace('USDT','')} SL/TP kurtarıldı ✅`);
            if (!trailingState.has(sym)) {
              trailingState.set(sym, {
                side:isLong?'LONG':'SHORT',
                entryPrice:ep, highWater:parseFloat(p.markPrice)||ep,
                breakEvenSet:false, currentSL:result.slPrice || slPrice, targetTP:result.tpPrice || tpPrice,
                sltpVerified:true, leverage:parseInt(p.leverage)||1
              });
            } else {
              const st = trailingState.get(sym);
              st.sltpVerified=true; st.currentSL=result.slPrice || slPrice; st.targetTP=result.tpPrice || tpPrice;
              trailingState.set(sym, st);
            }
            try { rememberOpenPositionForReentry({symbol:sym, positionAmt:parseFloat(p.positionAmt||0), entryPrice:ep, leverage:parseInt(p.leverage)||1}, trailingState.get(sym)||{}); saveLastKnownPositions(); } catch(_) {}
          } else if (result.skippedClosed) {
            logAuto(`⚠️ ${sym.replace('USDT','')} SL/TP rescue atlandı: pozisyon artık açık değil`);
          } else {
            pushCritical('SLTP_RESCUE_FAIL', `${sym}: SL/TP kurtarma başarısız — ${result.error}`, {symbol:sym});
            logAuto(`❌ ${sym.replace('USDT','')} SL/TP kurtarma başarısız: ${result.error||'bilinmeyen'}`);
          }
        }
      } catch(posErr) {
        console.log(`${sym} SLTP kontrol hata: ${posErr.message}`);
      }
    }
  } catch(e) {
    console.log('syncPositions genel hata:', e.message);
  }
}
let positionSyncTimer = null;

app.get('/api/trade-ledger-export', (_req, res) => {
  try {
    res.setHeader('Content-Disposition', 'attachment; filename="trade_ledger_backup.json"');
    res.setHeader('Content-Type', 'application/json');
    res.send(JSON.stringify(tradeLedger.slice(0,500), null, 2));
  } catch(e) { res.status(500).json({ok:false, error:e.message}); }
});

app.post('/api/trade-ledger-import', express.json({limit:'2mb'}), (req, res) => {
  try {
    const data = req.body;
    if (!Array.isArray(data)) return res.status(400).json({ok:false, error:'Array bekleniyor'});
    const newEntries = data.filter(r => r.symbol && r.openedAt && !tradeLedger.find(x=>x.id===r.id));
    tradeLedger = [...newEntries, ...tradeLedger].slice(0,500);
    try { fs.writeFileSync(tradeLedgerPath, JSON.stringify(tradeLedger.slice(0,250), null, 2)); } catch(_){}
    logAuto(`📥 tradeLedger import: ${newEntries.length} yeni kayıt eklendi`);
    res.json({ok:true, imported:newEntries.length, total:tradeLedger.length});
  } catch(e) { res.status(500).json({ok:false, error:e.message}); }
});

app.get('/api/trade-ledger', (_req, res) => {
  res.json({ ok:true, time:trTime(), timeZone:'Europe/Istanbul', trades:tradeLedger.slice(0,120), count:tradeLedger.length });
});
app.get('/api/trade-history', (_req, res) => {
  res.json({ ok:true, trades:tradeLedger.slice(0,120), count:tradeLedger.length });
});

const TG_BACKUP_MSG_PREFIX = '🗄️LAZARUS_LEDGER_BACKUP:';
const TG_BACKUP_INTERVAL_MS = 30 * 60 * 1000;

async function tgSaveLedgerBackup() {
  if (!TG_TOKEN || !TG_CHAT_ID) return;
  try {
    const data = JSON.stringify(tradeLedger.slice(0, 250));
    const payload = TG_BACKUP_MSG_PREFIX + data;
    if (payload.length <= 4000) {
      await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: TG_CHAT_ID,
          text: payload,
          disable_notification: true,
        }),
        signal: AbortSignal.timeout(8000),
      });
    } else {
      const FormData = (await import('form-data')).default;
      const form = new FormData();
      form.append('chat_id', TG_CHAT_ID);
      form.append('caption', TG_BACKUP_MSG_PREFIX + 'FILE');
      form.append('document', Buffer.from(data), { filename: 'ledger.json', contentType: 'application/json' });
      await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendDocument`, {
        method: 'POST',
        body: form,
        signal: AbortSignal.timeout(15000),
      });
    }
  } catch(_e) {}
}

async function tgRestoreLedgerBackup() {
  if (!TG_TOKEN || !TG_CHAT_ID) return false;
  try {
    const res = await fetch(
      `https://api.telegram.org/bot${TG_TOKEN}/getUpdates?limit=100&offset=-100`,
      { signal: AbortSignal.timeout(10000) }
    );
    const data = await res.json();
    if (!data.ok || !Array.isArray(data.result)) return false;
    const msgs = data.result
      .filter(u => u.message?.text?.startsWith(TG_BACKUP_MSG_PREFIX))
      .sort((a, b) => b.message.date - a.message.date);
    if (msgs.length === 0) return false;
    const json = msgs[0].message.text.slice(TG_BACKUP_MSG_PREFIX.length);
    const restored = JSON.parse(json);
    if (!Array.isArray(restored) || restored.length === 0) return false;
    tradeLedger = restored;
    try { fs.writeFileSync(tradeLedgerPath, JSON.stringify(tradeLedger.slice(0,250), null, 2)); } catch(_) {}
    return restored.length;
  } catch(_e) { return false; }
}

app.get('/api/performance-target', (_req, res) => {
  try {
    const obj = r170PerfMode();
    res.json({
      ok:true,
      build:LAZARUS_BUILD,
      target:'account_global_daily_weekly_monthly_wr_pnl_pf',
      targetBand:{min:60,max:85,unit:'percent'},
      note:'Bu endpoint coin winrate değil; hesap geneli günlük/haftalık/aylık performans ölçer.',
      updatedAt:Date.now(),
      mode:obj.mode,
      reason:obj.reason,
      frequencyLastHour:r170TradeFreq(60*60*1000),
      daily:obj.account?.day,
      weekly:obj.account?.week,
      monthly:obj.account?.month,
      recent12:obj.account?.recent,
      reconcile:{lastAt:r173LastAutoReconcileAt||0,last:r173LastAutoReconcileResult},
    });
  } catch(e) {
    res.status(500).json({ok:false,error:String(e?.message||e),build:LAZARUS_BUILD});
  }
});

app.get('/api/telegram-status', (_req, res) => {
  res.json({
    ok: true,
    enabled: tgEnabled(),
    hasToken: !!TG_TOKEN,
    hasChatId: !!TG_CHAT_ID,
    chatIdTail: TG_CHAT_ID ? String(TG_CHAT_ID).slice(-4) : null,
    build: LAZARUS_BUILD,
  });
});

app.post('/api/telegram-test', async (_req, res) => {
  const r = await tgSendNow([
    `🧪 <b>Telegram test başarılı</b>`,
    `🔧 Build: ${tgEsc(LAZARUS_BUILD)}`,
    `⏰ ${new Date().toLocaleString('tr-TR', { timeZone:'Europe/Istanbul' })}`,
    `Bu mesaj geldiyse işlem açılış/kapanış bildirim yolu aktif.`
  ].join('\n'), false);
  res.json({ ok: !!r?.ok, telegram: r, enabled: tgEnabled(), build: LAZARUS_BUILD });
});

app.get('/api/telegram-test', async (_req, res) => {
  const r = await tgSendNow([
    `🧪 <b>Telegram test başarılı</b>`,
    `🔧 Build: ${tgEsc(LAZARUS_BUILD)}`,
    `⏰ ${new Date().toLocaleString('tr-TR', { timeZone:'Europe/Istanbul' })}`,
    `GET test endpoint çalıştı.`
  ].join('\n'), false);
  res.json({ ok: !!r?.ok, telegram: r, enabled: tgEnabled(), build: LAZARUS_BUILD });
});

app.get('/api/telegram-trade-test', async (_req, res) => {
  try {
    const openMsg = [
      '🟢 İŞLEM AÇILDI',
      'Hey Ben Kripto Goxel Bot İşleme Girdi',
      'TESTUSDT LONG',
      'Marjin: 30 USDT | Lev: 15x',
      'Entry: 0.12345',
      'SL: 0.12160 | TP: 0.12980',
      'Build: ' + LAZARUS_BUILD,
      new Date().toLocaleString('tr-TR')
    ].join('\n');
    const closeMsg = [
      '✅ İŞLEM KAPANDI',
      'Kripto Goxel Tekkeyi Bekleyen Çorbayı İçer',
      'TESTUSDT LONG',
      'PnL: +2.34 USDT | ROI: +7.8%',
      'Entry: 0.12345 → Çıkış: 0.12600',
      'Build: ' + LAZARUS_BUILD,
      new Date().toLocaleString('tr-TR')
    ].join('\n');
    const openR = await r184DirectTelegramText(openMsg, false);
    const closeR = await r184DirectTelegramText(closeMsg, false);
    const err = (!openR?.ok ? (openR?.error || openR?.reason || 'open_failed') : null) || (!closeR?.ok ? (closeR?.error || closeR?.reason || 'close_failed') : null);
    res.json({ok:!!(openR?.ok && closeR?.ok), build:LAZARUS_BUILD, open:openR, close:closeR, error:err, mode:'r186_async_and_raw_direct'});
  } catch(e) { res.status(500).json({ok:false, build:LAZARUS_BUILD, error:String(e?.message||e)}); }
});
app.get('/api/telegram-card-test', async (req, res) => {
  try {
    const openMsg = [
      '🟢 İŞLEM AÇILDI',
      'Hey Ben Kripto Goxel Bot İşleme Girdi',
      'TESTUSDT LONG',
      'Marjin: 30 USDT | Lev: 15x',
      'Entry: 0.12345',
      'SL: 0.12160 | TP: 0.12980',
      'Build: ' + LAZARUS_BUILD,
      new Date().toLocaleString('tr-TR')
    ].join('\n');
    const closeMsg = [
      '✅ İŞLEM KAPANDI',
      'Kripto Goxel Tekkeyi Bekleyen Çorbayı İçer',
      'TESTUSDT LONG',
      'PnL: +2.34 USDT | ROI: +7.8%',
      'Entry: 0.12345 → Çıkış: 0.12600',
      'Build: ' + LAZARUS_BUILD,
      new Date().toLocaleString('tr-TR')
    ].join('\n');
    const openR = await r184DirectTelegramText(openMsg, false);
    const closeR = await r184DirectTelegramText(closeMsg, false);
    const err = (!openR?.ok ? (openR?.error || openR?.reason || 'open_failed') : null) || (!closeR?.ok ? (closeR?.error || closeR?.reason || 'close_failed') : null);
    res.json({ok:!!(openR?.ok && closeR?.ok), build:LAZARUS_BUILD, open:openR, close:closeR, error:err, mode:'r186_async_and_raw_direct'});
  } catch(e) { res.status(500).json({ok:false, build:LAZARUS_BUILD, error:String(e?.message||e)}); }
});
app.get('/api/telegram-debug-status', (_req, res) => {
  res.json({
    ok:true, build:LAZARUS_BUILD,
    enabled:tgEnabled(),
    tokenSet:!!TG_TOKEN,
    chatIdSet:!!TG_CHAT_ID,
    chatId:String(TG_CHAT_ID||'').replace(/.(?=.{3})/g,'*'),
    tokenHead:TG_TOKEN ? TG_TOKEN.slice(0,8)+'...' : '',
  });
});

app.get('/api/bootstrap-ledger-48h', async (_req, res) => {
  try {
    const r = autoConfig?.apiKey && autoConfig?.apiSecret ? await r179BootstrapLedger48h(autoConfig.apiKey, autoConfig.apiSecret, 48*60*60*1000, {replace:false}) : {ok:false, reason:'api_missing'};
    res.json({ok:!!r?.ok, build:LAZARUS_BUILD, bootstrap:r});
  } catch(e) { res.status(500).json({ok:false, build:LAZARUS_BUILD, error:String(e?.message||e)}); }
});
app.get('/api/rebuild-ledger-48h', async (_req, res) => {
  try {
    const r = autoConfig?.apiKey && autoConfig?.apiSecret ? await r179BootstrapLedger48h(autoConfig.apiKey, autoConfig.apiSecret, 48*60*60*1000, {replace:true}) : {ok:false, reason:'api_missing'};
    res.json({ok:!!r?.ok, build:LAZARUS_BUILD, bootstrap:r});
  } catch(e) { res.status(500).json({ok:false, build:LAZARUS_BUILD, error:String(e?.message||e)}); }
});

app.get('/api/telegram-flush-trade-alerts', async (_req, res) => {
  try {
    r176DedupeLedger(250);
    const sent = await r171TelegramPollLedger(true);
    res.json({ok:true, build:LAZARUS_BUILD, telegram:sent, reconcile:{skipped:true, reason:'TG_TEST_NO_BINANCE_CALL'}});
  } catch(e) { res.status(500).json({ok:false,error:String(e?.message||e),build:LAZARUS_BUILD}); }
});
app.get('/api/dedupe-trade-ledger', async (_req, res) => {
  try {
    const before = Array.isArray(tradeLedger) ? tradeLedger.length : 0;
    const after = r176DedupeLedger(250);
    saveTradeLedger();
    res.json({ok:true, build:LAZARUS_BUILD, before, after, removed:before-after});
  } catch(e) { res.status(500).json({ok:false,error:String(e?.message||e),build:LAZARUS_BUILD}); }
});

app.get('/api/reconcile-trade-ledger', async (_req, res) => {
  try {
    const rec = autoConfig?.apiKey && autoConfig?.apiSecret ? await r173AutoReconcileTick(true) : {ok:false, reason:'api_missing'};
    res.json({ok:true, build:LAZARUS_BUILD, reconcile:rec});
  } catch(e) { res.status(500).json({ok:false,error:String(e?.message||e),build:LAZARUS_BUILD}); }
});

async function r177FetchBinanceHistory(apiKey, apiSecret) {
  const r = await r179BootstrapLedger48h(apiKey, apiSecret, 48*60*60*1000, {replace:false});
  return Number(r?.restored || 0);
}

app.listen(PORT, async () => {
  console.log(`✅ Server ${PORT}`);

  await new Promise(r => setTimeout(r, 3000));

  try {
    const cfg0 = autoConfig || {};
    if (cfg0.apiKey && cfg0.apiSecret) {
      const added = await r177FetchBinanceHistory(cfg0.apiKey, cfg0.apiSecret);
      if (added > 0) logAuto('📊 R177 Binance geçmişten ' + added + ' işlem yüklendi — WR kalibrasyonu hazır');
    }
  } catch(_e0) {}

  let restoredCount = 0;
  if (tradeLedger.length === 0 && TG_TOKEN && TG_CHAT_ID) {
    console.log('📥 Ledger boş — Telegram yedekten geri yükleniyor...');
    restoredCount = await tgRestoreLedgerBackup();
    if (restoredCount) {
      console.log(`✅ ${restoredCount} işlem Telegram'dan geri yüklendi`);
    }
  }

  if (TG_TOKEN && TG_CHAT_ID) {
    const uptimeStr = new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' });
    const ledgerInfo = tradeLedger.length > 0
      ? `📋 Trade geçmişi: ${tradeLedger.length} işlem${restoredCount ? ` (${restoredCount} Telegram'dan)` : ' (diskten)'}`
      : '📋 Trade geçmişi: Yeni başlangıç';

    const wins = tradeLedger.filter(t => Number(t.pnlUSDT) > 0).length;
    const losses = tradeLedger.filter(t => Number(t.pnlUSDT) < 0).length;
    const total = wins + losses;
    const wrStr = total >= 3 ? ` | WR: %${Math.round(wins/total*100)}` : '';

    await tgSend([
      `🚀 <b>Lazarus Bot BAŞLADI</b>`,
      `⏰ ${uptimeStr}`,
      `🔧 Build: ${LAZARUS_BUILD}`,
      ledgerInfo + (total >= 3 ? wrStr : ''),
      ``,
      `✅ Otomatik tarama aktif`,
      `💬 İşlem açılınca / kapanınca bildirim gelecek`,
      ``,
      ...tgPerfSummaryLines(),
    ].join('\n'), false);
  } else if (!TG_TOKEN) {
    console.log('ℹ️  Telegram kapalı — TELEGRAM_BOT_TOKEN env var eklenmemiş');
  }

  if (TG_TOKEN && TG_CHAT_ID) {
    setInterval(tgSaveLedgerBackup, TG_BACKUP_INTERVAL_MS);
    setTimeout(tgSaveLedgerBackup, 5 * 60 * 1000);
  }

  setTimeout(r171MaintenanceTick, 12 * 1000);
  setInterval(r171MaintenanceTick, 20 * 1000);
});
