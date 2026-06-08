const express = require('express');
const cors    = require('cors');
const crypto  = require('crypto');
const fetch   = require('node-fetch');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
const PORT = process.env.PORT || 3000;
const FAPI = 'https://fapi.binance.com';
const FAPI_WS = 'wss://fstream.binance.com/stream';
// R132: Binance USDⓈ-M Futures WS upgrade sonrası routed endpoints zorunlu.
// Market streams (@aggTrade, @forceOrder, markPrice/kline) /market; depth/bookTicker public hızlı veri /public.
const FAPI_WS_PUBLIC = 'wss://fstream.binance.com/public';
const FAPI_WS_MARKET = 'wss://fstream.binance.com/market';

// R24: Auto monitor gerçek açılış sayaçları. Restart olsa bile mümkün olduğunca korunur.
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


// ── KRİTİK HATA GÖSTERİM MERKEZİ ─────────────────────────────────────────────
// Amaç: analiz motoru kırıldığında sessizce WAIT/boş ekran üretmemek.
// Dashboard /api/diagnostics/status üzerinden bunları gösterir.
const criticalEvents = [];
function safeErrMsg(err) {
  const raw = (err && (err.message || String(err))) || 'Bilinmeyen hata';
  // API secret, signature, uzun query parçaları ekrana/loga taşınmasın.
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

// ── CACHE ─────────────────────────────────────────────────────────────────────
const cache = new Map();
async function cached(key, ttl, fn) {
  const now = Date.now();
  if (cache.has(key)) { const {val,exp}=cache.get(key); if(now<exp)return val; }
  const val = await fn();
  cache.set(key, { val, exp: now+ttl });
  return val;
}

// ── R30 SAFE-MM PATCH — canlı risk ve karar güvenlik versiyonu ────────────────
const LAZARUS_BUILD = 'R176_TG_DEDUP_RATE_LIMIT_FIX';
// R151: R150 üzerine kurulu. İşlem açma potansiyelini ARTIRIRKEN kalite koruma:
// 1) Priority wake eşiği 18 → 14: daha erken uyansın, daha fazla tarama fırsatı
// 2) Sıfır/az geçmiş (< 3 trade) coin için kaldıraç koruması: işlem açılır ama safer
// 3) R150 cache/scan-gap/mikro-cap ATR korumaları aynen korunur
// Ana motto: küçük bakiyede test, büyük bakiyede %5 ROI hedefi × işlem sıklığı = toplam kar
const R150_MIN_SCAN_GAP_MS = 8 * 1000;
let r150LastScanBeginTs = 0;

// ── KONSERVATİF BINANCE REQUEST GOVERNOR ─────────────────────────────────────
// Amaç: tarama/pozisyon/SLTP çağrılarını tek sıraya alıp 429/418/-1003 riskini azaltmak.
// Kesin Binance limitine yaslanmak yerine güvenli alt eşik kullanılır; WS verisi analizde önceliklidir.
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
    // Konservatif eşikler: gerçek limitlere yaklaşmadan sıraya al.
    if (binanceGov.usedWeight + weight > 850 || binanceGov.usedOrders + orderWeight > 70) {
      const wait = 60_000 - (Date.now() - binanceGov.minuteStart) + 250;
      await sleep(wait);
      _resetGovWindowIfNeeded();
    }
    binanceGov.usedWeight += weight;
    binanceGov.usedOrders += orderWeight;
    // Çok sık istek atma: public daha seyrek, emir/pozisyon daha kontrollü.
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
// R95: Binance 418/429 geldiğinde istek bekleyip taramayı kilitleme; merkezi frenle güvenli dur.
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
  // R154: 418 global backoff 180sn→60sn. positionRisk kendi cooldown'unda ayrıca 90sn bekler.
  // 180sn tüm sistemi durduruyordu; 60sn yeterli — positionRisk zaten kendi TTL/cooldown ile korunur.
  const retry = parseInt(retryHeader || (Number(status) === 418 ? '60' : '60'), 10);
  const sec = Math.max(Number(status) === 418 ? 60 : 30, Math.min(120, Number(retry)||60));
  registerBinanceBackoff(`HTTP ${status} ${scope}`, sec);
  throw makeBinanceBackoffError(`HTTP ${status} ${scope}`, sec, status);
}

// ── RATE LIMIT ────────────────────────────────────────────────────────────────
let reqCount=0, reqWindow=Date.now();
// ── ALGO ORDER (yeni coinler için: OPG, PENDLE, HUSDT vb.) ──────────────────
// Signature query string'de, body JSON — Binance algo endpoint zorunluluğu
async function bAlgo(apiKey, apiSecret, params, _retry=false) {
  // Binance SIGNED endpoint kuralı: imza, gönderilen TÜM parametrelerin
  // query string hali üzerinden üretilmelidir. Önceki sürüm sadece
  // timestamp/recvWindow imzalayıp algo parametrelerini JSON body'de yolluyordu;
  // bu da /fapi/v1/algoOrder üzerinde -1022 INVALID_SIGNATURE üretebiliyordu.
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

// ── ALGO CANCEL — tüm algo emirlerini iptal et (2025-12-09 sonrası zorunlu) ──
async function cancelAlgoOrders(apiKey, apiSecret, symbol, emergency=false) {
  // 1. Normal emirleri iptal et (MARKET vs.)
  const em = emergency ? {__emergency:true} : {};
  try { await bReq(apiKey, apiSecret, 'DELETE', '/fapi/v1/allOpenOrders', {symbol, ...em}); } catch(e) {}
  // 2. Algo emirlerini de iptal et (STOP_MARKET, TP artık algo)
  try { await bReq(apiKey, apiSecret, 'DELETE', '/fapi/v1/algoOpenOrders', {symbol, ...em}); } catch(e) {}
}

// ── ALGO SL/TP — Lazarus V8.10.72 kanıtlanmış format ───────────────────────
// DOĞRU: algoType=CONDITIONAL + type=STOP_MARKET/TAKE_PROFIT_MARKET
//        + triggerPrice (stopPrice DEĞİL) + closePosition=true
//        quantity ve reduceOnly GÖNDERİLMEZ — Lazarus V8.10.72 notunda açıkça belirtilmiş
// YANLIŞ olan: orderType=STOP, stopPrice, quantity, reduceOnly (bizim eski kod)
async function buildAlgoCloseParams(symbol, closeSide, orderType, triggerPrice, clientAlgoId) {
  return {
    algoType: 'CONDITIONAL',
    symbol,
    side: closeSide,
    type: orderType,           // "STOP_MARKET" veya "TAKE_PROFIT_MARKET"
    triggerPrice: triggerPrice.toString(),
    closePosition: 'true',     // quantity/reduceOnly YOK — tüm pozisyonu kapatır
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

// ── SLTP PROOF — Lazarus verify_live_sltp_visible mantığı ────────────────────
// SL/TP yazıldıktan sonra Binance'te gerçekten görünüyor mu diye kontrol et
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
    // R145: Binance 418/429 backoff aktifken açık emir sorgusu yeni 418 zinciri başlatmasın.
    // Cache yoksa boş döner; state.currentSL/targetTP zaten panel/manager için kaynak olur.
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
  return Math.abs(expected - actual) / expected < 0.005; // %0.5 tolerans, tick farklarını absorbe eder
}

// ── SLTP PROOF — Lazarus verify_live_sltp_visible mantığı ────────────────────
// SL/TP yazıldıktan sonra Binance'te gerçekten görünüyor mu diye kontrol et.
// Kritik düzeltme: sadece openAlgoOrders değil, standart openOrders da kontrol edilir.
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



// ── PRICE TICK NORMALIZER — SL/TP precision (-1111) koruması ────────────────
// Binance bazı coinlerde BEAT gibi 4 decimal, bazı coinlerde 5/6 decimal kabul eder.
// SL/TP kurtarma veya trailing tarafında .toFixed(8) ile ham fiyat gönderilirse
// "Precision is over the maximum defined for this asset (-1111)" hatası alınır.
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

// ── R88: Binance izinli kaldıraç okuma ───────────────────────────────────────
// GET /fapi/v1/leverageBracket USER_DATA cevabındaki initialLeverage alanını kullanır.
// Hata olursa emri bozmaz; panel kaldıracıyla devam edilir.
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


// ── R137: Binance sembol kaldıraç sınırı / -4028 fallback ───────────────────
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

// R24: kapanmış pozisyona SL/TP rescue yazma koruması.
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
      const rows = await getPositionRisk(apiKey, apiSecret, {symbol:sym}); // symbol-specific: cache bypass
      const p = Array.isArray(rows) ? rows.find(x=>String(x.symbol||'').toUpperCase()===sym) : null;
      if (p && Math.abs(parseFloat(p.positionAmt || 0)) > 0) return { open:true, pos:p };
    } catch(e) {
      // API geçici patladıysa SL/TP yazmayı tamamen kesme; sadece fresh kontrol başarısız say.
      if (i === attempts-1) return { open:null, error:e };
    }
    if (i < attempts-1) await new Promise(r=>setTimeout(r, i===0 ? 450 : 850));
  }
  return { open:false, pos:null };
}

// ── R167: Güvenli market kapatma merkezi ───────────────────────────────────
async function safeMarketClosePosition(apiKey, apiSecret, symbol, opts={}) {
  const sym = normalizeSymbol(symbol);
  const reason = String(opts.reason || 'SAFE_MARKET_CLOSE');
  const fresh1 = await freshOpenPositionForSymbol(apiKey, apiSecret, sym, 2);
  if (fresh1.open === false) return { ok:true, alreadyClosed:true, reason };
  if (!fresh1.pos) return { ok:false, error:'fresh position okunamadı', reason };
  let amt = parseFloat(fresh1.pos.positionAmt || 0);
  if (!amt) return { ok:true, alreadyClosed:true, reason };
  let side = amt > 0 ? 'SELL' : 'BUY';
  let qty = Math.abs(amt).toString();
  try { await cancelAlgoOrders(apiKey, apiSecret, sym, true); } catch(_) {}
  try {
    const r = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
      symbol:sym, side, type:'MARKET', quantity:qty, reduceOnly:'true', positionSide:'BOTH', __emergency:true
    });
    invalidatePositionRiskCache(`${reason}_REDUCEONLY_OK`);
    return { ok:true, order:r, reduceOnly:true, reason };
  } catch(e1) {
    const msg = String(e1.message || e1);
    if (!msg.includes('-2022') && !/ReduceOnly/i.test(msg)) return { ok:false, error:msg, reason, stage:'reduceOnly' };
    await sleep(450);
    const fresh2 = await freshOpenPositionForSymbol(apiKey, apiSecret, sym, 3);
    if (fresh2.open === false) {
      invalidatePositionRiskCache(`${reason}_ALREADY_CLOSED_AFTER_2022`);
      return { ok:true, alreadyClosed:true, reduceOnlyRejected:true, reason };
    }
    if (!fresh2.pos) return { ok:false, error:`reduceOnly rejected; fresh position okunamadı: ${msg}`, reason };
    amt = parseFloat(fresh2.pos.positionAmt || 0);
    if (!amt) return { ok:true, alreadyClosed:true, reduceOnlyRejected:true, reason };
    side = amt > 0 ? 'SELL' : 'BUY';
    qty = Math.abs(amt).toString();
    try { await cancelAlgoOrders(apiKey, apiSecret, sym, true); } catch(_) {}
    try {
      const r2 = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
        symbol:sym, side, type:'MARKET', quantity:qty, positionSide:'BOTH', __emergency:true
      });
      invalidatePositionRiskCache(`${reason}_FALLBACK_OK`);
      return { ok:true, order:r2, reduceOnly:false, fallback:true, reduceOnlyError:msg, reason };
    } catch(e2) {
      return { ok:false, error:`reduceOnly:${msg} | fallback:${String(e2.message||e2)}`, reason, stage:'fallback' };
    }
  }
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

// ── SL/TP ÇİFTİ YAZ + İSPAT AL — install_live_sltp_pair_with_proof ──────────
// Lazarus'un en önemli pattern'i: yaz, 250ms bekle, Binance'te gözle
async function installSLTPWithProof(apiKey, apiSecret, symbol, closeSide, slPrice, tpPrice, sym) {
  // Çalışan Python çekirdeği kuralı:
  // cancel-first → SL+TP çiftini ALGO endpoint'e yaz → Binance'te görünür proof al → proof yoksa başarılı sayma.
  // R11: Her SL/TP çağrısı burada tek merkezden tickSize'a yuvarlanır.
  // R13 kritik düzeltme: STOP_MARKET / TAKE_PROFIT_MARKET için /fapi/v1/order fallback'i KALDIRILDI.
  // Binance yeni sembollerde -4120 ile açıkça "Algo Order API kullan" diyor; bu yüzden standart endpoint'e düşmek
  // sahte kritik hata üretir ve bazı hesaplarda koruma yazımını gereksiz bozabilir.
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

  let lastErr = null;
  let lastProof = null;

  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      await cancelAlgoOrders(apiKey, apiSecret, symbol);

      const slOrder = await placeAlgoSL(apiKey, apiSecret, symbol, closeSide, slPrice, null);
      const tpOrder = await placeAlgoTP(apiKey, apiSecret, symbol, closeSide, tpPrice, null);

      // Binance algo emirleri açık emir listesine bazen 300ms'den geç düşüyor.
      // Önce kısa, sonra biraz daha uzun proof kontrolü yap; proof gecikmesini hata sanma.
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
      // İlk denemede geçici API/propagation olabilir; ikinci denemeye izin ver.
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
  await binanceThrottle('PUBLIC_REST', path.includes('/ticker/24hr') ? 5 : 1, 0);
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

// ── İMZA + BINANCE SAAT SENKRON ───────────────────────────────────────────────
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
  // R9: Ana Binance signed request tekrar çalışan eski gövdeye alındı.
  // GET/DELETE query string, POST form body. AlgoOrder ayrı bAlgo ile query-string çalışır.
  // R145: reduceOnly acil kapanış emirleri merkezi backoff beklemesine takılmasın.
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
  const res = await fetch(finalUrl, options);
  if (res.status === 429 || res.status === 418) {
    registerHttpBackoffAndThrow(path, res.status, res.headers.get('Retry-After'));
  }
  const text = await res.text();
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

// ── R18 POSITIONRISK CACHE / SINGLE-FLIGHT / RATE-LIMIT GUARD ────────────────
// Binance -1003 hatasının kökü: /positionRisk farklı döngülerden art arda çağrılıyordu.
// R19: R17/R14 balance gövdesine dokunmadan positionRisk için global cache + single-flight.
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
const POS_RISK_TTL_NORMAL = 20000;   // 20sn (pozisyon yok)
const POS_RISK_TTL_ACTIVE = 10000;   // R154: 10sn (eskiden 4sn → dakikada 12 istek → 418 ban). fastManager da 10sn ile sync.
const POS_RISK_RATELIMIT_MS = 90000; // R154: 60sn→90sn. 418 sonrası positionRisk özel cooldown.
const POS_RISK_INFLIGHT_TIMEOUT_MS = 15000; // R95: tek-uçuş 15sn üstü takılırsa temizle

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

// Ham positionRisk: symbol-specific çağrılar için doğrudan Binance'e gider.
// Toplu çağrılar getPositionRiskCached ile yapılmalı.
async function getPositionRisk(apiKey, apiSecret, params={}) {
  const rows = await fetchPositionRiskRaw(apiKey, apiSecret);
  return filterPositionRiskRows(rows, params);
}

async function getPositionRiskCached(apiKey, apiSecret, params={}) {
  resetStuckPositionRiskInflight('getPositionRiskCached');
  const now = Date.now();
  const apiFp = keyFingerprint(apiKey);

  // R95: Binance 418/429 merkezi istek freni aktifken yeni positionRisk isteği açma.
  if (isBinanceBackoffActive()) {
    if (posRiskCache.data && posRiskCache.lastApiKey === apiFp) return filterPositionRiskRows(posRiskCache.data, params);
    throw makeBinanceBackoffError('Binance geçici istek freni', Math.ceil(getBinanceBackoffMs()/1000), 418);
  }

  // -1003 cooldown aktifse cache döndür; cache yoksa yeni emir akışını güvenli durdur.
  if (now < posRiskCache.rateLimitUntil) {
    if (posRiskCache.data && posRiskCache.lastApiKey === apiFp) return filterPositionRiskRows(posRiskCache.data, params);
    throw new Error('positionRisk rate-limit cooldown');
  }

  // Symbol-specific sorgular doğrudan git (cache bypass)
  if (params && params.symbol) {
    return getPositionRisk(apiKey, apiSecret, params);
  }

  const hasOpen = posRiskCache.data && Array.isArray(posRiskCache.data) &&
    posRiskCache.data.some(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0);
  const ttl = hasOpen ? POS_RISK_TTL_ACTIVE : POS_RISK_TTL_NORMAL;

  if (posRiskCache.data && now - posRiskCache.ts < ttl &&
      posRiskCache.lastApiKey === apiFp) {
    return posRiskCache.data;
  }

  // Single-flight: başka istek uçaktaysa aynı promise'i bekle.
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
// Eski isimle çağıran yerler kırılmasın.
function invalidatePositionRiskCache(reason='manual') {
  invalidatePosRiskCache(reason);
}

// Eski dashboard diagnostik alanı için uyumluluk objesi.
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
  if (globalLiqWS && (globalLiqWS.readyState === WebSocket.OPEN || globalLiqWS.readyState === WebSocket.CONNECTING)) return;
  const ws = new WebSocket(`${FAPI_WS_MARKET}/ws/!forceOrder@arr`);
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
  ws.on('error', (e) => { try { r130CombinedTickLastErr = String(e?.message || e || 'ws_error').slice(0,120); } catch(_) {} });
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


// ── R125: 5m ORDERFLOW ÖNCELİK BEYNİ ───────────────────────────────────────
// Amaç: bot veriyi toplamakla kalmasın; depth@100ms/aggTrade/forceOrder akışını
// tek beyin edge formülüne canlı olarak taşısın. Yeni REST yükü bindirmez.
const r125BookHistory = new Map();       // symbol → [{ts, imb, spread, bid, ask}]
const r125PriorityWake = new Map();      // symbol → {ts, reason, score}
let r125FastWakeTimer = null;


// ═══════════════════════════════════════════════════════════════════════════
// R140: Pump Cycle Dedektörü + Equal H/L Tuzak + OI Velocity + BTC Diverjans
// Top 10 gainer saat bağımsız kendi dinamiği ile çalışır
// ═══════════════════════════════════════════════════════════════════════════

// PO3 Pump Phase Classifier
function r140PumpPhase(k5m, atrPct) {
  if (!k5m || k5m.length < 20) return { phase:'UNKNOWN', score:0, label:'bilinmiyor' };
  const rows = k5m.slice(-30).map(c=>({
    o:Number(c[1]),h:Number(c[2]),l:Number(c[3]),c:Number(c[4]),v:Number(c[5]||0)
  })).filter(r=>[r.o,r.h,r.l,r.c].every(Number.isFinite));
  if (rows.length < 15) return { phase:'UNKNOWN', score:0, label:'yetersiz veri' };

  const atr = Math.max(0.3, Number(atrPct)||1);
  const recent8 = rows.slice(-8);
  const recent3  = rows.slice(-3);

  // ATR8 vs ATR20
  function calcAtr(r) {
    const trs = r.map((c,i,a)=>{
      if(i===0)return c.h-c.l;
      return Math.max(c.h-c.l, Math.abs(c.h-a[i-1].c), Math.abs(c.l-a[i-1].c));
    });
    return trs.reduce((s,v)=>s+v,0)/trs.length;
  }
  const atr8  = calcAtr(rows.slice(-8));
  const atr20 = calcAtr(rows.slice(-20));
  const lp = rows.at(-1).c;
  const volAvg = rows.slice(-20,-1).reduce((s,r)=>s+r.v,0)/19;
  const lastVol = rows.at(-1).v;
  const volRatio = volAvg > 0 ? lastVol/volAvg : 1;

  // Trend: ardışık yükselen düşükler
  let bullSeq = 0, bearSeq = 0;
  for (let i=rows.length-1; i>=Math.max(0,rows.length-8); i--) {
    if(rows[i].c>=rows[i].o){ if(!bearSeq) bullSeq++; else break; }
    else { if(!bullSeq) bearSeq++; else break; }
  }

  // Üst wick büyüyor mu (dağıtım izi)
  const upperWicks = recent3.map(r=>(r.h-Math.max(r.o,r.c))/lp*100);
  const wickGrowing = upperWicks[2] > upperWicks[0]*1.5 && upperWicks[2] > 0.15;

  // Faz kararı
  if (atr8 < atr20*0.65 && bullSeq < 2 && bearSeq < 2) {
    return { phase:'ACCUMULATION', score:1, label:'sıkışma·birikim', atRatio:+(atr8/atr20).toFixed(2), bullSeq, bearSeq, volRatio:+volRatio.toFixed(2) };
  }
  if (bullSeq >= 4 && volRatio < 0.7 && wickGrowing) {
    return { phase:'DISTRIBUTION', score:-2, label:'dağıtım·sahte pump', atRatio:+(atr8/atr20).toFixed(2), bullSeq, wickGrowing, volRatio:+volRatio.toFixed(2) };
  }
  if (bullSeq >= 3 && atr8 > atr20*1.2 && volRatio >= 1.2) {
    return { phase:'EXPANSION', score:2, label:'trend·genişleme', atRatio:+(atr8/atr20).toFixed(2), bullSeq, volRatio:+volRatio.toFixed(2) };
  }
  return { phase:'TRANSITION', score:0, label:'geçiş·nötr', atRatio:+(atr8/atr20).toFixed(2), bullSeq, bearSeq, volRatio:+volRatio.toFixed(2) };
}

// Equal Highs / Lows Tuzak Dedektörü
function r140EqualLevels(k5m, k1h, lastPrice) {
  const lp = Number(lastPrice||0);
  if (!k5m || k5m.length < 20 || !lp) return { eqHighs:[], eqLows:[], nearHighTrap:false, nearLowTrap:false };
  const tol = 0.0015; // %0.15

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

  // Fiyata yakın mı? (%0.5 içinde = tuzak aktif)
  const nearHighTrap = eqHighs.some(g=>g.distPct>0 && g.distPct<0.5);
  const nearLowTrap  = eqLows.some(g=>g.distPct<0  && g.distPct>-0.5);
  return { eqHighs, eqLows, nearHighTrap, nearLowTrap,
    summary: `eqHigh:${eqHighs.length} eqLow:${eqLows.length} yakınTuzak:${nearHighTrap||nearLowTrap?'EVET':'YOK'}` };
}

// OI Velocity — sahte pump dedektörü
function r140OiVelocity(oiHist5m, lastPrice, prevClose) {
  if (!oiHist5m || oiHist5m.length < 4) return { velocity:0, fakePump:false, summary:'OI veri yok' };
  const fn = x => Number(x?.sumOpenInterestValue||x?.sumOpenInterest||0);
  const vals = oiHist5m.slice(-6).map(fn).filter(v=>v>0);
  if (vals.length < 3) return { velocity:0, fakePump:false, summary:'OI yetersiz' };
  const latest = vals.at(-1);
  const base   = vals[0];
  const velocity = base>0 ? ((latest-base)/base)*100 : 0;
  const priceUp  = Number(lastPrice||0) > Number(prevClose||0)*1.001;
  // Sahte pump: fiyat yukarı ama OI azalıyor (kapanış, yeni alım değil)
  const fakePump = priceUp && velocity < -0.3;
  const oiConfirmed = priceUp && velocity > 0.2; // Gerçek pump: fiyat ve OI birlikte
  return { velocity:+velocity.toFixed(3), fakePump, oiConfirmed,
    summary:`OI${velocity>0?'+':''}${velocity.toFixed(2)}% ${fakePump?'SAHTE_PUMP':''}${oiConfirmed?'GERCEK_PUMP':''}` };
}

// BTC Diverjans Sinyal
function r140BtcDivergence(btc5mCtx, coinChange15m, coinChange60m) {
  if (!btc5mCtx?.ok) return { divergence:false, strong:false, score:0, label:'' };
  const coinC15 = Number(coinChange15m||0);
  const coinC60 = Number(coinChange60m||0);
  // BTC düşüyor ama coin tutunuyor → kurumsal birikim
  const btcDrop = btc5mCtx.dropping || btc5mCtx.change15m < -0.25;
  const coinHolds = btcDrop && coinC15 > -0.1; // coin %0.1'den az düştü
  // BTC düşerken coin yükseliyor → ekstrem güç
  const strongDiv = btcDrop && coinC15 > 0.2;
  if (strongDiv) return { divergence:true, strong:true, score:20, label:`BTC düşerken coin +${coinC15.toFixed(2)}% = ekstrem güç` };
  if (coinHolds) return { divergence:true, strong:false, score:10, label:`BTC drop ama coin tutuyor = birikim` };
  // BTC toparlanıyor + coin zaten güçlü → momentum devam
  if (btc5mCtx.bouncing && coinC15 > 0.3) return { divergence:false, strong:false, score:8, label:`BTC toparlanıyor + coin güçlü` };
  return { divergence:false, strong:false, score:0, label:'' };
}

// Coin-özgü RVOL (kendi 48 mum ortalamasına göre)
function r140CoinRvol(k5m) {
  if (!k5m || k5m.length < 10) return { rvol:1, signal:'UNKNOWN' };
  const vols = k5m.map(c=>Number(c[5]||0)).filter(v=>v>0);
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
  // Gürültü önleme: aynı sembol için 3 sn içinde daha zayıf uyandırma yazma.
  if (prev && now - prev.ts < 3000 && r125Num(prev.score) >= r125Num(score)) return;
  r125PriorityWake.set(full, { ts:now, reason:String(reason||'flow').slice(0,80), score:r125Num(score) });
}
function r125BookMetricsFromDepth(symbol, depth, lastPrice) {
  const full = r125NormSymbol(symbol);
  const lp = r125Num(lastPrice);
  const bids = Array.isArray(depth?.bids) ? depth.bids.slice(0,50) : [];
  const asks = Array.isArray(depth?.asks) ? depth.asks.slice(0,50) : [];
  let bidTop=0, askTop=0, nearBid=0, nearAsk=0, bestBid=0, bestAsk=0;
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
  const denom = bidTop + askTop;
  const nearDenom = nearBid + nearAsk;
  const imb = denom > 0 ? ((bidTop - askTop) / denom) * 100 : 0;
  const nearImb = nearDenom > 0 ? ((nearBid - nearAsk) / nearDenom) * 100 : imb;
  const spreadPct = (bestAsk>0 && bestBid>0) ? ((bestAsk-bestBid)/lp)*100 : 0;
  const now = Date.now();
  const hist = (r125BookHistory.get(full) || []).filter(x => now - x.ts < 3*60*1000);
  const prev = hist.length ? hist[hist.length-1] : null;
  hist.push({ ts:now, imb, nearImb, spread:spreadPct, bid:bidTop, ask:askTop });
  while (hist.length > 120) hist.shift();
  r125BookHistory.set(full, hist);
  const velocity = prev ? imb - prev.imb : 0;
  const nearVelocity = prev ? nearImb - prev.nearImb : 0;
  const side = nearImb > 12 || imb > 18 ? 'LONG' : nearImb < -12 || imb < -18 ? 'SHORT' : 'NEUTRAL';
  const strength = Math.min(100, Math.max(0, Math.abs(nearImb)*0.8 + Math.abs(velocity)*1.2 + Math.abs(nearVelocity)));
  return {
    ok: denom > 0,
    side, imb:+imb.toFixed(1), nearImb:+nearImb.toFixed(1), velocity:+velocity.toFixed(1), nearVelocity:+nearVelocity.toFixed(1),
    bidTop:+bidTop.toFixed(0), askTop:+askTop.toFixed(0), nearBid:+nearBid.toFixed(0), nearAsk:+nearAsk.toFixed(0),
    spreadPct:+spreadPct.toFixed(4), strength:+strength.toFixed(0), ageMs:0
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
  // Fiyatın %0.35 bandında likidasyonları kümeler. Amaç hedef mıknatısını bulmak.
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

  // R129: canlı akış sadece defter fotoğrafı değildir. Defter tek başına spoof olabilir.
  // Öncelik: son 30sn aggTrade/tick delta. Yoksa CVD fallback. Hiçbiri yoksa flow ısınıyor sayılır.
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
    (book.velocity < -8 ? 2 : book.velocity > 8 ? -2 : 0) +
    (liveSide === 'SHORT' ? 6 : liveSide === 'LONG' ? -5 : 0) +
    (tickData?.whaleBias === 'WHALE_SELL' ? 3 : tickData?.whaleBias === 'WHALE_BUY' ? -3 : 0) +
    (tickData?.deltaFlip === 'BULL_TO_BEAR' ? 4 : tickData?.deltaFlip === 'BEAR_TO_BULL' ? -4 : 0) +
    (r126Extra.askAbsorb ? 8 : r126Extra.bidAbsorb ? -6 : 0) +
    (r126Extra.forecast?.side === 'SHORT' ? Math.min(5, Math.round(r125Num(r126Extra.forecast.confidence,0)/20)) : r126Extra.forecast?.side === 'LONG' ? -3 : 0) +
    (r126Extra.aggressionTrend?.side === 'SHORT' && r126Extra.aggressionTrend?.phase === 'ACCELERATING' ? Math.min(6, Math.round(r125Num(r126Extra.aggressionTrend.strength,0)/16)) : r126Extra.aggressionTrend?.side === 'LONG' && r126Extra.aggressionTrend?.phase === 'ACCELERATING' ? -4 : 0) +
    (r126Extra.deltaImprint?.coiled && liveSide === 'SHORT' ? 2 : 0) +
    (liqData?.cascade?.signal === 'SHORT_FIRSAT' ? 5 : liqData?.cascade?.signal === 'LONG_FIRSAT' ? -5 : 0);

  // R129: tick/CVD yokken defter imbalance tek başına edge değildir; spoof olabilir.
  // Panelde bilgi olarak görünür ama beyni trade'e taşımaz.
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
  const summary = `${flowState} · ${r130sum} · book:${book.side} imb:${book.nearImb}% v:${book.velocity} · delta:${liveSide} ${deltaPct.toFixed(1)}% src:${liveSource} tr:${trades} · edge L${longEdge}/S${shortEdge} · ${clusters.summary} · ${r126Extra.summary}`;
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

// ── R126: ADAPTİF PLAYBOOK + AKIŞ MİKRO-SİNYALLERİ ─────────────────────────
// Yeni REST yükü yok. Var olan depth@100ms + aggTrade + forceOrder verisini tek beyne
// daha erken ve daha canlı bağlar: absorpsiyon, mum kapanış tahmini, delta imprint,
// aggression trend ve playbook win-rate kalibrasyonu.
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
  if (!st || Number(st.n || 0) < 6) return 0; // erken aşamada overfit yapma
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
  // R155: late-chase -12→-8. TOP10 5m scalp'te geç giriş çok agresif ceza alıyordu.
  if (late && !['HTF_REVERSAL','HTF_RECLAIM'].includes(String(mode||''))) { adj -= 8; notes.push('late-chase:-8'); }
  const liveAgainst = (side==='LONG' && deltaPct <= -18) || (side==='SHORT' && deltaPct >= 18) || (forecastSide && forecastSide !== side && forecastConf >= 70);
  if (liveAgainst) { adj -= 10; notes.push('live-opposite:-10'); }
  const liveWith = (side==='LONG' && deltaPct >= 18) || (side==='SHORT' && deltaPct <= -18) || (forecastSide === side && forecastConf >= 70);
  if (liveWith && !late) { adj += 5; notes.push('live-align:+5'); }
  // Trade frequency protection: do not turn soft calibration into a new hard gate.
  // Raw edge remains visible and may still pass if data is fresh; calibration mainly ranks side and weakens repeated losers.
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
  // Hacim/trade var ama delta kapanışta nötre yaklaşıyor: bir sonraki mum için enerji birikimi.
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
  const bidAbsorb = deltaPct < -22 && r125Num(book.nearImb,0) > 10 && priceHeldUp;   // satış akıyor ama bid tutuyor
  const askAbsorb = deltaPct > 22 && r125Num(book.nearImb,0) < -10 && priceHeldDown; // alım akıyor ama ask tutuyor
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


// ─────────────────────────────────────────────────────────────────────────────
// R130 — COMBINED AGGTRADE FLOW ENGINE FIX
// Sorun: R129'da panelde bütün coinlerde tr:0 / delta:0 kalabiliyordu. Sebep,
// per-symbol WS'lerin geç ısınması veya aynı anda çok sayıda ayrı bağlantının
// Railway/Binance tarafında gecikmesiydi. TOP10 scalper için tek combined stream
// daha hızlı ve daha az bağlantılıdır.
// ─────────────────────────────────────────────────────────────────────────────
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
  // R131 kritik fix: Binance futures stream adı case-sensitive'dir.
  // Sembol küçük harf olur ama event adı aggTrade olarak kalmalıdır.
  // Eski R130: full.toLowerCase() + '@aggTrade' ifadesini komple lowercase yapıp
  // '@aggtrade' üretiyordu; bağlantı OPEN/CONNECTING görünse bile tick gelmiyordu.
  // '@' karakterini encode etmiyoruz; sadece sembol parçası URL-safe yapılır.
  return `${full.toLowerCase()}@aggTrade`;
}

function r130StartCombinedAggTradeStream(symbols=[], opts={}) {
  try {
    const clean = [];
    for (const s0 of (Array.isArray(symbols)?symbols:[symbols])) {
      const full = normalizeSymbol ? normalizeSymbol(s0) : r125NormSymbol(s0);
      if (!full || !full.endsWith('USDT')) continue;
      // R133: combined stream'i bozabilecek non-ASCII sembolleri combined listeden çıkar.
      // Bu semboller için CVD/single fallback çalışır; tek bir riskli sembol TOP10 canlı akışını kapatmasın.
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
    // Bağlantı şişmesin. TOP10/TOP24 yeterli; eski scan sembolleri replace:true ile temizlenir.
    const arr = Array.from(next).slice(0, 30);
    const streams = arr.map(r130StreamName).filter(Boolean).sort();
    const key = streams.join('/');
    const rs = r130CombinedTickWS?.readyState;
    if (r130CombinedTickWS && (rs === WebSocket.OPEN || rs === WebSocket.CONNECTING) && key === r130CombinedTickKey) return;
    // R133: scan içinde art arda farklı alt liste çağrıları WS'yi saniyede onlarca kez kapatmasın.
    // Son bağlantı çok yeniyse ve elde canlı tick varsa mevcut bağlantıyı koru.
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
  // R43: OPEN yanında CONNECTING de korunur. Aksi halde getCVD() art arda çağrılınca
  // aynı sembol için yeni WS açılıyor, CVD store sıfırlanıyor ve veri sürekli 50/UNKNOWN kalabiliyordu.
  if (existing?.ws && (rs === WebSocket.OPEN || rs === WebSocket.CONNECTING)) return;

  // R43: reconnect veya prewarm sırasında biriken buy/sell/history silinmesin.
  const store = existing || { buy:0, sell:0, history:[], lastReset: Date.now(), ws:null };
  cvdStore.set(full, store);

  const wsUrl = `${FAPI_WS_MARKET}/ws/${encodeURIComponent(full.toLowerCase())}@aggTrade`;
  const ws = new WebSocket(wsUrl);
  store.ws = ws;

  ws.on('message', (data) => {
    try {
      const t = JSON.parse(data.toString());
      const qty = parseFloat(t.q) * parseFloat(t.p); // USDT hacim
      if (t.m) store.sell += qty;  // maker=true → taker SATTI
      else     store.buy  += qty;  // maker=false → taker ALDI

      // Her 5 dakikada bir geçmişe kaydet.
      // Önemli: tamamen sıfırlamak yerine küçük bir iz bırakıyoruz.
      // Railway/WS yeniden ısınırken CVD'nin sürekli $0 görünmesini azaltır.
      const now = Date.now();
      if (now - store.lastReset > 5 * 60 * 1000) {
        store.history.push({ ts: now, buy: store.buy, sell: store.sell,
          delta: store.buy - store.sell });
        if (store.history.length > 48) store.history.shift(); // 4 saatlik tarih
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
  startCVDStream(full); // yoksa başlat
  const store = cvdStore.get(full);
  if (!store) return { buy:0, sell:0, ratio:50, momentum:'UNKNOWN', trend:'UNKNOWN', historyLen:0, valid:false };

  const buy=store.buy, sell=store.sell;
  const total=buy+sell;
  const ratio=total>0?buy/total*100:50;
  const delta=buy-sell;

  // Tarihsel CVD trendi
  let trend='NEUTRAL', acceleration='NONE';
  if (store.history.length >= 3) {
    const recent = store.history.slice(-3).reduce((s,h)=>s+h.delta,0);
    const older  = store.history.slice(-6,-3).reduce((s,h)=>s+h.delta,0);
    trend = delta > 0 ? 'POSITIVE' : delta < 0 ? 'NEGATIVE' : 'NEUTRAL';
    // R43: işaret güvenli CVD momentum. Eski kıyas negatif older değerinde
    // hâlâ net satış varken ACCELERATING_BULL üretebiliyordu.
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
    bigTrades: [],        // $50K+ büyük işlemler
    // R52: getTickAnalysis VPIN hesabı engine.lastTicks okuyordu ama createTickEngine içinde yoktu.
    // Büyük trade geldikten sonra bu undefined kalırsa tick/orderflow analizi sessizce patlayabilirdi.
    lastTicks: [],        // son 500 aggTrade; VPIN + mikro akış için
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
  // R130: aynı aggTrade hem combined hem legacy WS'den gelirse delta iki kez yazılmasın.
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
  const isBuy  = !trade.m; // maker=true → taker SATTI (bear), maker=false → taker ALDI (bull)
  const ts     = trade.T || Date.now();
  engine.lastTickTs = ts;
  engine.lastTickWallTs = Date.now();
  if (trade.s && usdt >= 12000) r125RegisterPriorityWake(trade.s, `aggTrade ${isBuy?'BUY':'SELL'} $${Math.round(usdt)}`, Math.min(100, usdt/1000));

  engine.lastPrice = price;
  // R52: ham tick ring buffer. VPIN ve mikro-akış bu listeyi okur.
  if (!Array.isArray(engine.lastTicks)) engine.lastTicks = [];
  engine.lastTicks.push({ price, isBuy, usdt, ts });
  if (engine.lastTicks.length > 500) engine.lastTicks.shift();

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


// ── R94: 5m CANLI MERDİVEN + DÖNÜŞ RADARI ─────────────────────────────────
// Ek Binance çağrısı yapmaz. Mevcut 5m mumları kullanır.
// Amaç: Top Gainers merdiven hareketini EMA kilidine takmadan yakalamak;
// tepede/dipte yorulma başladığında karşı yön vur-kaç radarını erken açmak.

// ─────────────────────────────────────────────────────────────────────────────
// R109: Temiz Reclaim Skoru
// Pro scalper prensibi: 3 sinyal yeterli.
//   1. Sweep: Fiyat belirli bir seviyenin altına/üstüne fitil attı
//   2. Reclaim: Fiyat hemen geri kapandı (stop hunt tamamlandı)
//   3. Akış: CVD/Tick yönü değişti
// Bu 3 şey varsa = yüksek kalite giriş. Başka fren YOK.
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// R110: ICT Likidite Sweep Motoru
// Prensip: "Pro scalper sıraya koyar, skora koymaz"
// Akış:
//   ADIM 1 — 1H/15m'den SSL/BSL seviyeleri belirle (Equal Highs/Lows + Swing)
//   ADIM 2 — Fiyat o seviyeye yaklaşıyor mu?
//   ADIM 3 — 5m GÖVDE KAPANIŞI: Sadece iğne değil, gövde seviyenin içine kapandı mı?
//   ADIM 4 — ChoCH/MSS tespiti: Sweep sonrası yön değişimi
//   ADIM 5 — FVG tespiti: Giriş bölgesi
// Her adım zorunlu. Atlanan adım = işlem yok.
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// R111: Sıkışma Patlaması + Squeeze Flow Motoru
// Tamamen göstergeden bağımsız — sadece ham OHLCV + Binance akış verileri.
//
// MODÜL A: ATR Squeeze — saf True Range matematiği
//   tr = max(high-low, |high-prevC|, |low-prevC|)
//   Sıkışma: atr8 < atr30 × 0.65 AND ≥8 mum sıkışmış
//   Patlama: son mum tr > atr30 × 1.8 AND gövde > range×0.5
//   Skor 0-4: impulse+hacim+HTF hiza+uzun sıkışma
//
// MODÜL B: Squeeze Flow — 4 Binance endpoint + saf mum OB
//   Short squeeze: funding<-0.03% + short>%60 + OI arıyor + fiyat arttı
//   Long squeeze:  funding>+0.05% + long>%65 + OI artıyor + fiyat düştü
//   4H Order Block: büyük mum öncesi hamleden saf gövde/hacim tespiti
//
// KESİN SINIR: R94(merdiven) veya R110(ChoCH) aktifse R111 çalışmaz.
// R111 yalnızca "fiyat yatay sıkışmış" durumda anlam taşır.
// ─────────────────────────────────────────────────────────────────────────────
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

  // ── MODÜL A: Saf True Range Sıkışma ─────────────────────────────────────
  function calcTR(a, b) {
    // b = önceki mum, a = mevcut mum
    if (!b) return a.h - a.l;
    return Math.max(a.h - a.l, Math.abs(a.h - b.c), Math.abs(a.l - b.c));
  }

  const trs = r5.map((k, i) => i === 0 ? k.h - k.l : calcTR(k, r5[i-1]));
  const last = r5.at(-1);
  const lastTR = trs.at(-1);

  // atr8 = son 8 mumun TR ortalaması
  const atr8  = trs.slice(-8).reduce((s, v) => s + v, 0) / 8;
  // atr30 = son 30 mumun TR ortalaması
  const atr30 = trs.slice(-30).reduce((s, v) => s + v, 0) / Math.min(30, trs.length);

  // Kaç ardışık mum sıkışmış?
  let siksmaAdet = 0;
  for (let i = trs.length - 2; i >= 0; i--) {
    const windowAvg = trs.slice(Math.max(0, i-29), i).reduce((s,v)=>s+v,0) /
                      Math.min(30, i);
    if (windowAvg > 0 && trs[i] < windowAvg * 0.70) siksmaAdet++;
    else break;
  }

  const siksmaVarMi = atr8 < atr30 * 0.65 && siksmaAdet >= 8;

  // Patlama mumu: son mum hem TR büyük hem gövde güçlü
  const govde = Math.abs(last.c - last.o);
  const range = last.h - last.l;
  const squeezeBreakout = siksmaVarMi &&
    lastTR > atr30 * 1.8 &&
    range > 0 && govde / range > 0.5;

  // Skor 0-4
  let squeezeSkor = 0;
  if (squeezeBreakout) squeezeSkor += 1;                              // +1 impulse
  // Hacim spike: son mumu ortalamaya dahil etme; aksi halde spike kendi eşiğini yükseltir.
  const prevVolRows = r5.slice(-11, -1);
  const avgVol5 = prevVolRows.length ? prevVolRows.reduce((s,k)=>s+k.v,0)/prevVolRows.length : 0;
  if (avgVol5 > 0 && last.v > avgVol5 * 1.5) squeezeSkor += 1;       // +1 hacim spike
  // HTF hizası: veri yoksa otomatik ters sayma yok; sadece gerçek 1H/4H son mumu varsa puan ver.
  const h1Last = r1h.at(-1), h4Last = r4h.at(-1);
  const htfYon = (h1Last ? (h1Last.c > h1Last.o ? 1 : -1) : 0) +
                 (h4Last ? (h4Last.c > h4Last.o ? 1 : -1) : 0);
  const breakDir = last.c > last.o ? 1 : -1;
  if (htfYon !== 0 && breakDir === Math.sign(htfYon)) squeezeSkor += 1; // +1 HTF hizası
  if (siksmaAdet >= 12) squeezeSkor += 1;                             // +1 uzun sıkışma

  const squeezeOk = squeezeBreakout && squeezeSkor >= 3;
  const squeezeLong  = squeezeOk && last.c > last.o;
  const squeezeShort = squeezeOk && last.c < last.o;

  // ── MODÜL B-1: Squeeze Flow — Funding + L/S + Taker + OI ─────────────────
  // Funding: son 3 dönemin oranına bak
  const fundArr = Array.isArray(fundingData) ? fundingData.slice(-3) : [];
  const fundAvg = fundArr.length
    ? fundArr.reduce((s, f) => s + Number(f.fundingRate || f.lastFundingRate || 0), 0) / fundArr.length
    : 0;
  const fundAsiriNeg = fundAvg < -0.0003;  // -0.03%
  const fundAsiriPos = fundAvg > +0.0005;  // +0.05%

  // L/S Oranı — global ve top trader birlikte okunur. Binance cevabında longAccount/shortAccount zaten oran olarak gelir.
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
  // Kalabalıklaşma açısından en tehlikeli tarafı dikkate al: ya genel kitle ya top trader tarafı aşırıysa sıkışma potansiyeli var.
  const shortRatio = Math.max(lsGlobalParsed.short, lsTopParsed.short);
  const longRatio  = Math.max(lsGlobalParsed.long,  lsTopParsed.long);
  const shortKalabaligi = shortRatio > 0.60;  // >%60 short
  const longKalabaligi  = longRatio  > 0.65;  // >%65 long

  // Taker Buy/Sell
  const takArr = Array.isArray(takerData) ? takerData.slice(-3) : [];
  const takBuySell = takArr.length
    ? takArr.reduce((s, t) => s + Number(t.buySellRatio || 1), 0) / takArr.length
    : 1;
  const takerBuyBaskin  = takBuySell > 1.15;  // Alıcılar baskın
  const takerSellBaskin = takBuySell < 0.87;  // Satıcılar baskın

  // OI trendi — R111 için hızlı 5m OI histogramı kullanılır; yoksa nötr kalır, crash atmaz.
  function oiVal(x) { return Number(x?.sumOpenInterestValue || x?.sumOpenInterest || x?.openInterest || 0); }
  const oi5 = Array.isArray(oiHist5mData) ? oiHist5mData.slice(-6) : [];
  const oiNow = Number(oiNowData?.openInterest || oiVal(oi5.at(-1)) || 0);
  let oiChgPct = 0;
  if (oi5.length >= 4) {
    const base = oiVal(oi5[oi5.length - 4]);
    const lat  = oiVal(oi5.at(-1));
    oiChgPct = base > 0 ? ((lat - base) / base) * 100 : 0;
  }
  const oiArtis = oiChgPct > 0.20;       // son ~15dk OI artışı: yeni pozisyon ekleniyor
  const oiDusuyor = oiChgPct < -0.30;    // pozisyon çözülüyor, squeeze sonlanıyor olabilir

  // Fiyat değişimi: son mum yönü
  const fiyatArti  = last.c > last.o;
  const fiyatDustu = last.c < last.o;

  // Short Squeeze: funding negatif + short kalabalık + OI artıyor + fiyat yukarı + taker alış baskın
  const shortSqueeze = fundAsiriNeg && shortKalabaligi && oiArtis && fiyatArti && takerBuyBaskin;
  // Long Squeeze: funding pozitif + long kalabalık + OI artıyor + fiyat aşağı + taker satış baskın
  const longSqueeze  = fundAsiriPos && longKalabaligi  && oiArtis && fiyatDustu && takerSellBaskin;

  // ── MODÜL B-2: 4H/1H Order Block — Saf mum matematiği ───────────────────
  // Demand OB: 4H'ta son güçlü bullish hamlenenin hemen öncesindeki bearish mum
  // Supply OB: 4H'ta son güçlü bearish hamlenenin hemen öncesindeki bullish mum
  function findOB(rows, type) {
    if (rows.length < 6) return null;
    const avgBody = rows.slice(-20).reduce((s,k)=>s+Math.abs(k.c-k.o),0)/20;

    for (let i = rows.length - 2; i >= 3; i--) {
      const k = rows[i], kNext = rows[i+1];
      if (type === 'demand') {
        // Büyük bearish mum (body > avg×1.5) ardından güçlü bullish hamle
        const bearBody = k.o - k.c;  // kırmızı
        if (bearBody < avgBody * 1.5) continue;
        // Ardından 3 mumda %3+ yükseliş
        const upMove = (rows[i+3]?.h - k.l) / k.l * 100;
        if ((upMove || 0) > 3) {
          return { low: R(k.l), high: R(k.h), mid: R((k.l+k.h)/2), tf: '4H' };
        }
      } else {
        // Büyük bullish mum ardından güçlü bearish hamle
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

  // Fiyat OB bölgesinde mi?
  const inDemand = demandOB && lp >= demandOB.low * 0.995 && lp <= demandOB.high * 1.005;
  const inSupply = supplyOB && lp >= supplyOB.low * 0.995 && lp <= supplyOB.high * 1.005;
  const obBaskisi = inDemand ? 'DEMAND_OB' : inSupply ? 'SUPPLY_OB' : 'YOK';

  // ── NİHAİ KARAR — tek çizgi ───────────────────────────────────────────────
  // Long: (Squeeze patlama yukarı) VEYA (Short squeeze + Demand OB)
  const r111LongOk = !!(
    (squeezeLong) ||
    (shortSqueeze && (inDemand || takerBuyBaskin) && !inSupply)
  );

  // Short: (Squeeze patlama aşağı) VEYA (Long squeeze + Supply OB)
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


// ─────────────────────────────────────────────────────────────────────────────
// R114: 5M SHIFT / MM TUZAK KORUMASI
// Amaç: Top-gainer coinde sırf "Sweep + Stop Hunt + MM_UP_SWEEP" gördü diye
// düşen 5m body-shift içine LONG basmamak. Bu yeni bir kural yığını değildir;
// tek disiplin: yön değişimi/reclaim yoksa sweep sadece "avlandı" bilgisidir, emir değildir.
// ─────────────────────────────────────────────────────────────────────────────
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

  // Body-shift: sadece tek kırmızı/yeşil mum değil; 4-9 mumluk gövde akışı.
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

  // Reclaim/shift onayı: Wick değil, gövde gerçekten yönü geri almalı.
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
  // R115: HTF Likidite Haritası + 5m Body-Reclaim İcra Motoru
  // Bu fonksiyon R110 adını korur ki eski entegrasyon kırılmasın; içerik R115 mantığıdır.
  // Ana disiplin: 15m/1H/4H güçlü SSL/BSL seviyesi yoksa 5m wick arama yok.
  // Sweep mumu: wick dışarı taşar, 5m kapanış/gövde tekrar seviyenin içine veya güvenli tarafına döner.
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
  // Top-gainer 5m çok oynak olabilir; zone çok dar olursa gerçek sweep kaçırır, çok geniş olursa rastgele wick sayar.
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
        // Güç: HTF ağırlığı + temas sayısı + tazelik. Tek 15m swing zayıf; 1H/4H tek swing yine bilgi taşır.
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
    // Yakın kümeleri birleştir; 4H/1H seviyeyi 15m gürültüsünden üstün tut.
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

  // Altın kural: seviye önceden çizili olacak; 5m mumu o seviyeyi wick ile aşacak;
  // kapanış/gövde tekrar seviye içine veya güvenli tarafa dönecek. Gövde dışarıda kapatırsa kırılım sayılır, sweep girişi değildir.
  if (nearSSL && approachingSSL) {
    for (let i = 0; i < Math.min(6, r5.length); i++) {
      const m = r5.at(-(i+1));
      const st = candleStats(m);
      const pierced = m.l < nearSSL.zoneLow * (1 - piercePct/100);
      const reclaimedClose = m.c >= nearSSL.zoneLow;              // kapanış zone içine/üstüne döndü
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
      const reclaimedClose = m.c <= nearBSL.zoneHigh;              // kapanış zone içine/altına döndü
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

  // FVG yoksa bile güçlü MSS gövdesi yeterli olabilir; ama wick-only asla yeterli değildir.
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
    dashboardText: dashParts.join(' · '),
    liquidityMapText: mapParts.join(' | '),
    allSSL: allSSL.slice(0, 3),
    allBSL: allBSL.slice(0, 3),
  };
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
    // Sweep: son 3 mumda birinin low, önceki 3 mumun mininden düşük
    const prevMin3 = Math.min(prev2.l, prev3.l, rows.at(-5)?.l || prev3.l);
    const sweepMum = [prev, prev2, prev3].find(k => k.l < prevMin3 * 0.999);
    swept = !!sweepMum;
    if (swept) score += 3;

    // Reclaim: sweep mumundan sonra kapanış dönüşü
    const reclaimTarget = sweepMum ? sweepMum.h : prev.h;
    reclaimed = last.c > reclaimTarget * 0.998 && last.c > last.o;
    if (reclaimed) score += 4;

    // Gövde güçlü mü?
    const body = last.c - last.o;
    const range = last.h - last.l;
    if (body > 0 && range > 0 && body / range > 0.5) score += 2;

    // Son mum öncekinden güçlü mü?
    if (last.c > prev.h) score += 1;

  } else {
    // SHORT için simetrik
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

  // CVD hizalaması: son 2 mumun close > open toplamı (LONG) veya < open (SHORT)
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

  // R94: Merdiven sadece son 12 mumdaki kusursuz higher-low değildir.
  // Top Gainers'ta trend 30-36 mum boyunca merdiven gibi gider, son 2-3 mumda yorulabilir.
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

const tickStarting = new Set(); // R26: aynı sembole paralel WS açılışını engeller

// ── R28: PİYASA GENELİ YÖNÜ (Market Breadth) ─────────────────────────────────
// Son 40 analizden piyasanın genel yönünü hesapla
const marketBreadthStore = {
  signals: [], // [{sym, rec, score, ts}] max 60 kayıt
  bull: 0, bear: 0, neutral: 0,
  breadthScore: 0, // -1 tam ayı, +1 tam boğa
  ts: 0,
};
function updateMarketBreadth(sym, rec, score) {
  const now = Date.now();
  // Eski kayıtları temizle (20 dakikadan eski)
  marketBreadthStore.signals = marketBreadthStore.signals.filter(s => now-s.ts < 20*60*1000);
  // Aynı sembolün eski kaydını güncelle
  const idx = marketBreadthStore.signals.findIndex(s => s.sym === sym);
  if (idx >= 0) marketBreadthStore.signals[idx] = { sym, rec, score, ts:now };
  else marketBreadthStore.signals.push({ sym, rec, score, ts:now });
  if (marketBreadthStore.signals.length > 60) marketBreadthStore.signals.shift();
  // Breadth hesapla
  const recent = marketBreadthStore.signals.filter(s => now-s.ts < 10*60*1000);
  const bull = recent.filter(s => s.rec==='LONG').length;
  const bear = recent.filter(s => s.rec==='SHORT').length;
  const total = bull + bear;
  marketBreadthStore.bull = bull; marketBreadthStore.bear = bear;
  marketBreadthStore.neutral = recent.length - total;
  marketBreadthStore.breadthScore = total > 0 ? (bull-bear)/total : 0;
  marketBreadthStore.ts = now;
}

// ── R27: MM AVLAMA — GLOBAL DEPOLAR ──────────────────────────────────────────
// BTC 5m değişim cache (tüm coinler için ortak, 25sn TTL)
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

// Spread geçmişi (per symbol, 3dk ring)
const r27SpreadHistory = new Map(); // symbol → {cur, prev3m, ts}
function updateSpreadHistory(sym, spreadPct) {
  const now = Date.now();
  const prev = r27SpreadHistory.get(sym);
  if (!prev) { r27SpreadHistory.set(sym, { cur:spreadPct, prev3m:spreadPct, ts:now }); return; }
  if (now - prev.ts > 3*60*1000) {
    r27SpreadHistory.set(sym, { cur:spreadPct, prev3m:prev.cur, ts:now });
  } else {
    prev.cur = spreadPct; // anlık güncelle
  }
}

// Tick WS başlat — @trade stream (aggTrade'den daha granüler)
async function startTickStream(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  const existing = tickStore.get(full);
  if (existing?.ws && (existing.ws.readyState === WebSocket.OPEN || existing.ws.readyState === WebSocket.CONNECTING)) return;
  if (tickStarting.has(full)) return;
  tickStarting.add(full);

  const tickSz = full.startsWith('BTC')?0.1:full.startsWith('ETH')?0.01:
                 full.startsWith('BNB')?0.01:0.0001;

  // R129: engine'i REST ticker beklemeden hemen oluştur.
  // Önceki sürümde bPub('/ticker/24hr') gecikirse tickStore boş kalıyor, analiz ilk dakikalarda
  // delta/trade=0 görüp sadece defter imbalance'a bakıyordu. 5m scalper için bu körlük yapar.
  const engine = existing || createTickEngine(tickSz, 5);
  engine.tickSize = tickSz;
  if (!Array.isArray(engine.lastTicks)) engine.lastTicks = [];
  if (!Array.isArray(engine.candles)) engine.candles = [];
  tickStore.set(full, engine);

  // Volatiliteye göre candleMs'i arka planda güncelle; WS açılışını bloke etme.
  bPub('/fapi/v1/ticker/24hr', 'symbol='+full).then(ticker => {
    const vol24h = Math.abs(parseFloat(ticker.priceChangePercent)||5);
    const tmp = createTickEngine(tickSz, vol24h);
    engine.candleMs = tmp.candleMs;
    engine.vol24h = +vol24h.toFixed(2);
    console.log(`${full} tick engine: candleMs=${engine.candleMs}ms vol24h=${vol24h.toFixed(1)}%`);
  }).catch(()=>{});

  // @trade yerine @aggTrade kullan (Binance Futures'ta @trade yok)
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
  r130StartCombinedAggTradeStream([full]);
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
  const nowTick = Date.now();
  const recentRaw30s = (Array.isArray(engine.lastTicks)?engine.lastTicks:[]).filter(t=>nowTick-r125Num(t.ts,0)<30*1000);
  const recent30Buy = recentRaw30s.filter(t=>t.isBuy).reduce((s,t)=>s+r125Num(t.usdt),0);
  const recent30Sell = recentRaw30s.filter(t=>!t.isBuy).reduce((s,t)=>s+r125Num(t.usdt),0);
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
  // R52: defensive read; eski state/WS objesi gelirse engine.lastTicks yok diye analiz düşmesin.
  const rawTicksForVpin = Array.isArray(engine.lastTicks)
    ? engine.lastTicks
    : (Array.isArray(engine.sweepDet?.lastTicks) ? engine.sweepDet.lastTicks : []);
  const vpinResult = calcVPIN(rawTicksForVpin.length > 0
    ? rawTicksForVpin.slice(-500).map(t => ({...t, usdt:t.usdt||0}))
    : [], 30);

  // Delta microstructure
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

// ═══════════════════════════════════════════════════════════════════════════════
// 2. WEBSOCKET ICEBERG TESPİTİ — Order book değişimlerini izle
// ═══════════════════════════════════════════════════════════════════════════════
const icebergStore = new Map(); // symbol → {bids, asks, hiddenBuy, hiddenSell, events}

function startIcebergStream(symbol) {
  const full = symbol.toUpperCase().endsWith('USDT') ? symbol.toUpperCase() : symbol.toUpperCase()+'USDT';
  const existing = icebergStore.get(full);
  const rs = existing?.ws?.readyState;
  // R43: CONNECTING sırasında tekrar depth WS açma; mevcut book/events korunur.
  if (existing?.ws && (rs === WebSocket.OPEN || rs === WebSocket.CONNECTING)) return;

  const store = existing || { bids: new Map(), asks: new Map(), hiddenBuy:0, hiddenSell:0,
    events:[], ws:null, lastUpdate: Date.now() };
  icebergStore.set(full, store);

  // Depth stream: her 100ms'de book güncelleme
  const wsUrl = `${FAPI_WS_PUBLIC}/ws/${full.toLowerCase()}@depth@100ms`;
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
      { headers:{ 'accept':'application/json' }, signal: AbortSignal.timeout(3000) }
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

// ── R25: TR SAAT FORMATI (Europe/Istanbul = UTC+3) ─────────────────────────
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

// ── R25: İŞLEM KARNESİ — DOSYAYA YAZILIR (Railway restart'ta kaybolmaz) ─────

// ── R168b: TELEGRAM BİLDİRİM SİSTEMİ — KUYRUK + TEST + HTML SAFE ────────
// .env veya Railway env: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
// @BotFather'dan bot oluştur → token al → bota /start yaz → chat ID al
const TG_TOKEN   = process.env.TELEGRAM_BOT_TOKEN || '';
const TG_CHAT_ID = process.env.TELEGRAM_CHAT_ID   || '';
let   tgLastSent = 0;
let   tgQueue = Promise.resolve();
const TG_MIN_GAP = 1200; // art arda mesajı düşürme; kısa gecikmeyle sıraya al

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
      // R171: HTML parse vb. yüzünden trade kartı düşmesin; düz metin fallback dene.
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
      try { pushCritical('TELEGRAM_SEND_FAIL', new Error(err), { symbol:'TELEGRAM' }, 'WARN'); } catch(_) {}
      console.log('⚠️ Telegram gönderim hatası:', err);
      return {ok:false, error:err};
    }
    return {ok:true};
  } catch(e) {
    const err = String(e?.message || e).slice(0,160);
    try { pushCritical('TELEGRAM_SEND_FAIL', new Error(err), { symbol:'TELEGRAM' }, 'WARN'); } catch(_) {}
    console.log('⚠️ Telegram gönderim exception:', err);
    return {ok:false, error:err};
  }
}

function tgSend(text, silent=false) {
  // Eski kod 3sn içinde gelen mesajı direkt çöpe atıyordu. R168b: kuyruk var, mesaj kaybolmaz.
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



// ── R168d: TELEGRAM EVENT HOOK — emir açılış/kapanış bildirimi ledger olayından gider ──
// R168c'de başlangıç/backup çalışıyor ama scan-loop içindeki tgTradeOpen çağrısı bazı yollarda kaçabiliyordu.
// Bu yüzden bildirim artık recordTradeOpen / recordTradeClose olayına bağlandı. Bir işlem bir kez bildirilir.
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
    try { pushCritical('TELEGRAM_OPEN_NOTIFY_FAIL', new Error(String(e?.message||e)), {symbol:String(row.symbol||'TELEGRAM')}, 'WARN'); } catch(_) {}
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
    try { pushCritical('TELEGRAM_CLOSE_NOTIFY_FAIL', new Error(String(e?.message||e)), {symbol:String(row.symbol||'TELEGRAM')}, 'WARN'); } catch(_) {}
  }
}


const tradeLedgerPath = './trade_ledger_live.json';
let tradeLedger = [];
try {
  tradeLedger = JSON.parse(fs.readFileSync(tradeLedgerPath, 'utf8') || '[]');
  if (!Array.isArray(tradeLedger)) tradeLedger = [];
} catch(_) { tradeLedger = []; }
function r176LedgerKey(row={}) {
  const sym = normalizeSymbol(String(row.symbol||'')).replace('USDT','');
  const t = Math.round(Number(row.closedAt || row.openedAt || 0) / 60000); // 1dk bucket
  const pnl = Number(row.pnlUSDT||0).toFixed(2);
  return `${sym}_${t}_${pnl}`;
}
function r176DedupeLedger(limit=250) {
  const seen = new Set();
  const out = [];
  for (const r of (Array.isArray(tradeLedger) ? tradeLedger : [])) {
    if (!r || !(r.openedAt || r.closedAt)) continue;
    const key = r.id && !String(r.id).startsWith('RESTORE_') ? String(r.id) : r176LedgerKey(r);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(r);
  }
  tradeLedger = out.sort((a,b)=>Number(b.closedAt||b.openedAt||0)-Number(a.closedAt||a.openedAt||0)).slice(0, limit);
  return tradeLedger.length;
}
function saveTradeLedger() {
  try {
    r176DedupeLedger(250);
    fs.writeFileSync(tradeLedgerPath, JSON.stringify(tradeLedger.slice(0,250), null, 2));
  } catch(_) {}
}

// ── R26: LAST-KNOWN POSITIONS — Railway restart/state kaybında kapanış tespiti ──
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
    slPct:safeNum(state.slPct || state.entrySLPct || old.slPct, 3),
    tpPct:safeNum(state.tpPct || old.tpPct, 3),
    peakPnl:safeNum(state.peakPnl || old.peakPnl, 3),
    peakRealPct:safeNum(state.peakRealPct || old.peakRealPct, 3),
    sltpVerified:!!(state.sltpVerified || old.sltpVerified),
    usdtAmount:safeNum(state.usdtAmount || autoConfig?.usdtAmount || old.usdtAmount, 2),
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
    quantity:safeNum(qty || state?.quantity || state?.positionAmt, 8),
    marginUSDT:safeNum(state?.usdtAmount || autoConfig?.usdtAmount,2),
    sl:safeNum(state?.currentSL || state?.slPrice, 10), tp:safeNum(state?.targetTP || state?.tpPrice, 10),
    slPct:safeNum(state?.slPct || state?.entrySLPct, 3), tpPct:safeNum(state?.tpPct, 3),
    entryReason:state?.openReason || state?.entryReason?.reason || `${normalizeSide(side)} açıldı`,
    score:state?.score || state?.entryReason?.score || null, tier:state?.tier || state?.entryReason?.tier || null,
    exitReason:null,resultNote:null,pnlUSDT:null,roiPct:null,
    closedAt:null,closedAtTR:null,cooldownMin:null,
  };
  tradeLedger = [row,...tradeLedger.filter(x=>x.id!==id)].slice(0,250);
  saveTradeLedger();
  // R168d: açılış bildirimi scan-loop yerine ledger event hook'tan gider.
  try { tgNotifyTradeOpenOnce(row, state); } catch(_) {}
  return row;
}
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
  const rowQty = Math.abs(Number(state?.positionAmt || state?.qty || state?.quantity || row.quantity || 0));
  const approxLedgerPnl = entry>0 && close>0 && rowQty>0 ? (close-entry)*rowQty*(normalizeSide(row.side)==='SHORT'?-1:1) : null;
  const finalPnl = Number.isFinite(Number(cls.realizedPnl)) && Math.abs(Number(cls.realizedPnl))>0.000001
    ? Number(cls.realizedPnl)
    : (Number.isFinite(approxLedgerPnl) ? approxLedgerPnl : null);
  Object.assign(row, {
    status:'CLOSED', closedAt, closedAtTR:trTime(closedAt), closePrice:safeNum(close,10),
    pnlUSDT:Number.isFinite(finalPnl)?safeNum(finalPnl,4):null,
    roiPct:Number.isFinite(rawMove)?safeNum(rawMove*lev,2):(Number.isFinite(Number(cls.roiPct))?safeNum(cls.roiPct,2):null),
    exitReason:cls.code||'CLOSED', exitLabel:cls.label||'Binance kapanışı',
    resultNote:buildResultNote(cls,state),
    sl:cls.sl||state?.currentSL||null, tp:cls.tp||state?.targetTP||null,
    cooldownMin:cdMs>0?Math.ceil(cdMs/60000):null,
  });
  tradeLedger = [row,...tradeLedger.filter(x=>x!==row&&x.id!==row.id)].slice(0,250);
  try { r126UpdatePlaybookStats(state, cls); } catch(_) {}
  // R168d: kapanış bildirimi ledger event hook'tan gider; 0/null PnL olsa bile gönderilir.
  try { tgNotifyTradeCloseOnce(row, state, cls); } catch(_tge) {}

  saveTradeLedger(); return row;
}

// ── R171: TELEGRAM POLLING NOTIFIER + PNL RECONCILE ─────────────────────────
// Başlangıç/backup mesajı geliyorsa Telegram ayarı doğrudur. Trade kartı gelmiyorsa olay hook'u kaçmıştır.
// Bu katman ledger'ı periyodik tarar; açılış/kapanış kartını kaçırmadan gönderir.
const TG_NOTIFY_STATE_PATH = './telegram_notify_state.json';
let tgNotifyState = {open:{}, close:{}};
try {
  const _tn = JSON.parse(fs.readFileSync(TG_NOTIFY_STATE_PATH,'utf8') || '{}');
  if (_tn && typeof _tn === 'object') tgNotifyState = {open:_tn.open||{}, close:_tn.close||{}};
} catch(_) {}
function saveTgNotifyState() {
  try { fs.writeFileSync(TG_NOTIFY_STATE_PATH, JSON.stringify(tgNotifyState, null, 2)); } catch(_) {}
}
function r171TradeRowId(row={}) {
  return row.id || `${row.symbol||'?'}_${row.side||'?'}_${row.openedAt||0}`;
}
function r171RowIsClosed(row={}) {
  return !!(row.status === 'CLOSED' || row.closedAt || row.exitReason || row.exitLabel);
}
function r171NotifyOpenFromLedger(row={}) {
  const id = r171TradeRowId(row);
  if (!row.openedAt || tgNotifyState.open[id]) return false;
  tgNotifyState.open[id] = Date.now(); saveTgNotifyState();
  try {
    tgTradeOpen(row.symbol || '?', row.side || '?',
      row.score ?? '-', row.edge ?? row.brainConfidence ?? '-',
      row.marginUSDT ?? autoConfig?.usdtAmount,
      row.leverage ?? autoConfig?.leverage,
      row.entryReason || `${row.side||''} açıldı`,
      {
        entryPrice: row.entryPrice,
        slPrice: row.sl,
        tpPrice: row.tp,
        quantity: row.quantity,
        leverage: row.leverage,
        margin: row.marginUSDT,
        tier: row.tier,
        slPct: row.slPct,
        tpPct: row.tpPct,
        mode: row.brainMode || '',
      });
    return true;
  } catch(e) {
    try { pushCritical('TELEGRAM_LEDGER_OPEN_FAIL', new Error(String(e?.message||e)), {symbol:String(row.symbol||'TELEGRAM')}, 'WARN'); } catch(_) {}
    return false;
  }
}
function r171NotifyCloseFromLedger(row={}) {
  const id = r171TradeRowId(row);
  if (!r171RowIsClosed(row) || tgNotifyState.close[id]) return false;
  tgNotifyState.close[id] = Date.now(); saveTgNotifyState();
  try {
    const pnl = Number(row.pnlUSDT);
    const roi = Number(row.roiPct);
    const dur = row.openedAt
      ? (() => { const m = Math.max(0, Math.round((Number(row.closedAt||Date.now())-Number(row.openedAt))/60000)); return m < 60 ? `${m}dk` : `${Math.floor(m/60)}s ${m%60}dk`; })()
      : '';
    tgTradeClose(row.symbol || '?', row.side || '?',
      Number.isFinite(pnl) ? pnl : null,
      Number.isFinite(roi) ? roi : null,
      row.exitLabel || row.exitReason || 'Kapanış algılandı',
      {
        entryPrice: row.entryPrice,
        closePrice: row.closePrice,
        duration: dur,
        sl: row.sl,
        tp: row.tp,
        leverage: row.leverage,
        quantity: row.quantity,
        marginUSDT: row.marginUSDT,
        resultNote: row.resultNote,
      });
    return true;
  } catch(e) {
    try { pushCritical('TELEGRAM_LEDGER_CLOSE_FAIL', new Error(String(e?.message||e)), {symbol:String(row.symbol||'TELEGRAM')}, 'WARN'); } catch(_) {}
    return false;
  }
}
async function r171TelegramPollLedger(force=false) {
  let sentOpen=0, sentClose=0;
  try {
    const rows = Array.isArray(tradeLedger) ? tradeLedger.slice(0,80) : [];
    for (const row of rows) {
      if (!row || !row.openedAt) continue;
      if (force || Number(row.openedAt||0) > Date.now() - 24*60*60*1000) {
        if (r171NotifyOpenFromLedger(row)) sentOpen++;
        if (r171NotifyCloseFromLedger(row)) sentClose++;
      }
    }
  } catch(e) {
    try { pushCritical('TELEGRAM_LEDGER_POLL_FAIL', new Error(String(e?.message||e)), {symbol:'TELEGRAM'}, 'WARN'); } catch(_) {}
  }
  return {sentOpen, sentClose};
}

async function r171ReconcileTradeLedgerPnL(apiKey, apiSecret, maxRows=30) {
  if (!apiKey || !apiSecret) return {ok:false, reason:'api_missing'};
  let fixed=0, checked=0, errors=0;
  const rows = Array.isArray(tradeLedger) ? tradeLedger : [];
  for (const row of rows.slice(0, maxRows)) {
    try {
      if (!row || !r171RowIsClosed(row) || !row.openedAt) continue;
      const pnlNow = Number(row.pnlUSDT);
      const needs = (!Number.isFinite(pnlNow) || Math.abs(pnlNow) < 0.000001 || !row.closePrice);
      if (!needs) continue;
      checked++;
      const sym = normalizeSymbol(String(row.symbol||''));
      const startTime = Math.max(0, Number(row.openedAt||0) - 120000);
      const endTime = Number(row.closedAt||Date.now()) + 180000;
      let incomePnl = null, tradePnl = null, closePx = null, qty = null;
      try {
        const inc = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/income', {
          symbol:sym, incomeType:'REALIZED_PNL', startTime, endTime, limit:100
        });
        if (Array.isArray(inc) && inc.length) {
          const sum = inc.reduce((s,x)=>s+Number(x.income||0),0);
          if (Number.isFinite(sum) && Math.abs(sum) > 0.000001) incomePnl = sum;
        }
      } catch(ei) {}
      try {
        const trades = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/userTrades', {symbol:sym, startTime, endTime, limit:1000});
        if (Array.isArray(trades) && trades.length) {
          const closeSide = normalizeSide(row.side) === 'SHORT' ? 'BUY' : 'SELL';
          const afterOpen = trades.filter(t => Number(t.time||0) >= Number(row.openedAt||0) + 500 && String(t.side||'').toUpperCase() === closeSide);
          const use = afterOpen.length ? afterOpen : trades.filter(t=>String(t.side||'').toUpperCase() === closeSide);
          let notional=0, q=0, rp=0;
          for (const t of use) {
            const p=Number(t.price||0), qq=Math.abs(Number(t.qty||0)), r=Number(t.realizedPnl||0);
            if (p>0 && qq>0) { notional += p*qq; q += qq; }
            if (Number.isFinite(r)) rp += r;
          }
          if (q>0) { closePx = notional/q; qty = q; }
          if (Number.isFinite(rp) && Math.abs(rp)>0.000001) tradePnl = rp;
        }
      } catch(et) {}
      const finalPnl = Number.isFinite(incomePnl) ? incomePnl : (Number.isFinite(tradePnl) ? tradePnl : null);
      let changed=false;
      if (Number.isFinite(finalPnl)) { row.pnlUSDT = safeNum(finalPnl,4); changed=true; }
      if (Number.isFinite(closePx) && closePx>0) { row.closePrice = safeNum(closePx,10); changed=true; }
      if ((!row.quantity || Number(row.quantity) === 1) && Number.isFinite(qty) && qty>0) { row.quantity = safeNum(qty,8); changed=true; }
      if (Number.isFinite(finalPnl)) {
        const margin = Number(row.marginUSDT || autoConfig?.usdtAmount || 0);
        if (margin > 0) row.roiPct = safeNum(finalPnl / margin * 100, 2);
      } else if (row.entryPrice && row.closePrice && row.leverage) {
        const raw = (Number(row.closePrice)-Number(row.entryPrice))/Number(row.entryPrice)*100*(normalizeSide(row.side)==='SHORT'?-1:1);
        row.roiPct = safeNum(raw*Number(row.leverage||1),2);
      }
      if (changed) fixed++;
    } catch(e) { errors++; }
  }
  if (fixed) saveTradeLedger();
  return {ok:true, checked, fixed, errors};
}


async function r174BootstrapLedgerFromIncome(apiKey, apiSecret, lookbackMs=48*60*60*1000) {
  // R174: tradeLedger boşsa reconcile düzeltemez; önce Binance REALIZED_PNL gelirinden minimum ledger kur.
  // Bu, Railway deploy/restart sonrası Telegram document backup geri alınamadığında WR panelinin 0 işlem görünmesini düzeltir.
  if (!apiKey || !apiSecret) return {ok:false, reason:'api_missing'};
  try {
    const startTime = Date.now() - Math.max(60*60*1000, Number(lookbackMs)||48*60*60*1000);
    const endTime = Date.now() + 5000;
    const inc = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/income', {
      incomeType:'REALIZED_PNL', startTime, endTime, limit:1000
    });
    if (!Array.isArray(inc) || !inc.length) return {ok:true, restored:0, source:'income_empty'};
    r176DedupeLedger(250);
    const existing = new Set((Array.isArray(tradeLedger)?tradeLedger:[]).map(r => r176LedgerKey(r)));
    let restored = 0;
    for (const x of inc) {
      const pnl = Number(x.income || 0);
      const t = Number(x.time || x.timestamp || 0);
      const symFull = normalizeSymbol(String(x.symbol || ''));
      if (!symFull || !t || !Number.isFinite(pnl) || Math.abs(pnl) < 0.000001) continue;
      const probe = {symbol:symFull.replace('USDT',''), closedAt:t, openedAt:Math.max(0,t-60*1000), pnlUSDT:pnl};
      const key = r176LedgerKey(probe);
      const id = `RESTORE_${symFull}_${t}_${String(x.tranId||x.info||'')}`;
      if (existing.has(key)) continue;
      existing.add(key);
      const margin = Number(autoConfig?.usdtAmount || 0);
      const row = {
        id,
        symbol: symFull.replace('USDT',''),
        side: 'UNKNOWN',
        status: 'CLOSED',
        openedAt: Math.max(0, t - 60*1000),
        openedAtTR: trTime(Math.max(0, t - 60*1000)),
        closedAt: t,
        closedAtTR: trTime(t),
        entryPrice: null,
        closePrice: null,
        leverage: Number(autoConfig?.leverage || 0) || null,
        quantity: null,
        marginUSDT: margin || null,
        pnlUSDT: safeNum(pnl,4),
        roiPct: margin > 0 ? safeNum(pnl / margin * 100, 2) : null,
        exitReason: 'BINANCE_INCOME_RESTORE',
        exitLabel: 'Binance gelir kaydından geri yüklendi',
        resultNote: 'R174 bootstrap: Railway/Telegram ledger boşken Binance REALIZED_PNL üzerinden oluşturuldu',
        sl:null, tp:null, cooldownMin:null,
      };
      tradeLedger.push(row);
      existing.add(id);
      restored++;
    }
    if (restored) {
      tradeLedger = tradeLedger.sort((a,b)=>Number(b.closedAt||b.openedAt||0)-Number(a.closedAt||a.openedAt||0)).slice(0,250);
      saveTradeLedger();
      try { logAuto(`🧾 R174 ledger bootstrap: Binance income üzerinden ${restored} kapanış geri yüklendi`); } catch(_) {}
    }
    return {ok:true, restored, source:'binance_income', incomeRows:inc.length};
  } catch(e) {
    try { pushCritical('R174_LEDGER_BOOTSTRAP_FAIL', new Error(String(e?.message||e)), {symbol:'PNL'}, 'WARN'); } catch(_) {}
    return {ok:false, error:String(e?.message||e)};
  }
}

let r173LastAutoReconcileAt = 0;
let r173LastAutoReconcileResult = null;
async function r173AutoReconcileTick(force=false) {
  if (!autoConfig?.apiKey || !autoConfig?.apiSecret) return {ok:false, reason:'api_missing'};
  // R176: Income endpoint 429 üretiyordu. Otomatik reconcile 15dk; manuel PnL Düzelt force çalışır.
  if (!force && Date.now() - Number(r173LastAutoReconcileAt||0) < 15*60*1000) return {ok:true, skipped:true, last:r173LastAutoReconcileResult, throttle:'15m'};
  try {
    if (!force && typeof binanceGov === 'object' && Number(binanceGov.backoffUntil||0) > Date.now()) {
      return {ok:true, skipped:true, reason:'BINANCE_BACKOFF_ACTIVE', backoffMs:Number(binanceGov.backoffUntil)-Date.now(), last:r173LastAutoReconcileResult};
    }
  } catch(_) {}
  r173LastAutoReconcileAt = Date.now();
  r176DedupeLedger(250);
  const closedKnown = (Array.isArray(tradeLedger)?tradeLedger:[]).filter(t => t && (t.status==='CLOSED'||t.closedAt) && Number.isFinite(Number(t.pnlUSDT)) && Number(t.pnlUSDT)!==0).length;
  let bootstrap = {ok:true, restored:0, skipped:closedKnown>0};
  // R176: otomatikte sadece ledger boşsa bootstrap; manuel PnL Düzelt'te force bootstrap yapılabilir.
  if (force || closedKnown === 0) bootstrap = await r174BootstrapLedgerFromIncome(autoConfig.apiKey, autoConfig.apiSecret, 48*60*60*1000);
  const rec = await r171ReconcileTradeLedgerPnL(autoConfig.apiKey, autoConfig.apiSecret, 60);
  r176DedupeLedger(250);
  saveTradeLedger();
  r173LastAutoReconcileResult = {ok:!!rec?.ok, bootstrap, reconcile:rec, deduped:true};
  return r173LastAutoReconcileResult;
}

async function r171MaintenanceTick() {
  // Telegram ledger kart poll Binance'e gitmez; 429'dan etkilenmez.
  try { await r171TelegramPollLedger(false); } catch(_) {}
  try { await r173AutoReconcileTick(false); } catch(_) {}
}



// ── R84: 5 DAKİKA UYUMLU BEKLEME SÜRELERİ — botu kilitlemez, tekrar hatayı önler ──
// 90dk 5m scalp için ağırdı. Yeni mantık mum sayısına göre çalışır:
// kâr = 2 mum, manuel/dış kapanış = 3 mum, zarar = 5-7 mum, emir hatası = 2 mum.
const R84_MUM_MS = 5 * 60 * 1000;
const CD_PROFIT_MS  = 10 * 60 * 1000;  // TP / kâr taşıma: 2 mum
const CD_BE_MS      =  8 * 60 * 1000;  // başabaş / küçük koruma kapanışı: yaklaşık 1-2 mum
const CD_MANUAL_MS  = 15 * 60 * 1000;  // kullanıcı/Binance dış kapanış: 3 mum
const CD_LOSS_MS    = 60 * 60 * 1000;  // R149: normal zarar/SL sonrası aynı yön 1 saat; ters yön serbest
const CD_HARD_LOSS_MS = 60 * 60 * 1000; // R149: sert zarar/acil koruma sonrası aynı yön 1 saat; ters yön serbest
const CD_ERR_MS_R25 = 8 * 60 * 1000;  // emir hatası: yaklaşık 1-2 mum
const CD_AFTER_CLOSE_PAUSE_MS = 45 * 1000; // kapanış sonrası aynı tarama döngüsünde yeni emir açma

function entryPatternKeyFromText(txt='') {
  const t = String(txt||'').toLowerCase();
  const parts = [];
  if (t.includes('r134') || t.includes('hızlı edge') || t.includes('fast edge') || t.includes('mikro-scalp')) parts.push('R134_FAST');
  if (t.includes('trend devam')) parts.push('TREND_CONT');
  if (t.includes('tuzak dönüş') || t.includes('counter_trap')) parts.push('COUNTER_TRAP');
  if (t.includes('5m momentum')) parts.push('MOMENTUM');
  if (t.includes('5m akış') || t.includes('flow_scalp')) parts.push('FLOW');
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
  // R135: öğrenme freni artık kör GENERIC değil. Aynı coin + aynı playbook/setup tekrar zarar yazarsa keser;
  // globalde ise ancak aynı setup 3 kez zarar yazarsa devreye girer. Böylece bot boğulmaz ama WLD/OPN boşluğu kapanır.
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


// R33: Binance Top Gainers kilidi — bot artık sadece özel interest sırasına değil,
// Binance Futures 24h değişim listesindeki ilk hareketli coinlere de zorunlu bakar.
// Ek endpoint yok: zaten kullanılan /fapi/v1/ticker/24hr verisinden hesaplanır.
const FUTURES_TICKERS_CACHE_MS = 75 * 1000; // R150: ticker yükünü azalt; TOP10 için yeterince taze
const R33_TOP_GAINER_LOCK_COUNT = 10;
const R33_TOP_GAINER_MIN_QUOTE_VOL = 1_000_000;
const R54_SCAN_MODES = new Set(['FAST6','TOP10','TOP24']);
function normalizeR54ScanMode(v) {
  const raw = String(v || '').toUpperCase();
  if (raw === '6' || raw === 'FAST' || raw === 'FAST6' || raw === 'TOP3_PLUS3') return 'FAST6';
  if (raw === '10' || raw === 'TOP10') return 'TOP10';
  if (raw === '24' || raw === 'TOP24' || raw === 'FULL24') return 'TOP24';
  return 'FAST6';
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
    .filter(c => Number.isFinite(c.change24h) && c.change24h > 0)
    .sort((a,b) => (b.change24h - a.change24h) || (b.volume - a.volume))
    .slice(0, n);
  const out = new Map();
  rows.forEach((c, i) => out.set(c.fullSymbol, {...c, topGainerRank:i+1, topGainerLocked:true}));
  return out;
}

// ── R152: YENİ COİN YAŞ FİLTRESİ ─────────────────────────────────────────────
// Binance Futures'a 15 günden az önce eklenmiş coinler TOP10'dan çıkarılır.
// Listedeki boşluk rank 11, 12, 13... ile doldurulur — toplam işlem potansiyeli korunur.
// onboardDate: /fapi/v1/exchangeInfo yanıtında her sembolün içinde ms cinsinden gelir.
const R152_NEW_COIN_AGE_MS = 15 * 24 * 60 * 60 * 1000; // 15 gün
const R152_EXCHANGE_INFO_CACHE_MS = 6 * 60 * 60 * 1000; // 6 saatte bir yenile

let r152OnboardCache = { ts: 0, map: null };

async function r152GetOnboardDateMap() {
  try {
    const now = Date.now();
    if (r152OnboardCache.map && now - r152OnboardCache.ts < R152_EXCHANGE_INFO_CACHE_MS) {
      return r152OnboardCache.map;
    }
    // Tüm semboller için tek çağrı — weight 1
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
    // exchangeInfo geçici hata verirse mevcut cache döner (yoksa boş Map)
    return r152OnboardCache.map || new Map();
  }
}

// r33TopGainersFromTickers'ın yaş filtreli versiyonu.
// onboardMap yoksa veya boşsa orijinal davranış korunur (hiçbir coin filtrelenmez).
function r152FilterAndExtendGainers(data=[], onboardMap=new Map(), n=R33_TOP_GAINER_LOCK_COUNT) {
  if (!Array.isArray(data)) return new Map();
  const now = Date.now();

  // Tüm adayları sırala (R33 mantığıyla aynı)
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
    .filter(c => Number.isFinite(c.change24h) && c.change24h > 0)
    .sort((a,b) => (b.change24h - a.change24h) || (b.volume - a.volume));

  // Yaş filtresini uygula: yeni coinleri ayır, eskilerden ilerle
  const hasAgeData = onboardMap.size > 0;
  const selected = [];
  const skipped = []; // yeni coin — log için

  for (const c of allCandidates) {
    if (selected.length >= n) break;
    const onboard = onboardMap.get(c.fullSymbol);
    const isNewCoin = hasAgeData && Number.isFinite(onboard) && (now - onboard) < R152_NEW_COIN_AGE_MS;
    if (isNewCoin) {
      const ageDays = Math.floor((now - onboard) / (24*60*60*1000));
      skipped.push({ sym: c.symbol, ageDays });
      continue; // Bu coin'i atla, bir sonraki rank'a geç
    }
    selected.push(c);
  }

  // Log: kaç coin değiştirildi — aynı mesajı 60sn içinde tekrar yazma (scan spam önleme)
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
    const scored = tickers
      .filter(t => t.symbol.endsWith('USDT') &&
        parseFloat(t.quoteVolume) > 20000000 && // min 20M hacim
        parseFloat(t.lastPrice) > 0)
      .map(t => {
        const vol   = parseFloat(t.quoteVolume);
        const count = parseInt(t.count) || 0;
        const high  = parseFloat(t.highPrice);
        const low   = parseFloat(t.lowPrice);
        const last  = parseFloat(t.lastPrice);

        // R28: Yönlü volatilite skoru — düşen bıçak filtresi
        const rawChg = parseFloat(t.priceChangePercent); // işaretli
        const absChg = Math.abs(rawChg);
        // Hacim bazlı baz skor (yön bağımsız)
        const baseVol =
          (absChg > 10 ? 40 : absChg > 5 ? 30 : absChg > 3 ? 20 : absChg > 1 ? 10 : 3) +
          (vol > 1e9 ? 30 : vol > 5e8 ? 20 : vol > 1e8 ? 12 : vol > 5e7 ? 6 : 3) +
          (count > 500000 ? 20 : count > 200000 ? 12 : count > 100000 ? 6 : 2) +
          ((high - low) / last * 100 > 8 ? 10 : (high - low) / last * 100 > 4 ? 5 : 0);
        // Yönlü çarpan: pozitif = tercihli, -%3 altı = cezalı (falling knife)
        const dirMult = rawChg > 3 ? 1.25 : rawChg > 0 ? 1.0 : rawChg > -2 ? 0.85 : rawChg > -5 ? 0.55 : 0.30;
        const volScore = Math.round(baseVol * dirMult);

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
      .slice(0, 40); // R29: Top 40 gerçek hareket havuzu

    volatilityStore.coins = scored;
    volatilityStore.lastUpdate = now;
  } catch(e) {
    console.log('Volatilite scanner hata:', e.message);
  }
}

// Her 3 dakikada bir güncelle
setInterval(scanVolatility, 3 * 60 * 1000);
scanVolatility(); // Hemen başlat

// ── WEBSOCKET BELLEK TEMİZLEME — aktif olmayan streamler kapat ───────────────
// Sürekli çalışan WS'ler bellek dolduruyor; 1 saatte 1 temizle
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

// ── KILL ZONE DEVRE DIŞI ─────────────────────────────────────────────────────
// Kripto 24/7 çalışır. London/NY/Asia etiketleri artık skor, min skor veya tarama
// sayısını değiştirmez. Endpoint sadece geriye uyumluluk için sabit değer döndürür.
function getKillZone() {
  return { zone:'CRYPTO_24_7', strength:1.0, label:'⚡ Kripto 24/7', active:true, disabled:true };
}

app.get('/api/killzone', (req, res) => {
  res.json({ ok:true, ...getKillZone(), time: trTime(), timeUTC: new Date().toUTCString() });
});


// ── R21 ORTAK TARAMA LİSTESİ ────────────────────────────────────────────────
// Long/Short ekranı ile Canlı Auto aynı coin havuzunu kullanır.
// Böylece panelde A-Tier görünen RENDER/ZEC/ALGO gibi coinler auto scanner dışında kalmaz.
function calcScanInterest(c={}) {
  const vol = Number(c.volume || c.quoteVolume || 0);
  const signed = Number(c.change24h ?? c.priceChangePercent ?? 0);
  const absChg = Math.abs(signed);
  const trades = Number(c.trades || c.count || 0);
  const range = Number(c.rangePct || 0);

  // R29: Tarama artık "sadece hacim" değil; gerçek hareket + işlem yoğunluğu + yön bilgisi.
  // Negatif coin taramadan atılmaz; ama LONG için sonra bağlam terazisinde ağır sorgulanır.
  let interest = 0;
  if (vol > 1e9) interest += 42;
  else if (vol > 5e8) interest += 34;
  else if (vol > 1e8) interest += 22;
  else if (vol > 5e7) interest += 14;
  else if (vol > 2e7) interest += 8;
  else interest += 2;

  // Gerçek volatilite: 24h yönlü değişim + gün içi high/low range. Hacim tek başına aday yapamaz.
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

  // Hacim var ama hareket yoksa robot boş coin taramasın.
  if (absChg < 1.0 && range < 2.4) interest -= 14;
  // Pozitif momentum long havuzuna, negatif momentum short havuzuna adaydır; yön kararı analyze tarafında verilir.
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


// ═══════════════════════════════════════════════════════════════════════════════
// R22 — HASSAS TERAZİ KATMANI
// Ek Binance REST çağrısı yapmaz. Sadece zaten hesaplanan analiz/veri sonuçlarını
// öncelik puanına bağlar: sektör rotasyon, sweep cluster, signal decay,
// funding trap, likidasyon şelalesi, ilk/kaç test kalitesi.
// ═══════════════════════════════════════════════════════════════════════════════
const r22AnalysisMemory = new Map(); // symbol -> son analiz özeti
const R22_MEMORY_MS = 12 * 60 * 1000;

function r22BaseSymbol(symbol) {
  return String(symbol || '').toUpperCase().replace(/USDT$/,'').replace(/^1000/,'');
}
function r22SectorOf(symbol) {
  const s = r22BaseSymbol(symbol);
  // R22 BEST: GPT + Claude sektör haritaları birleştirildi — daha geniş kapsam
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

// ── R25: FİTİL DİRENÇ TUZAĞI DEDEKTÖRÜ (VAH/VAL/Liq entegre) ───────────────
// GPT R25'in r25DetectWickTrapMap fonksiyonu: Ham wick sayısına ek olarak
// Volume Profile (VAH/VAL), Premium/Discount ve Liq seviyeleri ile güçlendirme.
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
  // Volume Profile ve P/D entegrasyonu
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


// ═════════════════════════════════════════════════════════════════════════════
// R32: GRAFİK OKUMA KATMANLARI — pure JS, ekstra Binance API yok
// Amaç: 5m fırsatları kaçırmadan, R31 karar mimarisini körleştirmeden pattern/renko
// verilerini sadece AMPLIFIER ve risk filtresi olarak kullanmak.
// ═════════════════════════════════════════════════════════════════════════════
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
      if (lastPrice > c.v * 0.995) { patterns.push({name:'DoubleBottom',str:4,dir:'BULL'}); bullScore += 16; }
      else { patterns.push({name:'DoubleBottomForming',str:2,dir:'BULL'}); bullScore += 6; }
    }
    if (a.type==='L' && b.type==='H' && c.type==='L' && d.type==='H' && near(b.v,d.v) && c.v < Math.min(a.v,d.v)) {
      if (lastPrice < c.v * 1.005) { patterns.push({name:'DoubleTop',str:4,dir:'BEAR'}); bearScore += 16; }
      else { patterns.push({name:'DoubleTopForming',str:2,dir:'BEAR'}); bearScore += 6; }
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


// ── R118: HTF bölgeye özel 5m mum formasyon playbook'u ─────────────────────
// Amaç: 1H/4H BSL/SSL bölgesinde kör ters işlem açmak değil; 5m mumun gerçekten
// reddettiğini/geri aldığını görmek. Bu motor gösterge değil, sadece OHLC gövde-fitil
// matematiği kullanır. R117 ters-köşe hedefinde kanıt kalitesi olarak kullanılır.
function r118AnalyzeCandlePlaybook(klines, dir='LONG', level=null) {
  const out = { ok:false, dir, score:0, strong:false, names:[], trNames:[], reject:false, rejectReason:'', ozet:'formasyon yok' };
  const rows = (Array.isArray(klines) ? klines : []).slice(-14).map(k => ({
    t:+k[0], o:+k[1], h:+k[2], l:+k[3], c:+k[4], v:+k[5]
  })).filter(k => [k.o,k.h,k.l,k.c].every(Number.isFinite) && k.h > 0 && k.l > 0 && k.c > 0);
  if (rows.length < 5) return out;
  const R = (v,d=3) => +Number(v||0).toFixed(d);
  const pctAbs = (a,b) => b > 0 ? Math.abs(a-b)/b*100 : 999;
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

  // Güçlü karşı formasyon varsa o yönü kovalamayı engelle. Bu blok emir öldürücü değil;
  // R117 kanıtını zayıflatır ve loga sebep verir.
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

  // Kontekst kalitesi: formasyon tek başına değil, gövde+fitil kalitesiyle puanlanır.
  if (volBoost) out.score += 1;
  if (impulse0) out.score += 1;
  out.score = Math.min(12, out.score);
  out.strong = out.score >= 8; // R158: strong threshold 7→8 (daha güçlü reversal konfirmasyonu)
  out.ok = out.score >= 6 && !out.reject; // R158: ok threshold 5→6 (yanlış reversal engeli)
  out.trNames = out.names.map(n => trPatternName(n));
  out.ozet = out.ok
    ? `${dir} mum teyidi: ${out.trNames.slice(0,3).join(' + ')} · puan ${out.score}/12 · vol:${volBoost?'VAR':'yok'} impuls:${impulse0?'VAR':'yok'}`
    : `${dir} mum teyidi zayıf: ${out.trNames.slice(0,3).join(' + ') || 'net formasyon yok'} · puan ${out.score}/12${out.rejectReason ? ' · '+out.rejectReason : ''}`;
  return out;
}


// ── R86: FORMASYON ADI TÜRKÇELEŞTİRME ───────────────────────────────────────
// R32 formasyon motoruna dokunmaz; sadece karar/ekran dilini Türkçeleştirir.
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



// ── R128: ANALYZE JSON CIRCULAR-SAFE ÇIKTI ─────────────────────────────────
// R127'de WATCH tarafını taşımak için decisionChain.sideDecisions = {LONG,SHORT}
// eklenmişti. decisionChain zaten LONG veya SHORT nesnesinin kendisi olduğunda bu,
// decisionChain -> sideDecisions -> LONG -> decisionChain döngüsü üretip res.json'u kırıyordu.
// Çözüm: ham referansları decisionChain içine gömmek yok; JSON çıktısı WeakSet ile temizlenir.
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

// ── R120: TEK BEYİN KARAR ÇEKİRDEĞİ ────────────────────────────────────────
// Eski Rxx blokları artık emir vermiyor; onlar yalnızca sensör/veri sağlayıcıdır.
// Son karar burada tek bir beyin tarafından verilir: HTF harita, 5m mum, flow/OI,
// squeeze, trend-devam ve risk tek raporda birleştirilir.
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
  if (flow.length) bits.push(`Akış:${flow.slice(0,5).join(', ')}`);
  if (d.r93PiyasaEtiketi) bits.push(`Zemin:${d.r93PiyasaEtiketi}`);
  return bits.join(' · ');
}
function r120SingleBrainDecision(side, raw={}, sideScore=0, minAutoScore=72) {
  // R121: 5m kaldıraçlı fırsat beyni.
  // Eski Rxx modülleri burada sadece sensör kabul edilir. Nihai emir: tek beyin.
  // Mantık: "kural geçti/kaldı" değil; o anki TOP10 5m koşulunda hangi oyun daha yüksek edge veriyor?
  const d = raw && typeof raw === 'object' ? {...raw} : { pass:false, tier:'WAIT', score:0 };
  // R163: karar nesnesi kendi yönünü açıkça taşısın.
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

  const r125SideFlow = r125FlowForSide(d.r125Flow, side);
  const flowOk = r120Bool(
    flowPts >= 1 || d.r42FlowGate || d.cvdBridgePass || d.r53CvdSmartSafe ||
    d.r45CvdAlternativeOk || d.r117FlowOk || d.r111ObBaskisi || d.microConfirm || r125SideFlow.ok
  );
  // R124: CVD yokken "CVD/flow uygun" diye düşük kaliteli trend devamını açma.
  // CVD yoksa en azından taker/OI/orderbook gibi açık alternatif akış kanıtı gerekir.
  const liveFlowOk = r120Bool(
    (flowOk && !d.cvdMissing) || d.r117FlowOk || d.r111ObBaskisi ||
    Math.abs(r120BrainNum(d.r111OiChgPct, 0)) >= 0.35 || d.r42FlowGate || d.r45CvdAlternativeOk || r125SideFlow.ok
  );
  const timingOk = r120Bool(
    timingPts >= 1 || d.r117TrapSweepTaken || d.r117BodyReclaimOk || d.r118CandleOk ||
    d.r111SiksmaBreakout || d.fresh5mImpulseOrRecent || d.r37EarlyOk || d.directSweepOk
  );
  const contextOk = r120Bool(contextPts >= 2 || d.r93PiyasaIslemYapilabilir || d.r93DalgaliAmaIslemYapilabilir || !d.r88PiyasaBozuk);
  const r125OpposingFlow = r120Bool(r125SideFlow.against >= r125SideFlow.edge + 6 && r125SideFlow.against >= 8);
  // R154: rvolVeryLow toxicFlow/fatalDanger'dan çıkarıldı — rawEdge ceza puanı olarak kalır.
  // Güçlü flow+edge olan coinlerde (BAS, VELVET gibi) düşük RVOL hard-block değil, sadece ceza olmalı.
  const toxicFlow = r120Bool(d.cvdToxic || d.deltaToxic || d.r22LiqWaterfall?.adverse || d.poorLiquidity || r125OpposingFlow);

  // Oyun türleri: Bunlar kapı değil, beynin seçebileceği playbook'lardır.
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
    (d.r74Top10ProScalperOk || d.r68EntryEventOk || d.r67ScalperCoreHuntEntryOk || d.r37EarlyOk || d.fresh5mImpulseOrRecent) &&
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
  // R146: HTF karşı duvar çok yakınken hızlı-edge mikro-scalp kör giriş yapmasın.
  // MON/HMSTR örneği: live delta güçlü görünür ama 15m/1H/4H supply/BSL burnun üstündeyse
  // ve 5m mum formasyonu yoksa bu çoğu zaman breakout değil, ask-wall absorpsiyonu / likidite dağıtımıdır.
  const r146R116CounterDist = r120BrainNum(d.r116CounterLevel?.dist ?? d.r116CounterLevel?.distPct, 999);
  const r146CounterDist = Math.min(counterDist, r146R116CounterDist);
  const r146CandleText0 = String(d.r118CandleOzet || '');
  const r146CandleScore0 = r120BrainNum(d.r118Candle?.score, 0);
  const r146No5mPattern = r120Bool(/formasyon yok/i.test(r146CandleText0) || (!d.r118CandleOk && r146CandleScore0 <= 2));
  const r146HtfCounterNear = r120Bool(r146CounterDist <= 0.70 || (d.r116HtfGuardBlock && !d.r117HtfReverseOk));
  const r146CounterWallPressure = r120Bool(r146HtfCounterNear && !d.r116AcceptedCounterBreak && !d.r117HtfReverseOk && !d.r117BodyReclaimOk);
  const r146LateWallNoPattern = r120Bool(r146CounterWallPressure && r146No5mPattern && !squeeze && !htfReclaim);
  // R134: ölümcül güvenlik ile legacy sensör vetosunu ayır.
  // Eski R65/R68 modülleri artık tek beynin ham sensörüdür; canlı tick + HTF sweep + yüksek edge varsa
  // bu eski modül vetoları hesabı koruyan gerçek risk gibi davranamaz. Fakat likidite/ATR/trap/knife
  // gibi hesabı yakabilecek riskler hâlâ asla bypass edilmez.
  const r134LegacySensorVeto = r120Bool(d.r68CriticalHardBlock || d.r65ScalperCoreHardVeto || d.r66WyckoffHardVeto);
  const fatalDanger = r120Bool(
    d.hardVeto || d.r114TrapBlock ||
    // R155: htfOpposite sadece HTF tam karşı VE mesafe >%0.5 ise blok — %0.02 gibi yakın seviyeler fatalDanger değil
    (htfOpposite && r120BrainNum(d.r116CounterLevel?.dist ?? d.r116CounterLevel?.distPct, 999) > 0.5) ||
    d.poorLiquidity || d.atrExtremeBlock || d.signalDecayAutoBlock ||
    (side === 'LONG' ? d.r41FallingKnifeBlock : d.r41RisingKnifeBlock)
  );
  const hardDanger = r120Bool(
    fatalDanger || toxicFlow ||
    // R155: MOMENTUM_SCALP ve FLOW_SCALP kendi modeQualityBlock kontrollerini yapıyor — piyasaBozuk'tan muaf
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
  candidates.sort((a,b)=>b.base-a.base);
  const primaryMode = candidates[0]?.mode || 'NO_EDGE';
  const playbookActive = primaryMode !== 'NO_EDGE';

  // Fırsat puanı: tek bir beyin, veri kaynağının o an edge üretme gücünü tartar.
  const rawEdge = Math.max(0, Math.min(100, Math.round(
    score*0.32 + priority*0.25 + r47*2.2 + flowPts*5 + timingPts*4 + contextPts*2 + structurePts*2 +
    (htfReverse?20:0) + (htfReclaim?16:0) + (squeeze?15:0) + (trend?12:0) +
    (counterTrap?13:0) + (momentumScalp?11:0) + (flowScalp?9:0) + Math.min(8, r125SideFlow.edge) +
    (d.r125Flow?.r126?.bidAbsorb && side==='LONG' ? 7 : 0) + (d.r125Flow?.r126?.askAbsorb && side==='SHORT' ? 7 : 0) +
    (d.r125Flow?.r126?.forecast?.side === side ? Math.min(4, Math.round(r120BrainNum(d.r125Flow?.r126?.forecast?.confidence,0)/25)) : 0) +
    (d.r125Flow?.r126?.aggressionTrend?.side === side && d.r125Flow?.r126?.aggressionTrend?.phase === 'ACCELERATING' ? Math.min(5, Math.round(r120BrainNum(d.r125Flow?.r126?.aggressionTrend?.strength,0)/18)) : 0) +
    (d.r125Flow?.r126?.deltaImprint?.coiled && (squeeze || flowScalp) ? 3 : 0) + r126PlaybookAdj(primaryMode) +
    // R141: R140 sinyalleri yön-duyarlı. R142 bunları bir de geçmiş sonuç/late-chase ile kalibre eder.
    (d.r140Phase?.phase==='EXPANSION' ? (side==='LONG' ? 8 : -5)
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
    (r146LateWallNoPattern ? -14 : r146CounterWallPressure ? -8 : 0) -
    (hardDanger?55:0) - (d.cvdMissing?5:0) - (r125OpposingFlow?12:0)
  )));
  const r142Cal = r142CalibrateEdge(side, d, primaryMode, rawEdge, score);
  let edge = r142Cal.calibratedEdge;

  // 5m kaldıraçlı TOP10 için panel skoru tek başına amir değil. Temiz playbook + edge varsa floor esner.
  // R155: adaptiveFloor gevşetildi — COUNTER_TRAP/MOMENTUM/FLOW için daha erişilebilir.
  // 5m TOP10 scalp'te score 51-58 arası kalite sinyaller fırsat; katı floor sık işlemi engelliyordu.
  const adaptiveFloor = playbookActive
    ? (['HTF_REVERSAL','HTF_RECLAIM','SQUEEZE_BREAKOUT'].includes(primaryMode)
        ? Math.max(40, minScore - 32)
        : primaryMode === 'TREND_CONTINUATION'
          ? Math.max(52, minScore - 18)
          : Math.max(50, minScore - 20))
    : Math.max(44, minScore - 16);
  const needsPremiumProof = ['TREND_CONTINUATION','MOMENTUM_SCALP','FLOW_SCALP'].includes(primaryMode);
  // R158: modeQualityBlock — COUNTER_TRAP için r125 canlı akış teyidi eklendi.
  // ZEC/FOLKS COUNTER_TRAP analiz hatası: bot karşı akış varken tuzak dönüşü açıyordu.
  // Artık COUNTER_TRAP için r125SideFlow.ok (aynı yön canlı orderflow) veya deltaOkStrict zorunlu.
  // R158b: deltaOkStrict brain scope'unda yok — d.r125Flow.deltaPct'den hesapla
  const r158DeltaOk = r120Bool(
    side === 'LONG' ? r120BrainNum(d.r125Flow?.deltaPct, 0) >= 15 : r120BrainNum(d.r125Flow?.deltaPct, 0) <= -15
  );
  const r158CounterTrapFlowOk = r120Bool(r125SideFlow.ok || r125SideFlow.strong || r158DeltaOk);
  const modeQualityBlock = r120Bool(
    (needsPremiumProof && htfCounterWait) ||
    (primaryMode === 'COUNTER_TRAP' && htfCounterWait && counterDist <= 1.0 && !d.r117HtfReverseOk) ||
    // R158: COUNTER_TRAP için canlı akış YOKsa blok (karşı akışla tuzak dönüşü açma)
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
  // R144: hızlı-edge mikro-scalp gerçek scalptir, HTF ters-köşe yerine geçemez.
  // HMSTR örneğinde olduğu gibi 1H/4H karşı seviye burnun üstündeyken, mum hacim teyidi yoksa
  // sadece live delta + rawEdge 100 ile LONG açmak geç giriş / dağıtım riski üretir.
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

  // R147: ROBO tipi hata freni.
  // Örnek: LONG seçiliyor ama aynı anda 5m formasyon yok + HTF karşı seviye %0.3 civarı +
  // R125 akış SHORT + mumTahmin SHORT + zemin BOZUK. Bu artık "tuzak dönüşü" değil,
  // karşı duvarda geç alıcıların absorbe edilmesidir. İşlem sayısını öldürmez; aynı şartta
  // SHORT tarafı temizse onu seçmeye izin verir, yalnızca yanlış yöne LONG/SHORT açmayı keser.
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

  // R148: LONG/SHORT eşit terazi.
  // Top10 gainer listesi doğal olarak LONG skorlarını şişirir. Bu yüzden final beyin artık
  // önce LONG düşünmez; iki yönü aynı kuralla tartar. Eğer LONG tarafı HTF direnç/BSL/supply
  // dibinde ve canlı akış SHORT diyorsa bu LONG fırsatı değil, SHORT/izleme fırsatıdır.
  // Simetrik olarak dipte SHORT da aynı şekilde kesilir.
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
  // R142: kalibrasyon hard block değildir. Raw edge çok güçlü ve taze akış varsa işlem sayısı potansiyeli korunur;
  // fakat aynı anda late-chase/live-opposite varsa bu koruma devreye girmez.
  const r142FrequencySafeEdge = r120Bool(rawEdge >= edgeNeed + 8 && !r142Cal.late && !r142Cal.liveAgainst && r134FlowAligned && r134HtfOrCandleProof && !fatalDanger && !r147NoProofCounterTrap && !r148WrongSideBlock);
  const passEdge = r142FrequencySafeEdge ? Math.max(edge, edgeNeed) : edge;
  // R156: TOP10 5m Hızlı Bypass — güçlü flow + yeterli edge + score varsa karmaşık katmanları atla.
  // Amaç: BABY L11/S0 delta>30%, edge>40, score>44 gibi gerçek sinyaller hiçbir katmandan geçemediğinde
  // direkt TRADE kararı ver. Sadece gerçek tehlikeler (poorLiquidity, atrExtremeBlock, r148WrongSideBlock) engeller.
  const r156RealHardBlock = r120Bool(
    d.poorLiquidity || d.atrExtremeBlock ||
    (side === 'LONG' ? d.r41FallingKnifeBlock : d.r41RisingKnifeBlock) ||
    d.hardVeto
  );
  // R157: R156 bypass sıkılaştırıldı — BABY/FOLKS likidasyon analizi:
  // 1. Kalibrasyon datası < 2 tamamlanan işlem → bypass KAPALI (edge 100 = veri yok = güvenilmez)
  // 2. score eşiği minScore-32 → minScore-20 (daha seçici)
  // 3. Tek yön flow eşiği: edge>=8 → edge>=10, count>=15 → count>=25
  // 4. r147 tuzak freni bypass'ı engelliyor: r147NoProofCounterTrap=true ise bypass KAPALI
  const r156CalibrationOk = r120Bool(r142Cal.mem.same.n >= 2);
  const r156FastTop10Bypass = r120Bool(
    !r156RealHardBlock && !r148WrongSideBlock && !r147NoProofCounterTrap &&
    r156CalibrationOk &&                  // en az 2 geçmiş işlem (edge 100 yeni coin engeli)
    r125SideFlow.edge >= 10 &&            // daha güçlü tek yön orderflow
    edge >= 55 &&                         // calibrated edge daha yüksek
    score >= Math.max(45, minScore - 20) && // score daha seçici
    primaryMode !== 'NO_EDGE' &&
    r133LiveTradeCount >= 25              // daha fazla canlı tick teyidi
  );

  // ── R160: TRADER DECISION — Puansız, 4 soru, trader mantığı ─────────────────
  // "Şu an ne görüyorum?" — Her soru kendi içinde akıllıca. 3/4 = TRADE.
  // Araştırma: HaasOnline, TradingView Scalp Signal Bot, CVD+OI+structure üçlüsü.

  const ictTxt = String(d.r110ICT?.ictState || d.r110ICT?.dashboardText || '');

  // SORU 1 — YAPI: Fiyat kritik seviyede mi ve yön uyumlu mu?
  const r160Q1Structure = r120Bool(
    // SSL/BSL sweep olmuş veya yakın — tam ICT setup
    (side === 'LONG'
      ? /SSL_ALINDI|HTF_SSL|SSL_SWEEP|SSL.*BODY_RECLAIM/i.test(ictTxt)
      : /BSL_ALINDI|HTF_BSL|BSL_SWEEP|BSL.*BODY_RECLAIM/i.test(ictTxt)) ||
    // Sweep bölgesinde (5m mum izleme)
    (side === 'LONG'
      ? /HTF_SSL_SEVIYESINDE/i.test(ictTxt)
      : /HTF_BSL_SEVIYESINDE/i.test(ictTxt)) ||
    // r117 trap seviyesine yakın
    r120Bool(d.r117NearTrapHTF) ||
    // HTF engel yok + ters bölge yok
    (!d.r116HtfGuardBlock && !d.tersBolge)
  );

  // SORU 2 — AKIŞ: Canlı piyasa bu yönde mi?
  const r160LiveDeltaOk = r120Bool(
    side === 'LONG'
      ? r120BrainNum(d.r125Flow?.deltaPct, 0) >= 15
      : r120BrainNum(d.r125Flow?.deltaPct, 0) <= -15
  );
  const r160FlowNotAgainst = r120Bool(r125SideFlow.against < r125SideFlow.edge + 5);
  // r160Q2Flow: R161'de Q4 bloğunda yeniden tanımlandı (delta teyidiyle güçlendirildi)

  // SORU 3 — MOMENTUM: Coin'in döngüsü ve hacmi uygun mu?
  const r160Phase = String(d.r140Phase?.phase || '').toUpperCase();
  const r160OiPos = r120Bool(d.r140OiVel && !d.r140OiVel.fakePump && r120BrainNum(d.r140OiVel?.velocity, 0) > 0);
  const r160RvolOk = r120Bool(r120BrainNum(d.r140Rvol?.ratio, 0) >= 0.45);
  const r160Q3Momentum = r120Bool(
    r160Phase === 'EXPANSION' ||
    r160Phase === 'ACCUMULATION' ||
    (r160Phase === 'TRANSITION' && r160OiPos) ||
    (r160RvolOk && r160OiPos) ||
    r160RvolOk   // minimum hacim var
  );

  // SORU 4 — KANIT: R161: Güçlendirildi — tek kanıt yetmiyor, delta teyidi gerekli
  // FOLKS/ZEC analizi: sweep var ama delta karşıydı → yanlış giriş
  // Şimdi: sweep + delta uyum VEYA candle + body reclaim VEYA güçlü forecast
  const r161DeltaConfirm = r120Bool(
    side === 'LONG'
      ? r120BrainNum(d.r125Flow?.deltaPct, 0) >= 10 || r125SideFlow.strong
      : r120BrainNum(d.r125Flow?.deltaPct, 0) <= -10 || r125SideFlow.strong
  );
  const r160ForecastOk = r120Bool(
    // R161 FIX: forecastDir d.r125Flow.r126.forecast.side üzerinden geliyor
    d.r125Flow?.r126?.forecast?.side === side ||
    (side === 'LONG' ? r120BrainNum(d.r125Flow?.r126?.forecast?.confidence, 0) >= 65 && r120BrainNum(d.r125Flow?.deltaPct, 0) >= 5
                     : r120BrainNum(d.r125Flow?.r126?.forecast?.confidence, 0) >= 65 && r120BrainNum(d.r125Flow?.deltaPct, 0) <= -5)
  );
  const r160Q4Proof = r120Bool(
    // Sweep + delta teyidi = güçlü kanıt
    (r120Bool(d.r117TrapSweepTaken) && r161DeltaConfirm) ||
    // Candle formasyon + body reclaim = yapısal kanıt
    (r120Bool(d.r118CandleOk) && (r120Bool(d.r114ReclaimOk) || r120Bool(d.r117BodyReclaimOk))) ||
    // Sadece güçlü candle (strong = 8/12 puan)
    r120Bool(d.r118CandleStrong) ||
    // Sweep alındı + HTF reclaim
    (r120Bool(d.r117TrapSweepTaken) && r120Bool(d.r117BodyReclaimOk)) ||
    // Güçlü forecast + canlı tick + delta
    (r160ForecastOk && r133LiveTradeCount >= 25 && r161DeltaConfirm)
  );

  // R161: Q2 akış güçlendirildi — delta veya strong flow zorunlu
  // Sadece edge>=5 yetmez, delta veya güçlü orderbook teyidi lazım
  const r160Q2Flow = r120Bool(
    r160FlowNotAgainst && (
      r125SideFlow.strong ||                          // çok güçlü tek yön
      (r125SideFlow.edge >= 5 && r161DeltaConfirm) || // flow + delta birlikte
      (r125SideFlow.edge >= 8)                         // çok güçlü tek yön orderbook
    )
  );

  // R160 gerçek tehlike: SADECE bunlar engeller
  const r160HardBlock = r120Bool(
    d.poorLiquidity ||
    d.atrExtremeBlock ||
    (side === 'LONG' ? d.r41FallingKnifeBlock : d.r41RisingKnifeBlock) ||
    (d.hardVeto && !d.r117HtfReverseOk) ||
    r148WrongSideBlock
  );

  // Kaç soru TRUE?
  const r160TrueCount = [r160Q1Structure, r160Q2Flow, r160Q3Momentum, r160Q4Proof].filter(Boolean).length;
  
  // R169: R160 3/4 kapısı sıkılaştırıldı.
  // Canlı sonuçta JTO/B/PORTAL gibi 3/4 ama ivme veya kanıt eksiği olan LONG'lar büyük SL yazdı.
  // Artık 3/4 sadece AKIŞ+KANIT+ya İVME ya çok güçlü edge varsa geçer; 4/4 ise yine çalışır ama min kalite ister.
  const r169R160FullProof = r120Bool(
    r160TrueCount >= 4 &&
    r125SideFlow.edge >= 5 &&
    edge >= 60 &&
    score >= Math.max(42, minScore - 30) &&
    r133LiveTradeCount >= 15
  );
  const r169R160StrictThree = r120Bool(
    r160TrueCount >= 3 &&
    r160Q2Flow && r160Q4Proof &&
    (r160Q3Momentum || edge >= 90) &&
    r125SideFlow.edge >= 7 &&
    edge >= 72 &&
    score >= Math.max(50, minScore - 22) &&
    r133LiveTradeCount >= 25
  );
  const r160TraderDecision = r120Bool(
    !r160HardBlock && (r169R160FullProof || r169R160StrictThree)
  );

  // R159: MOMENTUM PASS — araştırma sonucu: 5m scalp botlarda yüksek win rate için
  // momentum + volume + structure üçlüsü yeterli. Tüm karmaşık katmanlar bypass.
  // 5 puandan 4'ü varsa VE gerçek tehlike yoksa → TRADE.
  // Kaynak: HaasOnline %65-75 WR, CVD+OI+delta üçlüsü, PSAR+ADX kombinasyonu.
  const r159Points = (
    (r125SideFlow.edge >= 6 ? 2 : r125SideFlow.edge >= 3 ? 1 : 0) +        // canlı orderflow
    (edge >= 60 ? 2 : edge >= 45 ? 1 : 0) +                                  // calibrated edge
    (r120BrainNum(d.r140Rvol?.ratio, 0) >= 0.6 ? 1 : 0) +                   // RVOL yeterli
    (d.r140OiVel && !d.r140OiVel.fakePump && r120BrainNum(d.r140OiVel?.velocity,0) > 0 ? 1 : 0) + // OI pozitif
    (r160ForecastOk ? 1 : 0) +                                                 // mum tahmini aynı yön (R163: doğru forecast kaynağı)
    (score >= minScore - 28 ? 1 : 0) +                                        // score kabul edilebilir
    (primaryMode !== 'NO_EDGE' ? 1 : 0) +                                     // bir playbook var
    (r133LiveTradeCount >= 20 ? 1 : 0)                                        // canlı tick teyidi
  );
  // R168: R159 artık "6 puan gördüm, açayım" kadar gevşek değil.
  // JTO örneği: R159 6p + score 35 ile LONG açıp -13.4% ROI yazdı.
  // Çözüm: 6p sinyal yalnızca ek kanıt varsa geçer; score çok düşükse 7+ puan ister.
  // İşlem sıklığını boğmamak için 8-9p güçlü momentumlar aynen geçer.
  const r168R159WeakSix = r159Points === 6;
  const r168R159ScoreFloorOk = score >= 45 || r159Points >= 7;
  const r168R159ProofOk = r159Points >= 7 || (r160TrueCount >= 3 && r160Q2Flow && r160Q4Proof && r125SideFlow.edge >= 8);
  const r159MomentumPass = r120Bool(
    r159Points >= 6 &&
    r168R159ScoreFloorOk &&
    r168R159ProofOk &&
    !r156RealHardBlock &&
    !r148WrongSideBlock &&
    r125SideFlow.edge >= 5 &&   // minimum tek yön akış zorunlu
    edge >= 40 &&               // minimum edge zorunlu
    r133LiveTradeCount >= 15    // minimum canlı tick
  );

  const ok = r120Bool(
    (!hardDanger && !modeQualityBlock && !r147NoProofCounterTrap && !r148WrongSideBlock && dataMinimum && r148ScoreOk && passEdge >= edgeNeed) ||
    (r133FastScalpOverride && !r147NoProofCounterTrap && !r148WrongSideBlock) ||
    r156FastTop10Bypass ||
    r159MomentumPass ||
    r160TraderDecision
  );

  const sensorSummary = r120BrainSensorSummary(d);
  const modeLabel = r120BrainModeLabel(primaryMode);
  const r142Txt = `raw:${rawEdge} kalibre:${edge}${r142Cal.notes.length ? ' · '+r142Cal.notes.slice(0,3).join(',') : ''}`;
  const r133FastScalpWhy = r133FastScalpOverride
    ? `R144 hızlı 5m scalp: HTF/mum kanıtı + canlı tick ${r133LiveTradeCount} trade + delta ${r133LiveDeltaAbs.toFixed(1)}% + edge ${edge}`
    : '';
  const r144WatchExtra = r148Note ? ` · ${r148Note}` : (r147TrapGuardReason ? ` · ${r147TrapGuardReason}` : (r144FastBlockReason ? ` · ${r144FastBlockReason}` : ''));
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
  d.r160Q2Flow = r160Q2Flow;
  d.r160Q3Momentum = r160Q3Momentum;
  d.r160Q4Proof = r160Q4Proof;
  d.r169R160FullProof = r169R160FullProof;
  d.r169R160StrictThree = r169R160StrictThree;
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
  d.entryPermissionReason = ok ? (r148ReversalSideOk ? 'R148_BALANCED_TRAP_INVERSION' : (r133FastScalpOverride ? 'R135_FAST_EDGE_PASS' : `R121_SINGLE_BRAIN_${primaryMode}`)) : 'R121_SINGLE_BRAIN_WATCH';
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


// ── R37: EARLY MOVE CAPTURE + NO LATE CHASE ────────────────────────────────
// Amaç: 5m hareket bitmeye yaklaşırken market emir kovalamayı engellemek,
// fakat ilk kırılım / ilk retest / taze impuls geldiğinde botu kurallarla kör etmemek.
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
  const nearUpperTarget = distHigh < Math.max(0.28, atr*0.28) || rangePos > 0.86 || (vpvr1h?.vah && Math.abs(lp-Number(vpvr1h.vah))/lp*100 < Math.max(0.35, atr*0.35));
  const nearLowerTarget = distLow  < Math.max(0.28, atr*0.28) || rangePos < 0.14 || (vpvr1h?.val && Math.abs(lp-Number(vpvr1h.val))/lp*100 < Math.max(0.35, atr*0.35));
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


// ── R39: 5M S/R + DAILY PIVOT + PDH/PDL KURUMSAL SCALP KATMANI ─────────────
// Amaç: 5m market girişinde fiyatın hedefe çok yakın olup olmadığını anlamak.
// ID SHORT gibi PDL / local low dibinde short kovalamayı engeller;
// BILL benzeri taze impuls + hedeften uzak long'u ise boğmaz.
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
    const d = new Date(r.t || Date.now()).toISOString().slice(0,10); // Binance UTC günü
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
      // Mitigasyon: fiyat OB içine birkaç kere döndüyse ama altına sert kırmadıysa hâlâ aktif say.
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
    if (r.h>recent[i-1].h && r.h>recent[i-2].h && r.h>recent[i+1].h && r.h>recent[i+2].h) swingHighs.push({type:'5M_SWING_HIGH', price:r39Round(r.h), strength:1, age:recent.length-1-i});
    if (r.l<recent[i-1].l && r.l<recent[i-2].l && r.l<recent[i+1].l && r.l<recent[i+2].l) swingLows.push({type:'5M_SWING_LOW', price:r39Round(r.l), strength:1, age:recent.length-1-i});
  }
  const supports=[], resistances=[];
  const addS=(type, price, strength=1, extra={})=>{ price=Number(price); if(price>0 && price<lp*1.003) supports.push({type, price:r39Round(price), distPct:r39Round((lp-price)/lp*100,2), strength, ...extra}); };
  const addR=(type, price, strength=1, extra={})=>{ price=Number(price); if(price>0 && price>lp*0.997) resistances.push({type, price:r39Round(price), distPct:r39Round((price-lp)/lp*100,2), strength, ...extra}); };
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

async function getUnifiedScanCandidates(limit=6, mode='FAST6') {
  // R54: Pro scalper tarama modu. FAST6 = Top Gainers ilk 10 içinden en volatil 3 + top10'a girmeye aday 3.
  // TOP10 = Binance Futures Top Gainers ilk 10. TOP24 = eski geniş havuz.
  const scanMode = normalizeR54ScanMode(mode || limit);
  const lim = r54ScanLimitForMode(scanMode, limit);
  const EXCL = new Set(['BTCUSDT','ETHUSDT','BNBUSDT','XRPUSDT','SOLUSDT','ADAUSDT',
    'DOGEUSDT','DOTUSDT','MATICUSDT','LTCUSDT','TRXUSDT','AVAXUSDT','LINKUSDT','UNIUSDT','WBTCUSDT','SHIBUSDT']);

  const merged = new Map();
  let topLocked = new Map();
  try {
    // R33: 15dk cache 5m bot için çok bayattı; 60sn cache tek endpoint ile güncel Top Gainers sağlar.
    const data = await cached('futures_tickers', FUTURES_TICKERS_CACHE_MS, () => bPub('/fapi/v1/ticker/24hr'));
    // R152: Yaş filtreli versiyon — 15 günden genç coinler yerine rank 11/12/13... eklenir
    const onboardMap = await r152GetOnboardDateMap();
    topLocked = r152FilterAndExtendGainers(data, onboardMap, R33_TOP_GAINER_LOCK_COUNT);
    if (Array.isArray(data)) {
      for (const t of data) {
        if (!String(t.symbol||'').endsWith('USDT') || EXCL.has(String(t.symbol))) continue;
        const c = normalizeTickerToCoin(t);
        // Normal havuz halen 20M üstü; ama R33 Top Gainers kilidi 1M+ coinleri ayrıca ekler.
        if (c.volume <= 20000000 || !c.price) continue;
        c.source = 'futures';
        const tg = topLocked.get(c.fullSymbol);
        merged.set(c.fullSymbol, tg ? {...c, ...tg, source:'futures+top_gainers_lock'} : c);
      }
    }
  } catch(e) {
    console.log('[R33_SCAN] futures_tickers/top_gainers alınamadı:', e.message);
  }

  // Binance Top Movers ilk 10: hacmi düşük olsa bile analiz listesine girer; emir yine RVOL/spread/ATR kapılarından geçer.
  for (const [sym, c] of topLocked.entries()) {
    const old = merged.get(sym) || {};
    merged.set(sym, { ...old, ...c, source: old.source ? old.source + '+top_gainers_lock' : 'top_gainers_lock' });
  }

  // Volatilite motorundaki coinleri de aynı havuza kat; aynı sembol varsa volScore/range bilgisini zenginleştir.
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

  // Duplicate koruma: FAST6/TOP10/TOP24 sırasını koru.
  const seen = new Set();
  const ordered = [];
  for (const c of rawOrdered) {
    const key = c.fullSymbol;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    ordered.push(c);
    if (ordered.length >= lim) break;
  }
  return ordered;
}

app.get('/api/scan-candidates', async (req, res) => {
  try {
    // R55: Long/Short ekranı ile Otomatik İşlem paneli aynı tarama modunu görsün.
    // Eski kod limit=40 gönderildiğinde modu yanlışlıkla FAST6'a düşürüyordu.
    const serverMode = autoConfig?.scanMode || autoScanState?.settings?.scanMode || autoScanState?.scanMode || 'FAST6';
    const mode = normalizeR54ScanMode(req.query.mode || req.query.scanMode || serverMode);
    const limit = r54ScanLimitForMode(mode, req.query.limit || r54ScanLimitForMode(mode, 6));
    const coins = await getUnifiedScanCandidates(limit, mode);
    res.json({ ok:true, count:coins.length, limit, scanMode:mode, coins, source:'R55_SYNCED_SCANMODE_LIST' });
  } catch(e) { res.status(400).json({ ok:false, error:e.message }); }
});

// ── COIN LİSTESİ ──────────────────────────────────────────────────────────────
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

// ── PRO ANALİZ ────────────────────────────────────────────────────────────────
app.get('/api/analyze/:symbol', async (req, res) => {
  const sym  = req.params.symbol.toUpperCase();
  const full = sym.endsWith('USDT') ? sym : sym+'USDT';

  try {
    // WS stream'leri başlat
    startCVDStream(full);
    startIcebergStream(full);
    // tickStream analyze'da await ile çağrılıyor

    const [r4h,r1h,r15m,r5m,rFunding,rOIHist,rLS_global,rLS_top,rDepth,rTaker,rOIHist5m,rOINow,rBtc5m] =
      await Promise.allSettled([
        cached(`k4h_${full}`,  30*60*1000, ()=>bPub('/fapi/v1/klines',`symbol=${full}&interval=4h&limit=200`)),
        cached(`k1h_${full}`,   5*60*1000, ()=>bPub('/fapi/v1/klines',`symbol=${full}&interval=1h&limit=200`)),
        cached(`k15m_${full}`, 90*1000, ()=>bPub('/fapi/v1/klines',`symbol=${full}&interval=15m&limit=200`)),
        cached(`k5m_${full}`,  45*1000, ()=>bPub('/fapi/v1/klines',`symbol=${full}&interval=5m&limit=100`)),
        cached(`fund_${full}`, 30*60*1000, ()=>bPub('/fapi/v1/fundingRate',`symbol=${full}&limit=10`)),
        cached(`oih_${full}`,  15*60*1000, ()=>bPub('/futures/data/openInterestHist',`symbol=${full}&period=1h&limit=24`)),
        cached(`lsg_${full}`,  15*60*1000, ()=>bPub('/futures/data/globalLongShortAccountRatio',`symbol=${full}&period=1h&limit=12`)),
        cached(`lst_${full}`,  15*60*1000, ()=>bPub('/futures/data/topLongShortPositionRatio',`symbol=${full}&period=1h&limit=12`)),
        cached(`dep_${full}`,  60*1000, ()=>bPub('/fapi/v1/depth',`symbol=${full}&limit=100`)),
        cached(`tak_${full}`,  5*60*1000, ()=>bPub('/futures/data/takerlongshortRatio',`symbol=${full}&period=5m&limit=6`)),
        cached(`oih5_${full}`, 90*1000, ()=>bPub('/futures/data/openInterestHist',`symbol=${full}&period=5m&limit=12`)),
        cached(`oin_${full}`,  60*1000, ()=>bPub('/fapi/v1/openInterest',`symbol=${full}`)),
        // R153: btc5m paralel çekilir — seri await kaldırıldı
        cached('btc5m_r29_ctx', 45*1000, () => bPub('/fapi/v1/klines', `symbol=BTCUSDT&interval=5m&limit=24`)),
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
    const takerArr = rTaker?.status==='fulfilled'&&Array.isArray(rTaker.value)?rTaker.value:[];
    const oiHist5m = rOIHist5m?.status==='fulfilled'&&Array.isArray(rOIHist5m.value)?rOIHist5m.value:[];
    const oiNowObj = rOINow?.status==='fulfilled'&&rOINow.value?rOINow.value:null;

    const lastPrice = k5m.length?parseFloat(k5m[k5m.length-1][4]):(k15m.length?parseFloat(k15m[k15m.length-1][4]):0);
    const lastTime  = k15m.length?parseInt(k15m[k15m.length-1][6]):Date.now();

    // R38: Top Gainers / Top Mover bağlamı. Ek endpoint yok; getUnifiedScanCandidates ile aynı
    // futures_tickers cache anahtarı kullanılır. Amaç 5m top mover coinlerde Fear/Retest cezasının
    // botu körleştirmesini önlemek, fakat geç chase hard-veto'yu korumak.
    let r38MarketCtx = { topMover:false, topRank:null, change24h:0, volume:0, fearSoft:true };
    try {
      const _tickers38 = await cached('futures_tickers', FUTURES_TICKERS_CACHE_MS, () => bPub('/fapi/v1/ticker/24hr'));
      if (Array.isArray(_tickers38)) {
        // _top38: sadece topGainerRank/topMover tespiti için — yaş filtresi + log burada gereksiz
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
    // R143: atrPct R140/R142 modüllerinden önce hesaplanmalı.
    // Önceki R142'de r140PumpPhase(k5m, atrPct) satırı atrPct tanımından önce çalışıyor,
    // bu yüzden analyze içinde "Cannot access 'atrPct' before initialization" hatası veriyordu.
    const atrPct=lastPrice>0?(atr1h/lastPrice)*100:1;
    const vwap1h=vwap(k1h),vwap4h=vwap(k4h);
    const bb1h=bollinger(k1h),bb15m_=bollinger(k15m);

    // R29: BTC göreli güç bağlamı. R153: artık Promise.allSettled içinde paralel çekildi (rBtc5m).
    let btc5mCtx = { ok:false, change15m:0, change60m:0, dropping:false, bouncing:false, redCandles:0 };
    try {
      const btc5m = rBtc5m?.status==='fulfilled'&&Array.isArray(rBtc5m.value)?rBtc5m.value:null;
      if (btc5m && btc5m.length >= 13) {
        const bLast = Number(btc5m.at(-1)[4]);
        const b3 = Number(btc5m.at(-4)[1]);
        const b12 = Number(btc5m.at(-13)[1]);
        // R140: BTC art arda kırmızı mum sayısı
        let redCount = 0;
        for (let i = btc5m.length-1; i >= Math.max(0,btc5m.length-5); i--) {
          if (Number(btc5m[i][4]) < Number(btc5m[i][1])) redCount++;
          else break;
        }
        const chg15 = b3>0 ? +(((bLast-b3)/b3)*100).toFixed(3) : 0;
        const chg60 = b12>0 ? +(((bLast-b12)/b12)*100).toFixed(3) : 0;
        // BTC bounce: son mum yeşil + önceki mum kırmızıydı
        const lastGreen  = Number(btc5m.at(-1)[4]) > Number(btc5m.at(-1)[1]);
        const prevRed    = Number(btc5m.at(-2)[4]) < Number(btc5m.at(-2)[1]);
        btc5mCtx = {
          ok:true,
          change15m: chg15,
          change60m: chg60,
          redCandles: redCount,
          dropping: redCount >= 3 && chg15 < -0.3,  // BTC 3+ kırmızı ve -%0.3 altı
          bouncing:  lastGreen && prevRed && chg15 > -0.5, // BTC toparlanıyor
        };
      }
    } catch(_) {}

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
    // R110: ICT Likidite Sweep Motoru — SSL/BSL + ChoCH + FVG sıralı kontrol
    const r110ICT = r110AnalyzeICT(k5m, k15m, k1h, k4h, lastPrice);
    // R111: Sıkışma + Squeeze Flow — mevcut verileri kullanır, sıfır ek çağrı

    // R140: Pump Cycle + Equal H/L + OI Velocity + BTC Divergence + Coin RVOL
    const r140Phase  = r140PumpPhase(k5m, atrPct);
    const r140EqHL   = r140EqualLevels(k5m, k1h, lastPrice);
    const r140OiVel  = r140OiVelocity(oiHist5m, lastPrice, k5m.length>=2?k5m.at(-2)[4]:lastPrice);
    const _coin15m   = (k5m.length>=4&&Number(k5m.at(-4)[1])>0) ? ((lastPrice-Number(k5m.at(-4)[1]))/Number(k5m.at(-4)[1])*100) : 0;
    const _coin60m   = (k5m.length>=13&&Number(k5m.at(-13)[1])>0) ? ((lastPrice-Number(k5m.at(-13)[1]))/Number(k5m.at(-13)[1])*100) : 0;
    const r140BtcDiv = r140BtcDivergence(btc5mCtx, _coin15m, _coin60m);
    const r140Rvol   = r140CoinRvol(k5m);
    const r111Siksma = r111AnalyzeSiksmaFlow(k5m, k1h, k4h, lastPrice, fundArr, lsGlobal, lsTop, takerArr, oiHist5m, oiNowObj);

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
    r130StartCombinedAggTradeStream([full]); // R130: tek combined aggTrade stream, ayrı WS bekleme yok
    const tickEng = r130EnsureTickEngine(full);
    if (tickEng && k5m.length > 0) {
      updateSwingLevels(tickEng.sweepDet, k5m); // Dinamik lookback, volatiliteye göre
    }
    const tickData = getTickAnalysis(full); // Tek referans, hem AMD hem skor için
    const amd5m = detectAMD(k5m, k15m, tickData?.tickSweep);

    // R32: HA + Candle + ZigZag/Chart + Harmonic + Renko katmanları
    // Ek Binance çağrısı yok; mevcut k5m/k1h/k4h + atr1h üzerinden hesaplanır.
    const _r32AtrPct = lastPrice > 0 ? (atr1h / lastPrice) * 100 : 1;
    const _cdl5m = cdlPatterns(k5m);
    const _cdl1h = cdlPatterns(k1h);
    const _ha5m  = haSignal(calcHeikinAshi(k5m));
    const _ha1h  = haSignal(calcHeikinAshi(k1h));
    const _chart = detectChartPatterns(k1h, lastPrice, _r32AtrPct);
    const _harm  = detectHarmonicPatterns(k4h, lastPrice);
    const _renko = renkoSignal(k1h, lastPrice, atr1h);

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


    // ── R22 SİNYAL YAŞI / İLK TEST ÖLÇÜMÜ ──────────────────────────────────
    // Bu fonksiyonlar yeni REST çağrısı yapmaz. Var olan sweep/tick/AMD bilgisinin
    // tazeliğini hesaplar. Bayat sweep A/B+ otomatik hakkını düşürür.
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
      // R60-FIX2: Bayat sweep ama RVOL güçlüyse skor çarpanı daha az agresif
      // RVOL 5.58x ile 0.45 çarpanı 138→62 yapıyordu (72 altına düşürüyor).
      // Ceza korunuyor ama skor minScore altına baskılanmıyor.
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
    // /api/analyze içinde aşağıdaki skor/response blokları liqData kullanıyor.
    // Önceki build'de bu değişken tanımlanmadığı için tüm analizler ReferenceError ile ERR oluyordu.
    const liqData = getLiqData(full);
    const r125Flow = r125BuildOrderflowContext(full, lastPrice, depth, tickData, liqData);

    // ── COINGLASS LİKİDATE (R153: non-blocking — scan başında prefetch edildi, cache'den oku) ──
    // Prefetch tamamlandıysa anlık döner, tamamlanmadıysa null — analiz bloklanmaz.
    const cgData = (cache.has(`cg_${full}`) ? cache.get(`cg_${full}`)?.val : null) ?? null;

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
        // MM tuzağı: SL'yi swing low'un tam altına koymak tehlikeli — MM oraya vurur.
        // Daha güvenli: swing low - 0.3×ATR (biraz daha aşağıya itmek, ama fazla marj yemeden)
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
        // SHORT için: swing high + 0.3×ATR (MM sweep buffer)
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
    // ── KALDIRAÇ ─────────────────────────────────────────────────────────────
    const t4up=ema20_4h>ema50_4h&&ema50_4h>ema200_4h;
    const t4dn=ema20_4h<ema50_4h&&ema50_4h<ema200_4h;

    // ═════════════════════════════════════════════════════════════════════════
    // R15 AÇIK KAYNAK MODÜLLER — Boğmadan sinyal kalitesi artırma
    // Kaynak: LazyBear, Chaikin, Bill Williams, LuxAlgo SMC, Jesse Framework
    // ═════════════════════════════════════════════════════════════════════════

    // ── R15-1. SQUEEZE MOMENTUM (LazyBear / TradingView) ─────────────────────
    // BB (Bollinger) KC (Keltner) içinde kaldığında patlama enerjisi birikir.
    // Momentum yönü = patlama yönü. Kripto'da en güvenilir pre-breakout sinyali.
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

    // ── R15-2. CHAIKIN MONEY FLOW (Marc Chaikin) ─────────────────────────────
    // Volume × fiyatın high-low içindeki pozisyonu. +0.1 üstü alım baskısı,
    // -0.1 altı satış baskısı. CVD'den bağımsız — mum kapanışlarına bakar.
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

    // ── R15-3. ELLIOTT WAVE OSCILLATOR (Bill Williams) ───────────────────────
    // 5 periyot EMA - 35 periyot EMA. Histo sıfır üstü ve büyüyorsa = bull wave.
    // Trend gücünü ölçer, RSI'dan farklı olarak dalga yapısını yansıtır.
    function calcEWO(kl) {
      if(!kl||kl.length<40)return{ok:false};
      const c=kl.map(k=>parseFloat(k[4]));
      function e(arr,p){const k=2/(p+1);let v=arr.slice(0,p).reduce((a,b)=>a+b,0)/p;for(let i=p;i<arr.length;i++)v=arr[i]*k+v*(1-k);return v;}
      const ewo=(e(c,5)-e(c,35))/c[c.length-1]*100;
      const prev=(e(c.slice(0,-1),5)-e(c.slice(0,-1),35))/c[c.length-2]*100;
      return{ok:true,value:+ewo.toFixed(3),growing:ewo>prev,
        signal:ewo>0.2&&ewo>prev?'BULL_WAVE':ewo<-0.2&&ewo<prev?'BEAR_WAVE':'NEUTRAL'};
    }

    // ── R15-4. WEIS WAVE VOLUME (Richard Weis) ───────────────────────────────
    // Mumları yön bazlı dalgalara gruplar. Alım dalgası hacmi > satım dalgası = güç.
    // Effort vs Result: fiyat hareket ediyor ama hacim yoksa = zayıf hareket.
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

    // ── R15-5. SMART MONEY ChoCH (LuxAlgo / ICT) ─────────────────────────────
    // Change of Character: ardışık HH/HL → LL/LH dönüşü veya tersi.
    // Wyckoff Spring'den farkı: YAPI değişimini ölçer, tek mum eventi değil.
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

    // ── R15-6. SPREAD / LİKİDİTE KALİTE SKORU ────────────────────────────────
    // UB sorununun kökenü: düşük likidite + dar depth = büyük slippage.
    // En iyi ask - en iyi bid / fiyat = spread yüzdesi.
    // depth top-5 USDT hacmi = gerçek likidite.
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

    // ── R15-7. FUNDING RATE MOMENTUM ─────────────────────────────────────────
    // Anlık funding değil TREND. Funding hızla negatife gidiyorsa short squeeze yakın.
    // Hızla pozitife gidiyorsa long'lar aşırı ısınıyor → dikkat.
    function calcFundingMomentum(funArr) {
      if(!funArr||funArr.length<4)return{ok:false};
      const rates=funArr.slice(-8).map(f=>parseFloat(f.fundingRate)*100);
      const trend=rates[rates.length-1]-rates[0];
      const accel=rates[rates.length-1]-rates[rates.length-2];
      return{ok:true,trend:+trend.toFixed(5),acceleration:+accel.toFixed(5),
        signal:trend<-0.015&&accel<0?'STRONG_LONG_BIAS':trend<-0.008?'LONG_BIAS':
               trend>0.015&&accel>0?'STRONG_SHORT_BIAS':trend>0.008?'SHORT_BIAS':'NEUTRAL'};
    }

    // ── R15-8. ATR KALİTE GATE ──────────────────────────────────────────────
    // Girişten ÖNCE: coinın ATR yüzdesi kullanıcının SL ayarından büyükse
    // coin çok volatil = SL yetersiz = UB riski.
    // atrPct > slPct*1.5 → skor düşür, A-Tier engellenebilir
    const slPctForGate=parseFloat(autoConfig?.slPct||2);
    const atrGateWarn=atrPct>slPctForGate*1.5; // ATR SL'den %50 büyükse uyarı
    const atrGateBlock=atrPct>slPctForGate*2.5; // ATR SL'den %150 büyükse blok

    // Hesapla
    const sqz1h=calcSqueeze(k1h);
    const sqz4h=calcSqueeze(k4h);
    const cmf1h=calcCMF(k1h);
    const cmf4h=calcCMF(k4h);
    const ewo1h=calcEWO(k1h);
    const weis1h=calcWeisWave(k1h);
    const choch1h=detectChoCH(k1h);
    const choch4h=detectChoCH(k4h);
    const liqQual=calcLiqQuality(depth,lastPrice);
    // R27: Spread geçmişini güncelle + BTC cache yenile
    if (liqQual.ok && liqQual.spread > 0) updateSpreadHistory(full, liqQual.spread);
    refreshBtcChange5m().catch(()=>{});
    const fundMom=calcFundingMomentum(fundArr);



    // ── 1. VOLUME PROFILE (VPVR) ──────────────────────────────────────────────
    // POC = en çok volume olan seviye → kurumlar buradan order koyar
    // VAH/VAL = value area sınırları → price buraya döner veya itilir
    function calcVolumeProfile(klines, numBuckets=50) {
      if(!klines||klines.length<10)return null;
      const highs=klines.map(k=>parseFloat(k[2]));
      const lows=klines.map(k=>parseFloat(k[3]));
      const vols=klines.map(k=>parseFloat(k[5]));
      const maxH=Math.max(...highs), minL=Math.min(...lows);
      const range=maxH-minL; if(range===0)return null;
      const bucketSz=range/numBuckets;
      const buckets=new Array(numBuckets).fill(0);
      for(let i=0;i<klines.length;i++){
        const loIdx=Math.max(0,Math.floor((lows[i]-minL)/bucketSz));
        const hiIdx=Math.min(numBuckets-1,Math.floor((highs[i]-minL)/bucketSz));
        const span=Math.max(1,hiIdx-loIdx+1);
        for(let b=loIdx;b<=hiIdx;b++) buckets[b]+=vols[i]/span;
      }
      const pocIdx=buckets.indexOf(Math.max(...buckets));
      const poc=minL+(pocIdx+0.5)*bucketSz;
      const totalVol=buckets.reduce((s,v)=>s+v,0);
      let vahIdx=pocIdx,valIdx=pocIdx,accVol=buckets[pocIdx];
      while(accVol<totalVol*0.7){
        const up=vahIdx+1<numBuckets?buckets[vahIdx+1]:0;
        const dn=valIdx-1>=0?buckets[valIdx-1]:0;
        if(up>=dn&&vahIdx+1<numBuckets){vahIdx++;accVol+=up;}
        else if(valIdx-1>=0){valIdx--;accVol+=dn;}
        else break;
      }
      return{
        poc:+poc.toFixed(8),
        vah:+(minL+(vahIdx+1)*bucketSz).toFixed(8),
        val:+(minL+valIdx*bucketSz).toFixed(8),
        rangeHigh:+maxH.toFixed(8), rangeLow:+minL.toFixed(8),
        topLevels:buckets.map((v,i)=>({price:+(minL+(i+0.5)*bucketSz).toFixed(8),vol:+v.toFixed(0)}))
          .sort((a,b)=>b.vol-a.vol).slice(0,5)
      };
    }

    // ── 2. EQUAL HIGHS/LOWS (İNDUCEMENT) ────────────────────────────────────
    // Retail bu seviyelere limit koyar → MM oraya sweep atar, sonra tersine döner
    // Fiyat eşit yüksek yakınındaysa → sahte kırılım riski → long alma!
    function detectEqualLevels(klines, tol=0.0015) {
      if(!klines||klines.length<10) return {eqHighs:[],eqLows:[]};
      const recent=klines.slice(-30);
      const sHighs=[],sLows=[];
      for(let i=2;i<recent.length-2;i++){
        const h=parseFloat(recent[i][2]), l=parseFloat(recent[i][3]);
        const isH=h>parseFloat(recent[i-1][2])&&h>parseFloat(recent[i-2][2])&&h>parseFloat(recent[i+1][2])&&h>parseFloat(recent[i+2][2]);
        const isL=l<parseFloat(recent[i-1][3])&&l<parseFloat(recent[i-2][3])&&l<parseFloat(recent[i+1][3])&&l<parseFloat(recent[i+2][3]);
        if(isH)sHighs.push({price:h,idx:i});
        if(isL)sLows.push({price:l,idx:i});
      }
      function findEq(levels){
        const groups=[];
        for(let i=0;i<levels.length;i++){
          const matches=levels.filter((l,j)=>j!==i&&Math.abs(l.price-levels[i].price)/levels[i].price<tol);
          if(matches.length>=1){
            const avg=(levels[i].price+matches.reduce((s,m)=>s+m.price,0))/(matches.length+1);
            if(!groups.find(g=>Math.abs(g.price-avg)/avg<tol))
              groups.push({price:+avg.toFixed(8),count:matches.length+1});
          }
        }
        return groups.sort((a,b)=>b.count-a.count).slice(0,3);
      }
      return{eqHighs:findEq(sHighs),eqLows:findEq(sLows)};
    }

    // ── 3. PREMIUM / DISCOUNT ZONES (ICT) ────────────────────────────────────
    // Long için discount bölge (alt %50), short için premium (üst %50)
    // MM discount'tan alır, premium'dan satar — retail tam tersini yapar
    function calcPremiumDiscount(klines, curPrice, lookback=20) {
      if(!klines||klines.length<lookback)return null;
      const recent=klines.slice(-lookback);
      const high=Math.max(...recent.map(k=>parseFloat(k[2])));
      const low=Math.min(...recent.map(k=>parseFloat(k[3])));
      const range=high-low; if(range===0)return null;
      const pct=(curPrice-low)/range*100;
      const mid=(high+low)/2;
      return{
        pct:+pct.toFixed(1),
        equilibrium:+mid.toFixed(8),
        rangeHigh:+high.toFixed(8),rangeLow:+low.toFixed(8),
        zone:pct>=75?'PREMIUM_HIGH':pct>=50?'PREMIUM':pct>=25?'DISCOUNT':'DISCOUNT_LOW',
        forLong:pct<45,forShort:pct>55,
        signal:pct<20?'DEEP_DISCOUNT':pct>80?'DEEP_PREMIUM':pct<40?'DISCOUNT_OK':pct>60?'PREMIUM_OK':'EQUILIBRIUM'
      };
    }

    // ── 4. RELATIVE VOLUME (RVOL) ─────────────────────────────────────────────
    // Hareketin arkasında gerçek kurumsal volume var mı?
    // RVOL < 0.7 → kuru hareket → MM süpürmesi olabilir, sinyali küçült
    function calcRVOL(klines, period=20) {
      if(!klines||klines.length<period+1)return null;
      const recent=klines.slice(-(period+1));
      const avg=recent.slice(0,period).reduce((s,k)=>s+parseFloat(k[5]),0)/period;
      const cur=parseFloat(recent[recent.length-1][5]);
      const rvol=avg>0?cur/avg:1;
      return{
        rvol:+rvol.toFixed(2),curVol:+cur.toFixed(0),avgVol:+avg.toFixed(0),
        signal:rvol>=2?'VERY_HIGH':rvol>=1.5?'HIGH':rvol>=1?'NORMAL':rvol>=0.7?'LOW':'VERY_LOW',
        valid:rvol>=0.8
      };
    }

    // ── 5. BREAKER BLOCKS ─────────────────────────────────────────────────────
    // Kırılmış OB → Breaker: eski destek direnç olur, MM buradan iter
    // Bull Breaker: eski bearish OB aşağı kırıldı → şimdi fiyat geri geldi = SHORT
    // Bear Breaker: eski bullish OB yukarı kırıldı → şimdi fiyat geri geldi = LONG
    function detectBreakerBlocks(klines) {
      if(!klines||klines.length<20)return{bullBreakers:[],bearBreakers:[]};
      const recent=klines.slice(-40);
      const curPrice=parseFloat(recent[recent.length-1][4]);
      const bullBreakers=[],bearBreakers=[];
      for(let i=1;i<recent.length-4;i++){
        const o=parseFloat(recent[i][1]),c=parseFloat(recent[i][4]);
        const h=parseFloat(recent[i][2]),l=parseFloat(recent[i][3]);
        const isBullMover=parseFloat(recent[i+1][4])>parseFloat(recent[i+1][1]); // sonraki bullish
        const isBearMover=parseFloat(recent[i+1][4])<parseFloat(recent[i+1][1]);
        // Bearish mum (OB) → sonraki bullish mum yeni yüksek yapıyor → Bull Breaker
        if(c<o&&isBullMover){
          for(let j=i+2;j<Math.min(i+8,recent.length);j++){
            if(parseFloat(recent[j][2])>h){
              // OB kırıldı. Fiyat şimdi OB içindeyse → Bear Breaker (fiyat geri döndü)
              if(curPrice>=l&&curPrice<=h) bearBreakers.push({high:+h.toFixed(8),low:+l.toFixed(8),mid:+((h+l)/2).toFixed(8),distPct:+((h-curPrice)/curPrice*100).toFixed(2)});
              break;
            }
          }
        }
        // Bullish mum (OB) → sonraki bearish mum yeni düşük yapıyor → Bear Breaker
        if(c>o&&isBearMover){
          for(let j=i+2;j<Math.min(i+8,recent.length);j++){
            if(parseFloat(recent[j][3])<l){
              if(curPrice>=l&&curPrice<=h) bullBreakers.push({high:+h.toFixed(8),low:+l.toFixed(8),mid:+((h+l)/2).toFixed(8),distPct:+((curPrice-l)/curPrice*100).toFixed(2)});
              break;
            }
          }
        }
      }
      return{bullBreakers:bullBreakers.slice(0,3),bearBreakers:bearBreakers.slice(0,3)};
    }

    // ── 6. RSI DİVERJANS (Klasik + Gizli) ───────────────────────────────────
    // Klasik bearish div: price higher high + RSI lower high → dönüş gelir
    // Klasik bullish div: price lower low + RSI higher low → yükseliş gelir
    // Gizli bullish div: price higher low + RSI lower low → trend devam (long)
    function detectRSIDivergence(klines, rsiPeriod=14) {
      if(!klines||klines.length<30)return{bullDiv:false,bearDiv:false,hiddenBull:false,hiddenBear:false};
      function calcRsiArr(kl){
        if(kl.length<rsiPeriod+1)return[];
        const c=kl.map(k=>parseFloat(k[4]));
        const result=[];
        let g=0,l2=0;
        for(let i=c.length-20;i<c.length;i++){
          if(i<1)continue;
          const d=c[i]-c[i-1];
          g=g*(rsiPeriod-1)/rsiPeriod+(d>0?d:0)/rsiPeriod;
          l2=l2*(rsiPeriod-1)/rsiPeriod+(d<0?-d:0)/rsiPeriod;
          result.push(l2===0?100:Math.round(100-100/(1+g/l2)));
        }
        return result;
      }
      const rsiArr=calcRsiArr(klines);
      if(rsiArr.length<6)return{bullDiv:false,bearDiv:false,hiddenBull:false,hiddenBear:false};
      const prices=klines.slice(-rsiArr.length).map(k=>parseFloat(k[4]));
      const r=rsiArr,p=prices;
      const n=r.length;
      // Son 2 swing noktası karşılaştır (basit)
      const p1=p[n-1],p2=p[n-4];
      const r1=r[n-1],r2=r[n-4];
      return{
        bullDiv:p1<p2&&r1>r2,   // Price ↓ RSI ↑ → klasik bullish divergence
        bearDiv:p1>p2&&r1<r2,   // Price ↑ RSI ↓ → klasik bearish divergence
        hiddenBull:p1>p2&&r1<r2&&r1>30, // Gizli bullish → trend devam
        hiddenBear:p1<p2&&r1>r2&&r1<70, // Gizli bearish → trend devam
      };
    }

    // ── 7. MULTI-TIMEFRAME BIAS ───────────────────────────────────────────────
    // Kaç zaman dilimi aynı yönde? Ters TF varsa giriş riskli
    function calcMTFBias(rsi4h,rsi1h,rsi15m,rsi5m,ema20_4h,ema50_4h,ema200_4h,ema20_1h,ema50_1h) {
      let bull=0,bear=0;
      if(ema20_4h>ema50_4h&&ema50_4h>ema200_4h){bull+=3;}else if(ema20_4h<ema50_4h&&ema50_4h<ema200_4h){bear+=3;}
      if(ema20_1h>ema50_1h){bull+=2;}else{bear+=2;}
      if(rsi4h>55&&rsi4h<75){bull++;}else if(rsi4h<45&&rsi4h>25){bear++;}
      if(rsi1h>55){bull++;}else if(rsi1h<45){bear++;}
      if(rsi15m>58){bull++;}else if(rsi15m<42){bear++;}
      if(rsi5m>60&&rsi5m<80){bull++;}else if(rsi5m<40&&rsi5m>20){bear++;}
      const total=bull+bear;
      const bullPct=total>0?bull/total*100:50;
      return{
        bull,bear,bullPct:+bullPct.toFixed(0),total,
        bias:bullPct>=70?'STRONG_BULL':bullPct>=58?'BULL':bullPct<=30?'STRONG_BEAR':bullPct<=42?'BEAR':'NEUTRAL',
        aligned:Math.abs(bull-bear)>=4,
        forLong:bullPct>=58,forShort:bullPct<=42
      };
    }

    // ── 8. LİKİDİTE BOŞLUĞU (Fair Value Gap genişletilmiş) ──────────────────
    // Fiyat çok hızlı geçti → volume ince → fiyat geri gelip doldurur
    // Büyük FVG = hedef seviye
    function detectLiquidityVoids(klines) {
      if(!klines||klines.length<5)return[];
      const voids=[];
      const avgBody=klines.slice(-20).reduce((s,k)=>s+Math.abs(parseFloat(k[4])-parseFloat(k[1])),0)/20;
      for(let i=1;i<klines.length-1;i++){
        const prev=klines[i-1],curr=klines[i],next=klines[i+1];
        const ph=parseFloat(prev[2]),pl=parseFloat(prev[3]);
        const nh=parseFloat(next[2]),nl=parseFloat(next[3]);
        const body=Math.abs(parseFloat(curr[4])-parseFloat(curr[1]));
        // Bullish void: prev.high < next.low (gap yukarı)
        if(ph<nl&&body>avgBody*1.5){
          voids.push({type:'BULL_VOID',top:+nl.toFixed(8),bottom:+ph.toFixed(8),size:+((nl-ph)/lastPrice*100).toFixed(2),idx:i});
        }
        // Bearish void: prev.low > next.high (gap aşağı)
        if(pl>nh&&body>avgBody*1.5){
          voids.push({type:'BEAR_VOID',top:+pl.toFixed(8),bottom:+nh.toFixed(8),size:+((pl-nh)/lastPrice*100).toFixed(2),idx:i});
        }
      }
      return voids.slice(-5);
    }

    // ── YENİ ANALİZLERİ ÇALIŞTIR ─────────────────────────────────────────────
    const vpvr1h   = calcVolumeProfile(k1h, 60);
    const vpvr4h   = calcVolumeProfile(k4h, 60);
    const eqLvl1h  = detectEqualLevels(k1h);
    const eqLvl4h  = detectEqualLevels(k4h);
    const pd1h     = calcPremiumDiscount(k1h, lastPrice, 20);
    const pd4h     = calcPremiumDiscount(k4h, lastPrice, 20);
    const rvol1h   = calcRVOL(k1h);
    const rvol4h   = calcRVOL(k4h);
    const brk1h    = detectBreakerBlocks(k1h);
    const brk4h    = detectBreakerBlocks(k4h);
    const rsiDiv1h = detectRSIDivergence(k1h);
    const rsiDiv4h = detectRSIDivergence(k4h);
    const mtfBias  = calcMTFBias(rsi4h,rsi1h,rsi15m,rsi5m,ema20_4h,ema50_4h,ema200_4h,ema20_1h,ema50_1h);
    const liqVoids1h = detectLiquidityVoids(k1h.slice(-30));
    const r37Timing = r37MoveTiming(k5m, k15m, lastPrice, atrPct, vpvr1h, liq1h);

    // R40 FIX: R39 5m destek/direnç haritası vpvr1h/liq1h hazırlandıktan SONRA hesaplanmalı.
    // Önceki R39_TREND_HEALTH_TRAIL build'inde vpvr1h init edilmeden çağrıldığı için
    // /api/analyze tüm sembollerde "Cannot access 'vpvr1h' before initialization" ERR üretiyordu.
    const r39SR = r39FiveMinuteSR(k5m, k1h, lastPrice, atrPct, vpvr1h, liq1h);

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
    // Tick sweep tek başına skor verir ama sadece diğer sinyallerle birlikte güçlü sayılır
    // Tek başına: 12 puan (MM genellikle sweep sonrası hemen ters gider)
    // CVD veya AMD da aynı yöndeyse: 25 puan
    if (tickData?.tickSweep?.type==='BULL_SWEEP'&&tickData.tickSweep.fresh) {
      const cvdConfirm = cvd?.ratio > 50 || cvd?.momentum === 'POSITIVE' || cvd?.momentum === 'ACCELERATING_BULL';
      const amdConfirm = amd5m?.signal === 'AMD_LONG';
      const bonus = (cvdConfirm || amdConfirm) ? 25 : 12;
      longScore += bonus;
      signals.long.push(`⚡ Tick Sweep Bull${cvdConfirm||amdConfirm?' +teyit':' (tek)'}`);
    }
    if (tickData?.tickSweep?.type==='BEAR_SWEEP'&&tickData.tickSweep.fresh) {
      const cvdConfirm = cvd?.ratio < 50 || cvd?.momentum === 'NEGATIVE' || cvd?.momentum === 'ACCELERATING_BEAR';
      const amdConfirm = amd5m?.signal === 'AMD_SHORT';
      const bonus = (cvdConfirm || amdConfirm) ? 25 : 12;
      shortScore += bonus;
      signals.short.push(`⚡ Tick Sweep Bear${cvdConfirm||amdConfirm?' +teyit':' (tek)'}`);
    }
    if (amd5m.signal === 'WAIT_MSS') {
      // MSS bekleniyor — skor verme ama bilgi ver
    }

    // Kill zone skor cezası kaldırıldı.
    // Kripto 24/7 olduğu için saat bazlı long/short skor azaltma yapılmaz.

    // 0a. WYCKOFF — MM'nin gerçek dip/zirve tespiti
    // Spring = en güçlü sinyal, ML backtested
    if(wyckoff1h.recentEvents?.some(e=>e.type==='SPRING'))  {longScore+=30;  signals.long.push('🌊 Wyckoff Spring! MM dip topladı');}
    if(wyckoff4h.recentEvents?.some(e=>e.type==='SPRING'))  {longScore+=20;  signals.long.push('🌊 4h Wyckoff Spring');}
    if(wyckoff1h.recentEvents?.some(e=>e.type==='SOS'))     {longScore+=15;  signals.long.push('💪 Wyckoff SOS - kırılım teyidi');}
    if(wyckoff1h.recentEvents?.some(e=>e.type==='UTAD'))    {shortScore+=25; signals.short.push('🚨 Wyckoff UTAD - sahte kırılım ↓');
      // UTAD geldiğinde long giriş tehlikeli — MM dağıtım yapıyor, long skorunu cezalandır
      longScore = Math.round(longScore * 0.5);
      signals.long.push('⚠️ UTAD: Long engellendi');
    }
    if(wyckoff4h.recentEvents?.some(e=>e.type==='UTAD'))    {shortScore+=20; signals.short.push('🚨 4h UTAD dağıtım');
      longScore = Math.round(longScore * 0.6);
    }

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

    // R125 — depth@100ms/aggTrade canlı orderflow tek beyne yakıt verir
    if (r125Flow?.ok) {
      if (r125Flow.longEdge >= 6) { longScore += Math.min(8, Math.round(r125Flow.longEdge)); signals.long.push(`⚡ R125 OrderFlow L${r125Flow.longEdge}`); }
      // ── R140: Pump Cycle Skoru ──────────────────────────────────────────────
      if (r140Phase?.phase === 'EXPANSION') {
        // Genişleme: trend var, hacim güçlü
        longScore  += 8; signals.long.push('R140 pump-expansion');
        shortScore -= 5; // ters yönde ihtiyat
      }
      if (r140Phase?.phase === 'ACCUMULATION') {
        // Sıkışma: yakında breakout gelir ama yön belirsiz
        longScore  += 4; shortScore += 4;
        signals.long.push('R140 birikim-hazırlık'); signals.short.push('R140 birikim-hazırlık');
      }
      if (r140Phase?.phase === 'DISTRIBUTION') {
        // Dağıtım/sahte pump: LONG tehlikeli, SHORT fırsatı
        longScore  -= 12; signals.long.push('⚠️ R140 dağıtım-azalt');
        shortScore += 10; signals.short.push('R140 dağıtım-short-fırsat');
      }

      // ── R140: BTC Diverjans Skoru ────────────────────────────────────────────
      if (r140BtcDiv?.divergence || r140BtcDiv?.score > 0) {
        longScore  += r140BtcDiv.score;
        signals.long.push(`R140 BTC-div:${r140BtcDiv.label.slice(0,40)}`);
      }
      if (btc5mCtx.dropping && !r140BtcDiv.divergence) {
        // BTC düşüyor ve coin de düşüyor → LONG için olumsuz sinyal
        longScore  -= 8;
        signals.long.push('R140 BTC-drop-coin-zayıf');
      }

      // ── R140: OI Velocity Skoru ──────────────────────────────────────────────
      if (r140OiVel?.oiConfirmed) {
        // Fiyat yukarı + OI artıyor = gerçek alım
        longScore  += 6; signals.long.push(`R140 OI-onaylı+${r140OiVel.velocity.toFixed(1)}%`);
      }
      if (r140OiVel?.fakePump) {
        // Fiyat yukarı ama OI azalıyor = sahte pump / kapanış
        longScore  -= 10; signals.long.push(`⚠️ R140 sahte-pump OI${r140OiVel.velocity.toFixed(1)}%`);
        shortScore += 8;  signals.short.push(`R140 sahte-pump SHORT-fırsat`);
      }

      // ── R140: Coin RVOL Skoru ────────────────────────────────────────────────
      if (r140Rvol?.signal === 'VERY_HIGH') {
        longScore  += 6; shortScore += 6;
        signals.long.push(`R140 RVOL×${r140Rvol.rvol}`); signals.short.push(`R140 RVOL×${r140Rvol.rvol}`);
      }
      if (r140Rvol?.signal === 'LOW' || r140Rvol?.signal === 'VERY_LOW') {
        longScore  -= 4; shortScore -= 4; // Düşük hacim = sahte sinyal riski
      }

      // ── R140: Equal Highs/Lows Tuzak Skoru ──────────────────────────────────
      if (r140EqHL?.nearHighTrap) {
        // Eşit yükseklere yakın: LONG için tuzak riski, SHORT için fırsat
        longScore  -= 8; signals.long.push('⚠️ R140 eşit-tepe-tuzak');
        shortScore += 6; signals.short.push('R140 eşit-tepe-short');
      }
      if (r140EqHL?.nearLowTrap) {
        // Eşit düşüklere yakın: SHORT için tuzak riski, LONG için fırsat
        shortScore -= 8; signals.short.push('⚠️ R140 eşit-dip-tuzak');
        longScore  += 6; signals.long.push('R140 eşit-dip-long');
      }

      if (r125Flow.shortEdge >= 6) { shortScore += Math.min(8, Math.round(r125Flow.shortEdge)); signals.short.push(`⚡ R125 OrderFlow S${r125Flow.shortEdge}`); }
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

    // R39: 5m S/R + PDH/PDL/Pivot. Skor verir ama tek başına emir açtırmaz;
    // hedefe yapışmış geç girişleri cezalandırır, taze kırılım/retest'i destekler.
    if(r39SR?.ok){
      const L=r39SR.long||{}, S=r39SR.short||{};
      if(L.supportConfluence){ longScore=Math.min(longScore+10,100); signals.long.push(`🧱 R39 5m destek: ${L.nearestSupport?.type||''} (${L.nearestSupport?.distPct ?? '?'}%)`); }
      if(S.resistanceConfluence){ shortScore=Math.min(shortScore+10,100); signals.short.push(`🧱 R39 5m direnç: ${S.nearestResistance?.type||''} (${S.nearestResistance?.distPct ?? '?'}%)`); }
      if(L.breakConfirmed){ longScore=Math.min(longScore+9,100); signals.long.push(`🚀 R39 direnç kırılımı: ${L.nearestResistance?.type||''}`); }
      if(S.breakConfirmed){ shortScore=Math.min(shortScore+9,100); signals.short.push(`🔻 R39 destek kırılımı: ${S.nearestSupport?.type||''}`); }
      if(L.nearResistance && !L.breakConfirmed){ longScore=Math.round(longScore*0.88); signals.long.push(`⚠️ R39 üst hedef yakın: ${L.nearestResistance?.type||''} ${L.nearestResistance?.distPct ?? '?'}%`); }
      if(S.nearSupport && !S.breakConfirmed){ shortScore=Math.round(shortScore*0.88); signals.short.push(`⚠️ R39 alt hedef yakın: ${S.nearestSupport?.type||''} ${S.nearestSupport?.distPct ?? '?'}%`); }
    }
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

    // ── R15: 8 YENİ AÇIK KAYNAK MODÜL SKORLAMASI ─────────────────────────────

    // SQUEEZE MOMENTUM (LazyBear)
    if(sqz1h.ok){
      if(sqz1h.signal==='SQ_BULL'){longScore+=14;signals.long.push(`⚡ 1h Squeeze Bull patlamak üzere`);}
      if(sqz1h.signal==='SQ_BEAR'){shortScore+=14;signals.short.push(`⚡ 1h Squeeze Bear patlamak üzere`);}
      if(sqz1h.signal==='BULL_FREE'&&sqz1h.acceleration==='GROWING'){longScore+=6;}
      if(sqz1h.signal==='BEAR_FREE'&&sqz1h.acceleration==='GROWING'){shortScore+=6;}
    }
    if(sqz4h.ok){
      if(sqz4h.signal==='SQ_BULL'){longScore+=10;signals.long.push(`⚡ 4h Squeeze Bull`);}
      if(sqz4h.signal==='SQ_BEAR'){shortScore+=10;signals.short.push(`⚡ 4h Squeeze Bear`);}
    }

    // CHAIKIN MONEY FLOW (CMF) — volume+fiyat kombinasyonu
    if(cmf1h.ok){
      if(cmf1h.signal==='STRONG_BUY'){longScore+=12;signals.long.push(`💧 CMF1h Güçlü Alım ${cmf1h.value}`);}
      else if(cmf1h.signal==='BUY'){longScore+=6;}
      if(cmf1h.signal==='STRONG_SELL'){shortScore+=12;signals.short.push(`💧 CMF1h Güçlü Satım ${cmf1h.value}`);}
      else if(cmf1h.signal==='SELL'){shortScore+=6;}
      // CMF ters → ceza
      if(cmf1h.signal==='STRONG_SELL'&&longScore>shortScore){longScore=Math.round(longScore*0.85);}
      if(cmf1h.signal==='STRONG_BUY'&&shortScore>longScore){shortScore=Math.round(shortScore*0.85);}
    }
    if(cmf4h.ok){
      if(cmf4h.signal==='STRONG_BUY'){longScore+=8;signals.long.push(`💧 CMF4h ${cmf4h.value}`);}
      if(cmf4h.signal==='STRONG_SELL'){shortScore+=8;signals.short.push(`💧 CMF4h ${cmf4h.value}`);}
    }

    // ELLIOTT WAVE OSCILLATOR (Bill Williams)
    if(ewo1h.ok){
      if(ewo1h.signal==='BULL_WAVE'){longScore+=8;signals.long.push(`🌊 EWO Bull ${ewo1h.value}%`);}
      if(ewo1h.signal==='BEAR_WAVE'){shortScore+=8;signals.short.push(`🌊 EWO Bear ${ewo1h.value}%`);}
    }

    // WEIS WAVE VOLUME (Richard Weis) — Effort vs Result
    if(weis1h.ok){
      if(weis1h.signal==='BULL_EFFORT'){longScore+=10;signals.long.push(`🌊 Weis Bull Effort ${weis1h.ratio}x`);}
      if(weis1h.signal==='BEAR_EFFORT'){shortScore+=10;signals.short.push(`🌊 Weis Bear Effort`);}
    }

    // SMART MONEY CHOCH (LuxAlgo/ICT) — Yapı değişimi
    if(choch1h.ok){
      if(choch1h.signal==='BULL_CHOCH'){longScore+=16;signals.long.push(`🔄 1h ChoCH Bull — yapı değişimi`);}
      if(choch1h.signal==='BEAR_CHOCH'){shortScore+=16;signals.short.push(`🔄 1h ChoCH Bear — yapı değişimi`);}
    }
    if(choch4h.ok){
      if(choch4h.signal==='BULL_CHOCH'){longScore+=12;signals.long.push(`🔄 4h ChoCH Bull`);}
      if(choch4h.signal==='BEAR_CHOCH'){shortScore+=12;signals.short.push(`🔄 4h ChoCH Bear`);}
    }

    // LİKİDİTE KALİTE SKORU — UB sorununun önleyicisi
    if(liqQual.ok){
      if(liqQual.slippageRisk){
        // Spread geniş veya derinlik az → kayma riski → skor düşür
        longScore=Math.round(longScore*0.78);shortScore=Math.round(shortScore*0.78);
        signals.long.push(`⚠️ Spread %${liqQual.spread} düşük likidite — kayma riski`);
        signals.short.push(`⚠️ Spread %${liqQual.spread} düşük likidite — kayma riski`);
      }
      if(liqQual.quality==='EXCELLENT'){longScore+=5;shortScore+=5;}
      if(liqQual.quality==='POOR'&&!liqQual.slippageRisk){longScore=Math.round(longScore*0.88);shortScore=Math.round(shortScore*0.88);}
    }

    // FUNDING RATE MOMENTUM
    if(fundMom.ok){
      if(fundMom.signal==='STRONG_LONG_BIAS'){longScore+=12;signals.long.push(`📉 Funding hızla negatife → short squeeze yakın`);}
      else if(fundMom.signal==='LONG_BIAS'){longScore+=6;}
      if(fundMom.signal==='STRONG_SHORT_BIAS'){shortScore+=12;signals.short.push(`📈 Funding hızla pozitife → longs tükenebilir`);}
      else if(fundMom.signal==='SHORT_BIAS'){shortScore+=6;}
    }

    // ATR KALİTE GATE — UB tipi volatil coin için son savunma
    if(atrGateBlock){
      // ATR SL'nin %150 üstünde → bu coine verilen SL ayarı yetersiz
      longScore=Math.round(longScore*0.65);shortScore=Math.round(shortScore*0.65);
      signals.long.push(`⛔ ATR %${atrPct.toFixed(1)} >> SL %${slPctForGate} — volatilite riski`);
      signals.short.push(`⛔ ATR %${atrPct.toFixed(1)} >> SL %${slPctForGate} — volatilite riski`);
    } else if(atrGateWarn){
      longScore=Math.round(longScore*0.85);shortScore=Math.round(shortScore*0.85);
      signals.long.push(`⚠️ ATR %${atrPct.toFixed(1)} yüksek — giriş dikkatli`);
    }


    if(vpvr1h) {
      const distPOC=Math.abs(lastPrice-vpvr1h.poc)/lastPrice*100;
      // VAL altında = value area dışı → kurumlar geri alır
      if(lastPrice<vpvr1h.val*1.002&&distPOC>0.3) {
        longScore+=12; signals.long.push(`📊 VP VAL altı $${vpvr1h.val.toFixed(4)} → kurumsal alım bölgesi`);
      }
      // POC altında ama VAL üstü = discount ama value area içi
      else if(lastPrice<vpvr1h.poc&&lastPrice>=vpvr1h.val) {longScore+=5;}
      // VAH üstünde = value area dışı yüksek → geri döner
      if(lastPrice>vpvr1h.vah*0.998&&distPOC>0.3) {
        shortScore+=10; signals.short.push(`📊 VP VAH üstü $${vpvr1h.vah.toFixed(4)} → ret riski`);
      }
      // POC yakınında (±0.2%) = güçlü destek/direnç, hareketi durdurabilir
      if(distPOC<0.2) {signals.long.push(`📊 VP POC: $${vpvr1h.poc.toFixed(4)} — pivot`);}
    }
    if(vpvr4h) {
      // 4h POC ile destekleniyor mu?
      if(lastPrice>vpvr4h.val&&lastPrice<vpvr4h.poc){longScore+=6;}
      if(lastPrice<vpvr4h.vah&&lastPrice>vpvr4h.poc){shortScore+=4;}
    }

    // ── 12. PREMIUM / DISCOUNT ZONES (ICT) ───────────────────────────────────
    if(pd1h) {
      if(pd1h.signal==='DEEP_DISCOUNT') {longScore+=14;signals.long.push(`💎 Derin Discount %${pd1h.pct} — MM alım bölgesi`);}
      else if(pd1h.signal==='DISCOUNT_OK') {longScore+=7;}
      else if(pd1h.signal==='DEEP_PREMIUM') {shortScore+=14;signals.short.push(`🔴 Derin Premium %${pd1h.pct} — MM satış bölgesi`);}
      else if(pd1h.signal==='PREMIUM_OK') {shortScore+=7;}
      // Yanlış bölgede ceza: premium'da long = MM tuzağı
      if(!pd1h.forLong&&pd1h.pct>60) {longScore=Math.round(longScore*0.80);}
      if(!pd1h.forShort&&pd1h.pct<40) {shortScore=Math.round(shortScore*0.80);}
    }

    // ── 13. EQUAL HIGHS/LOWS — İNDUCEMENT TESPİTİ ───────────────────────────
    if(eqLvl1h) {
      // Fiyat eşit yüksek yakınında → retail long fomosunu çekiyor → MM sweep atacak
      const nearEH=eqLvl1h.eqHighs.find(l=>Math.abs(lastPrice-l.price)/lastPrice<0.008&&lastPrice>l.price*0.994);
      const nearEL=eqLvl1h.eqLows.find(l=>Math.abs(lastPrice-l.price)/lastPrice<0.008&&lastPrice<l.price*1.006);
      if(nearEH) {
        shortScore+=18; signals.short.push(`⚠️ Equal Highs $${nearEH.price.toFixed(4)} (${nearEH.count}x) → MM sweep riski, long alma`);
        longScore=Math.round(longScore*0.65); // Long skoru ciddi şekilde cezalandır
      }
      if(nearEL) {
        longScore+=18; signals.long.push(`⚠️ Equal Lows $${nearEL.price.toFixed(4)} (${nearEL.count}x) → MM sweep sonrası long fırsat`);
        shortScore=Math.round(shortScore*0.65);
      }
    }
    if(eqLvl4h) {
      const nearEH4=eqLvl4h.eqHighs.find(l=>Math.abs(lastPrice-l.price)/lastPrice<0.01&&lastPrice>l.price*0.992);
      const nearEL4=eqLvl4h.eqLows.find(l=>Math.abs(lastPrice-l.price)/lastPrice<0.01&&lastPrice<l.price*1.008);
      if(nearEH4) {shortScore+=12; signals.short.push(`⚠️ 4h Equal Highs $${nearEH4.price.toFixed(4)}`); longScore=Math.round(longScore*0.75);}
      if(nearEL4) {longScore+=12; signals.long.push(`⚠️ 4h Equal Lows $${nearEL4.price.toFixed(4)}`); shortScore=Math.round(shortScore*0.75);}
    }

    // ── 14. RELATIVE VOLUME (RVOL) ────────────────────────────────────────────
    if(rvol1h) {
      if(!rvol1h.valid) {
        // Düşük volume: hareket arkasında kurumsal destek yok → sahte olabilir
        longScore=Math.round(longScore*0.80);
        shortScore=Math.round(shortScore*0.80);
        signals.long.push(`⚠️ RVOL düşük ${rvol1h.rvol}x — hareket kuru olabilir`);
      } else if(rvol1h.signal==='VERY_HIGH'||rvol1h.signal==='HIGH') {
        // Yüksek volume: kurumsal katılım var → sinyal güçlü
        if(longScore>shortScore){longScore+=8;signals.long.push(`📊 RVOL ${rvol1h.rvol}x güçlü volume`);}
        else{shortScore+=8;signals.short.push(`📊 RVOL ${rvol1h.rvol}x güçlü volume`);}
      }
    }

    // ── 15. BREAKER BLOCKS ────────────────────────────────────────────────────
    if(brk1h.bullBreakers.length>0) {
      const bb=brk1h.bullBreakers[0];
      if(lastPrice>=bb.low&&lastPrice<=bb.high) {
        longScore+=15; signals.long.push(`🧱 Bull Breaker 1h: $${bb.low.toFixed(4)}-$${bb.high.toFixed(4)} — MM bu bölgeden alır`);
      }
    }
    if(brk1h.bearBreakers.length>0) {
      const bb=brk1h.bearBreakers[0];
      if(lastPrice>=bb.low&&lastPrice<=bb.high) {
        shortScore+=15; signals.short.push(`🧱 Bear Breaker 1h: $${bb.low.toFixed(4)}-$${bb.high.toFixed(4)} — MM bu bölgeden satar`);
      }
    }
    if(brk4h.bullBreakers.length>0&&lastPrice>=brk4h.bullBreakers[0].low&&lastPrice<=brk4h.bullBreakers[0].high)
      {longScore+=8; signals.long.push(`🧱 Bull Breaker 4h: $${brk4h.bullBreakers[0].mid.toFixed(4)}`);}
    if(brk4h.bearBreakers.length>0&&lastPrice>=brk4h.bearBreakers[0].low&&lastPrice<=brk4h.bearBreakers[0].high)
      {shortScore+=8; signals.short.push(`🧱 Bear Breaker 4h: $${brk4h.bearBreakers[0].mid.toFixed(4)}`);}

    // ── 16. RSI DİVERJANS ────────────────────────────────────────────────────
    if(rsiDiv1h.bullDiv) {longScore+=10; signals.long.push('📈 RSI Bullish Div 1h — price↓ RSI↑');}
    if(rsiDiv1h.bearDiv) {shortScore+=10; signals.short.push('📉 RSI Bearish Div 1h — price↑ RSI↓');}
    if(rsiDiv1h.hiddenBull) {longScore+=7; signals.long.push('📈 RSI Gizli Bull Div — trend devam');}
    if(rsiDiv1h.hiddenBear) {shortScore+=7; signals.short.push('📉 RSI Gizli Bear Div — trend devam');}
    if(rsiDiv4h.bullDiv) {longScore+=8; signals.long.push('📈 RSI Bullish Div 4h');}
    if(rsiDiv4h.bearDiv) {shortScore+=8; signals.short.push('📉 RSI Bearish Div 4h');}

    // ── 17. MULTI-TIMEFRAME BIAS ──────────────────────────────────────────────
    if(mtfBias.aligned) {
      if(mtfBias.bias==='STRONG_BULL'){longScore+=14;signals.long.push(`📐 MTF ${mtfBias.bull}/${mtfBias.total} bull — tüm TF uyumlu`);}
      if(mtfBias.bias==='STRONG_BEAR'){shortScore+=14;signals.short.push(`📐 MTF ${mtfBias.bear}/${mtfBias.total} bear — tüm TF uyumlu`);}
    } else {
      if(mtfBias.forLong){longScore+=6;}
      if(mtfBias.forShort){shortScore+=6;}
    }
    // MTF ters yönde ise skor cezası — TF uyumsuzluğu büyük risk
    if((mtfBias.bias==='STRONG_BEAR'||mtfBias.bias==='BEAR')&&longScore>shortScore)
      {longScore=Math.round(longScore*0.72); signals.long.push(`⚠️ MTF 4h bear iken long riski`);}
    if((mtfBias.bias==='STRONG_BULL'||mtfBias.bias==='BULL')&&shortScore>longScore)
      {shortScore=Math.round(shortScore*0.72); signals.short.push(`⚠️ MTF 4h bull iken short riski`);}

    // ── 18. LİKİDİTE BOŞLUKLARI (HEDEF SEVİYELER) ───────────────────────────
    if(liqVoids1h.length>0) {
      // Fiyat, bullish void'in içindeyse → void dolduruluyor = momentum long
      const inBullVoid=liqVoids1h.find(v=>v.type==='BULL_VOID'&&lastPrice>=v.bottom&&lastPrice<=v.top);
      const inBearVoid=liqVoids1h.find(v=>v.type==='BEAR_VOID'&&lastPrice<=v.top&&lastPrice>=v.bottom);
      if(inBullVoid){longScore+=8;signals.long.push(`⚡ Bullish Void %${inBullVoid.size} dolduruluyor`);}
      if(inBearVoid){shortScore+=8;signals.short.push(`⚡ Bearish Void %${inBearVoid.size} dolduruluyor`);}
    }

    // ── R25 FİTİL TUZAK HARİTASI (VAH/VAL/Liq/P/D entegre) ─────────────────────
    const r25WickTrapMap = r25DetectWickTrapMap(k1h, k4h, lastPrice, {
      atrPct, pd1h, pd4h, vpvr1h, vpvr4h, liq1h, liq4h
    });
    if (r25WickTrapMap?.upperTrap) {
      shortScore += Math.min(22, 12 + Math.round(r25WickTrapMap.upperStrength * 2));
      longScore   = Math.round(longScore * (r25WickTrapMap.upperStrength >= 4 ? 0.66 : 0.74));
      signals.short.push(`🧲 Üst fitil/direnç tuzağı → SHORT izle (${r25WickTrapMap.upperStrength}x, ${r25WickTrapMap.notes.join('+')})`);
      signals.long.push(`🚫 Üst fitil tuzağı: LONG riski yüksek (${r25WickTrapMap.upperStrength}x)`);
    }
    if (r25WickTrapMap?.lowerTrap) {
      longScore  += Math.min(22, 12 + Math.round(r25WickTrapMap.lowerStrength * 2));
      shortScore  = Math.round(shortScore * (r25WickTrapMap.lowerStrength >= 4 ? 0.66 : 0.74));
      signals.long.push(`🧲 Alt fitil/destek tuzağı → LONG izle (${r25WickTrapMap.lowerStrength}x, ${r25WickTrapMap.notes.join('+')})`);
    }
    // ── R25 Wick Trap bitti ──────────────────────────────────────────────────

    // ══════════════════════════════════════════════════════════════════════════
    // ── R27: MM AVLAMA MODÜLLERİ (5 dedektör, önem sırasına göre) ─────────────
    // Öncelik: Spread Hızlanması > Absorption > BTC Kopuşu > Hacim Çürümesi > Tape
    // ══════════════════════════════════════════════════════════════════════════
    {
      // ── 1. SPREAD HIZLANMASI (Hard veto — MM likidite çekiyor) ──────────────
      // MM büyük hamle öncesi emir kitabından çekilir → spread ani genişler
      const _sh = r27SpreadHistory.get(full);
      if (_sh && _sh.prev3m > 0 && _sh.cur > 0) {
        const _sv = (_sh.cur - _sh.prev3m) / _sh.prev3m;
        if (_sv > 0.45) { // %45+ 3dk'da spread genişlemesi
          // Hard veto — her iki yön için de tehlikeli
          longScore  = Math.round(longScore  * 0.55);
          shortScore = Math.round(shortScore * 0.55);
          signals.long.push(`🚨 Spread hızlanması +${(_sv*100).toFixed(0)}%: MM likidite çekiyor`);
          signals.short.push(`🚨 Spread hızlanması: MM çekiliyor`);
        } else if (_sv > 0.25) { // %25+ uyarı
          longScore  = Math.round(longScore  * 0.78);
          shortScore = Math.round(shortScore * 0.78);
          signals.long.push(`⚠️ Spread genişliyor +${(_sv*100).toFixed(0)}%: likidite azalıyor`);
        }
      }

      // ── 2. ABSORPTION (MM dağıtım tespiti — yüksek hacim + fiyat kıpırdamıyor) ──
      // MM retail alımlarını emerek satar; hacim var ama fiyat hareket etmez
      if (k5m && k5m.length >= 25) {
        const _lastN  = k5m.slice(-5);
        const _baseN  = k5m.slice(-25, -5);
        const _recVol = _lastN.reduce((s,c) => s + Number(c[5]), 0);
        const _avgVol = _baseN.reduce((s,c) => s + Number(c[5]), 0) / _baseN.length;
        // Fiyat hareketi: son 5 mum giriş-çıkış farkı
        const _priceMove = _avgVol > 0
          ? Math.abs(Number(_lastN.at(-1)[4]) - Number(_lastN[0][1])) / lastPrice * 100
          : 999;
        const _absorption = _avgVol > 0
          && _recVol > _avgVol * 5 * 2.2  // 5 mumda toplam hacim avg'nin 11x üstü
          && _priceMove < atrPct * 0.30;   // Ama fiyat neredeyse kıpırdamadı
        if (_absorption) {
          // Long tarafına ağır ceza (MM muhtemelen satıyor)
          longScore  = Math.round(longScore  * 0.62);
          shortScore = Math.min(shortScore + 12, 100);
          signals.long.push(`🧲 Absorption: hacim ${(_recVol/_avgVol/5).toFixed(1)}x yüksek + fiyat ${_priceMove.toFixed(2)}% hareket → MM emme`);
          signals.short.push(`🧲 Absorption tespiti → SHORT lehine`);
        }
      }

      // ── 3. BTC KORELASYON KOPUŞU (izole pump = manipülasyon uyarısı) ─────────
      // Organik hareket BTC ile korele olur; coin tek başına büyük hareket = şüphe
      if (k5m && k5m.length >= 3 && btcPriceRef.p > 0) {
        const _coinChg5 = (Number(k5m.at(-1)[4]) - Number(k5m.at(-3)?.[1] || k5m.at(-2)[1]))
                        / Number(k5m.at(-2)[1]) * 100;
        const _btcChg   = btcChange5mCache;
        if (Math.abs(_coinChg5) > 1.8 && _btcChg !== 0) {
          const _div = Math.abs(_coinChg5) / (Math.abs(_btcChg) + 0.02);
          if (_div > 6) { // Coin BTC'den 6x+ daha hızlı hareket ediyor
            if (_coinChg5 > 0) { // Yukarı izole pump
              longScore  = Math.round(longScore  * 0.70);
              signals.long.push(`📡 BTC kopuşu: coin +${_coinChg5.toFixed(1)}% / BTC ${_btcChg.toFixed(2)}% → izole pump, manipülasyon riski`);
              shortScore = Math.min(shortScore + 8, 100); // Ters yön fırsatı
            } else { // Aşağı izole dump
              shortScore = Math.round(shortScore * 0.70);
              signals.short.push(`📡 BTC kopuşu: izole dump → short riski`);
              longScore  = Math.min(longScore  + 8, 100);
            }
          } else if (_div > 3) { // Orta uyarı
            if (_coinChg5 > 0) longScore  = Math.round(longScore  * 0.82);
            else               shortScore = Math.round(shortScore * 0.82);
          }
        }
      }

      // ── 4. HACİM ÇÜRÜMESI (MM pump bitti → SHORT fırsatı) ────────────────────
      // Pump sonrası hacim hızla kuruyorsa MM dağıtım tamamladı = dönüş yakın
      if (k5m && k5m.length >= 20) {
        const _last20  = k5m.slice(-20);
        const _peakVol = Math.max(..._last20.map(c => Number(c[5])));
        const _curVol  = Number(k5m.at(-1)[5]);
        const _pxChg20 = _last20.length > 1
          ? (Number(k5m.at(-1)[4]) - Number(_last20[0][1])) / Number(_last20[0][1]) * 100
          : 0;
        const _decay   = _peakVol > 0 ? (_peakVol - _curVol) / _peakVol : 0;
        if (_decay > 0.70 && _pxChg20 > 7) {
          // Pump oldu ama hacim kurudu → MM çıktı, gerçek dönüş fırsatı
          shortScore = Math.min(shortScore + 14, 100);
          longScore  = Math.round(longScore  * 0.76);
          signals.short.push(`⚡ Hacim çürümesi: +${_pxChg20.toFixed(1)}% pump sonrası hacim %${(_decay*100).toFixed(0)} düştü → MM çıkış`);
          signals.long.push(`⚡ Hacim çürümesi: pump zayıflıyor`);
        }
      }

      // ── 5. TAPE BOYAMA (Algo düzenli tick = sahte hareket) ───────────────────
      // MM grafı boyamak için eşit büyüklükte, eşit aralıklı algo emirleri atar
      // Bu pattern düşük spread + yüksek fiyat trendi yanılgısı yaratır
      const _eng = tickStore?.get?.(full);
      if (_eng?.candles?.length >= 8) {
        const _recentCandles = _eng.candles.slice(-8);
        const _tradeCounts   = _recentCandles.map(c => c.trades || 0).filter(t => t > 0);
        if (_tradeCounts.length >= 6) {
          const _mean = _tradeCounts.reduce((s,v) => s+v, 0) / _tradeCounts.length;
          const _std  = Math.sqrt(_tradeCounts.reduce((s,v) => s+(v-_mean)**2, 0) / _tradeCounts.length);
          const _cv   = _mean > 0 ? _std / _mean : 1;
          // CV < 0.12 = çok düzenli trade sayısı = algo boyama
          if (_cv < 0.12 && _mean > 5) {
            longScore  = Math.round(longScore  * 0.76);
            shortScore = Math.round(shortScore * 0.76);
            signals.long.push('🎨 Tape boyama: düzenli tick → sahte trend şüphesi');
          }
        }
      }
    }
    // ── R27 MM Avlama Modülleri bitti ─────────────────────────────────────────

    // ── R32 PATTERN + RENKO SKORU — körleştirmeyen grafik okuma katmanı ──
    // Bu katman sadece amplifier/risk filtresi olarak çalışır. Tek başına A/B+ açtırmaz.
    // Pozitif pattern bonusları toplam +35 ile sınırlıdır; Renko spike/range cezaları ayrı çalışır.
    {
      const R32_PATTERN_CAP = 35;
      let r32LongBoost = 0, r32ShortBoost = 0;
      const addR32Long = (pts, msg) => {
        const add = Math.max(0, Math.min(Number(pts)||0, R32_PATTERN_CAP - r32LongBoost));
        if (add > 0) { longScore = Math.min(longScore + add, 100); r32LongBoost += add; if (msg) signals.long.push(msg.replace(/\+\d+/, `+${add}`)); }
        return add;
      };
      const addR32Short = (pts, msg) => {
        const add = Math.max(0, Math.min(Number(pts)||0, R32_PATTERN_CAP - r32ShortBoost));
        if (add > 0) { shortScore = Math.min(shortScore + add, 100); r32ShortBoost += add; if (msg) signals.short.push(msg.replace(/\+\d+/, `+${add}`)); }
        return add;
      };

      // 1) Heikin Ashi: 5m zamanlama + 1h bağlam, ters 1h'e kör long/short basmaz.
      const haCombo = (_ha5m.signal === 'STRONG_BULL' && _ha1h.signal !== 'STRONG_BEAR') ? 'BULL'
        : (_ha5m.signal === 'STRONG_BEAR' && _ha1h.signal !== 'STRONG_BULL') ? 'BEAR'
        : (_ha5m.signal === 'BULL' || _ha1h.signal === 'STRONG_BULL') ? 'MILD_BULL'
        : (_ha5m.signal === 'BEAR' || _ha1h.signal === 'STRONG_BEAR') ? 'MILD_BEAR'
        : _ha5m.signal === 'WEAKENING' ? 'WEAKENING' : 'NEUTRAL';
      if (haCombo === 'BULL') addR32Long(10, '🕯️ HA güçlü yükseliş +10');
      if (haCombo === 'MILD_BULL') addR32Long(5, null);
      if (haCombo === 'BEAR') addR32Short(10, '🕯️ HA güçlü düşüş +10');
      if (haCombo === 'MILD_BEAR') addR32Short(5, null);
      if (haCombo === 'WEAKENING' && longScore > shortScore) { longScore = Math.round(longScore * 0.88); signals.long.push('⚠️ HA zayıflıyor'); }
      if (haCombo === 'WEAKENING' && shortScore > longScore) shortScore = Math.round(shortScore * 0.88);

      // 2) CDL paternleri: 5m giriş mumu, 1h bağlam; karşı patern varsa puan keser.
      const cdlBullStr5 = (_cdl5m.bull||[]).reduce((s,p)=>s+p.str,0);
      const cdlBearStr5 = (_cdl5m.bear||[]).reduce((s,p)=>s+p.str,0);
      const cdlBullStr1 = (_cdl1h.bull||[]).reduce((s,p)=>s+p.str,0);
      const cdlBearStr1 = (_cdl1h.bear||[]).reduce((s,p)=>s+p.str,0);
      const topBull5 = (_cdl5m.bull||[]).slice().sort((a,b)=>b.str-a.str).slice(0,2).map(p=>p.name).join('+');
      const topBear5 = (_cdl5m.bear||[]).slice().sort((a,b)=>b.str-a.str).slice(0,2).map(p=>p.name).join('+');
      if (cdlBullStr5 >= 4) { const b = Math.min(cdlBullStr5 >= 8 ? 18 : cdlBullStr5 >= 5 ? 12 : 8, 18); addR32Long(b, `🕯️ CDL ${topBull5} +${b}`); }
      if (cdlBearStr5 >= 4) { const b = Math.min(cdlBearStr5 >= 8 ? 18 : cdlBearStr5 >= 5 ? 12 : 8, 18); addR32Short(b, `🕯️ CDL ${topBear5} +${b}`); }
      if (cdlBullStr1 >= 5 && longScore > shortScore) addR32Long(8, null);
      if (cdlBearStr1 >= 5 && shortScore > longScore) addR32Short(8, null);
      if (cdlBearStr5 >= 5 && longScore > shortScore && cdlBullStr5 < cdlBearStr5) { longScore = Math.round(longScore * 0.82); signals.long.push(`⚠️ CDL karşı: ${topBear5}`); }
      if (cdlBullStr5 >= 5 && shortScore > longScore && cdlBullStr5 > cdlBearStr5) shortScore = Math.round(shortScore * 0.82);

      // 3) ZigZag chart formasyonları: 1h yapı okur; tepede long / dipte short kovalamayı cezalar.
      const cBull = _chart?.bullScore || 0, cBear = _chart?.bearScore || 0;
      const bullPatt = (_chart?.patterns||[]).filter(p=>p.dir==='BULL').map(p=>p.name).slice(0,2).join('+');
      const bearPatt = (_chart?.patterns||[]).filter(p=>p.dir==='BEAR').map(p=>p.name).slice(0,2).join('+');
      if (cBull > 0 && cBull > cBear) { const b = Math.min(Math.round(cBull * 0.55), 22); addR32Long(b, b >= 8 ? `📐 ${bullPatt} +${b}` : null); }
      if (cBear > 0 && cBear > cBull) { const b = Math.min(Math.round(cBear * 0.55), 22); addR32Short(b, b >= 8 ? `📐 ${bearPatt} +${b}` : null); }
      if ((_chart?.patterns||[]).some(p=>['HeadShoulders','DoubleTop'].includes(p.name)) && longScore > shortScore) { longScore = Math.round(longScore * 0.72); signals.long.push('📐 H&S/DoubleTop: LONG tehlikeli'); }
      if ((_chart?.patterns||[]).some(p=>['InvHeadShoulders','DoubleBottom'].includes(p.name)) && shortScore > longScore) shortScore = Math.round(shortScore * 0.72);

      // 4) Harmonik PRZ: 4h PRZ bağlamı; sadece güçlü mevcut yönü destekler.
      const hBull = _harm?.bullScore || 0, hBear = _harm?.bearScore || 0;
      const bullHarm = (_harm?.patterns||[]).filter(p=>p.dir==='BULL').map(p=>p.name).slice(0,1).join('');
      const bearHarm = (_harm?.patterns||[]).filter(p=>p.dir==='BEAR').map(p=>p.name).slice(0,1).join('');
      if (hBull > 0 && hBull > hBear) { const b = Math.min(Math.round(hBull * 0.60), 22); addR32Long(b, b >= 8 ? `🔮 Harmonik PRZ: ${bullHarm} +${b}` : null); }
      if (hBear > 0 && hBear > hBull) { const b = Math.min(Math.round(hBear * 0.60), 22); addR32Short(b, b >= 8 ? `🔮 Harmonik PRZ: ${bearHarm} +${b}` : null); }

      // 5) Renko manipülasyon filtresi: spike/range önce güvenlik, trend sadece destek.
      if (_renko && _renko.brickCount >= 4) {
        if (_renko.spikeTrap) {
          if (longScore > shortScore) { longScore = Math.round(longScore * 0.72); signals.long.push('🧱 Renko spike tuzağı: MM manipülasyonu'); }
          else { shortScore = Math.round(shortScore * 0.72); signals.short.push('🧱 Renko spike tuzağı'); }
        } else if (_renko.ranging) {
          longScore = Math.round(longScore * 0.85); shortScore = Math.round(shortScore * 0.85);
          signals.long.push(`🧱 Renko ranging: yön belirsiz (${_renko.bulls}↑/${_renko.bears}↓)`);
        } else if (_renko.signal === 'STRONG_BULL' && longScore > shortScore) {
          addR32Long(12, `🧱 Renko güçlü yükseliş (${_renko.consecutive} brick) +12`);
        } else if (_renko.signal === 'STRONG_BEAR' && shortScore > longScore) {
          addR32Short(12, `🧱 Renko güçlü düşüş (${_renko.consecutive} brick) +12`);
        } else if ((_renko.signal === 'BEAR' || _renko.signal === 'STRONG_BEAR') && longScore > shortScore) {
          longScore = Math.round(longScore * 0.82); signals.long.push('🧱 Renko bear, LONG karşıt');
        } else if ((_renko.signal === 'BULL' || _renko.signal === 'STRONG_BULL') && shortScore > longScore) {
          shortScore = Math.round(shortScore * 0.82);
        }
      }
      if (r32LongBoost >= R32_PATTERN_CAP) signals.long.push('🔒 R32 pattern bonus tavanı +35');
      if (r32ShortBoost >= R32_PATTERN_CAP) signals.short.push('🔒 R32 pattern bonus tavanı +35');
    }
    // ── R32 Pattern + Renko bitti ─────────────────────────────────────────────

    // ── R31 MODÜL KONSENSÜS SAYACI — tek modül değil, aile çoğunluğu konuşsun ──
    // Amaç: Sweep/StopHunt gibi hızlı sinyaller tek başına A/B+ şişirmesin.
    // CMF + RSI Div + EWO + Weis + MTF + Wyckoff + ChoCH + Premium/Discount + VPVR + VPIN
    // aynı yönde 3+ oy verirse ters yöne otomatik emir ciddi cezalanır.
    const r31ModuleConsensus = (() => {
      let bearCount = 0, bullCount = 0;
      const bearReasons = [], bullReasons = [];
      const addBear = (w, r) => { bearCount += Number(w)||0; bearReasons.push(r); };
      const addBull = (w, r) => { bullCount += Number(w)||0; bullReasons.push(r); };

      if (cmf1h?.signal === 'STRONG_SELL') addBear(1.2, 'CMF1h');
      else if (cmf1h?.signal === 'SELL') addBear(0.8, 'CMF1h');
      if (cmf4h?.signal === 'STRONG_SELL') addBear(1.5, 'CMF4h');
      else if (cmf4h?.signal === 'SELL') addBear(0.7, 'CMF4h');
      if (cmf1h?.signal === 'STRONG_BUY') addBull(1.2, 'CMF1h');
      else if (cmf1h?.signal === 'BUY') addBull(0.8, 'CMF1h');
      if (cmf4h?.signal === 'STRONG_BUY') addBull(1.5, 'CMF4h');
      else if (cmf4h?.signal === 'BUY') addBull(0.7, 'CMF4h');

      if (rsiDiv1h?.bearDiv || rsiDiv1h?.hiddenBear) addBear(1.0, 'RSIDiv1h');
      if (rsiDiv4h?.bearDiv || rsiDiv4h?.hiddenBear) addBear(1.3, 'RSIDiv4h');
      if (rsiDiv1h?.bullDiv || rsiDiv1h?.hiddenBull) addBull(1.0, 'RSIDiv1h');
      if (rsiDiv4h?.bullDiv || rsiDiv4h?.hiddenBull) addBull(1.3, 'RSIDiv4h');

      if (ewo1h?.signal === 'BEAR_WAVE') addBear(Math.abs(Number(ewo1h.value)||0) > 0.5 ? 0.8 : 0.5, 'EWO');
      if (ewo1h?.signal === 'BULL_WAVE') addBull(Math.abs(Number(ewo1h.value)||0) > 0.5 ? 0.8 : 0.5, 'EWO');

      if (weis1h?.signal === 'BEAR_EFFORT') addBear(0.8, 'Weis');
      if (weis1h?.signal === 'BULL_EFFORT') addBull(0.8, 'Weis');

      if (mtfBias?.bias === 'STRONG_BEAR') addBear(1.5, 'MTF↓↓');
      else if (mtfBias?.bias === 'BEAR') addBear(0.9, 'MTF↓');
      if (mtfBias?.bias === 'STRONG_BULL') addBull(1.5, 'MTF↑↑');
      else if (mtfBias?.bias === 'BULL') addBull(0.9, 'MTF↑');

      const wyEvents1h = Array.isArray(wyckoff1h?.recentEvents) ? wyckoff1h.recentEvents : [];
      const wyEvents4h = Array.isArray(wyckoff4h?.recentEvents) ? wyckoff4h.recentEvents : [];
      const wyEvents = [...wyEvents1h, ...wyEvents4h];
      if (wyEvents.some(e => ['UTAD','UTAD_CONFIRMED'].includes(e?.type))) addBear(1.5, 'UTAD');
      if (wyEvents.some(e => ['SPRING','SPRING_CONFIRMED','SOS'].includes(e?.type))) addBull(1.5, 'Spring/SOS');

      if (choch1h?.signal === 'BEAR_CHOCH' || choch4h?.signal === 'BEAR_CHOCH') addBear(1.2, 'ChoCH↓');
      if (choch1h?.signal === 'BULL_CHOCH' || choch4h?.signal === 'BULL_CHOCH') addBull(1.2, 'ChoCH↑');

      const pdPct = Number(pd1h?.pct || 0);
      if (pdPct > 78 || pd1h?.signal === 'DEEP_PREMIUM') addBear(0.8, 'Premium');
      if (pdPct < 22 || pd1h?.signal === 'DEEP_DISCOUNT') addBull(0.8, 'Discount');

      if (vpvr1h?.vah && lastPrice > vpvr1h.vah * 1.005) addBear(0.7, 'VP_VAH');
      if (vpvr1h?.val && lastPrice < vpvr1h.val * 0.995) addBull(0.7, 'VP_VAL');

      if (tickData?.vpin?.direction === 'SELL_DOMINANT') addBear(0.8, 'VPIN↓');
      if (tickData?.vpin?.direction === 'BUY_DOMINANT') addBull(0.8, 'VPIN↑');

      const bc = Math.round(bearCount * 10) / 10;
      const bl = Math.round(bullCount * 10) / 10;
      const ctx = { bear:bc, bull:bl, bearReasons:bearReasons.slice(0,8), bullReasons:bullReasons.slice(0,8), applied:null };

      if (bc >= 3 && bc > bl + 1 && longScore > shortScore) {
        const m = bc >= 6 ? 0.42 : bc >= 5 ? 0.52 : bc >= 4 ? 0.62 : 0.72;
        longScore = Math.round(longScore * m);
        ctx.applied = { side:'LONG', mult:m, reason:'bearish_consensus' };
        signals.long.push(`🔴 Bearish Konsensüs ${bc.toFixed(1)}x (${bearReasons.slice(0,4).join('+')}) → LONG x${m.toFixed(2)}`);
      }
      if (bl >= 3 && bl > bc + 1 && shortScore > longScore) {
        const m = bl >= 6 ? 0.42 : bl >= 5 ? 0.52 : bl >= 4 ? 0.62 : 0.72;
        shortScore = Math.round(shortScore * m);
        ctx.applied = { side:'SHORT', mult:m, reason:'bullish_consensus' };
        signals.short.push(`🟢 Bullish Konsensüs ${bl.toFixed(1)}x (${bullReasons.slice(0,4).join('+')}) → SHORT x${m.toFixed(2)}`);
      }
      if (bc >= 2 && bc < 3 && longScore > shortScore) {
        longScore = Math.round(longScore * 0.85);
        ctx.applied = ctx.applied || { side:'LONG', mult:0.85, reason:'light_bearish_context' };
        signals.long.push(`⚠️ Hafif bearish bağlam ${bc.toFixed(1)}x → LONG x0.85`);
      }
      if (bl >= 2 && bl < 3 && shortScore > longScore) {
        shortScore = Math.round(shortScore * 0.85);
        ctx.applied = ctx.applied || { side:'SHORT', mult:0.85, reason:'light_bullish_context' };
        signals.short.push(`⚠️ Hafif bullish bağlam ${bl.toFixed(1)}x → SHORT x0.85`);
      }
      return ctx;
    })();

    // R29: tek merkezli bağlam terazisi. Modüller birbirini boğmasın diye ceza/bonus burada toplanır.
    let r29Context = {
      version:'R29_CONTEXT_PRIORITY', longRisk:0, shortRisk:0, longMult:1, shortMult:1,
      longAutoBlock:false, shortAutoBlock:false, preferSide:'NEUTRAL',
      notes:[], longNotes:[], shortNotes:[], tags:[],
      vwap:{}, btc:btc5mCtx, wickField:{}, marketBreadth:null, change24h:null,
      moduleConsensus:r31ModuleConsensus,
      rule:'hard gate değil; tek bağlam terazisi + güvenlik bloğu'
    };

    // ══════════════════════════════════════════════════════════════════════════
    // ── R28: TREND & PİYASA ZEKASI (4 Özgün Modül) ────────────────────────────
    // ══════════════════════════════════════════════════════════════════════════

    // ── 1. EHLERS FISHER TRANSFORM (Açık kaynak DSP sinyali) ─────────────────
    // John Ehlers'ın fiyat serilerini Gaussian dağılıma normalize etme yöntemi.
    // Fisher > 0 ve yukarı kesiyor = güçlü al, < 0 ve aşağı = güçlü sat.
    // Kaynak: "Cybernetic Analysis for Stocks and Futures" (Ehlers 2004)
    if (k1h && k1h.length >= 12) {
      function calcFisher(kl, len=10) {
        const closes = kl.map(c => Number(c[4]));
        const highs  = kl.map(c => Number(c[2]));
        const lows   = kl.map(c => Number(c[3]));
        const results = [];
        let prevFisher = 0;
        for (let i = len-1; i < kl.length; i++) {
          const sliceH = highs.slice(i-len+1, i+1);
          const sliceL = lows.slice(i-len+1, i+1);
          const hh = Math.max(...sliceH), ll = Math.min(...sliceL);
          const range = hh - ll;
          let val = range > 0 ? (closes[i] - ll) / range * 2 - 1 : 0;
          val = Math.max(-0.999, Math.min(0.999, val));
          const fish = 0.5 * Math.log((1 + val) / (1 - val));
          results.push({ fish, prev: prevFisher });
          prevFisher = fish;
        }
        return results.slice(-3);
      }
      const fisherData = calcFisher(k1h, 10);
      if (fisherData.length >= 2) {
        const fNow  = fisherData.at(-1)?.fish || 0;
        const fPrev = fisherData.at(-2)?.fish || 0;
        // Bullish kesim: Fisher negatiften pozitife geçiyor
        if (fNow > fPrev && fNow > 0 && fPrev < 0.3) {
          longScore += 10;
          signals.long.push(`🎯 Fisher Kesimi ↑ (${fNow.toFixed(2)}): güçlü tersine dönüş sinyali`);
        }
        // Bearish kesim
        if (fNow < fPrev && fNow < 0 && fPrev > -0.3) {
          shortScore += 10;
          signals.short.push(`🎯 Fisher Kesimi ↓ (${fNow.toFixed(2)}): güçlü tersine dönüş`);
        }
        // Aşırı alım/satım bölgesi: Fisher > 2.0 = dağıtım, < -2.0 = birikim
        if (fNow > 2.0) {
          longScore  = Math.round(longScore  * 0.72);
          shortScore = Math.min(shortScore + 8, 100);
          signals.long.push(`⚠️ Fisher aşırı alım (${fNow.toFixed(2)}) → dağıtım bölgesi`);
        }
        if (fNow < -2.0) {
          shortScore = Math.round(shortScore * 0.72);
          longScore  = Math.min(longScore  + 8, 100);
          signals.short.push(`⚠️ Fisher aşırı satım (${fNow.toFixed(2)}) → birikim bölgesi`);
        }
      }
    }

    // ── 2. DÜŞEN BIÇAK FİLTRESİ (Falling Knife) ─────────────────────────────
    // Son 4 saatlik 1h mum kapanışları arka arkaya aşağıysa → LONG girme
    // FET gibi tüm gün düşen coinlerde sweep sonrası devam gelir, reversal değil
    if (k1h && k1h.length >= 6) {
      const _last4h = k1h.slice(-5, -1); // Son 4 kapanmış mum
      const _closes4h = _last4h.map(c => Number(c[4]));
      const _allDown = _closes4h.every((c, i) => i === 0 || c < _closes4h[i-1]);
      const _allUp   = _closes4h.every((c, i) => i === 0 || c > _closes4h[i-1]);
      if (_allDown) {
        // 4 arka arkaya kırmızı mum = düşen bıçak
        longScore  = Math.round(longScore  * 0.60);
        signals.long.push(`🔪 Düşen bıçak: 4 arka arkaya düşüş mumu → LONG tehlikeli`);
        shortScore = Math.min(shortScore + 10, 100);
        signals.short.push(`📉 Momentum SHORT yönünde: 4+ art arda kırmızı`);
      }
      if (_allUp) {
        // 4 arka arkaya yeşil = yükselen bıçak → short riskli
        shortScore = Math.round(shortScore * 0.60);
        signals.short.push(`🔪 Yükselen momentum: 4+ art arda yeşil → SHORT tehlikeli`);
        longScore  = Math.min(longScore + 10, 100);
      }
    }

    // ── 3. PİYASA GENELİ YÖN FİLTRESİ (Market Breadth) ─────────────────────
    // Son 10 dakikada 40 coinlin çoğunluğu bearish/bullish ise buna karşı gitme
    // FET ve ZEC aynı anda düştü → breadth ayı bölgesindeydi
    {
      const bs = marketBreadthStore.breadthScore;
      const bn = marketBreadthStore.bull;
      const be = marketBreadthStore.bear;
      const bTotal = bn + be;
      if (bTotal >= 8 && Date.now() - marketBreadthStore.ts < 15*60*1000) {
        // Güçlü ayı piyasası (%70+ sinyal bear)
        if (bs < -0.55 && longScore > shortScore) {
          longScore  = Math.round(longScore  * 0.65);
          signals.long.push(`🌍 Piyasa geneli AYICI (${be}↓/${bn}↑) → LONG cezası`);
        }
        // Güçlü boğa piyasası
        if (bs > 0.55 && shortScore > longScore) {
          shortScore = Math.round(shortScore * 0.65);
          signals.short.push(`🌍 Piyasa geneli BOĞA (${bn}↑/${be}↓) → SHORT cezası`);
        }
        // Sinyal yönüne göre bonus
        if (bs > 0.40 && longScore > shortScore) {
          longScore = Math.min(longScore + 8, 100);
          signals.long.push(`🌍 Piyasa boğa akışı (${bn}↑): LONG lehine`);
        }
        if (bs < -0.40 && shortScore > longScore) {
          shortScore = Math.min(shortScore + 8, 100);
          signals.short.push(`🌍 Piyasa ayı akışı (${be}↓): SHORT lehine`);
        }
      }
    }

    // ── 4. R29 BAĞLAM TERAZİSİ: VWAP + BTC GÖRELİ GÜÇ + 1H WICK SAHASI ──
    // EMA hard gate kaldırıldı. 5m için daha anlamlı olan şey: ödenen ortalama fiyat (VWAP),
    // BTC'ye göre para girişi/çıkışı ve 1h/4h direnç-destek fitil yoğunluğu.
    {
      const _coinMeta = volatilityStore.coins.find(c => c.fullSymbol===full || c.symbol===full.replace('USDT','')) || {};
      const _chg24hFromK1 = (k1h?.length >= 25 && Number(k1h.at(-25)?.[1]) > 0)
        ? (lastPrice - Number(k1h.at(-25)[1])) / Number(k1h.at(-25)[1]) * 100 : null;
      const _chg24h = Number.isFinite(Number(_coinMeta.change24h)) ? Number(_coinMeta.change24h) : (Number.isFinite(_chg24hFromK1) ? _chg24hFromK1 : 0);
      const _vwap5m  = vwap(k5m.slice(-72));   // yaklaşık son 6 saat
      const _vwap15m = vwap(k15m.slice(-40));  // yaklaşık son 10 saat
      const _last5   = k5m?.length ? Number(k5m.at(-1)[4]) : lastPrice;
      const _prev5   = k5m?.length >= 2 ? Number(k5m.at(-2)[4]) : _last5;
      const _coin15  = (k5m?.length >= 4 && Number(k5m.at(-4)[1])>0) ? ((_last5-Number(k5m.at(-4)[1]))/Number(k5m.at(-4)[1])*100) : 0;
      const _coin60  = (k5m?.length >= 13 && Number(k5m.at(-13)[1])>0) ? ((_last5-Number(k5m.at(-13)[1]))/Number(k5m.at(-13)[1])*100) : 0;
      const _rel15   = btc5mCtx.ok ? _coin15 - btc5mCtx.change15m : 0;
      const _rel60   = btc5mCtx.ok ? _coin60 - btc5mCtx.change60m : 0;
      const _flow1h  = (k15m?.length >= 5 && Number(k15m.at(-5)[1])>0) ? ((_last5-Number(k15m.at(-5)[1]))/Number(k15m.at(-5)[1])*100) : 0;
      const _flow4h  = (k1h?.length >= 5 && Number(k1h.at(-5)[1])>0) ? ((_last5-Number(k1h.at(-5)[1]))/Number(k1h.at(-5)[1])*100) : 0;

      function wickField(kl, look=36, nearPct=2.2) {
        const recent = (kl||[]).slice(-look);
        let upperCount=0, lowerCount=0, upperScore=0, lowerScore=0;
        let nearestUpper=null, nearestLower=null;
        for (const c of recent) {
          const o=Number(c[1]), h=Number(c[2]), l=Number(c[3]), cl=Number(c[4]);
          const range=h-l; if (!(range>0) || !(lastPrice>0)) continue;
          const bodyHi=Math.max(o,cl), bodyLo=Math.min(o,cl);
          const uw=(h-bodyHi)/range, lw=(bodyLo-l)/range;
          const distU=(h-lastPrice)/lastPrice*100;
          const distL=(lastPrice-l)/lastPrice*100;
          if (distU>=-0.12 && distU<=nearPct && uw>0.38) {
            upperCount++; upperScore += uw * (1 + Math.max(0, nearPct-distU)/nearPct);
            if (!nearestUpper || Math.abs(distU)<Math.abs(nearestUpper.distPct)) nearestUpper={price:+h.toFixed(8), distPct:+distU.toFixed(2), wick:+uw.toFixed(2)};
          }
          if (distL>=-0.12 && distL<=nearPct && lw>0.38) {
            lowerCount++; lowerScore += lw * (1 + Math.max(0, nearPct-distL)/nearPct);
            if (!nearestLower || Math.abs(distL)<Math.abs(nearestLower.distPct)) nearestLower={price:+l.toFixed(8), distPct:+distL.toFixed(2), wick:+lw.toFixed(2)};
          }
        }
        return { upperCount, lowerCount, upperScore:+upperScore.toFixed(2), lowerScore:+lowerScore.toFixed(2), nearestUpper, nearestLower,
          upperWall: upperCount>=3 || upperScore>=3.0, lowerWall: lowerCount>=3 || lowerScore>=3.0 };
      }
      const _wf1 = wickField(k1h, 42, Math.max(1.4, Math.min(3.2, atrPct*1.8 + 0.8)));
      const _wf4 = wickField(k4h, 36, Math.max(2.0, Math.min(5.0, atrPct*3.0 + 1.2)));
      const _upperWall = _wf1.upperWall || (_wf4.upperScore>=2.4);
      const _lowerWall = _wf1.lowerWall || (_wf4.lowerScore>=2.4);
      const _nearPremium = pd1h?.zone==='PREMIUM_HIGH' || pd4h?.zone==='PREMIUM_HIGH' || Number(pd1h?.pct||0)>78 || Number(pd4h?.pct||0)>82;
      const _nearDiscount = pd1h?.zone==='DISCOUNT_LOW' || pd4h?.zone==='DISCOUNT_LOW' || Number(pd1h?.pct||0)<22 || Number(pd4h?.pct||0)<18;
      const _aboveVwap = (_vwap5m>0 && lastPrice>_vwap5m) && (_vwap15m>0 && lastPrice>_vwap15m);
      const _belowVwap = (_vwap5m>0 && lastPrice<_vwap5m) && (_vwap15m>0 && lastPrice<_vwap15m);
      const _vwapLongReclaim = _vwap5m>0 && _prev5 < _vwap5m && _last5 > _vwap5m && (hunt15m?.direction==='BULL_HUNT' || sweep15m?.direction==='BULL_SWEEP' || hunt1h?.direction==='BULL_HUNT');
      const _vwapShortReject = _vwap5m>0 && _prev5 > _vwap5m && _last5 < _vwap5m && (hunt15m?.direction==='BEAR_HUNT' || sweep15m?.direction==='BEAR_SWEEP' || hunt1h?.direction==='BEAR_HUNT');

      const addRisk = (side, pts, note) => {
        if (side === 'LONG') { r29Context.longRisk += pts; r29Context.longNotes.push(note); }
        else { r29Context.shortRisk += pts; r29Context.shortNotes.push(note); }
        r29Context.notes.push(note);
      };
      const addTag = (tag) => { r29Context.tags.push(tag); };

      // 24h yön bilgisi: negatif coin long'a düşen bıçak riski, pozitif coin short'a sıkışma riski.
      if (_chg24h <= -5) addRisk('LONG', 28, `24h ${_chg24h.toFixed(1)}% düşüş: long düşen bıçak`);
      else if (_chg24h <= -2.2) addRisk('LONG', 18, `24h ${_chg24h.toFixed(1)}% negatif: long dikkat`);
      if (_chg24h >= 7) addRisk('SHORT', 20, `24h +${_chg24h.toFixed(1)}% güçlü: short squeeze riski`);
      else if (_chg24h >= 3.5) addRisk('SHORT', 12, `24h +${_chg24h.toFixed(1)}% pozitif: short dikkat`);

      if (_flow1h < -0.9 && _flow4h < -1.6) addRisk('LONG', 18, `1h/4h akış aşağı (${_flow1h.toFixed(1)} / ${_flow4h.toFixed(1)}%)`);
      if (_flow1h > 0.9 && _flow4h > 1.6) addRisk('SHORT', 18, `1h/4h akış yukarı (${_flow1h.toFixed(1)} / ${_flow4h.toFixed(1)}%)`);

      if (_belowVwap && !_vwapLongReclaim) addRisk('LONG', 16, 'VWAP altı: kurumsal ortalama üstünde değil');
      if (_aboveVwap && !_vwapShortReject) addRisk('SHORT', 16, 'VWAP üstü: short için ortalama üstü baskı');
      if (_vwapLongReclaim) { longScore=Math.min(longScore+12,100); addTag('VWAP_RECLAIM_LONG'); signals.long.push('🧭 VWAP reclaim + sweep: long lehine'); }
      if (_vwapShortReject) { shortScore=Math.min(shortScore+12,100); addTag('VWAP_REJECT_SHORT'); signals.short.push('🧭 VWAP reject + sweep: short lehine'); }

      if (btc5mCtx.ok && _rel15 < -0.65 && _rel60 < -0.9) addRisk('LONG', 18, `BTC'ye göre zayıf (${_rel15.toFixed(2)} / ${_rel60.toFixed(2)}%)`);
      if (btc5mCtx.ok && _rel15 > 0.65 && _rel60 > 0.9) addRisk('SHORT', 18, `BTC'ye göre güçlü (${_rel15.toFixed(2)} / ${_rel60.toFixed(2)}%)`);

      if (_upperWall || r25WickTrapMap?.upperTrap) {
        addRisk('LONG', r25WickTrapMap?.upperTrap ? 28 : 22, `1h/4h üst fitil/direnç likiditesi (${_wf1.upperCount}+${_wf4.upperCount})`);
        shortScore = Math.min(shortScore+12,100);
        signals.short.push('🧱 Üst fitil direnç sahası → short lehine');
        addTag('UPPER_WICK_FIELD');
      }
      if (_lowerWall || r25WickTrapMap?.lowerTrap) {
        addRisk('SHORT', r25WickTrapMap?.lowerTrap ? 28 : 22, `1h/4h alt fitil/destek likiditesi (${_wf1.lowerCount}+${_wf4.lowerCount})`);
        longScore = Math.min(longScore+12,100);
        signals.long.push('🧱 Alt fitil destek sahası → long lehine');
        addTag('LOWER_WICK_FIELD');
      }
      if (_nearPremium && _upperWall) addRisk('LONG', 12, 'Premium + üst fitil: long tuzak riski');
      if (_nearDiscount && _lowerWall) addRisk('SHORT', 12, 'Discount + alt fitil: short tuzak riski');

      const _bs = marketBreadthStore.breadthScore;
      const _bt = marketBreadthStore.bull + marketBreadthStore.bear;
      if (_bt >= 8 && Date.now()-marketBreadthStore.ts < 15*60*1000) {
        if (_bs < -0.48) addRisk('LONG', 16, `Piyasa geneli ayı (${marketBreadthStore.bear}↓/${marketBreadthStore.bull}↑)`);
        if (_bs >  0.48) addRisk('SHORT',16, `Piyasa geneli boğa (${marketBreadthStore.bull}↑/${marketBreadthStore.bear}↓)`);
      }

      // Tek bağlam tavanı: çok sayıda küçük ceza üst üste binip botu kör etmesin.
      r29Context.longRisk  = Math.min(100, Math.round(r29Context.longRisk));
      r29Context.shortRisk = Math.min(100, Math.round(r29Context.shortRisk));
      const riskMult = (risk) => risk>=75 ? 0.58 : risk>=60 ? 0.66 : risk>=45 ? 0.76 : risk>=30 ? 0.86 : 1;
      r29Context.longMult = riskMult(r29Context.longRisk);
      r29Context.shortMult = riskMult(r29Context.shortRisk);

      // Otomatik blok yalnızca ağır bağlam uyumsuzluğunda. Panel yine gösterir, bot kör olmaz.
      const _longRescue = _vwapLongReclaim && (r25WickTrapMap?.lowerTrap || _lowerWall) && longScore >= 76;
      const _shortRescue = _vwapShortReject && (r25WickTrapMap?.upperTrap || _upperWall) && shortScore >= 76;
      r29Context.longAutoBlock  = r29Context.longRisk  >= 76 && !_longRescue;
      r29Context.shortAutoBlock = r29Context.shortRisk >= 76 && !_shortRescue;
      if (r29Context.longRisk - r29Context.shortRisk >= 22) r29Context.preferSide = 'SHORT';
      else if (r29Context.shortRisk - r29Context.longRisk >= 22) r29Context.preferSide = 'LONG';
      else r29Context.preferSide = 'NEUTRAL';

      if (r29Context.longMult < 1) { longScore = Math.round(longScore * r29Context.longMult); signals.long.push(`⚖️ R29 bağlam LONG x${r29Context.longMult.toFixed(2)}: ${r29Context.longNotes.slice(0,2).join(' + ')}`); }
      if (r29Context.shortMult < 1) { shortScore = Math.round(shortScore * r29Context.shortMult); signals.short.push(`⚖️ R29 bağlam SHORT x${r29Context.shortMult.toFixed(2)}: ${r29Context.shortNotes.slice(0,2).join(' + ')}`); }
      if (r29Context.preferSide === 'LONG')  longScore = Math.min(longScore+8,100);
      if (r29Context.preferSide === 'SHORT') shortScore = Math.min(shortScore+8,100);

      r29Context.vwap = { vwap5m:+(_vwap5m||0).toFixed(8), vwap15m:+(_vwap15m||0).toFixed(8), above:_aboveVwap, below:_belowVwap, longReclaim:_vwapLongReclaim, shortReject:_vwapShortReject };
      r29Context.btc = { ...btc5mCtx, coin15m:+_coin15.toFixed(3), coin60m:+_coin60.toFixed(3), rel15m:+_rel15.toFixed(3), rel60m:+_rel60.toFixed(3) };
      r29Context.wickField = { oneH:_wf1, fourH:_wf4, upperWall:_upperWall, lowerWall:_lowerWall, premium:_nearPremium, discount:_nearDiscount };
      r29Context.marketBreadth = { bull:marketBreadthStore.bull, bear:marketBreadthStore.bear, score:+(marketBreadthStore.breadthScore||0).toFixed(3), ts:marketBreadthStore.ts };
      r29Context.change24h = +_chg24h.toFixed(2);
    }
    // ── R28 Trend & Piyasa Zekası bitti ──────────────────────────────────────

    // ── MM HEDEF CEZALARI — DOWN_SWEEP/GENUINE_DOWN longScore'u keser ──────────
    // MM aşağı sweep yapmak istiyorsa long girmek tuzağa girmek demektir
    if(mmTarget==='GENUINE_DOWN'&&mmConf>=50) {
      longScore=Math.round(longScore*0.55);
      signals.long.push(`⚠️ MM GENUINE_DOWN (%${mmConf}) — long tehlikeli`);
    } else if(mmTarget==='DOWN_SWEEP'&&mmConf>=40) {
      longScore=Math.round(longScore*0.70);
      signals.long.push(`⚠️ MM DOWN_SWEEP (%${mmConf}) — dikkat`);
    }
    if(mmTarget==='GENUINE_UP'&&mmConf>=50) {
      shortScore=Math.round(shortScore*0.55);
    } else if(mmTarget==='UP_SWEEP'&&mmConf>=40) {
      shortScore=Math.round(shortScore*0.70);
    }

    // ── RSI AŞIRI ALIM/SATIM CEZASI ──────────────────────────────────────────
    // RSI 4h > 75 = aşırı alım, long için tehlike bölgesi
    if(rsi4h>75) {
      longScore=Math.round(longScore*0.75);
      signals.long.push(`⚠️ RSI4h ${rsi4h} aşırı alım — long zayıflıyor`);
    }
    if(rsi4h<25) {
      shortScore=Math.round(shortScore*0.75);
    }
    // RSI 1h > 70 veya < 30 ek ceza
    if(rsi1h>70) { longScore=Math.round(longScore*0.90); }
    if(rsi1h<30) { shortScore=Math.round(shortScore*0.90); }

    // ── OI CONFIRMED_BEAR longScore cezası ───────────────────────────────────
    // OI artıyor + fiyat düşüyor = yeni short pozisyonlar açılıyor = bearish baskı
    if(oiDiv==='CONFIRMED_BEAR') {
      longScore=Math.round(longScore*0.82);
      signals.long.push(`⚠️ OI+Fiyat↓ CONFIRMED_BEAR — long baskı altında`);
    }
    if(oiDiv==='CONFIRMED_BULL') {
      shortScore=Math.round(shortScore*0.82);
    }

    // ── CVD VERİ YOK CEZASI ──────────────────────────────────────────────────
    // CVD $0 / UNKNOWN ise teyitsiz sinyal — score güvenilmez
    const cvdForCheck=getCVD(full);
    if(!cvdForCheck.valid || (cvdForCheck.buy===0 && cvdForCheck.sell===0)) {
      longScore=Math.round(longScore*0.85);
      shortScore=Math.round(shortScore*0.85);
      // CVD veri yokken A-Tier'a çıkmayı güçleştir
    }

    // ── FİNAL RİSK TAVANI — çelişkili/verisiz teyit 100/100 kart üretemez ─────
    // Multiplier tek başına yetmez; ham skor çok şişerse yine 100'e vurabilir.
    // Bu tavanlar ALLO tipi senaryoda (DOWN_SWEEP + RSI aşırı alım + CVD yok + OI bear)
    // long'u A-Tier/100 yapmayı engeller.
    const cvdInvalidForScore = (!cvdForCheck.valid || ((cvdForCheck.buy||0)===0 && (cvdForCheck.sell||0)===0));
    let longRiskCap=100, shortRiskCap=100;
    const longAdverse = [];
    const shortAdverse = [];
    if(mmTarget==='GENUINE_DOWN'&&mmConf>=50){ longRiskCap=Math.min(longRiskCap,55); longAdverse.push('MM_GENUINE_DOWN'); }
    else if(mmTarget==='DOWN_SWEEP'&&mmConf>=40){ longRiskCap=Math.min(longRiskCap,70); longAdverse.push('MM_DOWN_SWEEP'); }
    if(mmTarget==='GENUINE_UP'&&mmConf>=50){ shortRiskCap=Math.min(shortRiskCap,55); shortAdverse.push('MM_GENUINE_UP'); }
    else if(mmTarget==='UP_SWEEP'&&mmConf>=40){ shortRiskCap=Math.min(shortRiskCap,70); shortAdverse.push('MM_UP_SWEEP'); }
    if(rsi4h>78){ longRiskCap=Math.min(longRiskCap,62); longAdverse.push('RSI4H_OVERHEAT'); }
    else if(rsi4h>75){ longRiskCap=Math.min(longRiskCap,75); longAdverse.push('RSI4H_HIGH'); }
    if(rsi4h<22){ shortRiskCap=Math.min(shortRiskCap,62); shortAdverse.push('RSI4H_OVERSOLD'); }
    else if(rsi4h<25){ shortRiskCap=Math.min(shortRiskCap,75); shortAdverse.push('RSI4H_LOW'); }
    if(oiDiv==='CONFIRMED_BEAR'){ longRiskCap=Math.min(longRiskCap,70); longAdverse.push('OI_CONFIRMED_BEAR'); }
    if(oiDiv==='CONFIRMED_BULL'){ shortRiskCap=Math.min(shortRiskCap,70); shortAdverse.push('OI_CONFIRMED_BULL'); }
    if(cvdInvalidForScore){
      longRiskCap=Math.min(longRiskCap,72); shortRiskCap=Math.min(shortRiskCap,72);
      longAdverse.push('CVD_NOT_READY'); shortAdverse.push('CVD_NOT_READY');
    }
    // R29 bağlamı final risk tavanına bağla: tek merkezli tavan, EMA yok.
    if (r29Context.longRisk >= 76) { longRiskCap=Math.min(longRiskCap,58); longAdverse.push('R29_CONTEXT_HIGH'); }
    else if (r29Context.longRisk >= 60) { longRiskCap=Math.min(longRiskCap,68); longAdverse.push('R29_CONTEXT_MED'); }
    if (r29Context.shortRisk >= 76) { shortRiskCap=Math.min(shortRiskCap,58); shortAdverse.push('R29_CONTEXT_HIGH'); }
    else if (r29Context.shortRisk >= 60) { shortRiskCap=Math.min(shortRiskCap,68); shortAdverse.push('R29_CONTEXT_MED'); }

    if(longAdverse.length>=3) longRiskCap=Math.min(longRiskCap,55);
    if(longAdverse.length>=4) longRiskCap=Math.min(longRiskCap,50);
    if(shortAdverse.length>=3) shortRiskCap=Math.min(shortRiskCap,55);
    if(shortAdverse.length>=4) shortRiskCap=Math.min(shortRiskCap,50);
    longScore=Math.min(longScore,longRiskCap);
    shortScore=Math.min(shortScore,shortRiskCap);

    longScore =Math.min(Math.round(longScore*freshnessMult),100);
    shortScore=Math.min(Math.round(shortScore*freshnessMult),100);

    // ── R37: 5m erken yakalama / geç kovalamama skor düzeltmesi ───────────
    if (r37Timing?.ok) {
      if (r37Timing.long.earlyImpulse || r37Timing.long.retestOk) {
        const add = r37Timing.long.earlyImpulse ? 10 : 7;
        longScore = Math.min(longScore + add, 100);
        signals.long.push(`⚡ R37 erken LONG ${r37Timing.long.earlyImpulse?'ilk impuls':'retest'} (+${add})`);
      }
      if (r37Timing.short.earlyImpulse || r37Timing.short.retestOk) {
        const add = r37Timing.short.earlyImpulse ? 10 : 7;
        shortScore = Math.min(shortScore + add, 100);
        signals.short.push(`⚡ R37 erken SHORT ${r37Timing.short.earlyImpulse?'ilk impuls':'retest'} (+${add})`);
      }
      if (r37Timing.long.lateChase) {
        longScore = Math.round(longScore * 0.62);
        signals.long.push(`⛔ R37 geç LONG chase: ${r37Timing.long.reason}`);
      } else if (r37Timing.long.retestOnly) {
        // R38: Top mover + mikro yapı varsa retest bekleme cezası yumuşak olmalı.
        // Amaç ESPORTS/LAB gibi 5m coinleri 84→70'e düşürüp körleştirmemek.
        const _m = (r38TopMoverStrong && Number(r37Timing.long.earlyScore||0) >= 3) ? 0.94 : 0.82;
        longScore = Math.round(longScore * _m);
        signals.long.push(`⏳ R38 LONG retest bekle x${_m.toFixed(2)}: ${r37Timing.long.reason}`);
      }
      if (r37Timing.short.lateChase) {
        shortScore = Math.round(shortScore * 0.62);
        signals.short.push(`⛔ R37 geç SHORT chase: ${r37Timing.short.reason}`);
      } else if (r37Timing.short.retestOnly) {
        const _m = (r38TopMoverStrong && Number(r37Timing.short.earlyScore||0) >= 3) ? 0.94 : 0.82;
        shortScore = Math.round(shortScore * _m);
        signals.short.push(`⏳ R38 SHORT retest bekle x${_m.toFixed(2)}: ${r37Timing.short.reason}`);
      }
    }

    // ── R23: R22 BEST ÜSTÜ HASSAS SKOR KATMANI ─────────────────────────────
    // Burada ham longScore/shortScore güçlendirilir; evalDecision içinde ayrıca
    // priorityScore/terazi hesaplanır. Aynı aile bonusları tavanlıdır ki bot
    // kural yığmasıyla boğulmasın veya tek aileden aşırı şişmesin.
    const r23ScoreLedger={
      LONG:{macro:0,structure:0},
      SHORT:{macro:0,structure:0}
    };
    function r23AddScore(side, pts, label, family='macro', familyCap=28){
      const s=side==='SHORT'?'SHORT':'LONG';
      const capLeft=Math.max(0, Number(familyCap||0) - Number(r23ScoreLedger[s][family]||0));
      const add=Math.max(0, Math.min(Number(pts||0), capLeft));
      if(add<=0){
        const msg=`⚖️ ${label} aile tavanına takıldı (${familyCap})`;
        if(s==='LONG')signals.long.push(msg); else signals.short.push(msg);
        return 0;
      }
      r23ScoreLedger[s][family]=(r23ScoreLedger[s][family]||0)+add;
      if(s==='LONG') longScore=Math.min(longScore+add,100);
      else shortScore=Math.min(shortScore+add,100);
      if(s==='LONG')signals.long.push(`${label} +${add}`); else signals.short.push(`${label} +${add}`);
      return add;
    }
    function r23MulScore(side, mult, label){
      const s=side==='SHORT'?'SHORT':'LONG';
      if(s==='LONG'){ longScore=Math.round(longScore*mult); signals.long.push(label); }
      else { shortScore=Math.round(shortScore*mult); signals.short.push(label); }
    }

    // FONLAMA TUZAĞI SKORU — artık rapordaki gibi STRONG/NORMAL ayrımı gerçekten uygulanır.
    {
      const _ftSweepHunt = (sweep1h?.confirmed) || (hunt1h?.hunted) || !!(amd5m?.tickConfirm);
      if (_ftSweepHunt) {
        const _fundMomBull = fundMom?.ok && (fundMom.signal==='STRONG_LONG_BIAS' || fundMom.signal==='LONG_BIAS');
        const _fundMomBear = fundMom?.ok && (fundMom.signal==='STRONG_SHORT_BIAS' || fundMom.signal==='SHORT_BIAS');
        const _ftLong = (curFund < -0.08 || fundSig==='EXTREME_NEGATIVE') &&
          (oiChg1h < -0.4 || oiDiv==='SHORT_SQUEEZE' || oiDiv==='NEUTRAL');
        const _ftShort = (curFund > 0.08 || fundSig==='EXTREME_POSITIVE') &&
          (oiChg1h < -0.4 || oiDiv==='LONG_LIQUIDATION' || oiDiv==='NEUTRAL');
        if (_ftLong) {
          const strong = _fundMomBull || !fundMom?.ok;
          r23AddScore('LONG', strong ? 20 : 15, `💣 Fonlama Tuzağı ${strong?'STRONG':'NORMAL'}: Fund ${curFund.toFixed(4)}% + sweep/hunt + OI ${oiDiv}`, 'macro', 28);
        }
        if (_ftShort) {
          const strong = _fundMomBear || !fundMom?.ok;
          r23AddScore('SHORT', strong ? 20 : 15, `💣 Fonlama Tuzağı SHORT ${strong?'STRONG':'NORMAL'}: Fund ${curFund.toFixed(4)}% + sweep/hunt + OI ${oiDiv}`, 'macro', 28);
        }
      }
    }

    // LİKİDASYON ŞELALESİ SKORU — 1h + 4h cluster artık gerçekten hesaba katılır.
    {
      const _up1 = liq1h?.buyLiq?.[0];
      const _dn1 = liq1h?.sellLiq?.[0];
      const _up4 = liq4h?.buyLiq?.[0];
      const _dn4 = liq4h?.sellLiq?.[0];
      const _oiBig = Math.abs(oiChg1h) > 1.5 || Math.abs(oiChg4h) > 4 || ['SHORT_SQUEEZE','LONG_LIQUIDATION'].includes(oiDiv);
      const _near = (l,m=1.0,min=0.35)=>!!(l && Math.abs(Number(l.distPct||99)) < Math.max(min, atrPct*m));
      const _near4 = (l)=>!!(l && Math.abs(Number(l.distPct||99)) < Math.max(1.2, atrPct*2.2));
      const _upStrength = (_near(_up1,0.95,0.35)?Number(_up1.strength||0):0) + (_near4(_up4)?Number(_up4.strength||0)*0.65:0);
      const _dnStrength = (_near(_dn1,0.95,0.35)?Number(_dn1.strength||0):0) + (_near4(_dn4)?Number(_dn4.strength||0)*0.65:0);
      const _bullSweep = (sweep1h?.confirmed && sweep1h.direction==='BULL_SWEEP') || (hunt1h?.hunted && hunt1h.direction==='BULL_HUNT');
      const _bearSweep = (sweep1h?.confirmed && sweep1h.direction==='BEAR_SWEEP') || (hunt1h?.hunted && hunt1h.direction==='BEAR_HUNT');
      const _cascOppL = _bullSweep && _upStrength >= 2 && _oiBig;
      const _cascOppS = _bearSweep && _dnStrength >= 2 && _oiBig;
      const _cascRiskL = _dnStrength >= 3 && (oiDiv==='CONFIRMED_BEAR' || oiDiv==='LONG_LIQUIDATION');
      const _cascRiskS = _upStrength >= 3 && (oiDiv==='CONFIRMED_BULL' || oiDiv==='SHORT_SQUEEZE');
      if (_cascOppL) r23AddScore('LONG', 14, `🌊 Liq Şelalesi ↑: üst cluster ${_upStrength.toFixed(1)}x + OI`, 'macro', 28);
      if (_cascOppS) r23AddScore('SHORT', 14, `🌊 Liq Şelalesi ↓: alt cluster ${_dnStrength.toFixed(1)}x + OI`, 'macro', 28);
      if (_cascRiskL) r23MulScore('LONG',0.80,`⚠️ Liq Şelalesi Riski ↓: alt cluster ${_dnStrength.toFixed(1)}x`);
      if (_cascRiskS) r23MulScore('SHORT',0.80,`⚠️ Liq Şelalesi Riski ↑: üst cluster ${_upStrength.toFixed(1)}x`);
    }

    // SEKTÖR ROTASYON SKORU — sektör nabzı skor + teraziye ayrı ayrı bağlanır.
    {
      const _sideRec = longScore > shortScore ? 'LONG' : shortScore > longScore ? 'SHORT' : 'WAIT';
      if (_sideRec !== 'WAIT') {
        const rotBias = r22RotationBias(full, _sideRec);
        if (rotBias.active) {
          const _bonus = rotBias.tag === 'SECTOR_ROTATION_STRONG' ? 14 : Math.min(10, rotBias.score + 2);
          r23AddScore(_sideRec, _bonus, `🎯 Sektör Rotasyon [${rotBias.sector}]: ${rotBias.symbols.join('/')} aynı yön`, 'macro', 28);
        }
      }
    }

    // KAÇ. TEST? İLK TEST? — taze ilk test güçlenir, 3+ test yıpranır.
    {
      if (sweep1h?.confirmed) {
        const _sweepStr = sweep1h.sweepStrength || 1;
        if (_sweepStr <= 1) {
          if (sweep1h.direction==='BULL_SWEEP') r23AddScore('LONG',12,'🥇 İlk Seviye Testi — en temiz giriş','structure',18);
          else r23AddScore('SHORT',12,'🥇 İlk Seviye Testi — en temiz giriş','structure',18);
        } else if (_sweepStr === 2) {
          if (sweep1h.direction==='BULL_SWEEP') r23AddScore('LONG',5,'🥈 2. Test — hâlâ geçerli','structure',18);
          else r23AddScore('SHORT',5,'🥈 2. Test — hâlâ geçerli','structure',18);
        } else {
          if (sweep1h.direction==='BULL_SWEEP') r23MulScore('LONG',0.85,`⚠️ ${_sweepStr}. Test — seviye yorgun`);
          else r23MulScore('SHORT',0.85,`⚠️ ${_sweepStr}. Test — seviye yorgun`);
        }
      }
    }
    // ── R23 skor katmanı bitti ──────────────────────────────────────────────────

    // ── R42: FİNAL RİSK TAVANI TEKRAR UYGULAMA ────────────────────────────────
    // R41'e kadar riskCap R23/R37 ek puanlarından önce uygulanıyordu. Sonradan gelen
    // fonlama/likidite/graph bonusları CVD_NOT_READY, OI ters, MM ters, RSI uç gibi
    // risk tavanlarını tekrar yukarı şişirebiliyordu. AIGENSYN tarzı A/94-A/100
    // hatalarının ana sessiz kökü buydu. Bu tavan en son, karar seçilmeden hemen önce
    // yeniden uygulanır.
    if (longRiskCap < 100 && longScore > longRiskCap) {
      signals.long.push(`🛡️ R42 final risk tavanı: ${longScore}→${longRiskCap} (${longAdverse.join('+')})`);
      longScore = longRiskCap;
    }
    if (shortRiskCap < 100 && shortScore > shortRiskCap) {
      signals.short.push(`🛡️ R42 final risk tavanı: ${shortScore}→${shortRiskCap} (${shortAdverse.join('+')})`);
      shortScore = shortRiskCap;
    }

    // ── İKİ KATMANLI KARAR MEKANİZMASI ─────────────────────────────────────────
      // A-Tier: Yüksek güven → otomatik işlem açılır
      // B-Tier: Orta güven  → sinyal gösterilir, elle karar ver
      const rawRec=longScore>shortScore&&longScore>=50?'LONG':
                   shortScore>longScore&&shortScore>=50?'SHORT':'WAIT';

      // ── PRO TP/SL — SİNYAL YÖNÜNE GÖRE HESAPLA ──────────────────────────────
      // UYARI: calcProTPSL'e her zaman gerçek sinyal yönünü ver.
      // MM hedefi SL/TP side'ını belirlemez; aksi halde LONG sinyale SHORT TP/SL yazılır.
      const proTPSLSide = rawRec==='SHORT' ? 'SHORT' : 'LONG';
      const proTPSL=calcProTPSL(proTPSLSide,lastPrice,atr1h,atr4h,liq1h,liq4h,ob1h,ob4h,k1h);

      // ── SMART PRIORITY KARAR MOTORU — A / B+ / B TERAZİSİ ───────────────
      // R123: minAutoScore evalDecision dışına alındı. Tek Beyin karar seçimi de aynı eşik bilgisini kullanır.
      const minAutoScore = Math.max(55, Number(autoConfig?.minScore || 72));

      // R20: Modül sayma / sert kural yığma yerine ağırlıklı karar.
      // CVD veri yoksa tek başına veto değildir; yalnızca güven kırpar.
      // Gerçek hard veto sadece likidite, aşırı ATR, kesin ters MM/funding gibi teknik risklerde çalışır.
      function evalDecision(side){
        if(side==='WAIT')return{pass:false,tier:'WAIT',score:0,reasons:[],blocks:[],autoOk:false,priorityScore:0};
        const isL=side==='LONG';
        const sw1=sweep1h,wy1=wyckoff1h;
        const wy15=wyckoff15m, wy4=wyckoff4h;
        const cvdD=getCVD(full);
        const liqD=getLiqData(full);
        const sc=isL?longScore:shortScore;

        // Giriş sinyali: otomatik için en az bir gerçek entry izi isteriz.
        const hasEntry=
          (sw1?.confirmed&&(isL?sw1.direction==='BULL_SWEEP':sw1.direction==='BEAR_SWEEP'))||
          (sweep15m?.confirmed&&(isL?sweep15m.direction==='BULL_SWEEP':sweep15m.direction==='BEAR_SWEEP'))||
          (sweep4h?.confirmed&&(isL?sweep4h.direction==='BULL_SWEEP':sweep4h.direction==='BEAR_SWEEP'))||
          (tickData?.tickSweep?.type===(isL?'BULL_SWEEP':'BEAR_SWEEP')&&tickData.tickSweep.fresh)||
          (amd5m?.signal===(isL?'AMD_LONG':'AMD_SHORT'))||
          (wy1?.recentEvents?.some(e=>isL?(e.type==='SPRING'||e.type==='SOS'):e.type==='UTAD'))||
          (tickData?.imbalance?.bull&&tickData.whaleBias==='WHALE_BUY'&&isL)||
          (tickData?.imbalance?.bear&&tickData.whaleBias==='WHALE_SELL'&&!isL)||
          (tickData?.deltaFlip==='BEAR_TO_BULL'&&tickData.whaleBias==='WHALE_BUY'&&isL)||
          (tickData?.deltaFlip==='BULL_TO_BEAR'&&tickData.whaleBias==='WHALE_SELL'&&!isL);

        const softEntry=
          (sw1?.swept&&!sw1?.confirmed)||
          (isL?(ob1h?.bullOB&&lastPrice<=ob1h.bullOB.high*1.01):(ob1h?.bearOB&&lastPrice>=ob1h.bearOB.low*0.99))||
          mmTarget===(isL?'GENUINE_UP':'GENUINE_DOWN')||
          mmTarget===(isL?'UP_SWEEP':'DOWN_SWEEP');

        // CVD / tick: ikisi de yoksa veri eksik kabul edilir, tek başına veto değildir.
        const cvdRatio=Number.isFinite(Number(cvdD?.ratio)) ? Number(cvdD.ratio) : 50; // R43: 0%/100% CVD değerlerini 50'ye düşürme
        const cvdValid=!!(cvdD?.valid && ((cvdD.buy||0)+(cvdD.sell||0)>0));
        const deltaTrend=String(tickData?.deltaTrend||'UNKNOWN').toUpperCase();
        const tickDeltaKnown=!!(deltaTrend && deltaTrend!=='UNKNOWN');
        const tickDeltaOk=tickDeltaKnown ? (isL?deltaTrend==='BULL':deltaTrend==='BEAR') : false;
        const cvdSideOk=cvdValid?(isL?cvdRatio>55:cvdRatio<45):false; // R41B+R42: nötr CVD (50%) tuzak bypass edemez
        const deltaOkStrict=cvdSideOk||(tickDeltaKnown&&tickDeltaOk);
        const cvdMissing=!cvdValid&&!tickDeltaKnown;

        // Ana köprüler: bunlar eşit değil, ağırlıkları farklı.
        const hardSweepForBridge=
          (sw1?.confirmed&&(isL?sw1.direction==='BULL_SWEEP':sw1.direction==='BEAR_SWEEP'))||
          (sweep15m?.confirmed&&(isL?sweep15m.direction==='BULL_SWEEP':sweep15m.direction==='BEAR_SWEEP'))||
          (sweep4h?.confirmed&&(isL?sweep4h.direction==='BULL_SWEEP':sweep4h.direction==='BEAR_SWEEP'))||
          (tickData?.tickSweep?.type===(isL?'BULL_SWEEP':'BEAR_SWEEP')&&tickData.tickSweep.fresh)||
          (amd5m?.signal===(isL?'AMD_LONG':'AMD_SHORT')&&amd5m?.tickConfirm);
        const mtfBridgeOk=isL
          ? (mtfBias?.bias==='STRONG_BULL'||mtfBias?.bullPct>=65)
          : (mtfBias?.bias==='STRONG_BEAR'||mtfBias?.bullPct<=35);
        const mtfStrongOpposite=isL
          ? (mtfBias?.bias==='STRONG_BEAR'||mtfBias?.bullPct<=25)
          : (mtfBias?.bias==='STRONG_BULL'||mtfBias?.bullPct>=75);
        const fundBridgeOk=isL
          ? (fundSig==='NEGATIVE'||fundSig==='EXTREME_NEGATIVE')
          : (fundSig==='POSITIVE'||fundSig==='EXTREME_POSITIVE');
        const oiBridgeOk=isL
          ? (oiDiv==='SHORT_SQUEEZE'||oiDiv==='CONFIRMED_BULL')
          : (oiDiv==='LONG_LIQUIDATION'||oiDiv==='CONFIRMED_BEAR');
        const oiOpposite=isL
          ? (oiDiv==='LONG_LIQUIDATION'||oiDiv==='CONFIRMED_BEAR')
          : (oiDiv==='SHORT_SQUEEZE'||oiDiv==='CONFIRMED_BULL');
        const huntBridgeOk=isL
          ? ((hunt1h?.hunted&&hunt1h.direction==='BULL_HUNT')||(hunt15m?.hunted&&hunt15m.direction==='BULL_HUNT'))
          : ((hunt1h?.hunted&&hunt1h.direction==='BEAR_HUNT')||(hunt15m?.hunted&&hunt15m.direction==='BEAR_HUNT'));
        const mmSameSide=isL
          ? (mmTarget==='GENUINE_UP'||mmTarget==='UP_SWEEP')
          : (mmTarget==='GENUINE_DOWN'||mmTarget==='DOWN_SWEEP');
        const mmStrongOpposite=isL
          ? (mmTarget==='GENUINE_DOWN'&&mmConf>=60)
          : (mmTarget==='GENUINE_UP'&&mmConf>=60);
        const mmVeryStrongOpposite=isL
          ? (mmTarget==='GENUINE_DOWN'&&mmConf>=70)
          : (mmTarget==='GENUINE_UP'&&mmConf>=70);
        const obSameSide=isL
          ? (bookImb>8||iceberg?.signal==='HIDDEN_BUY'||iceberg?.signal==='STRONG_HIDDEN_BUY'||tickData?.imbalance?.bull)
          : (bookImb<-8||iceberg?.signal==='HIDDEN_SELL'||iceberg?.signal==='STRONG_HIDDEN_SELL'||tickData?.imbalance?.bear);
        const cmfSameSide=isL
          ? (cmf1h?.signal==='BUY'||cmf1h?.signal==='STRONG_BUY'||cmf4h?.signal==='STRONG_BUY')
          : (cmf1h?.signal==='SELL'||cmf1h?.signal==='STRONG_SELL'||cmf4h?.signal==='STRONG_SELL');
        const cmfOpposite=isL
          ? (cmf1h?.signal==='STRONG_SELL'||cmf4h?.signal==='STRONG_SELL')
          : (cmf1h?.signal==='STRONG_BUY'||cmf4h?.signal==='STRONG_BUY');
        const weisSameSide=isL ? (weis1h?.signal==='BULL_EFFORT') : (weis1h?.signal==='BEAR_EFFORT');
        const chochSameSide=isL
          ? (choch1h?.signal==='BULL_CHOCH'||choch4h?.signal==='BULL_CHOCH')
          : (choch1h?.signal==='BEAR_CHOCH'||choch4h?.signal==='BEAR_CHOCH');
        const ewoSameSide=isL ? (ewo1h?.signal==='BULL_WAVE') : (ewo1h?.signal==='BEAR_WAVE');
        const squeezeSameSide=isL
          ? (sqz1h?.direction==='BULL'&&sqz1h?.acceleration==='GROWING')
          : (sqz1h?.direction==='BEAR'&&sqz1h?.acceleration==='GROWING');
        const pdSameSide=isL ? (pd1h?.forLong||pd4h?.forLong) : (pd1h?.forShort||pd4h?.forShort);
        const pdOpposite=isL ? (pd1h?.zone==='PREMIUM_HIGH') : (pd1h?.zone==='DISCOUNT_LOW');

        // R35: 5m botun kör kalmaması için taze mikro mum/15m teyit köprüsü.
        // Ek Binance çağrısı yok; eldeki k5m/k15m, sweep ve hunt verisi kullanılır.
        const _r35k5 = Array.isArray(k5m) ? k5m : [];
        const _r35Last5 = _r35k5.at(-1);
        const _r35Prev5 = _r35k5.at(-2);
        const _r35Close = Number(_r35Last5?.[4] || 0);
        const _r35Open  = Number(_r35Last5?.[1] || 0);
        const _r35High  = Number(_r35Last5?.[2] || 0);
        const _r35Low   = Number(_r35Last5?.[3] || 0);
        const _r35PrevClose = Number(_r35Prev5?.[4] || _r35Open || 0);
        const _r35Vol = Number(_r35Last5?.[5] || 0);
        const _r35AvgVol = _r35k5.length > 12
          ? _r35k5.slice(-12,-1).reduce((a,k)=>a+Number(k?.[5]||0),0)/11
          : 0;
        const _r35Range = Math.max(0, _r35High - _r35Low);
        const _r35ClosePos = _r35Range > 0 ? (_r35Close - _r35Low) / _r35Range : 0.5;
        const _r35BodyPct = (_r35Close > 0 && _r35Open > 0) ? Math.abs(_r35Close - _r35Open) / _r35Close * 100 : 0;
        // R41: önceki gevşek koşul neredeyse her yeşil/kırmızı mumu "fresh impulse" sayıyordu.
        // R58-FIX2: Sadece son mum değil, son 2 mum kontrol ediliyor.
        // Sweep 2 dk önce olup son mum konsolidasyon ise yanlış bloke ediyordu.
        const _r35Prev2 = _r35k5.at(-3);
        const _r35Close2 = Number(_r35Prev2?.[4] || 0);
        const _r35Open2  = Number(_r35Prev2?.[1] || 0);
        const _r35High2  = Number(_r35Prev2?.[2] || 0);
        const _r35Low2   = Number(_r35Prev2?.[3] || 0);
        const _r35Range2 = Math.max(0, _r35High2 - _r35Low2);
        const _r35ClosePos2 = _r35Range2 > 0 ? (_r35Close2 - _r35Low2) / _r35Range2 : 0.5;
        const _r35BodyPct2 = (_r35Close2 > 0 && _r35Open2 > 0) ? Math.abs(_r35Close2 - _r35Open2) / _r35Close2 * 100 : 0;
        const fresh5mImpulse = !!(_r35Close > 0 && _r35Open > 0 && _r35PrevClose > 0 && (
          isL
            ? (_r35Close > _r35Open && _r35Close >= _r35PrevClose * 1.0005 && _r35BodyPct >= 0.05 && _r35ClosePos >= 0.55 && (!_r35AvgVol || _r35Vol >= _r35AvgVol * 0.90))
            : (_r35Close < _r35Open && _r35Close <= _r35PrevClose * 0.9995 && _r35BodyPct >= 0.05 && _r35ClosePos <= 0.45 && (!_r35AvgVol || _r35Vol >= _r35AvgVol * 0.90))
        ));
        // R58-FIX2b: Son 2. mum impulse (sweep sonrası konsolidasyon durumu)
        const fresh5mImpulse2 = !!(_r35Close2 > 0 && _r35Open2 > 0 && (
          isL
            ? (_r35Close2 > _r35Open2 && _r35BodyPct2 >= 0.08 && _r35ClosePos2 >= 0.60)
            : (_r35Close2 < _r35Open2 && _r35BodyPct2 >= 0.08 && _r35ClosePos2 <= 0.40)
        ));
        const fresh5mImpulse2Bridge = !!(fresh5mImpulse2 && hardSweepForBridge); // R59: eski mum puanı sadece gerçek sweep/stop-hunt bağlamında geçerli
        const fresh5mImpulseOrRecent = fresh5mImpulse || fresh5mImpulse2Bridge;
        const fresh15mConfirm = !!(
          (sweep15m?.confirmed && (isL ? sweep15m.direction==='BULL_SWEEP' : sweep15m.direction==='BEAR_SWEEP')) ||
          (hunt15m?.hunted && (isL ? hunt15m.direction==='BULL_HUNT' : hunt15m.direction==='BEAR_HUNT')) ||
          (tickData?.tickSweep?.fresh && tickData?.tickSweep?.type === (isL ? 'BULL_SWEEP' : 'BEAR_SWEEP')) ||
          (amd5m?.tickConfirm && amd5m?.signal === (isL ? 'AMD_LONG' : 'AMD_SHORT'))
        );

        // ── R22: Teraziye bağlanan ileri okuma katmanları ────────────────────
        const r22Side = isL ? 'LONG' : 'SHORT';
        const r22Rotation = r22RotationBias(full, r22Side);
        const r22Decay = r22SignalDecayForSide(isL);
        const r22TestQ = r22TestQualityForSide(isL);
        const r22FundingTrap = (() => {
          // R22 BEST: -0.08% eşiği (GPT) + fundMom momentum teyidi (benim eklentim)
          const sameSweepOrHunt = hardSweepForBridge || huntBridgeOk || amd5m?.signal === (isL ? 'AMD_LONG' : 'AMD_SHORT');
          const fundMomBull = fundMom?.ok && (fundMom?.signal === 'STRONG_LONG_BIAS' || fundMom?.signal === 'LONG_BIAS');
          const fundMomBear = fundMom?.ok && (fundMom?.signal === 'STRONG_SHORT_BIAS' || fundMom?.signal === 'SHORT_BIAS');
          if (isL) {
            const extreme = curFund < -0.08 || fundSig === 'EXTREME_NEGATIVE';
            const oiShortsClosing = oiChg1h < -0.4 || oiDiv === 'SHORT_SQUEEZE' || oiDiv === 'NEUTRAL';
            const fundMomConfirm = fundMomBull || !fundMom?.ok; // Funding negatife gidiyorsa ekstra güçlü
            const detected = !!(extreme && sameSweepOrHunt && oiShortsClosing);
            const strength = detected ? (fundMomConfirm ? 'STRONG' : 'NORMAL') : 'NONE';
            return { detected, strength, side:'LONG', funding:+curFund.toFixed(4), oiChg1h:+oiChg1h.toFixed(2),
                     scoreBonus: detected ? (strength === 'STRONG' ? 20 : 15) : 0,
                     psBonus: detected ? 16 : 0,
                     label: detected ? `Fonlama short tuzağı (Fund:${curFund.toFixed(4)}% OI:${oiChg1h.toFixed(1)}%)` : '' };
          }
          const extreme = curFund > 0.08 || fundSig === 'EXTREME_POSITIVE';
          const oiLongsClosing = oiChg1h < -0.4 || oiDiv === 'LONG_LIQUIDATION' || oiDiv === 'NEUTRAL';
          const detected = !!(extreme && sameSweepOrHunt && oiLongsClosing);
          const strength = detected ? (fundMomBear ? 'STRONG' : 'NORMAL') : 'NONE';
          return { detected, strength, side:'SHORT', funding:+curFund.toFixed(4), oiChg1h:+oiChg1h.toFixed(2),
                   scoreBonus: detected ? (strength === 'STRONG' ? 20 : 15) : 0,
                   psBonus: detected ? 16 : 0,
                   label: detected ? `Fonlama long tuzağı (Fund:${curFund.toFixed(4)}%)` : '' };
        })();
        const r22LiqWaterfall = (() => {
          const lvls = Array.isArray(cgData?.topLiqLevels) ? cgData.topLiqLevels : [];
          const above = lvls.filter(l => Number(l.price) > lastPrice).reduce((s,l)=>s+Number(l.total ?? (Number(l.buyLiq||0)+Number(l.sellLiq||0))),0);
          const below = lvls.filter(l => Number(l.price) < lastPrice).reduce((s,l)=>s+Number(l.total ?? (Number(l.buyLiq||0)+Number(l.sellLiq||0))),0);
          const levelSignal = lvls.length >= 2;
          const bookAbove = upLiqStr || 0, bookBelow = dnLiqStr || 0;
          const _lvlScore=(l,w=1)=>l ? (Number(l.strength||0)*w)/Math.max(Math.abs(Number(l.distPct||99)),0.15) : 0;
          const localAbove = bookAbove + _lvlScore(liq1h?.buyLiq?.[0], 1200) + _lvlScore(liq4h?.buyLiq?.[0], 750);
          const localBelow = bookBelow + _lvlScore(liq1h?.sellLiq?.[0], 1200) + _lvlScore(liq4h?.sellLiq?.[0], 750);
          const adverseFromMap = isL ? (below > 100000 && below > above * 1.55) : (above > 100000 && above > below * 1.55);
          const favorableFromMap = isL ? (above > 100000 && above > below * 1.25) : (below > 100000 && below > above * 1.25);
          const adverseFromBook = isL ? (localBelow > localAbove * 1.55) : (localAbove > localBelow * 1.55);
          const favorableFromBook = isL ? (localAbove > localBelow * 1.25) : (localBelow > localAbove * 1.25);
          const cascade = liqData?.cascade || null;
          const adverseCascade = cascade ? (isL ? cascade.direction==='LONG_CASCADE' : cascade.direction==='SHORT_CASCADE') : false;
          const favorableCascade = cascade ? (isL ? cascade.direction==='SHORT_CASCADE' : cascade.direction==='LONG_CASCADE') : false;
          return {
            above:+above.toFixed(0), below:+below.toFixed(0),
            localAbove:+localAbove.toFixed(2), localBelow:+localBelow.toFixed(2),
            adverse: !!(adverseCascade || adverseFromMap || (!levelSignal && adverseFromBook)),
            favorable: !!(favorableCascade || favorableFromMap || (!levelSignal && favorableFromBook)),
            source: levelSignal ? 'coinglass-cache' : 'local-liq-1h4h-strength',
            label: isL ? 'Long için alt şelale riski / üst squeeze potansiyeli' : 'Short için üst şelale riski / alt cascade potansiyeli'
          };
        })();

        // R36: RVOL VERY_LOW etiketi (<0.7x) 5m top-gainer scalper için fazla genişti.
        // Sadece gerçekten ölü hacim (<0.20x) otomatik köprüyü kilitler; 0.20-0.70 arası ceza/uyarı olarak kalır.
        const rvolVeryLow = !!(rvol1h && Number(rvol1h.rvol || 0) < 0.20);
        const _slPctEval = parseFloat(autoConfig?.slPct || 2);
        // R35: 1h ATR top gainer coinlerde doğal olarak yüksek olur.
        // Önceki R34 bunu hard-veto yaptığı için 5m fırsatlarını öldürüyordu.
        // Artık sadece aşırı uç volatilite hard block; normal yüksek ATR priority cezası/uyarıdır.
        const atrWarnForAuto = !!(atrGateBlock || atrPct > _slPctEval * 2.5);
        const atrExtremeBlock = atrPct > Math.max(14, _slPctEval * 7.0);
        const atrBlocking = !!atrExtremeBlock;
        const poorLiquidity = !!(liqQual?.quality==='POOR' || (liqQual?.slippageRisk && Number(liqQual?.spread||0)>0.10));
        const r37Side = r37Timing?.ok ? (isL ? r37Timing.long : r37Timing.short) : null;
        const r37LateChaseBlock = !!(r37Side?.lateChase);
        const r37RetestWaitBase = !!(r37Side?.retestOnly && !r37Side?.retestOk && !r37Side?.earlyImpulse);
        let r37RetestWait = r37RetestWaitBase;
        const r37EarlyOk = !!(r37Side?.earlyImpulse || r37Side?.retestOk || Number(r37Side?.earlyScore||0) >= 4);
        const r39Side = r39SR?.ok ? (isL ? r39SR.long : r39SR.short) : null;
        const r39TargetNearBlock = !!(r39Side?.targetTooNear && !r39Side?.breakConfirmed && !r37Side?.earlyImpulse && !r37Side?.retestOk);
        const r39AgainstZone = !!(isL ? r39Side?.nearResistance : r39Side?.nearSupport);
        const r39Confluence = !!(isL ? r39Side?.supportConfluence : r39Side?.resistanceConfluence);

        // R41: AIGENSYN tarzı büyük zarar kökü — long tarafında taze UTAD / dağıtım varken
        // "Sweep+Teyit + Wyckoff" etiketi yanlışlıkla A-tier long'a çevrilebiliyordu.
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
        // R42: ters Wyckoff tuzağı (LONG içinde UTAD / SHORT içinde Spring-SOS) sadece "erken impuls" geldi diye bypass edilemez.
        // Bypass için hem canlı flow hem de 5m S/R tarafında gerçek kırılım/reclaim gerekir.
        const r42TrapReclaimOk = !!(r41OppositeWyckoff && deltaOkStrict && r39Side?.breakConfirmed && r37EarlyOk);
        const r41TrapBlock = !!(r41OppositeWyckoff && !r42TrapReclaimOk);

        // ── R46: BİLEŞİK PATTERN BEYNİ ───────────────────────────────────────
        // Tek tek sinyalleri 0/1 saymak yerine, aynı yönde birlikte geldiklerinde
        // ekstra kalite puanı verir. Yeni Binance çağrısı yok; k5m/CVD/OI/funding/
        // sweep/MM verileri zaten evalDecision içinde hazır.
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
        // R46: bileşik pattern puanları — tekil sinyaller aynı yönde üst üste gelirse
        // karar terazisini kademeli güçlendirir, ama hard veto/late-chase/sweep kapısını bypass etmez.
        addP(r46PerfectAlignBonus > 0, r46PerfectAlignBonus, `R46Align${r46PerfectAlignCount}/5`);
        addP(r46CvdGradeBonus > 0, r46CvdGradeBonus, `R46CVD${Math.round(cvdRatio)}%`);
        addP(r46SqueezeQualityScore > 0, r46SqueezeQualityScore, `R46Squeeze${r46Rvol.toFixed(2)}x`);
        addP(r46SpringQuality > 0, r46SpringQuality, 'R46SpringQuality');
        addP(r46ExhaustionShort, 20, 'R46ExhaustionShort');
        // R37: 5m timing terazi katkısı — erken impuls/retest lehte, geç chase ters.
        addP(r37Side?.earlyImpulse, 10, 'R37Early');
        addP(r37Side?.retestOk, 8, 'R37Retest');
        addP(Number(r37Side?.earlyScore||0) >= 4, 5, 'R37Micro');
        subP(r37Side?.lateChase, 22, 'R37LateChase');
        subP(r37RetestWaitBase, (r38TopMoverStrong && Number(r37Side?.earlyScore||0) >= 3) ? 6 : 12, 'R37RetestWait');
        addP(r39Confluence, 8, 'R39_SR_Confluence');
        addP(r39Side?.breakConfirmed, 7, 'R39_Break');
        subP(r39TargetNearBlock, 18, 'R39_TargetNear');
        subP(r39AgainstZone && !r39Side?.breakConfirmed, 7, 'R39_AgainstSR');
        subP(r41TrapBlock, 26, 'R41_WyckoffTrap');
        subP(r41FallingKnifeBlock || r41RisingKnifeBlock, 20, 'R41_Falling/RisingKnife');
        // R22: öncelik terazi ekleri. Bunlar iyi sinyali hızlandırır ama güvenlik frenlerini devre dışı bırakmaz.
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

        // R32: Pattern + Renko priorityScore katkısı
        // Sadece mevcut yönün yapısal kalitesini ölçer; hard veto ve mikro teyitleri bypass etmez.
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

        // ── R27: MM avlama PS etkisi ──────────────────────────────────────────
        {
          const _sh = r27SpreadHistory.get(full);
          const _sv = (_sh && _sh.prev3m > 0) ? (_sh.cur - _sh.prev3m) / _sh.prev3m : 0;
          subP(_sv > 0.45, 20, 'SpreadHızlanma🚨');
          subP(_sv > 0.25 && _sv <= 0.45, 10, 'SpreadGenişliyor');
          // Absorption: son 5dk'da 3+ büyük satış emirleri
          const _eng2 = tickStore?.get?.(full);
          const _bigSells = (_eng2?.bigTrades||[]).filter(t=>t.side==='SELL'&&Date.now()-t.ts<5*60*1000).length;
          subP(_bigSells >= 3, 12, 'Absorption🧲');
          // BTC kopuşu PS
          if (k5m?.length >= 3 && btcPriceRef.p > 0) {
            const _cChg = (Number(k5m.at(-1)[4]) - Number(k5m.at(-2)[1])) / Number(k5m.at(-2)[1]) * 100;
            const _divR = Math.abs(_cChg) / (Math.abs(btcChange5mCache) + 0.02);
            subP(_divR > 6 && Math.abs(_cChg) > 1.8, 12, 'BTCKopuşu📡');
          }
          // Hacim çürümesi PS
          if (k5m?.length >= 20) {
            const _pv    = Math.max(...k5m.slice(-20).map(c=>Number(c[5])));
            const _cv    = Number(k5m.at(-1)[5]);
            const _pxChg = k5m.length>20 ? (Number(k5m.at(-1)[4])-Number(k5m.at(-21)?.[1]||k5m[0][1]))/Number(k5m.at(-21)?.[1]||k5m[0][1])*100 : 0;
            const _isDecay = _pv > 0 && (_pv-_cv)/_pv > 0.70 && _pxChg > 7;
            addP(!isL && _isDecay, 12, 'HacimÇürümesi⚡', 'macro', 28);
            subP( isL && _isDecay, 10, 'HacimÇürümesiLong');
          }
        }
        // ── R27 PS bitti ──────────────────────────────────────────────────────

        // R29: bağlam terazisi priorityScore'a tek aileden bağlanır; kural yığması yapmaz.
        {
          const r29Risk = isL ? Number(r29Context.longRisk||0) : Number(r29Context.shortRisk||0);
          const r29Fav  = isL ? r29Context.preferSide==='LONG'  : r29Context.preferSide==='SHORT';
          const r29Vwap = isL ? r29Context.vwap?.longReclaim : r29Context.vwap?.shortReject;
          addP(r29Fav, 10, 'R29BağlamLehte', 'macro', 28);
          addP(r29Vwap, 8, 'VWAPTeyit', 'structure', 16);
          subP(r29Risk >= 45, Math.min(22, Math.round(r29Risk/5)), 'R29BağlamRisk');
        }

        const bridgeCount=[mtfBridgeOk,fundBridgeOk,oiBridgeOk,huntBridgeOk].filter(Boolean).length;
        // R38: Retest-only bekleme, top mover + yüksek terazi + mikro yapı varsa otomatik köprüyü öldürmesin.
        // Geç chase hâlâ hard block; bu sadece 'hareket var ama ilk retest/akış devam ediyor' durumudur.
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
        // R42: CVD/tick yokken köprü otomatik emir için ancak çok güçlü ve çoklu teyitliyse kullanılabilir.
        // Böylece veri yokluğu "terazi yüksek" diye AIGENSYN benzeri kör B+ girişine dönüşmez.
        const r42FlowGate = !!(
          deltaOkStrict ||
          (tickData?.tickSweep?.fresh && hardSweepForBridge) ||
          (!cvdValid && priorityScore >= 76 && bridgeCount >= 3 && hardSweepForBridge && (fresh5mImpulseOrRecent || r37EarlyOk || r38RetestBridgeOk) && !r41OppositeWyckoff)
        );
        const deltaOk = deltaOkStrict || (cvdBridgePass && r42FlowGate);
        const microConfirm = deltaOkStrict || (hardSweepForBridge && (tickData?.tickSweep?.fresh || huntBridgeOk || obSameSide || oiBridgeOk || fundBridgeOk));
        const microConfirmR35 = microConfirm || (fresh5mImpulseOrRecent && (huntBridgeOk || obSameSide || oiBridgeOk || fundBridgeOk || mmSameSide || mtfBridgeOk));
        const cvdBridgeQualityOk = !cvdWarmingBridge || (cvdBridgePass && r42FlowGate);

        // Hard veto: bunlar skor değil, güvenlik frenidir.
        const fundOk=isL?fundSig!=='EXTREME_POSITIVE':fundSig!=='EXTREME_NEGATIVE';
        const rsiOk = isL ? rsi4h < 82 : rsi4h > 18; // Aşırı uçta otomatik açma yok, 78/22 artık yumuşak ceza.
        const mmOk = !mmVeryStrongOpposite;
        // R60-FIX1: Bayat sweep ≠ trend bitti. MTF 4/4 Bull + RVOL güçlü + OI pozitif = trend devam ediyor.
        // Önceden 10dk+ sweep → signalDecayAutoBlock=true, her şeyi killiyordu.
        // Güçlü trend devamında bu kör kilidi açıyoruz.
        // R61-FIX: R60 raporundaki trend continuation mantığı doğruydu ama kodda mtfBull
        // diye tanımsız bir değişken kullanılmıştı. Bu yüzden override fiilen hiç çalışmıyordu.
        // Burada MTF kontrolü side-aware hale getirildi: LONG için bull, SHORT için bear.
        const r61MtfFullTrendOk = !!(
          isL
            ? (mtfBridgeOk && (Number(mtfBias?.bull || 0) >= Math.min(4, Number(mtfBias?.total || 4)) || mtfBias?.bias === 'STRONG_BULL' || Number(mtfBias?.bullPct || 0) >= 65))
            : (mtfBridgeOk && (Number(mtfBias?.bear || 0) >= Math.min(4, Number(mtfBias?.total || 4)) || mtfBias?.bias === 'STRONG_BEAR' || Number(mtfBias?.bullPct || 100) <= 35))
        );
        const r60StrongTrendContinuation = !!(
          r22Decay.noAuto &&
          r61MtfFullTrendOk &&                            // side-aware MTF devamı
          (Number(rvol1h?.rvol||0) >= 1.5) &&             // RVOL gerçekten aktif
          oiBridgeOk &&                                    // OI fiyat yönünü destekliyor
          !r37LateChaseBlock && !r39TargetNearBlock &&    // geç giriş/hedef yakın değil
          !r41TrapBlock && !r41OppositeWyckoff &&          // Wyckoff ters tuzak yok
          (fresh5mImpulseOrRecent || fresh15mConfirm || r37EarlyOk || r39Confluence || r39Side?.breakConfirmed)
        );
        // R62: Trend devamı ile ters-fırsat arama ayrıldı.
        // Güçlü yukarı trend varsa LONG continuation değerlendirilir; ama bu SHORT fırsatlarını körlemesine kapatmaz.
        // Güçlü aşağı trend varsa SHORT continuation değerlendirilir; ama bu LONG dönüş/spring fırsatlarını kapatmaz.
        // MTF ters ise normalde sert frendir; sadece gerçek karşı-trend trap/sweep bağlamı varsa bypass adayı olur.
        const r62SideTrapEventOk = !!(isL
          ? (wy1?.recentEvents?.some(e=>e.type==='SPRING'||e.type==='SOS') || wy15?.recentEvents?.some(e=>e.type==='SPRING'||e.type==='SOS') || wy4?.recentEvents?.some(e=>e.type==='SPRING'||e.type==='SOS'))
          : (wy1?.recentEvents?.some(e=>e.type==='UTAD') || wy15?.recentEvents?.some(e=>e.type==='UTAD') || wy4?.recentEvents?.some(e=>e.type==='UTAD'))
        );
        // R63-FIX: directSweepOk burada henüz const olarak tanımlanmadan okunuyordu.
        // JS TDZ hatası: "Cannot access 'directSweepOk' before initialization".
        // directSweepOk zaten hardSweepForBridge || huntBridgeOk ailesi olduğu için burada
        // doğrudan bu iki güvenli değişken kullanılır.
        const r62CounterTrendTrapContextOk = !!(
          mtfStrongOpposite &&
          (r62SideTrapEventOk || hardSweepForBridge || huntBridgeOk || tickData?.tickSweep?.fresh || r39Confluence || r39Side?.breakConfirmed)
        );
        const r62CounterTrendTrapFlowOk = !!(
          deltaOkStrict || obSameSide || oiBridgeOk || fundBridgeOk || cmfSameSide || weisSameSide || chochSameSide || ewoSameSide || squeezeSameSide
        );
        const signalDecayAutoBlock = !!(r22Decay.noAuto && (hardSweepForBridge || huntBridgeOk) && !fresh15mConfirm && !fresh5mImpulseOrRecent && !r60StrongTrendContinuation);
        const r29CtxBlock = isL ? !!r29Context.longAutoBlock : !!r29Context.shortAutoBlock;
        // R75-FIX1: lateChase soft-veto. retestOk=true ise (hareket kaçmış ama fiyat geri döndü)
        // r74ImpulseEntryOk ve r74Top10ProScalperOk için hard-veto olmaktan çıkar.
        // retestOk yoksa (salt geç chase) hardVeto olarak kalır.
        const r75LateChaseHard = !!(r37LateChaseBlock && !r37Side?.retestOk);
        // R155b: r75LateChaseHard hardVeto'dan çıkarıldı.
        // TOP10 5m pump momentumunda "geç giriş" çok sık tetikleniyor ve hardVeto → fatalDanger → rawEdge-55 zincirini başlatıyordu.
        // Kalibrasyon cezası (-8), r37LateChaseBlock ve r147 kontrolleri yeterli koruma sağlar.
        const hardVeto = !!(poorLiquidity || atrBlocking || r39TargetNearBlock || r41TrapBlock || r41FallingKnifeBlock || r41RisingKnifeBlock || !fundOk || !rsiOk || !mmOk || r29CtxBlock);
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

        // R45: 5m SweepOnly REAL GATE — UI checkbox artık gerçek otomatik emir kapısıdır.
        // directSweepOk: gerçek likidite/sweep/stop-hunt ailesi. Sweep zorunlu AÇIK ise otomatik emir bununla sınırlıdır.
        // nonSweepQualityOk: Sweep zorunlu KAPALI ise, sadece çok kaliteli 5m erken impuls/retest + CVD/OI/book/MM uyumu ile B+ köprü açabilir.
        const sweepRequired = !!(autoConfig?.sweepOnly === true); // R58-FIX4: explicit true gerekiyor, undefined → false (önceden tersiydi)
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

        // R47: Fazla sıkı karar zinciri dengesi.
        // Eski non-sweep kapısı çok fazla AND koşulu istiyordu: CVD yoksa veya RVOL düşükse,
        // top-gainer 5m fırsatları B'de kalabiliyordu. R47 hard güvenliği bozmaz; yalnızca
        // timing + flow + context + yapı + RVOL parçalarını bileşik readiness skoru olarak okur.
        const r47CvdMissingBridge = !!(
          cvdMissing && (fresh5mImpulseOrRecent || r37EarlyOk || r38RetestBridgeOk) &&
          (oiBridgeOk || obSameSide || mmSameSide || fundBridgeOk) &&
          priorityScore >= (r38TopMoverStrong ? 68 : 72) &&
          !r41OppositeWyckoff && !r37LateChaseBlock && !r39TargetNearBlock
        );
        // R58-FIX1: CVD nötr ölü noktası düzeltmesi.
        // CVD %48-55 arasında MEVCUT ama nötr → cvdMissing=false → cvdMissingBridge devreye girmiyordu.
        // Ama hiç CVD yokken 1 puan veriyorduk. Nötr CVD hiç yoktan daha iyi bilgi içeriyor.
        // OI+MM güçlü hizalandıysa nötr CVD 1 flow puanı almalı.
        const r47CvdNeutralBridge = !!(
          !cvdMissing && !deltaOkStrict &&
          cvdValid && cvdRatio >= 46 && cvdRatio <= 55 &&
          (isL ? cvdRatio >= 48 : cvdRatio <= 52) &&
          (oiBridgeOk && mmSameSide) &&
          !r41OppositeWyckoff && !r37LateChaseBlock && !r39TargetNearBlock
        );
        const r47TimingPts = Math.min(4,
          (fresh5mImpulse ? 2 : 0) +
          (fresh5mImpulse2Bridge ? 1 : 0) +  // R59: 2. mum impulse sadece sweep/stop-hunt bağlamında puan alır
          (r37EarlyOk ? 2 : 0) +
          // R75-FIX5: earlyScore>=3 ama r37EarlyOk henüz true olmamışsa (eşik 4'ün altında kalmış)
          // zayıf ama var olan erken momentum için +1 timing bonusu ver.
          (!r37EarlyOk && Number(r37Side?.earlyScore||0) >= 3 ? 1 : 0) +
          (r38RetestBridgeOk ? 1 : 0) +
          (r39Side?.breakConfirmed ? 1 : 0)
        );
        const r47FlowPts = Math.min(4,
          (deltaOkStrict ? 3 : 0) +
          (r45CvdAlternativeOk ? 2 : 0) +
          (r47CvdMissingBridge ? 1 : 0) +
          (r47CvdNeutralBridge ? 1 : 0) +  // R58-FIX1: nötr CVD artık 1 puan veriyor
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
        // R48: Direct sweep varken de karar zinciri B'de kilitlenebiliyordu.
        // Eski direct-sweep B+ kapısı r42FlowGate + priority>=58 istiyordu; top-gainer 5m'de
        // sweep+stop-hunt doğru olsa bile CVD nötr/eksik veya terazi düşükse saatlerce bekliyordu.
        // Bu köprü kör giriş değildir: hard veto/late chase/hedef yakın/ters MM-MTF hâlâ yasak;
        // ayrıca 5m yapı + flow/context + RVOL okunur. Sweep kapalı modda kontrollü B+ üretir.
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

        // R49: Direct sweep + 5m scalper dengesini B+ kapısına gerçekten bağla.
        // R48 debug ekranında adaylar 8/8 veya 12/8 readiness verdiği halde B'de kalabiliyordu.
        // Sebep: eski B+ yolu hâlâ r42FlowGate / cvdBridgePass / mikro teyit üçlüsüne fazla bağlıydı.
        // R49 bunu kör gevşetmez: hardVeto, late chase, hedef yakın, ters MM/MTF, Wyckoff trap,
        // RVOL, yapı ve CVD karşıtlığı hâlâ kontrol edilir. Sadece direct sweep varsa ve
        // 5m okuma yeterince sağlamsa kontrollü B+ izni üretir.
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

        // R50: Otomatik emir izin matrisi.
        // R49'da /api/health HYPE gibi adaylarda R47=10/8 hazır gösterirken R48/R49=NO kaldığı
        // için karar B'de kilitleniyordu. Sweep kapalı modda bu sessiz logic kilididir:
        // R47 hazırsa, hard güvenlikler temizse ve fiyat chase/targetNear değilse B+ auto izni alır.
        // UB gibi R47=2/8 adaylar ise yine açılmaz.
        const r50PriorityBoost = r47Readiness >= 10 ? 15 : r47Readiness >= 8 ? 8 : 0;
        const r50EffectivePriority = Math.max(0, Number(priorityScore || 0) + r50PriorityBoost);
        // R79-FIX1: R75 retest-soft late chase bütün zincire yansıtıldı.
        // Eski r50HardClean ham r37LateChaseBlock kullanıyordu; retestOk=true olsa bile RIF gibi
        // Sweep+Teyit + R47 10/8 adayları R50/R51'de ölüyordu.
        const r50HardClean = !!(
          !hardVeto && !signalDecayAutoBlock && !r37RetestWait && !r39TargetNearBlock && // R155c: r75LateChaseHard kaldırıldı
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

        // R51: Direct sweep + stop-hunt adayında minimum edge kilidi.
        // /api/health R50'de HYPE gibi adaylarda sc=72, direct sweep+stop-hunt, R47=8/8,
        // S/V yapısı temiz olmasına rağmen P50=28 olduğu için R50:NO kalıyordu.
        // Bu, sweep kapalı 5m scalper modunda fazla sıkı logic kilidiydi.
        // R51 kör gevşetme yapmaz: hard veto, geç chase, hedef yakın, CVD karşıtlığı, RVOL,
        // yapı ve canlı flow/context kontrolleri hâlâ zorunlu. Sadece P50 top-mover direct-sweep
        // eşiği 32 yerine pratik minimum edge 24 olarak ayrı bir B+ yolu açar.
        // R79-FIX2: R51 direct-sweep min-edge yolu r50HardClean'e kör bağımlı değil.
        // Sweep+retest geldiğinde lateChase artık soft olmalı; r50HardClean içindeki diğer context
        // sıkılıkları R51'i tekrar boğmasın diye gerçek hard güvenlikler ayrı tutulur.
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

        // R53: Smart edge score priority.
        // R52 health'te WLD gibi adaylar R47=10/8 ve P50=87 verdiği halde raw score 65<72 diye B'de kalıyordu.
        // Bu eski kural-yığma etkisidir: amaç panel minScore'u körce silmek değil; güçlü MM/flow/readiness varsa
        // küçük bir kalite telafisiyle efektif skoru okumak. Alt zemin hâlâ serttir: score minScore-8 altına düşemez,
        // hard veto/late chase/targetNear/ters MM-MTF/ters CVD/RVOL zayıflığı varsa açmaz.
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

        // R57: 5m scalper B-tier bridge.
        // R56 ekranında H gibi adaylarda skor panel minScore'a eşit (72), R47=8/8,
        // Sweep+Teyit/UTAD var; fakat P50 47 ve S=1 olduğu için R50/R51/R54 NO kalıp
        // 1 saat boyunca hiç emir çıkmayabiliyordu. Bu kör gevşetme değildir:
        // minScore altı 55-63 adayları yine açılmaz; sadece panel skorunu geçen/eşitleyen,
        // R47 hazır, CVD güvenli, RVOL kullanılabilir ve en az bir gerçek 5m trap/sweep/flow
        // izi olan B adayı kontrollü B+ otomatik adaya çevrilir.
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
        // R61: Trend continuation bridge.
        // FLNC gibi Top10 5m momentum adaylarında eski sweep bayat olabilir; ama MTF full uyum + RVOL + OI +
        // taze 5m/retest/break devamı varsa, "sweep 180dk" tek başına otomatik emri öldürmemeli.
        // Bu yol H SHORT gibi MTF ters/whale-long/UTAD riskli adayları açmaz; çünkü side-aware MTF + hard-clean ister.
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
        // R62: Karşı trend trap/reversal köprüsü.
        // Amaç: yükseliş trendi var diye SHORT fırsatını, düşüş trendi var diye LONG fırsatını baştan öldürmemek.
        // Bu sadece MTF'ye karşı gerçek UTAD/Spring-SOS/sweep/hunt + canlı flow geldiğinde çalışır.
        // R64-FIX: R62 karşı-trend trap yolu r50HardClean kullanıyordu.
        // r50HardClean içinde !mtfStrongOpposite var; oysa karşı-trend trap tam olarak
        // MTF ters iken UTAD/Spring + sweep/hunt yakalamak içindir. Bu çakışma R62'yi
        // fiilen kilitliyordu. Burada MTF tersliği sadece r62CounterTrendTrapContextOk ile
        // kontrollü olarak izinlenir; diğer gerçek hard güvenlikler korunur.
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

        // R65: SCALPER_CORE — karar katmanını profesyonel scalper gibi sadeleştirir.
        // Sweep zorunlu KAPALI iken, panel minScore geçtiyse ve 3 ana şart geldiyse
        // R47/R50/R57/R61/R62 zinciri yüzünden B'de bekletmez:
        // 1) gerçek direct sweep/stop-hunt, 2) MTF yön uyumu, 3) R47 readiness >= 5.
        // Güvenlikte sadece gerçek hard stop ailesi kalır: poor liquidity, late chase, target near,
        // ters Wyckoff trap/knife ve aşırı ATR. Funding/RSI/R29/MM korku kapıları bu yolda veto değil,
        // sadece skor/etiket bilgisidir.
        // R66: Wyckoff trap artık kör hard-veto değil.
        // UTAD/Spring hâlâ tehlike sayılır; ama 5m scalper core tarafında fiyat trap bölgesini
        // taze sweep/stop-hunt + MTF + R47 + canlı context ile geri aldıysa, yalnızca bu nedenle
        // emir öldürülmez. Böylece WLD tipi score>=minScore + R47 8/8 + sweep/hunt adayları
        // 'UTAD: long engellendi' etiketi yüzünden otomatik tamamen ölmez.
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
          // R155c: r75LateChaseHard tüm kritik köprülerden çıkarıldı — kalibrasyon -8 cezası yeterli
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
        // Zıt trend fırsatı tamamen ölmesin: MTF tersken yalnızca gerçek UTAD/Spring/SOS + sweep/hunt
        // ve en az bir canlı flow/context desteği varsa scalper core'a alınır.
        const r65ScalperCoreCounterTrapOk = !!(
          !sweepRequired &&
          sc >= minAutoScore &&
          (directSweepOk || hardSweepForBridge || huntBridgeOk) &&
          r62CounterTrendTrapContextOk &&
          r47Readiness >= 5 &&
          !r65ScalperCoreHardVeto &&
          (r62CounterTrendTrapFlowOk || oiBridgeOk || obSameSide || fundBridgeOk || mmSameSide)
        );
        // R67: SCALPER_CORE_HUNT_ENTRY_FIX
        // Ekranda WLD gibi adaylarda score>=minScore + R47 8/8 + Sweep+Teyit+StopHunt + MTF aynı yön
        // varken R65:NO kalıyordu. Ana sebep trend yolunun yalnızca directSweep/hardSweep sayması
        // ve eski Wyckoff/decay korkusunun scalper core'a girmeden öldürmesiydi.
        // R67, STOP-HUNT destekli sweep'i gerçek scalper giriş izi sayar; sadece teknik hard güvenlik
        // kalır: poor liquidity, late chase, target near, aşırı ATR, falling/rising knife.
        const r67ScalperCoreHuntEntryOk = !!(
          !sweepRequired &&
          sc >= minAutoScore &&
          (directSweepOk || hardSweepForBridge || huntBridgeOk) &&
          mtfBridgeOk &&
          r47Readiness >= 5 &&
          !poorLiquidity && !r39TargetNearBlock && !atrExtremeBlock && // R155c: r75LateChaseHard kaldırıldı
          !r41FallingKnifeBlock && !r41RisingKnifeBlock
        );

        // R74: TOP10 5M IMPULSE PRO SCALPER
        // R68/R69 yalnızca sweep/hunt/tick-sweep beklediğinde TOP10 gainer impulse fazını kaçırabiliyordu.
        // Bu yol, sweep zorunlu kapalıyken TOP10/top-mover coinlerde taze 5m impulse + R47 + skor tabanı
        // ile girişe izin verir. Güvenlik kapıları korunur: poorLiquidity, lateChase, targetNear,
        // atrExtreme, falling/rising knife ve Wyckoff hard trap.
        const r74ScoreFloor = Math.max(40, Number(minAutoScore || 68) - 25);
        // R75-FIX2: r74ImpulseEntryOk içinde lateChase artık r75LateChaseHard kullanıyor.
        // retestOk=true ise (fiyat geri döndü) geç chase sayılmaz ve giriş açılır.
        const r74ImpulseEntryOk = !!(
          r38TopMoverStrong &&
          (fresh5mImpulseOrRecent || fresh5mImpulse || r37EarlyOk || r37Side?.retestOk || r39Confluence || r39Side?.breakConfirmed) &&
          !atrExtremeBlock && !poorLiquidity && !r39TargetNearBlock // R155c: r75LateChaseHard kaldırıldı
        );
        // R75-FIX3: TOP10 coinde CVD eksik/nötr sık olur → R47=4-5 kalıyor, eşik 6 geçilemiyor.
        // r38TopMoverStrong ise eşiği 5'e düşür; diğer coinlerde 6 korunur.
        const r75R47MinBypass = r38TopMoverStrong ? 5 : 6;
        const r74Top10ContextBypassOk = !!(
          r38TopMoverStrong && Number(r47Readiness || 0) >= r75R47MinBypass && r74ImpulseEntryOk &&
          Number(sc || 0) >= r74ScoreFloor
        );
        // R75-FIX4: sweepRequired=true iken sweep de geldiyse R74 bypass'ı öldürme.
        // sweep yoksa bypass yok kuralı korunur; sweep geldiyse TOP10 impulse izin alır.
        const r74Top10ProScalperOk = !!(
          (!sweepRequired || (sweepRequired && directSweepOk)) &&
          r74Top10ContextBypassOk && !r65ScalperCoreHardVeto
        );

        // R68: UNIFIED_SCALPER_CORE
        // R28->R67 arasında asıl sorun aynı adayın 10+ eski kapıdan geçmeye zorlanmasıydı.
        // R68'de eski analizler bilgi/puan olarak kalır; otomatik emir için tek sade çekirdek kullanılır:
        // skor >= panel minScore + sweep/stop-hunt + MTF yön uyumu veya gerçek karşı-trend trap + R47>=5.
        // CVD nötr, sweep yaşı, R47 7/8, eski UTAD/Spring etiketi, funding/RSI korkusu tek başına veto değildir.
        const r68EntryEventOk = !!(directSweepOk || hardSweepForBridge || huntBridgeOk || tickData?.tickSweep?.fresh || r74ImpulseEntryOk);
        const r68TrendContextOk = !!(mtfBridgeOk || r61MtfFullTrendOk);
        const r68CounterTrapContextOk = !!(mtfStrongOpposite && r62CounterTrendTrapContextOk && (r62CounterTrendTrapFlowOk || oiBridgeOk || obSameSide || fundBridgeOk || mmSameSide));
        const r68ReadinessOk = Number(r47Readiness || 0) >= 5;
        const r68ScoreOk = Number(sc || 0) >= Number(minAutoScore || 0);
        // R77: R75 retest soft-veto tutarlılığı.
        // retestOk=true iken r75LateChaseHard=false olmalı; aksi halde R69/R68 core
        // hâlâ eski r37LateChaseBlock yüzünden retest girişini öldürür.
        // R155c: r75LateChaseHard buradan da çıkarıldı — hardVeto'dan sonra burası da blokluyor.
        const r68CriticalHardBlock = !!(
          poorLiquidity || r39TargetNearBlock || atrExtremeBlock ||
          r41FallingKnifeBlock || r41RisingKnifeBlock
        );
        // R69: PRIORITY_CONTEXT_EXECUTION
        // R68 hâlâ MTF/trap-context'i fiili kapı gibi bırakıyordu. Son health'te H için
        // score>=minScore, Sweep+Hunt, R47 12/8, P50 116 olduğu halde R68:NO kaldı.
        // R69'da MTF yönü güçlü bilgi olarak kalır; ama R47+P50 çok güçlüyse tek başına
        // emir öldüren kilit değildir. Sadece gerçek hard bloklar emir öldürür.
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

        // R77: TDZ fix. r75RetestBridgeOk önce hesaplanmalı.
        // Önceki R75 kodunda r50AutoPermissionOk/nonSweepQualityOk bu değişkeni tanımlanmadan okuyordu;
        // node --check bunu yakalamaz ama canlı analizde "Cannot access ... before initialization" üretir.
        const r75RetestBridgeOk = !!(r37Side?.retestOk && r74ImpulseEntryOk && r38TopMoverStrong && !r75LateChaseHard);

        // R86: Formasyon + veri teyidi köprüsü.
        // R32 formasyon motoruna dokunmaz; sadece formasyon veriyle desteklenince otomatik girişe yol verir.
        // Tek başına mum/formasyon emir açtırmaz. En az 3 gerçek teyit + canlı 5m giriş izi + güvenlik temizliği ister.
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
          !sweepRequired && !hardVeto && !signalDecayAutoBlock && !r37RetestWait && // R155c: r75LateChaseHard kaldırıldı
          !poorLiquidity && !atrExtremeBlock && !r39TargetNearBlock && !r41FallingKnifeBlock && !r41RisingKnifeBlock &&
          r86FormasyonPuan >= 5 && !r86KarsiFormasyonGucu &&
          r86CanliTetikOk && r86VeriTeyitSayisi >= 3 &&
          r47Readiness >= 5 && r47ContextPts >= 2 && (r47FlowPts >= 1 || deltaOkStrict || obSameSide || oiBridgeOk) &&
          Number(sc || 0) >= Math.max(40, Number(minAutoScore || 68) - 30) &&
          !mmVeryStrongOpposite && (!mtfStrongOpposite || r62CounterTrendTrapContextOk || r86FormasyonPuan >= 8)
        );

        // ── R88: 5 Dakika Vur-Kaç Mikro Yapı Motoru ───────────────────────────
        // Eski 15 kapı burada emir öldürücü değil; timing/akış/bağlam/defter/formasyon
        // puanlarına çevrilir. Yalnız gerçek piyasa güvenliği bozuksa işlem kovalanmaz.
        const r88VurKacEnabled = !!((autoConfig?.vurKacEnabled !== false) || autoConfig?.scalpEngineEnabled); // R89: panel eskiyse/vur-kaç alanı gelmiyorsa motor varsayılan AÇIK, panelden kapatılabilir
        const r88Spread = Number(liqQual?.spread || 0);
        const r88Depth  = Number(liqQual?.depth || 0);
        const r88SpreadWide = !!(r88Spread > 0.12 || (r88Spread > 0.08 && r88Depth < 20000));
        const r88DefterInce = !!((r88Depth > 0 && r88Depth < 20000) || liqQual?.quality === 'POOR' || (liqQual?.quality === 'FAIR' && r88Spread > 0.06));
        const r88OynaklikAsiri = !!(atrPct > Math.max(12, slPctForGate * 6));
        const r88PiyasaBozuk = !!(poorLiquidity || r88SpreadWide || r88DefterInce || r88OynaklikAsiri || atrExtremeBlock);
        // R94: R92'de “piyasa bozuk” tek kutuydu. Top Gainers'ta defter dalgalı olabilir;
        // öldürücü olan sadece ölümcül kayma/boş defter/aşırı oynaklık kombinasyonudur.
        // R94: poorLiquidity tek başına artık ölümcül değildir. Top Gainers'ta defter anlık dalgalanabilir.
        // Ölümcül zemin: aşırı makas, çok boş defter + makas, aşırı oynaklık veya ATR bloktur.
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
        // R89: R47 çok güçlüyse eski kapıların hepsi KALDI görünse bile bunu gerçek 5m mikro-yapı say.
        // Örnek: R47 14/8 T4/F1/C5/S2/V2 = zaman + bağlam + yapı var; bunu sadece R68/R50 zinciri yok diye öldürme.
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
        // R94: USELESS tipi canlı trend kurtarma. Mikro işlem yok; sadece gerçek ölümcül zemin değilse
        // güçlü R47 + teyit + terazi/mikro yapı piyasa etiketini TEHLİKELİ'den DALGALI'ya çeker.
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
        // R94: Canlı 5m kopma yolu. R89'da mikro skor ve veri teyidi çok güçlü olsa bile
        // r38TopMoverStrong veya eski bağlam zinciri yüzünden R92VurKac KALDI kalabiliyordu.
        // Burada amaç kapıları kaldırmak değil: eski kapıları destek analizi olarak bırakıp,
        // gerçek canlı kopma varsa emir yolunu açmak. Piyasa bozuksa hâlâ işlem yok.
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
        // R97/R113-TDZ-FIX: Terazi tüm köprülerden önce hesaplanmalı.
        // R112'de R109 köprüsü r92Terazi'yi const tanımından önce okuyabiliyordu;
        // reclaim aktif olduğunda sessiz analiz hatası doğurmasın diye en üste alındı.
        const r92Terazi = Number(r50EffectivePriority || priorityScore || 0);

        // R109: Sweep-Reclaim skoru — sadece fırsat kalitesini artırır, fren koymaz.
        // R113 FIX: /api/analyze içinde 5m mum değişkeninin gerçek adı k5m'dir.
        // Eski "klines5m" referansı ANALYZE_* içinde "klines5m is not defined" hatası üretiyordu.
        const r109Reclaim = r109CalcSweepReclaimScore(k5m || [], isL ? 'LONG' : 'SHORT');
        const r109ReclaimOk  = !!(r109Reclaim.score >= 6 && r109Reclaim.swept && r109Reclaim.reclaimed);
        const r109ReclaimSkor = Number(r109Reclaim.score || 0);
        // R109 köprüsü: Reclaim ≥6 + temel şartlar. Ekstra fren YOK.
        const r109SweepReclaimKoprusuOk = !!(
          r88VurKacEnabled && !sweepRequired && !r93PiyasaHamTehlikeli &&
          r109ReclaimOk &&
          Number(r47Readiness || 0) >= 6 && Number(r88MikroSkor || 0) >= 8 &&
          Number(r88AkisTeyidiSayisi || 0) >= 2 && Number(r92Terazi || 0) >= 25 &&
          !mmVeryStrongOpposite && !r86KarsiFormasyonGucu &&
          !r41FallingKnifeBlock && !r41RisingKnifeBlock
        );
        // R97: r92VurKacAdayOk skor tabanı veya r38TopMoverStrong engeli nedeniyle KALDI kalsa da,
        // piyasa etiketi "DALGALI AMA İŞLEM YAPILABİLİR" ise bu köprü emir yolunu açar.
        // Kaynak: ChatGPT R97 mimarisi; r92NormalVurKacOk eşikleri R95 orijinalinde korundu (R47>=8, timingPts>=2).
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
        // R94: mikro marjlı deneme YOK. Defter inceyse ya da makas/oynaklık bozuksa işlem açılmaz.
        // Vur-kaç adayı; terazi, canlı kanıt ve piyasa kalitesine göre GÜÇLÜ / NORMAL / İZLE olarak sınıflanır.
        const r92DefterSaglam = !!(!poorLiquidity && !r88SpreadWide && !r88DefterInce && !r88OynaklikAsiri && !atrExtremeBlock);
        const r93EmirZeminiOk = !!(r92DefterSaglam || r93DalgaliAmaIslemYapilabilir);
        const r93PiyasaEtiketi = r92DefterSaglam ? 'SAĞLAM' : r93DalgaliAmaIslemYapilabilir ? 'DALGALI AMA İŞLEM YAPILABİLİR' : r93PiyasaTehlikeli ? 'TEHLİKELİ' : r88PiyasaBozuk ? 'BOZUK' : 'SAĞLAM';
        // R94: güçlü trendde son mum ters ve sertse kör kovalamaz; devam kırılımı/süpürme yoksa bekler veya karşı yön radarına bırakır.
        const r93SonMumKoru = !!(r93SonMumTers && (r93MerdivenDevamOk || r89SuperMikroYapiOk || (r88CanliHamleIzi && Number(r47Readiness||0) >= 8)) && !r39Side?.breakConfirmed && !fresh5mImpulse && !directSweepOk && !r93DonusRadariOk);
        // R110: ICT yerel değişkenler
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
        // R110 Köprüsü: Sıralı ICT akışı — her adım zorunlu
        // Temel şartlar R97 seviyesinde tutuldu: R47≥6, mikro≥8, teyit≥2
        // R111: Sıkışma + Squeeze Flow yerel değişkenler
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
        // R111 yön uyumu: R94 merdiven veya R110 ChoCH aktifse R111 çalışmaz
        const r111YonUyumlu      = !!(isL ? r111LongOk : r111ShortOk);
        const r111BloklandMi     = !!(r93MerdivenDevamOk || r110ChoCH);
        // R111 köprüsü: sıkışma ≥8 mum + patlama + skor≥3 + yön uyumlu + bloklanmamış
        const r111KoprusuOk = !!(
          !r111BloklandMi && r111YonUyumlu &&
          r111SiksmaAdet >= 8 && r111SqueezeSkor >= 3 &&
          !hardVeto && !mmVeryStrongOpposite &&
          Number(r47Readiness || 0) >= 5 &&
          Number(r88MikroSkor || 0) >= 6 &&
          !r41FallingKnifeBlock && !r41RisingKnifeBlock
        );
        const r110IctKoprusuOk = !!(
          r110YonUyumlu && r110ChoCH && !hardVeto &&
          Number(r47Readiness || 0) >= 6 && Number(r88MikroSkor || 0) >= 8 &&
          Number(r88AkisTeyidiSayisi || 0) >= 2 &&
          !mmVeryStrongOpposite && !r41FallingKnifeBlock && !r41RisingKnifeBlock
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

        const r50AutoPermissionOk = !!(r88VurKacOk || r86FormasyonVeriTeyitOk || r75RetestBridgeOk || r74Top10ProScalperOk || r68UnifiedScalperCoreOk || r67ScalperCoreHuntEntryOk || r65ScalperCoreOk || r50DirectSweepMatrixOk || r50NonSweepMatrixOk || r51DirectSweepMinEdgeOk || r53SmartEdgeScoreOk || r54MicroProbeOk || r57ScalperBTierBridgeOk || r61TrendContinuationBridgeOk || r62CounterTrendTrapBridgeOk); // R86/R77/R75

        const nonSweepQualityOk = r88VurKacOk || r86FormasyonVeriTeyitOk || r75RetestBridgeOk || r74Top10ProScalperOk || r68UnifiedScalperCoreOk || r67ScalperCoreHuntEntryOk || r65ScalperCoreOk || r47CompositeNonSweepOk || r48DirectSweepBalanceOk || r49DirectSweepUnlockOk || r50NonSweepMatrixOk || r53SmartEdgeScoreOk || r54MicroProbeOk || r57ScalperBTierBridgeOk || r61TrendContinuationBridgeOk || r62CounterTrendTrapBridgeOk; // R86/R77/R75

        // R116: HTF Likidite Süpervizörü — R115 seviyeleri sadece köprü değil, risk amiri de olmalı.
        // R113/R115'te eski R97/R88 yolları 1H/4H direnç/BSL dibinde LONG veya destek/SSL üstünde SHORT açabiliyordu.
        // Bu guard şunu yapar: Karşı HTF likidite seviyesi yakınsa, legacy köprüler emir açamaz;
        // sadece R115 body-reclaim/MSS veya gerçek kabul edilmiş kırılım/retest geçerse izin verilir.
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
            // Direnç üstüne tek wick değil: gövde/kapanış zone üstüne kabul edilmeli ve önceki mum da çok geride kalmamalı.
            return !!(zH > 0 && last.c > zH * 1.001 && bodyLow > zH * 0.998 && closePos >= 0.55 && prev.c > zH * 0.996);
          }
          // Destek altına tek wick değil: gövde/kapanış zone altına kabul edilmeli.
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

        // R114: MM sweep/stop-hunt artık tek başına yön değildir. Eğer 5m body-shift ters akıyorsa
        // ve gövde reclaim gelmediyse B+ emir açılmaz. R110 ICT veya R111 squeeze gibi gerçek yapı
        // onayı varsa bu koruma yolu boğmaz.
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

        // R117: HTF ters-köşe hedefleyici.
        // R116 sadece yanlış yönü blokluyordu. Bu katman aynı HTF seviyede doğru ters yönü
        // ayrıca hedefler: 1H/4H BSL/dirençte SHORT, 1H/4H SSL/destekte LONG.
        // Asla "direnç var hemen short" yapmaz; 5m wick/reclaim, MSS/ChoCH veya body-shift kanıtı ister.
        const r117TrapLevel = isL ? r110NearSSL : r110NearBSL;
        const r117TrapApproach = !!(isL ? r110ICT?.approachingSSL : r110ICT?.approachingBSL);
        const r117TrapSweepTaken = !!(isL ? r110SSLAlindi : r110BSLAlindi);
        const r117TrapTf = String(r117TrapLevel?.label || r117TrapLevel?.tf || '');
        const r117TrapDist = Number(r117TrapLevel?.dist ?? 999);
        const r117TrapMajor = !!(r117TrapLevel && (['4H','1H'].includes(r117TrapTf) || Number(r117TrapLevel?.strength||0) >= 6));
        const r117NearTrapHTF = !!(r117TrapMajor && (r117TrapApproach || r117TrapDist <= 0.95));
        // R118: HTF ters-köşe bölgesinde 5m mum formasyon teyidi. Engulf/star/hammer/shooting-star vb.
        // sadece OHLC gövde-fitil matematiğiyle okunur; tek başına emir değil, R117 kanıt kalitesi sağlar.
        const r118Candle = r118AnalyzeCandlePlaybook(k5m, isL ? 'LONG' : 'SHORT', r117TrapLevel);
        const r118CandleOk = !!(r118Candle?.ok);
        const r118CandleStrong = !!(r118Candle?.strong);
        const r118CandleOzet = r118Candle?.ozet || '';
        // LONG için destek/SSL altına gerçek kabul varsa long değil; SHORT için direnç/BSL üstüne gerçek kabul varsa short değil.
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
        // R118 hassasiyet: HTF ters-köşe için ya R115 ICT aynı yön temiz olacak, ya MSS+body-reclaim birlikte gelecek,
        // ya da 5m onaylı mum formasyonu olacak. Sadece “terazi/body-shift” artık ters işlem açtırmaz.
        const r117PrecisionCandleOk = !!(r117IctSameSideOk || (r117MssOk && r117BodyReclaimOk) || r118CandleOk || (r118CandleStrong && r117TrapSweepTaken));
        const r117TrapEvidenceOk = !!(r117TrapEvidenceRawOk && r117PrecisionCandleOk);
        // R158: r117FlowOk güçlendirildi — FOLKS/ZEC analiz hataları:
        // Sadece oiBridgeOk veya fundBridgeOk ile delta karşıyken HTF reversal açılıyordu.
        // Artık delta uyumu (CVD/tick) VEYA r125 canlı akış ZORUNLU + en az 1 ek teyit.
        // R158 FIX: r125SideFlow evalDecision scope'unda tanımlı değil — r125Flow'dan hesapla
        const r117R125SideFlow = r125FlowForSide(r125Flow, isL ? 'LONG' : 'SHORT');
        const r117LiveFlowBase = r120Bool(deltaOkStrict || r117R125SideFlow.ok || r117R125SideFlow.strong);
        const r117SecondaryTeyit = r120Bool(oiBridgeOk || obSameSide || fundBridgeOk || mmSameSide || cvdBridgePass ||
          (r22FundingTrap.detected && ((isL && r111ShortSqueeze) || (!isL && r111LongSqueeze))));
        const r117FlowOk = r120Bool(
          (r117LiveFlowBase && r117SecondaryTeyit) ||
          (r117LiveFlowBase && Number(r88AkisTeyidiSayisi||0) >= 3) ||
          (deltaOkStrict && r117R125SideFlow.ok)  // delta + canlı orderbook = güçlü teyit
        );
        // R158: r117HtfReverseOk — r117LiveFlowBase (delta/r125) ZORUNLU hale geldi.
        // FOLKS/ZEC hatası: sweep+choch var ama canlı akış karşı yönde → büyük kayıp.
        // Şimdi: delta veya canlı orderbook teyidi olmadan HTF reversal açılmıyor.
        const r117HtfReverseOk = !!(
          r117NearTrapHTF && !r117AcceptedAgainst && r117TrapEvidenceOk && r117FlowOk &&
          r117LiveFlowBase &&   // R158: canlı akış ZORUNSALlaştırıldı
          !hardVeto && !signalDecayAutoBlock && !poorLiquidity && !atrExtremeBlock &&
          r93EmirZeminiOk && Number(r47Readiness || 0) >= 6 && Number(r88MikroSkor || 0) >= 8 &&
          Number(r88AkisTeyidiSayisi || 0) >= 2 &&
          !(isL ? r41FallingKnifeBlock : r41RisingKnifeBlock)
        );
        const r117HtfReverseReason = r117HtfReverseOk
          ? `R118 HTF ters-köşe hedefi: ${isL?'SSL/destek/demand → LONG':'BSL/direnç/supply → SHORT'} (${r117TrapTf||'-'} ${r117TrapLevel?.price||'-'} uzaklık %${Number(r117TrapDist||0).toFixed(2)}) · kanıt:${r117TrapSweepTaken?'sweep ':''}${r117MssOk?'MSS ':''}${r117BodyReclaimOk?'body-reclaim ':''}${r117BodyShiftOk?'body-shift ':''}${r118CandleOk?'mum-formasyon ':''}· ${r118CandleOzet}`
          : '';

        const entryPermissionOk = (sweepRequired ? directSweepOk : (directSweepOk || nonSweepQualityOk || r50AutoPermissionOk || r117HtfReverseOk)) && !r114TrapBlock && (!r116HtfGuardBlock || r117HtfReverseOk);
        // R138 FIX1: r117HtfReverseOk (sweep+gövde+mum onaylı) → r116HtfGuardBlock bypass edilir
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
          // R75-FIX6: Retest ile giriş — hareket ilerlemiş ama fiyat EMA/VWAP bölgesine döndüyse
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

        // R35: 5m fırsat köprüsü. Bu kör giriş değildir: gerçek entry/soft-entry +
        // taze 5m/15m mikro teyit + en az iki ağırlıklı bağlam ister.
        // Amacı R34'teki 'her şey B, hiç emir yok' kilidini açmak.
        const r35ScalperBridge = !!(
          !hardVeto && !signalDecayAutoBlock && !rvolVeryLow && !poorLiquidity && r42FlowGate &&
          sc >= minAutoScore && priorityScore >= 64 &&
          (hasEntry || nonSweepQualityOk || (softEntry && fresh5mImpulseOrRecent) || (softEntry && r37EarlyOk)) &&
          microConfirmR35 && !r37RetestWait &&
          (bridgeCount >= 2 || priorityScore >= 72 || (r37EarlyOk && priorityScore >= 66)) &&
          !mtfStrongOpposite && !mmVeryStrongOpposite
        );

        // A: temiz otomatik. B+: kontrollü otomatik. R35 scalper bridge yüksek kaliteli B'leri B+ yapar.
        // R41: A-tier artık CVD/tick tamamen köprüyle varsayılarak geçemez; gerçek canlı flow ister.
        // Flow eksikse en fazla B+ kontrollü adaya düşer, A-tier otomatik güven etiketi alamaz.
        const isTierA = !hardVeto && !signalDecayAutoBlock && !r37RetestWait && entryPermissionOk && directSweepOk && hasEntry && deltaOkStrict && microConfirmR35 && sc>=Math.max(68, minAutoScore) && priorityScore>=62;
        const r53TierScoreOk = !!(sc >= minAutoScore || r117HtfReverseOk || r88VurKacOk || r86FormasyonVeriTeyitOk || r75RetestBridgeOk || r74Top10ProScalperOk || r68UnifiedScalperCoreOk || r67ScalperCoreHuntEntryOk || r65ScalperCoreOk || r53SmartEdgeScoreOk || r54MicroProbeOk || r57ScalperBTierBridgeOk || r61TrendContinuationBridgeOk || r62CounterTrendTrapBridgeOk); // R75
        const isTierBPlus = !isTierA && (r117HtfReverseOk || r88VurKacOk || r86FormasyonVeriTeyitOk || r75RetestBridgeOk || r74Top10ProScalperOk || r68UnifiedScalperCoreOk || r67ScalperCoreHuntEntryOk || r65ScalperCoreOk || (!hardVeto && !signalDecayAutoBlock && !r37RetestWait)) && entryPermissionOk && r53TierScoreOk && ( // R75: r75RetestBridgeOk eklendi
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
        const autoOk=(isTierA||isTierBPlus) && entryPermissionOk;
        return{ pass:tier!=='WAIT', tier, score:sc, passCount,
          reasons, blocks, autoOk,
          priorityScore, priorityTags:priorityTags.slice(0,10), priorityFamily, hardVeto, hardVetoReasons,
          cvdMissing, cvdWarmingBridge, bridgeCount, cvdBridgeQualityOk, cvdBridgePass, r42FlowGate, microConfirm,
          sweepRequired, directSweepOk, nonSweepQualityOk, entryPermissionOk, entryPermissionReason, r45CvdAlternativeOk, r45CvdOkForBridge, r45RvolStatus, r45Rvol, r45RvolOkForBridge, r45TopMoverSecondImpulseWatch,
          r47Readiness, r47Needed, r47TimingPts, r47FlowPts, r47ContextPts, r47StructurePts, r47RvolPts, r47FlowEnough, r47CompositeNonSweepOk, r48DirectSweepBalanceOk, r48CvdNotAgainst, r49DirectSweepUnlockOk, r49CvdSafe, r49ContextOk, r49TimingOk, r49StructureOk,
          r50AutoPermissionOk, r50DirectSweepMatrixOk, r50NonSweepMatrixOk, r51DirectSweepMinEdgeOk, r53SmartEdgeScoreOk, r54MicroProbeOk, r57ScalperBTierBridgeOk, r61TrendContinuationBridgeOk, r61TrendEffectiveScore, r61TrendContinuationBoost, r61TrendPriorityOk, r61MtfFullTrendOk, r60StrongTrendContinuation, r62CounterTrendTrapBridgeOk, r62TrapHardClean, r62CounterTrendTrapContextOk, r62CounterTrendTrapFlowOk, r62SideTrapEventOk, r88VurKacOk, r90CanliKopmaOk, r88VurKacEnabled, r92VurKacAdayOk, r92GucluVurKacOk, r92NormalVurKacOk, r92SadeceIzleOk, r92IslemTipi, r92RiskDurumu, r92Terazi, r92DefterSaglam, r93EmirZeminiOk, r93PiyasaEtiketi, r93PiyasaTehlikeli, r93PiyasaDalgali, r93DalgaliAmaIslemYapilabilir, r93PiyasaIslemYapilabilir, r93MerdivenDevamOk, r93DonusRadariOk, r93DonusSkor, r93MerdivenSkor, r93SonMumKoru, r93Merdiven, r89SuperMikroYapiOk, r94CanliTrendZeminKurtarmaOk, r96DalgaliZeminVurKacOk, r88MikroSkor, r88AkisTeyidiSayisi, r88ScoreFloor, r89ScoreFloor, r88PiyasaBozuk, r88SpreadWide, r88DefterInce, r88OynaklikAsiri, r88CanliHamleIzi, r86FormasyonVeriTeyitOk, r86FormasyonPuan, r86KarsiFormasyonPuan, r86VeriTeyitSayisi, r86CanliTetikOk, r86KarsiFormasyonGucu, r75RetestBridgeOk, r75LateChaseHard, r75R47MinBypass, r74Top10ProScalperOk, r74ImpulseEntryOk, r74Top10ContextBypassOk, r74ScoreFloor, r68UnifiedScalperCoreOk, r68EntryEventOk, r68TrendContextOk, r68CounterTrapContextOk, r68ReadinessOk, r68ScoreOk, r68CriticalHardBlock, r69PriorityContextOverrideOk, r69ContextOk, r69PriorityExecutionOk, r67ScalperCoreHuntEntryOk, r65ScalperCoreOk, r65ScalperCoreTrendOk, r65ScalperCoreCounterTrapOk, r65ScalperCoreHardVeto, r66WyckoffTrapReclaimOk, r66WyckoffHardVeto, r53SmartEdgeBoost, r53EffectiveScore, r53ScoreFloor, r53CvdSmartSafe, r53TierScoreOk, r50PriorityBoost, r50EffectivePriority, r50MinReadiness, r50HardClean, r50FlowOrContextOk, r50StructureOrTimingOk, r50RvolUsable,
          r46PerfectAlignCount, r46PerfectAlignBonus, r46CvdGradeBonus, r46SqueezeQualityScore, r46SpringQuality, r46ExhaustionShort,
          rvolVeryLow, atrBlocking, atrWarnForAuto, atrExtremeBlock, poorLiquidity, signalDecayAutoBlock, scalperBridge:r35ScalperBridge, fresh5mImpulse, fresh5mImpulse2Bridge, fresh5mImpulseOrRecent, fresh15mConfirm, r37Timing:r37Side, r37LateChaseBlock, r37RetestWait, r37EarlyOk, r38RetestBridgeOk, r38TopMoverStrong, r38MarketCtx, r39SR:r39Side, r39TargetNearBlock, r39AgainstZone, r39Confluence, r41TrapBlock, r42TrapReclaimOk, r41FallingKnifeBlock, r41RisingKnifeBlock,
          r116HtfGuardBlock, r116HtfGuardReason, r116CounterLevel, r116CounterTf, r116CounterDist, r116NearCounterHTF, r116AcceptedCounterBreak, r116CounterSweepTaken,
          r117HtfReverseOk, r117HtfReverseReason, r117TrapLevel, r117TrapTf, r117TrapDist, r117NearTrapHTF, r117TrapSweepTaken, r117AcceptedAgainst, r117BodyReclaimOk, r117BodyShiftOk, r117MssOk, r117FlowOk, r117TrapEvidenceOk, r117TrapEvidenceRawOk, r117PrecisionCandleOk, r118CandleOk, r118CandleStrong, r118CandleOzet, r118Candle,
          r125Flow, r125OrderflowSummary:r125Flow?.summary||'', r126FlowSummary:r125Flow?.r126?.summary||'', r125BookImb:r125Flow?.book?.nearImb, r125BookVelocity:r125Flow?.book?.velocity, r125LiveDelta:r125Flow?.delta, r125LiveDeltaPct:r125Flow?.deltaPct, r125Aggression:r125Flow?.aggression, r125BestSide:r125Flow?.bestSide,
          r140Phase, r140EqHL, r140OiVel, r140BtcDiv, r140Rvol,
          r114Shift, r114OppositeShift, r114ReclaimOk, r114ExtremeZone, r114SweepTrapFamily, r114ContinuationProof, r114TrapBlock, r114TrapReason,
          wickTrapFlip: {
            against:      r25WickTrapMap ? (isL ? r25WickTrapMap.upperTrap : r25WickTrapMap.lowerTrap) : false,
            favorable:    r25WickTrapMap ? (isL ? r25WickTrapMap.lowerTrap : r25WickTrapMap.upperTrap) : false,
            suggestedSide:r25WickTrapMap?.dominant==='UPPER_REJECTION_TRAP' ? 'SHORT'
                          : r25WickTrapMap?.dominant==='LOWER_REJECTION_TRAP' ? 'LONG' : null,
            strength:     r25WickTrapMap?.strength || 0,
            notes:        r25WickTrapMap?.notes || [],
          },
          r88VurKac: {
            aktif: r88VurKacEnabled, ok: r88VurKacOk, aday: r92VurKacAdayOk, islemTipi: r92IslemTipi, riskDurumu: r92RiskDurumu, terazi: r92Terazi, defterSaglam: r92DefterSaglam, emirZeminiOk: r93EmirZeminiOk, guclu: r92GucluVurKacOk, normal: r92NormalVurKacOk, sadeceIzle: r92SadeceIzleOk, canliKopma: r90CanliKopmaOk, superMikro: r89SuperMikroYapiOk, mikroSkor: r88MikroSkor, teyitSayisi: r88AkisTeyidiSayisi, puanTabani: r89ScoreFloor,
            canliHamleIzi: r88CanliHamleIzi, zeminKurtarma: r94CanliTrendZeminKurtarmaOk, dalgaliBaglanti: r96DalgaliZeminVurKacOk, r109SweepReclaimKoprusuOk, r109ReclaimOk, r109ReclaimSkor, r110IctKoprusuOk, r110Phase, r110SSLAlindi, r110BSLAlindi, r110ChoCH, r110YonUyumlu, r110NearSSL, r110NearBSL, r110FVG, r110EntryOk, ictDashboard: r110ICT?.dashboardText||'', r111KoprusuOk, r111SiksmaBreakout, r111SqueezeSkor, r111SiksmaAdet, r111ShortSqueeze, r111LongSqueeze, r111OiChgPct, r111ObBaskisi, r111DemandOB, r111SupplyOB, siksmaOzet: r111Siksma?.ozet||'', r116HtfGuardBlock, r116HtfGuardReason, r116CounterLevel, r116CounterDist, r116AcceptedCounterBreak, r117HtfReverseOk, r117HtfReverseReason, r117TrapLevel, r117TrapDist, r117AcceptedAgainst, r117PrecisionCandleOk, r118CandleOk, r118CandleOzet, r118Candle, piyasaBozuk: r88PiyasaBozuk, piyasaEtiketi: r93PiyasaEtiketi, piyasaTehlikeli: r93PiyasaTehlikeli, piyasaDalgali: r93PiyasaDalgali, dalgaliAmaIslemYapilabilir: r93DalgaliAmaIslemYapilabilir, makasGenis: r88SpreadWide, defterInce: r88DefterInce, oynaklikAsiri: r88OynaklikAsiri,
            merdivenDevam: r93MerdivenDevamOk, donusRadari: r93DonusRadariOk, donusSkor: r93DonusSkor, merdivenSkor: r93MerdivenSkor, sonMumKoru: r93SonMumKoru,
            not: r93PiyasaTehlikeli ? 'Piyasa zemini tehlikeli; işlem yok.' : (r93DalgaliAmaIslemYapilabilir ? 'Piyasa dalgalı ama canlı trend/dönüş kanıtı işlem yapılabilir düzeyde.' : (r88VurKacOk ? 'Vur-kaç motoru veriyle desteklenen 5m hamle gördü.' : 'Vur-kaç için canlı hamle veya teyit yetersiz.'))
          },
          r86PatternConfirm: {
            ok: r86FormasyonVeriTeyitOk,
            puan: r86FormasyonPuan,
            karsiPuan: r86KarsiFormasyonPuan,
            veriTeyitSayisi: r86VeriTeyitSayisi,
            canliTetik: r86CanliTetikOk,
            karsiFormasyonGucu: r86KarsiFormasyonGucu,
            formasyonlar: trPatternList(r86FormasyonAdlari),
            teyitler: r86VeriTeyitleri.filter(([,ok])=>!!ok).map(([name])=>name),
            not: r86FormasyonVeriTeyitOk ? 'Formasyon veriyle desteklendi; tek başına emir değildir.' : 'Formasyon tek başına yeterli değil; veri teyidi beklenir.'
          },
          r22: {
            sector: r22Rotation.sector,
            rotation: r22Rotation,
            signalDecay: r22Decay,
            testQuality: r22TestQ,
            fundingTrap: r22FundingTrap,
            liqWaterfall: r22LiqWaterfall,
            scoreLedger: r23ScoreLedger,
            priorityFamily,
          },
          r29: r29Context,
          reason: tier==='A'?`🎯 A-Tier: ${reasons.slice(0,3).join(' + ')} · terazi ${priorityScore}`:
                  tier==='B+'?`⚖️ B+ kontrollü: ${reasons.slice(0,3).join(' + ')} · terazi ${priorityScore}`:
                  tier==='B'?`👁 ${reasons.slice(0,2).join(' + ')} — elle bak · terazi ${priorityScore}`:
                  `❌ ${blocks.slice(0,2).join(', ')}` };
      }
      // R30: yön seçimi artık ham longScore/shortScore ile değil, iki tarafın final karar kalitesiyle yapılır.
      // Böylece ZEC/FET benzeri tepeden LONG veya dipten SHORT tuzaklarında ters tarafın temizliği kaçırılmaz.
      const longDecisionRaw  = evalDecision('LONG');
      const shortDecisionRaw = evalDecision('SHORT');
      const longDecision  = r120SingleBrainDecision('LONG', longDecisionRaw, longScore, minAutoScore);
      const shortDecision = r120SingleBrainDecision('SHORT', shortDecisionRaw, shortScore, minAutoScore);
      function decisionRank(side, d) {
        if (!d || !d.pass) return -9999;
        const tierPts = d.tier === 'A' ? 300 : d.tier === 'B+' ? 210 : d.tier === 'B' ? 90 : 0;
        const r117Bonus = d.r117HtfReverseOk ? 45 : 0;
        const scorePts = Number(d.score || 0);
        const calibPts = Number(d.r142CalibratedEdge || d.brainConfidence || 0) * 1.1;
        const psPts = Number(d.priorityScore || 0) * 1.1;
        const vetoPenalty = d.hardVeto ? 500 : 0;
        const missingPenalty = d.cvdMissing ? 18 : 0;
        const balancePts = Number(d.r148BalanceBonus || 0) * 3.0 - Number(d.r148WrongPenalty || 0) * 3.0 + (d.r148ReversalSideOk ? 38 : 0) - (d.r148WrongSideBlock ? 120 : 0);
        return tierPts + scorePts + calibPts + psPts + r117Bonus + balancePts - vetoPenalty - missingPenalty;
      }
      const lRank = decisionRank('LONG', longDecision);
      const sRank = decisionRank('SHORT', shortDecision);
      function watchRank(side, d) {
        if (!d) return -9999;
        const conf = Number(d.brainConfidence || 0);
        const sc = Number(d.score || 0);
        const flow = side === 'LONG' ? Number(d.r125Flow?.longEdge || 0) : Number(d.r125Flow?.shortEdge || 0);
        const modeBonus = String(d.brainMode||'NO_EDGE') === 'NO_EDGE' ? -25 : 0;
        const danger = d.brainModeQualityBlock || d.brainHtfCounterWait || d.hardVeto || d.r114TrapBlock || (d.r116HtfGuardBlock && !d.r117HtfReverseOk) ? -18 : 0;
        const balance = Number(d.r148BalanceBonus || 0) * 2.0 - Number(d.r148WrongPenalty || 0) * 2.0 + (d.r148ReversalSideOk ? 22 : 0) - (d.r148WrongSideBlock ? 60 : 0);
        return conf*1.8 + sc*0.55 + flow + modeBonus + danger + balance;
      }
      let recommendation='WAIT', decisionChain=null;
      if (lRank > -9999 || sRank > -9999) {
        if (Math.abs(lRank - sRank) < 18 && longDecision?.autoOk && shortDecision?.autoOk) {
          // iki taraf da güçlü ama fark zayıfsa MM kararsızlığı: işlem yok
          recommendation='WAIT';
          decisionChain={ pass:false, tier:'WAIT', score:Math.max(longScore, shortScore), autoOk:false, brainAction:'WATCH', brainMode:'CONFLICT', brainConfidence:Math.max(longDecision?.brainConfidence||0, shortDecision?.brainConfidence||0), priorityScore:Math.max(longDecision?.priorityScore||0, shortDecision?.priorityScore||0), reason:`❌ Çift yön çatışması: LONG ${longDecision?.tier}/${longDecision?.priorityScore} SHORT ${shortDecision?.tier}/${shortDecision?.priorityScore}`, sideDecisionSummary:r128SideDecisionSummary(longDecision, shortDecision) };
        } else if (lRank > sRank) { recommendation='LONG'; decisionChain=longDecision; }
        else { recommendation='SHORT'; decisionChain=shortDecision; }
      } else {
        // R127: PASS yokken decisionChain'i boş bırakma. Tek beyin gerçekten çalıştıysa,
        // en güçlü WATCH tarafını panel/auto monitöre taşı. Bu emir açmaz; sadece neden BEKLE dediğini gösterir.
        const lw = watchRank('LONG', longDecision), sw = watchRank('SHORT', shortDecision);
        decisionChain = (lw >= sw ? longDecision : shortDecision) || { pass:false, tier:'WAIT', score:Math.max(longScore, shortScore), autoOk:false, reason:'5m Fırsat Beyni WATCH: yeterli veri/edge yok' };
        decisionChain.pass = false;
        decisionChain.autoOk = false;
        decisionChain.entryPermissionOk = false;
        decisionChain.brainAction = decisionChain.brainAction || 'WATCH';
        decisionChain.reason = decisionChain.brainSummary || decisionChain.reason || `🧠 5m Fırsat Beyni WATCH: skor ${Math.max(longScore,shortScore)}/${minAutoScore}, trade eşiği oluşmadı`;
        // R128: decisionChain içine LONG/SHORT nesnelerini ham olarak koyma.
        // decisionChain çoğu zaman longDecision/shortDecision nesnesinin kendisidir; ham sideDecisions eklemek
        // decisionChain.sideDecisions.LONG === decisionChain döngüsünü üretir ve res.json JSON.stringify hatası verir.
        decisionChain.sideDecisionSummary = r128SideDecisionSummary(longDecision, shortDecision);
      }
      if (!decisionChain?.pass) recommendation='WAIT';
    const score=recommendation==='LONG'?longScore:recommendation==='SHORT'?shortScore:Math.max(longScore,shortScore);

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

    // R22 hafıza: Long/Short ekranı ile Auto aynı piyasa nabzını görsün.
    // Ek çağrı yok; yalnızca bu analiz sonucunun özetini 12dk saklar.
    r22RememberAnalysis(full, { recommendation, longScore, shortScore, signalAgeMin, decisionChain });
    // R29: Market breadth güncelle
    if (recommendation !== 'WAIT')
      updateMarketBreadth(full, recommendation, Math.max(longScore, shortScore));

    const r128DecisionChain = r128SafeForJson(decisionChain);
    const r128SideDecisions = { LONG: r128SafeForJson(longDecision), SHORT: r128SafeForJson(shortDecision) };
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
      scoreGuards:{
        cvdValid:!!(cvdForCheck.valid && ((cvdForCheck.buy||0)+(cvdForCheck.sell||0)>0)),
        longRiskCap, shortRiskCap, longAdverse, shortAdverse,
        rsi4h, mmTarget, mmConf, oiDiv
      },
      smartMoney:{topLongPct:+topLong.toFixed(1),topShortPct:+(100-topLong).toFixed(1),globalLongPct:+globalLong.toFixed(1),globalShortPct:+(100-globalLong).toFixed(1),divergence:smDiv},
      orderBook:{imbalance:+bookImb.toFixed(1)},
      r125OrderFlow:r125Flow,
      // ── YENİ ANALİZ KATMANLARI ──────────────────────────────────────────────
      volumeProfile: { '1h':vpvr1h, '4h':vpvr4h },
      premiumDiscount: { '1h':pd1h, '4h':pd4h },
      equalLevels: { '1h':eqLvl1h, '4h':eqLvl4h },
      rvol: { '1h':rvol1h, '4h':rvol4h },
      breakerBlocks: { '1h':brk1h, '4h':brk4h },
      rsiDivergence: { '1h':rsiDiv1h, '4h':rsiDiv4h },
      mtfBias,
      liquidityVoids: liqVoids1h,
      // ── R15 YENİ MODÜLLER ──────────────────────────────────────────────────
      r15: {
        squeeze: { '1h':sqz1h, '4h':sqz4h },
        cmf: { '1h':cmf1h, '4h':cmf4h },
        ewo: ewo1h,
        weisWave: weis1h,
        choch: { '1h':choch1h, '4h':choch4h },
        liquidityQuality: liqQual,
        fundingMomentum: fundMom,
        atrGate: { atrPct:+atrPct.toFixed(2), slPct:slPctForGate, warn:atrGateWarn, block:atrGateBlock },
      },
      longScore, shortScore, recommendation, decisionChain: r128DecisionChain,
      sideDecisions: r128SideDecisions,
      r22: decisionChain?.r22 || null,
      r29: r29Context,
      r37: r37Timing,
      r39: r39SR,
      signals:recommendation==='LONG'?signals.long.slice(0,8):signals.short.slice(0,8),
    });
  } catch(e) {
    const ev = pushCritical('ANALYZE_' + full, e, { symbol: full, route:'/api/analyze' });
    res.status(400).json({ ok:false, code:'ANALYZE_EXCEPTION', symbol:full, error:ev.message, scope:ev.scope, ts:ev.ts });
  }
});

// ── HESAP ─────────────────────────────────────────────────────────────────────
// ── RAILWAY IP — Binance IP whitelist için ────────────────────────────────────
app.get('/api/my-ip', async (_req, res) => {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  const providers = [
    'https://api.ipify.org?format=json',
    'https://api64.ipify.org?format=json',
    'https://ifconfig.me/all.json',
    'https://ifconfig.co/json',
    'https://icanhazip.com'
  ];
  const errors = [];
  for (const u of providers) {
    try {
      const r = await fetch(u, { headers:{'User-Agent':'lazarus-bot'}, signal:AbortSignal.timeout(8000) });
      const txt = await r.text();
      let ip = '';
      try {
        const j = JSON.parse(txt);
        ip = j.ip || j.ip_addr || j.remote_addr || j.query || '';
      } catch(e) {
        ip = txt.trim().split(/\s+/)[0];
      }
      if (/^(?:\d{1,3}\.){3}\d{1,3}$/.test(ip) || /^[a-f0-9:]{8,}$/i.test(ip)) {
        return res.json({ ok:true, ip, provider:u });
      }
    } catch(e) {
      errors.push(`${u}: ${e.message}`);
    }
  }
  res.status(500).json({ ok:false, error:'Railway dış IP alınamadı', detail:errors.slice(0,3) });
});

// ── HESAP ─────────────────────────────────────────────────────────────────────
// FUTURES BALANCE SAFE RESTORE v2
// Amaç: hatasız sürümün hesabı okuyabilen davranışını geri getirirken yeni SL/TP patch'ini bozmaz.
// - API key/secret her yerde trimlenir (telefonda kopyala/yapıştır boşluk/newline bırakabiliyor)
// - v2/account + v2/balance geri eklendi
// - v3/v2 account.assets içindeki USDT satırı da okunur
// - İmzalı bağlantı başarılı ama USDT satırı yoksa bunu açık diagnostik olarak döndürür
app.post('/api/account', async (req, res) => {
  let { apiKey, apiSecret } = req.body || {};
  apiKey = String(apiKey || '').trim();
  apiSecret = String(apiSecret || '').trim();
  if (!apiKey || !apiSecret) return res.status(400).json({ ok:false, error:'API key gerekli' });

  const errors = [];
  const sources = [];
  const balanceSources = [];
  const positionSources = [];
  const debug = [];
  let signedOk = false;
  let sawUsdtAsset = false;
  let w = null, a = null, u = null, positions = [];

  const num = (v) => {
    if (v === undefined || v === null || v === '') return null;
    const n = parseFloat(v);
    return Number.isFinite(n) ? n : null;
  };
  const uniqPush = (arr, v) => { if (v && !arr.includes(v)) arr.push(v); };
  const addErr = (label, e) => errors.push(`${label}: ${safeErrMsg(e)}`);

  function debugOk(label, data) {
    try {
      const item = { label, ok:true, type:Array.isArray(data)?'array':typeof data };
      if (Array.isArray(data)) {
        item.len = data.length;
        item.assets = data.slice(0,8).map(x => String(x.asset || x.marginAsset || x.accountAlias || '?')).join(',');
        const usdt = data.find(x => String(x.asset || x.marginAsset || '').toUpperCase() === 'USDT');
        if (usdt) item.usdtKeys = Object.keys(usdt).slice(0,18).join(',');
      } else if (data && typeof data === 'object') {
        item.keys = Object.keys(data).slice(0,24).join(',');
        if (Array.isArray(data.assets)) {
          item.assetsLen = data.assets.length;
          item.assets = data.assets.slice(0,8).map(x => String(x.asset || x.marginAsset || '?')).join(',');
          const usdt = data.assets.find(x => String(x.asset || x.marginAsset || '').toUpperCase() === 'USDT');
          if (usdt) item.usdtKeys = Object.keys(usdt).slice(0,18).join(',');
        }
      }
      debug.push(item);
      while (debug.length > 14) debug.shift();
    } catch(_) {}
  }
  function debugFail(label, e) {
    debug.push({ label, ok:false, error:safeErrMsg(e) });
    while (debug.length > 14) debug.shift();
  }

  function setBalanceNumbers(obj, source) {
    if (!obj || typeof obj !== 'object') return false;
    // R9: Binance Futures hesap varyasyonları için alan listesi genişletildi.
    // Eski çalışan sürüm sadece totalWalletBalance / availableBalance okuyordu;
    // yeni sürüm USDT satırı yok diye hata basmamalı, account-level toplamı da okuyabilmeli.
    const tw = num(
      obj.totalWalletBalance ?? obj.walletBalance ?? obj.balance ??
      obj.crossWalletBalance ?? obj.totalCrossWalletBalance ?? obj.marginBalance ??
      obj.totalMarginBalance ?? obj.maxWithdrawAmount
    );
    const av = num(
      obj.availableBalance ?? obj.maxWithdrawAmount ?? obj.withdrawAvailable ??
      obj.availableBalanceOfCross ?? obj.crossWalletBalance ?? obj.totalCrossWalletBalance
    );
    const up = num(
      obj.totalUnrealizedProfit ?? obj.totalUnrealizedPnL ?? obj.totalCrossUnPnl ??
      obj.crossUnPnl ?? obj.unrealizedProfit ?? obj.unRealizedProfit
    );
    let ok = false;
    if (tw !== null) { w = tw; ok = true; }
    if (av !== null) { a = av; ok = true; }
    if (up !== null) { u = up; ok = true; }
    if (ok) {
      uniqPush(sources, source);
      uniqPush(balanceSources, source);
    }
    return ok;
  }

  function pickStable(arr) {
    if (!Array.isArray(arr)) return null;
    return arr.find(x => String(x.asset || x.marginAsset || '').toUpperCase() === 'USDT') ||
           arr.find(x => ['FDUSD','USDC','BUSD'].includes(String(x.asset || x.marginAsset || '').toUpperCase())) ||
           null;
  }

  function setBalanceArray(arr, source) {
    const ub = pickStable(arr);
    if (!ub) return false;
    if (String(ub.asset || ub.marginAsset || '').toUpperCase() === 'USDT') sawUsdtAsset = true;
    return setBalanceNumbers(ub, `${source}.${String(ub.asset || ub.marginAsset || 'ASSET').toUpperCase()}`);
  }

  function setAccount(data, source) {
    if (!data || typeof data !== 'object') return false;
    let ok = false;
    ok = setBalanceNumbers(data, source) || ok;
    if (Array.isArray(data.assets)) ok = setBalanceArray(data.assets, `${source}.assets`) || ok;
    return ok;
  }

  function setPositions(arr, source) {
    if (!Array.isArray(arr)) return false;
    const mapped = arr.filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0).map(p => ({
      symbol:p.symbol,
      side:parseFloat(p.positionAmt)>0?'LONG':'SHORT',
      positionAmt:Math.abs(parseFloat(p.positionAmt)),
      entryPrice:parseFloat(p.entryPrice),
      markPrice:parseFloat(p.markPrice),
      unrealizedProfit:parseFloat(p.unRealizedProfit ?? p.unrealizedProfit ?? 0),
      leverage:parseInt(p.leverage)||0,
      liquidationPrice:parseFloat(p.liquidationPrice||0),
    }));
    if (mapped.length || !positions.length) positions = mapped;
    uniqPush(sources, source);
    uniqPush(positionSources, source);
    return true;
  }

  async function trySigned(label, fn) {
    try {
      const data = await fn();
      signedOk = true;
      debugOk(label, data);
      return data;
    } catch(e) {
      addErr(label, e);
      debugFail(label, e);
      return null;
    }
  }

  try {
    // Çalışan hatasız sürüme en yakın sıra: önce balance/account, sonra positionRisk.
    const bal3 = await trySigned('v3/balance', () => bReq(apiKey, apiSecret, 'GET', '/fapi/v3/balance'));
    if (Array.isArray(bal3) && !setBalanceArray(bal3, 'v3/balance')) errors.push('v3/balance: USDT/stable satırı yok');

    const acc3 = await trySigned('v3/account', () => bReq(apiKey, apiSecret, 'GET', '/fapi/v3/account'));
    if (acc3) { setAccount(acc3, 'v3/account'); setPositions(acc3.positions || [], 'v3/account.positions'); }

    const acc2 = await trySigned('v2/account', () => bReq(apiKey, apiSecret, 'GET', '/fapi/v2/account'));
    if (acc2) { setAccount(acc2, 'v2/account'); setPositions(acc2.positions || [], 'v2/account.positions'); }

    const bal2 = await trySigned('v2/balance', () => bReq(apiKey, apiSecret, 'GET', '/fapi/v2/balance'));
    if (Array.isArray(bal2) && !setBalanceArray(bal2, 'v2/balance')) errors.push('v2/balance: USDT/stable satırı yok');

    const acc1 = await trySigned('v1/account', () => bReq(apiKey, apiSecret, 'GET', '/fapi/v1/account'));
    if (acc1) { setAccount(acc1, 'v1/account'); setPositions(acc1.positions || [], 'v1/account.positions'); }

    // R18: /api/account içinde de positionRisk cache kullanılır; API sekmesi açıkken
    // aynı endpoint'i tekrar tekrar dövüp -1003 üretmez. Balance/account okuma sırası R14 gibi kalır.
    const pr = await trySigned('positionRisk/cache', () => getPositionRiskCached(apiKey, apiSecret));
    if (pr) setPositions(pr, positionRiskState.lastSource || 'positionRisk/cache');

    if ((!Number.isFinite(w) || w === 0) && Number.isFinite(a) && a > 0) w = a;
    if (!Number.isFinite(u)) u = 0;

    if (!signedOk) {
      return res.status(400).json({
        ok:false,
        error:'Binance Futures imzalı bağlantı kurulamadı. API key/secret, Futures yetkisi ve IP whitelist kontrol edilmeli.',
        errors: errors.slice(-12),
        debug,
        hint:'Sıfırla yapıp API Key + gerçek API Secretı yeniden yapıştır. Binance tarafında görünen Railway IP whitelistte olmalı.'
      });
    }

    if (!balanceSources.length) {
      return res.status(400).json({
        ok:false,
        signedOk:true,
        error:'Binance imzalı bağlantı var ama balance/account endpointlerinden bakiye alanı parse edilemedi.',
        errors: errors.slice(-14),
        debug,
        hint:sawUsdtAsset
          ? 'USDT asset satırı geldi ama balance/wallet alan adı beklenenden farklı. Debug içindeki usdtKeys gerekli.'
          : 'Balance/account endpointleri USDT/stable satırı veya account-level toplam döndürmedi. Debug satırındaki endpoint hataları gerekli.'
      });
    }

    return res.json({
      ok:true,
      signedOk:true,
      balanceOk:true,
      source:[...new Set(sources)].join(' + '),
      balanceSource:[...new Set(balanceSources)].join(' + '),
      positionSource:[...new Set(positionSources)].join(' + '),
      totalWalletBalance:Number.isFinite(w)?w:0,
      availableBalance:Number.isFinite(a)?a:0,
      totalUnrealizedProfit:Number.isFinite(u)?u:0,
      positions,
      warning: errors.length ? errors.slice(-4).join(' | ') : undefined,
      debug
    });
  } catch(e) {
    return res.status(400).json({ ok:false, error:safeErrMsg(e), errors:[safeErrMsg(e)], debug });
  }
});

// ── EMİR AÇ ──────────────────────────────────────────────────────────────────
app.post('/api/order', async (req, res) => {
  const{apiKey,apiSecret,symbol,side,leverage,marginType,targetPrice,stopPrice,usdtAmount,maxPositions}=req.body;
  if(!apiKey||!apiSecret||!symbol||!side||!leverage||!targetPrice||!stopPrice||!usdtAmount)
    return res.status(400).json({error:'Eksik parametre'});
  const sym=symbol.toUpperCase().includes('USDT')?symbol.toUpperCase():symbol.toUpperCase()+'USDT';
  const isLong=side.toUpperCase()==='LONG';
  const oSide=isLong?'BUY':'SELL', cSide=isLong?'SELL':'BUY';
  try{
    // R30: emir endpoint'i de pozisyon limitini korur; auto scan concurrency veya çift tıklama pozisyon bindirmesin.
    try {
      const rows = await getPositionRiskCached(apiKey, apiSecret);
      const openRows = Array.isArray(rows) ? rows.filter(p=>Math.abs(parseFloat(p.positionAmt||0))>0) : [];
      const sameSym = openRows.find(p=>String(p.symbol||'').toUpperCase()===sym);
      if (sameSym) throw new Error(`${sym} zaten açık pozisyon var; ikinci emir engellendi`);
      const maxP = normalizeUserMaxPositions(maxPositions || autoConfig?.maxPositions || 1, 1);
      if (openRows.length >= maxP) throw new Error(`Max pozisyon dolu (${openRows.length}/${maxP}); emir engellendi`);
    } catch(limitErr) {
      if (String(limitErr.message||'').includes('engellendi') || String(limitErr.message||'').includes('Max pozisyon')) throw limitErr;
      // PositionRisk okunamazsa güvenli tarafta kal: canlı otomatik emir açma.
      if (autoConfig?.enabled) throw new Error(`Pozisyon limiti doğrulanamadı: ${limitErr.message}`);
    }
    // R150: küçük bakiye / panel marj uyuşmazlığı emir hatasına dönüşmesin.
    // Sadece emirden hemen önce tek kez bakiye kontrolü yapılır; tarama sayısını azaltmaz.
    try {
      const balRows = await bReq(apiKey, apiSecret, 'GET', '/fapi/v2/balance');
      const usdtRow = Array.isArray(balRows) ? balRows.find(x => String(x.asset||'').toUpperCase()==='USDT') : null;
      const av = Number(usdtRow?.availableBalance ?? usdtRow?.balance ?? 0);
      const need = Number(usdtAmount||0) * 1.02;
      if (Number.isFinite(av) && av > 0 && av < need) {
        throw new Error(`Yetersiz kullanılabilir USDT: ${av.toFixed(2)} < panel marj ${Number(usdtAmount||0).toFixed(2)}. Marjı düşür veya bakiye ekle.`);
      }
    } catch(e) {
      if (String(e.message||'').includes('Yetersiz kullanılabilir USDT')) throw e;
      // Bakiye endpointi geçici 429/backoff verdiyse emir zaten bReq governor tarafından bekletilir/hata verir; burada ekstra gürültü üretme.
    }
    if(marginType){try{await bReq(apiKey,apiSecret,'POST','/fapi/v1/marginType',{symbol:sym,marginType:marginType.toUpperCase()});}catch(e){if(!e.message.includes('No need'))console.log('MarginType:',e.message);}}
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
    const levSet = await setSymbolLeverageSafe(apiKey, apiSecret, sym, leverage, Number(usdtAmount||0) * normalizeRequestedLeverage(leverage, 1));
    const safeLeverage = normalizeRequestedLeverage(levSet.leverage, 1);
    if (levSet.adjusted) {
      logAuto(`⚙️ ${sym} kaldıraç düzeltildi: panel ${levSet.requested}x → Binance izinli ${safeLeverage}x. Emir iptal edilmedi.`);
    }
    const qp=stepSize<1?-Math.floor(Math.log10(stepSize)):0;
    const pp=tickSize<1?-Math.floor(Math.log10(tickSize)):0;
    const qty=parseFloat(((parseFloat(usdtAmount)*safeLeverage)/curPrice).toFixed(qp));
    const rnd=p=>parseFloat(parseFloat(p).toFixed(pp));
    if(qty*curPrice<minNot)throw new Error(`Min işlem $${minNot}. Miktarı artır.`);
    const main=await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',{
      symbol:sym,side:oSide,type:'MARKET',quantity:qty,positionSide:'BOTH'
    });
    await new Promise(r=>setTimeout(r,800));
    let execPrice=parseFloat(main.avgPrice||curPrice);
    try{
      const pos=await getPositionRisk(apiKey,apiSecret,{symbol:sym});
      const p=Array.isArray(pos)?pos.find(x=>x.symbol===sym&&Math.abs(parseFloat(x.positionAmt))>0):null;
      if(p&&parseFloat(p.entryPrice)>0)execPrice=parseFloat(p.entryPrice);
    }catch(e){}
    const ratio=execPrice/curPrice;
    const realTP=rnd(parseFloat(targetPrice)*ratio);
    const realSL=rnd(parseFloat(stopPrice)*ratio);
    const ps=realTP.toString(),ss=realSL.toString(),qs=qty.toString();
    console.log(`${sym} giriş:${execPrice} TP:${realTP} SL:${realSL} lev:${safeLeverage}`);

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
    console.log(`${sym} lev:${safeLeverage} giriş:${execPrice} TP:${finalTP} SL:${finalSL} (isLong:${isLong})`);

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

    // Lazarus install_live_sltp_pair_with_proof: yaz + kanıtla + fallback
    const slResult = await installSLTPWithProof(
      apiKey, apiSecret, sym, cSide, finalSL, finalTP, sym
    );

    // ÇALIŞAN PYTHON ÇEKİRDEĞİ İLE AYNI GÜVENLİK:
    // SL/TP Binance üzerinde doğrulanmadan pozisyonu 'başarılı' sayma.
    // Korumasız pozisyon kalırsa acil reduce-only market kapat.
    if (!slResult.ok) {
      if (slResult.skippedClosed) {
        return res.status(400).json({
          ok:false,
          error:`${sym} pozisyon SL/TP yazılmadan önce kapanmış; rescue atlandı`,
          mainOrderId:main.orderId,
          skippedClosed:true,
          sltpError:slResult.error
        });
      }
      let emergencyClose = null;
      try {
        emergencyClose = await safeMarketClosePosition(apiKey, apiSecret, sym, {reason:'ORDER_SLTP_PROOF_FAIL'});
      } catch(closeErr) {
        emergencyClose = { error: closeErr.message };
      }
      return res.status(400).json({
        ok:false,
        error:`${sym} SL/TP Binance üzerinde doğrulanamadı; korumasız pozisyon bırakılmadı, acil kapatma denendi`,
        mainOrderId:main.orderId,
        emergencyClose,
        sltpProof:slResult.proof,
        sltpError:slResult.error
      });
    }

    res.json({ok:true,
      message:`${sym} ${side} açıldı ✅ SL/TP Binance doğrulandı ✅`,
      mainOrderId:main.orderId,
      slAlgoId:slResult.slOrder?.algoId||slResult.slOrder?.clientAlgoId,
      tpAlgoId:slResult.tpOrder?.algoId||slResult.tpOrder?.clientAlgoId,
      tpSuccess:true,slSuccess:true,
      slProof:slResult.proof,
      executedPrice:execPrice,
      details:{symbol:sym,side,quantity:qty,leverage:safeLeverage,requestedLeverage:levSet.requested,leverageAdjusted:levSet.adjusted,leverageReason:levSet.reason,entry:execPrice,target:finalTP,stop:finalSL}
    });
  }catch(e){
    pushCritical('ORDER_ROUTE_ERROR', `${sym}: ${e.message}`);
    res.status(400).json({error:e.message});
  }
});

// ── POZİSYONLAR ──────────────────────────────────────────────────────────────
// ── POZİSYON SNAPSHOT YARDIMCI ───────────────────────────────────────────────
function positionManagerSnapshot(pos, state, note='İzleniyor') {
  return {
    type: note,
    reason: `${pos.symbol||''} canlı izleniyor • BE:${state?.breakEvenSet?'AKTİF':'bekliyor'} • SL/TP:${state?.sltpVerified?'doğrulandı':'kontrol'}`,
    urgency: 'LOW',
    pnlPct: Number(pos.pnlPct||0),
    peakPnl: Number(state?.peakPnl||0),
    currentSL: state?.currentSL||null,
    targetTP: state?.targetTP||null,
    lastCheck: Date.now(),
  };
}

app.post('/api/positions', async (req, res) => {
  const{apiKey,apiSecret}=req.body;
  if(!apiKey||!apiSecret)return res.status(400).json({error:'API key gerekli'});
  try{
    const data=await getPositionRiskCached(apiKey,apiSecret);
    const rawOpen=Array.isArray(data)?data.filter(p=>parseFloat(p.positionAmt)!==0):[];
    const open=[];
    for(const p of rawOpen){
      const amt=parseFloat(p.positionAmt),ep=parseFloat(p.entryPrice),mp=parseFloat(p.markPrice);
      const full=normalizeSymbol(p.symbol);
      const state=trailingState.get(full)||trailingState.get(String(p.symbol||''))||{};
      // R37: Bazı positionRisk/account cevaplarında leverage 1/boş dönebiliyor; canlı state/panel leverage'ı ile tamamla.
      const lev=parseInt(p.leverage)||parseInt(state.leverage)||parseInt(autoConfig?.leverage)||1;
      const side=amt>0?'LONG':'SHORT';
      const pct=ep>0?((mp-ep)/ep*100*lev*(side==='SHORT'?-1:1)).toFixed(2):'0';
      // SL/TP bracket kontrolü — R145: her dashboard yenilemede openOrders/openAlgoOrders dövülmez.
      let brackets={hasSL:false,hasTP:false,sl:null,tp:null,orderCount:0,source:'state'};
      if (state?.sltpVerified && (state.currentSL || state.stopLoss) && (state.targetTP || state.tpPrice || state.target)) {
        brackets.hasSL=true; brackets.hasTP=true;
        brackets.sl=state.currentSL||state.stopLoss||null;
        brackets.tp=state.targetTP||state.tpPrice||state.target||null;
      } else if (!isBinanceBackoffActive()) {
        try{
          const orders=await liveOpenBracketOrders(apiKey,apiSecret,full,{ttlMs:60_000});
          brackets.source='rest-cache';
          brackets.orderCount=orders.length;
          for(const o of orders){
            const kind=orderKind(o);
            const trig=orderTriggerPrice(o);
            if(kind==='SL'){brackets.hasSL=true;if(!brackets.sl||Math.abs(trig-mp)<Math.abs(brackets.sl-mp))brackets.sl=trig;}
            if(kind==='TP'){brackets.hasTP=true;if(!brackets.tp||Math.abs(trig-mp)<Math.abs(brackets.tp-mp))brackets.tp=trig;}
          }
        }catch(e){brackets.error=e.message;}
      } else {
        brackets.source='backoff-skip';
      }
      const margin=ep>0?+(Math.abs(amt)*ep/lev).toFixed(4):null;
      open.push({
        symbol:p.symbol, symbolShort:String(p.symbol||'').replace(/USDT$/,''),
        side, positionAmt:Math.abs(amt), entryPrice:ep, markPrice:mp,
        unrealizedProfit:parseFloat(p.unRealizedProfit??p.unrealizedProfit??0),
        pnlPct:parseFloat(pct), leverage:lev,
        margin, liquidationPrice:parseFloat(p.liquidationPrice),
        stopLoss:state.currentSL||brackets.sl||null,
        target:state.targetTP||brackets.tp||null,
        brackets,
        managerStatus:{...(state.managerStatus||positionManagerSnapshot({symbol:full,pnlPct:parseFloat(pct)},state,'İZLEME')), lastCheck: state.lastCheck || state.managerStatus?.lastCheck || Date.now()},
        r91Exit: state.r91Exit || state.managerStatus?.r91Exit || null,
        exitMode: state.exitMode || state.managerStatus?.exitMode || null,
        profitLockLevel: state.profitLockLevel || state.managerStatus?.profitLockLevel || null,
        entryReason:state.entryReason||null,
        openTime:state.openedAt||state.entryAt||null,
        sltpVerified:!!(state.sltpVerified||(brackets.hasSL&&brackets.hasTP)),
        breakEvenSet:!!state.breakEvenSet,
        karSteps:{step1:!!state.step1Set,step2:!!state.step2Set,step3:!!state.step3Set},
        highWater:state.highWater||null,
        peakPnl:state.peakPnl||null,
      });
    }
    res.json({ok:true,positions:open,serverTime:Date.now()});
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
    const pos = await getPositionRisk(apiKey,apiSecret,{symbol:sym});
    const p = Array.isArray(pos) ? pos.find(x=>x.symbol===sym&&Math.abs(parseFloat(x.positionAmt))>0) : null;
    if (!p) return res.status(400).json({ error:'Açık pozisyon yok' });

    const isLong = parseFloat(p.positionAmt) > 0;
    const cSide  = isLong ? 'SELL' : 'BUY';
    const qty    = Math.abs(parseFloat(p.positionAmt)).toString();

    // Lazarus cancel_symbol_brackets + install_live_sltp_pair_with_proof
    const state = trailingState.get(sym) || {};
    const tp_price = await currentBracketTP(apiKey, apiSecret, sym, isLong, parseFloat(p.entryPrice||newSL), (autoConfig||{}).tpPct, state);
    const mark = parseFloat(p.markPrice||0) || parseFloat(newSL);
    const safeNewSL = isLong ? Math.min(parseFloat(newSL), mark * 0.9997) : Math.max(parseFloat(newSL), mark * 1.0003);
    const result = await installSLTPWithProof(apiKey, apiSecret, sym, cSide, safeNewSL, tp_price, sym);
    const oid = result.slOrder?.algoId || result.slOrder?.clientAlgoId;

    res.json({ ok:result.ok, message:`${sym} SL güncellendi → $${safeNewSL} ${result.ok?'✅':'⚠️'}`, orderId:oid, proof:result.proof });
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

// ── COOLDOWN SİSTEMİ ─────────────────────────────────────────────────────────
// Aynı coine tekrar tekrar girmesini önler. Manuel/SL/TP kapanışında,
// hata durumunda ve açık pozisyon varken cooldown eklenir.
const cooldownMap = new Map(); // symbol (FULLSYM) → expiry timestamp
const COOLDOWN_CLOSE_MS   = CD_MANUAL_MS; // R84: genel kapanış bekleme yedeği
const COOLDOWN_ERR_MS     = CD_ERR_MS_R25; // R84: emir hatası bekleme
// NOT: POSOPEN cooldown kaldırıldı — açık pozisyon koruması alreadyOpen kontrolüyle yapılır
let autoPauseUntil = 0;
let autoPauseReason = '';
function trSideLabel(side) {
  const s = normalizeSide(side);
  if (s === 'LONG') return 'YÜKSELİŞ';
  if (s === 'SHORT') return 'DÜŞÜŞ';
  return 'BEKLE';
}
function trTierLabel(t) {
  const v = String(t || '').toUpperCase();
  if (v === 'A') return 'A kalite';
  if (v === 'B+') return 'B artı';
  if (v === 'B') return 'B izleme';
  if (v === 'WAIT') return 'Bekle';
  if (v === 'CD') return 'Beklemede';
  if (v === 'ERR') return 'Hata';
  return v || 'Bekle';
}
function trEntryLabel(x='') {
  return toTurkishText(String(x || ''));
}
function toTurkishText(txt='') {
  let s = String(txt || '');
  const pairs = [
    [/ENTRY_PERMISSION_FAIL_R68/g, 'giriş izni yok'],
    [/POSITION_ALREADY_CLOSED/g, 'pozisyon zaten kapanmış'],
    [/SYNC_POSITION_ALREADY_CLOSED_BEFORE_SLTP_RESCUE/g, 'pozisyon kapalı; koruma emri yazılmadı'],
    [/EXTERNAL_OR_MANUAL/g, 'kullanıcı veya Binance kapanışı'],
    [/TAKE_PROFIT/g, 'kâr hedefi'],
    [/BREAK_EVEN_SL/g, 'başabaş koruma'],
    [/KAR_TASIMA_SL/g, 'kâr taşıma koruması'],
    [/STOP_LOSS/g, 'zarar kes'],
    [/R14_HARD_LOSS_GUARD/g, 'sert zarar koruması'],
    [/cooldown/ig, 'bekleme'],
    [/re-entry/ig, 'yeniden giriş'],
    [/side-rotation/ig, 'karşı yön radarı'],
    [/geç giriş riski/ig, 'geç giriş riski'],
    [/karşı tuzak/ig, 'karşı tuzak'],
    [/soft-pass/ig, 'kontrollü geçiş'],
    [/hard-gate/ig, 'sert kapı'],
    [/hard/ig, 'sert'],
    [/score-floor/ig, 'puan tabanı'],
    [/floor/ig, 'taban'],
    [/zone:/ig, 'bölge:'],
    [/tier:/ig, 'kademe:'],
    [/score:/ig, 'puan:'],
    [/entry:/ig, 'giriş izi:'],
    [/trap:/ig, 'tuzak:'],
    [/risk/ig, 'risk'],
    [/WAIT-Tier/ig, 'bekleme kademesi'],
    [/WAIT/g, 'BEKLE'],
    [/LONG/g, 'YÜKSELİŞ'],
    [/SHORT/g, 'DÜŞÜŞ'],
    [/\bNO\b/g, 'KALDI'],
    [/\bOK\b/g, 'GEÇTİ'],
    [/\bYES\b/g, 'EVET'],
    [/SCALP/ig, 'vur-kaç'],[/scalp/ig, 'vur-kaç'],[/MICRO/ig, 'mikro'],[/micro/ig, 'mikro'],[/NO_TRADE/ig, 'işlem yok'],[/VUR_KAC/ig, 'vur-kaç'],
  ];
  for (const [re, rep] of pairs) s = s.replace(re, rep);
  return s;
}
function pauseAutoAfterClose(ms=CD_AFTER_CLOSE_PAUSE_MS, reason='Kapanış sonrası kısa sakinleşme') {
  const until = Date.now() + Math.max(0, Number(ms)||0);
  if (until > autoPauseUntil) {
    autoPauseUntil = until;
    autoPauseReason = reason;
  }
}
function getAutoPauseRemainMs() {
  return Math.max(0, Number(autoPauseUntil||0) - Date.now());
}
function isAutoPauseActive() { return getAutoPauseRemainMs() > 0; }

function normalizeSymbol(s) {
  const str = String(s || '').toUpperCase().trim();
  return str.endsWith('USDT') ? str : str + 'USDT';
}
function isOnCooldown(symbol, desiredSide=null, decisionChain=null) {
  const info = getCooldownInfo(symbol);
  if (!info) return false;
  if (desiredSide && canBypassCooldownForReverse(info, desiredSide, decisionChain)) return false;
  return true;
}
// ── R25 SAME_SIDE COOLDOWN MİMARİSİ ───────────────────────────────────────────
// Kapanış cooldown'u sadece AYNI YÖNÜ kilitler; ters yön flip fırsatını kapatmaz.
function normalizeSide(s) {
  const v = String(s || '').toUpperCase().trim();
  if (v === 'LONG' || v === 'BUY') return 'LONG';
  if (v === 'SHORT' || v === 'SELL') return 'SHORT';
  return null;
}
function getCooldownInfo(symbol) {
  const sym = normalizeSymbol(symbol);
  const entry = cooldownMap.get(sym);
  if (!entry) return null;
  // Eski format: sadece sayı (timestamp)
  const info = typeof entry === 'number'
    ? { exp: entry, reason:'legacy', mode:'FULL', side:null, createdAt:Date.now() }
    : entry;
  if (Date.now() > Number(info.exp || 0)) { cooldownMap.delete(sym); return null; }
  return { ...info, symbol:sym, remainMs:Number(info.exp)-Date.now(),
           remainMin: Math.ceil((Number(info.exp)-Date.now())/60000) };
}
function canBypassCooldownForReverse(info, desiredSide, decisionChain) {
  // Sadece SAME_SIDE_AFTER_CLOSE modunda bypass kontrolü yapılır
  if (!info || info.mode !== 'SAME_SIDE_AFTER_CLOSE') return false;
  const ds = normalizeSide(desiredSide);
  if (!ds || !info.side || ds === info.side) return false; // Aynı yön bypass yok
  // Ters yön için bypass: güçlü wick trap flip VEYA A-Tier terazi >= 75
  const flip = decisionChain?.wickTrapFlip || decisionChain?.r22?.wickTrapFlip || {};
  const ps = Number(decisionChain?.priorityScore || 0);
  const tier = String(decisionChain?.tier || '');
  return !!((flip.favorable || flip.suggestedSide === ds) && ps >= 55)
      || (tier === 'A' && ps >= 75);
}

// R166: Coin kısa vadeli performans hafızası — son 2 saatte kayıp/kazanç sayısı
// R157'yi tamamlar: R157 cooldown koyar, R166 edge eşiğini sıkılaştırır
function r166CoinRecentLosses(symbol, lookbackMs=2*60*60*1000) {
  const sym = String(symbol||'').replace('USDT','').toUpperCase();
  const cutoff = Date.now() - lookbackMs;
  return (Array.isArray(tradeLedger)?tradeLedger:[]).filter(t=>
    String(t.symbol||'').toUpperCase()===sym &&
    Number(t.closedAt||0)>=cutoff &&
    Number.isFinite(Number(t.pnlUSDT)) && Number(t.pnlUSDT)<0
  ).length;
}
function r166CoinRecentWR(symbol, lookbackMs=4*60*60*1000) {
  const sym = String(symbol||'').replace('USDT','').toUpperCase();
  const cutoff = Date.now() - lookbackMs;
  const rows = (Array.isArray(tradeLedger)?tradeLedger:[]).filter(t=>
    String(t.symbol||'').toUpperCase()===sym &&
    Number(t.closedAt||0)>=cutoff &&
    Number.isFinite(Number(t.pnlUSDT)) && Number(t.pnlUSDT)!==0
  );
  if (rows.length < 3) return null;
  const wins = rows.filter(t=>Number(t.pnlUSDT)>0).length;
  return wins / rows.length;
}


// R170b: Hedef performans profili — COIN WR DEĞİL, hesap geneli günlük/haftalık/aylık WR+PNL+PF.
// Kullanıcı hedefi: günlük / haftalık / aylık genel performans. Coin bazlı WR sadece tekrar-kayıp freni olabilir;
// hedef WR modu ASLA tek coin WR'ına göre belirlenmez.
function r170RowsByPeriod(lookbackMs, limit=9999) {
  const cutoff = Date.now() - lookbackMs;
  return (Array.isArray(tradeLedger) ? tradeLedger : [])
    .filter(t => t && t.status === 'CLOSED' && Number(t.closedAt||0) >= cutoff && Number.isFinite(Number(t.pnlUSDT)) && Number(t.pnlUSDT) !== 0)
    .sort((a,b)=>Number(b.closedAt||0)-Number(a.closedAt||0))
    .slice(0, Math.max(1, Number(limit)||9999));
}
function r170CalcPerf(rows=[]) {
  const closed = rows.length;
  const wins = rows.filter(t => Number(t.pnlUSDT) > 0).length;
  const losses = rows.filter(t => Number(t.pnlUSDT) < 0).length;
  const net = rows.reduce((s,t)=>s+Number(t.pnlUSDT||0),0);
  const grossWin = rows.filter(t=>Number(t.pnlUSDT)>0).reduce((s,t)=>s+Number(t.pnlUSDT),0);
  const grossLoss = Math.abs(rows.filter(t=>Number(t.pnlUSDT)<0).reduce((s,t)=>s+Number(t.pnlUSDT),0));
  const wr = closed ? wins/closed : null;
  const avgWin = wins ? grossWin / wins : 0;
  const avgLoss = losses ? grossLoss / losses : 0;
  const pf = grossLoss > 0 ? grossWin / grossLoss : (grossWin > 0 ? 9.99 : 0);
  return {closed,wins,losses,wr,net,grossWin,grossLoss,avgWin,avgLoss,pf};
}
function r170AccountPerf() {
  const dayRows   = r170RowsByPeriod(24*60*60*1000);
  const weekRows  = r170RowsByPeriod(7*24*60*60*1000);
  const monthRows = r170RowsByPeriod(30*24*60*60*1000);
  const recentRows = dayRows.slice(0,12);
  return {
    recent: r170CalcPerf(recentRows),
    day:    r170CalcPerf(dayRows),
    week:   r170CalcPerf(weekRows),
    month:  r170CalcPerf(monthRows),
  };
}
function r170FmtPerf(p={}) {
  const wr = p.wr !== null && p.wr !== undefined ? (p.wr*100).toFixed(0) : '?';
  return `WR%${wr} Net:${Number(p.net||0).toFixed(2)}$ PF:${Number(p.pf||0).toFixed(2)} N:${Number(p.closed||0)}`;
}
function r170TradeFreq(lookbackMs=60*60*1000) {
  const cutoff = Date.now() - lookbackMs;
  return (Array.isArray(tradeLedger) ? tradeLedger : []).filter(t => Number(t.openedAt||0) >= cutoff).length;
}

function r173AnyText(...xs) {
  return xs.map(x => {
    try { return typeof x === 'string' ? x : JSON.stringify(x || ''); } catch(_) { return ''; }
  }).join(' ').toLowerCase();
}
function r173Num(...xs) {
  for (const x of xs) {
    const n = Number(x);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return 0;
}
function r173ContextQuality(decisionChain={}, analysis={}) {
  const txt = r173AnyText(
    decisionChain?.reason, decisionChain?.brainSummary, decisionChain?.entryPermissionReason,
    decisionChain?.r118CandleOzet, decisionChain?.r125OrderflowSummary, decisionChain?.r126FlowSummary,
    decisionChain?.r140Summary, decisionChain?.r110IctSummary, decisionChain?.r115Summary,
    analysis?.signals, analysis?.reason, analysis?.ict, analysis?.smc, analysis?.r140Summary
  );
  const sweep = !!(
    decisionChain?.directSweepOk || decisionChain?.hardSweepForBridge ||
    decisionChain?.r51DirectSweepMinEdgeOk || decisionChain?.r117TrapSweepTaken ||
    decisionChain?.r117HtfReverseOk || decisionChain?.r110IctKoprusuOk ||
    /sweep|süpür|ssl|bsl|stop.?hunt|likidite.?av|stop av/.test(txt)
  );
  const fvg = !!(
    decisionChain?.fvgOk || decisionChain?.r110FvgOk || decisionChain?.r118FvgOk ||
    decisionChain?.bullishFvg || decisionChain?.bearishFvg ||
    /fvg|fair value gap|imbalance|dengesizlik|boşluk/.test(txt)
  );
  const mss = !!(
    decisionChain?.mssOk || decisionChain?.chochOk || decisionChain?.r110ChochOk ||
    decisionChain?.r115ChochOk || decisionChain?.r117MssOk ||
    /mss|choch|change of character|market structure shift|yapı değiş/.test(txt)
  );
  const candleCloseOk = !!(
    decisionChain?.r118CandleOk || decisionChain?.r118CandleStrong ||
    decisionChain?.r117PrecisionCandleOk || decisionChain?.hassasMumOk ||
    /mum:ok|candle.?ok|confirmed|kapanış|gövde reclaim|body reclaim/.test(txt)
  );
  const retestOk = !!(
    decisionChain?.r75RetestBridgeOk || decisionChain?.r66WyckoffTrapReclaimOk ||
    decisionChain?.r67ScalperCoreHuntEntryOk || decisionChain?.r42TrapReclaimOk ||
    decisionChain?.vwapRetestOk || decisionChain?.emaRetestOk || decisionChain?.pullbackRetestOk ||
    /retest|pullback|geri test|geri dönüş|vwap|ema21|ema9|reclaim/.test(txt)
  );
  const rvol = r173Num(
    analysis?.rvol?.['1h']?.rvol, analysis?.rvol?.rvol, analysis?.r15?.rvol?.rvol,
    analysis?.r140Rvol?.rvol, decisionChain?.r140Rvol?.rvol, decisionChain?.rvol,
    decisionChain?.volumeSpike, analysis?.volumeSpike
  );
  const volSpike15 = rvol >= 1.5 || /rvol:(high|spike)|volume.?spike|hacim.?spike|hacim.?pat/.test(txt);
  const volOk12 = rvol >= 1.2 || volSpike15;
  const silverBullet = !!(sweep && fvg && mss && candleCloseOk && volOk12);
  return {txt, sweep, fvg, mss, candleCloseOk, retestOk, rvol, volSpike15, volOk12, silverBullet};
}
function r170PerfMode() {
  const a = r170AccountPerf();
  const d = a.day || {};
  const r = a.recent || {};
  // Hedef modu hesap geneline göre: günlük performans ana, son 12 işlem erken alarm.
  // Coin WR burada kullanılmaz.
  const enoughDay = Number(d.closed||0) >= 8;
  const enoughRecent = Number(r.closed||0) >= 6;
  const closedDay = Number(d.closed||0);
  const dayBad = enoughDay && (d.wr !== null && (d.wr < 0.60 || d.net < 0 || d.pf < 1.20));
  const recentBad = enoughRecent && (r.wr !== null && (r.wr < 0.55 || r.net < -2 || r.pf < 1.10));
  // R175: Ledger yeni/boşken veya ilk işlem zararken BALANCED_FREQ fazla gevşek kalıyordu.
  // İlk 6 kapalı işlemde hedef WR için warmup da seçici çalışır.
  const warmupBad = closedDay > 0 && closedDay < 6 && (Number(d.net||0) < 0 || (d.wr !== null && d.wr < 0.60));
  const warmupNew = closedDay === 0;
  if (dayBad || recentBad) return {mode:'HIGH_WR_RECOVERY', perf:d, account:a, reason: dayBad ? 'DAILY_TARGET_BELOW' : 'RECENT_DRAWDOWN'};
  if (warmupBad || warmupNew) return {mode:'WARMUP_HIGH_WR', perf:d, account:a, reason:warmupNew?'WARMUP_EMPTY_LEDGER':'WARMUP_NEGATIVE_OR_LOW_WR'};
  if (enoughDay && d.wr !== null && d.wr >= 0.60 && d.wr <= 0.85 && d.pf >= 1.20 && d.net >= 0) return {mode:'TARGET_OK', perf:d, account:a, reason:'DAILY_TARGET_OK'};
  return {mode:'BALANCED_FREQ', perf:d, account:a, reason:'WARMUP_OR_MIXED'};
}

function r167OppositeSideLosses(symbol, side, windowMs=2*60*60*1000) {
  const sym = String(symbol||'').replace('USDT','').toUpperCase();
  const opp = normalizeSide(side) === 'LONG' ? 'SHORT' : 'LONG';
  const since = Date.now() - windowMs;
  return (Array.isArray(tradeLedger)?tradeLedger:[]).filter(t =>
    String(t.symbol||'').replace('USDT','').toUpperCase() === sym &&
    normalizeSide(t.side) === opp &&
    Number(t.closedAt||0) >= since &&
    Number.isFinite(Number(t.pnlUSDT)) && Number(t.pnlUSDT) < 0
  ).length;
}

// R157: Ardışık kayıp tespiti — aynı coin+yönde son 4 saatte 2+ kayıp → 4 saat cooldown
// FOLKS 5 kez işlem, 3 zarar analizi: cooldown bitince tekrar giriyor ve tekrar kaybediyor.
function r157GetConsecutiveLosses(symbol, side, lookbackMs = 4*60*60*1000) {
  const sym = String(symbol||'').replace('USDT','').toUpperCase();
  const dirNorm = normalizeSide(side);
  const cutoff = Date.now() - lookbackMs;
  return (Array.isArray(tradeLedger) ? tradeLedger : []).filter(t =>
    t.status === 'CLOSED' &&
    String(t.symbol||'').toUpperCase() === sym &&
    normalizeSide(t.side) === dirNorm &&
    Number(t.closedAt||0) >= cutoff &&
    Number.isFinite(Number(t.pnlUSDT)) &&
    Number(t.pnlUSDT) < 0
  ).length;
}

function setCooldown(symbol, ms, reason, meta={}) {
  const sym = normalizeSymbol(symbol);
  const exp = Date.now() + ms;
  const info = { exp, reason:toTurkishText(String(reason||'')), rawReason:String(reason||''), mode:meta.mode||'FULL',
                 side:normalizeSide(meta.side), createdAt:Date.now() };
  cooldownMap.set(sym, info);
  const sideTxt = info.side ? ` (${trSideLabel(info.side)})` : '';
  const revTxt = info.mode==='SAME_SIDE_AFTER_CLOSE' ? ' • temiz ters yön serbest' : '';
  logAuto(`⏳ ${sym.replace('USDT','')} bekleme ${Math.ceil(ms/60000)}dk: ${info.reason}${sideTxt}${revTxt}`);
}
function closeCooldownMs(cls={}, state={}) {
  const code = String(cls?.code || '').toUpperCase();
  const pnl  = Number(cls?.realizedPnl);
  // R135: Binance dış/manuel gibi görünen ama realizedPnl negatif olan kapanış aslında risk olayıdır.
  // 15dk manuel beklemesiyle geçiştirme; zarar/hard-loss cooldown uygula.
  if (Number.isFinite(pnl) && pnl < 0) return Math.abs(pnl) >= 3 ? CD_HARD_LOSS_MS : CD_LOSS_MS;
  if (code === 'TAKE_PROFIT') return CD_PROFIT_MS;
  if (code === 'KAR_TASIMA_SL' || code === 'BREAK_EVEN_SL') return CD_BE_MS;
  if (code === 'R14_HARD_LOSS_GUARD') return CD_HARD_LOSS_MS;
  if (code === 'STOP_LOSS') return CD_LOSS_MS;
  if (Number.isFinite(pnl) && pnl > 0) return CD_PROFIT_MS;
  if (code === 'EXTERNAL_OR_MANUAL') return CD_MANUAL_MS;
  return CD_MANUAL_MS;
}
function setCloseCooldown(symbol, cls={}, state={}) {
  const side = normalizeSide(state?.side || cls?.side);
  const ms = closeCooldownMs(cls, state);
  const codeTR = toTurkishText(cls?.code || 'kapanış');
  setCooldown(symbol, ms, `${codeTR} sonrası aynı yön bekleme`,
    { mode:'SAME_SIDE_AFTER_CLOSE', side });
  const code = String(cls?.code || '').toUpperCase();
  const pauseMs = (code === 'EXTERNAL_OR_MANUAL' || code === 'STOP_LOSS' || code === 'R14_HARD_LOSS_GUARD') ? 45*1000 : 20*1000;
  pauseAutoAfterClose(pauseMs, `${symbol.replace('USDT','')} kapanış sonrası bot aynı döngüde acele etmiyor`);
  return ms;
}
function getCooldownRemainMs(symbol, desiredSide=null, decisionChain=null) {
  const info = getCooldownInfo(symbol);
  if (!info) return 0;
  if (desiredSide && canBypassCooldownForReverse(info, desiredSide, decisionChain)) return 0;
  return info.remainMs;
}
function getCooldownList() {
  const list = [];
  const now = Date.now();
  for (const [sym, raw] of cooldownMap.entries()) {
    const info = typeof raw === 'number' ? { exp: raw, reason:'legacy', mode:'FULL', side:null } : raw;
    const rem = Number(info?.exp || 0) - now;
    if (rem > 0) {
      list.push({
        symbol: sym.replace('USDT',''), remainMs: rem, remainMin: Math.ceil(rem/60000),
        reason: toTurkishText(info?.reason || ''), reasonTR: toTurkishText(info?.reason || ''), mode: info?.mode || 'FULL', modeTR: info?.mode==='SAME_SIDE_AFTER_CLOSE'?'Aynı yön beklemede / ters yön serbest':'Tam bekleme', side: info?.side || null, sideTR: trSideLabel(info?.side)
      });
    } else {
      cooldownMap.delete(sym);
    }
  }
  return list.sort((a,b) => b.remainMs - a.remainMs).slice(0, 15);
}


function calcFallbackTP(entryPrice, isLong, tpPct) {
  const pct = Math.max(0.05, parseFloat(tpPct || 10));
  return isLong
    ? +(entryPrice * (1 + pct/100)).toFixed(8)
    : +(entryPrice * (1 - pct/100)).toFixed(8);
}

async function currentBracketTP(apiKey, apiSecret, symbol, isLong, entryPrice, fallbackTpPct, state) {
  if (state && Number.isFinite(parseFloat(state.targetTP)) && parseFloat(state.targetTP) > 0) {
    return parseFloat(state.targetTP);
  }
  const orders = await liveOpenBracketOrders(apiKey, apiSecret, symbol);
  for (const o of orders) {
    if (orderKind(o) !== 'TP') continue;
    const trig = orderTriggerPrice(o);
    if (!trig) continue;
    // LONG TP entry üstünde, SHORT TP entry altında olmalı.
    if (isLong && trig > entryPrice) return trig;
    if (!isLong && trig < entryPrice) return trig;
  }
  return calcFallbackTP(entryPrice, isLong, fallbackTpPct);
}

async function bracketProtectionSnapshot(apiKey, apiSecret, symbol) {
  const orders = await liveOpenBracketOrders(apiKey, apiSecret, symbol, {ttlMs:60_000}).catch(()=>[]);
  const hasSL = orders.some(o => orderKind(o) === 'SL');
  const hasTP = orders.some(o => orderKind(o) === 'TP');
  return { hasSL, hasTP, orderCount:orders.length };
}

async function emergencyCloseIfBracketMissing(apiKey, apiSecret, pos, reason='SLTP_PROOF_FAIL') {
  const sym = pos.symbol;
  const snap = await bracketProtectionSnapshot(apiKey, apiSecret, sym);
  if (snap.hasSL && snap.hasTP) return { closed:false, protected:true, snap };
  const qty = Math.abs(parseFloat(pos.positionAmt || 0));
  if (!qty) return { closed:false, protected:false, snap, error:'qty yok' };
  try {
    const r = await safeMarketClosePosition(apiKey, apiSecret, sym, {reason});
    if (!r.ok) throw new Error(r.error || 'safe close başarısız');
    trailingState.delete(sym);
    setCooldown(sym, COOLDOWN_CLOSE_MS, `${reason} korumasız pozisyon acil kapatıldı`);
    pushCritical('SLTP_UPDATE_FAILSAFE_CLOSE', `${sym}: ${reason}; SL/TP eksikti, güvenli market kapatma denendi`, {...snap, close:r}, 'WARNING');
    return { closed:true, protected:false, snap, order:r.order, alreadyClosed:r.alreadyClosed, fallback:r.fallback };
  } catch(e) {
    pushCritical('SLTP_UPDATE_FAILSAFE_CLOSE_FAIL', `${sym}: ${reason}; acil kapatma başarısız ${e.message}`, snap, 'CRITICAL');
    return { closed:false, protected:false, snap, error:e.message };
  }
}

async function updateStopLossWithProofJS(apiKey, apiSecret, pos, newSL, reason) {
  const sym = pos.symbol;
  const isLong = pos.side === 'LONG';
  const closeSide = isLong ? 'SELL' : 'BUY';
  const cfg = autoConfig || {};
  const state = trailingState.get(sym) || {};
  const mark = parseFloat(pos.markPrice || pos.entryPrice || 0);
  const entry = parseFloat(pos.entryPrice || mark || 0);
  const safeSL = isLong
    ? Math.min(parseFloat(newSL), mark * 0.9997)
    : Math.max(parseFloat(newSL), mark * 1.0003);
  let tpPrice = await currentBracketTP(apiKey, apiSecret, sym, isLong, entry, cfg.tpPct, state);
  // TP mark'a yanlış tarafta kalmışsa emri anında tetikletmemek için panel TP'sinden yeniden hesapla.
  if (isLong && tpPrice <= mark) tpPrice = calcFallbackTP(entry, true, cfg.tpPct);
  if (!isLong && tpPrice >= mark) tpPrice = calcFallbackTP(entry, false, cfg.tpPct);

  const proof = await installSLTPWithProof(apiKey, apiSecret, sym, closeSide, safeSL, tpPrice, sym);
  if (!proof.ok) {
    logAuto(`❌ ${sym} ${reason} SL/TP çifti doğrulanamadı; yerel BE/trailing aktif sayılmadı`);
    const failsafe = await emergencyCloseIfBracketMissing(apiKey, apiSecret, pos, `${reason}_SLTP_PROOF_FAIL`);
    if (failsafe.closed) logAuto(`🛡️ ${sym} korumasız kalmasın diye acil reduce-only kapatıldı`);
    else if (failsafe.protected) logAuto(`🛡️ ${sym} proof eşleşmedi ama Binance'te SL+TP mevcut; pozisyon açık bırakıldı`);
    else logAuto(`🚨 ${sym} SL/TP proof yok ve failsafe kapatma da başarısız: ${failsafe.error||'bilinmeyen'}`);
    return { ok:false, proof, safeSL, tpPrice, failsafe };
  }
  state.currentSL = proof.slPrice || safeSL;
  state.targetTP = proof.tpPrice || tpPrice;
  state.currentSLAlgoId = proof.slOrder?.algoId || proof.slOrder?.clientAlgoId || proof.slOrder?.orderId || null;
  state.tpAlgoId = proof.tpOrder?.algoId || proof.tpOrder?.clientAlgoId || proof.tpOrder?.orderId || null;
  state.sltpVerified = true;
  state.lastSltpUpdate = Date.now();
  trailingState.set(sym, state);
  logAuto(`✅ ${sym} ${reason}: SL ${state.currentSL} + TP ${state.targetTP} Binance doğrulandı (tick:${proof.tickSize || '-'})`);
  return { ok:true, proof, safeSL: state.currentSL, tpPrice: state.targetTP };
}

// CVD + Tick delta flip tespiti — en kritik çıkış sinyali
async function updateBracketWithProofJS(apiKey, apiSecret, pos, newSL, newTP, reason) {
  const sym = pos.symbol;
  const isLong = pos.side === 'LONG';
  const closeSide = isLong ? 'SELL' : 'BUY';
  const mark = parseFloat(pos.markPrice || pos.entryPrice || 0);
  const safeSL = isLong ? Math.min(parseFloat(newSL), mark * 0.9997) : Math.max(parseFloat(newSL), mark * 1.0003);
  const proof = await installSLTPWithProof(apiKey, apiSecret, sym, closeSide, safeSL, newTP, sym);
  if (!proof.ok) {
    logAuto(`❌ ${sym} ${reason} bracket proof başarısız; failsafe kontrol`);
    const failsafe = await emergencyCloseIfBracketMissing(apiKey, apiSecret, pos, `${reason}_BRACKET_PROOF_FAIL`);
    return { ok:false, proof, failsafe, safeSL, newTP };
  }
  const state = trailingState.get(sym) || {};
  state.currentSL = proof.slPrice || safeSL;
  state.targetTP = proof.tpPrice || newTP;
  state.sltpVerified = true;
  state.lastSltpUpdate = Date.now();
  trailingState.set(sym, state);
  logAuto(`✅ ${sym} ${reason}: SL ${state.currentSL} + TP ${state.targetTP} Binance doğrulandı`);
  return { ok:true, proof, safeSL:state.currentSL, tpPrice:state.targetTP };
}

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
      entryPrice, highWater:curPrice, currentSL:null, currentSLAlgoId:null,
      breakEvenSet:false, tpExtended:false, trendHoldCount:0,
      step1Set:false, step2Set:false, step3Set:false,
      peakPnl:0, peakRealPct:0, lastCheck:Date.now()
    });
  }
  const state = trailingState.get(sym);
  state.side = side;
  state.entryPrice = state.entryPrice || entryPrice;
  state.leverage = state.leverage || leverage;
  state.peakPnl    = Math.max(Number(state.peakPnl || 0), Number(pnlPct || 0));
  state.peakRealPct= Math.max(Number(state.peakRealPct || 0), Number(realProfitPct || 0));
  // R94: vur-kaç çıkış motoru yüksek kârı takip eder; küçük kârda ücret/kayma yememek için acele kilitlemez.
  // LONG için en yüksek fiyat, SHORT için en düşük fiyat pozisyon açıldığı andan itibaren izlenir.
  state.highWater = isLong
    ? Math.max(Number(state.highWater || curPrice), curPrice)
    : Math.min(Number(state.highWater || curPrice), curPrice);
  // R37: canlı yönetim timestamp'i her hızlı döngüde güncellensin.
  // Önceki sürümde action yoksa lastCheck/managerStatus eski kalıyor, panel 'kontrol 15dk önce' gösteriyordu.
  const stampManager = (type='İZLEME', reason=null, urgency='LOW') => {
    state.lastCheck = Date.now();
    state.managerStatus = {
      type, urgency,
      reason: reason || `${sym} ${side} canlı izleniyor • PnL %${Number(pnlPct||0).toFixed(2)} • BE:${state.breakEvenSet?'AKTİF':'bekliyor'} • SL/TP:${state.sltpVerified?'doğrulandı':'kontrol'}`,
      pnlPct:Number(pnlPct||0), peakPnl:Number(state.peakPnl||0), peakRealPct:Number(state.peakRealPct||0),
      currentSL:state.currentSL||null, targetTP:state.targetTP||null, lastCheck:state.lastCheck,
      r91Exit: state.r91Exit || null,
      exitMode: state.exitMode || null,
      profitLockLevel: state.profitLockLevel || null
    };
    trailingState.set(sym, state);
  };

  const cfg = autoConfig || {};
  // parseFloat + fallback — NaN koruması
  const safe = (v, def) => { const n=parseFloat(v); return isNaN(n)?def:n; };
  const trailPct      = safe(cfg.trailingPct,  2);
  const trailStep     = safe(cfg.trailStep,    0.25); // R161: 0.3→0.25 — daha sık SL taşıması
  const breakEvenAt   = safe(cfg.breakEvenPct, 0.25); // R161: 0.3→0.25 — daha erken BE
  const karTasima1    = safe(cfg.karTasima1,   0.5);  // R161: 0.6→0.5 — %0.5 kârda kilitle
  const karTasima2    = safe(cfg.karTasima2,   1.0);  // R161: 1.2→1.0 — %1.0 kârda güçlü kilit
  const karTasima3    = safe(cfg.karTasima3,   1.8);  // R161: 2.0→1.8 — %1.8 kârda maksimum kilit
  const minRR         = safe(cfg.minRR,        1.0); // Min R/R oranı
  // R168: pozisyon yönetimi panel SL yerine pozisyon açılırken kullanılan gerçek SL/TP'yi okur.
  // R166/R167 adaptif SL/TP varsa, R14/R42/R41 eski panel SL ile yanlış hasar hesabı yapmamalı.
  const slPct         = safe(state.slPct ?? state.entrySLPct ?? cfg.slPct, 2);
  const tpPct         = safe(state.tpPct ?? state.entryTPPct ?? cfg.tpPct, 10);
  // BE emri sadece entry'ye değil, taker fee + olası stop-market kayması için
  // küçük kâr bölgesine taşınır. INJ örneğinde entry üstü kapanışa rağmen
  // komisyon/slippage nedeniyle eksi yazmıştı. Varsayılan %0.22 coin hareketi.
  const beFeeSafePct = Math.max(0.12, safe(cfg.beLockPct ?? cfg.breakEvenLockPct ?? cfg.beProfitLockPct, 0.22));

  // R39_TREND_HEALTH_TRAIL: trend sağlığı güçlüyse trailing'i hemen sıkıştırma.
  // Ama bu sadece SL sıkıştırmayı en fazla 3 kez erteler; BE, hard-loss, ters CVD/tick ve büyük geri çekilme hâlâ çalışır.
  const isCvdTrendHealthyForSide = () => {
    const cvd = getCVD(sym);
    const ratio = Number(cvd?.ratio ?? 50);
    const mom = String(cvd?.momentum || 'UNKNOWN');
    if (isLong) {
      return mom === 'ACCELERATING_BULL' || mom === 'POSITIVE' || ratio > 55; // R41B: cvdSideOk ile tutarlı
    }
    return mom === 'ACCELERATING_BEAR' || mom === 'NEGATIVE' || ratio < 45; // R41B: tutarlı
  };

  const calcPullbackFromHighWaterPct = (hw) => {
    const base = Number(hw || curPrice);
    if (!base || base <= 0) return 0;
    return isLong
      ? Math.max(0, (base - curPrice) / base * 100)
      : Math.max(0, (curPrice - base) / base * 100);
  };

  // R165: yüksek ROI kâr kilidi aşağıda, action/r91 helper hazırlandıktan sonra çalışır.

  // R94 AKTİF VUR-KAÇ ÇIKIŞ MOTORU — güvenli kâr bölgesinden önce küçük kâr kilidi yok
  // Amaç: Binance SL/TP emniyet kemeri olarak kalırken, kâr görüldüğünde TP'yi beklemeden
  // kâr kilidi / aktif çıkış kararı üretmek. EMA zorunlu kapı değildir; veri-terazi kullanılır.
  const calcR91ExitBrain = (extra={}) => {
    const cvd = getCVD(sym) || {};
    const tick = extra.tickSnap || null;
    const cascadeNow = extra.cascade || null;
    const div = detectDeltaDivergence(sym, side, entryPrice, curPrice) || {};
    const ratio = Number(cvd.ratio ?? 50);
    const mom = String(cvd.momentum || 'UNKNOWN');
    const cvdAgainst = isLong
      ? (ratio < 44 || mom === 'NEGATIVE' || mom === 'ACCELERATING_BEAR')
      : (ratio > 56 || mom === 'POSITIVE' || mom === 'ACCELERATING_BULL');
    const cvdSupport = isLong
      ? (ratio > 54 || mom === 'POSITIVE' || mom === 'ACCELERATING_BULL')
      : (ratio < 46 || mom === 'NEGATIVE' || mom === 'ACCELERATING_BEAR');
    const pullbackPct = calcPullbackFromHighWaterPct(state.highWater);
    const givebackRoi = Math.max(0, Number(state.peakPnl || 0) - Number(pnlPct || 0));
    const givebackReal = Math.max(0, Number(state.peakRealPct || 0) - Number(realProfitPct || 0));
    const adverseTick = !!(extra.tickFlip || extra.exhaustExit || extra.trappedExit);
    const adverseCascade = !!(cascadeNow && cascadeNow.adverse);
    let exitScore = 0;
    const reasons = [];
    if (extra.cvdFlip?.flip) { exitScore += 2; reasons.push('alım/satım baskısı ters döndü'); }
    if (cvdAgainst) { exitScore += 1.5; reasons.push('anlık baskı pozisyona karşı'); }
    if (extra.tickFlip) { exitScore += 2; reasons.push('işlem akışı ters döndü'); }
    if (extra.exhaustExit) { exitScore += 2; reasons.push('hareket yoruldu'); }
    if (extra.trappedExit) { exitScore += 2; reasons.push('tuzak izi var'); }
    if (adverseCascade) { exitScore += 2; reasons.push('ters likidasyon baskısı'); }
    if (div.divergence) { exitScore += 1.5; reasons.push('fiyat gidiyor ama veri desteklemiyor'); }
    if (pullbackPct >= 0.25) { exitScore += 1; reasons.push(`tepeden geri verme %${pullbackPct.toFixed(2)}`); }
    if (pullbackPct >= 0.45) { exitScore += 1; }
    if (givebackRoi >= 5) { exitScore += 1; reasons.push(`kârdan geri verme ROI %${givebackRoi.toFixed(1)}`); }
    if (givebackRoi >= 10) { exitScore += 1.5; }
    if (realProfitPct < 0 && openMinutes >= 3 && cvdAgainst && adverseTick) {
      exitScore += 2; reasons.push('işlem fikri erken bozuldu');
    }
    const devamGucu = cvdSupport && !extra.tickFlip && !extra.exhaustExit && !extra.trappedExit && pullbackPct < 0.22;
    if (devamGucu && exitScore > 0) exitScore = Math.max(0, exitScore - 1.5);
    const mode = pnlPct >= 20 ? 'KÂR-KORU' : pnlPct >= 10 ? 'VUR-KAÇ' : pnlPct >= 4 ? 'KİLİT-HAZIR' : 'İZLE';
    return {
      active: true, mode, exitScore:+exitScore.toFixed(1), reasons: reasons.slice(0,5),
      pnlPct:+Number(pnlPct||0).toFixed(2), realProfitPct:+Number(realProfitPct||0).toFixed(3),
      peakPnl:+Number(state.peakPnl||0).toFixed(2), peakRealPct:+Number(state.peakRealPct||0).toFixed(3),
      pullbackPct:+pullbackPct.toFixed(3), givebackRoi:+givebackRoi.toFixed(2), givebackReal:+givebackReal.toFixed(3),
      cvdRatio:Number.isFinite(ratio)?+ratio.toFixed(1):null, cvdMomentum:mom, devamGucu
    };
  };

  const r91LockPriceFromPct = (lockPct) => {
    const pct = Math.max(0.04, Number(lockPct)||0);
    return isLong
      ? +(entryPrice * (1 + pct/100)).toFixed(8)
      : +(entryPrice * (1 - pct/100)).toFixed(8);
  };

  let action = null; // { type, reason, urgency }

  // R24: 90dk+ açık kalan ve hâlâ BE bölgesine yaklaşmayan pozisyonu yormadan kapat.
  const openedTs = Number(state.openedAt || state.entryAt || 0);
  const openMinutes = openedTs > 0 ? (Date.now() - openedTs) / 60000 : 0;
  if (openMinutes > 90 && realProfitPct < breakEvenAt * 0.5) {
    action = {
      type:'MAX_SURE_KAPAT', urgency:'HIGH',
      reason:`90dk+ açık, kâr yok: hareket %${realProfitPct.toFixed(2)} < BE yarısı %${(breakEvenAt*0.5).toFixed(2)}`
    };
  }

  // ── 0. R14 ACİL ZARAR KORUMASI — SL çalışmaz/gecikirse kaçış ─────────────
  // Normalde Binance algo SL kapatmalı. Ama yeni sembol / aşırı hızlı fitil / proof gecikmesi
  // durumunda pozisyon SL seviyesinin ötesine sarkarsa 3 dakikalık taramayı beklemeden kapatılır.
  const hardLossReal = -Math.max(slPct + 0.25, slPct * 1.12); // SL %2 ise yaklaşık -%2.25 coin hareketi
  const hardLossRoi  = -Math.max((slPct * leverage) + 5, 30); // 15x %2 SL için yaklaşık -%35 ROI
  if (realProfitPct <= hardLossReal || pnlPct <= hardLossRoi) {
    action = {
      type:'EMERGENCY_EXIT', urgency:'CRITICAL',
      reason:`Acil hasar koruması: fiyat hareketi %${realProfitPct.toFixed(2)}, ROI %${pnlPct.toFixed(1)}; SL gecikmesi/slippage riski`
    };
  }

  // R42: Mutlak hasar sigortası. CVD sağlıklı görünse bile yüksek kaldıraçta
  // belirli ROI hasarından sonra Binance SL'yi bekleme; bu AIGENSYN -40%/-46% tipini keser.
  const r42AbsoluteDamageRoiCap = -Math.min(28, Math.max(18, slPct * leverage * 0.65));
  if (!action && openMinutes <= 45 && pnlPct <= r42AbsoluteDamageRoiCap) {
    action = {
      type:'EMERGENCY_EXIT', urgency:'CRITICAL',
      reason:`Mutlak hasar kes: ROI %${pnlPct.toFixed(1)} <= %${r42AbsoluteDamageRoiCap.toFixed(1)}; yüksek kaldıraçta SL sonunu bekleme`
    };
  }

  // R41: 20x/25 USDT gibi ayarlarda %2 coin SL = yaklaşık -%40 ROI demek.
  // AIGENSYN benzeri ters akışta tüm SL'yi beklemek yerine erken hasar kes.
  // Trend sağlığı hâlâ pozisyon yönündeyse dokunmaz; veri nötr/zayıfsa küçük SL'den önce kaçar.
  const r41EarlyDamageRoiCap = -Math.min(22, Math.max(14, slPct * leverage * 0.45));
  if (!action && openMinutes <= 30 && pnlPct <= r41EarlyDamageRoiCap && !isCvdTrendHealthyForSide()) {
    action = {
      type:'EMERGENCY_EXIT', urgency:'HIGH',
      reason:`Erken hasar kes: ROI %${pnlPct.toFixed(1)} <= %${r41EarlyDamageRoiCap.toFixed(1)} ve CVD/tick trend sağlığı yok`
    };
  }

  // R168: Momentum-pass erken tez bozulması.
  // 5m scalp'te R159 giriş 6-9 puanla açıldıysa ilk 12 dakikada BE görmeden -8/-10 ROI'a düşmesi
  // çoğu zaman "analiz ters yönde" demektir. CVD hâlâ sağlıklı görünse bile tam SL'yi bekleme.
  const r168WeakEntry = String(state.openReason||'').includes('R159 momentum') || String(state.brainMode||'').includes('MOMENTUM');
  const r168EarlyThesisCap = r168WeakEntry ? -8.5 : -10.5;
  if (!action && openMinutes <= 12 && !state.breakEvenSet && Number(state.peakPnl||0) < 4 && pnlPct <= r168EarlyThesisCap) {
    action = {
      type:'EMERGENCY_EXIT', urgency:'HIGH',
      reason:`R168 erken tez bozuldu: ${r168WeakEntry?'R159/MOMENTUM ':' '}giriş BE görmeden ROI %${pnlPct.toFixed(1)} <= %${r168EarlyThesisCap}; tam SL beklenmedi`
    };
  }

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
  // R164: CVD flip eşiği dinamik — yüksek ROI'da acil çıkış yerine SL koru
  // %80 ROI'da CVD flip → kapatma değil, SL sıkıştır
  // Düşük ROI'da (<5%) hızlı çıkış, yüksek ROI'da (>20%) sadece SL taşı
  const cvdFlipMinProfit = pnlPct >= 40 ? 2.0 : pnlPct >= 20 ? 1.0 : pnlPct >= 10 ? 0.5 : 0.2;
  if ((cvdFlip.flip || tickFlip || exhaustExit || trappedExit) && realProfitPct > cvdFlipMinProfit) {
    if (tickFlip)    cvdFlip.reason = `Tick delta flip: ${tickSnap?.deltaFlip}`;
    if (exhaustExit) cvdFlip.reason = `Exhaustion: momentum bitti`;
    if (trappedExit) cvdFlip.reason = `Trapped trader: hızlı ters dönüş bekleniyor`;
    // R164: Yüksek ROI'da (>%30 kaldıraçlı) flip = acil çıkış değil, SL koru
    if (pnlPct >= 30) {
      // SL kilitleme işareti — aşağıdaki kar koruma bloğu devreye girecek
      state.cvdFlipHighProfit = true;
      trailingState.set(sym, state);
    } else {
      action = { type:'EMERGENCY_EXIT', ...cvdFlip };
    }
  }

  // ── 2. ACİL ÇIKIŞ — Ters Cascade ────────────────────────────────────────
  let cascade = null;
  if (!action) {
    cascade = detectAdverseCascade(sym, side);
    if (cascade.adverse && realProfitPct > 0) { // Kârdayken cascade gelirse koru
      action = { type:'EMERGENCY_EXIT', ...cascade };
    }
  }

  // R24: küçük kârda tek zayıf flip yüzünden çıkma. En az iki ters teyit yoksa BE/trailing yönetsin.
  if (action?.type === 'EMERGENCY_EXIT' && realProfitPct > 0 && realProfitPct < 3.0) {
    const reversalHits = [
      !!cvdFlip.flip, !!tickFlip, !!exhaustExit, !!trappedExit, !!(cascade && cascade.adverse)
    ].filter(Boolean).length;
    if (reversalHits < 2) {
      logAuto(`⏳ ${sym} erken çıkış engellendi: kaldıraçsız kâr %${realProfitPct.toFixed(2)}, ters teyit ${reversalHits}/2`);
      action = null;
    }
  }

  // ── R94 AKTİF VUR-KAÇ ÇIKIŞ — TP beklemeden ama net kâr güvenli bölgeye gelince çıkış ────────
  const r91VurKacAktif = cfg.vurKacEnabled !== false; // panelde kapatılırsa sadece klasik SL/TP+BE kalır
  const r91Brain = calcR91ExitBrain({ cvdFlip, tickSnap, tickFlip, exhaustExit, trappedExit, cascade });
  state.r91Exit = r91Brain;
  state.exitMode = r91Brain.mode;

  // ── R165 ROI KÂR KİLİDİ — R164'teki sessiz hata düzeltildi ─────────────
  // R164 bloğu action ve r91LockPriceFromPct tanımlanmadan çalışıyordu; ayrıca
  // PROFIT_LOCK_UPDATE aksiyonu uygulama listesinde yoktu. Sonuç: %40-%80 ROI
  // görülse bile SL gerçekten Binance'e taşınmayabiliyordu. Bu blok aksiyon
  // nesnesi hazırlandıktan sonra çalışır ve R165_KAR_KILIDI olarak gerçek SL yazar.
  const r165PeakRoi = Number(state.peakPnl || 0);
  const r165GivebackRoi = Math.max(0, r165PeakRoi - Number(pnlPct || 0));
  const r165LockTable = [
    { min: 90, lockReal: 3.20, level: 7 },
    { min: 70, lockReal: 2.40, level: 6 },
    { min: 50, lockReal: 1.70, level: 5 },
    { min: 35, lockReal: 1.10, level: 4 },
    { min: 25, lockReal: 0.75, level: 3 },
    { min: 16, lockReal: 0.45, level: 2 },
    { min: 9,  lockReal: 0.24, level: 1 },
  ];
  if (!action && inProfit && r165PeakRoi >= 9) {
    const lock = r165LockTable.find(x => r165PeakRoi >= x.min);
    if (lock) {
      // Geri verme arttıysa kilidi biraz daha yukarı al; ama mevcut fiyatı geçip
      // stop emrini geçersiz yapmasın diye updateStopLossWithProofJS ayrıca mark'a göre clamp eder.
      const givebackBoost = r165GivebackRoi >= 18 ? 0.35 : r165GivebackRoi >= 10 ? 0.20 : 0;
      const lockRealPct = lock.lockReal + givebackBoost;
      const lockSL = r91LockPriceFromPct(lockRealPct);
      const currentSLNum = Number(state.currentSL || 0);
      const betterLock = isLong ? (!currentSLNum || lockSL > currentSLNum) : (!currentSLNum || lockSL < currentSLNum);
      if (betterLock) {
        action = {
          type:'R165_KAR_KILIDI', urgency: r165PeakRoi >= 35 ? 'MEDIUM' : 'LOW', newSL:lockSL,
          reason:`R165 kâr kilidi: zirve ROI %${r165PeakRoi.toFixed(1)}, mevcut %${Number(pnlPct||0).toFixed(1)}, geri verme %${r165GivebackRoi.toFixed(1)} → SL entry +%${lockRealPct.toFixed(2)} gerçek kâr bölgesine`,
          stateUpdates:{ breakEvenSet:true, r165ProfitLock:true, profitLockLevel:Math.max(Number(state.profitLockLevel||0), lock.level) }
        };
      }
    }
  }

  // +ROI görmüş işlem tekrar zarara gömülmesin. Büyük zirveden sonra runner gücü yoksa
  // market reduce-only kapatır; bu WR garantisi değildir, fakat +80 ROI → eksi kapanış
  // tipindeki ana sapmayı kesmek için tasarlandı.
  const r165WinnerNeverLoserExit = !!(!action && r165PeakRoi >= 20 && !r91Brain.devamGucu && (
    (r165PeakRoi >= 70 && pnlPct <= Math.max(12, r165PeakRoi * 0.30)) ||
    (r165PeakRoi >= 40 && pnlPct <= Math.max(8,  r165PeakRoi * 0.25)) ||
    (r165PeakRoi >= 20 && pnlPct <= Math.max(4,  r165PeakRoi * 0.18))
  ));
  if (r165WinnerNeverLoserExit) {
    action = {
      type:'R165_WINNER_NEVER_LOSER_KAPAT', urgency:'HIGH',
      reason:`R165 winner-never-loser: zirve ROI %${r165PeakRoi.toFixed(1)} → mevcut %${Number(pnlPct||0).toFixed(1)}; devam gücü yok, kâr zarara dönmeden market çıkış`
    };
  }

  // ── R149 ROI KÂR KASASI — işlem sayısını azaltmadan kârdaki pozisyonu daha iyi koru ──
  // Giriş kapısı değildir; sadece açık pozisyon yönetir. Amaç büyük bakiyede küçük ROI kârlarını
  // toplamaya uygun şekilde, +ROI görmüş işlemin tekrar zarara dönmesini azaltmaktır.
  const r149PeakRoi = Number(state.peakPnl || 0);
  const r149PeakReal = Number(state.peakRealPct || 0);
  const r149GivebackRoi = Math.max(0, r149PeakRoi - Number(pnlPct || 0));
  const r149EntryTxt = [state.openReason, state.brainMode, state.entryPermissionReason, state.entryReason?.reason].filter(Boolean).join(' ').toLowerCase();
  const r149ScalpLike = /mikro|scalp|vur-kaç|vurkac|flow|momentum|tuzak dönüşü|counter_trap|hızlı edge|fast edge/.test(r149EntryTxt);
  const r149ShouldLock = !!(!action && r149PeakRoi >= 8 && pnlPct > 2 && realProfitPct > 0.10 && (r149GivebackRoi >= 4 || Number(r91Brain.exitScore||0) >= 2 || r149PeakRoi >= 16));
  if (r149ShouldLock) {
    let keepRealPct = r149PeakReal * (r149ScalpLike ? 0.58 : 0.48);
    if (r149PeakRoi >= 20) keepRealPct = Math.max(keepRealPct, r149PeakReal * 0.64);
    if (r149PeakRoi >= 35) keepRealPct = Math.max(keepRealPct, r149PeakReal * 0.72);
    // Aşırı sıkıştırma yok: current price üstüne/altına SL basıp pozisyonu anında boğma.
    keepRealPct = Math.max(0.18, Math.min(Math.max(0.20, r149PeakReal - 0.10), keepRealPct));
    const rawVaultSL = r91LockPriceFromPct(keepRealPct);
    const vaultSL = isLong ? Math.min(rawVaultSL, +(curPrice * 0.998).toFixed(8)) : Math.max(rawVaultSL, +(curPrice * 1.002).toFixed(8));
    const betterVault = isLong ? (!state.currentSL || vaultSL > state.currentSL) : (!state.currentSL || vaultSL < state.currentSL);
    if (betterVault) {
      action = {
        type:'R149_ROI_VAULT_LOCK', urgency:'LOW', newSL:vaultSL,
        reason:`R149 ROI kâr kasası: zirve ROI %${r149PeakRoi.toFixed(1)}, mevcut %${pnlPct.toFixed(1)}, geri verme %${r149GivebackRoi.toFixed(1)} → SL kâr kasasına alındı`,
        stateUpdates:{ r149VaultLock:true, profitLockLevel:Math.max(Number(state.profitLockLevel||0), 4) }
      };
    }
  }

  // +ROI görmüş işlem tekrar sıfıra/zarara gömülmesin. Devam gücü varsa dokunmaz;
  // sadece kârı ciddi geri verip akış zayıfladığında market reduce-only kapatır.
  const r149WinnerGivebackExit = !!(!action && r149PeakRoi >= 12 && r149GivebackRoi >= Math.max(8, r149PeakRoi * 0.55) && pnlPct <= Math.max(1.5, r149PeakRoi * 0.18) && !r91Brain.devamGucu);
  if (r149WinnerGivebackExit) {
    action = {
      type:'R149_PROFIT_GIVEBACK_KAPAT', urgency:'HIGH',
      reason:`R149 kâr geri verme koruması: zirve ROI %${r149PeakRoi.toFixed(1)} → mevcut %${pnlPct.toFixed(1)}; runner gücü yok, kâr sıfıra dönmeden çıkış`
    };
  }

  if (!action && r91VurKacAktif) {
    // 1) İlk kâr görünür görünmez BE'den önce küçük kâr kilidi.
    // R136: 5m kaldıraçlı TOP10'da çok erken SL sıkıştırmak, EPIC gibi işlemi
    // +0.06$ küçük kâra boğabiliyor. Akış hâlâ pozisyon yönündeyse ilk kilidi
    // birkaç döngü nefeslendir; ters teyit gelirse eski emniyet yine çalışır.
    const r136RunnerBreathOk = !!(r91Brain.devamGucu && Number(r91Brain.exitScore||0) < 2.5 && realProfitPct < 1.45);
    if (!action && pnlPct >= 9 && realProfitPct >= 0.45 && !state.r91FirstLock && r136RunnerBreathOk && Number(state.r136FirstLockBreath||0) < 4) {
      state.r136FirstLockBreath = Number(state.r136FirstLockBreath||0) + 1;
      trailingState.set(sym, state);
      logAuto(`⏳ ${sym} R136 kâr nefesi: erken ilk kâr kilidi bekletildi [${state.r136FirstLockBreath}/4] · kâr %${realProfitPct.toFixed(2)} · çıkış puanı ${r91Brain.exitScore}/10`);
    } else if (pnlPct >= 9 && realProfitPct >= 0.45 && !state.r91FirstLock) {
      const lockPct = Math.max(0.22, Math.min(0.55, realProfitPct * 0.45));
      const lockSL = r91LockPriceFromPct(lockPct);
      const better = isLong ? (!state.currentSL || lockSL > state.currentSL) : (!state.currentSL || lockSL < state.currentSL);
      if (better) action = {
        type:'R97_KAR_KILIDI', urgency:'LOW', newSL:lockSL,
        reason:`5m Fırsat Beyni güvenli kâr kilidi: ROI %${pnlPct.toFixed(1)} → komisyon/kayma payı geçildi, SL kâr bölgesine alındı`,
        stateUpdates:{ r91FirstLock:true, profitLockLevel:1, breakEvenSet:true }
      };
    }

    // 2) Kâr büyüdüyse SL kâr bölgesine daha agresif taşınır.
    // R136: ikinci kilit de trend/flow sağlıklıysa %1.15 altında acele sıkışmasın.
    const r136SecondBreathOk = !!(r91Brain.devamGucu && Number(r91Brain.exitScore||0) < 3.0 && realProfitPct < 1.15);
    if (!action && pnlPct >= 14 && realProfitPct >= 0.70 && !state.r91SecondLock && r136SecondBreathOk && Number(state.r136SecondLockBreath||0) < 3) {
      state.r136SecondLockBreath = Number(state.r136SecondLockBreath||0) + 1;
      trailingState.set(sym, state);
      logAuto(`⏳ ${sym} R136 ikinci kâr kilidi nefesi [${state.r136SecondLockBreath}/3] · kâr %${realProfitPct.toFixed(2)} · çıkış puanı ${r91Brain.exitScore}/10`);
    } else if (!action && pnlPct >= 14 && realProfitPct >= 0.70 && !state.r91SecondLock) {
      const lockPct = Math.max(0.35, Math.min(0.85, realProfitPct * 0.55));
      const lockSL = r91LockPriceFromPct(lockPct);
      const better = isLong ? (!state.currentSL || lockSL > state.currentSL) : (!state.currentSL || lockSL < state.currentSL);
      if (better) action = {
        type:'R97_KAR_KILIDI', urgency:'LOW', newSL:lockSL,
        reason:`5m Fırsat Beyni ikinci kâr kilidi: ROI %${pnlPct.toFixed(1)}, net kâr korundu`,
        stateUpdates:{ r91SecondLock:true, profitLockLevel:2, breakEvenSet:true }
      };
    }

    // 3) Gerçek vur-kaç: kâr var + hareket bozulduysa TP beklenmez, reduce-only market çıkar.
    const r91ExitNow =
      (pnlPct >= 25 && r91Brain.exitScore >= 2.5) ||
      (pnlPct >= 18 && r91Brain.exitScore >= 3.0) ||
      (pnlPct >= 12 && r91Brain.exitScore >= 4.0) ||
      (pnlPct >= 10 && r91Brain.givebackRoi >= 7 && r91Brain.exitScore >= 3.0) ||
      (pnlPct >= 8  && realProfitPct >= 0.55 && r91Brain.givebackRoi >= 9 && r91Brain.exitScore >= 4.0);
    if (!action && r91ExitNow) {
      action = {
        type:'R97_VUR_KAC_KAPAT', urgency:'HIGH',
        reason:`5m Fırsat Beyni çıkışı: ROI %${pnlPct.toFixed(1)}, zirve %${r91Brain.peakPnl.toFixed(1)}, çıkış puanı ${r91Brain.exitScore}/10 — ${r91Brain.reasons.join(' + ') || 'kâr geri verilmeden alındı'}`
      };
    }

    // 4) İşlem fikri erken bozulursa SL sonunu bekleme; ama tek zayıf veriyle de kapatma.
    if (!action && openMinutes >= 3 && pnlPct <= -6 && r91Brain.exitScore >= 6 && !r91Brain.devamGucu) {
      action = {
        type:'R97_FIKIR_BOZULDU_KAPAT', urgency:'HIGH',
        reason:`5m Fırsat Beyni fikir bozuldu: ROI %${pnlPct.toFixed(1)}, çıkış puanı ${r91Brain.exitScore}/10 — ${r91Brain.reasons.join(' + ')}`
      };
    }

    // R144: HMSTR tipi hızlı-edge girişlerde fiyat hemen eksiye döner ve canlı akış tersleşirse
    // tam SL'yi bekleme. 20x/10x 5m scalp'te ROI -6/-8 erken uyarıdır; amaç kaybı büyümeden kesmek,
    // kârlı runner'ı boğmak değildir. Devam gücü varsa çalışmaz.
    const r144DamageControlNow = !!(
      !action && openMinutes >= 0.8 && pnlPct <= -4.5 && !r91Brain.devamGucu &&
      (Number(r91Brain.exitScore||0) >= 4 || tickFlip || cvdAgainst || cvdFlip?.flip || trappedExit || exhaustExit)
    );
    if (r144DamageControlNow) {
      action = {
        type:'R144_HASAR_KONTROL_KAPAT', urgency:'HIGH',
        reason:`R144 hasar kontrolü: ROI %${pnlPct.toFixed(1)} erken eksiye döndü, çıkış puanı ${r91Brain.exitScore}/10 — ${r91Brain.reasons.join(' + ') || 'canlı akış tersleşti'}`
      };
    }

    // R146: hızlı-edge mikro-scalp zarar büyütme freni.
    // Bu oyun runner değil, kısa tepki avıdır. İlk dakikalarda hiç kâr üretmeden ROI -7/-8 bölgesine inerse
    // exitScore düşük kalsa bile SL'nin tamamını beklemek küçük bakiyede gereksiz büyük ROI hasarı üretir.
    const r146EntryTxt = [state.openReason, state.brainMode, state.entryPermissionReason, state.entryReason?.reason].filter(Boolean).join(' ').toLowerCase();
    const r146FastEntry = /hızlı edge|fast edge|mikro-scalp|r135_fast|r144 hızlı/.test(r146EntryTxt);
    const r146NoProfitFastFail = r146FastEntry && openMinutes >= 1.0 && openMinutes <= 12 && Number(state.peakPnl||0) < 6 && pnlPct <= -7.0;
    if (!action && r146NoProfitFastFail) {
      action = {
        type:'R144_HASAR_KONTROL_KAPAT', urgency:'HIGH',
        reason:`R146 hızlı scalp başarısız: ROI %${pnlPct.toFixed(1)}, ilk ${openMinutes.toFixed(1)}dk içinde kâr üretemedi; SL sonu beklenmiyor`
      };
    }

    // R147: ROBO tipi yanlış yön tuzak dönüşü açıldıysa hasarı bekletme.
    // Bu, kârlı runner'ı boğmaz; sadece 5m formasyon yok/live-opposite/mum tahmini ters gibi
    // giriş anında zaten kötü kokan pozisyon ilk dakikalarda ROI -8/-10'a düşerse çalışır.
    const r147BadEntryTxt = /tuzak dönüşü|counter_trap|live-opposite|formasyon yok|mumtahmin:short|mumtahmin:düşüş|mumtahmin:long|mumtahmin:yükseliş|zemin:bozuk|r147/.test(r146EntryTxt);
    const r147DirectionForecastAgainst = (isLong && /mumtahmin:short|mumtahmin:düşüş/.test(r146EntryTxt)) || (!isLong && /mumtahmin:long|mumtahmin:yükseliş/.test(r146EntryTxt));
    const r147NoProfitTrapFail = r147BadEntryTxt && (r147DirectionForecastAgainst || /live-opposite|formasyon yok|zemin:bozuk/.test(r146EntryTxt)) &&
      openMinutes >= 0.8 && openMinutes <= 10 && Number(state.peakPnl||0) < 5 && pnlPct <= -7.5;
    if (!action && r147NoProfitTrapFail) {
      action = {
        type:'R147_TERS_AKIS_HASAR_KAPAT', urgency:'HIGH',
        reason:`R147 ters-akış hasar kontrolü: ROI %${pnlPct.toFixed(1)}, ilk ${openMinutes.toFixed(1)}dk içinde kâr üretemedi; girişteki canlı/mum akışı ters olduğu için SL sonu beklenmiyor`
      };
    }

    // 5) 5m vur-kaçta hareket yoksa ve veri tersleşmişse pozisyonu yorma.
    if (!action && openMinutes >= 12 && pnlPct > -5 && pnlPct < 4 && r91Brain.exitScore >= 5 && !r91Brain.devamGucu) {
      action = {
        type:'R97_VUR_KAC_KAPAT', urgency:'MEDIUM',
        reason:`5m Fırsat Beyni süre yönetimi: ${openMinutes.toFixed(0)}dk geçti, hareket zayıf, veri tersleşti — pozisyon yormadan kapatılıyor`
      };
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
      reason:`Fiyat %${realProfitPct.toFixed(2)} hareket etti (kaldıraçsız) → SL giriş + fee buffer %${beFeeSafePct.toFixed(2)}`,
      urgency:'LOW',
      newSL: +(entryPrice * (isLong ? (1 + beFeeSafePct/100) : (1 - beFeeSafePct/100))).toFixed(8),
      stateUpdates:{ breakEvenSet:true, beFeeSafePct }
    };
    logAuto(`${sym} Break-even: Gerçek hareket %${realProfitPct.toFixed(2)}, Kaldıraçlı PnL: %${pnlPct.toFixed(1)}, Lev:${leverage}x`);
  }

  // ── 4b. KÂR TAŞIMA ADIMLARI ─────────────────────────────────────────────────
  if (!action && state.breakEvenSet) {
    let stepSL = null, stepReason = null, stepUpdate = null;
    if (realProfitPct >= karTasima3 && !state.step3Set) {
      stepSL = isLong ? +(entryPrice*(1+0.015)).toFixed(8) : +(entryPrice*(1-0.015)).toFixed(8);
      stepReason = `Kâr taşıma 3: %${realProfitPct.toFixed(2)} → SL kâr +%1.5`;
      stepUpdate = { step3Set:true };
    } else if (realProfitPct >= karTasima2 && !state.step2Set) {
      stepSL = isLong ? +(entryPrice*(1+0.008)).toFixed(8) : +(entryPrice*(1-0.008)).toFixed(8);
      stepReason = `Kâr taşıma 2: %${realProfitPct.toFixed(2)} → SL kâr +%0.8`;
      stepUpdate = { step2Set:true };
    } else if (realProfitPct >= karTasima1 && !state.step1Set) {
      // R136: Kâr taşıma 1, erken çıkış gibi davranmasın.
      // Akış hâlâ pozisyon yönündeyse ve ters çıkış puanı düşükse ilk taşıma için
      // %1.60'a kadar veya en fazla 6 yönetim döngüsü nefes ver. BE emniyeti zaten aktif.
      const r136KarBreathOk = !!(r91Brain?.devamGucu && Number(r91Brain?.exitScore||0) < 2.5 && realProfitPct < 1.60 && Number(state.r136KarTasimaBreath||0) < 6);
      if (r136KarBreathOk) {
        state.r136KarTasimaBreath = Number(state.r136KarTasimaBreath||0) + 1;
        trailingState.set(sym, state);
        logAuto(`⏳ ${sym} R136 kâr taşıma nefesi: step1 bekletildi [${state.r136KarTasimaBreath}/6] · kâr %${realProfitPct.toFixed(2)} · trend/flow devam`);
      } else {
        stepSL = isLong ? +(entryPrice*(1+0.003)).toFixed(8) : +(entryPrice*(1-0.003)).toFixed(8);
        stepReason = `Kâr taşıma 1: %${realProfitPct.toFixed(2)} → SL giriş üstü +%0.3`;
        stepUpdate = { step1Set:true };
      }
    }
    if (stepSL) {
      const better = isLong ? (!state.currentSL||stepSL>state.currentSL) : (!state.currentSL||stepSL<state.currentSL);
      if (better) action = { type:'KAR_TASIMA', reason:stepReason, urgency:'LOW', newSL:stepSL, stateUpdates:stepUpdate };
    }
  }

  // ── 5. TRAILING SL (adım bazlı) + R39 trend sağlık bekletmesi ───────────
  if (!action && state.breakEvenSet) {
    const oldHW = state.highWater || curPrice;
    const newHW = isLong
      ? Math.max(oldHW, curPrice)
      : Math.min(oldHW, curPrice);
    // Min adım: fiyat trailStep% kadar ilerlemiş olmalı (sürekli güncellemeyi önler)
    const movedPct = oldHW
      ? Math.abs(newHW-oldHW)/oldHW*100 : 0;
    if (movedPct >= trailStep) {
      const trendHealthy = isCvdTrendHealthyForSide();
      const pullbackPct = calcPullbackFromHighWaterPct(newHW);
      const smallPullback = pullbackPct < (trailPct * 0.4);
      state.trendHoldCount = Number(state.trendHoldCount || 0);

      // Trend hâlâ sağlıklıysa ve geri çekilme küçükse SL'yi hemen sıkıştırma.
      // Bu, BILL gibi devam edebilecek top mover trendlerinde BE/trailing avına düşmeyi azaltır.
      if (trendHealthy && smallPullback && state.trendHoldCount < 3) {
        state.highWater = newHW;
        state.trendHoldCount += 1;
        stampManager('TREND_HOLD', `Trend devam görünüyor — trail bekleniyor [${state.trendHoldCount}/3] • HW=$${Number(newHW).toFixed(6)} • pullback %${pullbackPct.toFixed(2)}`, 'LOW');
        logAuto(`⏳ ${sym} Trend devam görünüyor — trail bekleniyor [${state.trendHoldCount}/3]`);
        return null;
      }

      const newSL = isLong
        ? +(newHW*(1-trailPct/100)).toFixed(8)
        : +(newHW*(1+trailPct/100)).toFixed(8);
      const better = isLong
        ? (!state.currentSL||newSL>state.currentSL)
        : (!state.currentSL||newSL<state.currentSL);
      if (better) action = {
        type:'TRAIL_SL',
        reason:`Trailing (%${trailStep} adım): HW=$${Number(newHW).toFixed(6)} → SL=$${newSL}`,
        urgency:'LOW', newSL,
        stateUpdates:{ highWater:newHW, trendHoldCount:0 }
      };
    }
  }

  // ── 6. TP GENİŞLETME — Momentum devam ediyorsa TP'yi yukarı taşı ────────
  // R39: TP genişletme artık hedefin %70'ini değil %50'sini geçince değerlendirir.
  const tpRealTarget = tpPct; // Kaldıraçsız hedef %
  if (!action && !state.tpExtended && realProfitPct > tpRealTarget * 0.5) {
    const cvd = getCVD(sym);
    const momentumStrong = isLong
      ? cvd?.momentum === 'ACCELERATING_BULL' || cvd?.ratio > 65
      : cvd?.momentum === 'ACCELERATING_BEAR' || cvd?.ratio < 35;
    if (momentumStrong) {
      const extendedTP = isLong
        ? +(entryPrice * (1 + (tpPct * 1.5) / 100)).toFixed(8)
        : +(entryPrice * (1 - (tpPct * 1.5) / 100)).toFixed(8);
      const fallbackSL = state.currentSL || (isLong
        ? +(entryPrice * (1 - slPct / 100)).toFixed(8)
        : +(entryPrice * (1 + slPct / 100)).toFixed(8));
      const betterTP = isLong ? extendedTP > Number(state.targetTP || 0) : (!state.targetTP || extendedTP < Number(state.targetTP));
      if (betterTP) {
        action = { type:'EXTEND_TP', urgency:'LOW', newSL:fallbackSL, newTP:extendedTP,
          reason:`Momentum güçlü — TP gerçek Binance emrine genişletiliyor: %${(tpPct*1.5).toFixed(1)}` ,
          stateUpdates:{ tpExtended:true } };
      }
    }
  }

  // ── AKSİYON UYGULA ────────────────────────────────────────────────────────
  if (!action) { stampManager('İZLEME', null, 'LOW'); return null; }

  stampManager(action.type, action.reason, action.urgency||'LOW');
  logAuto(`[${sym}] ${action.type} (${action.urgency}): ${action.reason}`);

  if (action.type === 'EMERGENCY_EXIT' || action.type === 'MAX_SURE_KAPAT' || action.type === 'R97_VUR_KAC_KAPAT' || action.type === 'R97_FIKIR_BOZULDU_KAPAT' || action.type === 'R144_HASAR_KONTROL_KAPAT' || action.type === 'R149_PROFIT_GIVEBACK_KAPAT' || action.type === 'R165_WINNER_NEVER_LOSER_KAPAT') {
    // Hem normal hem algo emirleri iptal et (2025-12-09 sonrası)
    try {
      const r = await safeMarketClosePosition(apiKey, apiSecret, sym, {reason:action.type});
      if (!r.ok) throw new Error(r.error || 'safe close başarısız');
      logAuto(`✅ ${sym} ${action.type==='R97_VUR_KAC_KAPAT'?'TEK BEYİN ÇIKIŞI':action.type==='R97_FIKIR_BOZULDU_KAPAT'?'TEK BEYİN FİKİR BOZULDU':action.type==='R149_PROFIT_GIVEBACK_KAPAT'?'R149 KÂR KORUMA ÇIKIŞI':action.type==='R165_WINNER_NEVER_LOSER_KAPAT'?'R165 KÂR ZARARA DÖNMESİN ÇIKIŞI':'ACİL ÇIKIŞ'}: PnL %${pnlPct.toFixed(2)} — ${r.order?.orderId || (r.alreadyClosed?'zaten kapalı':'safe-close')}`);
      trailingState.delete(sym);
      return { action:'CLOSED', pnl:pnlPct, reason:action.reason };
    } catch(e) {
      logAuto(`❌ ${sym} Acil çıkış hatası: ${e.message}`);
    }
  }

  if (action.type === 'BREAK_EVEN' || action.type === 'TRAIL_SL' || action.type === 'TIGHTEN_SL'
      || action.type === 'KAR_TASIMA' || action.type === 'R97_KAR_KILIDI' || action.type === 'R149_ROI_VAULT_LOCK' || action.type === 'R165_KAR_KILIDI') {
    const newSL = action.newSL;
    if (!newSL) return null;
    const upd = await updateStopLossWithProofJS(apiKey, apiSecret, pos, newSL, action.type);
    if (upd.ok) {
      Object.assign(state, action.stateUpdates || {});
      state.currentSL = upd.safeSL;
      state.targetTP = upd.tpPrice;
      state.sltpVerified = true;
      trailingState.set(sym, state);
      logAuto(`✅ ${sym} ${action.type} uygulandı: SL ${upd.safeSL} / TP ${upd.tpPrice}`);
    } else {
      // Çalışan python çekirdeği gibi: doğrulanmadıysa yerel state'i başarılı sayma.
      state.sltpVerified = false;
      trailingState.set(sym, state);
      return null;
    }
  }

  if (action.type === 'EXTEND_TP') {
    const upd = await updateBracketWithProofJS(apiKey, apiSecret, pos, action.newSL, action.newTP, action.type);
    if (upd.ok) {
      Object.assign(state, action.stateUpdates || {});
      state.currentSL = upd.safeSL;
      state.targetTP = upd.tpPrice;
      state.sltpVerified = true;
      trailingState.set(sym, state);
      logAuto(`📈 ${sym} TP genişletme gerçek Binance emrine yazıldı: TP ${upd.tpPrice}`);
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

function mapRiskRowsToManagerPositions(rows=[]) {
  return (Array.isArray(rows) ? rows : [])
    .filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0)
    .map(p => {
      const amt = parseFloat(p.positionAmt || 0);
      const ep  = parseFloat(p.entryPrice || 0);
      const mp  = parseFloat(p.markPrice || 0);
      const lev = parseInt(p.leverage) || 1;
      const side = amt > 0 ? 'LONG' : 'SHORT';
      const pnlPct = ep > 0 ? ((mp - ep) / ep * 100 * lev * (side === 'SHORT' ? -1 : 1)) : 0;
      return {
        symbol:p.symbol, side, positionAmt:Math.abs(amt), entryPrice:ep, markPrice:mp,
        unrealizedProfit:parseFloat(p.unRealizedProfit ?? p.unrealizedProfit ?? 0),
        leverage:lev, pnlPct
      };
    });
}

let fastManagerRunning = false;
let fastManagerTimer = null;
async function fastManageOpenPositions() {
  if (fastManagerRunning || !autoConfig?.enabled || !autoConfig?.apiKey || !autoConfig?.apiSecret) return;
  if (isPositionRiskCooldownActive()) return;
  fastManagerRunning = true;
  try {
    const rows = await getPositionRiskCached(autoConfig.apiKey, autoConfig.apiSecret);
    const mapped = mapRiskRowsToManagerPositions(rows);
    autoScanState.livePositions = mapped.length;
    if (mapped.length > 0) {
      await checkTrailingSL(autoConfig.apiKey, autoConfig.apiSecret, mapped);
    }
  } catch(e) {
    logAuto(`Hızlı pozisyon yöneticisi hata: ${String(e.message||e).slice(0,100)}`);
  } finally {
    fastManagerRunning = false;
  }
}

// ── KAPAT ─────────────────────────────────────────────────────────────────────
app.post('/api/close', async (req, res) => {
  const{apiKey,apiSecret,symbol}=req.body;
  if(!apiKey||!apiSecret||!symbol)return res.status(400).json({error:'Eksik parametre'});
  const sym=symbol.toUpperCase().includes('USDT')?symbol.toUpperCase():symbol.toUpperCase()+'USDT';
  try{
    try{await cancelAlgoOrders(apiKey,apiSecret,sym,true);}catch(e){}
    const pos=await getPositionRisk(apiKey,apiSecret,{symbol:sym});
    const arr=Array.isArray(pos)?pos:[];
    const p=arr.find(x=>Math.abs(parseFloat(x.positionAmt))>0);
    if(!p)return res.json({ok:true,message:'Açık pozisyon yok'});
    const order=await safeMarketClosePosition(apiKey, apiSecret, sym, {reason:'MANUAL_API_CLOSE'});
    if (!order.ok) return res.status(400).json({ok:false,error:order.error||'safe close başarısız'});
    res.json({ok:true,message:`${sym} kapatma denendi`,orderId:order.order?.orderId, alreadyClosed:order.alreadyClosed, fallback:order.fallback});
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
const AUTO_SCAN_INTERVAL_MS = 30 * 1000; // R65: scalper core için 30sn; TOP6/TOP10'da fırsat kaçırmayı azaltır
const autoLog = []; // Son 50 otomatik işlem logu

// ── CANLI TARAMA TELEMETRİSİ ────────────────────────────────────────────────
// Ek Binance çağrısı yapmaz; runAutoScan içinde zaten alınan analizleri hafızada tutar.
// Amaç: Dashboard'da "bot ne tarıyor, neyi bekliyor, neden işlem açmadı" sorusunu göstermek.
let autoScanState = {
  enabled:false, running:false, phase:'KAPALI',
  lastScanStart:null, lastScanEnd:null, nextScanDue:null, currentSymbol:null,
  scanList:[], checked:0, opened:0, skipped:0, livePositions:0, maxPositions:0,
  sessionOpened:autoPersistentStats.sessionOpened||0,
  totalOpenedAllTime:autoPersistentStats.totalOpenedAllTime||0,
  lastOpenedAt:autoPersistentStats.lastOpenedAt||0,
  lastOpenedSymbol:autoPersistentStats.lastOpenedSymbol||null,
  lastOpenedSide:autoPersistentStats.lastOpenedSide||null,
  effectiveMinScore:0, killZone:null, settings:{},
  topCandidates:[], skipReasons:{}, lastAction:'Henüz tarama yok'
};
function resetAutoScanState(patch={}) {
  const prev = autoScanState || {};
  autoScanState = {
    ...autoScanState, ...patch,
    topCandidates: patch.topCandidates || [],
    skipReasons: patch.skipReasons || {},
  };
  autoScanState.sessionOpened = autoPersistentStats.sessionOpened || prev.sessionOpened || 0;
  autoScanState.totalOpenedAllTime = autoPersistentStats.totalOpenedAllTime || prev.totalOpenedAllTime || 0;
  autoScanState.lastOpenedAt = autoPersistentStats.lastOpenedAt || prev.lastOpenedAt || 0;
  autoScanState.lastOpenedSymbol = autoPersistentStats.lastOpenedSymbol || prev.lastOpenedSymbol || null;
  autoScanState.lastOpenedSide = autoPersistentStats.lastOpenedSide || prev.lastOpenedSide || null;
}
function markAutoOpened(symbol, side) {
  autoPersistentStats.totalOpenedAllTime = Number(autoPersistentStats.totalOpenedAllTime || 0) + 1;
  autoPersistentStats.sessionOpened = Number(autoPersistentStats.sessionOpened || 0) + 1;
  autoPersistentStats.lastOpenedAt = Date.now();
  autoPersistentStats.lastOpenedSymbol = String(symbol || '').replace(/USDT$/,'');
  autoPersistentStats.lastOpenedSide = String(side || '').toUpperCase();
  saveAutoStats(autoPersistentStats);
  autoScanState.opened = Number(autoScanState.opened || 0) + 1;
  autoScanState.sessionOpened = autoPersistentStats.sessionOpened;
  autoScanState.totalOpenedAllTime = autoPersistentStats.totalOpenedAllTime;
  autoScanState.lastOpenedAt = autoPersistentStats.lastOpenedAt;
  autoScanState.lastOpenedSymbol = autoPersistentStats.lastOpenedSymbol;
  autoScanState.lastOpenedSide = autoPersistentStats.lastOpenedSide;
}

// R119: Auto panel tanı taşıyıcıları. R118 karar zinciri çalışsa bile markAutoSkip
// eski R97 satırıyla adayı güncellediğinde HTF/R115/R116/R117/R118 bilgileri panelden düşüyordu.
// Bu yardımcılar son kararın likidite haritası + mum playbook bilgisini sembol bazında korur.
const autoDiagBySymbol = new Map();
function r119FmtLevel(l) {
  try {
    if (!l) return '';
    const tf = String(l.label || l.tf || l.timeframe || '-');
    const px = Number(l.price || l.level || 0);
    const distRaw = l.dist ?? l.distancePct ?? l.distPct;
    const dist = Number.isFinite(Number(distRaw)) ? ` u:%${Number(distRaw).toFixed(2)}` : '';
    const strRaw = l.strength ?? l.guc ?? l.score;
    const str = Number.isFinite(Number(strRaw)) ? ` g:${Number(strRaw).toFixed(0)}` : '';
    return `${tf}${px ? ` ${px}` : ''}${dist}${str}`.trim();
  } catch { return ''; }
}
function r119BuildAutoDiag(dc={}) {
  try {
    const parts = [];
    if (dc.r110Phase) parts.push(`HTF faz:${dc.r110Phase}`);
    if (dc.r116HtfGuardBlock && !dc.r117HtfReverseOk) parts.push('HTF amir blok');
    if (dc.r117NearTrapHTF) parts.push(`ters-bölge ${r119FmtLevel(dc.r117TrapLevel)}`.trim());
    if (dc.r117HtfReverseOk) parts.push('ters-hedef HAZIR');
    if (dc.r118CandleOk) parts.push(`mum:${dc.r118CandleOzet||'OK'}`);
    else if (dc.r118CandleOzet) parts.push(`mum-izle:${dc.r118CandleOzet}`);
    if (dc.r111KoprusuOk) parts.push('sıkışma HAZIR');
    if (dc.r111SiksmaBreakout) parts.push(`patlama:${dc.r111SqueezeSkor||0}/4`);
    if (dc.r140Phase?.phase) parts.push(`R140 faz:${dc.r140Phase.phase}`);
    if (dc.r140OiVel?.fakePump) parts.push('R140 sahte-pump');
    if (dc.r140BtcDiv?.score) parts.push(`R140 BTC-div:+${dc.r140BtcDiv.score}`);

    const htf = [];
    if (dc.r110NearSSL) htf.push(`SSL ${r119FmtLevel(dc.r110NearSSL)}`);
    if (dc.r110NearBSL) htf.push(`BSL ${r119FmtLevel(dc.r110NearBSL)}`);
    if (dc.r116CounterLevel) htf.push(`Karşı ${r119FmtLevel(dc.r116CounterLevel)}`);
    if (dc.r117TrapLevel) htf.push(`Ters ${r119FmtLevel(dc.r117TrapLevel)}`);

    const ictDashboard = String(dc.ictDashboard || dc.r88VurKac?.ictDashboard || htf.join(' | ') || '').slice(0,240);
    const siksmaOzet = String(dc.siksmaOzet || dc.r88VurKac?.siksmaOzet || dc.r111Siksma?.ozet || '').slice(0,220);
    const r119HtfDiag = parts.join(' · ').slice(0,260);
    return {
      r111KoprusuOk: !!(dc.r111KoprusuOk || dc.r88VurKac?.r111KoprusuOk),
      siksmaOzet,
      ictDashboard,
      r119HtfDiag,
      r116HtfGuardBlock: !!dc.r116HtfGuardBlock,
      r116HtfGuardReason: String(dc.r116HtfGuardReason || '').slice(0,260),
      r117HtfReverseOk: !!dc.r117HtfReverseOk,
      r117HtfReverseReason: String(dc.r117HtfReverseReason || '').slice(0,260),
      r117NearTrapHTF: !!dc.r117NearTrapHTF,
      r117PrecisionCandleOk: !!dc.r117PrecisionCandleOk,
      r118CandleOk: !!dc.r118CandleOk,
      r118CandleStrong: !!dc.r118CandleStrong,
      r118CandleOzet: String(dc.r118CandleOzet || '').slice(0,220),
      r140Summary: String([
        dc.r140Phase?.phase ? `faz:${dc.r140Phase.phase}` : '',
        dc.r140OiVel?.summary ? dc.r140OiVel.summary : '',
        dc.r140BtcDiv?.label ? dc.r140BtcDiv.label : '',
        dc.r140EqHL?.summary ? dc.r140EqHL.summary : '',
        dc.r140Rvol?.signal ? `RVOL:${dc.r140Rvol.signal}×${dc.r140Rvol.rvol}` : ''
      ].filter(Boolean).join(' · ')).slice(0,260)
    };
  } catch {
    return {};
  }
}

function pushAutoCandidate(row) {
  const symKey = String(row.symbol||'').replace('USDT','');
  const recKey = row.rec || 'WAIT';
  const prevDiag = autoDiagBySymbol.get(`${symKey}_${recKey}`) || autoDiagBySymbol.get(symKey) || {};
  const r = {
    ts:Date.now(),
    symbol:symKey,
    rec:recKey, recTR:trSideLabel(row.rec), tier:row.tier||'WAIT', tierTR:trTierLabel(row.tier||'WAIT'), score:Number(row.score||0),
    longScore:Number(row.longScore||0), shortScore:Number(row.shortScore||0),
    priorityScore:Number(row.priorityScore||0),
    reason:toTurkishText(String(row.reason||row.block||'—')).slice(0,220),
    reasonTR:toTurkishText(String(row.reason||row.block||'—')).slice(0,220),
    action:toTurkishText(String(row.action||'İzle')).slice(0,120),
    actionTR:toTurkishText(String(row.action||'İzle')).slice(0,120),
    sweepRequired: row.sweepRequired,
    entryPermissionReason: row.entryPermissionReason || '',
    entryPermissionReasonTR: toTurkishText(row.entryPermissionReason || ''),
    cvdMissing: !!row.cvdMissing,
    r45RvolStatus: row.r45RvolStatus || '',
    sikismaAktif: !!(row.r111KoprusuOk ?? row.sikismaAktif ?? prevDiag.sikismaAktif),
    siksmaOzet: String(row.siksmaOzet || prevDiag.siksmaOzet || '').slice(0,180),
    ictDashboard: String(row.ictDashboard || prevDiag.ictDashboard || '').slice(0,180),
    htfTani: String(row.r119HtfDiag || row.htfTani || prevDiag.htfTani || '').slice(0,260),
    htfBlok: !!((row.r117HtfReverseOk ? false : row.r116HtfGuardBlock) ?? row.htfBlok ?? prevDiag.htfBlok),
    htfBlokSebep: row.r117HtfReverseOk ? '' : String(row.r116HtfGuardReason || row.htfBlokSebep || prevDiag.htfBlokSebep || '').slice(0,260),
    tersHedef: !!(row.r117HtfReverseOk ?? row.tersHedef ?? prevDiag.tersHedef),
    tersHedefSebep: String(row.r117HtfReverseReason || row.tersHedefSebep || prevDiag.tersHedefSebep || '').slice(0,260),
    tersBolge: !!(row.r117NearTrapHTF ?? row.tersBolge ?? prevDiag.tersBolge),
    hassasMumOk: !!(row.r117PrecisionCandleOk ?? row.hassasMumOk ?? prevDiag.hassasMumOk),
    mumOnay: !!(row.r118CandleOk ?? row.mumOnay ?? prevDiag.mumOnay),
    mumGuclu: !!(row.r118CandleStrong ?? row.mumGuclu ?? prevDiag.mumGuclu),
    mumOzet: String(row.r118CandleOzet || row.mumOzet || prevDiag.mumOzet || '').slice(0,220),
    brainMode: row.brainMode || row.brain?.mode || '',
    brainAction: row.brainAction || row.brain?.action || (row.autoOk ? 'TRADE' : 'WAIT'),
    brainConfidence: Number(row.brainConfidence || row.brain?.confidence || 0),
    brainSummary: String(row.brainSummary || row.brain?.summary || '').slice(0,500),
    r125OrderflowSummary: String(row.r125OrderflowSummary || row.r125Flow?.summary || '').slice(0,300),
    r126FlowSummary: String(row.r126FlowSummary || row.r125Flow?.r126?.summary || '').slice(0,260),
    r140Summary: String(row.r140Summary || prevDiag.r140Summary || '').slice(0,260),
    r126PlaybookAdj: Number(row.r126PlaybookAdj || (row.brainMode ? r126PlaybookAdj(row.brainMode) : 0)),
    r125BestSide: row.r125BestSide || row.r125Flow?.bestSide || '',
    r125LiveDeltaPct: Number(row.r125LiveDeltaPct || row.r125Flow?.deltaPct || 0),
    autoOk: !!row.autoOk
  };
  const diagKeep = {
    sikismaAktif:r.sikismaAktif, siksmaOzet:r.siksmaOzet, ictDashboard:r.ictDashboard,
    htfTani:r.htfTani, htfBlok:r.htfBlok, htfBlokSebep:r.htfBlokSebep,
    tersHedef:r.tersHedef, tersHedefSebep:r.tersHedefSebep, tersBolge:r.tersBolge,
    hassasMumOk:r.hassasMumOk, mumOnay:r.mumOnay, mumGuclu:r.mumGuclu, mumOzet:r.mumOzet,
    r125OrderflowSummary:r.r125OrderflowSummary, r126FlowSummary:r.r126FlowSummary, r140Summary:r.r140Summary, r126PlaybookAdj:r.r126PlaybookAdj, r125BestSide:r.r125BestSide, r125LiveDeltaPct:r.r125LiveDeltaPct
  };
  autoDiagBySymbol.set(`${r.symbol}_${r.rec}`, diagKeep);
  autoDiagBySymbol.set(r.symbol, diagKeep);
  // R34: Aynı sembol aynı taramada iki satır görünmesin. Önce aday, sonra atlandı gelirse son durum yazılır.
  const key = `${r.symbol}_${r.rec}`;
  autoScanState.topCandidates = (autoScanState.topCandidates||[]).filter(x => `${x.symbol}_${x.rec}` !== key);
  autoScanState.topCandidates.push(r);
  const tierRank = t => t==='A'?3:t==='B+'?2:t==='B'?1:0;
  autoScanState.topCandidates = autoScanState.topCandidates
    .sort((a,b)=>tierRank(b.tier)-tierRank(a.tier) || b.score-a.score || (b.priorityScore||0)-(a.priorityScore||0) || b.ts-a.ts)
    .slice(0,12);
}
function markAutoSkip(symbol, reason, row={}) {
  const key = toTurkishText(String(reason||'Bilinmeyen')).slice(0,90);
  autoScanState.skipped = (autoScanState.skipped||0) + 1;
  autoScanState.skipReasons[key] = (autoScanState.skipReasons[key]||0) + 1;
  if (symbol) pushAutoCandidate({symbol, reason:key, action:'Atlandı', ...row});
}


// ── DASHBOARD KRİTİK HATA DURUMU ─────────────────────────────────────────────
app.get('/api/diagnostics/status', (_req, res) => {
  const lastCritical = criticalEvents[criticalEvents.length - 1] || null;
  const recent = criticalEvents.slice(-20).reverse();
  res.json({
    ok: true,
    build: LAZARUS_BUILD,
    hasCritical: recent.some(e => e.level === 'CRITICAL'),
    count: criticalEvents.length,
    lastCritical,
    recent,
    autoPhase: autoScanState?.phase,
    autoLastAction: autoScanState?.lastAction,
    autoErrors: autoScanState?.skipReasons || {},
    positionRisk: {
      cooldownMs: getPositionRiskCooldownMs(),
      cacheAgeMs: positionRiskState.cache ? Date.now() - positionRiskState.cache.ts : null,
      cacheTtlMs: positionRiskState.cache?.ttl || null,
      openCount: positionRiskState.cache?.openCount || 0,
      source: positionRiskState.lastSource || null,
      lastError: positionRiskState.lastError || null,
      inflight: !!positionRiskState.inflight,
    },
    binanceRest: {
      backoffActive: isBinanceBackoffActive(),
      backoffMs: getBinanceBackoffMs(),
      usedWeight: binanceGov.usedWeight,
      queueActive: !!binanceGov.q,
    },
    serverTime: Date.now(),
  });
});
app.post('/api/diagnostics/clear', (_req, res) => {
  criticalEvents.length = 0;
  res.json({ ok:true, message:'Kritik hata ekran kayıtları temizlendi' });
});



// ── R46B HEALTH + SWEEP DEBUG ENDPOINT ───────────────────────────────────────
// Amaç: 1-2 saat beklemeden gerçek server ayarını görmek.
// /api/health içinde sweepOnly true/false, son loglar, son tarama, adaylar ve cooldownlar görünür.
app.get('/api/health', (_req, res) => {
  try {
    const cfg = autoConfig || {};
    const scan = autoScanState || {};
    const logs = Array.isArray(autoLog) ? autoLog.slice(-30) : [];
    const cooldowns = (typeof getCooldownList === 'function') ? getCooldownList() : [];
    const recentCritical = Array.isArray(criticalEvents) ? criticalEvents.slice(-10).reverse() : [];
    const sweepOnly = !!(cfg.sweepOnly === true); // R68: explicit true yoksa sweep zorunlu değildir; panel/health aynı okur.
    const positionRisk = (typeof positionRiskState !== 'undefined') ? {
      cooldownMs: (typeof getPositionRiskCooldownMs === 'function') ? getPositionRiskCooldownMs() : null,
      cacheAgeMs: positionRiskState.cache ? Date.now() - positionRiskState.cache.ts : null,
      cacheTtlMs: positionRiskState.cache?.ttl || null,
      openCount: positionRiskState.cache?.openCount || 0,
      source: positionRiskState.lastSource || null,
      lastError: positionRiskState.lastError || null,
      inflight: !!positionRiskState.inflight,
    } : null;

    res.set('Cache-Control', 'no-store');
    res.json({
      ok: true,
      service: 'Kripto Sinyal PRO',
      build: LAZARUS_BUILD,
      timeISO: new Date().toISOString(),
      serverTime: Date.now(),
      uptimeSec: Math.round(process.uptime()),
      node: process.version,
      auto: {
        enabled: !!cfg.enabled,
        running: !!autoRunning,
        phase: scan.phase || null,
        currentSymbol: scan.currentSymbol || null,
        checked: scan.checked ?? 0,
        opened: scan.opened ?? 0,
        skipped: scan.skipped ?? 0,
        livePositions: scan.livePositions ?? null,
        positionCount: (positionRisk && Number.isFinite(Number(positionRisk.openCount))) ? Number(positionRisk.openCount) : (scan.positionCount ?? null),
        maxPositions: cfg.maxPositions ?? scan.maxPositions ?? scan.settings?.maxPositions ?? null,
        minScore: cfg.minScore ?? scan.settings?.minScore ?? null,
        usdtAmount: cfg.usdtAmount ?? scan.settings?.usdtAmount ?? null,
        leverage: cfg.leverage ?? scan.settings?.leverage ?? null,
        vurKacEnabled: cfg.vurKacEnabled ?? scan.settings?.vurKacEnabled ?? null,
        vurKacAutoLev: cfg.vurKacAutoLev ?? scan.settings?.vurKacAutoLev ?? null,
        vurKacMaxLev: cfg.vurKacMaxLev ?? scan.settings?.vurKacMaxLev ?? null,
        allowLong: cfg.allowLong ?? scan.settings?.allowLong ?? null,
        allowShort: cfg.allowShort ?? scan.settings?.allowShort ?? null,
        sweepOnly,
        sweepMode: sweepOnly
          ? 'SWEEP_ZORUNLU_ACIK_DIRECT_SWEEP_GEREKIR'
          : 'SWEEP_ZORUNLU_KAPALI_NON_SWEEP_BRIDGE_AKTIF',
        nextScanDue: scan.nextScanDue || null,
        lastScanStart: scan.lastScanStart || null,
        lastScanEnd: scan.lastScanEnd || null,
      },
      gate: {
        sweepRequired: sweepOnly,
        expectedAutoLog: sweepOnly
          ? '5m Fırsat Beyni: Sweep AÇIK / net likidite olayı gerekli'
          : 'R153 5m Fırsat Beyni: paralel analiz + coinglass prefetch + btc5m paralel + cal/fg paralel',
        note: `R155; R154b/R154/R153/R152/R151 korunur. ① rvolVeryLow hard-block kaldırıldı (sadece ceza). ② late-chase -12→-8. ③ adaptiveFloor gevşetildi (COUNTER_TRAP floor -20). ④ positionRisk 418 fix. ⑤ Kar koruma erken: BE %0.3, kâr kilidi %0.6/%1.2/%2.0. ⑥ 5m scalp frekans + güvenli kar hedefi: ROI %3-%20 mümkün.`
      },
      lastScan: {
        source: scan.scanSource || null,
        scanMode: scan.scanMode || scan.settings?.scanMode || null,
        scanLimit: scan.scanLimit ?? null,
        scanList: Array.isArray(scan.scanList) ? scan.scanList : [],
        lastAction: scan.lastAction || null,
        skipReasons: scan.skipReasons || {},
        topCandidates: Array.isArray(scan.topCandidates) ? scan.topCandidates.slice(0, 12) : [],
      },
      safety: {
        cooldowns,
        criticalCount: Array.isArray(criticalEvents) ? criticalEvents.length : 0,
        recentCritical,
        positionRisk,
        binanceRest: {
          backoffActive: isBinanceBackoffActive(),
          backoffMs: getBinanceBackoffMs(),
          backoffUntil: Number(binanceGov.backoffUntil || 0),
          usedWeight: binanceGov.usedWeight,
          usedOrders: binanceGov.usedOrders,
          minuteStart: binanceGov.minuteStart,
        },
      },
      recentLogs: logs,
    });
  } catch (e) {
    res.status(500).json({ ok:false, error:e.message, build: typeof LAZARUS_BUILD !== 'undefined' ? LAZARUS_BUILD : 'UNKNOWN' });
  }
});

app.post('/api/auto/config', (req, res) => {
  autoConfig = { ...(req.body||{}) };
  autoConfig.maxPositions = normalizeUserMaxPositions(autoConfig.maxPositions, 3);
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
    config:autoConfig, scanState:autoScanState, recentLogs:autoLog.slice(-40).map(toTurkishText),
    cooldowns: getCooldownList(),
    turkceDurum:{
      faz: toTurkishText(autoScanState?.phase || ''),
      sonIslem: toTurkishText(autoScanState?.lastAction || ''),
      kisaDinlenme:{aktif:isAutoPauseActive(), kalanSaniye:Math.ceil(getAutoPauseRemainMs()/1000), sebep:autoPauseReason||''},
      aciklama:'Bot tüm sensörleri tek beyin içinde birleştirir: HTF likidite haritası, 5m mum, akış/OI/funding, orderbook ve volatilite birlikte okunur. Eski katmanlar emir vermez; sadece veri sağlar.'
    } });
});

function logAuto(msg) {
  // R26: TR saati (Europe/Istanbul = UTC+3)
  const ts = new Intl.DateTimeFormat('tr-TR', {
    timeZone:'Europe/Istanbul', hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false
  }).format(new Date());
  const entry = `${ts} ${toTurkishText(msg)}`;
  autoLog.push(entry);
  if (autoLog.length > 80) autoLog.shift();
  autoScanState.lastAction = entry;
  console.log('[AUTO]', entry); // TR saat
}

function stopAutoTrader(silent=false) {
  if (autoTimer) { clearInterval(autoTimer); autoTimer=null; }
  if (r125FastWakeTimer) { clearInterval(r125FastWakeTimer); r125FastWakeTimer=null; }
  if (positionSyncTimer) { clearInterval(positionSyncTimer); positionSyncTimer=null; }
  if (fastManagerTimer) { clearInterval(fastManagerTimer); fastManagerTimer=null; }
  autoRunning = false;
  if (!silent) {
    resetAutoScanState({enabled:false, running:false, phase:'KAPALI', currentSymbol:null, nextScanDue:null});
    logAuto('Otomatik işlem durduruldu');
  } else {
    resetAutoScanState({running:false, currentSymbol:null});
  }
}


function normalizeUserMaxPositions(v, def=3) {
  const n = Math.floor(Number(v));
  if (!Number.isFinite(n)) return Math.max(1, def|0 || 1);
  // R135: kullanıcı özgürlüğü. Gizli 1/3/10 tavanı yok; sadece anlamsız/negatif değer 1'e çekilir.
  return Math.max(1, n);
}

async function runAutoScan(prioritySymbol=null) {
  // R95: Scan promise eski Binance 418 uykusunda takılı kalırsa yeni scan'i sonsuza kadar engellemesin.
  if (autoRunning) {
    const age = Date.now() - Number(autoScanState.lastScanStart || 0);
    if (age > 75_000) {
      pushCritical('AUTO_SCAN_WATCHDOG', `Tarama ${Math.round(age/1000)}sn takıldı; kilit temizlendi`, {ageMs:age, phase:autoScanState.phase}, 'WARNING');
      autoRunning = false;
      autoScanState.running = false;
      autoScanState.currentSymbol = null;
      autoScanState.lastScanEnd = Date.now();
      autoScanState.phase = isBinanceBackoffActive() ? 'BINANCE_İSTEK_FRENİ' : 'TARAMA_RESETLENDI';
      autoScanState.lastAction = isBinanceBackoffActive()
        ? `Binance geçici istek freni ${Math.ceil(getBinanceBackoffMs()/1000)}sn — yeni işlem yok`
        : 'Takılan tarama temizlendi; yeni tarama sıraya alınacak';
    } else {
      return;
    }
  }
  if (!autoConfig?.enabled) return;
  // R150: priority wake 3-5sn içinde ardışık scan tetikleyip kline/depth/OI 429 üretmesin.
  // Pozisyon yönetimi fastManageOpenPositions ile ayrı çalışır; bu fren sadece yeni tarama/emir adayını geciktirir.
  const r150Now = Date.now();
  if (r150LastScanBeginTs && r150Now - r150LastScanBeginTs < R150_MIN_SCAN_GAP_MS) {
    autoScanState.nextScanDue = r150LastScanBeginTs + R150_MIN_SCAN_GAP_MS;
    if (prioritySymbol) autoScanState.lastAction = `R150 scan freni: ${Math.ceil((R150_MIN_SCAN_GAP_MS - (r150Now-r150LastScanBeginTs))/1000)}sn sonra öncelikli analiz`;
    return;
  }
  r150LastScanBeginTs = r150Now;
  autoRunning = true;
  resetAutoScanState({
    enabled:true, running:true, phase:'BAŞLADI', lastScanStart:Date.now(), lastScanEnd:null,
    currentSymbol:null, scanList:[], checked:0, opened:0, skipped:0, livePositions:0, positionCount:0,
    topCandidates:[], skipReasons:{}, lastAction:'Tarama başlıyor'
  });

  try {
    const cfg = autoConfig;
    const { apiKey, apiSecret, usdtAmount, leverage, marginType,
      maxPositions:rawMaxPositions=3, minScore=70, allowLong=true, allowShort=true,
      sweepOnly=false, scanMode='FAST6', scanLimit=null,
      trailingPct=2, trailStep=0.5, breakEvenPct=1, symbols=[],
      vurKacEnabled=false, vurKacAutoLev=false, vurKacMaxLev=50 } = cfg;

    const maxPositions = normalizeUserMaxPositions(rawMaxPositions, 3);
    const r54ScanMode = normalizeR54ScanMode(scanMode || scanLimit || 'FAST6');
    const r54ScanLimit = r54ScanLimitForMode(r54ScanMode, scanLimit || 6);
    autoScanState.settings = {usdtAmount, leverage, marginType, maxPositions, minScore, allowLong, allowShort, sweepOnly, scanMode:r54ScanMode, scanLimit:r54ScanLimit, trailingPct, trailStep, breakEvenPct, slPct:cfg.slPct, tpPct:cfg.tpPct, minRR:cfg.minRR, vurKacEnabled:!!vurKacEnabled, vurKacAutoLev:!!vurKacAutoLev, vurKacMaxLev:Number(vurKacMaxLev||50)};
    autoScanState.scanMode = r54ScanMode;
    autoScanState.maxPositions = Number(maxPositions||0);
    autoScanState.phase = 'POZİSYON_KONTROL';

    // R95: Binance 418/429 merkezi freni aktifken yeni REST yükü bindirme, taramayı güvenli beklet.
    if (isBinanceBackoffActive()) {
      const rem = Math.ceil(getBinanceBackoffMs()/1000);
      resetStuckPositionRiskInflight('auto-scan-backoff');
      autoScanState.phase = 'BINANCE_İSTEK_FRENİ';
      autoScanState.lastAction = `Binance geçici istek freni ${rem}sn — yeni işlem kapalı, açık pozisyon varsa takipte`;
      logAuto(`⏳ Binance geçici istek freni: ${rem}sn — yeni tarama/emir bekletiliyor`);
      return;
    }

    // 1. Mevcut pozisyonları kontrol et
    const posData = await getPositionRiskCached(apiKey,apiSecret);
    const openPos = Array.isArray(posData)
      ? posData.filter(p=>Math.abs(parseFloat(p.positionAmt))>0)
      : [];
    autoScanState.livePositions = openPos.length;
    autoScanState.positionCount = openPos.length;
    // R30: aynı yönde korele coinlere yığılmayı azalt. Max pozisyon 3 olsa bile
    // bot her scan'de sadece bir yeni işlem açar; aynı yönde ikinci pozisyon için kalite çıtası yükselir.
    const openSideCounts = openPos.reduce((acc,p)=>{
      const amt = parseFloat(p.positionAmt||0);
      const side = amt > 0 ? 'LONG' : amt < 0 ? 'SHORT' : 'WAIT';
      if (side !== 'WAIT') acc[side] = (acc[side]||0) + 1;
      return acc;
    }, {LONG:0, SHORT:0});

    // R18: positionRisk rate-limit aktifken yeni emir açma. Cache ile panel/pozisyon
    // görünür kalır ama taze pozisyon doğrulaması gelene kadar giriş güvenli değildir.
    if (isPositionRiskCooldownActive()) {
      autoScanState.phase = 'VERİ_İSTEĞİ_DİNLENİYOR';
      autoScanState.lastAction = `Pozisyon verisi yoğunluk beklemesi ${Math.ceil(getPositionRiskCooldownMs()/1000)}sn — yeni emir bekletiliyor`;
      logAuto(`⏳ Pozisyon verisi yoğunluğu: ${Math.ceil(getPositionRiskCooldownMs()/1000)}sn dinleniyor, yeni emir açılmayacak`);
      return;
    }

    if (isAutoPauseActive()) {
      const rem = Math.ceil(getAutoPauseRemainMs()/1000);
      autoScanState.phase = 'KISA_DİNLENME';
      autoScanState.lastAction = `Kapanış sonrası ${rem}sn kısa dinlenme — ${autoPauseReason || 'aynı döngüde acele emir yok'}`;
      logAuto(`⏸️ Kapanış sonrası kısa dinlenme: ${rem}sn — yeni emir bekletiliyor`);
      return;
    }

    // Kâr takibi / koruma kontrol
    if (openPos.length > 0) {
      const mapped = openPos.map(p=>{
        const amt = parseFloat(p.positionAmt);
        const ep  = parseFloat(p.entryPrice);
        const mp  = parseFloat(p.markPrice);
        const lev = parseInt(p.leverage)||1;
        const side = amt>0?'LONG':'SHORT';
        const pnlPct = ep>0 ? ((mp-ep)/ep*100*lev*(side==='SHORT'?-1:1)) : 0;
        return {
          symbol:p.symbol, side,
          positionAmt:Math.abs(amt),
          entryPrice:ep, markPrice:mp,
          unrealizedProfit:parseFloat(p.unRealizedProfit ?? p.unrealizedProfit ?? 0),
          leverage:lev,
          pnlPct
        };
      });
      // Her açık pozisyon için trailing state oluştur
      for (const pos of mapped) {
        if (!trailingState.has(pos.symbol)) {
          trailingState.set(pos.symbol, {
            entryPrice:pos.entryPrice, highWater:pos.markPrice,
            breakEvenSet:false, currentSL:null,
            targetTP:calcFallbackTP(pos.entryPrice, pos.side==='LONG', cfg.tpPct),
            leverage:pos.leverage||parseInt(leverage)||1,
            config:{ trailing:true, trailingPct, trailStep, breakEvenPct, entryPrice:pos.entryPrice, targetTP:calcFallbackTP(pos.entryPrice, pos.side==='LONG', cfg.tpPct) }
          });
        }
      }
      await checkTrailingSL(apiKey, apiSecret, mapped);
    }

    // Max pozisyon kontrolü
    if (openPos.length >= maxPositions) {
      autoScanState.phase = 'MAX_POZİSYON_DOLU';
      logAuto(`Max pozisyon (${maxPositions}) doldu, yeni sinyal taranmıyor`);
      return;
    }

    // 2. Kill zone kaldırıldı — kripto 24/7, saat bazlı tarama seyreltilmez.
    const kz = getKillZone();

    // 3. R22 ortak liste — Long/Short ekranı ve Auto aynı havuzdan tarar.
    // 20 coinlik ayrı auto listesi RENDER/ZEC/ALGO gibi A-Tier coinleri dışarıda bırakıyordu.
    const effectiveScanLimit = r54ScanLimit; // R54: panelden FAST6 / TOP10 / TOP24 seçilir
    let scanList = await getUnifiedScanCandidates(effectiveScanLimit, r54ScanMode);
    if (prioritySymbol) {
      const pFull = normalizeSymbol(prioritySymbol);
      const pBase = pFull.replace('USDT','');
      const found = (scanList||[]).find(c => normalizeSymbol(c.fullSymbol || c.symbol) === pFull);
      scanList = [found || {symbol:pBase, fullSymbol:pFull, r54Bucket:'R126_PRIORITY_WAKE'}, ...(scanList||[]).filter(c => normalizeSymbol(c.fullSymbol || c.symbol) !== pFull)];
      logAuto(`⚡ R126 öncelik kuyruğu: ${pBase} canlı orderflow spike nedeniyle ilk analiz ediliyor`);
    }
    if (symbols.length > 0) {
      const wanted = new Set(symbols.map(x => String(x).replace('USDT','').toUpperCase()));
      scanList = scanList.filter(c => wanted.has(String(c.symbol||'').replace('USDT','').toUpperCase()) || wanted.has(String(c.fullSymbol||'').replace('USDT','').toUpperCase()));
    }
    if (!scanList?.length) return;
    logAuto(`🔥 5m Fırsat Beyni ${r54ScanMode} tarama listesi ${scanList.length}: ${scanList.slice(0,8).map(c=>c.symbol).join(', ')}...`);

    // Kill zone bazlı min skor artırma kaldırıldı.
    const effectiveMinScore = minScore;
    autoScanState.killZone = null;
    autoScanState.effectiveMinScore = effectiveMinScore;

    // R153: Haber kontrolü + Fear&Greed paralel al — 2 seri localhost fetch → 1 paralel round-trip
    let fgSignal = 'NEUTRAL';
    try {
      const [calRes, fgRes] = await Promise.allSettled([
        fetch(`http://localhost:${PORT}/api/calendar`).then(r=>r.json()),
        fetch(`http://localhost:${PORT}/api/market-mood`).then(r=>r.json()),
      ]);
      if (calRes.status==='fulfilled' && calRes.value?.dangerZone) {
        logAuto('⛔ Haber saati — tarama durduruldu: ' + (calRes.value.todayEvents||[]).map(e=>e.event).join(', '));
        return;
      }
      if (fgRes.status==='fulfilled') {
        fgSignal = fgRes.value?.signal || 'NEUTRAL';
        if (fgRes.value?.mood === 'EXTREME_GREED' && !allowShort) {
          logAuto('⚠️ Extreme Greed — sadece short izinli, long atlanıyor');
        }
      }
    } catch(e) {}

    // R153: Coinglass prefetch — analiz başlamadan önce tüm scan coin'leri için
    // background'da cache'i ısıt. Analiz sırasında await yok → 0ms bekleme.
    // Coinglass timeout 8sn→3sn: Railway dış ağı için gerçekçi üst sınır.
    try {
      for (const sc of (scanList||[]).slice(0, 10)) {
        const scFull = normalizeSymbol(sc.fullSymbol || sc.symbol);
        cached(`cg_${scFull}`, 15*60*1000, ()=>getCoinglass(scFull)).catch(()=>{});
      }
    } catch(_) {}

    autoScanState.phase = 'TARIYOR';
    autoScanState.scanList = (scanList||[]).map(c=>String(c.symbol||c.fullSymbol||'').replace('USDT','')).slice(0,60);
    autoScanState.scanSource = LAZARUS_BUILD;
    autoScanState.scanLimit = effectiveScanLimit;
    autoScanState.lastScanTR = new Intl.DateTimeFormat('tr-TR', {
      timeZone:'Europe/Istanbul', day:'2-digit', month:'2-digit',
      hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false
    }).format(new Date());
    // R42: Analiz başlamadan önce ilk 24 aday için CVD/tick WS prewarm.
    // Bu REST isteği değildir; Binance ban riskini artırmadan CVD=0/UNKNOWN kaynaklı kör kararları azaltır.
    try {
      const warmSymbols = (scanList||[]).slice(0,24).map(c => normalizeSymbol(c.fullSymbol || c.symbol)).filter(Boolean);
      r130StartCombinedAggTradeStream(warmSymbols, {replace:true});
      for (const fs of warmSymbols) startCVDStream(fs);
      // R130: WS açılışını uzun bekletme; combined stream arka planda akacak.
      await sleep(900);
    } catch(_e) {}
    logAuto(`Tarama başladı: ${scanList.length} coin, max poz:${maxPositions}, mevcut:${openPos.length}`);

    // 3. Her coini analiz et
    for (const coin of scanList) {
      if ((await getNewPosCount()) >= maxPositions) { autoScanState.phase='MAX_POZİSYON_DOLU'; break; }
      autoScanState.currentSymbol = String(coin.symbol||coin.fullSymbol||'').replace('USDT','');
      autoScanState.checked = (autoScanState.checked||0) + 1;

      // Zaten pozisyon var mı?
      const alreadyOpen = openPos.some(p=>p.symbol===coin.fullSymbol);
      if (alreadyOpen) { markAutoSkip(coin.symbol, 'Zaten açık pozisyon var'); continue; }

      // Cooldown kontrolü:
      // FULL mod → analiz bile yapma
      // SAME_SIDE_AFTER_CLOSE → analizi yine yap (ters flip fırsatı olabilir)
      const fullSymCheck = normalizeSymbol(coin.fullSymbol || coin.symbol);
      const preCd = getCooldownInfo(fullSymCheck);
      if (preCd && preCd.mode !== 'SAME_SIDE_AFTER_CLOSE') {
        const remMin = Math.ceil(getCooldownRemainMs(fullSymCheck)/60000);
        markAutoSkip(coin.symbol, `Bekleme süresi ${remMin}dk kaldı`, {rec:'CD', tier:'CD', reason:`Bekleme süresi ${remMin}dk`});
        continue;
      }

      try {
        const analysis = await fetch(`http://localhost:${PORT}/api/analyze/${coin.fullSymbol}`)
          .then(r=>r.json());
        if (!analysis.ok) {
          const emsg = String(analysis.error || analysis.code || 'Analiz OK değil').slice(0,90);
          pushCritical('AUTO_ANALYZE_' + (coin.fullSymbol || coin.symbol), emsg, { symbol: coin.fullSymbol || coin.symbol, code:analysis.code || 'ANALYZE_NOT_OK' }, 'CRITICAL');
          markAutoSkip(coin.symbol, `Analiz OK değil: ${emsg}`, {rec:'ERR', tier:'ERR', longScore:0, shortScore:0, reason:emsg});
          continue;
        }

        const { longScore, shortScore, isExpired, freshness } = analysis;
        let recommendation = analysis.recommendation;
        let decisionChain = analysis.decisionChain || {};
        if (isExpired || freshness === 'EXPIRED') { markAutoSkip(coin.symbol, 'Sinyal süresi geçmiş'); continue; }

        // ── PRO TRADER KARAR ZİNCİRİ — A-Tier ana karar, toksik filtreler veto ──
        // Skor kafadan üretilmez: /api/analyze içindeki MM + CVD + OI + Funding + Tick + Sweep + Wyckoff katmanlarından gelir.
        // Otomatik işlemde tekrar tekrar aynı şeyi sert kapı yapıp botu boğma; A-Tier karar zaten hasEntry + delta + funding + skor kontrolü yapar.

        let score = recommendation==='LONG'?longScore:shortScore;
        let isLong = recommendation==='LONG';
        let isShort = recommendation==='SHORT';

        // R163: Beyin bypass durumu scan loop'un EN BAŞINDA hesaplanır.
        // R162b'de bu değişken entryPermission/tier kontrollerinden sonra doğduğu için
        // bazı eski kapılar R160/R159 kararını emirden önce kesebiliyordu.
        const r162BrainBypassActive = r120Bool(
          decisionChain?.r160TraderDecision ||
          decisionChain?.r159MomentumPass ||
          decisionChain?.r156FastBypass
        );
        const r121BrainTradeOk = !!(
          decisionChain?.brainAction === 'TRADE' &&
          (decisionChain?.autoOk === true || r162BrainBypassActive)
        );
        const r121BrainOwnsRisk = !!(r121BrainTradeOk && (r162BrainBypassActive || Number(decisionChain?.brainConfidence||0) >= 55));

        // R163: Nadir ama kritik durum: analiz tarafı brainAction=TRADE üretmiş ama
        // legacy recommendation WAIT kalmışsa, yönü decisionChain.side üzerinden geri al.
        // Bu sadece R160/R159/R156 gibi bypass kararlarında çalışır; normal WAIT korunur.
        if (recommendation === 'WAIT' && r162BrainBypassActive && ['LONG','SHORT'].includes(String(decisionChain?.side||''))) {
          recommendation = String(decisionChain.side);
          score = recommendation==='LONG' ? longScore : shortScore;
          isLong = recommendation==='LONG';
          isShort = recommendation==='SHORT';
          logAuto(`🧠 ${coin.symbol} R163 WAIT→${recommendation} düzeltmesi: beyin TRADE kararını legacy WAIT gölgesinden çıkardı`);
        }

        pushAutoCandidate({
          symbol:coin.symbol, rec:recommendation, tier:decisionChain?.tier||'WAIT', score, longScore, shortScore,
          priorityScore:decisionChain?.priorityScore, reason:decisionChain?.reason,
          action:decisionChain?.autoOk?(decisionChain?.entryPermissionReason||'Aday'):'İzle',
          sweepRequired:decisionChain?.sweepRequired, entryPermissionReason:decisionChain?.entryPermissionReason,
          cvdMissing:decisionChain?.cvdMissing, r45RvolStatus:decisionChain?.r45RvolStatus, autoOk:decisionChain?.autoOk, brainMode:decisionChain?.brainMode, brainAction:decisionChain?.brainAction, brainConfidence:decisionChain?.brainConfidence, brainSummary:decisionChain?.brainSummary, r133FastScalpOverride:decisionChain?.r133FastScalpOverride, r133FastScalpWhy:decisionChain?.r133FastScalpWhy, r134FastScalpOverride:decisionChain?.r134FastScalpOverride, r134FastScalpWhy:decisionChain?.r134FastScalpWhy,
          r125Flow:decisionChain?.r125Flow, r125OrderflowSummary:decisionChain?.r125OrderflowSummary, r126FlowSummary:decisionChain?.r126FlowSummary,
          ...r119BuildAutoDiag(decisionChain),
          r48DirectSweepBalanceOk:decisionChain?.r48DirectSweepBalanceOk,
          r49DirectSweepUnlockOk:decisionChain?.r49DirectSweepUnlockOk,
          r50AutoPermissionOk:decisionChain?.r50AutoPermissionOk,
          r50DirectSweepMatrixOk:decisionChain?.r50DirectSweepMatrixOk,
          r50NonSweepMatrixOk:decisionChain?.r50NonSweepMatrixOk,
          r51DirectSweepMinEdgeOk:decisionChain?.r51DirectSweepMinEdgeOk,
          r53SmartEdgeScoreOk:decisionChain?.r53SmartEdgeScoreOk,
          r54MicroProbeOk:decisionChain?.r54MicroProbeOk,
          r57ScalperBTierBridgeOk:decisionChain?.r57ScalperBTierBridgeOk,
          r61TrendContinuationBridgeOk:decisionChain?.r61TrendContinuationBridgeOk,
          r61TrendEffectiveScore:decisionChain?.r61TrendEffectiveScore,
          r61TrendContinuationBoost:decisionChain?.r61TrendContinuationBoost,
          r61TrendPriorityOk:decisionChain?.r61TrendPriorityOk,
          r60StrongTrendContinuation:decisionChain?.r60StrongTrendContinuation,
          r62CounterTrendTrapBridgeOk:decisionChain?.r62CounterTrendTrapBridgeOk,
          r62TrapHardClean:decisionChain?.r62TrapHardClean,
          r62CounterTrendTrapContextOk:decisionChain?.r62CounterTrendTrapContextOk,
          r62CounterTrendTrapFlowOk:decisionChain?.r62CounterTrendTrapFlowOk,
          r53EffectiveScore:decisionChain?.r53EffectiveScore,
          r53SmartEdgeBoost:decisionChain?.r53SmartEdgeBoost,
          r53CvdSmartSafe:decisionChain?.r53CvdSmartSafe,
          r50EffectivePriority:decisionChain?.r50EffectivePriority,
          r47:{ready:decisionChain?.r47Readiness, need:decisionChain?.r47Needed, t:decisionChain?.r47TimingPts, f:decisionChain?.r47FlowPts, c:decisionChain?.r47ContextPts, s:decisionChain?.r47StructurePts, v:decisionChain?.r47RvolPts}
        });

        // Yön izni
        if (isLong  && !allowLong)  { markAutoSkip(coin.symbol, 'Long kapalı', {rec:recommendation, score}); continue; }
        if (isShort && !allowShort) { markAutoSkip(coin.symbol, 'Short kapalı', {rec:recommendation, score}); continue; }
        if (recommendation==='WAIT') {
          const waitScore = Math.max(Number(longScore||0), Number(shortScore||0));
          const waitSide = Number(longScore||0) >= Number(shortScore||0) ? 'LONG' : 'SHORT';
          const waitReason = r120AutoReason(decisionChain, decisionChain?.reason || (waitScore >= Number(minScore||0)
            ? `WAIT_DIAG: skor ${waitScore}>=${minScore} ama 5m Fırsat Beyni TRADE eşiği görmedi`
            : `WAIT_DIAG: skor ${waitScore}<${minScore} / ${waitSide} izleme`));
          markAutoSkip(coin.symbol, 'WAIT karar', {rec:recommendation, tier:'WAIT', score:waitScore, longScore, shortScore, reason:waitReason, waitSide, waitDiagnostic:true,
            priorityScore:decisionChain?.priorityScore, entryPermissionReason:decisionChain?.entryPermissionReason, autoOk:false,
            brainMode:decisionChain?.brainMode, brainAction:decisionChain?.brainAction || 'WATCH', brainConfidence:decisionChain?.brainConfidence, brainSummary:decisionChain?.brainSummary, r133FastScalpOverride:decisionChain?.r133FastScalpOverride, r133FastScalpWhy:decisionChain?.r133FastScalpWhy, r134FastScalpOverride:decisionChain?.r134FastScalpOverride, r134FastScalpWhy:decisionChain?.r134FastScalpWhy,
            r125Flow:decisionChain?.r125Flow, r125OrderflowSummary:decisionChain?.r125OrderflowSummary, r126FlowSummary:decisionChain?.r126FlowSummary,
            ...r119BuildAutoDiag(decisionChain)
          });
          continue;
        }

        // R25: SAME_SIDE_AFTER_CLOSE → bypass kontrolü (ters fitil flip varsa geç)
        const postCd = getCooldownInfo(fullSymCheck);
        if (postCd) {
          const bypass = canBypassCooldownForReverse(postCd, recommendation, decisionChain);
          if (!bypass) {
            const remMin = Math.ceil(getCooldownRemainMs(fullSymCheck, recommendation, decisionChain)/60000);
            markAutoSkip(coin.symbol, `Bekleme ${remMin}dk: ${postCd.reason||'aynı yön beklemede'}`, {rec:recommendation, tier:'CD', score, reason:postCd.reason});
            continue;
          }
          logAuto(`🔁 ${coin.symbol} bekleme ters yön izni: eski ${trSideLabel(postCd.side)}, yeni ${trSideLabel(recommendation)}, terazi ${decisionChain?.priorityScore||0}`);
        }

        // R45: UI'daki Sweep/Likidite teyidi checkbox'ı artık gerçek emir kapısıdır.
        if (decisionChain && decisionChain.entryPermissionOk === false && !r162BrainBypassActive) {
          const r47Dbg = decisionChain?.sweepRequired ? '' : ` / R47 ${decisionChain?.r47Readiness||0}/${decisionChain?.r47Needed||0} T${decisionChain?.r47TimingPts||0}/F${decisionChain?.r47FlowPts||0}/C${decisionChain?.r47ContextPts||0}/S${decisionChain?.r47StructurePts||0}/V${decisionChain?.r47RvolPts||0}`;
          const why = r120AutoReason(decisionChain, `5m Fırsat Beyni izle: ${recommendation} için emir izni yok`);
          logAuto(`⛔ ${coin.symbol} ${why}`);
          markAutoSkip(coin.symbol, why, {rec:recommendation, tier:decisionChain?.tier, score, longScore, shortScore, reason:decisionChain?.reason, priorityScore:decisionChain?.priorityScore, entryPermissionReason:decisionChain?.entryPermissionReason, sweepRequired:decisionChain?.sweepRequired, autoOk:decisionChain?.autoOk, ...r119BuildAutoDiag(decisionChain), r48DirectSweepBalanceOk:decisionChain?.r48DirectSweepBalanceOk, r49DirectSweepUnlockOk:decisionChain?.r49DirectSweepUnlockOk, r50AutoPermissionOk:decisionChain?.r50AutoPermissionOk, r50DirectSweepMatrixOk:decisionChain?.r50DirectSweepMatrixOk, r50NonSweepMatrixOk:decisionChain?.r50NonSweepMatrixOk, r51DirectSweepMinEdgeOk:decisionChain?.r51DirectSweepMinEdgeOk, r53SmartEdgeScoreOk:decisionChain?.r53SmartEdgeScoreOk,
          r54MicroProbeOk:decisionChain?.r54MicroProbeOk, r57ScalperBTierBridgeOk:decisionChain?.r57ScalperBTierBridgeOk, r61TrendContinuationBridgeOk:decisionChain?.r61TrendContinuationBridgeOk, r62CounterTrendTrapBridgeOk:decisionChain?.r62CounterTrendTrapBridgeOk, r74Top10ProScalperOk:decisionChain?.r74Top10ProScalperOk, r74ImpulseEntryOk:decisionChain?.r74ImpulseEntryOk, r74Top10ContextBypassOk:decisionChain?.r74Top10ContextBypassOk, r74ScoreFloor:decisionChain?.r74ScoreFloor, r68UnifiedScalperCoreOk:decisionChain?.r68UnifiedScalperCoreOk, r68EntryEventOk:decisionChain?.r68EntryEventOk, r68TrendContextOk:decisionChain?.r68TrendContextOk, r68CounterTrapContextOk:decisionChain?.r68CounterTrapContextOk, r68CriticalHardBlock:decisionChain?.r68CriticalHardBlock, r69PriorityContextOverrideOk:decisionChain?.r69PriorityContextOverrideOk, r69ContextOk:decisionChain?.r69ContextOk, r69PriorityExecutionOk:decisionChain?.r69PriorityExecutionOk, r65ScalperCoreOk:decisionChain?.r65ScalperCoreOk, r65ScalperCoreTrendOk:decisionChain?.r65ScalperCoreTrendOk, r65ScalperCoreCounterTrapOk:decisionChain?.r65ScalperCoreCounterTrapOk, r65ScalperCoreHardVeto:decisionChain?.r65ScalperCoreHardVeto, r53EffectiveScore:decisionChain?.r53EffectiveScore, r53SmartEdgeBoost:decisionChain?.r53SmartEdgeBoost, r53CvdSmartSafe:decisionChain?.r53CvdSmartSafe, r50EffectivePriority:decisionChain?.r50EffectivePriority, r47:{ready:decisionChain?.r47Readiness, need:decisionChain?.r47Needed, t:decisionChain?.r47TimingPts, f:decisionChain?.r47FlowPts, c:decisionChain?.r47ContextPts, s:decisionChain?.r47StructurePts, v:decisionChain?.r47RvolPts}});
          continue;
        }

        // R20: A-Tier normal auto, B+ kontrollü auto. B normalde panelde görünür ama açılmaz.
        const tierOk = decisionChain?.autoOk === true && ['A','B+'].includes(String(decisionChain?.tier || ''));
        // R162/R163 FIX: R160/R159/R156 trader kararı brainAction=TRADE set ediyor ama autoOk eski tier sistemine bağlı.
        // R163'te bypass değişkenleri yukarı taşındı; burada yalnızca tier kapısı uygulanır.
        // R162 FIX: tierOk bloğu bypass aktifse atlanır
        if (!tierOk && !r162BrainBypassActive) {
          const r47Dbg = '';
          const why = r120AutoReason(decisionChain, `5m Fırsat Beyni izle: ${recommendation} için güven/kanıt yetersiz`);
          logAuto(`📊 ${coin.symbol} ${why} — otomatik açılmıyor`);
          markAutoSkip(coin.symbol, why, {rec:recommendation, tier:decisionChain?.tier, score, longScore, shortScore, reason:decisionChain?.reason, priorityScore:decisionChain?.priorityScore, autoOk:decisionChain?.autoOk, ...r119BuildAutoDiag(decisionChain), r48DirectSweepBalanceOk:decisionChain?.r48DirectSweepBalanceOk, r49DirectSweepUnlockOk:decisionChain?.r49DirectSweepUnlockOk, r50AutoPermissionOk:decisionChain?.r50AutoPermissionOk, r50DirectSweepMatrixOk:decisionChain?.r50DirectSweepMatrixOk, r50NonSweepMatrixOk:decisionChain?.r50NonSweepMatrixOk, r51DirectSweepMinEdgeOk:decisionChain?.r51DirectSweepMinEdgeOk, r53SmartEdgeScoreOk:decisionChain?.r53SmartEdgeScoreOk,
          r54MicroProbeOk:decisionChain?.r54MicroProbeOk, r57ScalperBTierBridgeOk:decisionChain?.r57ScalperBTierBridgeOk, r61TrendContinuationBridgeOk:decisionChain?.r61TrendContinuationBridgeOk, r62CounterTrendTrapBridgeOk:decisionChain?.r62CounterTrendTrapBridgeOk, r74Top10ProScalperOk:decisionChain?.r74Top10ProScalperOk, r74ImpulseEntryOk:decisionChain?.r74ImpulseEntryOk, r74Top10ContextBypassOk:decisionChain?.r74Top10ContextBypassOk, r74ScoreFloor:decisionChain?.r74ScoreFloor, r68UnifiedScalperCoreOk:decisionChain?.r68UnifiedScalperCoreOk, r68EntryEventOk:decisionChain?.r68EntryEventOk, r68TrendContextOk:decisionChain?.r68TrendContextOk, r68CounterTrapContextOk:decisionChain?.r68CounterTrapContextOk, r68CriticalHardBlock:decisionChain?.r68CriticalHardBlock, r69PriorityContextOverrideOk:decisionChain?.r69PriorityContextOverrideOk, r69ContextOk:decisionChain?.r69ContextOk, r69PriorityExecutionOk:decisionChain?.r69PriorityExecutionOk, r65ScalperCoreOk:decisionChain?.r65ScalperCoreOk, r65ScalperCoreTrendOk:decisionChain?.r65ScalperCoreTrendOk, r65ScalperCoreCounterTrapOk:decisionChain?.r65ScalperCoreCounterTrapOk, r65ScalperCoreHardVeto:decisionChain?.r65ScalperCoreHardVeto, r53EffectiveScore:decisionChain?.r53EffectiveScore, r53SmartEdgeBoost:decisionChain?.r53SmartEdgeBoost, r53CvdSmartSafe:decisionChain?.r53CvdSmartSafe, r50EffectivePriority:decisionChain?.r50EffectivePriority, r47:{ready:decisionChain?.r47Readiness, need:decisionChain?.r47Needed, t:decisionChain?.r47TimingPts, f:decisionChain?.r47FlowPts, c:decisionChain?.r47ContextPts, s:decisionChain?.r47StructurePts, v:decisionChain?.r47RvolPts}});
          continue;
        }
        // R20 savunma katmanı: CVD yokken sadece terazi/bridge zayıfsa durdur — bypass aktifse geç.
        if (decisionChain?.cvdWarmingBridge && !decisionChain?.cvdBridgeQualityOk && !decisionChain?.autoOk && !r162BrainBypassActive) {
          const why = `CVD köprüsü zayıf (${decisionChain?.bridgeCount||0}/4, skor ${score}, terazi ${decisionChain?.priorityScore||0}) — otomatik açılmıyor`;
          logAuto(`📊 ${coin.symbol} ${why}`);
          markAutoSkip(coin.symbol, why, {rec:recommendation, tier:decisionChain?.tier, score, longScore, shortScore, reason:decisionChain?.reason, priorityScore:decisionChain?.priorityScore, autoOk:decisionChain?.autoOk, ...r119BuildAutoDiag(decisionChain), r48DirectSweepBalanceOk:decisionChain?.r48DirectSweepBalanceOk, r49DirectSweepUnlockOk:decisionChain?.r49DirectSweepUnlockOk, r50AutoPermissionOk:decisionChain?.r50AutoPermissionOk, r50DirectSweepMatrixOk:decisionChain?.r50DirectSweepMatrixOk, r50NonSweepMatrixOk:decisionChain?.r50NonSweepMatrixOk, r51DirectSweepMinEdgeOk:decisionChain?.r51DirectSweepMinEdgeOk, r53SmartEdgeScoreOk:decisionChain?.r53SmartEdgeScoreOk,
          r54MicroProbeOk:decisionChain?.r54MicroProbeOk, r57ScalperBTierBridgeOk:decisionChain?.r57ScalperBTierBridgeOk, r61TrendContinuationBridgeOk:decisionChain?.r61TrendContinuationBridgeOk, r62CounterTrendTrapBridgeOk:decisionChain?.r62CounterTrendTrapBridgeOk, r74Top10ProScalperOk:decisionChain?.r74Top10ProScalperOk, r74ImpulseEntryOk:decisionChain?.r74ImpulseEntryOk, r74Top10ContextBypassOk:decisionChain?.r74Top10ContextBypassOk, r74ScoreFloor:decisionChain?.r74ScoreFloor, r68UnifiedScalperCoreOk:decisionChain?.r68UnifiedScalperCoreOk, r68EntryEventOk:decisionChain?.r68EntryEventOk, r68TrendContextOk:decisionChain?.r68TrendContextOk, r68CounterTrapContextOk:decisionChain?.r68CounterTrapContextOk, r68CriticalHardBlock:decisionChain?.r68CriticalHardBlock, r69PriorityContextOverrideOk:decisionChain?.r69PriorityContextOverrideOk, r69ContextOk:decisionChain?.r69ContextOk, r69PriorityExecutionOk:decisionChain?.r69PriorityExecutionOk, r65ScalperCoreOk:decisionChain?.r65ScalperCoreOk, r65ScalperCoreTrendOk:decisionChain?.r65ScalperCoreTrendOk, r65ScalperCoreCounterTrapOk:decisionChain?.r65ScalperCoreCounterTrapOk, r65ScalperCoreHardVeto:decisionChain?.r65ScalperCoreHardVeto, r53EffectiveScore:decisionChain?.r53EffectiveScore, r53SmartEdgeBoost:decisionChain?.r53SmartEdgeBoost, r53CvdSmartSafe:decisionChain?.r53CvdSmartSafe, r50EffectivePriority:decisionChain?.r50EffectivePriority, r47:{ready:decisionChain?.r47Readiness, need:decisionChain?.r47Needed, t:decisionChain?.r47TimingPts, f:decisionChain?.r47FlowPts, c:decisionChain?.r47ContextPts, s:decisionChain?.r47StructurePts, v:decisionChain?.r47RvolPts}});
          continue;
        }

        // MM yönü: sadece yüksek güvenli doğrudan ters MM hedefi veto eder. Nötr/likidite hedefleri skorda kalır, bot boğulmaz.
        const mm = analysis.marketMaker || {};
        const mmConf = parseFloat(mm.confidence || 0);
        const mmTarget = String(mm.target || 'UNKNOWN');
        const mmHardOpposite = isLong
          ? (mmTarget === 'GENUINE_DOWN' && mmConf >= 65)
          : (mmTarget === 'GENUINE_UP'   && mmConf >= 65);
        if (mmHardOpposite) {
          logAuto(`${coin.symbol} yüksek güvenli MM ters (${mmTarget}/${mmConf}) — atlandı`);
          markAutoSkip(coin.symbol, `MM ters ${mmTarget}/${mmConf}`, {rec:recommendation, tier:decisionChain?.tier, score});
          continue;
        }

        // CVD/Tick delta: sadece net toksik ters akış veto eder. Nötr akış fırsatı öldürmez.
        const cvd = analysis.cvd || {};
        const tick = analysis.tickData || {};
        const cvdRatio = parseFloat(cvd.ratio ?? 50);
        const tickTrend = String(tick.deltaTrend || 'UNKNOWN');
        const cvdToxic = isLong
          ? (cvdRatio < 35 && tickTrend === 'BEAR')
          : (cvdRatio > 65 && tickTrend === 'BULL');
        if (cvdToxic) {
          logAuto(`${coin.symbol} CVD+Tick net ters (${cvdRatio}%/${tickTrend}) — atlandı`);
          markAutoSkip(coin.symbol, `CVD+Tick ters ${cvdRatio}%/${tickTrend}`, {rec:recommendation, tier:decisionChain?.tier, score});
          continue;
        }

        // Funding: sadece aşırı ters funding veto eder.
        const fund = analysis.funding || {};
        const fundOk = isLong
          ? fund?.signal !== 'EXTREME_POSITIVE'
          : fund?.signal !== 'EXTREME_NEGATIVE';
        if (!fundOk) { logAuto(`${coin.symbol} Funding aşırı karşı (${fund?.current}) — atlandı`); markAutoSkip(coin.symbol, `Funding aşırı karşı ${fund?.current}`, {rec:recommendation, score}); continue; }

        // R78: R37/R35 B+ restore için skor filtresi artık ikinci kez öldürücü değil.
        // R37'de B+ olmuş bir aday R77'de burada tekrar minScore'a takılıyordu (ör: FLNC B+ skor 52<72).
        // Eğer karar çekirdeği B+/auto köprü üretmişse, TOP10 scalper floor kullanılır; aksi halde panel minScore korunur.
        // R80: B+ kontrollü aday, ikinci minScore filtresinde tekrar boğulmasın.
        // Panel minScore yüksek olabilir (72); TOP10 5m küçük caplerde CVD/veri ısınması yüzünden ham skor düşük kalır.
        // Bunu kör açmayız: sadece A/B+ + entryPermission/autoOk + gerçek hard block yok + R47>=5 + skor tabanı geçerse izin veririz.
        const r80BPlusScoreFloor = Math.max(32, Number(effectiveMinScore || 68) - 40, Math.min(40, Number(decisionChain?.r74ScoreFloor || 40)));
        const r80RealHardBlock = !!(
          decisionChain?.r68CriticalHardBlock || decisionChain?.r65ScalperCoreHardVeto ||
          decisionChain?.poorLiquidity || decisionChain?.atrExtremeBlock || decisionChain?.r41FallingKnifeBlock || decisionChain?.r41RisingKnifeBlock
        );
        // R85: B artı puan tabanı tek başına emir açtırmaz.
        // R84'te HYUNDAI örneğinde görülen hata: funding + 5m destek + bağlam ile
        // skor tabanı geçildi ama canlı 5m giriş olayı/terazi yeterli değildi.
        // Yeni disiplin: score-floor sadece canlı hareket izi + yeterli terazi varsa çalışır.
        const r85Terazi = Number(decisionChain?.priorityScore ?? decisionChain?.r50EffectivePriority ?? 0);
        const r85R47 = Number(decisionChain?.r47Readiness || 0);
        const r85TimingPts = Number(decisionChain?.r47TimingPts || 0);
        const r85FlowPts = Number(decisionChain?.r47FlowPts || 0);
        const r85ReasonText = String(decisionChain?.reason || '');
        const r85CanliGirisIziOk = !!(
          decisionChain?.directSweepOk || decisionChain?.hardSweepForBridge ||
          decisionChain?.r51DirectSweepMinEdgeOk || decisionChain?.r75RetestBridgeOk ||
          decisionChain?.r66WyckoffTrapReclaimOk || decisionChain?.r67ScalperCoreHuntEntryOk ||
          decisionChain?.r37EarlyOk || decisionChain?.fresh5mImpulseOrRecent ||
          decisionChain?.r62CounterTrendTrapBridgeOk || decisionChain?.r65ScalperCoreCounterTrapOk ||
          decisionChain?.r88VurKacOk || decisionChain?.r86FormasyonVeriTeyitOk || decisionChain?.r88VurKacOk ||
          (decisionChain?.r74Top10ProScalperOk && r85TimingPts >= 1)
        );
        const r85SadeceFundingDestek = !!(
          /funding/i.test(r85ReasonText) &&
          /(R39|destek|swing_low|swing high|SWING_LOW|SWING_HIGH)/i.test(r85ReasonText) &&
          !r85CanliGirisIziOk
        );
        const r85TeraziDisiplinOk = !!(
          r85Terazi >= 45 ||
          (r85Terazi >= 38 && r85R47 >= 8 && r85TimingPts >= 1) ||
          decisionChain?.r75RetestBridgeOk || decisionChain?.r51DirectSweepMinEdgeOk ||
          decisionChain?.r88VurKacOk || r121BrainOwnsRisk || decisionChain?.directSweepOk
        );
        const r85BartiDisiplinOk = !!(
          r85CanliGirisIziOk && r85TeraziDisiplinOk && !r85SadeceFundingDestek
        );
        const r80ControlledBPlusScoreOk = !!(
          ['A','B+'].includes(String(decisionChain?.tier || '')) &&
          (decisionChain?.autoOk === true || decisionChain?.entryPermissionOk === true || decisionChain?.r50AutoPermissionOk || decisionChain?.r88VurKacOk || decisionChain?.r86FormasyonVeriTeyitOk || decisionChain?.r75RetestBridgeOk || decisionChain?.r74Top10ProScalperOk) &&
          r85R47 >= 5 &&
          Number(score || 0) >= r80BPlusScoreFloor &&
          !r80RealHardBlock &&
          r85BartiDisiplinOk
        );
        const r78BridgeScoreFloor = Math.max(40, Number(effectiveMinScore || 68) - 25, Number(decisionChain?.r74ScoreFloor || 0));
        const r78BridgeScoreBypassOk = !!(
          String(decisionChain?.tier || '').includes('B+') &&
          Number(score || 0) >= r78BridgeScoreFloor &&
          !decisionChain?.r68CriticalHardBlock &&
          !decisionChain?.r65ScalperCoreHardVeto
        );
        const r78PermissionScoreBypassOk = !!(
          Number(score || 0) >= r78BridgeScoreFloor &&
          (decisionChain?.r88VurKacOk || decisionChain?.r75RetestBridgeOk || decisionChain?.r74Top10ProScalperOk || decisionChain?.r68UnifiedScalperCoreOk || decisionChain?.r67ScalperCoreHuntEntryOk || decisionChain?.r65ScalperCoreOk || decisionChain?.r50AutoPermissionOk)
        );
        // R85: düşük skor floor ile geçecekse önce giriş disiplini raporlanır.
        if (!r121BrainTradeOk && score < effectiveMinScore && ['A','B+'].includes(String(decisionChain?.tier || '')) && Number(score || 0) >= r80BPlusScoreFloor && !r85BartiDisiplinOk) {
          const why = r120AutoReason(decisionChain, `5m Fırsat Beyni izle: B+ görünüm var ama canlı giriş izi/akış yeterli değil — giriş:${r85CanliGirisIziOk?'VAR':'YOK'} güven:${decisionChain?.brainConfidence||0}/100`);
          logAuto(`⏳ ${coin.symbol} ${why}`);
          markAutoSkip(coin.symbol, why, {rec:recommendation, tier:decisionChain?.tier, score, longScore, shortScore, reason:decisionChain?.reason, priorityScore:decisionChain?.priorityScore, ...r119BuildAutoDiag(decisionChain), r85CanliGirisIziOk, r85Terazi, r85R47, r85TimingPts, r85FlowPts, r85SadeceFundingDestek, r85BartiDisiplinOk});
          continue;
        }

        if (!r121BrainTradeOk && score < effectiveMinScore && !r80ControlledBPlusScoreOk && !r78BridgeScoreBypassOk && !r78PermissionScoreBypassOk) {
          logAuto(`${coin.symbol} skor ${score} < ${effectiveMinScore} — atlandı`);
          markAutoSkip(coin.symbol, `Skor düşük ${score}<${effectiveMinScore}`, {rec:recommendation, tier:decisionChain?.tier, score, longScore, shortScore, reason:decisionChain?.reason, ...r119BuildAutoDiag(decisionChain), r80BPlusScoreFloor, r80ControlledBPlusScoreOk, r78BridgeScoreFloor, r78BridgeScoreBypassOk, r78PermissionScoreBypassOk});
          continue;
        }
        if (score < effectiveMinScore && (r80ControlledBPlusScoreOk || r121BrainTradeOk)) {
          logAuto(`🟢 ${coin.symbol} 5m Fırsat Beyni kontrollü aday: skor ${score}/${effectiveMinScore}, güven ${decisionChain?.brainConfidence||0}/100, mod ${r120BrainModeLabel(decisionChain?.brainMode)}`);
        }

        // R85 AKILLI GEÇ GİRİŞ + GİRİŞ DİSİPLİNİ:
        // Eski R80 mantığı zone=PREMIUM veya RSI4h>=72 gördüğü anda B+ adayı da öldürüyordu.
        // Bu RIF/FLNC gibi TOP10 5m devam/retest fırsatlarını boğdu.
        // Artık geç giriş riski üçe ayrılır:
        // 1) Hard: bağlam riski yüksek, hedef yakın, veya premium+RSI var ama güçlü B+/retest/sweep yok.
        // 2) Soft-pass: B+/A, R47 güçlü, score-floor geçti, hard block yok ve retest/sweep/entry izi var.
        // 3) Side-aware: LONG kovalanıyorsa sadece LONG engellenir; coin karşı yön setup'ı için sonraki taramada yaşamaya devam eder.
        const ctx = analysis.r29 || {};
        const sideCtxRisk = isLong ? Number(ctx.longRisk||0) : Number(ctx.shortRisk||0);
        const pdZone = String(analysis?.premiumDiscount?.['1h']?.zone || analysis?.premiumDiscount?.['4h']?.zone || '');
        const antiChaseZone = isLong ? pdZone.includes('PREMIUM') : pdZone.includes('DISCOUNT');
        const rsi4hNow = Number(analysis?.timeframes?.['4h']?.rsi || 50);
        const antiChaseRsi = isLong ? rsi4hNow >= 72 : rsi4hNow <= 28;
        const antiChase = !!((sideCtxRisk >= 45 || antiChaseZone || antiChaseRsi) && String(decisionChain?.tier||'') !== 'A');
        const eliteOverride = String(decisionChain?.tier||'') === 'A' && Number(decisionChain?.priorityScore||0) >= 82 && decisionChain?.microConfirm && !decisionChain?.cvdMissing;
        const r81EntryTraceOk = !!(
          decisionChain?.r75RetestBridgeOk || decisionChain?.r51DirectSweepMinEdgeOk ||
          decisionChain?.r50AutoPermissionOk || decisionChain?.r74Top10ProScalperOk ||
          decisionChain?.r67ScalperCoreHuntEntryOk || decisionChain?.r65ScalperCoreOk ||
          decisionChain?.directSweepOk || decisionChain?.r37EarlyOk || decisionChain?.fresh5mImpulseOrRecent
        );
        const r81StrongBPlusSoftPass = !!(
          ['A','B+'].includes(String(decisionChain?.tier || '')) &&
          (r80ControlledBPlusScoreOk || r78BridgeScoreBypassOk || r78PermissionScoreBypassOk || decisionChain?.autoOk === true) &&
          Number(decisionChain?.r47Readiness || 0) >= 7 &&
          Number(score || 0) >= r80BPlusScoreFloor &&
          !r80RealHardBlock && r81EntryTraceOk
        );

        // R87: R86'da görülen FLNC/JTO tipi hata düzeltildi.
        // B+ puan tabanı + formasyon/veri teyidi artık premium/RSI yüksek bölgede tek başına market emir açtırmaz.
        // Riskli yönde emir için gerçek DEVAM ONAYI gerekir: geri test, kırılım, geri kazanım,
        // taze 5m impuls + akış, ya da doğrudan sweep sonrası yeni 5m zamanlama.
        const r88GeriTestOnayiOk = !!(decisionChain?.r75RetestBridgeOk || decisionChain?.r37Timing?.retestOk);
        const r88KirilimOnayiOk = !!(decisionChain?.r39SR?.breakConfirmed || (decisionChain?.r39Confluence && r85TimingPts >= 2 && r85FlowPts >= 1));
        const r88GeriKazanimOnayiOk = !!(decisionChain?.r66WyckoffTrapReclaimOk || decisionChain?.r67ScalperCoreHuntEntryOk);
        const r88TazeImpulsOnayiOk = !!(
          decisionChain?.fresh5mImpulseOrRecent && (decisionChain?.r37EarlyOk || r85TimingPts >= 2) &&
          r85FlowPts >= 1 && r85R47 >= 7 && r85Terazi >= 45
        );
        const r88SweepZamanlamaOnayiOk = !!(
          decisionChain?.directSweepOk && r85TimingPts >= 2 && r85R47 >= 8 &&
          (r85FlowPts >= 1 || decisionChain?.r51DirectSweepMinEdgeOk)
        );
        const r88DevamOnayiOk = !!(
          r88GeriTestOnayiOk || r88KirilimOnayiOk || r88GeriKazanimOnayiOk ||
          r88TazeImpulsOnayiOk || r88SweepZamanlamaOnayiOk
        );
        const r88RiskliBolgeKuvvetli = !!(antiChaseZone || antiChaseRsi || decisionChain?.r39TargetNearBlock || sideCtxRisk >= 30);
        const r88RiskliYonDevamYok = !!(antiChase && !eliteOverride && r88RiskliBolgeKuvvetli && !r88DevamOnayiOk);

        const r81AntiChaseHard = !!(
          r88RiskliYonDevamYok ||
          (
            antiChase && !eliteOverride && !(r81StrongBPlusSoftPass && sideCtxRisk < 45 && r88DevamOnayiOk) &&
            (sideCtxRisk >= 45 || decisionChain?.r39TargetNearBlock || (antiChaseZone && antiChaseRsi) || !r81EntryTraceOk)
          )
        );
        if (r81AntiChaseHard && !r121BrainOwnsRisk) {
          // R85 KARŞI YÖN RADARI karşı tuzak:
          // LONG premium/RSI/geç giriş riski yediğinde coin komple atılmaz; SHORT karşı-trap tarafı kontrol edilir.
          // SHORT dip/discount geç giriş riski yediğinde LONG reclaim/retest tarafı kontrol edilir.
          // Bu kör ters işlem değildir: karşı tarafta A/B+, entryPermission, R47>=5, counter-trap izi ve hard güvenlik temizliği gerekir.
          const rotateSide = isLong ? 'SHORT' : 'LONG';
          const rotateAllowed = rotateSide === 'LONG' ? !!allowLong : !!allowShort;
          const rotateDC = analysis?.sideDecisions?.[rotateSide] || null;
          const rotateScore = rotateSide === 'LONG' ? Number(longScore || 0) : Number(shortScore || 0);
          const rotateFloor = Math.max(32, Number(effectiveMinScore || 68) - 40, Math.min(40, Number(rotateDC?.r74ScoreFloor || 40)));
          const rotateRealHardBlock = !!(
            rotateDC?.r68CriticalHardBlock || rotateDC?.r65ScalperCoreHardVeto ||
            rotateDC?.poorLiquidity || rotateDC?.atrExtremeBlock || rotateDC?.r41FallingKnifeBlock || rotateDC?.r41RisingKnifeBlock
          );
          const rotateEntryTraceOk = !!(
            rotateDC?.r75RetestBridgeOk || rotateDC?.r51DirectSweepMinEdgeOk ||
            rotateDC?.r50AutoPermissionOk || rotateDC?.r74Top10ProScalperOk ||
            rotateDC?.r67ScalperCoreHuntEntryOk || rotateDC?.r65ScalperCoreOk ||
            rotateDC?.directSweepOk || rotateDC?.r37EarlyOk || rotateDC?.fresh5mImpulseOrRecent
          );
          const rotateControlledBPlusOk = !!(
            ['A','B+'].includes(String(rotateDC?.tier || '')) &&
            (rotateDC?.autoOk === true || rotateDC?.entryPermissionOk === true || rotateDC?.r50AutoPermissionOk || rotateDC?.r75RetestBridgeOk || rotateDC?.r74Top10ProScalperOk || rotateDC?.r62CounterTrendTrapBridgeOk || rotateDC?.r65ScalperCoreCounterTrapOk) &&
            Number(rotateDC?.r47Readiness || 0) >= 5 &&
            rotateScore >= rotateFloor && !rotateRealHardBlock
          );
          const rotateTrapEvidenceOk = !!(
            rotateDC?.r62CounterTrendTrapBridgeOk || rotateDC?.r65ScalperCoreCounterTrapOk ||
            rotateDC?.r46ExhaustionShort || rotateDC?.wickTrapFlip?.favorable ||
            rotateDC?.r39SR?.breakConfirmed || rotateDC?.r39Confluence ||
            (rotateDC?.r41TrapBlock && rotateDC?.r42TrapReclaimOk) ||
            (rotateSide === 'SHORT' && (pdZone.includes('PREMIUM') || rsi4hNow >= 72)) ||
            (rotateSide === 'LONG' && (pdZone.includes('DISCOUNT') || rsi4hNow <= 28))
          );
          const rotateRisk = rotateSide === 'LONG' ? Number(ctx.longRisk||0) : Number(ctx.shortRisk||0);
          const rotateAntiChaseZone = rotateSide === 'LONG' ? pdZone.includes('PREMIUM') : pdZone.includes('DISCOUNT');
          const rotateAntiChaseRsi = rotateSide === 'LONG' ? rsi4hNow >= 72 : rsi4hNow <= 28;
          const rotateAntiChaseHard = !!((rotateRisk >= 45 || (rotateAntiChaseZone && rotateAntiChaseRsi)) && String(rotateDC?.tier||'') !== 'A');
          const r82CounterTrapOk = !!(
            rotateAllowed && rotateDC && rotateDC.pass && rotateControlledBPlusOk &&
            rotateDC.entryPermissionOk !== false && rotateEntryTraceOk && rotateTrapEvidenceOk &&
            !rotateAntiChaseHard
          );

          if (r82CounterTrapOk) {
            const oldSide = recommendation;
            logAuto(`🔁 ${coin.symbol} 5m Fırsat Beyni yön değiştirdi: ${oldSide} geç giriş riski → ${rotateSide} karşı tuzak aktif | güven ${rotateDC?.brainConfidence||0}/100 skor ${rotateScore}/${effectiveMinScore}, risk ${rotateRisk}`);
            recommendation = rotateSide;
            decisionChain = rotateDC;
            score = rotateScore;
            isLong = rotateSide === 'LONG';
            isShort = rotateSide === 'SHORT';
          } else {
            const why = `5m Fırsat Beyni riskli yön freni: ${recommendation} için risk ${sideCtxRisk}, bölge:${pdZone||'-'}, RSI4s:${rsi4hNow}; devam onayı:${r88DevamOnayiOk?'VAR':'YOK'}; ${rotateSide} kontrolü:${rotateAllowed?'açık':'kapalı'} skor:${rotateScore} tuzak:${rotateTrapEvidenceOk?'VAR':'YOK'} giriş-izi:${rotateEntryTraceOk?'VAR':'YOK'}`;
            logAuto(`⛔ ${coin.symbol} ${why}`);
            markAutoSkip(coin.symbol, why, {rec:recommendation, tier:decisionChain?.tier, score, priorityScore:decisionChain?.priorityScore, ...r119BuildAutoDiag(decisionChain), r81EntryTraceOk, r81StrongBPlusSoftPass, r88DevamOnayiOk, r88GeriTestOnayiOk, r88KirilimOnayiOk, r88GeriKazanimOnayiOk, r88TazeImpulsOnayiOk, r88SweepZamanlamaOnayiOk, rotateSide, rotateAllowed, rotateTier:rotateDC?.tier, rotateScore, rotateFloor, rotateR47:rotateDC?.r47Readiness, rotateEntryTraceOk, rotateTrapEvidenceOk, rotateAntiChaseHard});
            continue;
          }
        }
        if (antiChase && !eliteOverride && r81StrongBPlusSoftPass) {
          logAuto(`🟡 ${coin.symbol} 5m Fırsat Beyni riskli bölge izliyor: ${recommendation} ${decisionChain?.tier} skor ${score}/${effectiveMinScore}, güven ${decisionChain?.brainConfidence||0}/100, bölge:${pdZone||'-'}, RSI4s:${rsi4hNow}, devam onayı:${r88DevamOnayiOk?'VAR':'YOK'}`);
        }

        // R116: Son emir öncesi HTF amir kontrolü. Analiz zinciri bir sebeple geçse bile 1H/4H karşı seviyede legacy emir açılmaz.
        if (decisionChain?.r116HtfGuardBlock && !decisionChain?.r117HtfReverseOk && !(decisionChain?.r160TraderDecision && Number(decisionChain?.r160TrueCount||0) >= 4 && Number(decisionChain?.brainConfidence||0) >= 55)) {
          const why = r120AutoReason(decisionChain, `HTF likidite amiri — ${recommendation} emir yok`);
          logAuto(`⛔ ${coin.symbol} ${why}`);
          markAutoSkip(coin.symbol, why, {rec:recommendation, tier:decisionChain?.tier, score, priorityScore:decisionChain?.priorityScore, ...r119BuildAutoDiag(decisionChain), r116CounterLevel:decisionChain?.r116CounterLevel, r116CounterDist:decisionChain?.r116CounterDist, entryPermissionReason:decisionChain?.entryPermissionReason});
          continue;
        }

        // R114: Son savunma. WLD tipi hata: B+ / Sweep+StopHunt / MM_UP_SWEEP ama 5m body-shift aşağı.
        // Bu durumda wick avı bitmemiş sayılır; 5m gövde reclaim veya ICT/squeeze yapı onayı beklenir.
        if (decisionChain?.r114TrapBlock && !(decisionChain?.r160TraderDecision && Number(decisionChain?.r160TrueCount||0) >= 4 && Number(decisionChain?.brainConfidence||0) >= 60)) {
          const why = r120AutoReason(decisionChain, `5m body-shift tuzağı — ${recommendation} emir yok`);
          logAuto(`⛔ ${coin.symbol} ${why}`);
          markAutoSkip(coin.symbol, why, {rec:recommendation, tier:decisionChain?.tier, score, priorityScore:decisionChain?.priorityScore, ...r119BuildAutoDiag(decisionChain), r114Shift:decisionChain?.r114Shift, r114ReclaimOk:decisionChain?.r114ReclaimOk, entryPermissionReason:decisionChain?.entryPermissionReason});
          continue;
        }

        // R38 F&G: 5m Top Gainers scalping'de Fear/Greed tek başına hard veto değildir.
        // Sadece EXTREME durumda ve coin top-mover değilse ya da karar zinciri zayıfsa durdurur.
        const r38AutoTopMover = !!(coin.topGainerLocked || Math.abs(Number(coin.change24h||0)) >= 6 || Number(coin.volume||0) >= 100000000 || decisionChain?.r38TopMoverStrong);
        const r38FngStrongDecision = r162BrainBypassActive || (Number(decisionChain?.priorityScore||0) >= 76 && (decisionChain?.r37EarlyOk || decisionChain?.scalperBridge || decisionChain?.r38RetestBridgeOk));
        if (fgSignal==='EXTREME_GREED' && isLong && !(r38AutoTopMover && r38FngStrongDecision))  { logAuto(`${coin.symbol} Extreme Greed — long atlandı`); markAutoSkip(coin.symbol, 'Extreme Greed long veto', {rec:recommendation, score}); continue; }
        if (fgSignal==='EXTREME_FEAR'  && isShort && !(r38AutoTopMover && r38FngStrongDecision)) { logAuto(`${coin.symbol} Extreme Fear — short atlandı`); markAutoSkip(coin.symbol, 'Extreme Fear short veto', {rec:recommendation, score}); continue; }
        if ((fgSignal==='EXTREME_GREED' && isLong) || (fgSignal==='EXTREME_FEAR' && isShort)) logAuto(`🟡 ${coin.symbol} F&G soft geçildi: top-mover + güçlü 5m karar zinciri`);

        // Likidasyon cascade: sadece pozisyon yönüne direkt ters kaskad veto eder.
        const liq = analysis.liquidations;
        if (liq?.cascade) {
          const adverseCascade = isLong
            ? liq.cascade.direction==='LONG_CASCADE'
            : liq.cascade.direction==='SHORT_CASCADE';
          if (adverseCascade) { logAuto(`${coin.symbol} adverse cascade (${liq.cascade.direction}) — atlandı`); markAutoSkip(coin.symbol, `Adverse cascade ${liq.cascade.direction}`, {rec:recommendation, score}); continue; }
        }

        // ── R97 VUR-KAÇ PİYASA GÜVENLİĞİ ─────────────────────────────────────
        if (decisionChain?.r88VurKacEnabled && decisionChain?.r88PiyasaBozuk && !decisionChain?.r93DalgaliAmaIslemYapilabilir && !r121BrainOwnsRisk) {
          const why = `5m Fırsat Beyni işlem yok: piyasa zemini tehlikeli/uygunsuz — makas:${decisionChain?.r88SpreadWide?'GENİŞ':'normal'} defter:${decisionChain?.r88DefterInce?'İNCE':'normal'} oynaklık:${decisionChain?.r88OynaklikAsiri?'AŞIRI':'normal'} zemin:${decisionChain?.r93PiyasaEtiketi||'BOZUK'} dönüş:${decisionChain?.r93DonusRadariOk?'VAR':'YOK'}`;
          logAuto(`⛔ ${coin.symbol} ${why}`);
          markAutoSkip(coin.symbol, why, {rec:recommendation, tier:decisionChain?.tier, score, r88:decisionChain?.r88VurKac, r93:{etiket:decisionChain?.r93PiyasaEtiketi, tehlikeli:decisionChain?.r93PiyasaTehlikeli, dalgaliIslem:decisionChain?.r93DalgaliAmaIslemYapilabilir, merdiven:decisionChain?.r93MerdivenDevamOk, donus:decisionChain?.r93DonusRadariOk, donusSkor:decisionChain?.r93DonusSkor}});
          continue;
        }
        if (decisionChain?.r93DalgaliAmaIslemYapilabilir) {
          logAuto(`🟡 ${coin.symbol} 5m Fırsat Beyni dalgalı zemini izliyor: zemin ${decisionChain?.r93PiyasaEtiketi}, trend:${decisionChain?.r93MerdivenDevamOk?'VAR':'YOK'} dönüş:${decisionChain?.r93DonusRadariOk?'VAR':'YOK'} güven:${decisionChain?.brainConfidence||0}/100`);
        }

        // ── KULLANICI RİSK AYARLARI ───────────────────────────────────────────────
        // Otomatik emir gerçek panel değerlerini kullanır:
        // usdtAmount = kullanıcının marjı, leverage = kullanıcının kaldıracı,
        // slPct/tpPct = kaldıraçsız coin hareketi yüzdesi.
        const entryRef = parseFloat(analysis.price || coin.price || 0);
        if (!entryRef) { logAuto(`${coin.symbol} fiyat alınamadı — atlandı`); markAutoSkip(coin.symbol, 'Fiyat alınamadı'); continue; }

        let userSLPct = Math.max(0.05, parseFloat(cfg.slPct ?? 2));
        let userTPPct = Math.max(0.05, parseFloat(cfg.tpPct ?? 10));
        let userRR    = userTPPct / userSLPct;

        // ── R24 RVOL MİKRO LİKİDİTE FRENİ — performansı boğmadan sadece ekstrem zayıf hacmi keser
        const rvolNum = Number(analysis?.rvol?.['1h']?.rvol || analysis?.rvol?.rvol || analysis?.r15?.rvol?.rvol || 0);
        if (rvolNum > 0 && rvolNum < 0.08) {
          logAuto(`⛔ ${coin.symbol} RVOL çok düşük (${rvolNum.toFixed(2)}x) — likidite yetersiz, otomatik atlandı`);
          markAutoSkip(coin.symbol, `RVOL çok düşük ${rvolNum.toFixed(2)}x`, {rec:recommendation, tier:decisionChain?.tier, score});
          continue;
        }


        // ── R170 HEDEF WR + FREKANS KORUYUCU ─────────────────────────────────
        // Kullanıcı hedefi: işlem sıklığı boğulmayacak, fakat canlı WR %60-85 bandı asla göz ardı edilmeyecek.
        // Mantık: güçlü yollar açık kalır (R159 8/9p, R160 4/4); zayıf yollar canlı performans bozulunca kapanır.
        try {
          const r170ModeObj = r170PerfMode();
          const r170Mode = r170ModeObj.mode;
          const r170Perf = r170ModeObj.perf || {}; // hesap geneli GÜNLÜK performans
          const r170Acc = r170ModeObj.account || {};
          const r170True = Number(decisionChain?.r160TrueCount || 0);
          const r170Q2 = !!decisionChain?.r160Q2Flow;
          const r170Q3 = !!decisionChain?.r160Q3Momentum;
          const r170Q4 = !!decisionChain?.r160Q4Proof;
          const r170Edge = Number(decisionChain?.brainConfidence || 0);
          const r170R159Pts = Number(decisionChain?.r159Points || 0);
          const r170FlowEdge = Number(decisionChain?.r125SideFlow?.edge || decisionChain?.r125Flow?.sideEdge || decisionChain?.brainR125FlowEdge || 0);
          const r170LiveTicks = Number(decisionChain?.r133LiveTradeCount || decisionChain?.liveTradeCount || 0);
          // R173: Silver Bullet + mum kapanış + hacim spike + retest kalitesi.
          // Amaç: işlem sayısını normal modda boğmadan, recovery modda sadece gerçek kaliteyi geçirmek.
          const r173Q = r173ContextQuality(decisionChain, analysis);
          const r172RecoveryMode = (r170Mode === 'HIGH_WR_RECOVERY' || r170Mode === 'WARMUP_HIGH_WR');
          const r172VeryBadDay = !!(Number(r170Perf.closed||0) >= 8 && (Number(r170Perf.wr||0) < 0.45 || Number(r170Perf.pf||0) < 0.90 || Number(r170Perf.net||0) < -3));
          const r173SweepNeedsClose = !!(r173Q.sweep && !r173Q.candleCloseOk && !r173Q.retestOk && !r173Q.silverBullet);
          const r173RetestOrSilver = !!(r173Q.silverBullet || (r173Q.retestOk && r173Q.candleCloseOk && r173Q.volOk12));
          const r175MinScore = Number(effectiveMinScore || autoConfig?.minScore || 70);
          const r170StrongMomentum = !!(decisionChain?.r159MomentumPass && (
            r172RecoveryMode
              ? (
                  // R175: STG tipi R159 8p/score50 hasarı için warmup/recovery'de 8p geçmez; 9p elit ya da SilverBullet gerekir.
                  (r170R159Pts >= 9 && r170Edge >= 92 && score >= Math.max(65, r175MinScore-5) && r170Q2 && r170Q3 && r170Q4 && r170FlowEdge >= 8 && r170LiveTicks >= 18 && r173Q.volSpike15 && r173RetestOrSilver) ||
                  (r170R159Pts >= 8 && r170Edge >= 90 && score >= r175MinScore && r170Q2 && r170Q3 && r170Q4 && r173Q.silverBullet && r173Q.volSpike15)
                )
              : (r170R159Pts >= 8 && r170Edge >= 82 && score >= Math.max(58, r175MinScore-10) && r173Q.volOk12 && !r173SweepNeedsClose)
          ));
          const r170StrongR160 = !!(decisionChain?.r160TraderDecision && (
            r172RecoveryMode
              ? (r170True >= 4 && r170Q2 && r170Q3 && r170Q4 && r170Edge >= 90 && score >= Math.max(68, r175MinScore-2) && r170FlowEdge >= 8 && r170LiveTicks >= 18 && r173Q.volSpike15 && r173RetestOrSilver)
              : (r170True >= 4 && r170Q2 && r170Q3 && r170Q4 && r170Edge >= 78 && score >= Math.max(58, r175MinScore-10) && r173Q.volOk12 && !r173SweepNeedsClose)
          ));
          const r170GoodThree = !!(!r172RecoveryMode && decisionChain?.r160TraderDecision && r170True === 3 && r170Q2 && r170Q3 && r170Q4 && r170Edge >= 82 && score >= 58 && r170FlowEdge >= 6 && r173Q.volOk12);
          const r170FreqProtectedPath = r170StrongMomentum || r170StrongR160 || r170GoodThree;
          const r175WeakR159 = !!(decisionChain?.r159MomentumPass && r170R159Pts === 8 && (score < r175MinScore || !r170Q2 || !r170Q3 || !r170Q4 || !r173Q.volSpike15 || !r173RetestOrSilver));
          if (r172RecoveryMode && r175WeakR159) {
            const why = `R175 warmup/recovery R159 8p hasar freni: score:${score}/${r175MinScore}, Q2:${r170Q2?'1':'0'} Q3:${r170Q3?'1':'0'} Q4:${r170Q4?'1':'0'}, RVOL:${r173Q.rvol?r173Q.rvol.toFixed(2):'?'}, retest/silver:${r173RetestOrSilver?'VAR':'YOK'} — işlem yok`;
            logAuto(`⛔ ${coin.symbol} ${why}`);
            markAutoSkip(coin.symbol, why, {rec:recommendation, score, edge:r170Edge, r159:r170R159Pts, mode:r170Mode, rvol:r173Q.rvol, retest:r173Q.retestOk, silver:r173Q.silverBullet});
            continue;
          }


          if (r173SweepNeedsClose) {
            const why = `R173 mum kapanış/retest bekliyor: sweep var ama kapanış/retest teyidi yok; wick sweep tuzak riski`;
            logAuto(`⏳ ${coin.symbol} ${why}`);
            markAutoSkip(coin.symbol, why, {rec:recommendation, score, edge:r170Edge, r159:r170R159Pts, r160True:r170True, rvol:r173Q.rvol});
            continue;
          }

          if (r172RecoveryMode && !r173Q.volSpike15) {
            const why = `R173 recovery hacim spike filtresi: RVOL ${r173Q.rvol ? r173Q.rvol.toFixed(2)+'x' : 'yok'}; 1.5x+ kurumsal hacim bekleniyor`;
            logAuto(`⛔ ${coin.symbol} ${why}`);
            markAutoSkip(coin.symbol, why, {rec:recommendation, score, edge:r170Edge, rvol:r173Q.rvol, mode:r170Mode});
            continue;
          }

          if (r172RecoveryMode && r173Q.sweep && !r173RetestOrSilver) {
            const why = `R173 retest/Silver Bullet bekliyor: sweep sonrası direkt giriş yok; FVG+MSS veya ilk retest gerekli`;
            logAuto(`⏳ ${coin.symbol} ${why}`);
            markAutoSkip(coin.symbol, why, {rec:recommendation, score, edge:r170Edge, silver:r173Q.silverBullet, retest:r173Q.retestOk, fvg:r173Q.fvg, mss:r173Q.mss});
            continue;
          }

          if (r172RecoveryMode && !r170FreqProtectedPath) {
            const wrTxt = r170Perf.wr !== null ? (r170Perf.wr*100).toFixed(0) : '?';
            const why = `R172 HIGH_WR_RECOVERY elit filtre: günlük WR%${wrTxt}, PF:${Number(r170Perf.pf||0).toFixed(2)}, net:${Number(r170Perf.net||0).toFixed(2)}$ → sadece R159 9p elit veya R160 4/4 elit geçer`;
            logAuto(`⛔ ${coin.symbol} ${why}`);
            markAutoSkip(coin.symbol, why, {rec:recommendation, score, edge:r170Edge, r159:r170R159Pts, r160True:r170True, flow:r170FlowEdge, ticks:r170LiveTicks, mode:r170Mode});
            continue;
          }

          if (r172VeryBadDay && (score < 68 || r170Edge < 88 || r170FlowEdge < 7)) {
            const why = `R172 çok kötü gün freni: score/edge/flow elit değil — score:${score}, edge:${r170Edge}, flow:${r170FlowEdge}`;
            logAuto(`⛔ ${coin.symbol} ${why}`);
            markAutoSkip(coin.symbol, why, {rec:recommendation, score, edge:r170Edge, flow:r170FlowEdge, mode:r170Mode});
            continue;
          }

          // 6p/7p momentum sadece hedef sağlıklıyken ve yapı tam destekliyorsa geçer; aksi halde winrate'i aşağı çeker.
          if (decisionChain?.r159MomentumPass && r170R159Pts < 8 && !r170GoodThree) {
            const why = `R170 WR hedef freni: R159 ${r170R159Pts}p zayıf; 8p+ veya Q2+Q3+Q4 güçlü 3/4 gerekli`;
            logAuto(`⛔ ${coin.symbol} ${why}`);
            markAutoSkip(coin.symbol, why, {rec:recommendation, score, edge:r170Edge, r159:r170R159Pts, mode:r170Mode});
            continue;
          }

          // R160 3/4 içinde ivme yoksa, yapı+akış+kanıt tek başına son canlı veride kayıp üretiyor.
          if (decisionChain?.r160TraderDecision && r170True === 3 && !r170Q3 && !r170StrongMomentum) {
            const why = `R170 WR hedef freni: R160 3/4 ama ivme yok; frekans için 8p momentum veya 4/4 beklenir`;
            logAuto(`⛔ ${coin.symbol} ${why}`);
            markAutoSkip(coin.symbol, why, {rec:recommendation, score, edge:r170Edge, r160True:r170True, mode:r170Mode});
            continue;
          }

          // Canlı performans hedef altına inerse seçici moda geç: işlem tamamen durmaz, sadece güçlü yollar kalır.
          if (r170Mode === 'HIGH_WR_RECOVERY' && !r170FreqProtectedPath) {
            const wrTxt = r170Perf.wr !== null ? (r170Perf.wr*100).toFixed(0) : '?';
            const why = `R170b HIGH_WR_RECOVERY: HESAP GENELİ günlük WR%${wrTxt}, PF:${Number(r170Perf.pf||0).toFixed(2)}, net:${Number(r170Perf.net||0).toFixed(2)}$ | haftalık ${r170FmtPerf(r170Acc.week)} | aylık ${r170FmtPerf(r170Acc.month)} → sadece 8p+ momentum / 4-4 R160 / güçlü 3-4 geçer`;
            logAuto(`⛔ ${coin.symbol} ${why}`);
            markAutoSkip(coin.symbol, why, {rec:recommendation, score, edge:r170Edge, r159:r170R159Pts, r160True:r170True, mode:r170Mode, perf:r170Perf});
            continue;
          }

          // Frekans koruması: sistem hedefteyse güçlü yolları gereksiz eski kapılarla boğma; logla görünür yap.
          if (r170FreqProtectedPath) {
            const wrTxt = r170Perf.wr !== null ? (r170Perf.wr*100).toFixed(0) : '?';
            logAuto(`🧭 ${coin.symbol} R170b HESAP hedef profili: ${r170Mode} günlük WR%${wrTxt} | haftalık ${r170FmtPerf(r170Acc.week)} | aylık ${r170FmtPerf(r170Acc.month)} | freq:${r170TradeFreq(60*60*1000)}/saat → güçlü yol korundu (${decisionChain?.r159MomentumPass?'R159 '+r170R159Pts+'p':'R160 '+r170True+'/4'})`);
          }
        } catch(_e170) {}

        // R169: Canlı sonuç kalite freni — işlem sıklığını tamamen boğmadan zayıf 3/4 R160 ve R144 mikro-scalp'i keser.
        // Kayıp örnekleri: JTO/B/PORTAL 3/4 (yapı+akış+kanıt) ama ivme/gerçek devam yok → -12% ila -20% ROI.
        try {
          const r169T = Number(decisionChain?.r160TrueCount || 0);
          const r169Q2 = !!decisionChain?.r160Q2Flow;
          const r169Q3 = !!decisionChain?.r160Q3Momentum;
          const r169Q4 = !!decisionChain?.r160Q4Proof;
          const r169Edge = Number(decisionChain?.brainConfidence || 0);
          const r169FlowEdge = Number(decisionChain?.r125SideFlow?.edge || decisionChain?.r125Flow?.sideEdge || decisionChain?.brainR125FlowEdge || 0);
          const r169WeakR160 = !!(decisionChain?.r160TraderDecision && r169T === 3 && !(r169Q2 && r169Q4 && (r169Q3 || r169Edge >= 90)));
          const r169WeakScore = !!(decisionChain?.r160TraderDecision && r169T === 3 && (score < 50 || r169Edge < 72));
          const r169WeakR144 = !!(decisionChain?.r133FastScalpOverride && !decisionChain?.r159MomentumPass && !(r169T >= 4 || (score >= 60 && r169Edge >= 85)));
          const r169RecentAnyLoss = r166CoinRecentLosses(coin.fullSymbol, 90*60*1000);
          const r169HasFreshLossWeak = r169RecentAnyLoss >= 1 && !(decisionChain?.r159MomentumPass && Number(decisionChain?.r159Points||0) >= 8) && !(r169T >= 4 && r169Edge >= 80);
          if (r169WeakR160 || r169WeakScore || r169WeakR144 || r169HasFreshLossWeak) {
            const why = r169HasFreshLossWeak
              ? `R169 taze kayıp freni: son 90dk aynı coin kayıp var, sadece 8p+ momentum veya 4/4 güçlü R160 geçer`
              : r169WeakR144
                ? `R169 R144 mikro-scalp freni: hızlı edge tek başına yetmedi`
                : `R169 zayıf R160 3/4 freni: Q2:${r169Q2?'1':'0'} Q3:${r169Q3?'1':'0'} Q4:${r169Q4?'1':'0'} edge:${r169Edge} score:${score}`;
            logAuto(`⛔ ${coin.symbol} ${why}`);
            markAutoSkip(coin.symbol, why, {rec:recommendation, score, edge:r169Edge, r160True:r169T, flowEdge:r169FlowEdge});
            continue;
          }
        } catch(_e169q) {}

        // R172: aynı coin/yön son 3 saatte zarar verdiyse recovery modda sadece elit sinyal geçsin.
        // Bu TAG/JTO/BANK/B tekrar deneme hasarını azaltır; ters yön fırsatı serbest kalır.
        try {
          const r172ModeObj2 = r170PerfMode();
          const r172Mode2 = r172ModeObj2.mode;
          const r172SameSideLoss = r157GetConsecutiveLosses(coin.fullSymbol, recommendation, 3*60*60*1000);
          if (r172Mode2 === 'HIGH_WR_RECOVERY' && r172SameSideLoss >= 1) {
            const r172T = Number(decisionChain?.r160TrueCount || 0);
            const r172Edge = Number(decisionChain?.brainConfidence || 0);
            const r172Pts = Number(decisionChain?.r159Points || 0);
            const r172Q2 = !!decisionChain?.r160Q2Flow, r172Q3 = !!decisionChain?.r160Q3Momentum, r172Q4 = !!decisionChain?.r160Q4Proof;
            const r172Flow = Number(decisionChain?.r125SideFlow?.edge || decisionChain?.r125Flow?.sideEdge || decisionChain?.brainR125FlowEdge || 0);
            const r172Elite = !!(
              (decisionChain?.r159MomentumPass && r172Pts >= 9 && r172Edge >= 92 && score >= 65 && r172Q2 && r172Q3 && r172Q4 && r172Flow >= 8) ||
              (decisionChain?.r160TraderDecision && r172T >= 4 && r172Edge >= 90 && score >= 68 && r172Q2 && r172Q3 && r172Q4 && r172Flow >= 8)
            );
            if (!r172Elite) {
              const why = `R172 aynı yön taze kayıp freni: ${coin.symbol} ${recommendation} son 3s kayıp gördü; sadece elit sinyal geçer`;
              logAuto(`⛔ ${coin.symbol} ${why}`);
              markAutoSkip(coin.symbol, why, {rec:recommendation, score, edge:r172Edge, r159:r172Pts, r160True:r172T, flow:r172Flow});
              continue;
            }
          }
        } catch(_e172loss) {}

        // ── R15 ATR GATE — UB tipi yüksek volatilite bloğu ──────────────────
        const coinAtrPct = analysis.r15?.atrGate?.atrPct || 0;
        if (coinAtrPct > 0 && coinAtrPct > userSLPct * 2.5) {
          const atrBridgeAllowed = !!(decisionChain?.scalperBridge) || (
            ['A','B+'].includes(String(decisionChain?.tier||'')) &&
            Number(decisionChain?.priorityScore||0) >= 68 &&
            Number(score||0) >= Number(effectiveMinScore||0) &&
            !decisionChain?.poorLiquidity && !decisionChain?.rvolVeryLow
          );
          const atrExtreme = coinAtrPct > Math.max(14, userSLPct * 7.0);
          const atrLossGap = coinAtrPct > userSLPct * 3.2 &&
            String(decisionChain?.entryPermissionReason||'').includes('R135_FAST_EDGE_PASS') &&
            !(decisionChain?.r117HtfReverseOk || decisionChain?.r110IctKoprusuOk || decisionChain?.r111KoprusuOk || decisionChain?.r118CandleOk) &&
            Number(decisionChain?.brainConfidence||0) < 96;
          // R162: atrExtreme (>14% veya 7xSL) bypass EDİLMEZ — likidasyon riski var
          // Normal ATR (2.5x-7x SL arası) için r162 bypass bridge açar
          const atrBridgeAllowedFinal = atrBridgeAllowed || (r162BrainBypassActive && !atrExtreme && !atrLossGap);
          if (atrExtreme || atrLossGap || !atrBridgeAllowedFinal) {
            logAuto(`⛔ ${coin.symbol} ATR %${coinAtrPct.toFixed(1)} >> SL %${userSLPct} — volatilite riski yüksek, atlandı`);
            markAutoSkip(coin.symbol, atrLossGap ? `R135 ATR boşluk freni: ATR %${coinAtrPct.toFixed(1)} / SL %${userSLPct} hızlı-edge için fazla geniş` : `ATR %${coinAtrPct.toFixed(1)} > SL %${userSLPct}*2.5 volatilite`, {rec:recommendation, score, tier:decisionChain?.tier, priorityScore:decisionChain?.priorityScore, ...r119BuildAutoDiag(decisionChain)});
            continue;
          }
          logAuto(`⚠️ ${coin.symbol} ATR yüksek (%${coinAtrPct.toFixed(1)}) ama 5m Fırsat Beyni kontrollü girişe izin verdi — user SL/TP korunarak devam`);
        }
        // Likidite kalitesi çok düşükse skip
        if (analysis.r15?.liquidityQuality?.quality === 'POOR') {
          logAuto(`⛔ ${coin.symbol} POOR likidite (spread:%${analysis.r15.liquidityQuality.spread}) — kayma riski, atlandı`);
          markAutoSkip(coin.symbol, `POOR likidite spread:${analysis.r15?.liquidityQuality?.spread}`, {rec:recommendation, score});
          continue;
        }
        const minRR     = parseFloat(cfg.minRR ?? 1.0) || 1.0;

        if (userRR < minRR) {
          logAuto(`${coin.symbol} panel R/R ${userRR.toFixed(2)} < min ${minRR} — atlandı`);
          markAutoSkip(coin.symbol, `Panel RR düşük ${userRR.toFixed(2)}<${minRR}`, {rec:recommendation, score});
          continue;
        }

        // R135: pozisyon sayısı ve aynı yön serbestliği kullanıcı ayarıdır. Aynı yönde açık pozisyonu
        // sadece bilgi olarak not et; toplam maxPositions ve aynı sembol koruması zaten emir endpoint'inde korunur.
        const sameSideAlready = Number(openSideCounts?.[recommendation] || 0);
        if (sameSideAlready > 0) {
          logAuto(`ℹ️ ${coin.symbol} aynı yönde ${sameSideAlready} açık pozisyon var; kullanıcı max pozisyon hakkı korunuyor (${recommendation})`);
        }

        const lossGuard = recentLossPatternGuard(coin.fullSymbol || coin.symbol, recommendation, decisionChain);
        if (lossGuard.block) {
          logAuto(`🧠 ${coin.symbol} öğrenme freni: ${lossGuard.reason}`);
          markAutoSkip(coin.symbol, lossGuard.reason, {rec:recommendation, tier:decisionChain?.tier, score, priorityScore:decisionChain?.priorityScore});
          continue;
        }

        logAuto(`🔥 ${coin.symbol} ${decisionChain?.tier||'A'} PRO! ${decisionChain?.reason||'karar zinciri'} — tek beyin 5m edge gördü; ek katmanlı kapı yok.`);

        // R88: Normal otomatik işlem panel kaldıracını kullanır. Vur-kaç otomatik kaldıraç açık ise
        // Binance izinli maksimum okunur; piyasa güvenliği bozuksa zaten işlem kovalanmaz.
        let executeLeverage = normalizeRequestedLeverage(leverage, 1);
        let leverageNote = `panel kaldıracı ${executeLeverage}x`;
        const r137BaseMaxLev = await getSymbolMaxInitialLeverage(apiKey, apiSecret, coin.fullSymbol, Number(usdtAmount||0) * executeLeverage).catch(()=>null);
        if (r137BaseMaxLev && executeLeverage > r137BaseMaxLev) {
          leverageNote = `panel ${executeLeverage}x → Binance izinli ${r137BaseMaxLev}x`;
          executeLeverage = r137BaseMaxLev;
        }
        if (cfg.vurKacEnabled && cfg.vurKacAutoLev && decisionChain?.r88VurKacOk) {
          const structuralAutoLevOk = !!(
            (!decisionChain?.r116HtfGuardBlock || decisionChain?.r117HtfReverseOk) &&
            (decisionChain?.r110IctKoprusuOk || decisionChain?.r111KoprusuOk || decisionChain?.r117HtfReverseOk ||
             String(decisionChain?.entryPermissionReason||'').includes('R115_HTF') ||
             String(decisionChain?.entryPermissionReason||'').includes('R111_SIKISMA'))
          );
          if (structuralAutoLevOk) {
            // R163: panel/vurKacMaxLev değeri korunur; 20x hard cap kaldırıldı.
            // Binance izinli maksimum yine okunur. Aşırı SL×kaldıraç riski aşağıdaki guard ile sınırlanır.
            const userMaxLev = Math.max(1, parseInt(cfg.vurKacMaxLev || leverage || 20) || 20);
            const bracketMaxLev = await getSymbolMaxInitialLeverage(apiKey, apiSecret, coin.fullSymbol, Number(usdtAmount||0) * userMaxLev);
            if (bracketMaxLev) {
              executeLeverage = Math.max(executeLeverage, Math.min(userMaxLev, bracketMaxLev));
              leverageNote = `R116 temiz HTF/squeeze yapı otomatik kaldıraç ${executeLeverage}x (Binance izin ${bracketMaxLev}x, panel ${leverage}x)`;
            } else {
              leverageNote = `Binance kaldıraç sınırı okunamadı; panel kaldıracı ${executeLeverage}x`;
            }
          } else {
            leverageNote = `R116 legacy köprüde otomatik kaldıraç kapalı; panel kaldıracı ${executeLeverage}x`;
          }
        }

        // R157: SL×Kaldıraç guard — SL yüzdesi × kaldıraç > %25 ise kaldıracı düşür.
        // FOLKS 50x × %2 SL = %100 risk. Max kabul: %25 marjın riski.
        try {
          const r157SlPct = Number(userSLPct || cfg.slPct || 2);
          const r157MaxRisk = 40; // R163: panel 15x/SL%2 korunur; sadece aşırı kaldıraçlı risk düşürülür
          if (r157SlPct * executeLeverage > r157MaxRisk) {
            const oldLev = executeLeverage;
            executeLeverage = Math.max(1, Math.floor(r157MaxRisk / r157SlPct));
            leverageNote += ` · R157 SL×Kaldıraç guard ${oldLev}x→${executeLeverage}x (SL%${r157SlPct}×lev≤40%)`;
            logAuto(`🛡️ ${coin.symbol} SL${r157SlPct}%×${oldLev}x=%${(r157SlPct*oldLev).toFixed(0)} risk → ${executeLeverage}x'e düşürüldü`);
          }
        } catch(_e) {}

        // R150: mikro-cap + ATR yüksek coinlerde işlem sayısını kesmeden ROI hasarını eşitle.
        // 4USDT gibi 0.01 altı/çevresi coinlerde %1-2 fiyat oynama 20x ile -20/-40 ROI yapar.
        // Gerçek HTF reversal/squeeze varsa işlem korunur; sadece hızlı-edge/momentum scalp riskinde kaldıraç düşürülür.
        try {
          const r150MicroCap = Number(entryRef) > 0 && Number(entryRef) < 0.03;
          const r150AtrHot = Number(coinAtrPct||0) >= Math.max(4.5, Number(userSLPct||1.5) * 2.2);
          const r150RealStructure = !!(decisionChain?.r117HtfReverseOk || decisionChain?.r117BodyReclaimOk || decisionChain?.r110IctKoprusuOk || decisionChain?.r111KoprusuOk || decisionChain?.r111SiksmaOk || String(decisionChain?.entryPermissionReason||'').includes('R115_HTF'));
          const r150FastOnly = /momentum|mikro|scalp|hızlı|R134|R135|R144/i.test(String(decisionChain?.reason||'') + ' ' + String(decisionChain?.entryPermissionReason||'') + ' ' + String(decisionChain?.brainMode||''));
          if (r150MicroCap && r150AtrHot && !r150RealStructure && r150FastOnly) {
            const oldLev = executeLeverage;
            const capLev = Number(coinAtrPct||0) >= 10 ? 6 : 8;
            executeLeverage = Math.max(3, Math.min(executeLeverage, capLev));
            if (executeLeverage < oldLev) leverageNote += ` · R150 mikro-cap ATR eşitleme ${oldLev}x→${executeLeverage}x`;
          }
        } catch(_e) {}

        // R151: Kalibrasyon datası yetersiz (yeni) coin koruması.
        // r142 hafıza sıfırdan başlar ve edge 100 verir → işlem doğru açılır ama kaldıraç agresif olabilir.
        // < 3 geçmiş trade olan coin'de kaldıraç panelin %60'ı ile cap'lenir (işlem asla iptal edilmez).
        // 3+ trade varsa veya gerçek HTF yapı varsa bu koruma devre dışı kalır.
        try {
          const r151Mem = r142MemoryStats(recommendation, analysis, decisionChain?.brainMode||'NO_EDGE');
          const r151NewCoin = Number(r151Mem?.same?.n || 0) < 3;
          const r151HasStructure = !!(decisionChain?.r117HtfReverseOk || decisionChain?.r110IctKoprusuOk || decisionChain?.r111KoprusuOk || decisionChain?.r111SiksmaOk);
          if (r151NewCoin && !r151HasStructure) {
            const oldLev = executeLeverage;
            const capLev = Math.max(5, Math.round(executeLeverage * 0.6));
            executeLeverage = Math.min(executeLeverage, capLev);
            if (executeLeverage < oldLev) {
              leverageNote += ` · R151 yeni coin (${Number(r151Mem?.same?.n||0)} geçmiş) kaldıraç ${oldLev}x→${executeLeverage}x`;
              logAuto(`🆕 ${coin.symbol} kalibrasyon datası az (${Number(r151Mem?.same?.n||0)} trade) → kaldıraç ${oldLev}x→${executeLeverage}x (işlem açılıyor)`);
            }
          }
        } catch(_e) {}


        let targetPrice = isLong
          ? +(entryRef * (1 + userTPPct/100)).toFixed(8)
          : +(entryRef * (1 - userTPPct/100)).toFixed(8);
        let stopPrice = isLong
          ? +(entryRef * (1 - userSLPct/100)).toFixed(8)
          : +(entryRef * (1 + userSLPct/100)).toFixed(8);

        // R168: R159 momentum geçişi saf momentumdur; R160 tam trader kanıtı yoksa
        // SL daha kompakt olmalı. JTO R159 6p -13.4% ROI örneğinde büyük zarar burada oluştu.
        try {
          const r168OnlyR159 = !!(decisionChain?.r159MomentumPass && !decisionChain?.r160TraderDecision && !decisionChain?.r156FastBypass);
          const r168Pts = Number(decisionChain?.r159Points || 0);
          if (r168OnlyR159) {
            const oldSL168 = userSLPct;
            const oldTP168 = userTPPct;
            userSLPct = +(Math.min(Number(userSLPct||1.5), r168Pts >= 8 ? 1.15 : 0.95)).toFixed(2);
            userTPPct = +(Math.min(Math.max(Number(userTPPct||3), userSLPct * 2.2), 5.0)).toFixed(2);
            if (oldSL168 !== userSLPct || oldTP168 !== userTPPct) {
              logAuto(`🧠 ${coin.symbol} R168 R159 risk profili: ${r168Pts}p → SL:%${oldSL168}→%${userSLPct} TP:%${oldTP168}→%${userTPPct}`);
            }
          }
        } catch(_e168risk) {}

        // R125: forceOrder likidasyon kümesi aynı yönde yakın hedefse TP'yi kör yüzdeye değil
        // mıknatısa göre ayarla. Çok yakın hedefe düşürmez; çok uzak hedefe de taşırmaz.
        let r125TpNote = '';
        const tpMagnet = isLong ? analysis?.r125OrderFlow?.tpLong : analysis?.r125OrderFlow?.tpShort;
        if (tpMagnet && Math.abs(Number(tpMagnet.distPct||0)) >= Math.max(0.25, userSLPct*0.35) && Math.abs(Number(tpMagnet.distPct||0)) <= userTPPct*1.15) {
          const magnetTarget = isLong ? Number(tpMagnet.price)*0.997 : Number(tpMagnet.price)*1.003;
          if (magnetTarget > 0 && ((isLong && magnetTarget > entryRef) || (!isLong && magnetTarget < entryRef))) {
            targetPrice = +magnetTarget.toFixed(8);
            r125TpNote = ` · R125 TP liq-mıknatıs ${tpMagnet.price} g${tpMagnet.strength}`;
          }
        }

        // R157: Ardışık kayıp fren — aynı coin+yönde son 4 saatte 2+ kayıp → 4 saat bekleme
        const r157ConsecLosses = r157GetConsecutiveLosses(coin.fullSymbol, recommendation);
        if (r157ConsecLosses >= 2) {
          const r157WaitMs = 4 * 60 * 60 * 1000;
          logAuto(`🔁 ${coin.symbol} son 4 saatte ${r157ConsecLosses} kayıp — 4 saat cooldown (FOLKS/BABY likidasyon önleme)`);
          setCooldown(coin.fullSymbol, r157WaitMs, `R157 ardışık ${r157ConsecLosses} kayıp; 4 saat ${recommendation} bekleme`);
          markAutoSkip(coin.symbol, `R157 ardışık kayıp freni (${r157ConsecLosses}x)`, {rec:recommendation, score});
          continue;
        }

        // R166: ATR-ADAPTIVE SL/TP ─────────────────────────────────────────
        // BANK -28.78% analizi: ATR=%2, panel SL=%1.5 → normal gürültü SL'yi deliyor.
        // Çözüm: coin'in kendi ATR'ına göre dinamik SL/TP hesapla.
        // TP = max(panelTP, ATR×2.5) → gerçekçi kâr hedefi
        // SL = max(panelSL, ATR×0.7) → gürültüden korunma (max %3.0 cap)
        // Frekansı bozmaz — sadece mesafeler değişir, sinyal kararı aynı kalır.
        try {
          const r166AtrPct = Number(analysis?.atr?.pct || decisionChain?.coinAtrPct || 0);
          if (r166AtrPct > 0.8) {
            const panelSL = Number(userSLPct || 1.5);
            const panelTP = Number(userTPPct || 3.0);
            const atrSL = +(r166AtrPct * 0.70).toFixed(2);
            const atrTP = +(r166AtrPct * 2.50).toFixed(2);
            if (atrSL > panelSL) {
              const oldSL = userSLPct;
              userSLPct = +Math.min(atrSL, 3.0).toFixed(2);
              userTPPct = +Math.min(Math.max(panelTP, atrTP), 10.0).toFixed(2);
              logAuto(`📐 ${coin.symbol} R166 ATR:%${r166AtrPct.toFixed(2)} → adaptif SL:%${oldSL}→%${userSLPct} TP:→%${userTPPct}`);
            }
          }
        } catch(_e166) {}

        // R167: R166 adaptif SL/TP önce loga yazıyor ama targetPrice/stopPrice daha eski hesapla kalabiliyordu.
        // Emirden hemen önce SL/TP ve SL×kaldıraç guard tekrar hesaplanır.
        try {
          if (userSLPct * executeLeverage > 40) {
            const oldLev2 = executeLeverage;
            executeLeverage = Math.max(1, Math.floor(40 / Math.max(0.1, userSLPct)));
            if (executeLeverage < oldLev2) leverageNote += ` · R167 adaptif SL risk guard ${oldLev2}x→${executeLeverage}x`;
          }
          targetPrice = isLong ? +(entryRef * (1 + userTPPct/100)).toFixed(8) : +(entryRef * (1 - userTPPct/100)).toFixed(8);
          stopPrice   = isLong ? +(entryRef * (1 - userSLPct/100)).toFixed(8) : +(entryRef * (1 + userSLPct/100)).toFixed(8);
          if (tpMagnet && Math.abs(Number(tpMagnet.distPct||0)) >= Math.max(0.25, userSLPct*0.35) && Math.abs(Number(tpMagnet.distPct||0)) <= userTPPct*1.15) {
            const magnetTarget2 = isLong ? Number(tpMagnet.price)*0.997 : Number(tpMagnet.price)*1.003;
            if (magnetTarget2 > 0 && ((isLong && magnetTarget2 > entryRef) || (!isLong && magnetTarget2 < entryRef))) targetPrice = +magnetTarget2.toFixed(8);
          }
          userRR = userTPPct / Math.max(0.05, userSLPct);
        } catch(_e167a) {}

        // R167: hafıza/rejim freni emir-log-Telegram'dan önce. İşlem sıklığını tamamen kapatmaz;
        // sadece aynı coin iki yönde de zarar veriyorsa daha güçlü kanıt ister.
        try {
          const r167RecentLoss = r166CoinRecentLosses(coin.fullSymbol, 2*60*60*1000);
          const r167WR4h = r166CoinRecentWR(coin.fullSymbol, 4*60*60*1000);
          const r167OppLoss = r167OppositeSideLosses(coin.fullSymbol, recommendation, 2*60*60*1000);
          const r167Weak = (r167RecentLoss >= 2 && r167WR4h !== null && r167WR4h < 0.45) || r167OppLoss >= 1;
          if (r167Weak) {
            const minEdgeNeeded = r167OppLoss >= 1 ? 75 : 65;
            const calibEdge = Number(decisionChain?.brainConfidence || 0);
            const fullProof = !!(decisionChain?.r160TraderDecision && Number(decisionChain?.r160TrueCount||0) >= 4);
            if (calibEdge < minEdgeNeeded && !fullProof) {
              logAuto(`🧊 ${coin.symbol} R167 coin tekrar-kayıp freni: loss=${r167RecentLoss}, oppLoss=${r167OppLoss}, coinWR%${r167WR4h!==null?(r167WR4h*100).toFixed(0):'?'} → edge ${calibEdge}<${minEdgeNeeded}, atlandı`);
              markAutoSkip(coin.symbol, `R167 coin rejim freni: zayıf son performans / ters-yön kaybı`, {rec:recommendation, score});
              continue;
            }
          }
        } catch(_e167b) {}


        logAuto(`🎯 Sinyal: ${coin.symbol} ${trSideLabel(recommendation)} skor:${score} — marj:${usdtAmount} USDT ${leverageNote}  zarar-kes:%${userSLPct} kâr-al:%${userTPPct} oran:${userRR.toFixed(2)}${r125TpNote} — emir açılıyor`);
        // R168b: Telegram açılış bildirimi emir gerçekten açıldıktan sonra gönderilir.

        // R166: Coin performans hafızası — son 2 saatte 2+ kayıp varsa ve son 4 saatte WR<%40 ise
        // edge eşiğini %25 artır. İşlem açılmaya devam eder AMA düşük kaliteli sinyaller geçemez.
        try {
          const r166RecentL = r166CoinRecentLosses(coin.fullSymbol, 2*60*60*1000);
          const r166WR4h = r166CoinRecentWR(coin.fullSymbol, 4*60*60*1000);
          const r166WeakCoin = r166RecentL >= 2 && (r166WR4h !== null && r166WR4h < 0.40);
          if (r166WeakCoin) {
            const minEdgeNeeded = 65; // zayıf coin için minimum edge
            const calibEdge = Number(decisionChain?.brainConfidence || 0);
            if (calibEdge < minEdgeNeeded) {
              logAuto(`⚡ ${coin.symbol} R166 coin hafızası: ${r166RecentL} kayıp, coinWR%${r166WR4h!==null?(r166WR4h*100).toFixed(0):'?'} → edge ${calibEdge}<${minEdgeNeeded}, atlandı`);
              markAutoSkip(coin.symbol, `R166 coin hafızası: zayıf WR+${r166RecentL}kayıp, edge ${calibEdge}<${minEdgeNeeded}`, {rec:recommendation, score});
              continue;
            }
          }
        } catch(_e166b) {}

        // R175: R159 momentum geçişi yine de geçerse risk dar tutulur; 5m scalp'te ilk tepki yoksa büyük SL beklenmez.
        try {
          if (decisionChain?.r159MomentumPass && Number(decisionChain?.r159Points||0) <= 8) {
            const oldSL175 = Number(userSLPct||0);
            const oldTP175 = Number(userTPPct||0);
            if (oldSL175 > 0.95) userSLPct = 0.95;
            if (oldTP175 > 0 && userTPPct < 2.2) userTPPct = 2.2;
            targetPrice = isLong ? +(entryRef * (1 + userTPPct/100)).toFixed(8) : +(entryRef * (1 - userTPPct/100)).toFixed(8);
            stopPrice   = isLong ? +(entryRef * (1 - userSLPct/100)).toFixed(8) : +(entryRef * (1 + userSLPct/100)).toFixed(8);
            if (oldSL175 !== userSLPct) logAuto(`🛡 ${coin.symbol} R175 R159 risk daraltma: SL %${oldSL175}→%${userSLPct}, TP %${oldTP175}→%${userTPPct}`);
          }
        } catch(_e175risk) {}

        // İşlemi aç
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
          // R168d: Telegram açılış bildirimi recordTradeOpen event hook'una taşındı; burada duplicate yok.
          // Trailing state başlat + açılış sebebi kaydet (Pos kartında görünür)
          trailingState.set(coin.fullSymbol, {
            side: recommendation,
            entryPrice: orderResp.executedPrice||analysis.price,
            highWater: orderResp.executedPrice||analysis.price,
            breakEvenSet:false, currentSL:orderResp.details?.stop||stopPrice,
            targetTP:orderResp.details?.target||targetPrice,
            leverage:parseInt(executeLeverage)||parseInt(leverage)||1,
            slPct:userSLPct, tpPct:userTPPct,
            sltpVerified: !!orderResp.slSuccess && !!orderResp.tpSuccess,
            openedAt: Date.now(),
                openReason: decisionChain?.reason || '',
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
            recordTradeOpen(coin.fullSymbol, recommendation, orderResp.executedPrice||analysis.price, orderResp.details?.quantity||null, _stOpen);
            const _realQtyForKnown = Math.abs(Number(orderResp.details?.quantity || orderResp.quantity || 0)) || 1;
            rememberOpenPositionForReentry({symbol:coin.fullSymbol, positionAmt: recommendation==='LONG' ? _realQtyForKnown : -_realQtyForKnown, entryPrice:orderResp.executedPrice||analysis.price, leverage:parseInt(executeLeverage)||parseInt(leverage)||1}, _stOpen);
            saveLastKnownPositions();
          } catch(_e) { logAuto(`⚠️ Trade ledger açılış kaydı yazılamadı: ${String(_e.message||_e).slice(0,80)}`); }
          // R135: kullanıcı maxPositions ne verdiyse ona kadar tarama devam edebilir.
          // Fakat SL/TP doğrulanmadıysa yeni emir açma; önce koruma zinciri oturmalı.
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
          // Hata durumunda kısa cooldown — aynı coine 20dk tekrar girme
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

    // Görünürlük düzeltmesi: emir denemesi hata verdikten sonra eski MAX_POZİSYON_DOLU
    // fazı ekranda takılı kalmasın. Binance pozisyonu tekrar okunur; 0/1 ise BEKLİYOR gösterilir.
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
  // R34: Ana tarama 30sn kalır; R125 sadece canlı orderflow spike gelirse güvenli 5sn uyandırma yapar.
  autoTimer = setInterval(runAutoScan, AUTO_SCAN_INTERVAL_MS);
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
          if (Number(ev.score||0) >= 14 && now - Math.max(Number(autoScanState.lastScanStart||0), Number(autoScanState.lastScanEnd||0), r150LastScanBeginTs||0) > R150_MIN_SCAN_GAP_MS) {
            autoScanState.lastAction = `R151 canlı orderflow uyandırdı: ${sym.replace('USDT','')} ${ev.reason}`;
            r125PriorityWake.delete(sym);
            runAutoScan(sym);
            break;
          }
        }
      } catch(_) {}
    }, 5000);
  }
  // R94: aktif vur-kaç çıkış motoru taramayı beklemez; açık pozisyonları 5sn döngüyle izler.
  if (!fastManagerTimer) {
    fastManagerTimer = setInterval(fastManageOpenPositions, 10 * 1000); // R154: 5sn→10sn, TTL_ACTIVE ile sync
  }
  // Her 30 saniyede bir: manuel kapanış algıla + SL/TP eksikse kurtar
  if (!positionSyncTimer) {
    positionSyncTimer = setInterval(syncPositions, 30 * 1000);
  }
  runAutoScan(); // Hemen başlat
  fastManageOpenPositions();
}


// ── KAPANIŞ SEBEBİ SINIFLANDIRMA ────────────────────────────────────────────
// Binance positionRisk'te pozisyon kaybolduğunda eski sürüm bunu genel olarak
// "manuel/SL/TP" yazıyordu. Bu kafa karıştırıyordu. Burada best-effort şekilde
// son userTrades + state'teki SL/TP fiyatlarına göre sebep ayrıştırılır.
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
      // R169: Binance bazen çok yeni kapanışta dar pencereye trade döndürmüyor; 2 saatlik fallback.
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

  // Stop-market fill kayması olabilir; düşük likidite/top-mover coinlerde kayma %0.45'i aşabilir.
  // R41: directional SL/TP sınıflandırması da yapılır; 'manuel' diye yanlış yazmasın.
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
  // R166 FIX: wa.pnl=0 ise (dış kapanış) Binance income endpoint'ten gerçek PnL çek
  // Bu fix: r142MemoryStats, R157 ve tradeLedger'ın doğru çalışmasını sağlar
  let realPnlFromIncome = null;
  if ((!wa.pnl || wa.pnl === 0) && apiKey && apiSecret) {
    try {
      const openTs = Number(state?.openTs || state?.openedAt || 0);
      const incomeStart = openTs > 0 ? openTs - 5000 : Date.now() - 5*60*1000;
      const incomeEnd = Date.now() + 2000;
      const incomeData = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/income', {
        symbol, incomeType:'REALIZED_PNL', startTime:incomeStart, endTime:incomeEnd, limit:20
      });
      if (Array.isArray(incomeData) && incomeData.length > 0) {
        realPnlFromIncome = incomeData.reduce((sum, x) => sum + parseFloat(x.income||0), 0);
      }
      if ((!Number.isFinite(realPnlFromIncome) || Math.abs(realPnlFromIncome) < 0.000001)) {
        const wideIncome = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/income', {
          symbol, incomeType:'REALIZED_PNL', startTime:Math.max(0, Date.now() - 2*60*60*1000), endTime:Date.now()+5000, limit:50
        }).catch(()=>[]);
        if (Array.isArray(wideIncome) && wideIncome.length > 0) {
          // Tek pozisyon mantığında aynı sembolde en son kapanış gelirini al; 0.00 dashboard körlüğünü azaltır.
          realPnlFromIncome = wideIncome.slice(-3).reduce((sum, x) => sum + parseFloat(x.income||0), 0);
        }
      }
    } catch(_e) {}
  }
  // R167/R169: income yoksa/0 geldiyse entry-close-qty ile gerçekçi PnL fallback.
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

// ── POZİSYON SENKRON + SL/TP KURTARMA ───────────────────────────────────────
// Her 30 saniyede bir çalışır.
// 1. trailingState'de kayıtlı ama Binance'te kapanmış → manuel/SL/TP algılandı → cooldown
// 2. Binance'te açık ama SL/TP eksik → paneldeki değerlerle yeniden kurar
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

    // trailingState'de kayıtlı ama Binance'te artık olmayan = kapanmış.
    // Eski log "manuel/SL/TP" diye geneldi; şimdi mümkünse BE/TP/SL ayrıştırılır.
    for (const [sym, state] of trailingState.entries()) {
      if (!openMap.has(sym)) {
        const cls = await classifyClosedPosition(autoConfig.apiKey, autoConfig.apiSecret, sym, state);
        const px = cls.closePrice ? ` fiyat:${cls.closePrice}` : '';
        const pnl = Number.isFinite(cls.realizedPnl) ? ` pnl:${cls.realizedPnl}` : '';

        // R25 FIX 1: Kapanış türüne göre cooldown süresi
        const isLossClose = ['STOP_LOSS','R14_HARD_LOSS_GUARD'].includes(cls.code);
        const isManualClose = cls.code === 'EXTERNAL_OR_MANUAL';
        const isProfitClose = ['TAKE_PROFIT','KAR_TASIMA_SL','BREAK_EVEN_SL'].includes(cls.code);
        logAuto(`${cls.emoji} ${sym.replace('USDT','')} kapandı → ${cls.label}${px}${pnl}`);
        trailingState.delete(sym);

        // R25 FIX 4: İşlem Karnesi (file-based ledger)
        const cdMs = setCloseCooldown(sym, cls, state);
        Object.assign(cls, { cooldownMs: cdMs });
        recordTradeClose(sym, state, cls);
        try { forgetKnownPosition(sym); saveLastKnownPositions(); } catch(_) {}
      }
    }

    // R26: Railway restart / trailingState kaybı → last-known kapanış tespiti
    const closedHandled = new Set([...trailingState.keys()]);
    for (const sym of Object.keys(lastKnownPositions || {})) {
      if (openMap.has(sym) || closedHandled.has(sym)) continue;
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

    // R161 FIX: Açık pozisyonlarda trailingState yoksa restore et (Railway restart sonrası kayıp)
    // Restart'ta in-memory Map sıfırlanıyor; açık pozisyon için BE/trailing mekanizması çalışmıyor
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

    // Açık pozisyonlarda önce 30sn hard-loss guard, sonra SL/TP eksik mi? → kurtarmaya çalış
    for (const [sym, p] of openMap.entries()) {
      try {
        // R14: runAutoScan 3 dakikada bir çalıştığı için ani SL gecikmesini burada 30sn döngüde yakala.
        const amt = parseFloat(p.positionAmt || 0);
        const ep  = parseFloat(p.entryPrice || 0);
        const mp  = parseFloat(p.markPrice || 0);
        const lev = parseInt(p.leverage) || parseInt(autoConfig.leverage) || 1;
        const isLongGuard = amt > 0;
        const stGuard = trailingState.get(sym) || lastKnownPositions?.[sym] || {};
        const slPctGuard = Math.max(0.1, parseFloat(stGuard.slPct || stGuard.entrySLPct || autoConfig.slPct || 2));
        const realMoveGuard = ep > 0 && mp > 0
          ? ((mp - ep) / ep * 100 * (isLongGuard ? 1 : -1))
          : 0;
        const roiGuard = realMoveGuard * lev;
        const hardLossRealGuard = -Math.max(slPctGuard + 0.25, slPctGuard * 1.12);
        const hardLossRoiGuard  = -Math.max((slPctGuard * lev) + 5, 30);
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
            // R30: rescue dalında undefined orderAmt/leverage hatası giderildi.
            // Açılış ledger'ı burada tekrar yazılmaz; sadece last-known pozisyon canlı tutulur.
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

// ── R25: İŞLEM KARNESİ ENDPOINTi ─────────────────────────────────────────
// R166: tradeLedger export endpoint — Railway deploy öncesi geçmişi yedekle
app.get('/api/trade-ledger-export', (_req, res) => {
  try {
    res.setHeader('Content-Disposition', 'attachment; filename="trade_ledger_backup.json"');
    res.setHeader('Content-Type', 'application/json');
    res.send(JSON.stringify(tradeLedger.slice(0,500), null, 2));
  } catch(e) { res.status(500).json({ok:false, error:e.message}); }
});

// R166: tradeLedger import endpoint — yeni deploy sonrası geçmişi geri yükle
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
// Geriye dönük uyum için alias
app.get('/api/trade-history', (_req, res) => {
  res.json({ ok:true, trades:tradeLedger.slice(0,120), count:tradeLedger.length });
});

// ── R166: TAM OTOMATİK BAŞLANGIÇ ────────────────────────────────────────────
// 1. Bot başlar
// 2. Telegram'dan son yedek alınır (ledger boşsa)
// 3. Telegram'a "Bot başladı" mesajı gönderilir
// 4. Her 30 dakikada bir ledger Telegram'a yedeklenir
// ─────────────────────────────────────────────────────────────────────────────

const TG_BACKUP_MSG_PREFIX = '🗄️LAZARUS_LEDGER_BACKUP:';
const TG_BACKUP_INTERVAL_MS = 30 * 60 * 1000; // 30 dakika

// Telegram'a ledger yedekle
async function tgSaveLedgerBackup() {
  if (!TG_TOKEN || !TG_CHAT_ID) return;
  try {
    const data = JSON.stringify(tradeLedger.slice(0, 250));
    // Büyük veriyi Telegram mesajına sığdır (max 4096 char → parçala)
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
      // Büyükse document olarak gönder
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

// Telegram'dan son ledger yedeğini geri yükle
async function tgRestoreLedgerBackup() {
  if (!TG_TOKEN || !TG_CHAT_ID) return false;
  try {
    const res = await fetch(
      `https://api.telegram.org/bot${TG_TOKEN}/getUpdates?limit=100&offset=-100`,
      { signal: AbortSignal.timeout(10000) }
    );
    const data = await res.json();
    if (!data.ok || !Array.isArray(data.result)) return false;
    // En son backup mesajını bul (en yeniden eskiye doğru)
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


// R168b: Telegram test ve durum endpointleri
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


// R168d: Tam işlem kartı testi — gerçek emir açmadan açılış/kapanış kartını Telegram'a yollar.
app.get('/api/telegram-trade-test', async (_req, res) => {
  const sampleOpen = {
    id:'TEST_'+Date.now(), symbol:'JTO', side:'LONG', openedAt:Date.now(),
    entryPrice:0.6397, leverage:9, quantity:31.25, marginUSDT:20,
    sl:0.6301, tp:0.6590, slPct:1.5, tpPct:3.0,
    score:35, tier:'A', entryReason:'R159 momentum geçiş test kartı'
  };
  tgNotifyTradeOpenOnce(sampleOpen, {sltpVerified:true, brainMode:'R159 Momentum Pass'});
  const sampleClose = {...sampleOpen, status:'CLOSED', closedAt:Date.now()+9*60*1000, closePrice:0.6301, pnlUSDT:-2.70, roiPct:-13.4, resultNote:'Test kapanış kartı'};
  tgNotifyTradeCloseOnce(sampleClose, {currentSL:0.6301, targetTP:0.6590, leverage:9, positionAmt:31.25}, {label:'Telegram test kapanışı', closePrice:0.6301});
  res.json({ok:true, sent:'open_close_trade_cards', build:LAZARUS_BUILD});
});


// R170b: Hesap geneli günlük/haftalık/aylık performans durumu — coin WR değildir.
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
  } catch(e) { res.status(500).json({ok:false,error:String(e?.message||e),build:LAZARUS_BUILD}); }
});


// R171: Telegram trade kartlarını manuel zorla gönder + PnL reconcile.
app.get('/api/telegram-flush-trade-alerts', async (_req, res) => {
  try {
    // R176: TG Kart Test sadece Telegram kartını test eder; Binance income/reconcile çağırmaz.
    // Önceki sürüm income 429 yüzünden butonu bozuyordu.
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

app.listen(PORT, async () => {
  console.log(`✅ Server ${PORT}`);

  // Kısa gecikme — diğer init tamamlansın
  await new Promise(r => setTimeout(r, 3000));

  // ── ADIM 1: Ledger boşsa Telegram'dan geri yükle ─────────────────────────
  let restoredCount = 0;
  if (tradeLedger.length === 0 && TG_TOKEN && TG_CHAT_ID) {
    console.log('📥 Ledger boş — Telegram yedekten geri yükleniyor...');
    restoredCount = await tgRestoreLedgerBackup();
    if (restoredCount) {
      console.log(`✅ ${restoredCount} işlem Telegram'dan geri yüklendi`);
    }
  }

  // ── ADIM 2: Telegram başlangıç mesajı ────────────────────────────────────
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

  // ── ADIM 3: 30 dakikada bir ledger yedekle ───────────────────────────────
  if (TG_TOKEN && TG_CHAT_ID) {
    setInterval(tgSaveLedgerBackup, TG_BACKUP_INTERVAL_MS);
    // İlk yedek 5 dakika sonra
    setTimeout(tgSaveLedgerBackup, 5 * 60 * 1000);
  }

  // R171: trade açılış/kapanış Telegram kartı ve 0.00 PnL reconcile bakım döngüsü
  setTimeout(r171MaintenanceTick, 12 * 1000);
  setInterval(r171MaintenanceTick, 20 * 1000);
});
