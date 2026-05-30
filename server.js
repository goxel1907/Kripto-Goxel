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

// ── RATE LIMIT ────────────────────────────────────────────────────────────────
let reqCount=0, reqWindow=Date.now();
// ── ALGO ORDER (yeni coinler için: OPG, PENDLE, HUSDT vb.) ──────────────────
// Signature query string'de, body JSON — Binance algo endpoint zorunluluğu
async function bAlgo(apiKey, apiSecret, params) {
  // Binance SIGNED endpoint kuralı: imza, gönderilen TÜM parametrelerin
  // query string hali üzerinden üretilmelidir. Önceki sürüm sadece
  // timestamp/recvWindow imzalayıp algo parametrelerini JSON body'de yolluyordu;
  // bu da /fapi/v1/algoOrder üzerinde -1022 INVALID_SIGNATURE üretebiliyordu.
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
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); }
  catch(e) { throw new Error(`Algo JSON hatası: ${text.substring(0,100)}`); }
  if (data.code && data.code < 0) {
    if (Number(data.code) === -1021) {
      await syncBinanceTime(true);
    }
    throw new Error(formatBinanceError('/fapi/v1/algoOrder', data));
  }
  return data;
}

// ── ALGO CANCEL — tüm algo emirlerini iptal et (2025-12-09 sonrası zorunlu) ──
async function cancelAlgoOrders(apiKey, apiSecret, symbol) {
  // 1. Normal emirleri iptal et (MARKET vs.)
  try { await bReq(apiKey, apiSecret, 'DELETE', '/fapi/v1/allOpenOrders', {symbol}); } catch(e) {}
  // 2. Algo emirlerini de iptal et (STOP_MARKET, TP artık algo)
  try { await bReq(apiKey, apiSecret, 'DELETE', '/fapi/v1/algoOpenOrders', {symbol}); } catch(e) {}
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

async function liveOpenBracketOrders(apiKey, apiSecret, symbol) {
  const [algo, standard] = await Promise.all([
    liveOpenAlgoOrders(apiKey, apiSecret, symbol),
    liveOpenStandardOrders(apiKey, apiSecret, symbol),
  ]);
  return [...algo, ...standard];
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
    const orders = await liveOpenBracketOrders(apiKey, apiSecret, symbol);
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
async function normalizeSLTPToTick(symbol, slPrice, tpPrice) {
  const filters = await getSymbolFilters(symbol);
  const tickSize = Number(filters.tickSize) || 0.00000001;
  const sl = formatStepValue(slPrice, tickSize);
  const tp = formatStepValue(tpPrice, tickSize);
  return { sl, tp, slNum: Number(sl), tpNum: Number(tp), tickSize };
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
      // İlk denemede geçici API/propagation olabilir; ikinci denemeye izin ver.
      if (attempt === 1) await new Promise(r => setTimeout(r, 700));
    }
  }

  const msg = lastErr?.message || 'Algo SL/TP proof başarısız';
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
  if(reqCount>800){const w=60000-(now-reqWindow);await new Promise(r=>setTimeout(r,w+1000));reqCount=0;reqWindow=Date.now();}
  await new Promise(r=>setTimeout(r,80));
  const url=`${FAPI}${path}${qs?'?'+qs:''}`;
  const r=await fetch(url,{signal:AbortSignal.timeout(10000)});
  if(r.status===429||r.status===418){const retry=parseInt(r.headers.get('Retry-After')||'30');await new Promise(r=>setTimeout(r,retry*1000));return bPub(path,qs);}
  const text=await r.text();
  try{return JSON.parse(text);}catch(e){throw new Error(`JSON hatası: ${text.substring(0,80)}`);}
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
  if (!lastTimeSync) await syncBinanceTime(false);
  const ts = Date.now() + binanceTimeOffset;
  const obj = { ...params, timestamp: ts, recvWindow: 10000 };
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
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); }
  catch(e) { throw new Error(`JSON hatası: ${text.substring(0,120)}`); }
  if (data.code && data.code < 0) {
    if (Number(data.code) === -1021 && !_retry) {
      await syncBinanceTime(true);
      return bReq(apiKey,apiSecret,method,path,params,timeout,true);
    }
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
  rateLimitUntil: 0,
  lastApiKey: null,
  lastSource: null,
  lastError: null,
};
const POS_RISK_TTL_NORMAL = 20000;   // 20sn (pozisyon yok)
const POS_RISK_TTL_ACTIVE =  8000;   //  8sn (pozisyon açık)
const POS_RISK_RATELIMIT_MS = 60000; // -1003 sonrası 60sn dur

function keyFingerprint(apiKey) {
  const k = String(apiKey || '').trim();
  return k ? `${k.slice(0,6)}:${k.length}` : 'no-key';
}
function isPositionRiskRateLimitError(err) {
  const m = String(err && (err.message || err) || '');
  return m.includes('-1003') || /too many requests/i.test(m);
}
function isPositionRiskCooldownActive() {
  return Date.now() < Number(posRiskCache.rateLimitUntil || 0);
}
function getPositionRiskCooldownMs() {
  return Math.max(0, Number(posRiskCache.rateLimitUntil || 0) - Date.now());
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
  const now = Date.now();
  const apiFp = keyFingerprint(apiKey);

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
      if (msg.includes('-1003') || msg.includes('Too many requests')) {
        posRiskCache.rateLimitUntil = Date.now() + POS_RISK_RATELIMIT_MS;
        pushCritical('POSITION_RISK_RATELIMIT', e, {}, 'WARNING');
        console.log('⛔ positionRisk rate-limit: 60sn bekleniyor');
      }
      if (posRiskCache.data && posRiskCache.lastApiKey === apiFp) return posRiskCache.data;
      throw e;
    } finally {
      posRiskCache.fetching = false;
      posRiskCache.inflight = null;
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
  get inflight() { return posRiskCache.fetching; },
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
    trend = delta > 0 ? 'POSITIVE' : 'NEGATIVE';
    if (recent > older * 1.3)      acceleration = 'ACCELERATING_BULL';
    else if (recent < older * 0.7) acceleration = 'ACCELERATING_BEAR';
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
    // /api/analyze içinde aşağıdaki skor/response blokları liqData kullanıyor.
    // Önceki build'de bu değişken tanımlanmadığı için tüm analizler ReferenceError ile ERR oluyordu.
    const liqData = getLiqData(full);

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
    const atrPct=lastPrice>0?(atr1h/lastPrice)*100:1;

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
    if(longAdverse.length>=3) longRiskCap=Math.min(longRiskCap,55);
    if(longAdverse.length>=4) longRiskCap=Math.min(longRiskCap,50);
    if(shortAdverse.length>=3) shortRiskCap=Math.min(shortRiskCap,55);
    if(shortAdverse.length>=4) shortRiskCap=Math.min(shortRiskCap,50);
    longScore=Math.min(longScore,longRiskCap);
    shortScore=Math.min(shortScore,shortRiskCap);

    longScore =Math.min(Math.round(longScore*freshnessMult),100);
    shortScore=Math.min(Math.round(shortScore*freshnessMult),100);
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

        // KURAL 2: Delta (CVD) ters değil.
        // R9'da CVD/Tick ikisi de UNKNOWN ise A-Tier engelleniyordu. Bu güvenliydi,
        // ama Railway restart/WS ısınması sonrası 5m kripto fırsatlarını saatlerce kaçırdı.
        // R10: CVD yoksa sadece ÇOK GÜÇLÜ köprü şartlarıyla geçiş verilir.
        const cvdRatio=cvdD?.ratio||50;
        const cvdValid=!!(cvdD?.valid && ((cvdD.buy||0)+(cvdD.sell||0)>0));
        const deltaTrend=String(tickData?.deltaTrend||'UNKNOWN').toUpperCase();
        const tickDeltaKnown=!!(deltaTrend && deltaTrend!=='UNKNOWN');
        const tickDeltaOk=tickDeltaKnown ? (isL?deltaTrend==='BULL':deltaTrend==='BEAR') : false;
        const cvdSideOk=cvdValid?(isL?cvdRatio>40:cvdRatio<60):false;
        const deltaOkStrict=cvdSideOk||(tickDeltaKnown&&tickDeltaOk);

        const hardSweepForBridge=
          (sw1?.confirmed&&(isL?sw1.direction==='BULL_SWEEP':sw1.direction==='BEAR_SWEEP'))||
          (sweep15m?.confirmed&&(isL?sweep15m.direction==='BULL_SWEEP':sweep15m.direction==='BEAR_SWEEP'))||
          (sweep4h?.confirmed&&(isL?sweep4h.direction==='BULL_SWEEP':sweep4h.direction==='BEAR_SWEEP'))||
          (tickData?.tickSweep?.type===(isL?'BULL_SWEEP':'BEAR_SWEEP')&&tickData.tickSweep.fresh)||
          (amd5m?.signal===(isL?'AMD_LONG':'AMD_SHORT')&&amd5m?.tickConfirm);
        const mtfBridgeOk=isL
          ? (mtfBias?.bias==='STRONG_BULL'||mtfBias?.bullPct>=70)
          : (mtfBias?.bias==='STRONG_BEAR'||mtfBias?.bullPct<=30);
        const fundBridgeOk=isL
          ? (fundSig==='NEGATIVE'||fundSig==='EXTREME_NEGATIVE')
          : (fundSig==='POSITIVE'||fundSig==='EXTREME_POSITIVE');
        const oiBridgeOk=isL
          ? (oiDiv==='SHORT_SQUEEZE'||oiDiv==='CONFIRMED_BULL')
          : (oiDiv==='LONG_LIQUIDATION'||oiDiv==='CONFIRMED_BEAR');
        const huntBridgeOk=isL
          ? ((hunt1h?.hunted&&hunt1h.direction==='BULL_HUNT')||(hunt15m?.hunted&&hunt15m.direction==='BULL_HUNT'))
          : ((hunt1h?.hunted&&hunt1h.direction==='BEAR_HUNT')||(hunt15m?.hunted&&hunt15m.direction==='BEAR_HUNT'));
        const bridgeCount=[mtfBridgeOk,fundBridgeOk,oiBridgeOk,huntBridgeOk].filter(Boolean).length;
        const cvdWarmingBridge=!cvdValid && !tickDeltaKnown && hardSweepForBridge && sc>=72 && bridgeCount>=2;
        const deltaOk=deltaOkStrict||cvdWarmingBridge;

        // R19: CVD reset/ısınma sırasında bot saatlerce kilitlenmesin.
        // 4/4 köprü direkt geçer. 3/4 köprü ise skor >=72, RVOL aşırı ölü değil ve ATR gate yoksa A-Tier olabilir.
        const rvolVeryLow = !!(rvol1h && (rvol1h.signal === 'VERY_LOW' || Number(rvol1h.rvol || 0) < 0.15));
        const _slPctEval = parseFloat(autoConfig?.slPct || 2);
        const atrBlocking = atrPct > _slPctEval * 2.5;
        const cvdBridgeQualityOk = !cvdWarmingBridge || bridgeCount >= 4
          || (bridgeCount >= 3 && sc >= 72 && !rvolVeryLow && !atrBlocking);

        // KURAL 3: Funding zehirli değil
        const fundOk=isL?fundSig!=='EXTREME_POSITIVE':fundSig!=='EXTREME_NEGATIVE';

        // KURAL 4: RSI 4h aşırı alım/satım → A-Tier engelle
        const rsiOk = isL ? rsi4h < 78 : rsi4h > 22;

        // KURAL 5: MM kesin ters yönde → A-Tier engelle
        const mmOk = isL
          ? !(mmTarget==='GENUINE_DOWN' && mmConf>=60)
          : !(mmTarget==='GENUINE_UP' && mmConf>=60);

        // A-Tier: net sinyal + delta ok + funding ok + RSI ok + MM ok + skor≥68
        // R14: CVD bridge ile geçiyorsa bridge kalitesi de zorunlu.
        const isTierA=hasEntry&&deltaOk&&fundOk&&rsiOk&&mmOk&&cvdBridgeQualityOk&&sc>=68;

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
        if(cvdValid&&deltaOk) reasons.push(`📊 CVD${cvdRatio.toFixed(0)}%`);
        else if(cvdWarmingBridge) reasons.push(`🟡 CVD ısınıyor: güçlü köprü ${bridgeCount}/4`);
        else if(!cvdValid) reasons.push('📊 CVD ısınma/veri yok');
        if(liqD?.cascade) reasons.push(`💥 ${liqD.cascade.signal}`);
        if(softEntry&&!hasEntry) reasons.push('👁 Yumuşak sinyal');
        if(!hasEntry&&!softEntry) blocks.push('Sinyal yok');
        if(!deltaOk) blocks.push(cvdValid?`Delta ters(${cvdRatio.toFixed(0)}%)`:'CVD+Tick teyidi yok');
        if(cvdWarmingBridge && !cvdBridgeQualityOk) blocks.push(`CVD köprüsü zayıf (${bridgeCount}/4, skor ${sc}${rvolVeryLow?', RVOL çok düşük':''}${atrBlocking?', ATR gate':''})`);
        if(!fundOk)  blocks.push('Fund zehirli');
        if(!rsiOk)   blocks.push(`RSI4h ${rsi4h} ${isL?'aşırı alım':'aşırı satım'}`);
        if(!mmOk)    blocks.push(`MM kesin ters ${mmTarget}(%${mmConf})`);
        if(sc<55)    blocks.push(`Skor düşük(${sc})`);

        const tier=isTierA?'A':isTierB?'B':'WAIT';
        const passCount=[hasEntry||softEntry,deltaOk,fundOk,rsiOk,mmOk,sc>=55].filter(Boolean).length;
        return{ pass:tier!=='WAIT', tier, score:sc, passCount,
          reasons, blocks, autoOk:isTierA,
          cvdWarmingBridge, bridgeCount, cvdBridgeQualityOk, rvolVeryLow, atrBlocking,
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
      scoreGuards:{
        cvdValid:!!(cvdForCheck.valid && ((cvdForCheck.buy||0)+(cvdForCheck.sell||0)>0)),
        longRiskCap, shortRiskCap, longAdverse, shortAdverse,
        rsi4h, mmTarget, mmConf, oiDiv
      },
      smartMoney:{topLongPct:+topLong.toFixed(1),globalLongPct:+globalLong.toFixed(1),divergence:smDiv},
      orderBook:{imbalance:+bookImb.toFixed(1)},
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
      longScore, shortScore, recommendation, decisionChain,
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
      const pos=await getPositionRisk(apiKey,apiSecret,{symbol:sym});
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

    // Lazarus install_live_sltp_pair_with_proof: yaz + kanıtla + fallback
    const slResult = await installSLTPWithProof(
      apiKey, apiSecret, sym, cSide, finalSL, finalTP, sym
    );

    // ÇALIŞAN PYTHON ÇEKİRDEĞİ İLE AYNI GÜVENLİK:
    // SL/TP Binance üzerinde doğrulanmadan pozisyonu 'başarılı' sayma.
    // Korumasız pozisyon kalırsa acil reduce-only market kapat.
    if (!slResult.ok) {
      let emergencyClose = null;
      try {
        await cancelAlgoOrders(apiKey, apiSecret, sym);
        emergencyClose = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
          symbol:sym, side:cSide, type:'MARKET', quantity:qty, reduceOnly:'true', positionSide:'BOTH'
        });
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
      details:{symbol:sym,side,quantity:qty,leverage,entry:execPrice,target:finalTP,stop:finalSL}
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
      const amt=parseFloat(p.positionAmt),ep=parseFloat(p.entryPrice),mp=parseFloat(p.markPrice),lev=parseInt(p.leverage)||1;
      const side=amt>0?'LONG':'SHORT';
      const pct=ep>0?((mp-ep)/ep*100*lev*(side==='SHORT'?-1:1)).toFixed(2):'0';
      const full=normalizeSymbol(p.symbol);
      const state=trailingState.get(full)||trailingState.get(String(p.symbol||''))||{};
      // SL/TP bracket kontrolü — Binance'teki gerçek emir durumu
      let brackets={hasSL:false,hasTP:false,sl:null,tp:null,orderCount:0};
      try{
        const orders=await liveOpenBracketOrders(apiKey,apiSecret,full);
        brackets.orderCount=orders.length;
        for(const o of orders){
          const kind=orderKind(o);
          const trig=orderTriggerPrice(o);
          if(kind==='SL'){brackets.hasSL=true;if(!brackets.sl||Math.abs(trig-mp)<Math.abs(brackets.sl-mp))brackets.sl=trig;}
          if(kind==='TP'){brackets.hasTP=true;if(!brackets.tp||Math.abs(trig-mp)<Math.abs(brackets.tp-mp))brackets.tp=trig;}
        }
      }catch(e){brackets.error=e.message;}
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
        managerStatus:state.managerStatus||positionManagerSnapshot({symbol:full,pnlPct:parseFloat(pct)},state,'İZLEME'),
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
const COOLDOWN_CLOSE_MS   = 30 * 60 * 1000; // Manuel/SL/TP kapanış: 30dk
const COOLDOWN_ERR_MS     = 20 * 60 * 1000; // Emir hatası: 20dk
// NOT: POSOPEN cooldown kaldırıldı — açık pozisyon koruması alreadyOpen kontrolüyle yapılır

function normalizeSymbol(s) {
  const str = String(s || '').toUpperCase().trim();
  return str.endsWith('USDT') ? str : str + 'USDT';
}
function isOnCooldown(symbol) {
  const sym = normalizeSymbol(symbol);
  const exp = cooldownMap.get(sym);
  if (!exp) return false;
  if (Date.now() > exp) { cooldownMap.delete(sym); return false; }
  return true;
}
function setCooldown(symbol, ms, reason) {
  const sym = normalizeSymbol(symbol);
  const exp = Date.now() + ms;
  cooldownMap.set(sym, exp);
  logAuto(`⏳ ${sym.replace('USDT','')} cooldown ${Math.round(ms/60000)}dk: ${reason}`);
}
function getCooldownRemainMs(symbol) {
  const sym = normalizeSymbol(symbol);
  const exp = cooldownMap.get(sym);
  if (!exp) return 0;
  const rem = exp - Date.now();
  if (rem <= 0) { cooldownMap.delete(sym); return 0; }
  return rem;
}
function getCooldownList() {
  const list = [];
  const now = Date.now();
  for (const [sym, exp] of cooldownMap.entries()) {
    const rem = exp - now;
    if (rem > 0) {
      list.push({ symbol: sym.replace('USDT',''), remainMs: rem, remainMin: Math.ceil(rem/60000) });
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
    return { ok:false, proof, safeSL, tpPrice };
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
  // BE emri sadece entry'ye değil, taker fee + olası stop-market kayması için
  // küçük kâr bölgesine taşınır. INJ örneğinde entry üstü kapanışa rağmen
  // komisyon/slippage nedeniyle eksi yazmıştı. Varsayılan %0.22 coin hareketi.
  const beFeeSafePct = Math.max(0.12, safe(cfg.beLockPct ?? cfg.breakEvenLockPct ?? cfg.beProfitLockPct, 0.22));

  let action = null; // { type, reason, urgency }

  // ── 0. R14 ACİL ZARAR KORUMASI — SL çalışmaz/gecikirse kaçış ─────────────
  // Normalde Binance algo SL kapatmalı. Ama yeni sembol / aşırı hızlı fitil / proof gecikmesi
  // durumunda pozisyon SL seviyesinin ötesine sarkarsa 3 dakikalık taramayı beklemeden kapatılır.
  const hardLossReal = -Math.max(slPct + 0.25, slPct * 1.12); // SL %2 ise yaklaşık -%2.25 coin hareketi
  const hardLossRoi  = -Math.max((slPct * leverage) + 5, 30); // 15x %2 SL için yaklaşık -%35 ROI
  if (realProfitPct <= hardLossReal || pnlPct <= hardLossRoi) {
    action = {
      type:'EMERGENCY_EXIT', urgency:'CRITICAL',
      reason:`R14 hard-loss guard: fiyat hareketi %${realProfitPct.toFixed(2)}, ROI %${pnlPct.toFixed(1)}; SL gecikmesi/slippage riski`
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
      stepSL = isLong ? +(entryPrice*(1+0.003)).toFixed(8) : +(entryPrice*(1-0.003)).toFixed(8);
      stepReason = `Kâr taşıma 1: %${realProfitPct.toFixed(2)} → SL giriş üstü +%0.3`;
      stepUpdate = { step1Set:true };
    }
    if (stepSL) {
      const better = isLong ? (!state.currentSL||stepSL>state.currentSL) : (!state.currentSL||stepSL<state.currentSL);
      if (better) action = { type:'KAR_TASIMA', reason:stepReason, urgency:'LOW', newSL:stepSL, stateUpdates:stepUpdate };
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
      const newSL = isLong
        ? +(newHW*(1-trailPct/100)).toFixed(8)
        : +(newHW*(1+trailPct/100)).toFixed(8);
      const better = isLong
        ? (!state.currentSL||newSL>state.currentSL)
        : (!state.currentSL||newSL<state.currentSL);
      if (better) action = {
        type:'TRAIL_SL',
        reason:`Trailing (%${trailStep} adım): HW=$${newHW.toFixed(4)} → SL=$${newSL}`,
        urgency:'LOW', newSL,
        stateUpdates:{ highWater:newHW }
      };
    }
  }

  // ── 6. TP GENİŞLETME — Momentum devam ediyorsa TP'yi yukarı taşı ────────
  // TP genişletme: kaldıraçsız fiyat hedefin %70'ini geçti mi?
  const tpRealTarget = tpPct; // Kaldıraçsız hedef %
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
    // Hem normal hem algo emirleri iptal et (2025-12-09 sonrası)
    try {
      await cancelAlgoOrders(apiKey, apiSecret, sym);
      const qty = pos.positionAmt.toString();
      // MARKET emri eski endpoint'te çalışmaya devam eder
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

  if (action.type === 'BREAK_EVEN' || action.type === 'TRAIL_SL' || action.type === 'TIGHTEN_SL'
      || action.type === 'KAR_TASIMA') {
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

// ── KAPAT ─────────────────────────────────────────────────────────────────────
app.post('/api/close', async (req, res) => {
  const{apiKey,apiSecret,symbol}=req.body;
  if(!apiKey||!apiSecret||!symbol)return res.status(400).json({error:'Eksik parametre'});
  const sym=symbol.toUpperCase().includes('USDT')?symbol.toUpperCase():symbol.toUpperCase()+'USDT';
  try{
    try{await cancelAlgoOrders(apiKey,apiSecret,sym);}catch(e){}
    const pos=await getPositionRisk(apiKey,apiSecret,{symbol:sym});
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

// ── CANLI TARAMA TELEMETRİSİ ────────────────────────────────────────────────
// Ek Binance çağrısı yapmaz; runAutoScan içinde zaten alınan analizleri hafızada tutar.
// Amaç: Dashboard'da "bot ne tarıyor, neyi bekliyor, neden işlem açmadı" sorusunu göstermek.
let autoScanState = {
  enabled:false, running:false, phase:'KAPALI',
  lastScanStart:null, lastScanEnd:null, nextScanDue:null, currentSymbol:null,
  scanList:[], checked:0, opened:0, skipped:0, livePositions:0, maxPositions:0,
  effectiveMinScore:0, killZone:null, settings:{},
  topCandidates:[], skipReasons:{}, lastAction:'Henüz tarama yok'
};
function resetAutoScanState(patch={}) {
  autoScanState = {
    ...autoScanState, ...patch,
    topCandidates: patch.topCandidates || [],
    skipReasons: patch.skipReasons || {},
  };
}
function pushAutoCandidate(row) {
  const r = {
    ts:Date.now(),
    symbol:String(row.symbol||'').replace('USDT',''),
    rec:row.rec||'WAIT', tier:row.tier||'WAIT', score:Number(row.score||0),
    longScore:Number(row.longScore||0), shortScore:Number(row.shortScore||0),
    reason:String(row.reason||row.block||'—').slice(0,140),
    action:String(row.action||'İzle').slice(0,80)
  };
  autoScanState.topCandidates.push(r);
  autoScanState.topCandidates = autoScanState.topCandidates
    .sort((a,b)=>(b.tier==='A')-(a.tier==='A') || b.score-a.score || b.ts-a.ts)
    .slice(0,12);
}
function markAutoSkip(symbol, reason, row={}) {
  const key = String(reason||'Bilinmeyen').slice(0,60);
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
    serverTime: Date.now(),
  });
});
app.post('/api/diagnostics/clear', (_req, res) => {
  criticalEvents.length = 0;
  res.json({ ok:true, message:'Kritik hata ekran kayıtları temizlendi' });
});

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
    config:autoConfig, scanState:autoScanState, recentLogs:autoLog.slice(-40),
    cooldowns: getCooldownList() });
});

function logAuto(msg) {
  const entry = `${new Date().toLocaleTimeString('tr-TR')} ${msg}`;
  autoLog.push(entry);
  if (autoLog.length > 80) autoLog.shift();
  autoScanState.lastAction = entry;
  console.log('[AUTO]', entry);
}

function stopAutoTrader(silent=false) {
  if (autoTimer) { clearInterval(autoTimer); autoTimer=null; }
  if (positionSyncTimer) { clearInterval(positionSyncTimer); positionSyncTimer=null; }
  autoRunning = false;
  if (!silent) {
    resetAutoScanState({enabled:false, running:false, phase:'KAPALI', currentSymbol:null, nextScanDue:null});
    logAuto('Otomatik işlem durduruldu');
  } else {
    resetAutoScanState({running:false, currentSymbol:null});
  }
}

async function runAutoScan() {
  if (autoRunning || !autoConfig?.enabled) return;
  autoRunning = true;
  resetAutoScanState({
    enabled:true, running:true, phase:'BAŞLADI', lastScanStart:Date.now(), lastScanEnd:null,
    currentSymbol:null, scanList:[], checked:0, opened:0, skipped:0, livePositions:0,
    topCandidates:[], skipReasons:{}, lastAction:'Tarama başlıyor'
  });

  try {
    const cfg = autoConfig;
    const { apiKey, apiSecret, usdtAmount, leverage, marginType,
      maxPositions=3, minScore=70, allowLong=true, allowShort=true,
      sweepOnly=true,
      trailingPct=2, trailStep=0.5, breakEvenPct=1, symbols=[] } = cfg;

    autoScanState.settings = {usdtAmount, leverage, marginType, maxPositions, minScore, allowLong, allowShort, sweepOnly, trailingPct, trailStep, breakEvenPct, slPct:cfg.slPct, tpPct:cfg.tpPct, minRR:cfg.minRR};
    autoScanState.maxPositions = Number(maxPositions||0);
    autoScanState.phase = 'POZİSYON_KONTROL';

    // 1. Mevcut pozisyonları kontrol et
    const posData = await getPositionRiskCached(apiKey,apiSecret);
    const openPos = Array.isArray(posData)
      ? posData.filter(p=>Math.abs(parseFloat(p.positionAmt))>0)
      : [];
    autoScanState.livePositions = openPos.length;

    // R18: positionRisk rate-limit aktifken yeni emir açma. Cache ile panel/pozisyon
    // görünür kalır ama taze pozisyon doğrulaması gelene kadar giriş güvenli değildir.
    if (isPositionRiskCooldownActive()) {
      autoScanState.phase = 'RATE_LIMIT_COOLDOWN';
      autoScanState.lastAction = `positionRisk REST cooldown ${Math.ceil(getPositionRiskCooldownMs()/1000)}sn — yeni emir bekletiliyor`;
      logAuto(`⏳ RATE_LIMIT_COOLDOWN: positionRisk ${Math.ceil(getPositionRiskCooldownMs()/1000)}sn dinleniyor, yeni emir açılmayacak`);
      return;
    }

    // Trailing SL kontrol
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

    // 3. Volatil coinleri kullan — en çok hareket edenler önce
    let scanList;
    if (symbols.length > 0) {
      // Kullanıcı belirlemiş
      const coinsResp = await fetch(`http://localhost:${PORT}/api/futures-coins`).then(r=>r.json());
      scanList = (coinsResp.coins||[]).filter(c=>symbols.includes(c.symbol));
    } else {
      // Volatilite scanner'dan al — en aktif coinler
      if (volatilityStore.coins.length > 0) {
        scanList = volatilityStore.coins.slice(0, 20);
        logAuto(`🔥 Volatil top ${scanList.length}: ${scanList.slice(0,5).map(c=>c.symbol).join(', ')}...`);
      } else {
        const coinsResp = await fetch(`http://localhost:${PORT}/api/futures-coins`).then(r=>r.json());
        scanList = (coinsResp.coins||[]).sort((a,b)=>b.volume-a.volume).slice(0,20);
      }
    }
    if (!scanList?.length) return;

    // Kill zone bazlı min skor artırma kaldırıldı.
    const effectiveMinScore = minScore;
    autoScanState.killZone = null;
    autoScanState.effectiveMinScore = effectiveMinScore;

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

    autoScanState.phase = 'TARIYOR';
    autoScanState.scanList = (scanList||[]).map(c=>String(c.symbol||c.fullSymbol||'').replace('USDT','')).slice(0,30);
    logAuto(`Tarama başladı: ${scanList.length} coin, max poz:${maxPositions}, mevcut:${openPos.length}`);

    // 3. Her coini analiz et
    for (const coin of scanList) {
      if ((await getNewPosCount()) >= maxPositions) { autoScanState.phase='MAX_POZİSYON_DOLU'; break; }
      autoScanState.currentSymbol = String(coin.symbol||coin.fullSymbol||'').replace('USDT','');
      autoScanState.checked = (autoScanState.checked||0) + 1;

      // Zaten pozisyon var mı?
      const alreadyOpen = openPos.some(p=>p.symbol===coin.fullSymbol);
      if (alreadyOpen) { markAutoSkip(coin.symbol, 'Zaten açık pozisyon var'); continue; }

      // Cooldown kontrolü — aynı coine tekrar girmesini önler
      const fullSymCheck = normalizeSymbol(coin.fullSymbol || coin.symbol);
      if (isOnCooldown(fullSymCheck)) {
        const remMin = Math.ceil(getCooldownRemainMs(fullSymCheck)/60000);
        markAutoSkip(coin.symbol, `Cooldown ${remMin}dk kaldı`, {rec:'CD', tier:'CD', reason:`Cooldown ${remMin}dk`});
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

        const { longScore, shortScore, recommendation, isExpired, freshness } = analysis;
        const decisionChain = analysis.decisionChain || {};
        if (isExpired || freshness === 'EXPIRED') { markAutoSkip(coin.symbol, 'Sinyal süresi geçmiş'); continue; }

        // ── PRO TRADER KARAR ZİNCİRİ — A-Tier ana karar, toksik filtreler veto ──
        // Skor kafadan üretilmez: /api/analyze içindeki MM + CVD + OI + Funding + Tick + Sweep + Wyckoff katmanlarından gelir.
        // Otomatik işlemde tekrar tekrar aynı şeyi sert kapı yapıp botu boğma; A-Tier karar zaten hasEntry + delta + funding + skor kontrolü yapar.

        const score = recommendation==='LONG'?longScore:shortScore;
        const isLong = recommendation==='LONG';
        const isShort = recommendation==='SHORT';
        pushAutoCandidate({symbol:coin.symbol, rec:recommendation, tier:decisionChain?.tier||'WAIT', score, longScore, shortScore, reason:decisionChain?.reason, action:decisionChain?.autoOk?'Aday':'İzle'});

        // Yön izni
        if (isLong  && !allowLong)  { markAutoSkip(coin.symbol, 'Long kapalı', {rec:recommendation, score}); continue; }
        if (isShort && !allowShort) { markAutoSkip(coin.symbol, 'Short kapalı', {rec:recommendation, score}); continue; }
        if (recommendation==='WAIT') { markAutoSkip(coin.symbol, 'WAIT karar', {rec:recommendation, longScore, shortScore, reason:decisionChain?.reason}); continue; }

        // Sadece A-Tier otomatik açılır. B-Tier panelde görünür ama otomatik emir açmaz.
        const tierOk = decisionChain?.autoOk === true && decisionChain?.tier === 'A';
        if (!tierOk) {
          const why = `B/WAIT-Tier: ${decisionChain?.reason||'A-Tier değil'}`;
          logAuto(`📊 ${coin.symbol} ${why} — otomatik açılmıyor`);
          markAutoSkip(coin.symbol, why, {rec:recommendation, tier:decisionChain?.tier, score, longScore, shortScore, reason:decisionChain?.reason});
          continue;
        }
        // R14 savunma katmanı: CVD yokken zayıf bridge ile otomatik işlem açma.
        if (decisionChain?.cvdWarmingBridge && !decisionChain?.cvdBridgeQualityOk) {
          const why = `CVD köprüsü zayıf (${decisionChain?.bridgeCount||0}/4, skor ${score}) — otomatik açılmıyor`;
          logAuto(`📊 ${coin.symbol} ${why}`);
          markAutoSkip(coin.symbol, why, {rec:recommendation, tier:decisionChain?.tier, score, longScore, shortScore, reason:decisionChain?.reason});
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

        // Skor filtresi
        if (score < effectiveMinScore) { logAuto(`${coin.symbol} skor ${score} < ${effectiveMinScore} — atlandı`); markAutoSkip(coin.symbol, `Skor düşük ${score}<${effectiveMinScore}`, {rec:recommendation, tier:decisionChain?.tier, score, longScore, shortScore, reason:decisionChain?.reason}); continue; }

        // F&G: Extreme durumlarda ters yön yasak
        if (fgSignal==='EXTREME_GREED' && isLong)  { logAuto(`${coin.symbol} Extreme Greed — long atlandı`); markAutoSkip(coin.symbol, 'Extreme Greed long veto', {rec:recommendation, score}); continue; }
        if (fgSignal==='EXTREME_FEAR'  && isShort) { logAuto(`${coin.symbol} Extreme Fear — short atlandı`); markAutoSkip(coin.symbol, 'Extreme Fear short veto', {rec:recommendation, score}); continue; }

        // Likidasyon cascade: sadece pozisyon yönüne direkt ters kaskad veto eder.
        const liq = analysis.liquidations;
        if (liq?.cascade) {
          const adverseCascade = isLong
            ? liq.cascade.direction==='LONG_CASCADE'
            : liq.cascade.direction==='SHORT_CASCADE';
          if (adverseCascade) { logAuto(`${coin.symbol} adverse cascade (${liq.cascade.direction}) — atlandı`); markAutoSkip(coin.symbol, `Adverse cascade ${liq.cascade.direction}`, {rec:recommendation, score}); continue; }
        }

        // ── KULLANICI RİSK AYARLARI ───────────────────────────────────────────────
        // Otomatik emir gerçek panel değerlerini kullanır:
        // usdtAmount = kullanıcının marjı, leverage = kullanıcının kaldıracı,
        // slPct/tpPct = kaldıraçsız coin hareketi yüzdesi.
        const entryRef = parseFloat(analysis.price || coin.price || 0);
        if (!entryRef) { logAuto(`${coin.symbol} fiyat alınamadı — atlandı`); markAutoSkip(coin.symbol, 'Fiyat alınamadı'); continue; }

        const userSLPct = Math.max(0.05, parseFloat(cfg.slPct ?? 2));
        const userTPPct = Math.max(0.05, parseFloat(cfg.tpPct ?? 10));
        const userRR    = userTPPct / userSLPct;

        // ── R15 ATR GATE — UB tipi yüksek volatilite bloğu ──────────────────
        const coinAtrPct = analysis.r15?.atrGate?.atrPct || 0;
        if (coinAtrPct > 0 && coinAtrPct > userSLPct * 2.5) {
          logAuto(`⛔ ${coin.symbol} ATR %${coinAtrPct.toFixed(1)} >> SL %${userSLPct} — volatilite riski yüksek, atlandı`);
          markAutoSkip(coin.symbol, `ATR %${coinAtrPct.toFixed(1)} > SL %${userSLPct}*2.5 volatilite`, {rec:recommendation, score});
          continue;
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

        logAuto(`🔥 ${coin.symbol} A-Tier PRO! ${decisionChain?.reason||'A-Tier decisionChain'} — hard-gate değil, ağırlıklı karar geçti.`);

        // TP/SL: Paneldeki değerler kesin uygulanır; proTPSL sadece analiz kalitesine katkı verir.
        const targetPrice = isLong
          ? +(entryRef * (1 + userTPPct/100)).toFixed(8)
          : +(entryRef * (1 - userTPPct/100)).toFixed(8);
        const stopPrice = isLong
          ? +(entryRef * (1 - userSLPct/100)).toFixed(8)
          : +(entryRef * (1 + userSLPct/100)).toFixed(8);

        logAuto(`🎯 Sinyal: ${coin.symbol} ${recommendation} skor:${score} — marj:${usdtAmount} USDT lev:${leverage}x SL:%${userSLPct} TP:%${userTPPct} RR:${userRR.toFixed(2)} — emir açılıyor`);

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
          autoScanState.opened = (autoScanState.opened||0) + 1;
          invalidatePositionRiskCache('ORDER_OPENED');
          autoScanState.phase = 'EMİR_AÇILDI';
          logAuto(`✅ ${coin.symbol} ${recommendation} açıldı — ${orderResp.message}`);
          // Trailing state başlat + açılış sebebi kaydet (Pos kartında görünür)
          trailingState.set(coin.fullSymbol, {
            side: recommendation,
            entryPrice: orderResp.executedPrice||analysis.price,
            highWater: orderResp.executedPrice||analysis.price,
            breakEvenSet:false, currentSL:orderResp.details?.stop||stopPrice,
            targetTP:orderResp.details?.target||targetPrice,
            leverage:parseInt(leverage)||1,
            sltpVerified: !!orderResp.slSuccess && !!orderResp.tpSuccess,
            openedAt: Date.now(),
            step1Set:false, step2Set:false, step3Set:false,
            peakPnl:0, peakRealPct:0,
            config:{ trailing:true, trailingPct, trailStep, breakEvenPct,
              entryPrice:orderResp.executedPrice||analysis.price, targetTP:orderResp.details?.target||targetPrice },
            entryReason:{
              score, longScore, shortScore,
              reason: decisionChain?.reason || `${recommendation} A-Tier`,
              tags: (analysis.signals||[]).slice(0,6),
              mm: analysis.marketMaker?.target,
              cvd: analysis.cvd?.momentum,
              funding: analysis.funding?.current,
              panel:{ usdtAmount, leverage, slPct:userSLPct, tpPct:userTPPct },
            },
            managerStatus:{ type:'AÇILDI', reason:`${coin.symbol} ${recommendation} açıldı — skor:${score}`, urgency:'LOW', lastCheck:Date.now() },
          });
          // Bir sonraki coin için bekle
          await new Promise(r=>setTimeout(r,2000));
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
    if (isPositionRiskRateLimitError(e) || String(e?.message||'').includes('RATE_LIMIT_COOLDOWN')) {
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
    autoScanState.nextScanDue = autoConfig?.enabled ? Date.now() + 3*60*1000 : null;

    // Görünürlük düzeltmesi: emir denemesi hata verdikten sonra eski MAX_POZİSYON_DOLU
    // fazı ekranda takılı kalmasın. Binance pozisyonu tekrar okunur; 0/1 ise BEKLİYOR gösterilir.
    if (autoScanState.phase === 'MAX_POZİSYON_DOLU') {
      try {
        const cnt = await getNewPosCount();
        const maxP = parseInt(autoConfig?.maxPositions || autoScanState?.settings?.maxPositions || 1) || 1;
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
  // Her 3 dakikada bir tara.
  autoTimer = setInterval(runAutoScan, 3*60*1000);
  // Her 30 saniyede bir: manuel kapanış algıla + SL/TP eksikse kurtar
  if (!positionSyncTimer) {
    positionSyncTimer = setInterval(syncPositions, 30 * 1000);
  }
  runAutoScan(); // Hemen başlat
}


// ── KAPANIŞ SEBEBİ SINIFLANDIRMA ────────────────────────────────────────────
// Binance positionRisk'te pozisyon kaybolduğunda eski sürüm bunu genel olarak
// "manuel/SL/TP" yazıyordu. Bu kafa karıştırıyordu. Burada best-effort şekilde
// son userTrades + state'teki SL/TP fiyatlarına göre sebep ayrıştırılır.
async function getRecentUserTradesSafe(apiKey, apiSecret, symbol, state) {
  try {
    const openedAt = Number(state?.openedAt || state?.entryAt || 0);
    const startTime = openedAt > 0 ? Math.max(0, openedAt - 5*60*1000) : Date.now() - 6*60*60*1000;
    const rows = await bReq(apiKey, apiSecret, 'GET', '/fapi/v1/userTrades', {
      symbol,
      startTime,
      limit: 80,
    }, 10000);
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
  const side = inferStateSide(state);
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

  // Stop-market fill kayması olabilir; bu yüzden %0.45 tolerans kullanılır.
  const tol = 0.45;
  let code = 'BINANCE_CLOSED_UNKNOWN';
  let label = 'Binance kapanışı';
  let emoji = '🔍';

  if (closePrice > 0 && tp > 0 && pctDiff(closePrice, tp) <= tol) {
    code = 'TAKE_PROFIT'; label = 'TP ile kapandı'; emoji = '🎯';
  } else if (closePrice > 0 && sl > 0 && pctDiff(closePrice, sl) <= tol) {
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

  return {
    code, label, emoji,
    closePrice: closePrice ? +closePrice.toFixed(8) : null,
    realizedPnl: Number.isFinite(wa.pnl) ? +wa.pnl.toFixed(6) : null,
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
    const posData = await getPositionRiskCached(autoConfig.apiKey, autoConfig.apiSecret);
    const openMap = new Map();
    if (Array.isArray(posData)) {
      for (const p of posData) {
        if (Math.abs(parseFloat(p.positionAmt)) > 0) openMap.set(p.symbol, p);
      }
    }
    autoScanState.livePositions = openMap.size;

    // trailingState'de kayıtlı ama Binance'te artık olmayan = kapanmış.
    // Eski log "manuel/SL/TP" diye geneldi; şimdi mümkünse BE/TP/SL ayrıştırılır.
    for (const [sym, state] of trailingState.entries()) {
      if (!openMap.has(sym)) {
        const cls = await classifyClosedPosition(autoConfig.apiKey, autoConfig.apiSecret, sym, state);
        const px = cls.closePrice ? ` fiyat:${cls.closePrice}` : '';
        const pnl = Number.isFinite(cls.realizedPnl) ? ` pnl:${cls.realizedPnl}` : '';
        logAuto(`${cls.emoji} ${sym.replace('USDT','')} kapandı → ${cls.label}${px}${pnl} — state temizlendi, 30dk cooldown`);
        trailingState.delete(sym);
        setCooldown(sym, COOLDOWN_CLOSE_MS, `${cls.code} algılandı`);
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
        const slPctGuard = Math.max(0.1, parseFloat(autoConfig.slPct || 2));
        const realMoveGuard = ep > 0 && mp > 0
          ? ((mp - ep) / ep * 100 * (isLongGuard ? 1 : -1))
          : 0;
        const roiGuard = realMoveGuard * lev;
        const hardLossRealGuard = -Math.max(slPctGuard + 0.25, slPctGuard * 1.12);
        const hardLossRoiGuard  = -Math.max((slPctGuard * lev) + 5, 30);
        if (ep > 0 && mp > 0 && (realMoveGuard <= hardLossRealGuard || roiGuard <= hardLossRoiGuard)) {
          try {
            pushCritical('R14_HARD_LOSS_GUARD', `${sym}: SL ötesi zarar yakalandı; market reduceOnly kapatılıyor. move=${realMoveGuard.toFixed(2)}% roi=${roiGuard.toFixed(1)}%`, {symbol:sym, entry:ep, mark:mp, leverage:lev});
            await cancelAlgoOrders(autoConfig.apiKey, autoConfig.apiSecret, sym);
            await bReq(autoConfig.apiKey, autoConfig.apiSecret, 'POST', '/fapi/v1/order', {
              symbol:sym, side:isLongGuard?'SELL':'BUY', type:'MARKET',
              quantity:Math.abs(amt).toString(), reduceOnly:'true', positionSide:'BOTH'
            });
            logAuto(`🛑 ${sym.replace('USDT','')} R14 hard-loss guard kapattı: move ${realMoveGuard.toFixed(2)}% ROI ${roiGuard.toFixed(1)}%`);
            trailingState.delete(sym);
            setCooldown(sym, COOLDOWN_CLOSE_MS, 'R14_HARD_LOSS_GUARD');
            continue;
          } catch(closeErr) {
            pushCritical('R14_HARD_LOSS_CLOSE_FAIL', `${sym}: hard-loss close başarısız — ${closeErr.message}`, {symbol:sym});
          }
        }

        const orders = await liveOpenBracketOrders(autoConfig.apiKey, autoConfig.apiSecret, sym);
        const hasSL = orders.some(o => orderKind(o) === 'SL');
        const hasTP = orders.some(o => orderKind(o) === 'TP');
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

app.listen(PORT, ()=>console.log(`✅ Server ${PORT}`));
