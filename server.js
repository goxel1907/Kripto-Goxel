const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const fetch = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));

const PORT = process.env.PORT || 3000;
const BINANCE_BASES = [
  'https://fapi.binance.com',
  'https://fapi1.binance.com',
  'https://fapi2.binance.com',
  'https://fapi3.binance.com'
];
const DEFAULT_RECV_WINDOW = 10000;

// Rate-limit korumasÄ±: Binance public endpointlerine aynÄ± sembol iÃ§in tekrar tekrar vurma.
const CACHE = new Map();
const PENDING = new Map();
function cacheGet(key) {
  const v = CACHE.get(key);
  if (!v) return null;
  if (Date.now() > v.expires) { CACHE.delete(key); return null; }
  return v.value;
}
function cacheSet(key, value, ttlMs) {
  CACHE.set(key, { value, expires: Date.now() + ttlMs });
  return value;
}
async function cached(key, ttlMs, fn) {
  const hit = cacheGet(key);
  if (hit) return hit;
  if (PENDING.has(key)) return PENDING.get(key);
  const pr = Promise.resolve().then(fn).then(v => cacheSet(key, v, ttlMs)).finally(() => PENDING.delete(key));
  PENDING.set(key, pr);
  return pr;
}
function friendlyBinanceError(e) {
  const msg = String(e?.message || e || '');
  if (msg.includes('429')) return 'Binance rate limit 429: Railway IP / API isteÄŸi fazla. 2-5 dakika bekle, tekrar tekrar Test Etme. Yeni sÃ¼rÃ¼m istekleri cacheledi.';
  if (msg.includes('418')) return 'Binance geÃ§ici IP ban 418: 15-30 dakika bekle. Ã‡ok sÄ±k public veri istenmiÅŸ.';
  return msg || 'Bilinmeyen Binance hatasÄ±';
}

function sign(queryString, secret) {
  return crypto.createHmac('sha256', String(secret).trim()).update(queryString).digest('hex');
}

function normalizeSymbol(symbol) {
  const s = String(symbol || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (!s) return '';
  return s.endsWith('USDT') ? s : `${s}USDT`;
}

function toNum(v, fallback = 0) {
  const n = Number.parseFloat(v);
  return Number.isFinite(n) ? n : fallback;
}

function formatStep(value, stepSize) {
  const step = toNum(stepSize, 0);
  const val = toNum(value, 0);
  if (!step || !val) return String(val);
  const precision = step < 1 ? Math.min(12, Math.max(0, String(step).split('.')[1]?.replace(/0+$/, '').length || 0)) : 0;
  const floored = Math.floor(val / step) * step;
  return floored.toFixed(precision);
}

function formatTick(value, tickSize) {
  const tick = toNum(tickSize, 0);
  const val = toNum(value, 0);
  if (!tick || !val) return String(val);
  const precision = tick < 1 ? Math.min(12, Math.max(0, String(tick).split('.')[1]?.replace(/0+$/, '').length || 0)) : 0;
  const rounded = Math.round(val / tick) * tick;
  return rounded.toFixed(precision);
}

async function fetchJsonWithBases(path, options = {}, bases = BINANCE_BASES) {
  let lastError;
  for (const base of bases) {
    const url = `${base}${path}`;
    try {
      const res = await fetch(url, options);
      const text = await res.text();
      let data;
      try { data = text ? JSON.parse(text) : {}; } catch { data = { raw: text }; }
      if (!res.ok) {
        const msg = data?.msg || data?.message || data?.raw || `HTTP ${res.status}`;
        throw new Error(`${msg} (${res.status})`);
      }
      if (data && data.code && Number(data.code) < 0) throw new Error(data.msg || `Binance hata kodu ${data.code}`);
      return data;
    } catch (e) {
      lastError = e;
    }
  }
  throw lastError || new Error('Binance baÄŸlantÄ±sÄ± baÅŸarÄ±sÄ±z');
}

async function binanceRequest(apiKey, apiSecret, method, path, params = {}) {
  const cleanKey = String(apiKey || '').trim();
  const cleanSecret = String(apiSecret || '').trim();
  if (!cleanKey || !cleanSecret) throw new Error('API key ve secret gerekli');

  const queryObj = { ...params, recvWindow: DEFAULT_RECV_WINDOW, timestamp: Date.now() };
  const queryString = Object.entries(queryObj)
    .filter(([, v]) => v !== undefined && v !== null && v !== '')
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join('&');
  const signature = sign(queryString, cleanSecret);
  const signedPayload = `${queryString}&signature=${signature}`;

  const upper = String(method || 'GET').toUpperCase();
  const options = {
    method: upper,
    headers: {
      'X-MBX-APIKEY': cleanKey,
      'Content-Type': 'application/x-www-form-urlencoded'
    }
  };

  // Binance signed GET endpointleri query string ister. POST/DELETE emir endpointlerinde
  // body daha stabil Ã§alÄ±ÅŸÄ±r; bazÄ± proxy/Railway durumlarÄ±nda query parametreleri sorun Ã§Ä±karabiliyor.
  if (upper === 'GET') {
    return fetchJsonWithBases(`${path}?${signedPayload}`, options);
  }
  options.body = signedPayload;
  return fetchJsonWithBases(path, options);
}

function firstPositive(...values) {
  for (const v of values) {
    const n = toNum(v, NaN);
    if (Number.isFinite(n) && n > 0) return n;
  }
  for (const v of values) {
    const n = toNum(v, NaN);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

async function readFuturesAccount(apiKey, apiSecret) {
  const [bal3, bal2, acc3, acc2, pos2] = await Promise.allSettled([
    binanceRequest(apiKey, apiSecret, 'GET', '/fapi/v3/balance'),
    binanceRequest(apiKey, apiSecret, 'GET', '/fapi/v2/balance'),
    binanceRequest(apiKey, apiSecret, 'GET', '/fapi/v3/account'),
    binanceRequest(apiKey, apiSecret, 'GET', '/fapi/v2/account'),
    binanceRequest(apiKey, apiSecret, 'GET', '/fapi/v2/positionRisk')
  ]);

  const balanceArray = bal3.status === 'fulfilled' && Array.isArray(bal3.value) ? bal3.value
    : bal2.status === 'fulfilled' && Array.isArray(bal2.value) ? bal2.value
    : [];
  const account = acc3.status === 'fulfilled' && acc3.value && !Array.isArray(acc3.value) ? acc3.value
    : acc2.status === 'fulfilled' && acc2.value && !Array.isArray(acc2.value) ? acc2.value
    : {};
  const usdtBal = balanceArray.find(b => b.asset === 'USDT') || {};
  const assets = Array.isArray(account.assets) ? account.assets : [];
  const usdtAsset = assets.find(a => a.asset === 'USDT') || {};
  const posArr = Array.isArray(pos2.value) ? pos2.value : Array.isArray(account.positions) ? account.positions : [];

  const totalWalletBalance = firstPositive(
    usdtBal.balance,
    usdtBal.walletBalance,
    usdtBal.crossWalletBalance,
    account.totalWalletBalance,
    account.totalMarginBalance,
    usdtAsset.walletBalance,
    usdtAsset.marginBalance
  );
  const availableBalance = firstPositive(
    usdtBal.availableBalance,
    usdtBal.maxWithdrawAmount,
    usdtBal.withdrawAvailable,
    account.availableBalance,
    account.maxWithdrawAmount,
    usdtAsset.availableBalance,
    usdtAsset.maxWithdrawAmount
  );
  const totalUnrealizedProfit = firstPositive(
    usdtBal.crossUnPnl,
    usdtBal.unrealizedProfit,
    account.totalUnrealizedProfit,
    usdtAsset.unrealizedProfit,
    usdtAsset.crossUnPnl,
    0
  );

  const endpointStatus = {
    balanceV3: bal3.status === 'fulfilled' ? 'ok' : String(bal3.reason?.message || 'err'),
    balanceV2: bal2.status === 'fulfilled' ? 'ok' : String(bal2.reason?.message || 'err'),
    accountV3: acc3.status === 'fulfilled' ? 'ok' : String(acc3.reason?.message || 'err'),
    accountV2: acc2.status === 'fulfilled' ? 'ok' : String(acc2.reason?.message || 'err'),
    positionRisk: pos2.status === 'fulfilled' ? 'ok' : String(pos2.reason?.message || 'err')
  };

  return {
    ok: true,
    source: 'fapi/v3|v2 balance + fapi/v3|v2 account + positionRisk',
    endpointStatus,
    asset: 'USDT',
    totalWalletBalance,
    availableBalance,
    totalUnrealizedProfit,
    marginBalance: firstPositive(account.totalMarginBalance, usdtAsset.marginBalance, totalWalletBalance),
    rawBalanceCount: balanceArray.length,
    debugUsdtBalance: {
      balance: usdtBal.balance,
      availableBalance: usdtBal.availableBalance,
      crossWalletBalance: usdtBal.crossWalletBalance,
      crossUnPnl: usdtBal.crossUnPnl,
      walletBalance: usdtAsset.walletBalance,
      assetAvailable: usdtAsset.availableBalance
    },
    positions: posArr.filter(p => Math.abs(toNum(p.positionAmt, 0)) > 0).map(p => ({
      symbol: p.symbol,
      positionAmt: toNum(p.positionAmt, 0),
      side: toNum(p.positionAmt, 0) >= 0 ? 'LONG' : 'SHORT',
      entryPrice: toNum(p.entryPrice, 0),
      unrealizedProfit: toNum(p.unRealizedProfit ?? p.unrealizedProfit, 0),
      leverage: toNum(p.leverage, 0),
      markPrice: toNum(p.markPrice, 0),
      liquidationPrice: toNum(p.liquidationPrice, 0)
    }))
  };
}


async function getExchangeSymbol(sym) {
  const info = await cached('exchangeInfo', 30 * 60 * 1000, async () => {
    const x = await fetchJsonWithBases('/fapi/v1/exchangeInfo', { method: 'GET' });
    if (!x || !Array.isArray(x.symbols)) {
      throw new Error('Binance exchangeInfo sembol listesi alÄ±namadÄ±. 2-5 dakika bekle veya Railway deploy/log kontrol et.');
    }
    return x;
  });
  const symbols = Array.isArray(info?.symbols) ? info.symbols : [];
  const symbolInfo = symbols.find(s => s && s.symbol === sym);
  if (!symbolInfo) throw new Error(`${sym} Binance USD-M Futures iÃ§inde bulunamadÄ± veya geÃ§ici olarak listede yok`);
  if (!Array.isArray(symbolInfo.filters)) throw new Error(`${sym} Binance filtreleri alÄ±namadÄ±; exchangeInfo eksik dÃ¶ndÃ¼`);
  return symbolInfo;
}


async function getFuturesTradingSymbols() {
  const info = await cached('futures:exchangeInfo:trading', 30 * 60 * 1000, async () => {
    const x = await fetchJsonWithBases('/fapi/v1/exchangeInfo', { method: 'GET' });
    if (!Array.isArray(x?.symbols)) throw new Error('exchangeInfo symbols boÅŸ geldi');
    return x;
  });
  return info.symbols
    .filter(s => s.status === 'TRADING' && s.contractType === 'PERPETUAL' && s.quoteAsset === 'USDT')
    .map(s => s.symbol);
}

function opportunityScoreFromTicker(t) {
  const ch = Math.abs(toNum(t.priceChangePercent, 0));
  const vol = Math.log10(Math.max(1, toNum(t.quoteVolume, 0)));
  const trades = Math.log10(Math.max(1, toNum(t.count, 0)));
  const range = toNum(t.lowPrice, 0) > 0 ? ((toNum(t.highPrice, 0) - toNum(t.lowPrice, 0)) / toNum(t.lowPrice, 0)) * 100 : 0;
  return Math.round((ch * 2.2) + (range * 1.4) + (vol * 8) + (trades * 3));
}


function ema(values, period) {
  const arr = values.map(Number).filter(Number.isFinite);
  if (arr.length < period) return null;
  const k = 2 / (period + 1);
  let e = arr.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < arr.length; i++) e = arr[i] * k + e * (1 - k);
  return e;
}

function rsiFromCloses(closes, period = 14) {
  const arr = closes.map(Number).filter(Number.isFinite);
  if (arr.length <= period) return null;
  let gains = 0, losses = 0;
  for (let i = arr.length - period; i < arr.length; i++) {
    const diff = arr[i] - arr[i - 1];
    if (diff >= 0) gains += diff; else losses -= diff;
  }
  if (losses === 0) return 100;
  const rs = gains / losses;
  return 100 - (100 / (1 + rs));
}

function vwapFromKlines(klines) {
  let pv = 0, vol = 0;
  for (const k of klines) {
    const high = toNum(k[2]), low = toNum(k[3]), close = toNum(k[4]), v = toNum(k[5]);
    const typical = (high + low + close) / 3;
    pv += typical * v;
    vol += v;
  }
  return vol ? pv / vol : null;
}

function cvdFromKlines(klines) {
  let cvd = 0;
  for (const k of klines) {
    const volume = toNum(k[5]);
    const takerBuyBase = toNum(k[9]);
    cvd += (takerBuyBase - (volume - takerBuyBase));
  }
  return cvd;
}

function atrFromKlines(klines, period = 14) {
  if (!Array.isArray(klines) || klines.length <= period) return null;
  const trs = [];
  for (let i = 1; i < klines.length; i++) {
    const h = toNum(klines[i][2]), l = toNum(klines[i][3]), pc = toNum(klines[i - 1][4]);
    trs.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)));
  }
  return trs.slice(-period).reduce((a,b)=>a+b,0) / period;
}

function tfSummary(klines) {
  const closes = klines.map(k => toNum(k[4]));
  const last = closes[closes.length - 1] || 0;
  const first = closes[0] || last;
  const e9 = ema(closes, 9), e21 = ema(closes, 21), e50 = ema(closes, 50);
  const vw = vwapFromKlines(klines);
  const rsi = rsiFromCloses(closes, 14);
  const cvd = cvdFromKlines(klines);
  const atr = atrFromKlines(klines, 14);
  const trend = e9 && e21 && e50 ? (last > e9 && e9 > e21 && e21 > e50 ? 'BULL' : last < e9 && e9 < e21 && e21 < e50 ? 'BEAR' : 'MIXED') : 'UNKNOWN';
  return {
    last, changePct: first ? ((last - first) / first) * 100 : 0,
    ema9: e9, ema21: e21, ema50: e50, vwap: vw, rsi, cvd, atr,
    trend,
    aboveVwap: vw ? last >= vw : null,
    ema21DistancePct: e21 ? ((last - e21) / e21) * 100 : null
  };
}

function depthImbalance(depth) {
  const bids = Array.isArray(depth?.bids) ? depth.bids : [];
  const asks = Array.isArray(depth?.asks) ? depth.asks : [];
  const bidNotional = bids.slice(0, 20).reduce((a, [p, q]) => a + toNum(p) * toNum(q), 0);
  const askNotional = asks.slice(0, 20).reduce((a, [p, q]) => a + toNum(p) * toNum(q), 0);
  const total = bidNotional + askNotional;
  return { bidNotional, askNotional, imbalance: total ? (bidNotional - askNotional) / total : 0 };
}

function smartMoneyScore({ tf15, tf1h, tf4h, depth, fundingRate, openInterest, topLongShortRatio, globalLongShortRatio }) {
  let long = 0, short = 0, notes = [];
  if (tf4h.trend === 'BULL') { long += 18; notes.push('4s trend BULL'); }
  if (tf4h.trend === 'BEAR') { short += 18; notes.push('4s trend BEAR'); }
  if (tf1h.aboveVwap === true) long += 10;
  if (tf1h.aboveVwap === false) short += 10;
  if (tf15.cvd > 0) long += 10; else short += 10;
  if (depth.imbalance > 0.08) long += 10;
  if (depth.imbalance < -0.08) short += 10;
  if (Number.isFinite(fundingRate) && fundingRate > 0.035 && tf15.trend !== 'BULL') { short += 12; notes.push('pozitif funding sÄ±kÄ±ÅŸma riski'); }
  if (Number.isFinite(fundingRate) && fundingRate < -0.035 && tf15.trend !== 'BEAR') { long += 12; notes.push('negatif funding squeeze riski'); }
  if (Number.isFinite(topLongShortRatio) && topLongShortRatio > 2.2 && tf15.cvd < 0) { short += 12; notes.push('crowded long + CVD zayÄ±f'); }
  if (Number.isFinite(topLongShortRatio) && topLongShortRatio < 0.55 && tf15.cvd > 0) { long += 12; notes.push('crowded short + CVD gÃ¼Ã§lÃ¼'); }
  const side = long >= short ? 'LONG' : 'SHORT';
  const score = Math.max(long, short);
  return { side, longScore: long, shortScore: short, score, notes };
}


app.get('/', (req, res) => {
  res.json({ status: 'ok', message: 'Kripto Sinyal Backend Ã§alÄ±ÅŸÄ±yor', time: new Date().toISOString() });
});

app.post('/api/account', async (req, res) => {
  const { apiKey, apiSecret } = req.body || {};
  if (!apiKey || !apiSecret) return res.status(400).json({ error: 'API key ve secret gerekli' });
  try {
    // Bakiye endpointini cachelemiyoruz; kullanÄ±cÄ± Test Et/Bakiye Getir dediÄŸinde canlÄ± okur.
    const data = await readFuturesAccount(apiKey, apiSecret);
    res.json(data);
  } catch (e) { res.status(400).json({ error: friendlyBinanceError(e) }); }
});

app.post('/api/account-debug', async (req, res) => {
  const { apiKey, apiSecret } = req.body || {};
  if (!apiKey || !apiSecret) return res.status(400).json({ error: 'API key ve secret gerekli' });
  try {
    const data = await readFuturesAccount(apiKey, apiSecret);
    // Secret/key dÃ¶ndÃ¼rmez. Mobilde doÄŸrudan API cevabÄ±nÄ± gÃ¶rmek iÃ§in.
    res.json(data);
  } catch (e) { res.status(400).json({ error: friendlyBinanceError(e) }); }
});

app.post('/api/leverage', async (req, res) => {
  const { apiKey, apiSecret, symbol, leverage } = req.body || {};
  const sym = normalizeSymbol(symbol);
  if (!apiKey || !apiSecret || !sym || !leverage) return res.status(400).json({ error: 'Eksik parametre' });
  try {
    const lev = Math.max(1, Math.min(125, parseInt(leverage, 10) || 1));
    const data = await binanceRequest(apiKey, apiSecret, 'POST', '/fapi/v1/leverage', { symbol: sym, leverage: lev });
    res.json({ ok: true, leverage: data.leverage, symbol: data.symbol });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.post('/api/order', async (req, res) => {
  const { apiKey, apiSecret, symbol, leverage, entryPrice, targetPrice, stopPrice, usdtAmount } = req.body || {};
  const sym = normalizeSymbol(symbol);
  const lev = parseInt(leverage, 10);
  const entry = toNum(entryPrice), target = toNum(targetPrice), stop = toNum(stopPrice), margin = toNum(usdtAmount);
  if (!apiKey || !apiSecret || !sym || !lev || !entry || !target || !stop || !margin) return res.status(400).json({ error: 'Eksik veya geÃ§ersiz emir parametresi' });
  if (!(target > entry && stop < entry)) return res.status(400).json({ error: 'LONG iÃ§in TP giriÅŸten bÃ¼yÃ¼k, SL giriÅŸten kÃ¼Ã§Ã¼k olmalÄ±' });

  try {
    await binanceRequest(apiKey, apiSecret, 'POST', '/fapi/v1/leverage', { symbol: sym, leverage: Math.max(1, Math.min(125, lev)) });

    const symbolInfo = await getExchangeSymbol(sym);
    const filters = Array.isArray(symbolInfo?.filters) ? symbolInfo.filters : [];
    const lotFilter = filters.find(f => f && f.filterType === 'LOT_SIZE');
    const priceFilter = filters.find(f => f && f.filterType === 'PRICE_FILTER');
    const minNotionalFilter = filters.find(f => f && (f.filterType === 'MIN_NOTIONAL' || f.filterType === 'NOTIONAL'));
    if (!lotFilter || !priceFilter) throw new Error(`${sym} lot/tick filtreleri alÄ±namadÄ±`);

    const qty = formatStep((margin * lev) / entry, lotFilter.stepSize);
    if (toNum(qty) < toNum(lotFilter.minQty)) throw new Error(`Miktar minQty altÄ±nda: ${qty} < ${lotFilter.minQty}`);
    const notional = toNum(qty) * entry;
    const minNotional = toNum(minNotionalFilter?.notional ?? minNotionalFilter?.minNotional, 0);
    if (minNotional && notional < minNotional) throw new Error(`Pozisyon nominali Ã§ok kÃ¼Ã§Ã¼k: ${notional.toFixed(2)} USDT < ${minNotional} USDT`);

    const price = formatTick(entry, priceFilter.tickSize);
    const tp = formatTick(target, priceFilter.tickSize);
    const sl = formatTick(stop, priceFilter.tickSize);

    const mainOrder = await binanceRequest(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
      symbol: sym,
      side: 'BUY',
      type: 'LIMIT',
      timeInForce: 'GTC',
      quantity: qty,
      price,
      positionSide: 'BOTH'
    });

    let tpOrder = null, slOrder = null;
    try {
      tpOrder = await binanceRequest(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
        symbol: sym,
        side: 'SELL',
        type: 'TAKE_PROFIT_MARKET',
        stopPrice: tp,
        closePosition: 'true',
        workingType: 'MARK_PRICE',
        positionSide: 'BOTH'
      });
      slOrder = await binanceRequest(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
        symbol: sym,
        side: 'SELL',
        type: 'STOP_MARKET',
        stopPrice: sl,
        closePosition: 'true',
        workingType: 'MARK_PRICE',
        positionSide: 'BOTH'
      });
    } catch (protectError) {
      // Koruma emirleri kurulamazsa ana emri iptal etmeyi dene. KorumasÄ±z pozisyon riski bÄ±rakma.
      try { await binanceRequest(apiKey, apiSecret, 'DELETE', '/fapi/v1/order', { symbol: sym, orderId: mainOrder.orderId }); } catch {}
      throw new Error(`TP/SL kurulamadÄ±, ana emir iptal denendi: ${protectError.message}`);
    }

    res.json({
      ok: true,
      message: `${sym} LONG emri aÃ§Ä±ldÄ±`,
      mainOrderId: mainOrder.orderId,
      tpOrderId: tpOrder?.orderId,
      slOrderId: slOrder?.orderId,
      details: { symbol: sym, quantity: qty, leverage: lev, entry: price, target: tp, stop: sl, usdtAmount: margin }
    });
  } catch (e) {
    res.status(400).json({ error: friendlyBinanceError(e) });
  }
});

app.post('/api/positions', async (req, res) => {
  const { apiKey, apiSecret } = req.body || {};
  if (!apiKey || !apiSecret) return res.status(400).json({ error: 'API key gerekli' });
  try {
    const data = await binanceRequest(apiKey, apiSecret, 'GET', '/fapi/v2/positionRisk');
    const list = Array.isArray(data) ? data : [];
    const open = list.filter(p => Math.abs(toNum(p.positionAmt, 0)) > 0).map(p => {
      const amt = toNum(p.positionAmt, 0);
      const entry = toNum(p.entryPrice, 0);
      const mark = toNum(p.markPrice, 0);
      const lev = toNum(p.leverage, 1);
      const dir = amt >= 0 ? 1 : -1;
      return {
        symbol: p.symbol,
        positionAmt: amt,
        side: amt >= 0 ? 'LONG' : 'SHORT',
        entryPrice: entry,
        markPrice: mark,
        unrealizedProfit: toNum(p.unRealizedProfit ?? p.unrealizedProfit, 0),
        percentage: entry > 0 ? (((mark - entry) / entry) * 100 * lev * dir).toFixed(2) : '0',
        leverage: lev,
        liquidationPrice: toNum(p.liquidationPrice, 0)
      };
    });
    res.json({ ok: true, positions: open });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.post('/api/close', async (req, res) => {
  const { apiKey, apiSecret, symbol } = req.body || {};
  const sym = normalizeSymbol(symbol);
  if (!apiKey || !apiSecret || !sym) return res.status(400).json({ error: 'Eksik parametre' });
  try {
    try { await binanceRequest(apiKey, apiSecret, 'DELETE', '/fapi/v1/allOpenOrders', { symbol: sym }); } catch {}
    const pos = await binanceRequest(apiKey, apiSecret, 'GET', '/fapi/v2/positionRisk', { symbol: sym });
    const list = Array.isArray(pos) ? pos : [];
    const openPos = list.find(p => Math.abs(toNum(p.positionAmt, 0)) > 0);
    if (!openPos) return res.json({ ok: true, message: 'AÃ§Ä±k pozisyon yok' });
    const amt = toNum(openPos.positionAmt, 0);
    const side = amt > 0 ? 'SELL' : 'BUY';
    const closeOrder = await binanceRequest(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
      symbol: sym,
      side,
      type: 'MARKET',
      quantity: formatStep(Math.abs(amt), '0.000001'),
      reduceOnly: 'true',
      positionSide: 'BOTH'
    });
    res.json({ ok: true, message: `${sym} pozisyonu kapatÄ±ldÄ±`, orderId: closeOrder.orderId });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.get('/api/price/:symbol', async (req, res) => {
  try {
    const sym = normalizeSymbol(req.params.symbol);
    const data = await fetchJsonWithBases(`/fapi/v1/ticker/24hr?symbol=${encodeURIComponent(sym)}`, { method: 'GET' });
    res.json({
      symbol: sym,
      price: toNum(data.lastPrice),
      change24h: toNum(data.priceChangePercent),
      volume: toNum(data.quoteVolume),
      high: toNum(data.highPrice),
      low: toNum(data.lowPrice)
    });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.get('/api/market/:symbol', async (req, res) => {
  try {
    const sym = normalizeSymbol(req.params.symbol);
    const [funding, oi] = await Promise.allSettled([
      fetchJsonWithBases(`/fapi/v1/fundingRate?symbol=${encodeURIComponent(sym)}&limit=1`, { method: 'GET' }),
      fetchJsonWithBases(`/fapi/v1/openInterest?symbol=${encodeURIComponent(sym)}`, { method: 'GET' })
    ]);
    res.json({
      symbol: sym,
      fundingRate: funding.status === 'fulfilled' && funding.value?.[0] ? toNum(funding.value[0].fundingRate) * 100 : null,
      openInterest: oi.status === 'fulfilled' ? toNum(oi.value.openInterest) : null
    });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});


app.get('/api/futures-tickers', async (req, res) => {
  try {
    const out = await cached('futures:tickers:opportunity:v2', 20 * 1000, async () => {
      const [symbols, tickers] = await Promise.all([
        getFuturesTradingSymbols(),
        fetchJsonWithBases('/fapi/v1/ticker/24hr', { method: 'GET' })
      ]);
      const allowed = new Set(symbols);
      const coins = (Array.isArray(tickers) ? tickers : [])
        .filter(t => allowed.has(t.symbol) && String(t.symbol).endsWith('USDT'))
        .map(t => ({
          symbol: t.symbol,
          price: toNum(t.lastPrice, 0),
          change24h: toNum(t.priceChangePercent, 0),
          volume: toNum(t.quoteVolume, 0),
          trades: toNum(t.count, 0),
          high: toNum(t.highPrice, 0),
          low: toNum(t.lowPrice, 0),
          opportunityRank: opportunityScoreFromTicker(t)
        }))
        .sort((a, b) => b.opportunityRank - a.opportunityRank);
      return {
        ok: true,
        universe: 'BINANCE_USD_M_FUTURES_USDT_PERPETUAL',
        scanSeconds: 80,
        count: coins.length,
        coins
      };
    });
    res.json(out);
  } catch (e) {
    res.status(400).json({ error: friendlyBinanceError(e) });
  }
});


app.get('/api/market-intel/:symbol', async (req, res) => {
  try {
    const sym = normalizeSymbol(req.params.symbol);
    const out = await cached(`market-intel:${sym}`, 90 * 1000, async () => {
    const qsym = encodeURIComponent(sym);
    const [k15, k1h, k4h, depth, premium, oi, topRatio, globalRatio] = await Promise.allSettled([
      fetchJsonWithBases(`/fapi/v1/klines?symbol=${qsym}&interval=15m&limit=80`, { method: 'GET' }),
      fetchJsonWithBases(`/fapi/v1/klines?symbol=${qsym}&interval=1h&limit=120`, { method: 'GET' }),
      fetchJsonWithBases(`/fapi/v1/klines?symbol=${qsym}&interval=4h&limit=120`, { method: 'GET' }),
      fetchJsonWithBases(`/fapi/v1/depth?symbol=${qsym}&limit=100`, { method: 'GET' }),
      fetchJsonWithBases(`/fapi/v1/premiumIndex?symbol=${qsym}`, { method: 'GET' }),
      fetchJsonWithBases(`/fapi/v1/openInterest?symbol=${qsym}`, { method: 'GET' }),
      fetchJsonWithBases(`/futures/data/topLongShortAccountRatio?symbol=${qsym}&period=15m&limit=1`, { method: 'GET' }),
      fetchJsonWithBases(`/futures/data/globalLongShortAccountRatio?symbol=${qsym}&period=15m&limit=1`, { method: 'GET' })
    ]);
    if (k15.status !== 'fulfilled' || k1h.status !== 'fulfilled' || k4h.status !== 'fulfilled') {
      throw new Error('Kline verisi alÄ±namadÄ±');
    }
    const tf15 = tfSummary(k15.value);
    const tf1h = tfSummary(k1h.value);
    const tf4h = tfSummary(k4h.value);
    const ob = depth.status === 'fulfilled' ? depthImbalance(depth.value) : { bidNotional: 0, askNotional: 0, imbalance: 0 };
    const fundingRate = premium.status === 'fulfilled' ? toNum(premium.value.lastFundingRate) * 100 : null;
    const markPrice = premium.status === 'fulfilled' ? toNum(premium.value.markPrice) : tf15.last;
    const openInterest = oi.status === 'fulfilled' ? toNum(oi.value.openInterest) : null;
    const topLongShort = topRatio.status === 'fulfilled' && Array.isArray(topRatio.value) && topRatio.value[0] ? toNum(topRatio.value[0].longShortRatio) : null;
    const globalLongShort = globalRatio.status === 'fulfilled' && Array.isArray(globalRatio.value) && globalRatio.value[0] ? toNum(globalRatio.value[0].longShortRatio) : null;
    const sm = smartMoneyScore({ tf15, tf1h, tf4h, depth: ob, fundingRate, openInterest, topLongShortRatio: topLongShort, globalLongShortRatio: globalLongShort });
    return {
      ok: true,
      symbol: sym,
      timeframes: { '15m': tf15, '1h': tf1h, '4h': tf4h },
      orderBook: ob,
      fundingRate,
      openInterest,
      topLongShortRatio: topLongShort,
      globalLongShortRatio: globalLongShort,
      markPrice,
      smartMoney: sm,
      cachedSeconds: 90,
      usedForSignal: '4H ana trend + 1H giriÅŸ filtresi + 15M tetik + order book/CVD/funding/OI onayÄ±'
    };
    });
    res.json(out);
  } catch (e) {
    res.status(400).json({ error: friendlyBinanceError(e) });
  }
});

app.listen(PORT, () => console.log(`Server ${PORT} portunda Ã§alÄ±ÅŸÄ±yor`));
