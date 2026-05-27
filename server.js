const express = require('express');
const cors    = require('cors');
const crypto  = require('crypto');
const fetch   = require('node-fetch');

const app  = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const FAPI = 'https://fapi.binance.com';

// ── CACHE (429 engellemek için) ───────────────────────────────────────────────
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

  // GET → query string, POST/DELETE → body
  const finalUrl = (method.toUpperCase() === 'GET' || method.toUpperCase() === 'DELETE')
    ? `${url}?${fullQs}`
    : url;

  if (method.toUpperCase() === 'POST') options.body = fullQs;
  if (method.toUpperCase() === 'DELETE') options.method = 'DELETE';

  const res  = await fetch(finalUrl, options);
  const data = await res.json();
  if (data.code && data.code < 0) {
    const msg = data.msg || 'Binance hatası';
    if (data.code === -1121) throw new Error(`Sembol bulunamadı: ${params.symbol}`);
    throw new Error(`${msg} (${data.code})`);
  }
  return data;
}

// ── SAĞLIK ────────────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.json({ status: 'ok', time: new Date().toISOString() }));

// ── TÜM FUTURES PERP COİNLER (cache 5dk) ────────────────────────────────────
app.get('/api/futures-coins', async (req, res) => {
  try {
    const data = await cached('futures_tickers', 5 * 60 * 1000, async () => {
      const r = await fetch(`${FAPI}/fapi/v1/ticker/24hr`);
      return r.json();
    });

    if (!Array.isArray(data)) return res.json({ coins: [] });

    const EXCL = new Set(['BTCUSDT','ETHUSDT','BNBUSDT','XRPUSDT','SOLUSDT',
      'ADAUSDT','DOGEUSDT','DOTUSDT','MATICUSDT','LTCUSDT','TRXUSDT','AVAXUSDT',
      'LINKUSDT','UNIUSDT','WBTCUSDT','SHIBUSDT']);

    const coins = data
      .filter(t => t.symbol.endsWith('USDT') && !EXCL.has(t.symbol))
      .map(t => ({
        symbol:     t.symbol.replace('USDT', ''),
        fullSymbol: t.symbol,
        price:      parseFloat(t.lastPrice)         || 0,
        change1h:   0,
        change24h:  parseFloat(t.priceChangePercent) || 0,
        volume:     parseFloat(t.quoteVolume)        || 0,
        high:       parseFloat(t.highPrice)          || 0,
        low:        parseFloat(t.lowPrice)           || 0,
        trades:     parseInt(t.count)                || 0,
      }))
      .filter(c => c.volume > 100000); // Çok düşük hacimli olanları ele

    res.json({ ok: true, count: coins.length, coins });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// ── GELİŞMİŞ ANALİZ (tekil coin, cache 3dk) ──────────────────────────────────
app.get('/api/analyze/:symbol', async (req, res) => {
  const sym = req.params.symbol.toUpperCase();
  const fullSym = sym.endsWith('USDT') ? sym : sym + 'USDT';

  try {
    const [klines4h, klines1h, klines15m, funding, oi, ls] = await Promise.allSettled([
      // 4 saatlik mumlar (trend)
      cached(`klines4h_${fullSym}`, 4*60*1000, () =>
        fetch(`${FAPI}/fapi/v1/klines?symbol=${fullSym}&interval=4h&limit=50`).then(r=>r.json())),
      // 1 saatlik mumlar (momentum)
      cached(`klines1h_${fullSym}`, 60*1000, () =>
        fetch(`${FAPI}/fapi/v1/klines?symbol=${fullSym}&interval=1h&limit=50`).then(r=>r.json())),
      // 15 dakikalık mumlar (giriş zamanı)
      cached(`klines15m_${fullSym}`, 30*1000, () =>
        fetch(`${FAPI}/fapi/v1/klines?symbol=${fullSym}&interval=15m&limit=50`).then(r=>r.json())),
      // Funding rate
      cached(`funding_${fullSym}`, 5*60*1000, () =>
        fetch(`${FAPI}/fapi/v1/fundingRate?symbol=${fullSym}&limit=3`).then(r=>r.json())),
      // Open interest
      cached(`oi_${fullSym}`, 3*60*1000, () =>
        fetch(`${FAPI}/futures/data/openInterestHist?symbol=${fullSym}&period=1h&limit=6`).then(r=>r.json())),
      // Long/Short oranı
      cached(`ls_${fullSym}`, 3*60*1000, () =>
        fetch(`${FAPI}/futures/data/globalLongShortAccountRatio?symbol=${fullSym}&period=1h&limit=3`).then(r=>r.json())),
    ]);

    const k4h  = klines4h.status  === 'fulfilled' ? klines4h.value  : [];
    const k1h  = klines1h.status  === 'fulfilled' ? klines1h.value  : [];
    const k15m = klines15m.status === 'fulfilled' ? klines15m.value : [];
    const fund = funding.status   === 'fulfilled' ? funding.value   : [];
    const oiH  = oi.status        === 'fulfilled' ? oi.value        : [];
    const lsD  = ls.status        === 'fulfilled' ? ls.value        : [];

    // RSI hesapla
    function calcRSI(klines, period = 14) {
      if (!Array.isArray(klines) || klines.length < period + 1) return 50;
      const closes = klines.map(k => parseFloat(k[4]));
      let gains = 0, losses = 0;
      for (let i = closes.length - period; i < closes.length; i++) {
        const diff = closes[i] - closes[i-1];
        if (diff > 0) gains += diff; else losses -= diff;
      }
      const ag = gains / period, al = losses / period;
      return al === 0 ? 100 : Math.round(100 - (100 / (1 + ag / al)));
    }

    // EMA hesapla
    function calcEMA(klines, period) {
      if (!Array.isArray(klines) || klines.length < period) return 0;
      const closes = klines.map(k => parseFloat(k[4]));
      const k = 2 / (period + 1);
      let ema = closes.slice(0, period).reduce((a,b) => a+b, 0) / period;
      for (let i = period; i < closes.length; i++) ema = closes[i] * k + ema * (1-k);
      return ema;
    }

    // VWAP
    function calcVWAP(klines) {
      if (!Array.isArray(klines) || klines.length === 0) return 0;
      let tpv = 0, vol = 0;
      klines.forEach(k => {
        const tp = (parseFloat(k[2]) + parseFloat(k[3]) + parseFloat(k[4])) / 3;
        const v  = parseFloat(k[5]);
        tpv += tp * v; vol += v;
      });
      return vol > 0 ? tpv / vol : 0;
    }

    // Bollinger Bands
    function calcBB(klines, period = 20) {
      if (!Array.isArray(klines) || klines.length < period) return { upper: 0, lower: 0, mid: 0 };
      const closes = klines.slice(-period).map(k => parseFloat(k[4]));
      const mid = closes.reduce((a,b) => a+b, 0) / period;
      const std = Math.sqrt(closes.reduce((s,c) => s + Math.pow(c-mid, 2), 0) / period);
      return { upper: mid + 2*std, lower: mid - 2*std, mid };
    }

    // Hacim değişimi
    function calcVolChange(klines) {
      if (!Array.isArray(klines) || klines.length < 10) return 0;
      const vols = klines.map(k => parseFloat(k[5]));
      const avg  = vols.slice(0, -1).reduce((a,b) => a+b, 0) / (vols.length - 1);
      const last = vols[vols.length - 1];
      return avg > 0 ? ((last - avg) / avg) * 100 : 0;
    }

    const rsi4h  = calcRSI(k4h);
    const rsi1h  = calcRSI(k1h);
    const rsi15m = calcRSI(k15m);

    const ema20_4h = calcEMA(k4h, 20);
    const ema50_4h = calcEMA(k4h, 50);
    const ema20_1h = calcEMA(k1h, 20);
    const ema50_1h = calcEMA(k1h, 50);

    const vwap1h  = calcVWAP(k1h);
    const bb15m   = calcBB(k15m);

    const volChange1h  = calcVolChange(k1h);
    const volChange15m = calcVolChange(k15m);

    const lastPrice = k15m.length > 0 ? parseFloat(k15m[k15m.length-1][4]) : 0;

    // Funding rate
    const fundRate = fund.length > 0 ? parseFloat(fund[fund.length-1]?.fundingRate || 0) * 100 : 0;

    // OI değişimi
    let oiChange = 0;
    if (Array.isArray(oiH) && oiH.length >= 2) {
      const latest = parseFloat(oiH[oiH.length-1]?.sumOpenInterest || 0);
      const prev   = parseFloat(oiH[0]?.sumOpenInterest || 0);
      oiChange = prev > 0 ? ((latest - prev) / prev) * 100 : 0;
    }

    // Long/Short
    const lsRatio = lsD.length > 0 ? parseFloat(lsD[lsD.length-1]?.longShortRatio || 1) : 1;

    // Trend yönü (4h EMA)
    const trend4h = ema20_4h > ema50_4h ? 'UP' : 'DOWN';
    const momentum1h = ema20_1h > ema50_1h ? 'UP' : 'DOWN';

    // Price vs VWAP
    const aboveVWAP = lastPrice > vwap1h;

    // BB pozisyonu
    const bbPos = lastPrice > bb15m.upper * 0.98 ? 'TOP'
                : lastPrice < bb15m.lower * 1.02 ? 'BOTTOM'
                : 'MIDDLE';

    // LONG skoru (çoklu zaman dilimi)
    let longScore = 0;
    if (trend4h === 'UP')       longScore += 25; // 4h trend yukarı
    if (momentum1h === 'UP')    longScore += 20; // 1h momentum yukarı
    if (rsi4h < 50)             longScore += 10;
    if (rsi4h < 40)             longScore += 15; // bonus
    if (rsi1h < 50)             longScore += 10;
    if (rsi1h < 40)             longScore += 10; // bonus
    if (rsi15m < 45)            longScore += 10;
    if (bbPos === 'BOTTOM')     longScore += 20; // BB altından dönüş
    if (aboveVWAP)              longScore += 10;
    if (fundRate < 0)           longScore += 15; // Negatif funding = long fırsatı
    if (oiChange > 3)           longScore += 10;
    if (lsRatio < 0.9)          longScore += 15; // Short ağırlıklı = dönüş olabilir
    if (volChange1h > 50)       longScore += 10;
    longScore = Math.min(longScore, 100);

    // SHORT skoru
    let shortScore = 0;
    if (trend4h === 'DOWN')     shortScore += 25;
    if (momentum1h === 'DOWN')  shortScore += 20;
    if (rsi4h > 60)             shortScore += 10;
    if (rsi4h > 70)             shortScore += 15; // bonus
    if (rsi1h > 60)             shortScore += 10;
    if (rsi1h > 70)             shortScore += 10; // bonus
    if (rsi15m > 60)            shortScore += 10;
    if (bbPos === 'TOP')        shortScore += 20; // BB üstünden dönüş
    if (!aboveVWAP)             shortScore += 10;
    if (fundRate > 0.05)        shortScore += 15; // Pozitif funding = short baskı
    if (oiChange < -3)          shortScore += 10;
    if (lsRatio > 1.5)          shortScore += 15; // Long ağırlıklı = düşüş riski
    if (volChange15m > 100)     shortScore += 10;
    shortScore = Math.min(shortScore, 100);

    res.json({
      ok: true,
      symbol: fullSym,
      timeframes: {
        '4h': { rsi: rsi4h, trend: trend4h, ema20: ema20_4h.toFixed(4), ema50: ema50_4h.toFixed(4) },
        '1h': { rsi: rsi1h, trend: momentum1h, ema20: ema20_1h.toFixed(4), ema50: ema50_1h.toFixed(4), vwap: vwap1h.toFixed(4), volChange: volChange1h.toFixed(1) },
        '15m': { rsi: rsi15m, bbPos, volChange: volChange15m.toFixed(1) },
      },
      funding: { rate: fundRate.toFixed(4), signal: fundRate > 0.05 ? 'SHORT_PRESSURE' : fundRate < -0.02 ? 'LONG_PRESSURE' : 'NEUTRAL' },
      openInterest: { change: oiChange.toFixed(2) },
      longShort: { ratio: lsRatio.toFixed(2), signal: lsRatio > 1.5 ? 'TOO_LONG' : lsRatio < 0.7 ? 'TOO_SHORT' : 'BALANCED' },
      longScore,
      shortScore,
      recommendation: longScore > shortScore && longScore >= 50 ? 'LONG'
                    : shortScore > longScore && shortScore >= 50 ? 'SHORT'
                    : 'WAIT',
    });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// ── HESAP ─────────────────────────────────────────────────────────────────────
app.post('/api/account', async (req, res) => {
  const { apiKey, apiSecret } = req.body;
  if (!apiKey || !apiSecret) return res.status(400).json({ error: 'API key gerekli' });
  try {
    const data = await bReq(apiKey, apiSecret, 'GET', '/fapi/v2/account');
    res.json({
      ok: true,
      totalWalletBalance:    parseFloat(data.totalWalletBalance)    || 0,
      availableBalance:      parseFloat(data.availableBalance)      || 0,
      totalUnrealizedProfit: parseFloat(data.totalUnrealizedProfit) || 0,
      positions: (data.positions || [])
        .filter(p => parseFloat(p.positionAmt) !== 0)
        .map(p => ({
          symbol:           p.symbol,
          side:             parseFloat(p.positionAmt) > 0 ? 'LONG' : 'SHORT',
          positionAmt:      Math.abs(parseFloat(p.positionAmt)),
          entryPrice:       parseFloat(p.entryPrice),
          markPrice:        parseFloat(p.markPrice),
          unrealizedProfit: parseFloat(p.unRealizedProfit),
          leverage:         parseInt(p.leverage),
          liquidationPrice: parseFloat(p.liquidationPrice),
        }))
    });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ── KALDIRAÇ AYARLA ───────────────────────────────────────────────────────────
app.post('/api/leverage', async (req, res) => {
  const { apiKey, apiSecret, symbol, leverage } = req.body;
  if (!apiKey || !apiSecret || !symbol || !leverage)
    return res.status(400).json({ error: 'Eksik parametre' });
  const sym = symbol.toUpperCase().includes('USDT') ? symbol.toUpperCase() : symbol.toUpperCase() + 'USDT';
  try {
    const data = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/leverage', { symbol: sym, leverage: parseInt(leverage) });
    res.json({ ok: true, leverage: data.leverage, symbol: data.symbol });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ── MARJİN MODU AYARLA ────────────────────────────────────────────────────────
app.post('/api/margin-type', async (req, res) => {
  const { apiKey, apiSecret, symbol, marginType } = req.body; // ISOLATED / CROSSED
  if (!apiKey || !apiSecret || !symbol || !marginType)
    return res.status(400).json({ error: 'Eksik parametre' });
  const sym = symbol.toUpperCase().includes('USDT') ? symbol.toUpperCase() : symbol.toUpperCase() + 'USDT';
  try {
    const data = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/marginType', { symbol: sym, marginType: marginType.toUpperCase() });
    res.json({ ok: true, message: `${sym} margin type: ${marginType}` });
  } catch (e) {
    // Zaten o modda olabilir
    if (e.message.includes('No need to change')) return res.json({ ok: true, message: 'Zaten ayarlı' });
    res.status(400).json({ error: e.message });
  }
});

// ── EMİR AÇ ──────────────────────────────────────────────────────────────────
app.post('/api/order', async (req, res) => {
  const { apiKey, apiSecret, symbol, side, leverage, marginType,
          entryPrice, targetPrice, stopPrice, usdtAmount } = req.body;

  if (!apiKey || !apiSecret || !symbol || !side || !leverage ||
      !entryPrice || !targetPrice || !stopPrice || !usdtAmount)
    return res.status(400).json({ error: 'Eksik parametre' });

  const sym    = symbol.toUpperCase().includes('USDT') ? symbol.toUpperCase() : symbol.toUpperCase() + 'USDT';
  const isLong = side.toUpperCase() === 'LONG';

  try {
    // 1. Marjin tipi ayarla
    if (marginType) {
      try { await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/marginType', { symbol: sym, marginType: marginType.toUpperCase() }); }
      catch (e) { if (!e.message.includes('No need')) console.log('MarginType:', e.message); }
    }

    // 2. Kaldıraç ayarla
    await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/leverage', { symbol: sym, leverage: parseInt(leverage) });

    // 3. Sembol bilgisi - sadece o sembol için çek (hızlı)
    let stepSize = 0.001, tickSize = 0.01, minNot = 5;
    try {
      const siRes = await fetch(FAPI + '/fapi/v1/exchangeInfo?symbol=' + sym);
      const siData = await siRes.json();
      const si = Array.isArray(siData.symbols) ? siData.symbols.find(s => s.symbol === sym) : null;
      if (si) {
        const lotF = si.filters.find(f => f.filterType === 'LOT_SIZE');
        const prcF = si.filters.find(f => f.filterType === 'PRICE_FILTER');
        const minF = si.filters.find(f => f.filterType === 'MIN_NOTIONAL');
        if (lotF) stepSize = parseFloat(lotF.stepSize);
        if (prcF) tickSize = parseFloat(prcF.tickSize);
        if (minF) minNot = parseFloat(minF.notional || minF.minNotional || 5);
      }
    } catch(e) { console.log('ExchangeInfo fallback:', e.message); }

    const ep = parseFloat(entryPrice);
    const rawQty = (parseFloat(usdtAmount) * parseInt(leverage)) / ep;

    const qtyPrecision   = stepSize < 1 ? -Math.floor(Math.log10(stepSize)) : 0;
    const pricePrecision = tickSize < 1 ? -Math.floor(Math.log10(tickSize)) : 0;

    const qty  = parseFloat(rawQty.toFixed(qtyPrecision));
    const rnd  = p => parseFloat(parseFloat(p).toFixed(pricePrecision));

    if (qty * ep < minNot) throw new Error(`Minimum işlem büyüklüğü $${minNot} USDT. Miktarı artır.`);

    const orderSide = isLong ? 'BUY'  : 'SELL';
    const closeSide = isLong ? 'SELL' : 'BUY';

    // 4. Ana emir
    const mainOrder = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
      symbol: sym, side: orderSide, type: 'LIMIT',
      timeInForce: 'GTC', quantity: qty, price: rnd(ep), positionSide: 'BOTH'
    });

    // 5. Take Profit
    const tpOrder = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
      symbol: sym, side: closeSide, type: 'TAKE_PROFIT_MARKET',
      stopPrice: rnd(targetPrice), closePosition: 'true', timeInForce: 'GTC', positionSide: 'BOTH'
    });

    // 6. Stop Loss
    const slOrder = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
      symbol: sym, side: closeSide, type: 'STOP_MARKET',
      stopPrice: rnd(stopPrice), closePosition: 'true', timeInForce: 'GTC', positionSide: 'BOTH'
    });

    res.json({
      ok: true,
      message: `${sym} ${side} emri açıldı ✅`,
      mainOrderId: mainOrder.orderId,
      tpOrderId:   tpOrder.orderId,
      slOrderId:   slOrder.orderId,
      details: { symbol: sym, side, quantity: qty, leverage, marginType: marginType || 'ISOLATED',
                 entry: rnd(ep), target: rnd(targetPrice), stop: rnd(stopPrice) }
    });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ── POZİSYONLAR ──────────────────────────────────────────────────────────────
app.post('/api/positions', async (req, res) => {
  const { apiKey, apiSecret } = req.body;
  if (!apiKey || !apiSecret) return res.status(400).json({ error: 'API key gerekli' });
  try {
    const data = await bReq(apiKey, apiSecret, 'GET', '/fapi/v2/positionRisk');
    const open = data.filter(p => parseFloat(p.positionAmt) !== 0).map(p => {
      const amt  = parseFloat(p.positionAmt);
      const pnl  = parseFloat(p.unRealizedProfit);
      const ep   = parseFloat(p.entryPrice);
      const mp   = parseFloat(p.markPrice);
      const lev  = parseInt(p.leverage);
      const side = amt > 0 ? 'LONG' : 'SHORT';
      const pct  = ep > 0 ? ((mp - ep) / ep * 100 * lev * (side === 'SHORT' ? -1 : 1)).toFixed(2) : '0';
      return { symbol: p.symbol, side, positionAmt: Math.abs(amt), entryPrice: ep,
               markPrice: mp, unrealizedProfit: pnl, pnlPct: parseFloat(pct),
               leverage: lev, liquidationPrice: parseFloat(p.liquidationPrice) };
    });
    res.json({ ok: true, positions: open });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ── KAPAT ─────────────────────────────────────────────────────────────────────
app.post('/api/close', async (req, res) => {
  const { apiKey, apiSecret, symbol } = req.body;
  if (!apiKey || !apiSecret || !symbol) return res.status(400).json({ error: 'Eksik parametre' });
  const sym = symbol.toUpperCase().includes('USDT') ? symbol.toUpperCase() : symbol.toUpperCase() + 'USDT';
  try {
    await bReq(apiKey, apiSecret, 'DELETE', '/fapi/v1/allOpenOrders', { symbol: sym });
    const pos    = await bReq(apiKey, apiSecret, 'GET', '/fapi/v2/positionRisk', { symbol: sym });
    const openP  = pos.find(p => Math.abs(parseFloat(p.positionAmt)) > 0);
    if (!openP) return res.json({ ok: true, message: 'Açık pozisyon yok' });
    const qty    = Math.abs(parseFloat(openP.positionAmt));
    const isLong = parseFloat(openP.positionAmt) > 0;
    const order  = await bReq(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
      symbol: sym, side: isLong ? 'SELL' : 'BUY',
      type: 'MARKET', quantity: qty, reduceOnly: 'true', positionSide: 'BOTH'
    });
    res.json({ ok: true, message: `${sym} kapatıldı`, orderId: order.orderId });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.listen(PORT, () => console.log(`✅ Server ${PORT} portunda çalışıyor`));
