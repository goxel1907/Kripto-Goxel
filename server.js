const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const fetch = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const BINANCE_BASE = 'https://fapi.binance.com'; // Futures API

// ── BINANCE İMZA ─────────────────────────────────────────────────────────────
function sign(queryString, secret) {
  return crypto
    .createHmac('sha256', secret)
    .update(queryString)
    .digest('hex');
}

async function binanceRequest(apiKey, apiSecret, method, path, params = {}) {
  const timestamp = Date.now();
  const queryObj = { ...params, timestamp };
  const queryString = Object.entries(queryObj)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join('&');
  const signature = sign(queryString, apiSecret);
  
  const options = {
    method,
    headers: {
      'X-MBX-APIKEY': apiKey,
      'Content-Type': 'application/x-www-form-urlencoded'
    }
  };

  let url = `${BINANCE_BASE}${path}`;
  
  if (method === 'GET') {
    // GET: signature ve parametreler URL'de
    url += `?${queryString}&signature=${signature}`;
  } else {
    // POST/DELETE: parametreler ve signature body'de
    options.body = `${queryString}&signature=${signature}`;
  }

  const res = await fetch(url, options);
  const data = await res.json();
  
  // Hata kontrolü
  if (data.code && data.code < 0) {
    throw new Error(`Binance Error ${data.code}: ${data.msg || 'Bilinmeyen hata'}`);
  }
  
  return data;
}

// ── SAĞLIK KONTROLÜ ──────────────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({ status: 'ok', message: 'Kripto Sinyal Backend çalışıyor', time: new Date().toISOString() });
});

// ── HESAP BİLGİSİ (API KEY TEST) ─────────────────────────────────────────────
app.post('/api/account', async (req, res) => {
  const { apiKey, apiSecret } = req.body;
  if (!apiKey || !apiSecret) return res.status(400).json({ error: 'API key ve secret gerekli' });

  try {
    // Binance USD-M Futures /fapi/v2/account endpoint'i
    const accountData = await binanceRequest(apiKey, apiSecret, 'GET', '/fapi/v2/account', {});
    
    const totalWalletBalance = parseFloat(accountData.totalWalletBalance || 0) || 0;
    const availableBalance = parseFloat(accountData.availableBalance || 0) || 0;
    const totalUnrealizedProfit = parseFloat(accountData.totalUnrealizedProfit || 0) || 0;

    res.json({
      ok: true,
      source: 'fapi/v2/account',
      asset: 'USDT',
      totalWalletBalance,
      availableBalance,
      totalUnrealizedProfit,
      positions: (accountData.positions || []).filter(p => parseFloat(p.positionAmt) !== 0).map(p => ({
        symbol: p.symbol,
        positionAmt: p.positionAmt,
        entryPrice: p.entryPrice,
        unrealizedProfit: p.unrealizedProfit ?? p.unRealizedProfit,
        leverage: p.leverage,
        markPrice: p.markPrice
      }))
    });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// Bakiye teşhis endpointi
app.post('/api/account-debug', async (req, res) => {
  const { apiKey, apiSecret } = req.body;
  if (!apiKey || !apiSecret) return res.status(400).json({ error: 'API key ve secret gerekli' });
  try {
    const accountResult = await binanceRequest(apiKey, apiSecret, 'GET', '/fapi/v2/account', {});
    
    res.json({
      ok: true,
      debug: 'POST body yöntemi kullanılıyor',
      accountTotals: {
        totalWalletBalance: accountResult.totalWalletBalance,
        availableBalance: accountResult.availableBalance,
        totalUnrealizedProfit: accountResult.totalUnrealizedProfit,
        makerCommission: accountResult.makerCommission,
        takerCommission: accountResult.takerCommission
      },
      positionCount: (accountResult.positions || []).length,
      apiKeyValid: true
    });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// ── KALDIRAÇ AYARLA ────────────────────────────────────────────────────────────
app.post('/api/leverage', async (req, res) => {
  const { apiKey, apiSecret, symbol, leverage } = req.body;
  if (!apiKey || !apiSecret || !symbol || !leverage)
    return res.status(400).json({ error: 'Eksik parametre' });
  try {
    const data = await binanceRequest(apiKey, apiSecret, 'POST', '/fapi/v1/leverage', {
      symbol: symbol.toUpperCase() + 'USDT',
      leverage: parseInt(leverage)
    });
    res.json({ ok: true, leverage: data.leverage, symbol: data.symbol });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// ── EMİR AÇ (LONG - TP - SL) ─────────────────────────────────────────────────
app.post('/api/order', async (req, res) => {
  const { apiKey, apiSecret, symbol, leverage, entryPrice, targetPrice, stopPrice, usdtAmount } = req.body;

  if (!apiKey || !apiSecret || !symbol || !leverage || !entryPrice || !targetPrice || !stopPrice || !usdtAmount)
    return res.status(400).json({ error: 'Eksik parametre' });

  const sym = symbol.toUpperCase() + 'USDT';

  try {
    // 1. Kaldıraç ayarla
    await binanceRequest(apiKey, apiSecret, 'POST', '/fapi/v1/leverage', {
      symbol: sym, leverage: parseInt(leverage)
    });

    // 2. Fiyat bilgisi al (lot size için)
    const exchangeInfo = await fetch(`${BINANCE_BASE}/fapi/v1/exchangeInfo`).then(r => r.json());
    const symbolInfo = exchangeInfo.symbols.find(s => s.symbol === sym);
    if (!symbolInfo) throw new Error(`${sym} bulunamadı`);

    // Lot size ve price precision bul
    const lotFilter = symbolInfo.filters.find(f => f.filterType === 'LOT_SIZE');
    const priceFilter = symbolInfo.filters.find(f => f.filterType === 'PRICE_FILTER');
    const stepSize = parseFloat(lotFilter?.stepSize || '0.001');
    const tickSize = parseFloat(priceFilter?.tickSize || '0.01');

    // Miktar hesapla (USDT miktarı / giriş fiyatı * kaldıraç)
    const rawQty = (parseFloat(usdtAmount) * parseInt(leverage)) / parseFloat(entryPrice);
    const precision = stepSize < 1 ? Math.abs(Math.floor(Math.log10(stepSize))) : 0;
    const qty = parseFloat(rawQty.toFixed(precision));

    // Price precision
    const pricePrecision = tickSize < 1 ? Math.abs(Math.floor(Math.log10(tickSize))) : 0;
    const roundPrice = (p) => parseFloat(parseFloat(p).toFixed(pricePrecision));

    // 3. Ana LONG emri (LIMIT)
    const mainOrder = await binanceRequest(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
      symbol: sym,
      side: 'BUY',
      type: 'LIMIT',
      timeInForce: 'GTC',
      quantity: qty,
      price: roundPrice(entryPrice),
      positionSide: 'BOTH'
    });

    // 4. Take Profit emri
    const tpOrder = await binanceRequest(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
      symbol: sym,
      side: 'SELL',
      type: 'TAKE_PROFIT_MARKET',
      stopPrice: roundPrice(targetPrice),
      closePosition: 'true',
      timeInForce: 'GTC',
      positionSide: 'BOTH'
    });

    // 5. Stop Loss emri
    const slOrder = await binanceRequest(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
      symbol: sym,
      side: 'SELL',
      type: 'STOP_MARKET',
      stopPrice: roundPrice(stopPrice),
      closePosition: 'true',
      timeInForce: 'GTC',
      positionSide: 'BOTH'
    });

    res.json({
      ok: true,
      message: `${sym} LONG emri açıldı`,
      mainOrderId: mainOrder.orderId,
      tpOrderId: tpOrder.orderId,
      slOrderId: slOrder.orderId,
      details: {
        symbol: sym,
        quantity: qty,
        leverage: leverage,
        entry: roundPrice(entryPrice),
        target: roundPrice(targetPrice),
        stop: roundPrice(stopPrice),
        usdtAmount: usdtAmount
      }
    });

  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// ── AÇIK POZİSYONLAR ─────────────────────────────────────────────────────────
app.post('/api/positions', async (req, res) => {
  const { apiKey, apiSecret } = req.body;
  if (!apiKey || !apiSecret) return res.status(400).json({ error: 'API key gerekli' });
  try {
    const data = await binanceRequest(apiKey, apiSecret, 'GET', '/fapi/v2/positionRisk', {});
    const open = data.filter(p => parseFloat(p.positionAmt) !== 0).map(p => ({
      symbol: p.symbol,
      positionAmt: parseFloat(p.positionAmt),
      entryPrice: parseFloat(p.entryPrice),
      markPrice: parseFloat(p.markPrice),
      unrealizedProfit: parseFloat(p.unRealizedProfit),
      percentage: parseFloat(p.entryPrice) > 0
        ? ((parseFloat(p.markPrice) - parseFloat(p.entryPrice)) / parseFloat(p.entryPrice) * 100 * parseFloat(p.leverage)).toFixed(2)
        : '0',
      leverage: p.leverage,
      liquidationPrice: parseFloat(p.liquidationPrice)
    }));
    res.json({ ok: true, positions: open });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// ── POZİSYON KAPAT ────────────────────────────────────────────────────────────
app.post('/api/close', async (req, res) => {
  const { apiKey, apiSecret, symbol } = req.body;
  if (!apiKey || !apiSecret || !symbol) return res.status(400).json({ error: 'Eksik parametre' });
  const sym = symbol.toUpperCase().includes('USDT') ? symbol.toUpperCase() : symbol.toUpperCase() + 'USDT';
  try {
    // Önce tüm açık emirleri iptal et
    await binanceRequest(apiKey, apiSecret, 'DELETE', '/fapi/v1/allOpenOrders', { symbol: sym });
    // Pozisyonu market fiyatından kapat
    const pos = await binanceRequest(apiKey, apiSecret, 'GET', '/fapi/v2/positionRisk', { symbol: sym });
    const openPos = pos.find(p => Math.abs(parseFloat(p.positionAmt)) > 0);
    if (!openPos) return res.json({ ok: true, message: 'Açık pozisyon yok' });
    const qty = Math.abs(parseFloat(openPos.positionAmt));
    const closeOrder = await binanceRequest(apiKey, apiSecret, 'POST', '/fapi/v1/order', {
      symbol: sym, side: 'SELL', type: 'MARKET',
      quantity: qty, reduceOnly: 'true', positionSide: 'BOTH'
    });
    res.json({ ok: true, message: `${sym} pozisyonu kapatıldı`, orderId: closeOrder.orderId });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// ── WEBSOCKET FİYAT VERİSİ (REST proxy) ─────────────────────────────────────
app.get('/api/price/:symbol', async (req, res) => {
  try {
    const sym = req.params.symbol.toUpperCase() + 'USDT';
    const data = await fetch(`${BINANCE_BASE}/fapi/v1/ticker/24hr?symbol=${sym}`).then(r => r.json());
    res.json({
      symbol: sym,
      price: parseFloat(data.lastPrice),
      change24h: parseFloat(data.priceChangePercent),
      volume: parseFloat(data.quoteVolume),
      high: parseFloat(data.highPrice),
      low: parseFloat(data.lowPrice)
    });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// ── FUNDING RATE + OPEN INTEREST ─────────────────────────────────────────────
app.get('/api/market/:symbol', async (req, res) => {
  try {
    const sym = req.params.symbol.toUpperCase() + 'USDT';
    const [funding, oi] = await Promise.allSettled([
      fetch(`${BINANCE_BASE}/fapi/v1/fundingRate?symbol=${sym}&limit=1`).then(r => r.json()),
      fetch(`${BINANCE_BASE}/fapi/v1/openInterest?symbol=${sym}`).then(r => r.json())
    ]);
    res.json({
      symbol: sym,
      fundingRate: funding.status === 'fulfilled' && funding.value[0]
        ? parseFloat(funding.value[0].fundingRate) * 100
        : null,
      openInterest: oi.status === 'fulfilled' ? parseFloat(oi.value.openInterest) : null
    });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.listen(PORT, () => {
  console.log(`Server ${PORT} portunda çalışıyor`);
});
