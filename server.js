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

    // ── STOP HUNT ─────────────────────────────────────────────────────────────
    function detectStopHunt(klines,n=10){
      if(klines.length<n)return{hunted:false,direction:'NONE'};
      const recent=klines.slice(-n);
      for(let i=recent.length-3;i<recent.length;i++){
        const o=parseFloat(recent[i][1]),h=parseFloat(recent[i][2]),l=parseFloat(recent[i][3]),c=parseFloat(recent[i][4]);
        const body=Math.abs(c-o),range=h-l;if(range===0)continue;
        const uw=h-Math.max(o,c),lw=Math.min(o,c)-l;
        if(uw/range>0.6&&uw>body*2)return{hunted:true,direction:'BEAR_HUNT',wickPct:+(uw/lastPrice*100).toFixed(2),msg:'Üst wick → dönüş ↓'};
        if(lw/range>0.6&&lw>body*2)return{hunted:true,direction:'BULL_HUNT',wickPct:+(lw/lastPrice*100).toFixed(2),msg:'Alt wick → dönüş ↑'};
      }
      return{hunted:false,direction:'NONE'};
    }
    const hunt1h=detectStopHunt(k1h,15);
    const hunt15m=detectStopHunt(k15m,8);

    // ── ORDER BLOCKS ──────────────────────────────────────────────────────────
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

    // ── ORDER BOOK (snapshot) ─────────────────────────────────────────────────
    const bids=Array.isArray(depth.bids)?depth.bids.slice(0,30):[];
    const asks=Array.isArray(depth.asks)?depth.asks.slice(0,30):[];
    let totBid=0,totAsk=0;
    bids.forEach(([p,q])=>totBid+=parseFloat(p)*parseFloat(q));
    asks.forEach(([p,q])=>totAsk+=parseFloat(p)*parseFloat(q));
    const bookImb=totBid+totAsk>0?(totBid-totAsk)/(totBid+totAsk)*100:0;

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

    longScore =Math.min(Math.round(longScore*freshnessMult),100);
    shortScore=Math.min(Math.round(shortScore*freshnessMult),100);
    const recommendation=longScore>shortScore&&longScore>=50?'LONG':shortScore>longScore&&shortScore>=50?'SHORT':'WAIT';
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
      orderBlocks:{'1h':ob1h,'4h':ob4h},
      // WS gerçek zamanlı veriler
      cvd, iceberg,
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
      longScore, shortScore, recommendation,
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
          // -4120: algo gerekiyor → tüm formatları atla
          if (m.includes('-4120')) break;
          // -1111/-2021/-2013: fiyat sorunu → devam et
        }
      }
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

app.listen(PORT, ()=>console.log(`✅ Server ${PORT}`));
