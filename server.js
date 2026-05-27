const express = require('express');
const cors    = require('cors');
const crypto  = require('crypto');
const fetch   = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const FAPI = 'https://fapi.binance.com';

// ── CACHE ─────────────────────────────────────────────────────────────────────
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

// ── RATE LIMIT KORUMASI ───────────────────────────────────────────────────────
let reqCount = 0, reqWindow = Date.now();
async function bPub(path, qs = '') {
  const now = Date.now();
  if (now - reqWindow > 60000) { reqCount = 0; reqWindow = now; }
  reqCount++;
  if (reqCount > 800) {
    const wait = 60000 - (now - reqWindow);
    await new Promise(r => setTimeout(r, wait + 1000));
    reqCount = 0; reqWindow = Date.now();
  }
  await new Promise(r => setTimeout(r, 100));
  const url = `${FAPI}${path}${qs ? '?' + qs : ''}`;
  const r = await fetch(url, { signal: AbortSignal.timeout(10000) });
  if (r.status === 429 || r.status === 418) {
    const retry = parseInt(r.headers.get('Retry-After') || '30');
    await new Promise(r => setTimeout(r, retry * 1000));
    return bPub(path, qs);
  }
  return r.json();
}

// ── İMZALI İSTEK ─────────────────────────────────────────────────────────────
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
  const isGet = method.toUpperCase() === 'GET' || method.toUpperCase() === 'DELETE';
  const finalUrl = isGet ? `${url}?${fullQs}` : url;
  if (!isGet) options.body = fullQs;
  const res  = await fetch(finalUrl, options);
  const data = await res.json();
  if (data.code && data.code < 0) throw new Error(`${data.msg} (${data.code})`);
  return data;
}

app.get('/', (req, res) => res.json({ status: 'ok', time: new Date().toISOString() }));

// ── FUTURES COİN LİSTESİ ─────────────────────────────────────────────────────
app.get('/api/futures-coins', async (req, res) => {
  try {
    const data = await cached('futures_tickers', 15*60*1000, () => bPub('/fapi/v1/ticker/24hr'));
    if (!Array.isArray(data)) return res.json({ coins: [] });
    const EXCL = new Set(['BTCUSDT','ETHUSDT','BNBUSDT','XRPUSDT','SOLUSDT',
      'ADAUSDT','DOGEUSDT','DOTUSDT','MATICUSDT','LTCUSDT','TRXUSDT','AVAXUSDT',
      'LINKUSDT','UNIUSDT','WBTCUSDT','SHIBUSDT']);
    const coins = data
      .filter(t => t.symbol.endsWith('USDT') && !EXCL.has(t.symbol))
      .map(t => ({
        symbol:    t.symbol.replace('USDT',''),
        fullSymbol:t.symbol,
        price:     parseFloat(t.lastPrice)          || 0,
        change24h: parseFloat(t.priceChangePercent) || 0,
        volume:    parseFloat(t.quoteVolume)        || 0,
        high:      parseFloat(t.highPrice)          || 0,
        low:       parseFloat(t.lowPrice)           || 0,
        trades:    parseInt(t.count)               || 0,
      }))
      .filter(c => c.volume > 20000000);
    res.json({ ok: true, count: coins.length, coins });
  } catch(e) { res.status(400).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ── PRO ANALİZ ENGİNİ ─────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════
app.get('/api/analyze/:symbol', async (req, res) => {
  const sym  = req.params.symbol.toUpperCase();
  const full = sym.endsWith('USDT') ? sym : sym + 'USDT';

  try {
    const [r4h, r1h, r15m, r5m, rFunding, rOIHist, rLS_global, rLS_top, rTakerRatio, rTrades, rDepth] =
      await Promise.allSettled([
        cached(`k4h_${full}`,  30*60*1000, () => bPub('/fapi/v1/klines', `symbol=${full}&interval=4h&limit=200`)),
        cached(`k1h_${full}`,  10*60*1000, () => bPub('/fapi/v1/klines', `symbol=${full}&interval=1h&limit=200`)),
        cached(`k15m_${full}`,  5*60*1000, () => bPub('/fapi/v1/klines', `symbol=${full}&interval=15m&limit=200`)),
        cached(`k5m_${full}`,   3*60*1000, () => bPub('/fapi/v1/klines', `symbol=${full}&interval=5m&limit=100`)),
        cached(`fund_${full}`, 30*60*1000, () => bPub('/fapi/v1/fundingRate', `symbol=${full}&limit=10`)),
        cached(`oih_${full}`,  15*60*1000, () => bPub('/futures/data/openInterestHist', `symbol=${full}&period=1h&limit=24`)),
        cached(`lsg_${full}`,  15*60*1000, () => bPub('/futures/data/globalLongShortAccountRatio', `symbol=${full}&period=1h&limit=12`)),
        cached(`lst_${full}`,  15*60*1000, () => bPub('/futures/data/topLongShortPositionRatio', `symbol=${full}&period=1h&limit=12`)),
        cached(`tkr_${full}`,   5*60*1000, () => bPub('/futures/data/takerlongshortRatio', `symbol=${full}&period=5m&limit=48`)),
        cached(`trd_${full}`,   3*60*1000, () => bPub('/fapi/v1/aggTrades', `symbol=${full}&limit=1000`)),
        cached(`dep_${full}`,   2*60*1000, () => bPub('/fapi/v1/depth', `symbol=${full}&limit=100`)),
      ]);

    const k4h  = r4h.status==='fulfilled'&&Array.isArray(r4h.value)   ? r4h.value   : [];
    const k1h  = r1h.status==='fulfilled'&&Array.isArray(r1h.value)   ? r1h.value   : [];
    const k15m = r15m.status==='fulfilled'&&Array.isArray(r15m.value) ? r15m.value  : [];
    const k5m  = r5m.status==='fulfilled'&&Array.isArray(r5m.value)   ? r5m.value   : [];
    const fundArr   = rFunding.status==='fulfilled'&&Array.isArray(rFunding.value)    ? rFunding.value    : [];
    const oiHist    = rOIHist.status==='fulfilled'&&Array.isArray(rOIHist.value)      ? rOIHist.value     : [];
    const lsGlobal  = rLS_global.status==='fulfilled'&&Array.isArray(rLS_global.value)? rLS_global.value  : [];
    const lsTop     = rLS_top.status==='fulfilled'&&Array.isArray(rLS_top.value)      ? rLS_top.value     : [];
    const takerRatio= rTakerRatio.status==='fulfilled'&&Array.isArray(rTakerRatio.value)?rTakerRatio.value: [];
    const aggTrades = rTrades.status==='fulfilled'&&Array.isArray(rTrades.value)      ? rTrades.value     : [];
    const depth     = rDepth.status==='fulfilled' ? rDepth.value : { bids:[], asks:[] };

    const lastPrice = k15m.length ? parseFloat(k15m[k15m.length-1][4]) : 0;
    const lastTime  = k15m.length ? parseInt(k15m[k15m.length-1][6])   : Date.now(); // kline kapanış zamanı

    // ── TEKNİK FONKSİYONLAR ──────────────────────────────────────────────────
    function rsi(kl,p=14){
      if(kl.length<p+1)return 50;
      const c=kl.map(k=>parseFloat(k[4]));
      let g=0,l=0;
      for(let i=c.length-p;i<c.length;i++){const d=c[i]-c[i-1];d>0?g+=d:l-=d;}
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
      });
      return trs.slice(1).reduce((a,b)=>a+b,0)/p;
    }
    function vwap(kl){
      let tv=0,v=0;
      kl.forEach(k=>{const tp=(parseFloat(k[2])+parseFloat(k[3])+parseFloat(k[4]))/3;const vol=parseFloat(k[5]);tv+=tp*vol;v+=vol;});
      return v>0?tv/v:0;
    }

    const rsi4h=rsi(k4h),rsi1h=rsi(k1h),rsi15m=rsi(k15m),rsi5m=rsi(k5m);
    const ema20_4h=ema(k4h,20),ema50_4h=ema(k4h,50),ema200_4h=ema(k4h,200);
    const ema20_1h=ema(k1h,20),ema50_1h=ema(k1h,50),ema200_1h=ema(k1h,200);
    const atr4h=atr(k4h),atr1h=atr(k1h),atr15m_=atr(k15m);
    const vwap1h=vwap(k1h);

    // ── SİNYAL YAŞI & GEÇERLİLİK ─────────────────────────────────────────────
    // Son 15m mumun kapanışından bu yana geçen süre
    const signalAgeMs  = Date.now() - lastTime;
    const signalAgeMin = Math.floor(signalAgeMs / 60000);
    // 4h RSI varsa sinyal max 4 saat geçerli, 1h RSI varsa 2 saat
    const maxValidMin  = rsi4h < 40 || rsi4h > 60 ? 240 : 120;
    const isExpired    = signalAgeMin > maxValidMin;
    const freshness    = isExpired ? 'EXPIRED' : signalAgeMin < 15 ? 'FRESH' : signalAgeMin < 60 ? 'VALID' : 'AGING';

    // ── LİKİDİTE SEVİYELERİ ──────────────────────────────────────────────────
    function findLiqLevels(klines, lookback=50){
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
        buyLiq: buyLiq.filter(l=>l.price>lastPrice).sort((a,b)=>a.price-b.price).slice(0,3),
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
        const body=Math.abs(c-o),range=h-l;
        if(range===0)continue;
        const uw=h-Math.max(o,c),lw=Math.min(o,c)-l;
        if(uw/range>0.6&&uw>body*2)return{hunted:true,direction:'BEAR_HUNT',wickPct:+(uw/lastPrice*100).toFixed(2),msg:'Üst wick stop hunt ↓'};
        if(lw/range>0.6&&lw>body*2)return{hunted:true,direction:'BULL_HUNT',wickPct:+(lw/lastPrice*100).toFixed(2),msg:'Alt wick stop hunt ↑'};
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
    const ob1h=findOB(k1h);
    const ob4h=findOB(k4h);

    // ── MM DIRECTION ──────────────────────────────────────────────────────────
    const upLiqStr=liq1h.buyLiq.reduce((s,l)=>s+l.strength*(1/Math.max(l.distPct,0.1)),0);
    const dnLiqStr=liq1h.sellLiq.reduce((s,l)=>s+l.strength*(1/Math.max(l.distPct,0.1)),0);
    const globalLong=lsGlobal.length?parseFloat(lsGlobal[lsGlobal.length-1].longAccount||0.5)*100:50;
    const topLong   =lsTop.length?parseFloat(lsTop[lsTop.length-1].longAccount||0.5)*100:50;
    let oiTrend='FLAT';
    if(oiHist.length>=4){
      const fn=x=>parseFloat(x.sumOpenInterest||0);
      const lat=fn(oiHist[oiHist.length-1]),old=fn(oiHist[oiHist.length-4]);
      oiTrend=lat>old*1.02?'RISING':lat<old*0.98?'FALLING':'FLAT';
    }
    const retailBias=globalLong>65?'TOO_LONG':globalLong<35?'TOO_SHORT':'NEUTRAL';
    const smBias=topLong>60?'UP':topLong<40?'DOWN':'NEUTRAL';
    let mmTarget='UNKNOWN',mmConf=0,mmReasoning=[];
    if(upLiqStr>dnLiqStr*1.5){mmTarget='UP_SWEEP';mmConf+=25;mmReasoning.push(`Üstte liq havuzu $${liq1h.buyLiq[0]?.price?.toFixed(4)||'?'}`);}
    else if(dnLiqStr>upLiqStr*1.5){mmTarget='DOWN_SWEEP';mmConf+=25;mmReasoning.push(`Altta liq havuzu $${liq1h.sellLiq[0]?.price?.toFixed(4)||'?'}`);}
    if(retailBias==='TOO_LONG'){mmConf+=20;if(mmTarget!=='UP_SWEEP')mmTarget='DOWN_SWEEP';mmReasoning.push(`Perakende %${globalLong.toFixed(0)} long`);}
    else if(retailBias==='TOO_SHORT'){mmConf+=20;if(mmTarget!=='DOWN_SWEEP')mmTarget='UP_SWEEP';mmReasoning.push(`Perakende %${(100-globalLong).toFixed(0)} short`);}
    if(smBias==='UP'&&oiTrend==='RISING'){mmTarget='GENUINE_UP';mmConf+=30;mmReasoning.push('Whale long + OI artışı');}
    else if(smBias==='DOWN'&&oiTrend==='RISING'){mmTarget='GENUINE_DOWN';mmConf+=30;mmReasoning.push('Whale short + OI artışı');}
    mmConf=Math.min(mmConf,95);
    const mmNextTarget=mmTarget.includes('UP')?(liq1h.buyLiq[0]?.price||0):(liq1h.sellLiq[0]?.price||0);

    // ── CVD ───────────────────────────────────────────────────────────────────
    let cvdBuy=0,cvdSell=0,recBuy=0,recSell=0;
    const mid=Math.floor(aggTrades.length/2);
    aggTrades.forEach((t,i)=>{const q=parseFloat(t.q)*parseFloat(t.p);if(t.m){cvdSell+=q;if(i>=mid)recSell+=q;}else{cvdBuy+=q;if(i>=mid)recBuy+=q;}});
    const cvdTotal=cvdBuy-cvdSell;
    const cvdRatio=cvdBuy+cvdSell>0?cvdBuy/(cvdBuy+cvdSell)*100:50;
    const cvdMom=recBuy-recSell>(cvdBuy-cvdSell-(recBuy-recSell))*1.3?'ACCELERATING_BULL':recBuy-recSell<(cvdBuy-cvdSell-(recBuy-recSell))*0.7?'ACCELERATING_BEAR':cvdTotal>0?'POSITIVE':'NEGATIVE';

    // ── FUNDING ───────────────────────────────────────────────────────────────
    const curFund=fundArr.length?parseFloat(fundArr[fundArr.length-1].fundingRate)*100:0;
    const fundSig=curFund<-0.05?'EXTREME_NEGATIVE':curFund<-0.01?'NEGATIVE':curFund>0.1?'EXTREME_POSITIVE':curFund>0.05?'POSITIVE':'NEUTRAL';
    const fundAnn=+(curFund*3*365).toFixed(1);

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
    const smDiv=topLong>55&&globalLong<45?'SMART_BULL':topLong<45&&globalLong>55?'SMART_BEAR':topLong>60?'WHALE_LONG':topLong<40?'WHALE_SHORT':'NEUTRAL';

    // ── ORDER BOOK ────────────────────────────────────────────────────────────
    const bids=Array.isArray(depth.bids)?depth.bids.slice(0,30):[];
    const asks=Array.isArray(depth.asks)?depth.asks.slice(0,30):[];
    let totBid=0,totAsk=0;
    bids.forEach(([p,q])=>totBid+=parseFloat(p)*parseFloat(q));
    asks.forEach(([p,q])=>totAsk+=parseFloat(p)*parseFloat(q));
    const bookImb=totBid+totAsk>0?(totBid-totAsk)/(totBid+totAsk)*100:0;
    const maxBid=bids.reduce((m,[p,q])=>Math.max(m,parseFloat(p)*parseFloat(q)),0);
    const maxAsk=asks.reduce((m,[p,q])=>Math.max(m,parseFloat(p)*parseFloat(q)),0);
    const bookSig=bookImb>20?'STRONG_BIDS':bookImb<-20?'STRONG_ASKS':bookImb>10?'SLIGHT_BIDS':bookImb<-10?'SLIGHT_ASKS':'BALANCED';

    // ── TAKER FLOW ────────────────────────────────────────────────────────────
    let takerBuy=50;
    if(takerRatio.length>=3){
      const r3=takerRatio.slice(-3);
      const tb=r3.reduce((s,t)=>s+parseFloat(t.buyVol||0),0);
      const ts=r3.reduce((s,t)=>s+parseFloat(t.sellVol||0),0);
      takerBuy=tb+ts>0?tb/(tb+ts)*100:50;
    }
    const takerTrend=takerBuy>60?'AGGRESSIVE_BUYING':takerBuy<40?'AGGRESSIVE_SELLING':'NEUTRAL';

    // ── SKOR ─────────────────────────────────────────────────────────────────
    let longScore=0,shortScore=0;
    const signals={long:[],short:[]};

    // MM (ağır)
    if(mmTarget==='GENUINE_UP')    {longScore+=28;signals.long.push('🎯 MM Gerçek ↑');}
    if(mmTarget==='GENUINE_DOWN')  {shortScore+=28;signals.short.push('🎯 MM Gerçek ↓');}
    if(mmTarget==='UP_SWEEP')      {longScore+=12;signals.long.push('🪤 MM Üst Liq Avı');}
    if(mmTarget==='DOWN_SWEEP')    {shortScore+=12;signals.short.push('🪤 MM Alt Liq Avı');}
    // Stop hunt
    if(hunt1h.hunted&&hunt1h.direction==='BULL_HUNT') {longScore+=14;signals.long.push('🎣 Stop Hunt ↑');}
    if(hunt1h.hunted&&hunt1h.direction==='BEAR_HUNT') {shortScore+=14;signals.short.push('🎣 Stop Hunt ↓');}
    if(hunt15m.hunted&&hunt15m.direction==='BULL_HUNT'){longScore+=8;}
    if(hunt15m.hunted&&hunt15m.direction==='BEAR_HUNT'){shortScore+=8;}
    // OB
    if(ob1h.bullOB&&lastPrice<=ob1h.bullOB.high*1.005&&lastPrice>=ob1h.bullOB.low*0.995){longScore+=14;signals.long.push('📦 1h Bull OB');}
    if(ob1h.bearOB&&lastPrice>=ob1h.bearOB.low*0.995&&lastPrice<=ob1h.bearOB.high*1.005){shortScore+=14;signals.short.push('📦 1h Bear OB');}
    if(ob4h.bullOB&&ob4h.bullOB.distPct<2){longScore+=8;signals.long.push('📦 4h Bull OB yakın');}
    if(ob4h.bearOB&&ob4h.bearOB.distPct<2){shortScore+=8;signals.short.push('📦 4h Bear OB yakın');}
    // CVD
    if(cvdMom==='ACCELERATING_BULL'){longScore+=12;signals.long.push(`📊 CVD Acc.Buy ${cvdRatio.toFixed(0)}%`);}
    if(cvdMom==='ACCELERATING_BEAR'){shortScore+=12;signals.short.push(`📊 CVD Acc.Sell`);}
    if(cvdRatio>60){longScore+=5;}if(cvdRatio<40){shortScore+=5;}
    // Funding
    if(fundSig==='EXTREME_NEGATIVE'){longScore+=10;signals.long.push(`💸 Fund ${curFund.toFixed(4)}% aşırı negatif`);}
    if(fundSig==='NEGATIVE')        {longScore+=5;}
    if(fundSig==='EXTREME_POSITIVE'){shortScore+=10;signals.short.push(`💸 Fund ${curFund.toFixed(4)}% aşırı pozitif`);}
    if(fundSig==='POSITIVE')        {shortScore+=5;}
    // OI
    if(oiDiv==='CONFIRMED_BULL') {longScore+=10;signals.long.push('📈 OI+Fiyat ↑');}
    if(oiDiv==='SHORT_SQUEEZE')  {longScore+=8;signals.long.push('💥 Short Squeeze');}
    if(oiDiv==='CONFIRMED_BEAR') {shortScore+=10;signals.short.push('📉 OI+Fiyat ↓');}
    if(oiDiv==='LONG_LIQUIDATION'){shortScore+=8;signals.short.push('💥 Long Liq');}
    // Smart Money
    if(smDiv==='SMART_BULL'){longScore+=12;signals.long.push('🐋 Whale Long');}
    if(smDiv==='SMART_BEAR'){shortScore+=12;signals.short.push('🐋 Whale Short');}
    if(smDiv==='WHALE_LONG'){longScore+=6;}if(smDiv==='WHALE_SHORT'){shortScore+=6;}
    // Book
    if(bookSig==='STRONG_BIDS'){longScore+=6;signals.long.push('📗 Alım Duvarı');}
    if(bookSig==='STRONG_ASKS'){shortScore+=6;signals.short.push('📕 Satış Duvarı');}
    if(maxBid>totBid*0.25){longScore+=4;signals.long.push('🧊 Iceberg Alıcı');}
    if(maxAsk>totAsk*0.25){shortScore+=4;signals.short.push('🧊 Iceberg Satıcı');}
    // Taker
    if(takerTrend==='AGGRESSIVE_BUYING') {longScore+=6;signals.long.push(`⚡ Taker Buy ${takerBuy.toFixed(0)}%`);}
    if(takerTrend==='AGGRESSIVE_SELLING'){shortScore+=6;signals.short.push(`⚡ Taker Sell`);}
    // Teknik
    const t4up=ema20_4h>ema50_4h&&ema50_4h>ema200_4h;
    const t4dn=ema20_4h<ema50_4h&&ema50_4h<ema200_4h;
    if(t4up){longScore+=8;signals.long.push('📈 4h EMA stack ↑');}
    if(t4dn){shortScore+=8;signals.short.push('📉 4h EMA stack ↓');}
    if(rsi4h<35){longScore+=8;signals.long.push(`RSI4h oversold ${rsi4h}`);}
    if(rsi4h>65){shortScore+=8;signals.short.push(`RSI4h overbought ${rsi4h}`);}
    if(lastPrice>vwap1h){longScore+=3;}else{shortScore+=3;}

    longScore =Math.min(Math.round(longScore),100);
    shortScore=Math.min(Math.round(shortScore),100);

    // ── SİNYAL YAŞI → SKORU DÜŞÜR ────────────────────────────────────────────
    // Sinyal eskidikçe güvenilirlik düşer
    let freshnessMult = 1.0;
    if(freshness==='AGING')   freshnessMult = 0.75;
    if(freshness==='EXPIRED') freshnessMult = 0.0; // expired sinyal yayınlama
    longScore  = Math.round(longScore  * freshnessMult);
    shortScore = Math.round(shortScore * freshnessMult);

    const recommendation=longScore>shortScore&&longScore>=50?'LONG':shortScore>longScore&&shortScore>=50?'SHORT':'WAIT';

    // ── PRO TP/SL SEVİYELERİ ─────────────────────────────────────────────────
    // LONG için:
    //   SL: Son swing low altı veya 1.5x ATR altı (hangisi daha yakınsa)
    //   TP1: En yakın üst likidite seviyesi (stop havuzu)
    //   TP2: 4h direnç / order block üstü
    // SHORT için tersi

    function calcProTPSL(side, price, atr1h, atr4h, liq1h, liq4h, ob1h, ob4h, k1h, k4h) {
      const isLong = side === 'LONG';
      let sl = 0, tp = 0, tp2 = 0;
      const reasons = {};

      // ── STOP LOSS ─────────────────────────────────────────────────────────
      // Yöntem 1: Son swing low/high (15 mum geriye bak)
      const recent15 = isLong ? k1h.slice(-15) : k1h.slice(-15);
      if (isLong) {
        // Son 15 mumun en düşük low'u = swing low
        const swingLow = Math.min(...recent15.map(k => parseFloat(k[3])));
        // ATR bazlı SL: fiyat - 1.5 * ATR1h
        const atrSL = price - 1.5 * atr1h;
        // İkisinden en yüksek olanı (daha az risk)
        sl = Math.max(swingLow, atrSL);
        // OB varsa ve SL'den yüksekse OB low'u kullan
        if (ob1h.bullOB && ob1h.bullOB.low > sl && ob1h.bullOB.low < price * 0.99) {
          sl = ob1h.bullOB.low * 0.998; // OB'nin biraz altı
          reasons.sl = `1h Bull OB altı $${sl.toFixed(4)}`;
        } else {
          reasons.sl = `Swing Low + ATR1.5x = $${sl.toFixed(4)}`;
        }
      } else {
        // SHORT için: swing high + ATR
        const swingHigh = Math.max(...recent15.map(k => parseFloat(k[2])));
        const atrSL = price + 1.5 * atr1h;
        sl = Math.min(swingHigh, atrSL);
        if (ob1h.bearOB && ob1h.bearOB.high < sl && ob1h.bearOB.high > price * 1.01) {
          sl = ob1h.bearOB.high * 1.002;
          reasons.sl = `1h Bear OB üstü $${sl.toFixed(4)}`;
        } else {
          reasons.sl = `Swing High + ATR1.5x = $${sl.toFixed(4)}`;
        }
      }

      // ── TAKE PROFIT 1 (kısa hedef) ────────────────────────────────────────
      if (isLong) {
        // En yakın üst likidite havuzu (MM oraya çekecek)
        const nearLiq = liq1h.buyLiq.find(l => l.price > price * 1.005);
        if (nearLiq && nearLiq.price < price * 1.25) {
          tp = nearLiq.price * 0.998; // Likidite seviyesinin biraz altı (MM tam oraya değil yakınına gider)
          reasons.tp = `1h Liq havuzu altı $${tp.toFixed(4)} (güç:${nearLiq.strength})`;
        } else {
          // Likidite yoksa 4h OB bearish veya 2x ATR
          const atrTP = price + 2 * atr1h;
          tp = atrTP;
          reasons.tp = `2x ATR1h hedef $${tp.toFixed(4)}`;
        }
      } else {
        // SHORT için en yakın alt likidite
        const nearLiq = liq1h.sellLiq.find(l => l.price < price * 0.995);
        if (nearLiq && nearLiq.price > price * 0.75) {
          tp = nearLiq.price * 1.002;
          reasons.tp = `1h Liq havuzu üstü $${tp.toFixed(4)}`;
        } else {
          const atrTP = price - 2 * atr1h;
          tp = atrTP;
          reasons.tp = `2x ATR1h hedef $${tp.toFixed(4)}`;
        }
      }

      // ── TAKE PROFIT 2 (geniş hedef) ───────────────────────────────────────
      if (isLong) {
        // 4h üst likidite veya 4h OB bearish
        const liq4hUp = liq4h.buyLiq.find(l => l.price > price * 1.02);
        if (liq4hUp && liq4hUp.price < price * 1.5) {
          tp2 = liq4hUp.price * 0.997;
          reasons.tp2 = `4h Liq havuzu $${tp2.toFixed(4)}`;
        } else if (ob4h.bearOB && ob4h.bearOB.low > price * 1.02) {
          tp2 = ob4h.bearOB.low * 0.997;
          reasons.tp2 = `4h Bear OB altı $${tp2.toFixed(4)}`;
        } else {
          tp2 = price + 4 * atr4h;
          reasons.tp2 = `4x ATR4h $${tp2.toFixed(4)}`;
        }
      } else {
        const liq4hDn = liq4h.sellLiq.find(l => l.price < price * 0.98);
        if (liq4hDn && liq4hDn.price > price * 0.5) {
          tp2 = liq4hDn.price * 1.003;
          reasons.tp2 = `4h Liq havuzu $${tp2.toFixed(4)}`;
        } else if (ob4h.bullOB && ob4h.bullOB.high < price * 0.98) {
          tp2 = ob4h.bullOB.high * 1.003;
          reasons.tp2 = `4h Bull OB üstü $${tp2.toFixed(4)}`;
        } else {
          tp2 = price - 4 * atr4h;
          reasons.tp2 = `4x ATR4h $${tp2.toFixed(4)}`;
        }
      }

      // Geçerlilik kontrolleri
      if (isLong) {
        if (sl >= price) sl = price * 0.95;  // SL fiyatın üstünde olamaz
        if (tp <= price) tp = price * 1.05;  // TP fiyatın altında olamaz
        if (tp2 <= tp)   tp2 = tp * 1.05;    // TP2 TP'den büyük olmalı
      } else {
        if (sl <= price) sl = price * 1.05;
        if (tp >= price) tp = price * 0.95;
        if (tp2 >= tp)   tp2 = tp * 0.95;
      }

      const riskPct  = Math.abs(price - sl) / price * 100;
      const reward1  = Math.abs(tp  - price) / price * 100;
      const reward2  = Math.abs(tp2 - price) / price * 100;
      const rr1      = +(reward1 / riskPct).toFixed(2);
      const rr2      = +(reward2 / riskPct).toFixed(2);

      return {
        sl:     +sl.toFixed(8),
        tp:     +tp.toFixed(8),
        tp2:    +tp2.toFixed(8),
        riskPct:+riskPct.toFixed(2),
        rewardPct:+reward1.toFixed(2),
        reward2Pct:+reward2.toFixed(2),
        rr1, rr2,
        reasons,
      };
    }

    const proTPSL = calcProTPSL(
      recommendation === 'SHORT' ? 'SHORT' : 'LONG',
      lastPrice, atr1h, atr4h,
      liq1h, liq4h, ob1h, ob4h, k1h, k4h
    );

    // ── KALDIRAÇ ÖNERİSİ (3-50 arası, skor + volatilite bazlı) ──────────────
    const score=recommendation==='LONG'?longScore:shortScore;
    const atrPct=lastPrice>0?(atr1h/lastPrice)*100:1;
    // Düşük volatilite + yüksek skor = yüksek kaldıraç
    // Yüksek volatilite + düşük skor = düşük kaldıraç
    let suggestedLev=3;
    if(score>=85&&atrPct<0.5)      suggestedLev=25;
    else if(score>=80&&atrPct<0.8) suggestedLev=20;
    else if(score>=75&&atrPct<1.0) suggestedLev=15;
    else if(score>=70&&atrPct<1.5) suggestedLev=10;
    else if(score>=65&&atrPct<2.0) suggestedLev=7;
    else if(score>=60&&atrPct<2.5) suggestedLev=5;
    else if(score>=55)             suggestedLev=4;
    else                           suggestedLev=3;
    // Hard cap: 50
    suggestedLev=Math.min(suggestedLev,50);
    const levRisk=suggestedLev>=20?'YÜKSEK RİSK':suggestedLev>=10?'ORTA RİSK':suggestedLev>=5?'DÜŞÜK-ORTA':'DÜŞÜK RİSK';
    const levMsg=`${suggestedLev}x kaldıraç kullanabilirsin (${levRisk}) — Skor:${score} ATR:%${atrPct.toFixed(2)}`;

    res.json({
      ok:true, symbol:full, price:lastPrice,
      // Sinyal kalitesi
      freshness, signalAgeMin, isExpired, maxValidMin,
      // MM
      marketMaker:{ target:mmTarget, confidence:mmConf, reasoning:mmReasoning,
        nextTarget:mmNextTarget, retailBias, smartMoneyBias:smBias, oiTrend,
        upLiqStrength:+upLiqStr.toFixed(2), downLiqStrength:+dnLiqStr.toFixed(2) },
      liquidityLevels:{'1h':liq1h,'4h':liq4h},
      stopHunt:{'1h':hunt1h,'15m':hunt15m},
      orderBlocks:{'1h':ob1h,'4h':ob4h},
      // Kaldıraç önerisi
      leverage:{ suggested:suggestedLev, min:3, max:50, risk:levRisk, message:levMsg, atrPct:+atrPct.toFixed(2) },
      // Pro göstergeler
      timeframes:{
        '4h':{rsi:rsi4h,ema20:+ema20_4h.toFixed(4),ema50:+ema50_4h.toFixed(4),ema200:+ema200_4h.toFixed(4),trend:t4up?'UP':t4dn?'DOWN':'RANGE',atr:+atr4h.toFixed(4)},
        '1h':{rsi:rsi1h,ema20:+ema20_1h.toFixed(4),ema50:+ema50_1h.toFixed(4),ema200:+ema200_1h.toFixed(4),trend:ema20_1h>ema50_1h?'UP':'DOWN',vwap:+vwap1h.toFixed(4),atr:+atr1h.toFixed(4)},
        '15m':{rsi:rsi15m,atr:+atr15m_.toFixed(4)},
        '5m':{rsi:rsi5m},
      },
      cvd:{total:+cvdTotal.toFixed(0),buyRatio:+cvdRatio.toFixed(1),momentum:cvdMom},
      funding:{current:+curFund.toFixed(4),signal:fundSig,annualized:fundAnn},
      openInterest:{change1h:+oiChg1h.toFixed(2),change4h:+oiChg4h.toFixed(2),divergence:oiDiv},
      smartMoney:{topLongPct:+topLong.toFixed(1),globalLongPct:+globalLong.toFixed(1),divergence:smDiv},
      orderBook:{imbalance:+bookImb.toFixed(1),signal:bookSig,bidWall:maxBid>totBid*0.25,askWall:maxAsk>totAsk*0.25},
      takerFlow:{buyPct:+takerBuy.toFixed(1),trend:takerTrend},
      proTPSL,
      longScore, shortScore, recommendation,
      signals:recommendation==='LONG'?signals.long.slice(0,6):signals.short.slice(0,6),
    });
  } catch(e) { res.status(400).json({ error:e.message }); }
});

// ── HESAP ─────────────────────────────────────────────────────────────────────
app.post('/api/account', async (req, res) => {
  const { apiKey, apiSecret } = req.body;
  if(!apiKey||!apiSecret) return res.status(400).json({error:'API key gerekli'});
  try {
    let walletBal=0,availBal=0,unrealized=0;
    try{const b=await bReq(apiKey,apiSecret,'GET','/fapi/v3/balance');const u=Array.isArray(b)?b.find(x=>x.asset==='USDT'):null;if(u){walletBal=parseFloat(u.balance)||0;availBal=parseFloat(u.availableBalance)||0;}}catch(e){}
    const data=await bReq(apiKey,apiSecret,'GET','/fapi/v2/account');
    if(parseFloat(data.totalWalletBalance)>0)walletBal=parseFloat(data.totalWalletBalance);
    if(parseFloat(data.availableBalance)>0)availBal=parseFloat(data.availableBalance);
    unrealized=parseFloat(data.totalUnrealizedProfit)||0;
    res.json({ok:true,totalWalletBalance:walletBal,availableBalance:availBal,totalUnrealizedProfit:unrealized,
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
  const { apiKey, apiSecret, symbol, side, leverage, marginType, targetPrice, stopPrice, usdtAmount } = req.body;
  if(!apiKey||!apiSecret||!symbol||!side||!leverage||!targetPrice||!stopPrice||!usdtAmount)
    return res.status(400).json({error:'Eksik parametre'});

  const sym=symbol.toUpperCase().includes('USDT')?symbol.toUpperCase():symbol.toUpperCase()+'USDT';
  const isLong=side.toUpperCase()==='LONG';
  const oSide=isLong?'BUY':'SELL', cSide=isLong?'SELL':'BUY';

  try {
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

    // Ana emir
    const main=await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',{
      symbol:sym,side:oSide,type:'MARKET',quantity:qty,positionSide:'BOTH'
    });

    // Gerçek giriş fiyatını al
    await new Promise(r=>setTimeout(r,600));
    let execPrice=parseFloat(main.avgPrice||curPrice);
    try{
      const pos=await bReq(apiKey,apiSecret,'GET','/fapi/v2/positionRisk',{symbol:sym});
      const p=Array.isArray(pos)?pos.find(x=>x.symbol===sym&&Math.abs(parseFloat(x.positionAmt))>0):null;
      if(p&&parseFloat(p.entryPrice)>0)execPrice=parseFloat(p.entryPrice);
    }catch(e){}

    const ratio=execPrice/curPrice;
    const realTP=rnd(parseFloat(targetPrice)*ratio);
    const realSL=rnd(parseFloat(stopPrice)*ratio);
    console.log(`${sym} giriş:${execPrice} TP:${realTP} SL:${realSL}`);

    // ── TP/SL YERLEŞTİR ──────────────────────────────────────────────────────
    // Binance'da iki farklı API var:
    // 1. /fapi/v1/order → eski/standart coinler
    // 2. /fapi/v1/order (TAKE_PROFIT/STOP tipiyle) → bazı yeni coinler
    // Her iki formatta da dene

    let tp={orderId:null},sl={orderId:null};

    async function placeSLTP(type, price) {
      const price_str = price.toString();

      // Format 1: TAKE_PROFIT_MARKET / STOP_MARKET + closePosition
      const formats = [
        // Standart crypto futures
        {path:'/fapi/v1/order', params:{symbol:sym,side:cSide,type,
          stopPrice:price_str,closePosition:'true',positionSide:'BOTH',workingType:'MARK_PRICE'}},
        {path:'/fapi/v1/order', params:{symbol:sym,side:cSide,type,
          stopPrice:price_str,closePosition:'true',positionSide:'BOTH',workingType:'CONTRACT_PRICE'}},
        {path:'/fapi/v1/order', params:{symbol:sym,side:cSide,type,
          stopPrice:price_str,closePosition:'true',positionSide:'BOTH'}},
        // Quantity ile (closePosition yerine)
        {path:'/fapi/v1/order', params:{symbol:sym,side:cSide,type,
          stopPrice:price_str,quantity:qty.toString(),reduceOnly:'true',positionSide:'BOTH',workingType:'MARK_PRICE'}},
        {path:'/fapi/v1/order', params:{symbol:sym,side:cSide,type,
          stopPrice:price_str,quantity:qty.toString(),reduceOnly:'true',positionSide:'BOTH'}},
      ];

      for(const {path, params} of formats) {
        try{
          const r=await bReq(apiKey,apiSecret,'POST',path,params);
          if(r.orderId){
            console.log(`${type} BAŞARILI orderId:${r.orderId}`);
            return r;
          }
        }catch(e){
          const msg=e.message||'';
          // -4120 = algo endpoint gerekiyor → skip tüm /fapi/v1/order denemeleri
          if(msg.includes('-4120'))break;
          console.log(`${type} hata: ${msg.substring(0,80)}`);
        }
      }

      // -4120 aldıysak veya hiçbiri çalışmadıysa: Algo order API
      // Yeni Binance coinleri için - bu endpoint farklı auth gerektirebilir,
      // şimdilik pas geç ve kullanıcıya bilgi ver
      console.log(`${type} standart başarısız, coin yeni nesil olabilir`);
      return {orderId:null};
    }

    tp=await placeSLTP('TAKE_PROFIT_MARKET',realTP);
    await new Promise(r=>setTimeout(r,400));
    sl=await placeSLTP('STOP_MARKET',realSL);

    const tpOk=!!tp.orderId, slOk=!!sl.orderId;
    const msg=`${sym} ${side} açıldı ✅${tpOk?` TP#${tp.orderId}`:'❌ TP manuel ekle'}${slOk?` SL#${sl.orderId}`:'❌ SL manuel ekle'}`;

    res.json({ok:true,message:msg,mainOrderId:main.orderId,tpOrderId:tp.orderId,slOrderId:sl.orderId,
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
    const qty=Math.abs(parseFloat(p.positionAmt));
    const order=await bReq(apiKey,apiSecret,'POST','/fapi/v1/order',{
      symbol:sym,side:parseFloat(p.positionAmt)>0?'SELL':'BUY',
      type:'MARKET',quantity:qty,reduceOnly:'true',positionSide:'BOTH'
    });
    res.json({ok:true,message:`${sym} kapatıldı`,orderId:order.orderId});
  }catch(e){res.status(400).json({error:e.message});}
});

app.listen(PORT,()=>console.log(`✅ Server ${PORT}`));
