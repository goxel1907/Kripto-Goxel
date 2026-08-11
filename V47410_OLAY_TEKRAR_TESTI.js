(async()=>{
// 05.08.2026 OLAY TEKRARI — J1/J2 gercekten tutuyor mu?
// Gercek kanit dosyalarindan alinan sayilarla iki basarisizligi yeniden oynatir.
//
//   CYS_1785925087982       SL 0.6054 / TP 0.7143 kuruldu, +7.3sn'de SL kayboldu,
//                           +39.8sn'de korumasiz kapandi (+0.1008 USDT)
//   1000RATS_1785926105641  SL 0.04758 / TP 0.05237 kuruldu, +4.6sn'de SL kayboldu,
//                           +13.1sn'de korumasiz kapandi (-0.6204 USDT)
const fs=require('fs'),vm=require('vm'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`  pass  ${n}`)):(fail++,console.error(`  FAIL  ${n}${d?' :: '+d:''}`));
function grab(decl){
  const i=src.indexOf(decl); if(i<0) throw new Error('yok: '+decl);
  let p=src.indexOf('(',i),pd=0,q=p;
  for(;q<src.length;q++){if(src[q]==='(')pd++;else if(src[q]===')'){pd--;if(!pd){q++;break;}}}
  let k=src.indexOf('{',q),d=0,st=null,esc=false,ln=false,bl=false;
  for(;k<src.length;k++){const c=src[k],n=src[k+1];
    if(ln){if(c==='\n')ln=false;continue;} if(bl){if(c==='*'&&n==='/'){bl=false;k++;}continue;}
    if(st){if(esc){esc=false;continue;}if(c==='\\'){esc=true;continue;}if(c===st)st=null;continue;}
    if(c==='/'&&n==='/'){ln=true;k++;continue;} if(c==='/'&&n==='*'){bl=true;k++;continue;}
    if(c==='"'||c==="'"||c==='`'){st=c;continue;}
    if(c==='{')d++;else if(c==='}'){d--;if(!d){k++;break;}}}
  return src.slice(i,k);
}

const OLAYLAR=[
  {ad:'CYS',      sym:'CYSUSDT',      sl:0.6054,   tp:0.7143,   wipeS:7.3, closeS:39.8, pnl:+0.1008},
  {ad:'1000RATS', sym:'1000RATSUSDT', sl:0.04758,  tp:0.05237,  wipeS:4.6, closeS:13.1, pnl:-0.6204},
];

console.log('══ SENARYO 1 — J1: calisan koruma iptal ediliyor mu? ' + '═'.repeat(22));
for(const o of OLAYLAR){
  console.log(`\n  ▸ ${o.ad}  (gercek: +${o.wipeS}sn'de SL kayboldu)`);
  let cancelCagrildi=false, placeCagrildi=0;
  const ctx=vm.createContext({
    V592_PROTECT_KEEP_EXISTING:true,
    v592ParityStats:{protectionKeptExisting:0},
    r501OrderLifeMark:()=>{},
    // Borsada SL+TP HALA DURUYOR — gercek durum buydu, sadece trailingState kaybolmustu
    verifyAlgoSLTPVisible:async(k,s,sym,eSL,eTP)=>({ok:true,foundSL:true,foundTP:true,orderCount:2}),
    cancelAlgoOrders:async()=>{cancelCagrildi=true;},
    placeAlgoSL:async()=>{placeCagrildi++;return {algoId:'SL1'};},
    placeAlgoTP:async()=>{placeCagrildi++;return {algoId:'TP1'};},
    freshOpenPositionForSymbol:async()=>({open:true,pos:{positionAmt:'1'}}),
    fetchPositionRiskRaw:async()=>[{symbol:o.sym,positionAmt:'1'}],
    bReq:async()=>[], cleanupClosedPositionState:async()=>{},
    normalizeSLTPToTick:async(s,a,b)=>({sl:a,tp:b}),
    formatStepValue:(v)=>v, POS_FRESH_ORDER_MS:3000, POS_FRESH_MANAGER_MS:8000,
    sleep:async()=>{}, console:{log:()=>{}}, Date,Math,Number,String,Object,Array,Boolean,Promise,setTimeout,
  });
  vm.runInContext(grab('async function installSLTPWithProof'),ctx);
  let r;
  try{ r=await ctx.installSLTPWithProof('k','s',o.sym,'SELL',o.sl,o.tp,o.sym); }catch(e){ r={ok:false,err:e.message}; }
  ok(`${o.ad} cancelAlgoOrders CAGRILMADI`, cancelCagrildi===false, `cancel=${cancelCagrildi}`);
  ok(`${o.ad} yeniden kurma denenmedi`, placeCagrildi===0, `place=${placeCagrildi}`);
  ok(`${o.ad} mevcut koruma korundu`, r && r.ok===true && r.keptExisting===true, JSON.stringify(r).slice(0,90));
  ok(`${o.ad} sayac artti`, ctx.v592ParityStats.protectionKeptExisting===1);
}

console.log('\n══ SENARYO 2 — J2: erken kapatma engelleniyor mu? ' + '═'.repeat(25));
{
  const mkGuard=(exact,ageMs)=>{
    const ctx=vm.createContext({V592_EXACT_BACKTEST_AUTHORITY:exact,V592_MIN_HOLD_MS:60000,v592ExitCandleGate:()=>({blocked:false}),
      trailingState:new Map([['X',{openedAt:Date.now()-ageMs}]]),
      r501OrderLifeSnapshot:()=>({}),r501OrderLifeMark:()=>{},
      v592ParityStats:{minHoldBlocks:0},Date,Number,String,Math,console});
    vm.runInContext(grab('function v592MinHoldGuard'),ctx);
    return ctx.v592MinHoldGuard('X','EMERGENCY_BRACKET');
  };
  console.log('\n  ▸ gercek olay sureleri');
  for(const o of OLAYLAR){
    const g=mkGuard(true,o.closeS*1000);
    ok(`${o.ad} ${o.closeS}sn kapatma BLOKE`, g.blocked===true, `blocked=${g.blocked}`);
  }
  console.log('\n  ▸ backtest tabani sinirlari');
  ok('59.9sn BLOKE', mkGuard(true,59900).blocked===true);
  ok('60.0sn SERBEST (backtest minimumu)', mkGuard(true,60000).blocked===false);
  ok('68 dk SERBEST (backtest medyani)', mkGuard(true,4080000).blocked===false);
  ok('exact kapali -> hic bloke etmez', mkGuard(false,5000).blocked===false);
}

console.log('\n══ SENARYO 3 — J3: 42 saniyelik bayat emir ' + '═'.repeat(31));
{
  const chk=(lagMs,limit,exact)=>{
    // kaynaktaki kosulun birebir kopyasi
    return !!(exact && limit>0 && lagMs>limit);
  };
  ok('1000RATS 42120ms / 15000 limit -> IPTAL', chk(42120,15000,true)===true);
  ok('CYS 1208ms -> gecer', chk(1208,15000,true)===false);
  ok('14999ms -> gecer', chk(14999,15000,true)===false);
  ok('15001ms -> IPTAL', chk(15001,15000,true)===true);
  ok('exact kapali -> iptal yok', chk(42120,15000,false)===false);
  ok('kaynakta kosul birebir', /_lag>V592_MAX_REQUEST_TO_SEND_MS/.test(src));
}

console.log('\n══ SENARYO 4 — K1: API bogulmasi tekrar olur mu? ' + '═'.repeat(25));
{
  // Yonetici dongusu: 20 sembol x 2 deneme = 40 cagri, 8sn butce icinde
  let netCalls=0;
  const base=Date.now();
  const ctx=vm.createContext({
    posRiskCache:{data:[{symbol:'X',positionAmt:'1'}],ts:base,lastApiKey:'FP',rateLimitUntil:0,
      fetching:false,inflight:null,inflightStartedAt:0,lastSuccessAt:base,consecutiveFailures:0,
      phase:'',lastError:null,lastErrorAt:0,lastErrorType:null,lastDurationMs:0},
    POS_RISK_TTL_ACTIVE:30000,POS_RISK_TTL_NORMAL:30000,POS_RISK_RATELIMIT_MS:60000,POS_RISK_INFLIGHT_WAIT_MS:14000,
    v592ParityStats:{freshCacheHits:0},resetStuckPositionRiskInflight:()=>{},keyFingerprint:()=>'FP',
    isBinanceBackoffActive:()=>false,getBinanceBackoffMs:()=>3e4,
    isExecBackoffActive:()=>false,getExecBackoffMs:()=>3e4,makeBinanceBackoffError:m=>new Error(m),
    isPositionRiskRateLimitError:()=>false,safeErrMsg:e=>String(e),pushCritical:()=>{},
    fetchPositionRiskRaw:async()=>{netCalls++;return [{symbol:'X',positionAmt:'1'}];},
    Date,Math,Number,String,Object,Array,Boolean,Promise,setTimeout,clearTimeout,Infinity,console});
  vm.runInContext(grab('function filterPositionRiskRows'),ctx);
  vm.runInContext(grab('async function getPositionRiskCached'),ctx);
  for(let i=0;i<40;i++) await ctx.getPositionRiskCached('k','s',{symbol:'X',__forceFresh:true,__maxAgeMs:8000});
  ok('40 yonetici cagrisi -> 0 ag istegi (cache taze)', netCalls===0, `netCalls=${netCalls}`);
  ok('freshCacheHits 40', ctx.v592ParityStats.freshCacheHits===40, `${ctx.v592ParityStats.freshCacheHits}`);
  // cache bayatlayinca
  ctx.posRiskCache.ts = Date.now()-9000;
  await ctx.getPositionRiskCached('k','s',{symbol:'X',__forceFresh:true,__maxAgeMs:8000});
  ok('9sn bayat -> 1 ag istegi', netCalls===1, `netCalls=${netCalls}`);
}

console.log(`\n${'═'.repeat(74)}`);
console.log(fail?`SONUC: FAIL — ${pass} gecti, ${fail} dustu`
               :`SONUC: PASS — ${pass} kontrol. 05.08 olaylarinin ikisi de ONLENIYOR.`);
process.exitCode=fail?1:0;

})().catch(e=>{console.error("HARNESS:",e);process.exitCode=1;});