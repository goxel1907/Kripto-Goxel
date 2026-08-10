'use strict';
// V4.7.4.10 — TAZELIK BUTCESI (G1 gerilemesi duzeltmesi)
const fs=require('fs'),vm=require('vm'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`pass  ${n}`)):(fail++,console.error(`FAIL  ${n}${d?' :: '+d:''}`));
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
const mk=(ageMs,over={})=>{
  let fetched=0;
  const ctx=vm.createContext({
    posRiskCache:{data:[{symbol:'SYNUSDT',positionAmt:'1'}],ts:Date.now()-ageMs,lastApiKey:'FP',
      rateLimitUntil:0,fetching:false,inflight:null,inflightStartedAt:0,lastSuccessAt:Date.now()-ageMs,
      consecutiveFailures:0,phase:'',lastError:null,lastErrorAt:0,lastErrorType:null,lastDurationMs:0},
    POS_RISK_TTL_ACTIVE:30000,POS_RISK_TTL_NORMAL:30000,POS_RISK_RATELIMIT_MS:60000,
    POS_RISK_INFLIGHT_WAIT_MS:14000,
    v592ParityStats:{freshCacheHits:0},
    resetStuckPositionRiskInflight:()=>{},keyFingerprint:()=>'FP',
    isBinanceBackoffActive:()=>false,getBinanceBackoffMs:()=>30000,
    isExecBackoffActive:()=>false,getExecBackoffMs:()=>30000,
    makeBinanceBackoffError:m=>new Error('BACKOFF '+m),isPositionRiskRateLimitError:()=>false,
    safeErrMsg:e=>String(e&&e.message||e),pushCritical:()=>{},
    fetchPositionRiskRaw:async()=>{fetched++;return [{symbol:'SYNUSDT',positionAmt:'2'}];},
    Date,Math,Number,String,Object,Array,Boolean,Promise,setTimeout,clearTimeout,Infinity,console,...over});
  vm.runInContext(grab('function filterPositionRiskRows'),ctx);
  vm.runInContext(grab('async function getPositionRiskCached'),ctx);
  ctx.__f=()=>fetched; return ctx;
};

(async()=>{
console.log('── K1  Yas sinirli tazelik ' + '─'.repeat(42));
{
  let c=mk(2000);
  let r=await c.getPositionRiskCached('k','s',{__forceFresh:true,__maxAgeMs:3000});
  ok('K1a 2sn yas / 3sn limit -> AG CAGRISI YOK', c.__f()===0 && r[0].positionAmt==='1', `fetched=${c.__f()}`);
  ok('K1b freshCacheHits arttı', c.v592ParityStats.freshCacheHits===1);

  c=mk(5000);
  r=await c.getPositionRiskCached('k','s',{__forceFresh:true,__maxAgeMs:3000});
  ok('K1c 5sn yas / 3sn limit -> AGA GIDER', c.__f()===1 && r[0].positionAmt==='2', `fetched=${c.__f()}`);

  c=mk(7000);
  r=await c.getPositionRiskCached('k','s',{__forceFresh:true,__maxAgeMs:8000});
  ok('K1d 7sn yas / 8sn limit (yonetici) -> cache', c.__f()===0, `fetched=${c.__f()}`);

  c=mk(2000);
  r=await c.getPositionRiskCached('k','s',{__forceFresh:true});
  ok('K1e maxAge YOKSA eski davranis (aga gider)', c.__f()===1, `fetched=${c.__f()}`);

  c=mk(2000);
  r=await c.getPositionRiskCached('k','s',{});
  ok('K1f forceFresh yok -> TTL cache', c.__f()===0 && r[0].positionAmt==='1');
}
console.log('\n── K2  Bayat veri HALA reddediliyor ' + '─'.repeat(33));
{
  let c=mk(30000,{isBinanceBackoffActive:()=>true,isExecBackoffActive:()=>true});
  let t=false; try{ await c.getPositionRiskCached('k','s',{__forceFresh:true,__maxAgeMs:3000}); }catch(e){t=true;}
  ok('K2a backoff + eski cache + forceFresh -> THROW', t);

  c=mk(30000); c.posRiskCache.rateLimitUntil=Date.now()+60000;
  t=false; try{ await c.getPositionRiskCached('k','s',{__forceFresh:true,__maxAgeMs:3000}); }catch(e){t=true;}
  ok('K2b cooldown + eski cache + forceFresh -> THROW', t);

  c=mk(30000,{fetchPositionRiskRaw:async()=>{throw new Error('ECONNRESET');}});
  t=false; try{ await c.getPositionRiskCached('k','s',{__forceFresh:true,__maxAgeMs:3000}); }catch(e){t=true;}
  ok('K2c ag hatasi + eski cache + forceFresh -> THROW', t);
}
console.log('\n── K3  Butceler ve cagri yerleri ' + '─'.repeat(36));
{
  const h=re=>re.test(src);
  ok('K3a emir butcesi 3000', h(/POS_FRESH_ORDER_MS\|\|3000/));
  ok('K3b yonetici butcesi 8000', h(/POS_FRESH_MANAGER_MS\|\|8000/));
  ok('K3c inflight bekleme 14000', h(/POS_RISK_INFLIGHT_WAIT_MS\|\|14000/));
  ok('K3d SL\\/TP yolu SIKI butce', h(/freshOpenPositionForSymbol\(apiKey, apiSecret, symbol, 3, POS_FRESH_ORDER_MS\)/));
  ok('K3e fresh fonksiyonu maxAge parametreli', h(/async function freshOpenPositionForSymbol\(apiKey, apiSecret, symbol, attempts=3, maxAgeMs=POS_FRESH_MANAGER_MS\)/));
  ok('K3f __maxAgeMs Binance\'e sizmiyor', h(/delete queryParams\.__maxAgeMs;/));
  ok('K3g inflight timeout sabitten', h(/'POSITION_RISK_INFLIGHT_WAIT_TIMEOUT'\)\),POS_RISK_INFLIGHT_WAIT_MS\)/));
  ok('K3h getPositionRiskTruth 10sn siniri duruyor', h(/maxAgeMs=ORDER_TRUTH_MAX_AGE_MS/));
}
console.log('\n── K4  Onceki sozlesme bozulmadi ' + '─'.repeat(36));
{
  const h=re=>re.test(src);
  ok('K4a build V4.7.4.33', h(/V4_7_4_33_DISK_RISK41_10X/));
  ok('K4b session 4_7_4_33_DK1', h(/V592_EXACT_CLOSED1M_R495_72H_4_7_4_33_DK1/));
  ok('K4c J1 koruma koruma', h(/'PROTECTION_KEPT_EXISTING'/));
  ok('K4d J2 min hold', h(/function v592MinHoldGuard/));
  ok('K4e J3 bayat emir', h(/'ORDER_ABORTED_STALE'/));
  ok('K4f J4 non-backtest exit', h(/'NON_BACKTEST_EXIT'/));
  ok('K4g slot41 max2 risk4', h(/R497_SLOT_MARGIN_USDT \|\| 41/)&&h(/R486_MAX_POSITIONS \|\| 2/)&&h(/R495_FINAL_RISK_PCT \|\| 4/));
  ok('K4h testnet hard-lock', h(/BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/));
}
console.log(`\n${fail?'SONUC: FAIL':'SONUC: PASS'} — ${pass} gecti, ${fail} dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
