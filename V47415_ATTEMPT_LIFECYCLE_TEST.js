(async()=>{
const fs=require('fs'),vm=require('vm'),path=require('path'),crypto=require('crypto');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`  pass  ${n}`)):(fail++,console.error(`  FAIL  ${n}${d?' :: '+d:''}`));
const cnt=(re)=>(src.match(new RegExp(re,'g'))||[]).length;
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
// 05.08 ONUSDT gercek zaman damgalari
const T1=1785942572000; // 15:09:32 ilk ORDER_REQUEST_RECEIVED
const T2=1785945019000; // 15:50:19 ikinci denemede red

function mk(){
  let saat=T1;
  const map=new Map();
  const ctx=vm.createContext({
    r501OrderLifecycleBySymbol:map,
    r501LatestDecisionBySymbol:{get:()=>({decisionTimeMs:saat-500,candidateTimeMs:saat-1200})},
    r501ImmediateResearchSnapshot:()=>({stage:'X'}),
    R501_LIFECYCLE_RESEARCH_STAGES:new Set(['ORDER_REQUEST_RECEIVED','MAIN_ORDER_SEND','ORDER_REJECTED']),
    normalizeSymbol:x=>String(x).toUpperCase(), crypto,
    r501OrderLifeSnapshot:(sy)=>map.get(String(sy).toUpperCase())||null,
    r501EvidenceFunnel:()=>{}, r501ActiveEvidence:new Map(), r501PersistRec:()=>{},
    pushCritical:()=>{}, safeErrMsg:e=>String(e),
    v592ParityStats:{lifecycleAttemptResets:0},
    Date:{now:()=>saat}, Math,Number,String,Object,Array,Boolean,JSON,console,
    __set:(t)=>{saat=t;},
  });
  vm.runInContext(grab('function r501OrderLifeMark'),ctx);
  return {ctx,map,at:(t)=>ctx.__set(t)};
}

console.log('══ A — 05.08 ONUSDT olayinin tekrari ' + '═'.repeat(36));
{
  const {ctx,map,at}=mk();
  at(T1);          ctx.r501OrderLifeMark('ONUSDT','ORDER_REQUEST_RECEIVED',{});
  const a1=map.get('ONUSDT');
  at(T1+27000);    ctx.r501OrderLifeMark('ONUSDT','ORDER_REJECTED',{});
  at(T2);          ctx.r501OrderLifeMark('ONUSDT','ORDER_REQUEST_RECEIVED',{});
  const a2=map.get('ONUSDT');
  const lagYeni = T2 - a2.requestReceivedTime;
  const lagEski = T2 - a1.requestReceivedTime;
  console.log(`    ilk istek   ${new Date(T1).toISOString().slice(11,19)}`);
  console.log(`    ikinci istek ${new Date(T2).toISOString().slice(11,19)}  (40dk 47sn sonra)`);
  console.log();
  ok(`ESKI hesap  ${Math.round(lagEski/1000)}sn  (panelde yazan 2447sn)`, Math.round(lagEski/1000)===2447);
  ok(`YENI hesap  ${Math.round(lagYeni/1000)}sn  (temiz deneme)`, lagYeni===0, `${lagYeni}ms`);
  ok('requestReceivedTime YENILENDI', a2.requestReceivedTime===T2, `${a2.requestReceivedTime}`);
  ok('attemptId DEGISTI', a1.attemptId && a2.attemptId && a1.attemptId!==a2.attemptId);
  ok('onceki deneme izi tutuldu', a2.previousAttemptId===a1.attemptId
     && a2.previousRequestReceivedTime===T1);
  ok('sayac lifecycleAttemptResets', ctx.v592ParityStats.lifecycleAttemptResets===1);
  ok('J3 artik bloklamaz (0sn < 15000)', lagYeni<15000);
}

console.log('\n══ B — eski denemenin artiklari tasinmiyor ' + '═'.repeat(31));
{
  const {ctx,map,at}=mk();
  at(T1); ctx.r501OrderLifeMark('ONUSDT','ORDER_REQUEST_RECEIVED',{});
  at(T1+1000); ctx.r501OrderLifeMark('ONUSDT','MAIN_ORDER_SEND',{referencePrice:0.31,mainOrderId:'OLD1'});
  at(T1+2000); ctx.r501OrderLifeMark('ONUSDT','ORDER_REJECTED',{error:'ESKI_HATA'});
  const eski=map.get('ONUSDT');
  ok('eski denemede referencePrice var', eski.referencePrice===0.31);
  at(T2); ctx.r501OrderLifeMark('ONUSDT','ORDER_REQUEST_RECEIVED',{});
  const yeni=map.get('ONUSDT');
  ok('referencePrice temizlendi', !yeni.referencePrice, String(yeni.referencePrice));
  ok('eski mainOrderId tasinmadi', !yeni.mainOrderId, String(yeni.mainOrderId));
  ok('eski error tasinmadi', !yeni.error, String(yeni.error));
  ok('candidateTime YENI karardan okundu', yeni.candidateTime===T2-1200 && eski.candidateTime===T1-1200,
     `eski ${eski.candidateTime} yeni ${yeni.candidateTime}`);
  ok('decisionTime YENI karardan okundu', yeni.decisionTime===T2-500);
  ok('marketSnapshots KORUNDU (arastirma)', !!yeni.marketSnapshots);
}

console.log('\n══ C — normal tek deneme bozulmadi ' + '═'.repeat(39));
{
  const {ctx,map,at}=mk();
  at(T1);        ctx.r501OrderLifeMark('ONUSDT','ORDER_REQUEST_RECEIVED',{});
  at(T1+1200);   ctx.r501OrderLifeMark('ONUSDT','MAIN_ORDER_SEND',{orderSendTime:T1+1200,referencePrice:0.31});
  at(T1+1450);   ctx.r501OrderLifeMark('ONUSDT','MAIN_ORDER_ACK',{orderAckTime:T1+1450});
  at(T1+1450);   ctx.r501OrderLifeMark('ONUSDT','ENTRY_FILL_OBSERVED',{fillTime:T1+1450,executedPrice:0.3105});
  const r=map.get('ONUSDT');
  ok('requestToSend 1200ms', r.latencyMs.requestToSend===1200, String(r.latencyMs.requestToSend));
  ok('sendToAck 250ms', r.latencyMs.sendToAck===250, String(r.latencyMs.sendToAck));
  ok('referencePrice korundu', r.referencePrice===0.31);
  ok('slippage hesaplandi', Number.isFinite(r.slippageBps), String(r.slippageBps));
  ok('tek denemede reset YOK', ctx.v592ParityStats.lifecycleAttemptResets===0);
}

console.log('\n══ D — J3 makullik siniri (10dk) ' + '═'.repeat(41));
{
  // V4.7.4.16'dan sonra bu ms kurali YEDEKTIR (yalniz V592_ENTRY_CANDLE_PARITY=0 iken).
  const chk=(lag)=> lag>600000 ? 'OLCUM_GECERSIZ' : (lag>15000 ? 'IPTAL' : 'GECER');
  ok('2.447.000ms -> olcum gecersiz', chk(2447000)==='OLCUM_GECERSIZ');
  ok('  600.001ms -> olcum gecersiz', chk(600001)==='OLCUM_GECERSIZ');
  ok('  599.999ms -> IPTAL (gercek bayat)', chk(599999)==='IPTAL');
  ok('   27.000ms -> IPTAL (gercek bayat)', chk(27000)==='IPTAL');
  ok('   14.999ms -> gecer', chk(14999)==='GECER');
  ok('10dk makullik siniri yedek kuralda', /_lag<=600000/.test(src));
ok('ASIL kural artik MUM tabanli (V4.7.4.16-W)', /V592_ENTRY_CANDLE_PARITY/.test(src));
  ok('STALE_LIFECYCLE_LAG_IGNORED asamasi kayitli', /STALE_LIFECYCLE_LAG_IGNORED/.test(src));
  ok('gercek J3 kapisi duruyor', /_lag>V592_MAX_REQUEST_TO_SEND_MS/.test(src));
}

console.log('\n══ E — kaynak sozlesmesi ' + '═'.repeat(49));
ok('isNewAttempt tanimli', /const isNewAttempt = \(stage==='ORDER_REQUEST_RECEIVED'\)/.test(src));
ok('base yeni denemede temiz', /const base = isNewAttempt/.test(src));
ok('marketSnapshots tasinir', /marketSnapshots:prev\.marketSnapshots\|\|\{\}/.test(src));
ok('previousAttemptId izi', /previousAttemptId:prev\.attemptId\|\|null/.test(src));
ok('requestReceivedTime kosullu', /requestReceivedTime:isNewAttempt\?now:/.test(src));
ok('attemptId kosullu', /const attemptId = isNewAttempt/.test(src));
ok('sayac lifecycleAttemptResets', cnt('lifecycleAttemptResets:0')===1 && cnt('lifecycleAttemptResets\\+\\+')===1);
ok('telemetri lifecyclePerAttempt', cnt('lifecyclePerAttempt:true')===2);

console.log('\n══ F — onceki duzeltmeler bozulmadi ' + '═'.repeat(38));
ok('Q koruma-once', /PROTECT_FIRST_NO_PRECHECK/.test(src)&&/protectFirstInstalls\+\+/.test(src));
ok('S cikis arastirmasi', /closeSnap=rec\.closeResearchSnapshot/.test(src)&&/dCvdRatio:r501Delta/.test(src));
ok('S4 null-guvenli delta', /function r501Num2\(v\)/.test(src));
ok('J2 min hold', /function v592MinHoldGuard/.test(src));
ok('K1 tazelik butcesi', /freshCacheHits\+\+/.test(src));
ok('L fren ayrimi', /function isExecBackoffActive/.test(src));
ok('N WAIT atribusyonu', /waitSource:row\?\.waitSource\|\|null/.test(src));
ok('O testnet evreni', /v592IsTestnetTradable/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('build V4_7_4_41', /V4_7_4_41_PROBE_PERSIST_RISK41_10X/.test(src));
ok('eski build yok', !/V4_7_4_14_CLOSE_RESEARCH/.test(src));
ok('session 4_7_4_41_PS1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_41_PS1/.test(src));

console.log(`\n${'═'.repeat(74)}`);
console.log(fail?`SONUC: FAIL — ${pass} gecti, ${fail} dustu`:`SONUC: PASS — ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
