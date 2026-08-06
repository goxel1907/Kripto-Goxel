(async()=>{
const fs=require('fs'),vm=require('vm'),path=require('path');
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
// 05.08 TUT vakasinin gercek gecikmeleri
const GECIKME={openOrders:4200, accountPerDeneme:8800, algoWrite:240, proofRead:380};

function mkCtx({firstInstall,borsadaKoruma,pozisyonVar,log}){
  let saat=0; const ilerle=(ms)=>{saat+=ms;};
  let yazildi=false;   // gercek dunya: SL/TP yazildiktan SONRA proof gorunur olur
  const ctx=vm.createContext({
    V592_PROTECT_KEEP_EXISTING:true, POS_FRESH_ORDER_MS:3000, POS_FRESH_MANAGER_MS:8000,
    v592ParityStats:{protectionKeptExisting:0,protectFirstInstalls:0,protectFirstMs:0,
                     orphanProtectionCleaned:0,postWriteProofFail:0,postFillProofRescues:0},
    r501OrderLifeMark:(sym,st)=>{log.push(`${String(saat).padStart(5)}ms  ${st}`);},
    verifyAlgoSLTPVisible:async()=>{ilerle(GECIKME.openOrders);log.push(`${String(saat).padStart(5)}ms  [ag] openOrders`);
      const g=yazildi||borsadaKoruma;
      return {ok:g,foundSL:g,foundTP:g,orderCount:g?2:0};},
    freshOpenPositionForSymbol:async(k,s,sym,att)=>{
      for(let i=0;i<att;i++){ilerle(GECIKME.accountPerDeneme);log.push(`${String(saat).padStart(5)}ms  [ag] account #${i+1}`);}
      return pozisyonVar?{open:true,pos:{positionAmt:'1',markPrice:'0.0255'}}:{open:false,pos:null};},
    fetchPositionRiskRaw:async()=>{ilerle(GECIKME.accountPerDeneme);return [];},
    bReq:async()=>{ilerle(3000);return [];},
    cancelAlgoOrders:async()=>{ilerle(600);log.push(`${String(saat).padStart(5)}ms  cancelAlgoOrders`);},
    cleanupClosedPositionState:async()=>{},
    normalizeSLTPToTick:async(s,a,b)=>({sl:a,tp:b,slNum:a,tpNum:b,tickSize:1e-6}),
    placeAlgoSL:async()=>{ilerle(GECIKME.algoWrite);log.push(`${String(saat).padStart(5)}ms  [ag] SL YAZILDI`);return {orderId:'SL1'};},
    placeAlgoTP:async()=>{ilerle(GECIKME.algoWrite);yazildi=true;log.push(`${String(saat).padStart(5)}ms  [ag] TP YAZILDI`);return {orderId:'TP1'};},
    bAlgo:async()=>{ilerle(GECIKME.algoWrite);return {algoId:'A1'};},
    bPub:async()=>({price:'0.0255'}), pushCritical:()=>{}, isNoOpenPositionAlgoError:()=>false,
    formatStepValue:v=>v, sleep:async()=>{}, console:{log:()=>{}},
    setTimeout:(f)=>{f();return 0;}, Date:{now:()=>1785936789536+saat}, Math,Number,String,Object,Array,Boolean,Promise,Infinity,JSON,Error,
  });
  vm.runInContext(grab('async function installSLTPWithProof'),ctx);
  return ctx;
}

console.log('══ A — 05.08 TUT vakasinin tekrari ' + '═'.repeat(38));
{
  const eski=[],yeni=[];
  // ESKI davranis = firstInstall verilmemis (yonetici yolu gibi)
  await mkCtx({firstInstall:false,borsadaKoruma:false,pozisyonVar:true,log:eski})
    .installSLTPWithProof('k','s','TUTUSDT','SELL',0.0249,0.0261,'TUTUSDT');
  await mkCtx({firstInstall:true,borsadaKoruma:false,pozisyonVar:true,log:yeni})
    .installSLTPWithProof('k','s','TUTUSDT','SELL',0.0249,0.0261,'TUTUSDT',{firstInstall:true});
  const ilkYazim=(l)=>{const x=l.find(z=>z.includes('SL YAZILDI'));return x?parseInt(x):null;};
  const e=ilkYazim(eski), y=ilkYazim(yeni);
  console.log('\n  ESKI (on-dogrulama once):');  eski.slice(0,6).forEach(x=>console.log('    '+x));
  console.log('\n  YENI (koruma once):');        yeni.slice(0,6).forEach(x=>console.log('    '+x));
  console.log();
  ok(`ESKI: ilk SL/TP ${e}ms (>25sn — pozisyon oluyordu)`, e>25000, `${e}`);
  ok(`YENI: ilk SL/TP ${y}ms (<1sn)`, y<1000, `${y}`);
  ok(`kazanc ${Math.round((e-y)/1000)} saniye`, (e-y)>25000, `${e-y}ms`);
  ok('YENI yolda openOrders on-cagrisi YOK', !yeni.slice(0,3).some(x=>x.includes('openOrders')));
  ok('YENI yolda account on-cagrisi YOK', !yeni.slice(0,3).some(x=>x.includes('account #')));
  ok('PROTECT_FIRST_NO_PRECHECK isaretlendi', yeni.some(x=>x.includes('PROTECT_FIRST_NO_PRECHECK')));
}

console.log('\n══ B — yetim koruma temizligi ' + '═'.repeat(43));
{
  const log=[];
  const c=mkCtx({firstInstall:true,borsadaKoruma:false,pozisyonVar:false,log});
  const r=await c.installSLTPWithProof('k','s','X','SELL',1,2,'X',{firstInstall:true});
  ok('pozisyon yoksa skippedClosed', r.skippedClosed===true, JSON.stringify(r).slice(0,90));
  ok('yetim koruma iptal edildi', r.orphanCleaned===true && log.some(x=>x.includes('cancelAlgoOrders')));
  ok('sayac orphanProtectionCleaned', c.v592ParityStats.orphanProtectionCleaned===1);
  ok('ORPHAN_PROTECTION_CLEANED izi', log.some(x=>x.includes('ORPHAN_PROTECTION_CLEANED')));
  ok('koruma YINE DE once yazildi', log.some(x=>x.includes('SL YAZILDI')),
     'once yaz, sonra temizle — ters degil');
}

console.log('\n══ C — yonetici yolu DEGISMEDI (J1 korunur) ' + '═'.repeat(30));
{
  const log=[];
  const c=mkCtx({firstInstall:false,borsadaKoruma:true,pozisyonVar:true,log});
  const r=await c.installSLTPWithProof('k','s','X','SELL',1,2,'X');
  ok('J1 mevcut korumayi korudu', r.keptExisting===true);
  ok('sayac protectionKeptExisting', c.v592ParityStats.protectionKeptExisting===1);
  ok('cancelAlgoOrders CAGRILMADI', !log.some(x=>x.includes('cancelAlgoOrders')));
  const log2=[]; const c2=mkCtx({firstInstall:true,borsadaKoruma:true,pozisyonVar:true,log:log2});
  const r2=await c2.installSLTPWithProof('k','s','X','SELL',1,2,'X',{firstInstall:true});
  ok('ilk kurulumda J1 on-kontrolu ATLANIR', r2.keptExisting!==true && c2.v592ParityStats.protectionKeptExisting===0);
}

console.log('\n══ D — kaynak sozlesmesi ' + '═'.repeat(49));
ok('imza opts aldi', /installSLTPWithProof\(apiKey, apiSecret, symbol, closeSide, slPrice, tpPrice, sym, opts=\{\}\)/.test(src));
ok('J1 kosulunda !firstInstall', /V592_PROTECT_KEEP_EXISTING && !firstInstall/.test(src));
ok('on-kontrol firstInstall ile atlanir', /firstInstall\s*\n?\s*\?\s*\{ open:null, pos:null, skippedPreCheck:true \}/.test(src));
ok('emir yolu firstInstall:true', cnt("\\{firstInstall:true\\}")===1);
ok('yonetici cagrilari opts VERMIYOR', cnt("installSLTPWithProof\\(apiKey, apiSecret, sym, cSide, safeNewSL")===1
   && cnt("installSLTPWithProof\\(autoConfig\\.apiKey")===1);
ok('PROTECTION_LATENCY olcumu', /r501OrderLifeMark\(sym,'PROTECTION_LATENCY'/.test(src));
for(const [c,n] of [['protectFirstInstalls',1],['orphanProtectionCleaned',1],['postWriteProofFail',2]])
  ok(`sayac ${c}`, cnt(`${c}:0`)===1 && cnt(`v592ParityStats\\.${c}\\+\\+`)===n,
     `${cnt(`v592ParityStats\\.${c}\\+\\+`)}`);
ok('telemetri protectFirstActive', cnt('protectFirstActive:true')===2);
ok('yasam dongusune yeni asamalar', /PROTECT_FIRST_NO_PRECHECK','PROTECTION_LATENCY'/.test(src));

console.log('\n══ E — onceki duzeltmeler bozulmadi ' + '═'.repeat(38));
ok('J2 min hold', /function v592MinHoldGuard/.test(src)&&/minHoldBlocks\+\+/.test(src));
ok('J3 bayat emir', /_lag>V592_MAX_REQUEST_TO_SEND_MS/.test(src));
ok('K1 tazelik butcesi', /freshCacheHits\+\+/.test(src));
ok('L fren ayrimi', /function isExecBackoffActive/.test(src));
ok('N WAIT atribusyonu', /waitSource:row\?\.waitSource\|\|null/.test(src));
ok('O testnet evreni', /v592IsTestnetTradable/.test(src));
ok('G2 post-fill kanit kurtarma', /postFillProofRescues\+\+/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('build V4_7_4_19', /V4_7_4_19_MANAGER_EXIT_GUARD_RISK41_10X/.test(src));
ok('eski build yok', !/V4_7_4_12_WAIT_ATTRIBUTION/.test(src));
ok('session 4_7_4_19_MG1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_19_MG1/.test(src));

console.log(`\n${'═'.repeat(74)}`);
console.log(fail?`SONUC: FAIL — ${pass} gecti, ${fail} dustu`:`SONUC: PASS — ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
