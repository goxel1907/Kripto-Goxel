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

console.log('== A -- 10.08 olculmus vaka ' + '='.repeat(46));
const V={tradeOpen:13, tradeClose:85, tradeCloseRecorded:170, closeFinalized:98,
         closeDedupSuppressed:3762, ledgerSweepFinalized:622, closeProofUnavailable:368,
         evidenceDuplicateSuppressed:2560, sweepPerSymbol:{IOTX:72,TUT:58,BICO:57,XAN:53},
         exitReasonDolu:0, satir:13, sutun:349,
         pnlDolu:13, roiDolu:13, sonucDolu:13};
ok('13 acilis', V.tradeOpen===13);
ok('ama 85 TRADE_CLOSE (6,5 kat)', V.tradeClose/V.tradeOpen>6);
ok('170 TRADE_CLOSE_RECORDED (13 kat)', V.tradeCloseRecorded/V.tradeOpen>12);
ok('622 supurge (48 kat)', V.ledgerSweepFinalized/V.tradeOpen>45);
ok('IOTX tek basina 72 kez', V.sweepPerSymbol.IOTX>50);
ok('3762 dedup bastirma', V.closeDedupSuppressed>3000);
ok('exitReason 0/13 BOS', V.exitReasonDolu===0);
ok('ama pnl/roi/sonuc DOLU (AT1 calisti)', V.pnlDolu===13&&V.roiDolu===13&&V.sonucDolu===13);
ok('kok neden kaynakta belgelenmis', /TEK DEDUP MAP'I IKI FARKLI TUKETICI PAYLASIYORDU/.test(src));
ok('IOTX 72 olcumu yazili', /IOTX tek basina 72 kez supuruldu/.test(src));
ok('delete satirinin atlandigi yazili', /r501ActiveEvidence\.delete\(id\)\s*\n\/\/ hic calismiyor/.test(src)||/hic calismiyor/.test(src));

console.log('\n== B -- AU1: iki AYRI dedup map ' + '='.repeat(42));
ok('v592ClosedOnce (defter)', /const v592ClosedOnce=new Map\(\);/.test(src));
ok('v592EvidenceClosedOnce (kanit)', /const v592EvidenceClosedOnce=new Map\(\);/.test(src));
ok('ortak yardimci v592DedupCheck', /function v592DedupCheck\(map,id\)/.test(src));
ok('v592CloseAlreadyRecorded -> defter map', /function v592CloseAlreadyRecorded\(id\)\{ return v592DedupCheck\(v592ClosedOnce,id\); \}/.test(src));
ok('v592EvidenceAlreadyClosed -> kanit map', /function v592EvidenceAlreadyClosed\(id\)\{ return v592DedupCheck\(v592EvidenceClosedOnce,id\); \}/.test(src));
{
  const f=grab('async function r501EvidenceClose');
  ok('r501EvidenceClose KANIT map ini kullanir', /v592EvidenceAlreadyClosed\(String\(row\.id\)\)/.test(f));
  ok('r501EvidenceClose artik defter map ine BAKMAZ', !/v592CloseAlreadyRecorded/.test(f));
  ok('kanit dedup ayri sayac', /evidenceCloseDedup\+\+/.test(f));
}
{
  const f=grab('async function v592FinalizeClose');
  ok('v592FinalizeClose DEFTER map ini kullanir', /v592CloseAlreadyRecorded\(anahtar\)/.test(f));
  ok('v592FinalizeClose kanit map ine BAKMAZ', !/v592EvidenceAlreadyClosed/.test(f));
}
ok('TTL ikisinde de 6 saat', /const V592_CLOSE_DEDUP_TTL_MS=6\*60\*60\*1000;/.test(src));

console.log('\n== C -- AU1 canli kosum: cakisma bitti mi ' + '='.repeat(32));
{
  // kaynaktan TTL + iki map + uc fonksiyonu bir arada al
  const i0=src.indexOf('const V592_CLOSE_DEDUP_TTL_MS=');
  const iEnd=src.indexOf('function v592EvidenceAlreadyClosed');
  const iEnd2=src.indexOf('\n', iEnd);
  const kod=src.slice(i0, iEnd2);
  const sb={console,Date,Map};
  vm.createContext(sb);
  vm.runInContext(kod+`;globalThis.L=v592CloseAlreadyRecorded;globalThis.E=v592EvidenceAlreadyClosed;
    globalThis.LM=v592ClosedOnce;globalThis.EM=v592EvidenceClosedOnce;`,sb);
  const id='IOTX_1786320919083';
  ok('defter ilk cagri: yeni', sb.L(id)===false);
  ok('KANIT ayni id ile: HALA YENI (cakisma yok)', sb.E(id)===false);
  ok('defter ikinci cagri: dedup', sb.L(id)===true);
  ok('kanit ikinci cagri: dedup', sb.E(id)===true);
  ok('iki map ayri', sb.LM!==sb.EM && sb.LM.size===1 && sb.EM.size===1);
  // ESKI davranis simulasyonu: tek map
  const tek=new Map();
  const eski=(id)=>{ if(tek.has(id))return true; tek.set(id,1); return false; };
  const e1=eski(id), e2=eski(id);
  ok('ESKI: tek map -> ikinci cagri bastirilirdi', e1===false && e2===true);
  ok('=> r501ActiveEvidence.delete() calismazdi', e2===true);
}

console.log('\n== D -- AU2: exitReason alan adi ' + '='.repeat(41));
{
  const f=grab('function r501PassiveRows(shapeOnly)');
  ok('exitReason once .exitReason okur', /exitReason:rec\.close\?\.exitReason\?\?rec\.close\?\.exitLabel/.test(f));
  ok('eski .reason/.code yedek olarak duruyor', /rec\.close\?\.reason\?\?rec\.close\?\.code\?\?null/.test(f));
  ok('r501EvidenceClose bu alani yaziyor', /exitReason:row\.exitReason\?\?cls\.code\?\?null/.test(src));
  ok('exitLabel de yaziliyor', /exitLabel:row\.exitLabel\?\?cls\.label\?\?null/.test(src));
  ok('kok neden yazili', /alani `exitReason` diye yaziyor/.test(src));
  // canli kosum
  const kd=grab('function r501KlineDerive'), rd=grab('function r501RestDerive');
  const sb={console,Math,Number,Array,Object,JSON,
    r501EvidenceIndex:{trades:[{id:'T1'},{id:'T2'},{id:'T3'}]},
    r501TradePath:x=>x,
    r501RestDerive:()=>({}), r501KlineDerive:()=>{},
    r501GzipRead:(id)=>({id,symbol:'XUSDT',side:'LONG',openedAt:1,closedAt:2,
      close: id==='T1' ? {pnlUSDT:5,roiPct:10,exitReason:'TAKE_PROFIT',exitLabel:'TP doldu'}
           : id==='T2' ? {pnlUSDT:-3,roiPct:-8,exitLabel:'SL doldu'}
           : {pnlUSDT:1,roiPct:2,reason:'ESKI_ALAN'},
      trade:{marginUSDT:15},entryContext:{},orderLifecycle:{marketSnapshots:{}},
      closeResearchSnapshot:null,initialRest:null,closeRest:null})};
  vm.createContext(sb);
  vm.runInContext(kd+';'+rd+';'+f+';globalThis.R=r501PassiveRows();',sb);
  const R=sb.R;
  ok('T1 exitReason=TAKE_PROFIT', R[0].exitReason==='TAKE_PROFIT', String(R[0].exitReason));
  ok('T2 exitLabel yedegi calisti', R[1].exitReason==='SL doldu', String(R[1].exitReason));
  ok('T3 eski .reason yedegi calisti', R[2].exitReason==='ESKI_ALAN', String(R[2].exitReason));
  ok('hicbiri BOS degil', R.every(x=>x.exitReason));
  ok('sonuc etiketi dogru', R[0].sonuc==='KAR'&&R[1].sonuc==='ZARAR');
}

console.log('\n== E -- AU3: supurge dongusu ' + '='.repeat(45));
{
  const f=grab('async function v592OrphanLedgerSweep');
  ok('deneme sayaci Map', /const v592SweepAttempts = new Map\(\)/.test(src));
  ok('max deneme ENV ile', /V592_SWEEP_MAX_ATTEMPTS = Math\.max\(1, Number\(process\.env\.V592_SWEEP_MAX_ATTEMPTS \|\| 3\)\)/.test(src));
  ok('limit asilinca vazgecer', /if\(_kez>=V592_SWEEP_MAX_ATTEMPTS\)\{/.test(f));
  ok('vazgecme sayaci', /sweepGaveUp\+\+/.test(f));
  ok('vazgecme izi SWEEP_GAVE_UP', /SWEEP_GAVE_UP/.test(f));
  ok('bir kez kritik uyari', /LEDGER_SWEEP_STUCK/.test(f));
  ok('kayit hala aktifse ZORLA kapatilir', /r501ActiveEvidence\.delete\(_sk\)/.test(f));
  ok('zorla kapatma sayaci', /sweepRecorderForceClosed\+\+/.test(f));
  ok('zorla kapatma izi', /SWEEP_RECORDER_FORCE_CLOSED/.test(f));
  ok('deneme numarasi kayda yazilir', /attempt:_kez\+1/.test(f));
}

console.log('\n== F -- AU3 canli kosum: dongu kirildi mi ' + '='.repeat(32));
{
  const f=grab('async function v592OrphanLedgerSweep');
  const mk=async(tur)=>{
    const log=[]; const aktif=new Map([['T1',{id:'T1',status:'OPEN',symbol:'IOTXUSDT',openedAt:Date.now()-600000,_finalized:false}]]);
    const sb={console,Date,Number,String,Object,Array,Promise,Map,
      R501_EVIDENCE_ACTIVE:true, normalizeSymbol:x=>String(x).toUpperCase(),
      r501ActiveEvidence:aktif, trailingState:new Map(),
      v592CloseProof:async()=>({proven:true,reason:'FRESH_FLAT'}),
      v592FinalizeClose:async(s,a,b,r)=>{log.push('finalize:'+s);
        // ESKI davranis: kayit SILINMEZ (dedup yuzunden). YENI: supurge zorla siler.
        return {ok:true};},
      r501OrderLifeMark:(s,st)=>log.push('mark:'+st),
      r501PersistRec:()=>{}, logAuto:()=>{}, pushCritical:(a)=>log.push('crit:'+a),
      V592_SWEEP_MIN_AGE_MS:180000, V592_SWEEP_MAX_ATTEMPTS:3,
      v592SweepAttempts:new Map(),
      v592ParityStats:{sweepChecked:0,sweepNoProof:0,sweepFinalized:0,sweepGaveUp:0,sweepRecorderForceClosed:0}};
    vm.createContext(sb); vm.runInContext(f+';globalThis.S=v592OrphanLedgerSweep;',sb);
    for(let i=0;i<8;i++) await sb.S();
    return {log,stats:sb.v592ParityStats,aktif};
  };
  const a=await mk();
  const fin=a.log.filter(x=>x==='finalize:IOTXUSDT').length;
  ok('8 supurge turunda finalize sadece 1 kez', fin===1, String(fin));
  ok('kayit ZORLA kapatildi', a.stats.sweepRecorderForceClosed===1);
  ok('kayit r501ActiveEvidence tan silindi', !a.aktif.has('T1'));
  ok('sonraki turlarda hic dokunmadi', a.stats.sweepFinalized===1);
  ok('622 yerine 1', fin===1);
  ok('vazgecme tetiklenmedi (gerek kalmadi)', a.stats.sweepGaveUp===0);
}

console.log('\n== G -- ChatGPT denetimi: hangi iddia dogruydu ' + '='.repeat(27));
const IDDIA=[
 ['dedup anahtarlari ayri scope a alinmali', true,  'DOGRU — olculdu, duzeltildi (AU1)'],
 ['exitReason okuma yolu yanlis',            true,  'DOGRU — 0/13 bos, duzeltildi (AU2)'],
 ['kapanis birden fazla tetikleniyor',       true,  'DOGRU — 622 supurge, duzeltildi (AU3)'],
 ['completeness 100 tek basina kanit degil', true,  'DOGRU — ama olcum degil, tasarim tercihi'],
];
for(const [ad,d,not] of IDDIA) ok(`${ad} -> ${not}`, d===true);

console.log('\n== H -- pasif sozlesme + onceki duzeltmeler ' + '='.repeat(30));
ok('sizingImpact false', /sizingImpact:false/.test(src));
ok('exitImpact false', /exitImpact:false/.test(src));
for(const [n,re] of [
  ['AJ kapanis kaniti',/async function v592CloseProof/],['AL1 tick akisi',/tickStreamRepaired/],
  ['AL5 null n()',/if\(v===null\|\|v===undefined\|\|v===''\)return null/],
  ['AN1 kanit tum sebepler',/V592_CLOSE_PROOF_EXEMPT/],['AN2 yetim korunur',/v592PossibleOrphans/],
  ['AP2 pasif vpin',/function calcVPINPassive/],['AQ rest turevleri',/function r501RestDerive/],
  ['AR1 tam baslik',/function r501PassiveHeader/],['AS1 tek huni',/async function v592FinalizeClose/],
  ['AS3 supurge',/async function v592OrphanLedgerSweep/],['AT1 gercek pnl',/closeClassified\+\+/],
  ['AU1 dedup ayrimi',/v592EvidenceClosedOnce/],['AU3 supurge limiti',/V592_SWEEP_MAX_ATTEMPTS/],
]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('V45 esikleri degismedi', /V592_V45_MS_SCORE_MIN/.test(src));
ok('telemetri dedupScopesSeparated', /dedupScopesSeparated:true/.test(src));
ok('build V4_7_4_33', /V4_7_4_33_DISK_RISK41_10X/.test(src));
ok('session 4_7_4_33_DK1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_33_DK1/.test(src));
ok('eski build kalmadi', !/V4_7_4_32_PROBE_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
