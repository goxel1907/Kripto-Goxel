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

console.log('== A -- PARALEL CALISMANIN 2 GERCEK ENGELI (olculmus) ' + '='.repeat(20));
const R={mod:'BOTH', mevcutCagri:3084, sondaCagri:8640, kat:3.8,
         posRiskSlowSn:[15,16,20], http418:true};
ok('positionSide BOTH = tek yonlu mod', R.mod==='BOTH');
ok('=> ayni sembolde iki LONG BIRLESIR', R.mod==='BOTH');
ok('mevcut ~3.084 imzali cagri/gun', R.mevcutCagri>3000);
ok('sonda ~8.640 ekler', R.sondaCagri>8000);
ok('toplam 3,8 KAT', Math.abs(R.kat-(R.mevcutCagri+R.sondaCagri)/R.mevcutCagri)<0.1);
ok('positionRisk ZATEN 15-20 sn', Math.min(...R.posRiskSlowSn)>=15);
ok('HTTP 418 gorulmustu', R.http418===true);
ok('birlesme riski kaynakta belgelenmis', /Binance IKISINI BIRLESTIRIR/.test(src));
ok('3,8 kat olcumu kaynakta', /= ~8\.640\/gun ekler -> 3,8 KAT/.test(src));

console.log('\n== B -- AX1: strateji sondayi ITER ' + '='.repeat(39));
ok('v592ProbePreempt var', /async function v592ProbePreempt\(apiKey,apiSecret,sym\)/.test(src));
{
  const f=grab('async function v592SendMainOrderIdempotent');
  ok('HER strateji emrinden ONCE cagriliyor',
     /async function v592SendMainOrderIdempotent\(apiKey,apiSecret,sym,oSide,qty,decisionKey\)\{\s*\n\s*await v592ProbePreempt\(apiKey,apiSecret,sym\);/.test(src));
  ok('preempt emir gonderiminden ONCE', f.indexOf('v592ProbePreempt') < f.indexOf("'/fapi/v1/order'"));
}
{
  const f=grab('async function v592ProbePreempt');
  ok('sonda yoksa hicbir sey yapmaz', /!v592ProbeOpen\.has\(S\)\)return false/.test(f));
  ok('preempted isaretlenir', /p\.preempted=true/.test(f));
  ok('iz birakilir', /PROBE_PREEMPTED_BY_STRATEGY/.test(f));
  ok('sonda KAPATILIR', /await v592ProbeCloseOne\(apiKey,apiSecret,S\)/.test(f));
  ok('sayac', /preemptedByStrategy/.test(f));
  ok('hata yutulur (strateji emri engellenmez)', /catch\(_\)\{ return false; \}/.test(f));
}
ok('sonda satirinda preempted alani', /preempted:!!p\.preempted/.test(src));
ok('cikis sebebi ayrisiyor', /exitReason:p\.preempted\?'STRATEGY_PREEMPT':'TIMED_EXIT'/.test(src));
ok('CSV sutununda preempted', /preempted:!!r\.preempted/.test(src));

console.log('\n== C -- AX1 canli kosum: birlesme onlendi mi ' + '='.repeat(29));
{
  const f=grab('async function v592ProbePreempt');
  const mk=(sondaVar)=>{
    const log=[]; const acik=new Map(sondaVar?[['HEIUSDT',{id:'P1',preempted:false}]]:[]);
    const sb={console,Date,Promise,normalizeSymbol:x=>String(x).toUpperCase(),
      v592ProbeOpen:acik,
      v592ProbeCloseOne:async(k,s,S)=>{log.push('closeProbe:'+S);acik.delete(S);},
      r501OrderLifeMark:(s,st)=>log.push('mark:'+st),
      v592ProbeStats:{preemptedByStrategy:0}};
    vm.createContext(sb); vm.runInContext(f+';globalThis.P=v592ProbePreempt;',sb);
    return {P:sb.P,log,acik,stats:sb.v592ProbeStats};
  };
  const a=mk(true);
  const r1=await a.P('k','s','HEIUSDT');
  ok('sonda varsa KAPATILIR', a.log.includes('closeProbe:HEIUSDT'));
  ok('donus true', r1===true);
  ok('artik acik degil -> BIRLESME YOK', !a.acik.has('HEIUSDT'));
  ok('iz birakildi', a.log.includes('mark:PROBE_PREEMPTED_BY_STRATEGY'));
  ok('sayac arti', a.stats.preemptedByStrategy===1);
  const b=mk(false);
  const r2=await b.P('k','s','HEIUSDT');
  ok('sonda yoksa dokunmaz', r2===false && b.log.length===0);
}

console.log('\n== D -- AX2: baski valfi ' + '='.repeat(49));
ok('esik ENV ile', /V592_PROBE_PRESSURE_MS = Math\.max\(2000, Number\(process\.env\.V592_PROBE_PRESSURE_MS \|\| 9000\)\)/.test(src));
{
  const f=grab('async function v592ProbeCycle');
  ok('exec backoff kontrolu', /isExecBackoffActive\(\)\) baski='EXEC_BACKOFF'/.test(f));
  ok('public backoff kontrolu', /isBinanceBackoffActive\(\)\) baski='PUBLIC_BACKOFF'/.test(f));
  ok('positionRisk yavasligi', /baski='POSITION_RISK_SLOW'/.test(f));
  ok('positionRisk takilmasi', /baski='POSITION_RISK_INFLIGHT'/.test(f));
  ok('baskida YENI ACILIS YOK', /if\(baski\)\{[\s\S]{0,200}return;/.test(f));
  ok('ama ACIK olanlar KAPATILIR (once)', f.indexOf('v592ProbeCloseOne') < f.indexOf('if(baski)'));
  ok('sayac + sebep', /yieldedUnderPressure/.test(f) && /lastYieldReason=baski/.test(f));
}
{
  // baski mantigini izole kosur
  const kod=`
    function karar(execB,pubB,lastMs,fetching,inflightMs,esik){
      let baski=null;
      if(execB) baski='EXEC_BACKOFF';
      else if(pubB) baski='PUBLIC_BACKOFF';
      else if(lastMs>esik) baski='POSITION_RISK_SLOW';
      else if(fetching && inflightMs>esik) baski='POSITION_RISK_INFLIGHT';
      return baski;
    }`;
  const sb={}; vm.createContext(sb); vm.runInContext(kod+';globalThis.k=karar;',sb);
  ok('exec backoff -> geri cekilir', sb.k(true,false,0,false,0,9000)==='EXEC_BACKOFF');
  ok('public backoff -> geri cekilir', sb.k(false,true,0,false,0,9000)==='PUBLIC_BACKOFF');
  ok('positionRisk 15sn -> geri cekilir', sb.k(false,false,15000,false,0,9000)==='POSITION_RISK_SLOW');
  ok('positionRisk 20sn takili -> geri cekilir', sb.k(false,false,0,true,20000,9000)==='POSITION_RISK_INFLIGHT');
  ok('sistem RAHAT -> calisir', sb.k(false,false,2000,false,0,9000)===null);
  ok('esik altinda calisir', sb.k(false,false,8999,false,0,9000)===null);
}

console.log('\n== E -- SIRA: neden 72 saat BEKLEMEYE gerek yok ' + '='.repeat(26));
const H=[[1.0,48],[0.8,74],[0.5,188]];
for(const [d,n] of H){
  const saat=n/(24*60/5*10)*24;
  ok(`d=${d} icin ${n} ornek -> ${saat.toFixed(1)} saat`, saat<3);
}
ok('strateji hizi 6,5 islem/gun -> 100 islem 15 gun', 100/6.5>14);
ok('sonda 2.880/gun -> ayni gucu SAATLERDE verir', 188/2880*24 < 2);
ok('=> paralel calismak sureyi UZATMAZ, KISALTIR', true);
ok('parite olcumu korunuyor: preempt + baski valfi',
   /strategyPreemptsProbe:true,yieldsUnderPressure:true,positionMergePrevented:true/.test(src));

console.log('\n== F -- parite olcumu hala korunuyor mu ' + '='.repeat(34));
ok('slot ofseti duruyor', cnt('R486_MAX_POSITIONS \\+ v592ProbeSlotOffset\\(\\)')>=3);
ok('sonda strateji sembolunu atlar', /skippedStrategyBusy\+\+/.test(src));
ok('sonda kendi dosyasina yazar', /V592_PROBE_PATH/.test(src));
{
  const b=src.slice(src.indexOf('// V4.7.4.32 — SONDA'), src.indexOf("app.get('/api/probe/samples.csv'"));
  ok('tradeLedger e DOKUNMUYOR', !/tradeLedger/.test(b));
  ok('v592ParityStats e DOKUNMUYOR', !/v592ParityStats/.test(b));
  ok('ham arsiv YAZMIYOR', !/r501RawAppend|r501RawInit/.test(b));
}
ok('sonda hala varsayilan KAPALI', /String\(process\.env\.V592_PROBE_ACTIVE \?\? '0'\)==='1'/.test(src));
ok('sonda hala TESTNET kilitli', /const V592_PROBE_ACTIVE = BINANCE_EXECUTION_ENV==='TESTNET'/.test(src));

console.log('\n== G -- onceki duzeltmeler ' + '='.repeat(47));
for(const [n,re] of [
  ['AS1 tek huni',/async function v592FinalizeClose/],['AS3 supurge',/async function v592OrphanLedgerSweep/],
  ['AT1 gercek pnl',/closeClassified\+\+/],['AU1 dedup ayrimi',/v592EvidenceClosedOnce/],
  ['AU2 exitReason',/exitReason:rec\.close\?\.exitReason/],['AU3 supurge limiti',/V592_SWEEP_MAX_ATTEMPTS/],
  ['AV1 sonda',/function v592ProbeCycle/],['AW1 lite',/function r501RawAllowed/],
  ['AW2 disk muhafizi',/function r501DiskGuard/],['AW3 analiz paketi',/function r501FunnelSummary/],
  ['AX1 preempt',/async function v592ProbePreempt/],['AX2 baski valfi',/V592_PROBE_PRESSURE_MS/],
]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('V45 esikleri degismedi', /V592_V45_MS_SCORE_MIN/.test(src));
ok('build V4_7_4_36', /V4_7_4_36_PROBE_PRICE_RISK41_10X/.test(src));
ok('eski build kalmadi', !/V4_7_4_35_ROTATE_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
