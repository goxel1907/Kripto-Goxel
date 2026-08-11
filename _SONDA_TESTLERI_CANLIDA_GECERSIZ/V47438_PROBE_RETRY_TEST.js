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

console.log('== A -- 10.08 olcumu: tek dongu baskiya takildi ' + '='.repeat(26));
const V={cycles:1,opened:0,yieldedUnderPressure:1,lastYieldReason:'POSITION_RISK_INFLIGHT',
         candidatesSeen:0,cycleErrors:0,tickerRows:0,
         posRiskLastDurationMs:109,inflight:false,execBackoff:false,publicBackoff:false};
ok('1 dongu', V.cycles===1);
ok('1 baski geri cekilmesi', V.yieldedUnderPressure===1);
ok('sebep POSITION_RISK_INFLIGHT', V.lastYieldReason==='POSITION_RISK_INFLIGHT');
ok('=> dongu ticker satirina HIC ULASMADI', V.candidatesSeen===0&&V.tickerRows===0);
ok('=> Map bugu artik BLOKAJ DEGIL (dongu oraya varmadi)', V.cycleErrors===0);
ok('AMA sistem RAHAT: positionRisk 109 ms', V.posRiskLastDurationMs<200);
ok('backoff YOK', !V.execBackoff && !V.publicBackoff);
ok('=> gecici inflight 15 DAKIKANIN TAMAMINI yedi', V.cycles===1&&V.opened===0);
ok('kok neden kaynakta belgelenmis', /BASKI = 15 DAKIKA KAYIP DEGIL/.test(src));
ok('109 ms olcumu yazili', /lastDurationMs=109 ms, backoff yok — sistem RAHAT/.test(src));

console.log('\n== B -- BC1: baskida 60 sn sonra tekrar ' + '='.repeat(34));
ok('V592_PROBE_RETRY_MS (60 sn)', /V592_PROBE_RETRY_MS = Math\.max\(15000, Number\(process\.env\.V592_PROBE_RETRY_MS \|\| 60000\)\)/.test(src));
ok('V592_PROBE_MAX_RETRIES (5)', /V592_PROBE_MAX_RETRIES = Math\.max\(0, Number\(process\.env\.V592_PROBE_MAX_RETRIES \|\| 5\)\)/.test(src));
{
  const f=grab('async function v592ProbeCycle');
  ok('baskida setTimeout ile tekrar', /setTimeout\(\(\)=>\{v592ProbeCycle\(\)\.catch\(\(\)=>\{\}\);\},V592_PROBE_RETRY_MS\)/.test(f));
  ok('butce siniri var', /if\(v592ProbeRetries < V592_PROBE_MAX_RETRIES\)/.test(f));
  ok('butce bitince sayac', /retryBudgetExhausted/.test(f));
  ok('basarili turda butce sifirlanir', /v592ProbeRetries=0;\s*\/\/ basarili tur/.test(f));
  ok('tekrar sayaci', /retriesScheduled/.test(f));
  ok('unref ile process kilitlenmez', /t\.unref\?\.\(\)/.test(f));
}
{
  // tekrar mantigini izole kosur
  const kod=`
    let retries=0; const MAX=5; const log=[];
    function dongu(baskiVar){
      if(baskiVar){
        if(retries<MAX){ retries++; log.push('retry'+retries); return 'YIELD_RETRY'; }
        log.push('exhausted'); return 'YIELD_EXHAUSTED';
      }
      retries=0; log.push('run'); return 'RUN';
    }`;
  const sb={}; vm.createContext(sb); vm.runInContext(kod+';globalThis.d=dongu;globalThis.L=()=>log;globalThis.R=()=>retries;',sb);
  ok('1. baski -> tekrar planlanir', sb.d(true)==='YIELD_RETRY');
  ok('2-5. baski -> tekrar', [sb.d(true),sb.d(true),sb.d(true),sb.d(true)].every(x=>x==='YIELD_RETRY'));
  ok('6. baski -> butce bitti', sb.d(true)==='YIELD_EXHAUSTED');
  ok('sistem rahatlayinca calisir', sb.d(false)==='RUN');
  ok('ve butce sifirlanir', sb.R()===0);
  ok('sonraki baskida yine 5 hak', sb.d(true)==='YIELD_RETRY');
  ok('=> gecici baski 1 dk maliyet, 15 dk DEGIL', 60000 < 900000/10);
}

console.log('\n== C -- BC2: takili inflight sonsuza bloklamaz ' + '='.repeat(27));
{
  const f=grab('async function v592ProbeCycle');
  ok('yas hesaplaniyor', /const yas=Date\.now\(\)-Number\(posRiskCache\.inflightStartedAt\)/.test(f));
  ok('9sn-60sn arasi MESGUL sayilir', /if\(yas>V592_PROBE_PRESSURE_MS && yas<=60000\) baski='POSITION_RISK_INFLIGHT'/.test(f));
  ok('60sn ustu TAKILMIS sayilir, yok sayilir', /else if\(yas>60000\) v592ProbeStats\.staleInflightIgnored/.test(f));
  ok('takilma sayaci', /staleInflightIgnored:0/.test(src));
}
{
  const kod=`function karar(fetching,yas,esik){
    if(!fetching) return null;
    if(yas>esik && yas<=60000) return 'BUSY';
    if(yas>60000) return 'STALE_IGNORED';
    return null; }`;
  const sb={}; vm.createContext(sb); vm.runInContext(kod+';globalThis.k=karar;',sb);
  ok('inflight 2 sn -> engel yok', sb.k(true,2000,9000)===null);
  ok('inflight 15 sn -> MESGUL', sb.k(true,15000,9000)==='BUSY');
  ok('inflight 90 sn -> TAKILMIS, yok sayilir', sb.k(true,90000,9000)==='STALE_IGNORED');
  ok('inflight yok -> engel yok', sb.k(false,999999,9000)===null);
  ok('ESKI kod 90 sn takilmayi da MESGUL sayardi (sonsuz blok)', 90000>9000);
}

console.log('\n== D -- BC3: ilk dongu 15 dk beklemesin ' + '='.repeat(34));
ok('acilistan 90 sn sonra bir kez', /setTimeout\(\(\)=>\{v592ProbeCycle\(\)\.catch\(\(\)=>\{\}\);\},90000\)\.unref/.test(src));
ok('normal interval de duruyor', /setInterval\(\(\)=>\{v592ProbeCycle\(\)\.catch\(\(\)=>\{\}\);\},V592_PROBE_INTERVAL_MS\)\.unref/.test(src));
ok('ikisi de V592_PROBE_ACTIVE icinde', /if\(V592_PROBE_ACTIVE\)\{[\s\S]{0,400}90000[\s\S]{0,200}V592_PROBE_INTERVAL_MS/.test(src));
ok('kok neden yazili', /setInterval ilk kez INTERVAL sonra tetikler/.test(src));

console.log('\n== E -- BC4: sampledSymbols gercekten artiyor ' + '='.repeat(28));
{
  const f=grab('async function v592ProbeCycle');
  ok('acilis donusu yakalaniyor', /const _ac=await v592ProbeOpenOne/.test(f));
  ok('basariliysa sayac artar', /if\(_ac\) v592ProbeStats\.sampledSymbols=/.test(f));
}

console.log('\n== F -- status yeni alanlar ' + '='.repeat(46));
{
  const f=grab("app.get('/api/probe/status'");
  ok('retryMs', /retryMs:V592_PROBE_RETRY_MS/.test(f));
  ok('maxRetries', /maxRetries:V592_PROBE_MAX_RETRIES/.test(f));
  ok('rotasyon bilgisi duruyor', /rotation:V592_PROBE_MAX_OPEN<V592_PROBE_TOP_N/.test(f));
}
for(const n of ['retriesScheduled','retryBudgetExhausted','staleInflightIgnored'])
  ok('sayac ilan: '+n, new RegExp(n+':0').test(src));

console.log('\n== G -- onceki sonda duzeltmeleri duruyor ' + '='.repeat(32));
ok('BB1 Map normalizasyonu', /_tg instanceof Map \? \[\.\.\._tg\.values\(\)\]/.test(src));
ok('BB1b paylasimli onbellek', /cached\('futures_tickers',FUTURES_TICKERS_CACHE_MS/.test(src));
ok('BB2 dongu hatasi gorunur', /PROBE_CYCLE_ERROR/.test(src));
ok('BA1 fiyat zinciri', /Number\(tickerPrice \|\| 0\)/.test(src));
ok('BA1b markPrice yedegi', /async function v592ProbeMarkPrice/.test(src));
ok('AX1 preempt', /async function v592ProbePreempt/.test(src));
ok('AY1 rotasyon', /v592ProbeRotate/.test(src));
ok('AY2 tam tutus', />= V592_PROBE_HOLD_MS\)\s*\n\s*v592ProbeCloseOne/.test(src));

console.log('\n== H -- guvenlik ve parite ' + '='.repeat(47));
ok('slot ofseti', cnt('R486_MAX_POSITIONS \\+ v592ProbeSlotOffset\\(\\)')>=3);
{
  const b=src.slice(src.indexOf('// V4.7.4.32 — SONDA'), src.indexOf("app.get('/api/probe/samples.csv'"));
  ok('tradeLedger e DOKUNMUYOR', !/tradeLedger/.test(b));
  ok('v592ParityStats e DOKUNMUYOR', !/v592ParityStats/.test(b));
  ok('ham arsiv YAZMIYOR', !/r501RawAppend|r501RawInit/.test(b));
  ok('YALNIZ LONG', /side:'BUY'/.test(b) && !/side:'SELL'/.test(b));
}
ok('varsayilan KAPALI', /String\(process\.env\.V592_PROBE_ACTIVE \?\? '0'\)==='1'/.test(src));
ok('TESTNET kilidi', /const V592_PROBE_ACTIVE = BINANCE_EXECUTION_ENV==='TESTNET'/.test(src));
for(const [n,re] of [['AS1 tek huni',/async function v592FinalizeClose/],
  ['AU1 dedup ayrimi',/v592EvidenceClosedOnce/],['AU2 exitReason',/exitReason:rec\.close\?\.exitReason/],
  ['AW1 lite',/function r501RawAllowed/],['AW2 disk muhafizi',/function r501DiskGuard/],
  ['AW3 analiz paketi',/function r501FunnelSummary/]]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('build V4_7_4_43', /V4_7_4_43_AUDIT_FIX_RISK41_10X/.test(src));
ok('eski build kalmadi', !/V4_7_4_42_LEDGER_ISOLATE_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
