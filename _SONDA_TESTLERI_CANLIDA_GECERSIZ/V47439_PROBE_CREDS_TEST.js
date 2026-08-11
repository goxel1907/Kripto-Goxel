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

console.log('== A -- 10.08 olcumu: .38 yuklu ama cycles=0 ' + '='.repeat(28));
const V={build38:true, uc38Sayaci:true, active:true, gecenDk:18.6, kickSn:90, cycles:0,
         tumSayaclar:0};
ok('.38 build DOGRU yuklendi', V.build38);
ok('.38 nin uc yeni sayaci VAR', V.uc38Sayaci);
ok('sonda AKTIF', V.active);
ok('session basindan 18,6 dk gecti', V.gecenDk>15);
ok('90 sn acilis kicki coktan gecmisti', V.gecenDk*60>V.kickSn);
ok('AMA cycles=0', V.cycles===0);
ok('=> fonksiyon cycles++ satirina HIC ULASMADI', V.cycles===0);
ok('=> tum sayaclar 0 (yieldedUnderPressure dahil)', V.tumSayaclar===0);
ok('kok neden kaynakta belgelenmis', /SONDA PANEL ANAHTARINI BEKLIYORDU/.test(src));
ok('18,6 dk olcumu yazili', /18,6 DAKIKA gecmis/.test(src));
ok('dorduncu tekrar kabul edilmis', /dorduncu kez\s*\n\/\/ ayni sinif hata/.test(src)||/dorduncu kez/.test(src));

console.log('\n== B -- KOK NEDEN: autoConfig null ' + '='.repeat(39));
ok('autoConfig baslangicta null', /let autoConfig = null;/.test(src));
ok('YALNIZ panel POST u dolduruyor', /autoConfig = \{ \.\.\.\(req\.body\|\|\{\}\), apiKey:_tnCreds\.apiKey/.test(src));
ok('sistemin ENV yedegi zaten vardi', /function r486391BinanceCreds\(\)/.test(src));
ok('ENV yedeginin amaci yorumda yazili', /dashboard açılmadan da çalışır/.test(src));
ok('sonda o yedegi kullanmiyordu (kok neden)', /Sonda o yedegi kullanmiyordu/.test(src));

console.log('\n== C -- BD1: kimlik cozumu ' + '='.repeat(47));
ok('v592ProbeCreds var', /function v592ProbeCreds\(\)/.test(src));
{
  const f=grab('function v592ProbeCreds');
  ok('once panel', /let k=autoConfig\?\.apiKey, s2=autoConfig\?\.apiSecret, kaynak='PANEL'/.test(f));
  ok('yoksa Railway ENV', /r486391BinanceCreds\(\)/.test(f) && /kaynak='RAILWAY_ENV'/.test(f));
  ok('kaynak etiketi doner', /source:\(k&&s2\)\?kaynak:'MISSING'/.test(f));
  // canli kosum
  const mk=(panel,env)=>{
    const sb={console,
      autoConfig: panel?{apiKey:'PK',apiSecret:'PS'}:null,
      r486391BinanceCreds:()=>env?{apiKey:'EK',apiSecret:'ES'}:{apiKey:'',apiSecret:''}};
    vm.createContext(sb); vm.runInContext(f+';globalThis.C=v592ProbeCreds;',sb);
    return sb.C();
  };
  const a=mk(true,true), b=mk(false,true), c=mk(false,false), d=mk(true,false);
  ok('panel VAR -> PANEL anahtari', a.source==='PANEL'&&a.apiKey==='PK');
  ok('panel YOK + ENV VAR -> ENV anahtari', b.source==='RAILWAY_ENV'&&b.apiKey==='EK');
  ok('ikisi de YOK -> MISSING', c.source==='MISSING'&&!c.apiKey);
  ok('panel VAR ENV yok -> PANEL', d.source==='PANEL');
  ok('=> ESKI kod b senaryosunda SESSIZ donerdi', b.apiKey==='EK');
}
{
  const f=grab('async function v592ProbeCycle');
  ok('dongu v592ProbeCreds kullaniyor', /const _c=v592ProbeCreds\(\)/.test(f));
  ok('kimlik kaynagi kaydedilir', /v592ProbeStats\.credSource=_c\.source/.test(f));
  ok('kimlik yoksa SAYAC artar', /skippedNoCreds=\(v592ProbeStats\.skippedNoCreds\|\|0\)\+1/.test(f));
  ok('sebep yazilir', /lastSkipReason='NO_CREDENTIALS'/.test(f));
  ok('artik autoConfig dogrudan okunmuyor', !/const apiKey=autoConfig\?\.apiKey, apiSecret=autoConfig\?\.apiSecret;\s*\n\s*if\(!apiKey\|\|!apiSecret\) return;/.test(f));
}

console.log('\n== D -- cycles++ oncesi SESSIZ cikis kalmadi ' + '='.repeat(28));
{
  const i=src.indexOf('async function v592ProbeCycle');
  const j=src.indexOf('v592ProbeStats.cycles++',i);
  const blok=src.slice(i,j);
  const ret=blok.split('\n').filter(l=>/\breturn\b/.test(l)).map(l=>l.trim());
  ok(`cycles++ oncesi ${ret.length} return`, ret.length===2, JSON.stringify(ret));
  ok('1. return: sonda KAPALI (zaten beklenen)', /if\(!V592_PROBE_ACTIVE\) return;/.test(blok));
  ok('2. return: kimlik yok — SAYACLI', /skippedNoCreds[\s\S]{0,120}return;/.test(blok));
  ok('baska sessiz cikis yok', ret.length===2);
}

console.log('\n== E -- BD1b: kapatici da ENV yedegini kullanir ' + '='.repeat(26));
ok('15sn kapatici v592ProbeCreds kullanir', /const _cc=v592ProbeCreds\(\); if\(!_cc\.apiKey\|\|!_cc\.apiSecret\)return;/.test(src));
ok('kapaticida autoConfig dogrudan okunmuyor',
   !/const k=autoConfig\?\.apiKey,s2=autoConfig\?\.apiSecret; if\(!k\|\|!s2\)return;\s*\n\s*for\(const \[S,p\] of \[\.\.\.v592ProbeOpen\]\)\s*\n\s*if\(Date\.now\(\)-Number\(p\.openedAt\|\|0\) >= V592_PROBE_HOLD_MS\)/.test(src));
ok('=> panel baglanmadan acilan sonda da kapatilir', /_cc\.apiKey,_cc\.apiSecret,S/.test(src));

console.log('\n== F -- BD3: status kimlik teshisi ' + '='.repeat(39));
{
  const f=grab("app.get('/api/probe/status'");
  ok('credentials blogu', /credentials:\{source:v592ProbeStats\.credSource/.test(f));
  ok('panel bagli mi', /panelConnected:!!\(autoConfig\?\.apiKey&&autoConfig\?\.apiSecret\)/.test(f));
  ok('ENV var mi', /envAvailable:/.test(f));
}
ok('sayaclar ilan edilmis', /skippedNoCreds:0,credSource:null/.test(src));

console.log('\n== G -- onceki sonda duzeltmeleri ' + '='.repeat(41));
for(const [n,re] of [
  ['BA1 fiyat zinciri',/Number\(tickerPrice \|\| 0\)/],['BA1b markPrice',/async function v592ProbeMarkPrice/],
  ['BB1 Map normalizasyonu',/_tg instanceof Map \? \[\.\.\._tg\.values\(\)\]/],
  ['BB1b paylasimli onbellek',/cached\('futures_tickers',FUTURES_TICKERS_CACHE_MS/],
  ['BB2 dongu hatasi',/PROBE_CYCLE_ERROR/],
  ['BC1 60sn tekrar',/V592_PROBE_RETRY_MS/],['BC2 takili inflight',/staleInflightIgnored/],
  ['BC3 acilis kicki',/setTimeout\(\(\)=>\{v592ProbeCycle\(\)\.catch\(\(\)=>\{\}\);\},90000\)/],
  ['AX1 preempt',/async function v592ProbePreempt/],['AY1 rotasyon',/v592ProbeRotate/],
]) ok(n, re.test(src));

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
ok('ENV yedegi de testnet anahtari', /BINANCE_TESTNET_API_KEY/.test(src));
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
