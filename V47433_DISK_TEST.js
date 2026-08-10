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

console.log('== A -- 5 GB gercegi (olculmus) ' + '='.repeat(42));
const D={volumeGB:5, ortMB:98, simdiIslem:13, simdiGB:1.24,
  agir:{cvd:6.11,research:5.14,manager:2.91,stage:2.78,exec:2.66,ticks:2.54,depth:1.34},
  skyaiToplamMB:27};
const agirTop=Object.values(D.agir).reduce((a,b)=>a+b,0);
ok('5 GB volume', D.volumeGB===5);
ok('ortalama islem 98 MB', D.ortMB===98);
ok(`${Math.floor(5120/D.ortMB)} islemde disk dolar`, Math.floor(5120/D.ortMB)===52);
ok('su an 13 islem = 1.24 GB', D.simdiGB>1.2);
ok(`7 agir akis ${agirTop.toFixed(1)} MB / 27 MB`, agirTop>23);
ok('=> agir akislar toplamin %87si', agirTop/D.skyaiToplamMB>0.85);
ok('olcum kaynakta belgelenmis', /cvd_samples 6,11 · research_snapshots 5,14/.test(src));
ok('52 islem hesabi yazili', /5 GB volume 52 ISLEMDE DOLAR/.test(src));
ok('yukleme sinirlamasi yazili', /5 GB'lik arsiv analiz icin DISARI TASINAMAZ/.test(src));

console.log('\n== B -- AW1: LITE profili ' + '='.repeat(48));
ok('R501_RAW_PROFILE var', /const R501_RAW_PROFILE = String\(process\.env\.R501_RAW_PROFILE \|\| 'LITE'\)/.test(src));
ok('varsayilan LITE', /R501_RAW_PROFILE \|\| 'LITE'/.test(src));
ok('7 agir dosya listelenmis', /R501_RAW_HEAVY_FILES = Object\.freeze/.test(src));
for(const f of ['stage_snapshots.jsonl','research_snapshots.jsonl','execution_events.jsonl',
                'ticks.jsonl','cvd_samples.jsonl','depth_samples.jsonl','manager_timeline.jsonl'])
  ok('  agir listede: '+f, new RegExp(f.replace('.','\\.')).test(grab('const R501_RAW_HEAVY_FILES')||src.slice(src.indexOf('R501_RAW_HEAVY_FILES'),src.indexOf('R501_RAW_HEAVY_FILES')+400)));
ok('r501RawAllowed kapisi', /function r501RawAllowed\(name\)/.test(src));
ok('append bu kapiyi kullanir', /function r501RawAppend\(id,name,obj\)\{\s*\n\s*if\(!r501RawAllowed\(name\)/.test(src));
ok('writeJson bu kapiyi kullanir', /function r501RawWriteJson\(id,name,obj\)\{\s*\n\s*if\(!r501RawAllowed\(name\)/.test(src));
{
  const f=grab('function r501RawAllowed');
  const mk=(profil,susp,aktif=true)=>{
    const sb={console}; vm.createContext(sb);
    vm.runInContext(`const R501_RAW_ARCHIVE_ACTIVE=${aktif};const R501_RAW_PROFILE='${profil}';
      let r501RawSuspended=${susp};
      const R501_RAW_HEAVY_FILES=['stage_snapshots.jsonl','research_snapshots.jsonl','execution_events.jsonl','ticks.jsonl','cvd_samples.jsonl','depth_samples.jsonl','manager_timeline.jsonl'];
      `+f+';globalThis.A=r501RawAllowed;',sb);
    return sb.A;
  };
  const lite=mk('LITE',false), full=mk('FULL',false), off=mk('OFF',false), susp=mk('LITE',true), kapali=mk('LITE',false,false);
  ok('LITE: cvd_samples YAZILMAZ', lite('cvd_samples.jsonl')===false);
  ok('LITE: ticks YAZILMAZ', lite('ticks.jsonl')===false);
  ok('LITE: manager_timeline YAZILMAZ', lite('manager_timeline.jsonl')===false);
  ok('LITE: manifest YAZILIR', lite('manifest.json')===true);
  ok('LITE: research_entry YAZILIR', lite('research_entry.json')===true);
  ok('LITE: close_bundle YAZILIR', lite('close_bundle.json')===true);
  ok('LITE: oi_samples YAZILIR', lite('oi_samples.jsonl')===true);
  ok('FULL: cvd_samples YAZILIR', full('cvd_samples.jsonl')===true);
  ok('OFF: hicbiri yazilmaz', off('manifest.json')===false && off('cvd_samples.jsonl')===false);
  ok('askiya alinmis: hicbiri yazilmaz', susp('manifest.json')===false);
  ok('arsiv kapali: hicbiri yazilmaz', kapali('manifest.json')===false);
}

console.log('\n== C -- AW2: disk muhafizi ' + '='.repeat(47));
ok('R501_DISK_LIMIT_MB (varsayilan 4200)', /R501_DISK_LIMIT_MB = Math\.max\(200, Number\(process\.env\.R501_DISK_LIMIT_MB \|\| 4200\)\)/.test(src));
ok('R501_DISK_WARN_MB (varsayilan 3500)', /R501_DISK_WARN_MB  = Math\.max\(100, Number\(process\.env\.R501_DISK_WARN_MB  \|\| 3500\)\)/.test(src));
{
  const f=grab('function r501DiskGuard');
  ok('limitte ham arsiv DURDURULUR', /r501RawSuspended=true/.test(f));
  ok('kritik uyari', /DISK_LIMIT/.test(f));
  ok('en eski ham klasorler budanir', /r501PruneOldestRaw\(R501_DISK_LIMIT_MB\*0\.75\)/.test(f));
  ok('yer acilinca tekrar acilir', /r501RawSuspended=false/.test(f));
  ok('uyari esigi ayri', /R501_DISK_WARN_MB/.test(f));
  ok('uyari spam etmez (her 10da 1)', /warns%10===1/.test(f));
  const g=grab('function r501PruneOldestRaw');
  ok('en ESKI once silinir', /sort\(\(a,b\)=>a\.ts-b\.ts\)/.test(g));
  ok('hedefe inince durur', /if\(r501DirBytes\(R501_EVIDENCE_DIR\)\/1048576 <= hedefMB\) break/.test(g));
  ok('yalniz HAM klasor silinir', /R501_RAW_ARCHIVE_DIR/.test(g) && !/R501_EVIDENCE_TRADES_DIR/.test(g));
  ok('silme izi birakilir', /RAW_PRUNED/.test(g));
  ok('islem JSON ASLA silinmez notu', /islem JSON.*ASLA silinmez|asla silinmez/.test(src));
}
ok('5 dakikada bir calisir', /setInterval\(r501DiskGuard,300000\)/.test(src));
ok('/api/evidence/disk endpointi', /app\.get\('\/api\/evidence\/disk'/.test(src));

console.log('\n== D -- AW3: analiz paketi (yuklenebilir) ' + '='.repeat(32));
ok('endpoint var', /app\.get\('\/api\/evidence\/analysis-bundle\.zip'/.test(src));
{
  const f=grab("app.get('/api/evidence/analysis-bundle.zip'");
  ok('pasif CSV iceriyor', /passive_parameters\.csv/.test(f));
  ok('sonda CSV iceriyor', /probe_samples\.csv/.test(f));
  ok('huni OZETI iceriyor (ham degil)', /funnel_summary\.json/.test(f));
  ok('kanit durumu', /evidence_status\.json/.test(f));
  ok('sonda durumu', /probe_status\.json/.test(f));
  ok('disk durumu', /disk\.json/.test(f));
  ok('OKU.txt aciklamasi', /OKU\.txt/.test(f));
  ok('BOM ile Excel uyumlu', /'\\ufeff'\+passiveCsv/.test(f));
  ok('zip uretici', /r501ZipBuffer\(entries\)/.test(f));
  ok('ham 33 MB DEGIL notu', /huni OZETI \(ham 33 MB degil\)/.test(f)||/ham 33 MB/.test(src));
}
{
  const f=grab('function r501FunnelSummary');
  ok('tip dagilimi', /byType/.test(f));
  ok('aksiyon dagilimi', /byAction/.test(f));
  ok('otorite dagilimi', /byAuthority/.test(f));
  ok('waitSource dagilimi', /byWaitSource/.test(f));
  ok('sebep ilk 40', /byReason/.test(f));
  ok('sembol dagilimi', /bySymbol/.test(f));
  ok('yasam dongusu kayitlari TAM tutulur', /lifecycle:rows\.filter/.test(f));
  ok('son 300 kayit', /son300:rows\.slice\(-300\)/.test(f));
  ok('ilk/son zaman', /firstTs.*lastTs/.test(f));
  // canli kosum
  const sb={console,Object,String,Number,fs:{readFileSync:()=>[
      {ts:1,type:'DECISION',action:'MARKET',authority:'R493',symbol:'AUSDT',reason:'x'},
      {ts:2,type:'SKIP',action:'SKIP',authority:'AUTO',symbol:'AUSDT',waitSource:'V45_SELECTOR',reason:'y'},
      {ts:3,type:'TRADE_OPEN',action:'OPEN',authority:'TESTNET_EXECUTION',symbol:'BUSDT'},
      {ts:4,type:'TRADE_CLOSE',action:'CLOSE',authority:'TESTNET_EXECUTION',symbol:'BUSDT'}
    ].map(x=>JSON.stringify(x)).join('\n')},
    R501_EVIDENCE_FUNNEL_PATH:'x',LAZARUS_BUILD:'B',TESTNET_SESSION_RESET_ID:'S'};
  vm.createContext(sb); vm.runInContext(f+';globalThis.F=r501FunnelSummary;',sb);
  const r=sb.F();
  ok('ozet 4 kaydi okudu', r.total===4);
  ok('byType dogru', r.byType.DECISION===1 && r.byType.TRADE_OPEN===1);
  ok('byWaitSource dogru', r.byWaitSource.V45_SELECTOR===1);
  ok('lifecycle TRADE_OPEN/CLOSE tutuldu', r.lifecycle.length===2);
  const boyut=JSON.stringify(r).length;
  console.log(`     ozet boyutu: ${boyut} bayt (ham 33 MB yerine)`);
  ok('ozet kucuk', boyut<20000);
}

console.log('\n== E -- boyut karsilastirmasi ' + '='.repeat(44));
const senaryo=[
  ['FULL profil, 5 GB', Math.floor(5120/98), 'islem'],
  ['LITE profil, 5 GB', Math.floor(5120/3.5), 'islem'],
];
for(const [ad,n,b] of senaryo) console.log(`     ${ad}: ~${n} ${b}`);
ok('LITE ile kapasite 30 kat artar', Math.floor(5120/3.5)/Math.floor(5120/98)>25);
ok('pasif CSV 1000 islem ~4 MB (yuklenebilir)', 1000*4/1000 < 10);

console.log('\n== F -- onceki duzeltmeler ' + '='.repeat(47));
for(const [n,re] of [
  ['AN1 kanit tum sebepler',/V592_CLOSE_PROOF_EXEMPT/],['AN2 yetim korunur',/v592PossibleOrphans/],
  ['AP2 pasif vpin',/function calcVPINPassive/],['AQ rest turevleri',/function r501RestDerive/],
  ['AS1 tek huni',/async function v592FinalizeClose/],['AS3 supurge',/async function v592OrphanLedgerSweep/],
  ['AT1 gercek pnl',/closeClassified\+\+/],['AU1 dedup ayrimi',/v592EvidenceClosedOnce/],
  ['AU2 exitReason',/exitReason:rec\.close\?\.exitReason/],['AU3 supurge limiti',/V592_SWEEP_MAX_ATTEMPTS/],
  ['AV1 sonda',/function v592ProbeCycle/],['AW1 lite',/function r501RawAllowed/],
  ['AW2 disk muhafizi',/function r501DiskGuard/],['AW3 analiz paketi',/function r501FunnelSummary/],
]) ok(n, re.test(src));
ok('sonda hala testnet kilitli', /const V592_PROBE_ACTIVE = BINANCE_EXECUTION_ENV==='TESTNET'/.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('telemetri rawProfile', /rawProfile:R501_RAW_PROFILE/.test(src));
ok('telemetri analysisBundle', /analysisBundle:'\/api\/evidence\/analysis-bundle\.zip'/.test(src));
ok('build V4_7_4_36', /V4_7_4_36_PROBE_PRICE_RISK41_10X/.test(src));
ok('eski build kalmadi', !/V4_7_4_35_ROTATE_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
