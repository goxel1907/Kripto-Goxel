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

console.log('══ N1 — 90 karakter korlugu kapandi mi ' + '═'.repeat(35));
ok('panel anahtari hala 90 (dedup korunur)', /_fullReason\.slice\(0,90\)/.test(src));
ok('funnel tam metin aliyor', /reasonFull:_fullReason\.slice\(0,600\)/.test(src));
ok('funnel waitSource aliyor', /waitSource:row\?\.waitSource\|\|null/.test(src));
{
  let rec=null;
  const ctx=vm.createContext({
    toTurkishText:(x)=>String(x).replace(/WAIT/g,'BEKLE'),
    autoScanState:{skipped:0,skipReasons:{}},
    pushAutoCandidate:()=>{}, normalizeSymbol:(x)=>x,
    r501EvidenceFunnel:(o)=>{rec=o;}, console,Date,String,Number,Object});
  vm.runInContext(grab('function markAutoSkip'),ctx);
  const uzun='MEKANİK WAIT: R486.3.9 PİYASA ANATOMİSİ/NO_EDGE+NONE: 5m Fırsat Beyni İZLE: YÜKSELİŞ kalite yetersiz'
            +' · V4.5 PRE_R495 FILTER: MS_SCORE_LOW, NOT_TOP_GAINER · MS 21.4';
  ctx.markAutoSkip('SYNUSDT',uzun,{rec:'WAIT',waitSource:'V45_SELECTOR'});
  ok('kisa anahtar 90 karakter', rec.reason.length<=90, `${rec.reason.length}`);
  ok('TAM METIN kaydedildi', rec.reasonFull.includes('V4.5 PRE_R495 FILTER'), rec.reasonFull.slice(-60));
  ok('kaynak etiketi kaydedildi', rec.waitSource==='V45_SELECTOR', String(rec.waitSource));
  ok('05.08 korlugu: kisa anahtarda V4.5 GORUNMUYOR', !rec.reason.includes('V4.5'),
     'bu yuzden yanlis teshis koymustum');
}

console.log('\n══ N2 — her WAIT kaynagi kendini isaretliyor mu ' + '═'.repeat(27));
const KAYNAK=[['R493 giris kapisi',"v592WaitSource:storyWait\\?'R493_ENTRY_GATE':null"],
              ['R447 hikaye',"v592WaitSource:r447Wait\\?'R447_STORY':null"],
              ['V4.5 selektor',"ai\\.v592WaitSource='V45_SELECTOR'"],
              ['V4.5 fail-closed',"ai\\.v592WaitSource='V45_FAIL_CLOSED'"],
              ['R495',"ai\\.v592WaitSource='R495'"]];
for(const [n,re] of KAYNAK) ok(`${n} etiketli`, cnt(re)===1, `${cnt(re)}`);
ok('skip cagrisi kaynagi tasiyor', /waitSource:_ws\}\);\}/.test(src));
ok('MECHANICAL varsayilan', /String\(ai\.v592WaitSource\|\|'MECHANICAL'\)/.test(src));
for(const c of ['waitR493Gate','waitV45Selector','waitV45FailClosed','waitR495','waitMechanical','waitR447Story'])
  ok(`sayac ${c}`, cnt(`${c}:0`)===1 && cnt(`v592ParityStats\\.${c}\\+\\+`)===1);

console.log('\n══ N3 — DAVRANIS DEGISMEDI (kritik) ' + '═'.repeat(38));
ok('V4.5 selektor hala WAIT veriyor', /ai\.plannedSide='LONG';ai\.side='WAIT';ai\.karKosma='NORMAL';\s*\n\s*ai\.v592WaitSource='V45_SELECTOR'/.test(src));
ok('R495 PUSU hala WAIT veriyor', /ai\.plannedSide='LONG'; ai\.side='WAIT'; ai\.karKosma='NORMAL';\s*\n\s*ai\.v592WaitSource='R495'/.test(src));
ok('R493 kapisi hala aktif (FO 0.35)', /R493_MIN_FIRST_OBSTACLE_RR/.test(src)&&/r493GateBlocks\+\+/.test(src));
ok('mekanik sozlesme DOKUNULMADI', /const fullMechanicalOk = mechSide==='LONG' && mechTrigger && mechPermission/.test(src));
ok('storyWait mantigi degismedi', /const storyWait = \['PUSU','REJECT'\]\.includes\(authority\.action\)/.test(src));
ok('F01 uyari yorumu duruyor', /V4\.7\.4\.1-F01: genel story-shadow KALDIRILDI/.test(src));

console.log('\n══ O — testnet sembol evreni ' + '═'.repeat(45));
ok('exchangeInfo testnet hostundan', /\$\{BINANCE_EXECUTION_FAPI\}\/fapi\/v1\/exchangeInfo/.test(src));
ok('yalniz TRADING', /status\|\|''\)\.toUpperCase\(\)!=='TRADING'/.test(src));
ok('yalniz PERPETUAL', /contractType\|\|'PERPETUAL'\)\.toUpperCase\(\)!=='PERPETUAL'/.test(src));
ok('yalniz USDT', /quoteAsset\|\|''\)\.toUpperCase\(\)!=='USDT'/.test(src));
ok('leverage ONCESI kapi', src.indexOf("SYMBOL_NOT_ON_TESTNET")<src.indexOf("const _lockLev = (V592_EXACT_BACKTEST_AUTHORITY"));
ok('acilista yukleniyor', /v592RefreshTestnetUniverse\(true\)\.catch/.test(src));
ok('periyodik yenileme', /setInterval\(\(\)=>\{v592RefreshTestnetUniverse\(false\)/.test(src));
{
  const ctx=vm.createContext({v592TestnetUniverse:{set:null,ts:0},String,console});
  vm.runInContext(grab('function v592IsTestnetTradable'),ctx);
  ok('evren YOKKEN fail-open (engellemez)', ctx.v592IsTestnetTradable('BTWUSDT').ok===true
     && ctx.v592IsTestnetTradable('BTWUSDT').known===false);
  ctx.v592TestnetUniverse.set=new Set(['BTCUSDT','ETHUSDT','SYNUSDT']);
  ok('listede olan gecer', ctx.v592IsTestnetTradable('SYNUSDT').ok===true);
  ok('BTWUSDT reddedilir (gercek -1121 vakasi)', ctx.v592IsTestnetTradable('BTWUSDT').ok===false);
  ok('kucuk harf normalize', ctx.v592IsTestnetTradable('btcusdt').ok===true);
  ok('known bayragi true', ctx.v592IsTestnetTradable('X').known===true);
}

console.log('\n══ P — kimlik ve guvenlik ' + '═'.repeat(48));
ok('build V4_7_4_37', /V4_7_4_37_PROBE_MAP_RISK41_10X/.test(src));
ok('eski build yok', !/V4_7_4_11_SPLIT_BACKOFF/.test(src));
ok('session 4_7_4_37_PM1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_37_PM1/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('L fren ayrimi duruyor', /function isExecBackoffActive/.test(src)&&/execBackoffUntil/.test(src));
ok('J1 duruyor', /protectionKeptExisting\+\+/.test(src));
ok('J2 duruyor', /function v592MinHoldGuard/.test(src));
ok('K1 duruyor', /freshCacheHits\+\+/.test(src));
ok('telemetri evren durumu', cnt('testnetUniverseKnown:')===2);

console.log(`\n${'═'.repeat(74)}`);
console.log(fail?`SONUC: FAIL — ${pass} gecti, ${fail} dustu`:`SONUC: PASS — ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
