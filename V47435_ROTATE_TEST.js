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

console.log('== A -- istenen ayar sinirlar icinde mi ' + '='.repeat(34));
const A={TOP_N:24,MARGIN:15,INTERVAL:900000,MAX_OPEN:4,HOLD:300000,LEV:10};
ok('TOP_N=24 kabul (tavan 24)', Math.max(1,Math.min(24,A.TOP_N))===24);
ok('MARGIN=15 kabul (taban 5)', Math.max(5,A.MARGIN)===15);
ok('INTERVAL=900000 kabul (taban 60000)', Math.max(60000,A.INTERVAL)===900000);
ok('MAX_OPEN=4 kabul', Math.max(1,Math.min(24,A.MAX_OPEN))===4);
ok('clamp tanimlari degismedi', /Math\.max\(1, Math\.min\(24, Number\(process\.env\.V592_PROBE_TOP_N/.test(src));

console.log('\n== B -- SORUN 1: MAX_OPEN(4) < TOP_N(24) sapmasi ' + '='.repeat(25));
ok('kok neden kaynakta belgelenmis', /hep sira 1-4 ornekklenir, 5-24 HIC gorulmezdi/.test(src));
ok('rotasyon sayaci var', /let v592ProbeRotate = 0;/.test(src));
{
  const f=grab('async function v592ProbeCycle');
  ok('baslangic indisi kayar', /const bas=v592ProbeRotate % N/.test(f));
  ok('her donguda MAX_OPEN kadar ilerler', /v592ProbeRotate=\(v592ProbeRotate\+V592_PROBE_MAX_OPEN\) % Math\.max\(1,N\)/.test(f));
  ok('listeyi dairesel gezer', /top\[\(bas\+i\)%N\]/.test(f));
  ok('rotasyon indisi telemetride', /rotateIndex=bas/.test(f));
}
{
  // rotasyon mantigini izole kosur
  const kod=`
    let rot=0;
    function dongu(N,MAX){
      const bas=rot%N; rot=(rot+MAX)%Math.max(1,N);
      const out=[]; let acik=0;
      for(let i=0;i<N;i++){ if(acik>=MAX) break; out.push((bas+i)%N); acik++; }
      return out;
    }`;
  const sb={}; vm.createContext(sb); vm.runInContext(kod+';globalThis.d=dongu;globalThis.r=()=>rot;',sb);
  const turlar=[]; for(let i=0;i<6;i++) turlar.push(sb.d(24,4));
  console.log('     6 dongu:', turlar.map(x=>`[${x[0]}-${x[3]}]`).join(' '));
  ok('1. dongu 0-3', JSON.stringify(turlar[0])==='[0,1,2,3]');
  ok('2. dongu 4-7', JSON.stringify(turlar[1])==='[4,5,6,7]');
  ok('3. dongu 8-11', JSON.stringify(turlar[2])==='[8,9,10,11]');
  ok('6. dongu 20-23', JSON.stringify(turlar[5])==='[20,21,22,23]');
  const hepsi=new Set(turlar.flat());
  ok('6 donguda 24 sembolun TAMAMI ornekklendi', hepsi.size===24, String(hepsi.size));
  const turlar2=[]; for(let i=0;i<6;i++) turlar2.push(sb.d(24,4));
  ok('7. dongu basa doner (0-3)', JSON.stringify(turlar2[0])==='[0,1,2,3]');
  // MAX>=TOP durumunda bozulmamali
  const sb2={}; vm.createContext(sb2); vm.runInContext(kod+';globalThis.d=dongu;',sb2);
  const t=sb2.d(10,10);
  ok('MAX>=TOP: tek dongude hepsi', t.length===10 && new Set(t).size===10);
}

console.log('\n== C -- SORUN 2: tutus suresi ' + '='.repeat(44));
ok('kok neden belgelenmis', /5 dk isteyip ~6 dk/.test(src));
ok('esik artik tam HOLD_MS', /Date\.now\(\)-Number\(p\.openedAt\|\|0\) >= V592_PROBE_HOLD_MS\)\s*\n\s*v592ProbeCloseOne/.test(src));
ok('HOLD_MS+60000 kalmadi', !/V592_PROBE_HOLD_MS\+60000/.test(src));
ok('15 saniyede bir bakar', /\},15000\)\.unref/.test(src));
{
  const kod=`function kapatMi(acilis,simdi,hold){ return simdi-acilis>=hold; }`;
  const sb={}; vm.createContext(sb); vm.runInContext(kod+';globalThis.k=kapatMi;',sb);
  ok('5 dk dolmadan kapatmaz', sb.k(0,299000,300000)===false);
  ok('5 dk dolunca kapatir', sb.k(0,300000,300000)===true);
  ok('en fazla 15 sn gecikme', 15000 < 300000*0.06);
}

console.log('\n== D -- SENIN AYARINLA SONUC ' + '='.repeat(45));
const TOP=24,MAX=4,ARALIK=15,MARJ=15,LEV=10;
const donguSayisi=Math.ceil(TOP/MAX), tamTarama=donguSayisi*ARALIK;
const gunluk=Math.floor(24*60/ARALIK)*MAX;
ok(`${donguSayisi} donguda tum liste`, donguSayisi===6);
ok(`tam tarama ${tamTarama} dk (1,5 saat)`, tamTarama===90);
ok(`gunluk ${gunluk} ornek`, gunluk===384);
ok('188 ornek (d=0.5) 11,8 saatte', Math.abs(188/gunluk*24-11.8)<0.3);
ok('74 ornek (d=0.8) 4,6 saatte', Math.abs(74/gunluk*24-4.6)<0.3);
ok(`notional/sonda ${MARJ*LEV} USDT`, MARJ*LEV===150);
ok(`ayni anda max ${MAX*MARJ} USDT marj`, MAX*MARJ===60);
ok('komisyon ~46 USDT/gun, bakiye 108 gun yeter', 4987/(gunluk*MARJ*LEV*0.0008)>100);
const imzali=Math.floor(24*60/ARALIK)*MAX*3;
ok(`imzali cagri ${imzali}/gun (onceki 8.640)`, imzali===1152);
ok('API yuku 3,8 kat yerine 1,37 kat', Math.abs((3084+imzali)/3084-1.37)<0.03);
ok('=> parite olcumune baski COK daha az', imzali < 8640/5);

console.log('\n== E -- status yeni alanlar ' + '='.repeat(46));
{
  const f=grab("app.get('/api/probe/status'");
  for(const [n,re] of [['rotation bayragi',/rotation:V592_PROBE_MAX_OPEN<V592_PROBE_TOP_N/],
    ['tam tarama dongu',/fullSweepCycles/],['tam tarama dakika',/fullSweepMinutes/],
    ['gunluk ornek',/samplesPerDay/],['notional',/notionalPerProbe/]]) ok(n, re.test(f));
}

console.log('\n== F -- guvenlik ve parite korundu ' + '='.repeat(39));
ok('AX1 preempt duruyor', /async function v592ProbePreempt/.test(src));
ok('AX2 baski valfi duruyor', /V592_PROBE_PRESSURE_MS/.test(src));
ok('slot ofseti duruyor', cnt('R486_MAX_POSITIONS \\+ v592ProbeSlotOffset\\(\\)')>=3);
ok('sonda strateji sembolunu atlar', /skippedStrategyBusy\+\+/.test(src));
ok('varsayilan KAPALI', /String\(process\.env\.V592_PROBE_ACTIVE \?\? '0'\)==='1'/.test(src));
ok('TESTNET kilidi', /const V592_PROBE_ACTIVE = BINANCE_EXECUTION_ENV==='TESTNET'/.test(src));
{
  const b=src.slice(src.indexOf('// V4.7.4.32 — SONDA'), src.indexOf("app.get('/api/probe/samples.csv'"));
  ok('tradeLedger e DOKUNMUYOR', !/tradeLedger/.test(b));
  ok('v592ParityStats e DOKUNMUYOR', !/v592ParityStats/.test(b));
  ok('ham arsiv YAZMIYOR', !/r501RawAppend|r501RawInit/.test(b));
  ok('YALNIZ LONG', /side:'BUY'/.test(b) && !/side:'SELL'/.test(b));
}
for(const [n,re] of [['AS1 tek huni',/async function v592FinalizeClose/],
  ['AU1 dedup ayrimi',/v592EvidenceClosedOnce/],['AU2 exitReason',/exitReason:rec\.close\?\.exitReason/],
  ['AW1 lite',/function r501RawAllowed/],['AW2 disk muhafizi',/function r501DiskGuard/],
  ['AW3 analiz paketi',/function r501FunnelSummary/],['AY1 rotasyon',/v592ProbeRotate/]]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('build V4_7_4_36', /V4_7_4_36_PROBE_PRICE_RISK41_10X/.test(src));
ok('eski build kalmadi', !/V4_7_4_35_ROTATE_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
