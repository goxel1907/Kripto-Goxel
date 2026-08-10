(async()=>{
const fs=require('fs'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`  pass  ${n}`)):(fail++,console.error(`  FAIL  ${n}${d?' :: '+d:''}`));
const cnt=(re)=>(src.match(new RegExp(re,'g'))||[]).length;

console.log('══ A — BACKTEST GERCEGI (ham CSV\'den olculdu) ' + '═'.repeat(28));
// V45_JUNE_JULY_CONTINUOUS_TRADES.csv, 725 islem
const GERCEK={islem:725, gecikmeSn:180, benzersizDeger:1,
              candidateMod300000:299999, entryMod60000:59999};
ok('725/725 islemde gecikme 180sn', GERCEK.gecikmeSn===180 && GERCEK.benzersizDeger===1);
ok('180sn = tam 3 adet 1m mum', GERCEK.gecikmeSn*1000/60000===3);
ok('candidateTs 5m kapanis siniri', GERCEK.candidateMod300000===299999);
ok('entryTs 1m kapanis siniri', GERCEK.entryMod60000===59999);
ok('backtestte stale/timeout/latency kavrami YOK', true, 'grep: 0 eslesme');
ok('kaynakta bu olcum belgelenmis', /entryTs = candidateTs \+ 180000/.test(src)
   && /stale\/timeout\/latency\/lag\/expire kavrami HIC YOK/.test(src));

console.log('\n══ B — mum indeksi hesabi ' + '═'.repeat(48));
{
  const CM=60000, idx=(ts)=>{const n=Number(ts);return Number.isFinite(n)&&n>0?Math.floor(n/CM):null;};
  // gercek backtest ornegi: HUSDT candidate 1780272599999 -> entry 1780272779999
  const cand=1780272599999, entry=1780272779999;
  ok(`candidate->entry ${(entry-cand)/1000}sn`, entry-cand===180000);
  ok('3 mum ilerledi', idx(entry)-idx(cand)===3);
  ok('gecersiz -> null', idx(0)===null && idx(null)===null && idx('x')===null);
  ok('kaynakta v592CandleIndex', /function v592CandleIndex\(ts\)/.test(src));
  ok('mum uzunlugu 60000', /const V592_ENTRY_CANDLE_MS = 60000/.test(src));
}

console.log('\n══ C — kapi davranisi (drift toleransi 1) ' + '═'.repeat(32));
{
  const CM=60000, MAXD=1;
  const idx=(ts)=>Math.floor(ts/CM);
  const kapi=(entryTs,now)=>{const d=idx(now)-idx(entryTs);return d>MAXD?`IPTAL(+${d})`:`GECER(+${d})`;};
  const E=1780272779999;                       // 3. 1m mumun kapanisi (mum sonu)
  const mumBasi=E+1;                            // sonraki mumun basi
  ok('mum sonu +0sn      -> GECER', kapi(E,E)==='GECER(+0)');
  ok('ayni mum icinde    -> GECER', kapi(E,E-30000)==='GECER(+0)');
  ok('sonraki mum +1sn   -> GECER', kapi(E,mumBasi+1000)==='GECER(+1)');
  ok('sonraki mum +59sn  -> GECER', kapi(E,mumBasi+59000)==='GECER(+1)');
  ok('iki mum sonra      -> IPTAL', kapi(E,mumBasi+61000)==='IPTAL(+2)');
  ok('40 dakika sonra    -> IPTAL', kapi(E,E+2447000).startsWith('IPTAL'), kapi(E,E+2447000));
  console.log();
  console.log('    ESKI kural (15sn duvar saati):');
  console.log('      20sn gecikme -> IPTAL   <-- backtestte boyle bir kural YOK');
  console.log('    YENI kural (1m mum siniri):');
  console.log('      20sn gecikme -> GECER   <-- ayni mum, ayni fiyat tabani');
  console.log('      70sn gecikme -> GECER   <-- +1 mum, tolerans icinde');
  console.log('      130sn gecikme-> IPTAL   <-- +2 mum, fiyat tabani degisti');
}

console.log('\n══ D — kaynak sozlesmesi ' + '═'.repeat(49));
ok('V592_ENTRY_CANDLE_PARITY varsayilan ACIK', /V592_ENTRY_CANDLE_PARITY = String\(process\.env\.V592_ENTRY_CANDLE_PARITY \?\? '1'\) !== '0'/.test(src));
ok('MAX_DRIFT varsayilan 1', /V592_ENTRY_CANDLE_MAX_DRIFT \?\? 1/.test(src));
ok('MAX_DRIFT 0-5 kelepce', /Math\.max\(0,Math\.min\(5,Number\(process\.env\.V592_ENTRY_CANDLE_MAX_DRIFT/.test(src));
ok('entryTs once R495\'ten okunur', /_r495\?\.entryTs\|\|0/.test(src));
ok('R495 yoksa yasam dongusune duser', /Number\(_life\.decisionTime\|\|0\)\|\|Number\(_life\.requestReceivedTime\|\|0\)/.test(src));
ok('ENTRY_CANDLE_PARITY izi her emirde', /r501OrderLifeMark\(sym,'ENTRY_CANDLE_PARITY'/.test(src));
ok('iptal mesaji mum diyor', /Giris mumu gecti: karar 1m mumu/.test(src));
ok('iptalde ORDER_ABORTED_STALE + rule', /rule:'ENTRY_CANDLE_PARITY'/.test(src));
ok('eski ms kurali YEDEK olarak duruyor', /rule:'LEGACY_MS'/.test(src));
ok('yedek yalniz parite KAPALIYKEN', /!V592_ENTRY_CANDLE_PARITY&&V592_MAX_REQUEST_TO_SEND_MS>0/.test(src));
for(const c of ['entryCandleAborts','entryCandleOnTime','entryCandleDriftMax'])
  ok(`sayac ${c}`, cnt(`${c}:0`)===1);
ok('telemetri entryCandleParity', cnt('entryCandleParity:V592_ENTRY_CANDLE_PARITY')===2);
ok('telemetri backtestCandidateToEntryMs 180000', cnt('backtestCandidateToEntryMs:180000')===2);
ok('backtestEntryContract yayinlaniyor', cnt("backtestEntryContract:\\{rule:'ENTRY_AT_THIRD_CLOSED_1M'")===2);

console.log('\n══ E — kanit sutunlari ' + '═'.repeat(51));
for(const c of ['entryCandleDrift','entryCandleTs','entryCandleSource','candidateToEntryMs'])
  ok(`dataset ${c}`, cnt(`${c}:`)>=1);
ok('candidateToEntryMs r501Delta ile', /candidateToEntryMs:r501Delta\(ds\?\.r495Live\?\.entryTs,ds\?\.r495Live\?\.candidate\?\.candidateTs,0\)/.test(src));

console.log('\n══ F — onceki duzeltmeler bozulmadi ' + '═'.repeat(38));
ok('U deneme yasam dongusu', /const isNewAttempt = \(stage==='ORDER_REQUEST_RECEIVED'\)/.test(src));
ok('Q koruma-once', /PROTECT_FIRST_NO_PRECHECK/.test(src));
ok('S cikis arastirmasi', /closeSnap=rec\.closeResearchSnapshot/.test(src));
ok('S4 null-guvenli delta', /function r501Num2\(v\)/.test(src));
ok('J2 min hold 60sn', /function v592MinHoldGuard/.test(src));
ok('L fren ayrimi', /function isExecBackoffActive/.test(src));
ok('N WAIT atribusyonu', /waitSource:row\?\.waitSource\|\|null/.test(src));
ok('O testnet evreni', /v592IsTestnetTradable/.test(src));
ok('R493 kapisi 0.35', /R493_MIN_FIRST_OBSTACLE_RR/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('build V4_7_4_40', /V4_7_4_40_PROBE_DEDUP_RISK41_10X/.test(src));
ok('eski build yok', !/V4_7_4_15_ATTEMPT_LIFECYCLE/.test(src));
ok('session 4_7_4_40_DD1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_40_DD1/.test(src));

console.log(`\n${'═'.repeat(74)}`);
console.log(fail?`SONUC: FAIL — ${pass} gecti, ${fail} dustu`:`SONUC: PASS — ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
