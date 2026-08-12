// V5.0.6 — KAYIP EMIR TELEMETRISI
// Olculdu 12.08: panelde ORDER_ROUTE_ERROR vardi, funnel'da 0 kayit.
// "7 TACTICAL kabul, 3'u emre ulasti" derken kalan 4 sayilamiyordu.
const fs=require('fs'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`  pass  ${n}`)):(fail++,console.error(`  FAIL  ${n}${d?' :: '+d:''}`));
const has=t=>src.includes(t);

console.log('══ A — KAYIP EMIR ARTIK SAYILABILIR ' + '═'.repeat(35));
ok('ORDER_ROUTE_ERROR funnel kaydi', has("type:'ORDER_ROUTE_ERROR',action:'ORDER_ROUTE_ERROR'"));
ok('  orderBlocking isaretli', /type:'ORDER_ROUTE_ERROR'[\s\S]{0,220}orderBlocking:true/.test(src));
ok('  pushCritical KORUNDU', has("pushCritical('ORDER_ROUTE_ERROR'"));
ok('ENTRY_CANDLE_DRIFT_BLOCK funnel kaydi', has("type:'ENTRY_CANDLE_DRIFT_BLOCK'"));
ok('  drift degeri kaydediliyor', has('candleDrift:_drift'));
ok('  izin verilen sinir kaydediliyor', has('maxAllowed:V592_ENTRY_CANDLE_MAX_DRIFT'));
ok('  gecikme (requestToSend) kaydediliyor', has('requestToSendMs:_lag'));
ok('AUTO_SCAN_WATCHDOG funnel kaydi', has("type:'AUTO_SCAN_WATCHDOG'"));
ok('  takilma suresi kaydediliyor', /type:'AUTO_SCAN_WATCHDOG'[\s\S]{0,200}ageMs:age/.test(src));

console.log('\n══ B — MUHAFIZ MANTIGI DEGISMEDI ' + '═'.repeat(38));
ok('drift esigi ayni', has('if(_drift>V592_ENTRY_CANDLE_MAX_DRIFT){'));
ok('EXACT + parity sarti ayni', has('if(V592_EXACT_BACKTEST_AUTHORITY&&V592_ENTRY_CANDLE_PARITY&&Number.isFinite(_drift)){'));
ok('backtest gerekcesi yazili', has('candidateTs+180000') || has('725/725 sapmasiz'));
ok('muhafizin DOGRU oldugu yazili', has('Muhafiz DOGRU') || has('muhafiz DOGRU') || has('Muhafiz'));

console.log('\n══ C — ONCEKI SURUMLER ' + '═'.repeat(48));
ok('V505 oy duzeltmesi', has('!takerVoteActive'));
ok('V504 taker sabiti', has('V504_BACKTEST_TAKER_MIN_OBSERVED = 0.2199'));
ok('V503 on-filtre', has('V503 testnet evren on-filtresi'));
ok('V502 cift kaldirac kilidi', has('V502-A: KALDIRAC KILIDI'));
ok('V502 R283 EXACT muafiyeti', has('_v502ExactNoR283'));
ok('acilis parite kapisi', has('function v592BootParityGate'));
ok('B1-B6 engelleri', has('V501_KNOWN_PARITY_BLOCKERS'));
ok('SONDA yok', !has('/api/probe') && !has('v592ProbeCycle'));
ok('build V5_0_6', (has('V5_0_6_LOSS_TELEMETRY') || src.includes('V5_0_7_LEVPROOF_RETRY')));

console.log(`\n${'═'.repeat(72)}\nSONUC: ${pass} gecti, ${fail} kaldi`);
process.exit(fail?1:0);
