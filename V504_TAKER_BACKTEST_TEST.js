// V5.0.4 — R495 LIMITLERI BACKTESTTEN TURETILMIS OLMALI
// Olculdu: backtestin 1.461 kabul edilmis sinyalinde takerRatio min 0,2199.
// Eski esik 0,50 bunun %25,1'ini keserdi; eski clamp tabani 0,40 duzeltmeyi
// ENV'den IMKANSIZ kiliyordu (V5.0.1'deki Math.max(0.70,...) tuzaginin aynisi).
const fs=require('fs'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`  pass  ${n}`)):(fail++,console.error(`  FAIL  ${n}${d?' :: '+d:''}`));
const has=t=>src.includes(t);

console.log('══ A — TAKER ESIGI ' + '═'.repeat(52));
ok('clamp tabani 0.20', has('Math.max(.20, Math.min(.70, Number(process.env.R495_TAKER_RATIO_MIN || .20))'));
ok('eski 0.40 tabani kalmadi', !has('Math.max(.40, Math.min(.70, Number(process.env.R495_TAKER_RATIO_MIN'));
ok('varsayilan artik 0.50 degil', !/R495_TAKER_RATIO_MIN \|\| \.50/.test(src));
ok('V505: oyda taker sarti bayrakli', src.includes('!takerVoteActive'));
ok('olculen backtest ucu sabitlendi', has('V504_BACKTEST_TAKER_MIN_OBSERVED = 0.2199'));

console.log('\n══ B — DORT LIMITIN HEPSI BACKTESTTEN ' + '═'.repeat(33));
// olculen backtest uclari (1.461 kabul edilmis sinyal)
const OLCUM={taker:0.2199, driftMax:0.848, driftMin:-0.149, adverseMax:0.847};
ok('taker  : limit <= backtest min', has('Number(R495_TAKER_RATIO_MIN) > V504_BACKTEST_TAKER_MIN_OBSERVED'));
ok('drift+ : limit >= backtest max', has('Number(R495_MAX_ENTRY_DRIFT_ATR) < 0.848'));
ok('drift- : limit <= backtest min', has('Number(R495_MIN_ENTRY_DRIFT_ATR) > -0.149'));
ok('adverse: limit >= backtest max', has('Number(R495_MAX_ADVERSE_ATR) < 0.847'));
ok('dordu de ACILIS KAPISINDA', ['TAKER_ESIGI_BACKTEST_USTU','DRIFT_MAX_BACKTEST_ALTI',
   'DRIFT_MIN_BACKTEST_USTU','ADVERSE_MAX_BACKTEST_ALTI'].every(x=>has(x)));

console.log('\n══ C — OLCUM KAYNAKTA BELGELI ' + '═'.repeat(41));
ok('1461 sinyal olcumu', has('1.461 kabul edilmis sinyalinde'));
ok('min/medyan/max yazili', has('0,2199') && has('0,5405') && has('0,9898'));
ok('eski esigin maliyeti (366/1461)', has('366/1461'));
ok('diger uc limitin turetildigi yazili', has('backtest max 0,848') && has('backtest max 0,847'));
ok('OLCUM SINIRI durustce yazili', has('gosterge niteligindedir, birebir degil'));
ok('clamp tuzagi kok sebebi yazili', has('V5.0.1'));

console.log('\n══ D — ONCEKI SURUM KAZANIMLARI ' + '═'.repeat(39));
ok('V503 testnet on-filtre', has('V503 testnet evren on-filtresi'));
ok('V502 cift uclu kaldirac kilidi', has('V502-A: KALDIRAC KILIDI'));
ok('V502 R283 EXACT muafiyeti', has('_v502ExactNoR283'));
ok('acilis parite kapisi', has('function v592BootParityGate'));
ok('B1-B6 engelleri', has('V501_KNOWN_PARITY_BLOCKERS'));
ok('0.35 obstacle clamp (V501)', has('Math.max(0.35, Math.min(2.0'));
ok('HIGH=1.00 (V501)', has("r491EnvNumber('R493_HIGH_FACTOR', 1.00"));
ok('SONDA yok', !has('/api/probe') && !has('v592ProbeCycle'));

console.log('\n══ E — SURUM ' + '═'.repeat(58));
ok('build V5_0_4', ((has('V5_0_4_TAKER_FROM_BACKTEST') || src.includes('V5_0_5_VOTE_EXACT_BACKTEST')) || src.includes('V5_0_6_LOSS_TELEMETRY')));

console.log(`\n${'═'.repeat(72)}\nSONUC: ${pass} gecti, ${fail} kaldi`);
process.exit(fail?1:0);
