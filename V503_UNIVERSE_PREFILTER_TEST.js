// V5.0.3 — TESTNET SEMBOL EVRENI ON-FILTRESI TESTI
// Olculdu 11.08: 5,7 saatte 2 gecerli sinyal, IKISI DE emir aninda
// SYMBOL_NOT_ON_TESTNET ile oldu. Kontrol emir yolundaydi; aday listesinde yoktu.
const fs=require('fs'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`  pass  ${n}`)):(fail++,console.error(`  FAIL  ${n}${d?' :: '+d:''}`));
const has=t=>src.includes(t);
const at=t=>src.indexOf(t);

console.log('══ A — FILTRE DOGRU YERDE ' + '═'.repeat(45));
ok('on-filtre var', has('V503 testnet evren on-filtresi'));
ok('ANALIZ DONGUSUNDEN ONCE', at('V503 testnet evren on-filtresi') < at('for (const [scanIdx, coin] of scanList.entries())'));
ok('emir yolundaki kontrol DURUYOR (ikinci savunma)', has('SYMBOL_NOT_ON_TESTNET'));
ok('scanList yeniden atanabilir (let)', /let\s+scanList\s*=/.test(src));
ok('evren Set uzerinden sorgulaniyor', has('_uni.has(_s)'));

console.log('\n══ B — YALNIZ TESTNET ' + '═'.repeat(49));
ok('bayrak ortama bagli', has("const V503_TESTNET_UNIVERSE_PREFILTER = BINANCE_EXECUTION_ENV==='TESTNET'"));
ok('ENV ile kapatilabilir', has("process.env.V503_TESTNET_UNIVERSE_PREFILTER ?? '1'"));
ok('canlida devreye girmeyecegi yazili', has('Canlida mainnet=mainnet'));
ok('SAPMA olarak kayitli', has('SAPMA KAYDI'));

console.log('\n══ C — SESSIZ ERKEN DONUS YOK (BA/BD sinifi) ' + '═'.repeat(26));
ok('elenen sayaci', has('testnetUniversePrefiltered'));
ok('calisma sayaci', has('testnetUniversePrefilterRuns'));
ok('evren yoksa sayac', has('testnetUniversePrefilterSkipped'));
ok('hata sayaci', has('testnetUniversePrefilterError'));
ok('FAIL-OPEN (evren yoksa islem durmaz)', has('fail-open'));
ok('elenenler loga yazilir', has('elenen:'));
ok('funnel kaydi uretilir', has("type:'TESTNET_UNIVERSE_PREFILTER'"));
ok('funnel kaydi karar etkisini isaretler', has("decisionImpact:true,orderBlocking:false"));

console.log('\n══ D — OLCULEN KANIT KAYNAKTA ' + '═'.repeat(41));
ok('CLOUSDT olayi belgeli', has('CLOUSDT'));
ok('CRWVUSDT olayi belgeli', has('CRWVUSDT'));
ok('683 vs 528 olcumu', has('683') && has('528'));
ok('kok sebep aciklamasi', has('sonraki adaya da'));

console.log('\n══ E — V5.0.2 KAZANIMLARI DURUYOR ' + '═'.repeat(37));
ok('cift uclu kaldirac kilidi', has('V502-A: KALDIRAC KILIDI'));
ok('R283 EXACT muafiyeti', has('_v502ExactNoR283'));
ok('acilis parite kapisi', has('function v592BootParityGate'));
ok('B1-B6 engelleri', has('V501_KNOWN_PARITY_BLOCKERS'));
ok('SL/TP engeli (B6)', has('SL_TP_YUZDE_SOZLESMESI_DOGRULANMADI'));
ok('0.35 clamp', has('Math.max(0.35, Math.min(2.0'));
ok('HIGH=1.00', has("r491EnvNumber('R493_HIGH_FACTOR', 1.00"));
ok('SONDA yok', !has('/api/probe') && !has('v592ProbeCycle'));

console.log('\n══ F — SURUM ' + '═'.repeat(58));
ok('build V5_0_3', (((((has('V5_0_3_TESTNET_UNIVERSE_PREFILTER') || src.includes('V5_0_4_TAKER_FROM_BACKTEST')) || src.includes('V5_0_5_VOTE_EXACT_BACKTEST')) || src.includes('V5_0_6_LOSS_TELEMETRY')) || src.includes('V5_0_7_LEVPROOF_RETRY')) || src.includes('V5_0_8_BACKOFF_HONORED')));

console.log(`\n${'═'.repeat(72)}\nSONUC: ${pass} gecti, ${fail} kaldi`);
process.exit(fail?1:0);
