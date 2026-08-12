// V5.0.7 — KALDIRAC ISPATI RETRY + 418 TELEMETRISI
// OLCULDU 12.08: BEAT 12:14:29'da acildi, 12:16:21'de LEVERAGE_PARITY_UNWIND
// ile geri kapatildi. Ispat TEK atisti; 418 yuzunden proof=null oldu.
// 418 bizden degil: 97 dakikada 15 imzali cagri = 0,16/dk (limit 2400 weight/dk).
const fs=require('fs'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`  pass  ${n}`)):(fail++,console.error(`  FAIL  ${n}${d?' :: '+d:''}`));
const has=t=>src.includes(t);

console.log('══ A — ISPAT ARTIK N DENEMELI ' + '═'.repeat(41));
ok('deneme sayisi sabiti', has('V507_LEVERAGE_PROOF_MAX_ATTEMPTS'));
ok('  varsayilan 5', /V507_LEVERAGE_PROOF_MAX_ATTEMPTS \|\| 5/.test(src));
ok('  ENV ile degistirilebilir', has('process.env.V507_LEVERAGE_PROOF_MAX_ATTEMPTS'));
ok('bekleme sabiti', has('V507_LEVERAGE_PROOF_RETRY_MS'));
ok('retry dongusu var', has('for(_lpAttempt=1;_lpAttempt<=V507_LEVERAGE_PROOF_MAX_ATTEMPTS'));
ok('kanit gelince DONGU KIRILIR', has('if(Number.isFinite(_appliedLeverageProof)) break;'));
ok('son denemede beklemez', has('if(_lpAttempt<V507_LEVERAGE_PROOF_MAX_ATTEMPTS){'));
ok('tek atisli eski kod KALMADI', !/try\{\s*const _pr=await fetchPositionRiskRaw\(apiKey,apiSecret\);\s*const _p=Array[\s\S]{0,220}\}catch\(_e\)\{_appliedLeverageProof=null;\}/.test(src));

console.log('\n══ B — MUHAFIZ GEVSEMEDI ' + '═'.repeat(46));
ok('parite sarti AYNEN', has('if(V592_EXACT_BACKTEST_AUTHORITY&&V592_LEVERAGE_LOCK>0&&!_leverageParityOk){'));
ok('esitlik kontrolu AYNEN', has('_leverageParityOk = Number.isFinite(_appliedLeverageProof) && _appliedLeverageProof===_expectedLeverage;'));
ok('kanitsizsa UNWIND duruyor', has('LEVERAGE_PARITY_UNWIND'));
ok('sembol kilidi duruyor', has('v592LockSymbol(sym,`LEVERAGE_PARITY_FAILED_'));
ok('reduce-only kapatma duruyor', has("reduceOnly:'true'"));

console.log('\n══ C — SESSIZ DEGIL ' + '═'.repeat(51));
ok('deneme sayaci', has('leverageProofAttempts'));
ok('retry sayaci', has('leverageProofRetried'));
ok('retry funnel kaydi', has("type:'LEVERAGE_PROOF_RETRY'"));
ok('  deneme sayisi kaydediliyor', has('attempts:_lpAttempt'));
ok('  son hata kaydediliyor', has('lastError:_lpLastErr'));
ok('eski red sayaci duruyor', has('leverageProofRejects'));

console.log('\n══ D — 418 TELEMETRISI ' + '═'.repeat(48));
ok('rate limit funnel kaydi', has("type:'BINANCE_RATE_LIMIT'"));
ok('  EXEC/PUBLIC ayrimi', has("domain==='EXEC'?'TESTNET_EXECUTION':'PUBLIC_MARKET'"));
ok('  HTTP durumu kaydediliyor', has('httpStatus:status'));
ok('  retryAfter kaydediliyor', has('retryAfter:retryHeader'));
ok('  EXEC ise orderBlocking', has("orderBlocking:domain==='EXEC'"));

console.log('\n══ E — OLCUM KAYNAKTA BELGELI ' + '═'.repeat(41));
ok('BEAT olayi tarih/saatle', has('BEAT islemi 12:14:29') && has('12:16:21'));
ok('418 disallik olcumu', has('0,16/dk') && has('2400 weight/dk'));
ok('Railway IP notu', has('208.77.246.50'));
ok('muhafizin gevsemedigi yazili', has('MUHAFIZ GEVSEMIYOR'));

console.log('\n══ F — ONCEKI SURUMLER ' + '═'.repeat(48));
ok('V506 kayip telemetrisi', has("type:'ENTRY_CANDLE_DRIFT_BLOCK'"));
ok('V505 oy duzeltmesi', has('!takerVoteActive'));
ok('V504 taker sabiti', has('V504_BACKTEST_TAKER_MIN_OBSERVED = 0.2199'));
ok('V503 on-filtre', has('V503 testnet evren on-filtresi'));
ok('V502 kaldirac cift kilidi', has('V502-A: KALDIRAC KILIDI'));
ok('parite kapisi', has('function v592BootParityGate'));
ok('SONDA yok', !has('/api/probe'));
ok('build V5_0_7', has('V5_0_7_LEVPROOF_RETRY'));

console.log(`\n${'═'.repeat(72)}\nSONUC: ${pass} gecti, ${fail} kaldi`);
process.exit(fail?1:0);
