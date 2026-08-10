'use strict';
// V4.7.4.3 — R493 SON EMIR KILIDI TESTI
// Kok neden: gate 'PASS_BACKTEST_OBSERVABLE' donuyor, kilit 'PASS' bekliyordu.
const fs=require('fs'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`pass  ${n}`)):(fail++,console.error(`FAIL  ${n}${d?' :: '+d:''}`));

// Kilit ifadesini kaynaktan cikarip GERCEKTEN calistir
const m=src.match(/const _r493CodeOk = (\[[^\]]+\]\.includes\(_r493FinalCode\));/);
ok('D1a kod listesi kaynakta var', !!m, 'bulunamadi');
if(m){
  const test=(code)=>eval(m[1].replace('_r493FinalCode',JSON.stringify(code)));
  ok('D1b PASS_BACKTEST_OBSERVABLE kabul', test('PASS_BACKTEST_OBSERVABLE')===true);
  ok('D1c PASS kabul (V5.9.2 uyumu)', test('PASS')===true);
  ok('D1d LOW_FIRST_OBSTACLE_RR RED', test('LOW_FIRST_OBSTACLE_RR')===false);
  ok('D1e FIRST_OBSTACLE_RR_UNKNOWN RED', test('FIRST_OBSTACLE_RR_UNKNOWN')===false);
  ok('D1f DISABLED_OR_NON_LONG RED', test('DISABLED_OR_NON_LONG')===false);
  ok('D1g bos kod RED', test('')===false);
}
// Gate'in gercekten hangi kodu donduugu
const codes=[...src.matchAll(/code:'(PASS[A-Z_]*)'/g)].map(x=>x[1]);
ok('D1h gate PASS_BACKTEST_OBSERVABLE donuyor', codes.includes('PASS_BACKTEST_OBSERVABLE'), codes.join(','));
ok('D1i eski katı ="PASS" kontrolu kalmadi', !/String\(_r493FinalSafety\.code \|\| ''\) === 'PASS'/.test(src));
// blocked ve authority kosullari korunmus mu
ok('D1j blocked===false sarti duruyor', /_r493FinalSafety\.blocked === false/.test(src));
ok('D1k MARKET\\/TACTICAL sarti duruyor', /\['MARKET','TACTICAL'\]\.includes\(_r493FinalAction\)/.test(src));
// kanit + sayac
ok('D2a blok kaniti funnel\'a yaziliyor', /type:'R493_FINAL_LOCK_BLOCK'/.test(src));
ok('D2b safetyCode kanitta', /safetyCode:_r493FinalCode/.test(src));
ok('D3a r493GateBlocks sayaci artiyor', /v592ParityStats\.r493GateBlocks\+\+/.test(src));
// kimlik
ok('D4a build V4.7.4.37', /V4_7_4_37_PROBE_MAP_RISK41_10X/.test(src));
ok('D4b session 4_7_4_10', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_37_PM1/.test(src));
ok('D4c eski kimlik yok', !/V4_7_4_9_EXIT_CONTRACT_RISK41_10X/.test(src));
// sozlesme degismedi
ok('D5a slot 41', /R497_SLOT_MARGIN_USDT \|\| 41/.test(src));
ok('D5b max 2', /R486_MAX_POSITIONS \|\| 2/.test(src));
ok('D5c risk 4', /R495_FINAL_RISK_PCT \|\| 4/.test(src));
ok('D5d R493 FO 0.35 kapisi aktif', /const storyWait = \['PUSU','REJECT'\]\.includes\(authority\.action\);/.test(src));
ok('D5e testnet hard-lock', /BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('D5f leverage lock 10', /V592_LEVERAGE_LOCK=Math\.max\(0,Math\.min\(125,Number\(process\.env\.V592_LEVERAGE_LOCK\|\|10\)\)\)/.test(src));
console.log(`\n${fail?'SONUC: FAIL':'SONUC: PASS'} — ${pass} gecti, ${fail} dustu`);
process.exitCode=fail?1:0;
