// LAZARUS V5.0.1 — 725 replay parity audit regression
const fs=require('fs'), path=require('path');
const S=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
const B=fs.readFileSync(path.join(__dirname,'00_BUILD_TRANSFORMASYONU.py'),'utf8');
let p=0,f=0;
function ok(n,c){if(c){p++;console.log('  pass  '+n)}else{f++;console.log('  FAIL  '+n)}}
function has(x){return S.includes(x)}
console.log('\n══ V501 — 725 PORTFOY SOZLESMESI / KALDIRAC ══');
ok('EXACT leverage sizing oncesi 10x lock', has('V501 KRITIK PARITE') && S.indexOf('V501 KRITIK PARITE') < S.indexOf('R495 FINAL RISK AUTHORITY'));
ok('executeLeverage V592_LEVERAGE_LOCK kullanir', has('executeLeverage = Number(V592_LEVERAGE_LOCK||10)'));
ok('trailing state exchange-proven leverage first', has('leverage:Number(orderResp?.details?.leverage)||parseInt(executeLeverage)'));
ok('R458 state exchange-proven leverage first', has('_stOpen.leverage = Number(orderResp?.details?.leverage)'));
ok('AI snapshot exchange-proven leverage first', has('leverage: Number(orderResp?.details?.leverage) || Number(executeLeverage)'));
ok('known-position leverage exchange-proven', has('leverage:Number(orderResp?.details?.leverage)||parseInt(executeLeverage)||parseInt(leverage)||1}, _stOpen'));

console.log('\n══ V501 — SAYISAL BACKTEST KILITLARI ══');
ok('R486 FO clamp 0.35', has('Math.max(0.35, Math.min(2.0, Number(process.env.R486_FIRST_OBSTACLE_MIN_RR || 0.35)))'));
ok('R493 hard FO default 0.35', has("r491EnvNumber('R493_MIN_FIRST_OBSTACLE_RR', 0.35"));
ok('HIGH quality factor default 1.00', has("r491EnvNumber('R493_HIGH_FACTOR', 1.00"));
for(const x of ['V45_SCORE_35_DEGIL','V45_FO_035_DEGIL','V45_TOP_GAINER_ZORUNLU_DEGIL','R493_FO_035_DEGIL','R486_FO_035_DEGIL','SLOT_41_DEGIL','BUFFER_20_DEGIL','MAX_POS_2_DEGIL','RISK_4_DEGIL','DD_SOZLESME_UYUSMAZ','QUALITY_FACTOR_UYUSMAZ']) ok('parity gate: '+x,has(x));
ok('HOLD_FIXED gate',has('ABOVE_CAP_HOLD_FIXED_DEGIL'));
ok('DD throttle active gate',has('DD_THROTTLE_KAPALI'));

console.log('\n══ V501 — FAIL CLOSED / BILINEN ENGELLER ══');
for(const x of ['CANDIDATE_FEATURE_GENERATOR_NOT_INCLUDED','SAME_TIMESTAMP_SLOT_PRIORITY_NOT_PROVEN','COOLDOWN_AND_DAILY_STOP_RESTART_PERSISTENCE_NOT_PARITY','TRAILING_STATE_RESTART_NOT_PARITY','CLEAN_EXIT_ENGINE_PARITY_RUN_MISSING']) ok('live hard blocker: '+x,has(x));
ok('bilinen engeller yalniz LIVE dalinda hard-block',has("if(BINANCE_EXECUTION_ENV==='LIVE') for(const x of V501_KNOWN_PARITY_BLOCKERS)"));
ok('emir yolu parity gate fail-closed',has('PARITE_KAPISI_KAPALI'));

console.log('\n══ V501 — REFERANS / REPRODUCIBILITY ══');
ok('stale 547 historicalReference kaldirildi',!has("historicalReference:{status:'POLICY_REPLAY_REFERENCE_NOT_EXCHANGE_PARITY',trades:547"));
ok('725 replay referansi yazili',has("trades:725,pf:2.419668,winRatePct:66.6207,maxDrawdownPct:14.8052,netUSDT:3299.3164"));
ok('build source relative',B.includes("SRC = BASE / 'SOURCE_V47443_PATCHED' / 'server.js'"));
ok('build output relative',B.includes("OUT = BASE / 'server.rebuilt.js'"));
ok('absolute eski session path yok',!B.includes('/sessions/cool-hopeful-ptolemy/'));
ok('surum V501 audit candidate',(has('V5_0_1_PARITY_AUDIT_CANDIDATE')||has('V5_0_2_LEVLOCK_BOTH_ENDS')));

console.log(`\nSONUC: ${p} gecti, ${f} kaldi`); process.exit(f?1:0);
