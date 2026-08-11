// V5.0.2 — KALDIRAC/SL TUTARLILIK TESTI
// V5.0.1 kaldirac kilidini zincirin SONUNA koydu. Olculdu: SL turetme zinciri
// kilitten ONCE calisip SL'i kilitlenmemis kaldiraca gore yaziyordu.
const fs=require('fs'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`  pass  ${n}`)):(fail++,console.error(`  FAIL  ${n}${d?' :: '+d:''}`));
const has=t=>src.includes(t);
const lineOf=t=>src.slice(0,src.indexOf(t)).split('\n').length;

console.log('══ A — KALDIRAC KILIDI ZINCIRIN HER IKI UCUNDA ' + '═'.repeat(24));
ok('zincir BASINDA kilit var', has('V502-A: KALDIRAC KILIDI ZINCIRIN BASINDA'));
ok('zincir SONUNDA kilit duruyor', has('V501 EXACT'));
const bas=lineOf('V502-A: KALDIRAC KILIDI'), son=lineOf('V501 EXACT');
ok('bas kilidi sondan ONCE', bas<son, `bas ${bas} son ${son}`);

const atamalar=[...src.matchAll(/\bexecuteLeverage\s*=(?!=)/g)].map(m=>src.slice(0,m.index).split('\n').length);
ok('son kilit, SON atama', Math.max(...atamalar)<=son+2, `max ${Math.max(...atamalar)} son ${son}`);

console.log('\n══ B — SL TURETIMI KILITLER ARASINDA ' + '═'.repeat(34));
const slAta=[...src.matchAll(/\buserSLPct\s*=(?!=)/g)].map(m=>src.slice(0,m.index).split('\n').length);
const ilkBildirim=Math.min(...slAta);
const araligiDisi=slAta.filter(x=>x!==ilkBildirim && !(bas<x && x<son));
ok('tum SL turetimi iki kilit ARASINDA', araligiDisi.length===0, `disarda: ${araligiDisi}`);
ok('kilitten SONRA SL yazilmiyor', slAta.every(x=>x<son));

console.log('\n══ C — R283 EXACT MODDA DEVRE DISI ' + '═'.repeat(36));
ok('R283 muafiyet bayragi tanimli', has('const _v502ExactNoR283 = V592_EXACT_BACKTEST_AUTHORITY'));
ok('kaldirac cap EXACT modda atlanir', has('if (!_v502ExactNoR283) executeLeverage = Math.min(executeLeverage, r282CapLev);'));
ok('SL yeniden yazimi EXACT modda atlanir', has('if (!_v502ExactNoR283 && userSLPct * executeLeverage >'));
ok('bunun TUTARLILIK duzeltmesi oldugu yazili', has('SL\'in backtestle ayni olacagini GARANTI ETMEZ'));

console.log('\n══ D — ENGEL #6 KAPIDA ' + '═'.repeat(48));
ok('SL/TP sozlesme engeli listede', has('SL_TP_YUZDE_SOZLESMESI_DOGRULANMADI_22_NOKTA_R166_3PCT_R175_095PCT'));
ok('olculen sayilar kaynakta', has('%1,13-9,94') && has('medyan %4,26'));
ok('R166 tavani belgelenmis', has('%67,7'));
ok('R175 tavani belgelenmis', has("%100'u USTUNDE"));
ok('engel #1 ile iliskisi yazili', has("#1'in alt kumesi"));
ok('CANLI modda engeller kapiyi kapatir', has("if(BINANCE_EXECUTION_ENV==='LIVE') for(const x of V501_KNOWN_PARITY_BLOCKERS)"));
ok('cift-uc kilidi kapida denetlenir', has("hata.push('KALDIRAC_KILIDI_TEK_UCTA')"));

console.log('\n══ E — V5.0.1 SAYISAL SOZLESMESI KORUNDU ' + '═'.repeat(30));
for(const t of ['V45_SCORE_35_DEGIL','V45_FO_035_DEGIL','SLOT_41_DEGIL','BUFFER_20_DEGIL',
                'MAX_POS_2_DEGIL','RISK_4_DEGIL','DD_SOZLESME_UYUSMAZ','QUALITY_FACTOR_UYUSMAZ',
                'R493_FO_035_DEGIL','R486_FO_035_DEGIL'])
  ok(`kapi maddesi: ${t}`, has(t));
ok('0.35 clamp duzeltmesi duruyor', has('Math.max(0.35, Math.min(2.0, Number(process.env.R486_FIRST_OBSTACLE_MIN_RR || 0.35))'));
ok('HIGH=1.00 duruyor', has("r491EnvNumber('R493_HIGH_FACTOR', 1.00"));
ok('R493 FO varsayilani 0.35', has("r491EnvNumber('R493_MIN_FIRST_OBSTACLE_RR', 0.35"));

console.log('\n══ F — SURUM ' + '═'.repeat(58));
ok('build V5_0_2', (has('V5_0_2_LEVLOCK_BOTH_ENDS') || src.includes('V5_0_3_TESTNET_UNIVERSE_PREFILTER')));
ok('SONDA yok', !src.includes('/api/probe') && !src.includes('v592ProbeCycle'));

console.log(`\n${'═'.repeat(72)}\nSONUC: ${pass} gecti, ${fail} kaldi`);
process.exit(fail?1:0);
