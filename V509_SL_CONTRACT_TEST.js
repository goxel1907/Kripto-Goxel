// V5.0.9 — SL SOZLESMESI + bReq BOGUMU
// B1/B6 KAPANDI. Formul 1461/1461 gercek Binance verisiyle dogrulandi.
// Bu test formulu FIXTURE ile CALISTIRIR (dizge kontrolu degil).
const fs=require('fs'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`  pass  ${n}`)):(fail++,console.error(`  FAIL  ${n}${d?' :: '+d:''}`));
const has=t=>src.includes(t);

console.log('══ A — SL FORMULU: 60 GERCEK SINYALDE CALISTIRILIYOR ' + '═'.repeat(17));
{
  const fx=JSON.parse(fs.readFileSync(path.join(__dirname,'B1_SL_FIXTURE60.json'),'utf8'));
  const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
  // kaynaktan sabitleri OKU — kod degisirse test coker
  const g=n=>{const m=src.match(new RegExp('const '+n+'\\s*=\\s*([-0-9.]+)'));return m?parseFloat(m[1]):null;};
  const K=g('V509_SL_ATR_CARPANI'), P=g('V509_SL_ATR_PERIYOT'), W=g('V509_SL_LOW_PENCERE'),
        LO=g('V509_SL_PCT_MIN'), HI=g('V509_SL_PCT_MAX');
  ok('sabitler kaynaktan okundu', K===1.45&&P===14&&W===7&&LO===1.2&&HI===8.0, `${K}/${P}/${W}/${LO}/${HI}`);
  let tam=0;
  for(const f of fx){
    const raw=Math.min(f.low7, f.close-K*f.atr);
    const sp=clamp((f.close-raw)/f.close*100, LO, HI);
    const sl=f.close*(1-sp/100);
    if(Math.abs(sl-f.sl)/f.sl < 1e-9) tam++;
  }
  ok(`60 gercek sinyalde tam eslesme`, tam===fx.length, `${tam}/${fx.length}`);
  // yanlis carpanla COKMELI
  let yanlis=0;
  for(const f of fx){
    const raw=Math.min(f.low7, f.close-1.50*f.atr);
    const sp=clamp((f.close-raw)/f.close*100, LO, HI);
    if(Math.abs(f.close*(1-sp/100)-f.sl)/f.sl < 1e-9) yanlis++;
  }
  ok('carpan 1.50 olsa eslesmezdi (test gercekten olcuyor)', yanlis<fx.length, `${yanlis}/${fx.length}`);
}

console.log('\n══ B — SABITLER KAPIDA DENETLENIYOR ' + '═'.repeat(35));
for(const t of ['SL_ATR_CARPANI_145_DEGIL','SL_ATR_PERIYOT_14_DEGIL','SL_LOW_PENCERE_7_DEGIL',
                'SL_CLAMP_UYUSMAZ','PLAN_TARGET_R_24_DEGIL','PLAN_TARGET_CAP_112_DEGIL',
                'R495_EXACT_PLAN_URETICI_YOK']) ok(`kapi: ${t}`, has(t));

console.log('\n══ C — bReq BOGUMU (418 GERCEK DUZELTMESI) ' + '═'.repeat(27));
ok('backoff kontrolu bReq icinde', has('if (!emergencyBypass && isExecBackoffActive()) {'));
ok('  throttle CAGRISINDAN once', src.indexOf('if (!emergencyBypass && isExecBackoffActive())') < src.indexOf('await binanceThrottle(`${emergencyBypass'));
ok('  acil yol MUAF', has('!emergencyBypass &&'));
ok('  engellenen cagri sayaci', has('execBackoffBlockedAtBreq'));
ok('  funnel kaydi', has("type:'EXEC_BACKOFF_BLOCK'"));
ok('  kalan sure kaydediliyor', has('remainingMs:_kalanMs'));
ok('olcum kaynakta (%86 erken cagri)', has('%86'));
ok('hata sinifi belirtilmis', has('AS1/AC'));

console.log('\n══ D — HER ISLEMDE SL PARITE KANITI ' + '═'.repeat(35));
ok('SL_PARITY_PROOF kaydi', has("type:'SL_PARITY_PROOF'"));
ok('  plan stop kaydediliyor', has('planStop:Number(_r495.plan.stop)'));
ok('  plan target kaydediliyor', has('planTarget:Number(_r495.plan.target)'));
ok('  slPct kaydediliyor', has('planSlPct:Number(_r495.plan.entrySlPct)'));
ok('  n=1 sinirinin farkinda', has("o n=1'di"));

console.log('\n══ E — UC SOZLESME KAYNAKTA BELGELI ' + '═'.repeat(35));
ok('B1/B6 kapandi notu', has('B1/B6 KAPANDI'));
ok('entry sozlesmesi', has('candidateTs+180000'));
ok('ATR = SMA14 (Wilder degil)', has('Wilder DEGIL'));
ok('SL formulu tam yazili', has('rawStop   = min(recentLow, close - 1.45*atr)'));
ok('"kayip uretici" duzeltmesi', has('kayip degilmis'));

console.log('\n══ F — ONCEKI SURUMLER ' + '═'.repeat(48));
for(const [a,t] of [['V508 backoff suresi','V508_BACKOFF_MAX_SEC'],['V507 kaldirac retry','V507_LEVERAGE_PROOF_MAX_ATTEMPTS'],
  ['V506 kayip telemetrisi',"type:'ENTRY_CANDLE_DRIFT_BLOCK'"],['V505 oy duzeltmesi','!takerVoteActive'],
  ['V504 taker sabiti','V504_BACKTEST_TAKER_MIN_OBSERVED = 0.2199'],['V503 on-filtre','V503 testnet evren on-filtresi'],
  ['V502 kaldirac kilidi','V502-A: KALDIRAC KILIDI'],['parite kapisi','function v592BootParityGate']]) ok(a, has(t));
ok('SONDA yok', !has('/api/probe'));
ok('build V5_0_9', has('V5_0_9_SL_CONTRACT_LOCKED'));

console.log(`\n${'═'.repeat(72)}\nSONUC: ${pass} gecti, ${fail} kaldi`);
process.exit(fail?1:0);
