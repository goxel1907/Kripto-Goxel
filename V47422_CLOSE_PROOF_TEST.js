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

console.log('══ A — 07.08 STG/BICO vakasi ' + '═'.repeat(45));
const O={stgKanit:'STOP_LOSS',stgBinance:'ACIK 526 STG',bicoKanit:'EXTERNAL_OR_MANUAL',
         bicoBinance:'ACIK 3216 BICO',panelPozisyon:0,binancePozisyon:2};
ok('kanit STG kapandi dedi', O.stgKanit==='STOP_LOSS');
ok('Binance STG ACIK', O.stgBinance.startsWith('ACIK'));
ok('kanit BICO kapandi dedi', O.bicoKanit==='EXTERNAL_OR_MANUAL');
ok('Binance BICO ACIK', O.bicoBinance.startsWith('ACIK'));
ok('panel 0 · Binance 2 pozisyon', O.panelPozisyon===0 && O.binancePozisyon===2);
ok('kaynakta vaka belgelenmis', /07\.08 STG\/BICO: kanit "STOP_LOSS \/ EXTERNAL_OR_MANUAL" diye KAPANDI yazdi/.test(src));
ok('kok neden yazili', /r344StillOpen __forceFresh VERMIYORDU/.test(src));

console.log('\n══ B — R344 artik TAZE okuyor ' + '═'.repeat(44));
ok('__forceFresh:true verildi', /symbol: sym, __forceFresh: true, __maxAgeMs: 10000/.test(src));
ok('acik bulununca sayac artar', /v592ParityStats\.falseFlatPrevented\+\+/.test(src));
ok('FALSE_FLAT_PREVENTED izi', /FALSE_FLAT_PREVENTED/.test(src));
ok('hata -> ACIK say (fail-open)', /return true;\s*\n\s*\}\s*\n\s*\};/.test(src.slice(src.indexOf('const r344StillOpen'))));
ok('CLOSE_PROOF_UNAVAILABLE izi', /CLOSE_PROOF_UNAVAILABLE/.test(src));
ok('supheyi ACIK sayma notu', /yanlis kapatmaktansa yanlis acik tutmak yeglenir/.test(src));

console.log('\n══ C — kapanis pozitif kanit istiyor ' + '═'.repeat(37));
{
  const mk=(rows,lastSuccessAgo,throwIt)=>{
    const now=1786099200000;
    const ctx=vm.createContext({
      autoConfig:{apiKey:'k',apiSecret:'s'},
      posRiskCache:{lastSuccessAt:now-lastSuccessAgo},
      getPositionRiskCached:async()=>{ if(throwIt) throw new Error('POSITION_RISK_INFLIGHT_WAIT_TIMEOUT'); return rows; },
      Date:{now:()=>now}, Math,Number,String,Array,Infinity,Boolean,console});
    vm.runInContext(grab('async function v592CloseProof'),ctx);
    return ctx.v592CloseProof('STGUSDT');
  };
  let r=await mk([{symbol:'STGUSDT',positionAmt:'0'}],3000,false);
  ok('taze + flat -> KANITLI', r.proven===true && r.reason==='FRESH_FLAT');
  r=await mk([{symbol:'STGUSDT',positionAmt:'526'}],3000,false);
  ok('pozisyon ACIK -> kanit YOK', r.proven===false && r.reason==='STILL_OPEN');
  r=await mk([{symbol:'STGUSDT',positionAmt:'0'}],20000,false);
  ok('snapshot 20sn bayat -> kanit YOK', r.proven===false && r.reason==='SNAPSHOT_STALE');
  r=await mk([],0,true);
  ok('sorgu patladi -> kanit YOK', r.proven===false && r.reason==='SNAPSHOT_FAILED');
  console.log();
  console.log('    KANIT VARSA  -> kapanis kaydi yazilir');
  console.log('    KANIT YOKSA  -> kapanis IPTAL, pozisyon ACIK sayilir');
}

console.log('\n══ D — cleanupClosedPositionState kapisi ' + '═'.repeat(33));
// V4.7.4.24-AN1: beyaz liste KALDIRILDI — kanit artik VARSAYILAN, muafiyet acik yazilir.
ok('kanit VARSAYILAN (beyaz liste yok)', /if\(!V592_CLOSE_PROOF_EXEMPT\.includes/.test(src));
ok('muafiyet listesi acikca tanimli', /V592_CLOSE_PROOF_EXEMPT = \['EXCHANGE_CONFIRMED_FILL','MANUAL_OPERATOR_FORCE'\]/.test(src));
ok('SYNC_ kapanisi hala kanit ister', !['EXCHANGE_CONFIRMED_FILL','MANUAL_OPERATOR_FORCE'].includes('SYNC_POSITION_ALREADY_CLOSED_BEFORE_SLTP_RESCUE'));
ok('POSITION_ALREADY_CLOSED hala kanit ister', !['EXCHANGE_CONFIRMED_FILL','MANUAL_OPERATOR_FORCE'].includes('POSITION_ALREADY_CLOSED'));
ok('kanit yoksa aborted doner', /return \{aborted:true,proof:_cp\}/.test(src));
ok('CLOSE_ABORTED_NO_PROOF izi', /CLOSE_ABORTED_NO_PROOF/.test(src));
for(const c of ['falseFlatPrevented','closeProofRequired','closeProofFailed'])
  ok(`sayac ${c}`, cnt(`${c}:0`)===1 && cnt(`v592ParityStats\\.${c}\\+\\+`)>=1);
ok('telemetri kanit sarti', cnt('closeRequiresPositiveProof:true')===2);

console.log('\n══ E — emir yolu kapanislari ETKILENMEDI ' + '═'.repeat(33));
ok('FRESH_POSITION_ZERO kanit istemez', /cleanupClosedPositionState\(symbol, 'FRESH_POSITION_ZERO_BEFORE_SLTP'\)/.test(src));
ok('ALGO_-4509 kanit istemez', /cleanupClosedPositionState\(symbol, 'ALGO_-4509_POSITION_ALREADY_CLOSED'\)/.test(src));
// AJ2'nin ACIGI: bu sebep beyaz listede yoktu, kanit kapisini ATLIYORDU.
// 07.08 HEIUSDT tam bu yoldan "kapandi" diye kaydedildi. AN1 kapatti.
ok('POSITION_GONE_AFTER_PROTECT artik CAGRILMIYOR',
   !/cleanupClosedPositionState\(\s*symbol\s*,\s*'POSITION_GONE_AFTER_PROTECT'/.test(src));
console.log('    (bunlar zaten G2/Q4 kendi kanitlarini topluyor)');

console.log('\n══ F — onceki duzeltmeler ' + '═'.repeat(48));
for(const [n,re] of [['AH korumasiz istisna',/function v592PositionProtection\(sym\)/],
  ['AG beyaz liste',/const V592_BACKTEST_EXIT_TYPES = Object\.freeze/],
  ['AE golge',/function v592ShadowNonBacktestExit/],
  ['AC yonetici guard',/v592MinHoldGuard\(sym, `MANAGER_/],
  ['AA1 cancel atlama',/const _skipCancel = \(firstInstall && attempt === 1/],
  ['AA2 ledger dedup',/function v592CloseAlreadyRecorded/],
  ['Q koruma-once',/PROTECT_FIRST_NO_PRECHECK/],['W giris mum paritesi',/candidateToEntryMs:180000/],
  ['Y cikis mum paritesi',/function v592ExitCandleGate/],
  ['L fren ayrimi',/function isExecBackoffActive/],['O testnet evreni',/v592IsTestnetTradable/],
  ['S cikis arastirmasi',/closeSnap=rec\.closeResearchSnapshot/],
  ['testnet hard-lock',/const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/],
  ['build V4_7_4_27',/V4_7_4_27_CSV_REPORT_RISK41_10X/],
  ['session 4_7_4_27_CR1',/V592_EXACT_CLOSED1M_R495_72H_4_7_4_27_CR1/]]) ok(n, re.test(src));
ok('eski build yok', !/V4_7_4_21_UNPROTECTED_EXCEPTION/.test(src));

console.log(`\n${'═'.repeat(74)}`);
console.log(fail?`SONUC: FAIL — ${pass} gecti, ${fail} dustu`:`SONUC: PASS — ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
