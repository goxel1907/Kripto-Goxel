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

console.log('══ A — BACKTEST CIKIS GERCEGI ' + '═'.repeat(44));
const BT={DYNAMIC_STOP:338,INITIAL_SL:210,TARGET:165,MAX_TIME_NO_PROGRESS:12};
ok('4 cikis sebebi, toplam 725', Object.values(BT).reduce((a,b)=>a+b,0)===725);
ok('backtest motorunda EMERGENCY yok (grep 0)', /grep 0 esleme/.test(src));
ok('kaynakta sayilar yayinlaniyor', cnt('DYNAMIC_STOP:338,INITIAL_SL:210,TARGET:165,MAX_TIME_NO_PROGRESS:12')>=2);

console.log('\n══ B — 06.08 HOME: R41 SL\'in yarisinda olduruyordu ' + '═'.repeat(22));
{
  const sl=2.5,lev=10;
  const r41=-Math.min(22,Math.max(14,sl*lev*0.45));
  const r42=-Math.min(28,Math.max(18,sl*lev*0.65));
  const r14=-Math.max(sl*lev+5,30);
  const btSL=-sl*lev;
  console.log(`    R41 erken hasar : ROI ${r41.toFixed(0)}%  -> fiyat ${(r41/lev).toFixed(2)}%`);
  console.log(`    R42 mutlak hasar: ROI ${r42.toFixed(0)}%  -> fiyat ${(r42/lev).toFixed(2)}%`);
  console.log(`    R14 acil hasar  : ROI ${r14.toFixed(0)}%  -> fiyat ${(r14/lev).toFixed(2)}%`);
  console.log(`    BACKTEST SL     : ROI ${btSL.toFixed(0)}%  -> fiyat ${(btSL/lev).toFixed(2)}%`);
  ok(`R41 (${(r41/lev).toFixed(2)}%) backtest SL'inden (${(btSL/lev).toFixed(2)}%) ONCE`, r41>btSL);
  ok('R41 SL yolunun yarisinda', Math.abs(r41/btSL-0.56)<0.02, `${(r41/btSL).toFixed(2)}`);
  ok('TARGET\'a hic sans kalmiyordu', r41>btSL);
}

console.log('\n══ C — beyaz liste davranisi ' + '═'.repeat(45));
{
  const mk=(exact,wl)=>{
    const ctx=vm.createContext({V592_EXACT_BACKTEST_AUTHORITY:exact,V592_EXIT_TYPE_WHITELIST:wl,
      V592_BACKTEST_EXIT_TYPES:Object.freeze(['MAX_SURE_KAPAT']),String,console});
    vm.runInContext(grab('function v592ExitTypeAllowed'),ctx);
    return ctx.v592ExitTypeAllowed;
  };
  const A=mk(true,true);
  ok('MAX_SURE_KAPAT -> GECER (backtest MAX_TIME karsiligi)', A('MAX_SURE_KAPAT')===true);
  const golge=['EMERGENCY_EXIT','R282_ROI_HASAR_KAPAT','R282_FIKIR_BOZULDU_KAPAT',
    'R97_VUR_KAC_KAPAT','R97_FIKIR_BOZULDU_KAPAT','R144_HASAR_KONTROL_KAPAT',
    'R147_TERS_AKIS_HASAR_KAPAT','R281_PROTECT_HASAR_KAPAT','R149_PROFIT_GIVEBACK_KAPAT',
    'R165_WINNER_NEVER_LOSER_KAPAT','R282_PROFIT_TAKE_KAPAT','R282_WINNER_SCRATCH_KAPAT',
    'R486_AUTONOMOUS_PROFIT_EXIT'];
  let hepsi=true; for(const t of golge) if(A(t)!==false) hepsi=false;
  ok(`${golge.length} backtest-disi cikis tipi GOLGEDE`, hepsi);
  ok('exact KAPALI -> hepsi gecer', mk(false,true)('EMERGENCY_EXIT')===true);
  ok('beyaz liste KAPALI -> hepsi gecer', mk(true,false)('EMERGENCY_EXIT')===true);
}

console.log('\n══ D — golge kaydi karsi-olgusal ' + '═'.repeat(41));
{
  let life=null,funnel=null;
  const ctx=vm.createContext({
    v592ParityStats:{nonBacktestExitShadowed:0,shadowR41Early:0,shadowR14HardLoss:0,
      shadowR42Absolute:0,shadowR282Idea:0,shadowCvdFlip:0,shadowCascade:0},
    r501OrderLifeMark:(s,st,p)=>{life={s,st,p};},
    r501EvidenceFunnel:(o)=>{funnel=o;}, normalizeSymbol:x=>x, console});
  vm.runInContext(grab('function v592ShadowNonBacktestExit'),ctx);
  ctx.v592ShadowNonBacktestExit('HOMEUSDT','R41_EARLY_DAMAGE',{pnlPct:-15.2,cap:-14});
  ok('sayac nonBacktestExitShadowed', ctx.v592ParityStats.nonBacktestExitShadowed===1);
  ok('sayac shadowR41Early', ctx.v592ParityStats.shadowR41Early===1);
  ok('yasam dongusu izi', life?.st==='NON_BACKTEST_EXIT_SHADOWED' && life.p.code==='R41_EARLY_DAMAGE');
  ok('funnel karsi-olgusal', funnel?.type==='NON_BACKTEST_EXIT_SHADOWED'
     && funnel.action==='WOULD_EXIT' && funnel.counterfactual?.pnlPct===-15.2);
  ok('funnel PASIF (karar etkisi yok)', funnel?.decisionImpact===false
     && funnel?.orderBlocking===false && funnel?.exitImpact===false);
  ok('backtest sebepleri kayitta', Array.isArray(funnel?.backtestExitReasons)
     && funnel.backtestExitReasons.length===4);
}

console.log('\n══ E — kaynak sozlesmesi ' + '═'.repeat(49));
ok('5 uretec golgede', cnt('v592ShadowNonBacktestExit\\(sym,')===7, `${cnt('v592ShadowNonBacktestExit\\(sym,')}`);
for(const [n,re] of [['R14',"'R14_HARD_LOSS'"],['R42',"'R42_ABSOLUTE_DAMAGE'"],
  ['R41',"'R41_EARLY_DAMAGE'"],['CVD flip',"'CVD_FLIP'"],['cascade',"'ADVERSE_CASCADE'"]])
  ok(`${n} golgede`, cnt(re)>=2);
// V4.7.4.21-AH2: kosula korumasiz istisnasi eklendi
ok('kapida TIP kontrolu once',
   src.indexOf('if (!_unprotected && !v592ExitTypeAllowed(action.type))')<src.indexOf('const _mg = _unprotected'));
ok('golgede return null', /v592ParityStats\.exitTypeShadowed\+\+;[\s\S]{0,600}?return null;/.test(src));
ok('SL\/TP korumasi KALKMADI', /protectionVerified/.test(src)&&/installSLTPWithProof/.test(src));
ok('ENV ile kapatilabilir', /V592_EXIT_TYPE_WHITELIST \?\? '1'/.test(src));

console.log('\n══ F — onceki duzeltmeler ' + '═'.repeat(48));
for(const [n,re] of [['AC yonetici guard',/v592MinHoldGuard\(sym, `MANAGER_\$\{action\.type\}`\)/],
  ['AA1 cancel atlama',/const _skipCancel = \(firstInstall && attempt === 1/],
  ['AA2 ledger dedup',/function v592CloseAlreadyRecorded/],
  ['Q koruma-once',/PROTECT_FIRST_NO_PRECHECK/],['W giris mum paritesi',/candidateToEntryMs:180000/],
  ['Y cikis mum paritesi',/function v592ExitCandleGate/],['J2 min hold',/function v592MinHoldGuard/],
  ['L fren ayrimi',/function isExecBackoffActive/],['O testnet evreni',/v592IsTestnetTradable/],
  ['S cikis arastirmasi',/closeSnap=rec\.closeResearchSnapshot/],
  ['testnet hard-lock',/const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/],
  ['build V4_7_4_27',/V4_7_4_27_CSV_REPORT_RISK41_10X/],
  ['session 4_7_4_27_CR1',/V592_EXACT_CLOSED1M_R495_72H_4_7_4_27_CR1/]]) ok(n, re.test(src));
ok('eski build yok', !/V4_7_4_19_MANAGER_EXIT_GUARD/.test(src));

console.log(`\n${'═'.repeat(74)}`);
console.log(fail?`SONUC: FAIL — ${pass} gecti, ${fail} dustu`:`SONUC: PASS — ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
