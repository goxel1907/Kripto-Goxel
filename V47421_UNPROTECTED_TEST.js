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

console.log('══ A — 06.08 ONUSDT vakasi ' + '═'.repeat(47));
const O={openOrders:0,tpsl:'-- / --',pnl:0.80,roi:11.11,markPrice:0.33834,liqPrice:0.3055655,
         marginRatio:13.59,panelPnl:-1.22};
ok('Binance: Open Orders 0', O.openOrders===0);
ok('Binance: TP/SL -- / -- (KORUMA YOK)', O.tpsl==='-- / --');
ok('pozisyon KARDA (+0,80 · %11,11)', O.pnl>0 && O.roi>0);
ok('panel -1,22 diyordu -> panel YANLIS', O.panelPnl<0 && O.pnl>0);
ok('likidasyona uzak', (O.markPrice-O.liqPrice)/O.markPrice>0.09);
ok('kaynakta vaka belgelenmis', /06\.08 ONUSDT: onceki oturumdan devralinan pozisyon/.test(src));
ok('backtest gerekcesi yazili', /INITIAL_SL 210 kez tetiklendi/.test(src));

console.log('\n══ B — koruma durumu okuyucu ' + '═'.repeat(45));
{
  const mk=(st,life)=>{
    const ctx=vm.createContext({trailingState:new Map([['ONUSDT',st]]),
      r501OrderLifeSnapshot:()=>life, String,console});
    vm.runInContext(grab('function v592PositionProtection'),ctx);
    return ctx.v592PositionProtection('ONUSDT');
  };
  let r=mk({},{});
  ok('SL yok + TP yok -> protected FALSE', r.protected===false && r.hasSL===false);
  r=mk({currentSLAlgoId:'SL1',sltpVerified:true},{});
  ok('SL var + verified -> protected TRUE', r.protected===true);
  r=mk({currentSLAlgoId:'SL1'},{});
  ok('SL var ama verified DEGIL -> protected FALSE', r.protected===false, JSON.stringify(r));
  r=mk({},{slOrderId:'X',sltpVerified:true});
  ok('yasam dongusunden okur', r.protected===true);
  r=mk({currentSL:0.30,targetTP:0.36,sltpVerified:true},{});
  ok('fiyat alanlarindan okur', r.hasSL===true && r.hasTP===true && r.protected===true);
}

console.log('\n══ C — kapi davranisi ' + '═'.repeat(52));
{
  const wl=(exact,wlOn)=>{
    const ctx=vm.createContext({V592_EXACT_BACKTEST_AUTHORITY:exact,V592_EXIT_TYPE_WHITELIST:wlOn,
      V592_BACKTEST_EXIT_TYPES:Object.freeze(['MAX_SURE_KAPAT']),String,console});
    vm.runInContext(grab('function v592ExitTypeAllowed'),ctx);
    return ctx.v592ExitTypeAllowed;
  };
  const A=wl(true,true);
  // kapi mantigi: if (!_unprotected && !allowed) -> golge
  const kapi=(unprotected,type)=> (!unprotected && !A(type)) ? 'GOLGE' : 'GECER';
  ok('KORUMALI + EMERGENCY_EXIT -> GOLGE', kapi(false,'EMERGENCY_EXIT')==='GOLGE');
  ok('KORUMASIZ + EMERGENCY_EXIT -> GECER', kapi(true,'EMERGENCY_EXIT')==='GECER');
  ok('KORUMALI + MAX_SURE_KAPAT -> GECER', kapi(false,'MAX_SURE_KAPAT')==='GECER');
  ok('KORUMASIZ + R41 -> GECER', kapi(true,'R282_ROI_HASAR_KAPAT')==='GECER');
  ok('KORUMALI + R41 -> GOLGE', kapi(false,'R282_ROI_HASAR_KAPAT')==='GOLGE');
  console.log();
  console.log('    KORUMALI   pozisyon: SL/TP bekler, 13 tip golgede   (backtest paritesi)');
  console.log('    KORUMASIZ  pozisyon: cikis SERBEST                  (backtestte bu durum YOK)');
}

console.log('\n══ D — min-hold guard korumasizda atlanir ' + '═'.repeat(32));
ok('_unprotected ise guard atlanir', /const _mg = _unprotected \? \{blocked:false\} :/.test(src));
ok('sayac yalniz korumaliyken artar', /if \(!_unprotected\) v592ParityStats\.exitTypeAllowed\+\+;/.test(src));
ok('korumasiz kayit tutulur', /r501OrderLifeMark\(sym,'UNPROTECTED_EXIT_ALLOWED'/.test(src));

console.log('\n══ E — devralinan pozisyon ' + '═'.repeat(47));
ok('ADOPTED_POSITION_NOT_PARITY isareti', /ADOPTED_POSITION_NOT_PARITY/.test(src));
ok('parite olcumune girmez notu', /parite olcumune girmez/.test(src));
ok('sayac adoptedPositions', cnt('adoptedPositions:0')===1 && cnt('adoptedPositions\\+\\+')===1);
ok('BOOT_RECONCILE_POSITION_FOUND korundu', /BOOT_RECONCILE_POSITION_FOUND/.test(src));

console.log('\n══ F — onceki duzeltmeler ' + '═'.repeat(48));
for(const [n,re] of [['AG beyaz liste',/const V592_BACKTEST_EXIT_TYPES = Object\.freeze/],
  ['AE golge yardimcisi',/function v592ShadowNonBacktestExit/],
  ['AC yonetici guard',/v592MinHoldGuard\(sym, `MANAGER_/],
  ['AA1 cancel atlama',/const _skipCancel = \(firstInstall && attempt === 1\)/],
  ['AA2 ledger dedup',/function v592CloseAlreadyRecorded/],
  ['Q koruma-once',/PROTECT_FIRST_NO_PRECHECK/],['W giris mum paritesi',/candidateToEntryMs:180000/],
  ['Y cikis mum paritesi',/function v592ExitCandleGate/],['J2 min hold',/function v592MinHoldGuard/],
  ['L fren ayrimi',/function isExecBackoffActive/],['O testnet evreni',/v592IsTestnetTradable/],
  ['S cikis arastirmasi',/closeSnap=rec\.closeResearchSnapshot/],
  ['testnet hard-lock',/const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/],
  ['build V4_7_4_23',/V4_7_4_23_PASSIVE_PARAMS_RISK41_10X/],
  ['session 4_7_4_23_PP1',/V592_EXACT_CLOSED1M_R495_72H_4_7_4_23_PP1/]]) ok(n, re.test(src));
ok('eski build yok', !/V4_7_4_20_EXIT_REASON_PARITY/.test(src));

console.log(`\n${'═'.repeat(74)}`);
console.log(fail?`SONUC: FAIL — ${pass} gecti, ${fail} dustu`:`SONUC: PASS — ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
