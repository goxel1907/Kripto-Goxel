'use strict';
// V4.7.4.9 — BACKTEST CIKIS SOZLESMESI TESTI
const fs=require('fs'),vm=require('vm'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`pass  ${n}`)):(fail++,console.error(`FAIL  ${n}${d?' :: '+d:''}`));
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
console.log('── J1  Calisan koruma IPTAL EDILMEZ ' + '─'.repeat(32));
{
  ok('J1a on-kontrol var', /const _pre=await verifyAlgoSLTPVisible\(apiKey,apiSecret,symbol,slPrice,tpPrice\);/.test(src));
  ok('J1b gecerliyse erken donus', /if\(_pre&&_pre\.ok\)\{[\s\S]{0,400}keptExisting:true/.test(src));
  ok('J1c kanit kaydi', /'PROTECTION_KEPT_EXISTING'/.test(src));
  ok('J1d sayac', /protectionKeptExisting\+\+/.test(src));
  ok('J1e ENV ile kapatilabilir', /V592_PROTECT_KEEP_EXISTING=String\(process\.env/.test(src));
  const iPre=src.indexOf('const _pre=await verifyAlgoSLTPVisible');
  const iCancel=src.indexOf('await cancelAlgoOrders(apiKey, apiSecret, symbol)');
  ok('J1f on-kontrol cancelAlgoOrders\'dan ONCE', iPre>0&&iCancel>0&&iPre<iCancel, `pre=${iPre} cancel=${iCancel}`);
}
console.log('\n── J2  Minimum tutus 60sn (backtest tabani) ' + '─'.repeat(24));
{
  const mk=(exact,openedAgoMs,minHold)=>{
    const ctx=vm.createContext({V592_EXACT_BACKTEST_AUTHORITY:exact,V592_MIN_HOLD_MS:minHold,
      // V4.7.4.17: minHoldGuard artik once mum kapisina bakiyor.
      v592ExitCandleGate:()=>({blocked:false,reason:'TEST_OPEN'}),
      trailingState:new Map([['SYNUSDT',{openedAt:Date.now()-openedAgoMs}]]),
      r501OrderLifeSnapshot:()=>({}), r501OrderLifeMark:()=>{},
      v592ParityStats:{minHoldBlocks:0}, Date,Number,String,Math,console});
    vm.runInContext(grab('function v592MinHoldGuard'),ctx);
    return ctx.v592MinHoldGuard('SYNUSDT','TEST');
  };
  ok('J2a 18 sn -> BLOKE', mk(true,18000,60000).blocked===true);
  ok('J2b 43 sn -> BLOKE', mk(true,43000,60000).blocked===true);
  ok('J2c 59.9 sn -> BLOKE', mk(true,59900,60000).blocked===true);
  ok('J2d 60 sn -> serbest', mk(true,60000,60000).blocked===false);
  ok('J2e 233 sn -> serbest', mk(true,233000,60000).blocked===false);
  ok('J2f exact kapali -> hic bloke etmez', mk(false,5000,60000).blocked===false);
  ok('J2g minHold=0 -> bloke etmez', mk(true,5000,0).blocked===false);
  ok('J2h acil kapatma guard\'a bagli', /_g=v592MinHoldGuard\(pos\?\.symbol\|\|pos\?\.sym,'EMERGENCY_BRACKET'\)/.test(src));
  ok('J2i bloke edilince deferred doner', /return \{closed:false,deferred:true,minHoldGuard:true/.test(src));
  ok('J2j varsayilan 60000', /V592_MIN_HOLD_MS\|\|60000/.test(src));
  ok('J2k CIKIS artik mum kapanisinda (V4.7.4.17-Y)', /function v592ExitCandleGate/.test(src)
     && /\{const _c=v592ExitCandleGate\(symbol,reason\);/.test(src));
  ok('J2l backtest olcumu 725/725 exitTs%60000', /exitTsMod60000:59999/.test(src));
}
console.log('\n── J3  Bayat karar ile emir gonderilmez ' + '─'.repeat(28));
{
  // V4.7.4.16-W: asil kural artik 1m MUM siniri. ms olcumu kanit olarak duruyor.
  ok('J3a gecikme olculuyor', /const _lag=_reqAt\?_now-_reqAt:0;/.test(src));
  ok('J3a2 ASIL kural mum tabanli', /ORDER_ABORTED_STALE.*rule:'ENTRY_CANDLE_PARITY'/s.test(src)
     && /Giris mumu gecti/.test(src));
  ok('J3a3 backtest 180000ms sozlesmesi', /candidateToEntryMs:180000/.test(src));
  ok('J3b limit asilinca throw', /Emir hazirligi \$\{Math\.round\(_lag\/1000\)\}sn surdu/.test(src));
  ok('J3c ORDER_ABORTED_STALE kaydi', /'ORDER_ABORTED_STALE'/.test(src));
  ok('J3d sayac', /staleOrderAborts\+\+/.test(src));
  ok('J3e varsayilan 15sn', /V592_MAX_REQUEST_TO_SEND_MS\|\|15000/.test(src));
  const iChk=src.indexOf("ORDER_ABERTED_STALE")>0?0:src.indexOf("_lag>V592_MAX_REQUEST_TO_SEND_MS");
  const iSend=src.indexOf("const orderSendTime=Date.now();r501OrderLifeMark(sym,'MAIN_ORDER_SEND'");
  ok('J3f kontrol SEND\'den once', iChk>0&&iSend>0&&iChk<iSend);
}
console.log('\n── J4  Backtest disi cikis sayiliyor ' + '─'.repeat(31));
{
  ok('J4a NON_BACKTEST_EXIT kaydi', /'NON_BACKTEST_EXIT'/.test(src));
  ok('J4b 5 backtest nedeni listeli', /backtestExitReasons:\['INITIAL_SL','TARGET','DYNAMIC_STOP','MAX_TIME_NO_PROGRESS','MAX_24H'\]/.test(src));
  ok('J4c sayac', /nonBacktestExits\+\+/.test(src));
  ok('J4d status\'ta sozlesme', /allowedExits:\['INITIAL_SL','TARGET','DYNAMIC_STOP','MAX_TIME_NO_PROGRESS','MAX_24H'\]/.test(src));
}
console.log('\n── J5  Onceki duzeltmeler + sozlesme ' + '─'.repeat(31));
{
  const h=re=>re.test(src);
  ok('J5a build V4.7.4.38', h(/V4_7_4_38_PROBE_RETRY_RISK41_10X/));
  ok('J5b session 4_7_4_38_PR2', h(/V592_EXACT_CLOSED1M_R495_72H_4_7_4_38_PR2/));
  ok('J5c G1 forceFresh', h(/__forceFresh:true,/));
  ok('J5d G2 post-fill proof', h(/'POST_FILL_POSITION_PROOF'/));
  ok('J5e G3 dedup', h(/'EVIDENCE_DUPLICATE_SUPPRESSED'/));
  ok('J5f PIT hizalama', h(/R497_PIT_MAX_RANK=Math\.max/)&&h(/strictEligible/));
  ok('J5g R493 final lock', h(/\['PASS','PASS_BACKTEST_OBSERVABLE'\]\.includes\(_r493FinalCode\)/));
  ok('J5h idempotency', h(/newClientOrderId:cid/));
  ok('J5i slot 41 / max2 / risk4', h(/R497_SLOT_MARGIN_USDT \|\| 41/)&&h(/R486_MAX_POSITIONS \|\| 2/)&&h(/R495_FINAL_RISK_PCT \|\| 4/));
  ok('J5j V4.5 esikleri', h(/V592_V45_MS_SCORE_MIN\|\|35/)&&h(/V592_V45_FIRST_OBSTACLE_RR_MIN\|\|0\.35/));
  ok('J5k testnet hard-lock', h(/BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/));
}
console.log(`\n${fail?'SONUC: FAIL':'SONUC: PASS'} — ${pass} gecti, ${fail} dustu`);
process.exitCode=fail?1:0;
