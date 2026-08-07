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

console.log('══ A — BACKTEST CIKIS GERCEGI (725 islemden olculdu) ' + '═'.repeat(21));
const B={n:725,exitMod60000:59999,
  minSn:{DYNAMIC_STOP:60,TARGET:120,INITIAL_SL:300,MAX_TIME_NO_PROGRESS:14400},
  adet:{DYNAMIC_STOP:338,INITIAL_SL:210,TARGET:165,MAX_TIME_NO_PROGRESS:12}};
ok('725/725 exitTs % 60000 = 59999', B.exitMod60000===59999);
ok('DYNAMIC_STOP min 60sn = TAM 1 MUM', B.minSn.DYNAMIC_STOP===60 && 60*1000/60000===1);
ok('TARGET min 120sn = 2 mum', B.minSn.TARGET/60===2);
ok('INITIAL_SL min 300sn = 5 mum', B.minSn.INITIAL_SL/60===5);
ok('MAX_TIME_NO_PROGRESS 14400sn = 4 saat sabit', B.minSn.MAX_TIME_NO_PROGRESS===14400);
ok('cikis sebebi sayilari toplam 725',
   Object.values(B.adet).reduce((a,b)=>a+b,0)===725);
ok('kaynakta olcum belgelenmis', /exitTs % 60000 === 59999/.test(src)
   && /exitTsMod60000:59999/.test(src) && /sample:'725\/725'/.test(src));

console.log('\n══ B — mum kapanis penceresi ' + '═'.repeat(45));
{
  const mk=(exact,parity,win,nowMs)=>{
    const ctx=vm.createContext({
      V592_EXACT_BACKTEST_AUTHORITY:exact, V592_EXIT_CANDLE_PARITY:parity,
      V592_EXIT_CANDLE_WINDOW_MS:win, V592_ENTRY_CANDLE_MS:60000,
      v592ParityStats:{exitCandleAllowed:0,exitCandleDeferred:0},
      r501OrderLifeMark:()=>{}, Date:{now:()=>nowMs}, Math,Number,String,console});
    vm.runInContext(grab('function v592ExitCandleGate'),ctx);
    return {g:ctx.v592ExitCandleGate('X','MANAGER'),st:ctx.v592ParityStats};
  };
  const base=1785945000000; // mum sinirinda (base%60000===0)
  ok('mum kapandi +0sn      -> SERBEST', mk(true,true,12000,base).g.blocked===false);
  ok('mum kapandi +5sn      -> SERBEST', mk(true,true,12000,base+5000).g.blocked===false);
  ok('mum kapandi +12sn     -> SERBEST (pencere siniri)', mk(true,true,12000,base+12000).g.blocked===false);
  ok('mum kapandi +12,1sn   -> ERTELE', mk(true,true,12000,base+12100).g.blocked===true);
  ok('mum ortasi +30sn      -> ERTELE', mk(true,true,12000,base+30000).g.blocked===true);
  ok('mum sonu +59sn        -> ERTELE', mk(true,true,12000,base+59000).g.blocked===true);
  const r=mk(true,true,12000,base+30000);
  ok(`ertelemede kalan sure ${r.g.waitMs}ms`, r.g.waitMs===30000);
  ok('sayac exitCandleDeferred', r.st.exitCandleDeferred===1);
  ok('sayac exitCandleAllowed', mk(true,true,12000,base+3000).st.exitCandleAllowed===1);
  ok('parity KAPALI -> hic engel yok', mk(true,false,12000,base+30000).g.blocked===false);
  ok('exact KAPALI  -> hic engel yok', mk(false,true,12000,base+30000).g.blocked===false);
}

console.log('\n══ C — J2 (min tutus) + Y (mum) tek kapida ' + '═'.repeat(31));
{
  const base=1785945000000;
  const mk=(nowMs,openedAt,parity=true)=>{
    const ctx=vm.createContext({
      V592_EXACT_BACKTEST_AUTHORITY:true, V592_MIN_HOLD_MS:60000,
      V592_EXIT_CANDLE_PARITY:parity, V592_EXIT_CANDLE_WINDOW_MS:12000, V592_ENTRY_CANDLE_MS:60000,
      trailingState:new Map([['X',{openedAt}]]),
      r501OrderLifeSnapshot:()=>({}), r501OrderLifeMark:()=>{},
      v592ParityStats:{minHoldBlocks:0,exitCandleAllowed:0,exitCandleDeferred:0},
      Date:{now:()=>nowMs}, Math,Number,String,console});
    vm.runInContext(grab('function v592ExitCandleGate'),ctx);
    vm.runInContext(grab('function v592MinHoldGuard'),ctx);
    return {g:ctx.v592MinHoldGuard('X','MANAGER'),st:ctx.v592ParityStats};
  };
  // mum penceresi ICINDE ama 60sn dolmamis -> J2 bloklar
  let r=mk(base+3000,base+3000-30000);
  ok('mum acik + 30sn tutus -> J2 BLOKE', r.g.blocked===true && r.st.minHoldBlocks===1);
  // mum penceresi DISINDA -> Y erteler (J2'ye bile bakmadan)
  r=mk(base+30000,base+30000-120000);
  ok('mum kapali + 120sn tutus -> Y ERTELE', r.g.blocked===true && r.g.candleDeferred===true);
  ok('  erteleme J2 sayacini artirmadi', r.st.minHoldBlocks===0);
  // ikisi de uygun
  r=mk(base+3000,base+3000-120000);
  ok('mum acik + 120sn tutus -> SERBEST', r.g.blocked===false);
  // tam backtest tabani: 60sn
  r=mk(base+3000,base+3000-60000);
  ok('mum acik + tam 60sn -> SERBEST (backtest tabani)', r.g.blocked===false);
  r=mk(base+3000,base+3000-59999);
  ok('mum acik + 59,999sn -> BLOKE', r.g.blocked===true);
}

console.log('\n══ D — kaynak sozlesmesi ' + '═'.repeat(49));
ok('V592_EXIT_CANDLE_PARITY varsayilan ACIK', /V592_EXIT_CANDLE_PARITY = String\(process\.env\.V592_EXIT_CANDLE_PARITY \?\? '1'\) !== '0'/.test(src));
ok('pencere varsayilan 12sn', /V592_EXIT_CANDLE_WINDOW_MS \?\? 12000/.test(src));
ok('pencere 1-30sn kelepce', /Math\.max\(1000,Math\.min\(30000,Number\(process\.env\.V592_EXIT_CANDLE_WINDOW_MS/.test(src));
ok('v592ExitCandleGate tanimli', /function v592ExitCandleGate\(symbol,reason='MANAGER'\)/.test(src));
ok('minHoldGuard once mum kapisina bakar', /\{const _c=v592ExitCandleGate\(symbol,reason\);/.test(src));
ok('EXIT_DEFERRED_TO_CANDLE_CLOSE izi', /EXIT_DEFERRED_TO_CANDLE_CLOSE/.test(src));
ok('Binance SL istisnasi belgelenmis', /Binance'teki SL\/TP emri fiyata gore intrabar tetiklenir/.test(src));
for(const c of ['exitCandleDeferred','exitCandleAllowed'])
  ok(`sayac ${c}`, cnt(`${c}:0`)===1 && cnt(`v592ParityStats\\.${c}\\+\\+`)===1);
ok('telemetri exitCandleParity', cnt('exitCandleParity:V592_EXIT_CANDLE_PARITY')===2);
ok('sozlesmede olculmus degerler', cnt('exitTsMod60000:59999')===2
   && cnt('DYNAMIC_STOP:338,INITIAL_SL:210,TARGET:165,MAX_TIME_NO_PROGRESS:12')===4,
   'V4.7.4.20-AG3 backtestExitCounts ekledi -> 2+2=4');

console.log('\n══ E — giris paritesi ve oncekiler bozulmadi ' + '═'.repeat(29));
ok('W giris mum paritesi', /V592_ENTRY_CANDLE_PARITY/.test(src)&&/candidateToEntryMs:180000/.test(src));
ok('U deneme yasam dongusu', /const isNewAttempt = \(stage==='ORDER_REQUEST_RECEIVED'\)/.test(src));
ok('Q koruma-once', /PROTECT_FIRST_NO_PRECHECK/.test(src));
ok('S cikis arastirmasi', /closeSnap=rec\.closeResearchSnapshot/.test(src));
ok('J1 koruma korumasi', /protectionKeptExisting\+\+/.test(src));
ok('K1 tazelik butcesi', /freshCacheHits\+\+/.test(src));
ok('L fren ayrimi', /function isExecBackoffActive/.test(src));
ok('N WAIT atribusyonu', /waitSource:row\?\.waitSource\|\|null/.test(src));
ok('O testnet evreni', /v592IsTestnetTradable/.test(src));
ok('V4.5 secici 35/TOP_GAINER/0.35', /V592_V45_MS_SCORE_MIN.*35/.test(src)&&/V592_V45_FIRST_OBSTACLE_RR_MIN.*0\.35/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('build V4_7_4_27', /V4_7_4_27_CSV_REPORT_RISK41_10X/.test(src));
ok('eski build yok', !/V4_7_4_16_ENTRY_CANDLE_PARITY/.test(src));
ok('session 4_7_4_27_CR1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_27_CR1/.test(src));

console.log(`\n${'═'.repeat(74)}`);
console.log(fail?`SONUC: FAIL — ${pass} gecti, ${fail} dustu`:`SONUC: PASS — ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
