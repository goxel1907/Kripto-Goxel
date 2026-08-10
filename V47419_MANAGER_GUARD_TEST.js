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

console.log('══ A — 06.08 OLCUMU: guard neden hic calismadi ' + '═'.repeat(27));
const O={minHoldBlocks:0,exitCandleAllowed:0,exitCandleDeferred:0,nonBacktestExits:5,
         akeHoldMs:60847,j2Esik:60000};
ok('minHoldBlocks 0 idi', O.minHoldBlocks===0);
ok('exitCandleAllowed 0 idi', O.exitCandleAllowed===0);
ok('exitCandleDeferred 0 idi', O.exitCandleDeferred===0);
ok('nonBacktestExits 5', O.nonBacktestExits===5);
ok(`AKE ${O.akeHoldMs}ms · J2 esigi ${O.j2Esik}ms · fark ${O.akeHoldMs-O.j2Esik}ms`,
   O.akeHoldMs-O.j2Esik===847, 'tesadufi degildi');
ok('SEBEP: guard tek yerden cagriliyordu', /v592MinHoldGuard TEK bir yerden cagriliyordu/.test(src));

console.log('\n══ B — guard artik yonetici cikisinda ' + '═'.repeat(36));
ok('cagri yeri 2 oldu (tanim haric)', cnt('v592MinHoldGuard\\(')===3, `${cnt('v592MinHoldGuard\\(')}`);
// V4.7.4.21-AH2: korumasiz pozisyonda guard atlanir -> ucdeger ifade
ok('managePosition icinde cagri', /const _mg = _unprotected \? \{blocked:false\} : v592MinHoldGuard\(sym, `MANAGER_\$\{action\.type\}`\)/.test(src));
ok('korumasizda guard atlanir (V4.7.4.21)', /_unprotected \? \{blocked:false\}/.test(src));
ok('exact mod kosulu', /if \(V592_EXACT_BACKTEST_AUTHORITY && action\.type === 'EMERGENCY_EXIT'/.test(src));
ok('bloke olunca return null', /if \(_mg\.blocked\) \{[\s\S]{0,500}?return null;/.test(src));
{const gi=src.indexOf("const _mg = v592MinHoldGuard(sym");
 ok('asil cikis blogu guard\'dan SONRA', src.indexOf("await cancelAlgoOrders(apiKey, apiSecret, sym, true);", gi)>gi);}
ok('sayac managerExitDeferred', cnt('managerExitDeferred:0')===1 && cnt('managerExitDeferred\\+\\+')===1);
ok('sayac managerExitGuarded', cnt('managerExitGuarded:0')===1 && cnt('managerExitGuarded\\+\\+')===1);
ok('REDUCE_ONLY_CLOSE_SENT izi', /REDUCE_ONLY_CLOSE_SENT/.test(src));
ok('telemetri minHoldGuardCallSites:2', cnt('minHoldGuardCallSites:2')===2);

console.log('\n══ C — davranis (gercek AKE sayilariyla) ' + '═'.repeat(33));
{
  const base=1786014964206;   // AKE fillTime
  const mk=(nowMs,parity=true)=>{
    const ctx=vm.createContext({
      V592_EXACT_BACKTEST_AUTHORITY:true, V592_MIN_HOLD_MS:60000,
      V592_EXIT_CANDLE_PARITY:parity, V592_EXIT_CANDLE_WINDOW_MS:12000, V592_ENTRY_CANDLE_MS:60000,
      trailingState:new Map([['AKEUSDT',{openedAt:base}]]),
      r501OrderLifeSnapshot:()=>({}), r501OrderLifeMark:()=>{},
      v592ParityStats:{minHoldBlocks:0,exitCandleAllowed:0,exitCandleDeferred:0},
      Date:{now:()=>nowMs}, Math,Number,String,console});
    vm.runInContext(grab('function v592ExitCandleGate'),ctx);
    vm.runInContext(grab('function v592MinHoldGuard'),ctx);
    return {g:ctx.v592MinHoldGuard('AKEUSDT','MANAGER_R97_VUR_KAC_KAPAT'),st:ctx.v592ParityStats};
  };
  let r=mk(base+52000);
  ok('52 sn -> BLOKE (backtest tabani 60)', r.g.blocked===true);
  // AKE gercek kapanisi: yas 60.847ms, mum kapanisindan 5.053ms -> pencere ICINDE
  r=mk(base+60847);
  ok('AKE 60,847sn -> SERBEST (mum kapanisindan 5.053ms, pencere icinde)',
     r.g.blocked===false, JSON.stringify(r.g));
  // mum ortasi + 60sn ustu -> ERTELE
  r=mk(base+90000);
  ok('90 sn (mum kapanisindan 34.206ms) -> ERTELE',
     r.g.blocked===true && r.g.candleDeferred===true, JSON.stringify(r.g));
  r=mk(base+60847);
  ok('sayaclar artik ARTIYOR (exitCandleAllowed)', r.st.exitCandleAllowed===1, `${r.st.exitCandleAllowed}`);
  r=mk(base+90000);
  ok('erteleme sayaci ARTIYOR (exitCandleDeferred)', r.st.exitCandleDeferred===1, `${r.st.exitCandleDeferred}`);
  r=mk(base+30000);
  ok('30 sn -> BLOKE + minHoldBlocks artar', r.g.blocked===true && r.st.minHoldBlocks>=0);
  r=mk(base+52000,false);
  ok('mum paritesi KAPALI -> yalniz J2 calisir', r.g.blocked===true && !r.g.candleDeferred);
}

console.log('\n══ D — dokunulmayanlar ' + '═'.repeat(51));
ok('EMERGENCY_BRACKET yolu duruyor', /v592MinHoldGuard\(pos\?\.symbol\|\|pos\?\.sym,'EMERGENCY_BRACKET'\)/.test(src));
ok('manuel /api/close guard ALMAZ',
   src.indexOf("app.post('/api/close'")>0 && !/app\.post\('\/api\/close'[\s\S]{0,900}?v592MinHoldGuard/.test(src));
ok('kaldirac paritesi kapatmasi guard ALMAZ (guvenlik)',
   !/LEVERAGE_PARITY_FAILED[\s\S]{0,400}?v592MinHoldGuard/.test(src));
ok('AA1 cancel atlama', /const _skipCancel = \(firstInstall && attempt === 1/.test(src));
ok('AA2 ledger dedup', /function v592CloseAlreadyRecorded/.test(src));
ok('Q koruma-once', /PROTECT_FIRST_NO_PRECHECK/.test(src));
ok('W giris mum paritesi', /candidateToEntryMs:180000/.test(src));
ok('S cikis arastirmasi', /closeSnap=rec\.closeResearchSnapshot/.test(src));
ok('L fren ayrimi', /function isExecBackoffActive/.test(src));
ok('O testnet evreni', /v592IsTestnetTradable/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('build V4_7_4_37', /V4_7_4_37_PROBE_MAP_RISK41_10X/.test(src));
ok('eski build yok', !/V4_7_4_18_FAST_PROTECT/.test(src));
ok('session 4_7_4_37_PM1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_37_PM1/.test(src));

console.log(`\n${'═'.repeat(74)}`);
console.log(fail?`SONUC: FAIL — ${pass} gecti, ${fail} dustu`:`SONUC: PASS — ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
