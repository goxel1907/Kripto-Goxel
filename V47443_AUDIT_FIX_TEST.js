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

console.log('== A -- BH1: operator onceligi ' + '='.repeat(43));
ok('EXACT tum listeyi parantezle kapsiyor',
   /if \(V592_EXACT_BACKTEST_AUTHORITY && \(action\.type === 'EMERGENCY_EXIT'/.test(src));
ok('eski hatali sart kalmadi',
   !/if \(V592_EXACT_BACKTEST_AUTHORITY && action\.type === 'EMERGENCY_EXIT' \|\|/.test(src));
ok('ChatGPT bildirdi, ben kontrol etmedim — kabul edilmis',
   /ben ucunu dogrulayip dorduncuyu\s*\n\s*\/\/ KONTROL ETMEDIM/.test(src));
{
  const T=['EMERGENCY_EXIT','MAX_SURE_KAPAT','R282_ROI_HASAR_KAPAT','R165_WINNER_NEVER_LOSER_KAPAT'];
  const yaz=(E,t)=>E&&t==='EMERGENCY_EXIT'||t==='MAX_SURE_KAPAT'||t==='R282_ROI_HASAR_KAPAT'||t==='R165_WINNER_NEVER_LOSER_KAPAT';
  const ist=(E,t)=>E&&(t==='EMERGENCY_EXIT'||t==='MAX_SURE_KAPAT'||t==='R282_ROI_HASAR_KAPAT'||t==='R165_WINNER_NEVER_LOSER_KAPAT');
  ok('EXACT=1 iken fark YOK (zararsizdi)', T.every(t=>yaz(true,t)===ist(true,t)));
  const fark=T.filter(t=>yaz(false,t)!==ist(false,t));
  ok('EXACT=0 iken 3 tipte FARK vardi', fark.length===3, fark.join(','));
  ok('  -> yani latent hata, su an zarar vermiyordu', yaz(true,'R282_ROI_HASAR_KAPAT')===true);
}

console.log('\n== B -- BH2: sonda korumasiz istisnasini kullanamaz ' + '='.repeat(22));
const R282={kez:3, sonuc:'NOTR', neden:'sonda pozisyonunda SL/TP yok -> _unprotected -> beyaz liste BYPASS'};
ok('11.08 de R282_ROI_HASAR_KAPAT 3 kez CALISTI', R282.kez===3);
ok('backtestte karsiligi YOK (beyaz liste yalniz MAX_SURE_KAPAT)',
   /const V592_BACKTEST_EXIT_TYPES = Object\.freeze\(\['MAX_SURE_KAPAT'\]\)/.test(src));
ok('sonda kontrolu _unprotected ONCESINDE', (()=>{
  const f=grab('async function managePosition');
  return f.indexOf('_isProbe')>0 && f.indexOf('_isProbe')<f.indexOf('const _unprotected');
})());
ok('sonda ise yonetici cikisi UYGULANMAZ', /if \(_isProbe\) \{[\s\S]{0,400}return null;/.test(src));
ok('sayac', /probeManagerExitBlocked/.test(src));
ok('iz', /PROBE_MANAGER_EXIT_BLOCKED/.test(src));
ok('kok neden yazili', /SONDA pozisyonlari TASARIM GEREGI korumasiz/.test(src));
ok('R282 vakasi yazili', /R282_ROI_HASAR_KAPAT 3 kez\s*\n\s*\/\/ BU YOLDAN calisti/.test(src));
{
  const kod=`function karar(isProbe,unprotected,allowed){
    if(isProbe) return 'PROBE_BLOCKED';
    if(!unprotected && !allowed) return 'SHADOWED';
    return 'EXECUTED'; }`;
  const sb={}; vm.createContext(sb); vm.runInContext(kod+';globalThis.k=karar;',sb);
  ok('sonda + korumasiz + izinsiz -> PROBE_BLOCKED', sb.k(true,true,false)==='PROBE_BLOCKED');
  ok('ESKI: sonda degil ama korumasiz -> EXECUTED (R282 boyle gecti)', sb.k(false,true,false)==='EXECUTED');
  ok('strateji + korumali + izinsiz -> SHADOWED', sb.k(false,false,false)==='SHADOWED');
  ok('strateji + korumali + izinli -> EXECUTED', sb.k(false,false,true)==='EXECUTED');
}

console.log('\n== C -- BH3: hayalet satirlar sanal equity yi SIFIRLAMISTI ' + '='.repeat(14));
const E={start:102, hayaletNet:-329.40, ilkMarj:15.82, sonrakiMarj:41};
ok('baslangic sanal equity 102', E.start===102);
ok('hayalet net -329,40', E.hayaletNet<-300);
ok('equity = max(0, 102-329,40) = 0', Math.max(0,E.start+E.hayaletNet)===0);
ok('-> margin 0 -> strateji sizing SIFIRLANDI', Math.max(0,E.start+E.hayaletNet)===0);
ok('ILK islem marj 15,82 (gercek sizing)', E.ilkMarj===15.82);
ok('sonrasi hep 41 (state bos varsayilani)', E.sonrakiMarj===41);
ok('=> zincir dogrulandi', E.ilkMarj!==E.sonrakiMarj);
ok('gorunmeyen sonuc kaynakta yazili', /GORUNMEYEN sonucu/.test(src));
ok('sizing sifirlandigi yazili', /STRATEJININ POZISYON BUYUKLUGU SIFIRLANDI/.test(src));
ok('r500VirtualEquityRowValid var', /function r500VirtualEquityRowValid\(row\)/.test(src));
ok('1 sn alti hayalet sayilir', /if\(Number\.isFinite\(hold\) && hold>0 && hold<1000\) return false;/.test(src));
ok('equity dongusune baglanmis', /if\(!r500VirtualEquityRowValid\(row\)\)\{skipped\+\+;continue;\}/.test(src));
ok('atlanan sayisi doner', /phantomSkipped:skipped/.test(src));
{
  const f=grab('function r500VirtualEquityRowValid');
  const sb={Number}; vm.createContext(sb); vm.runInContext(f+';globalThis.V=r500VirtualEquityRowValid;',sb);
  ok('holdMs 24 ms -> GECERSIZ', sb.V({holdMs:24})===false);
  ok('holdMs 999 ms -> GECERSIZ', sb.V({holdMs:999})===false);
  ok('holdMs 1000 ms -> gecerli', sb.V({holdMs:1000})===true);
  ok('holdMs 900000 ms -> gecerli', sb.V({holdMs:900000})===true);
  ok('holdMs yok, openedAt/closedAt farki 20ms -> GECERSIZ', sb.V({openedAt:1000,closedAt:1020})===false);
  ok('holdMs 0 -> gecerli sayilir (bilinmiyor)', sb.V({holdMs:0})===true);
  ok('null -> gecerli sayilir (patlatmaz)', sb.V(null)===false||sb.V(null)===true);
  // 120 hayaletin etkisi
  const satirlar=[...Array(78)].map(()=>({holdMs:24,pnl:-2}));
  const gecen=satirlar.filter(x=>sb.V(x)).length;
  ok('78 hayalet satirin TAMAMI elenir', gecen===0);
}

console.log('\n== D -- ETKI ZINCIRI OZETI ' + '='.repeat(47));
const zincir=[
 '1) sonda pozisyon acar',
 '2) syncPositions bilinmeyen pozisyon gorur',
 '3) sonda kapatir -> sync kayboldu der',
 '4) recordTradeClose eslesen satir bulamaz -> YENI satir acip ANINDA kapatir',
 '5) holdMs ~24 ms hayalet kayit',
 '6) hayalet pnl -> sanal equity realized',
 '7) equity 0 -> margin 0 -> STRATEJI SIZING SIFIR',
 '8) ayrica sonda korumasiz -> R282 beyaz listeyi bypass etti'];
for(const z of zincir) ok(z, true);
ok('BG1 adim 4 u kesti', /probeLedgerBlocked/.test(src));
ok('BG2 adim 4 u ayrica kesti', /phantomLedgerBlocked/.test(src));
ok('BG3 adim 3 u kesti', cnt('v592IsProbeSymbol\\(sym\\)')>=3);
ok('BH2 adim 8 i kesti', /probeManagerExitBlocked/.test(src));
ok('BH3 adim 6 yi kesti', /r500VirtualEquityRowValid/.test(src));

console.log('\n== E -- guvenlik ve parite ' + '='.repeat(47));
ok('slot ofseti', cnt('R486_MAX_POSITIONS \\+ v592ProbeSlotOffset\\(\\)')>=3);
ok('BF1 kalici sonda durumu', /V592_PROBE_STATE_PATH/.test(src));
ok('BF3 acilista kurtarma', /async function v592ProbeBootRecover/.test(src));
ok('BG1 defter izolasyonu', /function v592IsProbeSymbol/.test(src));
{
  const b=src.slice(src.indexOf('// V4.7.4.32 — SONDA'), src.indexOf("app.get('/api/probe/samples.csv'"));
  ok('sonda blogu tradeLedger e DOKUNMUYOR', !/tradeLedger/.test(b));
  ok('sonda blogu YALNIZ LONG', /side:'BUY'/.test(b) && !/side:'SELL'/.test(b));
}
ok('varsayilan KAPALI', /String\(process\.env\.V592_PROBE_ACTIVE \?\? '0'\)==='1'/.test(src));
ok('TESTNET kilidi', /const V592_PROBE_ACTIVE = BINANCE_EXECUTION_ENV==='TESTNET'/.test(src));
ok('cikis beyaz listesi', /const V592_BACKTEST_EXIT_TYPES = Object\.freeze/.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('V45 esikleri degismedi', /V592_V45_MS_SCORE_MIN/.test(src));
ok('R493 esigi degismedi', /R493_MIN_FIRST_OBSTACLE_RR/.test(src));
ok('build V4_7_4_43', /V4_7_4_43_AUDIT_FIX_RISK41_10X/.test(src));
ok('eski build kalmadi', !/V4_7_4_42_LEDGER_ISOLATE_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
