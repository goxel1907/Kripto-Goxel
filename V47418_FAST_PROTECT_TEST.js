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
// 06.08 ACT olcumu: imzali cagri 20-60sn, ortalama ~40sn
const AG=40000, PROOF=40000;

function mk({firstInstall,log}){
  let saat=0; const ilerle=ms=>{saat+=ms;}; let yazildi=false;
  const ctx=vm.createContext({
    V592_PROTECT_KEEP_EXISTING:true, POS_FRESH_ORDER_MS:3000, POS_FRESH_MANAGER_MS:8000,
    v592ParityStats:{protectionKeptExisting:0,protectFirstInstalls:0,protectFirstMs:0,
      orphanProtectionCleaned:0,postWriteProofFail:0,cancelSkippedFirstInstall:0,protectParallelWrites:0},
    r501OrderLifeMark:(sy,st)=>{log.push([saat,st]);},
    verifyAlgoSLTPVisible:async()=>{ilerle(PROOF);log.push([saat,'[ag] openOrders']);
      const g=yazildi; return {ok:g,foundSL:g,foundTP:g,orderCount:g?2:0};},
    freshOpenPositionForSymbol:async(k,s,sym,att)=>{for(let i=0;i<att;i++){ilerle(AG);log.push([saat,'[ag] account']);}
      return {open:true,pos:{positionAmt:'1',markPrice:'0.0102'}};},
    fetchPositionRiskRaw:async()=>{ilerle(AG);return [];}, bReq:async()=>{ilerle(AG);return [];},
    cancelAlgoOrders:async()=>{ilerle(AG);log.push([saat,'[ag] cancelAlgoOrders']);},
    cleanupClosedPositionState:async()=>{},
    normalizeSLTPToTick:async(s,a,b)=>({sl:a,tp:b,slNum:a,tpNum:b,tickSize:1e-6}),
    // PARALEL testi: ikisi de ayni anda baslar, biri bitince saat ilerlemis olur
    placeAlgoSL:async()=>{const t0=saat;await Promise.resolve();ilerle(0);log.push([t0,'[ag] SL yazim BASLADI']);
      return new Promise(r=>{ilerle(AG);log.push([saat,'[ag] SL YAZILDI']);r({orderId:'SL1'});});},
    placeAlgoTP:async()=>{const t0=saat;log.push([t0,'[ag] TP yazim BASLADI']);
      return new Promise(r=>{log.push([saat,'[ag] TP YAZILDI']);yazildi=true;r({orderId:'TP1'});});},
    bPub:async()=>({price:'0.0102'}), pushCritical:()=>{}, isNoOpenPositionAlgoError:()=>false,
    formatStepValue:v=>v, console:{log:()=>{}}, setTimeout:(f)=>{f();return 0;},
    Date:{now:()=>1786005870565+saat}, Math,Number,String,Object,Array,Boolean,Promise,Infinity,JSON,Error,
  });
  vm.runInContext(grab('async function installSLTPWithProof'),ctx);
  return ctx;
}

console.log('══ A — 06.08 ACT vakasi: koruma gecikmesi ' + '═'.repeat(32));
{
  const eski=[],yeni=[];
  await mk({firstInstall:false,log:eski}).installSLTPWithProof('k','s','ACTUSDT','SELL',0.0099,0.0109,'ACTUSDT');
  await mk({firstInstall:true,log:yeni}).installSLTPWithProof('k','s','ACTUSDT','SELL',0.0099,0.0109,'ACTUSDT',{firstInstall:true});
  const agCagri=l=>l.filter(x=>String(x[1]).startsWith('[ag]')&&!String(x[1]).includes('BASLADI')).length;
  console.log(`\n  ESKI yol (${agCagri(eski)} agir ag cagrisi):`);
  eski.filter(x=>String(x[1]).startsWith('[ag]')).slice(0,6).forEach(([t,m])=>console.log(`    ${String(t).padStart(7)}ms  ${m}`));
  console.log(`\n  YENI yol (${agCagri(yeni)} agir ag cagrisi):`);
  yeni.filter(x=>String(x[1]).startsWith('[ag]')||String(x[1]).includes('CANCEL_SKIP')).slice(0,6).forEach(([t,m])=>console.log(`    ${String(t).padStart(7)}ms  ${m}`));
  console.log();
  ok('YENI: cancelAlgoOrders ATLANDI', !yeni.some(x=>String(x[1]).includes('cancelAlgoOrders')));
  ok('ESKI: cancelAlgoOrders vardi', eski.some(x=>String(x[1]).includes('cancelAlgoOrders')));
  ok('CANCEL_SKIPPED_FIRST_INSTALL izi', yeni.some(x=>x[1]==='CANCEL_SKIPPED_FIRST_INSTALL'));
  ok(`agir cagri ${agCagri(eski)} -> ${agCagri(yeni)}`, agCagri(yeni)<agCagri(eski), `${agCagri(eski)} vs ${agCagri(yeni)}`);
}

console.log('\n══ B — kaynak sozlesmesi ' + '═'.repeat(49));
ok('ilk denemede cancel atlanir', /const _skipCancel = \(firstInstall && attempt === 1/.test(src));
ok('2. denemede cancel YAPILIR', /\} else \{\s*\n\s*await cancelAlgoOrders\(apiKey, apiSecret, symbol\);/.test(src));
ok('SL+TP Promise.all ile PARALEL', /const \[slOrder, tpOrder\] = await Promise\.all\(\[/.test(src));
ok('sirali placeAlgoSL/TP kalmadi', !/const slOrder = await placeAlgoSL[\s\S]{0,120}const tpOrder = await placeAlgoTP/.test(src));
ok('sayac cancelSkippedFirstInstall', cnt('cancelSkippedFirstInstall:0')===1 && cnt('cancelSkippedFirstInstall\\+\\+')===1);
ok('sayac protectParallelWrites', cnt('protectParallelWrites:0')===1 && cnt('protectParallelWrites\\+\\+')===1);
ok('06.08 olcumu belgelenmis', /fillToProtectionMs = 192\.789 ms/.test(src));

console.log('\n══ C — ledger mukerrer kapanis ' + '═'.repeat(43));
{
  const ctx=vm.createContext({v592ParityStats:{ledgerCloseDedup:0},r501OrderLifeMark:()=>{},
    Date:{now:()=>1786007325577},Map,Number,String,console});
  // V4.7.4.31-AU1: TTL + iki map + uc fonksiyon tek blok halinde geliyor.
  // Ayri ayri yuklemek 'already declared' hatasi veriyordu.
  vm.runInContext(src.slice(src.indexOf('const V592_CLOSE_DEDUP_TTL_MS='),
                            src.indexOf('function v592EvidenceAlreadyClosed')+130),ctx);
  const ID='ACT_1786006063360';
  ok('1. kapanis -> kaydedilir', ctx.v592CloseAlreadyRecorded(ID)===false);
  ok('2. kapanis -> BASTIRILIR', ctx.v592CloseAlreadyRecorded(ID)===true);
  let bastirilan=0; for(let i=0;i<9;i++) if(ctx.v592CloseAlreadyRecorded(ID)) bastirilan++;
  ok(`06.08: 10 kapanis -> 1 kayit + ${bastirilan} bastirildi`, bastirilan===9);
  ok('farkli islem etkilenmez', ctx.v592CloseAlreadyRecorded('BASKA_1')===false);
  ok('TTL 6 saat', /V592_CLOSE_DEDUP_TTL_MS=6\*60\*60\*1000/.test(src));
  ok('kanit dedup r501EvidenceClose girisinde', /if\(v592EvidenceAlreadyClosed\(String\(row\.id\)\)\)\{/.test(src));
  // V4.7.4.28-AS1: v592FinalizeClose de ayni dedup sayacini kullaniyor -> 2 artis noktasi.
  // V4.7.4.31-AU1: kanit tarafi kendi sayacina gecti -> defter sayaci 1 yerde
  ok('sayac ledgerCloseDedup', cnt('ledgerCloseDedup:0')===1 && cnt('ledgerCloseDedup\\+\\+')===1);
  ok('kanit ayri sayac', cnt('evidenceCloseDedup\\+\\+')===1);
  ok('CLOSE_DEDUP_SUPPRESSED izi', /'CLOSE_DEDUP_SUPPRESSED'/.test(src));
  ok('06.08 olcumu belgelenmis', /TRADE_CLOSE_RECORDED 10 kez yazildi/.test(src));
}

console.log('\n══ D — onceki duzeltmeler bozulmadi ' + '═'.repeat(38));
ok('Q koruma-once', /PROTECT_FIRST_NO_PRECHECK/.test(src)&&/protectFirstInstalls\+\+/.test(src));
ok('Q4 yetim kontrolu (AN2: artik SILMIYOR, erteliyor)', /orphanCheckDeferred\+\+/.test(src));
ok('W giris mum paritesi', /candidateToEntryMs:180000/.test(src));
ok('Y cikis mum paritesi', /function v592ExitCandleGate/.test(src));
ok('U deneme yasam dongusu', /const isNewAttempt = \(stage==='ORDER_REQUEST_RECEIVED'\)/.test(src));
ok('S cikis arastirmasi', /closeSnap=rec\.closeResearchSnapshot/.test(src));
ok('J1 yonetici yolunda', /V592_PROTECT_KEEP_EXISTING && !firstInstall/.test(src));
ok('J2 min hold', /function v592MinHoldGuard/.test(src));
ok('L fren ayrimi', /function isExecBackoffActive/.test(src));
ok('O testnet evreni', /v592IsTestnetTradable/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('build V4_7_4_40', /V4_7_4_40_PROBE_DEDUP_RISK41_10X/.test(src));
ok('eski build yok', !/V4_7_4_17_CANDLE_PARITY/.test(src));
ok('session 4_7_4_40_DD1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_40_DD1/.test(src));

console.log(`\n${'═'.repeat(74)}`);
console.log(fail?`SONUC: FAIL — ${pass} gecti, ${fail} dustu`:`SONUC: PASS — ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
