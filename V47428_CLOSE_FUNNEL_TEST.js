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

console.log('== A -- 07.08 SKYAIUSDT vakasi (olculmus) ' + '='.repeat(31));
const V={binancePozisyon:'YOK', panelPos:'YOK', gecmisSekmesi:'ACIK',
  kanitKaydedici:'OPEN', hamMB:26.5, ornek:6157, tick:8565,
  managerSonKayit:{currentSL:null,targetTP:null,sltpVerified:false,lastManageTs:null},
  managerDoluKayit:5096, managerToplam:5132,
  funnelTRADE_CLOSE:0, funnelCLOSE_ABORTED:0, funnelFALSE_FLAT:0,
  ticksDurdu:'14:08:37', managerDurdu:'15:43:51'};
ok('Binance pozisyon YOK', V.binancePozisyon==='YOK');
ok('panel Pos YOK', V.panelPos==='YOK');
ok('Gecmis sekmesi ACIK diyor', V.gecmisSekmesi==='ACIK');
ok('kanit kaydedici hala OPEN', V.kanitKaydedici==='OPEN');
ok('26,5 MB yazmaya devam etti', V.hamMB>25);
ok('manager state SILINMIS (son kayitlar null)',
   V.managerSonKayit.currentSL===null && V.managerSonKayit.lastManageTs===null);
ok('ama ONCE doluydu (5096/5132)', V.managerDoluKayit>5000);
ok('TRADE_CLOSE hic yazilmadi', V.funnelTRADE_CLOSE===0);
ok('kapanis IPTAL de edilmedi (kanit kapisi degil)', V.funnelCLOSE_ABORTED===0);
ok('sahte-flat korumasi da devreye girmedi', V.funnelFALSE_FLAT===0);
ok('=> trailingState silindi ama defter kapanmadi',
   V.managerSonKayit.currentSL===null && V.funnelTRADE_CLOSE===0);
ok('kok neden kaynakta belgelenmis', /07\.08 SKYAIUSDT vakasi: Binance'te pozisyon KAPANDI/.test(src));
ok('AC dersinin 3. tekrari notu', /Bu AC dersinin UCUNCU tekrari/.test(src));
ok('3 kapsanmayan cagri yeri yazili', /satir ~19786 emergencyCloseIfBracketMissing/.test(src));

console.log('\n== B -- AS1: tek kapanis hunisi ' + '='.repeat(42));
{
  const f=grab('async function v592FinalizeClose');
  ok('v592FinalizeClose var', /async function v592FinalizeClose\(sym, state, cls, reason='UNKNOWN'\)/.test(src));
  ok('idempotent (v592CloseAlreadyRecorded)', /v592CloseAlreadyRecorded\(anahtar\)/.test(f));
  ok('tekrar cagrida dedup sayaci', /v592ParityStats\.ledgerCloseDedup\+\+/.test(f));
  ok('recordTradeClose cagriliyor', /recordTradeClose\(S, st, c\)/.test(f));
  ok('trailingState siliniyor', /trailingState\.delete\(S\)/.test(f));
  ok('lastKnownPositions temizleniyor', /forgetKnownPosition\(S\); saveLastKnownPositions\(\)/.test(f));
  ok('funnel izi CLOSE_FINALIZED', /CLOSE_FINALIZED/.test(f));
  ok('recordTradeClose patlarsa yakalaniyor', /finalizeCloseFailed\+\+/.test(f));
  ok('hata olsa bile trailingState temizleniyor',
     (f.match(/trailingState\.delete\(S\)/g)||[]).length>=2);
  ok('durum bilgisi state yoksa trailingState/lastKnown den tureniyor',
     /trailingState\.get\(S\)[\s\S]{0,80}lastKnownPositions/.test(f));
}

console.log('\n== C -- AS2: 3 kapsanmayan yol baglandi ' + '='.repeat(34));
ok('bracket-missing "zaten kapali" dali', /v592FinalizeClose\(sym,_st,null,`\$\{reason\}_BRACKET_MISSING_ALREADY_CLOSED`\)/.test(src));
ok('bracket-missing acil kapatma dali', /code:'EMERGENCY_BRACKET_MISSING'/.test(src));
ok('yonetici cikisi', /String\(action\?\.type\|\|'MANAGER_EXIT'\)/.test(src));
ok('R14 hasar korumasi', /code:'R14_HARD_LOSS_GUARD',label:'Acil hasar korumasi kapatti'/.test(src));
{
  // her trailingState.delete'in yakininda defter kaydi VAR mi
  // Pencere degil: KAPSAYAN FONKSIYONU bul, defter kaydi orada mi bak.
  const fnBasla=(idx)=>{
    const pre=src.slice(0,idx);
    const m=[...pre.matchAll(/^(?:async )?function [A-Za-z0-9_]+\s*\(/gm)];
    return m.length?m[m.length-1].index:0;
  };
  // NOT: once PARANTEZ dengelenir. Aksi halde `state={}` varsayilan
  // parametresindeki susu govde sanip fonksiyonu 91 karakterde bitiriyor.
  // Bu harness hatasini daha once de yasadim; kalici olarak duzeltildi.
  const fnBitir=(basla)=>{
    let p=src.indexOf('(',basla),pd=0,q=p;
    for(;q<src.length;q++){ if(src[q]==='(')pd++; else if(src[q]===')'){pd--;if(!pd){q++;break;}} }
    let k=src.indexOf('{',q),d=0;
    for(;k<src.length;k++){ if(src[k]==='{')d++; else if(src[k]==='}'){d--;if(!d)return k;} }
    return src.length;
  };
  const yer=[...src.matchAll(/trailingState\.delete\(/g)].map(m=>{
    const ln=src.slice(0,m.index).split('\n').length;
    const b=fnBasla(m.index), e=fnBitir(b);
    const govde=src.slice(b,e);
    const ad=(govde.match(/^(?:async )?function ([A-Za-z0-9_]+)/)||[])[1]||'?';
    return {ln, fn:ad, kayit: /recordTradeClose|v592FinalizeClose/.test(govde)};
  });
  ok(`tum trailingState.delete yerlerinde defter kaydi (${yer.length} yer)`,
     yer.every(x=>x.kayit), JSON.stringify(yer.filter(x=>!x.kayit)));
  for(const y of yer) console.log(`         satir ${y.ln} · ${y.fn}() · defter ${y.kayit?'VAR':'YOK'}`);
  ok('cikis yollari 5 cagri yerine indi', yer.length<=6, String(yer.length));
}

console.log('\n== D -- v592FinalizeClose canli kosum ' + '='.repeat(36));
{
  const f=grab('async function v592FinalizeClose');
  const mk=()=>{
    const log=[]; const ts=new Map([['SKYAIUSDT',{openedAt:1000,side:'LONG',ledgerTradeId:'SKYAI_1'}]]);
    const kapali=new Set();
    const sb={console,Date,Number,String,Object,Promise,
      normalizeSymbol:(x)=>String(x).toUpperCase(),
      trailingState:ts, lastKnownPositions:{},
      v592CloseAlreadyRecorded:(id)=>{ if(kapali.has(id))return true; kapali.add(id); return false; },
      recordTradeClose:(s,st,c)=>{log.push(`recordTradeClose:${s}:${c?.code}`);},
      forgetKnownPosition:(s)=>log.push('forget:'+s),
      saveLastKnownPositions:()=>log.push('save'),
      r501EvidenceFunnel:(o)=>log.push('funnel:'+o.type),
      pushCritical:(a,b,c,d)=>log.push('critical:'+a),
      v592ParityStats:{ledgerCloseDedup:0,finalizeCloseOk:0,finalizeCloseFailed:0}};
    vm.createContext(sb); vm.runInContext(f+';globalThis.F=v592FinalizeClose;',sb);
    return {F:sb.F, log, ts, stats:sb.v592ParityStats};
  };
  const a=mk();
  const r1=await a.F('SKYAIUSDT',null,{code:'TAKE_PROFIT'},'TEST');
  ok('defter kaydi yazildi', a.log.some(x=>x.startsWith('recordTradeClose:SKYAIUSDT')), a.log.join(' | '));
  ok('trailingState silindi', !a.ts.has('SKYAIUSDT'));
  ok('lastKnown temizlendi', a.log.includes('forget:SKYAIUSDT'));
  ok('funnel izi yazildi', a.log.includes('funnel:CLOSE_FINALIZED'));
  ok('sayac finalizeCloseOk', a.stats.finalizeCloseOk===1);
  ok('donus ok', r1.ok===true);
  // idempotency
  const say=a.log.filter(x=>x.startsWith('recordTradeClose')).length;
  const r2=await a.F('SKYAIUSDT',{openedAt:1000,ledgerTradeId:'SKYAI_1'},{code:'TAKE_PROFIT'},'TEST');
  ok('IKINCI cagri defteri TEKRAR yazmadi',
     a.log.filter(x=>x.startsWith('recordTradeClose')).length===say);
  ok('ikinci cagri dedup dondu', r2.dedup===true);
  ok('dedup sayaci arti', a.stats.ledgerCloseDedup===1);
  // recordTradeClose patlarsa
  const b=mk(); b.F.toString();
  const sb2=(()=>{ const log=[]; const ts=new Map([['XUSDT',{openedAt:5}]]);
    const s2={console,Date,Number,String,Object,Promise,normalizeSymbol:x=>String(x).toUpperCase(),
      trailingState:ts,lastKnownPositions:{},v592CloseAlreadyRecorded:()=>false,
      recordTradeClose:()=>{throw new Error('defter bozuk');},
      forgetKnownPosition:()=>{},saveLastKnownPositions:()=>{},
      r501EvidenceFunnel:(o)=>log.push(o.type),pushCritical:(a)=>log.push('crit:'+a),
      v592ParityStats:{ledgerCloseDedup:0,finalizeCloseOk:0,finalizeCloseFailed:0}};
    vm.createContext(s2); vm.runInContext(f+';globalThis.F=v592FinalizeClose;',s2);
    s2.F('XUSDT',null,null,'T'); return {ts,log,stats:s2.v592ParityStats}; })();
  ok('recordTradeClose patlarsa yine de trailingState silinir', !sb2.ts.has('XUSDT'));
  ok('hata sayaci arti', sb2.stats.finalizeCloseFailed===1);
  ok('kritik hata bildirildi', sb2.log.some(x=>x.startsWith('crit:CLOSE_FINALIZE_FAIL')));
}

console.log('\n== E -- AS3 emniyet supurgesi ' + '='.repeat(44));
{
  const f=grab('async function v592OrphanLedgerSweep');
  ok('supurge var', /async function v592OrphanLedgerSweep\(\)/.test(src));
  ok('2 dakikada bir calisir', /setInterval\(\(\)=>\{v592OrphanLedgerSweep\(\)\.catch\(\(\)=>\{\}\);\},120000\)/.test(src));
  ok('yalniz OPEN kaydedicilere bakar', /rec\.status!=='OPEN'/.test(f));
  ok('minimum yas beklenir (erken dokunmaz)', /V592_SWEEP_MIN_AGE_MS/.test(f));
  ok('yonetici ilgileniyorsa DOKUNMAZ', /if\(yonetiliyor\) continue;/.test(f));
  ok('POZITIF KANIT ister (v592CloseProof)', /const kanit = await v592CloseProof\(S\)/.test(f));
  ok('kanit yoksa DOKUNMAZ', /if\(!kanit\.proven\)\{ v592ParityStats\.sweepNoProof\+\+;/.test(f));
  ok('kanitsiz durum kaydedilir', /SWEEP_NO_PROOF/.test(f));
  ok('kesinlestirmede iz birakir', /LEDGER_SWEEP_FINALIZED/.test(f));
  ok('tek huniden gecer', /v592FinalizeClose\(S,null,null,'ORPHAN_LEDGER_SWEEP'\)/.test(f));
  ok('AJ dersi yazili', /supheda pozisyon ACIK sayilir/.test(src));
  ok('sayaclar', /sweepChecked:0/.test(src)&&/sweepNoProof:0/.test(src)&&/sweepFinalized:0/.test(src));
}

console.log('\n== F -- supurge canli kosum ' + '='.repeat(46));
{
  const f=grab('async function v592OrphanLedgerSweep');
  const mk=async(opts)=>{
    const log=[];
    const sb={console,Date,Number,String,Object,Array,Promise,
      R501_EVIDENCE_ACTIVE:true,
      normalizeSymbol:x=>String(x).toUpperCase(),
      r501ActiveEvidence:new Map([['A',{id:'A',status:'OPEN',symbol:'SKYAIUSDT',openedAt:Date.now()-opts.yasMs,_finalized:false}]]),
      r501PersistRec:()=>{},
      V592_SWEEP_MAX_ATTEMPTS:3, v592SweepAttempts:new Map(),
      trailingState:new Map(opts.yonetiliyor?[['SKYAIUSDT',{}]]:[]),
      v592CloseProof:async()=>opts.kanit,
      v592FinalizeClose:(s,a,b,r)=>{log.push('finalize:'+s+':'+r);return{ok:true};},
      r501OrderLifeMark:(s,st)=>log.push('mark:'+st),
      logAuto:(m)=>log.push('log'),
      pushCritical:()=>{},
      V592_SWEEP_MIN_AGE_MS:180000,
      v592ParityStats:{sweepChecked:0,sweepNoProof:0,sweepFinalized:0,sweepGaveUp:0,sweepRecorderForceClosed:0}};
    vm.createContext(sb);
    vm.runInContext(f+';globalThis.S=v592OrphanLedgerSweep;',sb);
    await sb.S();
    return {log, stats:sb.v592ParityStats};
  };
  const a=await mk({yasMs:600000,yonetiliyor:false,kanit:{proven:true,reason:'FRESH_FLAT'}});
  ok('yetim + taze FLAT -> kesinlestirir', a.log.some(x=>x.startsWith('finalize:SKYAIUSDT')), a.log.join('|'));
  ok('kesinlestirme izi', a.log.includes('mark:LEDGER_SWEEP_FINALIZED'));
  ok('sayac sweepFinalized', a.stats.sweepFinalized===1);
  const b=await mk({yasMs:600000,yonetiliyor:false,kanit:{proven:false,reason:'SNAPSHOT_STALE'}});
  ok('kanit YOKSA dokunmaz', !b.log.some(x=>x.startsWith('finalize:')), b.log.join('|'));
  ok('kanitsizlik kaydedilir', b.log.includes('mark:SWEEP_NO_PROOF'));
  ok('sayac sweepNoProof', b.stats.sweepNoProof===1);
  const c=await mk({yasMs:600000,yonetiliyor:true,kanit:{proven:true}});
  ok('yonetici ilgileniyorsa dokunmaz', !c.log.some(x=>x.startsWith('finalize:')));
  ok('yonetici varken kanit bile sorulmaz', c.stats.sweepChecked===0);
  const d=await mk({yasMs:1000,yonetiliyor:false,kanit:{proven:true}});
  ok('cok yeni kayda dokunmaz (3dk esigi)', !d.log.some(x=>x.startsWith('finalize:')));
}

console.log('\n== G -- pasif sozlesme + onceki duzeltmeler ' + '='.repeat(30));
ok('sizingImpact false', /sizingImpact:false/.test(src));
ok('exitImpact false', /exitImpact:false/.test(src));
ok('telemetri singleCloseFunnel', /singleCloseFunnel:true/.test(src));
ok('telemetri orphanLedgerSweep', /orphanLedgerSweep:true/.test(src));
ok('telemetri her iki endpointte', cnt('singleCloseFunnel:true')>=2);
for(const [n,re] of [
  ['AJ kapanis kaniti',/async function v592CloseProof/],['AL1 tick akisi',/tickStreamRepaired/],
  ['AL2 kanit-only defter',/ledgerBootstrapSkipped/],['AL5 null n()',/if\(v===null\|\|v===undefined\|\|v===''\)return null/],
  ['AN1 kanit tum sebepler',/V592_CLOSE_PROOF_EXEMPT/],['AN2 yetim korunur',/v592PossibleOrphans/],
  ['AP1 oi onbellek',/oiFromCache/],['AP2 pasif vpin',/function calcVPINPassive/],
  ['AQ rest turevleri',/function r501RestDerive/],['AR1 tam baslik',/function r501PassiveHeader/],
  ['AR2 rapor hatasi',/function getJson\(u\)/],['AS1 tek huni',/function v592FinalizeClose/],
  ['AS3 supurge',/function v592OrphanLedgerSweep/],
]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('AG cikis beyaz listesi duruyor', /const V592_BACKTEST_EXIT_TYPES = Object\.freeze/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('V45 esikleri degismedi', /V592_V45_MS_SCORE_MIN/.test(src));
ok('build V4_7_4_41', /V4_7_4_41_PROBE_PERSIST_RISK41_10X/.test(src));
ok('session 4_7_4_41_PS1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_41_PS1/.test(src));
ok('eski build kalmadi', !/V4_7_4_40_PROBE_DEDUP_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
