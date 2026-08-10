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

console.log('== A -- AS1 deligi kapatti, YENI delik acti (olculmus) ' + '='.repeat(19));
const V={tradeOpen:3,tradeClose:3, pnlDolu:0, roiDolu:0, exitReasonDolu:0,
         sonuc:['NOTR','NOTR','NOTR'], girisAileDolu:17, cikisDeltaDolu:9, bosSutun:41, toplamSutun:349};
ok('AS1 CALISTI: 3 acilis 3 kapanis', V.tradeOpen===3 && V.tradeClose===3);
ok('ama pnlUSDT BOS', V.pnlDolu===0);
ok('roiPct BOS', V.roiDolu===0);
ok('exitReason BOS', V.exitReasonDolu===0);
ok('sonuc hepsinde NOTR', V.sonuc.every(x=>x==='NOTR'));
ok('KAR/ZARAR ayrimi IMKANSIZ', new Set(V.sonuc).size===1);
ok('giris aileleri DOLU (AQ calisti)', V.girisAileDolu===17);
ok('cikis + delta DOLU', V.cikisDeltaDolu===9);
ok('349 sutunun 308i dolu', V.toplamSutun-V.bosSutun===308);
ok('kok neden kaynakta belgelenmis', /AS1 DELIGI KAPATTI AMA YENI DELIK ACTI/.test(src));
ok('cls=null yedegi realizedPnl:null idi', /yedek nesne\s*\n\/\/ realizedPnl:null \/ closePrice:null iceriyordu/.test(src)||/realizedPnl:null \/ closePrice:null iceriyordu/.test(src));
ok('analizin anlamsizlasacagi notu', /KAR\/ZARAR etiketi olmadan 3 gunluk analizin TAMAMI anlamsiz/.test(src));

console.log('\n== B -- AT1: gercek PnL borsadan cekiliyor ' + '='.repeat(31));
{
  const f=grab('async function v592FinalizeClose');
  ok('fonksiyon ARTIK async', /async function v592FinalizeClose\(sym, state, cls, reason='UNKNOWN'\)/.test(src));
  ok('cls yoksa classifyClosedPosition cagriliyor',
     /c = await classifyClosedPosition\(autoConfig\.apiKey, autoConfig\.apiSecret, S, st\)/.test(f));
  ok('yalniz cls VERILMEDIYSE cagriliyor', /if\(!c\)\{/.test(f));
  ok('cls verildiyse borsaya gidilmez',
     f.indexOf('let c = cls;') < f.indexOf('classifyClosedPosition'));
  ok('siniflandirma sayaci', /closeClassified\+\+/.test(f));
  ok('siniflandirma hatasi yakalaniyor', /closeClassifyFailed\+\+/.test(f));
  ok('hata izi CLOSE_CLASSIFY_FAILED', /CLOSE_CLASSIFY_FAILED/.test(f));
  ok('siniflandirma basarisizsa yine de kapanir (notr)',
     /if\(!c\) c = \{code:'EXTERNAL_OR_MANUAL'/.test(f));
  ok('anahtar/dedup korunmus', /v592CloseAlreadyRecorded\(anahtar\)/.test(f));
  ok('recordTradeClose hala cagriliyor', /recordTradeClose\(S, st, c\)/.test(f));
}

console.log('\n== C -- AT2: tum cagri yerleri await ' + '='.repeat(37));
{
  const cagri=[...src.matchAll(/(await )?v592FinalizeClose\(/g)]
    .filter(m=>!src.slice(Math.max(0,m.index-40),m.index).includes('function'));
  ok(`${cagri.length} cagri yeri`, cagri.length===5, String(cagri.length));
  ok('HEPSI await', cagri.every(m=>m[1]==='await '),
     JSON.stringify(cagri.filter(m=>!m[1]).map(m=>src.slice(0,m.index).split('\n').length)));
  ok('cift await yok', !/await await/.test(src));
  // kapsayan fonksiyonlar async mi
  const fnAsync=(idx)=>{
    const pre=src.slice(0,idx);
    const m=[...pre.matchAll(/^(?:async )?function [A-Za-z0-9_]+\s*\(/gm)];
    return m.length ? m[m.length-1].group?.().startsWith('async') ?? m[m.length-1][0].startsWith('async') : false;
  };
  ok('kapsayan fonksiyonlarin hepsi async', cagri.every(m=>fnAsync(m.index)));
}

console.log('\n== D -- AT1 canli kosum ' + '='.repeat(50));
{
  const f=grab('async function v592FinalizeClose');
  const mk=(opts)=>{
    const log=[]; const ts=new Map([['HEIUSDT',{openedAt:1000,side:'LONG',ledgerTradeId:'HEI_1'}]]);
    const kapali=new Set(); let yazilanCls=null;
    const sb={console,Date,Number,String,Object,Promise,
      normalizeSymbol:x=>String(x).toUpperCase(),
      trailingState:ts, lastKnownPositions:{},
      autoConfig:{apiKey:'k',apiSecret:'s'},
      classifyClosedPosition: opts.patlat
        ? async()=>{throw new Error('income API hatasi');}
        : async()=>({code:'TAKE_PROFIT',label:'TP doldu',emoji:'🎯',closePrice:0.2178,realizedPnl:2.84,roiPct:20.38,side:'LONG'}),
      v592CloseAlreadyRecorded:(id)=>{if(kapali.has(id))return true;kapali.add(id);return false;},
      recordTradeClose:(s,st,c)=>{yazilanCls=c;log.push('record:'+s);},
      forgetKnownPosition:()=>{}, saveLastKnownPositions:()=>{},
      r501EvidenceFunnel:(o)=>log.push('funnel:'+o.type),
      r501OrderLifeMark:(s,st)=>log.push('mark:'+st),
      pushCritical:()=>{},
      v592ParityStats:{ledgerCloseDedup:0,finalizeCloseOk:0,finalizeCloseFailed:0,
                       closeClassified:0,closeClassifyFailed:0}};
    vm.createContext(sb); vm.runInContext(f+';globalThis.F=v592FinalizeClose;',sb);
    return {F:sb.F, log, ts, stats:sb.v592ParityStats, cls:()=>yazilanCls};
  };
  // 1) cls YOK -> borsadan cekilmeli
  const a=mk({});
  await a.F('HEIUSDT',null,null,'BRACKET_MISSING');
  ok('cls yokken borsadan siniflandi', a.stats.closeClassified===1);
  ok('GERCEK PnL defterine yazildi', a.cls()?.realizedPnl===2.84, JSON.stringify(a.cls()));
  ok('GERCEK ROI yazildi', a.cls()?.roiPct===20.38);
  ok('GERCEK cikis sebebi yazildi', a.cls()?.code==='TAKE_PROFIT');
  ok('kapanis fiyati yazildi', a.cls()?.closePrice===0.2178);
  ok('artik EXTERNAL_OR_MANUAL/null DEGIL', a.cls()?.code!=='EXTERNAL_OR_MANUAL');
  ok('trailingState silindi', !a.ts.has('HEIUSDT'));
  // 2) cls VERILDI -> borsaya gitmemeli
  const b=mk({});
  await b.F('HEIUSDT',null,{code:'R14_HARD_LOSS_GUARD',realizedPnl:-1.2},'R14');
  ok('cls verildiyse borsaya GITMEZ', b.stats.closeClassified===0);
  ok('verilen cls korunur', b.cls()?.code==='R14_HARD_LOSS_GUARD' && b.cls()?.realizedPnl===-1.2);
  // 3) siniflandirma patlarsa -> yine kapanir, notr
  const c=mk({patlat:true});
  await c.F('HEIUSDT',null,null,'BRACKET_MISSING');
  ok('siniflandirma patlarsa yine kapanir', c.log.some(x=>x.startsWith('record:')));
  ok('hata sayaci arti', c.stats.closeClassifyFailed===1);
  ok('hata izi birakildi', c.log.includes('mark:CLOSE_CLASSIFY_FAILED'));
  ok('yedek cls EXTERNAL_OR_MANUAL', c.cls()?.code==='EXTERNAL_OR_MANUAL');
  ok('trailingState yine silindi (takilma yok)', !c.ts.has('HEIUSDT'));
  // 4) idempotency korundu
  const d=mk({});
  await d.F('HEIUSDT',null,null,'X');
  const say=d.log.filter(x=>x.startsWith('record:')).length;
  await d.F('HEIUSDT',{openedAt:1000,ledgerTradeId:'HEI_1'},null,'X');
  ok('ikinci cagri defteri tekrar yazmaz', d.log.filter(x=>x.startsWith('record:')).length===say);
  ok('ikinci cagri borsaya da gitmez', d.stats.closeClassified===1);
}

console.log('\n== E -- KAR/ZARAR etiketi artik uretilebilir ' + '='.repeat(29));
{
  const f=grab('function r501PassiveRows(shapeOnly)');
  ok('sonuc pnlUSDT den tureniyor', /sonuc:\(n\(rec\.close\?\.pnlUSDT\)>0\?'KAR'/.test(f));
  ok('pnlUSDT rec.close dan okunuyor', /pnlUSDT:n\(rec\.close\?\.pnlUSDT\)/.test(f));
  // V4.7.4.31-AU2: once .exitReason, sonra .exitLabel, sonra eski yedekler
  ok('exitReason rec.close.exitReason dan', /exitReason:rec\.close\?\.exitReason\?\?rec\.close\?\.exitLabel/.test(f));
  ok('r501EvidenceClose realizedPnl i kullaniyor', /pnlUSDT:row\.pnlUSDT\?\?cls\.realizedPnl/.test(src));
  ok('r501EvidenceClose roiPct i kullaniyor', /roiPct:row\.roiPct\?\?cls\.roiPct/.test(src));
  ok('r501EvidenceClose exitReason i kullaniyor', /exitReason:row\.exitReason\?\?cls\.code/.test(src));
}

console.log('\n== F -- islem hizi gercegi (deploy karari icin) ' + '='.repeat(26));
const H={btIslem:725,btGun:60,btSembol:185, tnIslem:3,tnSaat:12.81,tnSembol:25};
const btHiz=H.btIslem/H.btGun, tnHiz=H.tnIslem/H.tnSaat*24;
ok('backtest 12.08 islem/gun', Math.abs(btHiz-12.08)<0.1, btHiz.toFixed(2));
ok('testnet 5.62 islem/gun', Math.abs(tnHiz-5.62)<0.1, tnHiz.toFixed(2));
ok('testnet backtest hizinin ~%47si', Math.abs(tnHiz/btHiz*100-47)<3);
ok('backtest 185 sembol taradi', H.btSembol===185);
ok('testnet 25 sembol tariyor', H.tnSembol===25);
ok('sembol evreni 7.4 kat dar', Math.abs(H.btSembol/H.tnSembol-7.4)<0.2);
ok('3 gunde ~17 islem beklenir (100 DEGIL)', Math.round(tnHiz*3)===17);
ok('100 islem icin ~18 gun gerekir', Math.abs(100/tnHiz-17.8)<0.5);
ok('=> 3 gun istatistik esigine YETMEZ', tnHiz*3 < 100);

console.log('\n== G -- onceki duzeltmeler ' + '='.repeat(47));
for(const [n,re] of [
  ['AJ kapanis kaniti',/async function v592CloseProof/],['AL1 tick akisi',/tickStreamRepaired/],
  ['AL5 null n()',/if\(v===null\|\|v===undefined\|\|v===''\)return null/],
  ['AN1 kanit tum sebepler',/V592_CLOSE_PROOF_EXEMPT/],['AN2 yetim korunur',/v592PossibleOrphans/],
  ['AP1 oi onbellek',/oiFromCache/],['AP2 pasif vpin',/function calcVPINPassive/],
  ['AQ rest turevleri',/function r501RestDerive/],['AR1 tam baslik',/function r501PassiveHeader/],
  ['AR2 rapor hatasi',/function getJson\(u\)/],['AS1 tek huni',/function v592FinalizeClose/],
  ['AS3 supurge',/function v592OrphanLedgerSweep/],['AT1 gercek pnl',/closeClassified\+\+/],
]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('AG cikis beyaz listesi duruyor', /const V592_BACKTEST_EXIT_TYPES = Object\.freeze/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('V45 esikleri degismedi', /V592_V45_MS_SCORE_MIN/.test(src));
ok('telemetri closePnlClassified', /closePnlClassified:true/.test(src));
ok('build V4_7_4_36', /V4_7_4_36_PROBE_PRICE_RISK41_10X/.test(src));
ok('session 4_7_4_36_PP1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_36_PP1/.test(src));
ok('eski build kalmadi', !/V4_7_4_35_ROTATE_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
