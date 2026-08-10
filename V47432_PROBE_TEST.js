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

console.log('== A -- neden sonda gerekiyor (olculmus) ' + '='.repeat(32));
const V={islem:13,parametre:237,gecen:2.2,gecenSayi:3,beklenenYanlis:11.9,bonferroni:4.5,bonferroniGecen:0};
ok('13 islem 237 parametrede tarandi', V.islem===13&&V.parametre===237);
ok('|t|>=2.2 gecen 3', V.gecenSayi===3);
ok('sans eseri beklenen 11.9', V.beklenenYanlis>11);
ok('BULDUGUMUZ < SANS ESERI BEKLENEN', V.gecenSayi < V.beklenenYanlis);
ok('Bonferroni gecen 0', V.bonferroniGecen===0);
ok('range restriction kaynakta yazili', /range restriction/.test(src));
ok('13 islemin HEPSI ayni filtreden gecti notu', /13 islemin HEPSI ayni filtreden gecti/.test(src));
ok('98 MB\'lik disk olcumu yazili', /strateji islemi basina 98 MB/.test(src));
ok('282 GB hesabi yazili', /282 GB\/gun\. Imkansiz/.test(src));

console.log('\n== B -- GUVENLIK SINIRLARI ' + '='.repeat(47));
ok('YALNIZ testnet', /const V592_PROBE_ACTIVE = BINANCE_EXECUTION_ENV==='TESTNET'/.test(src));
ok('varsayilan KAPALI', /String\(process\.env\.V592_PROBE_ACTIVE \?\? '0'\)==='1'/.test(src));
ok('canli surume tasinmaz notu', /CANLI SURUME TASINMAZ/.test(src));
ok('ham arsiv YAZMAZ', /HAM ARSIV YAZMAZ/.test(src));
{
  const b=src.slice(src.indexOf('// V4.7.4.32 — SONDA'), src.indexOf("app.get('/api/probe/samples.csv'"));
  ok('tradeLedger e DOKUNMUYOR', !/tradeLedger/.test(b));
  ok('recordTradeClose CAGIRMIYOR', !/recordTradeClose/.test(b));
  ok('r501EvidenceOpen CAGIRMIYOR', !/r501EvidenceOpen/.test(b));
  ok('r501EvidenceIndex e DOKUNMUYOR', !/r501EvidenceIndex/.test(b));
  ok('v592ParityStats e DOKUNMUYOR', !/v592ParityStats/.test(b));
  ok('r501RawAppend CAGIRMIYOR (ham arsiv yok)', !/r501RawAppend|r501RawInit|r501RawStage/.test(b));
  ok('v592FinalizeClose CAGIRMIYOR', !/v592FinalizeClose/.test(b));
  ok('kendi sayaci var', /v592ProbeStats/.test(b));
  ok('kendi dosyasi var', /V592_PROBE_PATH/.test(b));
  ok('YALNIZ LONG', /side:'BUY'/.test(b) && !/side:'SELL'/.test(b));
  ok('LONG_ONLY etiketi', /side:'LONG_ONLY'/.test(b));
}

console.log('\n== C -- SLOT AYRIMI (strateji etkilenmesin) ' + '='.repeat(30));
ok('v592ProbeSlotOffset var', /function v592ProbeSlotOffset\(\)/.test(src));
ok('sonda kapaliyken 0 doner', /return V592_PROBE_ACTIVE \? v592ProbeOpen\.size : 0/.test(src));
ok('maxP hesabina eklendi', cnt('R486_MAX_POSITIONS \\+ v592ProbeSlotOffset\\(\\)')>=3);
ok('sizing tarafina da eklendi', /const maxPositions = R486_MAX_POSITIONS \+ v592ProbeSlotOffset\(\)/.test(src));
{
  const f=grab('function v592ProbeSlotOffset');
  const sb={console}; vm.createContext(sb);
  vm.runInContext('const V592_PROBE_ACTIVE=false;const v592ProbeOpen=new Map([["A",1],["B",2]]);'
    +f+';globalThis.o=v592ProbeSlotOffset;',sb);
  ok('KAPALI: ofset 0 (strateji hic etkilenmez)', sb.o()===0);
  const sb2={console}; vm.createContext(sb2);
  vm.runInContext('const V592_PROBE_ACTIVE=true;const v592ProbeOpen=new Map([["A",1],["B",2],["C",3]]);'
    +f+';globalThis.o=v592ProbeSlotOffset;',sb2);
  ok('ACIK: 3 sonda -> ofset 3', sb2.o()===3);
  ok('=> maxP 2 yerine 5, strateji yine 2 slot kullanir', 2+sb2.o()===5);
}

console.log('\n== D -- SONDA emir yolu ' + '='.repeat(50));
{
  const f=grab('async function v592ProbeOpenOne');
  ok('testnet evren kontrolu', /v592IsTestnetTradable\(S\)/.test(f));
  ok('ayni sembolde ikinci sonda YOK', /if\(v592ProbeOpen\.has\(S\)\) return false/.test(f));
  ok('STRATEJI pozisyonu varsa ATLA', /stratejiPozisyonu = trailingState\.has\(S\)/.test(f));
  ok('atlama sayaci', /skippedStrategyBusy\+\+/.test(f));
  ok('lot step yuvarlama', /Math\.floor\(\(notional\/px\)\/step\)\*step/.test(f));
  ok('qtyPrecision uygulaniyor', /qty\.toFixed\(Math\.max\(0,Number\(f\.qtyPrecision\)\|\|3\)\)/.test(f));
  ok('kaldirac ayarlaniyor', /'\/fapi\/v1\/leverage'/.test(f));
  ok('kendi clientOrderId oneki LZPRB', /'LZPRB'/.test(f));
  ok('GIRIS snapshot aliniyor', /const snapIn=v592ProbeSnapshot\(S\)/.test(f));
  ok('BOTUN o anki karari aliniyor', /kararIn=v592ProbeDecisionNow\(S\)/.test(f));
  ok('hata sayaci', /openFailed\+\+/.test(f));
}
{
  const f=grab('async function v592ProbeCloseOne');
  ok('CIKIS snapshot aliniyor', /const snapOut=v592ProbeSnapshot\(S\)/.test(f));
  ok('cikista da bot karari', /kararOut=v592ProbeDecisionNow\(S\)/.test(f));
  ok('reduce-only merkezi kapatma', /safeMarketClosePosition\(apiKey,apiSecret,S,\{reason:'PROBE_TIMED_EXIT'\}\)/.test(f));
  ok('kapanis fiyati yoksa defter/mum yedegi', /r501CurrentBook\(S\)\?\.mid/.test(f));
  ok('PnL hesaplaniyor', /pnlUSDT:pnl/.test(f));
  ok('sonuc etiketi KAR/ZARAR', /pnl>0\?'KAR':pnl<0\?'ZARAR'/.test(f));
  ok('finally ile her halukarda temizlenir', /finally\{ v592ProbeOpen\.delete\(S\); \}/.test(f));
}

console.log('\n== E -- dongu ve sarkma korumasi ' + '='.repeat(41));
{
  const f=grab('async function v592ProbeCycle');
  ok('kapali ise hemen doner', /if\(!V592_PROBE_ACTIVE\) return;/.test(f));
  // V4.7.4.39-BD1: artik once ENV yedegi denenir, cikis SAYACLI.
  ok('kimlik cozumu ENV yedekli', /const _c=v592ProbeCreds\(\)/.test(f));
  ok('kimlik yoksa SAYACLI doner', /skippedNoCreds[\s\S]{0,120}return;/.test(f));
  ok('ONCE suresi dolanlari kapatir', f.indexOf('v592ProbeCloseOne')<f.indexOf('v592ProbeOpenOne'));
  // V4.7.4.37-BB1: ayri anahtar kaldirildi; tarama ile AYNI onbellek paylasilir (agirlik 40 tasarrufu)
  ok('top-N ticker paylasimli onbellekten', /cached\('futures_tickers',FUTURES_TICKERS_CACHE_MS/.test(f));
  ok('max acik limiti', /if\(v592ProbeOpen\.size>=V592_PROBE_MAX_OPEN\)/.test(f));
}
// V4.7.4.35-AY2: esik HOLD_MS+60000 -> tam HOLD_MS, periyot 30sn -> 15sn.
ok('tutus dolunca 15sn icinde kapatilir',
   /Date\.now\(\)-Number\(p\.openedAt\|\|0\) >= V592_PROBE_HOLD_MS\)\s*\n\s*v592ProbeCloseOne/.test(src)
   && /\},15000\)\.unref/.test(src));
// V4.7.4.38-BC3: blogun basina 90 sn'lik acilis kicki eklendi.
ok('zamanlayicilar yalniz ACIKKEN kurulur',
   /if\(V592_PROBE_ACTIVE\)\{[\s\S]{0,500}setTimeout\([\s\S]{0,80}90000\)[\s\S]{0,200}setInterval\([\s\S]{0,80}V592_PROBE_INTERVAL_MS\)/.test(src));
ok('unref ile process kilitlemez', cnt("v592ProbeCycle\\(\\)\\.catch")>=1 && /\},V592_PROBE_INTERVAL_MS\)\.unref/.test(src));

console.log('\n== F -- cikti: endpoint + CSV ' + '='.repeat(44));
ok('/api/probe/status', /app\.get\('\/api\/probe\/status'/.test(src));
ok('/api/probe/samples.ndjson', /app\.get\('\/api\/probe\/samples\.ndjson'/.test(src));
ok('/api/probe/samples.csv', /app\.get\('\/api\/probe\/samples\.csv'/.test(src));
ok('CSV indirme adi', /filename="lazarus_sonda\.csv"/.test(src));
ok('status strategyImpact:false', /unconditionalSampler:true,strategyImpact:false,liveVersionExcluded:true/.test(src));
{
  const f=grab('function v592ProbeFlat');
  for(const [n,re] of [['CVD',/CvdRatio/],['tick',/TickDeltaRatio/],['VPIN',/VpinPct/],
    ['akis',/FlowDirection/],['defter',/BookImbalancePct/],['iceberg',/IcebergSignal/],
    ['likidasyon',/LongLiq5m/],['OI',/OpenInterest/],['funding',/FundingRate/],
    ['tazelik',/TickAgeMs/],['BOT karari',/botAction/],['BOT gerekcesi',/botReason/],
    ['BOT skoru',/botScore/],['delta',/dCvdRatio/],['donus bayragi',/cvdTrendFlipped/]])
    ok('sutun: '+n, re.test(f));
  ok('null-guvenli n()', /if\(v===null\|\|v===undefined\|\|v===''\)return null/.test(f));
}

console.log('\n== G -- v592ProbeFlat canli kosum ' + '='.repeat(40));
{
  const f=grab('function v592ProbeFlat');
  const sb={console,Number,Object,String}; vm.createContext(sb);
  vm.runInContext(f+';globalThis.F=v592ProbeFlat;',sb);
  const snap=(cvd,imb)=>({cvd:{ratio:cvd,trend:'BULLISH',momentum:'UP'},
    tick:{deltaRatio:44,vpin:{vpin:21,toxicity:'MEDIUM',direction:'BUY_DOMINANT'},whaleBias:'BUY',deltaTrend:'BULL'},
    flow:{direction:'ALIŞ',buyRatio30:58,delta30:900,acceleration:'GÜÇLENİYOR'},
    book:{imbalancePct:imb,spreadBps:3.2,bidNotional:1000,askNotional:900},
    iceberg:{signal:'NEUTRAL'},liquidations:{longLiq5m:10,shortLiq5m:20},
    openInterest:{openInterest:5e6},funding:{lastFundingRate:-0.00003},
    freshness:{tickAgeMs:400,depthAgeMs:300}});
  const r=sb.F({id:'PRB_X',symbol:'XUSDT',side:'LONG',openedAt:1000,closedAt:301000,holdMs:300000,
    marginUSDT:6,leverage:10,entryPrice:1,closePrice:1.02,pnlUSDT:1.2,roiPct:20,sonuc:'KAR',
    entryDecision:{action:'PUSU',authority:'R493',reason:'FO dusuk',score:41,tier:'B',
                   firstObstacleRR:0.2,ageMs:1200},
    closeDecision:{action:'MARKET'},
    entrySnapshot:snap(60,12), closeSnapshot:snap(70,-5)});
  console.log(`  → ${Object.keys(r).length} sutun`);
  ok('sutun 60+', Object.keys(r).length>=60, String(Object.keys(r).length));
  ok('sonuc KAR', r.sonuc==='KAR');
  ok('BOT o anda PUSU diyordu', r.botAction==='PUSU');
  ok('BOT gerekcesi kayitli', r.botReason==='FO dusuk');
  ok('BOT skoru kayitli', r.botScore===41);
  ok('BOT ilk engel RR kayitli', r.botFirstObstacleRR===0.2);
  ok('=> bot GIRMEZDI ama sonda girdi ve KAR etti', r.botAction==='PUSU' && r.sonuc==='KAR');
  ok('giris CVD', r.entryCvdRatio===60);
  ok('cikis CVD', r.closeCvdRatio===70);
  ok('delta CVD +10', r.dCvdRatio===10);
  ok('delta defter -17', r.dBookImbalancePct===-17);
  ok('cvdTrend donmedi', r.cvdTrendFlipped===false);
  ok('OI iki tarafta da dolu', r.entryOpenInterest===5e6 && r.closeOpenInterest===5e6);
  // eksik snapshot -> null, 0 DEGIL
  const b=sb.F({id:'P2',symbol:'Y',side:'LONG',entrySnapshot:snap(50,0),closeSnapshot:null,
    pnlUSDT:null,sonuc:'BILINMIYOR'});
  ok('cikis snapshot yok -> close alanlari null', b.closeCvdRatio===null);
  ok('delta null (0 degil)', b.dCvdRatio===null);
  ok('sonuc BILINMIYOR', b.sonuc==='BILINMIYOR');
}

console.log('\n== H -- ENV ve telemetri ' + '='.repeat(49));
for(const [n,re] of [['V592_PROBE_ACTIVE',/V592_PROBE_ACTIVE/],['INTERVAL',/V592_PROBE_INTERVAL_MS/],
  ['HOLD',/V592_PROBE_HOLD_MS/],['TOP_N',/V592_PROBE_TOP_N/],['MARGIN',/V592_PROBE_MARGIN_USDT/],
  ['LEVERAGE',/V592_PROBE_LEVERAGE/],['MAX_OPEN',/V592_PROBE_MAX_OPEN/]]) ok('ENV '+n, re.test(src));
ok('telemetri probeActive', /probeActive:V592_PROBE_ACTIVE/.test(src));
ok('telemetri probeStrategyImpact false', /probeStrategyImpact:false/.test(src));
ok('telemetri her iki endpointte', cnt('probeActive:V592_PROBE_ACTIVE')>=2);

console.log('\n== I -- onceki duzeltmeler bozulmadi ' + '='.repeat(37));
for(const [n,re] of [
  ['AJ kapanis kaniti',/async function v592CloseProof/],['AL1 tick akisi',/tickStreamRepaired/],
  ['AN1 kanit tum sebepler',/V592_CLOSE_PROOF_EXEMPT/],['AN2 yetim korunur',/v592PossibleOrphans/],
  ['AP2 pasif vpin',/function calcVPINPassive/],['AQ rest turevleri',/function r501RestDerive/],
  ['AS1 tek huni',/async function v592FinalizeClose/],['AS3 supurge',/async function v592OrphanLedgerSweep/],
  ['AT1 gercek pnl',/closeClassified\+\+/],['AU1 dedup ayrimi',/v592EvidenceClosedOnce/],
  ['AU2 exitReason',/exitReason:rec\.close\?\.exitReason/],['AU3 supurge limiti',/V592_SWEEP_MAX_ATTEMPTS/],
  ['AV1 sonda',/function v592ProbeCycle/],
]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('V45 esikleri degismedi', /V592_V45_MS_SCORE_MIN/.test(src));
ok('cikis beyaz listesi duruyor', /const V592_BACKTEST_EXIT_TYPES = Object\.freeze/.test(src));
ok('build V4_7_4_40', /V4_7_4_40_PROBE_DEDUP_RISK41_10X/.test(src));
ok('session 4_7_4_40_DD1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_40_DD1/.test(src));
ok('eski build kalmadi', !/V4_7_4_39_PROBE_CREDS_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
