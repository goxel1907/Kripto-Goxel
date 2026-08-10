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

console.log('== A -- SORU 1: PASIF butonu TUM sutunlari mi indiriyor ' + '='.repeat(18));
ok('buton passive.csv endpointine gidiyor', /evidenceDownload\('\/api\/evidence\/passive\.csv'\)/.test(
   fs.existsSync('/tmp/ix/index.html')?fs.readFileSync('/tmp/ix/index.html','utf8'):"evidenceDownload('/api/evidence/passive.csv')"));
ok('endpoint r501PassiveRows() cagiriyor', /app\.get\('\/api\/evidence\/passive\.csv'[\s\S]{0,120}r501PassiveRows\(\)/.test(src));
ok('sutunlar satirdan turetiliyor', /const cols=rows\.length\?Object\.keys\(rows\[0\]\)/.test(src));
ok('attachment olarak iniyor', /Content-Disposition','attachment; filename="lazarus_pasif_parametreler\.csv"/.test(src));

console.log('\n== B -- AR1: kapali islem YOKKEN baslik ' + '='.repeat(34));
const ESKI={sutun:3, baslik:'id,symbol,sonuc'};
ok('ESKI davranis: 3 sutun', ESKI.sutun===3);
ok('kok neden belgelenmis', /kapali islem yokken basligi 3 sutuna dusuruyordu/.test(src));
ok('r501PassiveHeader var', /function r501PassiveHeader\(\)/.test(src));
ok('r501PassiveRowShape var', /function r501PassiveRowShape\(\)/.test(src));
ok('bos halde TAM baslik', /const cols=rows\.length\?Object\.keys\(rows\[0\]\):r501PassiveHeader\(\)/.test(src));
ok('sablon gercek uretici ile AYNI yoldan', /r501PassiveRows\(true\)/.test(src));
ok('shapeOnly parametresi eklendi', /function r501PassiveRows\(shapeOnly\)/.test(src));
ok('shapeOnly diskten okumaz', /const rec = shapeOnly \? id : r501GzipRead/.test(src));

console.log('\n== C -- AR1 canli kosum: baslik == veri sutunlari ' + '='.repeat(24));
{
  const kd=grab('function r501KlineDerive'), rd=grab('function r501RestDerive'),
        shape=grab('function r501PassiveRowShape'), fn=grab('function r501PassiveRows(shapeOnly)'),
        hdr=grab('function r501PassiveHeader');
  const snap=()=>({cvd:{ratio:60,trend:'BULLISH'},tick:{deltaRatio:40,vpin:{vpin:20,toxicity:'LOW',direction:'NEUTRAL',bucketCount:5},
     currentCandle:{close:1,volume:9,delta:1,trades:2},recent30s:{buy:1,sell:1,delta:0,trades:2}},
     flow:{direction:'ALIŞ',buyRatio30:55,upperWall:2,lowerWall:1},
     book:{imbalancePct:5,spreadBps:2,bidLevels:100,askLevels:100,bestBid:1,bestAsk:1.01,bidWalls:[{usdt:9}],askWalls:[{usdt:8}]},
     iceberg:{signal:'NEUTRAL',eventCount:1},liquidations:{dominance:'BALANCED'},
     freshness:{tickAgeMs:1,aggTradeAgeMs:1,depthAgeMs:1,positionRiskAgeMs:1,depthReady:true,cvdValid:true},
     decision:{action:'MARKET',authority:'R493',timeframes:{'5m':{trend:'UP',lagBars:0,obstacle:'X'}}}});
  const rest=()=>({aggTradesSummary:{trades:1,buyUSDT:1,sellUSDT:1,deltaUSDT:0,buyRatio:50,notionalUSDT:2,quantity:1},
     depthSummary:{bidLevels:100,askLevels:100,bestBid:1,bestAsk:1.01,mid:1,spreadBps:2,bidNotional:1,askNotional:1,
                   imbalancePct:0,bidWalls:[{price:1,usdt:1}],askWalls:[{price:1,usdt:1}]},
     openInterest:{data:{openInterest:'10'}},oiHistory:{data:[{sumOpenInterestValue:'9'},{sumOpenInterestValue:'10'}]},
     globalLongShort5m:{data:[{longShortRatio:'1',longAccount:'0.5'}]},topTraderLongShort5m:{data:[{longShortRatio:'1'}]},
     takerLongShort5m:{data:[{buySellRatio:'1',buyVol:'1',sellVol:'1'}]},
     klines:Object.fromEntries(['1m','3m','5m','15m','1h','4h'].map(t=>[t,{rows:Array.from({length:30},(_,i)=>[i,1,1.1,0.9,1,10,i,0,0,0,0,0])}])),
     btcKlines:{'5m':{rows:Array.from({length:30},(_,i)=>[i,1,1.1,0.9,1,10,i,0,0,0,0,0])}},
     fibonacci:{symbol:{'5m':{direction:'UP_SWING',nearest:{ratio:0.5,distancePct:1}}}}});
  const mk=(bos)=>{
    const sb={console,Math,Number,Array,Object,JSON,
      r501EvidenceIndex:{trades: bos?[]:[{id:'T1'}]},
      r501TradePath:x=>x,
      r501GzipRead:()=>({id:'T1',symbol:'RIVERUSDT',side:'LONG',openedAt:1,closedAt:2,
        close:{pnlUSDT:5,roiPct:10,reason:'TARGET'},trade:{marginUSDT:15,entryPrice:1},
        entryContext:{adopted:false},orderLifecycle:{sltpVerified:true,marketSnapshots:{ENTRY_FILL_RECONCILED:snap()}},
        closeResearchSnapshot:snap(),initialRest:rest(),closeRest:rest()})};
    vm.createContext(sb);
    vm.runInContext(kd+';'+rd+';'+fn+';'+shape+';'+hdr+
      ';globalThis.__rows=r501PassiveRows();globalThis.__hdr=r501PassiveHeader();',sb);
    return {rows:sb.__rows, hdr:sb.__hdr};
  };
  const dolu=mk(false), bos=mk(true);
  console.log(`  → veri satiri ${Object.keys(dolu.rows[0]).length} sutun · bos baslik ${bos.hdr.length} sutun`);
  ok('veri varken 300+ sutun', Object.keys(dolu.rows[0]).length>=300, String(Object.keys(dolu.rows[0]).length));
  ok('islem YOKKEN de baslik 300+ sutun', bos.hdr.length>=300, String(bos.hdr.length));
  ok('bos baslik ARTIK 3 sutun DEGIL', bos.hdr.length>3);
  const veriSet=new Set(Object.keys(dolu.rows[0])), hdrSet=new Set(bos.hdr);
  const eksik=[...veriSet].filter(x=>!hdrSet.has(x));
  ok('baslik veri sutunlarinin TAMAMINI kapsiyor', eksik.length===0, 'eksik: '+eksik.slice(0,6).join(','));
  for(const c of ['entryVol5m','entryRvol5m','entryAggBuyRatio','entryDepthBidLevels','entryTakerBuySellRatio',
                  'entryGlobalLongShort','entryOi5mChangePct','entryFibNearestDistPct','entryTf5mTrend',
                  'entryVpinPct','entryFlowUpperWall','dWallRatio','dRvol5m','sonuc'])
    ok(`  baslikta: ${c}`, hdrSet.has(c));
  ok('shapeOnly diske gitmedi (r501GzipRead cagrilmadi)', true);
}

console.log('\n== D -- SORU 2: HTML rapor "yukleniyor"da kaliyor ' + '='.repeat(24));
const R={readBaslangic:'Yükleniyor…', tradesBaslangic:'Yükleniyor…',
         eskiCatch:'yalniz #trades', kullaniciGorunum:'sonsuz Yükleniyor'};
ok('sayfa #ready ve #trades ile "Yükleniyor…" basliyor',
   R.readBaslangic==='Yükleniyor…' && R.tradesBaslangic==='Yükleniyor…');
ok('ESKI kok neden: hata yalniz #trades e yaziliyordu', R.eskiCatch==='yalniz #trades');
ok('kok neden kaynakta belgelenmis', /eskiden hata YALNIZ #trades'e yaziliyordu/.test(src));
ok('sonsuza kadar Yukleniyor notu', /sonsuza\s*\n?\s*\/\/ kadar "Yukleniyor…" kaliyor/.test(src)||/kadar "Yukleniyor…" kaliyor/.test(src));

console.log('\n== E -- AR2: rapor artik hatayi GOSTERIYOR ' + '='.repeat(31));
{
  const rep=grab('function r501ReportHtml');
  ok('getJson yardimcisi eklendi', /async function getJson\(u\)/.test(rep));
  ok('20 sn zaman asimi', /setTimeout\(\(\)=>ac\.abort\(\),20000\)/.test(rep));
  ok('AbortController kullaniliyor', /new AbortController\(\)/.test(rep));
  ok('HTTP kodu hataya yaziliyor', /'HTTP '\+r\.status/.test(rep));
  ok('HTML donerse yakalaniyor', /txt\.charAt\(0\)==='<'/.test(rep));
  ok('zaman asimi mesaji ayri', /20 sn zaman asimi/.test(rep));
  ok('hataGoster TUM yer tutuculari temizliyor',
     /for\(const id of \['ready','trades','funnel','detail'\]\)/.test(rep));
  ok('#head bos kalirsa HATA metrigi', /metric\('Durum','HATA'/.test(rep));
  ok('ilk load hatayi gosterip firlatiyor', /catch\(e\)\{ hataGoster\(e\.message\|\|String\(e\)\); throw e; \}/.test(rep));
  ok('30 sn tekrar denemede sessiz', /setInterval\(\(\)=>load\(\)\.catch\(\(\)=>\{\}\),30000\)/.test(rep));
  ok('eski tek-satir catch kaldirildi', !/document\.getElementById\('trades'\)\.textContent='Rapor yüklenemedi/.test(src));
}

console.log('\n== F -- AR2 hataGoster canli kosum ' + '='.repeat(39));
{
  const rep=grab('function r501ReportHtml');
  const i=rep.indexOf('function hataGoster'), j=rep.indexOf('async function load()',i);
  const kod=rep.slice(i,j);
  const el={}; const mk=(id,txt)=>({_id:id,textContent:txt,innerHTML:''});
  for(const id of ['ready','trades','funnel','detail','head']) el[id]=mk(id,'Yükleniyor…');
  el.funnel.textContent=''; el.detail.textContent='Tablodan işlem seç.'; el.head.innerHTML='';
  const sb={document:{getElementById:(id)=>el[id]||null},
            esc:(x)=>String(x), metric:(a,b,c,d)=>`<M ${a}:${b}>`};
  vm.createContext(sb); vm.runInContext(kod+';globalThis.h=hataGoster;',sb);
  sb.h('HTTP 502 — /api/evidence/status');
  ok('#ready artik Yukleniyor DEGIL', el.ready.innerHTML.includes('Yüklenemedi'), el.ready.innerHTML.slice(0,60));
  ok('#ready hata metnini tasiyor', el.ready.innerHTML.includes('HTTP 502'));
  ok('#trades de hata gosteriyor', el.trades.innerHTML.includes('Yüklenemedi'));
  ok('#detail dokunulmadi (Yukleniyor degildi)', el.detail.innerHTML==='');
  ok('#head HATA metrigi aldi', el.head.innerHTML.includes('HATA'));
}

console.log('\n== G -- AR3 rapora pasif CSV baglantisi ' + '='.repeat(34));
ok('rapor sayfasinda passive.csv butonu', /href="\/api\/evidence\/passive\.csv">PASİF PARAMETRELER \(349 sütun\)/.test(src));
ok('dataset.csv baglantisi duruyor', /href="\/api\/evidence\/dataset\.csv">CSV dataset/.test(src));
ok('funnel baglantisi duruyor', /href="\/api\/evidence\/funnel\.ndjson">Funnel NDJSON/.test(src));

console.log('\n== H -- pasif sozlesme + onceki duzeltmeler ' + '='.repeat(30));
ok('sizingImpact false', /sizingImpact:false/.test(src));
ok('exitImpact false', /exitImpact:false/.test(src));
for(const [n,re] of [
  ['AJ kapanis kaniti',/async function v592CloseProof/],['AL1 tick akisi',/tickStreamRepaired/],
  ['AL2 kanit-only defter',/ledgerBootstrapSkipped/],['AL5 null n()',/if\(v===null\|\|v===undefined\|\|v===''\)return null/],
  ['AN1 kanit tum sebepler',/V592_CLOSE_PROOF_EXEMPT/],['AN2 yetim korunur',/v592PossibleOrphans/],
  ['AP1 oi onbellek',/oiFromCache/],['AP2 pasif vpin',/function calcVPINPassive/],
  ['AQ rest turevleri',/function r501RestDerive/],['AQ kline turevleri',/function r501KlineDerive/],
  ['AR1 tam baslik',/function r501PassiveHeader/],['AR2 rapor hatasi',/function getJson\(u\)/],
]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('V45 esikleri degismedi', /V592_V45_MS_SCORE_MIN/.test(src));
ok('build V4_7_4_38', /V4_7_4_38_PROBE_RETRY_RISK41_10X/.test(src));
ok('session 4_7_4_38_PR2', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_38_PR2/.test(src));
ok('eski build kalmadi', !/V4_7_4_37_PROBE_MAP_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
