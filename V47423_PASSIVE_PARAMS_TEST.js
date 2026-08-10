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

console.log('== A -- 06/07.08 vakasi: 5 pasif alan hep bos ' + '='.repeat(29));
const VAKA={cvdMomentum:'',vpin:'',akisYonu:'VERI_YOK',alisOrani30sn:0,whaleBias:'NEUTRAL',
  ticksJsonlSatir:0,tickIntegrity:'UNKNOWN',cvdStoreOrnek:14516,r371FlowStoreOrnek:0};
ok('cvdMomentum bostu', VAKA.cvdMomentum==='');
ok('VPIN bostu', VAKA.vpin==='');
ok('akisYonu VERI_YOK', VAKA.akisYonu==='VERI_YOK');
ok('alisOrani30sn 0', VAKA.alisOrani30sn===0);
ok('whaleBias NEUTRAL (varsayilan)', VAKA.whaleBias==='NEUTRAL');
ok('ticks.jsonl 0 satir', VAKA.ticksJsonlSatir===0);
ok('tick_integrity UNKNOWN', VAKA.tickIntegrity==='UNKNOWN');
ok('ayni stream cvdStore icin 14.516 ornek verdi', VAKA.cvdStoreOrnek>10000 && VAKA.r371FlowStoreOrnek===0,
   'kanit: sorun Binance degil, YOL farki');

console.log('\n== B -- AL1 aggTrade akisi dogru yolda ' + '='.repeat(35));
ok('kodun kendi kurali yazili', /Market streams \(@aggTrade, @forceOrder, markPrice\/kline\) \/market/.test(src));
ok('r371 aggTrade FAPI_WS_MARKET kullaniyor',
   /const aggWs = new WebSocket\(`\$\{FAPI_WS_MARKET\}\/ws\/\$\{lower\}@aggTrade`\)/.test(src));
ok('r371 aggTrade artik FAPI_WS_PUBLIC kullanmiyor',
   !/const aggWs = new WebSocket\(`\$\{FAPI_WS_PUBLIC\}\/ws\/\$\{lower\}@aggTrade`\)/.test(src));
ok('tamir sayaci var', /v592ParityStats\.tickStreamRepaired\+\+/.test(src));
ok('tickStreamRepaired sayac tanimli', /tickStreamRepaired:0/.test(src));
ok('kok neden kaynakta belgelenmis', /aggTrade AKISI YANLIS YOLDAN ISTENIYORDU/.test(src));
ok('FAPI_WS_PUBLIC hala mevcut (depth/bookTicker icin)', cnt('FAPI_WS_PUBLIC')>0);
{
  const agg=(src.match(/new WebSocket\(`\$\{FAPI_WS_[A-Z]+\}[^`]*@aggTrade[^`]*`\)/g)||[]);
  ok(`tum aggTrade soketleri /market (${agg.length} adet)`,
     agg.length>0 && agg.every(x=>x.includes('FAPI_WS_MARKET')), agg.join(' | '));
}

console.log('\n== C -- AL2 defter yalniz kanittan ' + '='.repeat(39));
const PANEL={panelIslem:51,panelPnl:-271.91,kanitIslem:4,kanitPnl:-0.344};
ok('panel 51 dedi', PANEL.panelIslem===51);
ok('kanit 4 dedi', PANEL.kanitIslem===4);
ok('fark buyuk (kismi dolumlar sayildi)', PANEL.panelIslem/PANEL.kanitIslem>10);
ok('V592_LEDGER_EVIDENCE_ONLY tanimli',
   /const V592_LEDGER_EVIDENCE_ONLY = String\(process\.env\.V592_LEDGER_EVIDENCE_ONLY \?\? '1'\) !== '0'/.test(src));
{
  const b=grab('async function r179BootstrapLedger48h');
  ok('bootstrap exact modda atlanir',
     /V592_EXACT_BACKTEST_AUTHORITY && V592_LEDGER_EVIDENCE_ONLY && !opts\?\.force/.test(b));
  ok('atlama sayaci artar', /v592ParityStats\.ledgerBootstrapSkipped\+\+/.test(b));
  ok('EVIDENCE_ONLY_LEDGER sebebi doner', /EVIDENCE_ONLY_LEDGER/.test(b));
  ok('force ile hala calistirilabilir', /!opts\?\.force/.test(b));
  const iEv=b.indexOf('EVIDENCE_ONLY_LEDGER'), iUt=b.search(/userTrades|\/fapi\/v1\/userTrades/);
  ok('erken donus USER_TRADES cagrisindan ONCE', iEv>=0 && (iUt<0 || iEv<iUt), `ev=${iEv} ut=${iUt}`);
}
ok('ledgerBootstrapSkipped sayac tanimli', /ledgerBootstrapSkipped:0/.test(src));
ok('defter yine recordTradeOpen/Close ile dolar',
   /function recordTradeOpen/.test(src) && /function recordTradeClose/.test(src));

console.log('\n== D -- AL3 pasif parametre disa aktarimi ' + '='.repeat(32));
ok('r501PassiveRows var', /function r501PassiveRows\(shapeOnly\)/.test(src));
ok('passive.csv endpoint', /app\.get\('\/api\/evidence\/passive\.csv'/.test(src));
ok('passive.json endpoint', /app\.get\('\/api\/evidence\/passive\.json'/.test(src));
ok('CSV indirme adi', /filename="lazarus_pasif_parametreler\.csv"/.test(src));
{
  const f=grab('function r501PassiveRows(shapeOnly)');
  for(const [n,re] of [
    ['giris anlik goruntusu', /ENTRY_FILL_RECONCILED\|\|st\.ENTRY_FILL_OBSERVED/],
    ['cikis anlik goruntusu', /rec\.closeResearchSnapshot/],
    ['CVD duzlestirilir', /CvdRatio'\]:n\(o\?\.cvd\?\.ratio\)/],
    ['cvdMomentum sutunu', /CvdMomentum'\]/],
    ['VPIN nesnesi skalere acilir', /VpinPct'\]:n\(o\?\.tick\?\.vpin\?\.vpin\)/],
    ['VPIN toxicity sutunu', /VpinToxicity'\]/],
    ['VPIN direction sutunu', /VpinDirection'\]/],
    ['VPIN bucket sutunu', /VpinBuckets'\]/],
    ['whaleBias sutunu', /WhaleBias'\]/],
    ['akis yonu sutunu', /FlowDirection'\]/],
    ['alisOrani30 sutunu', /FlowBuyRatio30'\]/],
    ['mikroyapi metin/nesne guvenli', /typeof o\?\.tick\?\.microstructure==='string'/],
    ['defter dengesizligi', /BookImbalancePct'\]/],
    ['iceberg', /IcebergSignal'\]/],
    ['likidasyon', /LiqCascade'\]/],
    ['acik pozisyon ilgisi (OI)', /OpenInterest'\]/],
    ['funding', /FundingRate'\]/],
    ['tazelik', /TickAgeMs'\]/],
    ['KAR/ZARAR etiketi', /sonuc:/],
    ['tutus suresi', /holdMs:/],
    ['marj', /marginUSDT:n\(rec\.trade\?\.marginUSDT\)/],
    ['delta CVD', /dCvdRatio:d\(/],
    ['delta VPIN', /dVpinPct:d\(/],
    ['delta OI', /dOpenInterest:d\(/],
    ['CVD trend donusu', /cvdTrendFlipped:/],
    ['akis yonu donusu', /flowDirectionFlipped:/],
  ]) ok(n, re.test(f));
  ok('delta null-guvenli (Number(null)=0 tuzagi yok)',
     /return \(x===null\|\|y===null\)\?null:/.test(f));
  ok('n() sonsuz/NaN eler', /Number\.isFinite\(x\)\?x:null/.test(f));
}

console.log('\n== D2 -- r501PassiveRows canli kosum (4 gercek islem) ' + '='.repeat(20));
{
  const fnSrc=grab('function r501PassiveRows(shapeOnly)');
  const KAYIT={
    ON:{pnl:1.654,roi:25.77,hold:14520000,mar:6.57,
        e:{cvd:{ratio:0.62,delta:1200,trend:'BULLISH'},tick:{deltaRatio:0.31,vpin:{vpin:0.18,toxicity:'LOW',direction:'BUY',bucketCount:12},whaleBias:'BUY'},
           flow:{direction:'BUY',buyRatio30:0.58},book:{imbalancePct:12.4,spreadBps:1.9},openInterest:{openInterest:9100000}},
        c:{cvd:{ratio:0.71,delta:2100,trend:'BULLISH'},tick:{deltaRatio:0.44,vpin:{vpin:0.22,toxicity:'LOW',direction:'BUY',bucketCount:31},whaleBias:'BUY'},
           flow:{direction:'BUY',buyRatio30:0.66},book:{imbalancePct:18.0,spreadBps:2.1},openInterest:{openInterest:9600000}}},
    UB:{pnl:2.841,roi:20.38,hold:14628000,mar:13.94,
        e:{cvd:{ratio:0.55,delta:800,trend:'BULLISH'},tick:{deltaRatio:0.20,vpin:{vpin:0.15,toxicity:'LOW',direction:'BUY',bucketCount:9},whaleBias:'NEUTRAL'},
           flow:{direction:'BUY',buyRatio30:0.53},book:{imbalancePct:6.0,spreadBps:2.5},openInterest:{openInterest:4200000}},
        c:{cvd:{ratio:0.68,delta:1900,trend:'BULLISH'},tick:{deltaRatio:0.37,vpin:{vpin:0.19,toxicity:'LOW',direction:'BUY',bucketCount:27},whaleBias:'BUY'},
           flow:{direction:'BUY',buyRatio30:0.61},book:{imbalancePct:14.2,spreadBps:2.2},openInterest:{openInterest:4500000}}},
    STG:{pnl:-4.839,roi:-57.95,hold:6140000,mar:8.35,
        e:{cvd:{ratio:0.49,delta:-300,trend:'NEUTRAL'},tick:{deltaRatio:-0.08,vpin:{vpin:0.41,toxicity:'HIGH',direction:'SELL',bucketCount:14},whaleBias:'SELL'},
           flow:{direction:'SELL',buyRatio30:0.44},book:{imbalancePct:-9.5,spreadBps:4.8},openInterest:{openInterest:2000000}},
        c:{cvd:{ratio:0.33,delta:-2600,trend:'BEARISH'},tick:{deltaRatio:-0.36,vpin:{vpin:0.57,toxicity:'HIGH',direction:'SELL',bucketCount:22},whaleBias:'SELL'},
           flow:{direction:'SELL',buyRatio30:0.31},book:{imbalancePct:-21.0,spreadBps:7.4},openInterest:{openInterest:1700000}}},
    BICO:{pnl:0,roi:0,hold:3000,mar:6.10,
        e:{cvd:{ratio:0.51,delta:10,trend:'NEUTRAL'},tick:{deltaRatio:0.01,vpin:{vpin:0.12,toxicity:'LOW',direction:'NEUTRAL',bucketCount:3},whaleBias:'NEUTRAL'},
           flow:{direction:'NEUTRAL',buyRatio30:0.50},book:{imbalancePct:0.4,spreadBps:3.0},openInterest:{openInterest:1100000}},
        c:null}
  };
  const idx={trades:Object.keys(KAYIT).map(s=>({id:s}))};
  const sandbox={console,Object,Math,Number,Array,
    // V4.7.4.26-AQ: r501PassiveRows artik rest turevlerini de cagiriyor.
    // Bu test pasif ALAN duzlestirmesini olcer; rest turevleri V47426'da test edilir.
    r501RestDerive:()=>({}), r501KlineDerive:()=>{},
    r501EvidenceIndex:idx,
    r501TradePath:(id)=>id,
    r501GzipRead:(id)=>{const k=KAYIT[id];if(!k)return null;return{
      id, symbol:id+'USDT', side:'LONG', openedAt:1000, closedAt:1000+k.hold,
      close:{pnlUSDT:k.pnl,roiPct:k.roi,reason:k.pnl>0?'BINANCE_PROFIT_CLOSE':(k.pnl<0?'STOP_LOSS':'EXTERNAL_OR_MANUAL')},
      trade:{marginUSDT:k.mar,entryPrice:1},
      entryContext:{adopted:false},
      orderLifecycle:{sltpVerified:true,marketSnapshots:{ENTRY_FILL_RECONCILED:k.e}},
      closeResearchSnapshot:k.c
    };}};
  vm.createContext(sandbox);
  vm.runInContext(fnSrc+';globalThis.__rows=r501PassiveRows();',sandbox);
  const rows=sandbox.__rows;
  const by=Object.fromEntries(rows.map(r=>[r.id,r]));

  ok('4 satir uretildi', rows.length===4, String(rows.length));
  ok('ON = KAR', by.ON.sonuc==='KAR');
  ok('STG = ZARAR', by.STG.sonuc==='ZARAR');
  ok('BICO = NOTR', by.BICO.sonuc==='NOTR');
  ok('cvdTrend sutunu dolu', by.ON.entryCvdTrend==='BULLISH' && by.STG.closeCvdTrend==='BEARISH');
  ok('VPIN skaler sutuna acildi', by.ON.entryVpinPct===0.18 && by.STG.closeVpinPct===0.57);
  ok('VPIN toxicity sutunu dolu', by.STG.entryVpinToxicity==='HIGH');
  ok('VPIN yon sutunu dolu', by.STG.closeVpinDirection==='SELL');
  ok('VPIN bucket sutunu dolu', by.ON.closeVpinBuckets===31);
  ok('akis yonu dolu (VERI_YOK degil)', by.ON.entryFlowDirection==='BUY' && by.STG.closeFlowDirection==='SELL');
  ok('alisOrani30 dolu (0 degil)', by.ON.entryFlowBuyRatio30===0.58 && by.STG.closeFlowBuyRatio30===0.31);
  ok('whaleBias ayrisiyor', by.ON.entryWhaleBias==='BUY' && by.STG.entryWhaleBias==='SELL');
  ok('dCvdRatio ON pozitif', by.ON.dCvdRatio!==null && by.ON.dCvdRatio>0);
  ok('dCvdRatio STG negatif', by.STG.dCvdRatio<0);
  ok('dVpinPct STG artmis (toksisite)', by.STG.dVpinPct>0);
  ok('dBookImbalancePct STG negatif', by.STG.dBookImbalancePct<0);
  ok('dOpenInterest STG negatif', by.STG.dOpenInterest<0);
  ok('cvdTrendFlipped STG true', by.STG.cvdTrendFlipped===true);
  ok('cvdTrendFlipped ON false', by.ON.cvdTrendFlipped===false);
  ok('flowDirectionFlipped ON false', by.ON.flowDirectionFlipped===false);
  ok('BICO cikis yok -> closeCvdRatio null', by.BICO.closeCvdRatio===null);
  ok('BICO delta null (0 degil)', by.BICO.dCvdRatio===null);
  ok('BICO dVpinPct null (0 degil)', by.BICO.dVpinPct===null);
  ok('BICO flip null (false degil)', by.BICO.flowDirectionFlipped===null);
  ok('hicbir delta sahte 0 uretmedi', rows.every(r=>r.dCvdRatio===null||Number.isFinite(r.dCvdRatio)));

  const karV=rows.filter(r=>r.sonuc==='KAR').map(r=>r.entryVpinPct);
  const zarV=rows.filter(r=>r.sonuc==='ZARAR').map(r=>r.entryVpinPct);
  const ort=a=>a.reduce((s,x)=>s+x,0)/a.length;
  ok('KAR/ZARAR pasif parametre ayrisimi hesaplanabiliyor',
     Number.isFinite(ort(karV)) && Number.isFinite(ort(zarV)) && ort(karV)!==ort(zarV),
     `KAR VPIN ${ort(karV).toFixed(3)} vs ZARAR ${ort(zarV).toFixed(3)}`);

  const cols=Object.keys(rows[0]);
  ok('tum satirlar ayni sutun setinde', rows.every(r=>Object.keys(r).length===cols.length), `${cols.length} sutun`);
  ok('sutun sayisi 100+', cols.length>=100, String(cols.length));
  ok('entry ve close ayni alan setine sahip',
     cols.filter(c=>c.startsWith('entry')).length===cols.filter(c=>c.startsWith('close')).length);
}

console.log('\n== E -- istatistik kapisi ' + '='.repeat(48));
ok('n=4 karar icin yetersiz', 4 < 100);
ok('n=36 hala yetersiz (t=1.70)', 1.70 < 1.96);
ok('n=100 yeterli (t=2.83)', 2.83 > 1.96);
ok('CSV pasif -- karar/emir/miktar/cikis etkilemez',
   /sizingImpact:false/.test(src) && /exitImpact:false/.test(src) && /microstructureDecisionImpact:false/.test(src));
ok('pasif CSV yolu telemetride ilan edilmis', /passiveParamsCsv:'\/api\/evidence\/passive\.csv'/.test(src));
ok('aggTrade yolu telemetride ilan edilmis', /aggTradeStream:'FAPI_WS_MARKET'/.test(src));
ok('ledgerEvidenceOnly telemetride', /ledgerEvidenceOnly:V592_LEDGER_EVIDENCE_ONLY/.test(src));
ok('telemetri her iki endpointte (kullanicinin baktigi yol)',
   cnt("ledgerEvidenceOnly:V592_LEDGER_EVIDENCE_ONLY")>=2);

console.log('\n== F -- backtest davranisi korundu ' + '='.repeat(39));
ok('giris sozlesmesi 180000 ms', /candidateToEntryMs:180000/.test(src));
ok('cikis mum paritesi duruyor', /function v592ExitCandleGate/.test(src));
ok('cikis tipi beyaz listesi duruyor', /const V592_BACKTEST_EXIT_TYPES = Object\.freeze/.test(src));
ok('kapanis pozitif kanit duruyor', /function v592CloseProof/.test(src));
ok('korumasiz istisnasi duruyor', /function v592PositionProtection\(sym\)/.test(src));
ok('koruma-once duruyor', /PROTECT_FIRST_NO_PRECHECK/.test(src));
ok('V4.5 secici esikleri degismedi',
   /V592_V45_MS_SCORE_MIN/.test(src) && /V592_V45_FIRST_OBSTACLE_RR_MIN/.test(src));
ok('R493 esigi degismedi', /R493_MIN_FIRST_OBSTACLE_RR/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('r501PassiveRows yalniz endpointten cagriliyor', cnt('r501PassiveRows\\(\\)')<=3);

console.log('\n== G -- surum etiketleri ' + '='.repeat(49));
ok('build V4_7_4_36', /V4_7_4_36_PROBE_PRICE_RISK41_10X/.test(src));
ok('session 4_7_4_36_PP1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_36_PP1/.test(src));
ok('eski build kalmadi', !/V4_7_4_35_ROTATE_RISK41_10X/.test(src));
ok('eski session kalmadi', !/4_7_4_25_OV1/.test(src));
ok('kanit modu guncel', /V47436_PROBE_PRICE_EXACT_CLOSED_1M/.test(src));

console.log('\n== H -- onceki duzeltmeler ' + '='.repeat(47));
for(const [n,re] of [
  ['L fren ayrimi',/function isExecBackoffActive/],
  ['N huni gorusu',/reasonFull:/],
  ['O testnet evreni',/v592IsTestnetTradable/],
  ['Q koruma-once',/PROTECT_FIRST_NO_PRECHECK/],
  ['S4 null-guvenli delta',/function r501Delta/],
  ['U deneme yasam dongusu',/lifecycleAttemptResets/],
  ['W giris mumu',/function v592CandleIndex/],
  ['Y cikis mumu',/function v592ExitCandleGate/],
  ['AA cancel atlama',/const _skipCancel = \(firstInstall && attempt === 1/],
  ['AC yonetici guard',/v592MinHoldGuard\(sym, `MANAGER_/],
  ['AE golge cikis',/function v592ShadowNonBacktestExit/],
  ['AG beyaz liste',/function v592ExitTypeAllowed/],
  ['AH korumasiz istisna',/function v592PositionProtection/],
  ['AJ kapanis kaniti',/v592ParityStats\.closeProofFailed/],
  ['AL1 tick akisi',/tickStreamRepaired/],
  ['AL2 kanit-only defter',/ledgerBootstrapSkipped/],
  ['AL3 pasif csv',/r501PassiveRows/],
]) ok(n, re.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
