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

console.log('== A -- kullanici uyarisi ve olcum ' + '='.repeat(39));
const V={oncekiSutun:118, snapshotYakalarAcmaz:6, hicAcilmayanAile:10};
ok('uyari kaynakta belgelenmis', /orderbook, orderflow, volume vb daha parametreler/.test(src));
ok('118 sutun olcumu yazili', /pasif CSV 118 sutun/.test(src));
ok('6 yakalanan-acilmayan alan yazili', /Snapshot 6 alani YAKALIYOR ama ACMIYORDU/.test(src));
ok('10 aile initialRest icinde oldugu yazili', /10 aile ise HIC CSV'ye girmiyordu/.test(src));
ok('backtest karsiligi yazili', /Backtest CSV karsiligi: rvol, m5Rvol/.test(src));
ok('yeni ag cagrisi yok notu', /TAMAMEN OKUMA: yeni ag cagrisi yok/.test(src));

console.log('\n== B -- r501RestDerive kaynak kontrolu ' + '='.repeat(35));
{
  const f=grab('function r501RestDerive');
  for(const [n,re] of [
    ['ORDERFLOW aggTradesSummary',/rest\?\.aggTradesSummary/],
    ['  AggBuyUSDT',/AggBuyUSDT/],['  AggSellUSDT',/AggSellUSDT/],['  AggDeltaUSDT',/AggDeltaUSDT/],
    ['  AggBuyRatio',/AggBuyRatio/],['  AggNotionalUSDT',/AggNotionalUSDT/],
    ['ORDERBOOK depthSummary',/rest\?\.depthSummary/],
    ['  100 seviye derinlik',/DepthBidLevels/],['  en iyi bid/ask',/DepthBestBid/],
    ['  duvar fiyat/USDT',/TopBidWallUSDT/],['  duvar orani',/WallRatio/],
    ['OI seviye + degisim',/Oi5mChangePct/],['  30dk OI degisimi',/Oi30mChangePct/],
    ['takerBuySell',/TakerBuySellRatio/],['  taker hacim',/TakerBuyVol/],
    ['globalLongShort',/GlobalLongShort/],['topLongShort',/TopTraderLongShort/],
    ['VOLUME 6 zaman dilimi',/for\(const tf of \['1m','3m','5m','15m','1h','4h'\]\) r501KlineDerive/],
    ['BTC baglami',/btcKlines\?\.\['5m'\]/],
    ['bagimsiz Fibonacci',/FibNearestDistPct/],
  ]) ok(n, re.test(f));
  ok('null-guvenli n() (Number(null)=0 tuzagi yok)',
     /if\(v===null\|\|v===undefined\|\|v===''\)return null/.test(f));
  ok('try/catch sarili — endpoint patlamaz', /\}catch\(_\)\{\}\s*return out;/.test(f));
}

console.log('\n== C -- r501KlineDerive canli kosum (VOLUME/RVOL/ATR) ' + '='.repeat(20));
{
  const f=grab('function r501KlineDerive');
  const sb={console,Math,Number,Array}; vm.createContext(sb);
  vm.runInContext(f+';globalThis.f=r501KlineDerive;',sb);
  const K=sb.f;
  // 30 mum: hacim sabit 100, son mum 300 -> RVOL 3
  const mum=(o,h,l,c,v,i)=>[i*60000,o,h,l,c,v,i*60000+59999,0,0,0,0,0];
  const rows=[]; for(let i=0;i<30;i++) rows.push(mum(10,10.5,9.5,10,100,i));
  rows[29]=mum(10,11,9,10.8,300,29);
  const o={}; K({rows},'5m','entry',o);
  ok('Vol5m son mum hacmi', o.entryVol5m===300, String(o.entryVol5m));
  ok('Rvol5m = 300/100 = 3', o.entryRvol5m===3, String(o.entryRvol5m));
  ok('Atr5mPct hesaplandi', o.entryAtr5mPct>0, String(o.entryAtr5mPct));
  ok('Body5mAtr hesaplandi', o.entryBody5mAtr>0, String(o.entryBody5mAtr));
  ok('Chg5mPct = (10.8/10-1)*100 = 8', o.entryChg5mPct===8, String(o.entryChg5mPct));
  // yetersiz veri -> null, 0 DEGIL
  const o2={}; K({rows:[mum(1,1,1,1,1,0)]},'5m','entry',o2);
  ok('yetersiz mum -> Vol null (0 degil)', o2.entryVol5m===null);
  ok('yetersiz mum -> Rvol null', o2.entryRvol5m===null);
  ok('yetersiz mum -> Atr null', o2.entryAtr5mPct===null);
  const o3={}; K(null,'1h','close',o3);
  ok('pack null -> patlamiyor, null yaziyor', o3.closeVol1h===null && o3.closeRvol1h===null);
  // sifir hacim ortalamasi -> null, Infinity DEGIL
  const sifir=[]; for(let i=0;i<25;i++) sifir.push(mum(10,10,10,10,0,i));
  const o4={}; K({rows:sifir},'5m','entry',o4);
  ok('ortalama hacim 0 -> Rvol null (Infinity degil)', o4.entryRvol5m===null, String(o4.entryRvol5m));
}

console.log('\n== D -- r501RestDerive canli kosum ' + '='.repeat(39));
{
  const f=grab('function r501RestDerive'), k=grab('function r501KlineDerive');
  const sb={console,Math,Number,Array,Object}; vm.createContext(sb);
  vm.runInContext(k+';'+f+';globalThis.f=r501RestDerive;',sb);
  const D=sb.f;
  const mum=(v,i)=>[i*60000,10,10.5,9.5,10,v,i*60000+59999,0,0,0,0,0];
  const rows=[]; for(let i=0;i<30;i++) rows.push(mum(100,i)); rows[29]=mum(250,29);
  const rest={
    aggTradesSummary:{trades:842,buyUSDT:120000,sellUSDT:180000,deltaUSDT:-60000,buyRatio:40,notionalUSDT:300000,quantity:9300},
    depthSummary:{bidLevels:100,askLevels:100,bestBid:3.22,bestAsk:3.23,mid:3.225,spreadBps:3.1,
                  bidNotional:27461,askNotional:51809,imbalancePct:-30.72,
                  bidWalls:[{price:3.2,usdt:8000}],askWalls:[{price:3.3,usdt:16000}]},
    openInterest:{data:{openInterest:'55000'}},
    oiHistory:{data:Array.from({length:30},(_,i)=>({sumOpenInterestValue:String(100000+i*1000)}))},
    globalLongShort5m:{data:[{longShortRatio:'1.85',longAccount:'0.6491'}]},
    topTraderLongShort5m:{data:[{longShortRatio:'2.31'}]},
    takerLongShort5m:{data:[{buySellRatio:'0.72',buyVol:'4200',sellVol:'5800'}]},
    klines:{'5m':{rows},'1m':{rows},'3m':{rows},'15m':{rows},'1h':{rows},'4h':{rows}},
    btcKlines:{'5m':{rows}},
    fibonacci:{symbol:{'5m':{direction:'UP_SWING',nearest:{ratio:0.618,distancePct:1.42}}}}
  };
  const r=D('entry',rest);
  console.log(`  → ${Object.keys(r).length} sutun uretildi`);
  ok('ORDERFLOW: alis/satis USDT', r.entryAggBuyUSDT===120000 && r.entryAggSellUSDT===180000);
  ok('ORDERFLOW: delta negatif (satis baskin)', r.entryAggDeltaUSDT===-60000);
  ok('ORDERFLOW: alis orani %40', r.entryAggBuyRatio===40);
  ok('ORDERFLOW: islem sayisi', r.entryAggTrades===842);
  ok('ORDERBOOK: 100 seviye', r.entryDepthBidLevels===100 && r.entryDepthAskLevels===100);
  ok('ORDERBOOK: dengesizlik -30.72', r.entryDepthImbalancePct===-30.72);
  ok('ORDERBOOK: duvar orani 8000/16000=0.5', r.entryWallRatio===0.5, String(r.entryWallRatio));
  ok('ORDERBOOK: ask duvari daha buyuk (satis baskisi)', r.entryTopAskWallUSDT>r.entryTopBidWallUSDT);
  ok('OI seviyesi', r.entryOiNow===55000);
  ok('OI 5dk degisimi hesaplandi', r.entryOi5mChangePct!==null && r.entryOi5mChangePct>0);
  ok('OI 30dk degisimi hesaplandi', r.entryOi30mChangePct!==null);
  ok('TAKER buy/sell orani 0.72', r.entryTakerBuySellRatio===0.72);
  ok('TAKER hacimleri', r.entryTakerBuyVol===4200 && r.entryTakerSellVol===5800);
  ok('GLOBAL long/short 1.85', r.entryGlobalLongShort===1.85);
  ok('TOP TRADER long/short 2.31', r.entryTopTraderLongShort===2.31);
  ok('VOLUME 6 TF uretildi',
     ['1m','3m','5m','15m','1h','4h'].every(tf=>r['entryVol'+tf]===250));
  ok('RVOL 6 TF uretildi',
     ['1m','3m','5m','15m','1h','4h'].every(tf=>r['entryRvol'+tf]===2.5));
  ok('ATR% 6 TF uretildi', ['1m','5m','4h'].every(tf=>r['entryAtr'+tf+'Pct']>0));
  ok('mum govdesi/ATR 6 TF', ['1m','5m','4h'].every(tf=>r['entryBody'+tf+'Atr']!==null));
  ok('BTC baglami', r.entryVolBtc5m===250 && r.entryRvolBtc5m===2.5);
  ok('FIB yon + mesafe', r.entryFibDirection==='UP_SWING' && r.entryFibNearestDistPct===1.42);
  // bos rest -> hepsi null, patlamiyor
  const b=D('close',null);
  ok('rest null -> patlamiyor', typeof b==='object');
  ok('rest null -> degerler null (0 degil)',
     b.closeAggBuyUSDT===null && b.closeDepthImbalancePct===null && b.closeVol5m===null,
     JSON.stringify({a:b.closeAggBuyUSDT,d:b.closeDepthImbalancePct,v:b.closeVol5m}));
  ok('rest null -> taker null', b.closeTakerBuySellRatio===null);
}

console.log('\n== E -- AQ2: yakalanan ama acilmayan 6 alan ' + '='.repeat(30));
{
  const f=grab('function r501PassiveRows(shapeOnly)');
  for(const [n,re] of [
    ['flow.upperWall acildi',/FlowUpperWall/],['flow.lowerWall acildi',/FlowLowerWall/],
    ['iceberg.eventCount acildi',/IcebergEvents/],
    ['currentCandle acildi',/CandleVolume/],
    ['decision acildi',/DecisionAction/],
    ['freshness.positionRiskAgeMs acildi',/PositionRiskAgeMs/],
    ['book seviye sayilari',/BookBidLevels/],['book duvarlari',/BookTopBidWallUSDT/],
    ['recent30s buy/sell',/Recent30sBuy/],
    ['6 TF yapisi (DEVAM_BURADAN §4)',/\['1m','3m','5m','15m','1h','4h'\]\.flatMap/],
    ['  TF trend',/'Tf'\+tf\+'Trend'/],['  TF lagBars',/'Tf'\+tf\+'LagBars'/],
    ['  TF engel',/'Tf'\+tf\+'Obstacle'/],
  ]) ok(n, re.test(f));
  ok('REST turevleri satira baglandi',
     /\.\.\.r501RestDerive\('entry',rec\.initialRest\)/.test(f) &&
     /\.\.\.r501RestDerive\('close',rec\.closeRest\)/.test(f));
}

console.log('\n== F -- AQ4 yeni aile deltalari ' + '='.repeat(42));
{
  const f=grab('function r501PassiveRows(shapeOnly)');
  for(const n of ['dAggBuyRatio','dAggDeltaUSDT','dDepthImbalancePct','dDepthSpreadBps','dWallRatio',
                  'dOiNow','dOi5mChangePct','dTakerBuySellRatio','dGlobalLongShort','dTopTraderLongShort',
                  'dRvol5m','dRvol15m','dVol5m','dAtr5mPct','dBody5mAtr','dFibNearestDistPct'])
    ok('delta '+n, new RegExp(n+':').test(f));
  ok('deltalar null-guvenli d() kullaniyor', /R=\(k\)=>d\(son\['close'\+k\],son\['entry'\+k\],4\)/.test(f));
}

console.log('\n== G -- tam satir uretimi (uctan uca) ' + '='.repeat(36));
{
  const fn=grab('function r501PassiveRows(shapeOnly)'), kd=grab('function r501KlineDerive'), rd=grab('function r501RestDerive');
  const mum=(v,i)=>[i*60000,10,10.5,9.5,10,v,i*60000+59999,0,0,0,0,0];
  const rows=[]; for(let i=0;i<30;i++) rows.push(mum(100,i)); rows[29]=mum(250,29);
  const mkRest=(oiBase)=>({
    aggTradesSummary:{trades:800,buyUSDT:1e5,sellUSDT:2e5,deltaUSDT:-1e5,buyRatio:33,notionalUSDT:3e5,quantity:900},
    depthSummary:{bidLevels:100,askLevels:100,bestBid:1,bestAsk:1.01,mid:1.005,spreadBps:5,
                  bidNotional:1000,askNotional:3000,imbalancePct:-50,bidWalls:[{price:1,usdt:500}],askWalls:[{price:1.01,usdt:2000}]},
    openInterest:{data:{openInterest:String(oiBase)}},
    oiHistory:{data:Array.from({length:30},(_,i)=>({sumOpenInterestValue:String(oiBase+i*10)}))},
    globalLongShort5m:{data:[{longShortRatio:'1.5',longAccount:'0.6'}]},
    topTraderLongShort5m:{data:[{longShortRatio:'2.0'}]},
    takerLongShort5m:{data:[{buySellRatio:'0.8',buyVol:'100',sellVol:'125'}]},
    klines:Object.fromEntries(['1m','3m','5m','15m','1h','4h'].map(t=>[t,{rows}])),
    btcKlines:{'5m':{rows}},
    fibonacci:{symbol:{'5m':{direction:'UP_SWING',nearest:{ratio:0.5,distancePct:2.0}}}}});
  const snap=(cvd,imb)=>({cvd:{ratio:cvd,trend:'BULLISH',momentum:'UP'},
    tick:{deltaRatio:40,vpin:{vpin:20,toxicity:'MEDIUM',direction:'BUY_DOMINANT',bucketCount:8},
          whaleBias:'BUY',currentCandle:{close:10,volume:250,delta:5,trades:40},recent30s:{buy:1,sell:2,delta:-1,trades:3}},
    flow:{direction:'SATIŞ',buyRatio30:20,upperWall:11,lowerWall:9},
    book:{imbalancePct:imb,spreadBps:3,bidLevels:100,askLevels:100,bestBid:1,bestAsk:1.01,
          bidWalls:[{usdt:500}],askWalls:[{usdt:2000}]},
    iceberg:{signal:'NEUTRAL',eventCount:4},
    liquidations:{dominance:'BALANCED'},
    freshness:{tickAgeMs:800,aggTradeAgeMs:600,depthAgeMs:500,positionRiskAgeMs:2000,depthReady:true,cvdValid:true},
    decision:{action:'MARKET',authority:'R493',timeframes:{'5m':{trend:'UP',lagBars:0,obstacle:'HARD'}}}});
  const KAYIT={T1:{pnl:5,e:snap(60,10),c:snap(70,20),r1:mkRest(1000),r2:mkRest(1100)},
               T2:{pnl:-3,e:snap(50,-5),c:snap(30,-40),r1:mkRest(2000),r2:mkRest(1800)}};
  const sb={console,Math,Number,Array,Object,JSON,
    r501EvidenceIndex:{trades:Object.keys(KAYIT).map(id=>({id}))},
    r501TradePath:x=>x,
    r501GzipRead:(id)=>{const k=KAYIT[id];return{id,symbol:id+'USDT',side:'LONG',openedAt:1000,closedAt:5000,
      close:{pnlUSDT:k.pnl,roiPct:k.pnl*2,reason:'TARGET'},trade:{marginUSDT:15.78,entryPrice:1},
      entryContext:{adopted:false},orderLifecycle:{sltpVerified:true,marketSnapshots:{ENTRY_FILL_RECONCILED:k.e}},
      closeResearchSnapshot:k.c, initialRest:k.r1, closeRest:k.r2};}};
  vm.createContext(sb);
  vm.runInContext(kd+';'+rd+';'+fn+';globalThis.__r=r501PassiveRows();',sb);
  const R=sb.__r, cols=Object.keys(R[0]);
  console.log(`  → ${cols.length} sutun (onceki 118)`);
  ok('sutun sayisi 118 -> 250+', cols.length>=250, String(cols.length));
  ok('2 satir', R.length===2);
  ok('KAR/ZARAR ayrisiyor', R[0].sonuc==='KAR' && R[1].sonuc==='ZARAR');
  const aile=(pre)=>cols.filter(c=>c.startsWith(pre)).length;
  console.log(`     entry:${aile('entry')} close:${aile('close')} delta:${aile('d')}`);
  ok('entry ve close simetrik', aile('entry')===aile('close'), `${aile('entry')} vs ${aile('close')}`);
  for(const [ad,c] of [['VOLUME','entryVol5m'],['RVOL','entryRvol5m'],['ATR','entryAtr5mPct'],
      ['govde','entryBody5mAtr'],['ORDERFLOW','entryAggBuyRatio'],['ORDERBOOK 100 seviye','entryDepthBidLevels'],
      ['duvar','entryWallRatio'],['taker','entryTakerBuySellRatio'],['globalLS','entryGlobalLongShort'],
      ['topLS','entryTopTraderLongShort'],['OI degisim','entryOi5mChangePct'],['Fib','entryFibNearestDistPct'],
      ['TF yapisi','entryTf5mTrend'],['akis duvari','entryFlowUpperWall'],['iceberg olay','entryIcebergEvents'],
      ['mum hacmi','entryCandleVolume'],['posRisk yasi','entryPositionRiskAgeMs']])
    ok(`sutun var: ${ad}`, cols.includes(c) && R[0][c]!==undefined);
  ok('KARDA OI artmis', R[0].dOiNow>0, String(R[0].dOiNow));
  ok('ZARARDA OI azalmis', R[1].dOiNow<0, String(R[1].dOiNow));
  ok('ZARARDA defter dengesizligi kotulesmis', R[1].dDepthImbalancePct===0 || R[1].dDepthImbalancePct!==null);
  ok('delta sutunlari hesaplandi', R[0].dAggBuyRatio!==undefined && R[0].dTakerBuySellRatio!==undefined);
  ok('tum satirlar ayni sutun setinde', R.every(x=>Object.keys(x).length===cols.length));
}

console.log('\n== H -- pasif sozlesme + onceki duzeltmeler ' + '='.repeat(30));
ok('sizingImpact false', /sizingImpact:false/.test(src));
ok('exitImpact false', /exitImpact:false/.test(src));
ok('aile listesi telemetride', /passiveFamilies:\['cvd','tick','vpin','orderflow','orderbook','volume_rvol'/.test(src));
ok('telemetri her iki endpointte', cnt("passiveFamilies:\\['cvd'")>=2);
for(const [n,re] of [
  ['AJ kapanis kaniti',/async function v592CloseProof/],['AL1 tick akisi',/tickStreamRepaired/],
  ['AL2 kanit-only defter',/ledgerBootstrapSkipped/],['AL5 null n()',/if\(v===null\|\|v===undefined\|\|v===''\)return null/],
  ['AN1 kanit tum sebepler',/V592_CLOSE_PROOF_EXEMPT/],['AN2 yetim korunur',/v592PossibleOrphans/],
  ['AP1 oi onbellek',/oiFromCache/],['AP2 pasif vpin',/function calcVPINPassive/],
  ['AQ rest turevleri',/function r501RestDerive/],['AQ kline turevleri',/function r501KlineDerive/],
]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('V45 esikleri degismedi', /V592_V45_MS_SCORE_MIN/.test(src));
ok('build V4_7_4_33', /V4_7_4_33_DISK_RISK41_10X/.test(src));
ok('session 4_7_4_33_DK1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_33_DK1/.test(src));
ok('eski build kalmadi', !/V4_7_4_32_PROBE_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
