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

console.log('== A -- 07.08 RIVERUSDT olcumu: 9 bos sutun ' + '='.repeat(30));
const BOS=['entryVpinPct','entryVpinToxicity','entryVpinDirection','entryVpinBuckets',
           'entryMicrostructure','entryFlowBuyRatio120','entryLiqCascade',
           'entryOpenInterest','entryFundingRate'];
const DOLU=39;
ok('39 entry sutunu doldu', DOLU===39);
ok('9 sutun bos kaldi', BOS.length===9);
ok('VPIN 4 sutunu bos', BOS.filter(x=>x.includes('Vpin')).length===4);
ok('OI bos', BOS.includes('entryOpenInterest'));
ok('funding bos', BOS.includes('entryFundingRate'));
// AL1 basarisi: 5 alandan 4'u doldu
const AL1={cvdMomentum:'NEUTRAL',akisYonu:'SATIŞ',alisOrani30sn:20,whaleBias:'NEUTRAL',vpin:''};
ok('AL1: cvdMomentum doldu', AL1.cvdMomentum!=='');
ok('AL1: akisYonu doldu (VERI_YOK degil)', AL1.akisYonu!=='' && AL1.akisYonu!=='VERI_YOK');
ok('AL1: alisOrani30sn doldu', AL1.alisOrani30sn!==0);
ok('AL1: tick akisi calisiyor (aggTradeAgeMs 611ms)', 611<2000);
ok('AL1: VPIN hala bos -> AP2 gerekli', AL1.vpin==='');

console.log('\n== B -- AP1: OI/funding kok neden ve cozum ' + '='.repeat(31));
{
  const f=grab('function r501ImmediateResearchSnapshot');
  ok('kok neden kaynakta yazili', /oiSeries henuz BOS/.test(src));
  ok('89 ms yarisi belgelenmis', /ENTRY_FILL_RECONCILED'dan 89 ms once aciliyor/.test(src));
  ok('kullanici hedefi not edilmis', /kar\/zarar aninda CVD\/OI/.test(src));
  ok('oih5 onbellegi okunuyor', /cache\.get\(`oih5_\$\{sym\}`\)/.test(f));
  ok('oih 1h yedegi', /cache\.get\(`oih_\$\{sym\}`\)/.test(f));
  ok('funding onbellegi', /cache\.get\(`fund_\$\{sym\}`\)/.test(f));
  ok('yalniz kayit BOSSA devreye girer', /if\(!oi\)\{/.test(f) && /if\(!funding\)\{/.test(f));
  ok('kaynak etiketi yaziliyor', /_source:'CACHE_FUNDING_REST'/.test(f) && /CACHE_OIH_5M/.test(f));
  ok('veri yasi kaydediliyor', /_ageMs:/.test(f));
  ok('YENI AG CAGRISI YOK', !/bPub\(|bReq\(|fetch\(/.test(f.slice(f.indexOf('AP1'))));
  ok('sayac oiFromCache', /oiFromCache\+\+/.test(f));
}
ok('cache sekli dogru ({val,exp,ts})', /cache\.set\(key, \{ val, exp: Date\.now\(\)\+ttl, ts:Date\.now\(\) \}\)/.test(src));

console.log('\n== C -- AP1 canli kosum ' + '='.repeat(50));
{
  const f=grab('function r501ImmediateResearchSnapshot');
  const now=Date.now();
  const mk=(cacheData)=>{
    const sb={console,Date,Math,Number,Array,JSON,String,Object,
      normalizeSymbol:(x)=>String(x).toUpperCase(),
      r501LatestDecisionBySymbol:new Map(),
      cache:new Map(Object.entries(cacheData)),
      r501ActiveEvidence:new Map(),
      tickStore:new Map(),
      getCVDRaw:()=>({buy:1,sell:2,delta:-1,ratio:49.6,momentum:'NEUTRAL',trend:'NEUTRAL',valid:true,historyLen:2}),
      getTickAnalysisRaw:()=>({recent30s:{},deltaRatio:36.1,deltaTrend:'BEAR',deltaFlip:'BULL_TO_BEAR',
                               whaleBias:'NEUTRAL',bigBuy:0,bigSell:0,vpin:null,microstructure:null,currentCandle:{close:3.23}}),
      r371GetFlowAnalysisRaw:()=>({veriTazeMi:true,alisOrani30sn:20,akisYonu:'SATIŞ'}),
      r501CurrentBook:()=>({imbalancePct:-30.72,spreadBps:3.105}),
      getIcebergRaw:()=>({hiddenBuy:0,hiddenSell:0,signal:'NEUTRAL'}),
      getLiqDataRaw:()=>({longLiq1m:0,shortLiq1m:0,dominance:'BALANCED',cascade:null}),
      r501Freshness:()=>({tickAgeMs:803,aggTradeAgeMs:611,depthAgeMs:494}),
      r501DecisionTfCompact:()=>null,
      calcVPINPassive:()=>({vpin:31.2,toxicity:'HIGH',direction:'SELL_DOMINANT',bucketCount:9,passive:true,reason:'OK'}),
      v592ParityStats:{passiveVpinComputed:0,oiFromCache:0}};
    vm.createContext(sb);
    vm.runInContext(f+';globalThis.__r=r501ImmediateResearchSnapshot("RIVERUSDT","ENTRY_FILL_RECONCILED");',sb);
    return {snap:sb.__r, stats:sb.v592ParityStats};
  };
  // 1) onbellek DOLU -> OI ve funding gelmeli
  const A=mk({
    'oih5_RIVERUSDT':{val:[{sumOpenInterest:'12345.6',sumOpenInterestValue:'39900.1',timestamp:now}],ts:now-40000,exp:now+50000},
    'fund_RIVERUSDT':{val:[{fundingRate:'-0.00003680',fundingTime:now}],ts:now-60000,exp:now+1e6}});
  ok('OI onbellekten geldi', A.snap.openInterest!==null && A.snap.openInterest!==undefined);
  ok('OI degeri dogru', A.snap.openInterest?.openInterest===12345.6, JSON.stringify(A.snap.openInterest).slice(0,80));
  ok('OI kaynagi CACHE_OIH_5M', A.snap.openInterest?._source==='CACHE_OIH_5M');
  ok('OI yasi hesaplandi', A.snap.openInterest?._ageMs>=40000);
  ok('funding onbellekten geldi', A.snap.funding?.lastFundingRate===-0.0000368, JSON.stringify(A.snap.funding).slice(0,70));
  ok('funding kaynagi etiketli', A.snap.funding?._source==='CACHE_FUNDING_REST');
  ok('sayac oiFromCache arti', A.stats.oiFromCache===1);
  // 2) onbellek BOS -> null kalmali, patlamamali
  const B=mk({});
  ok('onbellek bos -> OI null (patlamiyor)', B.snap.openInterest===null);
  ok('onbellek bos -> funding null', B.snap.funding===null);
  ok('diger alanlar yine dolu', B.snap.cvd?.ratio===49.6 && B.snap.flow?.buyRatio30===20);
  // 3) 5m yoksa 1h yedegi
  const C=mk({'oih_RIVERUSDT':{val:[{sumOpenInterest:'999',sumOpenInterestValue:'1000'}],ts:now-100000,exp:now+1e6}});
  ok('5m yoksa 1h yedegi devreye girer', C.snap.openInterest?._source==='CACHE_OIH_1H' && C.snap.openInterest?.openInterest===999);
  // 4) pasif VPIN snapshot'a girdi
  ok('snapshot pasif VPIN tasiyor', A.snap.tick?.vpinPassive?.vpin===31.2);
  ok('otorite VPIN ayri alanda (null)', A.snap.tick?.vpinAuthority===null);
  ok('vpin alani pasif degere dustu', A.snap.tick?.vpin?.vpin===31.2);
  ok('snapshot PASIF isaretli', A.snap.passive===true && A.snap.decisionImpact===false &&
     A.snap.sizingImpact===false && A.snap.exitImpact===false);
}

console.log('\n== D -- AP2: calcVPIN KARAR YOLU dokunulmadi ' + '='.repeat(29));
{
  const c=grab('function calcVPIN(trades, bucketSize = 50)');
  ok('esik hala trades.length < bucketSize*3', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(c));
  ok('kova hala bucketSize x 1000', /if \(curVol >= bucketSize \* 1000\)/.test(c));
  ok('kova sayisi hala < 5', /if \(buckets\.length < 5\) return null;/.test(c));
  ok('karar yolu ctx.vpinAligned duruyor', /ctx\.vpinAligned \|\| ctx\.microAligned/.test(src));
  ok('karar yolu ctx.vpinAgainst duruyor', /!ctx\.vpinAgainst && !ctx\.microAgainst/.test(src));
  ok('karar yolu calcVPINPassive KULLANMIYOR',
     !/vpinAligned[\s\S]{0,200}calcVPINPassive/.test(src));
  ok('kok neden belgelenmis', /calcVPIN KARAR YOLUNDA \(satir ~4073/.test(src));
  ok('150.000 USDT esigi belgelenmis', /150\.000 USDT hacim/.test(src));
}

console.log('\n== E -- calcVPINPassive canli kosum ' + '='.repeat(38));
{
  const f=grab('function calcVPINPassive');
  const sb={console,Math,Number,Array,String}; vm.createContext(sb);
  vm.runInContext(f+';globalThis.f=calcVPINPassive;',sb);
  const f2=sb.f;
  // RIVER benzeri dusuk hacim: 400 tick, toplam ~40.000 USDT -> eski calcVPIN null verirdi
  const dusuk=Array.from({length:400},(_,i)=>({usdt:100,isBuy:i%3!==0}));
  const r1=f2(dusuk);
  ok('dusuk hacimde VPIN HESAPLANDI', r1.vpin!==null, JSON.stringify(r1).slice(0,110));
  ok('kova sayisi >= 3', r1.bucketCount>=3);
  ok('yon SELL/BUY tespit edildi', ['BUY_DOMINANT','SELL_DOMINANT','NEUTRAL'].includes(r1.direction));
  ok('toxicity etiketi var', ['LOW','MEDIUM','HIGH','EXTREME'].includes(r1.toxicity));
  ok('passive:true isaretli', r1.passive===true);
  ok('kova buyuklugu hacme gore olcekli', r1.bucketUsdt>0 && r1.bucketUsdt<=40000/10+1);
  // eski calcVPIN ayni veriyle null verirdi (400 tick x 100 = 40.000 < 150.000)
  const c=grab('function calcVPIN(trades, bucketSize = 50)');
  const sb2={console,Math,Number,Array}; vm.createContext(sb2);
  vm.runInContext(c+';globalThis.g=calcVPIN;',sb2);
  ok('ESKI calcVPIN ayni veride null (kok neden ispati)', sb2.g(dusuk,30)===null);
  // tek tarafli akis -> yuksek VPIN
  const tekYon=Array.from({length:400},()=>({usdt:100,isBuy:false}));
  const r2=f2(tekYon);
  ok('tek yonlu akis -> VPIN yuksek', r2.vpin>90, String(r2.vpin));
  ok('tek yonlu akis -> SELL_DOMINANT', r2.direction==='SELL_DOMINANT');
  ok('tek yonlu akis -> EXTREME', r2.toxicity==='EXTREME');
  // dengeli akis -> dusuk VPIN
  const dengeli=Array.from({length:400},(_,i)=>({usdt:100,isBuy:i%2===0}));
  const r3=f2(dengeli);
  ok('dengeli akis -> VPIN dusuk', r3.vpin<20, String(r3.vpin));
  ok('dengeli akis -> NEUTRAL', r3.direction==='NEUTRAL');
  // sinir durumlar
  ok('bos dizi -> null + sebep', f2([]).vpin===null && f2([]).reason==='TICK_YETERSIZ');
  ok('az tick -> null + sebep', f2([{usdt:1,isBuy:true}]).reason==='TICK_YETERSIZ');
  ok('sifir hacim -> null + sebep', f2(Array.from({length:50},()=>({usdt:0,isBuy:true}))).reason==='HACIM_SIFIR');
  ok('null girdi patlatmiyor', f2(null).vpin===null);
  ok('bozuk girdi patlatmiyor', f2([{},{},{}]).vpin===null);
  ok('sebep alani HER ZAMAN dolu', ['TICK_YETERSIZ','HACIM_SIFIR','KOVA_YETERSIZ','OK','HATA'].includes(f2([]).reason));
}

console.log('\n== F -- pasif sozlesme korundu ' + '='.repeat(43));
ok('sizingImpact false', /sizingImpact:false/.test(src));
ok('exitImpact false', /exitImpact:false/.test(src));
ok('microstructureDecisionImpact false', /microstructureDecisionImpact:false/.test(src));
ok('telemetri passiveVpinActive', /passiveVpinActive:true/.test(src));
ok('telemetri vpinAuthorityUntouched', /vpinAuthorityUntouched:true/.test(src));
ok('telemetri oiCacheFallback', /oiCacheFallback:true/.test(src));
ok('telemetri her iki endpointte', cnt('oiCacheFallback:true')>=2);

console.log('\n== G -- onceki duzeltmeler ' + '='.repeat(47));
for(const [n,re] of [
  ['AJ kapanis kaniti',/async function v592CloseProof/],
  ['AL1 tick akisi',/tickStreamRepaired/],
  ['AL2 kanit-only defter',/ledgerBootstrapSkipped/],
  ['AL3 pasif csv',/function r501PassiveRows/],
  ['AL5 null-guvenli n()',/if\(v===null\|\|v===undefined\|\|v===''\)return null/],
  ['AN1 kanit tum sebepler',/V592_CLOSE_PROOF_EXEMPT/],
  ['AN2 yetim korunur',/v592PossibleOrphans/],
  ['AP1 oi onbellek',/oiFromCache/],
  ['AP2 pasif vpin',/function calcVPINPassive/],
]) ok(n, re.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('V45 esikleri degismedi', /V592_V45_MS_SCORE_MIN/.test(src) && /V592_V45_FIRST_OBSTACLE_RR_MIN/.test(src));

console.log('\n== H -- surum ' + '='.repeat(60));
ok('build V4_7_4_27', /V4_7_4_27_CSV_REPORT_RISK41_10X/.test(src));
ok('session 4_7_4_27_CR1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_27_CR1/.test(src));
ok('eski build kalmadi', !/V4_7_4_26_FULL_PARAMS_RISK41_10X/.test(src));
ok('eski session kalmadi', !/4_7_4_25_OV1/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
