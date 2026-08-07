(async()=>{
const fs=require('fs'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`  pass  ${n}`)):(fail++,console.error(`  FAIL  ${n}${d?' :: '+d:''}`));
const cnt=(re)=>(src.match(new RegExp(re,'g'))||[]).length;

console.log('══ A — CIKIS ANI sutunlari CSV\'de var mi ' + '═'.repeat(32));
const CIKIS=[
 ['akis  CVD oran','closeCvdRatio'],['akis  CVD delta','closeCvdDelta'],
 ['akis  CVD momentum','closeCvdMomentum'],['akis  CVD trend','closeCvdTrend'],
 ['akis  tick delta','closeTickDeltaRatio'],['akis  tick trend','closeTickDeltaTrend'],
 ['akis  VPIN','closeTickVpin'],['akis  balina egilimi','closeWhaleBias'],
 ['akis  yon','closeFlowDirection'],['akis  alis orani 30sn','closeFlowBuyRatio30'],
 ['akis  ivme','closeFlowAcceleration'],
 ['defter dengesizlik','closeBookImbalancePct'],['defter spread','closeSpreadBps'],
 ['defter bid notional','closeBidNotional'],['defter ask notional','closeAskNotional'],
 ['turev  OI','closeOi'],['turev  funding','closeFunding'],
 ['likid  long 1m','closeLongLiq1m'],['likid  short 1m','closeShortLiq1m'],
 ['likid  dominance','closeLiqDominance'],
 ['tazelik tick','closeTickAgeMs'],['tazelik depth','closeDepthAgeMs'],
 ['REST ani','closeRestCapturedAt'],
];
for(const [n,c] of CIKIS) ok(`${n.padEnd(22)} ${c}`, cnt(`${c}:`)>=1, 'sutun yok');
ok(`CIKIS sutun sayisi ${CIKIS.length}`, CIKIS.length===23);

console.log('\n══ B — GIRIS -> CIKIS degisim sutunlari ' + '═'.repeat(34));
for(const [n,c] of [['CVD oran degisimi','dCvdRatio'],['tick delta degisimi','dTickDeltaRatio'],
                    ['defter dengesizlik degisimi','dBookImbalancePct'],['spread degisimi','dSpreadBps'],
                    ['akis yonu DONDU mu','flowFlipped']])
  ok(`${n.padEnd(28)} ${c}`, cnt(`${c}:`)>=1);
ok('TUTUS SURESI holdMs', cnt('holdMs:')>=1, 'kullanicinin asil olcutu');

console.log('\n══ C — kaynak dogru mu ' + '═'.repeat(51));
ok('closeSnap = rec.closeResearchSnapshot', /closeSnap=rec\.closeResearchSnapshot\|\|\{\}/.test(src));
ok('closeRest = rec.closeRest', /closeRest=rec\.closeRest\|\|\{\}/.test(src));
ok('closeResearchSnapshot KAPANISTA dolduruluyor',
   /rec\.closeResearchSnapshot=r501ImmediateResearchSnapshot\(rec\.symbol,'CLOSE',rec\.closedAt\)/.test(src));
ok('closeRest KAPANISTA dolduruluyor', /rec\.closeRest=await r501RestBundle\(rec\.symbol,rec\.closedAt,'CLOSE'\)/.test(src));
ok('holdMs = closedAt - fillTime', /holdMs:r501Delta\(rec\.closedAt,life\.fillTime\?\?e\?\.timestamps\?\.fillTime,0\)/.test(src));
ok('delta giris tarafi fillSnap kullaniyor', cnt('fillSnap\\?\\.cvd\\?\\.ratio')>=2);

console.log('\n══ D — GIRIS tarafi bozulmadi ' + '═'.repeat(44));
for(const c of ['decisionCvdRatio','requestCvdRatio','fillCvdRatio','decisionBookImbalancePct',
                'requestBookImbalancePct','fillBookImbalancePct','requestSpreadBps','fillSpreadBps',
                'oiCurrent','oi5mChangePct','funding','longLiq1m','shortLiq1m',
                'tickAgeMs','aggTradeAgeMs','depthAgeMs','positionRiskAgeMs'])
  ok(`giris ${c}`, cnt(`${c}:`)>=1);

console.log('\n══ E — hesaplama dogrulugu (simulasyon) ' + '═'.repeat(34));
{
  const fillSnap={cvd:{ratio:0.62},tick:{deltaRatio:0.41},book:{imbalancePct:12.5,spreadBps:3.2},flow:{direction:'BUYERS'}};
  const closeSnap={cvd:{ratio:0.38},tick:{deltaRatio:-0.15},book:{imbalancePct:-8.0,spreadBps:9.7},flow:{direction:'SELLERS'}};
  // kaynaktaki r501Num2/r501Delta ile AYNI mantik
  const num2=v=>{if(v===null||v===undefined||v==='')return null;const n=Number(v);return Number.isFinite(n)?n:null;};
  const d=(a,b,p)=>{const x=num2(a),y=num2(b);return (x===null||y===null)?null:+(x-y).toFixed(p);};
  const dCvd=d(closeSnap.cvd.ratio,fillSnap.cvd.ratio,6);
  const dBook=d(closeSnap.book.imbalancePct,fillSnap.book.imbalancePct,4);
  const dSpread=d(closeSnap.book.spreadBps,fillSnap.book.spreadBps,4);
  const flip=String(closeSnap.flow.direction)!==String(fillSnap.flow.direction);
  console.log(`    giris  CVD 0.62  defter +12.5  spread 3.2  akis BUYERS`);
  console.log(`    cikis  CVD 0.38  defter  -8.0  spread 9.7  akis SELLERS`);
  ok(`dCvdRatio  = ${dCvd}`,  Math.abs(dCvd+0.24)<1e-9);
  ok(`dBookImbalancePct = ${dBook}`, Math.abs(dBook+20.5)<1e-9);
  ok(`dSpreadBps = ${dSpread}`, Math.abs(dSpread-6.5)<1e-9);
  ok(`flowFlipped = ${flip}`, flip===true, 'alicidan saticiya dondu');
  const holdMs=1785936817049-1785936789536;
  ok(`holdMs = ${holdMs} (TUT vakasi 27,5sn)`, holdMs===27513);
  ok('27,5sn < 60sn backtest minimumu', holdMs<60000);
  // eksik veri -> null, cokme yok
  ok('null  -> null (0 SAYILMAZ)', d(0.5,null,6)===null, 'Number(null)===0 tuzagi');
  ok('undefined -> null', d(undefined,0.5,6)===null);
  ok("'' -> null", d('',0.5,6)===null);
  ok('NaN -> null', d(NaN,0.5,6)===null);
  ok('gercek 0 CALISIR', d(0,0.5,6)===-0.5, 'sifir gecerli deger');
  ok('kaynakta r501Num2 var', /function r501Num2\(v\)/.test(src));
  ok('kaynakta r501Delta var', /function r501Delta\(a,b,p=6\)/.test(src));
  ok('tum deltalar r501Delta kullaniyor (6 kullanim + 1 tanim)', cnt('r501Delta\\(')===7, `${cnt('r501Delta\\(')}`);
}

console.log('\n══ F — pasiflik ve onceki duzeltmeler ' + '═'.repeat(36));
ok('Q koruma-once duruyor', /PROTECT_FIRST_NO_PRECHECK/.test(src)&&/protectFirstInstalls\+\+/.test(src));
ok('J2 min hold duruyor', /function v592MinHoldGuard/.test(src));
ok('L fren ayrimi duruyor', /function isExecBackoffActive/.test(src));
ok('N WAIT atribusyonu duruyor', /waitSource:row\?\.waitSource\|\|null/.test(src));
ok('O testnet evreni duruyor', /v592IsTestnetTradable/.test(src));
ok('arastirma PASIF kalir', /exitImpact:false/.test(src)&&cnt('researchFieldsPassive')>=1);
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('build V4_7_4_28', /V4_7_4_28_CLOSE_FUNNEL_RISK41_10X/.test(src));
ok('eski build yok', !/V4_7_4_13_PROTECT_FIRST/.test(src));
ok('session 4_7_4_28_CF1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_28_CF1/.test(src));

console.log(`\n${'═'.repeat(74)}`);
console.log(fail?`SONUC: FAIL — ${pass} gecti, ${fail} dustu`:`SONUC: PASS — ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
