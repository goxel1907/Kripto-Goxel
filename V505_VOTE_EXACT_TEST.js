// V5.0.5 — R495 OYU: DAVRANIS TESTI (dizge degil, FONKSIYON CALISTIRILIR)
// Backtestin 1.461 kabul edilmis sinyali uzerinde uc test yapildi:
//  1) karar mantigi 1461/1461 uyusuyor
//  2) takerRatio ile oy arasinda iliski yok (medyan farki +0,0057)
//  3) votes=3 olan 140 sinyalin 36'sinda takerRatio<0,50 (en dusuk 0,2680)
// -> backtestin oyu yalnizca close>open idi.
const fs=require('fs'),path=require('path'),vm=require('vm');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`  pass  ${n}`)):(fail++,console.error(`  FAIL  ${n}${d?' :: '+d:''}`));

// ── R495 oy motorunu kaynaktan cikar ve CALISTIR ──
const i=src.indexOf('function parseBinance1m');
const j=src.indexOf("code: 'R495_CLOSED_1M_NOT_ACCEPTED'", src.indexOf('function evaluateClosed1m'));
const k=src.indexOf('\n}', j)+2;
const kesit=`function n(v){const x=Number(v);return Number.isFinite(x)?x:0;}\n`+src.slice(i,k);
const ctx={module:{},console};
vm.createContext(ctx);
vm.runInContext(kesit+'\nthis.evaluateClosed1m=evaluateClosed1m;',ctx);
const ev=ctx.evaluateClosed1m;
ok('oy motoru kaynaktan calistirilabildi', typeof ev==='function');

// ── sentetik 1m mum uretici (Binance kline dizisi formati) ──
const T0=1786000000000;
function mum(idx,{yukselen,taker,kapanis}){
  const o=100, c=kapanis!==undefined?kapanis:(yukselen?101:99), qv=1000, tbq=qv*taker;
  return [T0+idx*60000, String(o), '102', '98', String(c), '10', T0+(idx+1)*60000-1, String(qv), 100, String(tbq), '0', '0'];
}
const calistir=(barlar,opt={})=>ev({rawBars:barlar, candidateTs:T0, candidatePrice:100, atr:5,
  now:T0+600000, takerRatioMin:0.20, ...opt});

console.log('══ A — BACKTEST OYU: yalniz close>open ' + '═'.repeat(32));
{ // 3 yukselen mum, taker DUSUK (0.30) -> backtestte votes=3 olurdu
  const r=calistir([0,1,2].map(i=>mum(i,{yukselen:true,taker:0.30})));
  ok('3 yukselen + taker 0,30 -> votes 3', r.votes===3, `votes=${r.votes}`);
  ok('  aksiyon MARKET', r.action==='MARKET', r.action);
}
{ // 2 yukselen 1 dusen, taker dusuk
  const r=calistir([mum(0,{yukselen:true,taker:0.25}),mum(1,{yukselen:true,taker:0.28}),mum(2,{yukselen:false,taker:0.9})]);
  ok('2 yukselen + taker 0,25 -> votes 2', r.votes===2, `votes=${r.votes}`);
}
{ // taker COK dusuk (0.22 — backtestin gozlenen mini)
  const r=calistir([0,1,2].map(i=>mum(i,{yukselen:true,taker:0.2199})));
  ok('backtestin en dusuk takeri (0,2199) oyu DUSURMUYOR', r.votes===3, `votes=${r.votes}`);
}

console.log('\n══ B — BAYRAK ACILIRSA ESKI DAVRANIS ' + '═'.repeat(34));
{
  const b=[0,1,2].map(i=>mum(i,{yukselen:true,taker:0.30}));
  const kapali=calistir(b);
  const acik  =calistir(b,{takerVoteActive:true, takerRatioMin:0.50});
  ok('bayrak KAPALI  -> votes 3', kapali.votes===3, `votes=${kapali.votes}`);
  ok('bayrak ACIK    -> votes 0', acik.votes===0, `votes=${acik.votes}`);
  ok('  fark GERCEK (bayrak isliyor)', kapali.votes!==acik.votes);
  ok('  acikken PUSU', acik.action==='PUSU', acik.action);
}

console.log('\n══ C — KARAR MANTIGI DEGISMEDI ' + '═'.repeat(40));
{
  const r=calistir([0,1,2].map(i=>mum(i,{yukselen:true,taker:0.9})));
  ok('votes 3 + drift>=0 -> MARKET x1.00', r.action==='MARKET'&&r.scale===1.0, `${r.action}/${r.scale}`);
}
{
  // son mum kapanisi 99,5 -> driftAtr -0,1 (taktik tabani -0,15'in USTUNDE)
  const r=calistir([mum(0,{yukselen:true,taker:0.9}),mum(1,{yukselen:true,taker:0.9}),mum(2,{yukselen:false,taker:0.9,kapanis:99.5})]);
  ok('votes 2 + drift -0,10 -> TACTICAL x0.60', r.votes===2&&r.action==='TACTICAL'&&r.scale===0.60, `votes=${r.votes} ${r.action}/${r.scale}`);
  // taktik tabaninin ALTI -> PUSU (sinir davranisi)
  const r2=calistir([mum(0,{yukselen:true,taker:0.9}),mum(1,{yukselen:true,taker:0.9}),mum(2,{yukselen:false,taker:0.9,kapanis:99.0})]);
  ok('votes 2 + drift -0,20 -> PUSU (taban altinda)', r2.action==='PUSU', `${r2.action}`);
}
{
  const r=calistir([0,1,2].map(i=>mum(i,{yukselen:false,taker:0.9})));
  ok('votes 0 -> PUSU', r.action==='PUSU', r.action);
}

console.log('\n══ D — KAYNAKTA BELGE ve KAPI ' + '═'.repeat(41));
const has=t=>src.includes(t);
ok('bayrak varsayilan KAPALI', has("R495_TAKER_VOTE_ACTIVE = String(process.env.R495_TAKER_VOTE_ACTIVE ?? '0') === '1'"));
ok('cagri bayragi geciriyor', has('takerVoteActive:R495_TAKER_VOTE_ACTIVE'));
ok('kapi acikken emri durdurur', has('R495_TAKER_OYU_ACIK_BACKTESTTE_YOK'));
ok('uc testin kaniti kaynakta', has('1461/1461') && has('%25,7') && has('0,2680'));
ok('onceki surumler duruyor', has('V504_BACKTEST_TAKER_MIN_OBSERVED') && has('V503 testnet evren on-filtresi') && has('V502-A: KALDIRAC KILIDI'));
ok('build V5_0_5', ((has('V5_0_5_VOTE_EXACT_BACKTEST') || src.includes('V5_0_6_LOSS_TELEMETRY')) || src.includes('V5_0_7_LEVPROOF_RETRY')));

console.log(`\n${'═'.repeat(72)}\nSONUC: ${pass} gecti, ${fail} kaldi`);
process.exit(fail?1:0);
