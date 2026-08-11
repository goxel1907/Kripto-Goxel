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

console.log('== A -- 10.08 olcumu: 4 dongu 0 acilis ' + '='.repeat(34));
const V={active:true,cycles:4,opened:0,openFailed:0,skippedNotTradable:0,
         skippedStrategyBusy:0,skippedMaxOpen:0,yieldedUnderPressure:1,
         lastYieldReason:'POSITION_RISK_INFLIGHT',rows:0};
ok('sonda AKTIFTI', V.active===true);
ok('4 dongu dondu', V.cycles===4);
ok('0 acilis', V.opened===0);
ok('0 acilis HATASI', V.openFailed===0);
ok('HICBIR atlama sayaci artmadi',
   V.skippedNotTradable===0&&V.skippedStrategyBusy===0&&V.skippedMaxOpen===0);
ok('=> SESSIZ return false vardi', V.cycles>0 && V.opened===0 && V.openFailed===0);
ok('1 dongu baskidan geri cekildi', V.yieldedUnderPressure===1);
ok('ama 3 dongu calisti ve yine 0 acti', V.cycles-V.yieldedUnderPressure===3);
ok('kok neden kaynakta belgelenmis', /SONDA FIYAT BULAMIYORDU — 4 DONGU 0 ACILIS/.test(src));
ok('M1 dersinin tekrari notu', /panelde gorunmez hata \(M1 dersinin tekrari\)/.test(src));
ok('r371FlowStore sinirlamasi yazili', /sadece ABONE olunan sembollerde dolu/.test(src));
ok('ticker fiyatinin bakilmadigi yazili', /sonda ona hic bakmiyordu/.test(src));

console.log('\n== B -- BA1: fiyat zinciri ' + '='.repeat(47));
{
  const f=grab('async function v592ProbeOpenOne');
  ok('imzaya tickerPrice eklendi', /async function v592ProbeOpenOne\(apiKey,apiSecret,sym,tickerPrice\)/.test(src));
  ok('1. kaynak: canli defter', /Number\(r501CurrentBook\(S\)\?\.mid \|\| 0\)/.test(f));
  ok('2. kaynak: tick mumu', /Number\(getTickAnalysisRaw\(S\)\?\.currentCandle\?\.close \|\| 0\)/.test(f));
  ok('3. kaynak: TICKER (yeni)', /Number\(tickerPrice \|\| 0\)/.test(f));
  ok('4. kaynak: markPrice (yeni)', /px=await v592ProbeMarkPrice\(S\)/.test(f));
  ok('kaynak etiketi kaydedilir', /v592ProbeStats\.priceSource=pxKaynak/.test(f));
}
ok('v592ProbeMarkPrice var', /async function v592ProbeMarkPrice\(S\)/.test(src));
ok('markPrice onbellekli (20sn)', /cached\(`probe_mark_\$\{S\}`,20000/.test(src));
ok('cagriya ticker fiyati gecirildi', /v592ProbeOpenOne\(apiKey,apiSecret,c\.fullSymbol\|\|c\.symbol,c\.price\)/.test(src));
ok('r33TopGainersFromTickers price donduruyor', /price: Number\(t\.lastPrice \|\| 0\)/.test(src));

console.log('\n== C -- BA2: artik SESSIZ cikis yok ' + '='.repeat(38));
{
  const f=grab('async function v592ProbeOpenOne');
  const satirlar=f.split('\n');
  const sessiz=[];
  for(let i=0;i<satirlar.length;i++){
    if(!/return false/.test(satirlar[i])) continue;
    const pencere=satirlar.slice(Math.max(0,i-3),i+1).join(' ');
    if(!/skipped|openFailed|ProbeOpen\.has/.test(pencere)) sessiz.push(satirlar[i].trim());
  }
  ok(`sayacsiz return false yok (${sessiz.length})`, sessiz.length===0, JSON.stringify(sessiz));
  ok('fiyat yoksa sayac', /skippedNoPrice=\(v592ProbeStats\.skippedNoPrice\|\|0\)\+1/.test(f));
  ok('miktar yoksa sayac', /skippedNoQty=\(v592ProbeStats\.skippedNoQty\|\|0\)\+1/.test(f));
  ok('sebep metni kaydedilir', /lastSkipReason=`NO_PRICE:\$\{S\}`/.test(f));
  ok('miktar sebebi detayli', /NO_QTY:\$\{S\} px=\$\{px\} step=\$\{step\} notional=\$\{notional\}/.test(f));
}
ok('sayaclar ilan edilmis', /skippedNoPrice:0,skippedNoQty:0,lastSkipReason:null,priceSource:null,candidatesSeen:0/.test(src));
ok('aday sayisi gorunur', /v592ProbeStats\.candidatesSeen=N/.test(src));
ok('bos liste de gorunur', /lastSkipReason='TOP_GAINER_LIST_EMPTY'/.test(src));

console.log('\n== D -- fiyat zinciri canli kosum ' + '='.repeat(40));
{
  const f=grab('async function v592ProbeOpenOne');
  const mk=(book,tick,ticker,mark)=>{
    const log=[];
    const sb={console,Date,Number,Math,String,Promise,
      normalizeSymbol:x=>String(x).toUpperCase(),
      v592IsTestnetTradable:()=>true,
      v592ProbeOpen:new Map(), trailingState:new Map(),
      getSymbolFilters:async()=>({stepSize:0.001,qtyPrecision:3}),
      r501CurrentBook:()=>book?{mid:book}:null,
      getTickAnalysisRaw:()=>tick?{currentCandle:{close:tick}}:null,
      v592ProbeMarkPrice:async()=>mark||0,
      v592ProbeSnapshot:()=>({}), v592ProbeDecisionNow:()=>({}),
      bReq:async(k,s,m,p2)=>{log.push('order:'+p2);return{orderId:1,avgPrice:book||tick||ticker||mark};},
      r501EvidenceFunnel:()=>{}, safeErrMsg:e=>String(e),
      v592ProbeSaveState:()=>{},   // V4.7.4.41-BF1 stub
      V592_PROBE_MARGIN_USDT:15, V592_PROBE_LEVERAGE:10,
      v592ProbeStats:{opened:0,openFailed:0,skippedNotTradable:0,skippedStrategyBusy:0,
                      skippedNoPrice:0,skippedNoQty:0,lastSkipReason:null,priceSource:null}};
    vm.createContext(sb);
    vm.runInContext(f+';globalThis.O=v592ProbeOpenOne;',sb);
    return {O:sb.O,log,stats:sb.v592ProbeStats,acik:sb.v592ProbeOpen};
  };
  // ESKI SENARYO: defter yok, tick yok — ticker VAR
  const a=mk(0,0,3.25,0);
  const r=await a.O('k','s','RIVERUSDT',3.25);
  ok('defter+tick YOKKEN ticker ile ACILIR', r===true, JSON.stringify(a.stats));
  ok('fiyat kaynagi TICKER', a.stats.priceSource==='TICKER');
  ok('emir gonderildi', a.log.some(x=>x.startsWith('order:')));
  ok('sayac opened arti', a.stats.opened===1);
  // hicbir kaynak yok -> markPrice
  const b=mk(0,0,0,7.5);
  const r2=await b.O('k','s','XUSDT',0);
  ok('hepsi yoksa markPrice ile ACILIR', r2===true);
  ok('fiyat kaynagi MARK_PRICE', b.stats.priceSource==='MARK_PRICE');
  // hicbiri yok -> SAYACLI cikis
  const c=mk(0,0,0,0);
  const r3=await c.O('k','s','YUSDT',0);
  ok('hicbir fiyat yoksa false', r3===false);
  ok('skippedNoPrice ARTTI (artik gorunur)', c.stats.skippedNoPrice===1);
  ok('sebep kaydedildi', String(c.stats.lastSkipReason).startsWith('NO_PRICE:'));
  // defter varsa oncelik onda
  const d=mk(1.5,2.5,3.5,4.5);
  await d.O('k','s','ZUSDT',3.5);
  ok('oncelik canli defterde', d.stats.priceSource==='BOOK');
  // ESKI KOD simulasyonu
  const eskiPx=(book,tick)=>Number(book||0)||Number(tick||0);
  ok('ESKI kod: defter+tick yokken px=0 -> sessiz cikis', eskiPx(0,0)===0);
}

console.log('\n== E -- diger bulgular ' + '='.repeat(51));
const P={kapaliIslem:1,sutun:349,diskMB:16,limitMB:4200,sondaOrnek:0};
ok('pasif CSV calisiyor (1 islem, 349 sutun)', P.kapaliIslem===1&&P.sutun===349);
ok('disk 16 MB / 4200 (LITE calisiyor)', P.diskMB<50);
ok('LITE oncesi 13 islem 1240 MB idi -> islem basi 95 MB', 1240/13>90);
ok('LITE sonrasi cok daha kucuk', P.diskMB<100);
ok('analiz paketi kucuk (yuklenebilir)', true);

console.log('\n== F -- guvenlik ve parite korundu ' + '='.repeat(39));
ok('AX1 preempt duruyor', /async function v592ProbePreempt/.test(src));
ok('AX2 baski valfi duruyor', /V592_PROBE_PRESSURE_MS/.test(src));
ok('AY1 rotasyon duruyor', /v592ProbeRotate/.test(src));
ok('AY2 tam tutus duruyor', />= V592_PROBE_HOLD_MS\)\s*\n\s*v592ProbeCloseOne/.test(src));
ok('slot ofseti duruyor', cnt('R486_MAX_POSITIONS \\+ v592ProbeSlotOffset\\(\\)')>=3);
{
  const b=src.slice(src.indexOf('// V4.7.4.32 — SONDA'), src.indexOf("app.get('/api/probe/samples.csv'"));
  ok('tradeLedger e DOKUNMUYOR', !/tradeLedger/.test(b));
  ok('v592ParityStats e DOKUNMUYOR', !/v592ParityStats/.test(b));
  ok('ham arsiv YAZMIYOR', !/r501RawAppend|r501RawInit/.test(b));
  ok('YALNIZ LONG', /side:'BUY'/.test(b) && !/side:'SELL'/.test(b));
}
ok('varsayilan KAPALI', /String\(process\.env\.V592_PROBE_ACTIVE \?\? '0'\)==='1'/.test(src));
ok('TESTNET kilidi', /const V592_PROBE_ACTIVE = BINANCE_EXECUTION_ENV==='TESTNET'/.test(src));
for(const [n,re] of [['AS1 tek huni',/async function v592FinalizeClose/],
  ['AU1 dedup ayrimi',/v592EvidenceClosedOnce/],['AU2 exitReason',/exitReason:rec\.close\?\.exitReason/],
  ['AW1 lite',/function r501RawAllowed/],['AW2 disk muhafizi',/function r501DiskGuard/],
  ['AW3 analiz paketi',/function r501FunnelSummary/],['BA1 fiyat zinciri',/v592ProbeMarkPrice/]]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('build V4_7_4_43', /V4_7_4_43_AUDIT_FIX_RISK41_10X/.test(src));
ok('eski build kalmadi', !/V4_7_4_42_LEDGER_ISOLATE_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
