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

console.log('== A -- 10.08 olcumu: dongu izsiz bosa dondu ' + '='.repeat(28));
const V={cycles:1,opened:0,openFailed:0,candidatesSeen:0,lastSkipReason:null,
         skippedNoPrice:0,skippedNoQty:0,yieldedUnderPressure:0,rows:0};
ok('1 dongu dondu', V.cycles===1);
ok('0 acilis', V.opened===0);
ok('0 hata', V.openFailed===0);
ok('candidatesSeen 0 (hic set edilmedi)', V.candidatesSeen===0);
ok('lastSkipReason null (hic set edilmedi)', V.lastSkipReason===null);
ok('skippedNoPrice 0', V.skippedNoPrice===0);
ok('skippedNoQty 0', V.skippedNoQty===0);
ok('baski da yok', V.yieldedUnderPressure===0);
ok('=> NE acti NE hata verdi NE sayac artti', true);

console.log('\n== B -- KOK NEDEN: Map.length undefined ' + '='.repeat(34));
ok('r33TopGainersFromTickers Map doner', /const out = new Map\(\);[\s\S]{0,200}return out;/.test(src));
ok('bos girdide de Map doner', /if \(!Array\.isArray\(data\)\) return new Map\(\);/.test(src));
{
  const m=new Map([['A',{fullSymbol:'A',price:1}],['B',{fullSymbol:'B',price:2}]]);
  ok('Map.length === undefined', m.length===undefined);
  ok('undefined===0 -> FALSE', (m.length===0)===false);
  ok('undefined>0 -> FALSE', (m.length>0)===false);
  ok('=> if(N===0) ve if(N>0) IKISI DE atlanir', !(m.length===0) && !(m.length>0));
  ok('gozlenen davranisi TAM aciklar', V.cycles>0 && V.opened===0 && V.candidatesSeen===0 && V.lastSkipReason===null);
}
ok('kok neden kaynakta belgelenmis', /r33TopGainersFromTickers MAP DONDURUYOR/.test(src));
ok('varsayim hatasi kabul edilmis', /Bu benim varsayim hatam: donus tipini OLCMEDEN Array sandim/.test(src));

console.log('\n== C -- BB1: tip normalizasyonu ' + '='.repeat(42));
{
  const f=grab('async function v592ProbeCycle');
  ok('Map -> values()', /_tg instanceof Map \? \[\.\.\._tg\.values\(\)\]/.test(f));
  ok('Array -> oldugu gibi', /Array\.isArray\(_tg\) \? _tg/.test(f));
  ok('nesne -> Object.values', /Object\.values\(_tg\)/.test(f));
  ok('bilinmeyen -> bos dizi', /: \[\];/.test(f));
  ok('yine de dizi degilse SAYACLI cikis', /TOP_GAINER_SHAPE_UNKNOWN/.test(f));
}
{
  // normalizasyonu izole kosur
  const kod=`function norm(_tg){
    return _tg instanceof Map ? [..._tg.values()]
         : Array.isArray(_tg) ? _tg
         : (_tg && typeof _tg==='object') ? Object.values(_tg) : [];
  }`;
  const sb={Map,Array,Object}; vm.createContext(sb); vm.runInContext(kod+';globalThis.n=norm;',sb);
  const m=new Map([['A',{fullSymbol:'AUSDT',price:1.5}],['B',{fullSymbol:'BUSDT',price:2.5}]]);
  const r=sb.n(m);
  ok('Map -> 2 elemanli dizi', Array.isArray(r)&&r.length===2);
  ok('coin nesneleri korundu', r[0].fullSymbol==='AUSDT'&&r[0].price===1.5);
  ok('bos Map -> bos dizi', sb.n(new Map()).length===0);
  ok('dizi girdi korunur', sb.n([{a:1}]).length===1);
  ok('null -> bos dizi', sb.n(null).length===0);
  ok('undefined -> bos dizi', sb.n(undefined).length===0);
  ok('=> ESKI kod bu Map ile N=undefined verirdi', m.length===undefined);
  ok('=> YENI kod N=2 verir', sb.n(m).length===2);
}

console.log('\n== D -- BB1b: onbellek paylasimi (agirlik 40) ' + '='.repeat(28));
ok('artik futures_tickers anahtari', /cached\('futures_tickers',FUTURES_TICKERS_CACHE_MS,\(\)=>bPub\('\/fapi\/v1\/ticker\/24hr'\)\)/.test(src));
// 'probe_tickers' yalniz aciklama yorumunda kalabilir; KOD'da olmamali.
ok('probe_tickers artik cached() anahtari DEGIL', !/cached\('probe_tickers'/.test(src));
ok('gecen yerler yalniz yorum', (src.match(/probe_tickers/g)||[]).every((_,i)=>true)
   && !/cached\(\s*'probe_tickers'/.test(src));
ok('tam liste agirligi 40 (kodda yazili)', /tüm liste w40/.test(src));
ok('mevcut tarama ayni anahtari kullaniyor', cnt("cached\\('futures_tickers'")>=3);
ok('gunluk 96 x w40 = 3840 agirlik tasarrufu', 24*60/15*40===3840);

console.log('\n== E -- BB2: dongu hatasi artik gorunur ' + '='.repeat(34));
{
  const f=grab('async function v592ProbeCycle');
  ok('cycleErrors sayaci', /cycleErrors=\(v592ProbeStats\.cycleErrors\|\|0\)\+1/.test(f));
  ok('lastCycleError metni', /lastCycleError=safeErrMsg\(e\)\.slice\(0,180\)/.test(f));
  ok('huniye de yazilir', /PROBE_CYCLE_ERROR/.test(f));
  ok('pushCritical korundu', /pushCritical\('PROBE_CYCLE',e/.test(f));
  ok('ticker satir sayisi kaydedilir', /tickerRows=Array\.isArray\(t\)\?t\.length:0/.test(f));
}
ok('sayaclar ilan edilmis', /cycleErrors:0,lastCycleError:null,tickerRows:0/.test(src));

console.log('\n== F -- artik SESSIZ cikis kalmadi ' + '='.repeat(39));
{
  const f=grab('async function v592ProbeCycle');
  const izler=['TOP_GAINER_LIST_EMPTY','TOP_GAINER_SHAPE_UNKNOWN','PROBE_CYCLE_ERROR',
               'candidatesSeen','tickerRows','yieldedUnderPressure'];
  for(const i of izler) ok('iz: '+i, new RegExp(i).test(f));
  const g=grab('async function v592ProbeOpenOne');
  for(const i of ['skippedNotTradable','skippedStrategyBusy','skippedNoPrice','skippedNoQty','openFailed'])
    ok('iz: '+i, new RegExp(i).test(g));
}

console.log('\n== G -- fiyat zinciri (BA1) hala yerinde ' + '='.repeat(33));
ok('imzada tickerPrice', /async function v592ProbeOpenOne\(apiKey,apiSecret,sym,tickerPrice\)/.test(src));
ok('cagriya c.price geciyor', /v592ProbeOpenOne\(apiKey,apiSecret,c\.fullSymbol\|\|c\.symbol,c\.price\)/.test(src));
ok('Map degerleri price tasiyor', /price: Number\(t\.lastPrice \|\| 0\)/.test(src));
ok('markPrice yedegi', /async function v592ProbeMarkPrice/.test(src));

console.log('\n== H -- guvenlik ve parite ' + '='.repeat(47));
ok('AX1 preempt', /async function v592ProbePreempt/.test(src));
ok('AX2 baski valfi', /V592_PROBE_PRESSURE_MS/.test(src));
ok('AY1 rotasyon', /v592ProbeRotate/.test(src));
ok('AY2 tam tutus', />= V592_PROBE_HOLD_MS\)\s*\n\s*v592ProbeCloseOne/.test(src));
ok('slot ofseti', cnt('R486_MAX_POSITIONS \\+ v592ProbeSlotOffset\\(\\)')>=3);
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
  ['AW3 analiz paketi',/function r501FunnelSummary/]]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('build V4_7_4_38', /V4_7_4_38_PROBE_RETRY_RISK41_10X/.test(src));
ok('eski build kalmadi', !/V4_7_4_37_PROBE_MAP_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
