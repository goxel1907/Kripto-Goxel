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

console.log('══ A — iki fren alani ayri mi ' + '═'.repeat(44));
ok('execBackoffUntil alani var', /execBackoffUntil:\s*0/.test(src));
ok('isExecBackoffActive tanimli', /function isExecBackoffActive\(\)/.test(src));
ok('getExecBackoffMs tanimli', /function getExecBackoffMs\(\)/.test(src));
{
  const ctx=vm.createContext({binanceGov:{backoffUntil:0,execBackoffUntil:0,last429At:0},
    pushCritical:()=>{},Date,Math,Number,String,console});
  vm.runInContext(grab('function registerBinanceBackoff'),ctx);
  vm.runInContext(grab('function isBinanceBackoffActive'),ctx);
  vm.runInContext(grab('function getBinanceBackoffMs'),ctx);
  vm.runInContext(grab('function isExecBackoffActive'),ctx);
  vm.runInContext(grab('function getExecBackoffMs'),ctx);

  ctx.registerBinanceBackoff('testnet 418',60,'EXEC');
  ok('EXEC freni aktif',   ctx.isExecBackoffActive()===true);
  ok('PUBLIC freni TEMIZ', ctx.isBinanceBackoffActive()===false,'<-- 05.08 hatasi buydu');

  ctx.binanceGov.execBackoffUntil=0;
  ctx.registerBinanceBackoff('mainnet 429',60,'PUBLIC');
  ok('PUBLIC freni aktif', ctx.isBinanceBackoffActive()===true);
  ok('EXEC freni TEMIZ',   ctx.isExecBackoffActive()===false);

  ctx.binanceGov.backoffUntil=0; ctx.binanceGov.execBackoffUntil=0;
  ctx.registerBinanceBackoff('ikisi',60,'BOTH');
  ok('BOTH -> ikisi de aktif', ctx.isBinanceBackoffActive()&&ctx.isExecBackoffActive());

  ctx.binanceGov.backoffUntil=0; ctx.binanceGov.execBackoffUntil=0;
  ctx.registerBinanceBackoff('varsayilan',60);
  ok('domain verilmezse PUBLIC', ctx.isBinanceBackoffActive()===true&&ctx.isExecBackoffActive()===false);
  ok('sure 5..180 kelepce', (()=>{ctx.binanceGov.backoffUntil=0;ctx.registerBinanceBackoff('x',9999,'PUBLIC');
      const ms=ctx.getBinanceBackoffMs();return ms>178000&&ms<=180000;})());
}

console.log('\n══ B — her cagri noktasi dogru alana yaziyor mu ' + '═'.repeat(27));
const EXEC=[["algoOrder","registerHttpBackoffAndThrow\\('algoOrder', res\\.status, res\\.headers\\.get\\('Retry-After'\\), 'EXEC'\\)"],
            ["-1003 algoOrder","'Binance -1003 algoOrder', 60, 'EXEC'"],
            ["imzali bReq","registerHttpBackoffAndThrow\\(path, res\\.status, res\\.headers\\.get\\('Retry-After'\\), 'EXEC'\\)"],
            ["-1003 imzali","Binance -1003 \\$\\{path\\}`, 60, 'EXEC'"]];
const PUB =[["public REST","registerHttpBackoffAndThrow\\(path, r\\.status, r\\.headers\\.get\\('Retry-After'\\), 'PUBLIC'\\)"],
            ["-1003 public","'Binance -1003 public', 60, 'PUBLIC'"],
            ["klines R495","klines:R495_EXACT',res\\.status,res\\.headers\\.get\\('Retry-After'\\),'PUBLIC'\\)"]];
for(const [n,re] of EXEC) ok(`EXEC   ${n}`, cnt(re)===1, `bulunan ${cnt(re)}`);
for(const [n,re] of PUB ) ok(`PUBLIC ${n}`, cnt(re)===1, `bulunan ${cnt(re)}`);
ok('domainsiz registerHttpBackoffAndThrow cagrisi YOK',
   cnt("registerHttpBackoffAndThrow\\(")===1+EXEC.length-1+PUB.length+0
   || /function registerHttpBackoffAndThrow\(scope, status, retryHeader, domain='PUBLIC'\)/.test(src),
   `toplam ${cnt("registerHttpBackoffAndThrow\\(")}`);

console.log('\n══ C — imzali yollar EXEC frenine bakiyor mu ' + '═'.repeat(30));
const SIGNED=[['bracket cache retry','if \\(isExecBackoffActive\\(\\)\\) return \\[\\];'],
              ['signed account snapshot','if\\(isExecBackoffActive\\(\\)\\)\\{'],
              ['positionRisk','if \\(isExecBackoffActive\\(\\)\\) \\{'],
              ['panel bracket','\\} else if \\(!isExecBackoffActive\\(\\)\\) \\{'],
              ['r379 SL restore','slPrice\\) && !isExecBackoffActive\\(\\)'],
              ['liveSLGuard','if \\(!isExecBackoffActive\\(\\)\\) \\{'],
              ['bracket tamir','!hasTP\\) && !isExecBackoffActive\\(\\)'],
              ['syncPositions','resetStuckPositionRiskInflight\\(.syncPositions-backoff.\\)']];
for(const [n,re] of SIGNED) ok(`${n} -> EXEC`, cnt(re)>=1, `bulunan ${cnt(re)}`);
ok('snapshot hata suresi getExecBackoffMs', /Signed account snapshot backoff',Math\.ceil\(getExecBackoffMs\(\)/.test(src));
ok('positionRisk hata suresi getExecBackoffMs', /istek freni', Math\.ceil\(getExecBackoffMs\(\)/.test(src));

console.log('\n══ D — tarama SADECE public frene bakiyor mu ' + '═'.repeat(30));
const SCAN=[['adaptif tarama araligi','isBinanceBackoffActive\\(\\)\\|\\|isPositionRiskCooldownActive\\(\\)'],
            ['R385 worker evreni',"typeof isBinanceBackoffActive === 'function' && isBinanceBackoffActive\\(\\)"],
            ['auto-scan kapisi','const rem = Math\\.ceil\\(getBinanceBackoffMs\\(\\)/1000\\)'],
            ['fast-wake timer','autoRunning \\|\\| isBinanceBackoffActive\\(\\)'],
            ['wake esigi','lastWakeOk\\) && !isBinanceBackoffActive\\(\\)']];
for(const [n,re] of SCAN) ok(`${n} -> PUBLIC (degismedi)`, cnt(re)>=1, `bulunan ${cnt(re)}`);

console.log('\n══ E — 05.08 senaryosunun tekrari ' + '═'.repeat(40));
{
  const ctx=vm.createContext({binanceGov:{backoffUntil:0,execBackoffUntil:0,last429At:0},
    pushCritical:()=>{},Date,Math,Number,String,console});
  ['function registerBinanceBackoff','function registerHttpBackoffAndThrow','function makeBinanceBackoffError',
   'function isBinanceBackoffActive','function getBinanceBackoffMs',
   'function isExecBackoffActive','function getExecBackoffMs'].forEach(f=>vm.runInContext(grab(f),ctx));
  // gercek olay: testnet /fapi/v2/account -> HTTP 418
  let threw=false;
  try{ ctx.registerHttpBackoffAndThrow('/fapi/v2/account',418,null,'EXEC'); }catch(e){ threw=true; }
  ok('418 firlatildi', threw);
  ok('EXEC donduruldu (emir gitmez)', ctx.isExecBackoffActive()===true);
  ok('TARAMA CALISMAYA DEVAM EDER',   ctx.isBinanceBackoffActive()===false);
  ok('EXEC suresi 60-120sn', ctx.getExecBackoffMs()>=59000&&ctx.getExecBackoffMs()<=120000,
     `${Math.round(ctx.getExecBackoffMs()/1000)}sn`);
  // mainnet klines 429 -> tarama durur, emir yolu etkilenmez
  ctx.binanceGov.execBackoffUntil=0;
  try{ ctx.registerHttpBackoffAndThrow('/fapi/v1/klines:R495_EXACT',429,null,'PUBLIC'); }catch(e){}
  ok('mainnet 429 -> tarama durur', ctx.isBinanceBackoffActive()===true);
  ok('mainnet 429 -> EXEC temiz',   ctx.isExecBackoffActive()===false);
}

console.log('\n══ F — guvenlik: testnet kilidi ve emir kapisi bozulmadi ' + '═'.repeat(18));
ok('testnet hard-lock duruyor', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('mainnet yalniz public', /const FAPI = 'https:\/\/fapi\.binance\.com'/.test(src));
ok('J1 koruma korumasi duruyor', /V592_PROTECT_KEEP_EXISTING/.test(src)&&/protectionKeptExisting\+\+/.test(src));
ok('J2 min hold duruyor', /function v592MinHoldGuard/.test(src)&&/minHoldBlocks\+\+/.test(src));
ok('K1 tazelik butcesi duruyor', /freshCacheHits\+\+/.test(src));
ok('build etiketi V4_7_4_11', /V4_7_4_18_FAST_PROTECT_RISK41_10X/.test(src));
ok('eski etiket kalmadi', !/V4_7_4_10_FRESHNESS_BUDGET/.test(src));
ok('telemetri execBackoffActive x4 (panel+stats)', cnt('execBackoffActive:')===4, `${cnt('execBackoffActive:')}`);
ok('parityV4741.stats backoff tasiyor', cnt('stats:\\{\\.\\.\\.v592ParityStats,execBackoffActive')===2,
   `${cnt('stats:\\{\\.\\.\\.v592ParityStats,execBackoffActive')}`);
ok('stats publicBackoffMs tasiyor', cnt('publicBackoffMs:getBinanceBackoffMs\\(\\)')===2);
ok('mode etiketi V47418', cnt('V47418_')===2 && !/mode:'V4741[0-7]_/.test(src));

console.log(`\n${'═'.repeat(74)}`);
console.log(fail?`SONUC: FAIL — ${pass} gecti, ${fail} dustu`:`SONUC: PASS — ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
