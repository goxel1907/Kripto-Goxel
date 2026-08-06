'use strict';
// V4.7.4.6 — KORUMASIZ POZISYON + COKLU KAYIT REGRESYONU
const fs=require('fs'),vm=require('vm'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`pass  ${n}`)):(fail++,console.error(`FAIL  ${n}${d?' :: '+d:''}`));
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
const sb=(e={})=>vm.createContext({Date,Math,Number,String,Object,Array,Boolean,JSON,Promise,Map,Set,Error,
  parseFloat,parseInt,isNaN,console,setTimeout,clearTimeout,...e});

(async()=>{
console.log('── G1  freshOpenPositionForSymbol GERCEK force-fresh ' + '─'.repeat(18));
{
  let seen=[];
  const ctx=sb({POS_FRESH_MANAGER_MS:8000,POS_FRESH_ORDER_MS:3000,
    getPositionRiskCached:async(k,s,p)=>{seen.push(p);return [{symbol:'SYNUSDT',positionAmt:'691'}];}});
  vm.runInContext(grab('async function freshOpenPositionForSymbol'),ctx);
  const r=await ctx.freshOpenPositionForSymbol('k','s','SYNUSDT',1);
  ok('G1a pozisyon bulundu', r.open===true);
  ok('G1b __forceFresh:true gecildi', seen[0] && seen[0].__forceFresh===true, JSON.stringify(seen[0]));
  ok('G1c symbol de gecildi', seen[0] && seen[0].symbol==='SYNUSDT');
  ok('G1d eski "cache bypass" yorumu kalmadi', !/getPositionRiskCached\(apiKey, apiSecret, \{symbol:sym\}\); \/\/ symbol-specific: cache bypass/.test(src));
  ok('G1e V4.7.4.10: yas siniri da gecildi', seen[0] && seen[0].__maxAgeMs===8000, JSON.stringify(seen[0]));
}

console.log('\n── G2  Dolum sonrasi pozisyon kaniti (fail-closed) ' + '─'.repeat(20));
{
  ok('G2a POST_FILL_POSITION_PROOF kaydi var', /'POST_FILL_POSITION_PROOF'/.test(src));
  ok('G2b positionRisk kaniti', /const _rows=await fetchPositionRiskRaw\(apiKey,apiSecret\);/.test(src));
  ok('G2c userTrades kaniti', /\/fapi\/v1\/userTrades/.test(src));
  ok('G2d net pozisyon hesabi', /net \+= \(x\.buyer\? q : -q\)/.test(src));
  ok('G2e kanit varsa SL\\/TP devam eder', /_via\} pozisyonu dogruladi; SL\/TP yazimi devam ediyor/.test(src));
  ok('G2f hicbir kanit yoksa POSITION_ALREADY_CLOSED', /if\(!_proofOpen\)\{[\s\S]{0,200}POSITION_ALREADY_CLOSED/.test(src));
  ok('G2g rescue sayaci', /v592ParityStats\.postFillProofRescues\+\+/.test(src));
  ok('G2h freshStart artik let (yeniden atanabilir)',
   /let freshStart = firstInstall/.test(src) && /: await freshOpenPositionForSymbol/.test(src));
  ok('G2i SL\/TP yolu SIKI butce kullaniyor', /freshOpenPositionForSymbol\(apiKey, apiSecret, symbol, 3, POS_FRESH_ORDER_MS\)/.test(src));
}

console.log('\n── G3  Ayni emir icin ikinci kanit kaydi acilmaz ' + '─'.repeat(22));
{
  const ctx=sb({R501_EVIDENCE_ACTIVE:true,BINANCE_EXECUTION_ENV:'TESTNET',
    r501ActiveEvidence:new Map(), normalizeSymbol:x=>String(x),
    r501OrderLifeSnapshot:()=>({mainOrderId:304913736,clientOrderId:'LZabc',attemptId:'ATT1'}),
    r501OrderLifeMark:()=>{}, v592ParityStats:{evidenceDuplicateSuppressed:0}});
  vm.runInContext(grab('function r501OrderKeyOf'),ctx);
  const k1=ctx.r501OrderKeyOf({symbol:'SYNUSDT'},{});
  ok('G3a orderId anahtar uretiliyor', k1==='OID:304913736', k1);
  const ctx2=sb({normalizeSymbol:x=>String(x), r501OrderLifeSnapshot:()=>({clientOrderId:'LZabc'})});
  vm.runInContext(grab('function r501OrderKeyOf'),ctx2);
  ok('G3b orderId yoksa clientOrderId', ctx2.r501OrderKeyOf({symbol:'X'},{})==='CID:LZabc');
  const ctx3=sb({normalizeSymbol:x=>String(x), r501OrderLifeSnapshot:()=>({attemptId:'ATT9'})});
  vm.runInContext(grab('function r501OrderKeyOf'),ctx3);
  ok('G3c ikisi de yoksa attemptId', ctx3.r501OrderKeyOf({symbol:'X'},{})==='ATT:ATT9');
  ok('G3d duplicate bastirma kaydi', /'EVIDENCE_DUPLICATE_SUPPRESSED'/.test(src));
  ok('G3e duplicate sayaci', /evidenceDuplicateSuppressed/.test(src));
  ok('G3f mevcut kayit dondurulur', /return r501ActiveEvidence\.get\(prev\)\|\|null;/.test(src));
}

console.log('\n── G4  418 sirasinda bracket proof sahte basarisiz olmaz ' + '─'.repeat(14));
{
  ok('G4a backoff\'ta once retry', /await sleep\(1500\);[\s\S]{0,160}getBracketOrdersCached\(symbol, ttlMs\*2\)/.test(src));
  ok('G4b retry cache varsa doner', /const retry = getBracketOrdersCached\(symbol, ttlMs\*2\);\s*\n\s*if \(retry\) return retry;/.test(src));
}

console.log('\n── G5  Kimlik + sozlesme ' + '─'.repeat(45));
{
  const h=re=>re.test(src);
  ok('G5a build V4.7.4.17', h(/V4_7_4_17_CANDLE_PARITY_RISK41_10X/));
  ok('G5b session 4_7_4_17_CP1', h(/V592_EXACT_CLOSED1M_R495_72H_4_7_4_17_CP1/));
  ok('G5c eski kimlik yok', !h(/V4_7_4_9_EXIT_CONTRACT/));
  ok('G5d yeni bayraklar', h(/postFillPositionProof:true/)&&h(/evidenceOrderDedup:true/));
  ok('G5e slot 41', h(/R497_SLOT_MARGIN_USDT \|\| 41/));
  ok('G5f max 2', h(/R486_MAX_POSITIONS \|\| 2/));
  ok('G5g risk 4', h(/R495_FINAL_RISK_PCT \|\| 4/));
  ok('G5h R493 kapisi', h(/const storyWait = \['PUSU','REJECT'\]\.includes\(authority\.action\);/));
  ok('G5i R493 final lock fix', h(/\['PASS','PASS_BACKTEST_OBSERVABLE'\]\.includes\(_r493FinalCode\)/));
  ok('G5j testnet hard-lock', h(/BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/));
  ok('G5k leverage lock 10', h(/V592_LEVERAGE_LOCK\|\|10/));
  ok('G5l idempotency', h(/newClientOrderId:cid/));
}
console.log(`\n${fail?'SONUC: FAIL':'SONUC: PASS'} — ${pass} gecti, ${fail} dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
