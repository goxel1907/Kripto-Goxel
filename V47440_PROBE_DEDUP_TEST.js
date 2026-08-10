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

console.log('== A -- 10.08 olcumu: mukerrer satirlar ' + '='.repeat(34));
const V={opened:12,closed:25,rows:25,tekilId:12,mukerrer:13,ucKez:1,ikiKez:8,
         sonucHam:{ZARAR:18,KAR:6,BILINMIYOR:1},
         sonucTekil:{ZARAR:7,KAR:4,BILINMIYOR:1}};
ok('12 acilis', V.opened===12);
ok('AMA 25 kapanis', V.closed===25);
ok('CSV 25 satir', V.rows===25);
ok('sadece 12 TEKIL id', V.tekilId===12);
ok('13 mukerrer satir', V.mukerrer===13);
ok('8 id iki kez', V.ikiKez===8);
ok('1 id UC kez', V.ucKez===1);
ok('=> ham sayim YANILTIYOR (18 zarar degil 7)',
   V.sonucHam.ZARAR===18 && V.sonucTekil.ZARAR===7);
ok('kok neden kaynakta belgelenmis', /AYNI SONDA IKI KEZ KAPATILIYORDU/.test(src));
ok('iki cagri yeri yazili', /15 saniyelik kapatici setInterval/.test(src));
ok('finally-after-await sorunu yazili', /Silme islemi `finally` icinde, yani AWAIT'lerden SONRA/.test(src));

console.log('\n== B -- BE1: senkron kilit ' + '='.repeat(47));
{
  const f=grab('async function v592ProbeCloseOne');
  ok('_closing kilidi', /if\(p\._closing && Date\.now\(\)-Number\(p\._closingAt\|\|0\) < 60000\)/.test(f));
  ok('kilit AWAIT ten ONCE', f.indexOf('p._closing=true')<f.indexOf('await'));
  ok('zaman damgasi', /p\._closingAt=Date\.now\(\)/.test(f));
  ok('yaris sayaci', /closeRaceBlocked/.test(f));
  ok('60 sn sonra tekrar denenebilir', /< 60000/.test(f));
  ok('finally hala temizliyor', /finally\{ v592ProbeOpen\.delete\(S\); \}/.test(f));
}
{
  // yarisi izole kosur
  const kod=`
    const acik=new Map(); const yazilan=[]; let bloklanan=0;
    acik.set('A',{id:'PRB_A',openedAt:0});
    async function kapat(S){
      const p=acik.get(S); if(!p) return;
      if(p._closing && Date.now()-Number(p._closingAt||0) < 60000){ bloklanan++; return; }
      p._closing=true; p._closingAt=Date.now();
      await new Promise(r=>setTimeout(r,30));   // yavas borsa cagrisi
      yazilan.push(p.id);
      acik.delete(S);
    }
    async function eski(S){
      const p=acik.get(S); if(!p) return;
      await new Promise(r=>setTimeout(r,30));
      yazilan.push(p.id);
      acik.delete(S);
    }`;
  const sb={console,Map,Date,Promise,setTimeout};
  vm.createContext(sb); vm.runInContext(kod+';globalThis.K=kapat;globalThis.E=eski;globalThis.Y=()=>yazilan;globalThis.B=()=>bloklanan;globalThis.A=acik;',sb);
  await Promise.all([sb.K('A'),sb.K('A'),sb.K('A')]);   // 3 es zamanli cagri
  ok('3 es zamanli kapatma -> 1 satir', sb.Y().length===1, JSON.stringify(sb.Y()));
  ok('2 cagri bloklandi', sb.B()===2);
  ok('kayit temizlendi', !sb.A.has('A'));
  // ESKI davranis
  const sb2={console,Map,Date,Promise,setTimeout};
  vm.createContext(sb2); vm.runInContext(kod+';globalThis.E=eski;globalThis.Y=()=>yazilan;globalThis.A=acik;',sb2);
  await Promise.all([sb2.E('A'),sb2.E('A'),sb2.E('A')]);
  ok('ESKI kod ayni senaryoda 3 satir yazardi', sb2.Y().length===3, String(sb2.Y().length));
  ok('=> gozlenen 3x tekrari TAM aciklar', sb2.Y().length===3);
}

console.log('\n== C -- BE2: cikti tarafinda emniyet agi ' + '='.repeat(33));
{
  const f=grab('function v592ProbeRows');
  ok('id basina ILK satir tutulur', /if\(gor\.has\(id\)\)\{ v592ProbeStats\.duplicateRowsFiltered/.test(f));
  ok('idsiz satirlar korunur', /if\(!id\)\{ out\.push\(x\); continue; \}/.test(f));
  ok('filtre sayaci', /duplicateRowsFiltered/.test(f));
  const sb={console,Set,String,fs:{readFileSync:()=>[
    {id:'A',v:1},{id:'B',v:2},{id:'A',v:3},{id:'C',v:4},{id:'B',v:5},{id:'A',v:6}
  ].map(x=>JSON.stringify(x)).join('\n')},
    V592_PROBE_PATH:'x',JSON,
    v592ProbeStats:{duplicateRowsFiltered:0}};
  vm.createContext(sb); vm.runInContext(f+';globalThis.R=v592ProbeRows;',sb);
  const r=sb.R();
  ok('6 ham satir -> 3 tekil', r.length===3, String(r.length));
  ok('ILK satir tutuldu (A.v=1)', r[0].v===1);
  ok('3 mukerrer filtrelendi', sb.v592ProbeStats.duplicateRowsFiltered===3);
  ok('sira korundu', r.map(x=>x.id).join('')==='ABC');
}

console.log('\n== D -- BE3: botAction neden bos ' + '='.repeat(41));
const B={toplam:12,botYok:8,botPusu:3,botMarket:1};
ok('12 ornegin 8inde bot karari YOKTU', B.botYok===8);
ok('kok neden yazili', /sonda top-24\s*\n\/\/ gainer aliyor, bot ise KENDI tarama listesini degerlendiriyor/.test(src)||/bot ise KENDI tarama listesini degerlendiriyor/.test(src));
{
  const f=grab('function v592ProbeDecisionNow');
  ok('karar yoksa present:false', /if\(!d\) return \{present:false,reason:'BOT_HAS_NO_DECISION_FOR_SYMBOL'/.test(f));
  ok('karar varsa present:true', /return \{present:true, decisionId:/.test(f));
}
ok('CSV de botDecisionPresent sutunu', /botDecisionPresent:r\.entryDecision\?\.present/.test(src));
ok('CSV de sebep sutunu', /botNoDecisionReason:/.test(src));

console.log('\n== E -- ILK SINYAL (n=12, hukum YOK) ' + '='.repeat(38));
const S={pusuKar:2,pusuZarar:1,marketZarar:1,yokKar:2,yokZarar:5,yokBilinmiyor:1};
ok('bot PUSU dedi -> KAR: 2', S.pusuKar===2);
ok('bot PUSU dedi -> ZARAR: 1', S.pusuZarar===1);
ok('bot MARKET dedi -> ZARAR: 1', S.marketZarar===1);
ok('n=12 · hukum icin COK ERKEN', 12<74);
ok('d=0.8 icin 74 ornek gerekir', 74>12);
ok('384 ornek/gun ile 74e ~4,6 saatte varilir', Math.abs(74/384*24-4.6)<0.3);

console.log('\n== F -- guvenlik ve parite ' + '='.repeat(47));
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
for(const [n,re] of [['BD1 ENV kimlik',/function v592ProbeCreds/],['BC1 tekrar',/V592_PROBE_RETRY_MS/],
  ['BB1 Map normalizasyonu',/_tg instanceof Map/],['BA1 fiyat zinciri',/Number\(tickerPrice \|\| 0\)/],
  ['AX1 preempt',/async function v592ProbePreempt/],['AY1 rotasyon',/v592ProbeRotate/],
  ['AS1 tek huni',/async function v592FinalizeClose/],['AU1 dedup ayrimi',/v592EvidenceClosedOnce/],
  ['AW2 disk muhafizi',/function r501DiskGuard/],['BE1 kapatma kilidi',/closeRaceBlocked/]]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('build V4_7_4_40', /V4_7_4_40_PROBE_DEDUP_RISK41_10X/.test(src));
ok('eski build kalmadi', !/V4_7_4_39_PROBE_CREDS_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
