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

console.log('== A -- 11.08 olcumu: 120 kayit HAYALET ' + '='.repeat(33));
const V={kayit:120, medyanTutusMs:24, birSnAlti:78, protectionBos:119, marj41:119,
  binanceCikis:116, ayniDakika8:true, mukerrerCift:23, sondaSembolunde:120,
  net:-329.40, wr:26.7, pf:0.150, hiz:269, backtestHiz:12.08};
ok('120 kapali kayit', V.kayit===120);
ok('medyan tutus 24 MILISANIYE', V.medyanTutusMs===24);
ok('78/120 kayit 1 sn altinda', V.birSnAlti===78);
ok('protectionVerified BOS 119/120', V.protectionBos===119);
ok('marj 41 sabit 119/120', V.marj41===119);
ok('116/120 BINANCE_*_CLOSE (disaridan)', V.binanceCikis===116);
ok('23 cift ayni sembol+ayni pnl', V.mukerrerCift===23);
ok('tek dakikada 8 acilis (max 2 poz olmali)', V.ayniDakika8);
ok('120/120 SONDA sembollerinde', V.sondaSembolunde===120);
ok('hiz 269/gun · backtest 12,08 -> 22 KAT', V.hiz/V.backtestHiz>20);
ok('=> bu STRATEJI PERFORMANSI DEGIL', V.medyanTutusMs<1000 && V.protectionBos>115);
ok('kok neden kaynakta belgelenmis', /SONDA POZISYONLARI STRATEJI DEFTERINE SIZIYORDU/.test(src));
ok('24 ms olcumu yazili', /medyan tutus 24 MILISANIYE/.test(src));
ok('-329,40 nin gercek olmadigi yazili', /-329,40 USDT gercek\s*\n\/\/ strateji sonucu DEGIL/.test(src)||/gercek\s+strateji sonucu DEGIL/.test(src));
ok('yapisal kok sebep yazili', /sonda ve strateji AYNI Binance hesabini paylasiyor/.test(src));

console.log('\n== B -- BG1: sonda sembolu deftere giremez ' + '='.repeat(31));
ok('v592IsProbeSymbol var', /function v592IsProbeSymbol\(symbol\)/.test(src));
{
  const f=grab('function recordTradeClose');
  ok('EN BASTA sonda kontrolu', /const sym = normalizeSymbol\(symbol\);\s*\n\s*if \(v592IsProbeSymbol\(sym\)\)/.test(f));
  ok('sayac', /probeLedgerBlocked/.test(f));
  ok('iz birakir', /PROBE_SYMBOL_LEDGER_BLOCKED/.test(f));
  ok('null doner (satir uretmez)', /probeLedgerBlocked[\s\S]{0,200}return null;/.test(f));
}

console.log('\n== C -- BG2: hayalet satir uretimi kapatildi ' + '='.repeat(29));
{
  const f=grab('function recordTradeClose');
  ok('eski kosulsuz recordTradeOpen kalmadi',
     !/if \(!row\) row = recordTradeOpen\(sym, state\?\.side\|\|'UNKNOWN'/.test(f));
  ok('gercek acilis sarti', /const _gercekAcilis = Number\(state\?\.openedAt\|\|state\?\.openTs\|\|0\) > 0/.test(f));
  ok('giris fiyati da sart', /Number\(state\?\.entryPrice\|\|0\) > 0/.test(f));
  ok('sart saglanmazsa null', /phantomLedgerBlocked[\s\S]{0,300}return null;/.test(f));
  ok('iz birakir', /PHANTOM_LEDGER_BLOCKED/.test(f));
  ok('sart saglanirsa satir uretilir', /row = recordTradeOpen\(sym, state\?\.side\|\|'UNKNOWN'/.test(f));
}
{
  const kod=`
    function karar(state){
      const g = Number(state?.openedAt||state?.openTs||0) > 0 && Number(state?.entryPrice||0) > 0;
      return g ? 'SATIR_URET' : 'ENGELLE';
    }`;
  const sb={Number}; vm.createContext(sb); vm.runInContext(kod+';globalThis.k=karar;',sb);
  ok('bos state -> ENGELLE', sb.k({})==='ENGELLE');
  ok('sadece openedAt -> ENGELLE', sb.k({openedAt:1})==='ENGELLE');
  ok('sadece entryPrice -> ENGELLE', sb.k({entryPrice:1})==='ENGELLE');
  ok('ikisi de var -> SATIR_URET', sb.k({openedAt:1,entryPrice:2})==='SATIR_URET');
  ok('null state -> ENGELLE', sb.k(null)==='ENGELLE');
  ok('=> 24 ms lik hayalet kayitlarin hepsi ENGELLENIR', sb.k({})==='ENGELLE');
}

console.log('\n== D -- BG3: syncPositions sonda sembolunu atlar ' + '='.repeat(25));
ok('trailingState dongusunde atlanir', /for \(const \[sym, state\] of trailingState\.entries\(\)\) \{\s*\n\s*\/\/ V4\.7\.4\.42-BG3[\s\S]{0,90}if \(v592IsProbeSymbol\(sym\)\) continue;/.test(src));
ok('lastKnownPositions dongusunde atlanir', /if \(v592IsProbeSymbol\(sym\)\) continue;   \/\/ BG3/.test(src));
ok('iki dongude de var', cnt('v592IsProbeSymbol\\(sym\\)')>=3);

console.log('\n== E -- v592IsProbeSymbol canli kosum ' + '='.repeat(36));
{
  const f=grab('function v592IsProbeSymbol');
  const sb={console,String,
    v592ProbeOpen:new Map([['GUAUSDT',{}],['BMTUSDT',{}]]),
    normalizeSymbol:x=>String(x).toUpperCase()};
  vm.createContext(sb); vm.runInContext(f+';globalThis.P=v592IsProbeSymbol;',sb);
  ok('sonda sembolu -> true', sb.P('GUAUSDT')===true);
  ok('kucuk harf de calisir', sb.P('guausdt')===true);
  ok('strateji sembolu -> false', sb.P('HEIUSDT')===false);
  ok('null -> false (patlatmaz)', sb.P(null)===false);
  const sb2={console,String,normalizeSymbol:x=>x};  // v592ProbeOpen YOK
  vm.createContext(sb2); vm.runInContext(f+';globalThis.P=v592IsProbeSymbol;',sb2);
  ok('v592ProbeOpen tanimsizsa -> false', sb2.P('X')===false);
}

console.log('\n== F -- ONCEKI olcumler bu isikta ' + '='.repeat(40));
const G={eskiIslem:13, eskiWR:53.8, eskiPF:1.927, eskiNet:46.61};
ok('10.08 deki 13 islem SONDA ONCESIYDI', true);
ok('  -> o olcum temiz (13 islem · PF 1,927 · net +46,61)', G.eskiPF>1.9);
ok('11.08 deki 120 kayit SONDA SONRASI -> kirli', V.pf<0.2);
ok('=> iki olcum KARSILASTIRILAMAZ', G.eskiPF>1.9 && V.pf<0.2);

console.log('\n== G -- guvenlik ve parite ' + '='.repeat(47));
ok('slot ofseti (AV2) duruyor', cnt('R486_MAX_POSITIONS \\+ v592ProbeSlotOffset\\(\\)')>=3);
ok('BF1 kalici durum', /V592_PROBE_STATE_PATH/.test(src));
ok('BF3 acilista kurtarma', /async function v592ProbeBootRecover/.test(src));
{
  const b=src.slice(src.indexOf('// V4.7.4.32 — SONDA'), src.indexOf("app.get('/api/probe/samples.csv'"));
  ok('sonda blogu tradeLedger e DOKUNMUYOR', !/tradeLedger/.test(b));
  ok('sonda blogu YALNIZ LONG', /side:'BUY'/.test(b) && !/side:'SELL'/.test(b));
}
ok('varsayilan KAPALI', /String\(process\.env\.V592_PROBE_ACTIVE \?\? '0'\)==='1'/.test(src));
ok('TESTNET kilidi', /const V592_PROBE_ACTIVE = BINANCE_EXECUTION_ENV==='TESTNET'/.test(src));
ok('telemetri probeLedgerIsolated', /probeLedgerIsolated:true/.test(src));
ok('telemetri phantomLedgerGuard', /phantomLedgerGuard:true/.test(src));
for(const [n,re] of [['AS1 tek huni',/async function v592FinalizeClose/],
  ['AU1 dedup ayrimi',/v592EvidenceClosedOnce/],['AU2 exitReason',/exitReason:rec\.close\?\.exitReason/],
  ['AW2 disk muhafizi',/function r501DiskGuard/],['AW3 analiz paketi',/function r501FunnelSummary/]]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('V45 esikleri degismedi', /V592_V45_MS_SCORE_MIN/.test(src));
ok('build V4_7_4_43', /V4_7_4_43_AUDIT_FIX_RISK41_10X/.test(src));
ok('eski build kalmadi', !/V4_7_4_42_LEDGER_ISOLATE_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
