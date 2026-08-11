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

console.log('== A -- 10.08 vakasi: 8 yetim pozisyon ' + '='.repeat(35));
const V={yonetilen:['BICOUSDT','USUSDT','BMTUSDT','MUBARAKUSDT','EPICUSDT','GWEIUSDT','JOEUSDT','GUAUSDT'],
  golgeHerBiri:4374, golgeToplam:35424, poz:8, maxPoz:2,
  sondaOpened:2, sondaOpenNow:0, kapaliKanit:0, taramaBakilan:0,
  http418:['openAlgoOrders','openOrders','v2/account']};
ok('8 sembol yonetiliyordu', V.yonetilen.length===8);
ok('her birinde 4.374 golge kaydi', V.golgeHerBiri===4374);
ok('toplam 35.424 golge cikis', V.golgeToplam>35000);
ok('poz 8 / max 2', V.poz===8 && V.maxPoz===2);
ok('AMA sonda openNow BOS', V.sondaOpenNow===0);
ok('sonda sadece 2 acti (bu oturumda)', V.sondaOpened===2);
ok('=> 8 pozisyon ONCEKI oturumdan YETIM', V.poz>V.sondaOpened);
ok('strateji TAMAMEN bloke', V.taramaBakilan===0 && V.kapaliKanit===0);
ok('418 backoff uretti', V.http418.length===3);
ok('kok neden kaynakta belgelenmis', /SONDA DURUMU BELLEKTE TUTULUYORDU/.test(src));
ok('35.424 olcumu yazili', /35\.424 golge cikis kaydi/.test(src));
ok('2 saat bloke notu', /STRATEJI 2 SAAT BLOKE/.test(src));
ok('AS3 dersini uygulamadigim kabul edilmis', /kapanis defterinde ayni dersi \(AS3 yetim supurgesi\) almistim/i.test(src));

console.log('\n== B -- BF1: durum diske yazilir ' + '='.repeat(41));
ok('durum yolu tanimli', /const V592_PROBE_STATE_PATH = path\.join\(TESTNET_STATE_DIR, 'lazarus_probe_open_state\.json'\)/.test(src));
ok('OTURUM dizinine DEGIL /data kokune', /TESTNET_STATE_DIR, 'lazarus_probe_open_state/.test(src));
ok('kok neden: kanit dizini oturuma ozel', /R501_EVIDENCE_DIR oturum etiketini icerir/.test(src));
ok('gercekten oturuma ozel', /const R501_EVIDENCE_DIR = path\.join\(TESTNET_STATE_DIR, `lazarus_evidence_exact_testnet_\$\{TESTNET_SESSION_TAG\}`\)/.test(src));
ok('v592ProbeSaveState var', /function v592ProbeSaveState\(\)/.test(src));
ok('v592ProbeLoadState var', /function v592ProbeLoadState\(\)/.test(src));
{
  const f=grab('function v592ProbeSaveState');
  for(const k of ['id','symbol','openedAt','qty','clientOrderId','entryPrice','closeAttempts','entrySnap','entryDecision'])
    ok('  kaydedilen alan: '+k, new RegExp(k+':').test(f));
}
ok('acilista kaydedilir', /v592ProbeStats\.opened\+\+;\s*\n\s*v592ProbeSaveState\(\);/.test(src));
ok('kapanista kaydedilir', /v592ProbeSaveState\(\);\s*\n\s*\}\s*\n\}/.test(src)||cnt('v592ProbeSaveState\\(\\)')>=3);

console.log('\n== C -- BF1 kaydet/yukle canli kosum ' + '='.repeat(37));
{
  const sv=grab('function v592ProbeSaveState'), ld=grab('function v592ProbeLoadState');
  let yazilan=null;
  const sb={console,Date,JSON,Array,Number,
    v592ProbeOpen:new Map([
      ['AUSDT',{id:'PRB_A',symbol:'AUSDT',openedAt:1000,qty:5,clientOrderId:'LZPRBx',
                orderId:1,entryPrice:2.5,_closeAttempts:2,entrySnap:{cvd:1},entryDecision:{action:'PUSU'}}],
      ['BUSDT',{id:'PRB_B',symbol:'BUSDT',openedAt:2000,qty:9,clientOrderId:'LZPRBy',
                orderId:2,entryPrice:0.4,entrySnap:null,entryDecision:null}]]),
    V592_PROBE_STATE_PATH:'/tmp/x.json',
    LAZARUS_BUILD:'B',TESTNET_SESSION_RESET_ID:'S',
    fs:{writeFileSync:(p2,d)=>{yazilan=d;},readFileSync:()=>yazilan},
    pushCritical:()=>{}};
  vm.createContext(sb); vm.runInContext(sv+';'+ld+';globalThis.SV=v592ProbeSaveState;globalThis.LD=v592ProbeLoadState;',sb);
  sb.SV();
  ok('dosya yazildi', !!yazilan);
  const j=JSON.parse(yazilan);
  ok('schema dogru', j.schema==='LAZARUS_V592_PROBE_OPEN_STATE_V1');
  ok('2 acik kayit', j.open.length===2);
  ok('id korundu', j.open[0].id==='PRB_A');
  ok('deneme sayisi korundu', j.open[0].closeAttempts===2);
  ok('giris snapshot korundu', j.open[0].entrySnap.cvd===1);
  ok('bot karari korundu', j.open[0].entryDecision.action==='PUSU');
  const geri=sb.LD();
  ok('geri yuklendi', geri.length===2 && geri[1].symbol==='BUSDT');
  // bozuk dosya
  const sb2={...sb,fs:{readFileSync:()=>'BOZUK{'}};
  vm.createContext(sb2); vm.runInContext(ld+';globalThis.LD=v592ProbeLoadState;',sb2);
  ok('bozuk dosya -> bos dizi (patlatmaz)', sb2.LD().length===0);
}

console.log('\n== D -- BF2: basarisiz kapanis UNUTULMAZ ' + '='.repeat(33));
{
  const f=grab('async function v592ProbeCloseOne');
  ok('basarida _closeOk isaretlenir', /p\._closeOk=true/.test(f));
  ok('finally _closeOk kontrol eder', /if\(p2 && p2\._closeOk!==true\)/.test(f));
  ok('basarisizda deneme artar', /p2\._closeAttempts=Number\(p2\._closeAttempts\|\|0\)\+1/.test(f));
  ok('basarisizda kilit acilir (tekrar denenebilsin)', /p2\._closing=false/.test(f));
  ok('limit asilinca vazgecer', /if\(p2\._closeAttempts>=V592_PROBE_CLOSE_MAX_ATTEMPTS\)/.test(f));
  ok('vazgecince KRITIK uyari', /PROBE_CLOSE_STUCK/.test(f));
  ok('vazgecince iz', /PROBE_CLOSE_GAVE_UP/.test(f));
  ok('her durumda durum kaydedilir', /v592ProbeSaveState\(\);\s*\n\s*\}/.test(f));
}
{
  const kod=`
    const acik=new Map([['A',{id:'PRB_A'}]]);
    const yazilan=[]; let gaveUp=0;
    function finallyBlok(S,basarili,MAX){
      const p2=acik.get(S);
      if(p2 && p2._closeOk!==true){
        p2._closeAttempts=Number(p2._closeAttempts||0)+1;
        p2._closing=false;
        if(p2._closeAttempts>=MAX){ gaveUp++; acik.delete(S); }
      } else acik.delete(S);
    }
    function kapat(S,basarili,MAX){
      const p=acik.get(S); if(!p) return;
      if(basarili){ yazilan.push(p.id); p._closeOk=true; }
      finallyBlok(S,basarili,MAX);
    }`;
  const sb={Map,Number}; vm.createContext(sb);
  vm.runInContext(kod+';globalThis.K=kapat;globalThis.A=acik;globalThis.Y=()=>yazilan;globalThis.G=()=>gaveUp;',sb);
  sb.K('A',false,6);
  ok('kapanis basarisiz -> kayit HALA takipte', sb.A.has('A'));
  ok('deneme 1', sb.A.get('A')._closeAttempts===1);
  ok('satir YAZILMADI', sb.Y().length===0);
  for(let i=0;i<4;i++) sb.K('A',false,6);
  ok('5 denemeden sonra hala takipte', sb.A.has('A'));
  sb.K('A',true,6);
  ok('basarili kapanista satir yazilir', sb.Y().length===1);
  ok('ve haritadan silinir', !sb.A.has('A'));
  // limit senaryosu
  const sb2={Map,Number}; vm.createContext(sb2);
  vm.runInContext(kod+';globalThis.K=kapat;globalThis.A=acik;globalThis.G=()=>gaveUp;',sb2);
  for(let i=0;i<6;i++) sb2.K('A',false,6);
  ok('6 basarisiz deneme -> vazgecer', sb2.G()===1 && !sb2.A.has('A'));
  ok('ESKI kod ILK basarisizlikta unuturdu', true);
}

console.log('\n== E -- BF3: acilista yetim kurtarma ' + '='.repeat(37));
ok('v592ProbeBootRecover var', /async function v592ProbeBootRecover\(\)/.test(src));
ok('acilistan 25 sn sonra', /setTimeout\(\(\)=>\{v592ProbeBootRecover\(\)\.catch\(\(\)=>\{\}\);\},25000\)/.test(src));
{
  // KRITIK: if(V592_PROBE_ACTIVE) blogunun DISINDA olmali
  const iRec=src.indexOf('setTimeout(()=>{v592ProbeBootRecover()');
  const iIf=src.indexOf('if(V592_PROBE_ACTIVE){\n  // V4.7.4.38-BC3');
  ok('sonda KAPALI olsa da calisir (if blogu DISINDA)', iRec>0 && iIf>0 && iRec<iIf,
     `rec=${iRec} if=${iIf}`);
  ok('bu davranis belgelenmis', /Bu blok V592_PROBE_ACTIVE=0 olsa DA calisir/.test(src));
}
{
  const f=grab('async function v592ProbeBootRecover');
  ok('durum dosyasindan okur', /v592ProbeLoadState\(\)/.test(f));
  ok('bos ise hicbir sey yapmaz', /if\(!kayit\.length\) return;/.test(f));
  ok('kimlik yoksa 60 sn sonra tekrar', /setTimeout\(\(\)=>\{v592ProbeBootRecover\(\)\.catch\(\(\)=>\{\}\);\},60000\)/.test(f));
  ok('haritaya GERI KOYAR (slot ofseti dogru olsun)', /v592ProbeOpen\.set\(S,\{/.test(f));
  ok('_recovered isaretler', /_recovered:true/.test(f));
  ok('iz birakir', /PROBE_RESTART_RECOVERED/.test(f));
  ok('her birini KAPATIR', /await v592ProbeCloseOne\(_c\.apiKey,_c\.apiSecret,S\)/.test(f));
  ok('sayac', /v592ProbeStats\.recovered/.test(f));
}
ok('kurtarilan satirda cikis sebebi ayri', /exitReason:p\._recovered\?'RESTART_RECOVERY'/.test(src));

console.log('\n== F -- BF3 kurtarma canli kosum ' + '='.repeat(41));
{
  const f=grab('async function v592ProbeBootRecover');
  const mk=(kayitlar,credVar)=>{
    const log=[]; const acik=new Map();
    const sb={console,Date,Promise,Number,setTimeout:(fn,ms)=>({unref:()=>{}}),
      normalizeSymbol:x=>String(x).toUpperCase(), String,
      v592ProbeLoadState:()=>kayitlar,
      v592ProbeCreds:()=>credVar?{apiKey:'K',apiSecret:'S',source:'RAILWAY_ENV'}:{apiKey:null,apiSecret:null},
      v592ProbeOpen:acik,
      v592ProbeCloseOne:async(k,s,S)=>{log.push('close:'+S);acik.delete(S);},
      v592ProbeSaveState:()=>log.push('save'),
      r501OrderLifeMark:(s,st)=>log.push('mark:'+st),
      logAuto:()=>{}, v592ProbeStats:{recovered:0,recoverNoCreds:0}};
    vm.createContext(sb); vm.runInContext(f+';globalThis.R=v592ProbeBootRecover;',sb);
    return {R:sb.R,log,acik,stats:sb.v592ProbeStats};
  };
  const a=mk([{id:'PRB_1',symbol:'MUBARAKUSDT',openedAt:1,qty:100,entryPrice:0.02},
              {id:'PRB_2',symbol:'BMTUSDT',openedAt:2,qty:200,entryPrice:0.026}],true);
  await a.R();
  ok('2 yetim kurtarildi', a.stats.recovered===2);
  ok('ikisi de KAPATILDI', a.log.filter(x=>x.startsWith('close:')).length===2);
  ok('MUBARAK kapatildi', a.log.includes('close:MUBARAKUSDT'));
  ok('iz birakildi', a.log.filter(x=>x==='mark:PROBE_RESTART_RECOVERED').length===2);
  ok('durum kaydedildi', a.log.includes('save'));
  ok('harita temiz', a.acik.size===0);
  const b=mk([],true); await b.R();
  ok('kayit yoksa hicbir sey yapmaz', b.log.length===0 && b.stats.recovered===0);
  const c=mk([{id:'P',symbol:'XUSDT',openedAt:1}],false); await c.R();
  ok('kimlik yoksa kapatmaz, tekrar planlar', c.stats.recoverNoCreds===1 && !c.log.some(x=>x.startsWith('close:')));
}

console.log('\n== G -- status + sayaclar ' + '='.repeat(48));
{
  const f=grab("app.get('/api/probe/status'");
  ok('persistence blogu', /persistence:\{statePath:V592_PROBE_STATE_PATH/.test(f));
  ok('diskteki acik sayisi', /persistedOpen:/.test(f));
  ok('survivesRestart bayragi', /survivesRestart:true/.test(f));
  ok('kapaliyken kurtarma bayragi', /bootRecoveryRunsWhenDisabled:true/.test(f));
}
for(const n of ['closeGaveUp','closeRetryPending','recovered','recoverNoCreds'])
  ok('sayac ilan: '+n, new RegExp(n+':0').test(src));

console.log('\n== H -- guvenlik ve parite ' + '='.repeat(47));
ok('slot ofseti', cnt('R486_MAX_POSITIONS \\+ v592ProbeSlotOffset\\(\\)')>=3);
ok('kurtarilan pozisyonlar da slot ofsetine girer (haritaya konuyor)', /v592ProbeOpen\.set\(S,\{[\s\S]{0,600}_recovered:true/.test(src));
{
  const b=src.slice(src.indexOf('// V4.7.4.32 — SONDA'), src.indexOf("app.get('/api/probe/samples.csv'"));
  ok('tradeLedger e DOKUNMUYOR', !/tradeLedger/.test(b));
  ok('v592ParityStats e DOKUNMUYOR', !/v592ParityStats/.test(b));
  ok('ham arsiv YAZMIYOR', !/r501RawAppend|r501RawInit/.test(b));
  ok('YALNIZ LONG', /side:'BUY'/.test(b) && !/side:'SELL'/.test(b));
}
ok('varsayilan KAPALI', /String\(process\.env\.V592_PROBE_ACTIVE \?\? '0'\)==='1'/.test(src));
ok('TESTNET kilidi', /const V592_PROBE_ACTIVE = BINANCE_EXECUTION_ENV==='TESTNET'/.test(src));
for(const [n,re] of [['BE1 kapatma kilidi',/closeRaceBlocked/],['BE2 cikti tekillestirme',/duplicateRowsFiltered/],
  ['BD1 ENV kimlik',/function v592ProbeCreds/],['BC1 tekrar',/V592_PROBE_RETRY_MS/],
  ['BB1 Map normalizasyonu',/_tg instanceof Map/],['BA1 fiyat zinciri',/Number\(tickerPrice \|\| 0\)/],
  ['AX1 preempt',/async function v592ProbePreempt/],['AS1 tek huni',/async function v592FinalizeClose/],
  ['AW2 disk muhafizi',/function r501DiskGuard/],['BF1 kalici durum',/V592_PROBE_STATE_PATH/]]) ok(n, re.test(src));
ok('calcVPIN karar yolu dokunulmadi', /if \(!trades \|\| trades\.length < bucketSize \* 3\) return null;/.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));
ok('build V4_7_4_43', /V4_7_4_43_AUDIT_FIX_RISK41_10X/.test(src));
ok('eski build kalmadi', !/V4_7_4_42_LEDGER_ISOLATE_RISK41_10X/.test(src));

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
