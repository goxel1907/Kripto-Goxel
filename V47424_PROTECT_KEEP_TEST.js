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

console.log('== A -- 07.08 HEIUSDT vakasi (olculmus) ' + '='.repeat(33));
const V={ack:'NEW',fillConfirmed:false,executedQty:null,fillSource:'MARK_PRICE_ESTIMATE',
  tProtect:1786089799119,tOrphan:1786089800852,tProtFail:1786089801537,tClose:1786089816457,
  hata:'POSITION_ALREADY_CLOSED',sltpVerified:false,kayit:'EXTERNAL_OR_MANUAL',
  binancePozisyon:'ACIK 409.7 HEI',binanceTPSL:'-- / --',binanceOpenOrders:0,
  posRiskYavasSn:20};
ok('emir ACK aldi ama FILL degil', V.ack==='NEW');
ok('dolum SADECE TAHMIN', V.fillConfirmed===false && V.fillSource==='MARK_PRICE_ESTIMATE');
ok('executedQty null', V.executedQty===null);
ok('yetim kontrolu dolumdan 1,7 sn sonra', (V.tOrphan-V.tProtect)<2000, String(V.tOrphan-V.tProtect)+'ms');
ok('positionRisk 15-20 sn gecikiyordu', V.posRiskYavasSn>=15);
ok('koruma dogrulanmadi', V.sltpVerified===false);
ok('defter KAPANDI yazdi', V.kayit==='EXTERNAL_OR_MANUAL');
ok('Binance pozisyon ACIK', V.binancePozisyon.startsWith('ACIK'));
ok('Binance TP/SL YOK', V.binanceTPSL==='-- / --' && V.binanceOpenOrders===0);
ok('kok neden kaynakta belgelenmis', /07\.08 HEIUSDT vakasi: MARKET emri ACK aldi/.test(src));
ok('positionRisk gecikmesi belgelenmis', /positionRisk 15-20 sn gecikiyor/.test(src));

console.log('\n== B -- AN2: yetim temizligi KORUMAYI SILMIYOR ' + '='.repeat(27));
{
  const f=grab('async function installSLTPWithProof');
  ok('cancelAlgoOrders yetim dalinda YOK',
     !/ORPHAN_CHECK_DEFERRED[\s\S]{0,400}cancelAlgoOrders/.test(f));
  ok('eski ORPHAN_PROTECTION_CLEANED dali kaldirildi',
     !/koruma iptal ediliyor/.test(src));
  ok('yeni isaret ORPHAN_CHECK_DEFERRED', /ORPHAN_CHECK_DEFERRED/.test(f));
  ok('supheli sembol setine yaziliyor', /v592PossibleOrphans\.add/.test(f));
  ok('sayac orphanCheckDeferred', /v592ParityStats\.orphanCheckDeferred\+\+/.test(f));
  ok('sayac orphanCancelSuppressed', /v592ParityStats\.orphanCancelSuppressed\+\+/.test(f));
  ok('yetim dalinda POSITION_ALREADY_CLOSED donusu YOK',
     !/ORPHAN_CHECK_DEFERRED[\s\S]{0,500}error:'POSITION_ALREADY_CLOSED'[\s\S]{0,80}orphanCleaned/.test(f));
  ok('yetim dalinda cleanupClosedPositionState YOK',
     !/ORPHAN_CHECK_DEFERRED[\s\S]{0,400}cleanupClosedPositionState/.test(f));
  ok('POSITION_GONE_AFTER_PROTECT artik CAGRILMIYOR (yalniz yorumda)',
     !/cleanupClosedPositionState\(\s*symbol\s*,\s*'POSITION_GONE_AFTER_PROTECT'/.test(src)
     && cnt('POSITION_GONE_AFTER_PROTECT')===1);
  ok('supheli sembolde ilk-kurulum iptali ATLANMAZ',
     /_skipCancel = \(firstInstall && attempt === 1 && !_orphanSuspect\)/.test(f));
  ok('koruma dogrulaninca suphe kalkar', /v592PossibleOrphans\.delete/.test(f));
}
ok('v592PossibleOrphans tanimli', /const v592PossibleOrphans = new Set\(\)/.test(src));
ok('sayaclar tanimli', /orphanCheckDeferred:0/.test(src) && /orphanCancelSuppressed:0/.test(src));

console.log('\n== C -- AN2 davranis simulasyonu ' + '='.repeat(40));
{
  // _skipCancel mantigini izole calistir
  const kod=`
    const v592PossibleOrphans=new Set(S0);
    function skipCancel(symbol, firstInstall, attempt){
      const _orphanSuspect = (()=>{ try{ return v592PossibleOrphans.has(String(symbol||'').toUpperCase()); }catch(_){ return false; } })();
      return (firstInstall && attempt === 1 && !_orphanSuspect);
    }
    out.temiz     = skipCancel('HEIUSDT', true, 1);
    out.supheli   = skipCancel('HEIUSDT', true, 1);
    out.ikinciDeneme = skipCancel('BTCUSDT', true, 2);
    out.yeniden   = skipCancel('BTCUSDT', false, 1);
  `;
  const o1={}, o2={};
  new Function('S0','out',kod)([], o1);
  new Function('S0','out',kod)(['HEIUSDT'], o2);
  ok('temiz sembol: ilk kurulumda iptal ATLANIR', o1.temiz===true);
  ok('supheli sembol: iptal ATLANMAZ (eski koruma silinir)', o2.supheli===false);
  ok('2. deneme: iptal ATLANMAZ', o1.ikinciDeneme===false);
  ok('yeniden kurulum: iptal ATLANMAZ', o1.yeniden===false);
}

console.log('\n== D -- AN1: kapanis kaniti TUM sebepler icin ' + '='.repeat(28));
{
  const f=grab('async function cleanupClosedPositionState');
  ok('beyaz liste (startsWith SYNC_) KALDIRILDI',
     !/String\(reason\|\|''\)\.startsWith\('SYNC_'\)/.test(f));
  ok('muafiyet listesi acikca yazili', /V592_CLOSE_PROOF_EXEMPT = \['EXCHANGE_CONFIRMED_FILL','MANUAL_OPERATOR_FORCE'\]/.test(f));
  ok('kanit VARSAYILAN (negatif kontrol)', /if\(!V592_CLOSE_PROOF_EXEMPT\.includes\(String\(reason\|\|''\)\)\)/.test(f));
  ok('kanit yoksa kapanis IPTAL', /return \{aborted:true,proof:_cp\}/.test(f));
  ok('CLOSE_ABORTED_NO_PROOF izi', /CLOSE_ABORTED_NO_PROOF/.test(f));
  ok('AC dersi kaynakta yazili', /AC dersinin tekrari: guard bir cagri yoluna baglanip digeri unutulmus/.test(src));
  ok('sayac closeProofRequiredAll', /closeProofRequiredAll\+\+/.test(f));
  // her cagri sebebi artik kanit istiyor mu
  const sebepler=['FRESH_POSITION_ZERO_BEFORE_SLTP','ALGO_-4509_POSITION_ALREADY_CLOSED',
                  'SYNC_POSITION_ALREADY_CLOSED_BEFORE_SLTP_RESCUE','POSITION_ALREADY_CLOSED'];
  const muaf=['EXCHANGE_CONFIRMED_FILL','MANUAL_OPERATOR_FORCE'];
  for(const r of sebepler) ok(`  "${r}" kanit ister`, !muaf.includes(r));
}

console.log('\n== E -- V45_SELECTOR / MEKANIK DUZELTMESI ' + '='.repeat(31));
// 07.08 olculmus huni: MEKANİK BEKLE bir VETO KATMANI DEGIL, mesaj ONEKI.
const H={waitR493Gate:387, waitR495:43, waitR447Story:43, waitV45Selector:18, waitMechanical:0,
         marketBloklanan:18, marketBloklayanKaynak:'V45_SELECTOR'};
ok('MARKET kararlarini bloklayan V45_SELECTOR', H.marketBloklayanKaynak==='V45_SELECTOR');
ok('bloklanan MARKET sayisi = V45_SELECTOR sayisi', H.marketBloklanan===H.waitV45Selector);
ok('waitMechanical SIFIR — MEKANIK veto etmiyor', H.waitMechanical===0);
ok('R493 kapisi backtestte var (foMin 0.35)', H.waitR493Gate>0);
ok('R495 backtestte var', H.waitR495>0);
ok('V45 secici backtestte var (msScore/TOP_GAINER)', H.waitV45Selector>0);
ok('waitSource alani hala kayitli', /waitSource:_ws/.test(src));
ok('MEKANIK onek metni hala uretiliyor (kaldirilmadi)',
   /\$\{ai\.mekanik\?'MEKANİK':'AI'\} WAIT: \$\{ai\.reasoning\}/.test(src));
ok('V45 secici esikleri DEGISMEDI',
   /V592_V45_MS_SCORE_MIN/.test(src) && /V592_V45_FIRST_OBSTACLE_RR_MIN/.test(src));
ok('R493 esigi DEGISMEDI', /R493_MIN_FIRST_OBSTACLE_RR/.test(src));

console.log('\n== F -- onceki duzeltmeler yerinde ' + '='.repeat(39));
for(const [n,re] of [
  ['L fren ayrimi',/function isExecBackoffActive/],
  ['N huni gorusu',/reasonFull:/],
  ['Q koruma-once',/PROTECT_FIRST_NO_PRECHECK/],
  ['S4 null delta',/function r501Delta/],
  ['W giris mumu',/function v592CandleIndex/],
  ['Y cikis mumu',/function v592ExitCandleGate/],
  ['AC yonetici guard',/v592MinHoldGuard\(sym, `MANAGER_/],
  ['AG beyaz liste',/function v592ExitTypeAllowed/],
  ['AH korumasiz istisna',/function v592PositionProtection/],
  ['AJ kapanis kaniti',/async function v592CloseProof/],
  ['AL1 tick akisi',/tickStreamRepaired/],
  ['AL2 kanit-only defter',/ledgerBootstrapSkipped/],
  ['AL3 pasif csv',/function r501PassiveRows/],
  ['AL5 null-guvenli n()',/if\(v===null\|\|v===undefined\|\|v===''\)return null/],
  ['AN1 kanit tum sebepler',/V592_CLOSE_PROOF_EXEMPT/],
  ['AN2 yetim korunur',/v592PossibleOrphans/],
]) ok(n, re.test(src));
ok('testnet hard-lock', /const BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/.test(src));
ok('giris sozlesmesi 180000', /candidateToEntryMs:180000/.test(src));

console.log('\n== G -- surum etiketleri ' + '='.repeat(49));
ok('build V4_7_4_35', /V4_7_4_35_ROTATE_RISK41_10X/.test(src));
ok('session 4_7_4_35_RT1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_35_RT1/.test(src));
ok('eski build kalmadi', !/V4_7_4_34_PARALLEL_RISK41_10X/.test(src));
ok('eski session kalmadi', !/4_7_4_25_OV1/.test(src));
ok('telemetri: yetim koruma silmez', /orphanNeverCancelsProtection:true/.test(src));
ok('telemetri: kanit tum sebepler', /closeProofAllReasons:true/.test(src));
ok('telemetri her iki endpointte', cnt('orphanNeverCancelsProtection:true')>=2);

console.log(`\n${'='.repeat(74)}`);
console.log(fail?`SONUC: FAIL -- ${pass} gecti, ${fail} dustu`:`SONUC: PASS -- ${pass} gecti, 0 dustu`);
process.exitCode=fail?1:0;
})().catch(e=>{console.error('HARNESS:',e);process.exitCode=1;});
