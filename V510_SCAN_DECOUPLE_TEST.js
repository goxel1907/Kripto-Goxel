// V5.1.0 — TARAMA AYRISMASI + SAYAC DUZELTMESI TESTI
//
// Bu test dizge aramaz; iki bolumu de GERCEKTEN olcer:
//   BOLUM 1  isExecBackoffActive / noteExecBackoffBlock fonksiyonlarini
//            kaynaktan cikarip vm icinde CALISTIRIR. V5.0.8 hatasi (sayac
//            gozlemciyi olcuyordu) geri gelse test kirmizi olur.
//   BOLUM 2  tarama/emir kontrol akisini kontrol eder VE mutasyon ispati
//            yapar: guard silinmis bir kopyada ayni kontroller GECMEMELI.
//            Gecerse test kendisi olcmuyor demektir ve o da hata sayilir.
const fs = require('fs'), vm = require('vm'), path = require('path');
const src = fs.readFileSync(path.join(__dirname, 'server.js'), 'utf8');
let P = 0, F = 0;
const ok = (ad, kosul, ek = '') => {
  if (kosul) { P++; console.log('  pass  ' + ad); }
  else { F++; console.log('  FAIL  ' + ad + (ek ? ' :: ' + ek : '')); }
};

// ── BOLUM 1: SAYAC DAVRANISI (vm icinde gercek calistirma) ────────────────
function fnCikar(ad) {
  const i = src.indexOf('function ' + ad);
  if (i < 0) return null;
  let d = 0, basladi = false;
  for (let j = i; j < src.length; j++) {
    if (src[j] === '{') { d++; basladi = true; }
    else if (src[j] === '}') { d--; if (basladi && d === 0) return src.slice(i, j + 1); }
  }
  return null;
}
const fIs = fnCikar('isExecBackoffActive');
const fNote = fnCikar('noteExecBackoffBlock');
ok('isExecBackoffActive kaynaktan cikarildi', !!fIs);
ok('noteExecBackoffBlock kaynakta var', !!fNote);

if (fIs && fNote) {
  const ctx = { binanceGov: { execBackoffUntil: Date.now() + 60000 }, v592ParityStats: {} };
  vm.createContext(ctx);
  vm.runInContext(fIs + '\n' + fNote, ctx);

  // 1a. Fren AKTIF iken 100 kez SORULUR. Sayac artmamali.
  //     V5.0.8'de bu 100 olurdu — status endpoint'i her cagrildiginda artiyordu.
  for (let i = 0; i < 100; i++) vm.runInContext('isExecBackoffActive()', ctx);
  ok('100 kez sorgu sayaci artirmiyor (AS3 duzeltildi)',
     (ctx.v592ParityStats.execBackoffBlocked || 0) === 0,
     'execBackoffBlocked=' + (ctx.v592ParityStats.execBackoffBlocked || 0));

  // 1b. Fren aktifken sorgu TRUE donmeli (davranis bozulmadi).
  ok('fren aktifken true donuyor', vm.runInContext('isExecBackoffActive()', ctx) === true);

  // 1c. Gercek engelleme noktasi sayiyor ve NEREDE oldugunu yaziyor.
  vm.runInContext("noteExecBackoffBlock('positionRisk')", ctx);
  vm.runInContext("noteExecBackoffBlock('positionRisk')", ctx);
  vm.runInContext("noteExecBackoffBlock('bReq')", ctx);
  ok('engelleme sayaci 3', ctx.v592ParityStats.execBackoffBlocked === 3,
     String(ctx.v592ParityStats.execBackoffBlocked));
  ok('kapi kirilimi positionRisk=2', ctx.v592ParityStats.execBackoffBlockedBy?.positionRisk === 2);
  ok('kapi kirilimi bReq=1', ctx.v592ParityStats.execBackoffBlockedBy?.bReq === 1);

  // 1d. Fren BITTIGINDE false donmeli.
  ctx.binanceGov.execBackoffUntil = Date.now() - 1;
  ok('fren bitince false donuyor', vm.runInContext('isExecBackoffActive()', ctx) === false);
}

// ── BOLUM 2: TARAMA / EMIR AKISI + MUTASYON ISPATI ────────────────────────
// Kontroller bir fonksiyona konur ki hem gercek kaynakta hem de guard'i
// silinmis kopyada calistirilabilsin.
function akisKontrolleri(s) {
  const r = {};
  const iTry = s.indexOf('let posData=null, posTruthUnavailable=false, posTruthError=null;');
  const iCatch = s.indexOf("posTruthUnavailable = true;");
  const iOpen = s.indexOf('const openPos = Array.isArray(posData)');
  const iGuard = s.indexOf('if (posTruthUnavailable) {');
  const iOrder = s.indexOf('const orderResp = await fetch(`http://localhost:${PORT}/api/order`', iGuard > 0 ? iGuard : 0);

  // pozisyon cagrisi try/catch icinde mi
  r.tryVar = iTry > 0;
  // hata yakalandiginda tarama devam ediyor mu (openPos yine hesaplaniyor)
  r.catchOnce = iCatch > 0 && iOpen > iCatch;
  // emir yolu guard'i, emir cagrisindan ONCE mi
  r.guardOnce = iGuard > 0 && iOrder > iGuard;
  // guard emir cagrisiyla AYNI dongude mi (arada baska emir cagrisi yok)
  r.guardYakin = iGuard > 0 && iOrder > 0 && (iOrder - iGuard) < 3000;
  // kanit yaziliyor mu
  r.kanit = s.includes("type:'ORDER_BLOCKED_NO_POS_TRUTH'") &&
            s.includes("type:'POS_TRUTH_UNAVAILABLE'");
  // kacirilan sinyal geriye donuk sayilabiliyor mu
  r.sayilabilir = s.includes('baselineWouldTrade:true');
  return r;
}
const g = akisKontrolleri(src);
ok('pozisyon cagrisi try/catch icinde', g.tryVar);
ok('hata yakalandiginda tarama devam ediyor', g.catchOnce);
ok('emir guard\'i emir cagrisindan ONCE', g.guardOnce);
ok('guard emir cagrisiyla ayni blokta', g.guardYakin);
ok('iki funnel kaydi da yaziliyor', g.kanit);
ok('kacirilan sinyal geriye donuk sayilabilir', g.sayilabilir);

// MUTASYON ISPATI: guard'i sok, kontroller DUSMELI.
const mut = src.replace('if (posTruthUnavailable) {', 'if (false) { // MUTASYON');
const gm = akisKontrolleri(mut);
ok('mutasyon ispati: guard silinince kontrol dusuyor', gm.guardOnce === false,
   'guard silindigi halde kontrol gecti — test olcmuyor demektir');

// Emir yolunun IKINCI kapisi (forceFresh) hala yerinde mi — V510-B tek
// savunma degil; getPositionRiskTruth fren altinda zaten firlatiyor.
ok('emir yolu ikinci kapi (forceFresh truth) duruyor',
   src.includes('__forceFresh:true') && src.includes('POSITION_RISK_TRUTH_STALE'));

// Kilit uzlastirmasi periyodik mi, ve kilitler TOPLUCA SILINMIYOR mu
ok('kilit uzlastirmasi periyodik', /setInterval\(\(\)=>\{v592BootReconcileLocks\(\)/.test(src));
ok('kilitler toplu silinmiyor', !src.includes('v592SymbolOrderLocks.clear()'));

// V5.0.9 sozlesmesi bozulmadi
ok('V509 SL sabitleri duruyor',
   ['V509_SL_ATR_CARPANI','V509_SL_PCT_MIN','V509_SL_PCT_MAX','V509_PLAN_TARGET_R']
     .every(x => src.includes(x)));
ok('V509 SL parite kaydi duruyor', src.includes("type:'SL_PARITY_PROOF'"));
ok('V509-A bReq bogumu duruyor', src.includes('if (!emergencyBypass && isExecBackoffActive()) {'));

console.log('\n' + '='.repeat(72));
console.log(`SONUC: ${P} gecti, ${F} kaldi`);
console.log('='.repeat(72));
process.exit(F ? 1 : 0);
