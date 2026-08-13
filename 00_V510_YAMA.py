# -*- coding: utf-8 -*-
# V5.1.0 — TARAMA IMZALI CAGRIDAN AYRILDI + TELEMETRI DUZELTMELERI
#
# OLCUM (13.08, V5.0.9 canli oturumu, /api/backtest-parity/status):
#   waitR493Gate 0 · waitV45Selector 0 · waitR495 0 · waitMechanical 0
#   freshCacheHits 0 · symbolLocks 0 · marketTimeSyncedAt 0
#   -> selector bu oturumda BIR KEZ BILE calismadi. "Az" degil, SIFIR.
#   panel: "V7 positionRisk son basarili —" -> imzali hesap cagrisi hic tutmadi.
#   execBackoffBlockedAtBreq ANAHTARI YOK -> V509-A bogumu HIC atesalmadi.
import pathlib, hashlib, sys, re
p=pathlib.Path('server.js'); s=p.read_text(encoding='utf-8')
GIRDI='380077ddbe1af2c03b47c00f344910252855458820aa95721f8e0d02a7fa1b77'
h=hashlib.sha256(s.encode()).hexdigest()
if h!=GIRDI: sys.exit(f'BUILD DURDU: girdi SHA {h} != {GIRDI}')
log=[]
def rep(et,eski,yeni,adet=1):
    global s
    n=s.count(eski)
    if n!=adet: sys.exit(f'BUILD DURDU [{et}]: beklenen {adet}, bulunan {n}')
    s=s.replace(eski,yeni); log.append(f'{et}: {adet} yer')

# ══ V510-A: TARAMA IMZALI POZISYON CAGRISINA BAGIMLI DEGIL ═════════════
rep('V510_A_TARAMA_AYRILDI',
"""    // 1. Mevcut pozisyonları kontrol et
    const posData = await getPositionRiskCached(apiKey,apiSecret);
    const openPos = Array.isArray(posData)
      ? posData.filter(p=>Math.abs(parseFloat(p.positionAmt))>0)
      : [];
    autoScanState.livePositions = openPos.length;
    autoScanState.positionCount = openPos.length;""",
"""    // 1. Mevcut pozisyonları kontrol et
    // ══ V5.1.0 — TARAMA IMZALI CAGRIYA BAGIMLI DEGIL ═══════════════════
    // OLCULDU 13.08 (V509 oturumu): waitV45Selector 0, waitR495 0,
    // freshCacheHits 0, marketTimeSyncedAt 0 -> selector bu oturumda BIR KEZ
    // BILE calismadi. Panel "positionRisk son basarili —" diyordu: imzali
    // hesap cagrisi boot'tan beri hic tutmamis (paylasimli Railway IP'sinde
    // testnet 418'i). getPositionRiskCached cache bossa FIRLATIYOR (R95 dali);
    // firlatinca runAutoScan POZISYON_KONTROL fazinda oluyor ve hicbir sembol
    // degerlendirilmiyordu.
    //
    // Oysa aday uretimi + funnel kaniti YALNIZ PUBLIC LIVE veriyle olur;
    // imzali cagri gerektirmez. Uc satir yukaridaki kendi yorumumuz da bunu
    // vaat ediyordu: "Testnet 418'i taramayi durdurmaz; aday/funnel kaniti
    // yazilmaya devam eder, yalnizca emir gonderimi engellenir."
    // Kod o vaadi tutmuyordu. Simdi tutuyor:
    //   - pozisyon gercegi alinamazsa TARAMA DEVAM eder (kanit birikir)
    //   - EMIR YOLU o turda SERT KAPALI (V510-B)
    // Guvenlik: emir yolu zaten getPositionRiskTruth (forceFresh) kullanir ve
    // o da firlatir; V510-B ikinci ve acik bir kapidir. Ikisi de fail-closed.
    let posData=null, posTruthUnavailable=false, posTruthError=null;
    try {
      posData = await getPositionRiskCached(apiKey,apiSecret);
    } catch(e) {
      posTruthUnavailable = true;
      posTruthError = String(e?.message||e).slice(0,120);
      try{ v592ParityStats.scanWithoutPosTruth=(v592ParityStats.scanWithoutPosTruth||0)+1; }catch(_){}
      try{ if(typeof r501EvidenceFunnel==='function') r501EvidenceFunnel({
        type:'POS_TRUTH_UNAVAILABLE',action:'SCAN_CONTINUES_ORDERS_BLOCKED',
        authority:'TESTNET_EXECUTION',symbol:null,decisionImpact:false,orderBlocking:true,
        reason:posTruthError}); }catch(_){}
      logAuto(`⚠️ Pozisyon gerçeği alınamadı — tarama sürüyor, emir yolu kapalı: ${posTruthError}`);
    }
    const openPos = Array.isArray(posData)
      ? posData.filter(p=>Math.abs(parseFloat(p.positionAmt))>0)
      : [];
    autoScanState.posTruthUnavailable = posTruthUnavailable;
    autoScanState.posTruthError = posTruthError;
    autoScanState.livePositions = openPos.length;
    autoScanState.positionCount = openPos.length;""")

# ══ V510-B: POZISYON GERCEGI YOKSA EMIR YOK ════════════════════════════
rep('V510_B_EMIR_KAPALI',
"        logAuto(`🎯 Sinyal: ${coin.symbol} ",
"""        // ══ V5.1.0 — POZISYON GERCEGI YOKSA EMIR GONDERILMEZ ════════════
        // V510-A taramanin devam etmesini saglar; buraya gelen aday kaniti
        // yazilir ama emir CIKMAZ. Sebep: imzali cagri tutmadiysa openPos bu
        // turda 0 gorunur; max-pozisyon mantigi guvenilmez olur.
        // baselineWouldTrade:true -> bu kayit "backtest islem acardi" demektir;
        // 72 saatlik olcumde kac sinyalin sirf altyapi yuzunden kacirildigi
        // buradan sayilir.
        if (posTruthUnavailable) {
          try{ v592ParityStats.orderBlockedNoPosTruth=(v592ParityStats.orderBlockedNoPosTruth||0)+1; }catch(_){}
          try{ if(typeof r501EvidenceFunnel==='function') r501EvidenceFunnel({
            type:'ORDER_BLOCKED_NO_POS_TRUTH',action:'WOULD_ORDER',
            authority:'EXACT_V592_EXECUTION',symbol:normalizeSymbol(coin.fullSymbol||coin.symbol),
            decisionImpact:true,orderBlocking:true,baselineWouldTrade:true,
            reason:`pozisyon gercegi yok: ${posTruthError||'bilinmiyor'}`}); }catch(_){}
          logAuto(`⛔ ${coin.symbol} sinyal VAR ama pozisyon gerçeği yok — emir gönderilmedi (kanıt yazıldı)`);
          markAutoSkip(coin.symbol, 'Pozisyon gerçeği yok — emir kapalı', {rec:recommendation, score});
          continue;
        }
        logAuto(`🎯 Sinyal: ${coin.symbol} """)

# ══ V510-C: BOZUK SAYAC DUZELTILDI (gozlemciyi olcuyordu) ══════════════
rep('V510_C_SAYAC',
"""function isExecBackoffActive() {
  const aktif = Date.now() < Number(binanceGov.execBackoffUntil || 0);
  // V5.0.8: yasak sirasinda engellenen cagri sayilir. Bu sayac dusuyorsa
  // duzeltme calisiyordur; yuksek kaliyorsa cagri yollari hala zorluyordur.
  if(aktif){
    try{ v592ParityStats.execBackoffBlocked=(v592ParityStats.execBackoffBlocked||0)+1; }catch(_){}
  }
  return aktif;
}""",
"""function isExecBackoffActive() {
  // V5.1.0 DUZELTME: V5.0.8'de sayac BU FONKSIYONUN ICINDEYDI. Fonksiyonu
  // status endpoint'i de cagiriyor (execBackoffActive: isExecBackoffActive()),
  // yani JSON'a her bakista sayi artiyordu. Engellenen cagriyi degil GOZLEMCIYI
  // olcuyordu -> devir belgesindeki AS3 sinifi. Sayac artik gercek engelleme
  // noktalarina tasindi ve HANGI kapinin kestigi ayri ayri yaziliyor.
  return Date.now() < Number(binanceGov.execBackoffUntil || 0);
}
// V5.1.0: gercek engelleme noktasi. AS2 (sayacsiz erken return/throw) icin:
// her erken cikisin kendi sayaci var ve nerede kesildigi kayitli.
function noteExecBackoffBlock(nerede) {
  try{
    v592ParityStats.execBackoffBlocked=(v592ParityStats.execBackoffBlocked||0)+1;
    v592ParityStats.execBackoffBlockedBy=v592ParityStats.execBackoffBlockedBy||{};
    v592ParityStats.execBackoffBlockedBy[nerede]=(v592ParityStats.execBackoffBlockedBy[nerede]||0)+1;
  }catch(_){}
}""")

# ══ V510-C2: uc engelleme noktasina sayac ══════════════════════════════
rep('V510_C2_POSRISK',
"    throw makeBinanceBackoffError('Binance geçici istek freni', Math.ceil(getExecBackoffMs()/1000), 418);",
"""    // V5.1.0: bu firlatis panelde gorunmuyordu. lastErrorType yazilmadigi icin
    // panel "hata serisi 0" gosterirken 10 saniyede bir hata veriyorduk (AS2).
    noteExecBackoffBlock('positionRisk');
    posRiskCache.lastError='EXEC_BACKOFF';
    posRiskCache.lastErrorAt=Date.now();
    posRiskCache.lastErrorType='EXEC_BACKOFF';
    throw makeBinanceBackoffError('Binance geçici istek freni', Math.ceil(getExecBackoffMs()/1000), 418);""")

rep('V510_C3_ACCOUNT',
"    throw makeBinanceBackoffError('Signed account snapshot backoff',Math.ceil(getExecBackoffMs()/1000),418);",
"""    noteExecBackoffBlock('accountSnapshot');
    throw makeBinanceBackoffError('Signed account snapshot backoff',Math.ceil(getExecBackoffMs()/1000),418);""")

rep('V510_C4_BREQ',
"    try{ v592ParityStats.execBackoffBlockedAtBreq=(v592ParityStats.execBackoffBlockedAtBreq||0)+1; }catch(_){}",
"""    try{ v592ParityStats.execBackoffBlockedAtBreq=(v592ParityStats.execBackoffBlockedAtBreq||0)+1; }catch(_){}
    noteExecBackoffBlock('bReq');""")

# ══ V510-D: KILIT UZLASTIRMASI PERIYODIK ═══════════════════════════════
rep('V510_D_KILIT',
"setTimeout(()=>{v592BootReconcileLocks().catch(e=>console.log('[V4742] boot reconcile:',String(e?.message||e).slice(0,120)))",
"""// V5.1.0: OLCULDU — lockedSymbols ["BEATUSDT"] varken symbolLocks 0 idi.
// Yani kilit bu oturumda ALINMADI, diskten geri yuklendi ve boot reconcile
// onu cozemedi (buyuk ihtimalle imzali cagri 418 yedigi icin). Boot'ta tek
// deneme vardi; basarisiz olursa sembol SONSUZA DEK kapali kaliyordu.
// Kilit silinmiyor (duplicate emir riski) — uzlastirma 30 dakikada bir
// TEKRARLANIYOR; cagrilar duzelince kilit kendiliginden cozulur.
setInterval(()=>{v592BootReconcileLocks().catch(e=>console.log('[V510] periyodik reconcile:',String(e?.message||e).slice(0,120)))},30*60*1000);
setTimeout(()=>{v592BootReconcileLocks().catch(e=>console.log('[V4742] boot reconcile:',String(e?.message||e).slice(0,120)))""")

# ══ V510-F: PANEL POZISYON GERCEGI DURUMUNU GORSUN ═════════════════════
rep('V510_F_HEALTH',
"""        phase: scan.phase || null,
        currentSymbol: scan.currentSymbol || null,""",
"""        phase: scan.phase || null,
        // V5.1.0: panel "neden emir yok" sorusunu artik kendi basina yanitlayabilsin.
        // Eskiden tarama sessizce dusuyordu ve panelde hicbir iz yoktu.
        posTruthUnavailable: !!scan.posTruthUnavailable,
        posTruthError: scan.posTruthError || null,
        currentSymbol: scan.currentSymbol || null,""")

m=re.search(r"const LAZARUS_BUILD = '([^']+)'",s); assert m
s=s.replace(m.group(0),"const LAZARUS_BUILD = 'R493_V5_9_2_CANLI_EXACT_CLOSED1M_R495_V5_1_0_SCAN_DECOUPLED_SL_CONTRACT_LOCKED_RISK41_10X'")
log.append('V510_E_SURUM')

k=[("tarama try/catch icinde", "let posData=null, posTruthUnavailable=false, posTruthError=null;" in s),
   ("  tarama devam ediyor", "SCAN_CONTINUES_ORDERS_BLOCKED" in s),
   ("  scanWithoutPosTruth sayaci", "scanWithoutPosTruth" in s),
   ("emir yolu sert kapali", "if (posTruthUnavailable) {" in s),
   ("  ORDER_BLOCKED_NO_POS_TRUTH", "type:'ORDER_BLOCKED_NO_POS_TRUTH'" in s),
   ("  kacirilan sinyal sayilabilir", "baselineWouldTrade:true" in s),
   ("  emir bloku logAuto'dan ONCE", s.index("if (posTruthUnavailable) {") < s.index("logAuto(`🎯 Sinyal:")),
   ("bozuk sayac kaldirildi", "if(aktif){" not in s.split('function isExecBackoffActive')[1][:400]),
   ("  noteExecBackoffBlock var", "function noteExecBackoffBlock(nerede)" in s),
   ("  kapi kirilimi", "execBackoffBlockedBy" in s),
   ("  positionRisk sayaci", "noteExecBackoffBlock('positionRisk')" in s),
   ("  accountSnapshot sayaci", "noteExecBackoffBlock('accountSnapshot')" in s),
   ("  bReq sayaci", "noteExecBackoffBlock('bReq')" in s),
   ("  panel hata tipi yaziliyor", "posRiskCache.lastErrorType='EXEC_BACKOFF'" in s),
   ("kilit uzlastirmasi periyodik", "periyodik reconcile" in s),
   ("panel pozisyon gercegini goruyor", "posTruthUnavailable: !!scan.posTruthUnavailable" in s),
   ("  kilit SILINMIYOR", "v592SymbolOrderLocks.clear()" not in s),
   ("V509 SL sabitleri duruyor", all(x in s for x in ("V509_SL_ATR_CARPANI","V509_SL_PCT_MIN","V509_PLAN_TARGET_R"))),
   ("V509 SL parite kaydi duruyor", "type:'SL_PARITY_PROOF'" in s),
   ("V509-A bogum duruyor", "if (!emergencyBypass && isExecBackoffActive()) {" in s),
   ("V508 duruyor", "V508_BACKOFF_MAX_SEC" in s),
   ("V507 duruyor", "V507_LEVERAGE_PROOF_MAX_ATTEMPTS" in s),
   ("V502 kaldirac kilidi", "V502-A: KALDIRAC KILIDI" in s),
   ("SONDA yok", "/api/probe" not in s),
   # DIKKAT: LAZARUS_LIVE_ARM kaynakta OLMALI — kapi onu ARIYOR. Ilk yazdigim
   # kontrol "hic gecmesin" diyordu ve build'i hakli olarak durdurdu. Dogru
   # kontrol: canli mod hala acik onay istiyor mu, ve onay kodu kaynakta
   # sabitlenmis mi (sabitlenmisse kapi anlamsizlasir).
   ("canli mod hala onay istiyor", "CANLI_SILAHLANDIRILMADI:LAZARUS_LIVE_ARM" in s),
   ("onay kodu kaynakta sabitlenmemis", s.count("'CANLI-PARA-ONAY'")==2)]
for ad,ok in k:
    if not ok: sys.exit(f'BUILD DURDU [DOGRULAMA]: {ad}')
p.write_text(s,encoding='utf-8')
print('\n'.join('  ✓ '+x for x in log)); print(f'  dogrulama {len(k)}/{len(k)} GECTI')
print(f'  SHA: {hashlib.sha256(s.encode()).hexdigest()}')
