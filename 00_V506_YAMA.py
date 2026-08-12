# -*- coding: utf-8 -*-
# V5.0.6 — KAYIP EMIRLERI OLCULEBILIR YAP (saf telemetri, parite riski YOK)
import pathlib, hashlib, sys
p=pathlib.Path('server.js'); s=p.read_text(encoding='utf-8')
GIRDI='fecca1cc884052e5afb7d0f1becc6f2e3a0d1b72ed55679c81dd44b0354cad96'
h=hashlib.sha256(s.encode()).hexdigest()
if h!=GIRDI: sys.exit(f'BUILD DURDU: girdi SHA {h} != {GIRDI}')
log=[]
def rep(et,eski,yeni,adet=1):
    global s
    n=s.count(eski)
    if n!=adet: sys.exit(f'BUILD DURDU [{et}]: beklenen {adet}, bulunan {n}')
    s=s.replace(eski,yeni); log.append(f'{et}: {adet} yer')

# ══ V506-A: ORDER_ROUTE_ERROR FUNNEL'A ═════════════════════════════════
rep('V506_A_ROUTE_ERROR',
"    pushCritical('ORDER_ROUTE_ERROR', `${sym}: ${e.message}`);",
"""    pushCritical('ORDER_ROUTE_ERROR', `${sym}: ${e.message}`);
    // ══ V5.0.6 — KAYIP EMIR OLCULEBILIR OLSUN ═══════════════════════
    // OLCULDU 12.08: panelde ORDER_ROUTE_ERROR gorunuyordu ama funnel'da
    // 0 kayit vardi — yalnizca pushCritical'a gidiyordu. Bu yuzden "7 TACTICAL
    // kabul edildi, 3'u emir asamasina ulasti" derken kalan 4'un NEREDE
    // oldugunu sayamadim. Artik funnel'a yaziliyor: kayip emirler sayilabilir.
    try{ r501EvidenceFunnel({type:'ORDER_ROUTE_ERROR',action:'ORDER_ROUTE_ERROR',
      authority:'TESTNET_EXECUTION',symbol:normalizeSymbol(sym),decisionImpact:true,
      orderBlocking:true,reason:String(e&&e.message||e).slice(0,300)}); }catch(_){}""")

# ══ V506-B: GIRIS MUMU DRIFT BLOGU FUNNEL'A ════════════════════════════
rep('V506_B_DRIFT',
"       if(_drift>V592_ENTRY_CANDLE_MAX_DRIFT){",
"""       if(_drift>V592_ENTRY_CANDLE_MAX_DRIFT){
         // V5.0.6: drift blogu artik funnel'a da yazilir. Backtest girisi
         // candidateTs+180000 (725/725 sapmasiz) yani drift 0. Muhafiz DOGRU
         // calisiyor; sorun veriyi gec almamiz. Kac emrin bu yuzden dustugunu
         // ve drift dagilimini olcebilmek icin kayit sart.
         try{ r501EvidenceFunnel({type:'ENTRY_CANDLE_DRIFT_BLOCK',action:'ENTRY_CANDLE_DRIFT_BLOCK',
           authority:'EXACT_V592_EXECUTION',symbol:normalizeSymbol(sym),decisionImpact:true,
           orderBlocking:true,candleDrift:_drift,maxAllowed:V592_ENTRY_CANDLE_MAX_DRIFT,
           requestToSendMs:_lag,entryCandle:_entryCandle,nowCandle:_nowCandle,
           reason:`giris mumu gecti: drift ${_drift} > izin ${V592_ENTRY_CANDLE_MAX_DRIFT}`}); }catch(_){}""")

# ══ V506-C: TARAMA SURESI OLCUMU ═══════════════════════════════════════
rep('V506_C_SCAN_SURE',
"""      pushCritical('AUTO_SCAN_WATCHDOG', `Tarama ${Math.round(age/1000)}sn takıldı; kilit temizlendi`, {ageMs:age, phase:autoScanState.phase}, 'WARNING')""",
"""      // V5.0.6: watchdog olayi funnel'a da yazilir — tarama gecikmesi ile
      // giris mumu drift'i arasindaki iliski ancak boyle olculebilir.
      try{ r501EvidenceFunnel({type:'AUTO_SCAN_WATCHDOG',action:'SCAN_STUCK',
        authority:'AUTO_PIPELINE',symbol:autoScanState.currentSymbol||null,decisionImpact:false,
        ageMs:age,phase:autoScanState.phase,checked:autoScanState.checked||0,
        reason:`tarama ${Math.round(age/1000)}sn takildi`}); }catch(_){}
      pushCritical('AUTO_SCAN_WATCHDOG', `Tarama ${Math.round(age/1000)}sn takıldı; kilit temizlendi`, {ageMs:age, phase:autoScanState.phase}, 'WARNING')""")

import re
m=re.search(r"const LAZARUS_BUILD = '([^']+)'",s); assert m
s=s.replace(m.group(0),"const LAZARUS_BUILD = 'R493_V5_9_2_CANLI_EXACT_CLOSED1M_R495_V5_0_6_LOSS_TELEMETRY_NOPROBE_RISK41_10X'")
log.append('V506_D_SURUM')

k=[("route error funnel'a", "type:'ORDER_ROUTE_ERROR',action:'ORDER_ROUTE_ERROR'" in s),
   ("drift blogu funnel'a", "type:'ENTRY_CANDLE_DRIFT_BLOCK'" in s),
   ("watchdog funnel'a", "type:'AUTO_SCAN_WATCHDOG'" in s),
   ("drift degeri kaydediliyor", "candleDrift:_drift" in s),
   ("gecikme degeri kaydediliyor", "requestToSendMs:_lag" in s),
   ("muhafiz MANTIGI degismedi", "if(_drift>V592_ENTRY_CANDLE_MAX_DRIFT){" in s),
   ("pushCritical duruyor", "pushCritical('ORDER_ROUTE_ERROR'" in s),
   ("V505 oy duzeltmesi duruyor", "!takerVoteActive" in s),
   ("V504 taker sabiti duruyor", "V504_BACKTEST_TAKER_MIN_OBSERVED = 0.2199" in s),
   ("V503 on-filtre duruyor", "V503 testnet evren on-filtresi" in s),
   ("V502 kaldirac kilidi duruyor", "V502-A: KALDIRAC KILIDI" in s),
   ("parite kapisi duruyor", "v592BootParityGate" in s),
   ("SONDA yok", "/api/probe" not in s)]
for ad,ok in k:
    if not ok: sys.exit(f'BUILD DURDU [DOGRULAMA]: {ad}')
p.write_text(s,encoding='utf-8')
print('\n'.join('  ✓ '+x for x in log)); print(f'  dogrulama {len(k)}/{len(k)} GECTI')
print(f'  SHA: {hashlib.sha256(s.encode()).hexdigest()}')
