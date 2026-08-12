# -*- coding: utf-8 -*-
# V5.0.4 — R495 TAKER ESIGI BACKTESTTEN TURETILDI
import pathlib, hashlib, sys
p=pathlib.Path('server.js'); s=p.read_text(encoding='utf-8')
GIRDI='6cd5794f5750aeb7704bad07ce2d66c441bd8af0525b31af91db0d8cc19af4dc'
h=hashlib.sha256(s.encode()).hexdigest()
if h!=GIRDI: sys.exit(f'BUILD DURDU: girdi SHA {h} != {GIRDI}')
log=[]
def rep(et,eski,yeni,adet=1):
    global s
    n=s.count(eski)
    if n!=adet: sys.exit(f'BUILD DURDU [{et}]: beklenen {adet}, bulunan {n}')
    s=s.replace(eski,yeni); log.append(f'{et}: {adet} yer')

# ══ V504-A: CLAMP TABANI + VARSAYILAN ══════════════════════════════════
rep('V504_A_TAKER_ESIGI',
"const R495_TAKER_RATIO_MIN = Math.max(.40, Math.min(.70, Number(process.env.R495_TAKER_RATIO_MIN || .50)));",
"""// ══ V5.0.4 — R495 TAKER ESIGI BACKTESTTEN TURETILDI ═════════════════
// OLCULDU: backtestin 1.461 kabul edilmis sinyalinde takerRatio
//   min 0,2199 · p05 0,4294 · medyan 0,5405 · max 0,9898
// Eski deger 0,50 backtestin DORTTE BIRINI keserdi (366/1461 = %25,1).
// Eski clamp tabani 0,40 ise ENV'den daha dusuk deger yazmayi IMKANSIZ
// kiliyordu — V5.0.1'de duzelttigimiz Math.max(0.70,...) tuzaginin aynisi.
//
// Diger uc R495 limiti zaten backtest verisinden turetilmisti (limit ~ gozlenen uc):
//   MAX_ENTRY_DRIFT_ATR 0,85 vs backtest max 0,848  -> 0 red
//   MIN_ENTRY_DRIFT_ATR -0,35 vs backtest min -0,149 -> 0 red
//   MAX_ADVERSE_ATR     0,85 vs backtest max 0,847  -> 0 red
// Dorduncusu turetilmemisti. 0,20 ile backtestin 1461/1461'i gecer.
//
// UYARI: MATCHED_SIGNALS'taki takerRatio SINYAL duzeyindedir; canli oy MUM
// basina bakar. Bu yuzden %25,1 gosterge niteligindedir, birebir degil.
// Kesin olan: 0,50 backtestten turetilmedi, 0,20 turetildi.
const R495_TAKER_RATIO_MIN = Math.max(.20, Math.min(.70, Number(process.env.R495_TAKER_RATIO_MIN || .20)));
const V504_BACKTEST_TAKER_MIN_OBSERVED = 0.2199;""")

# ══ V504-B: KAPIYA BACKTEST-TUREVLILIK DENETIMI ════════════════════════
rep('V504_B_KAPI',
"  if(!eq(R495_FINAL_RISK_PCT,4)) hata.push(`RISK_4_DEGIL:${R495_FINAL_RISK_PCT}`);",
"""  if(!eq(R495_FINAL_RISK_PCT,4)) hata.push(`RISK_4_DEGIL:${R495_FINAL_RISK_PCT}`);
  // V504: R495'in DORT limiti de backtest verisinden turetilmis olmali.
  // Elle konmus "makul gorunen" sayi kabul edilmez.
  if(Number(R495_TAKER_RATIO_MIN) > V504_BACKTEST_TAKER_MIN_OBSERVED)
    hata.push(`TAKER_ESIGI_BACKTEST_USTU:${R495_TAKER_RATIO_MIN}>${V504_BACKTEST_TAKER_MIN_OBSERVED}`);
  if(Number(R495_MAX_ENTRY_DRIFT_ATR) < 0.848) hata.push(`DRIFT_MAX_BACKTEST_ALTI:${R495_MAX_ENTRY_DRIFT_ATR}`);
  if(Number(R495_MIN_ENTRY_DRIFT_ATR) > -0.149) hata.push(`DRIFT_MIN_BACKTEST_USTU:${R495_MIN_ENTRY_DRIFT_ATR}`);
  if(Number(R495_MAX_ADVERSE_ATR) < 0.847) hata.push(`ADVERSE_MAX_BACKTEST_ALTI:${R495_MAX_ADVERSE_ATR}`);""")

# ══ V504-C: SURUM ═══════════════════════════════════════════════════════
import re
m=re.search(r"const LAZARUS_BUILD = '([^']+)'",s); assert m
s=s.replace(m.group(0),"const LAZARUS_BUILD = 'R493_V5_9_2_CANLI_EXACT_CLOSED1M_R495_V5_0_4_TAKER_FROM_BACKTEST_NOPROBE_RISK41_10X'")
log.append('V504_C_SURUM')

k=[("clamp tabani 0.20", "Math.max(.20, Math.min(.70, Number(process.env.R495_TAKER_RATIO_MIN || .20))" in s),
   ("eski taban 0.40 kalmadi", "Math.max(.40, Math.min(.70, Number(process.env.R495_TAKER_RATIO_MIN" not in s),
   ("olculen backtest ucu sabit", "V504_BACKTEST_TAKER_MIN_OBSERVED = 0.2199" in s),
   ("kapi taker denetimi", "TAKER_ESIGI_BACKTEST_USTU" in s),
   ("kapi drift/adverse denetimi", "DRIFT_MAX_BACKTEST_ALTI" in s and "ADVERSE_MAX_BACKTEST_ALTI" in s),
   ("olcum kaynakta belgeli", "366/1461" in s and "0,2199" in s),
   ("sinirlama durustce yazili", "gosterge niteligindedir" in s),
   ("V503 on-filtre duruyor", "V503 testnet evren on-filtresi" in s),
   ("V502 cift kilit duruyor", "V502-A: KALDIRAC KILIDI" in s),
   ("parite kapisi duruyor", "v592BootParityGate" in s),
   ("SONDA yok", "/api/probe" not in s)]
for ad,ok in k:
    if not ok: sys.exit(f'BUILD DURDU [DOGRULAMA]: {ad}')
p.write_text(s,encoding='utf-8')
print('\n'.join('  ✓ '+x for x in log)); print(f'  dogrulama {len(k)}/{len(k)} GECTI')
print(f'  SHA: {hashlib.sha256(s.encode()).hexdigest()}')
