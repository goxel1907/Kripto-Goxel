# -*- coding: utf-8 -*-
# V5.0.5 — R495 OYUNDAN TAKER SARTI KALDIRILDI (backtestte yoktu)
import pathlib, hashlib, sys
p=pathlib.Path('server.js'); s=p.read_text(encoding='utf-8')
GIRDI='d703025d7f29adcb5ed25979b5abf66220bf9139ed977532eec758de0ac1eead'
h=hashlib.sha256(s.encode()).hexdigest()
if h!=GIRDI: sys.exit(f'BUILD DURDU: girdi SHA {h} != {GIRDI}')
log=[]
def rep(et,eski,yeni,adet=1):
    global s
    n=s.count(eski)
    if n!=adet: sys.exit(f'BUILD DURDU [{et}]: beklenen {adet}, bulunan {n}')
    s=s.replace(eski,yeni); log.append(f'{et}: {adet} yer')

# ══ V505-A: FONKSIYON PARAMETRESI ══════════════════════════════════════
rep('V505_A_PARAM',
"""  takerRatioMin = 0.50,
  adverseAtrMax = 0.85,""",
"""  takerRatioMin = 0.50,
  // V5.0.5: backtestin R495 oyunda taker sarti YOKTU. Varsayilan KAPALI.
  takerVoteActive = false,
  adverseAtrMax = 0.85,""")

# ══ V505-B: OY KRITERI (iki yer) ═══════════════════════════════════════
rep('V505_B_OY_SAYIM',
"    (sum, x) => sum + (x.close > x.open && x.takerRatio >= takerRatioMin ? 1 : 0),",
"    (sum, x) => sum + (x.close > x.open && (!takerVoteActive || x.takerRatio >= takerRatioMin) ? 1 : 0),")
rep('V505_B_OY_KANIT',
"      vote: x.close > x.open && x.takerRatio >= takerRatioMin,",
"      vote: x.close > x.open && (!takerVoteActive || x.takerRatio >= takerRatioMin),\n      takerVoteActive,")

# ══ V505-C: SABIT + CAGRI ══════════════════════════════════════════════
rep('V505_C_SABIT',
"const V504_BACKTEST_TAKER_MIN_OBSERVED = 0.2199;",
"""const V504_BACKTEST_TAKER_MIN_OBSERVED = 0.2199;
// ══ V5.0.5 — R495 OYUNDA TAKER SARTI YOK ════════════════════════════
// UC TEST, BACKTESTIN 1.461 KABUL EDILMIS SINYALI UZERINDE:
//  1) Canli karar mantigi (oy sayisi -> aksiyon, drift/adverse limitleri)
//     backtest sonucunu 1461/1461 UYUSMAYAN 0 ile yeniden uretiyor. DOGRU.
//  2) takerRatio medyani votes=3'te 0,5459 · votes=2'de 0,5402 (fark +0,0057).
//     Oy takerRatio ile ILISKISIZ.
//  3) BELIRLEYICI: votes=3 olan 140 sinyalin 36'sinda (%25,7) takerRatio<0,50,
//     en dususu 0,2680. Eger oy her uc mumda takerRatio>=0,50 isteseydi
//     sinyal duzeyinde 0,2680'lik bir votes=3 IMKANSIZ olurdu.
// SONUC: backtestin oyu yalnizca close>open idi. Taker sarti canliya
// sonradan eklenmis, backtestte karsiligi olmayan bir kapidir.
// Varsayilan KAPALI. Acilirsa acilis kapisi emir yolunu kapatir.
const R495_TAKER_VOTE_ACTIVE = String(process.env.R495_TAKER_VOTE_ACTIVE ?? '0') === '1';""")

rep('V505_C_CAGRI',
"          takerRatioMin:R495_TAKER_RATIO_MIN,",
"          takerRatioMin:R495_TAKER_RATIO_MIN,\n          takerVoteActive:R495_TAKER_VOTE_ACTIVE,")

# ══ V505-D: KAPI ═══════════════════════════════════════════════════════
rep('V505_D_KAPI',
"""  if(Number(R495_TAKER_RATIO_MIN) > V504_BACKTEST_TAKER_MIN_OBSERVED)
    hata.push(`TAKER_ESIGI_BACKTEST_USTU:${R495_TAKER_RATIO_MIN}>${V504_BACKTEST_TAKER_MIN_OBSERVED}`);""",
"""  // V505: backtestin oyunda taker sarti yoktu; acikken parite bozulur.
  if(R495_TAKER_VOTE_ACTIVE) hata.push('R495_TAKER_OYU_ACIK_BACKTESTTE_YOK');
  if(R495_TAKER_VOTE_ACTIVE && Number(R495_TAKER_RATIO_MIN) > V504_BACKTEST_TAKER_MIN_OBSERVED)
    hata.push(`TAKER_ESIGI_BACKTEST_USTU:${R495_TAKER_RATIO_MIN}>${V504_BACKTEST_TAKER_MIN_OBSERVED}`);""")

import re
m=re.search(r"const LAZARUS_BUILD = '([^']+)'",s); assert m
s=s.replace(m.group(0),"const LAZARUS_BUILD = 'R493_V5_9_2_CANLI_EXACT_CLOSED1M_R495_V5_0_5_VOTE_EXACT_BACKTEST_NOPROBE_RISK41_10X'")
log.append('V505_E_SURUM')

k=[("bayrak varsayilan kapali", "R495_TAKER_VOTE_ACTIVE = String(process.env.R495_TAKER_VOTE_ACTIVE ?? '0') === '1'" in s),
   ("oy sayimi bayrakli", "(!takerVoteActive || x.takerRatio >= takerRatioMin)" in s),
   ("oy sayimi 2 yerde", s.count("(!takerVoteActive || x.takerRatio >= takerRatioMin)")==2),
   ("cagri bayragi geciyor", "takerVoteActive:R495_TAKER_VOTE_ACTIVE" in s),
   ("kapi acikken engelliyor", "R495_TAKER_OYU_ACIK_BACKTESTTE_YOK" in s),
   ("uc testin kaniti kaynakta", "1461/1461" in s and "%25,7" in s and "0,2680" in s),
   ("karar mantigi degismedi", "if (votes === 3 && finalPrice >= candidatePrice)" in s),
   ("V504 duruyor", "V504_BACKTEST_TAKER_MIN_OBSERVED = 0.2199" in s),
   ("V503 duruyor", "V503 testnet evren on-filtresi" in s),
   ("V502 duruyor", "V502-A: KALDIRAC KILIDI" in s),
   ("SONDA yok", "/api/probe" not in s)]
for ad,ok in k:
    if not ok: sys.exit(f'BUILD DURDU [DOGRULAMA]: {ad}')
p.write_text(s,encoding='utf-8')
print('\n'.join('  ✓ '+x for x in log)); print(f'  dogrulama {len(k)}/{len(k)} GECTI')
print(f'  SHA: {hashlib.sha256(s.encode()).hexdigest()}')
