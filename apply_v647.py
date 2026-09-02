#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V6.4.7 — 15M HAFIZASI ARTIK KARARA GIRIYOR (+ kaldirac araligi altyapisi)

BULGU
=====
v644CandidateMemoryContext her aday icin 15m'den bunlari uretiyor ve 12 saat tutuyor:
    entryZone {low,high,type}   15M_DEMAND_OB · 15M_BULL_FVG · 15M_OTE
    stop                        prior plan / bolge dibi / 15m alt likidite
    target1                     ilk engel / 15m ust likidite
    inEntryZone                 fiyat bolgeye GERI DONDU mu
    reclaim                     teyit var mi
Dort durum uretiyor, ikisi kullaniliyor:
    WAIT_RECLAIM                -> PUSU   KULLANILIYOR
    NO_CHASE_AFTER_PRIOR_TARGET -> PUSU   KULLANILIYOR
    ENTRY_ZONE_RETEST           -> OLU
    TRACK_PRIOR_15M_PLAN        -> OLU
Yani hafiza yalnizca "girme" diyebiliyor; "bolge geldi, simdi gir" diyemiyor.
BTR bunun canli kanitiydi: alcalan 15m kanalin TEPESINDEN girildi, hafizadaki
gercek long bolgesi asagidaydi.

YAMA A — hafizadaki target1 ILK ENGEL olarak kullanilir
-------------------------------------------------------
En sik blokaj FIRST_OBSTACLE_RR_UNKNOWN: canli hikaye ustte engel bulamiyor.
Hafiza o engeli zaten biliyor (target1). Kapi GEVSETILMIYOR — ayni 0.35 esigi
hafizadan gelen gercek engelle hesaplaniyor. Bloke aday, gercek R/R'li isleme doner.

YAMA B — ENTRY_ZONE_RETEST pozitif giris yolu
---------------------------------------------
Fiyat isaretli 15m long bolgesine DONDU (inEntryZone) ve teyit geldiyse (reclaim),
bu artik `retestProof` sayilir. Veto degil, DESTEK. Hicbir kapi gevsemiyor;
var olan "kanit" kumesine gercek bir kanit ekleniyor.

YAMA C — kaldirac araligi altyapisi (VARSAYILAN KAPALI)
-------------------------------------------------------
Parite kapisi ve emir yolu tam esitlik yerine [MIN..MAX] araligi kabul eder.
VARSAYILAN KAPALI cunku OLCUM 10x'i su bakiyede DESTEKLEMIYOR:
    bakiye 151$ · 50$ marj · sabit %8 butce
      7x  -> SL tavani %3,46
     10x  -> SL tavani %2,42      dar stop = daha cok stop yeme
    olcum (108$): 7x +1,027 $/islem · 10x +0,680 $/islem  (%34 daha kotu)
    10x ancak ~200$ ustunde 7x'i geciyor.
Acmak icin: V647_KALDIRAC_ARALIK="1"  (ve R486_MAX_LEVERAGE ile tavan)

GERI ALMA (deploy gerekmez)
    V647_HAFIZA_ENGEL="0"    · V647_BOLGE_GIRISI="0"    · V647_KALDIRAC_ARALIK="0"
"""
import shutil, subprocess, sys, pathlib

KOK = pathlib.Path.home() / "mnt/Downloads/Kripto-Goxel-v644-work"
SRC = KOK / "server.js"
CRLF = "\r\n"

def blok(satirlar):
    return CRLF.join(satirlar)

YAMALAR = [
    # ── bayraklar ────────────────────────────────────────────────────────────
    (
        "const V646_ZAYIF_KURULUMLAR = Object.freeze(['SWEEP_RECLAIM_HIGH_ATR','CEKIC_ALT_FITIL']);",
        blok([
            "const V646_ZAYIF_KURULUMLAR = Object.freeze(['SWEEP_RECLAIM_HIGH_ATR','CEKIC_ALT_FITIL']);",
            "// ═══ V647 ═══ 15m hafizasi artik KARARA girer (once yalnizca veto uretiyordu)",
            "// A: hafizadaki target1 ilk engel olarak kullanilir (FIRST_OBSTACLE_RR_UNKNOWN blokajini cozer)",
            "const V647_HAFIZA_ENGEL = String(process.env.V647_HAFIZA_ENGEL ?? '1') !== '0';",
            "// B: ENTRY_ZONE_RETEST + reclaim artik POZITIF kanit (retestProof)",
            "const V647_BOLGE_GIRISI = String(process.env.V647_BOLGE_GIRISI ?? '1') !== '0';",
            "// C: kaldirac tam esitlik yerine aralik. VARSAYILAN KAPALI — olcum 10x'i bu bakiyede desteklemiyor.",
            "const V647_KALDIRAC_ARALIK = String(process.env.V647_KALDIRAC_ARALIK ?? '0') !== '0';",
            "const V647_MAX_KALDIRAC = Math.max(3, Math.min(20, Math.floor(Number(process.env.R486_MAX_LEVERAGE || 10))));",
        ]),
    ),
    # ── YAMA A: hafizadaki target1 ilk engel olarak ──────────────────────────
    (
        "  if(R493_REQUIRE_FIRST_OBSTACLE&&!firstKnown)return {...base,action:'PUSU',code:'FIRST_OBSTACLE_RR_UNKNOWN',reason:'ilk engel R/R bilinmiyor: MARKET/TACTICAL yok'};\r\n"
        "  if(firstKnown&&firstRR<R493_MIN_FIRST_OBSTACLE_RR)return {...base,action:'PUSU',code:'LOW_FIRST_OBSTACLE_RR',reason:`ilk engel R/R ${firstRR.toFixed(2)} < ${R493_MIN_FIRST_OBSTACLE_RR.toFixed(2)}: daha iyi fiyat/reclaim bekle`};",
        blok([
            "  // ═══ V647-A ═══ Canli hikaye ustte engel bulamadiysa 15m HAFIZADAKI target1'i kullan.",
            "  // Kapi GEVSEMIYOR: ayni 0.35 esigi, sadece engel kaynagi hafizadan geliyor.",
            "  let _v647RR = firstRR, _v647Known = firstKnown, _v647Kaynak = null;",
            "  if (V647_HAFIZA_ENGEL && !_v647Known && candidateMemory?.available) {",
            "    const _e = Number(entryTruth?.plannedEntry || entryTruth?.originalEntry || opts?.entry || 0);",
            "    const _s = Number(entryTruth?.recommendedSl || entryTruth?.originalSl || opts?.sl || 0);",
            "    const _t = Number(candidateMemory?.target1 || 0);",
            "    if (_e > 0 && _s > 0 && _s < _e && _t > _e) {",
            "      const _rr = (_t - _e) / (_e - _s);",
            "      if (Number.isFinite(_rr) && _rr > 0) { _v647RR = _rr; _v647Known = true; _v647Kaynak = '15M_HAFIZA_TARGET1'; }",
            "    }",
            "  }",
            "  if(R493_REQUIRE_FIRST_OBSTACLE&&!_v647Known)return {...base,action:'PUSU',code:'FIRST_OBSTACLE_RR_UNKNOWN',reason:'ilk engel R/R bilinmiyor (15m hafizada da yok): MARKET/TACTICAL yok'};",
            "  if(_v647Known&&_v647RR<R493_MIN_FIRST_OBSTACLE_RR)return {...base,firstObstacleRR:+_v647RR.toFixed(3),firstObstacleSource:_v647Kaynak,action:'PUSU',code:'LOW_FIRST_OBSTACLE_RR',reason:`ilk engel R/R ${_v647RR.toFixed(2)} < ${R493_MIN_FIRST_OBSTACLE_RR.toFixed(2)}${_v647Kaynak?' (15m hafiza)':''}: daha iyi fiyat/reclaim bekle`};",
            "  if(_v647Kaynak){ try{ logAuto(`🧠 ${story?.symbol||''} V647 ilk engel 15m HAFIZADAN: target1 ${candidateMemory.target1} → R/R ${_v647RR.toFixed(2)} · ${candidateMemory.summary}`); }catch(_){} }",
        ]),
    ),
    # PASS satirinda da hafiza kaynagini goster
    (
        "  return {...base,blocked:false,action:'ALLOW',code:'PASS_BACKTEST_OBSERVABLE',reason:`backtestte gözlenebilir ilk-engel sözleşmesi uygun${candidateMemory?.available?` · ${candidateMemory.summary}`:''}; canlı mikro-yapı yalnız kaydedildi`};",
        blok([
            "  return {...base,blocked:false,action:'ALLOW',code:'PASS_BACKTEST_OBSERVABLE',",
            "    firstObstacleRR:Number.isFinite(_v647RR)?+_v647RR.toFixed(3):base.firstObstacleRR,firstObstacleSource:_v647Kaynak,",
            "    reason:`backtestte gözlenebilir ilk-engel sözleşmesi uygun${_v647Kaynak?' (ilk engel 15m hafızadan)':''}${candidateMemory?.available?` · ${candidateMemory.summary}`:''}; canlı mikro-yapı yalnız kaydedildi`};",
        ]),
    ),
    # ── YAMA B: ENTRY_ZONE_RETEST pozitif kanit ──────────────────────────────
    (
        "retestProof=!!(micro.confirmedPullback||story?.validatedTrendRetest||decision?.r117BodyReclaimOk||decision?.r117TrapSweepTaken)",
        "retestProof=!!(micro.confirmedPullback||story?.validatedTrendRetest||decision?.r117BodyReclaimOk||decision?.r117TrapSweepTaken"
        "||(V647_BOLGE_GIRISI&&entryTruth?.r493EntrySafety?.candidateMemory?.state==='ENTRY_ZONE_RETEST'"
        "&&entryTruth?.r493EntrySafety?.candidateMemory?.reclaim===true))",
    ),
    # ── YAMA C: kaldirac araligi (varsayilan kapali) ─────────────────────────
    (
        "    if(Number(V592_LEVERAGE_LOCK)!==_v602BekLev)\r\n"
        "      hata.push(`KALDIRAC_KILIDI_UYUSMAZ:${V592_LEVERAGE_LOCK}!=${_v602BekLev}`);",
        blok([
            "    // ═══ V647-C ═══ aralik modunda tam esitlik yerine [MIN..MAX] kabul edilir.",
            "    if(V647_KALDIRAC_ARALIK && !_v602Sert){",
            "      const _lo=Math.max(1,Number(R486_MIN_LEVERAGE)||7), _hi=V647_MAX_KALDIRAC;",
            "      if(!(_lo>=3 && _hi>=_lo && _hi<=20)) hata.push(`KALDIRAC_ARALIK_GECERSIZ:${_lo}-${_hi}`);",
            "      if(!(Number(V592_LEVERAGE_LOCK)>=_lo && Number(V592_LEVERAGE_LOCK)<=_hi))",
            "        hata.push(`KALDIRAC_KILIDI_ARALIK_DISI:${V592_LEVERAGE_LOCK} ∉ [${_lo},${_hi}]`);",
            "    } else if(Number(V592_LEVERAGE_LOCK)!==_v602BekLev)",
            "      hata.push(`KALDIRAC_KILIDI_UYUSMAZ:${V592_LEVERAGE_LOCK}!=${_v602BekLev}`);",
        ]),
    ),
    (
        "    if (V592_EXACT_BACKTEST_AUTHORITY && V592_LEVERAGE_LOCK > 0 && safeLeverage !== V592_LEVERAGE_LOCK) {",
        blok([
            "    // ═══ V647-C ═══ aralik modunda [MIN..MAX] icindeki kaldirac kabul edilir.",
            "    const _v647LevOk = V647_KALDIRAC_ARALIK",
            "      ? (safeLeverage >= Math.max(1, Number(R486_MIN_LEVERAGE)||7) && safeLeverage <= V647_MAX_KALDIRAC)",
            "      : (safeLeverage === V592_LEVERAGE_LOCK);",
            "    if (V592_EXACT_BACKTEST_AUTHORITY && V592_LEVERAGE_LOCK > 0 && !_v647LevOk) {",
        ]),
    ),
]


def main():
    ham = open(SRC, encoding="utf-8", newline="").read()
    crlf_once = ham.count("\r\n")

    for capa, _ in YAMALAR:
        adet = ham.count(capa)
        if adet != 1:
            print("DURDURULDU: capa {} kez gecti (1 bekleniyordu):\n---\n{}\n---".format(adet, capa[:170]))
            return 1

    yedek = SRC.with_suffix(".js.v646.yedek")
    shutil.copy2(SRC, yedek)
    print("yedek: {}".format(yedek))

    for capa, yeni in YAMALAR:
        ham = ham.replace(capa, yeni, 1)

    eski = "V6_4_6_BE_GEO_ZAYIF_ELEME"
    if eski not in ham:
        print("DURDURULDU: V6_4_6 surum adi bulunamadi"); return 1
    ham = ham.replace(eski, "V6_4_7_15M_HAFIZA_KARARA_GIRDI")

    open(SRC, "w", encoding="utf-8", newline="").write(ham)
    crlf_sonra = open(SRC, encoding="utf-8", newline="").read().count("\r\n")
    print("{} yama uygulandi · CRLF {} -> {} ({:+d})".format(len(YAMALAR), crlf_once, crlf_sonra, crlf_sonra - crlf_once))
    if crlf_sonra < crlf_once:
        print("DURDURULDU: CRLF KAYBI — geri aliniyor"); shutil.copy2(yedek, SRC); return 1

    r = subprocess.run(["node", "--check", str(SRC)], capture_output=True, text=True)
    if r.returncode != 0:
        print("SOZDIZIMI HATASI — geri aliniyor:\n" + r.stderr)
        shutil.copy2(yedek, SRC); return 1
    print("node --check TEMIZ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
