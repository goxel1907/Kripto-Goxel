# V6.4.7 — 15M HAFIZASI ARTIK KARARA GIRIYOR

## Bulgu

`v644CandidateMemoryContext` her aday icin 15m'den bunlari uretip 12 saat tutuyor:

```
entryZone {low,high,type}   15M_DEMAND_OB · 15M_BULL_FVG · 15M_OTE
stop                        prior plan / bolge dibi / 15m alt likidite
target1                     ilk engel / 15m ust likidite
inEntryZone                 fiyat bolgeye GERI DONDU mu
reclaim                     teyit var mi
```

Dort durum uretiyor, **ikisi kullaniliyordu**:

```
WAIT_RECLAIM                -> PUSU   kullaniliyor
NO_CHASE_AFTER_PRIOR_TARGET -> PUSU   kullaniliyor
ENTRY_ZONE_RETEST           -> OLU
TRACK_PRIOR_15M_PLAN        -> OLU
```

Hafiza yalnizca "girme" diyebiliyordu; "bolge geldi, simdi gir" diyemiyordu.
BTR bunun canli kaniti: alcalan 15m kanalin **tepesinden** girildi (aralik konumu %100,
RSI 74), hafizadaki gercek long bolgesi asagidaydi.

## A — hafizadaki target1 ILK ENGEL olarak kullanilir

En sik blokaj `FIRST_OBSTACLE_RR_UNKNOWN`: canli hikaye ustte engel bulamiyor, aday oluyor.
Hafiza o engeli zaten biliyor. Artik canli bulamazsa hafizadaki `target1` kullanilir.

**Kapi GEVSEMIYOR** — ayni `R493_MIN_FIRST_OBSTACLE_RR` (0.35) esigi, sadece engelin
kaynagi hafizadan geliyor. Kullanildiginda loglanir:

```
🧠 SYMBOL V647 ilk engel 15m HAFIZADAN: target1 X → R/R Y · 15m hafıza ...
```

## B — ENTRY_ZONE_RETEST pozitif kanit

Fiyat isaretli 15m long bolgesine dondu (`inEntryZone`) **ve** teyit geldiyse (`reclaim`),
bu artik `retestProof` sayilir — veto degil DESTEK. Mevcut kanitlar korunuyor, kume
genisliyor. Hicbir kapi kaldirilmadi.

## C — kaldirac araligi altyapisi (VARSAYILAN KAPALI)

Parite kapisi ve emir yolu tam esitlik yerine `[R486_MIN_LEVERAGE .. R486_MAX_LEVERAGE]`
araligini kabul edebilir. **Varsayilan KAPALI**, cunku olcum 10x'i bu bakiyede desteklemiyor:

```
bakiye 151$ · 50$ marj · sabit %8 butce
   7x -> SL tavani %3,46
  10x -> SL tavani %2,42      dar stop = daha cok stop yeme
olcum (108$):  7x +1,027 $/islem  ·  10x +0,680 $/islem   (%34 daha kotu)
10x ancak ~200$ ustunde 7x'i geciyor.
```

**EKSIK:** aralik acilsa bile ikinci bir kural daha var — `CANLI_KALDIRAC_7X_DEGIL`
canlida tam 7x istiyor. Bilerek dokunmadim; olcum 10x'i hakli cikarmadan parite
sozlesmesinin ikinci ayagini da gevsetmek istemedim. Bakiye 200$ ustune ciktiginda
tekrar olcup birlikte karar veririz.

Acmak icin: `V647_KALDIRAC_ARALIK="1"` + `R486_MAX_LEVERAGE="10"` (ve yukaridaki
ikinci kural da ele alinmali).

## Degismeyenler

50$ marj tabani, 1 pozisyon, %8 risk butcesi, TOP24, R495 kabul zinciri, V646 BE/eleme,
V637 PUSU devri, Binance guvenlikleri. `index.html` degismedi.

## Geri alma (deploy gerekmez)

```
V647_HAFIZA_ENGEL="0"      hafiza engeli kapanir
V647_BOLGE_GIRISI="0"      bolge retesti kaniti kapanir
V647_KALDIRAC_ARALIK="0"   zaten varsayilan
```

## Dogrulama

```
node --check        TEMIZ
testler             43/43 gecti (9 yeni V647 testi)
boot testi          aralik KAPALI ve ACIK, ikisinde de 0 calisma zamani hatasi
kontrol testi       12x kaldirac REDDEDILDI: KALDIRAC_KILIDI_ARALIK_DISI:12 ∉ [7,10]
diff                40 ekleme / 7 silme (CRLF korundu)
```
