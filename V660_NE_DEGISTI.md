# V6.6.0 — BACKTEST STOP GEOMETRİSİ + LİKİDASYON KORUMASI

**31 Ağustos 2026 · kullanıcı kararı: risk bütçesi %8 → %23**

## Ölçüm

```
BACKTEST (run_6tf_motor.py, 1060 sinyal)
  stop genişliği: slPct/ATR ortanca 2,35  (%10 dilim 1,20 · %90 dilim 3,85)
  marj 10–40$ DALGALI · LEV 10 · işlem başı risk ~%20,7

CANLI (önce)
  stop tabanı 0,52 × ATR (tavan 0,90) · marj 50$ SABİT · bütçe %8
  → stop 4,5 KAT dar
```

Backtestin kendi verisi stop/ATR düştükçe edge'in bittiğini gösteriyor:

```
ATR %0-3  (2,84 kat)  +0,667R/işlem   stop-out %16
ATR %3-5  (1,92 kat)  +0,111R         stop-out %21
ATR %5-7  (1,27 kat)  +0,048R         stop-out %19
ATR %7-9  (1,02 kat)  +0,025R         stop-out %43
ATR %9+   (0,88 kat)  −0,100R         stop-out %31
```

ZORA'da bu oran **0,40–0,54**'tü. −10,67$ tam olarak buydu.

## Üç sessiz kelepçe

Bunlar env'den açılamıyordu; kodda tavan vardı:

```
6623  R486_MIN_STOP_ATR     clamp(0.30, 0.90)  → 2,35 istense 0,90'a kırpılırdı
7921  minStopPct            clamp(0.80, 4.50)  → ATR>%1,9'da devreye girip stopu boğuyordu
6710  V634_TOPLAM_RISK_PCT  clamp(2, 12)       → 23 istense 12'ye kırpılırdı
30356 boot parite kapısı    eq(risk, 8) ZORUNLU → 23 yazılsa bot HİÇ emir açmazdı
```

Dördü de açıldı. Parite kapısı hâlâ fail-closed — sadece artık `V660_SOZLESME_RISK_PCT`
sabitine bakıyor, böylece kasıtsız kayma yine yakalanıyor.

## Sert duvar: likidasyon

`minStopPct` artık sabit %4,5 tavanıyla değil, **likidasyondan türetilen** tavanla
kelepçeleniyor:

```
 7x → likidasyon ~%14,3 → güvenli tavan %12,86 → parite ATR ≤ %5,47
10x → likidasyon ~%10,0 → güvenli tavan  %9,00 → parite ATR ≤ %3,83
```

Gereken stop bu tavanı aşıyorsa **işlem açılmaz** (`V660 DURDURULAMAZ`). Bu "az işlem
olsun" kuralı değil: **durduramayacağın işleme girilmez.** ZORA (ATR %8,48 → gereken
stop %19,9) hiçbir kaldıraçta durdurulamazdı.

## Kullanıcının sözleşmesi korundu

```
marj 50$ SABİT ✓   ·   max 1 pozisyon ✓   ·   7–10x ✓   ·   stop gerekirse büyüsün ✓
```

Değişen tek şey risk bütçesi — ve onu backtestin kendi seviyesine yaklaştırdık
(backtest %20,7 · biz artık %23).

```
%23 bütçe · 50$ · 7x · bakiye 143$ → izin verilen stop %9,41
2,35 × ATR ≤ %9,41  →  ATR ≤ %4,00 işleme girebilir
```

## Doğrulama

106/106 test · `node --check` temiz · **iki yönlü boot testi**:
- yeni sözleşme (%23) → `RISK SOZLESMESI: 1 pozisyon x islem basi %23`, kapı temiz
- eski değer (%8) → `EMIR ACILMAYACAK: TOPLAM_RISK_SOZLESME_DISI:8!=23` — fail-closed çalışıyor

TDZ kontrolü ayrı test: `R486_MIN_LEVERAGE`, `V660_LIK_PAYI`, `V660_DURDURULAMAZ_VETO`,
`R486_MIN_STOP_ATR` hepsi kullanımdan önce tanımlı (V6.3.x'te bu hatayı yapmıştım).

## RAILWAY'DE MUTLAKA DEĞİŞMELİ

Bu ikisi env'de eski değerde kalırsa bot **hiç emir açmaz** (parite kapısı kapanır):

```
V634_TOPLAM_RISK_PCT = 23      (şu an 8)
R486_MIN_STOP_ATR    = 2.35    (şu an 0.52)
```

## Geri alma

```
V660_DURDURULAMAZ_VETO="0"   yüksek ATR vetosu kapanır (ÖNERİLMEZ)
V660_LIK_PAYI="0.80"         likidasyon payı daralır (daha muhafazakâr)
R486_MIN_STOP_ATR="1.20"     backtestin %10'luk dilimi — daha dar stop
V634_TOPLAM_RISK_PCT + V660_SOZLESME_RISK_PCT  ikisi BİRLİKTE değişmeli
```

## Sıradaki (bu sürümde YOK)

Kâr takip katmanı. Ölçüm hazır: 36/78 işlem artıdaki zirvesini stopa kadar geri verdi,
%72'si ilk koruma eşiğine hiç ulaşmadı, canlı zirve ortancası +%6,8 ROI. Ama stop
genişleyince zirveler de büyüyecek — mevcut merdiven (BE %16–21 ROI) kendiliğinden
anlamlı hale gelebilir. Önce bunu ölçmek gerek; ikisini aynı anda değiştirirsem
hangisinin işe yaradığı bir daha ölçülemez.
