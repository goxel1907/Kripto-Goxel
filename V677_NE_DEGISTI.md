# V6.7.6 — Kâr geri verme: kural şekli kesinleşti, parametre gölgeye alındı

## Sonuç: geri verme puanı ile sonuç arasındaki ilişki TEK YÖNLÜ

dataset v3.44, 85 kapanmış işlem:

| geri verme (zirve − kapanış) | n | kazanan | net |
|---|---|---|---|
| < 1 puan | 16 | 14 | **+61,13 $** |
| 1–3 puan | 9 | 6 | +31,26 $ |
| 3–6 puan | 4 | 4 | +19,48 $ |
| 6–10 puan | 7 | 5 | +11,65 $ |
| 10–20 puan | 25 | 7 | **−52,41 $** |
| 20+ puan | 24 | 1 | **−192,70 $** |

**≤10 puan geri veren 36 işlem net +123,52 $ · >10 puan veren 49 işlem net −245,11 $.**

Ve iki grup kendi davranışlarıyla ayrışıyor:
- **kazananların** geri verme medyanı **2,8 puan** (%90 dilim 13,3)
- **kaybedenlerin** geri verme medyanı **19,2 puan**

Yani ~10 puanlık bir eşik, kazananların %90'ına dokunmadan kaybedenlerin
tipik davranışını yakalıyor. Bu bir uydurma değil, verinin kendi dağılımı.

## Zirve zamanlaması — konfonderden arındırılmış

Ham gradyan ("geç zirve iyi") tuzak olabilirdi: stop olan işlem kısadır, zirvesi
zorunlu olarak erkendir. **Süreyi sabitleyip** karşılaştırdım:

| işlem süresi | erken zirve (ilk yarı) | geç zirve (ikinci yarı) |
|---|---|---|
| 0–30 dk | n=5 · **−%11,6** | n=10 · **+%5,9** |
| 30–90 dk | n=4 · **−%7,9** | n=13 · **+%2,3** |
| 90+ dk | n=4 · **−%25,6** | n=16 · **+%11,1** |

Üç bandın üçünde de aynı yön. Bulgu süre yan etkisi değil.
Medyan zirve: **22,6. dakika**, işlem süresinin %63'ünde.

## Parametre neden hâlâ verilmiyor

"Zirve ≥A ise zirveden G puan düşünce çık" taraması (A=3):

| G | 6 | 8 | 10 | 12 | 15 | 20 |
|---|---|---|---|---|---|---|
| net | 73,1 | 53,4 | 35,0 | 18,3 | −3,3 | −34,1 |

G küçüldükçe **hep** iyileşiyor — optimum "tam zirvede çık"a yakınsıyor. Sebep:
`peakRoiSeen` **geriye dönük** biliniyor. Canlıda takip eden zirve her an nihai
zirveden düşüktür; bir işlem G kadar düşüp sonra YENİ bir zirve yapmışsa canlı
kural onu erken keserdi, kapalı-form veri bunu göremez. **Ölçülmemiş tek risk bu.**

Not: tetiklenen işlem asla zarar görmez — `zirve − kapanış > G` ise çıkış
seviyesi (zirve − G) gerçek kapanışın **üstündedir**. Tek maliyet, toparlanacak
bir işlemi erken kesmek. Gölge tam olarak onu ölçer.

## Ne yazıldı

`r501Sample` içine (saniyede bir, her açık işlem için) gölge kaydedici:

- **KOŞAN** zirveyi izler, G ∈ {6, 8, 10, 12} için ilk tetiklenme anını kaydeder
- `V677_MIN_ZIRVE` (varsayılan %3) altında hiç kurulmaz
- her G bir kez tetiklenir
- `dataset.csv`'ye çıkar: `gb<G>AtMs · gb<G>Roi · gb<G>Peak · gb<G>PeakAtMs · gbLivePeak`

**Emre, SL/TP'ye, yöneticiye dokunmaz.** Test bunu doğruluyor: blokta
`closePosition`, `sendOrder`, `cancelOrder`, `setStopLoss`, `trailingState.set`,
`reduceOnly` geçmiyor; içindeki tek `continue` G döngüsünün muhafızı ve sayısı
teste sabitlendi.

## Göndermeden yakalanan hata

İlk yazdığımda ROI'yi fiyattan türetiyordum:
`(price/entryPrice − 1) × leverage × 100`.

ZORA'nın gerçek 64 örneğinde replay ettim: benimki **%0,27**, yöneticinin kendi
sayısı **%2,72** — 10 kat sapma. Gölge, karşılaştırılacağı `peakRoi`/`roiPct` ile
aynı birimde olmak zorunda; öyle kalsaydı ölçüm sessizce anlamsız olurdu.

Düzeltildi: ROI artık `manager.r91Exit.pnlPct`'den okunuyor. Tekrar replay:
koşan zirve **%2,72 @27sn** — kayıttaki `manager.peakRoi` 2,718 ile birebir.

Sentetik doğrulama: zirve %20 → %−5 yolunda G6/G8 70sn, G10 80sn, G12 90sn'de
tetikleniyor; zirve %2 kalan yolda (eşik %3) hiç tetiklenmiyor.

## Yan bulgu — yönetici bunu ZATEN hesaplıyor

`manager.r91Exit` şunları taşıyor: `pnlPct` · `peakPnl` · `pullbackPct` ·
**`givebackRoi`** · `profitLockLevel` · `exitMode`. Yani kâr-kilit altyapısı
mevcut ve ölçülüyor. Sıradaki iş, gölge sonuçları geldiğinde bu mevcut
mekanizmanın eşiğini ayarlamak olabilir — sıfırdan katman yazmak yerine.

## Doğrulama

- **160/160 test.** Yeni: `tests/v677-geri-verme-golgesi.test.js` (6 test).
- Sahte kimlikle boot: `✅ CANLI parite kapisi TEMIZ — emir yolu acik.`
- CRLF 30.515 → 30.555, `node --check` temiz.

## Sırada

25–30 işlem sonra `dataset.csv`: `gb<G>Roi` ile gerçek `roiPct` karşılaştırılır.
Gölge gerçekten daha iyi çıkıyorsa parametre **ölçülmüş** olur ve kuralı
canlıya almak tahmin değil, karar olur.
