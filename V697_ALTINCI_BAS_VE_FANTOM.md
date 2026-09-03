# V6.9.7 — ailenin 6. başı + fantom kaldıraç

## 1) Fantom kaldıraç (R431 / R461)

Canlı log: `⚙️ APR R431 kaldıraç: 5x→12x (… band 10x-BinanceMax …)`

Zinciri sonuna kadar okudum:

| adım | satır | ne yapıyor |
|---|---|---|
| V502-A | 27679 | `executeLeverage = V592_LEVERAGE_LOCK` (kilit zincirin **başında**) |
| R431 | 27843 | haritayı uygular → **12x** ← log burada yazılıyor |
| R461 | 29132 | 25x'e kadar çıkarabilir |
| V501 EXACT | 29312 | `executeLeverage = V592_LEVERAGE_LOCK` (**geri 5x**) |
| /api/order | 21955 | `_lockLev = V592_LEVERAGE_LOCK` → borsaya **5x** |
| miktar | 21982 | `qty = usdtAmount × safeLeverage` → **safeLeverage = 5** |

**Para yolu bozuk değildi.** 12x hiçbir zaman borsaya gitmedi, pozisyon boyutu da 12x'ten
türemedi. Bozuk olan **kayıttı**: sen panelde 12x okuyordun, borsa 5x alıyordu.

İki gerçek zarar vardı:
1. Kendi botun hakkında yanlış şey biliyordun — bu oturumun tekrar eden hata sınıfının aynısı
   ("değer var ama karar noktasına bağlı değil"), sadece ters yönü.
2. **Tuzak kapısı**: `V592_EXACT_BACKTEST_AUTHORITY=0` yazıldığı an, 50$ marj sessizce
   12x-25x'e çıkardı. Tek bir env satırı hesabı riske atıyordu.

Düzeltme: kilit açıkken R431/R461 haritası **uygulanmaz**, log gerçeği söyler.

## 2) Ailenin 6. başı — V644 hafızası

V6.9.2 ilk-engel ailesinin 4 başını, V6.9.5 gizli 5.'yi kaldırdı. 6.'sı hafıza kılığındaydı:

```js
const target = tf15.obstacle?.firstObstacle?.price || prior.liveDecision?.target || ...
const targetReached = price >= target*0.999;
```

"Önceki hedefe ulaşıldı" aslında **ilk engelden** okunuyordu. İlk engel girişe yapışıktır —
bu yüzden `targetReached` bedavaya doluyor, `V644_PRIOR_TARGET_NO_CHASE` PUSU'su çıkıyordu.
Kapı kalkıkken hedef artık yalnız gerçek bir önceki hedeften veya üst likiditeden okunur.

## 3) "Geç kovalama" eşiği — işareti TERS çıktı

`lateEvidence` içinde: `distanceFromBreakoutAtr > 0.35` → "hareket geç, kovalama".

Ölçtüm. 879 grafik kümesi, 18.123 kırılım-üstü nokta, ileri ufuk 24 mum, kümeye-sağlam t-testi:

| kesit (ATR) | n | ort MFE % | ort MAE % | 24 mum getiri % |
|---|---|---|---|---|
| >0.15 | 14.136 | 7.81 | 4.08 | 2.06 |
| >0.35 | 9.647 | 7.90 | 4.11 | 2.06 |
| >0.60 | 6.078 | 8.30 | 4.25 | 2.08 |
| >1.00 | 3.093 | 8.88 | 4.62 | 1.95 |
| >1.60 | 1.233 | 9.67 | 5.20 | 1.57 |

Kırılımdan **uzaklaştıkça MFE ARTIYOR**. Kümeye-sağlam fark testi:

| kesim | küme | MFE farkı | t |
|---|---|---|---|
| 0.35 | 878 | +0.475 | **3.66** |
| 0.60 | 873 | +0.724 | **4.76** |
| 1.00 | 762 | +1.052 | **5.13** |
| 1.60 | 361 | +1.742 | **5.46** |
| 2.20 | 129 | +3.119 | **4.40** |

Keşif eşiği (seçim-sonrası, M=20 korelasyonlu) z=2.28. Sonuç eşiği **aşıyor ama ters yönde**.
Hiçbir kesimde "geç olan kötü" çıkmadı.

MAE %4.08 → %5.20 yükseliyor. Yani uzama bir **stop genişliği** meselesi (V691'in alanı),
**giriş vetosu** değil. Senin sözleşmen zaten bunu söylüyordu: stop büyüyebilir, fırsatı bloklayamaz.

Eşik varsayılan **0 = kapalı**. Geri açmak: `V697_GEC_KOVALAMA_ATR=0.35`.
Yapısal kanıtlar (parabolicChase, targetRejection, TRAP) duruyor — onları ölçmedim, dokunmadım.

## Açılışta göreceğin yeni satırlar
```
[V697] V644 HAFIZASI: 'onceki hedef' artik ilk engelden OKUNMUYOR (6. bas kapandi) · gec-kovalama esigi KAPALI
[V697] KALDIRAC KAYDI: R431/R461 haritasi kilit aciksa UYGULANMAZ -> borsaya giden = V592_LEVERAGE_LOCK=5x
```

## Testler
264/264 geçti (yeni: `v697-fantom-kaldirac`, `v644` içine 2 yeni durum).
