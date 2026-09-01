# V6.7.4 — İşlem içi CVD yörüngesi ve kâr geri verme ölçülebilir oldu

## Bulgu: veri zaten var, dışarı çıkamıyordu

Önceki turda CVD tetiğini "kanıt değil" diye işaretlemiştim: `dCvdRatio` karar→kapanış
ölçüldüğü için sonucun bir kısmını zaten taşıyor. Dürüst test, işlem *sırasında*
örneklenmiş CVD'nin sonucu **önceden** haber verip vermediğine bakmalı.

O veri **var**. Kanıt indeksi doğruluyor:

| işlem | örnek | işlem süresi |
|---|---|---|
| PROM | 2.957 | 2.953 sn |
| COLLECT | 2.251 | 2.260 sn |
| BTW | 2.260 | 2.257 sn |
| ZORA | 597 | — |
| CYS | 121 | 155 sn |

Yani `samples` **1 Hz, işlemin tamamı boyunca** kayıtlı. Sorun sadece erişim:
kayıt başına 2,6 MB ve HTTP tarafı belgeyi ~70.000. baytta kesiyor.

*(Elimdeki üç eski paket — VELVET, AIO, AVAAI — 2 örnek taşıyor; ikisi de 12 Ağustos
öncesi build. Bugünkü kayıtlar tam seriyi tutuyor.)*

## Ne eklendi

`r501DatasetRows` içine, mevcut `samples` dizisinden türetilen 15 kolon:

**CVD yörüngesi** — erken boşalma sonucu haber veriyor mu?
`cvdRatio0` · `cvdRatio1m` · `cvdRatio5m` · `cvdRatio15m` ·
`cvdRatioMin` · `cvdRatioMax` · `cvdRatioMinAtMs` · `cvdDrop1m` · `cvdDrop5m`

**Kâr geri verme** — senin uzun süredir istediğin katman
`peakRoiSeen` · `peakRoiAtMs` · `dipRoiSeen` · `givebackPct` (tepe − kapanış)

**Kapsam kontrolü** — `sampleCountUsed` · `sampleSpanMs`

**Geriye dönük çalışır.** 87 kapalı işlemin hepsinde dolu gelir; yeni kayıt beklemeye
gerek yok.

## Sessiz bozulmayı önleyen ayrıntı

İlk yazdığım halde `at(300000)` fonksiyonu, işlem 5 dakikaya ulaşmasa bile **son**
örneği döndürüyordu. ZORA'nın 61 saniyelik kaydında `cvdRatio5m` = 46,8 çıktı — ama
bu 5. dakikanın değeri değil, işlemin son değeriydi. Bu haliyle bırakılsaydı ölçüm
"5. dakikadaki CVD" ile "işlemin son CVD'si"ni karıştırırdı ve sonuç sahte olurdu.

Düzeltildi: `if(!(span>=ms)) return null`. İşlem o dakikaya ulaşmadıysa kolon boş.

## Doğrulama

ZORA'nın **gerçek 64 örneği** üzerinde blok birebir koşturuldu:

```
sampleCountUsed  64        cvdRatio0        47
sampleSpanMs     61.160    cvdRatioMin      45,7 @ 20.328 ms
peakRoiSeen      2,72      cvdRatioMax      48,8
peakRoiAtMs      27.330    cvdDrop1m        −0,5
```

Kâr tepesi 27. saniyede %2,72 — sonra stopa gitti. Tam olarak ölçmek istediğimiz şey.

- **151/151 test.** Yeni: `tests/v675-islem-ici-yorunge.test.js` (4 test), biri
  span korumasını ayrıca doğruluyor.
- Sahte kimlikle boot: `✅ CANLI parite kapisi TEMIZ — emir yolu acik.`
- Yalnız dışa aktarım: karar yolu, parite ve `chartStory.fib` kilidi değişmedi.
- CRLF 30.474 → 30.511, `node --check` temiz.

## Sırada

Deploy sonrası `/api/evidence/dataset.csv` tek indirmeyle şunu yanıtlayacak:
**ilk 1–5 dakikadaki CVD boşalması, işlemin sonunu önceden haber veriyor mu?**
Cevap evetse kâr-koruma katmanının tetiği ölçülmüş olur — tahmin değil.
