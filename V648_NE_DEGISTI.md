# V6.4.8 — EN YAKIN ENGEL + SWING PİVOT + GEO DONDURMA

**29 Ağustos 2026 · tetikleyen: canlı KOMAUSDT LONG işlemi**

## Kanıt

```
KOMAUSDT LONG · giriş 0.015132 · marj 50$ · 7x · bakiye 151,78$
orijinal SL 0.01477018512  (-%2,391)  → 50×7×2,391% = 8,37$   bütçe 12,14$  ✓
mevcut   SL 0.014844       (-%1,903)
TP 0.0158232 (+%4,57) · R/R 2,40
```

Risk sözleşmesi doğruydu. Engel seçimi yanlıştı.

| kaynak | seviye | mesafe | R/R | sonuç |
|---|---|---|---|---|
| hikâye (canlı) | 0.01645 aralık tepesi | +%8,89 | 2,40 | **geçti** |
| 15m hafıza TP1 | 0.015167 | +%0,23 | 0,12 | engellemeliydi |
| 15m grafik rafı | 0.0152 | +%0,45 | 0,24 | engellemeliydi |

15m'de **0.015198 ve 0.015208'de iki ardışık ret** vardı. Giriş, hafızanın kendi
bölge tavanının (0.015006) **%0,84 üstünde**; bölgeden TP1'e giden yolun **%78'i**
giriş anında yenmişti.

---

## V647-A neden hiç çalışmadı — iki ayrı sebep

**1) Ölü koddu.** `r486EntryTruthGuard` kapıyı şöyle çağırıyordu:

```js
r493EntrySafetyGate(story,{firstObstacleRR,firstObstacleRole,plannedEntry},{side})
```

İçinde `recommendedSl` yok, `opts.sl` yok. V647-A'nın `_s > 0` şartı bu yüzden
**her zaman false**'tu. Dal bir kez bile çalışmadı — canlıda tek bir
`🧠 V647 ilk engel 15m HAFIZADAN` satırı görülmemesinin sebebi bu.

**2) Mantık hatası.** Şart `!_v647Known` idi: hikâye bir engel bulduysa hafıza
hiç sorulmuyordu. Bilinen daha **yakın** bir engeli görmezden gelen çözücü
tanımı gereği iyimserdir.

---

## Yamalar

**A — kapıya gerçek stop/giriş geçirilir.** Ölü kod canlanır.

**B — en YAKIN engel kazanır:** `min(hikâye R/R, hafıza R/R)`.
KOMA bu kuralla açılmazdı (0,12 < 0,35).

**C — swing-high pivotlar engel adayı.** Backtest 15m'de swing-high pivot
kullanıyor; canlı yalnızca semantik yapı (likidite / OB / bear FVG / VAH /
ghost POC) arıyordu. 0.0152 rafı bunların hiçbiri değildi.
Pivotlar zaten `tfRow.structure.pivots.high` içinde **hesaplanmış duruyordu**,
sadece aday listesine eklenmiyordu.

**D — V646 geo artık `initialSL`'den.** Backtestte `slpct` işlem boyunca sabit;
canlıda `currentSL` okunuyordu. KOMA'da stop BE'den önce %2,391 → %1,903
daraldı, geo 1,4065 yerine 1,1196 oldu, BE **%20,4 erken** kuruldu
(%0,84 yerine %1,06 olmalıydı).

**E — sessiz no-op alarmı.** Hafıza var ama giriş/stop hesaplanamıyorsa
`⚠️ V648 hafiza engeli HESAPLANAMADI` loglanır. Bu satır V647-A'da olsaydı
ölü kod ilk dakikada görülürdü.

---

## Doğrulama

- 59/59 test geçiyor (16'sı yeni V648 testi)
- `node --check` temiz · boot temiz (bayraklar AÇIK ve KAPALI iken ayrı ayrı)
- diff: server.js 51 satır · CRLF korundu (30.245 → 30.272)
- sürüm pini artık `package.json` ile karşılaştırılıyor — unutulan bump da yakalanır

## Geri alma (deploy gerekmez)

```
V648_EN_YAKIN_ENGEL="0"   hafıza yalnız boşluk doldurur (V647 mantığı)
V648_PIVOT_ENGEL="0"      swing-high pivotlar engel sayılmaz
V648_GEO_DONDUR="0"       geo tekrar currentSL'den
V647_HAFIZA_ENGEL="0"     hafıza engeli tamamen kapanır
```

## İzlenecek risk

C yaması engelleri **yakınlaştırır** → `LOW_FIRST_OBSTACLE_RR` payı artabilir,
işlem sayısı düşebilir. Ölçülen 20 sembollük hafıza örneğinde 16'sı 0,35 eşiğini
geçiyordu; pivot eklenince bu oran düşer. İşlem tamamen durursa ilk çevrilecek
kol **`V648_PIVOT_ENGEL="0"`** — tek başına, diğerleri açık kalarak.
