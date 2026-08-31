# V6.5.1 — GERÇEK GİRİŞ REFERANSI (ZORA kanıtı)

**31 Ağustos 2026 · `ZORA_1788162608211.evidence.json` ile bulundu**

Bu sürüm yeni bir özellik değil, **kendi iki yamamdaki tek kök hatanın düzeltmesi.**

## Kanıt

```
entryTruth.plannedEntry    0.01060939025
entryTruth.originalEntry   0.010209        ← o anki gerçek fiyat
chartStory.firstObstacle   0.0105935       ← HARD_OBSTACLE (SWING_HIGH + 15m_SUPPLY_OB)
hafıza entryZone           0.009166 - 0.010065
hafıza target1             0.0105174167
v649Sapma                  5.409  → "bayat" sayıldı, ATLANDI
r493 firstObstacleSource   HIKAYE          ← hafıza dalı hiç çalışmadı
```

## Kök sebep

`story.timing === 'WAIT_BREAK_RETEST'` iken:

```js
plannedEntry = first > entry ? first * 1.0015 : ...
```

Yani `plannedEntry`, **ilk engelin üstünde bir kırılım projeksiyonu** — giriş fiyatı değil.
`0.0105935 × 1.0015 = 0.01060939` tam olarak gözlenen değer.

İki yamam da onu "giriş" sandı:

**V649-B kırıldı:** sapma `(plannedEntry − bölgeTavanı)` diye ölçüldü → **%5,41** → %4 bayat
sınırını aştı → atladı. Gerçek girişle ölçülseydi **%1,43** → banda düşer, giriş bölgeye
çekilirdi.

**V648 kırıldı:** hafıza `target1` (0.0105174) `plannedEntry`'nin (0.0106094) **altında**
kaldı → `_t > _e` şartı false → hafıza dalı sessizce atlandı → kaynak `HIKAYE` kaldı.

## Düzeltmeler

**A** — V649-B sapmayı **gerçek girişten** ölçer. Ayrıca `plannedEntry` asla yukarı
itilmez: `Math.min(plannedEntry, bölgeTavanı)`.

**B** — R493 giriş kapısı bir LONG'u **güncel fiyatın üstündeki** bir fiyattan
değerlendirmez; `plannedEntry` güncel fiyatın üstündeyse gerçek giriş kullanılır.

## Düzeltmeden sonra ZORA'da ne olurdu

```
sapma %1,43        → giriş 0.010065'e çekilir, MARKET yerine PUSU
min() kuralı       → hafıza engeli seçilir: R/R 0,685  (hikâye 0,854 yerine)
bölgeden bakınca   → R/R 1,478
```

## Yamalanmayan ama kayda geçen

UI "R39 üst hedef yakın: **5M_SWING_HIGH %0,67**" diyordu. O seviyeden R/R **0,194** —
eşiğin altında. Fiyat şu an tam orada takıldı (0.010169). **Kapı 5m swing high'ı görmüyor.**

`V650_PIVOT_ENGEL` bunu çözer ama **KAPALI kalıyor**: funnel sayımı baskın engelin
`LOW_FIRST_OBSTACLE_RR` (281 olay, %9,3) olduğunu, `FIRST_OBSTACLE_RR_UNKNOWN`'ın ise
3 olay (%0,1) olduğunu gösteriyor. Engel kaynağı eklemek önce o 281'lik grubu büyütür.
Sıra: **önce V649-B ölçülecek, sonra pivot açılacak.**

## Doğrulama

90/90 test · `node --check` temiz · boot temiz üç konfigürasyonda · server.js 19 satır ·
CRLF korundu (30.333 → 30.344)

## İzlenecek

Deploy sonrası aranacak satır — bu sefer görünmeli:

```
🎯 SYM V649 giriş bölgeye çekildi: X → Y · bölge A-B · sapma %Z
```

Görünmüyorsa ya sapma bandın dışında ya da hafıza bölgesi yok; ikisi de logda ayrı
satırla görünür.
