# V6.9.2 — İlk engel / hedef-yakın ailesi kalktı

Haklıydın, kapı denetlemeyi bırakıp kaldırdım. Ama körlemesine değil — **üretim hunisine
bakıp hangileri gerçekten ateşliyor** ona göre.

## Huni ne diyor (13.711 kayıt, canlı oturum)

| | sayı |
|---|---|
| **PUSU** (pusu kur, market açma) | **5.445** |
| SKIP | 5.111 |
| **MARKET** | **21** |
| TACTICAL | 78 |

Bot 5.445 kez "pusu kur" dedi, 21 kez market açtı. "Bot işlem yapmıyor" şikâyetin tek satırda bu.

**En çok kesen adlandırılmış sebepler:**

| sebep | blok |
|---|---|
| **TARGET_TOO_NEAR** | **1.161** |
| V45_SELECTOR otoritesi (foRR eşiği) | **2.258** |
| R493 `[LOW_FIRST_OBSTACLE_RR]` (0,01/0,02/0,03 < 0,10) | ~400 |
| R493_ENTRY_SAFETY otoritesi | 2.631 |
| Short kapalı (senin panel anahtarın) | 1.084 |

Hepsi **tek bir aileden**: "ilk engel / hedef çok yakın."

## Ölçüm bu aileyi zaten çürütmüştü

88 canlı işlemden:

* ilk engel R/R ↔ ROI: **r = −0,016** → sıfır
* tam R/R ↔ ROI: **r = +0,343** → gerçek işaret bu (V686'da skora girdi)
* Dört kutu: `R/R≥4 & ilkEngel<0,65` → WR **%62**, net **+48,5$** — **en iyi kutu**
* `R/R≥4 & ilkEngel≥0,65` → WR %36, net −15,7$

**Bot kapıyı ters tarafa koymuştu.** Düşük ilk-engel R/R, iyi kutunun işaretiydi.

## Neden şimdi güvenli

Bu aileyi açmanın tek gerçek tehlikesi vardı: yakın engelin V662 üzerinden **ATR stop
tabanını çökertmesi** — EGLD'yi öldüren şey tam buydu (−%26). O kategori hatası **V6.9.1'de
düzeltildi**, taban artık ezilemiyor. Tehlikenin sebebi ortadan kalktı, kapı kalkıyor.

## Kaldırılan üç kapı

1. **`TARGET_TOO_NEAR`** — yanlış kovadaydı. Spread/kayma ile aynı "operasyonel güvenlik"
   grubuna konmuştu; oysa o *fiziksel* risk, bu *cazibe görüşü*. Artık `TARGET_NEAR_NOT_BLOCKING`
   diye görünüyor ama kesmiyor.
2. **`R493_MIN_FIRST_OBSTACLE_RR`** 0,10 → fiilen kapalı (0,01).
3. **`V592_V45_FIRST_OBSTACLE_RR_MIN`** 0,10 → 0. **Bunu az kalsın kaçırıyordum** — R493'ten
   *önce* eliyor, tek başına 2.258 blok. Kendi eski testimin yorumu uyardı:
   *"Yalnız R493 indirilseydi hiçbir şey değişmezdi."*

## İki yan etki yakaladım

**Arka kapı:** `unknownLegacyCritical` koşulu `!targetNear` kullanıyordu. `targetNear` artık hep
`false` olduğu için, eskiden "hedef yakın" diye açıklanan legacy bloklar *"bilinmeyen kritik"*
olarak geri dönecekti. Ham hâli (`targetNearRaw`) yazıldı.

**TDZ:** bayrağı 7182. satıra koymuştum ama 974. satırda da kullanılıyor → bot açılışta
`ReferenceError` ile çökerdi. `node -c` bunu yakalamaz (sözdizimi değil, çalışma zamanı).
Gerçek açılış denemesiyle doğruladım, sabiti en başa taşıdım.

## Dokunmadıklarım ve nedeni

* **Short kapalı** (1.084) — senin panel anahtarın, benim işim değil.
* **FALLING_KNIFE** (167) — hakkında ölçümüm yok. Ölçmeden kaldırmam.
* **ATR_EXTREME** — V6.9.1'in stop tabanı bunun ilkeli hâli ve daha canlıda sınanmadı.
* **Emir güvenlikleri** — yön tutarsızlığı, TP/SL eksik, borsa hatası, pozisyon gerçeği.
  Bunlar görüş değil, bozuk emri engelliyor.
* **PUSU yolu** — "market kovalanmaz, bölgeye pusu kurulur" senin istediğin davranış.

## ENV

Değişiklik gerekmiyor. Hepsini geri açmak: `V692_ILK_ENGEL_KAPISI_KALKTI=0`

## Test

**240 test, 240 geçti.** Ayrıca gerçek açılış denemesi yapıldı (TDZ için).
