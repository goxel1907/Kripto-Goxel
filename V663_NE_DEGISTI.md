# V6.6.3 — ENGEL GÖRÜŞ ALANI: 30m / 1h / 4h

**31 Ağustos 2026 · canlı kanıt**

## Kanıt

```
23:09:19Z · build V6_6_2 · TWTUSDT
  giriş 0.5204 · SL 0.51186544 (%1,64) · TP 0.57244 (+%10,0)
  firstObstacleRR: null          ← girişin üstünde HİÇ engel bulunamadı
  → PUSU · CORRECTLY_BLOCKED
```

Engel bulunamayınca R/R hesaplanamıyor, aday pusuya düşüyor. LA'da da aynısı olmuştu
(tek üst seviye 4H BSL 0.06986 idi, çözücü görmedi).

## Sebep

`fallbackObstacles` yalnız şunlara bakıyordu:

```
likidite : 3m · 5m · 15m · 1h
arz OB   : 3m · 5m · 15m
bear FVG : 3m · 5m · 15m
```

**30m, 4h ve 1h/4h OB-FVG hiç okunmuyordu.**

## Veri zaten vardı

`r483ChartStory` bunları **tüm zaman dilimleri için hesaplıyor**:

```js
const fvg={},ob={},fibResearch={},fib={},liq={};
for(const k of ['1m','3m','5m','15m','30m','1h','4h','1d']){
  fvg[k]=r483Fvg(rows[k],price); ob[k]=r483Ob(rows[k],price); ...
}
```

Yani hesaplanıyor, story'ye giriyor, sadece **okunmuyordu.** V6.5.0'da `story.pivots`
diye olmayan bir alanı okumaya kalkmıştım; bu sefer önce veri yolunu kanıtladım ve
teste bağladım.

## Değişen

Hem `fallbackObstacles`'a hem V662 stop-tavanı aday listesine eklendi:

```
likidite : 30m · 4h
arz OB   : 1h · 4h
bear FVG : 1h · 4h
```

**Hiçbir kararı daraltamaz.** Liste `entry`nin üstündekilerle filtrelenip sıralanıyor
ve `[0]` (en yakın) alınıyor. Uzak bir HTF seviyesi ancak daha yakını **yokken** seçilir
— yani yalnızca "engel bulunamadı" vakasını çözer, mevcut R/R'leri düşürmez.

## Doğrulama

127/127 test · `node --check` temiz · boot temiz · CRLF korundu (30.429 → 30.437)

Test, veri yolunu ayrıca kilitliyor: `r483ChartStory`'nin TF listesi değişirse
(`'4h'` düşerse) test kırılır — sessiz no-op bir daha olmasın.

## Deploy sonrası

TWT gibi adaylarda artık `firstObstacleRR` sayı dönmeli, `null` değil. Hâlâ `null`
geliyorsa o sembolde gerçekten hiçbir zaman diliminde üst seviye yok demektir.
