# V6.7.1 — Botun kendi kalite okuması artık karar veriyor

## A) V671: DÜŞÜK kalite bandı adayı ELİYOR

### Bulgu

Bot her aday için bir kalite skoru üretiyor (`analysis.r480Shadow.score`) ve
onu HIGH / MID / LOW bandına ayırıyor. Canlıda bu bandın **tek etkisi marjı
çarpmaktı**: LOW → `×0,60`. Ama V601 taban marjı 50$ o çarpanı hemen geri
alıyordu (50 × 0,60 = 30 → tabana geri 50). Yani **bandın sıfır etkisi vardı**
— log satırı `DÜŞÜK→ele` yazıyor ve hiçbir şey elemiyordu.

### Ölçüm

`LAZARUS_HAZIRAN_2026_6TF_BACKTEST_PAKETI/all_scenario_trades.csv`,
senaryo `R495_3M_ACCEPT`. Bantlar canlıyla **birebir aynı eşikte**:
HIGH ≥ 0,660 · LOW < 0,550 (`R493_HIGH_MIN` 0,66 / `R493_LOW_MAX` 0,55).

Tüm adaylar (backtest 2 slot):

| | n | WR | PF | net | işlem-başı | maxDD |
|---|---|---|---|---|---|---|
| kapı yok | 555 | %65,8 | 1,83 | +1186,77$ | 2,14$ | 97,69$ |
| LOW bandı ele | 348 | %65,2 | 2,25 | +1022,55$ | 2,94$ | 74,74$ |
| yalnız HIGH | 52 | %63,5 | 3,04 | +177,35$ | 3,41$ | 17,28$ |

**Canlı gerçeği (MAX 1 POZİSYON, zaman sıralı, çakışan atlanır):**

| | n | WR | PF | net | işlem-başı | maxDD |
|---|---|---|---|---|---|---|
| kapı yok | 305 | %66,9 | 1,91 | +716,31$ | 2,35$ | 73,86$ |
| **LOW bandı ele** | **229** | **%67,2** | **2,79** | **+836,38$** | **3,65$** | **57,62$** |
| conf ≥ 73 | 295 | %67,5 | 2,13 | +797,31$ | 2,70$ | 54,54$ |
| foRR ≥ 0,10 (V6.7.0) | 210 | %67,1 | 2,23 | +606,12$ | 2,89$ | 64,10$ |
| LOW ele + foRR ≥ 0,10 | 158 | %66,5 | 2,85 | +639,86$ | 4,05$ | 85,85$ |

LOW bandını elemek **üç eksende birden** kazanıyor: daha çok para (+836 vs +716),
daha yüksek PF (2,79 vs 1,91), daha sığ dip (57,6$ vs 73,9$). Bu nadirdir.
V6.7.0'ın foRR kapısını da her ölçütte geçiyor.

İkisini birleştirmek daha kötü (net 640$, dip 85,9$) — **tek başına LOW elemesi** kazanan.

### Ne değişti

`R493 kalite-sizing` bloğunda, marj çarpanı hesaplanmadan **önce**:
skor `R493_LOW_MAX`'ın altındaysa `markAutoSkip` + `continue`.
Kanıt hunisine `V671_LOW_QUALITY_BLOCK` yazılır.

Bayrak: `V671_DUSUK_KALITE_ELE` (varsayılan `1`). Railway'de `0` yaparsan eski
davranışa döner.

---

## B) V672: Kelepçeler env'e açıldı — **hiçbir varsayılan değişmedi**

Sekiz sabitin etrafındaki `Math.max/min` kelepçesi env değerini sessizce
yutuyordu; ya da kelepçe geçiriyor ama açılış parite kapısı **tam eşitlik**
istediği için ayarı değiştirmek kapıyı gevşetmiyor, **tüm emir yolunu
kapatıyordu** (`EMIR ACILMAYACAK`).

| Sabit | Eski | Yeni | Varsayılan |
|---|---|---|---|
| `V628_ATR_TAVAN` | üst kelepçe 8,0 (varsayılan zaten orada) | 20,0 | 8,0 · değişmedi |
| `R495_TAKER_RATIO_MIN` | alt kelepçe 0,20 (varsayılan zaten orada) | 0,05 | 0,20 · değişmedi |
| `V619_TOP10_FAST_SCORE_MIN` | taban = `V619_TOP10_SCORE_MIN` | 30 | 64 · değişmedi |
| `V619_TOP24_FAST_SCORE_MIN` | taban = `V619_TOP24_SCORE_MIN` | 30 | 69 · değişmedi |
| `V619_EXPLOSION_FAST_SCORE_MIN` | taban = `V619_EXPLOSION_SCORE_MIN` | 30 | 67 · değişmedi |

Parite kapısında tam-eşitlik → **aralık**:

| Eski kimlik | Yeni kimlik | Aralık |
|---|---|---|
| `CANLI_KALDIRAC_7X_DEGIL` | `CANLI_KALDIRAC_ARALIK_DISI` | 7–10x *(senin sözleşmen)* |
| `V45_SCORE_35_DEGIL` | `V45_SCORE_ARALIK_DISI` | 20–60 |
| `CANLI_MAX_POZ_1_DEGIL` | `CANLI_MAX_POZ_ARALIK_DISI` | 1–2 |
| `CANLI_ATR_TAVAN_8_DEGIL` | `CANLI_ATR_TAVAN_ARALIK_DISI` | 1–20 |
| `DRIFT_MAX_BACKTEST_ALTI` | `DRIFT_MAX_ARALIK_DISI` | 0,30–1,50 |
| `DRIFT_MIN_BACKTEST_USTU` | `DRIFT_MIN_ARALIK_DISI` | −1,50 … −0,05 |
| `ADVERSE_MAX_BACKTEST_ALTI` | `ADVERSE_MAX_ARALIK_DISI` | 0,30–1,50 |

Sözleşme kaybolmadı — **sınır** oldu. Giriş kaymasını (`R495_MAX_ENTRY_DRIFT_ATR`)
sıkılaştırmak artık mümkün; eskiden parite ≥0,848 istediği için imkânsızdı.

**Bugünkü davranış birebir aynı.** Değişen tek şey: Railway'de bir env yazarsan
artık sessizce yutulmuyor ve botu fail-closed etmiyor. Deploy değil, tek satır.

---

## Doğrulama

- **139/139 test.** Yeni: `tests/v671-kalite-bandi-karar.test.js` (7 test).
- `v639-live-contract` ve `v670` testleri yeni parite kimliklerine güncellendi;
  v670'in build pini artık `package.json` sürümünden türetiliyor (sabit string değil).
- Sahte kimlikle boot: `✅ CANLI parite kapisi TEMIZ — emir yolu acik.`
- CRLF 30.437 → 30.447, `node --check` temiz.
