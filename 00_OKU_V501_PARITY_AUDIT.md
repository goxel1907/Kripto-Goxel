# LAZARUS V5.0.1 — PARITY AUDIT CANDIDATE

**Durum:** CANLIYA HAZIR DEĞİL / LIVE EMİR YOLU BİLİNÇLİ HARD-BLOCK.

Bu paket, V4.7.4.43 → V5.0.0 canlı dönüşümünü 725 işlemlik `recovered_portfolio_sim.py` sözleşmesine karşı yeniden denetler. Gerçek para açmak için hazırlanmış bir release değildir; bulunan parite kırıklarını görünür ve fail-closed hale getiren ara denetim paketidir.

## Doğrulanmış 725 referansı

- 725 işlem; 483 K / 242 Z
- WR %66.6207
- PF 2.419668
- net +3299.3164 USDT
- max DD %14.8052
- START 102 / SLOT 41 / BUFFER 20 / max 2 / LEV 10
- V45: msScore >= 35 + TOP_GAINER + firstObstacleRR >= 0.35
- giriş: candidateTs + 180000
- exit candle: exitTs % 60000 = 59999

## V5.0.0'da bulunan ve V5.0.1'de düzeltilen kritik kırıklar

1. **Kaldıraç iki farklı gerçeklikteydi.** Aday yolunda `executeLeverage` fırsata göre 20/30/50x kalabiliyor; R495 sizing bu değerle hesaplanırken `/api/order` Binance'i 10x'e kilitliyordu. State de eski değeri yazabiliyordu. V5.0.1 EXACT modda sizing'den önce 10x'e zorlar ve state'te exchange-proven leverage'ı önceler.
2. **`R486_FIRST_OBSTACLE_MIN_RR=0.35` ENV'i etkisizdi.** Kod `Math.max(0.70, ...)` ile 0.35'i tekrar 0.70'e çekiyordu. Clamp/default 0.35'e hizalandı.
3. **R493 hard first-obstacle default 0.80 idi.** Fresh deploy'da ENV eksikse 0.80 devreye girebilirdi. Default 0.35 oldu.
4. **HIGH kalite faktörü default 1.10 idi.** Recovered portfolio sim HIGH=1.00 kullanıyor. Default 1.00 oldu.
5. **Parity gate yalnız boolean bayrakları kontrol ediyordu.** Gate artık 35/0.35/TOP_GAINER/41/20/max2/risk4/DD 0.08-0.30-0.45/quality factor/10x gibi sayısal sözleşmeyi de kontrol ediyor.
6. **Evidence endpoint eski 547 işlem referansını gösteriyordu.** 725 portfolio replay referansına güncellendi.
7. **Build script mutlak eski session path'lerine bağlıydı.** Kaynak ve çıktı artık paket-relative. Rebuild byte-for-byte doğrulandı.

## Hâlâ açık yapısal parite engelleri — bu yüzden LIVE hard-block

1. **Historical candidate/feature generator pakette yok.** 1461 matched sinyali sıfırdan bağımsız üretmek kanıtlanamıyor.
2. **Aynı timestamp slot önceliği birebir değil.** Backtest: R481 strategy score → `score` → `r495Scale`. Canlı pre-sort: opsiyonel R482 lane/source → R481 → confidence → original rank. 1461 sinyalde 74 timestamp'te çoklu aday var (153 sinyal; max grup 4).
3. **Outcome cooldown + günlük stop restart paritesi yok.** Backtest sürekli state taşır; canlıdaki ilgili state'lerin bir kısmı memory-only ve restart'ta sıfırlanabilir. Historical 725 run'da dailyStop/coinBlock skip oluşmadı, fakat gelecekte davranış sözleşmesi yine birebir değildir.
4. **`trailingState` restart paritesi yok.** Restart sonrası adopted position `ADOPTED_POSITION_NOT_PARITY` olur.
5. **Temiz exit-engine forward parity run yok.** Sonda kontaminasyonu sonrası yeterli temiz strateji örneği hâlâ yok.

Bu beş madde `V501_KNOWN_PARITY_BLOCKERS` olarak LIVE parity gate'e bağlandı. `BINANCE_EXECUTION_ENV=LIVE` iken emir yolu bu aday sürümde açılmaz.

## Test sonucu

- `node --check server.js` → PASS
- V500 C1-C5 regresyonu (V501 build adına uyarlanmış) → 79/79 PASS
- `V501_PARITY_AUDIT_TEST.js` → 35/35 PASS
- `99_TEST_KOSUCU.js` → 1612 geçen iddia / 65 beklenen eski-kalıp / **0 beklenmeyen hata**
- build transform → 18 dönüşüm + 11/11 son doğrulama PASS
- `server.rebuilt.js` == `server.js` byte-for-byte

## Deploy notu

Bu paketi gerçek para için deploy etme. Gerekirse TESTNET'te kullan; minimal ENV farkı `RAILWAY_ENV_V501_TESTNET_MIN_DIFF.txt` içindedir. **Raw Editor kullanma; API değişkenleri görünmüyorsa toplu yapıştırma onları silebilir.**
