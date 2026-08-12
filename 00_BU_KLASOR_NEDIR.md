# Bu klasör İKİ işi birden yapar

`server.js` SHA-256 `eed7859178fed4ae3542657d70e4c5eee60963385c0616c04a5d5b92483ab965`
build `R493_V5_9_2_CANLI_EXACT_CLOSED1M_R495_V5_0_6_LOSS_TELEMETRY_NOPROBE_RISK41_10X`

## 1. Bugün: TESTNET ölçüm sürümü

`BINANCE_EXECUTION_ENV="TESTNET"` ile koşar. **Deploy edilecek olan budur.**

## 2. Aynı zamanda: CANLI adayı

Ayrı bir "canlı sürüm" **yok**. Bu dosya canlıya da gider. Ölçüldü:

| kontrol | |
|---|---|
| C1 · backtest otoritesi ortamdan bağımsız | ✓ |
| C2 · V4.5 seçici ortamdan bağımsız | ✓ |
| C3 · kimlik ortama göre (`BINANCE_LIVE_API_KEY`) | ✓ |
| C4 · emir URL'i ortamdan türetiliyor | ✓ |
| C5 · SONDA kodu yok | ✓ |
| açılış parite kapısı | ✓ |

Çalıştırılarak doğrulandı:

```
BINANCE_EXECUTION_ENV=TESTNET   ✅ parite kapısı TEMİZ — emir yolu açık
BINANCE_EXECUTION_ENV=LIVE      ⛔ KAPALI: BILINEN_PARITE_ENGELI:CANDIDATE_FEATURE_GENERATOR_NOT_INCLUDED
```

**Canlıda emir yolu B1–B6 yüzünden bilerek kapalı.** `LAZARUS_LIVE_ARM` verilse bile
gerçek para emri açmaz. Altı engel kapanıp her biri kendi testiyle kanıtlandığında
kapı kendiliğinden açılır — yeni sürüm hazırlamak gerekmez.

Testnet'e özel tek şey V5.0.3 sembol evreni ön-filtresidir; `BINANCE_EXECUTION_ENV==='TESTNET'`
şartına bağlı, canlıda devreye girmez.

## Deploy

1. Bu klasörün içeriğini GitHub repona koy — **tek commit**
2. Railway → `TESTNET_SESSION_RESET_ID="V506_72H_TEMIZ_1"` · başka ENV değişikliği **yok**
3. Binance Demo → `Positions (0)` · `Open Orders (0)`
4. **Tek** Railway servisi açık olsun
5. Restart

**Ekleme:** `LAZARUS_LIVE_ARM` · `BINANCE_LIVE_API_KEY` · `BINANCE_LIVE_API_SECRET` ·
`R495_TAKER_RATIO_MIN` · `R495_TAKER_VOTE_ACTIVE`

## Bu koşuda ölçülecek yeni şey

V5.0.6 kayıp emirleri funnel'a yazar. 72 saat sonunda şunlar sayılabilir:

```
ORDER_ROUTE_ERROR          kaç emir yola çıkamadı
ENTRY_CANDLE_DRIFT_BLOCK   kaçı giriş mumu geçtiği için düştü + drift değeri + gecikme
AUTO_SCAN_WATCHDOG         tarama kaç kez takıldı, ne kadar
```

Önceki koşuda 7 TACTICAL kabul edilip yalnız 3'ü emre ulaşmıştı; kalan 4'ün nerede
kaybolduğu **sayılamıyordu**. Artık sayılacak.
