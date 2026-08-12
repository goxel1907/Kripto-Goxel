# Bu klasör İKİ işi birden yapar

`server.js` SHA-256 `fecca1cc884052e5afb7d0f1becc6f2e3a0d1b72ed55679c81dd44b0354cad96`
build `R493_V5_9_2_CANLI_EXACT_CLOSED1M_R495_V5_0_5_VOTE_EXACT_BACKTEST_NOPROBE_RISK41_10X`

## 1. Şu an: TESTNET ölçüm sürümü

`BINANCE_EXECUTION_ENV="TESTNET"` ile koşar. Yürütme paritesini ve pasif
parametreleri ölçer. **Bugün deploy edilecek olan budur.**

## 2. Aynı zamanda: CANLI adayı

Ayrı bir "canlı sürüm" **yok**. Bu dosya canlıya da gider. Ölçüldü:

| kontrol | durum |
|---|---|
| C1 · backtest otoritesi ortamdan bağımsız | ✓ |
| C2 · V4.5 seçici ortamdan bağımsız | ✓ |
| C3 · kimlik ortama göre (`BINANCE_LIVE_API_KEY`) | ✓ |
| C4 · emir URL'i ortamdan türetiliyor | ✓ |
| C5 · SONDA kodu yok | ✓ |
| açılış parite kapısı | ✓ |

Çalıştırılarak doğrulandı:

```
BINANCE_EXECUTION_ENV=TESTNET
  ✅ TESTNET parite kapısı TEMİZ — emir yolu açık

BINANCE_EXECUTION_ENV=LIVE + LAZARUS_LIVE_ARM + canlı anahtarlar
  ⛔ CANLI PARİTE KAPISI KAPALI: BILINEN_PARITE_ENGELI:CANDIDATE_FEATURE_GENERATOR_NOT_INCLUDED | …
```

**Canlıda emir yolu B1–B6 engelleri yüzünden bilerek kapalıdır.** `LAZARUS_LIVE_ARM`
verilse bile gerçek para emri açmaz. Altı engel kapanıp her biri kendi testiyle
kanıtlandığında kapı kendiliğinden açılır.

Testnet'e özel tek şey V5.0.3 sembol evreni ön-filtresidir; o da
`BINANCE_EXECUTION_ENV==='TESTNET'` şartına bağlı, canlıda devreye girmez.

## Bugün ne yapacaksın

Bu klasörün içeriğini GitHub repona koy (tek commit), Railway'de yalnız
`TESTNET_SESSION_RESET_ID="V505_72H_TEMIZ_1"` değiştir.

**Ekleme:** `LAZARUS_LIVE_ARM` · `BINANCE_LIVE_API_KEY` · `BINANCE_LIVE_API_SECRET`
