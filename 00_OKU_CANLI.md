# LAZARUS CANLI V5.0.0 — kenarda duran canlı sürüm

**Build:** `R493_V5_9_2_CANLI_EXACT_CLOSED1M_R495_V5_0_0_LIVE_C1_C5_NOPROBE_RISK41_10X`
**Kaynak:** testnet V4.7.4.43 (`472d999e…`), doğrulanmış transformasyonla üretildi
**Durum:** DEPLOY EDİLMEDİ. Senin açık talimatın olmadan deploy edilmeyecek.

---

## 1. Bu paket ne, ne değil

**Ne:** bugüne kadar backtestte ölçülmüş ve testnette doğrulanmış her şeyin,
gerçek para ortamında **aynı** davranacak hale getirilmiş hali. Bundan sonraki
optimizasyonları doğrudan bu dosyaya ekleyeceğiz.

**Ne değil:** "hazır, aç gitsin" demek değil. Temiz bir testnet ölçümü hâlâ yok
(bkz. §6). Kod hazır, veri hazır değil.

---

## 2. Canlıya geçerken SESSİZCE bozulacak 5 şey vardı — beşi de kapatıldı

Testnet sürümünde `BINANCE_EXECUTION_ENV = 'TESTNET'` **sabit** yazılıydı.
Birisi o tek satırı `'LIVE'` yapsaydı, hiçbir hata almadan şunlar olurdu:

### C1 — Backtest otoritesi kapanırdı (27 çağrı noktası)

`V592_EXACT_BACKTEST_AUTHORITY` ortama bağlıydı. Canlıda `false` olurdu ve
**ölçülen 13 davranış** birden değişirdi:

| kapanan şey | backtestteki karşılığı |
|---|---|
| kaldıraç kilidi | 10x sabit → serbest sürüklenir |
| giriş mum paritesi | `candidateTs + 180000` (725/725) → bozulur |
| çıkış mum paritesi | `exitTs % 60000 = 59999` (725/725) → bozulur |
| çıkış tipi beyaz listesi | yalnız 4 çıkış → `R14_HARD_LOSS`, `R42`, `R41`, `CVD_FLIP`, `ADVERSE_CASCADE` **canlı olurdu** |
| min-hold guard | DYNAMIC_STOP 60sn / INITIAL_SL 300sn → kalkar |
| operasyonel hard safety | backtestte olmayan bir REJECT devreye girerdi |
| defter kanıt-modu | kapanır |

**Düzeltme:** ortam bağı kaldırıldı. Bu sabitler backtestin *ne yaptığını*
kodlar; hangi borsaya emir gittiğiyle ilgisi yok.

### C2 — Botun TEK giriş filtresi kapanırdı

`V592_V45_TESTNET_ACTIVE` de ortama bağlıydı. Bu seçici, Python motorundaki
`mask_rule`'un ta kendisi: `msScore ≥ 35 + TOP_GAINER + firstObstacleRR ≥ 0.35`.
Canlıda `false` olsaydı bot bu filtreyi hiç uygulamazdı — **725 işlem / WR %66,6
/ PF 2,420** ölçümünün canlıda hiçbir karşılığı kalmazdı.

**Düzeltme:** ortam bağı kaldırıldı.

### C3 — Bot hiç emir açamazdı

`bReq`, `bAlgo` ve `r486391BinanceCreds` üç ayrı katmanda **sabit**
`BINANCE_TESTNET_API_KEY` okuyordu. Canlıda bu değişken yok → imzalı her uç
`throw` ederdi.

**Düzeltme:** tek çözücü, ortama göre `BINANCE_LIVE_*` ya da
`BINANCE_TESTNET_*` okur. Sistemdeki 10+ çağrı yeri otomatik doğru anahtarı alır.

### C4 — Emirler testnet'e gitmeye devam ederdi

`BINANCE_EXECUTION_FAPI` sabit `testnet.binancefuture.com` idi.

**Düzeltme:** ortamdan türetiliyor. Açılış kapısı URL ile ortamın uyuştuğunu
ayrıca doğruluyor.

### C5 — SONDA canlıya taşınırdı

524 satırlık koşulsuz örnekleme modülü **tamamen silindi**: emir açma, kapatma,
döngü, zamanlayıcı, `/api/probe` uçları. 23 silinen adın kaynakta **sıfır**
kalıntısı olduğu build sırasında doğrulanıyor.

Geriye 5 etkisiz saplama kaldı (`v592ProbeSlotOffset()` → `0` gibi). Sebebi
dürüstçe şu: bu adlar strateji kodunun 15 ayrı yerinden çağrılıyor ve o 15 yeri
elle düzenlemek tam olarak yeni hata üreteceğim yer. Saplamalar sabit döner.

---

## 3. AÇILIŞ PARİTE KAPISI — "gözümden kaçmış"ın panzehiri

Bu kod tabanında en pahalı hata sınıfı hep aynıydı: **bir kural sessizce
kapanır, kod çalışmaya devam eder, haftalar sonra sonuçların neden backtestle
tutmadığı aranır.**

Artık bu imkânsız. Bot açılışta backtest sözleşmesinin **9 maddesini** tek tek
doğruluyor. Biri bile tutmazsa `V592_TRADING_HARD_BLOCK` dolar ve **emir yolu
kapanır**. Bot çalışır, panel çalışır, kanıt toplar — ama işlem açmaz.

Ölçüldü, tahmin değil — altı senaryo gerçekten çalıştırıldı:

```
CANLI · silahsız · anahtarsız   ⛔ KIMLIK_EKSIK | CANLI_SILAHLANDIRILMADI
CANLI · silahsız · anahtar var  ⛔ CANLI_SILAHLANDIRILMADI
CANLI · SİLAHLI  · anahtar var  ✅ TEMİZ — 9 madde doğrulandı, emir yolu açık
CANLI · SİLAHLI  · V45 kapalı   ⛔ SOZLESME_KAPALI:V592_V45_TESTNET_ACTIVE
CANLI · SİLAHLI  · EXACT kapalı ⛔ SOZLESME_KAPALI:V592_EXACT_BACKTEST_AUTHORITY
TESTNET · testnet anahtarları   ✅ TEMİZ
```

Durum ucu: `GET /api/canli/parity-gate`

**Ek emniyet:** gerçek para için `LAZARUS_LIVE_ARM="CANLI-PARA-ONAY"` açıkça
verilmeli. Yanlışlıkla deploy = bot işlem açmaz.

---

## 4. Değişmeyenler (backtest sözleşmesi aynen duruyor)

Giriş `candidateTs+180000` · çıkış `%60000=59999` · çıkış sayıları
338/210/165/12 · izinli çıkışlar 4+MAX_24H · V4.5 eşikleri 35/0.35/TOP_GAINER ·
Fixed41 · 10x · risk %4 · hayalet defter muhafızı · sanal özkaynak filtresi ·
BH1 operatör önceliği · AS1 tek kapanış hunisi · AU1 ayrı dedup Map'leri ·
AN1 kapanış ispatı.

---

## 5. Doğrulama

```
node 99_TEST_KOSUCU.js
  geçen iddia       : 1577
  beklenen kalıf    : 65   (sonda silindi / sürüm adı / ortam-türetimli URL)
  BEKLENMEYEN HATA  : 0
node V500_CANLI_TEST.js   → 79 geçti, 0 kaldı
node --check server.js    → geçti
```

Build betiği (`00_BUILD_TRANSFORMASYONU.py`) 18 dönüşümün **her birini tam bir
kez** uygulamayı zorunlu kılar; biri tutmazsa build çöker. Yeniden üretilebilir.

### 5 hata sınıfı taraması (canlı sürüm üzerinde ölçüldü)

| sınıf | sonuç |
|---|---|
| operatör önceliği (BH1) | üst seviyede `&&` sonra `||` kalıbı: **0** |
| Map/Array dönüş varsayımı (BB) | **0** |
| sayaçsız erken return (BA/BD) | emir/kapanış yollarının hepsi izli |
| guard tek çağrı yerine bağlı (AS1) | kapanış hunisi 6, `trailingState.delete` 5, `recordTradeClose` 5 — kapsanmış |
| restart'ta ölen state (BF) | emir kilitleri atomik diske yazılıyor; defter yazılıyor |

---

## 6. DÜRÜST AÇIK MADDE — canlıya geçmenin önündeki tek engel

**Hâlâ temiz bir strateji performans ölçümü yok.**

- 10.08 · 13 işlem · WR %53,8 · PF 1,927 · net +46,61 → temiz ama **n=13, gürültünün altında**
- 11.08 · 120 kayıt · −329,40 · PF 0,150 → **gerçek değil**, sonda kontaminasyonu

Backtest hedefi: 725 işlem / 60 gün / WR %66,6 / PF 2,420.

**Gereken:** V4.7.4.43'ü `V592_PROBE_ACTIVE="0"` ile 24 saat temiz koştur.
Doğrulanacaklar: medyan tutuş > 60 sn, `protectionVerified: true`, marj ~15
(41 değil), `exitReason: MAX_SURE_KAPAT`, ~12 işlem/gün.

Bu ölçüm gelmeden bu paketi deploy etmeni önermiyorum. Kod hazır olması, veri
hazır olması demek değil.

**İkinci bilinen kısıt:** `trailingState` diske yazılmıyor. Restart olursa açık
pozisyonlar borsadan yeniden benimsenir ve `ADOPTED_POSITION_NOT_PARITY` diye
işaretlenir. Yeni bir hata değil, v5.9.2'den gelen davranış — ama restart
sırasındaki işlemin parite verisi kirlenir. Sistem bunu telemetriye yazıyor.

---

## 7. Bundan sonra optimizasyon eklerken

Optimizasyonlar **doğrudan bu `server.js`'e** eklenecek. Her eklemede:

1. `node V500_CANLI_TEST.js` → 79/79 kalmalı
2. `node 99_TEST_KOSUCU.js` → BEKLENMEYEN HATA **0** kalmalı
3. Yeni bir kural ekleniyorsa açılış kapısındaki `sozlesme={}` nesnesine de eklenmeli —
   böylece o kural da sessizce kapanamaz

Ön kayıtlı hipotezler (bir sonraki temiz partide, eşik ~2.6, gereken n≥74):
`entryBody1mAtr` (d=−1,45) · `dTakerBuySellRatio` (d=−1,31) · `entryChg4hPct` (d=+1,30)
