# V6.7.9 — CANLI ÇÖKME DÜZELTİLDİ + BEYİN GERÇEKTEN DOLUYOR + PANEL TEMİZLİĞİ

## 1. Kritik: bot her tutarsız engelde ÇÖKÜYORDU (benim V6.7.2 hatam)

Panel logu, 02.09.2026 17:11:17:

    SKYAI AI beyin bağlama hatası: Assignment to constant variable.

V6.7.2'de eklediğim V673-B tutarlılık yaması `first` değişkenini yeniden atıyor:

    if (V673_ENGEL_HEDEF_TUTARLILIK && first > 0 && ... ) { first = target; }

Ama `first` **const** ile tanımlıydı. Koşul her tuttuğunda `r486EntryTruthGuard`
TypeError fırlatıyor, o aday için TÜM giriş-gerçeği hesabı (recommendedSl /
firstObstacleRR / marketAllowed) yok oluyor, istisna emir döngüsündeki geniş
try/catch'e düşüyor ve aday sessizce eleniyordu.

Yani "önemsiz bir kural yüzünden kaçırılan fırsat" değil — doğrudan çökme.
Ve tam da tutarsızlığı düzeltmek için yazdığım kod, düzeltmeye çalıştığı adayı
öldürüyordu.

Düzeltme: `const first` → `let first`.

Kaynağın tamamı acorn ile kapsam-duyarlı taranarak doğrulandı:
**dosyada başka const-yeniden-atama yok (0 adet).**

## 2. Beyin tamponu neden boştu

Aynı turda panel: Taranan 16 / Açılan 0 / Atlanan 16 — ama beyin "0 kayıt".

V679 kaydı emir döngüsünün EN SONUNDAYDI; önünde ~15 ayrı `continue` kapısı var
(R493 final kilidi, R308I, R325, mekanik BEKLE…). Adayların hepsi çok daha önce
eleniyordu, kayıt satırına hiç ulaşılmıyordu.

- **V680-C**: kayıt artık karar kapılarından ÖNCE açılıyor.
- **V680-B**: `v679SonKarar()` aynı turdaki kaydın sonucunu yazıyor (3 dk pencere).
- **V680-D**: `markAutoSkip` her atlama sebebini beyne geçiriyor — tek kancayla
  tüm atlama yolları kapsanıyor.
- **V680-F**: açılan emir de yazılıyor; kart artık "gördü → ne yaptı" zincirini
  tamamlıyor.
- **V680-E**: V678 bloğundaki artık çift olan kayıt kaldırıldı.

Uç özeti genişledi: `emir`, `atlandi`, `beklemede` ve `enCokSebep` (en sık 6
atlama sebebi, adetli). Kart bunları en üstte gösteriyor.

Hiçbiri karara dokunmuyor: `v679SonKarar` içinde markAutoSkip / return false /
blocked yok, testle sabitlendi.

## 3. index.html — bilgi kirliliği temizliği

Her biri kaynaktan kanıtlandı, tahminle silinen hiçbir şey yok.

**Kaldırıldı (ölü olduğu kanıtlanmış):**

- *Kaldıracı otomatik ayarla* + *Legacy ön-kaldıraç tavanı (125)* — `V592_LEVERAGE_LOCK=7`
  aktifken kaldıraç kilitli ve farklı kaldıraçla gelen emir `LEVERAGE_PARITY_LOCK`
  ile Binance'e gitmeden reddediliyor (server.js:21576). Sunucuya giden varsayılan
  değerler değişmedi.

**Doğrusuyla değiştirildi:**

- *USDT/işlem* alanı panelde **30** gösteriyordu; sözleşme 50$ taban / 100$ tavan.
  `R497_SLOT_MARGIN_USDT = V601_HARD_MARGIN_FLOOR_USDT = 50` sabit ve bileşik marj
  50–100$ arasına kelepçeli (server.js:7149) — panel değeri zaten geçmiyordu.
  Alan artık salt-okunur sözleşme metni; gönderilen gövde de 50.
- *SL % / TP % / Min R/R* → "yalnız ön-hesap" etiketi + açıklama kutusu: borsaya
  giden gerçek stop/hedef R495 planından türetiliyor
  (`ai.entry/sl/tp = plan.*`, server.js:28378).
- R486 banner'ındaki "fırsata göre Binance izinli maksimum kaldıraç" ifadesi
  gerçekle değiştirildi: 7x kilitli, max 1 pozisyon, marj 50$, kâr hasadı SHADOW.

**Gösterge listesi ikiye ayrıldı** — en büyük yanıltma buradaydı. Panel 14 göstergeyi
"Aktif" diye sıralıyordu; oysa `V592_POLICY_PARITY_MODE = true` (server.js:472)
karar anında şunları sıfırlıyor (`r592NeutralizeDecisionData`):

    cvdDelta:0 · aggBuy/aggSell:0 · takerRatio:1 · orderBookImbalance:0
    fundingRate:0 · oiDegisim:0 · iceberg/liquidations nötr
    fib[8 zaman dilimi] = {ok:false, policyNeutral:true}
    live15 = null  (kapanmamış 15m mum)

Yani CVD, order book, funding, OI, iceberg, likidasyon, Smart Money L/S ve
**Fibonacci/OTE** karara girmiyor — toplanıyor ama nötrleniyor. Liste artık
"Karara Giren" (mum tabanlı: yapı, SMC/ICT, FVG, order block, likidite seviyeleri,
trend çizgisi, formasyon, ATR, RSI/MACD, R39 S/R) ve "Toplanıyor ama KARARA
GİRMİYOR" diye ikiye ayrıldı.

## Testler

180/180 geçiyor. Yeni: `tests/v680-cokme-ve-beyin.test.js` (8 test) —
`let first` pini, V673-B'nin yerinde durması, kaydın kapılardan önce olması,
çift kaydın olmaması, markAutoSkip/markAutoOpened kancaları, v679SonKarar'ın
karara dokunmaması, özetin karar dağılımı vermesi.

`const first` metnine bakan 3 eski test (v650/v662/v663) yeni hâle güncellendi.
