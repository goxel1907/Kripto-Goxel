# V6.7.2 — Canlı grafik testi iki mantık hatası buldu

## Test nasıl yapıldı

ZORA'nın kendi kanıt paketinde (`ZORA_1788162608211.evidence.json`) botun **karar
anındaki ham mumları** duruyor: `initialRest.klines.{1m,3m,5m,15m,1h,4h}`, her biri
119 mum. `server.js`'ten **gerçek dedektörleri** (satır 7177–7440: `r483Pivots`,
`r484Structure`, `r484Trendline`, `r483Fvg`, `r483Ob`, `r483Liquidity`, `r483Fib`)
ayrı bir modüle çıkarıp o mumlara koşturdum. Simülasyon değil — botun kendi kodu,
botun o an gördüğü veri.

## Bot ne görüyordu (31.08.2026 07:50:02Z, giriş 0,010209)

| TF | Yapı | Aralık poz. | En yakın engel | Arz OB |
|---|---|---|---|---|
| **1m** | COMPRESSION | **%95** | **SWING_HIGH 0,010217 → %0,08** | 0,010158–0,010296 **İÇİNDE** |
| 3m | **DOWN_LH_LL** | %36 | 0,010277 → %0,67 | 0,010189–0,010338 **İÇİNDE** |
| 5m | **DOWN_LH_LL** | %44 | 0,010277 → %0,67 | 0,010073–0,010312 **İÇİNDE** |
| 15m | COMPRESSION | %71 | 0,010574 → %3,58 | 0,010613–0,011239 |
| 1h | UP_HH_HL | %80 | 0,011239 → %10,09 | — |
| 4h | EXPANSION | %88 | — | — |

1m Fibonacci: **SHORT** OTE 0,010223–0,010377 — girişin %0,14 üstünde başlıyor.
3m/5m Fibonacci: SHORT OTE, geri çekilme derinliği **%94–96** (short kurulumunun
tamamlandığı yer).

**Yani göz gördü:** üç zaman diliminde birden arz bölgesinin içinde, anlık aralığın
%95'inde, ilk engel %0,08 ötede, 3m ve 5m yapı düşüş trendinde. Bot LONG açtı.

## Hata A — çözücü 1m'i HİÇ okumuyordu

`fallbackObstacles` listesi: 3m · 5m · 15m · 30m · 1h · 4h. **1m yok.**

| | seviye | mesafe | R/R |
|---|---|---|---|
| Botun kaydettiği ilk engel | 0,0105935 (15m/1h swing) | %3,77 | **0,85** → kapıyı geçti |
| Gerçek en yakın engel (1m) | 0,010217 | **%0,08** | **0,023** |

R/R = (0,010217 − 0,010209) / (0,010209 − 0,00985985) = **0,023**.
Bu ne eski 0,35 kapısını ne de V6.7.0'ın 0,10 kapısını geçebilirdi.

**Düzeltme:** `fallbackObstacles`'a 1m likidite, 1m arz OB ve 1m ayı FVG eklendi.
Liste yine girişin üstünde filtrelenip sıralanıyor ve `[0]` alınıyor — **yeni TF
ancak DAHA YAKIN bir seviye varsa bağlar, asla uzaklaştıramaz.**

## Hata B — ilk engel kendi hedefinin ötesindeydi

Kayıtta: `firstObstacle 0,0105935` · `targetLiquidity 0,010277`.

**Engel hedeften %3,1 daha uzakta.** Bu mantıken imkânsız — hedefine giderken
çarpacağın ilk şey, hedefinden uzakta olamaz. Hiçbir kapı yakalamadı.

**Düzeltme:** `first > target && target > entry` ise çözücü yanlış seviyeyi
seçmiştir; hedef ilk engel sayılır. R/R hesabından **önce** çalışır.
Bayrak: `V673_ENGEL_HEDEF_TUTARLILIK` (varsayılan `1`).

## Aynı kayıttaki diğer kırmızı bayraklar (bu sürümde ele alınmadı)

- `quality: 100` — aynı anda `entryReason`: *"edge 4/100 · skor 9/72 · kalite/edge
  yetersiz · sert risk aktif · late-chase:-8"*. İki sayı aynı işlemi anlatıyor ve
  taban tabana zıt.
- `timing: WAIT_BREAK_RETEST`, `trend: ASCENDING_FALSE_BREAK`, `earlyRisk: true`
  — ve `marketAllowed: true`.
- `tightStop: true` (`stopPct 1,64` < `minStopPct 4,41`); `recommendedSl 0,009758`
  önerildi, emre giden SL 0,00985985 oldu — ne özgün ne önerilen değer.
- `plannedEntry 0,01060939` vs `originalEntry 0,010209` · `v649Sapma 5,409`
  (V6.5.1'de düzeltilen ölçüm hatasının bu kayıttaki izi).

## Doğrulama

- **143/143 test.** Yeni: `tests/v673-en-yakin-engel-1m.test.js` (4 test), içinde
  ZORA aritmetiğini birebir doğrulayan bir vaka testi var.
- `v671`'in build pini de `package.json` sürümünden türetiliyor artık (v670'te
  aynı kırılganlık vardı).
- Sahte kimlikle boot: `✅ CANLI parite kapisi TEMIZ — emir yolu acik.`
- CRLF 30.447 → 30.455, `node --check` temiz, diff 8 satır.
