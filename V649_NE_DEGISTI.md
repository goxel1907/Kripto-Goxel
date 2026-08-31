# V6.4.9 — V646/V648-D GERİ ALINDI (çift geo hatası)

**30 Ağustos 2026 · kendi yamamın hatası, ölçümle bulundu**

## Ne yanlıştı

`server.js:22603`'teki `breakEvenAt` **operatif eşik değil, TABAN.** Zincir:

```
22603  breakEvenAt                                   (taban)
23002  r192BreakEvenAt = breakEvenAt                 (momentum nefesi)
23323  r283DynamicBE = runner ? max(r192BreakEvenAt*geo, 2*geo, slPct*.75)
                              : max(r192BreakEvenAt, .65) * geo      ← KARARI BU VERİR
```

`geo` (= `r339GeoScale`) **zaten 23323'te uygulanıyordu** ve zincirin tamamı backtest ile birebir aynıydı:

```
backtest non-runner: max(base_be, .65) * geo
backtest runner    : max(base_be*geo, 2*geo, slpct*.75)
```

V646'da geo'yu **tabana da** ekledim → 23323 bir kez daha çarptı → **geo²**.

```
slPct   backtest   V646'lı hali
%2,07     %0,97      %1,19      1,22× geç
%3,00     %1,41      %2,49      1,76× geç
%4,20     %1,98      %4,88      2,47× geç
%5,50     %2,40      %7,20      3,00× geç
```

Yani başa-baş, olması gerekenden **3 kata kadar GEÇ** kuruluyordu (non-runner'da).

## Neden canlıda fark etmedim

Runner işlemlerde `2*geo` terimi çoğu zaman baskın çıkıyor, o yüzden sonuç değişmiyor.
ZKPUSDT tam öyle oldu: runner, slPct %3,47 → **her iki sürümde de %4,08**.
Canlı logda gördüğüm `%0.80 → %1.63` satırı ise operatif eşik değil, tabandı — ben onu
eşik sanıp "yama çalıştı" dedim. Yanlıştı.

## Yanlış olan diğer teşhisim

"Kâr-kilit merdiveninde `ktm*geo` eksik" dedim — **yoktu, vardı:**

```js
23346:  const _runnerMult = state.aiRunner ? 1.8 : 1.0;
        const kT1 = karTasima1 * _runnerMult * r339GeoScale, ...
23389:  trailPctEff = Math.min(trailPct * r339GeoScale, Math.max(trailPct, slPct*1.1));
23392:  if (state.aiRunner) trailPctEff = Math.max(trailPctEff * 1.8, 2.5);
```

Merdiven ve trailing **zaten backtest paritesinde.** Önerdiğim "V6.4.9 düzeltmesi" olduğu gibi
uygulansaydı geo ve ktm ikinci kez eklenecekti — sorunu büyütecekti.

## Gerçekten eksik olan tek şey

`22788` MAX_SÜRE çıkışı. Backtest tam eşiği kullanıyor (`real < be_thr*.5`), canlı **ham tabanı**
kullanıyordu → bu çıkış backtestten çok daha erken tetikleniyordu. Operatif eşikle düzeltildi.

## Yapılanlar

1. `breakEvenAt` tabana geri döndürüldü — çift geo bitti
2. MAX_SÜRE çıkışı operatif eşiği kullanıyor (backtest paritesi)
3. Operatif eşik pozisyon başına bir kez loglanıyor:
   `🎚️ SYM V649 BE OPERATIF esik %X · taban %Y · geo Z · slPct %W · RUNNER/PROTECT/NORMAL`
   Bir daha "taban"ı "eşik" sanmak mümkün olmasın diye.

## Dokunulmayanlar

`V646_ZAYIF_ELEME` (zayıf kurulum elemesi), `V648_EN_YAKIN_ENGEL`, `V648_PIVOT_ENGEL` —
bunlar giriş tarafı, ayrı işler, yerinde duruyor.

## Doğrulama

69/69 test · `node --check` temiz · boot temiz · server.js 25 satır · CRLF korundu (30.272 → 30.289)

## Açık kalan

**−20$'ın sebebi hâlâ bilinmiyor.** Çıkış merdiveni backtest paritesindeymiş; kaybın oradan
geldiği tezim çürüdü. Yeni bir sebep uydurmuyorum — canlı işlem kayıtları (giriş, zirve, çıkış
sebebi, çıkış fiyatı) elime geçtiğinde ölçeceğim.
