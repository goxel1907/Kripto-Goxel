# MAGMA tipi zararların sebebi — başa-baş eşiği backtestten farklı

**28 Ağustos 2026 · canlı V6.4.5 · kaynak `Kripto-Goxel-v644-work`**

fino.txt'te ChatGPT'nin "henüz çözmedim" dediği 1. madde:
> *MAGMA erken başa-baş çıkışı: Stop, yalnız anlık %1,28 kârla değil; kapanmış 15m mum teyidi veya en az 0.5R–1R ilerleme sonrasında taşınmalı.*

Sebebini buldum. Tahmin değil — iki tarafın formülünü de okudum.

---

## Fark tek bir çarpanda

**Backtest** (`h6tf_core.py` satır 182 + 198-200):

```python
geo     = clamp(slpct/1.7, 1, 3)          # <<< STOP GENISLIGIYLE OLCEKLENIR
r390    = clamp(atr/3, .3, 1)
base_be = .8 * r390
be_thr  = max(base_be, .65) * geo         # <<< geo CARPANI
be_lock = max(.08, .22*r390)              # SL girisin %0,08-0,22 USTUNE

if not be and real >= be_thr:
    ns = entry*(1 + be_lock/100)
    if ns > current_sl: current_sl = ns; be = True
```

**Canlı** (`server.js` satır 22523):

```js
const r390K       = r390Atr > 0 ? Math.min(1, Math.max(0.3, r390Atr/3.0)) : 1;
const breakEvenAt = (Number(cfg.aiBE) > 0 ? Number(cfg.aiBE) : 0.8) * r390K;
//                                                                   ^^^^^ geo YOK, taban YOK
```

Canlıda **`geo` çarpanı ve `max(...,0.65)` tabanı yok.**

---

## Rakamla ne demek

Plan SL'i %5,5 olan tipik bir işlemde:

```
geo = clamp(5,5/1,7 , 1, 3) = 3

BACKTEST  be_thr = max(0,8 x 1 , 0,65) x 3 = %2,40   <- BE burada kurulur
CANLI     breakEvenAt = 0,8 x 1            = %0,80   <- BE burada kurulur
                        (panelde aiBE=0,5 ise %0,50)
```

**Canlı, backtestten 3–5 kat erken başa-baş kuruyor.**

MAGMA kaydı tam bu: giriş 0.39077, fiyat **+%1,28**'de SL 0.38249 → 0.39163'e taşınmış, normal geri çekilme stopu tetiklemiş, çıkış 0.39033. Sonra fiyat 0.418'e gitmiş (+%7). Backtestin eşiğiyle (%2,40) o taşıma **hiç yapılmayacaktı.**

İkinci fark: backtest SL'i girişin sadece **%0,08–0,22 üstüne** koyuyor. Canlıda taşıma daha agresif — normal salınım stopu yiyor.

---

## Neden bu kadar önemli

Ölçtüm: backtestin kârının **%97'si `DYNAMIC_STOP`'tan** (trailing) geliyor — 1060 işlemde +498,3R / +513,8R. Başa-baş, trailing daha işe başlamadan pozisyonu 0'da kapatıyor. Yani kâr motorunun önünü kesiyor.

Modelledim (812 işlem, 50$ marj, %8 bütçe):

```
BE yok            +2,269 $/islem
BE %0,5 (canli)   -4,234 $/islem
BE %4,0           -3,070 $/islem
```

> **Bu model bir ÜST SINIR.** Zirve ile geri çekilmenin hangisinin önce geldiği veride yok; her `dus>0` durumunu "BE tetiklendi" saydım, bu hasarı abartıyor. Kesin rakam değil — ama yön, iki taraftaki formül farkıyla bağımsız olarak doğrulanıyor.

---

## Önerilen düzeltme (tek satır)

```js
// ONCE
const breakEvenAt = (Number(cfg.aiBE) > 0 ? Number(cfg.aiBE) : 0.8) * r390K;

// SONRA — backtestin kendi formulu
const _geoBE = Math.max(1, Math.min(3, Number(state.slPct || slPct || 1.7) / 1.7));
const breakEvenAt = Math.max((Number(cfg.aiBE) > 0 ? Number(cfg.aiBE) : 0.8) * r390K, 0.65) * _geoBE;
```

`aiBE` panelden `[0.3, 4]` aralığına kelepçeli olduğu için **env'den çözülemez** — kod değişikliği şart.

Trailing'e dokunulmuyor. `breakEvenSet` yine kuruluyor (trailing bloğu ona bağlı, `if (!action && state.breakEvenSet)`), sadece **daha geç** kuruluyor. Yani kâr motoru kapanmıyor, erken tetiklenmesi engelleniyor.

---

## Ayrıca: 50 USDT marj DOĞRU

Şüphelenmiştim, ölçtüm, yanılmışım. Sabit %8 bütçede kayıp zaten bakiyenin %8'ine sabit; marj yalnız kazanan tarafı büyütüyor:

```
bakiye 108,56$ · 1 poz · %8 · 7x · 812 islem
  30$ marj -> SL %4,14 -> stop %34,6 -> +1,707 $/islem
  50$ marj -> SL %2,48 -> stop %49,1 -> +2,269 $/islem   <- olculen en iyi
  70$ marj -> SL %1,77 -> stop %61,5 -> +1,474 $/islem
```

ChatGPT'nin 30 → 50 değişikliği veriyle destekleniyor. Bakiye 150$'ı geçerse optimum 70$'a kayıyor.

---

## Doğrulanan diğer şeyler

V6.3.x'te yaptığım sözleşmenin tamamı canlıda duruyor:

```
R486_MAX_POSITIONS=1 · V634_TOPLAM_RISK_PCT=8 · V601_SL_MOD=aktif
V625_TEPE_VETO=0 · V628_ATR_TAVAN=8.0 · V637_PUSU_R495E_DEVRET=1
V625_KAR_TASIMA=1 · V607_KAR_KILIDI=0 · 7x kilit · 100$ tavan
```

HEMI/MELANIA giriş kalitesi (ChatGPT'nin 2. maddesi) ayrı bir iş; ona dokunmadım.
