# V6.4.6 — BE_GEO + ZAYIF_KURULUM_ELEME

Olcum tabani: **75 canli islem (net -68,77$)** + **1060 backtest sinyali**.

## Canli tablo (kok sebep motoru)

```
GIRIS   28 islem  -138,21$      GEC_GIRIS          15  -72,38$
temiz   21 islem  +112,08$      YAPI_BOZULDU       13  -65,82$
CIKIS   18 islem   -41,52$      ZIRVE_GERI_VERILDI 10  -50,86$
PLAN     7 islem    -1,05$
```

Son 4 islem: NIL +9,97$ · **ROBO +0,71$ BREAK_EVEN_SL** · TRUMP -8,79$ · MAGMA -11,58$

## Yama 1 — basa-bas esigi (CIKIS tarafi)

Backtest `h6tf_core.py:182`:
```
geo    = clamp(slPct/1.7, 1, 3)          <- STOP GENISLIGIYLE OLCEKLENIR
be_thr = max(.8*r390, .65) * geo
```
Canli `server.js:22523` (eski):
```
breakEvenAt = (cfg.aiBE || 0.8) * r390K  <- geo YOK, taban YOK
```

Plan SL %5,5'te backtest **%2,40**'ta kurar, canli **%0,80**'de — **3 kat erken**.
MAGMA +%1,28'de SL tasindi, geri cekilme stopu yedi, fiyat sonra +%7 yapti.
ROBO ayni sekilde +0,71$'da kesildi.

**Onemli:** `state.slPct` bu kodda acilista HIC yazilmiyor. Onu kullansaydik `geo`
hep 1 cikar, yama sessiz NO-OP olurdu. Gercek stop mesafesi `entryPrice`/`currentSL`
farkindan turetiliyor; hangi kaynagin kullanildigi her pozisyonda loglanir:

```
📐 SYMBOL V646 BE esigi: %0.80 → %2.40 · SL %5.50 (currentSL) · geo 3.00
```

`geo 1.00` ve `(YOK)` gorursen yama fiilen calismiyordur.

## Yama 2 — backtestte DE zarar eden imzalar elendi

Backtest (258$ bakiye · 50$ marj · 7x):
```
SWEEP_RECLAIM_HIGH_ATR   n=16  stop %56,2   -7,119$/islem   toplam -113,9$
CEKIC_ALT_FITIL          n=25  stop %40,0   -2,163$/islem   toplam  -54,1$
--- karsilastirma ---
KURU_SONRASI_SPIKE       n=60  stop %18,3   +9,409$/islem
CIFT_DIP                n=229  stop %19,7   +6,027$/islem
MOMENTUM_KIRILIMI        n=92  stop %14,1   +5,853$/islem
```

SWEEP canlida da kaybediyor: **PF 0,74 · -17,92$ (n=24)**. Iki bagimsiz kaynak ayni
seyi soyluyor. Kodda zaten R448 notu var ("SWEEP_RECLAIM mekanik motordan CIKARILDI")
ama filtre uygulanmamis; imza uretilmeye devam ediyordu.

## Degismeyenler

Marj sozlesmesi (1 poz · 50-100$ · %8 risk · 7x), TOP24, R495 kabul zinciri,
V637 PUSU devri, V634 risk sozlesmesi, V628 koprusu, Binance guvenlikleri.
`index.html` degismedi.

## Geri alma (deploy gerekmez)

```
V646_BE_GEO="0"        basa-bas eski haline doner
V646_ZAYIF_ELEME="0"   iki kurulum tekrar acilir
```

## Dogrulama
```
node --check        TEMIZ
testler             34/34 gecti
boot testi          35sn ayakta, 0 calisma zamani hatasi
diff                32 ekleme / 2 silme (CRLF korundu)
```
