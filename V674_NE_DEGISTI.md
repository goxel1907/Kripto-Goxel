# V6.7.3 — Fibonacci ölçülebilir oldu · mum-dışı körlüğün ölçümü

## Neden bu sürüm

Geriye dönük Fibonacci ölçümünü (senin seçtiğin 2. yol) **yapamadım.**
Sebebi teknik ve doğrulanmış:

- `chartStory.researchPassive.fib` her işlemde **doğru hesaplanıyor** ve arşivde
  duruyor. ZORA kaydı: `1m: ok=true · SHORT · derinlik %84 · skor 23,48 · 33 mum`.
- Ama işlem başına kayıt **2,62 MB** ve bu alan **202.625. baytta** (belgenin
  %7,7'si). HTTP tarafı belgeyi ~70.000. baytta kesiyor — CYS kaydında chartStory
  anahtarları 58'de (`fvg`) kesildi, `researchPassive`'e hiç ulaşamadı.
- `/api/evidence/analysis-bundle.zip` içinde de yok: sadece `evidence_status.json`,
  `funnel_summary.json`, `passive_parameters.csv`, `disk.json`.
- `passive.csv` CVD/tick/whale odaklı; fib taşımıyor. Üstelik 88 dosyayı sunucuda
  açtığı için istek zaman aşımına uğradı.

87 işlemi tek tek indirmek pratik değil. **Bu yüzden ölçümü kalıcı olarak mümkün
kıldım:** fib artık `dataset.csv`'de kolon. Tek indirmeyle bütün geçmiş ölçülür.

## Ne eklendi

`r501DatasetRows` içine, `1m · 5m · 15m · 1h` için yedi kolon:

```
fib<TF>Ok · fib<TF>Side · fib<TF>InZone · fib<TF>Depth · fib<TF>Score · fib<TF>Eff · fib<TF>Bos
```

Kaynak `researchPassive.fib` — **zaten `decisionImpact:false`**. Bu sürüm yalnız
**dışa aktarımı** değiştirir. Karar yolu birebir aynı: `chartStory.fib` hâlâ
`V592_POLICY_PARITY_MODE` ile boş gidiyor, parite kırılmadı. Blok `catch` ile
fail-open: eski kayıtlarda alan yoksa CSV üretimi kırılmaz, kolonlar boş kalır.

## Yan bulgu — mum-dışı körlük ölçüldü

78 canlı işlemin araştırma veri setinde (`ds.csv`) dört kolon **sabit sıfır**:
`decisionBookImbalancePct` · `oi5mChangePct` · `bidNotional` · `askNotional`.
Aynı büyüklükler emir/dolum/kapanış anında 53–82 farklı değer taşıyor. Yani
`r592NeutralizeDecisionData` yalnız kararı değil, **karar anının kaydını da**
sıfırlıyor.

Sıfır olmayan kolonlarla ölçüm:

**Giriş anında körlük ölçülebilir bir zarar üretmiyor.** Order book dengesizliği,
CVD oranı, tick delta, spread — hiçbirinde temiz bir ilişki yok, bütün kovalar
zararda (PF 0,35–1,17). Parite kararı bu açıdan savunulabilir.

**İşlem sırasında ise çok pahalı:**

| CVD değişimi (karar→kapanış) | n | WR | PF | işlem-başı |
|---|---|---|---|---|
| −5 puandan fazla düşüş | 37 | %32,4 | **0,23** | **−3,12 $** |
| −5 … −1 | 7 | %42,9 | 1,18 | +0,47 $ |
| +1 … +5 | 9 | %55,6 | 1,21 | +0,30 $ |
| **+5 puandan fazla artış** | 13 | **%76,9** | **6,77** | **+3,63 $** |

CVD'si ≥5 puan düşen 37 işlem toplam **−115,60 $** kaybettirdi — hesabın tüm net
zararından (−90,88 $) fazlası. Ve `exitImpact:false` canlı mikro-yapıyı çıkışlarda
da yasaklıyor.

**Uyarı — bu henüz kanıt değil:** `dCvdRatio` karar→kapanış ölçüldüğü için sonucun
bir kısmını zaten içinde taşıyor (düşen işlemde CVD de düşer). Gerçek test, işlem
*sırasında* sabit aralıklarla örneklenen CVD'nin sonucu **önceden** haber verip
vermediğine bakmalı. Kanıt paketlerindeki `samples` dizisi (ZORA'da 64 örnek) bunu
taşıyor; ölçüm oradan yapılmalı.

## Doğrulama

- **147/147 test.** Yeni: `tests/v674-fibonacci-olculebilir.test.js` (4 test),
  biri parite kilidine dokunulmadığını ayrıca doğruluyor.
- Sahte kimlikle boot: `✅ CANLI parite kapisi TEMIZ — emir yolu acik.`
- CRLF 30.455 → 30.474, `node --check` temiz.
