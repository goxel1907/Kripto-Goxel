# GitHub → V5.0.2A TESTNET geçişi

## Eski repo durumu
- Eski `server.js` SHA-256: `83d129b11d393bff2b0db85dc285af12e1dd00fc23d04825bfc1a8b5be0a45f9`
- Eski server build etiketi: `V4_7_4_41_PROBE_PERSIST`
- Eski `package.json` adı/sürümü: `v4.7.4.10`
- Yani eski GitHub kökü kendi içinde sürüm olarak karışık/stale.

## Ne yapılacak
GitHub deploy kökündeki eski backend dosyalarını V5.0.2A paketindekilerle değiştir.

Özellikle:
1. `server.js` → V5.0.2A `server.js` (SHA `16b2baf3cba97a4f8d533cc47fa9f4429fbe2faf27dfe59585148b76ea24bc70`)
2. `package.json` → V5.0.2A `package.json`
3. Ortak `V474*.js` test dosyalarını da V5.0.2A kopyalarıyla değiştir.
4. Eski kökteki SONDA testlerini (`V47432`, `V47434`…`V47441`) kökten kaldır. Arşiv gerekiyorsa paket içindeki `_SONDA_TESTLERI_CANLIDA_GECERSIZ/` klasöründe tutulur.
5. Yeni testleri/build kanıtlarını ekle: `V47442`, `V47443`, `V500`, `V501`, `V502`, `V502A`, `99_TEST_KOSUCU.js`, build script, manifest ve blocker registry.
6. `server.rebuilt.js` runtime tarafından kullanılmaz; reproducibility kanıtıdır, repoda kalabilir.

## /data hakkında
`/data` klasörünü topluca SİLME.
Yeni `TESTNET_SESSION_RESET_ID=V502A_TEMIZ_OLCUM_1` ledger/evidence/state dosyalarını yeni session tag altında ayırır.
`/data/unresolved_orders.json` session-tag kullanmaz ve güvenlik için kalıcıdır. Bunu körlemesine silme; deploy sonrası parity/status üzerinden kilitli sembol varsa önce incele.

## Netlify / index.html
Yüklenen `Kripto-Goxel-main.zip` içinde hiçbir `.html` dosyası yok. V5.0.2A backend paketinde de panel HTML yok.
Bu nedenle mevcut Netlify `index.html` dosyasını görmeden birebir `index_v502a.html` üretilmiş/denetlenmiş sayılmaz.
Mevcut panel HTML ayrıca alınmalı; `/api/canli/parity-gate` kartı onun üzerine eklenmeli.
