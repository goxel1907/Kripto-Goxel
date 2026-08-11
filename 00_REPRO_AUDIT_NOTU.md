# V5.0.2A — yalnız paket bütünlüğü düzeltmesi

Bu katman strateji davranışını değiştirmez. Claude V5.0.2 `server.js` SHA-256 değeri aynen korunur: `16b2baf3cba97a4f8d533cc47fa9f4429fbe2faf27dfe59585148b76ea24bc70`.

Düzeltilen paketleme kusurları:
- `00_BUILD_TRANSFORMASYONU.py` artık V5.0.2 değişikliklerini de uygular ve `server.rebuilt.js` ile `server.js` aynı SHA verir.
- `MANIFEST_SHA256.txt` tekil ve güncel hashlerden yeniden oluşturulur.
- blocker registry B6 ile güncellenir.
- `V502A_REPRO_INTEGRITY_TEST.js` build/manifest/blocker bütünlüğünü denetler.

Bu paket CANLI deploy adayı değildir; B1-B6 açık kalır.
