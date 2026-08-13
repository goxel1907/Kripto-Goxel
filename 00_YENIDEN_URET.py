# -*- coding: utf-8 -*-
# TAM YENIDEN URETIM ZINCIRI: V4.7.4.43(yamali) -> V5.0.2 -> V5.0.3
# Cikti: server.rebuilt.js   Beklenen: server.js ile BYTE-FOR-BYTE ayni
import pathlib, subprocess, hashlib, sys, shutil, tempfile, os
kok = pathlib.Path(__file__).parent.resolve()
kaynak = kok/'SOURCE_V47443_PATCHED'/'server.js'
if not kaynak.exists(): sys.exit('SOURCE_V47443_PATCHED/server.js yok')
with tempfile.TemporaryDirectory() as td:
    td = pathlib.Path(td)
    (td/'SOURCE_V47443_PATCHED').mkdir()
    shutil.copy(kaynak, td/'SOURCE_V47443_PATCHED'/'server.js')
    for betik in ('00_BUILD_TRANSFORMASYONU.py','00_V503_YAMA.py','00_V504_YAMA.py','00_V505_YAMA.py','00_V506_YAMA.py','00_V507_YAMA.py','00_V508_YAMA.py','00_V509_YAMA.py','00_V510_YAMA.py'):
        shutil.copy(kok/betik, td/betik)
    for betik in ('00_BUILD_TRANSFORMASYONU.py','00_V503_YAMA.py','00_V504_YAMA.py','00_V505_YAMA.py','00_V506_YAMA.py','00_V507_YAMA.py','00_V508_YAMA.py','00_V509_YAMA.py','00_V510_YAMA.py'):
        r = subprocess.run([sys.executable, betik], cwd=td, capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f'ZINCIR DURDU [{betik}]:\n{r.stdout}\n{r.stderr}')
        print(f'  ✓ {betik}')
        if betik=='00_BUILD_TRANSFORMASYONU.py' and (td/'server.rebuilt.js').exists():
            shutil.move(str(td/'server.rebuilt.js'), str(td/'server.js'))
    uretilen = (td/'server.js').read_bytes()
(kok/'server.rebuilt.js').write_bytes(uretilen)
a = hashlib.sha256(uretilen).hexdigest()
b = hashlib.sha256((kok/'server.js').read_bytes()).hexdigest()
print(f'  rebuilt : {a}')
print(f'  server  : {b}')
print(f'  SONUC   : {"BYTE-FOR-BYTE AYNI ✓" if a==b else "UYUSMUYOR ✗"}')
sys.exit(0 if a==b else 1)
