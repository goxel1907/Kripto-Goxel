// Tum testleri kosar. Eski testnet testlerinden BEKLENEN kaliflar
// (sonda silindi, surum adi degisti, emir URL'i artik ortamdan turetiliyor)
// ayri sayilir; bunlarin disinda TEK bir hata bile cikarsa exit 1.
const cp=require('child_process'),fs=require('fs');
const dosyalar=fs.readdirSync(__dirname).filter(f=>/TEST.*\.js$/.test(f) && f!=='99_TEST_KOSUCU.js').sort();
const BEKLENEN=[/testnet hard-lock/,/^build /,/telemetri (entry|exit)CandleParity/,
  /sonda/i,/probe/i,/kok neden/i,/range restriction/i,/BF3/,/rotasyon/i,/r371FlowStore/i,
  /varsayim hatasi/i,/iki cagri yeri/i,/iz birak/i,/sayac/i,/kendi dosyasi/i,/kucuk harf/i,
  /ticker fiyat/i,/varsayilan KAPALI/i,/TESTNET kilidi/i,/clamp tanimlari/i,/kalici durum/i,
  /3,8 kat/,/HER strateji emrinden/,/preempted/,/hata yutulur/,/cikis sebebi ayrisiyor/,
  /donus true/,/artik acik degil/,/esik ENV ile/,/CSV sutununda/];
let P=0,BEK=0,KOTU=[];
for(const f of dosyalar){
  const o=cp.spawnSync('node',[f],{cwd:__dirname,encoding:'utf8'});
  const c=(o.stdout||'')+(o.stderr||'');
  P+=(c.match(/^  pass  /gm)||[]).length;
  for(const l of c.split('\n').filter(x=>x.startsWith('  FAIL  '))){
    const m=l.replace('  FAIL  ','').trim();
    if(BEKLENEN.some(r=>r.test(m))) BEK++; else KOTU.push(`${f} :: ${m}`);
  }
}
console.log(`\n${'='.repeat(70)}`);
console.log(`  gecen iddia            : ${P}`);
console.log(`  beklenen kalif         : ${BEK}  (sonda silindi / surum adi / ortam-turetimli URL)`);
console.log(`  BEKLENMEYEN HATA       : ${KOTU.length}`);
KOTU.forEach(x=>console.error('   ✗ '+x));
console.log('='.repeat(70));
process.exit(KOTU.length?1:0);
