const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const root = path.join(__dirname, '..');
const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
const env = fs.readFileSync(path.join(root, 'CANLI.env'), 'utf8');
const pkg = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8'));

test('V6.4.5 locks live sizing to one position with a 50 USDT floor and 100 USDT cap', () => {
  assert.equal(pkg.version, '6.4.5');
  assert.match(server, /const V601_HARD_MARGIN_FLOOR_USDT = 50;/);
  assert.match(server, /const V601_HARD_MARGIN_CAP_USDT = 100;/);
  assert.match(env, /^R486_MAX_POSITIONS="1"$/m);
  assert.match(env, /^V601_MARJ_TABAN="50"$/m);
  assert.match(env, /^V601_MARJ_TAVAN="100"$/m);
});

test('an open position pauses every candidate-producing path but keeps position managers alive', () => {
  for (const worker of ['R385', 'R370', 'R328', 'R366']) {
    assert.match(server, new RegExp(`v644PauseCandidateWorker\\('${worker}'\\)`));
  }
  assert.match(server, /v644PauseCandidateWorker\('R442'\)/,
    'R442 pusu kontrolü de açık pozisyonda tamamen susmalı');
  assert.match(server, /function r442PusuKur[\s\S]{0,180}if\(v644PositionFocusActive\(\)\)return;/,
    'pozisyon odağında yeni R442 planı kurulmamalı');
  assert.ok((server.match(/if\(v644PositionFocusActive\(\)\)break;/g)||[]).length >= 5,
    'worker ve yardımcı veri döngüleri pozisyon açılır açılmaz kesilmeli');
  assert.match(server, /v644PauseCandidateWorker\(String\(worker\|\|'RADAR'\)/);
  assert.match(server, /v644PositionFocusActive\(\)\)\{scheduleNextScan\(10_000\);return;\}/);
  assert.match(server, /v644SetPositionFocus\(\[sym\],'PROTECTION_VERIFIED'\)/);
  assert.match(server, /v644SetPositionFocus\(mapped\.map\(x=>x\.symbol\),'FAST_POSITION_MANAGER'\)/);
  assert.match(server, /v644SetPositionFocus\(\[\.\.\.v644ConfirmedOpen\],'POSITION_SYNC_CONFIRMED'\)/);
  assert.match(server, /await checkTrailingSL\(apiKey, apiSecret, mapped\);[\s\S]{0,500}return \{skipped:'position_focus'/);
  const focusReturn = server.indexOf("return {skipped:'position_focus',symbols:mapped.map(x=>x.symbol)};");
  const candidatePool = server.indexOf("autoScanState.phase = 'ADAY_HAVUZU_HAZIRLANIYOR';", focusReturn);
  assert.ok(focusReturn > 0 && candidatePool > focusReturn, 'pozisyon odağı aday havuzundan önce dönmeli');
});

test('15m journey memory uses only earlier snapshots and produces entry/stop/target timing states', () => {
  const start = server.indexOf('function v644CandidateMemoryContext(');
  const end = server.indexOf('function v644CandidateMemoryRecent(', start);
  assert.ok(start >= 0 && end > start, '15m hafıza fonksiyonu bulunmalı');
  const context = {
    V644_15M_MEMORY_ACTIVE:true,
    V644_15M_MEMORY_HOURS:12,
    r49356Snapshots:new Map(),
    r49356Sym:v=>String(v).toUpperCase().endsWith('USDT')?String(v).toUpperCase():`${String(v).toUpperCase()}USDT`,
    r49356Round:(v,d=6)=>Number.isFinite(Number(v))?+Number(v).toFixed(d):null,
    Date,
  };
  vm.createContext(context);
  vm.runInContext(`${server.slice(start,end)}\nthis.memory=v644CandidateMemoryContext;`, context);
  const now = Date.now();
  context.r49356Snapshots.set('A', {
    symbol:'TESTUSDT', candidateProduced:true, decisionTimeMs:now-60_000,
    liveDecision:{action:'PUSU',entry:100,stop:95,target:110},
    timeframes:{'15m':{valid:true,orderBlocks:{demand:{low:98,high:100}},inefficiencies:{fvg:{}},fib:{},liquidity:{below:95,above:110},obstacle:{firstObstacle:{price:110}}}},
  });
  const reclaimWait = context.memory('TESTUSDT',94,{symbol:'TESTUSDT'});
  assert.equal(reclaimWait.state,'WAIT_RECLAIM');
  assert.equal(reclaimWait.noLookahead,true);
  assert.equal(reclaimWait.backtestObservable,true);
  const retest = context.memory('TESTUSDT',99,{symbol:'TESTUSDT'});
  assert.equal(retest.state,'ENTRY_ZONE_RETEST');
  const noChase = context.memory('TESTUSDT',111,{symbol:'TESTUSDT',parabolicChase:true});
  assert.equal(noChase.state,'NO_CHASE_AFTER_PRIOR_TARGET');
});

test('15m memory can block only a broken prior stop or a consumed-target chase', () => {
  assert.match(server, /V644_PRIOR_STOP_WAIT_RECLAIM/);
  assert.match(server, /V644_PRIOR_TARGET_NO_CHASE/);
  assert.match(server, /candidateMemory\?\.state==='WAIT_RECLAIM'/);
  assert.match(server, /candidateMemory\?\.state==='NO_CHASE_AFTER_PRIOR_TARGET'/);
  assert.match(server, /contract:'CLOSED_15M_NO_LOOKAHEAD'/);
});
