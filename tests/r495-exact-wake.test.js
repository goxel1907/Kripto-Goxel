const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.join(__dirname, '..');
const source = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
const env = fs.readFileSync(path.join(root, 'CANLI.env'), 'utf8');

test('R495 exact wake retries instead of disappearing while the scanner is busy', () => {
  assert.match(source, /function r495ScheduleExactWake\(fullSymbol,key,candidateTs,retryDelayMs=null\)/);
  assert.match(source, /if\(autoRunning\)[\s\S]*?BUSY_RETRY[\s\S]*?r495ScheduleExactWake\(fullSymbol,key,candidateTs/);
  assert.match(source, /gapLeft>0[\s\S]*?SCAN_GAP_RETRY[\s\S]*?r495ScheduleExactWake\(fullSymbol,key,candidateTs/);
  assert.match(source, /NON_FINAL_RETRY/);
  assert.match(source, /pendingWakeNow:r495ExactWakeTimers\.size/);
});

test('R495 wake remains rate-safe and never sends a late order', () => {
  assert.match(source, /oneTimerPerSignal:true/);
  assert.match(source, /if\(r495ExactWakeTimers\.has\(key\)\)return/);
  assert.match(source, /if\(now>deadline\)[\s\S]*?WINDOW_MISSED/);
  assert.match(env, /^R495_EXACT_WAKE_RETRY_MS="2500"$/m);
  assert.match(env, /^R495_EXACT_WAKE_SETTLE_MS="1200"$/m);
});

test('decision evidence cannot label a final WAIT as MARKET', () => {
  assert.match(source, /const finalSide=String\(ai\?\.side\|\|''\)\.toUpperCase\(\),finalWait=finalSide==='WAIT'/);
  assert.match(source, /finalWait\?'PUSU':auth\?\.action/);
});
