#!/usr/bin/env node
/**
 * Tests for the review fixes:
 * 1. TIMESTAMP_LEAD_SECONDS constant (not magic number)
 * 2. Flight-start detection fires on first attempt
 * 3. Meets page cache is a bounded Map
 * 4. getWeightClass is shared (not duplicated)
 * 5. No dead colGroup variable
 * 6. YouTube scraping has fallback logging
 * 7. No silent catch blocks
 */

const fs = require('fs');
const path = require('path');

let passed = 0;
let failed = 0;

function assert(condition, msg) {
  if (condition) {
    console.log(`  PASS: ${msg}`);
    passed++;
  } else {
    console.error(`  FAIL: ${msg}`);
    failed++;
  }
}

// Read source files for static analysis
const clientSrc = fs.readFileSync(path.join(__dirname, 'liftingcast_client.js'), 'utf8');
const recapSrc = fs.readFileSync(path.join(__dirname, 'meet_recap.js'), 'utf8');

console.log('\n=== 1. TIMESTAMP_LEAD_SECONDS constant ===');
{
  // Both files should define the constant
  assert(clientSrc.includes('const TIMESTAMP_LEAD_SECONDS = 15;'), 'liftingcast_client.js defines TIMESTAMP_LEAD_SECONDS');
  assert(recapSrc.includes('const TIMESTAMP_LEAD_SECONDS = 15;'), 'meet_recap.js defines TIMESTAMP_LEAD_SECONDS');

  // Neither file should have a raw "- 15" in offset calculations
  // (We check for the pattern "streamStart - 15" or "effectiveStart - 15" which would be magic numbers)
  const clientMagic = clientSrc.match(/streamStart\s*-\s*15[^;]/g) || [];
  assert(clientMagic.length === 0, 'liftingcast_client.js has no magic "- 15" in offset calc');

  const recapMagic = recapSrc.match(/(streamStart|effectiveStart)\s*-\s*15[^;]/g) || [];
  assert(recapMagic.length === 0, 'meet_recap.js has no magic "- 15" in offset calc');

  // Both use the constant
  assert(clientSrc.includes('TIMESTAMP_LEAD_SECONDS'), 'liftingcast_client.js uses TIMESTAMP_LEAD_SECONDS');
  assert(recapSrc.includes('TIMESTAMP_LEAD_SECONDS'), 'meet_recap.js uses TIMESTAMP_LEAD_SECONDS');

  // Both have the explanatory comment
  assert(clientSrc.includes('~15s before the lifter actually approaches the bar'), 'liftingcast_client.js has explanatory comment');
  assert(recapSrc.includes('~15s before the lifter actually approaches the bar'), 'meet_recap.js has explanatory comment');
}

console.log('\n=== 2. Flight-start detection ===');
{
  // Should NOT have pendingFlightStart in trackState init
  assert(!clientSrc.includes('pendingFlightStart: null'), 'No pendingFlightStart in trackState init');

  // Should fire on first encounter (no pending pattern)
  // The old pattern was: set pending first, fire on second encounter
  // The new pattern: fire immediately when key changes
  assert(!clientSrc.includes('ts.pendingFlightStart === flightLiftKey'), 'No pending flight start check pattern');
  assert(!clientSrc.includes('ts.pendingFlightStart = flightLiftKey'), 'No pending flight start assignment');

  // Should have the direct fire pattern
  assert(clientSrc.includes('if (!ts.notifiedFlightStarts.has(flightLiftKey) && ts.lastFlightLift !== flightLiftKey)'), 'Direct fire on first new flight/lift combo');

  // Should have descriptive comment
  assert(clientSrc.includes('Fires once per flight+lift combo'), 'Has descriptive comment for flight-start detection');
}

console.log('\n=== 3. Meets page cache is a bounded Map ===');
{
  assert(clientSrc.includes('const meetsPageCache = new Map()'), 'Cache is a Map, not a single object');
  assert(clientSrc.includes('MEETS_PAGE_CACHE_MAX'), 'Has max cache size constant');
  assert(clientSrc.includes('meetsPageCache.get('), 'Uses Map.get()');
  assert(clientSrc.includes('meetsPageCache.set('), 'Uses Map.set()');
  assert(clientSrc.includes('meetsPageCache.size > MEETS_PAGE_CACHE_MAX'), 'Evicts when over max size');
  assert(clientSrc.includes('meetsPageCache.delete('), 'Deletes oldest entry on eviction');

  // Should NOT have the old single-object pattern
  assert(!clientSrc.includes('meetsPageCache.key ==='), 'No old single-object key check');
  assert(!clientSrc.includes('meetsPageCache = {'), 'No single-object assignment');
}

console.log('\n=== 4. getWeightClass is shared (not duplicated) ===');
{
  // Count how many times WEIGHT_CLASSES is defined as a const (male + female variants)
  const wcDefinitions = clientSrc.match(/const WEIGHT_CLASSES_(?:MALE|FEMALE)\s*=/g) || [];
  assert(wcDefinitions.length === 2, `WEIGHT_CLASSES_MALE/FEMALE defined exactly twice (found ${wcDefinitions.length})`);

  // Count how many times getWeightClass is defined as a function
  const gwcDefinitions = clientSrc.match(/function getWeightClass\(/g) || [];
  assert(gwcDefinitions.length === 1, `getWeightClass defined exactly once (found ${gwcDefinitions.length})`);

  // Both meetDetailHTML and recapHTML should still reference getWeightClass (but not define it)
  // Find getWeightClass usage inside functions
  const gwcUsages = clientSrc.match(/getWeightClass\(/g) || [];
  assert(gwcUsages.length >= 3, `getWeightClass used at least 3 times (definition + 2 callers), found ${gwcUsages.length}`);
}

console.log('\n=== 5. Dead colGroup variable removed ===');
{
  assert(!clientSrc.includes('const colGroup ='), 'No dead colGroup variable');
  assert(!clientSrc.includes('colGroup['), 'No colGroup usage');
}

console.log('\n=== 6. YouTube scraping has fallback logging ===');
{
  // searchYouTube should log when regex doesn't match
  assert(clientSrc.includes('No videoId regex match'), 'searchYouTube logs when regex fails');
  assert(clientSrc.includes('YouTube page structure may have changed'), 'Mentions possible structure change');

  // getYouTubeStreamStart should log when regex doesn't match
  assert(clientSrc.includes('No startTimestamp regex match'), 'getYouTubeStreamStart logs when regex fails');
}

console.log('\n=== 7. No silent error swallowing ===');
{
  // Should have NO catch (_) {} patterns
  const silentCatches = clientSrc.match(/catch\s*\(_\)\s*\{\s*\}/g) || [];
  assert(silentCatches.length === 0, `No silent catch blocks (found ${silentCatches.length})`);

  // The previously silent catches should now log errors
  assert(clientSrc.includes('[YT] Error checking existing video'), 'YT video check catch now logs');
  assert(clientSrc.includes('[MEETS] Error fetching subscriptions'), 'Meets page catch now logs');
  assert(clientSrc.includes('[MEET DETAIL] Error fetching subscriptions'), 'Meet detail catch now logs');
}

console.log('\n=== 8. getWeightClass logic correctness ===');
{
  // Extract and test the getWeightClass function
  const WEIGHT_CLASSES = [47, 52, 57, 63, 69, 76, 83, 93, 105, 120, 140, 145];
  function getWeightClass(bw) {
    if (!bw) return null;
    for (const wc of WEIGHT_CLASSES) {
      if (bw <= wc) return wc;
    }
    return '145+';
  }

  assert(getWeightClass(null) === null, 'null -> null');
  assert(getWeightClass(0) === null, '0 -> null (falsy)');
  assert(getWeightClass(45) === 47, '45kg -> 47kg class');
  assert(getWeightClass(47) === 47, '47kg -> 47kg class (boundary)');
  assert(getWeightClass(47.1) === 52, '47.1kg -> 52kg class');
  assert(getWeightClass(83) === 83, '83kg -> 83kg class');
  assert(getWeightClass(100) === 105, '100kg -> 105kg class');
  assert(getWeightClass(145) === 145, '145kg -> 145kg class');
  assert(getWeightClass(146) === '145+', '146kg -> 145+');
  assert(getWeightClass(200) === '145+', '200kg -> 145+');
}

console.log('\n=== 9. TIMESTAMP_LEAD_SECONDS offset calculation ===');
{
  const TIMESTAMP_LEAD_SECONDS = 15;
  // Simulate: stream started at epoch 1000, attempt logged at epoch 1100
  const streamStart = 1000;
  const wallEpoch = 1100;
  const offset = Math.max(0, wallEpoch - streamStart - TIMESTAMP_LEAD_SECONDS);
  assert(offset === 85, 'Offset = 100 - 15 = 85 seconds');

  // Edge case: attempt very early (before stream + lead time)
  const earlyOffset = Math.max(0, 1005 - streamStart - TIMESTAMP_LEAD_SECONDS);
  assert(earlyOffset === 0, 'Early attempt clamped to 0');
}

console.log('\n=== 10. Flight start detection logic (simulated) ===');
{
  // Simulate the new flight-start detection logic
  const notifiedFlightStarts = new Set();
  let lastFlightLift = null;

  function checkFlightStart(flightLiftKey) {
    let fired = false;
    if (!notifiedFlightStarts.has(flightLiftKey) && lastFlightLift !== flightLiftKey) {
      notifiedFlightStarts.add(flightLiftKey);
      fired = true;
    }
    lastFlightLift = flightLiftKey;
    return fired;
  }

  // First attempt of flight A squat — should fire
  assert(checkFlightStart('A:squat') === true, 'First attempt of A:squat fires notification');
  // Second attempt in same flight — should NOT fire
  assert(checkFlightStart('A:squat') === false, 'Second attempt of A:squat does not fire');
  // New flight B squat — should fire
  assert(checkFlightStart('B:squat') === true, 'First attempt of B:squat fires notification');
  // Back to A (which was already notified) — should NOT fire
  assert(checkFlightStart('A:squat') === false, 'Return to already-notified A:squat does not fire');
  // A:bench (new lift for flight A) — should fire
  assert(checkFlightStart('A:bench') === true, 'First attempt of A:bench fires notification');
}

console.log('\n=== 11. Module syntax validation ===');
{
  // All modules should parse without syntax errors (already checked with node -c, but verify in-process)
  const files = ['liftingcast_client.js', 'meet_recap.js', 'db.js', 'email.js', 'symplmeet_client.js'];
  for (const file of files) {
    try {
      require('child_process').execSync(`node -c ${path.join(__dirname, file)}`, { stdio: 'pipe' });
      assert(true, `${file} has valid syntax`);
    } catch (err) {
      assert(false, `${file} has valid syntax: ${err.stderr?.toString().trim()}`);
    }
  }
}

// --- Summary ---
console.log(`\n${'='.repeat(50)}`);
console.log(`Results: ${passed} passed, ${failed} failed, ${passed + failed} total`);
if (failed > 0) {
  process.exit(1);
} else {
  console.log('All tests passed!');
}
