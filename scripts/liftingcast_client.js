/**
 * LiftingCast CouchDB Client
 *
 * Connects to LiftingCast's public readonly CouchDB endpoint.
 * No API key or password needed — uses the same data source as
 * the public spectator board view.
 *
 * Monitors a specific lifter and detects when they are "on deck"
 * (within 2 positions of lifting).
 *
 * CouchDB attempt ID format: a{attemptNumber}{liftInitial}-{lifterId}
 *   liftInitial: s=squat, b=bench, d=deadlift
 *   Example: a1d-l0jd58nr3af4 = deadlift attempt 1 for lifter l0jd58nr3af4
 *
 * Configuration (env vars or CLI args):
 *   MEET_ID / --meet-id       Meet ID (e.g. mfmnsrd1fve8)
 *   TRACK_LIFTER / --track    Lifter name to track (partial match, case-insensitive)
 *   COUCHDB_URL / --url       Custom CouchDB base URL
 *   PORT                      HTTP health check port (default: 3000)
 *
 * CLI-only options:
 *   --list-lifters            Just list all lifters and exit
 */
const http = require('http');
const https = require('https');

// --- Parse CLI arguments ---
const args = {};
for (const arg of process.argv.slice(2)) {
  if (arg.startsWith('--')) {
    const [key, ...rest] = arg.slice(2).split('=');
    args[key] = rest.length > 0 ? rest.join('=') : true;
  }
}

const COUCHDB_BASE = 'https://couchdb.liftingcast.com';

const meetId = process.env.MEET_ID || args['meet-id'] || '';
const trackName = process.env.TRACK_LIFTER || args['track'] || '';
const listLifters = args['list-lifters'] || false;
const couchdbBase = process.env.COUCHDB_URL || args['url'] || COUCHDB_BASE;
const dbUrl = `${couchdbBase}/${meetId}_readonly`;

if (!meetId) {
  console.error('Error: MEET_ID is required. Set via env var or --meet-id=...');
  process.exit(1);
}

// --- Attempt ID parsing ---
// Format: a{attemptNum}{liftInitial}-{lifterId}
const LIFT_MAP = { s: 'squat', b: 'bench', d: 'dead' };
const LIFT_ORDER = { squat: 0, bench: 1, dead: 2 };

function parseAttemptId(attemptId) {
  if (!attemptId || !attemptId.startsWith('a')) return null;
  // e.g. "a1d-l0jd58nr3af4" -> attemptNumber=1, lift=d(ead), lifterId=l0jd58nr3af4
  const match = attemptId.match(/^a(\d)([sbd])-(.+)$/);
  if (!match) return null;
  return {
    attemptNumber: match[1],
    liftName: LIFT_MAP[match[2]],
    liftInitial: match[2],
    lifterId: match[3],
  };
}

// --- HTTP health check server (Railway requires a listening port) ---
const PORT = process.env.PORT || 3000;

// In-memory meet state built from CouchDB docs
const state = {
  meet: null,
  platforms: {},   // platformId -> platform doc
  lifters: {},     // lifterId -> lifter doc
  divisions: {},   // divisionId -> division doc
  attempts: {},    // attemptId -> attempt doc
  lastSeq: '0',
};

// Tracking state per platform
const trackState = {};

// --- Compute attempt order for a platform ---
// Sort: session -> lift type -> flight -> attempt number -> weight -> lot
function computeAttemptOrder(platformId) {
  const platformLifters = Object.values(state.lifters).filter(l => l.platformId === platformId);
  const pending = [];

  for (const lifter of platformLifters) {
    if (!lifter.lifts) continue;
    for (const [liftName, attempts] of Object.entries(lifter.lifts)) {
      for (const [attemptNum, attempt] of Object.entries(attempts)) {
        if (attemptNum === '4') continue; // Skip 4th attempts for ordering
        if (attempt.result !== null && attempt.result !== undefined && attempt.result !== '') continue; // Already done
        pending.push({
          lifterId: lifter._id,
          lifterName: lifter.name,
          liftName,
          attemptNumber: attemptNum,
          weight: attempt.weight || 9999,
          lot: lifter.lot || 999,
          session: lifter.session || 1,
          flight: lifter.flight || 'Z',
          attemptId: attempt.id || `a${attemptNum}${liftName[0]}-${lifter._id}`,
        });
      }
    }
  }

  pending.sort((a, b) => {
    if (a.session !== b.session) return a.session - b.session;
    if (LIFT_ORDER[a.liftName] !== LIFT_ORDER[b.liftName]) return LIFT_ORDER[a.liftName] - LIFT_ORDER[b.liftName];
    if (a.flight !== b.flight) return a.flight.localeCompare(b.flight);
    if (a.attemptNumber !== b.attemptNumber) return a.attemptNumber - b.attemptNumber;
    if (a.weight !== b.weight) return a.weight - b.weight;
    return a.lot - b.lot;
  });

  return pending;
}

const healthServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      tracking: trackName || null,
      meetId,
      lifterCount: Object.keys(state.lifters).length,
      platformCount: Object.keys(state.platforms).length,
    }));
  } else if (req.url === '/state') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    const summary = {};
    for (const [pid, platform] of Object.entries(state.platforms)) {
      const parsed = parseAttemptId(platform.currentAttemptId);
      const currentLifter = parsed ? state.lifters[parsed.lifterId] : null;
      const order = computeAttemptOrder(pid);
      // Find current position in order
      let currentIdx = -1;
      if (platform.currentAttemptId) {
        currentIdx = order.findIndex(a => a.attemptId === platform.currentAttemptId);
      }
      const nextUp = currentIdx >= 0 ? order.slice(currentIdx + 1, currentIdx + 6).map(a => ({
        name: a.lifterName, lift: a.liftName, attempt: a.attemptNumber,
      })) : [];

      summary[pid] = {
        platformName: platform.name,
        currentLifter: currentLifter?.name || null,
        liftName: parsed?.liftName || null,
        attemptNumber: parsed?.attemptNumber || null,
        nextUp,
      };
    }
    res.end(JSON.stringify({ meetName: state.meet?.name || meetId, platforms: summary }, null, 2));
  } else {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('LiftAlert is running');
  }
});
healthServer.listen(PORT, () => {
  console.log(`[HEALTH] HTTP server listening on port ${PORT}`);
});

// --- Simple HTTPS fetch helper ---
function fetchJSON(url, options = {}) {
  return new Promise((resolve, reject) => {
    const timeout = options.timeout || 90000;
    const req = https.get(url, { timeout }, (res) => {
      if (res.statusCode < 200 || res.statusCode >= 300) {
        res.resume();
        reject(new Error(`HTTP ${res.statusCode} from ${url}`));
        return;
      }
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(body)); }
        catch (e) { reject(new Error(`Invalid JSON from ${url}`)); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout fetching ${url}`)); });
  });
}

// --- Process a CouchDB document into our state ---
function processDoc(doc) {
  if (!doc || !doc._id) return;
  const id = doc._id;

  if (id === meetId) {
    state.meet = doc;
  } else if (id.startsWith('p') && !id.startsWith('pi')) {
    // Platform document (exclude "pi" prefix if any)
    state.platforms[id] = doc;
  } else if (id.startsWith('l')) {
    state.lifters[id] = doc;
  } else if (id.startsWith('d')) {
    state.divisions[id] = doc;
  } else if (id.startsWith('a')) {
    state.attempts[id] = doc;
  }
}

// --- Initial load: fetch all docs ---
async function initialLoad() {
  console.log(`\n=== LiftAlert CouchDB Client ===`);
  console.log(`CouchDB: ${dbUrl}`);
  console.log(`Meet ID: ${meetId}`);
  if (trackName) console.log(`Tracking: "${trackName}"`);
  console.log('');

  console.log('[LOADING] Fetching all documents...');
  const result = await fetchJSON(`${dbUrl}/_all_docs?include_docs=true`);

  for (const row of result.rows) {
    if (row.doc) processDoc(row.doc);
  }

  console.log(`[LOADED] Meet: "${state.meet?.name || '(unknown)'}" | ${Object.keys(state.lifters).length} lifters | ${Object.keys(state.platforms).length} platforms`);

  // List lifters mode
  if (listLifters) {
    console.log(`\n=== Lifters in "${state.meet?.name || meetId}" ===\n`);
    const sorted = Object.values(state.lifters).sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    for (const l of sorted) {
      console.log(`  ${l.name || '(unnamed)'} | ${l.team || ''} | ${l.gender || ''} | ${l.bodyWeight || ''}kg | Flight ${l.flight || '?'} | Session ${l.session || '?'}`);
    }
    console.log(`\nTotal: ${sorted.length} lifters`);
    process.exit(0);
  }

  // Show initial platform state
  checkPlatforms();

  // Get the current update_seq for changes feed
  const dbInfo = await fetchJSON(dbUrl);
  state.lastSeq = dbInfo.update_seq || '0';
  console.log(`[SYNC] Starting changes feed from seq: ${String(state.lastSeq).substring(0, 20)}...`);
}

// --- Check platforms for tracked lifter ---
function checkPlatforms() {
  for (const [platformId, platform] of Object.entries(state.platforms)) {
    const parsed = parseAttemptId(platform.currentAttemptId);
    if (!parsed) continue;

    const currentLifter = state.lifters[parsed.lifterId];
    const currentName = currentLifter?.name || 'Unknown';

    // Compute attempt order to find next lifters
    const order = computeAttemptOrder(platformId);
    const currentIdx = order.findIndex(a => a.attemptId === platform.currentAttemptId);
    const nextAttempts = currentIdx >= 0 ? order.slice(currentIdx + 1) : [];

    // Initialize tracking state for this platform
    if (!trackState[platformId]) {
      trackState[platformId] = { lastCurrentAttemptId: null, notifiedOnDeck: false, notifiedInTheHole: false };
    }
    const ts = trackState[platformId];

    // Log lifter change
    if (platform.currentAttemptId !== ts.lastCurrentAttemptId) {
      ts.lastCurrentAttemptId = platform.currentAttemptId;
      ts.notifiedOnDeck = false;
      ts.notifiedInTheHole = false;

      console.log(`\n[CURRENT] ${currentName} - ${parsed.liftName} attempt ${parsed.attemptNumber} (${platform.name || platformId})`);

      if (nextAttempts.length > 0) {
        const upcoming = nextAttempts.slice(0, 5).map((a, i) => {
          const label = i === 0 ? 'ON DECK' : i === 1 ? 'IN HOLE' : `#${i + 2}`;
          return `  ${label}: ${a.lifterName} (${a.liftName} ${a.attemptNumber})`;
        });
        console.log(upcoming.join('\n'));
      }
    }

    // Track specific lifter
    if (trackName) {
      const trackLower = trackName.toLowerCase();

      if (currentLifter?.name?.toLowerCase().includes(trackLower)) {
        console.log(`\n*** ALERT: ${currentLifter.name} IS LIFTING NOW! ***\n`);
      }

      if (nextAttempts.length > 0 && nextAttempts[0].lifterName?.toLowerCase().includes(trackLower) && !ts.notifiedOnDeck) {
        ts.notifiedOnDeck = true;
        console.log(`\n*** ALERT: ${nextAttempts[0].lifterName} IS ON DECK (next to lift)! ***`);
        console.log(`*** >> This is when we would send the notification email ***\n`);
      }

      if (nextAttempts.length > 1 && nextAttempts[1].lifterName?.toLowerCase().includes(trackLower) && !ts.notifiedInTheHole) {
        ts.notifiedInTheHole = true;
        console.log(`\n*** HEADS UP: ${nextAttempts[1].lifterName} is in the hole (2 lifters away) ***\n`);
      }
    }
  }
}

// --- Long-poll the _changes feed ---
async function watchChanges() {
  let retryDelay = 2000;

  while (true) {
    try {
      const url = `${dbUrl}/_changes?include_docs=true&since=${encodeURIComponent(state.lastSeq)}&feed=longpoll&timeout=60000`;
      const changes = await fetchJSON(url, { timeout: 90000 });

      if (changes.results && changes.results.length > 0) {
        let needsCheck = false;
        for (const change of changes.results) {
          if (change.doc) {
            processDoc(change.doc);
            // Re-check if platform or lifter changed (lifter changes affect attempt order)
            if (change.id.startsWith('p') || change.id.startsWith('l')) needsCheck = true;
          }
        }
        if (needsCheck) checkPlatforms();
      }

      if (changes.last_seq) state.lastSeq = changes.last_seq;
      retryDelay = 2000;
    } catch (err) {
      console.error(`[CHANGES ERROR] ${err.message}`);
      console.log(`[RECONNECT] Retrying in ${retryDelay / 1000}s...`);
      await new Promise(r => setTimeout(r, retryDelay));
      retryDelay = Math.min(retryDelay * 2, 30000);
    }
  }
}

// --- Main ---
async function main() {
  try {
    await initialLoad();
    await watchChanges();
  } catch (err) {
    console.error(`[FATAL] ${err.message}`);
    console.log('[RESTART] Retrying in 10s...');
    setTimeout(main, 10000);
  }
}

main();
