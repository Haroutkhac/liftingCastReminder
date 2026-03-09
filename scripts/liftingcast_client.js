/**
 * LiftingCast CouchDB Client
 *
 * Connects to LiftingCast's public readonly CouchDB endpoint.
 * No API key or password needed — uses the same data source as
 * the public spectator board view.
 *
 * Multi-meet support: monitors all meets that have active subscriptions
 * in Postgres, plus any meet specified via MEET_ID env var.
 *
 * CouchDB attempt ID format: a{attemptNumber}{liftInitial}-{lifterId}
 *   liftInitial: s=squat, b=bench, d=deadlift
 *   Example: a1d-l0jd58nr3af4 = deadlift attempt 1 for lifter l0jd58nr3af4
 *
 * Configuration (env vars or CLI args):
 *   MEET_ID / --meet-id       Meet ID (e.g. mfmnsrd1fve8) — optional with DB subscriptions
 *   TRACK_LIFTER / --track    Lifter name to track via console (partial match, case-insensitive)
 *   COUCHDB_URL / --url       Custom CouchDB base URL
 *   PORT                      HTTP server port (default: 3000)
 *   DATABASE_URL              Postgres connection string (Railway auto-provides)
 *   RESEND_API_KEY            Resend email API key
 *   RESEND_FROM               Sender email (default: onboarding@resend.dev)
 *
 * CLI-only options:
 *   --list-lifters            Just list all lifters and exit
 */
const http = require('http');
const https = require('https');
const { initDB, getSubscriptions, getAllMeetIds, addSubscription, removeSubscription, getSubscriptionsByEmail } = require('./db');
const { sendOnDeckEmail, sendSubscriptionConfirmation } = require('./email');
const { getMeetPlatform, loadSymPlmeetMeet, watchSymPlmeet, stopSymPlmeet,
        stopAllSymPlmeet, discoverTodaysSymPlmeetMeets, normalizeSymPlmeetData } = require('./symplmeet_client');

// --- Parse CLI arguments ---
const args = {};
for (const arg of process.argv.slice(2)) {
  if (arg.startsWith('--')) {
    const [key, ...rest] = arg.slice(2).split('=');
    args[key] = rest.length > 0 ? rest.join('=') : true;
  }
}

const COUCHDB_BASE = 'https://couchdb.liftingcast.com';

const cliMeetId = process.env.MEET_ID || args['meet-id'] || '';
const trackName = process.env.TRACK_LIFTER || args['track'] || '';
const listLifters = args['list-lifters'] || false;
const couchdbBase = process.env.COUCHDB_URL || args['url'] || COUCHDB_BASE;

// --- Attempt ID parsing ---
const LIFT_MAP = { s: 'squat', b: 'bench', d: 'dead' };
const LIFT_ORDER = { squat: 0, bench: 1, dead: 2 };

const SYMPLMEET_LIFT_MAP = { sq: 'squat', bp: 'bench', dl: 'dead' };

function parseAttemptId(attemptId) {
  if (!attemptId) return null;

  // SymPlmeet format: sa-{sq|bp|dl}{1-3}-{lifterId}
  if (attemptId.startsWith('sa-')) {
    const match = attemptId.match(/^sa-(sq|bp|dl)(\d)-(.+)$/);
    if (!match) return null;
    return {
      attemptNumber: match[2],
      liftName: SYMPLMEET_LIFT_MAP[match[1]],
      liftInitial: match[1],
      lifterId: `sl-${match[3]}`,
    };
  }

  // LiftingCast format: a{attemptNumber}{s|b|d}-{lifterId}
  if (!attemptId.startsWith('a')) return null;
  const match = attemptId.match(/^a(\d)([sbd])-(.+)$/);
  if (!match) return null;
  return {
    attemptNumber: match[1],
    liftName: LIFT_MAP[match[2]],
    liftInitial: match[2],
    lifterId: match[3],
  };
}

// --- Per-meet state ---
// meets: { [meetId]: { meet, platforms, lifters, divisions, attempts, lastSeq, trackState } }
const meets = {};

// --- Subscription cache (avoids hitting Postgres on every attempt change) ---
const subsCache = {};
const SUBS_CACHE_TTL = 30_000; // 30 seconds

async function getCachedSubscriptions(meetId) {
  const cached = subsCache[meetId];
  if (cached && Date.now() - cached.ts < SUBS_CACHE_TTL) return cached.subs;
  const subs = await getSubscriptions(meetId);
  subsCache[meetId] = { subs, ts: Date.now() };
  return subs;
}

function getMeetState(meetId) {
  if (!meets[meetId]) {
    meets[meetId] = {
      meet: null,
      platforms: {},
      lifters: {},
      divisions: {},
      attempts: {},
      lastSeq: '0',
      trackState: {},
    };
  }
  return meets[meetId];
}

// --- Compute lifter's best completed lifts from attempt docs ---
function computeLifterBests(meetState, lifterId) {
  // Use pre-computed bests from SymPlmeet if available
  const lifter = meetState.lifters[lifterId];
  if (lifter?.bestLifts) {
    return { squat: lifter.bestLifts.squat || 0, bench: lifter.bestLifts.bench || 0, dead: lifter.bestLifts.deadlift || 0 };
  }

  const bests = { squat: 0, bench: 0, dead: 0 };
  for (const attempt of Object.values(meetState.attempts)) {
    if (attempt.lifterId !== lifterId) continue;
    if (attempt.result !== 'good') continue;
    const weight = typeof attempt.weight === 'number' ? attempt.weight : 0;
    if (attempt.liftName && weight > bests[attempt.liftName]) {
      bests[attempt.liftName] = weight;
    }
  }
  return bests;
}

// --- Compute standings for all lifters on a platform ---
function computeStandings(meetState, platformId) {
  const platformLifterIds = Object.values(meetState.lifters)
    .filter(l => l.platformId === platformId)
    .map(l => l._id);

  const standings = [];
  for (const lid of platformLifterIds) {
    const bests = computeLifterBests(meetState, lid);
    const total = bests.squat + bests.bench + bests.dead;
    standings.push({ lifterId: lid, total, bests });
  }
  // Sort descending by total (higher total = better place)
  standings.sort((a, b) => b.total - a.total);
  return standings;
}

// --- Get place info for a lifter, including projected place if attempt succeeds ---
function getPlaceInfo(meetState, platformId, lifterId, attemptWeight, liftName) {
  const standings = computeStandings(meetState, platformId);
  const totalCompetitors = standings.length;

  // Current place (among lifters with total > 0)
  const withTotals = standings.filter(s => s.total > 0);
  const currentIdx = withTotals.findIndex(s => s.lifterId === lifterId);
  const currentPlace = currentIdx >= 0 ? currentIdx + 1 : null;

  // Projected place if this attempt succeeds
  let projectedPlace = null;
  if (attemptWeight && liftName) {
    const currentBests = computeLifterBests(meetState, lifterId);
    const projectedBest = Math.max(currentBests[liftName] || 0, attemptWeight);
    const projectedTotal = (liftName === 'squat' ? projectedBest : currentBests.squat)
      + (liftName === 'bench' ? projectedBest : currentBests.bench)
      + (liftName === 'dead' ? projectedBest : currentBests.dead);

    // Count how many other lifters have a higher total
    let rank = 1;
    for (const s of standings) {
      if (s.lifterId === lifterId) continue;
      if (s.total > projectedTotal) rank++;
    }
    projectedPlace = rank;
  }

  return { currentPlace, projectedPlace, totalCompetitors };
}

// --- Compute attempt order for a platform ---
function computeAttemptOrder(meetState, platformId) {
  const platformLifterIds = new Set(
    Object.values(meetState.lifters)
      .filter(l => l.platformId === platformId)
      .map(l => l._id)
  );
  const pending = [];

  for (const [attemptId, attempt] of Object.entries(meetState.attempts)) {
    if (!attempt.lifterId || !platformLifterIds.has(attempt.lifterId)) continue;
    if (attempt.attemptNumber === '4') continue;
    // Skip completed attempts (result is "good" or "bad")
    if (attempt.result === 'good' || attempt.result === 'bad') continue;
    // Skip attempts with no weight set
    const weight = (typeof attempt.weight === 'number' && attempt.weight > 0) ? attempt.weight : null;
    if (weight === null) continue;

    const lifter = meetState.lifters[attempt.lifterId];
    if (!lifter) continue;

    pending.push({
      lifterId: attempt.lifterId,
      lifterName: lifter.name,
      liftName: attempt.liftName,
      attemptNumber: attempt.attemptNumber,
      weight,
      lot: lifter.lot || 999,
      session: lifter.session || 1,
      flight: lifter.flight || 'Z',
      endOfRound: attempt.endOfRound || 0,
      attemptId: attempt._id,
    });
  }

  pending.sort((a, b) => {
    if (a.session !== b.session) return a.session - b.session;
    if (LIFT_ORDER[a.liftName] !== LIFT_ORDER[b.liftName]) return LIFT_ORDER[a.liftName] - LIFT_ORDER[b.liftName];
    if (a.flight !== b.flight) return a.flight.localeCompare(b.flight);
    if (a.attemptNumber !== b.attemptNumber) return a.attemptNumber - b.attemptNumber;
    if (a.endOfRound !== b.endOfRound) return a.endOfRound - b.endOfRound;
    if (a.weight !== b.weight) return a.weight - b.weight;
    return a.lot - b.lot;
  });

  return pending;
}

// --- Process a CouchDB document into meet state ---
function processDoc(meetId, doc) {
  if (!doc || !doc._id) return;
  const id = doc._id;
  const st = getMeetState(meetId);

  if (id === meetId) {
    st.meet = doc;
  } else if (id.startsWith('p') && !id.startsWith('pi')) {
    st.platforms[id] = doc;
  } else if (id.startsWith('l')) {
    st.lifters[id] = doc;
  } else if (id.startsWith('d')) {
    st.divisions[id] = doc;
  } else if (id.startsWith('a')) {
    st.attempts[id] = doc;
  }
}

// --- Send email notifications for a lifter match ---
async function notifySubscribers(meetId, lifterName, liftName, position, details) {
  try {
    const subs = await getCachedSubscriptions(meetId);
    if (subs.length === 0) return;
    const meetName = getMeetState(meetId).meet?.name || meetId;
    const nameLower = lifterName.toLowerCase();

    for (const sub of subs) {
      if (nameLower === sub.lifter_name.toLowerCase()) {
        console.log(`[NOTIFY] Match: "${lifterName}" is ${position} — notifying ${sub.email}`);
        await sendOnDeckEmail(sub.email, lifterName, meetName, liftName, position, meetId, sub.lifter_name, details);
      }
    }
  } catch (err) {
    console.error(`[NOTIFY ERROR] ${err.message}`);
  }
}

// --- Check platforms for tracked lifter ---
function checkPlatforms(meetId) {
  const st = getMeetState(meetId);

  for (const [platformId, platform] of Object.entries(st.platforms)) {
    const parsed = parseAttemptId(platform.currentAttemptId);
    if (!parsed) continue;

    const currentLifter = st.lifters[parsed.lifterId];
    const currentName = currentLifter?.name || 'Unknown';

    const order = computeAttemptOrder(st, platformId);
    const currentIdx = order.findIndex(a => a.attemptId === platform.currentAttemptId);
    const nextAttempts = currentIdx >= 0 ? order.slice(currentIdx + 1) : [];

    if (!st.trackState[platformId]) {
      st.trackState[platformId] = { lastCurrentAttemptId: null, notifiedOnDeck: new Set(), notifiedInTheHole: new Set(), notifiedLifting: new Set() };
    }
    const ts = st.trackState[platformId];

    // Reset notifications when current attempt changes
    if (platform.currentAttemptId !== ts.lastCurrentAttemptId) {
      ts.lastCurrentAttemptId = platform.currentAttemptId;
      ts.notifiedOnDeck = new Set();
      ts.notifiedInTheHole = new Set();
      ts.notifiedLifting = new Set();

      console.log(`\n[CURRENT] ${currentName} - ${parsed.liftName} attempt ${parsed.attemptNumber} (${platform.name || platformId}) [${meetId}]`);

      if (nextAttempts.length > 0) {
        const upcoming = nextAttempts.slice(0, 5).map((a, i) => {
          const label = i === 0 ? 'ON DECK' : i === 1 ? 'IN HOLE' : `#${i + 2}`;
          return `  ${label}: ${a.lifterName} (${a.liftName} ${a.attemptNumber})`;
        });
        console.log(upcoming.join('\n'));
      }
    }

    // Console tracking (CLI --track)
    if (trackName) {
      const trackLower = trackName.toLowerCase();
      if (currentLifter?.name?.toLowerCase().includes(trackLower)) {
        console.log(`\n*** ALERT: ${currentLifter.name} IS LIFTING NOW! ***\n`);
      }
      if (nextAttempts.length > 0 && nextAttempts[0].lifterName?.toLowerCase().includes(trackLower) && !ts.notifiedOnDeck.has('cli')) {
        ts.notifiedOnDeck.add('cli');
        console.log(`\n*** ALERT: ${nextAttempts[0].lifterName} IS ON DECK (next to lift)! ***\n`);
      }
      if (nextAttempts.length > 1 && nextAttempts[1].lifterName?.toLowerCase().includes(trackLower) && !ts.notifiedInTheHole.has('cli')) {
        ts.notifiedInTheHole.add('cli');
        console.log(`\n*** HEADS UP: ${nextAttempts[1].lifterName} is in the hole (2 lifters away) ***\n`);
      }
    }

    // Email notifications for current lifter
    if (currentLifter?.name && !ts.notifiedLifting.has(currentLifter.name)) {
      ts.notifiedLifting.add(currentLifter.name);
      const currentAttempt = st.attempts[platform.currentAttemptId];
      const currentWeight = currentAttempt?.weight || null;
      const placeInfo = getPlaceInfo(st, platformId, parsed.lifterId, currentWeight, parsed.liftName);
      notifySubscribers(meetId, currentLifter.name, parsed.liftName, 'lifting', {
        weight: currentWeight,
        attemptNumber: parsed.attemptNumber,
        liftName: parsed.liftName,
        ...placeInfo,
      });
    }

    // Email notifications for on deck
    if (nextAttempts.length > 0 && nextAttempts[0].lifterName && !ts.notifiedOnDeck.has(nextAttempts[0].lifterName)) {
      ts.notifiedOnDeck.add(nextAttempts[0].lifterName);
      const a = nextAttempts[0];
      const placeInfo = getPlaceInfo(st, platformId, a.lifterId, a.weight, a.liftName);
      notifySubscribers(meetId, a.lifterName, a.liftName, 'on-deck', {
        weight: a.weight,
        attemptNumber: a.attemptNumber,
        liftName: a.liftName,
        ...placeInfo,
      });
    }

    // Email notifications for in the hole
    if (nextAttempts.length > 1 && nextAttempts[1].lifterName && !ts.notifiedInTheHole.has(nextAttempts[1].lifterName)) {
      ts.notifiedInTheHole.add(nextAttempts[1].lifterName);
      const a = nextAttempts[1];
      const placeInfo = getPlaceInfo(st, platformId, a.lifterId, a.weight, a.liftName);
      notifySubscribers(meetId, a.lifterName, a.liftName, 'in-the-hole', {
        weight: a.weight,
        attemptNumber: a.attemptNumber,
        liftName: a.liftName,
        ...placeInfo,
      });
    }
  }
}

// --- Simple HTTPS fetch helper ---
function fetchJSON(url, options = {}) {
  return new Promise((resolve, reject) => {
    const timeout = options.timeout || 90000;
    let settled = false;
    const settle = (fn, val) => { if (!settled) { settled = true; fn(val); } };

    const req = https.get(url, { timeout }, (res) => {
      if (res.statusCode < 200 || res.statusCode >= 300) {
        res.resume();
        settle(reject, new Error(`HTTP ${res.statusCode} from ${url}`));
        return;
      }
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('error', (err) => settle(reject, err));
      res.on('end', () => {
        try { settle(resolve, JSON.parse(body)); }
        catch (e) { settle(reject, new Error(`Invalid JSON from ${url}`)); }
      });
    });
    req.on('error', (err) => settle(reject, err));
    req.on('timeout', () => { req.destroy(); settle(reject, new Error(`Timeout fetching ${url}`)); });
  });
}

// --- Initial load for a single meet ---
async function loadMeet(meetId) {
  const dbUrl = `${couchdbBase}/${meetId}_readonly`;
  console.log(`[LOADING] Fetching docs for meet ${meetId}...`);

  // Capture update_seq BEFORE loading docs so the changes feed will replay
  // anything that arrives during the _all_docs fetch (processDoc is idempotent).
  const dbInfo = await fetchJSON(dbUrl);
  const startSeq = dbInfo.update_seq || '0';

  const result = await fetchJSON(`${dbUrl}/_all_docs?include_docs=true`);
  for (const row of result.rows) {
    if (row.doc) processDoc(meetId, row.doc);
  }

  const st = getMeetState(meetId);
  console.log(`[LOADED] Meet: "${st.meet?.name || '(unknown)'}" | ${Object.keys(st.lifters).length} lifters | ${Object.keys(st.platforms).length} platforms`);

  // List lifters mode (only for CLI meet)
  if (listLifters && meetId === cliMeetId) {
    console.log(`\n=== Lifters in "${st.meet?.name || meetId}" ===\n`);
    const sorted = Object.values(st.lifters).sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    for (const l of sorted) {
      console.log(`  ${l.name || '(unnamed)'} | ${l.team || ''} | ${l.gender || ''} | ${l.bodyWeight || ''}kg | Flight ${l.flight || '?'} | Session ${l.session || '?'}`);
    }
    console.log(`\nTotal: ${sorted.length} lifters`);
    process.exit(0);
  }

  st.lastSeq = startSeq;
}

// --- Graceful shutdown ---
let shuttingDown = false;

// --- Long-poll the _changes feed for a single meet ---
async function watchChanges(meetId) {
  const dbUrl = `${couchdbBase}/${meetId}_readonly`;
  let retryDelay = 2000;

  while (!shuttingDown && watchingMeets.has(meetId)) {
    try {
      const st = getMeetState(meetId);
      const url = `${dbUrl}/_changes?include_docs=true&since=${encodeURIComponent(st.lastSeq)}&feed=longpoll&timeout=60000`;
      const changes = await fetchJSON(url, { timeout: 90000 });

      if (changes.results && changes.results.length > 0) {
        let needsCheck = false;
        for (const change of changes.results) {
          if (change.doc) {
            processDoc(meetId, change.doc);
            if (change.id.startsWith('p') || change.id.startsWith('l') || change.id.startsWith('a')) needsCheck = true;
          }
        }
        if (needsCheck) checkPlatforms(meetId);
      }

      if (changes.last_seq) st.lastSeq = changes.last_seq;
      retryDelay = 2000;
    } catch (err) {
      if (shuttingDown) break;
      console.error(`[CHANGES ERROR] ${meetId}: ${err.message}`);
      console.log(`[RECONNECT] ${meetId}: Retrying in ${retryDelay / 1000}s...`);
      await new Promise(r => setTimeout(r, retryDelay));
      retryDelay = Math.min(retryDelay * 2, 30000);
    }
  }
  console.log(`[WATCH] Stopped changes feed for ${meetId}`);
}

// --- Meet date helpers ---
function parseMeetDate(meetDoc) {
  if (!meetDoc || !meetDoc.date) return null;
  const fmt = meetDoc.dateFormat || 'MM/DD/YYYY';
  const parts = meetDoc.date.split('/');
  if (parts.length !== 3) return null;
  let y, m, d;
  if (fmt === 'MM/DD/YYYY') { [m, d, y] = parts; }
  else if (fmt === 'DD/MM/YYYY') { [d, m, y] = parts; }
  else { [m, d, y] = parts; } // default
  const date = new Date(Number(y), Number(m) - 1, Number(d));
  return isNaN(date.getTime()) ? null : date;
}

function isMeetReady(meetId) {
  if (getMeetPlatform(meetId) === 'symplmeet') return true; // always ready
  const st = meets[meetId];
  if (!st || !st.meet) return true; // if we can't tell, assume ready
  const meetDate = parseMeetDate(st.meet);
  if (!meetDate) return true; // no date info, assume ready
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return meetDate <= today;
}

// --- Fetch today's meets from LiftingCast API ---
const LIFTINGCAST_API = 'https://liftingcast.com/api/meets';

function isMeetToday(meetEntry) {
  const fmt = meetEntry.dateFormat || 'MM/DD/YYYY';
  const parts = (meetEntry.date || '').split('/');
  if (parts.length !== 3) return false;
  let y, m, d;
  if (fmt === 'DD/MM/YYYY') { [d, m, y] = parts; }
  else { [m, d, y] = parts; }
  const meetDate = new Date(Number(y), Number(m) - 1, Number(d));
  if (isNaN(meetDate.getTime())) return false;
  // Match today AND tomorrow to handle timezone differences
  // (server runs UTC, meets are in local time — e.g. a US meet on March 8
  // needs to be discoverable when UTC is still March 7 evening)
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const tomorrow = new Date(today);
  tomorrow.setDate(tomorrow.getDate() + 1);
  return meetDate.getTime() === today.getTime() ||
         meetDate.getTime() === tomorrow.getTime();
}

async function fetchTodaysMeetIds() {
  try {
    const data = await fetchJSON(LIFTINGCAST_API);
    const allMeets = data.docs || [];
    const todaysMeets = allMeets.filter(isMeetToday);
    console.log(`[DISCOVER] LiftingCast API returned ${allMeets.length} meets, ${todaysMeets.length} are today`);
    return todaysMeets.map(m => m._id);
  } catch (err) {
    console.error(`[DISCOVER ERROR] Failed to fetch meets from LiftingCast API: ${err.message}`);
    return [];
  }
}

// --- Load a meet's lifters for autocomplete only (no changes feed) ---
async function indexMeet(meetId) {
  if (pendingMeets.has(meetId)) return;
  // Re-index if previously loaded with 0 lifters (roster may have been added since)
  if (loadedMeets.has(meetId) && Object.keys(getMeetState(meetId).lifters).length > 0) return;
  pendingMeets.add(meetId);
  try {
    if (getMeetPlatform(meetId) === 'symplmeet') {
      const st = getMeetState(meetId);
      await loadSymPlmeetMeet(meetId, st);
    } else {
      await loadMeet(meetId);
    }
    loadedMeets.add(meetId);
  } catch (err) {
    console.error(`[INDEX ERROR] Failed to index ${meetId}: ${err.message}`);
  } finally {
    pendingMeets.delete(meetId);
  }
}

// --- Start monitoring a meet (load + watch) ---
const loadedMeets = new Set();  // meets with data loaded (for autocomplete)
const watchingMeets = new Set(); // meets with active changes feed
const pendingMeets = new Set();  // meets currently being loaded (prevents duplicate loads)

async function startMeet(meetId) {
  if (getMeetPlatform(meetId) === 'symplmeet') {
    return startSymPlmeetMeet(meetId);
  }
  // Ensure data is loaded first
  await indexMeet(meetId);
  if (!loadedMeets.has(meetId)) return; // indexMeet failed
  // Start watching if not already and meet is ready
  if (!watchingMeets.has(meetId) && isMeetReady(meetId)) {
    checkPlatforms(meetId);
    console.log(`[SYNC] Starting changes feed for ${meetId} from seq: ${String(getMeetState(meetId).lastSeq).substring(0, 20)}...`);
    watchingMeets.add(meetId);
    watchChanges(meetId); // runs forever, don't await
  } else if (!isMeetReady(meetId)) {
    console.log(`[WAITING] Meet ${meetId} ("${getMeetState(meetId).meet?.name}") is on ${getMeetState(meetId).meet?.date} — will start polling on meet day`);
  }
}

async function startSymPlmeetMeet(meetId) {
  if (loadedMeets.has(meetId) && watchingMeets.has(meetId)) return;
  if (pendingMeets.has(meetId)) return;
  pendingMeets.add(meetId);
  try {
    const st = getMeetState(meetId);
    await loadSymPlmeetMeet(meetId, st);
    loadedMeets.add(meetId);
    if (!watchingMeets.has(meetId)) {
      checkPlatforms(meetId);
      watchingMeets.add(meetId);
      watchSymPlmeet(meetId, (data) => {
        normalizeSymPlmeetData(meetId, data, getMeetState(meetId));
        checkPlatforms(meetId);
      });
    }
  } catch (err) {
    console.error(`[SYMPLMEET ERROR] Failed to load meet ${meetId}: ${err.message}`);
  } finally {
    pendingMeets.delete(meetId);
  }
}

// For backwards compat with /health endpoint
const activeMeets = loadedMeets;

// --- Poll for new meets from subscriptions + start watching meets that have reached their date ---
// Cleans up changes feeds for meets that ended (not today/tomorrow).
// Track which SymPlmeet meet IDs were in the latest /api/todayMeets response
let activeSymPlmeetIds = new Set();

function isMeetStale(meetId) {
  // SymPlmeet meets are stale if they're no longer in /api/todayMeets
  if (getMeetPlatform(meetId) === 'symplmeet') return !activeSymPlmeetIds.has(meetId);
  const st = meets[meetId];
  if (!st || !st.meet) return false;
  // A meet is stale if its date is before today (not today, not tomorrow)
  return !isMeetToday(st.meet);
}

async function discoverTodaysMeets() {
  // LiftingCast meets
  const todaysMeetIds = await fetchTodaysMeetIds();
  const newMeets = todaysMeetIds.filter(mid => !loadedMeets.has(mid) && !pendingMeets.has(mid));
  if (newMeets.length > 0) {
    console.log(`[DISCOVER] Indexing ${newMeets.length} new LiftingCast meets for autocomplete...`);
    const BATCH_SIZE = 5;
    for (let i = 0; i < newMeets.length; i += BATCH_SIZE) {
      const batch = newMeets.slice(i, i + BATCH_SIZE);
      await Promise.all(batch.map(mid => indexMeet(mid)));
    }
  }

  // SymPlmeet meets
  try {
    const symplMeets = await discoverTodaysSymPlmeetMeets();
    activeSymPlmeetIds = new Set(symplMeets.map(m => m.id));

    for (const m of symplMeets) {
      if (pendingMeets.has(m.id)) continue;
      try {
        const st = getMeetState(m.id);
        await loadSymPlmeetMeet(m.id, st);
        loadedMeets.add(m.id);
      } catch (err) {
        console.error(`[DISCOVER] Failed to index SymPlmeet meet ${m.id}: ${err.message}`);
      }
    }
  } catch (err) {
    console.error(`[DISCOVER] SymPlmeet discovery error: ${err.message}`);
  }
}

async function pollForNewMeets() {
  while (!shuttingDown) {
    try {
      // Discover today's meets from LiftingCast API (for autocomplete)
      await discoverTodaysMeets();

      // Start changes feeds for meets with active subscriptions
      const meetIds = await getAllMeetIds();
      for (const mid of meetIds) {
        if (!loadedMeets.has(mid) && !pendingMeets.has(mid)) {
          console.log(`[NEW MEET] Found subscription for meet ${mid}, loading...`);
        }
        await startMeet(mid);
      }
      // Check if any loaded-but-not-watching subscription meets are now ready
      const subMeetSet = new Set(meetIds);
      for (const mid of loadedMeets) {
        if (subMeetSet.has(mid) && !watchingMeets.has(mid) && isMeetReady(mid)) {
          // SymPlmeet meets are handled by startMeet → startSymPlmeetMeet
          if (getMeetPlatform(mid) === 'symplmeet') {
            await startMeet(mid);
            continue;
          }
          console.log(`[MEET DAY] Meet ${mid} ("${getMeetState(mid).meet?.name}") is starting — re-fetching docs and beginning changes feed`);
          try {
            const dbUrl = `${couchdbBase}/${mid}_readonly`;
            const dbInfo = await fetchJSON(dbUrl);
            getMeetState(mid).lastSeq = dbInfo.update_seq || '0';
            const result = await fetchJSON(`${dbUrl}/_all_docs?include_docs=true`);
            for (const row of result.rows) {
              if (row.doc) processDoc(mid, row.doc);
            }
            console.log(`[MEET DAY] Re-loaded ${result.rows.length} docs for ${mid}`);
          } catch (e) {
            console.error(`[MEET DAY ERROR] Failed to re-fetch docs for ${mid}: ${e.message} — starting changes feed anyway`);
          }
          watchingMeets.add(mid);
          watchChanges(mid);
        }
      }
      // Clean up stale meets — stop changes feeds and remove from index
      // (keeps subscribed meets that still have active subs, even if stale)
      const subMeetIds = new Set(meetIds);
      for (const mid of [...watchingMeets]) {
        if (isMeetStale(mid) && !subMeetIds.has(mid)) {
          console.log(`[CLEANUP] Stopping changes feed for stale meet ${mid} ("${getMeetState(mid).meet?.name}")`);
          watchingMeets.delete(mid);
          if (getMeetPlatform(mid) === 'symplmeet') stopSymPlmeet(mid);
        }
      }
      // Remove stale indexed meets (frees memory, clears old lifters from autocomplete)
      for (const mid of [...loadedMeets]) {
        if (isMeetStale(mid) && !subMeetIds.has(mid)) {
          console.log(`[CLEANUP] Removing stale meet ${mid} ("${getMeetState(mid).meet?.name}") from index`);
          loadedMeets.delete(mid);
          watchingMeets.delete(mid);
          if (getMeetPlatform(mid) === 'symplmeet') stopSymPlmeet(mid);
          delete meets[mid];
        }
      }
    } catch (err) {
      console.error(`[POLL ERROR] ${err.message}`);
    }
    await new Promise(r => setTimeout(r, 60000)); // check every 60s
  }
}

// --- Parse URL-encoded form body ---
function parseFormBody(body) {
  const params = new URLSearchParams(body);
  return Object.fromEntries(params.entries());
}

// --- HTML subscription form ---
const FORM_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>LiftAlert - Get Notified</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; align-items: center; justify-content: center; }
    .card { background: #1e293b; border-radius: 12px; padding: 2rem; max-width: 420px; width: 90%; box-shadow: 0 4px 24px rgba(0,0,0,0.3); }
    h1 { font-size: 1.5rem; margin-bottom: 0.25rem; }
    .subtitle { color: #94a3b8; margin-bottom: 1.5rem; font-size: 0.9rem; }
    label { display: block; font-size: 0.85rem; color: #94a3b8; margin-bottom: 0.25rem; margin-top: 1rem; }
    input { width: 100%; padding: 0.6rem 0.75rem; border-radius: 6px; border: 1px solid #334155; background: #0f172a; color: #e2e8f0; font-size: 1rem; }
    input:focus { outline: none; border-color: #3b82f6; }
    button { margin-top: 1.5rem; width: 100%; padding: 0.7rem; border: none; border-radius: 6px; background: #3b82f6; color: white; font-size: 1rem; font-weight: 600; cursor: pointer; }
    button:hover { background: #2563eb; }
    .help { font-size: 0.75rem; color: #64748b; margin-top: 0.25rem; }
    .msg { margin-top: 1rem; padding: 0.75rem; border-radius: 6px; font-size: 0.9rem; }
    .msg.ok { background: #064e3b; color: #6ee7b7; }
    .msg.err { background: #7f1d1d; color: #fca5a5; }
    .autocomplete-wrapper { position: relative; }
    .suggestions { position: absolute; top: 100%; left: 0; right: 0; background: #1e293b; border: 1px solid #334155; border-top: none; border-radius: 0 0 6px 6px; max-height: 240px; overflow-y: auto; z-index: 10; display: none; }
    .suggestion-item { padding: 0.5rem 0.75rem; cursor: pointer; }
    .suggestion-item:hover, .suggestion-item.active { background: #334155; }
    .suggestion-item .name { color: #e2e8f0; }
    .suggestion-item .meet-name { color: #64748b; font-size: 0.8rem; }
    .selected-meet { font-size: 0.8rem; color: #6ee7b7; margin-top: 0.25rem; display: none; }
    .selected-pill { display: none; margin-top: 0.75rem; padding: 0.5rem 0.75rem; background: #064e3b; border-radius: 6px; align-items: center; justify-content: space-between; }
    .selected-pill .pill-text { color: #6ee7b7; font-size: 0.85rem; }
    .selected-pill .pill-text .pill-meet { color: #94a3b8; font-size: 0.75rem; }
    .selected-pill .pill-clear { color: #94a3b8; cursor: pointer; font-size: 1.1rem; padding: 0 0.25rem; }
    .selected-pill .pill-clear:hover { color: #fca5a5; }
    .no-results { padding: 0.5rem 0.75rem; color: #64748b; font-size: 0.85rem; }
  </style>
</head>
<body>
  <div class="card">
    <h1>LiftAlert</h1>
    <p class="subtitle">Get email alerts when your lifter is on deck.</p>
    <form method="POST" action="/subscribe" id="subForm">
      <label for="email">Email</label>
      <input type="email" id="email" name="email" required placeholder="you@example.com">

      <label for="lifterInput">Lifter Name</label>
      <div class="autocomplete-wrapper" id="autocompleteWrapper">
        <input type="text" id="lifterInput" placeholder="Start typing a lifter name..." autocomplete="off">
        <div class="suggestions" id="suggestions"></div>
      </div>
      <p class="help">Select a lifter from the dropdown</p>

      <input type="hidden" id="lifterHidden" name="lifter">
      <input type="hidden" id="meetHidden" name="meet">
      <div class="selected-pill" id="selectedPill">
        <span class="pill-text"><span id="pillName"></span><br><span class="pill-meet" id="pillMeet"></span></span>
        <span class="pill-clear" id="pillClear" title="Clear selection">&times;</span>
      </div>

      <button type="submit">Subscribe</button>
    </form>
    <div style="text-align:center;margin-top:1.25rem;"><a href="/my-subscriptions" style="color:#94a3b8;font-size:0.85rem;">Manage my subscriptions</a> &middot; <a href="/meets" style="color:#94a3b8;font-size:0.85rem;">Today's meets</a></div>
  </div>
  <script>
    const lifterInput = document.getElementById('lifterInput');
    const suggestionsEl = document.getElementById('suggestions');
    const lifterHidden = document.getElementById('lifterHidden');
    const meetHidden = document.getElementById('meetHidden');
    const selectedPill = document.getElementById('selectedPill');
    const pillName = document.getElementById('pillName');
    const pillMeet = document.getElementById('pillMeet');
    const pillClear = document.getElementById('pillClear');
    const autocompleteWrapper = document.getElementById('autocompleteWrapper');
    const form = document.getElementById('subForm');

    let activeIdx = -1;
    let currentResults = [];
    let allLifters = [];

    // Load all lifters once on page load
    fetch('/api/lifters').then(r => r.json()).then(data => { allLifters = data; });

    // Block submit unless a lifter was selected from dropdown
    form.addEventListener('submit', (e) => {
      if (!lifterHidden.value || !meetHidden.value) {
        e.preventDefault();
        lifterInput.focus();
        lifterInput.style.borderColor = '#ef4444';
        setTimeout(() => { lifterInput.style.borderColor = ''; }, 2000);
      }
    });

    lifterInput.addEventListener('input', () => {
      const q = lifterInput.value.trim().toLowerCase();
      if (q.length < 2) { closeSuggestions(); return; }
      filterSuggestions(q);
    });

    lifterInput.addEventListener('keydown', (e) => {
      if (suggestionsEl.style.display === 'none') return;
      const items = suggestionsEl.querySelectorAll('.suggestion-item');
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        activeIdx = Math.min(activeIdx + 1, items.length - 1);
        updateActive(items);
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        activeIdx = Math.max(activeIdx - 1, 0);
        updateActive(items);
      } else if (e.key === 'Enter' && activeIdx >= 0) {
        e.preventDefault();
        selectResult(currentResults[activeIdx]);
      } else if (e.key === 'Escape') {
        closeSuggestions();
      }
    });

    document.addEventListener('click', (e) => {
      if (!e.target.closest('.autocomplete-wrapper')) closeSuggestions();
    });

    pillClear.addEventListener('click', clearSelection);

    function clearSelection() {
      lifterHidden.value = '';
      meetHidden.value = '';
      selectedPill.style.display = 'none';
      autocompleteWrapper.style.display = 'block';
      lifterInput.value = '';
      lifterInput.focus();
    }

    function updateActive(items) {
      items.forEach((el, i) => el.classList.toggle('active', i === activeIdx));
      if (items[activeIdx]) items[activeIdx].scrollIntoView({ block: 'nearest' });
    }

    function closeSuggestions() {
      suggestionsEl.style.display = 'none';
      activeIdx = -1;
      currentResults = [];
    }

    function selectResult(r) {
      lifterHidden.value = r.name;
      meetHidden.value = r.meetId;
      pillName.textContent = r.name;
      pillMeet.textContent = r.meetName;
      selectedPill.style.display = 'flex';
      autocompleteWrapper.style.display = 'none';
      lifterInput.value = '';
      closeSuggestions();
    }

    function filterSuggestions(q) {
      const data = allLifters.filter(l => l.name.toLowerCase().includes(q)).slice(0, 20);
      currentResults = data;
      activeIdx = -1;
      if (data.length === 0) {
        suggestionsEl.innerHTML = '<div class="no-results">No lifters found</div>';
        suggestionsEl.style.display = 'block';
        return;
      }
      suggestionsEl.innerHTML = data.map((r, i) =>
        '<div class="suggestion-item" data-idx="' + i + '">' +
          '<div class="name">' + escHtml(r.name) + '</div>' +
          '<div class="meet-name">' + escHtml(r.meetName) + '</div>' +
        '</div>'
      ).join('');
      suggestionsEl.style.display = 'block';
      suggestionsEl.querySelectorAll('.suggestion-item').forEach(el => {
        el.addEventListener('mousedown', (e) => {
          e.preventDefault();
          selectResult(currentResults[parseInt(el.dataset.idx)]);
        });
      });
    }

    function escHtml(s) {
      const d = document.createElement('div');
      d.textContent = s;
      return d.innerHTML;
    }
  </script>
</body>
</html>`;

function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#x27;');
}

function errorHTML(msg) {
  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Error</title>
<style>* { box-sizing: border-box; margin: 0; padding: 0; } body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; align-items: center; justify-content: center; } .card { background: #1e293b; border-radius: 12px; padding: 2rem; max-width: 420px; width: 90%; text-align: center; } h1 { color: #fca5a5; margin-bottom: 0.5rem; } p { color: #94a3b8; } a { color: #3b82f6; }</style>
</head><body><div class="card"><h1>Error</h1><p>${msg}</p><br><a href="/">Go back</a></div></body></html>`;
}

function successHTML(lifter, meetId, allSubs) {
  const meetName = meets[meetId]?.meet?.name || meetId;

  const subsRows = allSubs.map(s => {
    const mName = meets[s.meet_id]?.meet?.name || s.meet_id;
    const meetDate = meets[s.meet_id]?.meet?.date || '';
    const meetLocation = meets[s.meet_id]?.meet?.location || meets[s.meet_id]?.meet?.city || '';
    const details = [escHtml(meetDate), escHtml(meetLocation)].filter(Boolean).join(' &middot; ');
    const unsubUrl = `/unsubscribe?email=${encodeURIComponent(s.email || '')}&lifter=${encodeURIComponent(s.lifter_name)}&meet=${encodeURIComponent(s.meet_id)}`;
    return `<tr>
      <td style="padding:0.4rem 0.5rem">${escHtml(s.lifter_name)}</td>
      <td style="padding:0.4rem 0.5rem">${escHtml(mName)}${details ? '<br><span style="font-size:0.75rem;color:#64748b">' + details + '</span>' : ''}</td>
      <td style="padding:0.4rem 0.5rem"><a href="${unsubUrl}" style="color:#f87171;font-size:0.8rem">remove</a></td>
    </tr>`;
  }).join('');

  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Subscribed!</title>
<style>* { box-sizing: border-box; margin: 0; padding: 0; } body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 1rem; } .card { background: #1e293b; border-radius: 12px; padding: 2rem; max-width: 520px; width: 100%; } h1 { color: #6ee7b7; margin-bottom: 0.5rem; text-align: center; } .subtitle { color: #94a3b8; text-align: center; margin-bottom: 1.5rem; } a { color: #3b82f6; } h2 { font-size: 1rem; color: #94a3b8; margin-bottom: 0.5rem; } table { width: 100%; border-collapse: collapse; margin-bottom: 1rem; } th { text-align: left; padding: 0.4rem 0.5rem; color: #64748b; font-size: 0.75rem; border-bottom: 1px solid #334155; } td { border-bottom: 1px solid #1e293b; font-size: 0.85rem; } .cta { text-align: center; margin-top: 1rem; }</style>
</head><body><div class="card"><h1>Subscribed!</h1><p class="subtitle">You'll get an email when <strong>${escHtml(lifter)}</strong> is on deck at <strong>${escHtml(meetName)}</strong>.</p><div style="background:#1a1a2e;border:1px solid #334155;border-radius:8px;padding:0.75rem 1rem;margin-bottom:1.5rem;font-size:0.85rem;color:#fbbf24;text-align:center;">Check your spam/junk folder and mark our emails as "Not Spam" to make sure you get alerts on time.</div><h2>Your Subscriptions</h2><table><thead><tr><th>Lifter</th><th>Meet</th><th></th></tr></thead><tbody>${subsRows}</tbody></table><div class="cta"><a href="/">Subscribe to another lifter</a></div></div></body></html>`;
}

function unsubHTML(success) {
  const msg = success ? 'You have been unsubscribed.' : 'Subscription not found (may already be removed).';
  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Unsubscribed</title>
<style>* { box-sizing: border-box; margin: 0; padding: 0; } body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; align-items: center; justify-content: center; } .card { background: #1e293b; border-radius: 12px; padding: 2rem; max-width: 420px; width: 90%; text-align: center; } p { color: #94a3b8; } a { color: #3b82f6; }</style>
</head><body><div class="card"><p>${msg}</p><br><a href="/">Back to LiftAlert</a></div></body></html>`;
}

function meetsHTML(meetList) {
  const meetCards = meetList.length === 0
    ? '<p style="color:#94a3b8;text-align:center;margin:2rem 0;">No meets currently indexed.</p>'
    : meetList.map(m => {
      const details = [m.date, m.location].filter(Boolean).map(s => escHtml(s)).join(' &middot; ');
      const statusColor = m.watching ? '#6ee7b7' : '#fbbf24';
      const statusText = m.watching ? 'Live' : 'Indexed';
      const platformRows = m.platforms.map(p => {
        const current = p.currentLifter
          ? `<strong>${escHtml(p.currentLifter)}</strong> — ${escHtml(p.liftName || '')} attempt ${escHtml(String(p.attemptNumber || ''))}`
          : '<span style="color:#64748b;">No current lifter</span>';
        const next = p.nextUp.length > 0
          ? p.nextUp.map((n, i) => `<span style="color:#94a3b8;font-size:0.8rem;">${i === 0 ? 'On deck' : 'In hole'}: ${escHtml(n.name)}</span>`).join('<br>')
          : '';
        return `<div style="margin-top:0.5rem;padding:0.5rem 0.75rem;background:#0f172a;border-radius:6px;">
          <div style="font-size:0.8rem;color:#64748b;margin-bottom:0.25rem;">${escHtml(p.name)}</div>
          <div style="font-size:0.9rem;">${current}</div>
          ${next ? `<div style="margin-top:0.25rem;">${next}</div>` : ''}
        </div>`;
      }).join('');
      return `<div style="background:#1e293b;border-radius:10px;padding:1.25rem;margin-bottom:1rem;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.25rem;">
          <h2 style="font-size:1.1rem;margin:0;">${escHtml(m.name)}</h2>
          <span style="font-size:0.75rem;color:${statusColor};border:1px solid ${statusColor};padding:0.15rem 0.5rem;border-radius:99px;">${statusText}</span>
        </div>
        ${details ? `<p style="font-size:0.85rem;color:#64748b;margin-bottom:0.5rem;">${details}</p>` : ''}
        <p style="font-size:0.85rem;color:#94a3b8;">${m.lifterCount} lifters &middot; ${m.platformCount} platform${m.platformCount !== 1 ? 's' : ''}</p>
        ${platformRows}
      </div>`;
    }).join('');

  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Today's Meets - LiftAlert</title>
<style>* { box-sizing: border-box; margin: 0; padding: 0; } body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; padding: 1rem; } .container { max-width: 600px; margin: 0 auto; } h1 { font-size: 1.5rem; margin-bottom: 0.25rem; } .subtitle { color: #94a3b8; margin-bottom: 1.5rem; font-size: 0.9rem; } a { color: #3b82f6; } .nav { margin-bottom: 1.5rem; font-size: 0.85rem; }</style>
</head><body><div class="container">
  <div class="nav"><a href="/">&larr; Back to LiftAlert</a></div>
  <h1>Today's Meets</h1>
  <p class="subtitle">${meetList.length} meet${meetList.length !== 1 ? 's' : ''} currently indexed</p>
  ${meetCards}
</div></body></html>`;
}

function mySubscriptionsHTML(email, subs) {
  const heading = email ? `Subscriptions for ${escHtml(email)}` : 'My Subscriptions';
  let content = '';
  if (email && subs.length === 0) {
    content = `<p style="color:#94a3b8;text-align:center;margin:1.5rem 0;">No subscriptions found for this email.</p>`;
  } else if (email) {
    const rows = subs.map(s => {
      const mName = meets[s.meet_id]?.meet?.name || s.meet_id;
      const meetDate = meets[s.meet_id]?.meet?.date || '';
      const meetLocation = meets[s.meet_id]?.meet?.location || meets[s.meet_id]?.meet?.city || '';
      const details = [escHtml(meetDate), escHtml(meetLocation)].filter(Boolean).join(' &middot; ');
      return `<tr>
        <td style="padding:0.5rem 0.5rem">${escHtml(s.lifter_name)}</td>
        <td style="padding:0.5rem 0.5rem">${escHtml(mName)}${details ? '<br><span style="font-size:0.75rem;color:#64748b">' + details + '</span>' : ''}</td>
        <td style="padding:0.5rem 0.5rem;text-align:right">
          <form method="POST" action="/unsubscribe" style="display:inline" onsubmit="return confirm('Remove alert for ${escHtml(s.lifter_name).replace(/'/g, "\\'")}?')">
            <input type="hidden" name="email" value="${escHtml(s.email)}">
            <input type="hidden" name="lifter" value="${escHtml(s.lifter_name)}">
            <input type="hidden" name="meet" value="${escHtml(s.meet_id)}">
            <input type="hidden" name="return" value="my-subscriptions">
            <button type="submit" style="background:none;border:none;color:#f87171;cursor:pointer;font-size:0.85rem;padding:0.25rem 0.5rem;">remove</button>
          </form>
        </td>
      </tr>`;
    }).join('');
    content = `<table style="width:100%;border-collapse:collapse;margin-top:1rem;">
      <thead><tr><th style="text-align:left;padding:0.4rem 0.5rem;color:#64748b;font-size:0.75rem;border-bottom:1px solid #334155;">Lifter</th><th style="text-align:left;padding:0.4rem 0.5rem;color:#64748b;font-size:0.75rem;border-bottom:1px solid #334155;">Meet</th><th></th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
  }
  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>My Subscriptions - LiftAlert</title>
<style>* { box-sizing: border-box; margin: 0; padding: 0; } body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 1rem; } .card { background: #1e293b; border-radius: 12px; padding: 2rem; max-width: 520px; width: 100%; box-shadow: 0 4px 24px rgba(0,0,0,0.3); } h1 { font-size: 1.5rem; margin-bottom: 0.25rem; } .subtitle { color: #94a3b8; margin-bottom: 1.5rem; font-size: 0.9rem; } label { display: block; font-size: 0.85rem; color: #94a3b8; margin-bottom: 0.25rem; } input { width: 100%; padding: 0.6rem 0.75rem; border-radius: 6px; border: 1px solid #334155; background: #0f172a; color: #e2e8f0; font-size: 1rem; } input:focus { outline: none; border-color: #3b82f6; } button { margin-top: 1rem; width: 100%; padding: 0.7rem; border: none; border-radius: 6px; background: #3b82f6; color: white; font-size: 1rem; font-weight: 600; cursor: pointer; } button:hover { background: #2563eb; } a { color: #3b82f6; } table td { border-bottom: 1px solid #334155; font-size: 0.85rem; }</style>
</head><body><div class="card">
  <h1>${heading}</h1>
  <p class="subtitle">View and manage your LiftAlert subscriptions.</p>
  <form method="GET" action="/my-subscriptions">
    <label for="email">Email</label>
    <input type="email" id="email" name="email" required placeholder="you@example.com" value="${email ? escHtml(email) : ''}">
    <button type="submit">Look up</button>
  </form>
  ${content}
  <div style="text-align:center;margin-top:1.25rem;"><a href="/" style="color:#94a3b8;font-size:0.85rem;">Subscribe to a lifter</a></div>
</div></body></html>`;
}

// --- HTTP Server ---
const PORT = process.env.PORT || 3000;

const server = http.createServer(async (req, res) => {
  try {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (req.method === 'GET' && url.pathname === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(FORM_HTML);

  } else if (req.method === 'GET' && url.pathname === '/my-subscriptions') {
    const email = url.searchParams.get('email') || '';
    let subs = [];
    if (email) {
      try {
        subs = await getSubscriptionsByEmail(email);
      } catch (err) {
        console.error(`[LOOKUP ERROR] ${err.message}`);
      }
    }
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(mySubscriptionsHTML(email, subs));

  } else if (req.method === 'POST' && url.pathname === '/subscribe') {
    let body = '';
    let tooLarge = false;
    const MAX_BODY = 8192; // 8KB — more than enough for a subscription form
    for await (const chunk of req) {
      body += chunk;
      if (body.length > MAX_BODY) { tooLarge = true; break; }
    }
    if (tooLarge) {
      res.writeHead(413, { 'Content-Type': 'text/plain' });
      res.end('Request body too large');
      return;
    }
    const { email, lifter, meet } = parseFormBody(body);

    if (!email || !lifter || !meet) {
      res.writeHead(400, { 'Content-Type': 'text/plain' });
      res.end('Missing required fields: email, lifter, meet');
      return;
    }

    // Validate lifter exists in the specified meet
    const meetState = meets[meet];
    if (!meetState) {
      res.writeHead(400, { 'Content-Type': 'text/html' });
      res.end(errorHTML('Meet not found. Please select a lifter from the dropdown.'));
      return;
    }
    const lifterLower = lifter.toLowerCase();
    const lifterExists = Object.values(meetState.lifters).some(l => l.name && l.name.toLowerCase() === lifterLower);
    if (!lifterExists) {
      res.writeHead(400, { 'Content-Type': 'text/html' });
      res.end(errorHTML('Lifter not found in this meet. Please select a lifter from the dropdown.'));
      return;
    }

    try {
      await addSubscription(email, lifter, meet);
      delete subsCache[meet];
      console.log(`[SUBSCRIBE] ${email} -> "${lifter}" in meet ${meet}`);
      startMeet(meet);
      const meetName = meets[meet]?.meet?.name || meet;
      const meetDate = meets[meet]?.meet?.date || '';
      sendSubscriptionConfirmation(email, lifter, meetName, meetDate, meet);
      const allSubs = await getSubscriptionsByEmail(email);
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(successHTML(lifter, meet, allSubs));
    } catch (err) {
      console.error(`[SUBSCRIBE ERROR] ${err.message}`);
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Failed to subscribe. Please try again.');
    }

  } else if (req.method === 'GET' && url.pathname === '/unsubscribe') {
    const email = url.searchParams.get('email');
    const lifter = url.searchParams.get('lifter');
    const meet = url.searchParams.get('meet');

    if (!email || !lifter || !meet) {
      res.writeHead(400, { 'Content-Type': 'text/plain' });
      res.end('Missing required params: email, lifter, meet');
      return;
    }

    const meetName = meets[meet]?.meet?.name || meet;
    const confirmHtml = `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Unsubscribe</title>
<style>* { box-sizing: border-box; margin: 0; padding: 0; } body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; align-items: center; justify-content: center; } .card { background: #1e293b; border-radius: 12px; padding: 2rem; max-width: 420px; width: 90%; text-align: center; } p { color: #94a3b8; margin-bottom: 1.5rem; } button { padding: 0.6rem 1.5rem; border: none; border-radius: 6px; background: #ef4444; color: white; font-size: 1rem; cursor: pointer; } a { color: #3b82f6; display: block; margin-top: 1rem; }</style>
</head><body><div class="card">
  <p>Remove alert for <strong style="color:#e2e8f0">${escHtml(lifter)}</strong> at <strong style="color:#e2e8f0">${escHtml(meetName)}</strong>?</p>
  <form method="POST" action="/unsubscribe">
    <input type="hidden" name="email" value="${escHtml(email)}">
    <input type="hidden" name="lifter" value="${escHtml(lifter)}">
    <input type="hidden" name="meet" value="${escHtml(meet)}">
    <button type="submit">Yes, unsubscribe</button>
  </form>
  <a href="/">Cancel</a>
</div></body></html>`;
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(confirmHtml);

  } else if (req.method === 'POST' && url.pathname === '/unsubscribe') {
    let body = '';
    for await (const chunk of req) {
      body += chunk;
      if (body.length > 8192) break;
    }
    const formData = parseFormBody(body);
    const { email, lifter, meet } = formData;
    const returnTo = formData.return;

    if (!email || !lifter || !meet) {
      res.writeHead(400, { 'Content-Type': 'text/plain' });
      res.end('Missing required params: email, lifter, meet');
      return;
    }

    try {
      const removed = await removeSubscription(email, lifter, meet);
      delete subsCache[meet];
      console.log(`[UNSUBSCRIBE] ${email} -> "${lifter}" in meet ${meet} (${removed ? 'removed' : 'not found'})`);
      if (returnTo === 'my-subscriptions') {
        res.writeHead(302, { 'Location': `/my-subscriptions?email=${encodeURIComponent(email)}` });
        res.end();
      } else {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(unsubHTML(removed));
      }
    } catch (err) {
      console.error(`[UNSUBSCRIBE ERROR] ${err.message}`);
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Failed to unsubscribe. Please try again.');
    }

  } else if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      tracking: trackName || null,
      activeMeets: [...activeMeets],
      totalLifters: Object.values(meets).reduce((sum, m) => sum + Object.keys(m.lifters).length, 0),
    }));

  } else if (url.pathname === '/state') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    const allMeets = {};
    for (const [mid, st] of Object.entries(meets)) {
      const summary = {};
      for (const [pid, platform] of Object.entries(st.platforms)) {
        const parsed = parseAttemptId(platform.currentAttemptId);
        const currentLifter = parsed ? st.lifters[parsed.lifterId] : null;
        const order = computeAttemptOrder(st, pid);
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
      allMeets[mid] = { meetName: st.meet?.name || mid, platforms: summary };
    }
    res.end(JSON.stringify(allMeets, null, 2));

  } else if (req.method === 'GET' && url.pathname === '/meets') {
    const meetList = [];
    for (const [mid, st] of Object.entries(meets)) {
      const lifterCount = Object.keys(st.lifters).length;
      const platformCount = Object.keys(st.platforms).length;
      const meetDoc = st.meet || {};
      const platformSummaries = [];
      for (const [pid, platform] of Object.entries(st.platforms)) {
        const parsed = parseAttemptId(platform.currentAttemptId);
        const currentLifter = parsed ? st.lifters[parsed.lifterId] : null;
        const order = computeAttemptOrder(st, pid);
        const currentIdx = platform.currentAttemptId ? order.findIndex(a => a.attemptId === platform.currentAttemptId) : -1;
        const nextUp = currentIdx >= 0 ? order.slice(currentIdx + 1, currentIdx + 3) : [];
        platformSummaries.push({
          name: platform.name || pid,
          currentLifter: currentLifter?.name || null,
          liftName: parsed?.liftName || null,
          attemptNumber: parsed?.attemptNumber || null,
          nextUp: nextUp.map(a => ({ name: a.lifterName, lift: a.liftName, attempt: a.attemptNumber })),
        });
      }
      meetList.push({
        id: mid,
        name: meetDoc.name || mid,
        date: meetDoc.date || '',
        location: meetDoc.location || meetDoc.city || '',
        lifterCount,
        platformCount,
        watching: watchingMeets.has(mid),
        platforms: platformSummaries,
      });
    }
    meetList.sort((a, b) => a.name.localeCompare(b.name));
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(meetsHTML(meetList));

  } else if (req.method === 'GET' && url.pathname === '/api/lifters') {
    const results = [];
    for (const [meetId, st] of Object.entries(meets)) {
      const meetName = st.meet?.name || meetId;
      for (const lifter of Object.values(st.lifters)) {
        if (lifter.name) {
          results.push({ name: lifter.name, meetId, meetName });
        }
      }
    }
    results.sort((a, b) => a.name.localeCompare(b.name));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(results));

  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found');
  }
  } catch (err) {
    console.error(`[HTTP ERROR] ${err.message}`);
    if (!res.headersSent) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Internal server error');
    }
  }
});

// --- Main ---
async function main() {
  console.log('\n=== LiftAlert CouchDB Client ===');
  console.log(`CouchDB: ${couchdbBase}`);
  if (trackName) console.log(`Console tracking: "${trackName}"`);
  console.log('');

  // Start HTTP server first (Railway healthcheck needs it immediately)
  server.listen(PORT, () => {
    console.log(`[HTTP] Server listening on port ${PORT}`);
  });

  // Initialize database
  try {
    await initDB();
  } catch (err) {
    console.error(`[DB ERROR] ${err.message}`);
    console.log('[DB] Continuing without database — subscription features disabled');
  }

  // Discover and index all of today's meets from LiftingCast API
  await discoverTodaysMeets();

  // Load CLI-specified meet (with changes feed)
  if (cliMeetId) {
    try {
      await startMeet(cliMeetId);
    } catch (err) {
      console.error(`[FATAL] Failed to load meet ${cliMeetId}: ${err.message}`);
    }
  }

  // Load meets from existing subscriptions (with changes feeds)
  try {
    const dbMeets = await getAllMeetIds();
    for (const mid of dbMeets) {
      startMeet(mid);
    }
  } catch (err) {
    console.error(`[DB ERROR] Could not load subscription meets: ${err.message}`);
  }

  // Periodically discover new meets and check for new subscriptions
  pollForNewMeets();
}

main().catch(err => {
  console.error(`[FATAL] ${err.message}`);
  process.exit(1);
});

// --- Graceful shutdown on SIGTERM/SIGINT ---
function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`\n[SHUTDOWN] Received ${signal}, closing gracefully...`);

  // Disconnect all SymPlmeet sockets
  stopAllSymPlmeet();

  // Stop accepting new connections, let in-flight requests finish
  server.close(() => {
    console.log('[SHUTDOWN] HTTP server closed');
    process.exit(0);
  });

  // Force exit if server doesn't close within 5 seconds
  setTimeout(() => {
    console.log('[SHUTDOWN] Forcing exit after timeout');
    process.exit(0);
  }, 5000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
