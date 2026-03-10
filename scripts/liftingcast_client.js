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
const { initDB, getSubscriptions, getAllMeetIds, addSubscription, removeSubscription, getSubscriptionsByEmail,
        addPersistentSubscription, removePersistentSubscription, getPersistentSubscriptionsByEmail, getAllPersistentSubscriptions,
        getStats, logAttemptTimestamp, getAttemptTimestampsByMeet, getMeetVideo, getMeetVideos, setMeetVideo,
        getRecapLifterNames, getRecapMeets, getEmailStats } = require('./db');
const { sendOnDeckEmail, sendSubscriptionConfirmation, sendAutoSubscribeNotification, sendRecapEmail } = require('./email');
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
const recapsSent = new Set(); // meetIds that have had recap emails dispatched

// --- Meets page cache (avoids recomputing attempt order on every page load) ---
// Bounded LRU-ish map: max 20 entries, keyed by email (or empty string for anonymous)
const meetsPageCache = new Map();
const MEETS_PAGE_CACHE_TTL = 5_000; // 5 seconds
const MEETS_PAGE_CACHE_MAX = 20;

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

// --- IPF/USAPL weight classes (kg) by gender ---
const WEIGHT_CLASSES_MALE   = [53, 59, 66, 74, 83, 93, 105, 120];
const WEIGHT_CLASSES_FEMALE = [43, 47, 52, 57, 63, 69, 76, 84];
function getWeightClass(bw, gender, declaredWc) {
  // Use declared weight class from meet software if available
  if (declaredWc) {
    const n = Number(declaredWc);
    return n > 0 ? n : declaredWc; // e.g. "120+" stays as string
  }
  if (!bw) return null;
  const isFemale = gender && /^f/i.test(gender);
  const classes = isFemale ? WEIGHT_CLASSES_FEMALE : WEIGHT_CLASSES_MALE;
  for (const wc of classes) {
    if (bw <= wc) return wc;
  }
  return isFemale ? '84+' : '120+';
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
    const meetDate = getMeetState(meetId).meet?.date || '';
    const nameLower = lifterName.toLowerCase();

    for (const sub of subs) {
      if (nameLower !== sub.lifter_name.toLowerCase()) continue;
      const prefs = (sub.notify_prefs || 'in-the-hole').split(',');
      if (!prefs.includes(position)) continue;
      console.log(`[NOTIFY] Match: "${lifterName}" is ${position} — notifying ${sub.email}`);
      await sendOnDeckEmail(sub.email, lifterName, meetName, liftName, position, meetId, sub.lifter_name, details, meetDate);
    }
  } catch (err) {
    console.error(`[NOTIFY ERROR] ${err.message}`);
  }
}

// --- Send recap emails when a meet ends ---
async function sendMeetRecaps(meetId) {
  if (recapsSent.has(meetId)) return;
  recapsSent.add(meetId);

  try {
    const subs = await getSubscriptions(meetId);
    if (subs.length === 0) {
      console.log(`[RECAP] No subscribers for meet ${meetId}, skipping`);
      return;
    }

    const timestamps = await getAttemptTimestampsByMeet(meetId);
    if (timestamps.length === 0) {
      console.log(`[RECAP] No timestamps for meet ${meetId}, skipping`);
      return;
    }

    const video = await getMeetVideo(meetId);
    const videoId = video?.youtube_video_id || null;
    const streamStart = video ? Number(video.stream_start_epoch) : null;

    const meetName = getMeetState(meetId).meet?.name || video?.meet_name || meetId;
    const meetDate = getMeetState(meetId).meet?.date || video?.meet_date || '';

    // Group timestamps by lifter_name (lowercase key)
    const byLifter = {};
    for (const t of timestamps) {
      const key = t.lifter_name.toLowerCase();
      if (!byLifter[key]) byLifter[key] = [];
      byLifter[key].push(t);
    }

    for (const sub of subs) {
      const lifterKey = sub.lifter_name.toLowerCase();
      const lifterTimestamps = byLifter[lifterKey];
      if (!lifterTimestamps || lifterTimestamps.length === 0) continue;

      const attempts = lifterTimestamps.map(t => {
        const wallEpoch = Math.floor(new Date(t.wall_clock_time).getTime() / 1000);
        let youtubeLink = null;
        let timeFormatted = '';
        if (videoId && streamStart) {
          const offset = Math.max(0, wallEpoch - streamStart - TIMESTAMP_LEAD_SECONDS);
          const h = Math.floor(offset / 3600);
          const m = Math.floor((offset % 3600) / 60);
          const s = Math.floor(offset % 60);
          timeFormatted = h > 0 ? `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}` : `${m}:${String(s).padStart(2, '0')}`;
          youtubeLink = `https://www.youtube.com/watch?v=${videoId}&t=${offset}`;
        }
        return {
          lift_name: t.lift_name,
          attempt_number: t.attempt_number,
          weight: t.weight,
          timeFormatted,
          youtubeLink,
        };
      });

      console.log(`[RECAP] Sending recap to ${sub.email} for ${sub.lifter_name} at "${meetName}" (${attempts.length} attempts)`);
      sendRecapEmail(sub.email, sub.lifter_name, meetName, attempts, videoId, meetDate)
        .catch(err => console.error(`[RECAP ERROR] Email to ${sub.email}: ${err.message}`));
    }
  } catch (err) {
    console.error(`[RECAP ERROR] Failed to send recaps for meet ${meetId}: ${err.message}`);
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

    // Cache platform summary for /meets page (avoids recomputing order on page load)
    st.platformSummaryCache = st.platformSummaryCache || {};
    st.platformSummaryCache[platformId] = {
      currentLifter: currentName,
      liftName: parsed?.liftName || null,
      attemptNumber: parsed?.attemptNumber || null,
      nextUp: (currentIdx >= 0 ? order.slice(currentIdx + 1, currentIdx + 3) : [])
        .map(a => ({ name: a.lifterName, lift: a.liftName, attempt: a.attemptNumber })),
    };

    if (!st.trackState[platformId]) {
      st.trackState[platformId] = {
        lastCurrentAttemptId: null,
        notifiedOnDeck: new Set(), notifiedInTheHole: new Set(), notifiedLifting: new Set(),
        notified5MinOut: new Set(), notified10MinOut: new Set(),
        notifiedFlightStarts: new Set(),
        lastFlightLift: null,
      };
    }
    const ts = st.trackState[platformId];

    // Reset per-attempt dedup sets when current attempt changes
    // (flight-start tracking persists for the entire meet)
    if (platform.currentAttemptId !== ts.lastCurrentAttemptId) {
      ts.lastCurrentAttemptId = platform.currentAttemptId;
      ts.notifiedOnDeck = new Set();
      ts.notifiedInTheHole = new Set();
      ts.notifiedLifting = new Set();
      ts.notified5MinOut = new Set();
      ts.notified10MinOut = new Set();

      console.log(`\n[CURRENT] ${currentName} - ${parsed.liftName} attempt ${parsed.attemptNumber} (${platform.name || platformId}) [${meetId}]`);

      // Log timestamp for meet recap feature
      const currentAttemptDoc = st.attempts[platform.currentAttemptId];
      const currentLifterDoc = st.lifters[parsed.lifterId];
      logAttemptTimestamp(
        meetId, platformId, platform.currentAttemptId,
        parsed.lifterId, currentName, parsed.liftName, parsed.attemptNumber,
        currentAttemptDoc?.weight, currentLifterDoc?.bodyWeight
      ).catch(err => console.error(`[TIMESTAMP ERROR] ${err.message}`));

      // Auto-discover YouTube VOD for this meet (fire-and-forget, runs once per meet)
      autoLinkYouTubeVideo(meetId, st.meet?.name || meetId, st.meet?.date || '').catch(err => console.error(`[YT ERROR] ${err.message}`));

      if (nextAttempts.length > 0) {
        const upcoming = nextAttempts.slice(0, 5).map((a, i) => {
          const label = i === 0 ? 'ON DECK' : i === 1 ? 'IN HOLE' : `#${i + 2}`;
          return `  ${label}: ${a.lifterName} (${a.liftName} ${a.attemptNumber})`;
        });
        console.log(upcoming.join('\n'));
      }

      // --- Flight start detection ---
      // Fires once per flight+lift combo (e.g. "A:squat") when the first
      // attempt of that combo becomes current on the platform.
      const flightLiftKey = `${currentLifter?.flight}:${parsed.liftName}`;
      if (!ts.notifiedFlightStarts.has(flightLiftKey) && ts.lastFlightLift !== flightLiftKey) {
        ts.notifiedFlightStarts.add(flightLiftKey);
        const flightLifters = Object.values(st.lifters)
          .filter(l => l.platformId === platformId && l.flight === currentLifter?.flight);
        for (const lifter of flightLifters) {
          notifySubscribers(meetId, lifter.name, parsed.liftName, 'flight-start', { flight: currentLifter?.flight });
        }
      }
      ts.lastFlightLift = flightLiftKey;
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

    // --- Email notifications at various positions ---

    // "lifting" — current lifter
    if (currentLifter?.name && !ts.notifiedLifting.has(currentName)) {
      ts.notifiedLifting.add(currentName);
      const currentAttemptDoc = st.attempts[platform.currentAttemptId];
      const placeInfo = getPlaceInfo(st, platformId, parsed.lifterId, currentAttemptDoc?.weight, parsed.liftName);
      notifySubscribers(meetId, currentName, parsed.liftName, 'lifting', {
        weight: currentAttemptDoc?.weight,
        attemptNumber: parsed.attemptNumber,
        liftName: parsed.liftName,
        ...placeInfo,
      });
    }

    // "on-deck" — next lifter
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

    // "in-the-hole" — 2 lifters away
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

    // "5-min-out" — ~5 attempts away
    if (nextAttempts.length > 4 && nextAttempts[4].lifterName && !ts.notified5MinOut.has(nextAttempts[4].lifterName)) {
      ts.notified5MinOut.add(nextAttempts[4].lifterName);
      const a = nextAttempts[4];
      const placeInfo = getPlaceInfo(st, platformId, a.lifterId, a.weight, a.liftName);
      notifySubscribers(meetId, a.lifterName, a.liftName, '5-min-out', {
        weight: a.weight,
        attemptNumber: a.attemptNumber,
        liftName: a.liftName,
        ...placeInfo,
      });
    }

    // "10-min-out" — ~10 attempts away
    if (nextAttempts.length > 9 && nextAttempts[9].lifterName && !ts.notified10MinOut.has(nextAttempts[9].lifterName)) {
      ts.notified10MinOut.add(nextAttempts[9].lifterName);
      const a = nextAttempts[9];
      const placeInfo = getPlaceInfo(st, platformId, a.lifterId, a.weight, a.liftName);
      notifySubscribers(meetId, a.lifterName, a.liftName, '10-min-out', {
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

// --- Auto-subscribe persistent followers when a meet is indexed ---
async function autoSubscribeForMeet(meetId) {
  try {
    const st = getMeetState(meetId);
    const lifterNames = Object.values(st.lifters).map(l => l.name).filter(Boolean);
    if (lifterNames.length === 0) return;
    const persistentSubs = await getAllPersistentSubscriptions();
    if (persistentSubs.length === 0) return;
    const lifterNamesLower = lifterNames.map(n => n.toLowerCase());
    let created = 0;
    for (const ps of persistentSubs) {
      const idx = lifterNamesLower.indexOf(ps.lifter_name.toLowerCase());
      if (idx === -1) continue;
      const actualName = lifterNames[idx];
      try {
        await addSubscription(ps.email, actualName, meetId, ps.notify_prefs);
        created++;
        const meetName = st.meet?.name || meetId;
        const meetDate = st.meet?.date || '';
        sendAutoSubscribeNotification(ps.email, actualName, meetName, meetDate, meetId);
        console.log(`[AUTO-SUB] ${ps.email} auto-subscribed to "${actualName}" in meet ${meetId}`);
      } catch (err) {
        console.error(`[AUTO-SUB ERROR] ${ps.email} -> "${ps.lifter_name}": ${err.message}`);
      }
    }
    if (created > 0) {
      delete subsCache[meetId];
      console.log(`[AUTO-SUB] Created ${created} auto-subscriptions for meet ${meetId}`);
    }
  } catch (err) {
    console.error(`[AUTO-SUB ERROR] Failed for meet ${meetId}: ${err.message}`);
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
    await autoSubscribeForMeet(meetId);
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

  // SymPlmeet meets (API returns all open/non-completed meets)
  try {
    const symplMeets = await discoverTodaysSymPlmeetMeets();
    activeSymPlmeetIds = new Set(symplMeets.map(m => m.id));

    // Pre-set discovered names so indexMeet → loadSymPlmeetMeet has them as fallback
    for (const m of symplMeets) {
      const st = getMeetState(m.id);
      if (!st.meet || st.meet.name === `SymPlmeet #${m.id}`) {
        st.meet = { _id: String(m.id), name: m.name };
      }
    }

    const newSymplMeets = symplMeets.filter(m => !loadedMeets.has(m.id) && !pendingMeets.has(m.id));
    if (newSymplMeets.length > 0) {
      console.log(`[DISCOVER] Indexing ${newSymplMeets.length} new SymPlmeet meets for autocomplete...`);
      const BATCH_SIZE = 5;
      for (let i = 0; i < newSymplMeets.length; i += BATCH_SIZE) {
        const batch = newSymplMeets.slice(i, i + BATCH_SIZE);
        await Promise.all(batch.map(m => indexMeet(m.id)));
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
      // Send recap emails for stale meets that have subscribers
      const subMeetIds = new Set(meetIds);
      for (const mid of [...watchingMeets]) {
        if (isMeetStale(mid) && subMeetIds.has(mid)) {
          sendMeetRecaps(mid).catch(err => console.error(`[RECAP ERROR] ${err.message}`));
        }
      }

      // Clean up stale meets — stop changes feeds and remove from index
      // (keeps subscribed meets that still have active subs, even if stale)
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

// Timestamps are logged when an attempt becomes "current" on the platform,
// which is typically ~15s before the lifter actually approaches the bar.
// Subtract this offset so YouTube links land closer to the actual lift.
const TIMESTAMP_LEAD_SECONDS = 15;

// --- YouTube auto-discovery ---
const videoSearched = new Set(); // track which meets we've already searched for

function ytFetch(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'en' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return ytFetch(res.headers.location).then(resolve, reject);
      }
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve(body));
      res.on('error', reject);
    }).on('error', reject);
  });
}

async function searchYouTube(query) {
  try {
    const encoded = encodeURIComponent(query);
    // sp=EgJAAQ%3D%3D filters for "Live" streams — try this first
    const liveHtml = await ytFetch(`https://www.youtube.com/results?search_query=${encoded}&sp=EgJAAQ%3D%3D`);
    const liveMatch = liveHtml.match(/"videoId":"([^"]{11})"/);
    if (liveMatch) return liveMatch[1];

    // Fall back to regular search — take the first YouTube result
    console.log(`[YT] No live stream found for "${query}", trying regular search...`);
    const html = await ytFetch(`https://www.youtube.com/results?search_query=${encoded}`);
    const match = html.match(/"videoId":"([^"]{11})"/);
    if (!match) {
      console.log(`[YT] No videoId regex match in ${html.length} bytes of HTML — YouTube page structure may have changed`);
    }
    return match ? match[1] : null;
  } catch (err) {
    console.error(`[YT SEARCH ERROR] ${err.message}`);
    return null;
  }
}

async function getYouTubeStreamStart(videoId) {
  try {
    const html = await ytFetch(`https://www.youtube.com/watch?v=${videoId}`);
    const match = html.match(/"startTimestamp":"([^"]+)"/);
    if (match) {
      return Math.floor(new Date(match[1]).getTime() / 1000);
    }
    console.log(`[YT] No startTimestamp regex match in ${html.length} bytes of HTML for video ${videoId} — YouTube page structure may have changed`);
    return null;
  } catch (err) {
    console.error(`[YT META ERROR] ${err.message}`);
    return null;
  }
}

async function autoLinkYouTubeVideo(meetId, meetName, meetDate) {
  if (videoSearched.has(meetId)) return;
  videoSearched.add(meetId);

  // Check if already linked
  try {
    const existing = await getMeetVideo(meetId);
    if (existing) {
      console.log(`[YT] Meet ${meetId} already linked to ${existing.youtube_video_id}`);
      return;
    }
  } catch (err) {
    console.error(`[YT] Error checking existing video for ${meetId}: ${err.message}`);
  }

  console.log(`[YT] Searching YouTube for "${meetName}"...`);
  const videoId = await searchYouTube(meetName);
  if (!videoId) {
    console.log(`[YT] No video found for "${meetName}"`);
    return;
  }

  console.log(`[YT] Found video ${videoId}, fetching stream start time...`);
  const streamStart = await getYouTubeStreamStart(videoId);
  if (!streamStart) {
    console.log(`[YT] Could not get stream start for ${videoId} — linking video without timestamps`);
  }

  try {
    await setMeetVideo(meetId, videoId, `https://www.youtube.com/watch?v=${videoId}`, streamStart || 0, meetName, meetDate);
    console.log(`[YT] Auto-linked meet ${meetId} -> https://www.youtube.com/watch?v=${videoId}${streamStart ? ` (start: ${new Date(streamStart * 1000).toISOString()})` : ' (no stream start — VOD only)'}`);
  } catch (err) {
    console.error(`[YT DB ERROR] ${err.message}`);
  }
}

// --- Shared HTML design tokens ---
const FONT_LINKS = '<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">';

const SHARED_STYLES = `
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Outfit', -apple-system, BlinkMacSystemFont, sans-serif; background: #0A0A0A; color: #F0F0F0; min-height: 100vh; -webkit-font-smoothing: antialiased; }
  a { color: #DC2626; text-decoration: none; transition: color 0.2s; }
  a:hover { color: #EF4444; }
  .brand { font-family: 'Bebas Neue', sans-serif; font-size: 2.25rem; letter-spacing: 0.06em; line-height: 1; }
  .brand-lift { color: #DC2626; }
  .brand-alert { color: #F0F0F0; }
  .card { background: #141414; border: 1px solid #1F1F1F; border-radius: 14px; padding: 2.25rem; max-width: 440px; width: 92%; position: relative; overflow: hidden; }
  .card::before { content: ''; position: absolute; top: 0; left: 0; bottom: 0; width: 3px; background: #DC2626; }
  .subtitle { color: #777; font-size: 0.95rem; margin-top: 0.5rem; font-weight: 300; }
  label { display: block; font-size: 0.72rem; color: #999; margin-bottom: 0.35rem; margin-top: 1.25rem; text-transform: uppercase; letter-spacing: 0.1em; font-weight: 600; }
  input[type="email"], input[type="text"] { width: 100%; padding: 0.7rem 0.85rem; border-radius: 8px; border: 1px solid #252525; background: #0D0D0D; color: #F0F0F0; font-size: 1rem; font-family: 'Outfit', sans-serif; transition: border-color 0.2s, box-shadow 0.2s; }
  input:focus { outline: none; border-color: #DC2626; box-shadow: 0 0 0 3px rgba(220,38,38,0.1); }
  .btn-primary { margin-top: 1.5rem; width: 100%; padding: 0.85rem; border: none; border-radius: 8px; background: #DC2626; color: white; font-size: 1.15rem; font-family: 'Bebas Neue', sans-serif; letter-spacing: 0.14em; cursor: pointer; transition: all 0.2s; }
  .btn-primary:hover { background: #B91C1C; transform: translateY(-1px); box-shadow: 0 4px 12px rgba(220,38,38,0.3); }
  .btn-primary:active { transform: translateY(0); }
  .btn-danger { padding: 0.65rem 1.5rem; border: none; border-radius: 8px; background: #DC2626; color: white; font-size: 1rem; font-family: 'Bebas Neue', sans-serif; letter-spacing: 0.1em; cursor: pointer; transition: background 0.2s; }
  .btn-danger:hover { background: #B91C1C; }
  .msg { margin-top: 1rem; padding: 0.75rem 1rem; border-radius: 8px; font-size: 0.88rem; }
  .msg.ok { background: #052E16; color: #4ADE80; border: 1px solid #166534; }
  .msg.err { background: #450A0A; color: #FCA5A5; border: 1px solid #7F1D1D; }
  h1 { font-family: 'Bebas Neue', sans-serif; letter-spacing: 0.04em; }
  h2 { font-family: 'Bebas Neue', sans-serif; letter-spacing: 0.04em; }
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; padding: 0.5rem 0.5rem; color: #666; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em; border-bottom: 1px solid #252525; font-weight: 600; }
  td { padding: 0.5rem 0.5rem; border-bottom: 1px solid #1A1A1A; font-size: 0.88rem; }
  @keyframes fadeUp { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }
  @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
  .animate-in { animation: fadeUp 0.5s ease-out both; }
  .remove-wrap .confirm-btns { display: none; }
  .remove-wrap.confirming .remove-btn { display: none; }
  .remove-wrap.confirming .confirm-btns { display: inline !important; }
  .pref-options { display: grid; grid-template-columns: 1fr 1fr; gap: 0.35rem 0.75rem; margin-top: 0.25rem; }
  .pref-options label.pref { display: flex; align-items: center; gap: 0.4rem; font-size: 0.82rem; color: #CCC; text-transform: none; letter-spacing: 0; font-weight: 400; margin: 0; cursor: pointer; }
  .pref-options input[type="checkbox"] { width: 15px; height: 15px; accent-color: #DC2626; cursor: pointer; flex-shrink: 0; }
`;

// --- HTML subscription form ---
const FORM_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>LiftAlert - Get Notified</title>
  ${FONT_LINKS}
  <style>
    ${SHARED_STYLES}
    body { display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 1.5rem 1rem; min-height: 100vh; gap: 1.25rem; }
    .help { font-size: 0.72rem; color: #555; margin-top: 0.35rem; }
    .autocomplete-wrapper { position: relative; }
    .suggestions { position: absolute; top: 100%; left: 0; right: 0; background: #141414; border: 1px solid #252525; border-top: none; border-radius: 0 0 8px 8px; max-height: 240px; overflow-y: auto; z-index: 10; display: none; }
    .suggestion-item { padding: 0.55rem 0.85rem; cursor: pointer; transition: background 0.15s; }
    .suggestion-item:hover, .suggestion-item.active { background: #1F1F1F; }
    .suggestion-item .name { color: #F0F0F0; font-size: 0.95rem; }
    .suggestion-item .meet-name { color: #666; font-size: 0.8rem; margin-top: 0.1rem; }
    .selected-pills { margin-top: 0.75rem; display: flex; flex-direction: column; gap: 0.4rem; }
    .pill { padding: 0.45rem 0.75rem; background: #1A1A1A; border: 1px solid #252525; border-radius: 8px; display: flex; align-items: center; justify-content: space-between; }
    .pill .pill-text { color: #F0F0F0; font-size: 0.85rem; }
    .pill .pill-meet { color: #777; font-size: 0.72rem; }
    .pill .pill-clear { color: #777; cursor: pointer; font-size: 1.2rem; padding: 0 0.25rem; transition: color 0.2s; margin-left: 0.5rem; }
    .pill .pill-clear:hover { color: #FCA5A5; }
    .pill-count { font-size: 0.75rem; color: #555; margin-top: 0.25rem; text-transform: uppercase; letter-spacing: 0.05em; }
    .no-results { padding: 0.55rem 0.85rem; color: #555; font-size: 0.85rem; }
    .meet-count { text-align: center; margin-top: 1.25rem; font-size: 0.8rem; color: #555; display: none; align-items: center; justify-content: center; gap: 0.4rem; }
    .meet-count .pulse-dot { width: 6px; height: 6px; border-radius: 50%; background: #22C55E; animation: pulse 2s ease-in-out infinite; display: inline-block; }
    .features { display: grid; grid-template-columns: repeat(3, 1fr); gap: 0.6rem; width: 92%; max-width: 440px; }
    .feature-card { background: #141414; border: 1px solid #1F1F1F; border-radius: 12px; padding: 1rem 0.75rem; text-decoration: none; color: inherit; transition: border-color 0.2s, background 0.2s; cursor: pointer; text-align: center; display: flex; flex-direction: column; align-items: center; gap: 0.35rem; }
    .feature-card:hover { background: #1A1A1A; border-color: #DC2626; }
    .feature-icon { width: 24px; height: 24px; color: #DC2626; flex-shrink: 0; }
    .feature-title { font-family: 'Bebas Neue', sans-serif; font-size: 0.95rem; letter-spacing: 0.06em; color: #F0F0F0; line-height: 1; }
    .feature-desc { font-size: 0.68rem; color: #555; line-height: 1.3; }
  </style>
</head>
<body>
  <div class="card animate-in">
    <div style="margin-bottom: 1.25rem;">
      <span class="brand"><span class="brand-lift">LIFT</span><span class="brand-alert">ALERT</span></span>
      <p class="subtitle">Never miss a lift.</p>
    </div>
    <form method="POST" action="/subscribe" id="subForm">
      <label for="email">Email</label>
      <input type="email" id="email" name="email" required placeholder="you@example.com">

      <label for="lifterInput">Lifter Names</label>
      <div class="autocomplete-wrapper" id="autocompleteWrapper">
        <input type="text" id="lifterInput" placeholder="Start typing a lifter name..." autocomplete="off">
        <div class="suggestions" id="suggestions"></div>
      </div>
      <p class="help">Select one or more lifters from the dropdown</p>

      <div class="selected-pills" id="selectedPills"></div>
      <div class="pill-count" id="pillCount"></div>

      <label>Notify me when</label>
      <div class="pref-options">
        <label class="pref"><input type="checkbox" name="pref" value="in-the-hole" checked> In the Hole (2 away)</label>
        <label class="pref"><input type="checkbox" name="pref" value="on-deck"> On Deck (next up)</label>
        <label class="pref"><input type="checkbox" name="pref" value="lifting"> Lifting Now</label>
        <label class="pref"><input type="checkbox" name="pref" value="5-min-out"> ~5 Minutes Out</label>
        <label class="pref"><input type="checkbox" name="pref" value="10-min-out"> ~10 Minutes Out</label>
        <label class="pref"><input type="checkbox" name="pref" value="flight-start"> Start of Flight</label>
      </div>

      <button type="submit" class="btn-primary">SUBSCRIBE</button>
    </form>
    <div class="meet-count" id="meetCount"><span class="pulse-dot"></span> <span id="meetCountText"></span></div>
  </div>
  <div class="features animate-in" style="animation-delay:0.12s;">
    <a href="/meets" class="feature-card">
      <svg class="feature-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="2"/><path d="M16.24 7.76a6 6 0 0 1 0 8.49"/><path d="M7.76 16.24a6 6 0 0 1 0-8.49"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/><path d="M4.93 19.07a10 10 0 0 1 0-14.14"/></svg>
      <div class="feature-title">LIVE MEETS</div>
      <div class="feature-desc">Browse &amp; track open meets</div>
    </a>
    <a href="/my-subscriptions" class="feature-card">
      <svg class="feature-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>
      <div class="feature-title">MY ALERTS</div>
      <div class="feature-desc">Manage subscriptions</div>
    </a>
    <a href="/recaps" class="feature-card">
      <svg class="feature-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="4" width="20" height="16" rx="2"/><path d="m10 9 5 3-5 3z" fill="currentColor"/></svg>
      <div class="feature-title">RECAPS</div>
      <div class="feature-desc">Relive meet highlights</div>
    </a>
  </div>
  <script>
    const lifterInput = document.getElementById('lifterInput');
    const suggestionsEl = document.getElementById('suggestions');
    const pillsContainer = document.getElementById('selectedPills');
    const pillCount = document.getElementById('pillCount');
    const form = document.getElementById('subForm');

    // Pre-fill email from localStorage
    try {
      const savedEmail = localStorage.getItem('liftalert_email');
      if (savedEmail) document.getElementById('email').value = savedEmail;
    } catch(e) {}

    let activeIdx = -1;
    let currentResults = [];
    let allLifters = [];
    let selections = []; // { name, meetId, meetName }

    fetch('/api/lifters').then(r => r.json()).then(data => {
      allLifters = data;
      const meetIds = new Set(data.map(l => l.meetId));
      const count = meetIds.size;
      if (count > 0) {
        const el = document.getElementById('meetCount');
        document.getElementById('meetCountText').textContent = 'Monitoring ' + count + ' meet' + (count !== 1 ? 's' : '');
        el.style.display = 'flex';
      }
    });

    form.addEventListener('submit', (e) => {
      e.preventDefault();
      if (selections.length === 0) {
        lifterInput.focus();
        lifterInput.style.borderColor = '#ef4444';
        setTimeout(() => { lifterInput.style.borderColor = ''; }, 2000);
        return;
      }
      const email = document.getElementById('email').value;
      if (!email) return;
      try { localStorage.setItem('liftalert_email', email); } catch(e) {}
      const btn = form.querySelector('button[type="submit"]');
      btn.disabled = true;
      btn.textContent = 'SUBSCRIBING...';
      const notifyPrefs = [...document.querySelectorAll('input[name="pref"]:checked')].map(c => c.value).join(',');
      fetch('/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, selections, notifyPrefs })
      }).then(r => r.text()).then(html => {
        document.open(); document.write(html); document.close();
      }).catch(() => {
        btn.disabled = false;
        btn.textContent = 'SUBSCRIBE';
        alert('Failed to subscribe. Please try again.');
      });
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

    function removeSelection(idx) {
      selections.splice(idx, 1);
      renderPills();
      lifterInput.focus();
    }

    function renderPills() {
      pillsContainer.innerHTML = selections.map((s, i) =>
        '<div class="pill">' +
          '<span class="pill-text">' + escHtml(s.name) + '<br><span class="pill-meet">' + escHtml(s.meetName) + '</span></span>' +
          '<span class="pill-clear" data-idx="' + i + '" title="Remove">&times;</span>' +
        '</div>'
      ).join('');
      pillsContainer.querySelectorAll('.pill-clear').forEach(el => {
        el.addEventListener('click', () => removeSelection(parseInt(el.dataset.idx)));
      });
      pillCount.textContent = selections.length > 0
        ? selections.length + ' lifter' + (selections.length !== 1 ? 's' : '') + ' selected'
        : '';
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
      const dup = selections.some(s => s.name === r.name && s.meetId === r.meetId);
      if (!dup) {
        selections.push({ name: r.name, meetId: r.meetId, meetName: r.meetName });
        renderPills();
      }
      lifterInput.value = '';
      closeSuggestions();
      lifterInput.focus();
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
      suggestionsEl.innerHTML = data.map((r, i) => {
        const already = selections.some(s => s.name === r.name && s.meetId === r.meetId);
        return '<div class="suggestion-item' + (already ? ' already' : '') + '" data-idx="' + i + '">' +
          '<div class="name">' + escHtml(r.name) + (already ? ' <span style="color:#555;font-size:0.75rem;">(added)</span>' : '') + '</div>' +
          '<div class="meet-name">' + escHtml(r.meetName) + (r.meetDate ? ' (' + escHtml(r.meetDate) + ')' : '') + '</div>' +
        '</div>';
      }).join('');
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
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Error - LiftAlert</title>
${FONT_LINKS}
<style>
  ${SHARED_STYLES}
  body { display: flex; align-items: center; justify-content: center; padding: 1rem; }
  .error-msg { margin-top: 1.25rem; padding: 1rem; background: #450A0A; border: 1px solid #7F1D1D; border-radius: 8px; color: #FCA5A5; font-size: 0.95rem; text-align: center; }
  .back-link { display: block; text-align: center; margin-top: 1.5rem; color: #777; font-size: 0.88rem; }
  .back-link:hover { color: #F0F0F0; }
</style>
</head><body><div class="card animate-in">
  <span class="brand"><span class="brand-lift">LIFT</span><span class="brand-alert">ALERT</span></span>
  <div class="error-msg">${msg}</div>
  <a href="/" class="back-link">&larr; Go back</a>
</div></body></html>`;
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
      <td>${escHtml(s.lifter_name)}</td>
      <td>${escHtml(mName)}${details ? '<br><span style="font-size:0.75rem;color:#555">' + details + '</span>' : ''}</td>
      <td><a href="${unsubUrl}" style="color:#DC2626;font-size:0.8rem">remove</a></td>
    </tr>`;
  }).join('');

  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Subscribed! - LiftAlert</title>
${FONT_LINKS}
<style>
  ${SHARED_STYLES}
  body { display: flex; align-items: center; justify-content: center; padding: 1rem; }
  .card { max-width: 520px; }
  .success-heading { font-family: 'Bebas Neue', sans-serif; font-size: 2rem; color: #22C55E; text-align: center; letter-spacing: 0.06em; margin-bottom: 0.5rem; }
  .confirm-text { color: #999; text-align: center; margin-bottom: 1.5rem; font-size: 0.95rem; }
  .confirm-text strong { color: #F0F0F0; }
  .spam-warning { background: #1A1700; border: 1px solid #422006; border-radius: 8px; padding: 0.75rem 1rem; margin-bottom: 1.75rem; font-size: 0.85rem; color: #F59E0B; text-align: center; }
  .subs-heading { font-family: 'Bebas Neue', sans-serif; font-size: 1.15rem; color: #777; letter-spacing: 0.06em; margin-bottom: 0.5rem; }
  .cta { text-align: center; margin-top: 1.5rem; }
  .cta a { color: #777; font-size: 0.88rem; }
  .cta a:hover { color: #F0F0F0; }
</style>
</head><body><div class="card animate-in">
  <div style="text-align:center;margin-bottom:1.25rem;"><span class="brand"><span class="brand-lift">LIFT</span><span class="brand-alert">ALERT</span></span></div>
  <div class="success-heading">SUBSCRIBED!</div>
  <p class="confirm-text">You'll get an email when <strong>${escHtml(lifter)}</strong> is almost up (2 lifters away) at <strong>${escHtml(meetName)}</strong>.<br><span style="font-size:0.85rem;color:#777;">You'll also be auto-subscribed when they compete in future meets.</span></p>
  <div class="spam-warning">Check your spam/junk folder and mark our emails as &ldquo;Not Spam&rdquo; to make sure you get alerts on time.</div>
  <div class="subs-heading">YOUR SUBSCRIPTIONS</div>
  <table><thead><tr><th>Lifter</th><th>Meet</th><th></th></tr></thead><tbody>${subsRows}</tbody></table>
  <div class="cta"><a href="/">&larr; Subscribe to another lifter</a></div>
</div></body></html>`;
}

function bulkSuccessHTML(email, items, allSubs, notifyPrefs) {
  const namesList = items.map(i => `<strong>${escHtml(i.name)}</strong>`).join(', ');
  const prefsList = (notifyPrefs || 'in-the-hole').split(',').join(', ');
  const subsRows = allSubs.map(s => {
    const mName = meets[s.meet_id]?.meet?.name || s.meet_id;
    const meetDate = meets[s.meet_id]?.meet?.date || '';
    const meetLocation = meets[s.meet_id]?.meet?.location || meets[s.meet_id]?.meet?.city || '';
    const details = [escHtml(meetDate), escHtml(meetLocation)].filter(Boolean).join(' &middot; ');
    const unsubUrl = `/unsubscribe?email=${encodeURIComponent(email)}&lifter=${encodeURIComponent(s.lifter_name)}&meet=${encodeURIComponent(s.meet_id)}`;
    return `<tr>
      <td>${escHtml(s.lifter_name)}</td>
      <td>${escHtml(mName)}${details ? '<br><span style="font-size:0.75rem;color:#555">' + details + '</span>' : ''}</td>
      <td><a href="${unsubUrl}" style="color:#DC2626;font-size:0.8rem">remove</a></td>
    </tr>`;
  }).join('');
  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Subscribed! - LiftAlert</title>
${FONT_LINKS}
<style>
  ${SHARED_STYLES}
  body { display: flex; align-items: center; justify-content: center; padding: 1rem; }
  .card { max-width: 520px; }
  .success-heading { font-family: 'Bebas Neue', sans-serif; font-size: 2rem; color: #22C55E; text-align: center; letter-spacing: 0.06em; margin-bottom: 0.5rem; }
  .confirm-text { color: #999; text-align: center; margin-bottom: 1.5rem; font-size: 0.95rem; }
  .confirm-text strong { color: #F0F0F0; }
  .spam-warning { background: #1A1700; border: 1px solid #422006; border-radius: 8px; padding: 0.75rem 1rem; margin-bottom: 1.75rem; font-size: 0.85rem; color: #F59E0B; text-align: center; }
  .subs-heading { font-family: 'Bebas Neue', sans-serif; font-size: 1.15rem; color: #777; letter-spacing: 0.06em; margin-bottom: 0.5rem; }
  .cta { text-align: center; margin-top: 1.5rem; }
  .cta a { color: #777; font-size: 0.88rem; }
  .cta a:hover { color: #F0F0F0; }
</style>
</head><body><div class="card animate-in">
  <div style="text-align:center;margin-bottom:1.25rem;"><span class="brand"><span class="brand-lift">LIFT</span><span class="brand-alert">ALERT</span></span></div>
  <div class="success-heading">SUBSCRIBED!</div>
  <p class="confirm-text">You'll get alerts for ${namesList}: <span style="font-size:0.85rem;color:#999;">${escHtml(prefsList)}</span>.<br><span style="font-size:0.85rem;color:#777;">You'll also be auto-subscribed when they compete in future meets.</span></p>
  <div class="spam-warning">Check your spam/junk folder and mark our emails as &ldquo;Not Spam&rdquo; to make sure you get alerts on time.</div>
  <div class="subs-heading">YOUR SUBSCRIPTIONS</div>
  <table><thead><tr><th>Lifter</th><th>Meet</th><th></th></tr></thead><tbody>${subsRows}</tbody></table>
  <div class="cta"><a href="/">&larr; Subscribe to more lifters</a></div>
</div></body></html>`;
}

function unsubHTML(success) {
  const msg = success ? 'You have been unsubscribed. You will no longer be auto-subscribed to this lifter in future meets.' : 'Subscription not found (may already be removed).';
  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Unsubscribed - LiftAlert</title>
${FONT_LINKS}
<style>
  ${SHARED_STYLES}
  body { display: flex; align-items: center; justify-content: center; padding: 1rem; }
  .unsub-msg { color: #999; text-align: center; margin-top: 1.25rem; font-size: 0.95rem; }
  .back-link { display: block; text-align: center; margin-top: 1.5rem; color: #777; font-size: 0.88rem; }
  .back-link:hover { color: #F0F0F0; }
</style>
</head><body><div class="card animate-in">
  <span class="brand"><span class="brand-lift">LIFT</span><span class="brand-alert">ALERT</span></span>
  <p class="unsub-msg">${msg}</p>
  <a href="/" class="back-link">&larr; Back to LiftAlert</a>
</div></body></html>`;
}

function formatMeetDate(dateStr, dateFormat) {
  if (!dateStr) return '';
  const parts = dateStr.split('/');
  if (parts.length !== 3) return dateStr;
  const fmt = dateFormat || 'MM/DD/YYYY';
  let y, mo, d;
  if (fmt === 'DD/MM/YYYY') { [d, mo, y] = parts; }
  else { [mo, d, y] = parts; }
  const date = new Date(Number(y), Number(mo) - 1, Number(d));
  if (isNaN(date.getTime())) return dateStr;
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  return `${days[date.getDay()]}, ${months[date.getMonth()]} ${date.getDate()}, ${date.getFullYear()}`;
}

function meetsHTML(meetList, subscribedMeetIds) {
  const subSet = new Set(subscribedMeetIds || []);

  // Partition into 3 groups: subscribed, live (not subscribed), rest
  const subscribed = meetList.filter(m => subSet.has(m.id));
  const live = meetList.filter(m => !subSet.has(m.id) && m.isLive);
  const rest = meetList.filter(m => !subSet.has(m.id) && !m.isLive);


  // Sort CANPL meets to top within each group
  const canplFirst = (a, b) => (/canpl/i.test(b.name) ? 1 : 0) - (/canpl/i.test(a.name) ? 1 : 0);
  subscribed.sort(canplFirst);
  live.sort(canplFirst);
  rest.sort(canplFirst);

  function renderCard(m) {
    const isLive = m.isLive;
    const isSub = subSet.has(m.id);
    const borderColor = isLive ? '#22C55E' : isSub ? '#DC2626' : '#1F1F1F';
    const accentColor = isLive ? '#22C55E' : isSub ? '#DC2626' : '#333';
    const badge = isLive
      ? '<span class="badge badge-live"><span class="pulse-dot"></span>Live</span>'
      : '';
    const subBadge = isSub
      ? '<span class="badge badge-sub">Subscribed</span>'
      : '';
    const formattedDate = formatMeetDate(m.date, m.dateFormat);
    const locationStr = m.location ? escHtml(m.location) : '';
    const meta = [formattedDate ? escHtml(formattedDate) : '', locationStr, `${m.lifterCount} lifters`].filter(Boolean).join(' &middot; ');

    // Current status line for live meets
    let statusLine = '';
    if (isLive && m.currentLift) {
      statusLine = `<div class="meet-status"><span style="color:#F0F0F0;font-weight:500;">${escHtml(m.currentLift.lifter)}</span> <span style="color:#666;">&mdash; ${escHtml(m.currentLift.liftName || '')} attempt ${escHtml(String(m.currentLift.attemptNumber || ''))}</span></div>`;
    }

    const liveLink = isLive
      ? `<div style="margin-top:0.5rem;"><a href="/live/${escHtml(m.id)}" class="watch-live-link" onclick="event.stopPropagation();" style="color:#22C55E;font-size:0.82rem;font-weight:600;">WATCH LIVE &rarr;</a></div>`
      : '';

    return `<div class="meet-card" style="border-color:${borderColor};cursor:pointer;" onclick="window.location='/meets/${escHtml(m.id)}'" data-name="${escHtml(m.name.toLowerCase())}" data-lifters="${escHtml((m.lifterNames || []).join('|').toLowerCase())}" data-lifters-display="${escHtml((m.lifterNames || []).join('|'))}">
      <div style="position:absolute;top:0;left:0;bottom:0;width:3px;background:${accentColor};"></div>
      <div class="meet-card-header">
        <h2 class="meet-card-title">${escHtml(m.name)}</h2>
        <div class="meet-card-badges">${badge}${subBadge}</div>
      </div>
      <p class="meet-card-meta">${meta}</p>
      ${statusLine}
      ${liveLink}
      <div class="matched-lifters"></div>
    </div>`;
  }

  const subscribedSection = subscribed.length > 0
    ? `<div class="section-label section-group">YOUR MEETS</div>${subscribed.map(renderCard).join('')}`
    : '';
  const liveSection = live.length > 0
    ? `<div class="section-label section-group" style="margin-top:1.5rem;">LIVE NOW</div>${live.map(renderCard).join('')}`
    : '';
  const restSection = rest.length > 0
    ? `<div class="section-label section-group" style="margin-top:1.5rem;">ALL MEETS</div>${rest.map(renderCard).join('')}`
    : '';
  const empty = meetList.length === 0
    ? '<p style="color:#555;text-align:center;margin:2rem 0;">No meets currently indexed.</p>'
    : '';

  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>All Meets - LiftAlert</title>
${FONT_LINKS}
<style>
  ${SHARED_STYLES}
  body { padding: 1.5rem 1rem; }
  .container { max-width: 600px; margin: 0 auto; }
  .nav { margin-bottom: 1.5rem; font-size: 0.85rem; }
  .nav a { color: #777; }
  .nav a:hover { color: #F0F0F0; }
  .page-heading { font-family: 'Bebas Neue', sans-serif; font-size: 1.75rem; letter-spacing: 0.06em; margin-bottom: 0.25rem; }
  .section-label { font-size: 0.7rem; color: #666; text-transform: uppercase; letter-spacing: 0.12em; font-weight: 600; margin-bottom: 0.6rem; }
  .search-box { width: 100%; padding: 0.6rem 0.85rem; border-radius: 8px; border: 1px solid #252525; background: #0D0D0D; color: #F0F0F0; font-size: 0.85rem; font-family: 'Outfit', sans-serif; margin-bottom: 1.25rem; box-sizing: border-box; }
  .search-box:focus { outline: none; border-color: #DC2626; box-shadow: 0 0 0 3px rgba(220,38,38,0.1); }
  .meet-card {
    display: block; text-decoration: none; color: inherit;
    background: #141414; border: 1px solid #1F1F1F; border-radius: 12px;
    padding: 1rem 1.25rem; margin-bottom: 0.75rem; position: relative; overflow: hidden;
    transition: border-color 0.2s, background 0.2s; cursor: pointer;
  }
  .meet-card:hover { background: #1A1A1A; border-color: #333; }
  .meet-card-header { display: flex; align-items: center; justify-content: space-between; gap: 0.5rem; margin-bottom: 0.2rem; }
  .meet-card-title { font-size: 1.05rem; margin: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .meet-card-badges { display: flex; gap: 0.4rem; flex-shrink: 0; }
  .badge { font-size: 0.68rem; padding: 0.12rem 0.5rem; border-radius: 99px; display: inline-flex; align-items: center; gap: 0.25rem; white-space: nowrap; }
  .badge-live { color: #22C55E; border: 1px solid #166534; }
  .badge-sub { color: #DC2626; border: 1px solid #7F1D1D; }
  .badge .pulse-dot { width: 5px; height: 5px; border-radius: 50%; background: #22C55E; animation: pulse 2s ease-in-out infinite; display: inline-block; }
  .meet-card-meta { font-size: 0.82rem; color: #666; margin: 0; }
  .meet-status { font-size: 0.82rem; margin-top: 0.4rem; }
  .matched-lifters { font-size: 0.8rem; color: #DC2626; margin-top: 0.5rem; line-height: 1.5; }
  .matched-lifters:empty { display: none; }
  .matched-lifters span { display: inline-block; background: #1A0A0A; border: 1px solid #3B1111; border-radius: 4px; padding: 0.1rem 0.4rem; margin: 0.15rem 0.2rem 0.15rem 0; font-size: 0.75rem; }
</style>
</head><body><div class="container animate-in">
  <div class="nav"><a href="/">&larr; Back to <span class="brand" style="font-size:1rem;"><span class="brand-lift">LIFT</span><span class="brand-alert">ALERT</span></span></a></div>
  <div class="page-heading">ALL MEETS</div>
  <p class="subtitle" style="margin-bottom:1.25rem;">${meetList.length} meet${meetList.length !== 1 ? 's' : ''} currently indexed</p>
  <input type="text" class="search-box" placeholder="Search lifters or meets..." oninput="searchMeets(this.value)">
  ${subscribedSection}${liveSection}${restSection}${empty}
</div>
<script>
// Auto-redirect with email from localStorage if not already in URL
(function() {
  try {
    var email = new URLSearchParams(window.location.search).get('email');
    if (!email) {
      var saved = localStorage.getItem('liftalert_email');
      if (saved) {
        window.location.replace('/meets?email=' + encodeURIComponent(saved));
        return;
      }
    }
  } catch(e) {}
})();
function esc(s) { return s.replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":"&#39;"}[c])); }
function searchMeets(q) {
  const cards = document.querySelectorAll('.meet-card');
  const labels = document.querySelectorAll('.section-group');
  const lower = (q || '').trim().toLowerCase();
  cards.forEach(c => {
    const ml = c.querySelector('.matched-lifters');
    if (!lower || lower.length < 2) { c.style.display = ''; ml.innerHTML = ''; return; }
    const name = c.dataset.name || '';
    const nameMatch = name.includes(lower);
    const lifterList = (c.dataset.liftersDisplay || '').split('|').filter(Boolean);
    const matched = lifterList.filter(n => n.toLowerCase().includes(lower));
    if (nameMatch || matched.length > 0) {
      c.style.display = '';
      ml.innerHTML = matched.length > 0 ? matched.slice(0, 8).map(n => '<span>' + esc(n) + '</span>').join('') + (matched.length > 8 ? '<span style="color:#666;">+' + (matched.length - 8) + ' more</span>' : '') : '';
    } else {
      c.style.display = 'none';
      ml.innerHTML = '';
    }
  });
  labels.forEach(l => {
    let next = l.nextElementSibling;
    let anyVisible = false;
    while (next && !next.classList.contains('section-group')) {
      if (next.classList.contains('meet-card') && next.style.display !== 'none') anyVisible = true;
      next = next.nextElementSibling;
    }
    l.style.display = anyVisible ? '' : 'none';
  });
}
</script>
</body></html>`;
}

// --- Meet detail page (scoresheet table like recaps) ---
function meetDetailHTML(meetId, meetState, subscribedLifterNames, videoData, timestamps) {
  const subNameSet = new Set((subscribedLifterNames || []).map(n => n.toLowerCase()));
  const meetDoc = meetState.meet || {};
  const meetName = meetDoc.name || meetId;
  const dateStr = meetDoc.date || '';
  const locationStr = meetDoc.location || meetDoc.city || '';
  const meta = [dateStr, locationStr].filter(Boolean).join(' &middot; ');

  // Build VOD timestamp lookup: lifterId+liftName → best (highest weight) timestamp link
  const LIFT_KEY_MAP = { squat: 'sq', bench: 'bp', dead: 'dl', deadlift: 'dl' };
  const vodLinks = {}; // key: `${lifterName.toLowerCase()}:${sq|bp|dl}` → YouTube URL
  if (videoData && timestamps && timestamps.length > 0) {
    const streamStart = Number(videoData.stream_start_epoch);
    const videoId = videoData.youtube_video_id;
    // Group timestamps by lifter+lift, pick the highest weight attempt
    const bestByLifterLift = {};
    for (const t of timestamps) {
      const liftKey = LIFT_KEY_MAP[t.lift_name] || t.lift_name;
      const key = `${t.lifter_name.toLowerCase()}:${liftKey}`;
      const w = Number(t.weight) || 0;
      if (!bestByLifterLift[key] || w > bestByLifterLift[key].weight) {
        bestByLifterLift[key] = { weight: w, wallEpoch: Math.floor(new Date(t.wall_clock_time).getTime() / 1000) };
      }
    }
    for (const [key, val] of Object.entries(bestByLifterLift)) {
      const offset = Math.max(0, val.wallEpoch - streamStart - TIMESTAMP_LEAD_SECONDS);
      vodLinks[key] = `https://www.youtube.com/watch?v=${videoId}&t=${offset}`;
    }
  }

  // Build lifter rows with best lifts
  const lifters = Object.values(meetState.lifters).filter(l => l.name).map(l => {
    const bests = computeLifterBests(meetState, l._id);
    const total = bests.squat + bests.bench + bests.dead;
    const nameLower = (l.name || '').toLowerCase();
    return {
      id: l._id,
      name: l.name,
      bodyWeight: l.bodyWeight || null,
      weightClass: getWeightClass(l.bodyWeight, l.gender, l.declaredWeightClass),
      flight: l.flight || '',
      session: l.session || 1,
      team: l.team || '',
      bestSq: bests.squat || null,
      bestBp: bests.bench || null,
      bestDl: bests.dead || null,
      total: total || null,
      isSubscribed: subNameSet.has(nameLower),
      vodSq: vodLinks[`${nameLower}:sq`] || null,
      vodBp: vodLinks[`${nameLower}:bp`] || null,
      vodDl: vodLinks[`${nameLower}:dl`] || null,
    };
  });

  // Sort by weight class asc, then body weight asc, then name
  lifters.sort((a, b) => {
    const wcA = typeof a.weightClass === 'number' ? a.weightClass : 9999;
    const wcB = typeof b.weightClass === 'number' ? b.weightClass : 9999;
    if (wcA !== wcB) return wcA - wcB;
    const bwA = a.bodyWeight || 9999;
    const bwB = b.bodyWeight || 9999;
    if (bwA !== bwB) return bwA - bwB;
    return a.name.localeCompare(b.name);
  });

  // Platform status summary
  const platformStatuses = Object.entries(meetState.platforms).map(([pid, platform]) => {
    const parsed = parseAttemptId(platform.currentAttemptId);
    const currentLifter = parsed ? meetState.lifters[parsed.lifterId] : null;
    const order = computeAttemptOrder(meetState, pid);
    const currentIdx = platform.currentAttemptId ? order.findIndex(a => a.attemptId === platform.currentAttemptId) : -1;
    const nextUp = currentIdx >= 0 ? order.slice(currentIdx + 1, currentIdx + 3) : [];
    return {
      name: platform.name || pid,
      currentLifter: currentLifter?.name || null,
      liftName: parsed?.liftName || null,
      attemptNumber: parsed?.attemptNumber || null,
      nextUp: nextUp.map(a => ({ name: a.lifterName, lift: a.liftName, attempt: a.attemptNumber })),
    };
  });

  const platformHTML = platformStatuses.map(p => {
    if (!p.currentLifter) return '';
    const liftLabel = { squat: 'SQ', bench: 'BP', dead: 'DL', deadlift: 'DL' }[p.liftName] || p.liftName || '';
    const nextLabels = p.nextUp.map((n, i) => {
      const label = i === 0 ? 'On deck' : 'In hole';
      const nLift = { squat: 'SQ', bench: 'BP', dead: 'DL', deadlift: 'DL' }[n.lift] || '';
      return `<span style="color:#777;">${label}: <span style="color:#999;">${escHtml(n.name)}</span> <span style="color:#555;">${nLift}${n.attempt}</span></span>`;
    }).join('<br>');
    return `<div class="platform-status">
      <div style="font-size:0.68rem;color:#666;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:0.2rem;">${escHtml(p.name)}</div>
      <div><strong style="color:#F0F0F0;">${escHtml(p.currentLifter)}</strong> <span style="color:#666;">&mdash; ${liftLabel} attempt ${p.attemptNumber || ''}</span></div>
      ${nextLabels ? `<div style="margin-top:0.25rem;font-size:0.82rem;">${nextLabels}</div>` : ''}
    </div>`;
  }).filter(Boolean).join('');

  // Detect which lift columns have data
  const hasSq = lifters.some(l => l.bestSq);
  const hasBp = lifters.some(l => l.bestBp);
  const hasDl = lifters.some(l => l.bestDl);
  const hasTotal = lifters.some(l => l.total);

  const colDefs = [
    ...(hasSq ? [{ key: 'bestSq', label: 'SQ', group: 'SQUAT', vodKey: 'vodSq' }] : []),
    ...(hasBp ? [{ key: 'bestBp', label: 'BP', group: 'BENCH', vodKey: 'vodBp' }] : []),
    ...(hasDl ? [{ key: 'bestDl', label: 'DL', group: 'DEADLIFT', vodKey: 'vodDl' }] : []),
    ...(hasTotal ? [{ key: 'total', label: 'TOT', group: null, vodKey: null }] : []),
  ];

  const headerRow = colDefs.map(c =>
    `<th style="text-align:center;min-width:52px;padding:0.35rem 0.3rem;font-size:0.65rem;">${c.label}</th>`
  ).join('');

  // Build body rows with weight class separators
  const totalCols = colDefs.length + 1; // +1 for lifter name col
  let lastWc = null;
  const bodyRows = lifters.map(l => {
    let separator = '';
    if (l.weightClass !== lastWc) {
      lastWc = l.weightClass;
      const wcLabel = l.weightClass ? (typeof l.weightClass === 'number' ? `${l.weightClass} kg` : l.weightClass) : 'Unknown';
      separator = `<tr class="wc-separator"><td colspan="${totalCols}" style="padding:0.6rem 0.75rem 0.3rem;font-size:0.7rem;color:#DC2626;font-weight:600;letter-spacing:0.1em;text-transform:uppercase;border-bottom:2px solid #252525;background:#0F0F0F;">${wcLabel}</td></tr>`;
    }
    const bwLabel = l.bodyWeight ? `<span style="color:#555;font-size:0.72rem;font-weight:300;"> ${l.bodyWeight}</span>` : '';
    const subHighlight = l.isSubscribed ? ' style="color:#DC2626;"' : '';
    const cells = colDefs.map(c => {
      const val = l[c.key];
      if (!val) return '<td class="cell empty">&mdash;</td>';
      const isTotalCol = c.key === 'total';
      const vodUrl = c.vodKey ? l[c.vodKey] : null;
      if (vodUrl) {
        return `<td class="cell"><a href="${escHtml(vodUrl)}" target="_blank" style="color:#BBB;text-decoration:underline;text-decoration-color:#333;text-underline-offset:2px;">${val}</a></td>`;
      }
      const style = isTotalCol ? ' style="font-weight:600;color:#F0F0F0;"' : '';
      return `<td class="cell"${style}>${val}</td>`;
    }).join('');
    return `${separator}<tr class="lifter-row" data-name="${escHtml(l.name.toLowerCase())}" data-wc="${l.weightClass || ''}">
      <td class="lifter-name"><span${subHighlight}>${escHtml(l.name)}</span>${bwLabel}</td>
      ${cells}
    </tr>`;
  }).join('');

  const liftingStarted = hasSq || hasBp || hasDl;

  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>${escHtml(meetName)} - LiftAlert</title>
${FONT_LINKS}
<style>
  ${SHARED_STYLES}
  body { padding: 1.5rem 0.75rem; }
  .container { max-width: 700px; margin: 0 auto; }
  .nav { margin-bottom: 1.5rem; font-size: 0.85rem; }
  .nav a { color: #777; }
  .nav a:hover { color: #F0F0F0; }
  .page-heading { font-family: 'Bebas Neue', sans-serif; font-size: 1.75rem; letter-spacing: 0.06em; margin-bottom: 0.25rem; }
  .platform-status { padding: 0.65rem 0.85rem; background: #0D0D0D; border: 1px solid #1F1F1F; border-radius: 8px; margin-bottom: 0.5rem; }
  .filter-input { width: 100%; max-width: 300px; padding: 0.5rem 0.75rem; border-radius: 8px; border: 1px solid #252525; background: #0D0D0D; color: #F0F0F0; font-size: 0.85rem; font-family: 'Outfit', sans-serif; margin-bottom: 1rem; }
  .filter-input:focus { outline: none; border-color: #DC2626; box-shadow: 0 0 0 3px rgba(220,38,38,0.1); }
  .scoresheet { width: 100%; border-collapse: collapse; }
  .scoresheet th { position: sticky; top: 0; background: #0A0A0A; z-index: 2; }
  .scoresheet .lifter-name {
    position: sticky; left: 0; background: #0A0A0A; z-index: 1;
    padding: 0.5rem 0.75rem; white-space: nowrap; font-weight: 500; font-size: 0.85rem;
    border-bottom: 1px solid #1A1A1A; border-right: 2px solid #252525;
    max-width: 180px; overflow: hidden; text-overflow: ellipsis;
  }
  .scoresheet .cell { text-align: center; padding: 0.45rem 0.3rem; border-bottom: 1px solid #1A1A1A; font-size: 0.85rem; color: #BBB; }
  .scoresheet .cell.empty { color: #333; }
  .scoresheet .lifter-row:hover td { background: #141414; }
  .scoresheet .lifter-row:hover .lifter-name { background: #141414; }
  .table-wrap { overflow-x: auto; border: 1px solid #1F1F1F; border-radius: 12px; background: #0A0A0A; }
  .empty-state { text-align: center; padding: 3rem 1rem; color: #555; }
  .empty-state p { margin-bottom: 0.5rem; }
  .subscribe-link { display: inline-block; margin-top: 1rem; padding: 0.6rem 1.5rem; background: #DC2626; color: white; border-radius: 8px; font-family: 'Bebas Neue', sans-serif; font-size: 1rem; letter-spacing: 0.1em; transition: background 0.2s; }
  .subscribe-link:hover { background: #B91C1C; color: white; }
  @media (max-width: 600px) {
    .scoresheet .lifter-name { font-size: 0.75rem; padding: 0.4rem 0.5rem; max-width: 120px; }
    .scoresheet .cell { padding: 0.35rem 0.15rem; font-size: 0.75rem; }
  }
</style>
</head><body><div class="container animate-in">
  <div class="nav"><a href="/meets">&larr; All meets</a></div>
  <div class="page-heading">${escHtml(meetName)}</div>
  ${meta ? `<p class="subtitle" style="margin-bottom:0.5rem;">${meta}</p>` : ''}
  <p style="font-size:0.85rem;color:#555;margin-bottom:1rem;">${lifters.length} lifter${lifters.length !== 1 ? 's' : ''} &middot; ${Object.keys(meetState.platforms).length} platform${Object.keys(meetState.platforms).length !== 1 ? 's' : ''}${watchingMeets.has(meetId) && platformStatuses.some(p => p.currentLifter) ? ` &middot; <a href="/live/${escHtml(meetId)}" style="color:#22C55E;font-weight:600;">WATCH LIVE &rarr;</a>` : ''}</p>
  ${platformHTML ? `<div style="margin-bottom:1rem;">${platformHTML}</div>` : ''}
  ${!liftingStarted ? '<p style="font-size:0.82rem;color:#555;margin-bottom:1rem;">Lifting hasn\'t started yet &mdash; results will appear as attempts are recorded.</p>' : ''}
  <input type="text" class="filter-input" placeholder="Search lifters..." oninput="filterLifters(this.value)">
  <div class="table-wrap">
    <table class="scoresheet">
      <thead>
        <tr><th style="text-align:left;padding:0.35rem 0.75rem;font-size:0.65rem;border-bottom:2px solid #252525;${colDefs.length ? 'border-right:2px solid #252525;' : ''}">LIFTER</th>${headerRow}</tr>
      </thead>
      <tbody>${bodyRows}</tbody>
    </table>
  </div>
  <div style="text-align:center;margin-top:1.5rem;">
    <a href="/" class="subscribe-link">SUBSCRIBE TO A LIFTER</a>
  </div>
</div>
<script>
// Auto-redirect with email from localStorage if not already in URL
(function() {
  try {
    var email = new URLSearchParams(window.location.search).get('email');
    if (!email) {
      var saved = localStorage.getItem('liftalert_email');
      if (saved) {
        window.location.replace(window.location.pathname + '?email=' + encodeURIComponent(saved));
        return;
      }
    }
  } catch(e) {}
})();
function filterLifters(q) {
  const rows = document.querySelectorAll('.lifter-row');
  const seps = document.querySelectorAll('.wc-separator');
  const lower = q.toLowerCase();
  const visibleWcs = new Set();
  rows.forEach(r => {
    const show = r.dataset.name.includes(lower);
    r.style.display = show ? '' : 'none';
    if (show && r.dataset.wc) visibleWcs.add(r.dataset.wc);
  });
  seps.forEach(s => {
    const wcText = s.textContent.trim().replace(' kg','');
    s.style.display = (!lower || visibleWcs.has(wcText)) ? '' : 'none';
  });
}
</script>
</body></html>`;
}

function liveHTML(meetId, meetName) {
  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>${escHtml(meetName)} LIVE - LiftAlert</title>
${FONT_LINKS}
<style>
  ${SHARED_STYLES}
  body { padding: 1.5rem 0.75rem; }
  .container { max-width: 700px; margin: 0 auto; }
  .nav { margin-bottom: 1.5rem; font-size: 0.85rem; }
  .nav a { color: #777; }
  .nav a:hover { color: #F0F0F0; }
  .page-heading { font-family: 'Bebas Neue', sans-serif; font-size: 1.75rem; letter-spacing: 0.06em; margin-bottom: 0.25rem; display: flex; align-items: center; gap: 0.75rem; flex-wrap: wrap; }
  .live-badge { display: inline-flex; align-items: center; gap: 0.35rem; background: #22C55E; color: #fff; font-size: 0.7rem; font-family: 'Outfit', sans-serif; font-weight: 600; padding: 0.2rem 0.6rem; border-radius: 4px; letter-spacing: 0.08em; text-transform: uppercase; }
  .live-badge .pulse-dot { width: 6px; height: 6px; background: #fff; border-radius: 50%; animation: pulse 1.5s infinite; }
  .yt-link { font-size: 0.85rem; font-family: 'Outfit', sans-serif; color: #3b82f6; font-weight: 500; }
  .yt-link:hover { color: #60a5fa; }
  .hero { background: #141414; border: 1px solid #1F1F1F; border-radius: 12px; padding: 1.25rem; margin-bottom: 1rem; text-align: center; }
  .hero-label { font-size: 0.68rem; color: #666; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 0.3rem; }
  .hero-name { font-family: 'Bebas Neue', sans-serif; font-size: 2rem; letter-spacing: 0.04em; color: #F0F0F0; }
  .hero-detail { font-size: 0.9rem; color: #777; margin-top: 0.15rem; }
  .hero-weight { font-size: 1.5rem; font-weight: 700; color: #DC2626; margin-top: 0.25rem; }
  .queue-section { margin-bottom: 1.25rem; }
  .queue-label { font-size: 0.68rem; color: #666; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 0.5rem; font-weight: 600; }
  .queue-item { display: flex; align-items: center; gap: 0.75rem; padding: 0.5rem 0.75rem; background: #0D0D0D; border: 1px solid #1A1A1A; border-radius: 8px; margin-bottom: 0.35rem; font-size: 0.88rem; }
  .queue-pos { color: #555; font-size: 0.75rem; font-weight: 600; min-width: 1.2rem; }
  .queue-name { color: #CCC; font-weight: 500; }
  .queue-name a { color: #CCC; }
  .queue-name a:hover { color: #DC2626; }
  .queue-detail { color: #555; font-size: 0.82rem; margin-left: auto; white-space: nowrap; }
  .scoresheet { width: 100%; border-collapse: collapse; }
  .scoresheet th { position: sticky; top: 0; background: #0A0A0A; z-index: 2; }
  .scoresheet .lifter-name {
    position: sticky; left: 0; background: #0A0A0A; z-index: 1;
    padding: 0.5rem 0.75rem; white-space: nowrap; font-weight: 500; font-size: 0.85rem;
    border-bottom: 1px solid #1A1A1A; border-right: 2px solid #252525;
    max-width: 180px; overflow: hidden; text-overflow: ellipsis;
  }
  .scoresheet .lifter-name a { color: inherit; }
  .scoresheet .lifter-name a:hover { color: #DC2626; }
  .scoresheet .cell { text-align: center; padding: 0.45rem 0.3rem; border-bottom: 1px solid #1A1A1A; font-size: 0.85rem; color: #BBB; }
  .scoresheet .cell.empty { color: #333; }
  .scoresheet .lifter-row:hover td { background: #141414; }
  .scoresheet .lifter-row:hover .lifter-name { background: #141414; }
  .table-wrap { overflow-x: auto; border: 1px solid #1F1F1F; border-radius: 12px; background: #0A0A0A; }
  .filter-input { width: 100%; max-width: 300px; padding: 0.5rem 0.75rem; border-radius: 8px; border: 1px solid #252525; background: #0D0D0D; color: #F0F0F0; font-size: 0.85rem; font-family: 'Outfit', sans-serif; margin-bottom: 1rem; }
  .filter-input:focus { outline: none; border-color: #DC2626; box-shadow: 0 0 0 3px rgba(220,38,38,0.1); }
  .updated-ago { font-size: 0.75rem; color: #444; margin-top: 0.5rem; text-align: center; }
  .empty-state { text-align: center; padding: 2rem 1rem; color: #555; }
  @media (max-width: 600px) {
    .scoresheet .lifter-name { font-size: 0.75rem; padding: 0.4rem 0.5rem; max-width: 120px; }
    .scoresheet .cell { padding: 0.35rem 0.15rem; font-size: 0.75rem; }
    .hero-name { font-size: 1.5rem; }
  }
</style>
</head><body><div class="container animate-in">
  <div class="nav"><a href="/meets/${escHtml(meetId)}">&larr; Meet details</a></div>
  <div class="page-heading">${escHtml(meetName)} <span class="live-badge"><span class="pulse-dot"></span>LIVE</span></div>
  <div id="yt-link-wrap"></div>
  <div id="hero" class="hero"><div class="empty-state">Loading...</div></div>
  <div id="queue" class="queue-section"></div>
  <div style="font-size:0.72rem;color:#666;text-transform:uppercase;letter-spacing:0.1em;margin-bottom:0.5rem;font-weight:600;">SCORESHEET</div>
  <input type="text" class="filter-input" placeholder="Search lifters..." oninput="filterLifters(this.value)">
  <div id="scoresheet"></div>
  <div id="updated" class="updated-ago"></div>
  <div style="text-align:center;margin-top:1.5rem;">
    <a href="/" style="display:inline-block;padding:0.6rem 1.5rem;background:#DC2626;color:white;border-radius:8px;font-family:'Bebas Neue',sans-serif;font-size:1rem;letter-spacing:0.1em;transition:background 0.2s;">SUBSCRIBE TO A LIFTER</a>
  </div>
</div>
<script>
const MEET_ID = '${escHtml(meetId)}';
const LIFT_LABEL = { squat: 'SQ', bench: 'BP', dead: 'DL', deadlift: 'DL' };
let lastData = null;

function opLink(name) {
  const slug = name.toLowerCase().replace(/[^a-z]/g, '');
  return 'https://www.openpowerlifting.org/u/' + slug;
}

function renderHero(data) {
  const el = document.getElementById('hero');
  if (!data.platforms || data.platforms.length === 0) {
    el.innerHTML = '<div class="empty-state">Waiting for meet to start...</div>';
    return;
  }
  const parts = data.platforms.map(p => {
    if (!p.current) return '';
    const label = LIFT_LABEL[p.current.liftName] || p.current.liftName || '';
    return '<div class="hero-label">' + (p.name || 'Platform') + ' &mdash; NOW LIFTING</div>' +
      '<div class="hero-name"><a href="' + opLink(p.current.lifterName) + '" target="_blank" style="color:inherit;text-decoration:none;">' + esc(p.current.lifterName) + '</a></div>' +
      '<div class="hero-detail">' + label + ' attempt ' + (p.current.attemptNumber || '') + '</div>' +
      (p.current.weight ? '<div class="hero-weight">' + p.current.weight + ' kg</div>' : '');
  }).filter(Boolean);
  el.innerHTML = parts.length ? parts.join('<hr style="border:none;border-top:1px solid #252525;margin:0.75rem 0;">') : '<div class="empty-state">No active lifter</div>';
}

function renderQueue(data) {
  const el = document.getElementById('queue');
  if (!data.platforms) { el.innerHTML = ''; return; }
  const parts = data.platforms.map(p => {
    if (!p.queue || p.queue.length === 0) return '';
    const items = p.queue.slice(0, 8).map((q, i) => {
      const label = LIFT_LABEL[q.liftName] || '';
      const posLabels = ['ON DECK', 'IN HOLE'];
      const posLabel = i < 2 ? '<span style="color:' + (i === 0 ? '#22C55E' : '#EAB308') + ';font-size:0.68rem;font-weight:600;">' + posLabels[i] + '</span> ' : '';
      return '<div class="queue-item">' +
        '<span class="queue-pos">' + (i + 1) + '</span>' +
        '<span class="queue-name">' + posLabel + '<a href="' + opLink(q.lifterName) + '" target="_blank">' + esc(q.lifterName) + '</a></span>' +
        '<span class="queue-detail">' + label + q.attemptNumber + ' &middot; ' + (q.weight || '?') + 'kg</span>' +
        '</div>';
    }).join('');
    const title = data.platforms.length > 1 ? '<div class="queue-label">UP NEXT &mdash; ' + esc(p.name || 'Platform') + '</div>' : '<div class="queue-label">UP NEXT</div>';
    return title + items;
  }).filter(Boolean);
  el.innerHTML = parts.join('');
}

function renderScoresheet(data) {
  const el = document.getElementById('scoresheet');
  if (!data.lifters || data.lifters.length === 0) {
    el.innerHTML = '<div class="empty-state">No lifter data yet</div>';
    return;
  }
  const lifters = data.lifters;
  const hasSq = lifters.some(l => l.bestSq);
  const hasBp = lifters.some(l => l.bestBp);
  const hasDl = lifters.some(l => l.bestDl);
  const hasTotal = lifters.some(l => l.total);
  const cols = [];
  if (hasSq) cols.push({ key: 'bestSq', label: 'SQ' });
  if (hasBp) cols.push({ key: 'bestBp', label: 'BP' });
  if (hasDl) cols.push({ key: 'bestDl', label: 'DL' });
  if (hasTotal) cols.push({ key: 'total', label: 'TOT' });
  if (cols.length === 0) {
    el.innerHTML = '<div class="empty-state">Lifting hasn\\'t started yet &mdash; results will appear as attempts are recorded.</div>';
    return;
  }
  const totalCols = cols.length + 1;
  const hdr = cols.map(c => '<th style="text-align:center;min-width:52px;padding:0.35rem 0.3rem;font-size:0.65rem;">' + c.label + '</th>').join('');
  let lastWc = null;
  const rows = lifters.map(l => {
    let sep = '';
    if (l.weightClass !== lastWc) {
      lastWc = l.weightClass;
      const wcLabel = l.weightClass ? (typeof l.weightClass === 'number' ? l.weightClass + ' kg' : l.weightClass) : 'Unknown';
      sep = '<tr class="wc-separator"><td colspan="' + totalCols + '" style="padding:0.6rem 0.75rem 0.3rem;font-size:0.7rem;color:#DC2626;font-weight:600;letter-spacing:0.1em;text-transform:uppercase;border-bottom:2px solid #252525;background:#0F0F0F;">' + wcLabel + '</td></tr>';
    }
    const bwLabel = l.bodyWeight ? ' <span style="color:#555;font-size:0.72rem;font-weight:300;">' + l.bodyWeight + '</span>' : '';
    const cells = cols.map(c => {
      const val = l[c.key];
      if (!val) return '<td class="cell empty">&mdash;</td>';
      const isTot = c.key === 'total';
      return '<td class="cell"' + (isTot ? ' style="font-weight:600;color:#F0F0F0;"' : '') + '>' + val + '</td>';
    }).join('');
    return sep + '<tr class="lifter-row" data-name="' + esc(l.name.toLowerCase()) + '" data-wc="' + (l.weightClass || '') + '">' +
      '<td class="lifter-name"><a href="' + opLink(l.name) + '" target="_blank">' + esc(l.name) + '</a>' + bwLabel + '</td>' +
      cells + '</tr>';
  }).join('');
  el.innerHTML = '<div class="table-wrap"><table class="scoresheet"><thead><tr>' +
    '<th style="text-align:left;padding:0.35rem 0.75rem;font-size:0.65rem;border-bottom:2px solid #252525;border-right:2px solid #252525;">LIFTER</th>' + hdr +
    '</tr></thead><tbody>' + rows + '</tbody></table></div>';
}

function renderYT(data) {
  const el = document.getElementById('yt-link-wrap');
  if (data.video && data.video.youtubeVideoId) {
    el.innerHTML = '<p style="margin-bottom:1rem;"><a class="yt-link" href="https://www.youtube.com/watch?v=' + esc(data.video.youtubeVideoId) + '" target="_blank">&#x25B6; Watch on YouTube</a></p>';
  } else {
    el.innerHTML = '';
  }
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function filterLifters(q) {
  const rows = document.querySelectorAll('.lifter-row');
  const seps = document.querySelectorAll('.wc-separator');
  const lower = q.toLowerCase();
  const visibleWcs = new Set();
  rows.forEach(r => {
    const show = r.dataset.name.includes(lower);
    r.style.display = show ? '' : 'none';
    if (show && r.dataset.wc) visibleWcs.add(String(r.dataset.wc));
  });
  seps.forEach(s => {
    const wcText = s.textContent.trim().replace(' kg','');
    s.style.display = (!lower || visibleWcs.has(wcText)) ? '' : 'none';
  });
}

async function poll() {
  try {
    const res = await fetch('/api/meet/' + MEET_ID + '/live');
    if (!res.ok) return;
    const data = await res.json();
    lastData = data;
    renderHero(data);
    renderQueue(data);
    renderScoresheet(data);
    renderYT(data);
    document.getElementById('updated').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch (e) { /* retry next cycle */ }
}

poll();
setInterval(poll, 5000);
</script>
</body></html>`;
}

function recapHTML(meetId, meetName, videoId, streamStart, timestamps, meetDate) {
  const COLS = ['sq1','sq2','sq3','bp1','bp2','bp3','dl1','dl2','dl3'];
  const LIFT_KEY = { squat: 'sq', bench: 'bp', dead: 'dl', deadlift: 'dl' };

  function attemptKey(a) {
    const prefix = LIFT_KEY[a.lift_name] || a.lift_name.slice(0, 2);
    return prefix + a.attempt_number;
  }
  function fmtOffset(secs) {
    const h = Math.floor(secs / 3600);
    const m = Math.floor((secs % 3600) / 60);
    const s = Math.floor(secs % 60);
    return h > 0 ? `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}` : `${m}:${String(s).padStart(2, '0')}`;
  }

  // Group by lifter, build cell map
  const hasVod = videoId && streamStart;
  const byLifter = {};
  for (const t of timestamps) {
    if (!byLifter[t.lifter_id]) byLifter[t.lifter_id] = { name: t.lifter_name, cells: {}, earliest: Infinity, bodyWeight: null };
    const wallEpoch = Math.floor(new Date(t.wall_clock_time).getTime() / 1000);
    const key = attemptKey(t);
    let link = null;
    let timeStr = '';
    if (hasVod) {
      const offset = Math.max(0, wallEpoch - streamStart - TIMESTAMP_LEAD_SECONDS);
      timeStr = fmtOffset(offset);
      link = `https://www.youtube.com/watch?v=${videoId}&t=${offset}`;
    }
    byLifter[t.lifter_id].cells[key] = { weight: t.weight, link, timeStr };
    if (wallEpoch < byLifter[t.lifter_id].earliest) byLifter[t.lifter_id].earliest = wallEpoch;
    if (t.body_weight && !byLifter[t.lifter_id].bodyWeight) byLifter[t.lifter_id].bodyWeight = Number(t.body_weight);
  }

  // Sort by weight class (asc), then body weight (asc), then name
  const lifters = Object.values(byLifter).sort((a, b) => {
    const wcA = a.bodyWeight || 9999;
    const wcB = b.bodyWeight || 9999;
    if (wcA !== wcB) return wcA - wcB;
    return a.name.localeCompare(b.name);
  });

  // Detect which column groups are present
  const hasSq = lifters.some(l => COLS.slice(0, 3).some(c => l.cells[c]));
  const hasBp = lifters.some(l => COLS.slice(3, 6).some(c => l.cells[c]));
  const hasDl = lifters.some(l => COLS.slice(6, 9).some(c => l.cells[c]));
  const activeCols = [
    ...(hasSq ? ['sq1','sq2','sq3'] : []),
    ...(hasBp ? ['bp1','bp2','bp3'] : []),
    ...(hasDl ? ['dl1','dl2','dl3'] : []),
  ];
  const colLabels = { sq1:'SQ1',sq2:'SQ2',sq3:'SQ3',bp1:'BP1',bp2:'BP2',bp3:'BP3',dl1:'DL1',dl2:'DL2',dl3:'DL3' };

  // Build header with lift group spans
  const groups = [];
  if (hasSq) groups.push({ label: 'SQUAT', cols: 3 });
  if (hasBp) groups.push({ label: 'BENCH', cols: 3 });
  if (hasDl) groups.push({ label: 'DEADLIFT', cols: 3 });
  const groupRow = groups.map(g =>
    `<th colspan="${g.cols}" style="text-align:center;padding:0.4rem 0;color:#DC2626;font-size:0.65rem;letter-spacing:0.12em;border-bottom:1px solid #252525;">${g.label}</th>`
  ).join('');
  const subHeaderRow = activeCols.map(c =>
    `<th style="text-align:center;min-width:48px;padding:0.35rem 0.2rem;font-size:0.65rem;">${colLabels[c]}</th>`
  ).join('');

  // Build body rows with weight class separators
  const totalCols = activeCols.length + 1;
  let lastWc = null;
  const bodyRows = lifters.map((l, i) => {
    let separator = '';
    const wc = getWeightClass(l.bodyWeight, null, null);
    if (wc !== lastWc) {
      lastWc = wc;
      const wcLabel = wc ? `${wc} kg` : 'Unknown';
      separator = `<tr class="wc-separator"><td colspan="${totalCols}" style="padding:0.6rem 0.75rem 0.3rem;font-size:0.7rem;color:#DC2626;font-weight:600;letter-spacing:0.1em;text-transform:uppercase;border-bottom:2px solid #252525;background:#0F0F0F;">${wcLabel}</td></tr>`;
    }
    const bwLabel = l.bodyWeight ? `<span style="color:#555;font-size:0.72rem;font-weight:300;"> ${l.bodyWeight}</span>` : '';
    const cells = activeCols.map(c => {
      const cell = l.cells[c];
      if (!cell) return '<td class="cell empty">&mdash;</td>';
      const w = cell.weight ? cell.weight : '?';
      if (cell.link) {
        return `<td class="cell"><a href="${escHtml(cell.link)}" target="_blank" title="${cell.timeStr}">${w}</a></td>`;
      }
      return `<td class="cell">${w}</td>`;
    }).join('');
    return `${separator}<tr class="lifter-row" data-name="${escHtml(l.name.toLowerCase())}" data-wc="${wc || ''}">
      <td class="lifter-name">${escHtml(l.name)}${bwLabel}</td>
      ${cells}
    </tr>`;
  }).join('');

  const videoLink = videoId ? `<p style="margin-bottom:1.25rem;"><a href="https://www.youtube.com/watch?v=${escHtml(videoId)}" target="_blank" style="color:#3b82f6;font-weight:600;">Full VOD on YouTube &#x25B6;</a></p>` : '';

  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Meet Recap - LiftAlert</title>
${FONT_LINKS}
<style>
  ${SHARED_STYLES}
  body { padding: 1.5rem 0.75rem; }
  .container { max-width: 900px; margin: 0 auto; }
  .nav { margin-bottom: 1.5rem; font-size: 0.85rem; }
  .nav a { color: #777; }
  .nav a:hover { color: #F0F0F0; }
  .page-heading { font-family: 'Bebas Neue', sans-serif; font-size: 1.75rem; letter-spacing: 0.06em; margin-bottom: 0.25rem; }
  .filter-input { width: 100%; max-width: 300px; padding: 0.5rem 0.75rem; border-radius: 8px; border: 1px solid #252525; background: #0D0D0D; color: #F0F0F0; font-size: 0.85rem; font-family: 'Outfit', sans-serif; margin-bottom: 1rem; }
  .filter-input:focus { outline: none; border-color: #DC2626; box-shadow: 0 0 0 3px rgba(220,38,38,0.1); }
  .scoresheet { width: 100%; border-collapse: collapse; }
  .scoresheet th { position: sticky; top: 0; background: #0A0A0A; z-index: 2; }
  .scoresheet .lifter-name {
    position: sticky; left: 0; background: #0A0A0A; z-index: 1;
    padding: 0.5rem 0.75rem; white-space: nowrap; font-weight: 500; font-size: 0.85rem;
    border-bottom: 1px solid #1A1A1A; border-right: 2px solid #252525;
    max-width: 160px; overflow: hidden; text-overflow: ellipsis;
  }
  .scoresheet .cell { text-align: center; padding: 0.45rem 0.25rem; border-bottom: 1px solid #1A1A1A; font-size: 0.85rem; }
  .scoresheet .cell a {
    color: #3b82f6; text-decoration: none; display: inline-block;
    padding: 0.2rem 0.45rem; border-radius: 4px; transition: all 0.15s;
    font-variant-numeric: tabular-nums; border-bottom: 1px dashed #3b82f6;
  }
  .scoresheet .cell a:hover { background: #DC2626; color: #fff; border-bottom-color: transparent; }
  .scoresheet .cell.empty { color: #333; }
  .scoresheet .lifter-row:hover td { background: #141414; }
  .scoresheet .lifter-row:hover .lifter-name { background: #141414; }
  .table-outer { position: relative; }
  .table-outer::after { content: ''; position: absolute; top: 0; right: 0; bottom: 0; width: 32px; background: linear-gradient(to right, transparent, #0A0A0A); pointer-events: none; border-radius: 0 12px 12px 0; transition: opacity 0.3s; z-index: 1; }
  .table-outer.scrolled-end::after { opacity: 0; }
  .table-wrap { overflow-x: auto; border: 1px solid #1F1F1F; border-radius: 12px; background: #0A0A0A; }
  @media (max-width: 600px) {
    .scoresheet .lifter-name { font-size: 0.75rem; padding: 0.4rem 0.5rem; max-width: 110px; }
    .scoresheet .cell { padding: 0.35rem 0.15rem; font-size: 0.75rem; }
    .scoresheet .cell a { padding: 0.15rem 0.3rem; }
  }
</style>
</head><body><div class="container animate-in">
  <div class="nav"><a href="/">&larr; Back to <span class="brand" style="font-size:1rem;"><span class="brand-lift">LIFT</span><span class="brand-alert">ALERT</span></span></a></div>
  <div class="page-heading">MEET RECAP</div>
  <p class="subtitle" style="margin-bottom:0.5rem;">${escHtml(meetName || meetId)}${meetDate ? ` &mdash; ${escHtml(meetDate)}` : ''}</p>
  <p style="font-size:0.85rem;color:#555;margin-bottom:1rem;">${lifters.length} lifter${lifters.length !== 1 ? 's' : ''} &middot; ${timestamps.length} attempt${timestamps.length !== 1 ? 's' : ''}</p>
  <div style="background:#0D1B2A;border:1px solid #1B3A5C;border-radius:8px;padding:0.6rem 1rem;margin-bottom:1rem;font-size:0.82rem;color:#7CB3E0;">&#x25B6; Click any <span style="color:#3b82f6;border-bottom:1px dashed #3b82f6;">weight</span> to jump to that attempt in the YouTube VOD</div>
  ${videoLink}
  <input type="text" class="filter-input" placeholder="Search lifters..." oninput="filterLifters(this.value)">
  <div class="table-outer">
    <div class="table-wrap">
      <table class="scoresheet">
        <thead>
          <tr><th style="border-right:2px solid #252525;"></th>${groupRow}</tr>
          <tr><th style="text-align:left;padding:0.35rem 0.75rem;font-size:0.65rem;border-bottom:2px solid #252525;border-right:2px solid #252525;">LIFTER</th>${subHeaderRow}</tr>
        </thead>
        <tbody>${bodyRows}</tbody>
      </table>
    </div>
  </div>
</div>
<script>
const tw = document.querySelector('.table-wrap');
const to = document.querySelector('.table-outer');
if (tw && to) {
  const check = () => to.classList.toggle('scrolled-end', tw.scrollLeft + tw.clientWidth >= tw.scrollWidth - 2);
  tw.addEventListener('scroll', check);
  check();
}
function filterLifters(q) {
  const rows = document.querySelectorAll('.lifter-row');
  const seps = document.querySelectorAll('.wc-separator');
  const lower = q.toLowerCase();
  const visibleWcs = new Set();
  rows.forEach(r => {
    const show = r.dataset.name.includes(lower);
    r.style.display = show ? '' : 'none';
    if (show && r.dataset.wc) visibleWcs.add(r.dataset.wc);
  });
  seps.forEach(s => {
    const wcText = s.textContent.trim().replace(' kg','');
    s.style.display = (!lower || visibleWcs.has(wcText)) ? '' : 'none';
  });
}
</script>
</body></html>`;
}

function recapListHTML(recapMeets, lifterMap) {
  const cards = recapMeets.length === 0
    ? '<p style="color:#555;text-align:center;margin:2rem 0;">No meet recaps available yet.</p>'
    : recapMeets.map(v => {
      const lifters = (lifterMap && lifterMap[v.meet_id]) || [];
      const hasVideo = !!v.youtube_video_id;
      const vodBadge = hasVideo
        ? '<span style="font-size:0.7rem;background:#1A2A1A;color:#4ADE80;border:1px solid #2D5A2D;border-radius:4px;padding:0.1rem 0.4rem;margin-left:0.5rem;">VOD</span>'
        : '';
      return `<a href="/recap/${escHtml(v.meet_id)}" class="recap-card" data-name="${escHtml((v.meet_name || '').toLowerCase())}" data-lifters="${escHtml(lifters.join('|').toLowerCase())}" data-lifters-display="${escHtml(lifters.join('|'))}" style="display:block;text-decoration:none;color:inherit;">
        <div style="background:#141414;border:1px solid #1F1F1F;border-radius:12px;padding:1.25rem;margin-bottom:1rem;position:relative;overflow:hidden;">
          <div style="position:absolute;top:0;left:0;bottom:0;width:3px;background:#DC2626;"></div>
          <h2 style="font-size:1.1rem;margin-bottom:0.25rem;">${escHtml(v.meet_name || v.meet_id)}${vodBadge}</h2>
          <p style="font-size:0.85rem;color:#555;">${v.meet_date ? escHtml(v.meet_date) + ' &middot; ' : ''}${lifters.length} lifters</p>
          <div class="matched-lifters"></div>
        </div>
      </a>`;
    }).join('');

  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Meet Recaps - LiftAlert</title>
${FONT_LINKS}
<style>
  ${SHARED_STYLES}
  body { padding: 1.5rem 1rem; }
  .container { max-width: 600px; margin: 0 auto; }
  .nav { margin-bottom: 1.5rem; font-size: 0.85rem; }
  .nav a { color: #777; }
  .nav a:hover { color: #F0F0F0; }
  .page-heading { font-family: 'Bebas Neue', sans-serif; font-size: 1.75rem; letter-spacing: 0.06em; margin-bottom: 0.25rem; }
  .search-box { width: 100%; padding: 0.6rem 0.85rem; border-radius: 8px; border: 1px solid #252525; background: #0D0D0D; color: #F0F0F0; font-size: 0.85rem; font-family: 'Outfit', sans-serif; margin-bottom: 1.25rem; box-sizing: border-box; }
  .search-box:focus { outline: none; border-color: #DC2626; box-shadow: 0 0 0 3px rgba(220,38,38,0.1); }
  .matched-lifters { font-size: 0.8rem; color: #DC2626; margin-top: 0.5rem; line-height: 1.5; }
  .matched-lifters:empty { display: none; }
  .matched-lifters span { display: inline-block; background: #1A0A0A; border: 1px solid #3B1111; border-radius: 4px; padding: 0.1rem 0.4rem; margin: 0.15rem 0.2rem 0.15rem 0; font-size: 0.75rem; }
</style>
</head><body><div class="container animate-in">
  <div class="nav"><a href="/">&larr; Back to <span class="brand" style="font-size:1rem;"><span class="brand-lift">LIFT</span><span class="brand-alert">ALERT</span></span></a></div>
  <div class="page-heading">MEET RECAPS</div>
  <p class="subtitle" style="margin-bottom:1.25rem;">${recapMeets.length} meet recap${recapMeets.length !== 1 ? 's' : ''}</p>
  <input type="text" class="search-box" placeholder="Search lifters or meets..." oninput="filterRecaps(this.value)">
  ${cards}
</div>
<script>
function filterRecaps(q) {
  const cards = document.querySelectorAll('.recap-card');
  const lower = (q || '').trim().toLowerCase();
  cards.forEach(c => {
    const ml = c.querySelector('.matched-lifters');
    if (!lower || lower.length < 2) { c.style.display = ''; ml.innerHTML = ''; return; }
    const name = c.dataset.name || '';
    const nameMatch = name.includes(lower);
    const lifterList = (c.dataset.liftersDisplay || '').split('|').filter(Boolean);
    const matched = lifterList.filter(n => n.toLowerCase().includes(lower));
    if (nameMatch || matched.length > 0) {
      c.style.display = '';
      ml.innerHTML = matched.length > 0 ? matched.map(n => '<span>' + n.replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":"&#39;"}[c])) + '</span>').join('') : '';
    } else {
      c.style.display = 'none';
      ml.innerHTML = '';
    }
  });
}
</script>
</body></html>`;
}

function mySubscriptionsHTML(email, subs, persistentSubs) {
  const heading = email ? `Subscriptions for ${escHtml(email)}` : 'My Subscriptions';
  let content = '';
  if (email && subs.length === 0 && (!persistentSubs || persistentSubs.length === 0)) {
    content = `<p style="color:#555;text-align:center;margin:1.5rem 0;">No subscriptions found for this email.</p>`;
  } else if (email) {
    // Persistent "Following" section
    let followingSection = '';
    if (persistentSubs && persistentSubs.length > 0) {
      const pRows = persistentSubs.map(ps => `<tr data-action="/stop-following" data-email="${escHtml(email)}" data-lifter="${escHtml(ps.lifter_name)}">
        <td>${escHtml(ps.lifter_name)}</td>
        <td style="text-align:right">
          <span class="remove-wrap">
            <button type="button" onclick="this.parentElement.classList.add('confirming')" style="background:none;border:none;color:#DC2626;cursor:pointer;font-size:0.85rem;padding:0.25rem 0.5rem;font-family:'Outfit',sans-serif;" class="remove-btn">stop following</button>
            <span class="confirm-btns" style="display:none;">
              <button type="button" onclick="removeRow(this)" style="background:none;border:none;color:#4ADE80;cursor:pointer;font-size:1.1rem;padding:0.25rem 0.4rem;" title="Confirm">&#x2713;</button>
              <button type="button" onclick="this.closest('.remove-wrap').classList.remove('confirming')" style="background:none;border:none;color:#DC2626;cursor:pointer;font-size:1.1rem;padding:0.25rem 0.4rem;" title="Cancel">&#x2717;</button>
            </span>
          </span>
        </td>
      </tr>`).join('');
      followingSection = `<div style="margin-top:1.25rem;">
        <div style="font-family:'Bebas Neue',sans-serif;font-size:1.1rem;color:#777;letter-spacing:0.06em;margin-bottom:0.5rem;">FOLLOWING</div>
        <p style="font-size:0.8rem;color:#555;margin-bottom:0.5rem;">Auto-subscribed when these lifters compete in any meet.</p>
        <table><thead><tr><th>Lifter</th><th></th></tr></thead><tbody>${pRows}</tbody></table>
      </div>`;
    }

    // Per-meet subscriptions
    let meetSection = '';
    if (subs.length > 0) {
      const rows = subs.map(s => {
        const mName = meets[s.meet_id]?.meet?.name || s.meet_id;
        const meetDate = meets[s.meet_id]?.meet?.date || '';
        const meetLocation = meets[s.meet_id]?.meet?.location || meets[s.meet_id]?.meet?.city || '';
        const details = [escHtml(meetDate), escHtml(meetLocation)].filter(Boolean).join(' &middot; ');
        return `<tr data-action="/unsubscribe" data-email="${escHtml(s.email)}" data-lifter="${escHtml(s.lifter_name)}" data-meet="${escHtml(s.meet_id)}">
          <td>${escHtml(s.lifter_name)}</td>
          <td>${escHtml(mName)}${details ? '<br><span style="font-size:0.75rem;color:#555">' + details + '</span>' : ''}${s.notify_prefs ? '<br><span style="font-size:0.7rem;color:#666;">' + escHtml(s.notify_prefs.split(',').join(', ')) + '</span>' : ''}</td>
          <td style="text-align:right">
            <span class="remove-wrap">
              <button type="button" onclick="this.parentElement.classList.add('confirming')" style="background:none;border:none;color:#DC2626;cursor:pointer;font-size:0.85rem;padding:0.25rem 0.5rem;font-family:'Outfit',sans-serif;" class="remove-btn">remove</button>
              <span class="confirm-btns" style="display:none;">
                <button type="button" onclick="removeRow(this)" style="background:none;border:none;color:#4ADE80;cursor:pointer;font-size:1.1rem;padding:0.25rem 0.4rem;" title="Confirm">&#x2713;</button>
                <button type="button" onclick="this.closest('.remove-wrap').classList.remove('confirming')" style="background:none;border:none;color:#DC2626;cursor:pointer;font-size:1.1rem;padding:0.25rem 0.4rem;" title="Cancel">&#x2717;</button>
              </span>
            </span>
          </td>
        </tr>`;
      }).join('');
      meetSection = `<div style="margin-top:1.25rem;">
        <div style="font-family:'Bebas Neue',sans-serif;font-size:1.1rem;color:#777;letter-spacing:0.06em;margin-bottom:0.5rem;">MEET ALERTS</div>
        <table><thead><tr><th>Lifter</th><th>Meet</th><th></th></tr></thead><tbody>${rows}</tbody></table>
      </div>`;
    }
    content = followingSection + meetSection;
  }
  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>My Subscriptions - LiftAlert</title>
${FONT_LINKS}
<style>
  ${SHARED_STYLES}
  body { display: flex; align-items: center; justify-content: center; padding: 1rem; }
  .card { max-width: 520px; }
  .page-heading { font-family: 'Bebas Neue', sans-serif; font-size: 1.5rem; letter-spacing: 0.04em; margin-top: 1rem; }
  .back-link { display: block; text-align: center; margin-top: 1.5rem; color: #777; font-size: 0.88rem; }
  .back-link:hover { color: #F0F0F0; }
</style>
</head><body><div class="card animate-in">
  <span class="brand"><span class="brand-lift">LIFT</span><span class="brand-alert">ALERT</span></span>
  <div class="page-heading">${heading}</div>
  <p class="subtitle">View and manage your LiftAlert subscriptions.</p>
  <form method="GET" action="/my-subscriptions">
    <label for="email">Email</label>
    <input type="email" id="email" name="email" required placeholder="you@example.com" value="${email ? escHtml(email) : ''}">
  </form>
  ${content}
  <a href="/" class="back-link">&larr; Subscribe to a lifter</a>
</div>
<script>
// Auto-redirect with email from localStorage if not already in URL
(function() {
  try {
    var email = new URLSearchParams(window.location.search).get('email');
    if (!email) {
      var saved = localStorage.getItem('liftalert_email');
      if (saved) {
        window.location.replace('/my-subscriptions?email=' + encodeURIComponent(saved));
        return;
      }
    }
  } catch(e) {}
})();
// Save email to localStorage when form is submitted
document.querySelector('form')?.addEventListener('submit', function() {
  try {
    var el = document.getElementById('email');
    if (el && el.value) localStorage.setItem('liftalert_email', el.value);
  } catch(e) {}
});
function removeRow(btn) {
  var row = btn.closest('tr');
  var d = row.dataset;
  var body = 'email=' + encodeURIComponent(d.email) + '&lifter=' + encodeURIComponent(d.lifter);
  if (d.meet) body += '&meet=' + encodeURIComponent(d.meet);
  row.style.opacity = '0.4';
  fetch(d.action, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: body, redirect: 'manual' })
    .then(function() { row.remove(); })
    .catch(function() { row.style.opacity = '1'; });
}
</script>
</body></html>`;
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
    let persistentSubs = [];
    if (email) {
      try {
        [subs, persistentSubs] = await Promise.all([
          getSubscriptionsByEmail(email),
          getPersistentSubscriptionsByEmail(email),
        ]);
      } catch (err) {
        console.error(`[LOOKUP ERROR] ${err.message}`);
      }
    }
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(mySubscriptionsHTML(email, subs, persistentSubs));

  } else if (req.method === 'POST' && url.pathname === '/subscribe') {
    let body = '';
    let tooLarge = false;
    const MAX_BODY = 32768;
    for await (const chunk of req) {
      body += chunk;
      if (body.length > MAX_BODY) { tooLarge = true; break; }
    }
    if (tooLarge) {
      res.writeHead(413, { 'Content-Type': 'text/plain' });
      res.end('Request body too large');
      return;
    }

    // Support JSON bulk subscribe or legacy form body
    const isJSON = (req.headers['content-type'] || '').includes('application/json');
    let email, items, notifyPrefs; // items = [{ name, meetId }]
    if (isJSON) {
      try {
        const parsed = JSON.parse(body);
        email = parsed.email;
        items = (parsed.selections || []).map(s => ({ name: s.name, meetId: s.meetId }));
        notifyPrefs = parsed.notifyPrefs || 'in-the-hole';
      } catch {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('Invalid JSON');
        return;
      }
    } else {
      const form = parseFormBody(body);
      email = form.email;
      notifyPrefs = form.notifyPrefs || 'in-the-hole';
      if (form.lifter && form.meet) items = [{ name: form.lifter, meetId: form.meet }];
      else items = [];
    }

    if (!email || items.length === 0) {
      res.writeHead(400, { 'Content-Type': 'text/plain' });
      res.end('Missing required fields: email and at least one lifter selection');
      return;
    }

    // Validate all selections
    for (const item of items) {
      const meetState = meets[item.meetId];
      if (!meetState) {
        res.writeHead(400, { 'Content-Type': 'text/html' });
        res.end(errorHTML(`Meet not found for "${item.name}". Please select lifters from the dropdown.`));
        return;
      }
      const lifterLower = item.name.toLowerCase();
      const lifterExists = Object.values(meetState.lifters).some(l => l.name && l.name.toLowerCase() === lifterLower);
      if (!lifterExists) {
        res.writeHead(400, { 'Content-Type': 'text/html' });
        res.end(errorHTML(`Lifter "${item.name}" not found in meet. Please select from the dropdown.`));
        return;
      }
    }

    try {
      for (const item of items) {
        await addSubscription(email, item.name, item.meetId, notifyPrefs);
        await addPersistentSubscription(email, item.name, notifyPrefs);
        delete subsCache[item.meetId];
        console.log(`[SUBSCRIBE] ${email} -> "${item.name}" in meet ${item.meetId} (prefs: ${notifyPrefs}) (+ persistent follow)`);
        startMeet(item.meetId);
      }
      // Send one confirmation email for the batch
      const names = items.map(i => i.name);
      const firstMeet = items[0].meetId;
      const meetName = meets[firstMeet]?.meet?.name || firstMeet;
      const meetDate = meets[firstMeet]?.meet?.date || '';
      if (items.length === 1) {
        sendSubscriptionConfirmation(email, names[0], meetName, meetDate, firstMeet, notifyPrefs);
      } else {
        sendSubscriptionConfirmation(email, names.join(', '), meetName, meetDate, firstMeet, notifyPrefs);
      }
      const allSubs = await getSubscriptionsByEmail(email);
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(bulkSuccessHTML(email, items, allSubs, notifyPrefs));
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
    const meetDate = meets[meet]?.meet?.date || '';
    const confirmHtml = `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Unsubscribe - LiftAlert</title>
${FONT_LINKS}
<style>
  ${SHARED_STYLES}
  body { display: flex; align-items: center; justify-content: center; padding: 1rem; }
  .confirm-msg { color: #999; text-align: center; margin-top: 1.25rem; margin-bottom: 1.5rem; font-size: 0.95rem; }
  .confirm-msg strong { color: #F0F0F0; }
  .btn-wrap { text-align: center; }
  .cancel-link { display: block; text-align: center; margin-top: 1rem; color: #777; font-size: 0.88rem; }
  .cancel-link:hover { color: #F0F0F0; }
</style>
</head><body><div class="card animate-in">
  <span class="brand"><span class="brand-lift">LIFT</span><span class="brand-alert">ALERT</span></span>
  <p class="confirm-msg">Remove alert for <strong>${escHtml(lifter)}</strong> at <strong>${escHtml(meetName)}</strong>${meetDate ? ` (${escHtml(meetDate)})` : ''}?<br><span style="font-size:0.85rem;">This also stops auto-subscribing to this lifter in future meets.</span></p>
  <form id="unsub-form" method="POST" action="/unsubscribe">
    <input type="hidden" name="email" value="${escHtml(email)}">
    <input type="hidden" name="lifter" value="${escHtml(lifter)}">
    <input type="hidden" name="meet" value="${escHtml(meet)}">
    <div class="btn-wrap"><button type="submit" class="btn-danger" id="unsub-btn">YES, UNSUBSCRIBE</button></div>
  </form>
  <a href="/" class="cancel-link">Cancel</a>
</div>
<script>
document.getElementById('unsub-form').addEventListener('submit', function(e) {
  e.preventDefault();
  var btn = document.getElementById('unsub-btn');
  btn.disabled = true;
  btn.textContent = 'REMOVING...';
  var fd = new URLSearchParams(new FormData(this));
  fetch('/unsubscribe', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: fd.toString(), redirect: 'manual' })
    .then(function() {
      document.querySelector('.confirm-msg').innerHTML = 'You have been unsubscribed.<br><span style="font-size:0.85rem;">You will no longer be auto-subscribed to this lifter in future meets.</span>';
      btn.closest('.btn-wrap').remove();
    })
    .catch(function() {
      btn.disabled = false;
      btn.textContent = 'YES, UNSUBSCRIBE';
      document.querySelector('.confirm-msg').innerHTML = '<span style="color:#DC2626;">Something went wrong. Please try again.</span>';
    });
});
</script>
</body></html>`;
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
      await removePersistentSubscription(email, lifter);
      delete subsCache[meet];
      console.log(`[UNSUBSCRIBE] ${email} -> "${lifter}" in meet ${meet} (${removed ? 'removed' : 'not found'}) + persistent follow removed`);
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

  } else if (req.method === 'POST' && url.pathname === '/stop-following') {
    let body = '';
    for await (const chunk of req) {
      body += chunk;
      if (body.length > 8192) break;
    }
    const { email, lifter } = parseFormBody(body);
    if (!email || !lifter) {
      res.writeHead(400, { 'Content-Type': 'text/plain' });
      res.end('Missing required params: email, lifter');
      return;
    }
    try {
      await removePersistentSubscription(email, lifter);
      console.log(`[STOP-FOLLOWING] ${email} stopped following "${lifter}"`);
      res.writeHead(302, { 'Location': `/my-subscriptions?email=${encodeURIComponent(email)}` });
      res.end();
    } catch (err) {
      console.error(`[STOP-FOLLOWING ERROR] ${err.message}`);
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Failed to stop following. Please try again.');
    }

  } else if (url.pathname === '/stats') {
    try {
      const stats = await getStats();
      const emailStats = await getEmailStats();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ...stats, email: emailStats }, null, 2));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
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
    const email = url.searchParams.get('email') || '';
    // Get user's subscribed meet IDs
    let subscribedMeetIds = [];
    if (email) {
      try {
        const subs = await getSubscriptionsByEmail(email);
        subscribedMeetIds = [...new Set(subs.map(s => s.meet_id))];
      } catch (err) {
        console.error(`[MEETS] Error fetching subscriptions for ${email}: ${err.message}`);
      }
    }
    const cacheKey = email || '';
    const cached = meetsPageCache.get(cacheKey);
    if (cached && Date.now() - cached.ts < MEETS_PAGE_CACHE_TTL) {
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(cached.html);
    } else {
      const parseMeetDate = (m) => {
        const parts = (m.date || '').split('/');
        if (parts.length !== 3) return 0;
        const fmt = (meets[m.id]?.meet?.dateFormat) || 'MM/DD/YYYY';
        let y, mo, d;
        if (fmt === 'DD/MM/YYYY') { [d, mo, y] = parts; }
        else { [mo, d, y] = parts; }
        return new Date(Number(y), Number(mo) - 1, Number(d)).getTime() || 0;
      };
      const meetList = [];
      for (const [mid, st] of Object.entries(meets)) {
        const lifterCount = Object.keys(st.lifters).length;
        const platformCount = Object.keys(st.platforms).length;
        const meetDoc = st.meet || {};
        // Get current lifter from first platform (for status line on card)
        let currentLift = null;
        for (const [pid, platform] of Object.entries(st.platforms)) {
          const cached = st.platformSummaryCache?.[pid];
          if (cached?.currentLifter) {
            currentLift = { lifter: cached.currentLifter, liftName: cached.liftName, attemptNumber: cached.attemptNumber };
            break;
          }
          const parsed = parseAttemptId(platform.currentAttemptId);
          if (parsed) {
            const cl = st.lifters[parsed.lifterId];
            if (cl) {
              currentLift = { lifter: cl.name || 'Unknown', liftName: parsed.liftName, attemptNumber: parsed.attemptNumber };
              break;
            }
          }
        }
        // Determine if meet is live: date is today AND has active platform activity
        const meetIsToday = meetDoc.date ? isMeetToday(meetDoc) : false;
        const isLive = meetIsToday && currentLift !== null;
        // Collect lifter names for search filtering
        const lifterNames = Object.values(st.lifters).map(l => l.name).filter(Boolean);
        meetList.push({
          id: mid,
          name: meetDoc.name || mid,
          date: meetDoc.date || '',
          dateFormat: meetDoc.dateFormat || 'MM/DD/YYYY',
          location: meetDoc.location || meetDoc.city || '',
          lifterCount,
          platformCount,
          isLive,
          currentLift,
          lifterNames,
        });
      }
      // Sort chronologically, newest first
      meetList.sort((a, b) => parseMeetDate(b) - parseMeetDate(a));
      const html = meetsHTML(meetList, subscribedMeetIds);
      meetsPageCache.set(cacheKey, { html, ts: Date.now() });
      // Evict oldest entries if cache is too large
      if (meetsPageCache.size > MEETS_PAGE_CACHE_MAX) {
        const oldest = meetsPageCache.keys().next().value;
        meetsPageCache.delete(oldest);
      }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(html);
    }

  } else if (req.method === 'GET' && url.pathname.startsWith('/meets/')) {
    const detailMeetId = url.pathname.split('/')[2];
    const email = url.searchParams.get('email') || '';
    if (!detailMeetId || !meets[detailMeetId]) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Meet not found');
    } else {
      let subscribedLifterNames = [];
      let videoData = null;
      let timestamps = [];
      const fetches = [];
      if (email) {
        fetches.push(
          getSubscriptionsByEmail(email)
            .then(subs => { subscribedLifterNames = subs.filter(s => s.meet_id === detailMeetId).map(s => s.lifter_name); })
            .catch(err => console.error(`[MEET DETAIL] Error fetching subscriptions: ${err.message}`))
        );
      }
      fetches.push(
        getMeetVideo(detailMeetId)
          .then(v => { videoData = v; })
          .catch(() => {})
      );
      fetches.push(
        getAttemptTimestampsByMeet(detailMeetId)
          .then(ts => { timestamps = ts; })
          .catch(() => {})
      );
      await Promise.all(fetches);
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(meetDetailHTML(detailMeetId, meets[detailMeetId], subscribedLifterNames, videoData, timestamps));
    }

  } else if (req.method === 'GET' && url.pathname === '/api/lifters') {
    const results = [];
    for (const [meetId, st] of Object.entries(meets)) {
      const meetName = st.meet?.name || meetId;
      const meetDate = st.meet?.date || '';
      for (const lifter of Object.values(st.lifters)) {
        if (lifter.name) {
          results.push({ name: lifter.name, meetId, meetName, meetDate });
        }
      }
    }
    results.sort((a, b) => a.name.localeCompare(b.name));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(results));

  } else if (req.method === 'GET' && url.pathname === '/recaps') {
    const [recapMeets, lifterMap] = await Promise.all([getRecapMeets(), getRecapLifterNames()]);
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(recapListHTML(recapMeets, lifterMap));

  } else if (req.method === 'GET' && url.pathname.startsWith('/recap/')) {
    const recapMeetId = url.pathname.split('/')[2];
    if (!recapMeetId) {
      res.writeHead(400, { 'Content-Type': 'text/plain' });
      res.end('Missing meet ID');
    } else {
      const [video, timestamps] = await Promise.all([
        getMeetVideo(recapMeetId),
        getAttemptTimestampsByMeet(recapMeetId),
      ]);
      if (timestamps.length === 0) {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end(`No attempt timestamps found for meet ${recapMeetId}.`);
      } else {
        const videoId = video?.youtube_video_id || null;
        const streamStart = video ? Number(video.stream_start_epoch) : 0;
        const meetName = video?.meet_name || getMeetState(recapMeetId).meet?.name || recapMeetId;
        const meetDate = video?.meet_date || getMeetState(recapMeetId).meet?.date || '';
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(recapHTML(recapMeetId, meetName, videoId, streamStart, timestamps, meetDate));
      }
    }

  } else if (req.method === 'GET' && url.pathname.match(/^\/api\/meet\/[^/]+\/live$/)) {
    const liveMeetId = url.pathname.split('/')[3];
    const st = meets[liveMeetId];
    if (!st) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Meet not found' }));
    } else {
      const meetDoc = st.meet || {};
      const platforms = Object.entries(st.platforms).map(([pid, platform]) => {
        const parsed = parseAttemptId(platform.currentAttemptId);
        const currentLifter = parsed ? st.lifters[parsed.lifterId] : null;
        const currentAttempt = parsed && platform.currentAttemptId ? st.attempts[platform.currentAttemptId] : null;
        const order = computeAttemptOrder(st, pid);
        const currentIdx = platform.currentAttemptId ? order.findIndex(a => a.attemptId === platform.currentAttemptId) : -1;
        const queue = currentIdx >= 0 ? order.slice(currentIdx + 1, currentIdx + 9) : order.slice(0, 8);
        return {
          id: pid,
          name: platform.name || pid,
          current: currentLifter ? {
            lifterName: currentLifter.name || 'Unknown',
            liftName: parsed.liftName,
            attemptNumber: parsed.attemptNumber,
            weight: currentAttempt?.weight || null,
          } : null,
          queue: queue.map(a => ({
            lifterName: a.lifterName,
            liftName: a.liftName,
            attemptNumber: a.attemptNumber,
            weight: a.weight,
          })),
        };
      });
      const lifters = Object.values(st.lifters).filter(l => l.name).map(l => {
        const bests = computeLifterBests(st, l._id);
        const total = bests.squat + bests.bench + bests.dead;
        return {
          name: l.name,
          bodyWeight: l.bodyWeight || null,
          weightClass: getWeightClass(l.bodyWeight, l.gender, l.declaredWeightClass),
          bestSq: bests.squat || null,
          bestBp: bests.bench || null,
          bestDl: bests.dead || null,
          total: total || null,
        };
      });
      lifters.sort((a, b) => {
        const wcA = typeof a.weightClass === 'number' ? a.weightClass : 9999;
        const wcB = typeof b.weightClass === 'number' ? b.weightClass : 9999;
        if (wcA !== wcB) return wcA - wcB;
        const bwA = a.bodyWeight || 9999;
        const bwB = b.bodyWeight || 9999;
        if (bwA !== bwB) return bwA - bwB;
        return a.name.localeCompare(b.name);
      });
      let video = null;
      try { video = await getMeetVideo(liveMeetId); } catch (e) { /* ignore */ }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        meet: { name: meetDoc.name || liveMeetId, date: meetDoc.date || '' },
        watching: watchingMeets.has(liveMeetId),
        platforms,
        lifters,
        video: video ? { youtubeVideoId: video.youtube_video_id } : null,
      }));
    }

  } else if (req.method === 'GET' && url.pathname.match(/^\/live\/[^/]+$/)) {
    const liveMeetId = url.pathname.split('/')[2];
    const st = meets[liveMeetId];
    if (!st) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Meet not found');
    } else {
      const meetName = st.meet?.name || liveMeetId;
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(liveHTML(liveMeetId, meetName));
    }

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
