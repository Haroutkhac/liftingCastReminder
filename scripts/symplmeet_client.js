/**
 * SymPlmeet client — REST + Socket.IO integration for symplmeet.plmeet.com
 *
 * Normalizes SymPlmeet data into the same shape as LiftingCast meets
 * so the existing notification pipeline works unchanged.
 *
 * ID prefixes (avoid collisions with LiftingCast):
 *   sl-{id}  = lifter
 *   sa-{lift}{num}-{id} = attempt (e.g. sa-sq1-123)
 *   sp-default = virtual platform
 */
const https = require('https');
const { io } = require('socket.io-client');

const SYMPLMEET_BASE = 'https://symplmeet.plmeet.com';

// Active Socket.IO connections keyed by meetId
const sockets = {};

// --- HTTPS fetch with self-signed cert support ---
function symplmeetFetchJSON(path) {
  const url = `${SYMPLMEET_BASE}${path}`;
  return new Promise((resolve, reject) => {
    const req = https.get(url, { rejectUnauthorized: false, timeout: 15000 }, (res) => {
      if (res.statusCode < 200 || res.statusCode >= 300) {
        res.resume();
        reject(new Error(`HTTP ${res.statusCode} from ${url}`));
        return;
      }
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('error', err => reject(err));
      res.on('end', () => {
        try { resolve(JSON.parse(body)); }
        catch (e) { reject(new Error(`Invalid JSON from ${url}`)); }
      });
    });
    req.on('error', err => reject(err));
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout fetching ${url}`)); });
  });
}

// --- Platform detection ---
function getMeetPlatform(meetId) {
  return /^\d+$/.test(String(meetId)) ? 'symplmeet' : 'liftingcast';
}

// --- Map SymPlmeet lift prefix to canonical name ---
const LIFT_PREFIX_MAP = { sq: 'squat', bp: 'bench', dl: 'dead' };
const RESULT_MAP = { 1: 'good', 0: null, '-1': 'bad' };

// --- Parse lift prefix and attempt number from various API formats ---
function parseLiftEntry(entry) {
  if (!entry) return null;
  // Try round field first (e.g. "sq1", "bp2", "dl3")
  if (entry.round) {
    const match = entry.round.match(/^(sq|bp|dl)(\d)$/);
    if (match) return { prefix: match[1], attemptNum: match[2], lifterId: entry.lifterId || entry.id };
  }
  // Fall back to liftType/attemptNumber fields
  const liftType = (entry.liftType || entry.lift || '').toLowerCase();
  let prefix = null;
  if (liftType.includes('squat') || liftType === 'sq') prefix = 'sq';
  else if (liftType.includes('bench') || liftType === 'bp') prefix = 'bp';
  else if (liftType.includes('dead') || liftType === 'dl') prefix = 'dl';
  if (!prefix) return null;
  return { prefix, attemptNum: entry.attemptNumber || entry.attempt || 1, lifterId: entry.lifterId || entry.id };
}

// --- Normalize SymPlmeet data into meetState shape ---
function normalizeSymPlmeetData(meetId, data, meetState) {
  // API may return stringified JSON for these fields
  const meetInfo = typeof data.meetInfo === 'string' ? JSON.parse(data.meetInfo) : (data.meetInfo || {});
  const results = typeof data.results === 'string' ? JSON.parse(data.results) : (data.results || {});
  const rawLiftingOrder = typeof data.liftingOrderThisRound === 'string' ? JSON.parse(data.liftingOrderThisRound) : data.liftingOrderThisRound;
  const liftingOrder = Array.isArray(rawLiftingOrder) ? rawLiftingOrder : [];

  // Meet doc — check multiple possible name fields, preserve previously discovered name
  const meetName = meetInfo.meetName || meetInfo.name || meetInfo.title
    || data.meetName || data.name
    || (meetState.meet && meetState.meet.name)
    || `SymPlmeet #${meetId}`;
  meetState.meet = {
    _id: String(meetId),
    name: meetName,
  };

  // Only clear state if we have results data to replace it with
  // (Socket.IO updates may be partial, containing only currentLift changes)
  const hasResults = data.results && (typeof data.results === 'string' ? data.results !== '{}' && data.results !== '[]' : Object.keys(data.results).length > 0);
  if (hasResults) {
    meetState.lifters = {};
    meetState.attempts = {};
  }
  meetState.platforms = meetState.platforms || {};

  // Build lifters and attempts from results
  // API returns { "A": [lifters], "B": [lifters] } keyed by flight letter,
  // or older format with flights array or flat lifters array
  const allLifters = [];

  if (Array.isArray(results)) {
    // Array of flight objects: [{flight, lifters}, ...]
    for (const flight of results) {
      const flightName = flight.flight || 'A';
      const lifters = flight.lifters || flight.data || [];
      for (const l of lifters) {
        allLifters.push({ ...l, flight: flightName });
      }
    }
  } else if (typeof results === 'object') {
    // Check if results is keyed by flight letter: { "A": [...], "B": [...] }
    const keys = Object.keys(results);
    const isFlightKeyed = keys.length > 0 && keys.every(k => Array.isArray(results[k]));
    if (isFlightKeyed) {
      for (const [flightName, lifters] of Object.entries(results)) {
        for (const l of lifters) {
          allLifters.push({ ...l, flight: l.flight || flightName });
        }
      }
    } else {
      // Fallback: results.flights or results.lifters
      const flights = results.flights || results;
      if (Array.isArray(flights)) {
        for (const flight of flights) {
          const flightName = flight.flight || 'A';
          const lifters = flight.lifters || flight.data || [];
          for (const l of lifters) {
            allLifters.push({ ...l, flight: flightName });
          }
        }
      } else if (flights.lifters) {
        for (const l of flights.lifters) {
          allLifters.push({ ...l, flight: l.flight || 'A' });
        }
      }
    }
  }

  for (const l of allLifters) {
    const lid = `sl-${l.id || l.lifterId}`;
    const lifterObj = {
      _id: lid,
      name: [l.firstName || l.firstname, l.lastName || l.lastname].filter(Boolean).join(' ') || l.name || 'Unknown',
      lot: l.lotNumber ?? l.lot ?? 999,
      session: 1,
      flight: l.flight || 'A',
      platformId: 'sp-default',
    };

    // Store pre-computed best lifts if available
    const bl = l.bestlifts || l.bestLifts || {};
    if (bl.squat != null || bl.bench != null || bl.deadlift != null || l.bestSquat != null || l.bestBench != null || l.bestDeadlift != null) {
      lifterObj.bestLifts = {
        squat: bl.squat ?? l.bestSquat ?? 0,
        bench: bl.bench ?? l.bestBench ?? 0,
        deadlift: bl.deadlift ?? l.bestDeadlift ?? 0,
      };
    }

    meetState.lifters[lid] = lifterObj;

    // Explode attempt fields: sq1-sq3, bp1-bp3, dl1-dl3
    const rawId = l.id || l.lifterId;
    for (const [prefix, liftName] of Object.entries(LIFT_PREFIX_MAP)) {
      for (let num = 1; num <= 3; num++) {
        const weightKey = `${prefix}${num}`;
        const resultKey = `${prefix}${num}res`;
        const attemptId = `sa-${prefix}${num}-${rawId}`;

        const weight = l[weightKey] != null ? Number(l[weightKey]) : null;
        const rawResult = l[resultKey] != null ? Number(l[resultKey]) : null;
        const result = rawResult != null ? (RESULT_MAP[String(rawResult)] ?? null) : null;

        meetState.attempts[attemptId] = {
          _id: attemptId,
          lifterId: lid,
          liftName,
          attemptNumber: String(num),
          weight: weight && weight > 0 ? weight : null,
          result,
        };
      }
    }
  }

  // Virtual platform
  let rawCurrentLift = typeof data.currentLift === 'string' ? JSON.parse(data.currentLift) : data.currentLift;
  if (!rawCurrentLift) {
    rawCurrentLift = typeof meetInfo.currentLift === 'string' ? JSON.parse(meetInfo.currentLift) : meetInfo.currentLift;
  }
  rawCurrentLift = rawCurrentLift || {};
  let currentAttemptId = null;

  const currentParsed = parseLiftEntry(rawCurrentLift);
  if (currentParsed && currentParsed.lifterId) {
    currentAttemptId = `sa-${currentParsed.prefix}${currentParsed.attemptNum}-${currentParsed.lifterId}`;
  }

  // If we have a lifting order, use the first entry as current lifter
  if (!currentAttemptId && liftingOrder.length > 0) {
    const firstParsed = parseLiftEntry(liftingOrder[0]);
    if (firstParsed && firstParsed.lifterId) {
      currentAttemptId = `sa-${firstParsed.prefix}${firstParsed.attemptNum}-${firstParsed.lifterId}`;
    }
  }

  meetState.platforms['sp-default'] = {
    _id: 'sp-default',
    name: 'Platform 1',
    currentAttemptId,
  };

  // Inject lifting order as pre-computed order on the platform
  // (used by computeAttemptOrder when available)
  if (liftingOrder.length > 0) {
    meetState.platforms['sp-default']._liftingOrder = liftingOrder.map(entry => {
      const parsed = parseLiftEntry(entry);
      if (!parsed || !parsed.lifterId) return null;
      return `sa-${parsed.prefix}${parsed.attemptNum}-${parsed.lifterId}`;
    }).filter(Boolean);
  }
}

// --- Load meet via REST ---
async function loadSymPlmeetMeet(meetId, meetState, hintName) {
  console.log(`[SYMPLMEET] Loading meet ${meetId}...`);
  // Pre-set discovered name so normalizeSymPlmeetData can preserve it as fallback
  if (hintName && (!meetState.meet || meetState.meet.name === `SymPlmeet #${meetId}`)) {
    meetState.meet = { _id: String(meetId), name: hintName };
  }
  const data = await symplmeetFetchJSON(`/api/getSocketData/${meetId}`);
  normalizeSymPlmeetData(meetId, data, meetState);
  console.log(`[SYMPLMEET] Loaded: "${meetState.meet?.name}" | ${Object.keys(meetState.lifters).length} lifters`);
}

// --- Watch meet via Socket.IO ---
function watchSymPlmeet(meetId, onUpdate) {
  if (sockets[meetId]) return; // already watching

  console.log(`[SYMPLMEET] Connecting Socket.IO for meet ${meetId}...`);
  const socket = io(SYMPLMEET_BASE, {
    rejectUnauthorized: false,
    transports: ['websocket', 'polling'],
    reconnection: true,
    reconnectionDelay: 2000,
    reconnectionDelayMax: 30000,
  });

  socket.on('connect', () => {
    console.log(`[SYMPLMEET] Socket connected for meet ${meetId}`);
    socket.emit('subscribeToMeet', meetId);
  });

  socket.on('update', (data) => {
    onUpdate(data);
  });

  socket.on('disconnect', (reason) => {
    console.log(`[SYMPLMEET] Socket disconnected for meet ${meetId}: ${reason}`);
  });

  socket.on('connect_error', (err) => {
    console.error(`[SYMPLMEET] Socket error for meet ${meetId}: ${err.message}`);
  });

  sockets[meetId] = socket;
}

// --- Stop watching ---
function stopSymPlmeet(meetId) {
  const socket = sockets[meetId];
  if (socket) {
    console.log(`[SYMPLMEET] Disconnecting socket for meet ${meetId}`);
    socket.disconnect();
    delete sockets[meetId];
  }
}

function stopAllSymPlmeet() {
  for (const meetId of Object.keys(sockets)) {
    stopSymPlmeet(meetId);
  }
}

// --- Discover today's meets ---
async function discoverTodaysSymPlmeetMeets() {
  try {
    const data = await symplmeetFetchJSON('/api/todayMeets');
    const meets = Array.isArray(data) ? data : [];
    console.log(`[SYMPLMEET] Discovered ${meets.length} today's meets`);
    return meets.map(m => ({
      id: String(m.id || m.meetId),
      name: m.meetName || m.name || m.title || `SymPlmeet #${m.id || m.meetId}`,
    }));
  } catch (err) {
    console.error(`[SYMPLMEET] Failed to discover today's meets: ${err.message}`);
    return [];
  }
}

module.exports = {
  getMeetPlatform,
  loadSymPlmeetMeet,
  watchSymPlmeet,
  stopSymPlmeet,
  stopAllSymPlmeet,
  discoverTodaysSymPlmeetMeets,
  normalizeSymPlmeetData,
  symplmeetFetchJSON,
};
