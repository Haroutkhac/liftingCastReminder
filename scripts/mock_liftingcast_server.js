/**
 * Mock LiftingCast WebSocket Server
 *
 * Simulates the LiftingCast WebSocket API with realistic meet data.
 * Automatically advances through lifters every few seconds to test
 * "on deck" detection logic.
 *
 * Usage: node scripts/mock_liftingcast_server.js [port]
 * Default port: 8080
 */
const WebSocket = require('ws');

const PORT = parseInt(process.argv[2]) || 8080;

// --- Realistic test meet data ---

const MEET_ID = 'mtest12345678';
const MEET_PASSWORD = 'testpass';

let attemptCounter = 0;
function makeAttempt(weight) {
  attemptCounter++;
  return {
    id: `a${String(attemptCounter).padStart(4, '0')}`,
    weight: weight,
    result: null,
    records: [],
    decisions: {
      left: { decision: null, cards: {} },
      head: { decision: null, cards: {} },
      right: { decision: null, cards: {} },
    },
  };
}

function makeLifts(squatOpener, benchOpener, deadOpener) {
  return {
    squat: {
      '1': makeAttempt(squatOpener), '2': makeAttempt(null), '3': makeAttempt(null), '4': makeAttempt(null),
    },
    bench: {
      '1': makeAttempt(benchOpener), '2': makeAttempt(null), '3': makeAttempt(null), '4': makeAttempt(null),
    },
    dead: {
      '1': makeAttempt(deadOpener), '2': makeAttempt(null), '3': makeAttempt(null), '4': makeAttempt(null),
    },
  };
}

const lifters = {
  'ltf001': {
    id: 'ltf001', memberNumber: '12345', name: 'John Smith', gender: 'M',
    team: 'Iron House', state: 'CA', country: 'US', bodyWeight: 93.2,
    lot: 3, session: 1, flight: 'A', squatRackHeight: '12', benchRackHeight: '5',
    platform: 'p001',
    divisions: [{ divisionId: 'd001', weightClassId: 'w001', score: null, forecastedScore: null, place: null, forecastedPlace: null, total: null, forecastedTotal: null }],
    lifts: makeLifts(200, 130, 230),
  },
  'ltf002': {
    id: 'ltf002', memberNumber: '12346', name: 'Mike Johnson', gender: 'M',
    team: 'Barbell Club', state: 'TX', country: 'US', bodyWeight: 92.8,
    lot: 7, session: 1, flight: 'A', squatRackHeight: '11', benchRackHeight: '4',
    platform: 'p001',
    divisions: [{ divisionId: 'd001', weightClassId: 'w001', score: null, forecastedScore: null, place: null, forecastedPlace: null, total: null, forecastedTotal: null }],
    lifts: makeLifts(190, 125, 220),
  },
  'ltf003': {
    id: 'ltf003', memberNumber: '12347', name: 'Alex Rivera', gender: 'M',
    team: 'Power Gym', state: 'FL', country: 'US', bodyWeight: 94.1,
    lot: 1, session: 1, flight: 'A', squatRackHeight: '13', benchRackHeight: '6',
    platform: 'p001',
    divisions: [{ divisionId: 'd001', weightClassId: 'w001', score: null, forecastedScore: null, place: null, forecastedPlace: null, total: null, forecastedTotal: null }],
    lifts: makeLifts(210, 140, 250),
  },
  'ltf004': {
    id: 'ltf004', memberNumber: '12348', name: 'Chris Lee', gender: 'M',
    team: 'Strength Co', state: 'NY', country: 'US', bodyWeight: 91.5,
    lot: 5, session: 1, flight: 'A', squatRackHeight: '10', benchRackHeight: '4',
    platform: 'p001',
    divisions: [{ divisionId: 'd001', weightClassId: 'w001', score: null, forecastedScore: null, place: null, forecastedPlace: null, total: null, forecastedTotal: null }],
    lifts: makeLifts(185, 120, 215),
  },
  'ltf005': {
    id: 'ltf005', memberNumber: '12349', name: 'Sarah Williams', gender: 'F',
    team: 'Iron House', state: 'CA', country: 'US', bodyWeight: 63.0,
    lot: 2, session: 1, flight: 'A', squatRackHeight: '8', benchRackHeight: '3',
    platform: 'p001',
    divisions: [{ divisionId: 'd002', weightClassId: 'w002', score: null, forecastedScore: null, place: null, forecastedPlace: null, total: null, forecastedTotal: null }],
    lifts: makeLifts(120, 70, 140),
  },
  'ltf006': {
    id: 'ltf006', memberNumber: '12350', name: 'David Park', gender: 'M',
    team: 'Power Gym', state: 'FL', country: 'US', bodyWeight: 93.0,
    lot: 9, session: 1, flight: 'A', squatRackHeight: '12', benchRackHeight: '5',
    platform: 'p001',
    divisions: [{ divisionId: 'd001', weightClassId: 'w001', score: null, forecastedScore: null, place: null, forecastedPlace: null, total: null, forecastedTotal: null }],
    lifts: makeLifts(195, 135, 225),
  },
};

// Attempt order for squat round 1 (sorted by weight, then lot)
const lifterOrder = ['ltf004', 'ltf002', 'ltf006', 'ltf001', 'ltf003', 'ltf005'];

let currentLifterIndex = 0;

function buildAttemptRef(lifterId, liftName, attemptNumber) {
  return {
    id: lifters[lifterId].lifts[liftName][attemptNumber].id,
    liftName,
    attemptNumber: String(attemptNumber),
    lifter: { id: lifterId },
  };
}

function buildMeetState() {
  const currentLifterId = lifterOrder[currentLifterIndex];
  const nextAttempts = [];
  for (let i = currentLifterIndex + 1; i < lifterOrder.length && nextAttempts.length < 5; i++) {
    nextAttempts.push(buildAttemptRef(lifterOrder[i], 'squat', '1'));
  }

  return {
    name: 'LiftAlert Test Meet 2026',
    units: 'KG',
    lifters,
    teams: {
      'Iron House': { place: 1, points: 25 },
      'Barbell Club': { place: 2, points: 20 },
      'Power Gym': { place: 3, points: 18 },
      'Strength Co': { place: 4, points: 12 },
    },
    platforms: {
      'p001': {
        id: 'p001',
        name: 'Platform 1',
        clockState: 'initial',
        clockTimerLength: 60000,
        barAndCollarsWeight: 25,
        currentAttempt: {
          ...buildAttemptRef(currentLifterId, 'squat', '1'),
          ifSuccessfulPlaces: {},
          ifSuccessfulScores: {},
        },
        nextAttempts,
        refLights: {
          left: { decision: null },
          head: { decision: null },
          right: { decision: null },
        },
      },
    },
    divisions: {
      'd001': {
        id: 'd001', name: 'Open Men', scoreBy: 'DOTS',
        weightClasses: {
          'w001': { name: '93', maxWeight: 93 },
        },
      },
      'd002': {
        id: 'd002', name: 'Open Women', scoreBy: 'DOTS',
        weightClasses: {
          'w002': { name: '63', maxWeight: 63 },
        },
      },
    },
  };
}

// --- WebSocket Server ---

const wss = new WebSocket.Server({ port: PORT });

const clients = new Set();

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  const meetId = url.searchParams.get('meetId');
  const auth = url.searchParams.get('auth');
  const apiKey = url.searchParams.get('apiKey');

  console.log(`[CONNECT] meetId=${meetId}, apiKey=${apiKey ? '***' : 'none'}`);

  // Validate auth (Base64 of meetId:password)
  if (auth) {
    try {
      const decoded = Buffer.from(auth, 'base64').toString();
      const [authMeetId, authPassword] = decoded.split(':');
      if (authMeetId !== MEET_ID || authPassword !== MEET_PASSWORD) {
        console.log(`[AUTH] Invalid credentials: ${decoded}`);
        ws.send(JSON.stringify({ error: 'Invalid credentials' }));
        ws.close();
        return;
      }
    } catch (e) {
      // Allow connections without strict auth for testing
    }
  }

  clients.add(ws);

  // Send initial state
  const state = buildMeetState();
  ws.send(JSON.stringify(state));
  console.log(`[SEND] Initial state to client (${JSON.stringify(state).length} bytes)`);
  console.log(`[STATE] Current lifter: ${lifters[lifterOrder[currentLifterIndex]].name}`);
  logNextUp();

  // Handle pings
  ws.on('message', (msg) => {
    const text = msg.toString();
    if (text === 'ping') {
      ws.send('pong');
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log('[DISCONNECT] Client disconnected');
  });
});

function logNextUp() {
  const nextNames = [];
  for (let i = currentLifterIndex + 1; i < lifterOrder.length && nextNames.length < 3; i++) {
    nextNames.push(lifters[lifterOrder[i]].name);
  }
  console.log(`[STATE] Next up: ${nextNames.join(' -> ') || '(none)'}`);
}

function broadcast() {
  const state = buildMeetState();
  const data = JSON.stringify(state);
  for (const ws of clients) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(data);
    }
  }
}

// Simulate meet progression: advance lifter every 8 seconds
const ADVANCE_INTERVAL_MS = 8000;

setInterval(() => {
  if (clients.size === 0) return;

  // Mark current attempt as "good"
  const currentLifterId = lifterOrder[currentLifterIndex];
  lifters[currentLifterId].lifts.squat['1'].result = 'good';
  lifters[currentLifterId].lifts.squat['1'].decisions = {
    left: { decision: 'good', cards: {} },
    head: { decision: 'good', cards: {} },
    right: { decision: 'good', cards: {} },
  };

  // Advance to next lifter
  currentLifterIndex++;
  if (currentLifterIndex >= lifterOrder.length) {
    console.log('\n[MEET] All lifters completed squat round 1! Restarting...');
    // Reset for demo purposes
    currentLifterIndex = 0;
    for (const id of lifterOrder) {
      lifters[id].lifts.squat['1'].result = null;
      lifters[id].lifts.squat['1'].decisions = {
        left: { decision: null, cards: {} },
        head: { decision: null, cards: {} },
        right: { decision: null, cards: {} },
      };
    }
  }

  const newCurrentName = lifters[lifterOrder[currentLifterIndex]].name;
  console.log(`\n[ADVANCE] Now lifting: ${newCurrentName}`);
  logNextUp();

  broadcast();
}, ADVANCE_INTERVAL_MS);

console.log(`\n=== Mock LiftingCast WebSocket Server ===`);
console.log(`Port: ${PORT}`);
console.log(`Meet ID: ${MEET_ID}`);
console.log(`Password: ${MEET_PASSWORD}`);
console.log(`WebSocket URL: ws://localhost:${PORT}?meetId=${MEET_ID}&auth=${Buffer.from(`${MEET_ID}:${MEET_PASSWORD}`).toString('base64')}`);
console.log(`Lifter order (squat R1 by weight): ${lifterOrder.map(id => lifters[id].name).join(' -> ')}`);
console.log(`Advancing every ${ADVANCE_INTERVAL_MS / 1000}s`);
console.log(`\nWaiting for connections...\n`);
