/**
 * LiftingCast WebSocket Client
 *
 * Connects to either:
 *   - The mock server (default: ws://localhost:8080)
 *   - The real LiftingCast API (wss://backup.liftingcast.com/websocket)
 *
 * Monitors a specific lifter and detects when they are "on deck"
 * (within 2 positions of lifting).
 *
 * Configuration (env vars or CLI args):
 *   MEET_ID / --meet-id       Meet ID
 *   MEET_PASSWORD / --password Meet password
 *   API_KEY / --api-key        API key
 *   TRACK_LIFTER / --track     Lifter name to track (partial match, case-insensitive)
 *   LIFTINGCAST_URL / --url    Custom WebSocket URL
 *   PORT                       HTTP health check port (default: 3000)
 *
 * CLI-only options:
 *   --real                Use real LiftingCast API
 *   --list-lifters        Just list all lifters and exit
 */
const http = require('http');
const WebSocket = require('ws');

// --- Parse CLI arguments (local dev fallback) ---
const args = {};
for (const arg of process.argv.slice(2)) {
  if (arg.startsWith('--')) {
    const [key, ...rest] = arg.slice(2).split('=');
    args[key] = rest.length > 0 ? rest.join('=') : true;
  }
}

const MOCK_URL = 'ws://localhost:8080';
const REAL_URL = 'wss://backup.liftingcast.com/websocket';

// Environment variables take priority, CLI args as fallback
const meetId = process.env.MEET_ID || args['meet-id'] || 'mtest12345678';
const password = process.env.MEET_PASSWORD || args['password'] || 'testpass';
const apiKey = process.env.API_KEY || args['api-key'] || '';
const trackName = process.env.TRACK_LIFTER || args['track'] || '';
const listLifters = args['list-lifters'] || false;

let baseUrl;
if (process.env.LIFTINGCAST_URL) {
  baseUrl = process.env.LIFTINGCAST_URL;
} else if (args['url']) {
  baseUrl = args['url'];
} else if (args['real'] || process.env.MEET_ID) {
  // Auto-use real URL when MEET_ID env var is set (i.e. on Railway)
  baseUrl = REAL_URL;
} else {
  baseUrl = MOCK_URL;
}

// --- HTTP health check server (Railway requires a listening port) ---
const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', tracking: trackName || null, meetId }));
  } else {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('LiftAlert is running');
  }
});
server.listen(PORT, () => {
  console.log(`[HEALTH] HTTP server listening on port ${PORT}`);
});

const auth = Buffer.from(`${meetId}:${password}`).toString('base64');
const wsUrl = `${baseUrl}?meetId=${encodeURIComponent(meetId)}&auth=${encodeURIComponent(auth)}&apiKey=${encodeURIComponent(apiKey)}`;

// --- State ---
let lastCurrentLifterId = null;
let notifiedOnDeck = false;
let notifiedInTheHole = false;
let reconnectTimeout = 2000;
let pingInterval = null;

function connect() {
  console.log(`\n=== LiftingCast Client ===`);
  console.log(`Connecting to: ${baseUrl}`);
  console.log(`Meet ID: ${meetId}`);
  if (trackName) console.log(`Tracking: "${trackName}"`);
  console.log('');

  const ws = new WebSocket(wsUrl, {
    headers: {
      'Origin': 'https://liftingcast.com',
      'User-Agent': 'LiftAlert/1.0',
    },
    handshakeTimeout: 10000,
  });

  ws.on('open', () => {
    console.log('[CONNECTED]\n');
    reconnectTimeout = 2000;

    // Send heartbeat pings every 30s
    pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send('ping');
      }
    }, 30000);
  });

  ws.on('message', (data) => {
    const text = data.toString();

    // Ignore pong responses
    if (text === 'pong') return;

    try {
      const meet = JSON.parse(text);
      handleMeetUpdate(meet);
    } catch (e) {
      console.log(`[MSG] Non-JSON: ${text.substring(0, 200)}`);
    }
  });

  ws.on('error', (err) => {
    console.error(`[ERROR] ${err.message}`);
  });

  ws.on('close', (code, reason) => {
    console.log(`[DISCONNECTED] code=${code}`);
    if (pingInterval) clearInterval(pingInterval);

    // Reconnect with exponential backoff
    console.log(`[RECONNECT] Retrying in ${reconnectTimeout / 1000}s...`);
    setTimeout(connect, reconnectTimeout);
    reconnectTimeout = Math.min(reconnectTimeout * 2, 30000);
  });
}

function handleMeetUpdate(meet) {
  if (!meet.lifters || !meet.platforms) {
    console.log('[UPDATE] Received state but no lifters/platforms yet');
    return;
  }

  // List lifters mode
  if (listLifters) {
    console.log(`\n=== Lifters in "${meet.name}" ===\n`);
    const sorted = Object.values(meet.lifters).sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    for (const l of sorted) {
      console.log(`  ${l.name || '(unnamed)'} | ${l.team || ''} | ${l.gender || ''} | ${l.bodyWeight || ''}kg | Flight ${l.flight || '?'} | Session ${l.session || '?'}`);
    }
    console.log(`\nTotal: ${sorted.length} lifters`);
    process.exit(0);
  }

  // Process each platform
  for (const [platformId, platform] of Object.entries(meet.platforms)) {
    const currentAttempt = platform.currentAttempt;
    if (!currentAttempt) continue;

    const currentLifter = meet.lifters[currentAttempt.lifter.id];
    const currentName = currentLifter?.name || 'Unknown';

    // Log lifter change
    if (currentAttempt.lifter.id !== lastCurrentLifterId) {
      lastCurrentLifterId = currentAttempt.lifter.id;
      notifiedOnDeck = false;
      notifiedInTheHole = false;

      console.log(`[CURRENT] ${currentName} - ${currentAttempt.liftName} attempt ${currentAttempt.attemptNumber}`);

      // Show next lifters
      if (platform.nextAttempts && platform.nextAttempts.length > 0) {
        const upcoming = platform.nextAttempts.slice(0, 5).map((a, i) => {
          const lifter = meet.lifters[a.lifter.id];
          const label = i === 0 ? 'ON DECK' : i === 1 ? 'IN HOLE' : `#${i + 2}`;
          return `  ${label}: ${lifter?.name || 'Unknown'} (${a.liftName} ${a.attemptNumber})`;
        });
        console.log(upcoming.join('\n'));
      }
    }

    // Track specific lifter
    if (trackName) {
      const trackLower = trackName.toLowerCase();
      const nextAttempts = platform.nextAttempts || [];

      // Check if tracked lifter is currently lifting
      if (currentLifter?.name?.toLowerCase().includes(trackLower)) {
        console.log(`\n*** ALERT: ${currentLifter.name} IS LIFTING NOW! ***\n`);
      }

      // Check if tracked lifter is on deck (position 0)
      if (nextAttempts.length > 0) {
        const onDeckLifter = meet.lifters[nextAttempts[0].lifter.id];
        if (onDeckLifter?.name?.toLowerCase().includes(trackLower) && !notifiedOnDeck) {
          notifiedOnDeck = true;
          console.log(`\n*** ALERT: ${onDeckLifter.name} IS ON DECK (next to lift)! ***`);
          console.log(`*** >> This is when we would send the notification email ***\n`);
        }
      }

      // Check if tracked lifter is in the hole (position 1)
      if (nextAttempts.length > 1) {
        const inHoleLifter = meet.lifters[nextAttempts[1].lifter.id];
        if (inHoleLifter?.name?.toLowerCase().includes(trackLower) && !notifiedInTheHole) {
          notifiedInTheHole = true;
          console.log(`\n*** HEADS UP: ${inHoleLifter.name} is in the hole (2 lifters away) ***\n`);
        }
      }
    }
  }
}

connect();
