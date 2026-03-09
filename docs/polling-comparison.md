# LiftingCast vs SymPlmeet: Technical Implementation Comparison

## 1. Data Source & Protocol

### LiftingCast — CouchDB over HTTP

LiftingCast exposes a **public readonly CouchDB replica** per meet at `https://couchdb.liftingcast.com/{meetId}_readonly`. This is the same endpoint the spectator board reads from — no authentication, no API keys. The protocol is plain HTTPS with JSON responses, using Node's built-in `https` module directly (zero external dependencies).

CouchDB's HTTP API provides two critical primitives the system relies on:
- `GET /{db}/_all_docs?include_docs=true` — bulk fetch every document in one request
- `GET /{db}/_changes?feed=longpoll&since={seq}&include_docs=true&timeout=60000` — incremental change feed

The `_changes` endpoint is CouchDB's killer feature here: it maintains a monotonically increasing **sequence counter** (`update_seq`) and can return only documents that changed since a given sequence. This turns what would be a polling problem into an event-driven architecture over HTTP.

### SymPlmeet — REST + Socket.IO over WebSocket

SymPlmeet lives at `https://symplmeet.plmeet.com` and uses a **dual-channel approach**: REST for initial state, Socket.IO for live updates. Two notable quirks:

1. **Self-signed TLS certificate** — requires `rejectUnauthorized: false` on every HTTPS request and on the Socket.IO connection. This bypasses certificate validation entirely, meaning the connection is encrypted but not authenticated. A man-in-the-middle could intercept traffic without detection.
2. **Explicit dependency on `socket.io-client`** — this is the only npm dependency the SymPlmeet integration adds. Socket.IO negotiates WebSocket upgrade from HTTP, handles heartbeats, and provides room-based pub/sub (`subscribeToMeet`).

### Tradeoffs

| Factor | LiftingCast (CouchDB) | SymPlmeet (REST + Socket.IO) |
|---|---|---|
| Dependencies | Zero (stdlib `https`) | `socket.io-client` (~300KB) |
| Auth model | None needed (public replica) | None needed, but self-signed cert |
| Connection security | Full TLS verification | TLS with `rejectUnauthorized: false` |
| Server coupling | Low — standard CouchDB API | High — proprietary API + Socket.IO events |
| Offline resilience | Stateless reconnect via `since={seq}` | Socket.IO auto-reconnect, but state must be re-fetched |

**Improvement opportunity**: The SymPlmeet self-signed cert is a security concern. If the server ever provides a valid cert, removing `rejectUnauthorized: false` would restore proper TLS verification. Alternatively, pinning the server's specific certificate fingerprint would allow verification without trusting a CA.

---

## 2. Real-Time Update Mechanism

This is the most significant architectural difference between the two integrations.

### LiftingCast — CouchDB Long-Polling (`_changes` feed)

The update loop is a **pull-based, long-poll cycle** in `watchChanges()`:

```
1. GET /{db} → capture update_seq as startSeq
2. GET /{db}/_all_docs?include_docs=true → bulk load all docs
3. Loop forever:
   GET /{db}/_changes?feed=longpoll&since={lastSeq}&timeout=60000&include_docs=true
   → blocks up to 60s, returns immediately when any doc changes
   → process only the changed docs
   → update lastSeq
```

Key implementation details:

- **Sequence tracking** (`lastSeq`): CouchDB's `update_seq` is an opaque token (not necessarily a number). On reconnect, the client resumes from the exact point it left off — no missed updates, no duplicate processing.
- **Race condition mitigation**: The code captures `update_seq` *before* calling `_all_docs`. Any writes that arrive during the bulk load will have a sequence > `startSeq`, so the first `_changes` call will replay them. Since `processDoc()` is idempotent (it just overwrites the in-memory map entry), this is safe.
- **Selective processing**: After receiving changes, only platform/lifter/attempt changes (`p*`, `l*`, `a*` prefixes) trigger `checkPlatforms()`. Division and meet metadata changes are stored but don't trigger notification recalculation.
- **Exponential backoff**: On error, retry delay doubles from 2s → 4s → 8s → ... capped at 30s. Resets to 2s on successful poll.
- **Timeout budget**: The `_changes` request has a CouchDB-side timeout of 60s and a client-side timeout of 90s, providing a 30s buffer for network delays.

### SymPlmeet — Socket.IO Push

The update flow is **push-based** via WebSocket:

```
1. GET /api/getSocketData/{meetId} → initial state load
2. io(SYMPLMEET_BASE) → open Socket.IO connection
3. socket.emit('subscribeToMeet', meetId) → join meet room
4. socket.on('update', callback) → receive full state on every change
```

Key implementation details:

- **Full state replacement**: Every `update` event contains the complete meet state. `normalizeSymPlmeetData()` clears `meetState.lifters`, `meetState.attempts`, and `meetState.platforms` before rebuilding from scratch. There is no diffing.
- **Socket.IO reconnection**: Configured with `reconnectionDelay: 2000` (initial) and `reconnectionDelayMax: 30000`. Socket.IO handles reconnection automatically, including re-establishing the WebSocket transport.
- **Transport fallback**: Configured with `transports: ['websocket', 'polling']` — prefers WebSocket but falls back to HTTP long-polling if WebSocket upgrade fails (e.g., behind a corporate proxy).

### Bandwidth & Latency Comparison

| Metric | LiftingCast | SymPlmeet |
|---|---|---|
| Initial load | ~1 request (`_all_docs`) + 1 metadata request | 1 request (`getSocketData`) |
| Per-update payload | Only changed documents (typically 1-3 docs, ~0.5-2KB) | Full meet state (~50-200KB for a 60-lifter meet with all attempts) |
| Update latency | Long-poll return latency (~50-200ms after server write) | WebSocket push (~10-50ms after server write) |
| Idle bandwidth | One HTTP connection held open, ~0 bytes/sec | WebSocket heartbeat packets (~40 bytes every 25s) |
| Network interruption recovery | Resume from `lastSeq` — zero data loss | Socket.IO reconnects, but must re-fetch full state or rely on next `update` event |

**Tradeoff analysis**: SymPlmeet's push model delivers lower latency per-update (~10-50ms vs ~50-200ms), but at the cost of significantly higher bandwidth per update. For a 60-lifter meet with 540 attempts (60 × 3 lifts × 3 attempts), every single weight change sends the entire dataset. LiftingCast sends only the one attempt document that changed.

Over a full meet day (~8 hours, ~1000 attempt changes), approximate bandwidth:
- **LiftingCast**: ~1000 × 1KB = ~1MB total
- **SymPlmeet**: ~1000 × 100KB = ~100MB total

**Improvement opportunities**:
1. SymPlmeet's full-state-on-every-update is inherently wasteful. A diff-based protocol (like CouchDB's `_changes`) would reduce bandwidth by ~99%. However, this requires server-side changes.
2. On the client side, the `normalizeSymPlmeetData()` function could diff incoming state against previous state before triggering `checkPlatforms()`, avoiding unnecessary notification recalculation when irrelevant fields change (e.g., a lifter's bodyweight is updated but no attempt changed).
3. LiftingCast's long-poll could be replaced with CouchDB's `_changes?feed=continuous` (server-sent events style) to eliminate the per-poll HTTP overhead. However, long-poll is more reliable across proxies and load balancers.
4. Both integrations could benefit from a **WebSocket adapter for CouchDB** (e.g., using `_changes?feed=eventsource`) if CouchDB's EventSource support is enabled, combining LiftingCast's efficient diffs with SymPlmeet's push latency.

---

## 3. Data Model

### LiftingCast — Normalized, Document-Per-Entity

CouchDB stores each entity as a separate JSON document with a typed `_id` prefix:

| Prefix | Entity | Example `_id` | Key Fields |
|---|---|---|---|
| `m` | Meet | `m4hj5qja463e` | `name`, `date`, `dateFormat` |
| `p` | Platform | `p0` | `name`, `currentAttemptId` |
| `l` | Lifter | `l0jd58nr3af4` | `name`, `lot`, `session`, `flight`, `platformId`, `bodyWeight`, `gender` |
| `d` | Division | `d{id}` | Division metadata |
| `a` | Attempt | `a1d-l0jd58nr3af4` | `liftName`, `attemptNumber`, `lifterId`, `weight`, `result`, `endOfRound`, `changes` |

Attempt IDs encode semantics: `a{attemptNumber}{liftInitial}-{lifterId}` where `s`=squat, `b`=bench, `d`=deadlift. The `parseAttemptId()` function extracts these components via regex.

The `platform.currentAttemptId` field is the single source of truth for "who is currently lifting." When this pointer changes, the system knows the bar has been loaded for a new lifter.

Attempts have a `changes` array (audit log) and top-level `weight`/`result` fields (current values). The `result` field is `"good"`, `"bad"`, or absent/null for pending attempts.

### SymPlmeet — Denormalized, Lifter-Centric

SymPlmeet's API returns a monolithic JSON blob with three top-level keys:

```json
{
  "meetInfo": { "meetName": "...", ... },
  "results": { "A": [lifters...], "B": [lifters...] },
  "liftingOrderThisRound": [{ "lifterId": "...", "round": "sq1" }, ...]
}
```

Attempts are **flat fields on each lifter object**: `sq1`, `sq1res`, `sq2`, `sq2res`, `bp1`, `bp1res`, `dl1`, `dl1res`, etc. Each lifter carries all 18 data points (9 weights + 9 results) as scalar fields.

Results use a numeric encoding: `1` = good, `-1` = bad, `0` = pending.

There is no concept of a platform document. A single virtual platform `sp-default` is fabricated.

The `currentLift` field (or `liftingOrderThisRound[0]`) indicates who is currently lifting, but it uses a different format than LiftingCast: `{ "lifterId": "123", "round": "sq1" }` instead of a direct attempt pointer.

### Normalization Layer

All downstream logic (attempt ordering, on-deck detection, email notifications) is written against LiftingCast's normalized model. `normalizeSymPlmeetData()` transforms SymPlmeet's flat structure into discrete objects:

| Concept | LiftingCast native ID | SymPlmeet synthetic ID | Transform |
|---|---|---|---|
| Lifter | `l0jd58nr3af4` | `sl-{rawId}` | Prefix swap, field mapping |
| Attempt | `a1d-l0jd58nr3af4` | `sa-dl1-{rawId}` | Explode 18 flat fields → 9 attempt objects |
| Platform | `p{id}` | `sp-default` | Fabricated single platform |
| Result | `"good"` / `"bad"` / `null` | `1` / `-1` / `0` | Map via `RESULT_MAP` |

The `sl-`/`sa-`/`sp-` prefixes prevent ID collisions if both platforms are active simultaneously.

### Tradeoffs

**Normalized (LiftingCast)**:
- Fine-grained updates: changing one attempt weight sends ~200 bytes
- Referential integrity via `currentAttemptId` pointer — unambiguous "current lifter"
- More documents to manage (~60 lifters + ~540 attempts + platforms + divisions ≈ 600+ docs)
- Requires client-side joins (lifter → attempts → platform)

**Denormalized (SymPlmeet)**:
- Simple to understand: one lifter object = everything about that lifter
- No joins needed for display
- Wasteful for updates: any change re-sends everything
- Ambiguous state: `currentLift` field has multiple possible formats (round string, liftType+attemptNumber, etc.) requiring defensive parsing
- API response format is inconsistent — `results` can be an array of flights, an object keyed by flight letter, or a nested structure. The normalization code has 4 separate parsing branches to handle this.

**Improvement opportunity**: The SymPlmeet normalization layer is fragile due to API format inconsistency. Adding schema validation (e.g., a lightweight JSON schema check) before normalization would catch format changes early and produce actionable error messages instead of silent data corruption.

---

## 4. State Management

### LiftingCast — Incremental Merge

State is managed as an in-memory map (`meets[meetId]`) with five sub-maps:

```js
{ meet: {}, platforms: {}, lifters: {}, divisions: {}, attempts: {}, lastSeq: '0', trackState: {} }
```

`processDoc()` routes each incoming document to the correct sub-map by checking `_id` prefix. This is a simple key-value overwrite — the latest version of each document wins. CouchDB guarantees that `_changes` delivers documents in causal order, so this is safe.

The merge is **O(1) per changed document** — no scanning, no diffing, no garbage collection.

### SymPlmeet — Full State Replacement

Every `update` event triggers a complete rebuild:

```js
meetState.lifters = {};   // clear
meetState.attempts = {};  // clear
meetState.platforms = {};  // clear
// ... rebuild from scratch
```

This is **O(L × 9)** per update where L is the number of lifters (9 attempts per lifter). For a 60-lifter meet, that's 540 attempt object allocations per update.

The full replacement also means any client-side computed state (like `bestLifts` cache) is rebuilt every time, even if the underlying data hasn't changed.

### Tradeoffs

| Factor | LiftingCast (merge) | SymPlmeet (replace) |
|---|---|---|
| CPU per update | O(1) — single map write | O(L × 9) — full rebuild |
| Memory churn | Minimal — overwrite in place | High — GC pressure from discarded objects |
| Correctness guarantee | Monotonic sequence ensures no missed updates | Full state snapshot ensures eventual consistency |
| Staleness risk | None (server tracks sequence) | If a WebSocket message is dropped, stale until next `update` |
| Partial failure | Handles gracefully — missed changes are replayed from `lastSeq` | No partial state — either full snapshot or nothing |

**Improvement opportunities**:
1. SymPlmeet could diff incoming state against previous state before clearing. A simple hash or version comparison on the `results` field would skip unnecessary rebuilds.
2. The current implementation creates new objects for every lifter and attempt on every update. Object pooling or structural sharing (reusing unchanged lifter/attempt objects) would reduce GC pressure.
3. Both integrations keep all meet state in memory indefinitely. For long-running deployments monitoring many meets, stale meet data should be evicted more aggressively. Currently, cleanup only runs every 2 minutes in the poll loop.

---

## 5. Attempt Ordering

### LiftingCast — Client-Side 7-Way Sort

`computeAttemptOrder()` implements the official powerlifting attempt order:

```
1. Session (ascending)
2. Lift type (squat=0 → bench=1 → deadlift=2)
3. Flight (alphabetical: A → B → C)
4. Attempt number (1 → 2 → 3)
5. endOfRound flag (0 before 1)
6. Weight (ascending — lighter lifters go first)
7. Lot number (tiebreaker — lower lot goes first)
```

This runs on every `checkPlatforms()` call. It filters to pending attempts only (no result, positive weight, not 4th attempts), then sorts. The sort is **O(n log n)** where n is the number of pending attempts on the platform.

For a single-flight meet in the squat round with 15 pending lifters, that's ~15 × log₂(15) ≈ 60 comparisons — trivial.

### SymPlmeet — Server-Provided Order with Client Fallback

SymPlmeet provides a pre-computed `liftingOrderThisRound` array from the server. This is injected into the virtual platform as `_liftingOrder`:

```js
meetState.platforms['sp-default']._liftingOrder = liftingOrder.map(entry => {
  // convert to synthetic attempt IDs
  return `sa-${prefix}${attemptNum}-${entry.lifterId}`;
}).filter(Boolean);
```

The client-side `computeAttemptOrder()` also works for SymPlmeet data because the normalized attempt objects have the same fields — so if `_liftingOrder` is missing or empty, the standard 7-way sort is the fallback.

### Tradeoffs

**Client-computed (LiftingCast)**:
- Full control over sort logic — can be debugged and tested locally
- Always reflects the current in-memory state
- Must be kept in sync with LiftingCast's server-side ordering rules

**Server-provided (SymPlmeet)**:
- Guaranteed to match what the official scoreboard shows
- Less CPU work on the client
- Opaque — if the order seems wrong, you can't inspect the logic
- Only covers "this round" — doesn't provide a global order across rounds

**Improvement opportunity**: Currently `computeAttemptOrder()` doesn't use `_liftingOrder` at all — it always runs the full 7-way sort regardless of platform. The pre-computed order could be used as an optimization: if `_liftingOrder` is present and the attempt IDs all exist in `meetState.attempts`, use it directly and skip the sort. Fall back to the computed sort only when the pre-computed order is stale or missing.

---

## 6. Reconnection & Error Handling

### LiftingCast

Manual exponential backoff in `watchChanges()`:

```js
let retryDelay = 2000;
while (!shuttingDown && watchingMeets.has(meetId)) {
  try {
    const changes = await fetchJSON(url, { timeout: 90000 });
    // ... process changes ...
    retryDelay = 2000; // reset on success
  } catch (err) {
    await new Promise(r => setTimeout(r, retryDelay));
    retryDelay = Math.min(retryDelay * 2, 30000);
  }
}
```

On reconnect, the client resumes from `lastSeq` — CouchDB guarantees no data loss between the last successful poll and the reconnect. This is the strongest consistency guarantee either integration offers.

Failure modes:
- **Network timeout**: 90s client timeout, caught and retried
- **HTTP error (4xx/5xx)**: Caught by `fetchJSON`, retried with backoff
- **Invalid JSON**: Caught by `JSON.parse` in `fetchJSON`, retried
- **Meet deleted**: Would return 404 indefinitely — the 30s max backoff caps the retry frequency, but there's no circuit breaker

### SymPlmeet

Socket.IO's built-in reconnection:

```js
const socket = io(SYMPLMEET_BASE, {
  reconnection: true,
  reconnectionDelay: 2000,
  reconnectionDelayMax: 30000,
});
```

Socket.IO handles:
- Automatic reconnection with jittered exponential backoff
- Transport downgrade (WebSocket → HTTP long-polling)
- Heartbeat-based connection liveness detection (default: 25s ping interval, 20s timeout)

Failure modes:
- **Network interruption**: Socket.IO reconnects automatically. However, updates that occurred during the disconnection may be lost — the server doesn't replay missed events.
- **Server restart**: Socket disconnects with reason `"transport close"`, reconnects, re-emits `subscribeToMeet`. The next `update` event delivers full state, so consistency is restored.
- **Self-signed cert renewal**: If the server changes its certificate, all connections fail until the client restarts. No way to recover dynamically.

### Tradeoffs

| Factor | LiftingCast | SymPlmeet |
|---|---|---|
| Data loss on reconnect | None — `lastSeq` guarantees replay | Possible — missed events between disconnect and reconnect |
| Recovery complexity | Custom code (~15 lines) | Zero (Socket.IO built-in) |
| Liveness detection | Implicit (60s long-poll timeout) | Explicit (25s heartbeat) |
| Stale connection detection | Slow — up to 90s before timeout fires | Fast — 45s (25s ping + 20s timeout) |
| Max retry delay | 30s | 30s |

**Improvement opportunities**:
1. Add a **circuit breaker** for LiftingCast: after N consecutive failures (e.g., 10), log a warning and back off to 5-minute retries instead of hammering a dead endpoint every 30s.
2. SymPlmeet should **re-fetch full state on reconnect** to close the gap where events may have been missed during disconnection. Currently it relies on the next `update` event arriving eventually, but if no changes happen after reconnect, the client could be stale indefinitely.
3. Add **health metrics** tracking reconnection frequency, average latency per update, and data staleness (time since last successful update) for operational monitoring.

---

## 7. Meet Discovery & Lifecycle

### LiftingCast

Discovery polls `https://liftingcast.com/api/meets` and filters by `isMeetToday()`:

```js
function isMeetToday(meetEntry) {
  // Match today AND tomorrow to handle UTC/local timezone differences
  return meetDate === today || meetDate === tomorrow;
}
```

The today+tomorrow window handles the case where the server runs in UTC but meets are scheduled in local time (e.g., a US meet on March 8 needs to be discoverable when UTC is still March 7 evening).

Meets are indexed in batches of 5 (`BATCH_SIZE = 5`) to avoid overwhelming CouchDB with concurrent `_all_docs` requests.

### SymPlmeet

Discovery calls `GET /api/todayMeets` — the server handles the date filtering. Results are tracked in `activeSymPlmeetIds`:

```js
let activeSymPlmeetIds = new Set();
// ... on each poll:
activeSymPlmeetIds = new Set(symplMeets.map(m => m.id));
```

A SymPlmeet meet is considered stale when it disappears from this set. This is simpler but also means the client is entirely dependent on the server's definition of "today."

### Lifecycle (Both Platforms)

The `pollForNewMeets()` loop runs every 2 minutes and:

1. **Discovers** today's meets from both APIs (indexes them for autocomplete/search)
2. **Starts** changes feeds/socket connections for meets with active Postgres subscriptions
3. **Activates** future meets whose date has arrived (`isMeetReady()`)
4. **Cleans up** stale meets — stops changes feeds, disconnects sockets, removes from `watchingMeets`
5. **Auto-subscribes** persistent followers when a new meet is indexed and their tracked lifter appears in the roster

### Tradeoffs

| Factor | LiftingCast | SymPlmeet |
|---|---|---|
| Discovery frequency | Every 2 minutes | Every 2 minutes |
| Date logic | Client-side (today + tomorrow window) | Server-side (`/api/todayMeets`) |
| Timezone handling | Explicit today+tomorrow buffer | Opaque — depends on server's timezone |
| Staleness detection | Compare meet date against today/tomorrow | Check membership in `activeSymPlmeetIds` set |
| Future meet support | Yes — loads data, waits for date | No — only "today" meets from API |

**Improvement opportunities**:
1. The 2-minute discovery interval is a fixed constant. During active meet hours (Saturday mornings), more frequent discovery (30s) would pick up late-starting meets faster. During off-hours, less frequent polling (10 minutes) would reduce API load.
2. LiftingCast's `isMeetToday()` uses a 2-day window which could match meets from yesterday that haven't been cleaned up. Adding a "meet has ended" signal (e.g., all lifters have completed all attempts) would enable more precise cleanup.
3. The batch indexing (`BATCH_SIZE = 5`) could be adaptive — start with larger batches on startup, throttle down during steady-state operation to avoid impacting live meet polling.

---

## 8. Platform Detection

The routing logic is a single regex check:

```js
function getMeetPlatform(meetId) {
  return /^\d+$/.test(String(meetId)) ? 'symplmeet' : 'liftingcast';
}
```

All-numeric IDs → SymPlmeet. Alphanumeric IDs → LiftingCast. This is simple and works today but is brittle — if either platform changes its ID format, the heuristic breaks silently.

**Improvement opportunity**: Store the platform type explicitly when a meet is first discovered (during `discoverTodaysMeets()`), rather than inferring it from the ID format at every call site. This would also enable supporting additional platforms in the future without modifying the detection heuristic.

---

## 9. Notification Pipeline

Both platforms feed into the same notification path:

```
checkPlatforms(meetId)
  → computeAttemptOrder(meetState, platformId)
  → determine current/on-deck/in-the-hole lifters
  → notifySubscribers(meetId, lifterName, liftName, position, details)
    → getCachedSubscriptions(meetId)  [30s TTL Postgres cache]
    → sendOnDeckEmail(...)  [Resend API with dedup]
```

The deduplication layer uses per-platform `Set` objects (`notifiedOnDeck`, `notifiedInTheHole`, `notifiedLifting`) that reset when `currentAttemptId` changes. This ensures one email per position per lifter per attempt change.

The Postgres subscription cache (`SUBS_CACHE_TTL = 30_000`) avoids a database round-trip on every CouchDB change event. For LiftingCast, which may fire dozens of change events per second during active lifting, this reduces Postgres load by ~99%.

### Tradeoffs specific to update mechanisms

- **LiftingCast**: `checkPlatforms()` fires only when a relevant document changes (platform, lifter, or attempt). False triggers are possible but rare (e.g., a lifter's bodyweight is updated).
- **SymPlmeet**: `checkPlatforms()` fires on *every* `update` event, even if nothing relevant changed. The full state replacement means we can't tell what changed without diffing.

**Improvement opportunities**:
1. Add a **change-detection wrapper** for SymPlmeet: before calling `checkPlatforms()`, compare the new `currentAttemptId` against the previous one. Skip if unchanged.
2. The subscription cache TTL (30s) means a new subscription takes up to 30s to take effect. For better UX, invalidate the cache immediately when a subscription is added/removed via the HTTP API (the code already does `delete subsCache[meetId]` for auto-subscriptions but not for manual ones in all paths).
3. The `computeAttemptOrder()` call is O(n log n) and runs on every relevant change. For LiftingCast, where changes are granular, caching the sorted order and invalidating only when an attempt's weight/result changes would avoid redundant sorts.

---

## Summary Table

| Aspect | LiftingCast | SymPlmeet |
|---|---|---|
| **Transport** | HTTP long-poll (CouchDB `_changes`) | WebSocket (Socket.IO) |
| **External dependencies** | None (stdlib `https`) | `socket.io-client` |
| **Update granularity** | Individual changed docs (~0.5-2KB) | Full state snapshot (~50-200KB) |
| **Update latency** | ~50-200ms (long-poll return) | ~10-50ms (WebSocket push) |
| **Bandwidth efficiency** | ~1MB/meet/day | ~100MB/meet/day |
| **Data model** | Normalized (doc-per-entity, 600+ docs) | Denormalized (flat fields on lifter) |
| **State update cost** | O(1) per doc | O(L × 9) full rebuild |
| **Attempt ordering** | Client-computed 7-way sort | Server-provided + client fallback |
| **Reconnection** | Manual exponential backoff, zero data loss | Socket.IO built-in, possible missed events |
| **Stale detection** | 90s timeout | 45s heartbeat |
| **Meet ID format** | Alphanumeric (`m4hj5qja463e`) | Numeric (`12345`) |
| **TLS** | Full verification | `rejectUnauthorized: false` |
| **Consistency guarantee** | Sequential consistency (CouchDB sequence counter) | Eventual consistency (last snapshot wins) |

---

## Prioritized Improvement Roadmap

1. **SymPlmeet change detection** — Compare `currentAttemptId` before/after each update to skip unnecessary `checkPlatforms()` calls. Low effort, immediate CPU/notification savings.
2. **SymPlmeet reconnect state refresh** — Re-fetch full state via REST after Socket.IO reconnection to guarantee no stale data. Medium effort, fixes a real consistency gap.
3. **Circuit breaker for LiftingCast** — Cap retry attempts and degrade gracefully for deleted/unreachable meets. Low effort, prevents resource waste.
4. **Explicit platform type storage** — Record platform type at discovery time instead of inferring from ID format. Low effort, better extensibility.
5. **Adaptive discovery interval** — Poll more frequently during meet hours, less frequently overnight. Medium effort, reduces unnecessary API calls.
6. **Subscription cache invalidation** — Immediately invalidate on manual subscription changes for better UX. Low effort, already partially implemented.
7. **SymPlmeet state diffing** — Diff incoming state to avoid full rebuild when nothing meaningful changed. Medium effort, significant GC pressure reduction.
