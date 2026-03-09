# LiftingCast vs SymPlmeet: Implementation Comparison

## 1. Data Source & Protocol

**LiftingCast** uses **CouchDB** — each meet has its own database at `https://couchdb.liftingcast.com/{meetId}_readonly`. This is a full-fledged document database exposed over HTTP. No auth needed; it's the same public readonly replica the spectator board uses.

**SymPlmeet** uses a **REST API + Socket.IO** combo at `https://symplmeet.plmeet.com`. It requires `rejectUnauthorized: false` (self-signed cert). The `socket.io-client` npm package is an explicit dependency.

## 2. Polling / Real-Time Updates

This is the biggest architectural difference:

### LiftingCast — CouchDB Long-Polling (`_changes` feed)
- On startup, it fetches *all* documents in one shot via `_all_docs?include_docs=true` and records the `update_seq` (a sequence counter CouchDB maintains).
- It then enters an infinite loop calling `_changes?feed=longpoll&since={lastSeq}&timeout=60000`. This is a **long-poll**: the HTTP request hangs open for up to 60 seconds and returns immediately when *any* document changes. The server pushes only the changed docs, not the full state.
- Each changed doc is processed individually by `processDoc()` — it checks the `_id` prefix (`a` = attempt, `l` = lifter, `p` = platform, etc.) and merges it into the in-memory `meetState`.
- If a change touches a platform, lifter, or attempt doc, it triggers `checkPlatforms()` to re-evaluate who's on deck.
- On error, it uses exponential backoff (2s → 4s → 8s → ... → 30s max), then reconnects.

### SymPlmeet — Socket.IO Push
- On startup, it does a one-time REST fetch: `GET /api/getSocketData/{meetId}` to load the full meet state.
- Then it opens a **persistent WebSocket** (via Socket.IO) and emits `subscribeToMeet` to join that meet's room.
- The server pushes `update` events through the socket whenever anything changes. The callback receives a complete data payload (not a diff).
- Socket.IO handles reconnection automatically (configured with `reconnectionDelay: 2000`, max `30000`).

**Key difference**: LiftingCast gets **incremental diffs** (only changed docs) via long-polling. SymPlmeet gets **full state snapshots** on every update via WebSocket push. Long-polling is pull-based (your code decides when to ask); Socket.IO is push-based (the server decides when to send).

## 3. Data Model

**LiftingCast** has a **normalized, document-per-entity** model:
- Every entity is its own CouchDB document with an `_id` prefix: `m` = meet, `p` = platform, `l` = lifter, `d` = division, `a` = attempt.
- Attempts are **separate documents** with fields like `liftName`, `attemptNumber`, `lifterId`, `weight`, `result`, `endOfRound`.
- The platform doc has a `currentAttemptId` pointer — when this changes, you know the current lifter has changed.
- Attempt IDs encode semantics: `a1d-l0jd58nr3af4` = attempt 1, deadlift, for lifter `l0jd58nr3af4`.

**SymPlmeet** has a **denormalized, lifter-centric** model:
- The API returns everything in one blob: `meetInfo`, `results` (containing flights → lifters), and `liftingOrderThisRound`.
- Attempt data is **embedded as flat fields on each lifter**: `sq1`, `sq1res`, `sq2`, `sq2res`, `bp1`, `bp1res`, `dl1`, `dl1res`, etc.
- There's no concept of separate platform documents — a single virtual platform (`sp-default`) is fabricated.
- `normalizeSymPlmeetData()` explodes these flat fields into discrete attempt objects to match LiftingCast's shape.

## 4. Normalization Layer

The entire downstream pipeline (attempt ordering, on-deck detection, email notifications) is written against LiftingCast's data model. SymPlmeet data gets **normalized into that same shape** by `normalizeSymPlmeetData()`:

| Concept | LiftingCast native ID | SymPlmeet synthetic ID |
|---|---|---|
| Lifter | `l0jd58nr3af4` | `sl-{rawId}` |
| Attempt | `a1d-l0jd58nr3af4` | `sa-dl1-{rawId}` |
| Platform | `p{id}` | `sp-default` |

This means `checkPlatforms()`, `computeAttemptOrder()`, `notifySubscribers()`, etc. all work identically regardless of source — they just see the unified `meetState` object.

## 5. Meet Discovery

**LiftingCast**: Polls `https://liftingcast.com/api/meets` periodically, filters by `isMeetToday()` (matches today AND tomorrow to handle UTC/local timezone differences). All of today's meets get indexed for autocomplete in the subscription UI.

**SymPlmeet**: Calls `GET /api/todayMeets` to get a list of active meets. The result is tracked in `activeSymPlmeetIds` — if a meet disappears from this list, it's considered "stale" and eligible for cleanup.

## 6. Platform Detection

Dead simple — `getMeetPlatform(meetId)`: if the meet ID is all digits (`/^\d+$/`), it's SymPlmeet. Otherwise, it's LiftingCast (which uses alphanumeric IDs like `m4hj5qja463e`).

## 7. Attempt Ordering

**LiftingCast**: `computeAttemptOrder()` applies the full 7-way sort: session → lift type → flight → attempt number → endOfRound → weight → lot number. All the data needed is in the discrete attempt + lifter docs.

**SymPlmeet**: The server provides a pre-computed `liftingOrderThisRound` array. This gets injected into the platform as `_liftingOrder`. However, `computeAttemptOrder()` also works because the normalized attempt objects have the same fields — so it can fall back to the standard sort if the pre-computed order isn't available.

## 8. Lifecycle & Cleanup

Both platforms are managed through the same `startMeet()` / `pollForNewMeets()` cycle. Every 2 minutes the poller:
1. Discovers today's meets from both APIs
2. Starts changes feeds / socket connections for meets with active subscriptions
3. Cleans up stale meets — for LiftingCast, a meet is stale if its date is no longer today/tomorrow; for SymPlmeet, it's stale if it's no longer in `/api/todayMeets`.

## Summary Table

| Aspect | LiftingCast | SymPlmeet |
|---|---|---|
| Transport | HTTP long-poll (CouchDB `_changes`) | WebSocket (Socket.IO) |
| Update granularity | Individual changed docs (incremental) | Full state snapshot (replace all) |
| Data model | Normalized (doc-per-entity) | Denormalized (flat fields on lifter) |
| Attempt ordering | Computed client-side (7-way sort) | Server-provided + client fallback |
| Reconnection | Manual exponential backoff | Socket.IO built-in |
| Meet ID format | Alphanumeric (`m4hj5qja463e`) | Numeric (`12345`) |
| State management | Merge individual docs into `meetState` | Full replace of `meetState` on every update |
| External dependency | None (raw `https`) | `socket.io-client` npm package |
