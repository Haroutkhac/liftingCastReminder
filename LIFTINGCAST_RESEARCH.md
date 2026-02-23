# LiftingCast.com - Architecture & API Research

## Overview

LiftingCast is the most widely used powerlifting scoring system and meet-direction software. It manages meets from registration to awards with real-time scoring, referee lights, clock management, and spectator views.

## Tech Stack

- **Frontend**: React + Redux + PouchDB (single-page application)
- **Backend**: Ruby on Rails (minimal API)
- **Database**: CouchDB (with PouchDB client-side replication)
- **Real-time sync**: PouchDB changes feed + WebSocket API
- **Hosting**: Cloud-based with optional local relay server (Docker)

## URL Structure

```
liftingcast.com/                                              # Homepage
liftingcast.com/about                                         # About page
liftingcast.com/coach                                         # Coach view
liftingcast.com/instructions                                  # Instructions
liftingcast.com/changes.html                                  # Changelog
liftingcast.com/meets/{meetId}/registration                   # Meet registration
liftingcast.com/meets/{meetId}/lifter/{lifterId}              # Individual lifter page
liftingcast.com/meets/{meetId}/platforms/{platformId}/board    # Attempt board (spectator view)
```

## Authentication

Three credentials are needed:
1. **API Key** - LiftingCast account API key
2. **Meet ID** - Unique meet identifier (e.g., `mfmnsrd1fve8`)
3. **Meet Password** - Per-meet password set during meet setup

## WebSocket API

### Connection
```
wss://backup.liftingcast.com/websocket?meetId={meetId}&auth={base64(meetId:password)}
```
- Auth is Base64-encoded `meetId:password`
- Local relay alternative: `ws://{localIP}/websocket`
- Heartbeat: ping/pong every 30 seconds
- Reconnection with exponential backoff (starting 2000ms)

### Response Format (`MeetApiResponse`)
The WebSocket sends the full meet state as JSON with this structure:
```typescript
{
  meetName: string;
  units: string;                    // "kg" or "lbs"
  platforms: Record<string, Platform>;
  lifters: Record<string, Lifter>;
  divisions: Record<string, Division>;
  weightClasses: Record<string, WeightClass>;
  teams: Record<string, Team>;
}
```

## CouchDB Document Types

Documents are prefixed by type:

| Prefix | Type             | Description                        |
|--------|------------------|------------------------------------|
| `a`    | Attempt          | Individual lift attempt             |
| `d`    | Division         | Competition division                |
| `e`    | Entry            | Meet entry/registration             |
| `l`    | Lifter           | Lifter profile                      |
| `m`    | Meet             | Meet configuration                  |
| `p`    | Platform         | Competition platform                |
| `r`    | Ref              | Referee                            |
| `s`    | Restricted Lifter| Restricted lifter view              |
| `n`    | Restricted Meet  | Restricted meet view                |
| `w`    | Weight Class     | Weight class definition             |

## Key Data Models

### Lifter
```typescript
{
  id: string;
  memberNumber: string;
  name: string;
  gender: string;
  team: string;
  state: string;
  country: string;
  bodyWeight: number;
  lot: number;              // Random lot number for ordering tiebreaks
  session: number;          // Session number
  flight: string;           // Flight (A, B, C, etc.)
  platformId: string;       // Which platform they're competing on
  divisions: LifterDivision[];  // Division entries with scores/places
  lifts: {
    squat:  { "1": Attempt, "2": Attempt, "3": Attempt },
    bench:  { "1": Attempt, "2": Attempt, "3": Attempt },
    dead:   { "1": Attempt, "2": Attempt, "3": Attempt }
  }
}
```

### Attempt
```typescript
{
  id: string;
  weight: number;
  result: "good" | "bad" | null;    // null = not yet attempted
  records: RecordData[];
  decisions: {
    left: RefDecision;
    head: RefDecision;
    right: RefDecision;
  }
}
```

### Platform
```typescript
{
  currentAttempt: CurrentAttempt;    // The lifter currently on the platform
  nextAttempts: Attempt[];           // Ordered list of upcoming attempts
  clockState: "initial" | "started";
  clockTimerLength: number;          // Default 60000ms (1 minute)
  refLights: RefLights;             // Three referee light states
}
```

### CurrentAttempt
```typescript
{
  lifter: { id: string };
  liftName: "squat" | "bench" | "dead";
  attemptNumber: "1" | "2" | "3" | "4";
  // ... additional fields
}
```

## Attempt Ordering Logic

Attempts are sorted by this priority (from the DRL integration code):
1. **Session number** (ascending)
2. **Lift type** (squat=0, bench=1, deadlift=2)
3. **Flight** (alphabetical - A, B, C)
4. **Attempt number** (1, 2, 3)
5. **End of round flag** (default 0)
6. **Weight** (ascending - lightest goes first)
7. **Lot number** (random tiebreaker)

## "On Deck" / Current Lifter Detection

### How it works in the data model:
- `platform.currentAttempt` - The lifter currently lifting (on the platform)
- `platform.nextAttempts` - Ordered array of upcoming attempts
- `platform.nextAttempts[0]` = Next lifter (on deck)
- `platform.nextAttempts[1]` = Lifter in the hole

### For the LiftAlert notification use case:
To detect when a tracked lifter is "2 lifters away":
1. Connect to the WebSocket for the relevant meet
2. Monitor `platform.currentAttempt` and `platform.nextAttempts`
3. Check if the tracked lifter's ID appears in `nextAttempts[0]` or `nextAttempts[1]`
4. When detected, fire the notification email

## Key Considerations for LiftAlert

### Data Access Options

**Option A: WebSocket API (Recommended)**
- Connect via `wss://backup.liftingcast.com/websocket`
- Receives full meet state on every change
- Already provides `currentAttempt` and `nextAttempts`
- Requires API key + meet credentials
- Best for real-time monitoring

**Option B: CouchDB Replication**
- Direct CouchDB replication from liftingcast.com
- More complex setup but gives raw document access
- Can set up views for custom queries

### Scraping Considerations
- The site is a React SPA - traditional HTTP scraping won't work
- The WebSocket API is the proper way to get real-time data
- Public meet pages (board view, lifter pages) return 403 without proper auth
- Registration pages may be more accessible for initial lifter discovery

### Lifter Discovery
- Meet registration pages list all registered lifters
- The `lifters` object in `MeetApiResponse` contains all lifters with names
- Autocomplete could be built from this data
- Lifter pages follow pattern: `/meets/{meetId}/lifter/{lifterId}`

### Polling Strategy
The original plan mentions "polls every 30 seconds" but the WebSocket provides:
- Real-time push updates (sub-second latency)
- Already handles reconnection with exponential backoff
- Heartbeat pings every 30 seconds to maintain connection
- This is far superior to polling for detecting "on deck" status

## External Resources

- [LiftingCast Overlays (GitHub)](https://github.com/liftingcast/liftingcast-overlays) - Example overlay project showing WebSocket API usage
- [LiftingCast Local Relay Server (GitHub)](https://github.com/liftingcast/liftingcast-local-relay-server) - Docker-based local relay
- [DRL-LiftingCast Integration (GitHub)](https://github.com/spellman/drl-control-liftingcast) - CouchDB integration example
- [Python DRL Client (GitHub)](https://github.com/spellman/lifting_cast_drl_client) - Python client showing data model
