# MetricFlow — Design Notes

A small HTTP service that ingests metric events and serves running aggregates.
This file records *why* the code looks the way it does, so the reasoning
survives even when the code changes.

## Overview

| Endpoint        | Method | Purpose                                          |
| --------------- | ------ | ------------------------------------------------ |
| `/health`       | GET    | Liveness check. Returns `ok`.                    |
| `/ingest`       | POST   | Accept one metric event as a JSON body.          |
| `/stats`        | GET    | Per-metric count/avg/min/max over a time window, as JSON. Optional `?window=`. |

Wrong method on any route → `405` (§13). Details: JSON shape §14, `?window=` §11.

Event shape (see `Event` in `main.go`):

```json
{ "name": "cpu.load", "value": 0.8, "type": "gauge", "ts": 1735000000123 }
```

## Design choices

### 1. Store the conclusion, not the events

Each metric collapses into a single `Agg{Count, Sum, Min, Max}` — four numbers,
regardless of how many events arrive. A million `cpu.load` events use the same
memory as one.

- **Average** comes from `Sum / Count`; we never keep the raw values.
- **Trade-off:** we can't compute anything we didn't decide to track up front
  (e.g. percentiles, which need the distribution). Adding a new aggregate means
  adding a field and back-filling is impossible — old data is already gone.

### 2. In-memory state, no persistence

`aggs` is a plain map that lives only in the process.

- **Why:** simplest thing that works; the goal is to learn the aggregation
  pattern, not run a database.
- **Consequence:** all stats reset to empty on restart. Acceptable for now.
- **If this needed to persist:** periodic snapshot to disk, or write aggregates
  to a real store (SQLite/Redis) behind the same `record()` function.

### 3. Bucket maps hold `*Agg` — pointer values

State is `map[string]map[int64]*Agg` (metric name → bucket start → aggregate;
the buckets come from §8). The inner map holds `*Agg`, not `Agg`.

- Fetching a value-type `Agg` out of a map returns a **copy**; mutating it
  would not touch the map's copy. With `*Agg` we get the real struct and
  updates stick.
- Same rule as `&ev` in `json.Unmarshal` — use a pointer when you intend to
  modify the original.
- **Cost:** a new key returns `nil` (not a zero `Agg`), so `record()` has an
  explicit "create if missing" branch. That's the price of richer values;
  the earlier `map[string]int` got its zero for free.

### 4. Seed Min/Max with the first value

When a metric is first seen: `a = &Agg{Min: ev.Value, Max: ev.Value}`.

- Leaving them at `0` would be a correctness bug: `Min` would stay `0` for any
  metric whose values are all positive, and `Max` would stay `0` for any
  all-negative metric.
- The first real observation is the only correct starting point for both.

### 5. One mutex around the whole map

A `sync.Mutex` guards every read and write of the aggregate map.

- `net/http` runs each request on its own goroutine. Two concurrent `/ingest`
  calls writing the map — or `/stats` reading while `/ingest` writes — is a
  data race, which in Go can corrupt the map or crash the process.
- A mutex serializes access: "everyone touches the map at once" becomes
  "one at a time," which is what removes the race.
- **Why one coarse lock, not per-metric locks:** the critical sections are a
  handful of arithmetic ops — nanoseconds. Contention isn't a real problem at
  this scale, and one lock is far easier to reason about.
- **Known scaling limit:** every ingest and every query contends on this one
  lock, so it is the throughput ceiling once the load harness arrives — a
  single global lock serializes exactly the concurrency the project means to
  showcase. The fix when it matters: shard the map (and its lock) by metric
  name, so writes to different metrics don't block each other. Not worth the
  complexity until a profile says so.

### 5a. State lives on a `Store`, not in package globals

`mu` and the aggregate map are fields of a `Store` struct; `record`,
`handleIngest`, and `handleStats` are methods on `*Store`. `main` creates one
`Store` and closes the handlers over it.

- **Why:** package-level `var mu, aggs` meant every function reached into
  shared global state invisibly, and two independent engines (a server plus a
  test, or two tests) couldn't coexist. A `Store` makes state a value you
  hold — each test gets a fresh one from `newStore()` with no reset dance, and
  it's the seam a future lock-shard (§5) would slot into.
- **What stayed a free function:** `mergeBuckets`, `evict`, `windowStart` —
  they operate only on their arguments (a bucket map, a cutoff, a time), never
  on `Store` fields, so a method receiver there would be dead weight.

### 6. Route registration before `ListenAndServe`

`http.ListenAndServe` blocks for the life of the process, so every
`http.HandleFunc` call must come before it. A handler registered after it is
dead code. (This was an actual bug earlier in development.)

### 7. Missing JSON fields are not an error (except `name` and `ts`)

`json.Unmarshal` only fails on *syntactically* invalid JSON. A body like
`{"name":"cpu.load","ts":1757200000000}` parses fine with `Value` and `Type`
left at their zero values.

- We accept that for `Value` and `Type` — a zero-value event still records,
  and that's a reasonable default (a gauge reading of exactly `0` is
  meaningful; an unset field looking like one is a minor cost).
- `Name` and `TS` are the exceptions — see §12.

### 8. Time-windowed aggregates: 10-second buckets, 6 per window

All-time aggregates are being replaced with windowed ones ("avg over the last
minute"). The approach is **bucketing**: time is chopped into fixed slices, each
holding its own small `Agg`; a window query sums the buckets that overlap it,
and buckets older than the window are discarded.

Chosen defaults: **10-second bucket width**, **6 buckets = a 60-second window**.

The trade-off being balanced:

- Smaller buckets → finer time resolution, more memory, more buckets to sum
  per query.
- Bigger buckets → cheaper, coarser; "last minute" gets fuzzy at the edges
  (a 60s window built from 20s buckets really means 60–80s).

10s/6 is a clean, human-readable default, not a tuned value. This project
isn't bound to a specific company or use case yet, so there's no real
workload to optimize against — the right move is to pick a sensible default,
make the width a single named constant, and revisit it once there's an actual
usage pattern (or a load-harness measurement) to react to.

### 9. Windowing is by event time, not receive time

An event's bucket is chosen by its payload `ts`, not by `time.Now()` when the
request lands. See §16 for the mechanics and what's still open.

- **Why it matters:** a client that batches, buffers, or retries has its
  events bucketed by when they *happened*, not by when we happened to see
  them — so `avg over the last minute` means the last minute of real time.
- **Why it was staged, not one change:** trusting `ev.TS` means separate
  decisions for missing timestamps, stale ones, future ones, and duplicates.
  Each landed as its own slice (§12, §16).

### 10. Bucket eviction happens on write, not on a timer

Once a bucket ages out of the window it is *ignored* by `mergeBuckets`, but it
still occupies memory. `record` deletes aged-out buckets for the metric it just
touched, every time it runs (`evict(series, windowStart(now, window))`).

- **Why on-write, not a background goroutine:** the unbounded-growth risk is a
  metric that receives events forever — and on-write eviction caps *that* metric
  at ~`numBuckets` live buckets. It needs no extra goroutine, no second thing
  reasoning about the lock, and no cleanup work while the server is idle. The
  eviction runs exactly where we already hold `s.mu` and already have the
  metric's `series` in hand.
- **What it gives up:** a metric that stops receiving events keeps its last
  handful of buckets indefinitely — nothing triggers their cleanup. That is
  bounded (it stopped growing when the writes stopped) and small, so it is
  accepted. The whole metric entry also stays in the map forever once seen.
- **When to revisit:** if metric *names* churn heavily (many short-lived
  names), the retained-forever entries add up and a periodic sweep — a ticker
  goroutine that drops empty series and their map entry — becomes worth the
  extra moving part.

### 11. `/stats?window=` is caller-tunable but capped at retention

`/stats` takes an optional `?window=` (Go duration syntax: `30s`, `1m`,
`500ms`). With no parameter it uses the full retention window (`window`, 60s).

- **Why cap it at retention instead of clamping silently:** buckets older than
  `window` are already evicted, so a request for `?window=5m` *cannot* be
  answered correctly — we'd return 60s of data labelled as 5 minutes. Returning
  `400 window exceeds retention (1m0s)` is honest; the caller learns the
  system's real limit instead of getting quietly-wrong numbers.
- **Why reject `<= 0` and unparseable values:** same principle — a
  nonsensical window is a client bug, not something to paper over with a
  default.
- **Edge behaviour:** because bucket boundaries are 10s, a sub-10s window still
  returns the whole current bucket. That's the same coarseness trade-off from
  §8, not a separate bug.
- `window` is derived (`numBuckets * bucketWidth`), so retention and the
  default query window move together when the constants change.

### 12. `name` and `ts` are required

`/ingest` rejects, with `400`, an event missing `name` (`name is required`) or
`ts` (`ts is required`). `value` and `type` keep the zero-value-is-fine
behavior from §7.

- **Why `name`:** it's the map key everything is aggregated under. A missing
  `value`/`type` degrades gracefully to a zero — still a coherent data point.
  A missing `name` doesn't degrade: it silently merges into a `""` bucket,
  mixing unrelated events and corrupting every other metric's neighbor in
  `/stats`. That's not a lesser version of the data; it's wrong data.
- **Why `ts`:** it decides the event's time bucket (§16). `ts: 0` is "January
  1970" — always outside the window, so a zero would just be a confusing way
  to spell "rejected." Better to say so directly.
- **Why check after `Unmarshal` instead of during it:** `json.Unmarshal` only
  validates syntax (is this valid JSON), never meaning (is this a valid
  event). Keeping that boundary means the parse step stays generic and the
  validation step stays the readable, single place where "what makes an
  event acceptable" is decided — the natural spot to add more rules later.
- **Order:** `name` is checked before `ts`, before freshness (§16) — cheapest
  and most fundamental first.

### 13. One method per route, enforced by an `allow` wrapper

Each route accepts exactly one method (`GET /health`, `POST /ingest`,
`GET /stats`). Anything else returns `405 Method Not Allowed` with an `Allow`
header naming the permitted method (the HTTP spec requires that header on a
405).

- **Why a wrapper, not a check in each handler:** `allow(method, handler)`
  returns a handler that does the method check, then calls through. Three
  routes need identical logic and the roadmap adds more, so the decorator
  pays for itself immediately — and it keeps the method policy visible in
  `main()` at the routing table (`allow(http.MethodPost, s.handleIngest)`)
  rather than buried in handler bodies.
- **Why exact-match, not "GET implies HEAD":** simpler, and nothing here
  needs HEAD. If a real client needs it later, the wrapper is the one place
  to teach it.
- **405 vs 404:** the path exists, the method doesn't — 405 tells the caller
  "right URL, wrong verb" instead of sending them hunting for a typo.

### 14. `/stats` responds with JSON

`/stats` returns a JSON object — `{"window": "1m0s", "metrics": {"<name>":
{"count", "avg", "min", "max"}}}` — with `Content-Type: application/json`.
The old line-per-metric plaintext was always a placeholder.

- **Why JSON, no negotiation:** this is the "fast queries out" side of the
  system; the consumer is a program (dashboard, alerting loop, the future
  load harness), not a person reading a terminal. One machine format beats a
  format the caller has to parse with a regex. `Accept`-header negotiation
  would be real complexity for a human-readability nicety nothing needs yet.
- **Why echo `window` back:** the caller may have sent `?window=`, or hit the
  default, or the value may later get clamped — the response says exactly
  which window the numbers cover, so the client isn't guessing.
- **Why `avg` is served raw** (`0.6000000000000001`, not `0.60`): rounding is
  a display concern and the client owns display. Rounding server-side throws
  away precision that a caller doing its own math (e.g. alert thresholds)
  might want. `min`/`max`/`count` are already exact.
- **Why build the response under the lock but encode outside it:** copying
  the numbers into a plain struct is fast; JSON-serialising and writing to
  the socket is not. Holding `s.mu` across the write would block every
  `record` call for the duration of a slow client's read.

### 15. Graceful shutdown on SIGINT / SIGTERM

`main` builds a signal-cancelled context (`signal.NotifyContext` on
`os.Interrupt` / `SIGTERM`) and hands it to `run(ctx, ln)`. `run` serves until
the context is cancelled, then calls `srv.Shutdown` with a 5-second deadline.

- **Why it matters:** the plain `http.ListenAndServe` is killed mid-request on
  Ctrl-C — an in-flight `/ingest` is just dropped. `Shutdown` stops accepting
  new connections but lets open ones finish first. Under the load harness,
  "requests in flight when the process is told to stop" is exactly a failure
  mode worth handling correctly.
- **The 5s deadline:** a stuck client shouldn't hold the process open forever.
  After the deadline `Shutdown` returns an error and `main` exits non-zero.
- **`stop()` (deferred) after the signal:** restores default handling so a
  second Ctrl-C kills immediately — the standard "I really mean it" hatch.
- **Split into `run(ctx, ln)`:** `main` is then just listen + signal wiring +
  `run` — the part that's pure boilerplate. `run` takes a `net.Listener` so a
  test drives the entire lifecycle with a cancellable context and a
  `127.0.0.1:0` listener, no real signal (`TestRunServesThenStopsOnCancel`).
- **`routes(s *Store) http.Handler`:** the mux is its own function too, so the
  path→handler + method-gate wiring is unit-tested (`TestRoutesWireHandlers`).

### 16. Event time — bucketing and the accepted-`ts` range

`record` buckets an event by `time.UnixMilli(ev.TS).Truncate(bucketWidth)` —
event time, not arrival time. Eviction still runs off `now` (wall clock): it's
about memory pressure, not semantics, so it stays on the real clock. `record`
therefore takes both — `now` for eviction, `ev.TS` for the bucket.

`/ingest` accepts `ts` only in **`[now - window, now + bucketWidth]`**:

- **Too old → `400 event too old`.** Its bucket is already evicted; adding to
  it is impossible. Silently dropping would make `count` disagree with what
  the client sent, with no signal — a `400` tells the producer its clock or
  its retry is behind.
- **Too far future → `400 ts too far in the future`.** A future `ts` lands in
  a bucket that stays visible in `/stats` until wall time catches up — for a
  badly-wrong clock, effectively forever. One `bucketWidth` of slack absorbs
  ordinary client/server skew; beyond that is a client bug.
- **The lower bound is exactly `now - window`**, the same span `/stats` and
  eviction work over, so "accepted" and "visible" line up.
- **The guard is only on the HTTP boundary.** `record` itself trusts its
  caller — a direct call with an out-of-range `ts` just has its bucket
  evicted (too old) or left to age in (near future). The untrusted-input
  check belongs at the edge, not in the core (same split as §12).
- **Not yet handled:** de-duplication. Two events with identical
  `{name, value, ts}` are counted as two — for a metrics firehose that's
  almost always two real observations, not a resend. Real dedup needs
  per-event idempotency keys and a seen-set with its own eviction; deferred
  as its own milestone.

## Testing

See `main_test.go`. ~91% coverage — everything but `main()` (listen + signal
wiring). The strategy:

- **Handlers and `record` are `*Store` methods** (`s.handleIngest`,
  `s.handleStats`, `s.record`), so a test calls them directly on a store it
  owns. `handleHealth` and the pure helpers (`mergeBuckets`, `evict`,
  `windowStart`) are free functions and are tested as such.
- **Each test gets its own state.** `s := newStore()` at the top of the test;
  there is no shared global and no reset step, so tests can't leak into each
  other.
- **Time is data, not an ambient read.** `record` takes `now` (for eviction)
  and reads the bucket from `ev.TS`; `windowStart` takes `now`. The handlers
  call `time.Now()` once and pass it in. So a test drives event time and
  wall clock independently — advancing a synthetic clock through bucket
  boundaries (`TestWindowRollsAsClockAdvances`) or feeding an out-of-order
  `ts` (`TestRecordBucketsByEventTime`) with no sleeping.
- **`httptest.NewRecorder` / `httptest.NewRequest`** drive the handlers
  in-process — no real socket, no port binding.
- **Helpers:** `recordNow(s, name, value)` records a here-and-now event;
  `seedBucket(s, name, key, agg)` plants data at a chosen bucket;
  `postIngest(s, body)` / `ingestJSON(name, value)` for the HTTP path;
  `getStats(t, s, query)` calls the handler and decodes the JSON.

Run:

```
go test ./...          # all tests
go test ./... -v       # verbose, one line per test
go test ./... -cover   # coverage summary
go test ./... -race    # data-race detector (needs a 64-bit C toolchain;
                       # the concurrency tests still catch lost updates and
                       # Go's own concurrent-map-access panic without it)
```
