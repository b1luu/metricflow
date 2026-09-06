# MetricFlow — Design Notes

A small HTTP service that ingests metric events and serves running aggregates.
This file records *why* the code looks the way it does, so the reasoning
survives even when the code changes.

## Overview

| Endpoint        | Method | Purpose                                          |
| --------------- | ------ | ------------------------------------------------ |
| `/health`       | GET    | Liveness check. Returns `ok`.                    |
| `/ingest`       | POST   | Accept one metric event as a JSON body.          |
| `/stats`        | GET    | Per-metric aggregate: count, average, min, max.  |

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

### 3. `map[string]*Agg` — pointer values

The map holds `*Agg`, not `Agg`.

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

### 7. Missing JSON fields are not an error

`json.Unmarshal` only fails on *syntactically* invalid JSON. A body like
`{"name":"cpu.load"}` parses fine and leaves `Value`, `TS`, `Type` at their
zero values.

- We accept this for `Value`, `TS`, and `Type` — a zero-value event still
  records, and that's a reasonable default (a gauge reading of exactly `0`
  is meaningful; an unset numeric field looking like one is a minor cost).
- `Name` is the exception — see §12.

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

### 9. Windowing uses server receive time, not event time (for now)

An event's bucket is decided by `time.Now()` when the request is handled —
**not** by the `ts` field in the payload.

- **Why defer event-time:** trusting `ev.TS` means handling out-of-order
  arrivals, duplicates, and events timestamped in the past or future — a
  whole correctness problem in its own right. It's a named roadmap milestone,
  not something to smuggle into the first windowing step.
- **What receive-time gives up:** if a client batches or retries, events are
  bucketed by when we *saw* them, not when they *happened*. For the current
  demo scenario (live simulated services pushing in real time) the two are
  nearly identical, so the cost is small and visible.
- The code carries a comment at the bucketing call marking this as the
  deliberate simplification and pointing at the event-time milestone.

### 10. Bucket eviction happens on write, not on a timer

Once a bucket ages out of the window it is *ignored* by `mergeBuckets`, but it
still occupies memory. `record` deletes aged-out buckets for the metric it just
touched, every time it runs (`evict(series, windowStart())`).

- **Why on-write, not a background goroutine:** the unbounded-growth risk is a
  metric that receives events forever — and on-write eviction caps *that* metric
  at ~`numBuckets` live buckets. It needs no extra goroutine, no second thing
  reasoning about the lock, and no cleanup work while the server is idle. The
  eviction runs exactly where we already hold `mu` and already have the
  metric's `series` in hand.
- **What it gives up:** a metric that stops receiving events keeps its last
  handful of buckets indefinitely — nothing triggers their cleanup. That is
  bounded (it stopped growing when the writes stopped) and small, so it is
  accepted. The whole metric entry also stays in `aggs` forever once seen.
- **When to revisit:** if metric *names* churn heavily (many short-lived
  names), the retained-forever entries add up and a periodic sweep — a ticker
  goroutine that drops empty series and their `aggs` entry — becomes worth the
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

### 12. `Name` is the one required field

`/ingest` rejects an event whose `Name` is empty (missing from the JSON, or
explicitly `""`) with `400 name is required`. Every other field keeps the
zero-value-is-fine behavior from §7.

- **Why `Name` and not the others:** `Name` is the map key everything is
  aggregated under. A missing `Value`/`TS`/`Type` degrades gracefully to a
  zero, which is still a coherent (if uninteresting) data point. A missing
  `Name` doesn't degrade — it silently merges into a `""` bucket, mixing
  unrelated events together and corrupting every other metric's neighbor in
  `/stats` output. That's not a lesser version of the data; it's wrong data.
- **Why check after `Unmarshal` instead of during it:** `json.Unmarshal` only
  validates syntax (is this valid JSON), never meaning (is this a valid
  event). Keeping that boundary means the parse step stays generic and the
  validation step stays the readable, single place where "what makes an
  event acceptable" is decided — the natural spot to add more rules later.

### 13. One method per route, enforced by an `allow` wrapper

Each route accepts exactly one method (`GET /health`, `POST /ingest`,
`GET /stats`). Anything else returns `405 Method Not Allowed` with an `Allow`
header naming the permitted method (the HTTP spec requires that header on a
405).

- **Why a wrapper, not a check in each handler:** `allow(method, handler)`
  returns a handler that does the method check, then calls through. Three
  routes need identical logic and the roadmap adds more, so the decorator
  pays for itself immediately — and it keeps the method policy visible in
  `main()` at the routing table (`allow(http.MethodPost, handleIngest)`)
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
  the socket is not. Holding `mu` across the write would block every
  `record` call for the duration of a slow client's read.

## Testing

See `main_test.go`. The strategy:

- **Handlers are package-level functions** (`handleIngest`, `handleStats`, …),
  not closures inside `main()`, specifically so tests can call them directly.
- **`record(now, ev)`** holds the aggregation logic on its own, unit-tested
  without any HTTP machinery.
- **Time is a parameter, not an ambient read.** `record` and `windowStart`
  take `now time.Time`; the handlers call `time.Now()` once and pass it in.
  One operation uses one clock reading, and a test can advance a synthetic
  clock through bucket boundaries with no sleeping
  (`TestWindowRollsAsClockAdvances`).
- **`httptest.NewRecorder` / `httptest.NewRequest`** drive the handlers
  in-process — no real socket, no port binding.
- **`resetAggs()`** clears the shared map at the start of each test so they
  don't leak state into each other.

Run:

```
go test ./...          # all tests
go test ./... -v       # verbose, one line per test
go test ./... -race    # with the race detector (verifies the mutex)
```
