# MetricFlow — Design Notes

A small HTTP service that ingests metric events and serves running aggregates.
This file records *why* the code looks the way it does, so the reasoning
survives even when the code changes.

## Overview

| Endpoint        | Method | Purpose                                          |
| --------------- | ------ | ------------------------------------------------ |
| `/health`       | GET    | Liveness check. Returns `ok`. Outside both shedding and client identification, so a saturated server still answers it (§26). |
| `/ingest`       | POST   | One metric event as a JSON body.                 |
| `/ingest/batch` | POST   | Many events as newline-delimited JSON, with per-event results and partial success (§25). |
| `/stats`        | GET    | Per-metric count/avg/min/max/p50/p90/p99 over a time window, as JSON. |
| `/alerts`       | GET    | Current state of every alert rule, as JSON.      |

`/stats` accepts `?window=` (§11), and `?prefix=`, `?limit=` and `?after=` for
bounded, paged queries (§28). A value outside its limit is an error rather
than something silently clamped, because a caller who asks for 50 000 metrics
and receives 1 000 under a `200` has been handed wrong data.

Any request may carry an `X-Client-ID` header (§29). Absent means a shared
default bucket; over 64 bytes, or anything outside letters, digits and `.-_:`,
is a `400`. Wrong method on any route → `405` (§13). JSON shape §14, alerting
§18, percentiles §23.

Event shape (see `Event` in `main.go`):

```json
{ "name": "cpu.load", "value": 0.8, "type": "gauge", "ts": 1735000000123 }
```

## Index

Every section opens with a one-line summary, so this document can be skimmed
for *what* was decided and read in full only where the *why* matters.

**The shape of the store**

- [1. Store the conclusion, not the events](#1-store-the-conclusion-not-the-events)  
  A million events for one metric collapse into four numbers, so memory is flat in traffic and no event is ever written down.
- [2. In-memory state, no persistence](#2-in-memory-state-no-persistence)  
  Reads are served from memory alone; §32 later added snapshots, so a restart costs at most one interval rather than everything.
- [3. Bucket maps hold `*Agg` — pointer values](#3-bucket-maps-hold-agg--pointer-values)  
  Buckets hold pointers, so an update mutates an aggregate in place instead of copying a struct back into the map.
- [4. Seed Min/Max with the first value](#4-seed-minmax-with-the-first-value)  
  Min and Max are seeded from the first observed value, because a zero-initialised Min would make every metric's minimum zero.
- [5. One mutex around the whole map](#5-one-mutex-around-the-whole-map)  
  One global lock was correct and measurably a bottleneck; kept because §24's fix only makes sense against it.
- [5a. State lives on a `Store`, not in package globals](#5a-state-lives-on-a-store-not-in-package-globals)  
  State is a struct with methods rather than package globals, so every test gets a fresh store instead of sharing one.
- [6. Route registration before `ListenAndServe`](#6-route-registration-before-listenandserve)  
  ListenAndServe blocks for the life of the process, so a route registered after it is dead code — this was a real bug.
- [7. Missing JSON fields are not an error (except `name` and `ts`)](#7-missing-json-fields-are-not-an-error-except-name-and-ts)  
  json.Unmarshal only rejects bad syntax, so required fields need explicit checks rather than faith in the decoder.

**Time and the window**

- [8. Time-windowed aggregates: 10-second buckets, 6 per window](#8-time-windowed-aggregates-10-second-buckets-6-per-window)  
  Time is chopped into six ten-second buckets, and a window query sums the buckets that overlap it.
- [9. Windowing is by event time, not receive time](#9-windowing-is-by-event-time-not-receive-time)  
  An event lands in the bucket its own ts names, not the one it happened to arrive in.
- [10. Bucket eviction happens on write, not on a timer](#10-bucket-eviction-happens-on-write-not-on-a-timer)  
  Eviction rides on writes rather than a timer, which §27 later had to supplement for metrics nobody writes to.
- [11. `/stats?window=` is caller-tunable but capped at retention](#11-statswindow-is-caller-tunable-but-capped-at-retention)  
  Callers may shorten the window but not exceed retention, and an over-long one is an error rather than something silently clamped.

**The request contract**

- [12. `name` and `ts` are required](#12-name-and-ts-are-required)  
  Name and ts have no sane default so they are rejected explicitly; value and type may legitimately be absent.
- [13. One method per route, enforced by an `allow` wrapper](#13-one-method-per-route-enforced-by-an-allow-wrapper)  
  Each route accepts exactly one method and answers 405 with the Allow header the spec requires.
- [14. `/stats` responds with JSON](#14-stats-responds-with-json)  
  The response is a JSON object keyed by metric name, replacing a plaintext format that was always a placeholder.
- [15. Graceful shutdown on SIGINT / SIGTERM](#15-graceful-shutdown-on-sigint--sigterm)  
  A signal cancels the context, in-flight requests get a bounded drain, and a second Ctrl-C kills immediately.
- [16. Event time — bucketing and the accepted-`ts` range](#16-event-time--bucketing-and-the-accepted-ts-range)  
  Events too old or too far in the future are refused rather than silently mis-bucketed.
- [17. `/ingest` hardening for the hot path](#17-ingest-hardening-for-the-hot-path)  
  Body size, content type and method are all bounded, so one hostile request cannot degrade the server.

**Alerting**

- [18. Alerting: rules over the windowed aggregates](#18-alerting-rules-over-the-windowed-aggregates)  
  A rule is one predicate over a window, with ok/firing/nodata states and a For duration that stops a flapping metric alerting.

**Proving it works**

- [19. The load harness is `go test -bench`, not a separate load generator](#19-the-load-harness-is-go-test--bench-not-a-separate-load-generator)  
  Throughput is measured with Go's own benchmark machinery rather than a bespoke load tool.
- [20. Correctness under load: exactness, and tests with teeth](#20-correctness-under-load-exactness-and-tests-with-teeth)  
  The invariant is exactness: every accepted event counted once, and nothing else counted at all.
- [21. `cmd/loadgen`: the claim that needs a real socket](#21-cmdloadgen-the-claim-that-needs-a-real-socket)  
  A real socket is the only way to prove the server recorded exactly what it accepted over HTTP.
- [22. CI exists to run the things this machine can't](#22-ci-exists-to-run-the-things-this-machine-cant)  
  Every CI job runs something this development machine structurally cannot, starting with the race detector.

**Percentiles**

- [23. Percentiles: a bounded sketch, not the events](#23-percentiles-a-bounded-sketch-not-the-events)  
  A log-bucketed sketch buys p50/p90/p99 back at 1% relative error without keeping a single event.

**Throughput**

- [24. Sharding the lock by metric name](#24-sharding-the-lock-by-metric-name)  
  The lock is split 32 ways by metric name, turning §5's bottleneck into near-linear scaling across distinct metrics.
- [25. Batch ingest: the request boundary was the bottleneck](#25-batch-ingest-the-request-boundary-was-the-bottleneck)  
  The request boundary, not the engine, was the limit; NDJSON batching made the engine's real throughput visible.

**Surviving abuse and overload**

- [26. Surviving a bad client: timeouts, and shedding rather than queueing](#26-surviving-a-bad-client-timeouts-and-shedding-rather-than-queueing)  
  Every timeout is set, and under overload the server sheds rather than queues, because a queue hides work instead of reducing it.
- [27. Cardinality: the way metrics systems actually die](#27-cardinality-the-way-metrics-systems-actually-die)  
  An unbounded metric-name space is how metrics systems actually die, so names are capped per shard and idle ones swept.
- [28. Bounding the cost of a query, and surviving a panic](#28-bounding-the-cost-of-a-query-and-surviving-a-panic)  
  A query computes a bounded number of metrics however large the store, and a panicking handler returns 500 instead of dropping the connection.
- [29. Client identity, and the fairness it buys](#29-client-identity-and-the-fairness-it-buys)  
  A self-asserted X-Client-ID buys per-client shares of concurrency and new names, so one broken service cannot cost everyone else their monitoring.

**Parsing**

- [30. Parsing the one shape this server ingests](#30-parsing-the-one-shape-this-server-ingests)  
  A hand-written parser for the one shape this server ingests is about 4x faster than encoding/json, held honest by differential testing and fuzzing.

**Operating it**

- [31. The server watching itself](#31-the-server-watching-itself)  
  The server records its own telemetry as ordinary metrics, under a reserved prefix no client can write to.
- [32. Surviving a restart](#32-surviving-a-restart)  
  Periodic binary snapshots of the aggregates mean a crash costs at most one interval and a clean shutdown costs nothing.
- [33. When the snapshot is the thing that is broken](#33-when-the-snapshot-is-the-thing-that-is-broken)  
  A second generation is kept and a corrupt file no longer stops startup, because refusing to boot turns a damaged cache into a crash loop.

**What reads cost, and who pays**

- [34. The read path while the write path is busy](#34-the-read-path-while-the-write-path-is-busy)  
  A query roughly doubles in latency under a saturating firehose; the fix that mattered was an allocation, and an RWMutex measured worse.
- [35. Charging a client for the work it causes](#35-charging-a-client-for-the-work-it-causes)  
  Clients are billed for the work their queries cause — metrics computed plus names walked — rather than for the number of requests they made.
## Design choices

### 1. Store the conclusion, not the events

> **In one line:** A million events for one metric collapse into four
> numbers, so memory is flat in traffic and no event is ever written down.

Each metric collapses into a single `Agg{Count, Sum, Min, Max}` — four numbers,
regardless of how many events arrive. A million `cpu.load` events use the same
memory as one.

- **Average** comes from `Sum / Count`; we never keep the raw values.
- **Trade-off:** we can't compute anything we didn't decide to track up front.
  Adding a new aggregate means adding a field, and back-filling is impossible
  — old data is already gone.
- **Percentiles were the headline casualty of that, and are now bought back**
  (§23) — not by keeping events, but by keeping a bounded sketch of the
  distribution alongside the four numbers. The trade-off above still stands;
  percentiles just turned out to be affordable within it.

### 2. In-memory state, no persistence

> **In one line:** Reads are served from memory alone; §32 later added
> snapshots, so a restart costs at most one interval rather than
> everything.

*Revisited by §32. The store is still in memory and still the only copy that
serves a request - but it is now snapshotted to disk periodically and
restored at startup, so a restart is no longer a reset. The reasoning below
stands; what changed is that "no persistence" turned out to cost more than it
saved once the server had anything worth keeping.*

`aggs` is a plain map that lives only in the process.

- **Why:** simplest thing that works; the goal is to learn the aggregation
  pattern, not run a database.
- **Consequence:** all stats reset to empty on restart. Acceptable for now.
- **If this needed to persist:** periodic snapshot to disk, or write aggregates
  to a real store (SQLite/Redis) behind the same `record()` function.

### 3. Bucket maps hold `*Agg` — pointer values

> **In one line:** Buckets hold pointers, so an update mutates an
> aggregate in place instead of copying a struct back into the map.

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

> **In one line:** Min and Max are seeded from the first observed value,
> because a zero-initialised Min would make every metric's minimum zero.

When a metric is first seen: `a = &Agg{Min: ev.Value, Max: ev.Value}`.

- Leaving them at `0` would be a correctness bug: `Min` would stay `0` for any
  metric whose values are all positive, and `Max` would stay `0` for any
  all-negative metric.
- The first real observation is the only correct starting point for both.

### 5. One mutex around the whole map

> **In one line:** One global lock was correct and measurably a
> bottleneck; kept because §24's fix only makes sense against it.

*Superseded by §24, which shards the lock. Kept because the reasoning that
led here — and the measurement that ended it — is the point.*

A `sync.Mutex` guarded every read and write of the aggregate map.

- `net/http` runs each request on its own goroutine. Two concurrent `/ingest`
  calls writing the map — or `/stats` reading while `/ingest` writes — is a
  data race, which in Go can corrupt the map or crash the process.
- A mutex serializes access: "everyone touches the map at once" becomes
  "one at a time," which is what removes the race.
- **Why one coarse lock, not per-metric locks:** the critical sections are a
  handful of arithmetic ops — nanoseconds. Contention isn't a real problem at
  this scale, and one lock is far easier to reason about.
- **Known scaling limit — now measured, not predicted.** Every ingest and
  every query contends on this one lock, so it is the throughput ceiling: a
  single global lock serializes exactly the concurrency the project means to
  showcase. §19's benchmarks make it visible (Ryzen 7 7800X3D, 16 threads):

  | benchmark | ns/op | ≈ events/sec |
  | --- | --- | --- |
  | `Record` (1 goroutine) | 68.4 | 14.6 M |
  | `RecordParallelSameMetric` (16) | 108.7 | 9.2 M |
  | `RecordParallelDistinctMetrics` (16) | 130.0 | 7.7 M |

  Writes to *distinct* metrics touch disjoint data — the only thing they
  share is `s.mu`. With a sharded lock that row should scale with cores.
  Instead it is no faster than the same-metric case and **slower than a
  single goroutine**: adding parallelism costs throughput, because every
  goroutine serializes on one mutex and pays contention overhead on top of
  the work. That is the ceiling, quantified.

  (These three numbers predate §23's histogram, so `record` does more work
  now. §24 re-measures both sides on the same code rather than comparing
  against this table.)
- **Why this stopped being a note and became a task.** The first answer here
  was that 7.7 M events/sec is orders of magnitude past anything this project
  ingests, so a measured bottleneck nobody is hitting is a note, not work.
  That reasoning is sound for a throughput number and wrong for this one. The
  bottleneck was not "too slow"; it was *parallelism making the system
  slower*, in a project whose stated claim is correct aggregation under
  concurrency. A ceiling that contradicts the headline is worth removing even
  when nobody is pressed against it.
- **What replaced it:** the map and its lock are sharded by metric name, and
  writes to different metrics take different locks. §24 has the design, the
  before/after numbers, the cache-line problem that surfaced underneath, and
  what the query path gave up in exchange.

### 5a. State lives on a `Store`, not in package globals

> **In one line:** State is a struct with methods rather than package
> globals, so every test gets a fresh store instead of sharing one.

`mu` and the aggregate map are fields of a `Store` struct; `record`,
`handleIngest`, and `handleStats` are methods on `*Store`. `main` creates one
`Store` and closes the handlers over it.

- **Why:** package-level `var mu, aggs` meant every function reached into
  shared global state invisibly, and two independent engines (a server plus a
  test, or two tests) couldn't coexist. A `Store` makes state a value you
  hold — each test gets a fresh one from `newStore()` with no reset dance, and
  it's the seam the lock-shard in §24 slotted into, with no change to any
  caller.
- **What stayed a free function:** `mergeBuckets`, `evict`, `windowStart` —
  they operate only on their arguments (a bucket map, a cutoff, a time), never
  on `Store` fields, so a method receiver there would be dead weight.

### 6. Route registration before `ListenAndServe`

> **In one line:** ListenAndServe blocks for the life of the process, so a
> route registered after it is dead code — this was a real bug.

`http.ListenAndServe` blocks for the life of the process, so every
`http.HandleFunc` call must come before it. A handler registered after it is
dead code. (This was an actual bug earlier in development.)

### 7. Missing JSON fields are not an error (except `name` and `ts`)

> **In one line:** json.Unmarshal only rejects bad syntax, so required
> fields need explicit checks rather than faith in the decoder.

`json.Unmarshal` only fails on *syntactically* invalid JSON. A body like
`{"name":"cpu.load","ts":1757200000000}` parses fine with `Value` and `Type`
left at their zero values.

- We accept that for `Value` and `Type` — a zero-value event still records,
  and that's a reasonable default (a gauge reading of exactly `0` is
  meaningful; an unset field looking like one is a minor cost).
- `Name` and `TS` are the exceptions — see §12.

### 8. Time-windowed aggregates: 10-second buckets, 6 per window

> **In one line:** Time is chopped into six ten-second buckets, and a
> window query sums the buckets that overlap it.

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

> **In one line:** An event lands in the bucket its own ts names, not the
> one it happened to arrive in.

An event's bucket is chosen by its payload `ts`, not by `time.Now()` when the
request lands. See §16 for the mechanics and what's still open.

- **Why it matters:** a client that batches, buffers, or retries has its
  events bucketed by when they *happened*, not by when we happened to see
  them — so `avg over the last minute` means the last minute of real time.
- **Why it was staged, not one change:** trusting `ev.TS` means separate
  decisions for missing timestamps, stale ones, future ones, and duplicates.
  Each landed as its own slice (§12, §16).

### 10. Bucket eviction happens on write, not on a timer

> **In one line:** Eviction rides on writes rather than a timer, which §27
> later had to supplement for metrics nobody writes to.

*Still true for buckets, but no longer the whole story: §27 adds a sweeper,
because "on write" means a metric nobody writes to is never reclaimed at all.*

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

> **In one line:** Callers may shorten the window but not exceed
> retention, and an over-long one is an error rather than something
> silently clamped.

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

> **In one line:** Name and ts have no sane default so they are rejected
> explicitly; value and type may legitimately be absent.

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

> **In one line:** Each route accepts exactly one method and answers 405
> with the Allow header the spec requires.

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

> **In one line:** The response is a JSON object keyed by metric name,
> replacing a plaintext format that was always a placeholder.

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

> **In one line:** A signal cancels the context, in-flight requests get a
> bounded drain, and a second Ctrl-C kills immediately.

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

> **In one line:** Events too old or too far in the future are refused
> rather than silently mis-bucketed.

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

### 17. `/ingest` hardening for the hot path

> **In one line:** Body size, content type and method are all bounded, so
> one hostile request cannot degrade the server.

Ahead of the load harness, `/ingest` is tightened so a single bad or hostile
request can't degrade the whole server:

- **`http.MaxBytesReader`, 4 KiB.** One event is a few hundred bytes.
  Without a cap, `io.ReadAll` would buffer an arbitrarily large body into
  memory — a trivial DoS. Over the limit → `413`, before any parsing.
- **No per-request logging.** The handler used to `fmt.Printf` every parsed
  event. `os.Stdout` writes are synchronised and slow; at firehose rates
  that line *is* the bottleneck, and the benchmark would be measuring
  `Printf`, not the engine. Request-level observability, when it's needed,
  belongs in a logging decorator (like `allow`) that can be toggled — not
  hard-wired into the handler.
- **`/stats` ignores the `Encode` error explicitly** (`_ = ...`). By the time
  it fires the status and headers are already sent and the only cause is a
  client that hung up mid-read — nothing the server can or should act on.

### 18. Alerting: rules over the windowed aggregates

> **In one line:** A rule is one predicate over a window, with
> ok/firing/nodata states and a For duration that stops a flapping metric
> alerting.

A `Rule` is one predicate over a metric's window: *"`cpu.load`'s `avg` over
`30s` is `>` `0.9`"* — metric, stat, op, threshold, window. Which **stat**
carries most of the meaning: `max` catches spikes, `avg` catches sustained
load, `count` catches a metric that has gone quiet. Same machinery, very
different alerts.

Lives in `alert.go`, not `main.go` — same package, but a separate concern
from ingest and storage, and `main.go` was already doing two jobs.

**Three states, not a boolean.** `ok`, `firing`, and `nodata`.

- **Why `nodata` is its own state:** `mergeBuckets` already reports whether
  any bucket fell inside the window, so the information is free — discarding
  it is the extra step. Folding it into `ok` is actively dangerous: a service
  that stopped reporting would read as healthy. Folding it into `firing`
  means every low-frequency metric alarms forever. With no observations
  there is nothing to compare, and saying so is the only honest answer.
- A zero `Count` is treated as `nodata` too, which is also what keeps
  `avg = Sum/Count` from producing `NaN`.

**`evaluate(rule, agg, ok)` is pure** — no clock, no locks, no side effects,
no I/O. All the timing and transition bookkeeping is deliberately kept out of
it, which is what makes the comparison logic exhaustively table-testable
(every stat × every op, including exactly *on* the threshold, where `>` and
`>=` diverge).

**Validation happens once, at construction.** Rules are defined in code, so a
bad stat or op is a programming error that should surface at startup, not
evaluate to nonsense at 3am. `Rule.Validate` also caps a rule's window at the
retention window for the same reason `/stats?window=` does (§11): past that
the buckets are gone and the question can't be answered honestly. The
`switch` defaults in `statValue`/`breached` are unreachable for a validated
rule but degrade quietly rather than panic — tested, so that stays true.

**The `Alerter` owns the state, the `Store` knows nothing about it.**
`Alerter` holds the rules, a per-rule `AlertState`, and its own mutex; it
reads the `Store` and never the reverse. Aggregation is a lower layer that
alerting consumes — the same separation §5a drew between state and handlers.

- **`Since` moves only on a transition.** That's what makes "firing for how
  long" answerable, and it's exactly the field a `for` duration will need.
  `Value` refreshes on every evaluation; `Since` does not.
- **Only transitions are logged.** Steady state is silent, so a firing rule
  logs once rather than every ten seconds. `logTransition` is the whole
  notification story for now, and the single seam a webhook or pager would
  plug into — no `Notifier` interface until there is a second implementation
  to justify hiding behind one.
- **Duplicate rule names are a construction error.** The name keys the state
  map, so a duplicate would silently make two rules share one state.
- **Each rule is read separately, not from one shared snapshot.** Rules carry
  their own windows, so they have their own cutoffs; one snapshot can't serve
  them all. Hence `(*Store).aggFor(name, cutoff)` alongside `handleStats`'s
  all-metrics loop — and the two genuinely can't share a path, since ranging
  under `s.mu` while calling a method that takes `s.mu` would deadlock (Go
  mutexes aren't reentrant). The upside is that each rule holds the store
  lock for one short merge instead of one long one, which matters given §5.
- **The store is read outside `a.mu`, and logging happens outside it too.**
  Nesting the two locks invites deadlock; doing stdout I/O under a lock puts
  write latency into every other caller's critical path.

**Evaluation runs on a ticker, not on ingest.** `(*Alerter).Run(ctx, every)`
re-evaluates every rule on an interval until the context is cancelled.

- **Why not on ingest:** alerting cost should scale with the number of
  *rules*, not the number of *events*. Evaluating inside `record` would put
  that work on the hot path §17 just cleared, inside the critical section,
  lengthening exactly the lock §5 names as the throughput ceiling — and it
  would re-check thousands of times a second a threshold that meaningfully
  moves about once. Detection lag is instead bounded by one `evalInterval`.
- **Why not lazily on `GET /alerts`:** nothing would exist until someone
  looked, so nothing could ever be notified and "was it firing at 3am" would
  be unanswerable. That's a query endpoint, not alerting.
- **`every` is a parameter, not the constant.** Same reasoning as `run`
  taking a `net.Listener`: a test drives the loop at millisecond speed and
  proves the ticker really calls `evaluateAll`, rather than only that it
  starts and stops.
- **The alerter shares the server's shutdown signal** and `run` waits on a
  done channel before returning, so the process never exits mid-evaluation.
- **Rules live in code** (`defaultRules`). A `POST /rules` CRUD surface would
  add a lot of endpoint and nothing to the aggregation story. There is
  deliberately no "metric went silent" rule: a metric with no events has no
  aggregate to compare, so `count < n` can't catch it — silence surfaces as
  `nodata`. A test loads `defaultRules` so a typo fails at test time rather
  than at startup.

**`GET /alerts` reads, it does not evaluate.** It serves `Snapshot()` as
`{"alerts": [...]}`, one entry per configured rule.

- **Why a read and not an evaluation:** evaluation is the ticker's job, so
  polling this endpoint hard costs a lock and a copy — never a full re-check
  of every rule against the store. An endpoint that evaluated on demand would
  be a denial-of-service lever pointed at the ingest lock.
- **An object, not a bare array**, so fields can be added later without
  breaking every caller's parser. `Snapshot()` returns a non-nil slice, so an
  empty rule set encodes as `[]` rather than `null` and clients can range
  over it without a nil check.
- **Every rule appears, including ones that have never fired.** `since` on a
  `nodata` entry is the alerter's start time — visible proof that `Since`
  tracks the transition, not the last evaluation.

**`For` suppresses flapping via a fourth state.** A rule may require its
breach to hold for `For` before it counts. Until then the rule sits in
`pending`:

```
ok/nodata ──breach──> pending ──held for For──> firing
    ^                    │                         │
    └────────────────────┴─────────────────────────┘
                    breach ends
```

- **Why it's needed:** a metric oscillating around the threshold would
  otherwise fire and resolve on every tick. `For` turns "is it bad right
  now" into "has it been bad long enough to be worth saying".
- **`pending` is pre-announcement.** Entering it logs nothing; leaving it
  logs only if it became `firing`. So a flap produces *no notification at
  all*, rather than a fire/resolve pair every ten seconds. The state is
  still visible at `/alerts`. `announce(from, to)` is the one predicate that
  decides this, so the policy lives in a single testable place. The
  consequence to accept: `pending → nodata` is silent too.
- **`For: 0` fires immediately**, which is what every rule did before this
  existed — so the feature is opt-in and adds no behaviour to rules that
  don't want it. `defaultRules` uses both: `cpu-hot` waits, because
  sustained load is the point; `cpu-spike` doesn't, because a spike is
  instantaneous by nature and waiting for it to persist would mean never
  reporting the thing the rule exists to catch.
- **`applyFor` is pure, like `evaluate`.** `evaluate` still returns the
  *instantaneous* verdict and its slice-1 tests were untouched; `applyFor`
  maps that onto the durable state machine given the stored state. Splitting
  it that way meant adding `For` required no changes to the comparison core,
  and the whole `ok → pending → firing` walk is table-testable with no clock
  and no lock.
- **`Since` needs no new field.** It already meant "when this state began",
  and `pending` is only ever entered at the moment a breach starts — so
  while pending it *is* "breaching since", which is exactly what the
  promotion check needs. Designing that in at slice 2 is what made this
  slice additive rather than a rewrite.
- **`For` has no upper bound** (unlike `Window`, §11): it counts how long a
  breach persisted across evaluations, which is unrelated to how much data
  is retained. A five-minute `For` over a sixty-second window is coherent.

### 19. The load harness is `go test -bench`, not a separate load generator

> **In one line:** Throughput is measured with Go's own benchmark
> machinery rather than a bespoke load tool.

`bench_test.go` measures the engine through the ordinary Go benchmark
machinery:

```
go test -run=^$ -bench=. -benchmem
go test -run=^$ -bench=Record -cpuprofile=cpu.out
```

- **Why not a `cmd/loadgen` binary firing real HTTP:** it would measure the
  machine's network stack as much as the engine, vary run to run, and produce
  nothing assertable. Benchmarks are reproducible, run in CI, and plug
  straight into `-cpuprofile` / `-benchmem` — which is what turns "the global
  lock is the bottleneck" from a claim into evidence. A load generator is a
  better *demo*; this is better *proof*.
- **The design is the comparison, not the number.**
  `RecordParallelSameMetric` and `RecordParallelDistinctMetrics` exist as a
  pair: distinct metrics touch disjoint data, so the only thing they share is
  `s.mu`. Either the second scales with cores (the lock isn't the limit) or
  it doesn't (it is). It doesn't — see the table in §5. A single benchmark
  number couldn't have shown that.
- **Benchmark hygiene that mattered:**
  - `record` takes `now` as a parameter, so the clock is hoisted out of the
    hot loop — the measurement is the update path, not `time.Now()`.
  - `handleIngest` reads the clock itself and rejects a stale `ts` (§16), so
    a fixed body would silently start measuring the *reject* path on a long
    `-benchtime`. The body's timestamp is refreshed every 1024 iterations
    (under `StopTimer`), and the status is checked every iteration so the
    failure would be loud rather than silent.
  - `discardWriter` replaces `httptest.NewRecorder`, which buffers every
    response body — reusing one across a benchmark would measure its buffer
    growth as much as the handler.
  - `BenchmarkEvaluateAll` warms up once before timing: the first evaluation
    is a `nodata → ok` transition for every rule and would put 50 log writes
    inside the measurement. A tick costs ~4.3 µs for 50 rules, against a
    10-second interval — confirming §18's claim that alerting cost scales
    with rule count and is free at any event rate.

### 20. Correctness under load: exactness, and tests with teeth

> **In one line:** The invariant is exactness: every accepted event
> counted once, and nothing else counted at all.

`harness_test.go` is the other half. The benchmarks answer *how fast*; these
answer *still right*. The invariant is always **exactness** — every accepted
event counted once, nothing else counted at all — because "approximately the
right count under load" is indistinguishable from a lost-update bug.

- **`TestConcurrentRecordIsExact`** — 64 goroutines × 1000 events on one
  metric; `Count`, `Sum`, `Min`, `Max` must all be exact.
- **`TestConcurrentReadersAndWritersStayExact`** — writers against every
  production reader at once: `/stats`, `/alerts`, and the alerter's own
  `evaluateAll`. Go panics on concurrent map access even without `-race`, so
  a dropped lock anywhere fails loudly; the exact final count proves nothing
  was lost while the readers hammered.
- **`TestMixedStreamCountsOnlyAcceptedEvents`** — the injected-failure case.
  A concurrent stream mixing valid requests with all six rejectable kinds
  (malformed JSON, no name, no ts, ts too old, ts too far future, body over
  the 4 KiB cap). The store's count must equal exactly the number of `200`
  responses. The bad bodies deliberately carry the *same metric name* as the
  good traffic, so anything that leaked through would inflate that metric
  rather than hide under a name of its own.

**These tests were verified to fail.** Temporarily removing `s.mu` from
`record` produced, immediately:

```
--- FAIL: TestConcurrentRecordIsExact
    Count = 63666, want 64000 (lost or duplicated updates)
    Sum = 347071, want 352000
fatal error: concurrent map writes
```

334 lost increments out of 64,000, caught as a legible assertion rather than
a vague flake. A concurrency test that has never been seen to fail is a
guess; this one has a known failure mode and a known signature.

**On `float64` and interleaving.** Addition is not associative, so a
concurrent `Sum` over arbitrary values could legitimately differ in its low
bits purely by goroutine ordering — an exact-equality assertion on it would
be flaky for a *correct* implementation. The harness sidesteps that rather
than papering over it with an epsilon: all values are integer-valued floats
whose running total stays far below 2⁵³, where every intermediate is exactly
representable and any order yields the identical result. `Count`, `Min` and
`Max` are order-independent regardless, so they need no such care.

**On `go test -race`:** it needs a 64-bit C toolchain, which this development
machine (Windows, 32-bit gcc) doesn't have — the tests above catch lost
updates and Go's own concurrent-map panic, but not the subtler races a
detector would find. That gap is closed in CI rather than caveated forever;
see §22.

### 21. `cmd/loadgen`: the claim that needs a real socket

> **In one line:** A real socket is the only way to prove the server
> recorded exactly what it accepted over HTTP.

The benchmarks (§19) measure the engine and the harness (§20) proves it stays
exact under concurrency — but both run in-process. `cmd/loadgen` exists for
the one claim they structurally cannot make: **over real HTTP, the server
recorded exactly as many events as it told the client it accepted.**

```
go run ./cmd/loadgen -duration 3s -workers 8 -bad 0.25
```
```
141625 requests in 3s  (47206 req/s)
  200    106215   75.0%
  400     29511   20.8%
  413      5899    4.2%
latency  p50 <557µs  p90 <557µs  p99 1.042ms  max 23.891ms
         (clock resolution 557µs - faster than that is unresolvable)
verify: OK - 106215 accepted, 106215 recorded
```

- **A separate `main` package with no access to the server's internals.** A
  load generator linked against the thing it measures can test the wrong
  side of the wire; `statsResponse` is redeclared rather than shared,
  because that *is* the wire contract and a client reusing the server's
  struct could never notice it changing.
- **Verification is the point, not the throughput number.** `-bad 0.25`
  mixes in all six rejectable shapes under the same metric name as the good
  traffic, so anything that leaked past validation would inflate that
  metric. A mismatch exits non-zero, making this usable as a CI gate.
- **Two guards keep the comparison sound.** Metric names are scoped by a
  per-run ID, or a second run against a warm server would count the first
  run's still-in-window events as its own. And the server reports the window
  it applied, so loadgen reads that instead of hardcoding retention: a run
  at least as long as the window has already had its earliest events
  evicted, and the server would be *right* to report fewer. It declines to
  judge rather than assert something false.

**Three things this cost, all worth recording:**

- **Requests must not carry the run's context.** Binding it cancels whatever
  is in flight at the deadline — the server may already have recorded that
  event while the client scores it a failure. This surfaced immediately as a
  flaky "client counted N, server saw N+1". The deadline stops us *issuing*;
  outstanding requests finish.
- **The default transport keeps two idle connections per host**, so past two
  workers the run measures TCP handshakes. Response bodies must also be
  drained and closed, or the connection never returns to the idle pool and
  the tuning is silently undone.
- **`time.Now()` on this machine ticks every ~530–580µs** — 9999 of 10000
  back-to-back reads return an identical value. The first working version
  proudly reported `p50 0s`, which would mean instant responses; it actually
  meant most requests finished inside a single clock tick. `clockResolution()`
  now probes the tick and the report refuses to state a figure beneath it,
  printing `<557µs`. Same principle as `StateNoData` (§18): an instrument
  should say "I can't tell" rather than something confident and wrong.

### 22. CI exists to run the things this machine can't

> **In one line:** Every CI job runs something this development machine
> structurally cannot, starting with the race detector.

`.github/workflows/ci.yml` is not box-ticking. Every job is there because it
checks something the development environment structurally cannot.

- **`go test -race` is the whole point.** The race detector needs a 64-bit C
  toolchain; this machine's gcc is 32-bit, so §20's concurrency tests have
  only ever proven they catch *lost updates* and Go's own concurrent-map
  panic. Linux runners have the toolchain. Rather than caveat that
  permanently, the claim gets checked on every push.
- **`gofmt -l` catches what the local check can't see.** Git normalises to
  LF on commit but checks out CRLF on Windows, so local `gofmt -l` flags
  every file and the signal is useless. On a Linux runner it is exact. (The
  committed content was verified clean before this landed, by extracting the
  blobs and formatting those rather than the working copy.)
- **Benchmarks run, but are not a gate.** Runner hardware is far too noisy
  for a performance threshold — it would flap and get ignored, which is
  worse than no check. `-benchtime=10x` proves only that the measurement
  code still compiles and runs, so a refactor cannot quietly break the thing
  §5's numbers depend on.
- **The e2e job is `loadgen -verify` used as designed.** §21 claimed the
  non-zero exit made it a CI gate; this is that claim being cashed rather
  than asserted. It also exercises graceful shutdown (§15) for real: SIGTERM,
  then wait for the process to drain and exit on its own, failing if it
  doesn't within 5s.

The startup and shutdown waits poll rather than `sleep`. A fixed sleep is
either flaky on a slow runner or wasted time on a fast one, and there is a
readiness endpoint (`/health`) precisely so nobody has to guess.

### 23. Percentiles: a bounded sketch, not the events

> **In one line:** A log-bucketed sketch buys p50/p90/p99 back at 1%
> relative error without keeping a single event.

§1 named this as the price of storing conclusions: no percentiles, because
those need the distribution. `histogram.go` buys them back without keeping a
single event.

**Log-bucketing.** A value is bucketed by the logarithm of its magnitude, so
buckets widen as values grow and the *relative* error stays flat — each
reported quantile is within 1% of the truth at any magnitude. `gamma` is
chosen so a bucket's representative value is off by exactly `relAccuracy` at
both edges, which is the best a single representative can do.

- **Why not linear buckets,** which is what loadgen uses (§19)? That works
  there only because request latency has a known, narrow range. A general
  metrics engine sees `cpu.load` near 1 and `http.latency_ms` near 10000 in
  the same process, and a bucket width that suits one is useless for the
  other. Same lesson as §8's bucket width, one level down.
- **Sparse, not a fixed array.** Spanning 1e-9..1e9 at 1% needs ~1400
  buckets, but a real metric's values sit in a narrow band — `cpu.load`
  occupies a few dozen. Memory follows what actually arrived. A metric that
  never reports a negative never allocates a negative map.
- **Negatives get their own map**, keyed by magnitude, rather than being
  rejected. §4 already establishes all-negative metrics as a supported case,
  and a logarithm has no answer for a negative input. The quantile walk
  therefore goes negatives-descending-magnitude, then zeros, then positives.
- **Merging is exact** — bucket counts simply add — which is what lets a
  windowed query combine several time buckets (§8) with no loss beyond the
  bucketing already applied. A test asserts percentiles over a merge equal
  percentiles over the same values in a single bucket.

**`Agg.h` is an unexported pointer,** and that is load-bearing. `Agg` is
copied by value — `mergeBuckets` returns one — and a copy must never share a
histogram some other `Agg` is still writing into. `mergeBuckets` therefore
always allocates a fresh one; there is a test that writes to a query result
and asserts the store is unchanged.

**What it cost, measured (§19's benchmarks):**

| | before | after |
| --- | --- | --- |
| `record` | 68.4 ns/op, 0 allocs | 91.3 ns/op, 0 allocs |
| `hist.add` alone | — | 22.0 ns/op, 0 allocs |
| `/stats`, 100 metrics | — | 690 µs |

`record` stays allocation-free in steady state: the map allocation happens
only on first touch of a new bucket. The `/stats` figure has no "before",
because the old benchmark seeded `Agg`s directly and so had no histograms to
walk — it was quietly measuring almost none of what `/stats` does. Fixing
that benchmark was part of this work, not a footnote to it.

**Three quantiles cost the same as one.** The expense is sorting the bucket
keys, so `quantiles(...)` sorts once and walks once for all of them:
`BenchmarkHistQuantile` is 31 µs for one, `BenchmarkHistQuantilesTogether`
30 µs for three.

**Alert rules can use `p50`/`p90`/`p99`,** and `defaultRules`' latency rule
moved from `max` to `p99`. That change is the argument for the whole slice: a
`max` rule fires on one unlucky request, and an `avg` rule stays silent while
a real fraction of users suffer. A test with 990 requests at 20 ms and 10 at
900 ms has the p99 rule firing and the avg rule — identical threshold —
missing it entirely, because the average is ~29 ms.

**Accuracy and exactness are separate claims.** The bucket *values* are
approximate by design; the bucket *counts* are exact, and §20's concurrency
tests hold the histogram to the same standard as the summary numbers. A lost
update must not be able to hide behind "percentiles are estimates".

### 24. Sharding the lock by metric name

> **In one line:** The lock is split 32 ways by metric name, turning §5's
> bottleneck into near-linear scaling across distinct metrics.

§5 left a measured bottleneck and an explicit decision not to fix it. This
section is the fix, and the reason the decision changed: the ceiling wasn't
a footnote about throughput, it was the project's central claim. "Correct
aggregation under concurrency" is not worth much if adding concurrency makes
the thing slower, which is precisely what §5 measured.

**The shape.** `Store` no longer holds one mutex over one map. It holds
`shardCount` shards, each with its own mutex and its own
`map[string]map[int64]*Agg`, and a metric lives in the shard its *name*
hashes to. That choice is what makes the scheme sound: every bucket of a
metric is reachable through exactly one lock, so a write, an eviction and a
merge for that metric can never take different locks for the same data.

**Why hash the name, and not something easier.**

- Hashing the *bucket* would scatter one metric's buckets across shards, so
  merging a window would need every lock. Hashing the name keeps each
  metric's whole series together, which is the unit every operation works on.
- A `map[string]*lock` would need its own lock to look up the lock. A fixed
  array of shards needs no allocation and no bookkeeping.
- The hash is FNV-1a, hand-rolled in `shard.go` rather than taken from
  `hash/fnv`. The stdlib version returns an interface, which escapes to the
  heap; `record` is allocation-free and must stay that way.
  `TestFnv32DoesNotAllocate` holds that line, and `TestFnv32KnownValues`
  pins the hash against canonical vectors, so an "optimisation" that changes
  the mapping shows up as a failing test rather than as mysterious behaviour.
- `shardCount` is a power of two, so the index is `hash & (shardCount-1)` —
  a mask, not a division. `TestShardCountIsAPowerOfTwo` asserts the property
  the mask silently depends on.

**Why 32.** It is roughly twice the hardware thread count of a typical
machine, which leaves headroom for uneven hashing without making the store
large. It is a constant, not a tunable: exposing it would invite tuning
nobody has data for. The honest statement is that 32 was chosen, not tuned —
the benchmarks below are what a future tuning exercise would move.

**The measurement** (Ryzen 7 7800X3D, 16 threads, `-benchtime=2s -count=6`,
median; both sides measured on this machine on the same code, so the numbers
compare):

| benchmark | one lock | sharded | sharded + padded |
| --- | --- | --- | --- |
| `Record` (1 goroutine) | 97 ns | 101 ns | 100 ns |
| `RecordParallelSameMetric` (16) | 151 ns | 159 ns | 159 ns |
| `RecordParallelDistinctMetrics` (16) | 149 ns | 15.0 ns | **10.0 ns** |

Writes to distinct metrics go from 149 ns/op to 10.0 ns/op — **14.9×**, or
6.7 M events/sec to 100 M. The single-goroutine cost rises ~4 ns, which is
the hash; that is the price paid on every ingest for the parallel win.

That win was invisible end-to-end until §25: the server was answering ~53 k
requests/sec over loopback, so the store was never the thing a client was
waiting on. Batching is what let this number reach the wire.

§5's old table read 68.4 / 108.7 / 130.0. Those numbers are not the "before"
here and were not reused: they predate §23's histogram, so `record` does
strictly more work now. Comparing against them would have credited sharding
with a speedup it didn't produce.

**False sharing, and the second measurement.** Sharding removed the software
bottleneck and exposed a hardware one. A shard is an 8-byte mutex plus an
8-byte map header, so four of them fit in a 64-byte cache line — and a core
taking shard 0's lock invalidates that line for cores working on shards 1
through 3. Independent locks contend anyway, through the cache. Padding each
shard to a full line is the last column above: 15.0 ns → 10.0 ns, a further
1.5×, for 2 KB of padding.

The signature is what makes this a diagnosis rather than a guess: only the
distinct-metrics row moved. One goroutine has nobody to falsely share with,
and the same-metric case was already serialized on a single mutex. Padding
is invisible at runtime — delete it and nothing fails, throughput just
quietly drops — so `TestShardOwnsOneCacheLine` asserts the layout itself,
including the stride between adjacent shards.

**What `/stats` gave up.** The query path walks shards one at a time instead
of holding one lock over everything, so a response is no longer a single
instant across all metrics: a write can land between shard 0 and shard 31.
That is a real loss and it is deliberate. A globally atomic snapshot means
holding all 32 locks at once, which hands ingest back the exact stall this
section removes — the most expensive possible operation, in service of a
guarantee metrics queries don't need. Each metric's own numbers still come
from one merge under one lock, so no single metric is ever internally torn,
and §20's `TestStatsAcrossShardsIsNeverTornPerMetric` holds that line.

**What sharding does not fix.** `RecordParallelSameMetric` is unchanged, and
that is correct: 16 goroutines writing one metric still serialize, because
they genuinely contend for one `*Agg`. Sharding by name cannot help a single
hot name. The remaining ceiling is real contention on real shared state, not
an artifact of the locking scheme — a different problem (per-core
accumulators, or atomics on the `Agg` fields) for a different day, and one
nothing in this project is anywhere near needing.

### 25. Batch ingest: the request boundary was the bottleneck

> **In one line:** The request boundary, not the engine, was the limit;
> NDJSON batching made the engine's real throughput visible.

§24 ended with the store doing ~100 M events/sec on distinct metrics. The
server answered ~53 k requests/sec over loopback. Both numbers were true, and
together they said something uncomfortable: the engine's throughput was
invisible from outside, because one HTTP request per observation is the wrong
unit of work. Everything §24 bought was being spent on request framing.

`POST /ingest/batch` takes many events in one request. `/ingest` is unchanged
and stays the documented single-event contract.

**Newline-delimited JSON, not a JSON array.** The reason is memory, not
taste. An array has to be buffered and parsed whole before the first event
can be looked at, and its length comes from the client, so the server would
be sizing an allocation from untrusted input. A stream of JSON values decodes
one at a time in constant memory however long the batch runs, and lets the
server stop early — at a cap, or at a syntax error — without having parsed
the rest. `json.Decoder` reads concatenated values, so the newlines are a
convention for producers rather than something the parser depends on; the
endpoint accepts pretty-printed or space-separated events too, and a test
pins that so nobody later "fixes" it.

**Rejection is per event.** A firehose that discards 999 good samples because
the 1000th was malformed is worse than useless. A bad event costs exactly
itself — but never silently: the reject count is exact and the first
`maxBatchErrors` are named with their index in the submitted batch. The
listing is capped so a client cannot turn a 1 MiB request into a much larger
reply; the count is not capped, because the count is what tells a client how
much to resend.

**The accounting contract, which is the part that ties into everything
else.** `accepted` and `rejected` describe what actually happened on *every*
status this endpoint returns, not just 200. A 413 with no accounting would
leave a client unable to tell what landed, and would break the invariant this
project holds everywhere else — that the events the server says it accepted
are exactly the events it recorded. So a batch cut short by the byte cap, the
event cap, or a syntax error still reports what it applied, and `fatal` says
why it stopped. Partial application is the honest outcome here; pretending
otherwise would require either discarding good events or lying about them.

**Two caps, because they stop different things.** `maxBatchBody` (1 MiB)
bounds the bytes one request can make the server read and parse, which is
what a hostile client abuses. `maxBatchEvents` (10 000) bounds the work in
the unit everyone else pays for: a batch takes the store's locks once per
event, so an unbounded batch is an unbounded stall for `/stats` and for every
other writer. The event cap is checked *after* a successful decode, not
before — reaching the index with an event in hand means it is the
(cap+1)-th. Checking first rejected a batch of exactly the cap, which is the
off-by-one `TestBatchAcceptsExactlyTheEventCap` caught on its first run.

**A syntax error ends the batch.** Once the decoder is mid-token there is
nothing sound to resync to — a stray brace could be a truncated object or the
start of a valid one — so claiming to have skipped just that event would be a
guess. The stream stops, everything before it stays applied, and the response
says where.

**The measurement** (Ryzen 7 7800X3D, `-count=3`, median, per event):

| path | ns/event |
| --- | --- |
| `/ingest`, one event per request | 2215 |
| batch size=1 | 2843 |
| batch size=10 | 868 |
| batch size=100 | 766 |
| batch size=1000 | 777 |
| batch size=10000 | 713 |
| decode + validate only, no store | 536 |

Size 1 being *worse* than `/ingest` is the expected shape: identical request
overhead plus a JSON response body that `/ingest` does not write. The curve
flattens by size 100, which is the number worth telling clients — past that a
larger batch buys very little and costs latency and blast radius.

End to end over a real socket, 8 workers for 3 s:

| | events | events/sec |
| --- | --- | --- |
| `-batch 1` | 168 314 | 56 100 |
| `-batch 10` | 1 190 180 | 396 715 |
| `-batch 100` | 4 634 300 | 1 544 644 |
| `-batch 1000` | 8 559 000 | 2 851 755 |

**50.8×**, with `verify` passing at every size. That is what §24's 14.9× was
worth and could not show on its own.

**The optimisation this slice cancelled.** The plan was to group a batch by
shard so each lock is taken once instead of once per event — a natural
follow-on from §24, and it would have been real, measurable work.
`BenchmarkBatchDecodeOnly` said don't. Decoding and validating is 536 of the
~730 ns, or 73%; the entire store path — hash, lock, map lookups, `Agg`
update, histogram, eviction — is about 100 ns, or 14%. Grouping locks attacks
part of that 14%, so it could not move the total by more than a couple of
percent: measurable in a microbenchmark, invisible to any client.
`BenchmarkIngestBatchDistinctMetrics` checks the other half of that
reasoning — spreading a batch across shards instead of piling it on one is
836 ns/event against 777, slightly *worse* from touching more distinct
memory. So batching is not fixing lock contention. It is amortising request
overhead, and that is all it needs to do.

The next bottleneck is therefore named where it actually is: JSON decoding,
at roughly one allocation and 56 bytes per event, most of it the metric name
string. **Taken up in §30**, which replaces it with a parser for this one
schema and roughly doubles end-to-end throughput - the difference between
optimising the dominant term and optimising the 14% this section declined to.

**What batching costs, stated plainly.**

- *Latency.* An event now waits for its batch to fill. That is a producer-side
  choice, but it is a real trade against the "real-time" in the project's
  name, and it is why the default stays 1.
- *Atomicity.* A batch is not a transaction. Half of one can land and be
  reported as such. The alternative — all-or-nothing — throws away good data
  for the sake of a guarantee metrics ingestion does not need.
- *Blast radius.* One dropped connection now costs a batch rather than an
  event. The accounting contract is what keeps that recoverable: the client
  knows exactly how far it got.
- *Lock hold time.* A batch takes a shard's lock once per event in a tight
  loop, which is a longer stretch of contention for `/stats` than a single
  ingest. `maxBatchEvents` is the bound on that, and it is the reason the
  event cap exists at all rather than only the byte cap.

### 26. Surviving a bad client: timeouts, and shedding rather than queueing

> **In one line:** Every timeout is set, and under overload the server
> sheds rather than queues, because a queue hides work instead of reducing
> it.

The project's spine says the harness should prove throughput and correctness
*under injected failure*. Up to here "failure" meant bad input — `-bad`
sends events the server should refuse. That is the easy half. The other half
is a client that is not merely wrong but expensive: one that is slow, or
abandoned, or simply too numerous. Against those the server had no defence
at all.

**Two holes, both of them defaults.**

`run()` built `&http.Server{Handler: ...}` and nothing else. Every timeout
field was its zero value, which in `net/http` means no limit: no bound on how
long a client may take to send headers, to send a body, or to read the
response. This is the standard Go production bug, and it is dangerous
precisely because nothing looks wrong — the code is correct against every
well-behaved client and falls over against one that isn't. A client can open
a connection, dribble one header byte a minute, and hold a goroutine and a
socket indefinitely. A few thousand of those cost an attacker nothing.

Second, `net/http` runs every connection on its own goroutine and imposes no
limit on how many. The server's concurrency was whatever clients decided it
was.

**The timeouts, and how they compose.**

| field | value | what it stops |
| --- | --- | --- |
| `ReadHeaderTimeout` | 5s | slow-loris: headers that never end |
| `ReadTimeout` | 30s | a body delivered arbitrarily slowly |
| `WriteTimeout` | 30s | a client that never reads the response |
| `IdleTimeout` | 60s | abandoned keep-alive connections |
| `MaxHeaderBytes` | 64 KiB | headers, which `MaxBytesReader` never sees |

`ReadHeaderTimeout` is tightest because headers from anything legitimate
arrive in one packet. `ReadTimeout` is deliberately loose, because a 1 MiB
batch on a slow link is a real request that needs room.

The composition is the part worth noticing. `MaxBytesReader` (§17, §25)
bounds how *much* a client may send; `ReadTimeout` bounds how *long* it may
take. Only the two together bound the *rate*. Either one alone leaves a hole —
unbounded bytes, or a 1 MiB batch delivered one byte per second — and neither
is obviously incomplete on its own.

`IdleTimeout` is what makes the read timeouts safe to set at all. Without it
Go applies `ReadTimeout` to the idle wait between keep-alive requests, so a
well-behaved client reusing a connection gets disconnected for not yet having
anything to say. A test asserts `IdleTimeout >= ReadTimeout` rather than
leaving that as folklore in a comment.

**Shed, don't queue.** The concurrency limit is a middleware holding a
counting semaphore. Past `maxInFlight` in-flight requests, the next one gets
`503` and `Retry-After` immediately — it is never parked waiting for a slot.

That is the real decision, and it is worth stating why queueing is the wrong
answer rather than the safe one. Under sustained overload a queue does not
reduce the work; it hides it. Latency grows without bound, every client waits
longer for a response it will eventually time out on, and the server spends
its capacity producing answers nobody is listening for any more. Shedding
keeps the requests it does serve fast and hands the rest an immediate, honest
answer they can act on — for a metrics reporter, "retry" or "drop this
interval", both far better than hanging.

Shedding is only a defence if refusing is much cheaper than serving.
`BenchmarkShedRequest`: **179 ns and 64 B to refuse, against 1862 ns and
5.8 kB to serve an ingest** — about 10×, so an overloaded server spends
almost nothing on what it turns away.

**A shed request is shed completely.** The limiter sits outside the handler,
so a refused request never reaches the store. This is the property that makes
shedding compatible with everything else here: a request that were
half-applied and then refused would make the `503` a lie and leave the store
holding events no client was ever told about — worse than dropping them,
because it would be silent. Three concurrency tests hold that line, each at a
capacity small enough that shedding is guaranteed and each asserting shedding
actually happened, so a limiter that never shed could not pass them by
default.

It also means the capacity check runs *before* the method check, so `GET
/ingest` under overload is a `503` rather than a `405`. That is deliberate:
under overload the server should not spend cycles classifying requests it is
not going to serve.

*(The counters named here were exposed in §31, which reversed the "keep them
to tests" half of that call while keeping the reason behind it: they are
published as metrics under a reserved prefix, not as extra fields bolted onto
the stats response.)*

**`/health` is exempt, and it is the exemption that matters.** A health check
that gets shed makes an overloaded server look like a dead one, so whatever
is watching — an orchestrator, a load balancer — kills or depools the
instance. That turns a server still serving most of its traffic into one
serving none, and moves its load onto its equally-loaded neighbours. The
check costs nothing to answer; being wrong about it is how an overload
becomes an outage. `TestHealthIsNeverShed` uses a limiter of capacity zero,
which sheds everything, and then checks the other four routes *do* get `503` —
otherwise a `200` on `/health` would only prove the limiter was never wired
in.

**Why the limit is tunable when `shardCount` is not.**
`METRICFLOW_MAX_INFLIGHT` overrides `maxInFlight`. §24 argued that
`shardCount` should stay a constant because exposing it invites tuning nobody
has data for, and that still holds — `shardCount` is an algorithmic choice
whose right value follows from the code. A concurrency limit is an
operational one: it depends on the machine, the deployment, and what a
request costs there, none of which this code can know. A bad value fails
startup rather than falling back to the default, for the same reason a bad
alert rule does (§18) — a misconfigured limit that quietly ignores you is
found at 3am; a refusal to start is found immediately.

**Little's law, and why 400 clients could not overload this server.** The
first attempt to make a live server shed used `-workers 400` against a limit
of 256, and it never shed once. Concurrency in a handler is arrival rate ×
service time, not client count: 400 clients each waiting on a ~2 ms round
trip for a request that spends ~7 µs inside the handler put barely one
request in flight at a time. Saturating 256 honestly took 1500 workers
sending 2000-event batches — 5.86 M events/sec, and 92 requests shed. That is
a fine thing to learn about the server and a terrible basis for a CI gate,
which is the other reason the limit reads from the environment: CI starts a
server with 4 and overloads it on a two-core runner.

Live, with the limit at 4 and 64 workers sending 20-event batches: 282 136
requests, **61.8% shed**, and `verify` exact at 2 154 860 accepted and
2 154 860 recorded. The contrast with the unlimited run is the argument in
one line — the unlimited run dropped 205 connections outright with no
response at all, while the shedding run answered every single request, 38%
served and 62% refused in 179 ns each.

**Where exactness genuinely stops.** A request that gets no response is the
one case where the server does not owe the harness equality: it may have
recorded those events and failed on the way back. That is at-least-once, and
it is correct. `verify` now widens its check by exactly the number of events
in unanswered requests and says so, rather than either failing a correct
server or pretending the number is exact. Outside that window it still fails,
so it is still a gate. This is the honest limit of the invariant the whole
project is built on, and it only becomes visible once failure is injected at
the transport rather than at the contract.

**What this deliberately does not do.**

- *No per-client fairness.* The slots are global, so one noisy client can
  consume all of them and shed everyone else. Fixing that means per-client
  accounting and an identity to account against, and this server has no
  notion of client identity at all. Named here rather than half-solved.
  **Closed by §29**, which adds that identity and a per-client share on top
  of this pool — the global limit here still applies underneath it.
- *No smoothing.* With no queue whatsoever, a brief burst that a one-deep
  queue would have absorbed is shed instead. That is the accepted cost of
  refusing to hide work, and the right knob for it is the limit, not a queue.
- *No rate limiting.* A client within the concurrency limit may send as fast
  as it likes. Concurrency and rate are different quantities, and only the
  first is bounded here.

### 27. Cardinality: the way metrics systems actually die

> **In one line:** An unbounded metric-name space is how metrics systems
> actually die, so names are capped per shard and idle ones swept.

Every limit so far has bounded a *request* — its size (§17, §25), how long
it may take, how many may run at once (§26). None of them bounded what a
request could leave behind. Nothing stopped a client putting a request ID, a
user ID, or a UUID in a metric name, and every distinct name became a
permanent map entry. The store had no upper bound on memory that depended on
anything except client behaviour.

This is not a theoretical failure. A cardinality explosion is the single most
common way a real metrics system falls over, and it is almost always an
accident — one service adds a label that happens to be unique per request,
and the monitoring dies before the thing it was monitoring does.

**Two holes, and they need different fixes.**

*Nothing reclaimed.* §10 chose eviction on write, which quietly meant a metric
nobody writes to is never evicted by anything: its buckets age out of every
query but stay in memory, and its map entry lives forever. For a fixed set of
metrics that is exactly what §10 signed up for. For a client generating names
it is a leak.

*Nothing refused.* There was no cap on how many distinct names could exist,
and none on how long one could be.

Neither fix works alone. A sweeper cannot keep up with a client inventing
names as fast as it can send them, and a cap with no sweeper would fill once
and stay full, refusing legitimate new metrics forever.

**Sweeping.** `Store.sweep` walks every shard, evicts aged buckets, and
removes any metric left holding none — a metric with no buckets inside the
window is indistinguishable from one that never existed, so keeping the key
is keeping a name rather than data. It runs on a ticker at `bucketWidth`,
which is the granularity at which anything can age out; sweeping faster would
walk the same maps to find the same nothing.

One shard at a time, never all at once, for the same reason `/stats` walks
them that way (§24). A sweep is not a snapshot and does not need to be.

**The cap is per shard, not global.** A global counter would be exact, and
every ingest would touch it — precisely the single point of contention §24
spent a slice removing. A per-shard check costs nothing: the shard lock is
already held and the count is a `len()` on a map already in hand.

What that gives up is exactness. FNV spreads names evenly, so a realistic
workload fills shards at about the same rate, but an adversary who searches
for names landing on one shard can exhaust that shard's budget while the rest
sit empty. That turns out to be the *better* failure: the blast radius is one
shard and the other 31 are untouched, where a global cap would have let the
same attacker lock out every metric in the store.

**The check is only on the new-name path,** so an event for a metric that
already exists never even reads the count. That is both a performance
property and the point of the whole limit: a full shard keeps accepting
writes to every metric it already holds, so a client inventing names cannot
degrade the metrics that were already there.

**Ordering, which is load-bearing.** `admit` is now the single definition of
"the server took this event" — the contract check, then the capacity check,
shared by both ingest paths so they cannot drift. Validation comes first so a
malformed event is refused on its own merits before it can consume a
cardinality slot. Otherwise a client sending garbage could fill the store
with names that were never going to be valid, which is the cheapest possible
denial of service.

**Bounding the name, not just the count.** Writing this section exposed the
hole that made the cap nearly worthless: nothing limited how long a name
could be, and a batch body may be 1 MiB. 32 shards × 1024 names × ~1 MB is
tens of gigabytes. The two dimensions multiply, and capping one alone bounds
nothing. `maxMetricNameLen` is 256 bytes — generous against real names of
well under a hundred — which puts name memory at a few megabytes at full
cardinality.

**Status codes, and why not the obvious ones.** A cardinality refusal is a
**429** with `Retry-After`, not a 400 and not a 503.

- 400 would tell a client its request was malformed. It wasn't; the name is
  perfectly well-formed and the store is simply full.
- 503 is already this server's answer for too many requests at once (§26).
  Merging two unrelated conditions into one code would leave an operator
  unable to tell overload from a naming bug.
- 429 says what is actually true — you are asking for more than you are
  allowed — and carries backoff guidance. `Retry-After` is the window rather
  than the sweep interval, because a slot frees when some other metric ages
  out entirely, not merely when a sweep runs.

An oversized *name*, by contrast, is a 400: it will never be acceptable at
that length whatever the store does next.

**In a batch it is a per-event rejection,** so `BatchError` gains
`Retryable`. The distinction matters enormously to a client: a contract
violation is permanent and resending it wastes both sides' time, while a
cardinality refusal may succeed later. Without the field a client would have
to parse the message to tell them apart, which is no contract at all.

That has a consequence worth stating, because the live run caught it. A batch
rejected *entirely* for cardinality was answering 400 under §25's "nothing
accepted means client error" rule. But every event in it was well-formed, and
a client obeying that 400 would stop retrying data it should retry. Such a
batch now gets 429. One permanent rejection anywhere in it puts it back to
400, because then some of it really will never be valid.

**The measurements** (Ryzen 7 7800X3D, `-count=3`, median):

| | ns/op | allocs |
| --- | --- | --- |
| `record`, metric already exists | 99.4 | 0 |
| `record`, metric is new | 370 | 6 |
| `record`, refused by the cap | 25 | 0 |
| `sweep`, nothing to reclaim | 45 per metric | 0 |
| `sweep`, reclaiming everything | 167 per metric | — |

The first line is the one that had to be checked: 99.4 ns against §24's 100 ns
for the same benchmark, so the cap really is free for events on existing
metrics.

The third is the shape a limit like this should have. A client flooding new
names is refused for 25 ns and zero allocations — four times cheaper than a
legitimate update, fifteen times cheaper than the metric creation it is
trying to force. An attack that costs the server less per event than ordinary
traffic is not much of an attack.

Sweeping is a standing tax on a healthy server, since it runs whether or not
anything went idle: at a completely full store that is 1.5 ms every 10 s, a
0.015% duty cycle, allocating nothing. The number that matters for ingest is
not the total but the per-shard lock hold, because a sweep takes one shard at
a time — a full shard reclaiming everything is about 170 µs, which is the
longest any writer can be made to wait on it.

**Live**, 8 workers sending 50-event batches with 20 000 names each:
6 627 050 events, **3 878 893 refused for cardinality**, 57.9% of requests
answered 429 — and `verify` exact at 2 748 157 accepted and 2 748 157
recorded. The store filled to its cap and kept serving. `-expect-cardinality`
fails the run if the server never refused a name, so the gate cannot pass
against a server with no limit.

**What this deliberately does not do.**

- *No per-client attribution.* Same gap as §26's global slots, and the same
  cause: this server has no notion of client identity. One client's names can
  fill a shard that another client's legitimate new metric then can't enter.
  Fixing it needs identity first, which is a larger change than a limit.
  **Closed by §29** — as a rate on new names per client rather than a share
  of this stock, because a stock would need an owner recorded on every metric
  so the sweeper could give the budget back.
- *No eviction of existing metrics.* A full shard refuses new names rather
  than making room by dropping an old one. Evicting real data to admit what
  is usually garbage is the wrong trade, and an LRU here would let an
  attacker push out exactly the metrics an operator cares about.
- *The global cap is approximate.* `shardCount × maxMetricsPerShard` is the
  ceiling, but an uneven hash or a targeted attacker reaches a shard's limit
  before the store is anywhere near that total. Discussed above — it is the
  price of keeping the check off the contended path, and the failure it
  produces is contained rather than total.
- *No limit on values.* Cardinality is about names. A metric with a wild
  value distribution costs the same as any other, because §23's histogram is
  bounded by construction.

### 28. Bounding the cost of a query, and surviving a panic

> **In one line:** A query computes a bounded number of metrics however
> large the store, and a panicking handler returns 500 instead of dropping
> the connection.

§27 fixed the last unbounded *input* and, in doing so, created the problem
this section is about. Capping cardinality bounded memory — but it also made
the ceiling reachable and stable. An attacker can push the store to exactly
`shardCount × maxMetricsPerShard` names and park it there, and every `/stats`
then merged all 32 768 of them and sorted a histogram per metric for three
quantiles.

Measured before this change, at full cardinality: **77.2 ms of CPU, 62.2 MB
of allocation, and a 4.05 MB body — for one ~30-byte unauthenticated GET.**
§26's limiter admits 256 concurrent requests, so that is ~16 GB of allocation
churn and 20 CPU-seconds from clients sending 8 KB between them. §26 made
*shedding* cost 179 ns precisely so overload would be cheap to refuse; this
was the one endpoint where a single **admitted** request was expensive enough
to make that irrelevant. The cardinality cap had traded a memory exhaustion
for a CPU amplification, and the amplification was the larger of the two.

Every limit up to here bounded an input — body size (§17, §25), time and
concurrency (§26), cardinality (§27). None bounded the work one request could
ask for.

**Three phases.** `/stats` now collects names under each shard lock in turn,
then sorts and selects holding no lock at all, then merges each selected
metric under its own shard lock.

Phase three is where the quiet win is: `aggFor` returns an `Agg` whose
histogram `mergeBuckets` freshly allocated (§23 pins that with a test), so
the quantile sort — the expensive part — happens on a private copy *outside*
the lock, with nobody waiting on it. The lock-hold consequence matters as
much to ingest as the CPU does: the old code held one shard's lock through
~1024 merges and sorts; the longest hold is now a single metric's merge.

**Bounded selection.** Collecting every matching name and then sorting would
still allocate in proportion to the store — 2.6 MB of names gathered to keep
1000 — which is the same "one small request, unbounded work" shape moved from
CPU to memory. `nameSelector` keeps the smallest `limit` names while never
holding more than twice that many: once full, any name at or past the largest
it is keeping cannot make the cut, so it is rejected by one string compare
and never stored. The cutoff falls quickly, so nearly every name past the
first few hundred costs a comparison and nothing else.

| at 32 768 metrics | before | three phases | + bounded selection |
| --- | --- | --- | --- |
| `/stats` | 77.2 ms / 62.2 MB | 6.11 ms / 3.83 MB | **3.72 ms / 1.19 MB** |
| `selectNames` | — | 4.00 ms / 2.64 MB / 23 allocs | **1.33 ms / 32.8 kB / 1 alloc** |

**20.8× faster and 52× less allocated**, with the allocation *count* flat at
~10 000 whatever the store holds — which is what a bound looks like. A narrow
`?prefix=` query is 364 µs. 256 concurrent requests now churn ~305 MB rather
than ~16 GB.

**`?limit=` may only narrow**, exactly as `?window=` may only narrow retention
(§11): it is both the default and the ceiling, because a limit a caller can
raise is not a limit and this endpoint is unauthenticated. Out-of-range
values are rejected rather than clamped — a caller who asks for 50 000 and
silently receives 1000 has been handed wrong data with a 200 attached.

**Sorted order is load-bearing, not decoration.** Map iteration order in Go is
deliberately random and the names are spread across 32 shards, so an unsorted
selection would return a different subset on every call. Sorting makes a
truncated response reproducible, and it is what lets `?after=` page through
the store with no cursor the server has to remember — keyset pagination over
a key that already exists.

**A bounded answer that does not say it is bounded is a wrong answer**, so the
response carries `matched`, `truncated` and `next`. Two subtleties are worth
stating because both have tests:

- `matched` counts **names**, not metrics with data. Whether a metric has
  anything inside the window is only known after merging it, which is exactly
  the work the limit exists to avoid doing for everything. So a page can hold
  fewer metrics than its limit without being the last page.
- `next` is the last name **considered**, not the last one returned. Paging
  from the last returned name would stall forever on a run of metrics whose
  buckets have all aged out.

That flag immediately earned itself: bounding `/stats` broke `loadgen`, which
read one response and summed it, reporting "accepted 1790472 events but
recorded 56000". `verify` now follows the cursor until the response says it
is complete. It applies the prefix client-side as well as sending it, because
a server ignoring the parameter is precisely the bug the harness exists to
catch, and it bounds its own paging loop — a harness that can hang is worse
than one that fails.

---

**Panic recovery, which is two different problems.**

`run()` started the alerter and the sweeper as bare goroutines. A panic in
either takes the whole process down, and that is the worst outcome available
here: §2 keeps every aggregate in memory, so process death throws away the
last minute of every metric instantly — the only thing this server actually
holds. Losing the sweeper degrades memory slowly; losing the alerter stops
notifications. Losing the process loses everything.

The asymmetry is what decided it. `net/http` *already* recovers a panic in a
handler — it logs, drops that one connection, and the server carries on — so
the background loops were strictly **less** safe than the request path, which
is backwards for code that holds shard locks and runs unattended.

`supervise` recovers, logs with the loop's name and a stack, waits a backoff
and restarts. It does not give up after N attempts, because stopping silently
is the one outcome worse than crashing: an alerter that has died looks exactly
like one with nothing to report. A loop that panics every tick logs every
tick — noisy and impossible to miss. The backoff stops a loop that panics
before reaching its ticker from spinning as fast as the CPU allows, and is a
parameter rather than the constant for the same reason `Alerter.Run` takes its
interval (§18). Cancellation during the backoff is a `select`, not a sleep,
because `run()` waits on these loops before the process exits.

On the request path, `recoverPanic` is **not** about keeping the server alive —
the stdlib does that. It is about what the client is told. A dropped
connection is a transport error, indistinguishable from a network blip, and
§26 taught `loadgen` that a request with no response is *unknown*: the server
may have recorded those events before failing, so `verify` widens its check by
exactly that much. A panic delivered as a 500 is a definite failure and stays
out of that bucket; the same panic as a dropped connection quietly erodes the
exactness claim the whole project rests on.

Two panics it deliberately does not swallow:

- `http.ErrAbortHandler` is the documented way to abandon a response on
  purpose, so converting it to a 500 would break the one panic that is not a
  bug.
- A panic *after* the response has started cannot become a 500, because the
  status is already on the wire. Swallowing it would hand the client a
  truncated body under a 200 — corrupt data that looks complete, the worst
  outcome available in a project built on exactness. It is re-panicked so the
  connection breaks and the client sees a broken transfer, which is at least
  honest.

**What this deliberately does not do.**

- *The walk is still O(cardinality).* "Which metrics exist" cannot be answered
  for less without an index, and an index is a second structure to keep
  consistent with the store under every write. The walk is now ~300 µs of
  string comparisons at full cardinality, against the ~77 ms it used to
  guard, so the remaining floor is not what hurts.
- *No aggregation across pages.* A caller wanting a total over 32 768 metrics
  must page and add up. That is the honest consequence of refusing to compute
  an unbounded answer in one request.
- *The response is still built in memory before encoding* — but bounded by
  `limit` now rather than by the store, which was the actual problem.
- *A supervised loop that panics every tick still does no work.* It stays
  loud rather than silent, which is the best available outcome without a
  health signal the loops can fail; giving them one is a larger change than a
  recover.

### 29. Client identity, and the fairness it buys

> **In one line:** A self-asserted X-Client-ID buys per-client shares of
> concurrency and new names, so one broken service cannot cost everyone
> else their monitoring.

Three sections ended with the same admission. §26's shed slots are global, so
one noisy client can consume all 256 and shed everyone else. §27's cardinality
budget is shared, so one client's names can fill a shard another client's
legitimate metric then cannot enter. §28 bounded a single query but not a
client issuing many. All three said the fix needs a notion of client identity,
which the server did not have.

**Scope, which decides everything else.** The demo scenario is a fleet of
services under one operator, not a set of mutually distrusting tenants. So the
failure worth preventing is an *accident*: one buggy service emitting unbounded
names, or one runaway reporter opening hundreds of connections, degrading
observability of every other service at exactly the moment somebody needs it.

That calls for **resource fairness**. It does not call for data isolation, and
the metric namespace stays shared and global on purpose — a fleet's metrics are
meant to be queried together. Namespacing metrics per client would be a
different product, and it would break every cross-service query the project
exists to serve.

**The trust model, stated plainly because the limits would otherwise look like
a defence they are not.** `X-Client-ID` is self-asserted. That is the same
contract Cortex and Mimir give `X-Scope-OrgID`: a tenancy boundary, not an
authentication boundary. It isolates clients that are honest about who they
are — which is every client that is merely broken, and that is the population
this is for. A deployment facing untrusted callers must set the header at a
trusted proxy and strip whatever the caller sent.

An attacker who rotates the header simply gets a fresh budget each time. That
makes the identity table §27's problem one level up, and it has to be, because
the identifier comes from the caller: an unbounded set of IDs is an unbounded
set of counters. So the table is capped, and everyone past the cap shares one
bucket. **Under a rotating-ID attack the server degrades to precisely the
global limits it had before this section existed** — which is the honest
ceiling of what an unauthenticated identity can promise, and worth saying out
loud rather than leaving a reader to discover.

Overflow is its own bucket rather than the anonymous one, so a rotating caller
cannot degrade the honest callers who simply sent no header.

A malformed identifier is **rejected**, not quietly demoted — the call §11, §25
and §28 all made. A client that sent an ID and was silently pooled believes it
has a budget it does not have, and will be baffled when someone else's traffic
throttles it.

`/health` is outside `identify` as well as outside the limiter, for §26's
reason: a malformed header from a sidecar must not make health checks fail, or
an orchestrator kills a server that is serving everything else perfectly well.

**Per-client concurrency** (closes §26's gap). A request needs its caller's own
share as well as a slot in the shared pool. The caller's share is checked
first: it is the cheaper test, and checking it first means a client over its
limit never touches the shared pool at all, so its excess cannot even
momentarily displace anyone.

`perClientInFlight` is 32 against a global 256 — a *share*, not a partition.
Dividing the pool would waste capacity whenever fewer clients are active; this
only stops any one client taking enough to starve the rest, and eight clients
would have to misbehave at once before anybody else is shed.

The two refusals get different statuses. **503 says the server is at capacity;
429 says you are asking for more than you are allowed**, which can be true
while the server is nearly idle. Collapsing them would leave an operator unable
to tell a saturated fleet from one greedy reporter.

**Per-client name rate** (closes §27's gap), and this is the design decision
worth defending. It is a *rate*, not a stock.

A per-client stock limit would have to know which client created each name, so
the sweeper could return the budget when it reclaimed one — an ownership field
on every metric and a store restructure to carry it. A rate needs one counter
per client and nothing on the metric at all. The epoch is derived from the
clock rather than reset by a ticker, so there is no third background loop to
run, supervise and shut down, and a client that goes quiet needs no cleanup to
stop counting.

It **composes with §27 rather than replacing it**: the global cap still bounds
absolute memory, and this bounds how fast any one client can consume it. A
service with a stable set of names creates nothing after startup and never
touches the limit. A service emitting a name per request is held to 64 per
10 s, so reaching the global ceiling alone would take over an hour of sustained
flooding — by which time the sweeper has long since reclaimed its earlier
names, so in practice it never gets there.

Two orderings are load-bearing. The **global cap is checked before the client's
budget**, so a client is never charged for a name it could not have created
anyway, and a genuinely full store reports the reason the client cannot fix by
slowing down. **Validation still comes first**, which now protects two budgets
instead of one: a malformed event spends neither the store's cardinality nor
the caller's allowance.

The limit is on *new* names only, so a client that has run out can still write
to metrics it already has — otherwise a brief flood would silence a service's
real metrics too.

**The cost** (Ryzen 7 7800X3D, `-count=3`, median):

| | ns/op | allocs |
| --- | --- | --- |
| `record`, no client | 96.8 | 0 |
| `recordFor`, existing metric | 95.4 | 0 |
| `identify`, per request | 167 | 4 (408 B) |
| acquire + release, contended | 19.4 | 0 |
| refused by its own share | **3.1** | 0 |

The first two lines are the one that had to hold: the client budget is
consulted only when a name is created, so an event for a metric that already
exists costs exactly what it did before. `identify`'s allocations are the
request context, paid once per request rather than per event — a rounding error
at a batch of 1000.

The last line is the shape a fairness limit should have. Refusing a client over
its share costs 3.1 ns and no allocations: 57× cheaper than §26's shed, and
some 600× cheaper than serving the request. A client hammering past its limit
is very nearly free to say no to.

**Live.** A noisy client floods metric names; three seconds in, a quiet client
starts introducing new names of its own into the store under attack:

| | requests | events accepted |
| --- | --- | --- |
| noisy | 99.9% answered `429` | 1 920 |
| quiet | 100% answered `200` | 2 290 350, `verify: OK` |

And the store went from empty to **132 metrics against a 32 768 ceiling**. The
same flood before this section filled that ceiling in seconds, at which point
the quiet client's new names would all have been refused. That is the whole
difference: the budget is now spent by whoever is spending it.

**What this deliberately does not do.**

- *It is not authentication.* Stated above, and restated here because it is the
  limit that matters most: this stops accidents, not attackers. An attacker
  rotating the header falls back to the global limits, which still hold.
- *No data isolation.* Any client can write to any metric name, and `/stats`
  shows everything. That is intentional for a single-operator fleet and would
  be wrong for real tenants.
- *Query cost is bounded per request (§28) and per client only through
  concurrency.* A client within its concurrency share can still issue expensive
  queries back to back. A cost-weighted budget — charging a client for the
  metrics a query computed rather than the requests it made — is the natural
  next step, and needs a notion of cost the server does not have yet. (§34
  measured that cost and §35 charges it.)
- *The limits are fixed, not adaptive.* A client gets the same share whether it
  is the only one connected or one of two hundred. Adaptive shares would use
  the pool better and would need the server to track offered load per client
  over time, which is a scheduler, not a limit.
- *The overflow bucket is shared.* Past `maxClients`, honest newcomers land in
  the same bucket as whoever filled the table. Their traffic is still bounded
  and still served; they simply stop being isolated from each other.

### 30. Parsing the one shape this server ingests

> **In one line:** A hand-written parser for the one shape this server
> ingests is about 4x faster than encoding/json, held honest by
> differential testing and fuzzing.

§25 measured the batch path at ~730 ns per event and found decoding was 536 of
it — 73%, against about 100 ns for everything the store does. It named JSON
decoding as the next bottleneck and left it there. A probe split the number
further:

| | ns/op | allocs |
| --- | --- | --- |
| `json.Unmarshal`, one event | 372 | 1 (48 B) |
| `validateEvent` | 15 | 0 |

So the cost is reflection, not validation, and the allocation is the metric
name. Reflection is the right default for arbitrary structures and the wrong
one for a fixed schema of three fields, which is what this server ingests and
all it will ever ingest.

| | before | after |
| --- | --- | --- |
| plain event | 372 ns / 48 B / 1 alloc | **96 ns / 16 B / 1 alloc** |
| with unknown fields | 748 ns / 48 B / 1 alloc | **185 ns / 16 B / 1 alloc** |

**The safety argument, which matters more than the number.** Writing a JSON
parser is an excellent way to be subtly wrong, and being subtly wrong here
changes the wire format under existing clients — a worse bug than a slow
server. So the parser is not justified by reading the spec carefully. It is
held to `encoding/json` by differential testing: for any input, the two must
agree both on the decoded event and on whether the input was valid at all.

A table of ~150 inputs encodes the standard library's quirks, each of which
had to be taught rather than guessed:

- field names match exactly, then case-insensitively (`{"NAME":"x"}` sets
  `Name`);
- the later of two duplicate keys wins;
- `null` leaves a field at its zero value rather than erroring, for every
  type, including a bare top-level `null`;
- an integer field is parsed from the literal text, so `"ts":1e3` and
  `"ts":1.0` are errors though both name whole numbers;
- the number grammar rejects `01`, `1.`, `.5` and `+1`.

**The table found three divergences before the fuzzer ran.** A bare `null`
decodes to the zero value; my first version demanded an object. And skipping
unknown fields by counting brackets accepted `{"a":{"b":}}` and `{"a":[1,]}` —
both balance perfectly and neither is JSON. Skipping now parses properly, with
a depth limit matching `encoding/json`'s, because without one a body of open
brackets recurses once per byte and `maxBatchBody` allows a million of them.

**The fuzzer found the one no hand-written table here would have.**
`encoding/json` does not *reject* invalid UTF-8 inside a string — it silently
replaces each bad byte with U+FFFD. Passing the raw bytes through would have
stored metric names the old decoder could never produce. It surfaced on the
eighth seed within a tenth of a second, and ~24 M executions since have found
nothing else.

**Allocation took two passes after the first correct version.** Field names
were being turned into strings purely to be compared and thrown away — three
an event — and nested keys and strings inside *skipped* fields were too. Both
now work on bytes, and the float literal reaches `ParseFloat` through a stack
buffer. What remains is one allocation for the metric name, which has to
become a string because the store keeps it as a map key.

**Streaming had to be rebuilt, not swapped.** §25 chose a stream deliberately
so a batch decodes in constant memory however long it runs, and `json.Decoder`
was providing that. `eventReader` replaces it: a buffer, a parse, and a refill
when `parseEvent` reports the value is only a prefix so far. A value split
across two reads is re-parsed from its start, which is cheap and happens at
most once per refill, where a resumable parser would complicate every function
in the file to save nothing measurable.

It is **not line-based**, which was the tempting simplification. §25 documented
and tested that events may be separated by any whitespace — pretty-printed, or
several to a line — because a stream of JSON values is not a stream of lines.
Going line-based would have been easier and would have silently narrowed what
the server accepts. Every §25 batch test passing unchanged is the proof it
did not.

**Pooling the read buffers was not optional.** The first measurement showed
size-1 batches getting *slower* than the decoder they replaced — 3618 ns
against 2843 — because every request allocated 16 kB that a batch of one is
entirely dominated by. Pooled, the same case is 1845 ns. Only the original
buffer goes back; one that grew to hold an oversized event is dropped, or a
single hostile request would leave a megabyte resident for the life of the
process.

| batch size | before | after |
| --- | --- | --- |
| 1 | 2843 ns/event | 1845 |
| 10 | 868 | 604 |
| 100 | 766 | **243** |
| 1000 | 777 | **243** |
| 10000 | 713 | 229 |

Live, 8 workers over a real socket:

| | before | after |
| --- | --- | --- |
| `-batch 1` | 56 100 events/sec | 74 655 |
| `-batch 100` | 1 544 644 | **2 916 426** |
| `-batch 1000` | 2 851 755 | **5 822 152** |

Roughly double at realistic batch sizes, with `verify` exact at every size.
This is the difference §25 predicted between optimising the dominant term and
optimising a measurable but invisible one: decoding was 73% of per-event cost,
so halving it moves a number clients see. The shard-grouping idea §25 cancelled
was 14%, and would not have.

**One behaviour changed on purpose**, found by a test rather than planned. A
read error that is *not* EOF is now reported even when the leftover bytes are
whitespace. The events already handed over stand, but the stream broke rather
than ended, and the server cannot know whether more was in flight — calling
that a clean end would tell a client its whole batch had been seen.

**What this deliberately does not do.**

- *`encoding/json` is still here*, and should be: it encodes every response,
  it is what the load generator and the tests use to write events, and it is
  the oracle the parser is tested against. Replacing it on the output path
  would be optimising something nobody is waiting on.
- *The struct tags stay.* They no longer drive decoding, but they still
  describe the contract, they are what `encoding/json` uses when a test writes
  an event out, and changing one still changes the wire format.
- *The metric name is still allocated.* Interning it against the store's
  existing keys would remove that for established metrics, but it adds a map
  lookup to the hot path in exchange for an allocation — plausibly a wash.
  Not attempted, and explicitly not measured; it would need the measurement
  before the code.
- *The parser reads `Event` and nothing else.* It is not a general JSON
  library and must not become one: its correctness argument is that it agrees
  with `encoding/json` on this one shape, and that argument does not extend.

### 31. The server watching itself

> **In one line:** The server records its own telemetry as ordinary
> metrics, under a reserved prefix no client can write to.

§26, §27 and §29 each added a counter and each stopped short of exposing it.
`shedded()`, `throttledCount()` and `tracked()` had exactly **zero callers
outside tests**: the server counted how often it refused a request for
capacity, how often it throttled a client, and how many clients it was
tracking, and an operator running it could see none of that.

§26 argued for leaving them there — *"a property of the server, not of the
metrics, and putting it in /stats would mix the two."* Half of that was right,
and it is worth separating the halves.

Bolting server counters onto the *shape* of the stats response — extra fields
beside the per-metric numbers — really would have mixed two unrelated things,
and would have made every client parse a response whose structure depended on
what the server felt like reporting. But the conclusion did not follow. A
metrics server's own telemetry **is** metrics. The right way to expose it is as
metrics: a reserved name prefix, queried with the same `?prefix=`, alerted on
with the same rules, aged out by the same retention. Prometheus does exactly
this with its own series, for the same reason.

**Two properties make that safe**, and both matter more than the exposure.

*A client cannot write into the reserved namespace.* Without it, the one signal
an operator reaches for during an incident is the one an incident can forge: a
service filling `metricflow.requests.shed` with zeroes makes a shedding server
look calm. It is a **permanent** rejection rather than a retryable one (§27,
§29), because no amount of waiting makes the name acceptable.

*The reserved namespace ignores the cardinality cap.* Self-metrics must not be
the first thing to fail when the store fills, because a full store is exactly
when somebody needs to see that it is full. That exemption is only safe
because the set is fixed by `self.go` rather than by any client — it cannot
grow without bound, which is the whole reason §27's cap exists.

**Counters are cumulative, not per-interval deltas.** The store is windowed, so
a cumulative counter reads back as `max - min` over the window: the increase
across it. That is how Prometheus treats counters, and it survives a missed
sample where a delta would lose one permanently.

That choice left a gap in the alerting layer, which is the sort of consequence
worth following rather than shrugging at. None of §18's stats can read a
counter: `max` only ever rises, so a rule on it fires once and stays firing for
the life of the process, and an alert that is always on is an alert nobody
reads. **`StatIncrease`** is `max - min`, and closes it. It is meaningless on a
gauge — but so is `p99` on a counter, and which stat suits which metric was
already the rule author's business.

**Two default rules, which are the point rather than a garnish.** They are
ordinary `Rule`s over the ordinary store, and the alerter needed *no changes at
all* to reach them. That is the argument for recording self-telemetry as
metrics instead of as a special response field, made concrete:

- `server-shedding` on the increase, `For: 30s`, so a brief burst — which
  shedding exists to absorb (§26) — does not page anyone, while sustained
  shedding means the fleet has outgrown this server.
- `cardinality-pressure` on the gauge's max, firing at **80%** of the ceiling
  rather than at it. By the time the store is full, legitimate new metrics are
  already being refused; the point of an alert is to arrive before that. A test
  pins that it fires below the ceiling, because a threshold quietly set at 100%
  would look right and be useless.

Both are validated at startup or the server refuses to run (§18), which matters
most for exactly these: they are the rules nobody would notice were broken,
because they only fire during an incident.

**The counters are incremented once per request**, with a batch's totals, not
once per event. A batch of ten thousand would otherwise take ten thousand
atomic increments on a counter every core is touching — contended, and
measurable against a 96 ns parse (§30). Adding the totals once gives the same
number for the price of one.

**Live**, with the in-flight limit at 4 and 64 workers hammering it, the
server's account and the load generator's independent one agree:

| | |
| --- | --- |
| loadgen | 245 718 requests shed, 324 099 served |
| `metricflow.requests.shed` | increase **243 692** |
| `metricflow.events.accepted` | increase 6 429 840 |
| `metricflow.runtime.goroutines` | 6 → 108 |

The remaining gap is the tail after the final sample, which is what a
one-second sampling interval costs.

**What this deliberately does not do.**

- *No heap or GC statistics.* `runtime.ReadMemStats` stops the world, and doing
  that once a second to report on memory would be a real cost to justify a
  number nobody asked for yet. `runtime/metrics` reads most of the same figures
  without the pause and is the right way in if this is ever wanted.
- *Self-metrics consume store capacity.* Nine metrics and their buckets, out of
  a 32 768 ceiling. The gauges also report a store that includes themselves —
  a fixed offset, and cheaper to explain than to correct for, since the size is
  read before each sample writes itself.
- *Sampling bounds the resolution.* Anything shorter than a second is invisible
  except through its effect on a counter, and the last fraction of a second
  before a crash is never recorded at all.
- *The server cannot observe its own death.* Everything here lives in the same
  process and the same memory as the thing it measures (§2), so a crash takes
  the evidence with it. That is the honest limit of self-monitoring, and the
  reason it complements an external check rather than replacing one — which is
  what `/health` has always been for.

### 32. Surviving a restart

> **In one line:** Periodic binary snapshots of the aggregates mean a
> crash costs at most one interval and a clean shutdown costs nothing.

§2 chose in-memory state with no persistence, and every section since has been
written on top of it: a restart loses the last minute of every metric. §31
sharpened that into a sentence — the server cannot observe its own death,
because the evidence dies with it.

**What to persist follows from §1, not from taste.** That section chose to
store the conclusion rather than the events, and persistence inherits the
choice. A write-ahead log of events would contradict it outright, and the
arithmetic settles it: at the 5.8 M events/sec §30 measured, a WAL is
megabytes a second of disk written to reconstruct numbers the server already
has. The aggregates *are* the conclusion, they are small, and they are what a
restart needs back.

So: **a periodic snapshot of the aggregates**, loaded at startup. The cost is
bounded and stated rather than hidden — a crash loses at most one snapshot
interval, and a graceful shutdown loses nothing, because it takes a final one
on the way out.

**The histogram goes in the file too.** Dropping it would leave a restore that
looks right on count and average and is wrong on exactly the percentiles §23
exists for. A test asserts the p99 of a 990-fast, 10-slow distribution
survives, because that is the number a lossy round trip would quietly flatten.

**Three properties, all about a file that might be damaged**, none about
speed. Snapshots are written once an interval, so nothing here is on a hot
path.

*A half-written file is never loaded.* Temp file, sync, rename. Without the
sync a crash can land the rename before the contents and leave an empty file
where a good snapshot was; without the rename a crash mid-write leaves a
partial file that the next start would refuse — throwing away a perfectly good
older snapshot to no purpose.

*A corrupt file is refused, not misread.* Magic bytes, a version and a
checksum, all verified before a single aggregate is decoded. Every single-bit
flip and every truncation of a real snapshot is rejected — about a thousand
cases, asserted exhaustively rather than sampled.

*A damaged file cannot crash or exhaust the process.* Every length prefix is
checked against the bytes actually remaining, so a flipped byte claiming four
billion buckets is an error rather than an allocation. That is **not**
redundant with the checksum: a file can be internally consistent and still be
nonsense, so the test re-checksums after corrupting a length, to test the
bounds rather than the CRC. Removing those checks does not produce a wrong
answer — it panics the process on allocation.

**The reserved namespace is excluded** (§31). Self-metrics describe one
process, and the process loading the file is a different one: carrying a
predecessor's counters forward would make a counter appear to fall the moment
the new process starts its own at zero, which is the one thing a counter must
never do. A file that *does* contain them was not written by this server, so
loading refuses them too.

**Restoring respects what is already true of the store.** Buckets that aged out
while the server was down are dropped — retention (§10), not data loss. The
cardinality cap still applies (§27), so a snapshot taken under a larger cap
cannot exceed a smaller one just because the data used to fit. Both counts are
logged, because a restart that silently restored a third of its metrics would
look exactly like one that restored all of them.

**Two failures, treated oppositely**, and the asymmetry is deliberate. A path
the operator got wrong, or a corrupt file, is **fatal at startup**: it is
certainly wrong and nothing is lost by refusing, the same call §18 makes for a
bad alert rule and §26 for a bad in-flight limit. (§33 keeps half of this and
reverses the other half: a bad *path* stays fatal, a corrupt *file* does not,
because those are not the same kind of wrong.) A write that fails **later**
is logged and the loop carries on, because killing a working server to protest
a full disk would throw away the very data persistence exists to protect.

`METRICFLOW_SNAPSHOT` names the file; empty means off, which is the default, so
a server never configured for persistence behaves exactly as it did before —
including writing no files at all.

**The cost:**

| | |
| --- | --- |
| encoded size | 289 bytes per metric |
| a full store (32 582 metrics) | 9.00 MB |
| snapshot, 2 000 metrics | 1.14 ms |
| decode, 2 000 metrics | 0.89 ms |

So a full store is roughly 18 ms of encoding every ten seconds — under 0.2% of
one core — and 9 MB written. The allocation is the output buffer growing by
doubling; sizing it from the previous snapshot's length is the obvious
improvement if that ever matters, and it does not yet.

**Live, as a real crash rather than a clean one** — hard kill, no graceful
shutdown, no final snapshot:

```
before:   4 metrics, 2212100 / 2200100 / 2213300 / 2206000 events, p99 10.075
restart:  restored 4 metrics (4 buckets), 0 dropped as stale
after:    identical counts, identical p99
```

Identical because the last periodic snapshot caught everything. A crash
*between* snapshots loses up to one interval — that is the bound, not a
promise of zero.

**What this deliberately does not do.**

- *It is not a write-ahead log, and bounded loss is the design rather than a
  shortcoming.* Zero-loss durability means persisting every event before
  acknowledging it, which is a different system with a different throughput
  story — and one that contradicts §1.
- *One file, no history.* There is no rotation and no previous generation to
  fall back on: a snapshot that is corrupt on disk means starting empty, after
  being told so. Keeping the last good one would be cheap and is the first
  thing to add if this is ever operated seriously. (§33 adds it.)
- *No format migration.* The version is part of the magic, so a future format
  is refused rather than misread — an upgraded server starts empty and says
  why. That is the right failure, but it is a failure.
- *No compression.* 9 MB at full cardinality is not worth a dependency or a
  hand-rolled encoder, and the file is written once every ten seconds.
- *It does not make the store durable, only the process restartable.* The disk
  is the same machine's; this survives a process dying, not the machine.

### 33. When the snapshot is the thing that is broken

> **In one line:** A second generation is kept and a corrupt file no
> longer stops startup, because refusing to boot turns a damaged cache
> into a crash loop.

§32 ended with two admissions. There was one file and no history, so a
snapshot that was corrupt on disk meant starting empty. And starting empty
was fatal: the server refused to come up. This section is about both, and
about which half of that turned out to be wrong.

**A second generation, because one copy is not a backup.** Each write rotates
the file it replaces into `snap.bin.prev` before the new one takes its place.
Restore tries the current file, then the one behind it, and reports everything
it passed over on the way.

The subtle part is that **the first write of a process never rotates**. On
startup the file on disk may be the corrupt one that just failed to load;
promoting it would overwrite a perfectly good previous generation with a
known-bad file, turning one damaged copy into two. Only a file this process
wrote, and therefore knows to be good, is ever promoted. That is the whole
reason the snapshotter is a struct with state rather than two free functions —
rotation needs memory, and nothing stateless can answer "did I write this?".

A crash between the two renames leaves no current file and a good previous
one. Restore treats a missing current file as a fallback rather than a
failure, because that window is real.

**Reversing §32 on what is fatal.** The old behaviour conflated two different
failures under one word.

A snapshot *path* the operator got wrong is a configuration error. It is found
before anything has been served, nothing is lost by refusing, and refusing is
the only way to make it visible. That stays fatal, exactly like a bad alert
rule (§18) or a bad in-flight limit (§26).

A *file that will not decode* is a data problem at runtime. The configuration
is fine; the disk had a bad day. Refusing to start on that turns a damaged
cache into an outage, and a permanent one — the server cannot come back until
somebody deletes a file by hand, which under an orchestrator is a crash loop.
Serving is the job. Losing a minute of aggregates is a far smaller failure
than not booting.

So it starts empty, says so at the top of its voice, and counts it.

**Two counters, not one, and the live demo is what forced that.** The first
version counted "startups that found a snapshot and could not use it". Then a
crash test damaged the current generation, the fallback rescued it, and the
server reported zero — correct by that definition and useless, because a
broken newest snapshot looked exactly like a healthy restart. The log said it.
A log line is not an alert, which was the entire argument of §31.

| metric | what it means |
| --- | --- |
| `snapshot.load_failures` | snapshot files found unusable, *including* ones the fallback rescued us from |
| `snapshot.empty_starts` | startups that ended up with nothing — the one that means data is gone |
| `snapshot.write_failures` | writes that failed |
| `snapshot.age_seconds` | time since the last successful write |
| `snapshot.bytes` | how big it was |

**Age is the signal the failure counters cannot give.** A snapshotter goroutine
that is wedged, or whose ticker never fires, produces no failures and no
snapshots: the failure count sits at zero looking healthy. Age is the only
number that notices nothing is happening.

**They are published only when persistence is switched on.** A zero age reads
as "just snapshotted", which is the healthiest value there is — a staleness
alert would be permanently satisfied by a server that has never written a
snapshot in its life. Absence is the honest answer. Failures, though, are
published before any write has succeeded, because "every write so far has
failed" is precisely the state worth seeing.

**Age and size are atomics rather than fields under the snapshotter's lock.**
That lock is held across a rename and a directory sync, so a reporter taking
it would block on the very disk it is trying to report about — and a stalling
disk is exactly when somebody needs the snapshot age. A health signal must not
wait on the thing whose health it describes.

**A bug the restart tests found, which had nothing to do with snapshots.**
Running them fifteen times produced a shutdown that failed with a deadline
error after exactly five seconds, with nothing in flight. `Shutdown` returns
once every connection is idle, and a connection that has been accepted but has
sent no bytes is not idle — Go calls it *new*, and `Shutdown` will not close
it. Nothing frees it but `readHeaderTimeout`. The grace period was also five
seconds, so the two raced: one silent socket, which costs a port scanner
nothing to open, could consume the entire drain budget and make a clean
shutdown report a failed drain. The budgets are added now rather than shared.

**Live, as a real crash followed by a damaged snapshot** — hard kill, no
graceful shutdown, then the current generation overwritten with garbage:

```
snapshot: .../snap.bin is unusable: snapshot is too short to be one
restored 3 metrics (3 buckets) from .../snap.bin.prev, 0 dropped as stale

  svc.checkout     count=6  avg=5.33333  p99=13.0663  survived
  svc.login        count=6  avg=5.33333  p99=13.0663  survived
  svc.search       count=6  avg=5.33333  p99=13.0663  survived

  metricflow.snapshot.load_failures   1
  metricflow.snapshot.empty_starts    0
  metricflow.snapshot.age_seconds     0.0073
  metricflow.snapshot.bytes           481
```

One unusable file reported, zero empty starts, every number intact. That pair
is the claim: the damage is visible *and* nothing was lost.

**What this deliberately does not do.**

- *Two generations, not N.* The second copy covers the failure that actually
  happens — the newest write is the one a crash damages. A third would cover
  correlated corruption of two files, which on one local disk is a bad bet
  against a problem a real backup solves properly.
- *No repair, and no verification on write.* A snapshot is not read back after
  writing, so a file that was born corrupt is discovered at the next start
  rather than at the write. Reading back every snapshot to prove it decodes
  would double the I/O for a failure the fallback already covers.
- *Both copies damaged still means an empty start.* That is the bound, stated
  rather than hidden, and it is now counted instead of being fatal.
- *Nothing here makes the disk less of a single point of failure* (§32). Two
  files on one machine survive a bad write, not a bad machine.

### 34. The read path while the write path is busy

> **In one line:** A query roughly doubles in latency under a saturating
> firehose; the fix that mattered was an allocation, and an RWMutex
> measured worse.

Every query benchmark before this one ran against a store nobody was writing
to, which is the one condition this server never runs in. "Fast queries out"
is a third of what §1 set out to build, and the only evidence for it came
from a quiet store.

**The contention is structural, not incidental.** `selectNames` takes each
shard's lock in turn and walks every name under it; ingest needs that same
lock to record a single event. A query is not a reader politely sharing with
writers — it is their peer, holding one shard shut for the length of the
walk. Sharding (§24) means it shuts one thirty-second of the store at a time
rather than all of it, which is the whole reason the number below is a
doubling and not a collapse.

**Measured**, 2 000 metrics, p50 of one full `/stats`:

| writers | p50 | while ingesting |
| --- | --- | --- |
| 0 | 2.00 ms | — |
| 1 | 2.04 ms | 6.3 M ev/s |
| 4 | 2.07 ms | 16.9 M ev/s |
| 16 | 3.72 ms | 32.8 M ev/s |

and from the other side, `record` against a query loop running back to back
with no think time at all: **131 ns → 199 ns**.

So a saturating firehose roughly doubles query latency, and a pathological
query loop costs ingest about a third of its throughput. Neither is a
realistic load — a dashboard polls every ten seconds — which is the point of
measuring it: the worst case is bounded and unremarkable.

**Reporting p99 alone would have invented a problem that is not there.** The
benchmark reports p50, p99 and max, and p99 is five times p50 *at zero
writers*, where there is one goroutine and no contention whatsoever. That
tail is the Windows scheduler, not the server. `GOGC=off` left it unchanged,
so it is not the collector either. A tail that is identical with and without
the thing you are measuring is not evidence about the thing you are
measuring.

**The profile found something better than the contention.** A fifth of every
object the query path allocated was inside `reflect.unsafe_New` and
`reflectlite.Swapper` — neither of which appears anywhere in this codebase.
They come from `sort.Slice`, which takes its slice as an interface and builds
a reflect-based swapper, allocating twice per call. `sortedBuckets` runs once
per histogram per query, so a `/stats` over a thousand metrics was spending
two thousand allocations on reflection to sort a `[]int32`. `slices.Sort` is
the same sort without the interface:

| | before | after |
| --- | --- | --- |
| `BenchmarkHistQuantile` | 29.5 µs, 4 allocs | 21.4 µs, 2 allocs |
| `BenchmarkStats` | 695 µs, 1 014 allocs | 640 µs, 814 allocs |
| `/stats` under ingest | 2.30 ms, 10 028 allocs | 2.09 ms, 8 027 allocs |

**An `RWMutex` was tried and rejected.** The obvious response to "reads and
writes contend" is to let reads share, and the shard lock's read sites —
`selectNames`, `aggFor`, `size` — really are read-only. Measured against the
plain `Mutex`:

| | change |
| --- | --- |
| query p50 | −0.1% / +0.6% / +8.1% / −2.5% — noise |
| ingest throughput | **−15% to −19%**, consistently |
| uncontended `record` | +2.5% |

Rejected, because the benefit it buys is concurrent *readers* and there is
only ever one query goroutine to collect it, while every writer on the hot
path pays the heavier lock. That is not an argument against `RWMutex` in
general; it is an argument about a workload with one reader and sixteen
writers, which is what a metrics ingest engine is.

**What this deliberately does not do.**

- *No attempt to make queries cheaper than the walk.* `selectNames` is O(names
  in the store) by construction, and §28 bounds what happens after it rather
  than the walk itself. A secondary index over names would remove the walk and
  add a structure every ingest has to maintain — the wrong trade for a server
  that ingests millions of times more often than it queries.
- *The remaining allocations are not chased.* `hist.merge` is still 41% of the
  objects, and it is honest work: merging sparse maps across a metric's
  buckets. Removing it means not materialising a merged histogram at all,
  which is a different design and not one 2 ms is asking for.
- *These numbers are from one machine with no `-race` and a noisy scheduler.*
  The p50s are stable and the ingest deltas reproduce; the tails are not
  evidence of anything.

### 35. Charging a client for the work it causes

> **In one line:** Clients are billed for the work their queries cause —
> metrics computed plus names walked — rather than for the number of
> requests they made.

§29 gave each client a share of concurrency and a share of new metric names,
and left a hole it named: *"A client within its concurrency share can still
issue expensive queries back to back."* One request at a time, entirely
inside every limit the server kept, a caller could ask for a thousand metrics
forever. It stopped there because a cost-weighted budget "needs a notion of
cost the server does not have yet". §34 measured that cost; this spends it.

**The unit is a metric-equivalent**: merging and sorting one metric's
buckets, about 2.6 µs. §34 also measured a name walk at 7.9 ns, so walking
325 names costs what one metric-equivalent costs — and that ratio is what
lets a single number price both halves of a query:

```
cost = metricsComputed + namesWalked/325
```

**Both terms, deliberately.** Pricing only the metrics returned would leave a
client free to run unlimited full-store scans behind a prefix matching
nothing: 260 µs of real work each, and free under that pricing. A test fails
if the walk term is dropped.

**20 000 units per 10 s epoch** is roughly 52 ms of query work — half a
percent of one core, or about 1.3 cores if all 256 clients sat at their
ceiling at once. A dashboard polling six panels at `limit=100` over a full
store spends about 1 200 of it. The ceiling is around eighteen maximum-size
queries, or many thousands of narrow ones: invisible to a real caller, finite
to a loop. The epoch is derived from the clock exactly as §29's name budget
is, so there is no third background loop to run and a client that goes quiet
needs no cleanup.

**Checked before, charged after.** What a query cost is not known until it
has run, so the budget is tested on the way in and billed on the way out. A
client can therefore overshoot by exactly one query — bounded by §28's
per-request limit, and pinned by a test that it is one rather than unlimited.
The alternative is estimating the cost beforehand, which buys away the
overshoot by being wrong about the number, a poor trade for a limit whose
entire purpose is to be proportional to real work.

**`429`, not `503`**, for the reason §26 and §29 both give: the server may be
completely idle while one client is over its own share. And the refusals are
published as `metricflow.clients.query_budget_refusals`, counted apart from
`requests.throttled` because they describe different faults — throttled is
*too much at once*, this is *too much work*, which a client can reach one
polite request at a time.

**Live.** A store filled by 24 clients — one could not do it, because §29
caps a client at 64 new names per epoch, which is exactly what that limit is
for — then a greedy caller asking for 1 000 metrics in a loop while a quiet
one asks for 100:

| | 200 | 429 |
| --- | --- | --- |
| greedy, `limit=1000` | 20 | **40** |
| quiet, `limit=100` | 10 | 0 |

```
store holds 5930 metrics
metricflow.clients.query_budget_refusals   40
greedy after one epoch: 200
```

The twenty is the part worth checking, because the model predicts it: at
5 930 metrics a `limit=1000` query costs 1000 + 5930/325 ≈ 1018, and
20 000/1018 ≈ 19.6. The budget ran out where the arithmetic said it would,
the quiet client never noticed, and the greedy one was serving again one
epoch later.

**What this deliberately does not do.**

- *Only `/stats` is priced.* `/alerts` costs a walk of the rule set, which is
  fixed by configuration rather than chosen by the caller, and `/ingest` is
  priced by §27 and §29 already. A caller cannot make `/alerts` expensive.
- *The price is a model, not a measurement.* Cost is computed from counts, not
  from time spent, so a metric with six buckets and one with a single bucket
  are billed the same. Timing each query would be exact and would make the
  bill depend on how loaded the server happened to be, which is a worse
  property for a limit a client is meant to be able to predict.
- *The ratio is a constant, not a calibration.* 325 comes from one machine.
  It is the right order of magnitude rather than the right number, and the
  benchmarks that produced it are checked in so a future machine can say so.
- *Still not adaptive* (§29). A client gets the same budget whether it is
  alone or one of two hundred.

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
