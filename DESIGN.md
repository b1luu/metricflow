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
| `/alerts`       | GET    | Current state of every alert rule, as JSON.      |

Wrong method on any route → `405` (§13). Details: JSON shape §14, `?window=`
§11, alerting §18.

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
- **The fix when it matters:** shard the map and its lock by metric name, so
  writes to different metrics don't block each other. Still not worth the
  complexity — 7.7 M events/sec is orders of magnitude past anything this
  project ingests, and the honest engineering answer is that a measured
  bottleneck nobody is hitting is a note, not a task.

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

### 17. `/ingest` hardening for the hot path

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

**Not covered:** `go test -race`, which needs a 64-bit C toolchain this
machine doesn't have. These tests catch lost updates and Go's own
concurrent-map panic, but not subtler races the detector would find — worth
running under WSL or CI before claiming the concurrency is proven.

### 21. `cmd/loadgen`: the claim that needs a real socket

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
