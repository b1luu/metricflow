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

A `sync.Mutex` guards every read and write of `aggs`.

- `net/http` runs each request on its own goroutine. Two concurrent `/ingest`
  calls writing the map — or `/stats` reading while `/ingest` writes — is a
  data race, which in Go can corrupt the map or crash the process.
- A mutex serializes access: "everyone touches the map at once" becomes
  "one at a time," which is what removes the race.
- **Why one coarse lock, not per-metric locks:** the critical sections are a
  handful of arithmetic ops — nanoseconds. Contention isn't a real problem at
  this scale, and one lock is far easier to reason about. Revisit only if
  profiling shows lock contention.

### 6. Route registration before `ListenAndServe`

`http.ListenAndServe` blocks for the life of the process, so every
`http.HandleFunc` call must come before it. A handler registered after it is
dead code. (This was an actual bug earlier in development.)

### 7. Missing JSON fields are not an error

`json.Unmarshal` only fails on *syntactically* invalid JSON. A body like
`{"name":"cpu.load"}` parses fine and leaves `Value`, `TS`, `Type` at their
zero values.

- We currently accept this — a zero-value event still records.
- **Future:** explicit validation (reject events with an empty `Name`, etc.)
  is a separate step, deliberately not folded into parsing.

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

## Testing

See `main_test.go`. The strategy:

- **Handlers are package-level functions** (`handleIngest`, `handleStats`, …),
  not closures inside `main()`, specifically so tests can call them directly.
- **`record(ev Event)`** holds the aggregation logic on its own, unit-tested
  without any HTTP machinery.
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
