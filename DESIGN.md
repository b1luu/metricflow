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
