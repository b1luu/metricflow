# MetricFlow

[![CI](https://github.com/b1luu/metricflow/actions/workflows/ci.yml/badge.svg)](https://github.com/b1luu/metricflow/actions/workflows/ci.yml)

A small real-time metrics ingestion and aggregation engine in Go — a mini
Datadog. Events are pushed in over HTTP; per-metric aggregates (count, average,
min, max, and p50/p90/p99) are kept live in memory over a rolling time window
and served back on demand.

Standard library only (`net/http`), no database, no external services.

## Run

```
go run main.go
```

Listens on `:8080`. `Ctrl-C` (or `SIGTERM`) drains in-flight requests, then
exits; a second `Ctrl-C` kills immediately.

## Endpoints

| Method | Path      | Description                                             |
| ------ | --------- | ------------------------------------------------------ |
| GET    | `/health` | Liveness check. Returns `ok`.                          |
| POST   | `/ingest` | Submit one metric event as a JSON body.                |
| GET    | `/stats`  | Per-metric count/avg/min/max and p50/p90/p99 over a time window (JSON, default 60s). |
| GET    | `/alerts` | Current state of every alert rule (JSON).              |

`/alerts` reports each rule as `ok`, `pending`, `firing`, or `nodata`, with
the value it last saw and when it entered that state. Rules are defined in
code (`defaultRules` in `main.go`) and re-evaluated every 10 seconds, so a
threshold crossing shows up within one interval — and a rule whose metric has
no data reads as `nodata`, never as healthy.

A rule may set `For`, requiring the breach to hold that long before it counts;
until then it sits in `pending` and logs nothing, so a metric flapping across
its threshold never raises an alert at all.

Rules compare `avg`, `max`, `min`, `count`, or a percentile (`p50`/`p90`/`p99`).
For latency, a percentile is usually the right choice: a `max` rule fires on a
single unlucky request, while an `avg` rule stays quiet even when a real
fraction of users are suffering.

## Percentiles

`/stats` reports p50/p90/p99 alongside the summary numbers, each within 1% of
the true value. They come from a log-bucketed histogram kept per metric per
time bucket — bucket counts, never the events — so memory stays bounded
however long the process runs.

They exist because an average describes a distribution badly. 99 requests at
20 ms and one at 900 ms:

```
count=100  avg=28.8  min=20  max=900  p50=19.9  p90=19.9  p99=907
```

The average is stranded between the two modes and describes neither; the max
reflects one request. p99 is the number that says 1% of users waited nearly a
second.

`/stats` accepts an optional `?window=` (Go duration, e.g. `?window=30s`),
capped at the 60-second retention window. `/ingest` requires `name` and `ts`
(unix milliseconds; the event is bucketed by `ts`, which must fall between 60s
ago and ~10s ahead); `value` and `type` default to zero if omitted. Each route
accepts only the method shown above — anything else returns `405`.

Event body (`ts` is unix milliseconds; events are aggregated by `ts`, so a
late arrival still counts toward the minute it happened in):

```json
{ "name": "cpu.load", "value": 0.8, "type": "gauge", "ts": 1757200000000 }
```

## Example

```powershell
$ts = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
Invoke-RestMethod -Method Post -Uri http://localhost:8080/ingest `
  -Body "{""name"":""cpu.load"",""value"":0.8,""ts"":$ts}"
Invoke-RestMethod http://localhost:8080/stats
# window  metrics
# ------  -------
# 1m0s    @{cpu.load=@{count=1; avg=0.8; min=0.8; max=0.8; p50=0.8; p90=0.8; p99=0.8}}

(Invoke-RestMethod http://localhost:8080/alerts).alerts
# rule          metric          state  value since
# ----          ------          -----  ----- -----
# cpu-hot       cpu.load        ok       0.8 2026-09-09T13:48:00
# cpu-spike     cpu.load        ok       0.8 2026-09-09T13:48:00
# slow-requests http.latency_ms nodata     0 2026-09-09T13:47:50
```

On non-Windows / with real curl:

```
curl -XPOST localhost:8080/ingest -d "{\"name\":\"cpu.load\",\"value\":0.8,\"ts\":$(date +%s)000}"
curl localhost:8080/stats
```

(PowerShell's `curl` is an alias for `Invoke-WebRequest`, which takes
different flags — use `curl.exe` or `Invoke-RestMethod` there.)

## Load testing

With the server running, drive it over real HTTP:

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

`-bad` mixes in deliberately invalid requests. After the run, loadgen fetches
`/stats` and checks the server recorded exactly as many events as it answered
`200` to, exiting non-zero if not — so it works as a CI gate, not just a demo.

## Test

```
go test ./...                              # unit + concurrency tests
go test ./... -race                        # needs a 64-bit C toolchain
go test -run=^$ -bench=. -benchmem         # throughput of the engine
```

CI runs all three on every push, plus an end-to-end job that starts the
server, drives it with `loadgen`, and checks it shuts down cleanly on
`SIGTERM`.

## Design

Aggregates are stored as fixed-width time buckets (10s each, 6 in a 60-second
window) so "the last minute" collapses to a handful of small structs regardless
of event volume. Concurrent writes are serialised with a mutex. See
[DESIGN.md](DESIGN.md) for the reasoning behind these and other choices.

## Status

Done: ingest with validation, windowed aggregation over event time, per-metric
stats with a configurable query window, percentiles from a bounded histogram,
an alerting layer with flap suppression, graceful shutdown, and a load harness
(benchmarks, concurrency invariants, and an HTTP load generator that verifies
the server recorded exactly what it accepted).

Known limits, all deliberate and argued in [DESIGN.md](DESIGN.md): state is
in-memory and resets on restart (§2); only the aggregates decided up front are
kept, so a statistic nobody planned for can't be back-filled (§1); percentiles
are accurate to 1% rather than exact, which is the price of not keeping events
(§23); and a single mutex guards the whole store, which the benchmarks show is
the throughput ceiling at ~7.7M events/sec — measured, and not worth fixing
until something actually hits it (§5).
