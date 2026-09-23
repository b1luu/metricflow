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
| GET    | `/health` | Liveness check. Returns `ok`. Never shed, never needs a client ID. |
| POST   | `/ingest` | Submit one metric event as a JSON body.                |
| POST   | `/ingest/batch` | Submit many events as newline-delimited JSON.    |
| GET    | `/stats`  | Per-metric count/avg/min/max and p50/p90/p99 over a time window (JSON, default 60s). Bounded — see below. |
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

## Batch ingest

`POST /ingest/batch` takes newline-delimited JSON — one event object per line,
no enclosing array — and replies with what it did:

```json
{"accepted": 98, "rejected": 2, "errors": [{"index": 17, "error": "ts is required (unix milliseconds)"}]}
```

A bad event costs exactly itself. A firehose that discarded 98 good samples
because two were malformed would be worse than useless, so rejection is per
event — reported, never silent, with each reject's index in the batch you
sent. The error list is capped; the count is not.

`accepted` and `rejected` are accurate on **every** status this endpoint
returns, not just `200`. If a batch is cut short — by the 1 MiB body cap, the
10 000 event cap, or malformed JSON that the parser cannot resync past — the
events before the break stay applied and the reply says how far it got in a
`fatal` field. That is what lets a client know exactly what to resend, and it
keeps the invariant the rest of the project holds: the events the server says
it accepted are exactly the events it recorded.

Batching is worth about 50x end to end, and the curve flattens around 100
events per request — past that you are buying very little and paying for it in
latency and in how much one dropped connection costs. See [DESIGN.md](DESIGN.md)
§25 for the measurements and for what the batch path deliberately does *not*
optimise.

## Clients and fairness

A caller identifies itself with `X-Client-ID`. The server then budgets it
separately: at most 32 concurrent requests, and at most 64 *new* metric names
per 10 s. Both are shares of the global limits rather than replacements for
them — a client over its own share gets `429` while the server is still well
short of the `503` it returns when genuinely saturated.

This exists so one broken service cannot cost every other service its
monitoring. With a noisy client flooding metric names and a quiet client
starting mid-flood:

| | requests | events accepted |
| --- | --- | --- |
| noisy | 99.9% `429` | 1,920 |
| quiet | 100% `200` | 2,290,350, `verify: OK` |

The store went from empty to 132 metrics against a 32,768 ceiling. The same
flood previously filled that ceiling in seconds, after which the quiet
client's new names would all have been refused.

**It is not authentication.** The header is self-asserted, the same contract
Cortex and Mimir give `X-Scope-OrgID`: it isolates clients that are honest
about who they are, which is every client that is merely broken. An attacker
rotating the header falls back to the global limits, which still hold. A
deployment facing untrusted callers should set the header at a trusted proxy
and strip whatever the caller sent. See [DESIGN.md](DESIGN.md) §29.

## Querying

`/stats` is bounded. It computes at most 1000 metrics per request, whatever
the store holds, and says so:

```json
{"window":"1m0s","metrics":{...},"matched":32768,"truncated":true,"next":"svc.api.z"}
```

`?prefix=` narrows by name, `?limit=` narrows how many metrics are computed
(it may only narrow — asking for more is a `400`, not a silent clamp), and
`?after=` continues from a previous page's `next`.

This exists because it was the last unbounded thing in the server. With the
store at its cardinality ceiling, one ~30-byte `GET /stats` used to cost
77 ms of CPU and 62 MB of allocation — and the concurrency limiter will admit
256 at once. It is now 3.7 ms and 1.2 MB, and the allocation count no longer
depends on how many metrics exist at all. A narrow `?prefix=` query is 364 µs.

Two details worth knowing as a client: `matched` counts metric *names*, so a
page can hold fewer metrics than its limit without being the last page (a
metric whose data has aged out is skipped); and `next` is the last name
*considered*, so following it never stalls on such a gap.

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
159639 requests in 3s  (53206 req/s)
  200    119726   75.0%
  400     33262   20.8%
  413      6651    4.2%
latency  p50 <529µs  p90 <529µs  p99 1.026ms  max 8.709ms
         (clock resolution 529µs - faster than that is unresolvable)
verify: OK - 119726 accepted, 119726 recorded
```

`-bad` mixes in deliberately invalid events. After the run, loadgen fetches
`/stats` and checks the server recorded exactly as many events as it said it
accepted, exiting non-zero if not — so it works as a CI gate, not just a demo.

`-batch N` sends N events per request instead of one, which is where the
throughput actually is:

```
go run ./cmd/loadgen -duration 3s -workers 8 -batch 100 -bad 0.25
```
```
40417 requests in 3.001s  (13469 req/s)
4041700 events  (1346945 events/s)  3031275 accepted, 1010425 rejected
  200     40417  100.0%
latency  p50 <550µs  p90 1.052ms  p99 1.393ms  max 11.621ms
verify: OK - 3031275 accepted, 3031275 recorded
```

Every request there is a `200` carrying both outcomes, so the accepted count
comes from the reply body rather than from the status code — and 3 031 275
accepted events were 3 031 275 recorded events.

## Overload

The server caps how many requests are in a handler at once and refuses the
excess with `503` and `Retry-After` — immediately, rather than queueing them.
Under sustained overload a queue does not reduce the work, it hides it:
latency grows without bound and the server spends its capacity on answers
nobody is waiting for any more. Refusing costs 179 ns against 1862 ns to
serve, so an overloaded server spends almost nothing on what it turns away.

A shed request never reaches the store, so the counts stay exact while it is
happening. `/health` is never shed — a health check that fails under load
makes a busy server look like a dead one, and whatever is watching responds by
killing it.

`METRICFLOW_MAX_INFLIGHT` sets the limit (default 256); a value that isn't a
positive integer stops the server rather than being ignored. To see it work:

```
METRICFLOW_MAX_INFLIGHT=4 ./metricflow
go run ./cmd/loadgen -duration 3s -workers 64 -batch 20 -expect-shed
```
```
282136 requests in 3.001s  (94002 req/s)
5642720 events  (1880035 events/s)  2154860 accepted, 0 rejected
shed  3487860 events in 174393 requests refused for capacity (503)
  200    107743   38.2%
  503    174393   61.8%
verify: OK - 2154860 accepted, 2154860 recorded
```

`-expect-shed` fails the run if the server never shed, so the check cannot
pass against a server with no limiter at all. Every request got an answer —
38% served, 62% refused in 179 ns — and the accepted count still matched the
store exactly. See [DESIGN.md](DESIGN.md) §26 for the timeouts, the
shed-don't-queue argument, and what this deliberately does not do.

## Cardinality

A metric name that carries a request ID or a UUID is how real metrics systems
die, so the store bounds both halves of the problem: at most 1024 distinct
names per shard (32 shards, so ~32k in total), and at most 256 bytes per name.
Capping only the count would bound nothing — a 1 MiB batch body means names
could be megabytes each.

Past the limit a **new** name is refused with `429` and `Retry-After`; in a
batch it is a per-event rejection marked `"retryable": true`, to distinguish
it from a contract violation that will never succeed. Metrics that already
exist keep taking writes no matter how full their shard is — a client
inventing names cannot degrade the metrics that were already there.

A sweeper runs every 10s and removes metrics whose buckets have all aged out,
so the limit is a high-water mark rather than a one-way door. Refusing costs
25 ns and zero allocations, four times cheaper than accepting a normal event.

```
go run ./cmd/loadgen -duration 4s -workers 8 -batch 50 -cardinality 20000 -expect-cardinality
```
```
6627050 events  (1656710 events/s)  2748157 accepted, 40043 rejected
cardinality  3878893 valid events refused because the store was full
  200     55764   42.1%
  429     76777   57.9%
verify: OK - 2748157 accepted, 2748157 recorded
```

The store filled to its cap, refused 3.8M events, kept serving — and every
event it said it accepted was recorded. See [DESIGN.md](DESIGN.md) §27 for why
the cap is per shard rather than global, and what it deliberately does not do.

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
of event volume. Writes are serialised per metric: the store is split into 32
independently-locked shards, each padded to its own cache line, and a metric
lives in the shard its name hashes to — so two metrics are only ever in each
other's way if their names collide. See [DESIGN.md](DESIGN.md) for the
reasoning behind these and other choices.

## Status

Done: single-event and batch ingest with validation, windowed aggregation over event time, per-metric
stats with a configurable query window, percentiles from a bounded histogram,
an alerting layer with flap suppression, graceful shutdown, a fully
timed-out HTTP server that sheds load rather than queueing it, panic
recovery on both the request path and the background loops, a bounded and
pageable query path, per-client concurrency and name-rate budgets so one
noisy client cannot starve the rest, a bounded
metric cardinality with a sweeper to reclaim idle names, a store whose lock
is sharded by metric name (14.9x on concurrent writes to distinct metrics,
measured before and after), a batch endpoint that turns that into 50x end to
end (§25), and a load harness
(benchmarks, concurrency invariants, and an HTTP load generator that verifies
the server recorded exactly what it accepted).

Known limits, all deliberate and argued in [DESIGN.md](DESIGN.md): state is
in-memory and resets on restart (§2); only the aggregates decided up front are
kept, so a statistic nobody planned for can't be back-filled (§1); percentiles
are accurate to 1% rather than exact, which is the price of not keeping events
(§23); `/stats` reads one shard at a time, so a response is not a single
instant across all metrics, though no individual metric is ever internally
torn (§24); and many goroutines writing the *same* metric still serialize,
because they contend for the same aggregate rather than for the locking
scheme — 159 ns/op against 10 ns/op for writes to distinct metrics (§24); and
client identity is self-asserted, so the per-client budgets isolate accidents
rather than attackers, and a caller rotating the header falls back to the
global limits (§29); query cost is bounded per request but per client only
through concurrency, so a client within its share can still issue expensive
queries back to back (§29); and listing
metrics is still O(cardinality) even when the answer is bounded, because
"which metrics exist" has no index behind it (§28).
