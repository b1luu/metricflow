# MetricFlow

A small real-time metrics ingestion and aggregation engine in Go — a mini
Datadog. Events are pushed in over HTTP; per-metric aggregates (count, average,
min, max) are kept live in memory over a rolling time window and served back on
demand.

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
| GET    | `/stats`  | Per-metric aggregate over a time window (JSON, default 60s). |

`/stats` accepts an optional `?window=` (Go duration, e.g. `?window=30s`),
capped at the 60-second retention window. `/ingest` requires `name` and `ts`
(unix milliseconds, no older than the 60s window); `value` and `type` default
to zero if omitted. Each route accepts only the method shown above — anything
else returns `405`.

Event body (`ts` is unix milliseconds and must be within the last 60s):

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
# 1m0s    @{cpu.load=@{count=1; avg=0.8; min=0.8; max=0.8}}
```

On non-Windows / with real curl:

```
curl -XPOST localhost:8080/ingest -d "{\"name\":\"cpu.load\",\"value\":0.8,\"ts\":$(date +%s)000}"
curl localhost:8080/stats
```

(PowerShell's `curl` is an alias for `Invoke-WebRequest`, which takes
different flags — use `curl.exe` or `Invoke-RestMethod` there.)

## Test

```
go test ./...
```

## Design

Aggregates are stored as fixed-width time buckets (10s each, 6 in a 60-second
window) so "the last minute" collapses to a handful of small structs regardless
of event volume. Concurrent writes are serialised with a mutex. See
[DESIGN.md](DESIGN.md) for the reasoning behind these and other choices.

## Status

Work in progress. Done: ingest, windowed aggregation, per-metric stats with a
configurable query window.
Next: event-time handling, an alerting layer, and a load-generation harness.
