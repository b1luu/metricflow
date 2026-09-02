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

Listens on `:8080`.

## Endpoints

| Method | Path      | Description                                             |
| ------ | --------- | ------------------------------------------------------ |
| GET    | `/health` | Liveness check. Returns `ok`.                          |
| POST   | `/ingest` | Submit one metric event as a JSON body.                |
| GET    | `/stats`  | Per-metric aggregate over the last 60 seconds.         |

Event body:

```json
{ "name": "cpu.load", "value": 0.8, "type": "gauge", "ts": 1735000000123 }
```

## Example

```
curl.exe -X POST localhost:8080/ingest -d '{"name":"cpu.load","value":0.8,"type":"gauge","ts":1735000000123}'
curl.exe localhost:8080/stats
# cpu.load: count=1 avg=0.80 min=0.80 max=0.80
```

On Windows PowerShell, use `curl.exe` (not `curl`, which is an alias for
`Invoke-WebRequest`) or `Invoke-RestMethod`.

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

Work in progress. Done: ingest, windowed aggregation, per-metric stats.
Next: configurable query window, event-time handling, an alerting layer, and a
load-generation harness.
