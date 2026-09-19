package main

// Load harness, part one: benchmarks.
//
// These exist to produce evidence, not just numbers. In particular the pair
// BenchmarkRecordParallelSameMetric / ...DistinctMetrics is designed to
// *demonstrate* the claim in DESIGN §5 - that one global mutex is the
// throughput ceiling - rather than assert it. Writes to different metrics
// touch disjoint data, so with a sharded lock the distinct-metric case would
// scale with cores; through one lock the two run at the same speed.
//
// Run:
//
//	go test -run=^$ -bench=. -benchmem
//	go test -run=^$ -bench=Record -cpuprofile=cpu.out

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// discardWriter is a minimal http.ResponseWriter that keeps the status code
// and throws the body away. httptest.NewRecorder buffers every response, so
// reusing one across a benchmark measures its buffer growth as much as the
// handler; this measures the handler.
type discardWriter struct {
	header http.Header
	code   int
}

func (d *discardWriter) Header() http.Header {
	if d.header == nil {
		d.header = http.Header{}
	}
	return d.header
}

func (d *discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (d *discardWriter) WriteHeader(code int)        { d.code = code }

// ingestBody builds a valid /ingest payload stamped at now.
func ingestBody(name string, value float64, now time.Time) string {
	return fmt.Sprintf(`{"name":%q,"value":%v,"ts":%d}`, name, value, now.UnixMilli())
}

// --- the aggregation core ---

// BenchmarkRecord is the floor: one goroutine, one metric, no HTTP. now is
// hoisted out of the loop so no clock read lands in the measurement (and so
// nothing is ever evicted - this measures the steady-state update path).
func BenchmarkRecord(b *testing.B) {
	s := newStore()
	now := time.Now()
	ev := Event{Name: "cpu.load", Value: 1, TS: now.UnixMilli()}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.record(now, ev)
	}
}

// Every goroutine writes the same metric, so they contend on both the store
// lock and the same *Agg. This is the worst case.
func BenchmarkRecordParallelSameMetric(b *testing.B) {
	s := newStore()
	now := time.Now()
	ev := Event{Name: "cpu.load", Value: 1, TS: now.UnixMilli()}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.record(now, ev)
		}
	})
}

// Every goroutine writes its *own* metric, so the data they touch is
// disjoint - the only thing shared is the single store mutex. Compare this
// against SameMetric: if the numbers are alike, the lock is the ceiling and
// not the per-metric contention. That is the measurement DESIGN §5 predicts,
// and the one that would justify sharding the lock by metric name.
func BenchmarkRecordParallelDistinctMetrics(b *testing.B) {
	s := newStore()
	now := time.Now()
	var next atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ev := Event{
			Name:  fmt.Sprintf("metric.%d", next.Add(1)),
			Value: 1,
			TS:    now.UnixMilli(),
		}
		for pb.Next() {
			s.record(now, ev)
		}
	})
}

// --- the HTTP path ---

// BenchmarkIngestHandler measures read + parse + validate + record.
//
// The body's ts is refreshed every 1024 iterations: the handler rejects a ts
// older than the retention window, so a single fixed timestamp would
// silently start measuring the reject path on a long -benchtime. Amortised
// over 1024 iterations the clock read and Sprintf are noise. The status
// check in the loop makes the failure loud rather than silent either way.
func BenchmarkIngestHandler(b *testing.B) {
	s := newStore()
	body := ingestBody("cpu.load", 1, time.Now())
	w := &discardWriter{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%1024 == 0 {
			b.StopTimer()
			body = ingestBody("cpu.load", 1, time.Now())
			b.StartTimer()
		}

		w.code = http.StatusOK
		req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
		s.handleIngest(w, req)

		if w.code != http.StatusOK {
			b.Fatalf("iteration %d: status = %d, want 200", i, w.code)
		}
	}
}

// --- the query path ---

// BenchmarkStats measures a full /stats response over a realistic store:
// every metric holds a full window of buckets, so the merge does real work.
//
// The data is recorded rather than seeded. Seeding Aggs directly leaves them
// with no histogram, and the percentile work - the merge of bucket maps and
// the quantile walk - would be skipped entirely, so the benchmark would
// quietly stop measuring most of what /stats now does.
func BenchmarkStats(b *testing.B) {
	const (
		metrics       = 100
		perTimeBucket = 50
	)

	s := newStore()
	now := time.Now()
	for m := 0; m < metrics; m++ {
		name := fmt.Sprintf("metric.%d", m)
		for i := 0; i < numBuckets; i++ {
			at := now.Add(-time.Duration(i) * bucketWidth)
			for j := 0; j < perTimeBucket; j++ {
				s.record(at, Event{
					Name:  name,
					Value: float64(j%40 + 1), // ~40 distinct histogram buckets
					TS:    at.UnixMilli(),
				})
			}
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	w := &discardWriter{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.handleStats(w, req)
	}
}

// --- alerting ---

// BenchmarkEvaluateAll shows what one alerter tick costs. It should scale
// with the rule count, not the event rate - that is the whole reason
// evaluation runs on a ticker instead of inside record (§18).
func BenchmarkEvaluateAll(b *testing.B) {
	const rules = 50

	s := newStore()
	rs := make([]Rule, 0, rules)
	for i := 0; i < rules; i++ {
		name := fmt.Sprintf("metric.%d", i)
		seedBucket(s, name, bucketAt(0), &Agg{Count: 10, Sum: 50, Min: 1, Max: 9})
		rs = append(rs, Rule{
			Name: name, Metric: name,
			Stat: StatAvg, Op: OpGT, Value: 1000, // well above the data: stays ok
		})
	}

	a, err := newAlerter(time.Now(), s, rs)
	if err != nil {
		b.Fatal(err)
	}
	now := time.Now()

	// Settle every rule into ok first. The very first evaluation is a
	// transition (nodata -> ok) and would put 50 log writes inside the
	// measurement; after this, the timed loop is pure steady state, which
	// is what an ordinary tick actually costs.
	old := log.Writer()
	log.SetOutput(io.Discard)
	a.evaluateAll(now)
	log.SetOutput(old)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.evaluateAll(now)
	}
}

// --- the distribution (§23) ---

// What recording a value into the histogram costs. This is the tax the
// percentiles slice put on every single ingest, so it should stay small and
// stay allocation-free once the buckets exist.
func BenchmarkHistAdd(b *testing.B) {
	var h hist
	h.add(1) // create the map outside the measurement

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.add(float64(i%1000 + 1))
	}
}

// One quantile: what an alert rule pays per evaluation.
func BenchmarkHistQuantile(b *testing.B) {
	var h hist
	for i := 1; i <= 100000; i++ {
		h.add(float64(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.quantile(0.99)
	}
}

// Three quantiles in one pass, which is what /stats does. Compare against
// three times BenchmarkHistQuantile: the gap is the bucket sort this avoids.
func BenchmarkHistQuantilesTogether(b *testing.B) {
	var h hist
	for i := 1; i <= 100000; i++ {
		h.add(float64(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.quantiles(0.50, 0.90, 0.99)
	}
}

// --- the batch path ---

// BenchmarkIngestBatch measures the whole HTTP batch path at several batch
// sizes and reports ns/event, so the sizes are directly comparable to each
// other and to BenchmarkIngestHandler - which is the same measurement at a
// batch size of one, through the single-event endpoint.
//
// The question is where per-request cost stops dominating. At size 1 a
// batch is strictly worse than /ingest: identical request overhead, plus a
// JSON response body that /ingest does not write. Every size after that is
// the amortisation, and the point at which the curve flattens is the point
// at which the store, not the network, is the thing being measured again.
//
// Events all share one metric name on purpose. That is the *worst* case for
// the sharded store - every event in the batch takes the same lock - so a
// win here is not an artifact of the events spreading across shards.
func BenchmarkIngestBatch(b *testing.B) {
	for _, size := range []int{1, 10, 100, 1000, maxBatchEvents} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			s := newStore()
			w := &discardWriter{}
			body := ndjson(validEvents("cpu.load", size)...)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Same trap as BenchmarkIngestHandler: the accepted-ts
				// range is only 60 s wide, so a body built once would
				// silently start measuring the reject path on a long run.
				if i%1024 == 0 {
					b.StopTimer()
					body = ndjson(validEvents("cpu.load", size)...)
					b.StartTimer()
				}

				w.code = http.StatusOK
				s.handleIngestBatch(w, httptest.NewRequest(
					http.MethodPost, "/ingest/batch", strings.NewReader(body)))

				if w.code != http.StatusOK {
					b.Fatalf("iteration %d: status = %d, want 200", i, w.code)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/event")
		})
	}
}

// The same batch sizes with every event on a *different* metric, so the
// events spread across shards instead of queueing on one. Compared against
// BenchmarkIngestBatch this separates the two things batching could be
// buying: amortised request overhead, which both cases get, and reduced
// lock contention, which only this case gets.
func BenchmarkIngestBatchDistinctMetrics(b *testing.B) {
	for _, size := range []int{100, 1000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			s := newStore()
			w := &discardWriter{}

			build := func() string {
				evs := validEvents("cpu.load", size)
				for i := range evs {
					evs[i].Name = fmt.Sprintf("svc.metric.%d", i)
				}
				return ndjson(evs...)
			}
			body := build()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%1024 == 0 {
					b.StopTimer()
					body = build()
					b.StartTimer()
				}

				w.code = http.StatusOK
				s.handleIngestBatch(w, httptest.NewRequest(
					http.MethodPost, "/ingest/batch", strings.NewReader(body)))

				if w.code != http.StatusOK {
					b.Fatalf("iteration %d: status = %d, want 200", i, w.code)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/event")
		})
	}
}

// BenchmarkBatchDecodeOnly is the same stream without the store: decode and
// validate every event, record none. It splits a batch's per-event cost into
// the part JSON parsing owns and the part the store owns, which is what says
// whether optimising the locking below it could pay for itself at all.
func BenchmarkBatchDecodeOnly(b *testing.B) {
	const size = 1000

	body := ndjson(validEvents("cpu.load", size)...)
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dec := json.NewDecoder(strings.NewReader(body))
		for {
			var ev Event
			if err := dec.Decode(&ev); err != nil {
				break
			}
			if err := validateEvent(now, ev); err != nil {
				b.Fatalf("iteration %d: %v", i, err)
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/event")
}

// --- cardinality and sweeping ---

// The cap is checked only when a name is new, so an event for a metric that
// already exists should not pay for it at all. BenchmarkRecord is the
// comparison - this is the same measurement on the path that does create.
func BenchmarkRecordNewMetric(b *testing.B) {
	now := time.Now()
	ts := now.UnixMilli()

	// Names are built up front so the benchmark measures record, not
	// Sprintf. The store is rebuilt whenever the cap is reached, since past
	// that point record would be measuring the refusal path instead.
	names := make([]string, 4096)
	for i := range names {
		names[i] = fmt.Sprintf("new.metric.%d", i)
	}

	s := newStore()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%len(names) == 0 {
			b.StopTimer()
			s = newStore() // fresh budget
			b.StartTimer()
		}
		if err := s.record(now, Event{Name: names[i%len(names)], Value: 1, TS: ts}); err != nil {
			b.Fatalf("iteration %d: %v", i, err)
		}
	}
}

// Refusing has to be cheap too. A client flooding new names is exactly the
// case where the server is doing the most work per event and getting the
// least for it, so the refusal is the hot path under attack.
func BenchmarkRecordRefusedByCap(b *testing.B) {
	now := time.Now()
	ts := now.UnixMilli()

	// Fill one shard, then aim everything at it.
	s := newStore()
	full := make([]string, 0, maxMetricsPerShard)
	var overflow string
	for i := 0; len(full) <= maxMetricsPerShard; i++ {
		name := fmt.Sprintf("card.%d", i)
		if shardIndex(name) != 0 {
			continue
		}
		if len(full) == maxMetricsPerShard {
			overflow = name
			break
		}
		full = append(full, name)
		if err := s.record(now, Event{Name: name, Value: 1, TS: ts}); err != nil {
			b.Fatal(err)
		}
	}

	ev := Event{Name: overflow, Value: 1, TS: ts}
	if err := s.record(now, ev); err == nil {
		b.Fatal("the shard is not full; this would measure the accept path")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.record(now, ev); err == nil {
			b.Fatalf("iteration %d was accepted", i)
		}
	}
}

// A sweep that reclaims nothing is the common case - it runs every
// bucketWidth whether or not anything has gone idle, so its cost is a
// standing tax on a healthy server.
func BenchmarkSweepNothingToReclaim(b *testing.B) {
	const metrics = 2000

	s := newStore()
	now := time.Now()
	for i := 0; i < metrics; i++ {
		ev := Event{Name: fmt.Sprintf("live.metric.%d", i), Value: 1, TS: now.UnixMilli()}
		if err := s.record(now, ev); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got, _ := s.sweep(now); got != 0 {
			b.Fatalf("iteration %d reclaimed %d metrics from a live store", i, got)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*metrics), "ns/metric")
}

// And the case it exists for: a store full of names written once and
// abandoned. The store is rebuilt each iteration, outside the timer, because
// a sweep empties it.
func BenchmarkSweepReclaimingEverything(b *testing.B) {
	const metrics = 2000

	now := time.Now()
	old := now.Add(-window - time.Minute)
	names := make([]string, metrics)
	for i := range names {
		names[i] = fmt.Sprintf("idle.metric.%d", i)
	}

	fill := func() *Store {
		s := newStore()
		for _, n := range names {
			if err := s.record(old, Event{Name: n, Value: 1, TS: old.UnixMilli()}); err != nil {
				b.Fatal(err)
			}
		}
		return s
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := fill()
		b.StartTimer()

		if got, _ := s.sweep(now); got != metrics {
			b.Fatalf("iteration %d reclaimed %d of %d metrics", i, got, metrics)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*metrics), "ns/metric")
}
