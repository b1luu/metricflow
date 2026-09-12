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
func BenchmarkStats(b *testing.B) {
	const metrics = 100

	s := newStore()
	for m := 0; m < metrics; m++ {
		name := fmt.Sprintf("metric.%d", m)
		for i := 0; i < numBuckets; i++ {
			seedBucket(s, name, bucketAt(time.Duration(i)*bucketWidth),
				&Agg{Count: 10, Sum: 50, Min: 1, Max: 9})
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
