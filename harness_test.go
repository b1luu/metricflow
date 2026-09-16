package main

// Load harness, part two: correctness under concurrency.
//
// The benchmarks in bench_test.go answer "how fast"; these answer "still
// right". The invariant throughout is exactness - every accepted event is
// counted once, and nothing else is counted at all - because "approximately
// the right count under load" is indistinguishable from a lost-update bug.
//
// A note on float64: addition is not associative, so a concurrent Sum over
// arbitrary values could differ in its low bits purely by interleaving.
// These tests use integer-valued floats whose running total stays far below
// 2**53, where every intermediate is exactly representable and any order
// gives the same answer. Count, Min and Max are order-independent regardless.

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Values cycle 1..10, so a block of ten sums to 55 and the extremes are known.
func cycleValue(i int) float64 { return float64(i%10 + 1) }

// Heavy concurrent writes to one metric must aggregate exactly: no lost
// increments, no double counts, no drifted extremes.
func TestConcurrentRecordIsExact(t *testing.T) {
	const (
		workers   = 64
		perWorker = 1000 // a multiple of 10, so each worker's values sum to 55 per block
	)

	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				s.record(now, Event{Name: "cpu.load", Value: cycleValue(i), TS: ts})
			}
		}()
	}
	wg.Wait()

	got, ok := mergeAll(s, "cpu.load")
	if !ok {
		t.Fatal("no data recorded")
	}

	wantCount := workers * perWorker
	wantSum := float64(workers * (perWorker / 10) * 55)

	if got.Count != wantCount {
		t.Errorf("Count = %d, want %d (lost or duplicated updates)", got.Count, wantCount)
	}
	if got.Sum != wantSum {
		t.Errorf("Sum = %v, want %v", got.Sum, wantSum)
	}
	if got.Min != 1 {
		t.Errorf("Min = %v, want 1", got.Min)
	}
	if got.Max != 10 {
		t.Errorf("Max = %v, want 10", got.Max)
	}
}

// Everything that reads the store in production - /stats, /alerts, and the
// alerter's own evaluation - running flat out against writers. Go panics on
// concurrent map access even without -race, so a dropped lock anywhere here
// fails the test loudly; the exact final count proves nothing was lost while
// the readers were hammering.
func TestConcurrentReadersAndWritersStayExact(t *testing.T) {
	const (
		writers   = 32
		perWriter = 500
		readers   = 4
		perReader = 300
	)

	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	a, err := newAlerter(now, s, []Rule{
		{Name: "hot", Metric: "cpu.load", Stat: StatAvg, Op: OpGT, Value: 1000}, // stays ok
	})
	if err != nil {
		t.Fatal(err)
	}

	// captureLog keeps the alerter's one nodata -> ok line out of the test
	// output; the goroutines are all joined inside, so the logger is
	// restored only after every writer to it has stopped.
	_ = captureLog(t, func() {
		var wg sync.WaitGroup

		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perWriter; i++ {
					s.record(now, Event{Name: "cpu.load", Value: cycleValue(i), TS: ts})
				}
			}()
		}

		for r := 0; r < readers; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				w := &discardWriter{}
				for i := 0; i < perReader; i++ {
					s.handleStats(w, httptest.NewRequest(http.MethodGet, "/stats", nil))
					a.handleAlerts(w, httptest.NewRequest(http.MethodGet, "/alerts", nil))
					a.evaluateAll(now)
				}
			}()
		}

		wg.Wait()
	})

	got, ok := mergeAll(s, "cpu.load")
	if !ok {
		t.Fatal("no data recorded")
	}
	if want := writers * perWriter; got.Count != want {
		t.Errorf("Count = %d, want %d", got.Count, want)
	}
	if want := float64(writers * (perWriter / 10) * 55); got.Sum != want {
		t.Errorf("Sum = %v, want %v", got.Sum, want)
	}
}

// badBody builds one rejectable /ingest payload. Every kind must produce a
// non-2xx and leave the store untouched. They deliberately reuse the same
// metric name as the valid traffic, so a body that leaked through would
// inflate that metric's count rather than hide in one of its own.
var badBody = []struct {
	kind string
	make func(metric string, now time.Time) string
}{
	{"malformed json", func(string, time.Time) string {
		return `{"name":`
	}},
	{"no name", func(_ string, now time.Time) string {
		return fmt.Sprintf(`{"value":1,"ts":%d}`, now.UnixMilli())
	}},
	{"no ts", func(m string, _ time.Time) string {
		return fmt.Sprintf(`{"name":%q,"value":1}`, m)
	}},
	{"ts too old", func(m string, now time.Time) string {
		return ingestBody(m, 1, now.Add(-5*time.Minute))
	}},
	{"ts too far future", func(m string, now time.Time) string {
		return ingestBody(m, 1, now.Add(5*time.Minute))
	}},
	{"body too large", func(m string, now time.Time) string {
		return fmt.Sprintf(`{"name":%q,"value":1,"ts":%d,"type":%q}`,
			m, now.UnixMilli(), strings.Repeat("x", maxIngestBody))
	}},
}

// The injected-failure case: a concurrent stream of valid and invalid
// requests through the real handler. The store's count must equal exactly
// the number of requests that were answered 200 - no accepted event lost,
// no rejected event counted.
func TestMixedStreamCountsOnlyAcceptedEvents(t *testing.T) {
	const (
		workers   = 8
		perWorker = 300 // divisible by 3: every third request is valid
		metric    = "cpu.load"
	)

	s := newStore()
	var accepted, acceptedSum atomic.Int64

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			bad := 0 // separate counter so every bad kind gets used
			for i := 0; i < perWorker; i++ {
				now := time.Now()

				body, wantOK := "", i%3 == 0
				if wantOK {
					body = ingestBody(metric, cycleValue(i), now)
				} else {
					body = badBody[bad%len(badBody)].make(metric, now)
					bad++
				}

				rec := httptest.NewRecorder()
				s.handleIngest(rec, httptest.NewRequest(http.MethodPost, "/ingest",
					strings.NewReader(body)))

				switch {
				case rec.Code == http.StatusOK && wantOK:
					accepted.Add(1)
					acceptedSum.Add(int64(cycleValue(i)))
				case rec.Code == http.StatusOK:
					// t.Errorf is goroutine-safe; t.Fatal would not be.
					t.Errorf("a rejectable body was accepted: %q", body)
				case wantOK:
					t.Errorf("valid body rejected with %d: %q", rec.Code, body)
				}
			}
		}()
	}
	wg.Wait()

	wantCount := int(accepted.Load())
	if wantCount != workers*perWorker/3 {
		t.Fatalf("accepted %d requests, want %d", wantCount, workers*perWorker/3)
	}
	// Sanity: the invalid two thirds really were sent and really were refused.
	if sent := workers * perWorker; wantCount == sent {
		t.Fatalf("every one of %d requests was accepted; the bad ones aren't bad", sent)
	}

	got, ok := mergeAll(s, metric)
	if !ok {
		t.Fatal("no data recorded")
	}
	if got.Count != wantCount {
		t.Errorf("stored Count = %d, want %d (one per 200 response)", got.Count, wantCount)
	}
	if want := float64(acceptedSum.Load()); got.Sum != want {
		t.Errorf("stored Sum = %v, want %v", got.Sum, want)
	}
}

// --- the distribution under concurrency (§23) ---

// The histogram is written on the ingest hot path, so it needs the same
// exactness guarantee as the summary numbers: every observation lands in a
// bucket, none lost to a race. The counts have to survive even though the
// bucket *values* are approximate.
func TestConcurrentHistogramIsExact(t *testing.T) {
	const (
		workers   = 64
		perWorker = 1000
	)

	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				s.record(now, Event{Name: "latency", Value: cycleValue(i), TS: ts})
			}
		}()
	}
	wg.Wait()

	got, ok := mergeAll(s, "latency")
	if !ok {
		t.Fatal("no data recorded")
	}

	want := int64(workers * perWorker)
	if got.h.count != want {
		t.Errorf("histogram count = %d, want %d (observations lost)", got.h.count, want)
	}
	// The histogram's own count must agree with the summary count, or one
	// of the two update paths dropped something the other didn't.
	if got.h.count != int64(got.Count) {
		t.Errorf("histogram count %d disagrees with Agg.Count %d", got.h.count, got.Count)
	}

	// Values cycle 1..10 evenly, so the median is 5 or 6 and the extremes
	// are exact regardless of interleaving.
	if q := got.h.quantile(0.5); q < 5*(1-relAccuracy) || q > 6*(1+relAccuracy) {
		t.Errorf("median = %g, want about 5-6", q)
	}
	if q := got.h.quantile(1); !within(q, 10) {
		t.Errorf("max quantile = %g, want about 10", q)
	}
}

// Percentiles must be readable while writes are in flight without tearing.
// Go panics on concurrent map access even without -race, so an unguarded
// bucket map would fail this loudly.
func TestConcurrentPercentileReadsStayConsistent(t *testing.T) {
	const (
		writers   = 32
		perWriter = 500
		readers   = 4
		perReader = 200
	)

	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				s.record(now, Event{Name: "latency", Value: cycleValue(i), TS: ts})
			}
		}()
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := &discardWriter{}
			for i := 0; i < perReader; i++ {
				s.handleStats(w, httptest.NewRequest(http.MethodGet, "/stats", nil))
			}
		}()
	}
	wg.Wait()

	got, ok := mergeAll(s, "latency")
	if !ok {
		t.Fatal("no data recorded")
	}
	if want := int64(writers * perWriter); got.h.count != want {
		t.Errorf("histogram count = %d, want %d", got.h.count, want)
	}
}

// A percentile read must never observe a partially-merged histogram: every
// quantile has to be monotonic in q, at any moment, under any interleaving.
func TestConcurrentQuantilesStayMonotonic(t *testing.T) {
	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			s.record(now, Event{Name: "latency", Value: cycleValue(i), TS: ts})
		}
	}()

	for i := 0; i < 500; i++ {
		m, ok := mergeAll(s, "latency")
		if !ok {
			continue
		}
		q := m.h.quantiles(0.5, 0.9, 0.99)
		if q[0] > q[1] || q[1] > q[2] {
			t.Fatalf("quantiles out of order mid-write: p50=%g p90=%g p99=%g", q[0], q[1], q[2])
		}
	}

	close(stop)
	wg.Wait()
}
