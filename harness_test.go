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
	"encoding/json"
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

// --- exactness across shards (§24) ---

// Writing many metrics at once is the case sharding exists for: the workers
// land on different shards and stop queueing behind one lock. Correctness
// has to survive that. A per-shard lock is only sound because a metric's
// buckets never live outside the shard its name hashes to - if that ever
// stopped holding, a write and a merge would take different locks for the
// same data, and these counts would drift.
func TestConcurrentRecordAcrossShardsIsExact(t *testing.T) {
	const (
		metrics   = 200
		writers   = 8   // concurrent writers per metric
		perWriter = 200 // a multiple of 10, so each block of values sums to 55
	)

	names := make([]string, metrics)
	for i := range names {
		names[i] = fmt.Sprintf("svc.metric.%d", i)
	}

	// This test only says anything about sharding if the names actually
	// span shards. The hash is deterministic, so this cannot flake: it
	// either holds for these names or it never does.
	spread := map[int]bool{}
	for _, n := range names {
		spread[shardIndex(n)] = true
	}
	if len(spread) != shardCount {
		t.Fatalf("%d metric names covered %d of %d shards - the test would not "+
			"be exercising concurrent writes to different shards",
			metrics, len(spread), shardCount)
	}

	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	var wg sync.WaitGroup
	for _, name := range names {
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				for i := 0; i < perWriter; i++ {
					s.record(now, Event{Name: name, Value: cycleValue(i), TS: ts})
				}
			}(name)
		}
	}
	wg.Wait()

	wantCount := writers * perWriter
	wantSum := float64(writers * (perWriter / 10) * 55)

	for _, name := range names {
		got, ok := mergeAll(s, name)
		if !ok {
			t.Fatalf("%s: no data recorded", name)
		}
		if got.Count != wantCount {
			t.Errorf("%s: Count = %d, want %d", name, got.Count, wantCount)
		}
		if got.Sum != wantSum {
			t.Errorf("%s: Sum = %v, want %v", name, got.Sum, wantSum)
		}
		if got.Min != 1 || got.Max != 10 {
			t.Errorf("%s: Min/Max = %v/%v, want 1/10", name, got.Min, got.Max)
		}
	}

	// No metric invented, none lost, none filed under two shards.
	if got := metricCount(s); got != metrics {
		t.Errorf("store holds %d metrics, want %d", got, metrics)
	}
}

// The adversarial case for a per-shard lock: two *different* metrics that
// hash to the *same* shard, so they share one mutex and one map. Sharding
// buys nothing here by design - what matters is that it costs nothing
// either, and the two metrics' aggregates stay completely separate.
//
// The value ranges are disjoint, so any bleed between the two shows up in
// Min/Max, not only in a count that could drift for other reasons.
func TestConcurrentCollidingMetricsStaySeparate(t *testing.T) {
	const (
		writers   = 32
		perWriter = 500
	)

	a, b := collidingNames(t)

	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(2)
		// a gets 1..10, b gets 101..110.
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				s.record(now, Event{Name: a, Value: cycleValue(i), TS: ts})
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				s.record(now, Event{Name: b, Value: 100 + cycleValue(i), TS: ts})
			}
		}()
	}
	wg.Wait()

	wantCount := writers * perWriter
	base := float64(writers * (perWriter / 10) * 55)

	cases := []struct {
		name             string
		wantSum          float64
		wantMin, wantMax float64
	}{
		{a, base, 1, 10},
		{b, base + float64(wantCount*100), 101, 110},
	}
	for _, c := range cases {
		got, ok := mergeAll(s, c.name)
		if !ok {
			t.Fatalf("%s: no data recorded", c.name)
		}
		if got.Count != wantCount {
			t.Errorf("%s: Count = %d, want %d", c.name, got.Count, wantCount)
		}
		if got.Sum != c.wantSum {
			t.Errorf("%s: Sum = %v, want %v", c.name, got.Sum, c.wantSum)
		}
		if got.Min != c.wantMin || got.Max != c.wantMax {
			t.Errorf("%s: Min/Max = %v/%v, want %v/%v - the two metrics bled together",
				c.name, got.Min, got.Max, c.wantMin, c.wantMax)
		}
	}
}

// collidingNames finds two distinct metric names that hash to one shard.
// With shardCount shards a collision turns up within a few dozen tries; the
// search is bounded so a hash change fails the test rather than hanging.
func collidingNames(t *testing.T) (string, string) {
	t.Helper()
	seen := map[int]string{}
	for i := 0; i < 10000; i++ {
		n := fmt.Sprintf("collide.%d", i)
		idx := shardIndex(n)
		if prev, ok := seen[idx]; ok {
			return prev, n
		}
		seen[idx] = n
	}
	t.Fatalf("no two of 10000 names shared a shard, with only %d shards", shardCount)
	return "", ""
}

// /stats now walks the shards one at a time, so a response is no longer a
// single instant across all metrics: a write can land between shard 0 and
// shard 31. That is the trade §24 documents. What must never happen is a
// *metric* coming back torn - its Count, Sum, Min and Max come from one
// merge under one lock, so they always describe the same set of events.
//
// Every event for a metric carries that metric's own constant value, which
// makes the invariant exact and checkable from the response alone: its Avg
// is exactly that value, and so are its Min and Max. Avg is Sum/Count
// computed inside the merge, so a torn read - a Sum from before a write
// paired with the Count from after it - lands off the expected average.
func TestStatsAcrossShardsIsNeverTornPerMetric(t *testing.T) {
	const (
		metrics   = 64
		perMetric = 3000
		readers   = 4
	)

	names := make([]string, metrics)
	value := map[string]float64{}
	for i := range names {
		names[i] = fmt.Sprintf("torn.metric.%d", i)
		value[names[i]] = float64(i + 1) // constant per metric, and distinct
	}

	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	var (
		writers   sync.WaitGroup
		readersWG sync.WaitGroup
		done      atomic.Bool
		snapshots atomic.Int64
	)
	// Buffered and drained after every goroutine has joined: t.Errorf must
	// not be called from these goroutines, and a full channel must not
	// deadlock a writer either.
	failures := make(chan string, 64)

	for _, name := range names {
		writers.Add(1)
		go func(name string) {
			defer writers.Done()
			for i := 0; i < perMetric; i++ {
				s.record(now, Event{Name: name, Value: value[name], TS: ts})
			}
		}(name)
	}

	fail := func(msg string) {
		select {
		case failures <- msg:
		default: // already have plenty of evidence
		}
	}

	for r := 0; r < readers; r++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for !done.Load() {
				rec := httptest.NewRecorder()
				s.handleStats(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))

				var resp StatsResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					fail(fmt.Sprintf("invalid JSON: %v", err))
					return
				}
				snapshots.Add(1)

				for name, m := range resp.Metrics {
					v := value[name]
					if m.Avg != v {
						fail(fmt.Sprintf("%s: torn read - Count=%d Avg=%v, want %v",
							name, m.Count, m.Avg, v))
					}
					if m.Min != v || m.Max != v {
						fail(fmt.Sprintf("%s: Min/Max = %v/%v, want %v/%v",
							name, m.Min, m.Max, v, v))
					}
				}
			}
		}()
	}

	writers.Wait()
	done.Store(true)
	readersWG.Wait()
	close(failures)

	for msg := range failures {
		t.Error(msg)
	}
	if snapshots.Load() == 0 {
		t.Fatal("no snapshot completed; the readers never observed the store")
	}

	// And the final state is exact, so the readers cost nothing.
	for _, name := range names {
		got, ok := mergeAll(s, name)
		if !ok {
			t.Fatalf("%s: no data recorded", name)
		}
		if got.Count != perMetric {
			t.Errorf("%s: Count = %d, want %d", name, got.Count, perMetric)
		}
	}
}
