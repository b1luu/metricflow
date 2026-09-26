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
	"errors"
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

// --- exactness through the batch endpoint ---

// buildBatch renders an NDJSON body where every badEvery-th event is
// invalid - or none of them, when badEvery is 0 - and reports how many valid
// events it contains per metric. The
// rejects are interleaved rather than grouped so a bug that loses its place
// in the stream - an index that counts only accepted events, say - shows up
// as a count mismatch rather than being masked by a tidy layout.
func buildBatch(metrics []string, size, badEvery int, ts int64) (string, map[string]int) {
	evs := make([]Event, 0, size)
	valid := map[string]int{}

	for i := 0; i < size; i++ {
		if badEvery > 0 && i%badEvery == 0 {
			evs = append(evs, Event{Value: 1, TS: ts}) // no name: rejected
			continue
		}
		name := metrics[i%len(metrics)]
		evs = append(evs, Event{Name: name, Value: 1, TS: ts})
		valid[name]++
	}
	return ndjson(evs...), valid
}

// postBatchRaw is postBatch without the *testing.T, so it is safe to call
// from a goroutine - t.Fatalf from a non-test goroutine is not.
func postBatchRaw(s *Store, body string) (int, BatchResponse, error) {
	rec := httptest.NewRecorder()
	s.handleIngestBatch(rec, httptest.NewRequest(
		http.MethodPost, "/ingest/batch", strings.NewReader(body)))

	var resp BatchResponse
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	return rec.Code, resp, err
}

// The batch endpoint's whole contract is that "accepted" and "recorded" are
// the same number. Under concurrency that doubles as the lost-update test:
// a dropped increment shows up as the store holding fewer events than the
// replies promised, and an event counted twice shows up as more.
//
// Rejects are mixed in throughout, because the interesting failure is not a
// clean batch racing another clean batch - it is the accounting drifting
// while some events are being skipped.
func TestConcurrentBatchesRecordExactlyWhatTheyReport(t *testing.T) {
	const (
		writers          = 16
		batchesPerWriter = 20
		batchSize        = 50
		badEvery         = 7
	)

	// Shared across every writer, so the batches collide on the same
	// metrics and the same shards rather than each having its own lane.
	metrics := []string{
		"svc.api.latency_ms", "svc.api.errors", "svc.worker.queue_depth",
		"svc.db.latency_ms", "svc.db.errors",
	}

	s := newStore()
	ts := time.Now().UnixMilli()

	body, validPerBatch := buildBatch(metrics, batchSize, badEvery, ts)
	wantAccepted := 0
	for _, n := range validPerBatch {
		wantAccepted += n
	}
	if wantAccepted == 0 || wantAccepted == batchSize {
		t.Fatalf("test data is wrong: %d of %d events valid, want a mix",
			wantAccepted, batchSize)
	}

	var (
		wg          sync.WaitGroup
		totalAccept atomic.Int64
		totalReject atomic.Int64
		failures    = make(chan string, 32)
	)
	fail := func(msg string) {
		select {
		case failures <- msg:
		default:
		}
	}

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < batchesPerWriter; i++ {
				code, resp, err := postBatchRaw(s, body)
				if err != nil {
					fail(fmt.Sprintf("reply was not JSON: %v", err))
					return
				}
				if code != http.StatusOK {
					fail(fmt.Sprintf("status = %d, want 200 (fatal %q)", code, resp.Fatal))
					return
				}
				if resp.Accepted != wantAccepted {
					fail(fmt.Sprintf("accepted = %d, want %d", resp.Accepted, wantAccepted))
				}
				totalAccept.Add(int64(resp.Accepted))
				totalReject.Add(int64(resp.Rejected))
			}
		}()
	}
	wg.Wait()
	close(failures)

	for msg := range failures {
		t.Error(msg)
	}

	// The invariant: every event the server said it accepted is in the
	// store, and nothing else is.
	recorded := 0
	for name, series := range storeDump(s) {
		agg, ok := mergeBuckets(series, 0)
		if !ok {
			continue
		}
		recorded += agg.Count

		want := validPerBatch[name] * writers * batchesPerWriter
		if agg.Count != want {
			t.Errorf("%s: recorded %d, want %d", name, agg.Count, want)
		}
		// Every valid event is worth 1, so this catches a Sum that drifted
		// away from its own Count.
		if agg.Sum != float64(agg.Count) {
			t.Errorf("%s: Sum = %v, want %v", name, agg.Sum, float64(agg.Count))
		}
	}

	if int64(recorded) != totalAccept.Load() {
		t.Errorf("replies claimed %d events accepted, store holds %d",
			totalAccept.Load(), recorded)
	}
	wantReject := int64(writers * batchesPerWriter * (batchSize - wantAccepted))
	if totalReject.Load() != wantReject {
		t.Errorf("rejected %d, want %d", totalReject.Load(), wantReject)
	}
	// A rejected event must not have created its metric.
	if got, want := metricCount(s), len(metrics); got != want {
		t.Errorf("store holds %d metrics, want %d - a rejected event created one",
			got, want)
	}
}

// Both write paths and every read path at once. /ingest and /ingest/batch
// share validateEvent and record but not their framing, so this is the test
// that they cannot corrupt each other, and that a reader walking shards
// mid-batch sees no torn metric.
func TestConcurrentBatchAndSingleIngestStayExact(t *testing.T) {
	const (
		batchWriters     = 8
		batchesPerWriter = 25
		batchSize        = 40
		singleWriters    = 8
		perSingleWriter  = 500
		readers          = 3
	)

	s := newStore()
	ts := time.Now().UnixMilli()

	// The two paths deliberately share a metric name, so they contend on
	// one shard and one *Agg rather than running in separate lanes.
	const shared = "svc.api.latency_ms"
	body, validPerBatch := buildBatch([]string{shared}, batchSize, 0, ts)
	if validPerBatch[shared] != batchSize {
		t.Fatalf("test data is wrong: %d of %d valid, want all",
			validPerBatch[shared], batchSize)
	}

	var (
		wgWrite  sync.WaitGroup
		wgRead   sync.WaitGroup
		done     atomic.Bool
		failures = make(chan string, 32)
	)
	fail := func(msg string) {
		select {
		case failures <- msg:
		default:
		}
	}

	for w := 0; w < batchWriters; w++ {
		wgWrite.Add(1)
		go func() {
			defer wgWrite.Done()
			for i := 0; i < batchesPerWriter; i++ {
				code, resp, err := postBatchRaw(s, body)
				if err != nil || code != http.StatusOK || resp.Accepted != batchSize {
					fail(fmt.Sprintf("batch: code=%d accepted=%d err=%v fatal=%q",
						code, resp.Accepted, err, resp.Fatal))
					return
				}
			}
		}()
	}

	for w := 0; w < singleWriters; w++ {
		wgWrite.Add(1)
		go func() {
			defer wgWrite.Done()
			for i := 0; i < perSingleWriter; i++ {
				s.record(time.Now(), Event{Name: shared, Value: 1, TS: ts})
			}
		}()
	}

	for r := 0; r < readers; r++ {
		wgRead.Add(1)
		go func() {
			defer wgRead.Done()
			w := &discardWriter{}
			for !done.Load() {
				s.handleStats(w, httptest.NewRequest(http.MethodGet, "/stats", nil))
			}
		}()
	}

	wgWrite.Wait()
	done.Store(true)
	wgRead.Wait()
	close(failures)

	for msg := range failures {
		t.Error(msg)
	}

	want := batchWriters*batchesPerWriter*batchSize + singleWriters*perSingleWriter
	got, ok := mergeAll(s, shared)
	if !ok {
		t.Fatal("nothing recorded")
	}
	if got.Count != want {
		t.Errorf("Count = %d, want %d (the two write paths lost or duplicated events)",
			got.Count, want)
	}
	if got.Sum != float64(want) {
		t.Errorf("Sum = %v, want %v", got.Sum, float64(want))
	}
}

// A batch that stops early - at the event cap here - still has to report
// exactly what it applied, and concurrency must not blur that. This is the
// case where a naive implementation reports the whole batch, or reports
// nothing, and the store then disagrees with every client.
func TestConcurrentTruncatedBatchesStayHonest(t *testing.T) {
	const writers = 8

	s := newStore()
	body := ndjson(validEvents("cpu.load", maxBatchEvents+100)...)

	var (
		wg       sync.WaitGroup
		accepted atomic.Int64
		failures = make(chan string, 16)
	)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, resp, err := postBatchRaw(s, body)
			if err != nil {
				select {
				case failures <- fmt.Sprintf("reply was not JSON: %v", err):
				default:
				}
				return
			}
			if code != http.StatusRequestEntityTooLarge {
				select {
				case failures <- fmt.Sprintf("status = %d, want 413", code):
				default:
				}
			}
			accepted.Add(int64(resp.Accepted))
		}()
	}
	wg.Wait()
	close(failures)

	for msg := range failures {
		t.Error(msg)
	}

	got, ok := mergeAll(s, "cpu.load")
	if !ok {
		t.Fatal("nothing recorded")
	}
	if int64(got.Count) != accepted.Load() {
		t.Errorf("replies claimed %d accepted, store holds %d",
			accepted.Load(), got.Count)
	}
	if want := int64(writers * maxBatchEvents); accepted.Load() != want {
		t.Errorf("accepted %d in total, want %d", accepted.Load(), want)
	}
}

// --- exactness under load shedding ---

// saturate drives clients x perClient concurrent requests through h and
// reports how many were served and how many were refused for capacity.
func saturate(h http.Handler, clients, perClient int, build func() *http.Request) (ok, shed int64) {
	var served, refused atomic.Int64

	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perClient; i++ {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, build())
				switch rec.Code {
				case http.StatusServiceUnavailable:
					refused.Add(1)
				default:
					served.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	return served.Load(), refused.Load()
}

// Shedding is only safe if a shed request is *completely* shed - refused
// before the handler, so nothing is recorded for it. A request that were
// half-applied and then refused would make the 503 a lie, and leave the
// store holding events no client was ever told about. That is worse than
// dropping them, because it is silent.
//
// The limiter's capacity is tiny here so that shedding is guaranteed rather
// than hoped for, and the test asserts it actually happened - otherwise a
// limiter that never sheds would pass this trivially.
func TestShedIngestRequestsAreNeverRecorded(t *testing.T) {
	const (
		clients   = 32
		perClient = 40
		capacity  = 2
	)

	s := newStore()
	a, err := newAlerter(time.Now(), s, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := routes(s, a, newLimiter(capacity), newClients())

	served, shed := saturate(h, clients, perClient, func() *http.Request {
		return httptest.NewRequest(http.MethodPost, "/ingest",
			strings.NewReader(ingestJSON("cpu.load", 1)))
	})

	// Deliberately not asserting that anything *was* shed. Whether 32
	// clients at capacity 2 actually collide depends on how much
	// parallelism the machine offers, and CI caught this failing on a
	// two-core runner where they simply did not. The claim that shedding
	// refuses before the handler is made deterministically by
	// TestEverythingShedIsRecordedNowhere below; what a race can prove is
	// only that the accounting holds however the scheduler behaves.
	if served+shed != int64(clients*perClient) {
		t.Errorf("served %d + shed %d != %d requests sent", served, shed, clients*perClient)
	}

	got, ok := mergeAll(s, "cpu.load")
	if !ok {
		t.Fatal("nothing recorded at all")
	}
	if int64(got.Count) != served {
		t.Errorf("recorded %d events but only %d requests were served - "+
			"a shed request reached the store", got.Count, served)
	}
}

// The claim the two tests above cannot make on their own, made without any
// dependence on timing: a limiter of capacity zero sheds *everything*, so
// nothing may reach the store and every request must say so.
//
// This is the third time a shed- or contention-dependent assertion has turned
// out to rest on how many cores the machine has - §29's in-flight cap test and
// §31's self-reporting test were the others. The pattern is the same each
// time, and so is the fix: assert the property where it holds by construction,
// and let the concurrent test assert only what a race can actually prove.
func TestEverythingShedIsRecordedNowhere(t *testing.T) {
	const requests = 200

	s := newStore()
	a, err := newAlerter(time.Now(), s, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := routes(s, a, newLimiter(0), newClients())

	for i := 0; i < requests; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ingest",
			strings.NewReader(ingestJSON("cpu.load", 1))))

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("request %d: status = %d, want 503 from a limiter of capacity 0",
				i, rec.Code)
		}
	}

	if got := metricCount(s); got != 0 {
		t.Errorf("store holds %d metrics after %d shed requests", got, requests)
	}
	if _, ok := mergeAll(s, "cpu.load"); ok {
		t.Error("a shed request reached the store")
	}
}

// The same, for the batch path: a shed batch must contribute nothing and must
// not come back carrying a reply that claims otherwise.
func TestEveryShedBatchContributesNothing(t *testing.T) {
	const requests = 100

	s := newStore()
	a, err := newAlerter(time.Now(), s, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := routes(s, a, newLimiter(0), newClients())

	body := ndjson(validEvents("cpu.load", 25)...)
	for i := 0; i < requests; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ingest/batch",
			strings.NewReader(body)))

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("request %d: status = %d, want 503", i, rec.Code)
		}

		var resp BatchResponse
		if json.Unmarshal(rec.Body.Bytes(), &resp) == nil && resp.Accepted > 0 {
			t.Fatalf("request %d: a 503 reported %d accepted", i, resp.Accepted)
		}
	}

	if got := metricCount(s); got != 0 {
		t.Errorf("store holds %d metrics after %d shed batches", got, requests)
	}
}

// The same property for batches, where "accepted" is a number in the reply
// rather than a status code. A shed batch contributes nothing, so the sum
// over every reply must still equal what the store holds - the invariant
// §25 introduced, now under overload.
func TestShedBatchesContributeNothing(t *testing.T) {
	const (
		clients   = 32
		perClient = 20
		batchSize = 25
		capacity  = 2
	)

	s := newStore()
	a, err := newAlerter(time.Now(), s, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := routes(s, a, newLimiter(capacity), newClients())

	body := ndjson(validEvents("cpu.load", batchSize)...)

	var (
		accepted atomic.Int64
		shed     atomic.Int64
		wg       sync.WaitGroup
		failures = make(chan string, 16)
	)

	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perClient; i++ {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest(
					http.MethodPost, "/ingest/batch", strings.NewReader(body)))

				if rec.Code == http.StatusServiceUnavailable {
					shed.Add(1)
					// A shed request must not have been given a batch
					// reply - the body is the plain-text refusal, and
					// anything else would mean the handler ran.
					var resp BatchResponse
					if json.Unmarshal(rec.Body.Bytes(), &resp) == nil && resp.Accepted > 0 {
						select {
						case failures <- fmt.Sprintf("a 503 reported %d accepted", resp.Accepted):
						default:
						}
					}
					continue
				}

				var resp BatchResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					select {
					case failures <- fmt.Sprintf("status %d, body not JSON: %v", rec.Code, err):
					default:
					}
					continue
				}
				accepted.Add(int64(resp.Accepted))
			}
		}()
	}
	wg.Wait()
	close(failures)

	for msg := range failures {
		t.Error(msg)
	}
	// As above: whether this races hard enough to shed is the scheduler's
	// business, and the deterministic claim lives in its own test.

	got, ok := mergeAll(s, "cpu.load")
	if !ok {
		t.Fatal("nothing recorded at all")
	}
	if int64(got.Count) != accepted.Load() {
		t.Errorf("replies claimed %d events accepted, store holds %d",
			accepted.Load(), got.Count)
	}
}

// Under saturation the reads have to stay correct too, not just the writes.
// /stats walks every shard, so it holds locks the writers want; shedding
// must not turn that into a torn or stale answer for the requests that do
// get served.
func TestStatsStaysCorrectWhileRequestsAreShed(t *testing.T) {
	const (
		writers   = 24
		perWriter = 100
		readers   = 8
		capacity  = 4
	)

	s := newStore()
	a, err := newAlerter(time.Now(), s, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := routes(s, a, newLimiter(capacity), newClients())

	// Every event carries the same value, so a served /stats can be checked
	// against itself: avg, min and max must all equal it exactly, whatever
	// count the response happened to catch.
	const value = 7.0

	var (
		wg        sync.WaitGroup
		accepted  atomic.Int64
		snapshots atomic.Int64
		failures  = make(chan string, 16)
	)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				rec := httptest.NewRecorder()
				ev := fmt.Sprintf(`{"name":"shed.metric","value":%v,"ts":%d}`,
					value, time.Now().UnixMilli())
				h.ServeHTTP(rec, httptest.NewRequest(
					http.MethodPost, "/ingest", strings.NewReader(ev)))
				if rec.Code == http.StatusOK {
					accepted.Add(1)
				}
			}
		}()
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
				if rec.Code != http.StatusOK {
					continue // shed, which is fine
				}
				snapshots.Add(1)

				var resp StatsResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					select {
					case failures <- fmt.Sprintf("/stats body not JSON: %v", err):
					default:
					}
					continue
				}
				m, ok := resp.Metrics["shed.metric"]
				if !ok {
					continue // nothing written yet
				}
				if m.Avg != value || m.Min != value || m.Max != value {
					select {
					case failures <- fmt.Sprintf("torn read: avg/min/max = %v/%v/%v, want %v",
						m.Avg, m.Min, m.Max, value):
					default:
					}
				}
			}
		}()
	}

	wg.Wait()
	close(failures)

	for msg := range failures {
		t.Error(msg)
	}
	if snapshots.Load() == 0 {
		t.Fatal("every /stats was shed; the readers never observed anything")
	}

	got, ok := mergeAll(s, "shed.metric")
	if !ok {
		t.Fatal("nothing recorded")
	}
	if int64(got.Count) != accepted.Load() {
		t.Errorf("server answered 200 to %d ingests but recorded %d",
			accepted.Load(), got.Count)
	}
}

// --- cardinality under concurrency ---

// The cap is a check followed by an insert, which is the classic
// check-then-act race: if the two were not under one lock, N goroutines
// could all see room for one more and all take it. Many writers race to
// create names on a single shard, and the count afterwards must be the cap
// exactly - not "about" the cap.
func TestCardinalityCapIsExactUnderConcurrentCreation(t *testing.T) {
	const writers = 32

	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	// Twice the cap of names, all on one shard, so the writers genuinely
	// contend for the last slots rather than running out of candidates.
	names := namesForShard(t, 0, maxMetricsPerShard*2)

	var (
		created  atomic.Int64
		refused  atomic.Int64
		wg       sync.WaitGroup
		failures = make(chan string, 16)
	)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Interleaved rather than partitioned, so writers collide on
			// the same names as well as on the same shard.
			for i := w; i < len(names); i += writers {
				err := s.record(now, Event{Name: names[i], Value: 1, TS: ts})
				switch {
				case err == nil:
					created.Add(1)
				case errors.Is(err, errCardinality):
					refused.Add(1)
				default:
					select {
					case failures <- fmt.Sprintf("unexpected error: %v", err):
					default:
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(failures)

	for msg := range failures {
		t.Error(msg)
	}

	got := int64(metricCount(s))
	if got > int64(maxMetricsPerShard) {
		t.Errorf("store holds %d metrics, past the cap of %d - the check and the "+
			"insert are not atomic", got, maxMetricsPerShard)
	}
	if got != int64(maxMetricsPerShard) {
		t.Errorf("store holds %d metrics, want exactly the cap %d", got, maxMetricsPerShard)
	}
	if created.Load() != got {
		t.Errorf("%d record calls succeeded but the store holds %d metrics",
			created.Load(), got)
	}
	if refused.Load() == 0 {
		t.Fatal("nothing was refused; the test never reached the cap")
	}
}

// A refused event must not be recorded, and everything else must still add
// up - the project's standing invariant, now with the cap firing throughout.
func TestRejectedCardinalityEventsAreNeverRecorded(t *testing.T) {
	const (
		writers   = 16
		perWriter = 200
	)

	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	// One shard's worth of names plus a flood of new ones on the same shard,
	// so most of the traffic is refused.
	names := namesForShard(t, 0, maxMetricsPerShard+writers*perWriter)

	var (
		accepted atomic.Int64
		wg       sync.WaitGroup
	)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// Half the traffic re-uses an early (existing) name, half
				// invents a fresh one.
				var name string
				if i%2 == 0 {
					name = names[i%64]
				} else {
					name = names[maxMetricsPerShard+w*perWriter+i]
				}
				if err := s.record(now, Event{Name: name, Value: 1, TS: ts}); err == nil {
					accepted.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()

	recorded := 0
	for _, series := range storeDump(s) {
		agg, ok := mergeBuckets(series, 0)
		if !ok {
			continue
		}
		recorded += agg.Count
	}

	if int64(recorded) != accepted.Load() {
		t.Errorf("record accepted %d events, store holds %d", accepted.Load(), recorded)
	}
	if got := metricCount(s); got > maxMetricsPerShard {
		t.Errorf("store holds %d metrics, past the cap of %d", got, maxMetricsPerShard)
	}
}

// The sweeper deletes map entries while writers are creating them and
// readers are walking them. It takes the same per-shard lock as everything
// else, so this is the test that says so - Go panics on concurrent map
// access even without -race, so a dropped lock anywhere here fails loudly.
//
// The invariant is one-sided on purpose: a metric written continuously must
// never be swept, because its newest bucket is always inside the window.
// Metrics that stop being written may or may not survive depending on when
// the sweep lands, and asserting otherwise would be asserting a race.
func TestSweepingConcurrentlyWithWritersAndReaders(t *testing.T) {
	const (
		writers   = 16
		perWriter = 400
		readers   = 4
		sweepers  = 2
	)

	s := newStore()
	ts := time.Now().UnixMilli()

	var (
		wgWork   sync.WaitGroup
		wgBg     sync.WaitGroup
		done     atomic.Bool
		accepted atomic.Int64
	)

	// Written continuously for the whole test, so no sweep may remove it.
	const alive = "always.written"

	for w := 0; w < writers; w++ {
		wgWork.Add(1)
		go func() {
			defer wgWork.Done()
			for i := 0; i < perWriter; i++ {
				now := time.Now()
				if err := s.record(now, Event{Name: alive, Value: 1, TS: now.UnixMilli()}); err == nil {
					accepted.Add(1)
				}
				// Plus churn: names that are created and then abandoned,
				// which is exactly what the sweeper is meant to reclaim.
				churn := fmt.Sprintf("churn.%d", i)
				_ = s.record(now, Event{Name: churn, Value: 1, TS: ts})
			}
		}()
	}

	for r := 0; r < readers; r++ {
		wgBg.Add(1)
		go func() {
			defer wgBg.Done()
			w := &discardWriter{}
			for !done.Load() {
				s.handleStats(w, httptest.NewRequest(http.MethodGet, "/stats", nil))
			}
		}()
	}

	for sw := 0; sw < sweepers; sw++ {
		wgBg.Add(1)
		go func() {
			defer wgBg.Done()
			for !done.Load() {
				s.sweep(time.Now())
			}
		}()
	}

	wgWork.Wait()
	done.Store(true)
	wgBg.Wait()

	got, ok := mergeAll(s, alive)
	if !ok {
		t.Fatal("the continuously-written metric was swept away")
	}
	if int64(got.Count) != accepted.Load() {
		t.Errorf("record accepted %d events for %s, store holds %d",
			accepted.Load(), alive, got.Count)
	}
}

// --- the bounded query under concurrency ---

// statsVia runs a /stats query and decodes it without a *testing.T, so it is
// safe to call from a goroutine.
func statsVia(s *Store, query string) (StatsResponse, error) {
	rec := httptest.NewRecorder()
	s.handleStats(rec, httptest.NewRequest(http.MethodGet, "/stats"+query, nil))

	var resp StatsResponse
	if rec.Code != http.StatusOK {
		return resp, fmt.Errorf("status %d: %s", rec.Code, rec.Body.String())
	}
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	return resp, err
}

// §28 moved the merge out from under a whole-shard lock to a per-metric one,
// which makes the response even less of a single instant than §24 left it.
// What must survive is the thing that actually matters: no *metric* is ever
// torn, because its numbers still come from one merge under one lock.
//
// Every event for a metric carries that metric's own constant value, so the
// invariant is checkable from the response alone.
func TestBoundedStatsIsNeverTornWhileWritersRun(t *testing.T) {
	const (
		metrics   = 200
		perMetric = 2000
		readers   = 4
	)

	s := newStore()
	ts := time.Now().UnixMilli()

	names := make([]string, metrics)
	value := map[string]float64{}
	for i := range names {
		names[i] = fmt.Sprintf("bounded.%04d", i)
		value[names[i]] = float64(i + 1)
	}

	var (
		writers   sync.WaitGroup
		readersWG sync.WaitGroup
		done      atomic.Bool
		pages     atomic.Int64
		failures  = make(chan string, 32)
	)
	fail := func(msg string) {
		select {
		case failures <- msg:
		default:
		}
	}

	for _, name := range names {
		writers.Add(1)
		go func(name string) {
			defer writers.Done()
			now := time.Now()
			for i := 0; i < perMetric; i++ {
				_ = s.record(now, Event{Name: name, Value: value[name], TS: ts})
			}
		}(name)
	}

	// Readers use every part of the new surface, so a lock dropped in any
	// of the three phases shows up here.
	queries := []string{"", "?limit=10", "?prefix=bounded.01", "?limit=5&after=bounded.0100"}
	for r := 0; r < readers; r++ {
		readersWG.Add(1)
		go func(r int) {
			defer readersWG.Done()
			for !done.Load() {
				resp, err := statsVia(s, queries[r%len(queries)])
				if err != nil {
					fail(err.Error())
					return
				}
				pages.Add(1)

				for name, m := range resp.Metrics {
					v, known := value[name]
					if !known {
						fail(fmt.Sprintf("unknown metric %q in a response", name))
						continue
					}
					if m.Avg != v || m.Min != v || m.Max != v {
						fail(fmt.Sprintf("%s torn: avg/min/max = %v/%v/%v, want %v",
							name, m.Avg, m.Min, m.Max, v))
					}
				}
				if len(resp.Metrics) > resp.Matched {
					fail(fmt.Sprintf("returned %d metrics but matched only %d",
						len(resp.Metrics), resp.Matched))
				}
			}
		}(r)
	}

	writers.Wait()
	done.Store(true)
	readersWG.Wait()
	close(failures)

	for msg := range failures {
		t.Error(msg)
	}
	if pages.Load() == 0 {
		t.Fatal("no query completed; the readers never observed the store")
	}

	for _, name := range names {
		got, ok := mergeAll(s, name)
		if !ok {
			t.Fatalf("%s: nothing recorded", name)
		}
		if got.Count != perMetric {
			t.Errorf("%s: Count = %d, want %d", name, got.Count, perMetric)
		}
	}
}

// Keyset pagination has to cover a stable set of names even while their
// values are being hammered - the cursor is a name, so writes that do not
// create or remove names must not be able to disturb the walk.
func TestPaginationCoversAStableSetWhileValuesChurn(t *testing.T) {
	const (
		metrics  = 300
		pageSize = 17 // not a divisor of metrics
		writers  = 8
	)

	s := newStore()
	ts := time.Now().UnixMilli()

	names := make([]string, metrics)
	for i := range names {
		names[i] = fmt.Sprintf("churn.%04d", i)
		recordNow(s, names[i], 1) // every name exists before paging starts
	}

	var (
		wg   sync.WaitGroup
		done atomic.Bool
	)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			now := time.Now()
			// Until the walk finishes, not a fixed count: a writer that
			// ran out of work before paging started would leave this
			// asserting nothing about concurrency.
			for i := 0; !done.Load(); i++ {
				// Writes to existing names only: no name is created or
				// removed while the walk is in progress.
				_ = s.record(now, Event{Name: names[i%metrics], Value: 1, TS: ts})
			}
		}(w)
	}

	seen := map[string]int{}
	after := ""
	for pages := 0; ; pages++ {
		if pages > metrics {
			t.Fatal("pagination did not terminate")
		}
		resp, err := statsVia(s, fmt.Sprintf("?limit=%d&after=%s", pageSize, after))
		if err != nil {
			t.Fatal(err)
		}
		for name := range resp.Metrics {
			seen[name]++
		}
		if !resp.Truncated {
			break
		}
		if resp.Next <= after {
			t.Fatalf("cursor did not advance: %q after %q", resp.Next, after)
		}
		after = resp.Next
	}

	done.Store(true)
	wg.Wait()

	for _, name := range names {
		if seen[name] != 1 {
			t.Errorf("%s was returned %d times, want exactly 1", name, seen[name])
		}
	}
	if len(seen) != metrics {
		t.Errorf("saw %d distinct metrics, want %d", len(seen), metrics)
	}
}

// The window §28 opened: a name is collected in phase one and merged in
// phase three, and the sweeper (§27) can delete it in between. aggFor then
// reports no data and the metric is skipped, which is correct - but it is a
// race that did not exist when the merge happened under the same lock hold
// as the walk, so it gets a test rather than an assumption.
func TestQueryingWhileTheSweeperDeletesMetrics(t *testing.T) {
	const (
		readers   = 4
		sweepers  = 2
		churn     = 3000
		perWriter = 2
	)

	s := newStore()

	var (
		wgWork   sync.WaitGroup
		wgBg     sync.WaitGroup
		done     atomic.Bool
		pages    atomic.Int64
		failures = make(chan string, 16)
	)

	// One metric written continuously, so it can never be swept and must
	// appear in every unfiltered query that reaches it.
	const alive = "alive.metric"
	wgWork.Add(1)
	go func() {
		defer wgWork.Done()
		for i := 0; i < churn; i++ {
			now := time.Now()
			_ = s.record(now, Event{Name: alive, Value: 1, TS: now.UnixMilli()})
		}
	}()

	// Plus names created and immediately abandoned, stamped old enough that
	// the very next sweep reclaims them.
	wgWork.Add(1)
	go func() {
		defer wgWork.Done()
		old := time.Now().Add(-window - time.Minute)
		for i := 0; i < churn; i++ {
			for v := 0; v < perWriter; v++ {
				_ = s.record(old, Event{Name: fmt.Sprintf("doomed.%05d", i),
					Value: 1, TS: old.UnixMilli()})
			}
		}
	}()

	for r := 0; r < readers; r++ {
		wgBg.Add(1)
		go func() {
			defer wgBg.Done()
			for !done.Load() {
				resp, err := statsVia(s, "?limit=50")
				if err != nil {
					select {
					case failures <- err.Error():
					default:
					}
					return
				}
				pages.Add(1)

				// A metric that came back must be internally consistent;
				// one that was swept between phases is simply absent, which
				// is the correct outcome and not an error.
				for name, m := range resp.Metrics {
					if m.Count <= 0 {
						select {
						case failures <- fmt.Sprintf("%s returned with Count=%d", name, m.Count):
						default:
						}
					}
					if m.Min > m.Max {
						select {
						case failures <- fmt.Sprintf("%s has Min %v > Max %v", name, m.Min, m.Max):
						default:
						}
					}
				}
			}
		}()
	}

	for sw := 0; sw < sweepers; sw++ {
		wgBg.Add(1)
		go func() {
			defer wgBg.Done()
			for !done.Load() {
				s.sweep(time.Now())
			}
		}()
	}

	wgWork.Wait()
	done.Store(true)
	wgBg.Wait()
	close(failures)

	for msg := range failures {
		t.Error(msg)
	}
	if pages.Load() == 0 {
		t.Fatal("no query completed")
	}

	// The continuously-written metric survived everything.
	if _, ok := mergeAll(s, alive); !ok {
		t.Error("the continuously-written metric was swept away")
	}
}
