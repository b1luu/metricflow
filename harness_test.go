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
