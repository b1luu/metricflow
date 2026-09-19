package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// --- sweeping ---

// The hole sweep exists to close: eviction runs on write, so a metric nobody
// writes to any more is never evicted by anything. Its buckets age out of
// every query and stay in memory forever.
func TestSweepReclaimsAMetricNobodyWritesToAnyMore(t *testing.T) {
	s := newStore()
	now := time.Now()

	// Written once, a full window ago, and never again.
	old := now.Add(-window - time.Minute)
	s.record(old, Event{Name: "gone.away", Value: 1, TS: old.UnixMilli()})

	if metricCount(s) != 1 {
		t.Fatalf("setup: store holds %d metrics, want 1", metricCount(s))
	}
	// Nothing has written to it since, so nothing has evicted it.
	if _, ok := mergeAll(s, "gone.away"); !ok {
		t.Fatal("setup: the metric is already gone before the sweep")
	}

	metrics, buckets := s.sweep(now)

	if metrics != 1 {
		t.Errorf("sweep removed %d metrics, want 1", metrics)
	}
	if buckets != 1 {
		t.Errorf("sweep removed %d buckets, want 1", buckets)
	}
	if got := metricCount(s); got != 0 {
		t.Errorf("store still holds %d metrics after the sweep", got)
	}
}

// A metric still inside the window must survive, buckets and all. A sweeper
// that reclaimed live data would be far worse than one that reclaimed
// nothing.
func TestSweepLeavesLiveMetricsAlone(t *testing.T) {
	s := newStore()
	now := time.Now()

	recordNow(s, "live.metric", 5)

	metrics, buckets := s.sweep(now)

	if metrics != 0 || buckets != 0 {
		t.Errorf("sweep removed %d metrics and %d buckets from a live store, want 0 and 0",
			metrics, buckets)
	}
	got, ok := mergeAll(s, "live.metric")
	if !ok || got.Count != 1 || got.Sum != 5 {
		t.Errorf("live.metric = %+v (ok=%v), want Count=1 Sum=5", got, ok)
	}
}

// The partial case: a metric with both aged and current buckets keeps the
// current ones and keeps its entry.
func TestSweepTrimsBucketsWithoutRemovingTheMetric(t *testing.T) {
	s := newStore()
	now := time.Now()

	seedBucket(s, "half.stale", bucketAt(window+time.Minute), &Agg{Count: 3, Sum: 3, Min: 1, Max: 1})
	seedBucket(s, "half.stale", bucketAt(0), &Agg{Count: 7, Sum: 7, Min: 1, Max: 1})

	metrics, buckets := s.sweep(now)

	if metrics != 0 {
		t.Errorf("sweep removed %d metrics, want 0 - the metric still has a live bucket", metrics)
	}
	if buckets != 1 {
		t.Errorf("sweep removed %d buckets, want 1", buckets)
	}

	got, ok := mergeAll(s, "half.stale")
	if !ok {
		t.Fatal("the metric was removed despite having a live bucket")
	}
	if got.Count != 7 {
		t.Errorf("Count = %d, want 7 - only the aged bucket should be gone", got.Count)
	}
}

// Sweeping spans shards, so the walk has to cover all of them rather than
// whichever one the first metric happened to land in.
func TestSweepCoversEveryShard(t *testing.T) {
	const metrics = 500

	s := newStore()
	now := time.Now()
	old := now.Add(-window - time.Minute)

	spread := map[int]bool{}
	for i := 0; i < metrics; i++ {
		name := fmt.Sprintf("idle.metric.%d", i)
		spread[shardIndex(name)] = true
		s.record(old, Event{Name: name, Value: 1, TS: old.UnixMilli()})
	}
	if len(spread) != shardCount {
		t.Fatalf("%d names covered %d of %d shards; the test would not be "+
			"exercising a multi-shard sweep", metrics, len(spread), shardCount)
	}

	if removed, _ := s.sweep(now); removed != metrics {
		t.Errorf("sweep removed %d metrics, want all %d", removed, metrics)
	}
	if got := metricCount(s); got != 0 {
		t.Errorf("store still holds %d metrics", got)
	}
}

// An empty store must not be a special case.
func TestSweepOnAnEmptyStoreIsANoOp(t *testing.T) {
	s := newStore()
	if metrics, buckets := s.sweep(time.Now()); metrics != 0 || buckets != 0 {
		t.Errorf("sweep of an empty store removed %d metrics and %d buckets", metrics, buckets)
	}
}

// --- the sweeper loop ---

// The loop runs at whatever interval it is given and stops when its context
// does. Both halves matter: a sweeper that never fires reclaims nothing, and
// one that ignores cancellation outlives the server.
func TestSweepLoopReclaimsThenStopsOnCancel(t *testing.T) {
	s := newStore()
	old := time.Now().Add(-window - time.Minute)
	s.record(old, Event{Name: "gone.away", Value: 1, TS: old.UnixMilli()})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Sweep(ctx, time.Millisecond)
	}()

	deadline := time.After(3 * time.Second)
	for metricCount(s) != 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("the sweeper never reclaimed the idle metric")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the sweeper did not stop when its context was cancelled")
	}
}

// --- the cardinality cap ---

// namesForShard returns n distinct metric names that all hash to one shard,
// so a test can fill a single shard's budget without creating
// shardCount * maxMetricsPerShard metrics to do it.
func namesForShard(t *testing.T, shard, n int) []string {
	t.Helper()

	names := make([]string, 0, n)
	for i := 0; len(names) < n; i++ {
		if i > 50_000_000 {
			t.Fatalf("only found %d of %d names hashing to shard %d", len(names), n, shard)
		}
		name := fmt.Sprintf("card.%d", i)
		if shardIndex(name) == shard {
			names = append(names, name)
		}
	}
	return names
}

// Past the cap a *new* name is refused. The number is per shard, so this
// fills exactly one shard rather than the whole store.
func TestRecordRefusesNewMetricsPastTheShardCap(t *testing.T) {
	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	names := namesForShard(t, 0, maxMetricsPerShard+1)

	for i, name := range names[:maxMetricsPerShard] {
		if err := s.record(now, Event{Name: name, Value: 1, TS: ts}); err != nil {
			t.Fatalf("metric %d of %d was refused: %v", i+1, maxMetricsPerShard, err)
		}
	}

	err := s.record(now, Event{Name: names[maxMetricsPerShard], Value: 1, TS: ts})
	if !errors.Is(err, errCardinality) {
		t.Fatalf("the metric past the cap returned %v, want errCardinality", err)
	}
	if got := metricCount(s); got != maxMetricsPerShard {
		t.Errorf("store holds %d metrics, want exactly the cap %d", got, maxMetricsPerShard)
	}
}

// The property the limit exists for: a client inventing names must not be
// able to degrade the metrics that were already there. A full shard keeps
// serving every metric it already holds.
func TestAFullShardStillAcceptsItsExistingMetrics(t *testing.T) {
	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	names := namesForShard(t, 0, maxMetricsPerShard+1)
	for _, name := range names[:maxMetricsPerShard] {
		if err := s.record(now, Event{Name: name, Value: 1, TS: ts}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.record(now, Event{Name: names[maxMetricsPerShard], Value: 1, TS: ts}); err == nil {
		t.Fatal("setup: the shard is not actually full")
	}

	// Every existing metric still takes writes, and they still aggregate.
	for _, name := range names[:maxMetricsPerShard] {
		if err := s.record(now, Event{Name: name, Value: 3, TS: ts}); err != nil {
			t.Fatalf("%s: an existing metric was refused on a full shard: %v", name, err)
		}
	}
	got, ok := mergeAll(s, names[0])
	if !ok || got.Count != 2 || got.Sum != 4 {
		t.Errorf("%s = %+v (ok=%v), want Count=2 Sum=4", names[0], got, ok)
	}
}

// One shard filling must not close the others. This is the pay-off for
// checking per shard instead of globally: the blast radius of a name-flood
// is one shard, not the store.
func TestAFullShardDoesNotAffectTheOthers(t *testing.T) {
	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	for _, name := range namesForShard(t, 0, maxMetricsPerShard) {
		if err := s.record(now, Event{Name: name, Value: 1, TS: ts}); err != nil {
			t.Fatal(err)
		}
	}

	// A name on any other shard is still perfectly welcome.
	for shard := 1; shard < shardCount; shard++ {
		name := namesForShard(t, shard, 1)[0]
		if err := s.record(now, Event{Name: name, Value: 1, TS: ts}); err != nil {
			t.Errorf("shard %d refused a new metric while only shard 0 is full: %v", shard, err)
		}
	}
}

// A slot has to come back, or the cap is a one-way door: the store would
// fill once and refuse every new name forever, including legitimate ones.
func TestSweepingAFullShardFreesItsSlots(t *testing.T) {
	s := newStore()
	now := time.Now()

	// Written a full window ago, so a sweep at `now` reclaims them.
	old := now.Add(-window - time.Minute)
	names := namesForShard(t, 0, maxMetricsPerShard+1)
	for _, name := range names[:maxMetricsPerShard] {
		if err := s.record(old, Event{Name: name, Value: 1, TS: old.UnixMilli()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.record(now, Event{Name: names[maxMetricsPerShard], Value: 1, TS: now.UnixMilli()}); err == nil {
		t.Fatal("setup: the shard is not full")
	}

	if removed, _ := s.sweep(now); removed != maxMetricsPerShard {
		t.Fatalf("sweep removed %d metrics, want %d", removed, maxMetricsPerShard)
	}

	if err := s.record(now, Event{Name: names[maxMetricsPerShard], Value: 1, TS: now.UnixMilli()}); err != nil {
		t.Errorf("a new metric is still refused after the sweep freed the shard: %v", err)
	}
}

// A malformed event must not consume a slot. Otherwise a client sending
// garbage could fill the store with names that were never going to be
// valid - the cheapest possible way to deny service.
func TestInvalidEventsDoNotConsumeCardinality(t *testing.T) {
	s := newStore()
	now := time.Now()

	// Valid name, hopeless timestamp.
	for i := 0; i < 100; i++ {
		ev := Event{Name: fmt.Sprintf("never.valid.%d", i), Value: 1,
			TS: now.Add(-window - time.Hour).UnixMilli()}
		if err := s.admit(now, ev); err == nil {
			t.Fatalf("event %d was admitted despite an expired ts", i)
		}
	}

	if got := metricCount(s); got != 0 {
		t.Errorf("store holds %d metrics after 100 rejected events", got)
	}
}

// --- the cap over HTTP ---

// Cardinality is the one refusal about the server rather than the request,
// and the one that may stop being true, so it gets its own status and a
// Retry-After rather than being folded into the 400s.
func TestIngestReportsCardinalityAs429(t *testing.T) {
	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	names := namesForShard(t, 0, maxMetricsPerShard+1)
	for _, name := range names[:maxMetricsPerShard] {
		if err := s.record(now, Event{Name: name, Value: 1, TS: ts}); err != nil {
			t.Fatal(err)
		}
	}

	rec := postIngest(s, fmt.Sprintf(`{"name":%q,"value":1,"ts":%d}`,
		names[maxMetricsPerShard], ts))

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("no Retry-After; the client has nothing to back off on")
	}
	if !strings.Contains(rec.Body.String(), "cardinality") {
		t.Errorf("body = %q, want it to name the reason", rec.Body.String())
	}

	// A plain contract violation must still be a 400, or the two have been
	// merged and an operator cannot tell a naming bug from a bad client.
	if rec := postIngest(s, `{"value":1,"ts":1}`); rec.Code != http.StatusBadRequest {
		t.Errorf("a missing name gave %d, want 400", rec.Code)
	}
}

// In a batch a cardinality refusal is one event's problem, like any other
// rejection - but it is the only kind worth retrying, so the reply says so.
func TestBatchMarksCardinalityRejectsRetryable(t *testing.T) {
	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	names := namesForShard(t, 0, maxMetricsPerShard+2)
	for _, name := range names[:maxMetricsPerShard] {
		if err := s.record(now, Event{Name: name, Value: 1, TS: ts}); err != nil {
			t.Fatal(err)
		}
	}

	// One event for a metric that exists, one new name, one malformed.
	_, resp := postBatch(t, s, ndjson(
		Event{Name: names[0], Value: 1, TS: ts},
		Event{Name: names[maxMetricsPerShard], Value: 1, TS: ts},
		Event{Value: 1, TS: ts},
	))

	if resp.Accepted != 1 || resp.Rejected != 2 {
		t.Errorf("accepted/rejected = %d/%d, want 1/2", resp.Accepted, resp.Rejected)
	}
	if len(resp.Errors) != 2 {
		t.Fatalf("errors = %+v, want 2", resp.Errors)
	}

	cardinality, contract := resp.Errors[0], resp.Errors[1]
	if !cardinality.Retryable {
		t.Errorf("the cardinality reject at index %d is not marked retryable: %+v",
			cardinality.Index, cardinality)
	}
	if contract.Retryable {
		t.Errorf("the contract violation at index %d is marked retryable; resending it "+
			"would fail forever: %+v", contract.Index, contract)
	}
}

// A batch rejected entirely for cardinality is the case where 400 would be
// actively misleading: every event was well-formed and the store was simply
// full. A client obeying a 400 would stop retrying data it should retry.
func TestBatchRejectedOnlyForCardinalityIs429(t *testing.T) {
	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	names := namesForShard(t, 0, maxMetricsPerShard+3)
	for _, name := range names[:maxMetricsPerShard] {
		if err := s.record(now, Event{Name: name, Value: 1, TS: ts}); err != nil {
			t.Fatal(err)
		}
	}

	rec, resp := postBatch(t, s, ndjson(
		Event{Name: names[maxMetricsPerShard], Value: 1, TS: ts},
		Event{Name: names[maxMetricsPerShard+1], Value: 1, TS: ts},
	))

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429 - nothing was wrong with these events", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("no Retry-After on an entirely retryable batch")
	}
	if resp.Accepted != 0 || resp.Rejected != 2 {
		t.Errorf("accepted/rejected = %d/%d, want 0/2", resp.Accepted, resp.Rejected)
	}
	for _, e := range resp.Errors {
		if !e.Retryable {
			t.Errorf("error at index %d is not marked retryable: %+v", e.Index, e)
		}
	}
}

// One permanent rejection in the batch and it is a 400 again. Mixing a
// malformed event into a full store must not let the client believe the
// whole batch is worth resending unchanged.
func TestBatchWithAnyPermanentRejectIs400(t *testing.T) {
	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	names := namesForShard(t, 0, maxMetricsPerShard+2)
	for _, name := range names[:maxMetricsPerShard] {
		if err := s.record(now, Event{Name: name, Value: 1, TS: ts}); err != nil {
			t.Fatal(err)
		}
	}

	rec, resp := postBatch(t, s, ndjson(
		Event{Name: names[maxMetricsPerShard], Value: 1, TS: ts}, // retryable
		Event{Value: 1, TS: ts},                                  // permanent: no name
	))

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 - one of these events will never be valid", rec.Code)
	}
	if resp.Accepted != 0 || resp.Rejected != 2 {
		t.Errorf("accepted/rejected = %d/%d, want 0/2", resp.Accepted, resp.Rejected)
	}
}

// An empty batch keeps its own answer; the retryable check must not swallow
// the case where the client sent nothing at all.
func TestBatchEmptyIsStill400WithTheCapInPlay(t *testing.T) {
	s := newStore()
	rec, resp := postBatch(t, s, "")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(resp.Fatal, "empty batch") {
		t.Errorf("fatal = %q, want it to say the batch was empty", resp.Fatal)
	}
}
