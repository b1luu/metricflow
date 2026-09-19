package main

import (
	"context"
	"fmt"
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
