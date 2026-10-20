package main

// Bounding cardinality: how many distinct metric names the store will hold,
// and how a name that has gone quiet gets its memory back.
//
// This is the failure mode metrics systems actually die of. Nothing here
// stops a client putting a request ID, a user ID, or a UUID in a metric
// name, and until now every such name became a permanent map entry: §10's
// eviction runs on write, so it only ever trims the buckets of a metric
// somebody is still writing to. A name seen once was evicted down to an
// empty series and then kept that empty series forever, along with its key.
//
// So the store had no upper bound on memory that depended on anything but
// client behaviour. Two things fix that, and they are genuinely different:
// sweeping gives memory back when a metric goes quiet, and the cardinality
// cap refuses to let one client's bad naming fill the store in the first
// place. Neither alone is enough - a sweeper cannot keep up with a client
// inventing names as fast as it can send them, and a cap with no sweeper
// would fill once and stay full.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	// maxMetricsPerShard caps distinct metric names per shard, so the store
	// holds at most shardCount * this many. It is the default; an operator
	// can set a different total with METRICFLOW_MAX_METRICS (metricsLimit).
	//
	// Per shard rather than globally, and that is a deliberate trade. A
	// global counter would be exact, but every ingest would touch it, which
	// is precisely the single point of contention §24 spent a slice
	// removing. A per-shard check costs nothing: the shard lock is already
	// held, and the count is a len() on a map already in hand.
	//
	// What it gives up: the limit is only approximately global. FNV spreads
	// names evenly, so a realistic workload fills the shards at about the
	// same rate - but an adversary who searches for names landing on one
	// shard can exhaust that shard's budget while the rest sit empty. That
	// turns out to be the better failure: the blast radius is one shard, and
	// metrics on the other 31 are untouched. A global cap would have let the
	// same attacker lock out every metric in the store.
	maxMetricsPerShard = 1024

	// maxMetricNameLen bounds one name, which is the other half of bounding
	// cardinality. A limit on how many names exist says nothing about how
	// big each one is, and the two multiply: 32 shards x 1024 names at a
	// megabyte each - which the 1 MiB batch body would permit - is tens of
	// gigabytes, so the count cap alone would have been close to no cap.
	maxMetricNameLen = 256

	// sweepInterval is how often idle metrics are reclaimed. Tied to the
	// bucket width because that is the granularity at which anything can
	// actually age out; sweeping faster would walk the same maps to find
	// the same nothing.
	sweepInterval = bucketWidth
)

// errCardinality is returned by record when a metric name is new and its
// shard is full. It reads as a rejection of the event, because that is what
// it is - but the cause is the name, not the value or the timestamp.
//
// A plain sentinel with no number in it: the cap is configured at startup
// (METRICFLOW_MAX_METRICS), after this var is built, so record wraps it with
// the store's actual limit instead.
var errCardinality = errors.New(
	"cardinality limit reached: this shard is full and this metric name is new")

// metricsLimit is the per-shard cap, derived from METRICFLOW_MAX_METRICS when
// it is set and maxMetricsPerShard when it is not.
//
// Tunable for the reason inFlightLimit is and shardCount is not: the right
// cap depends on how much memory the deployment has and how widely its
// fleet's values spread (§47), none of which this code can know. §47 gives
// the operator the arithmetic; this gives them somewhere to put the answer.
//
// The operator writes a total, because §47's arithmetic is in total metrics
// and the shard count is an internal detail (§24). The per-shard figure
// rounds down, so the cap the operator sized against memory is never
// exceeded - at the cost of up to shardCount-1 names of headroom, which run
// logs so the number enforced is never a surprise.
//
// Fewer than one per shard is refused: it would turn away every client name,
// a server that cannot ingest, which is a mistake rather than a policy. A bad
// value fails startup, as a bad in-flight limit does (§18, §26).
func metricsLimit() (perShard int, err error) {
	v, ok := os.LookupEnv("METRICFLOW_MAX_METRICS")
	if !ok {
		return maxMetricsPerShard, nil
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("METRICFLOW_MAX_METRICS=%q is not an integer", v)
	}
	if n < shardCount {
		return 0, fmt.Errorf("METRICFLOW_MAX_METRICS=%d must be at least %d, "+
			"one per shard; fewer would refuse every new metric name", n, shardCount)
	}
	return n / shardCount, nil
}

// errClientNames is returned when a caller has introduced metric names
// faster than maxNewNamesPerEpoch allows.
//
// Distinct from errCardinality because the two mean different things to the
// client that hit them. errCardinality says the *store* is full and nobody
// can add a name; this says the store has room but *you* have had your share
// of it for now. Both are retryable, and this one always succeeds eventually
// - the epoch rolls over on its own (§29).
var errClientNames = fmt.Errorf(
	"new metric names are limited to %d per %s per client; this name is new",
	maxNewNamesPerEpoch, nameEpoch)

// sweep reclaims memory across every shard: buckets that have aged out of
// the window, and then any metric left holding none.
//
// It exists because eviction is otherwise on-write (§10). A metric nobody
// writes to any more is never evicted by anything, so without a sweep its
// last buckets - and its map entry - are immortal. That is fine for a fixed
// set of metrics and fatal for a client generating new names.
//
// One shard at a time, never all at once, for the same reason /stats walks
// them one at a time (§24): holding every lock would hand ingest back the
// stall sharding removed. A sweep is not a snapshot and does not need to be.
//
// Deleting from a map while ranging over it is defined in Go: an entry
// deleted during iteration is simply not produced later, and the current
// key has already been produced.
func (s *Store) sweep(now time.Time) (metrics, buckets int) {
	cutoff := windowStart(now, window)

	for i := range s.shards {
		sh := &s.shards[i]

		sh.mu.Lock()
		for name, series := range sh.aggs {
			before := len(series)
			evict(series, cutoff)
			buckets += before - len(series)

			// No buckets left means nothing inside the window, so the
			// metric is indistinguishable from one that never existed.
			// Keeping the key would be keeping a name, not data.
			if len(series) == 0 {
				delete(sh.aggs, name)
				metrics++
			}
		}
		sh.mu.Unlock()
	}
	return metrics, buckets
}

// size reports how many metrics and how many buckets the store holds.
//
// One shard lock at a time, like every other walk (§24). The answer is a
// gauge rather than a snapshot and does not need to be consistent across
// shards - it is read once a second to record §31's own metrics, where being
// a few writes out of date is not a defect.
func (s *Store) size() (metrics, buckets int) {
	m, b, _ := s.sizeWithHist()
	return m, b
}

// sizeWithHist also counts histogram buckets, which is what the store's
// memory actually tracks (§47).
//
// metrics and buckets say how many series exist; neither says how much
// each one holds. A fleet reporting one value per metric costs about 59 MB
// at the cardinality ceiling and one reporting latencies across seven
// orders of magnitude costs 1.8 GB, and those two look identical in the
// gauges §31 publishes. This is the number that tells them apart.
func (s *Store) sizeWithHist() (metrics, buckets, histBuckets int) {
	for i := range s.shards {
		sh := &s.shards[i]

		sh.mu.Lock()
		metrics += len(sh.aggs)
		for _, series := range sh.aggs {
			buckets += len(series)
			for _, a := range series {
				if a.h != nil {
					histBuckets += len(a.h.pos) + len(a.h.neg)
				}
			}
		}
		sh.mu.Unlock()
	}
	return metrics, buckets, histBuckets
}

// Sweep reclaims idle metrics every `every` until ctx is cancelled.
//
// The interval is a parameter rather than the constant so tests can run it
// at a millisecond, the same trick Alerter.Run uses (§18) and run() uses for
// its listener - the loop's behaviour is what needs testing, not the clock.
func (s *Store) Sweep(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-t.C:
			// The tick's own timestamp, not a fresh time.Now(): if the
			// process was descheduled the tick is the honest answer for
			// when this sweep was due.
			if metrics, buckets := s.sweep(tick); metrics > 0 {
				s.counts.addSwept(metrics)
				log.Printf("swept %d idle metrics (%d buckets)", metrics, buckets)
			}
		}
	}
}
