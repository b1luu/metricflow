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
	"fmt"
	"log"
	"time"
)

const (
	// maxMetricsPerShard caps distinct metric names per shard, so the store
	// holds at most shardCount * this many.
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

	// sweepInterval is how often idle metrics are reclaimed. Tied to the
	// bucket width because that is the granularity at which anything can
	// actually age out; sweeping faster would walk the same maps to find
	// the same nothing.
	sweepInterval = bucketWidth
)

// errCardinality is returned by record when a metric name is new and its
// shard is full. It reads as a rejection of the event, because that is what
// it is - but the cause is the name, not the value or the timestamp.
var errCardinality = fmt.Errorf(
	"cardinality limit reached: this shard already holds %d distinct metric "+
		"names and this one is new", maxMetricsPerShard)

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
				log.Printf("swept %d idle metrics (%d buckets)", metrics, buckets)
			}
		}
	}
}
