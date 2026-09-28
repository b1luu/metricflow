package main

// The server's own telemetry, recorded as metrics.
//
// §26, §27 and §29 each added a counter and each stopped short of exposing
// it. shedded(), throttledCount() and tracked() have exactly zero callers
// outside tests: the server counts how often it refused a request for
// capacity, how often it throttled a client, and how many clients it is
// tracking, and an operator running it can see none of that.
//
// §26 argued for leaving them there - "a property of the server, not of the
// metrics, and putting it in /stats would mix the two". Half of that was
// right. Bolting server counters onto the *shape* of the stats response, as
// extra fields beside the per-metric numbers, would have mixed two unrelated
// things. But the conclusion did not follow: a metrics server's own telemetry
// is metrics, and the right way to expose it is as metrics - a reserved name
// prefix, queried with the same ?prefix= and alerted on with the same rules
// as anything else. Prometheus does precisely this with its own series.
//
// Two properties make that safe, and both matter more than the exposure.
//
// A client cannot write into the reserved namespace. Without that, the one
// signal you reach for during an incident is the one an incident can forge:
// a service flooding metricflow.requests.shed with zeroes would make a
// shedding server look calm.
//
// And the reserved namespace ignores the cardinality cap. Self-metrics must
// not be the first thing to fail when the store fills up, because a full
// store is exactly when somebody needs to see that it is full. The set is
// fixed by this file rather than by any client, so it cannot grow without
// bound - which is the only reason it is safe to exempt.

import (
	"context"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// selfPrefix marks the reserved namespace. Chosen to look like an
	// ordinary metric name so it queries and alerts like one.
	selfPrefix = "metricflow."

	// selfInterval is how often the server samples itself. One second gives
	// ten samples per bucket (§8), which is enough to see a burst of
	// shedding inside the window rather than only its total.
	selfInterval = time.Second
)

// Counters are cumulative, not per-interval deltas.
//
// The store is windowed, so a counter recorded cumulatively is read back as
// max - min over the window: the increase across it. That is the same
// treatment Prometheus gives counters, and it survives a missed sample,
// where deltas would lose one permanently. Gauges are recorded as they are.
const (
	selfShed         = selfPrefix + "requests.shed"           // counter
	selfThrottled    = selfPrefix + "requests.throttled"      // counter
	selfAccepted     = selfPrefix + "events.accepted"         // counter
	selfRejected     = selfPrefix + "events.rejected"         // counter
	selfSweptMetrics = selfPrefix + "sweep.metrics_reclaimed" // counter
	selfClients      = selfPrefix + "clients.tracked"         // gauge
	selfStoreMetrics = selfPrefix + "store.metrics"           // gauge
	selfStoreBuckets = selfPrefix + "store.buckets"           // gauge
	selfGoroutines   = selfPrefix + "runtime.goroutines"      // gauge

	// Persistence (§33). Published only when a snapshot path is configured,
	// because a server with persistence switched off has no snapshot age,
	// and a zero there would read as "just snapshotted" - indistinguishable
	// from the healthiest possible value. Absence is the honest answer.
	selfSnapAge       = selfPrefix + "snapshot.age_seconds"    // gauge
	selfSnapBytes     = selfPrefix + "snapshot.bytes"          // gauge
	selfSnapWriteFail = selfPrefix + "snapshot.write_failures" // counter
	selfSnapLoadFail  = selfPrefix + "snapshot.load_failures"  // counter
)

// reserved reports whether a metric name belongs to the server rather than
// to a client.
func reserved(name string) bool { return strings.HasPrefix(name, selfPrefix) }

// counters are the totals the server keeps about itself.
//
// Incremented once per request rather than once per event, deliberately. A
// batch of ten thousand events would otherwise take ten thousand atomic
// increments on a counter every core is touching - contended, and measurable
// against a 96 ns parse (§30). Adding the batch's totals once gives the same
// number for the price of one.
type counters struct {
	accepted atomic.Int64
	rejected atomic.Int64
	swept    atomic.Int64

	// snapLoadFail counts startups that found a snapshot and could not use
	// it. Starting empty is no longer fatal (§33), so this is what stops it
	// being silent: it is the difference between "this server has no
	// history because it is new" and "this server has no history because
	// its snapshot was damaged".
	snapLoadFail atomic.Int64
}

func (c *counters) addAccepted(n int) {
	if n > 0 {
		c.accepted.Add(int64(n))
	}
}

func (c *counters) addRejected(n int) {
	if n > 0 {
		c.rejected.Add(int64(n))
	}
}

func (c *counters) addSwept(n int) {
	if n > 0 {
		c.swept.Add(int64(n))
	}
}

func (c *counters) addSnapshotLoadFailure() { c.snapLoadFail.Add(1) }

// selfReporter samples the server and records the result into the store it
// is sampling.
type selfReporter struct {
	store   *Store
	limiter *limiter
	clients *clients
	counts  *counters

	// nil when persistence is switched off, which is a state the reporter
	// has to represent rather than paper over.
	snap *snapshotter
}

func newSelfReporter(s *Store, l *limiter, cs *clients, c *counters, sn *snapshotter) *selfReporter {
	return &selfReporter{store: s, limiter: l, clients: cs, counts: c, snap: sn}
}

// sample records one observation of every self-metric.
//
// Recorded through the ordinary record path, with no client, so these consume
// nobody's budget (§29) and land in the same buckets, windows and histograms
// as any other metric. That is the point: the query path, the alerting layer
// and the retention rules all apply to the server's own numbers for free.
func (r *selfReporter) sample(now time.Time) {
	// Measured before this sample writes itself, so the very first sample
	// on an empty server honestly reports an empty store. After that the
	// gauges include the reporter's own series - a fixed offset of however
	// many metrics this file publishes, which is the whole extent of the
	// self-reference and is cheaper to explain than to correct for.
	metrics, buckets := r.store.size()

	observations := [...]struct {
		name  string
		value float64
	}{
		{selfShed, float64(r.limiter.shedded())},
		{selfThrottled, float64(r.limiter.throttledCount())},
		{selfAccepted, float64(r.counts.accepted.Load())},
		{selfRejected, float64(r.counts.rejected.Load())},
		{selfSweptMetrics, float64(r.counts.swept.Load())},
		{selfClients, float64(r.clients.tracked())},
		{selfStoreMetrics, float64(metrics)},
		{selfStoreBuckets, float64(buckets)},
		{selfGoroutines, float64(runtime.NumGoroutine())},
	}

	ms := now.UnixMilli()
	// An error from record here would mean the reserved namespace stopped
	// being exempt from the cardinality cap, which is a bug in this file
	// rather than a condition to handle. Ignored rather than logged,
	// because logging once a second about it would be worse.
	rec := func(name string, value float64) {
		_ = r.store.record(now, Event{Name: name, Value: value, TS: ms})
	}

	for _, o := range observations {
		rec(o.name, o.value)
	}

	if r.snap != nil {
		age, bytes, writeFailures, fresh := r.snap.stats(now)

		// Failures are published even before a first write has succeeded,
		// because "every write so far has failed" is precisely the state
		// worth seeing, and gating it on a success would hide it.
		rec(selfSnapWriteFail, float64(writeFailures))
		rec(selfSnapLoadFail, float64(r.counts.snapLoadFail.Load()))

		if fresh {
			rec(selfSnapAge, age.Seconds())
			rec(selfSnapBytes, float64(bytes))
		}
	}
}

// Run samples every `every` until ctx is cancelled.
//
// The interval is a parameter for the same reason Alerter.Run's is (§18): a
// test needs the behaviour, not the wait.
func (r *selfReporter) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()

	// One sample immediately, so a server that is asked about itself in its
	// first second has something to say.
	r.sample(time.Now())

	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-t.C:
			r.sample(tick)
		}
	}
}
