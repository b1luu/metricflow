package main

import (
	"bytes"
	"context"       // shutdown deadline + signal-cancelled context
	"errors"        // errors.As, to classify a read failure
	"fmt"           // formatted printing (stdout + http responses)
	"io"            // io.ReadAll, to slurp the request body
	"log"           // server lifecycle messages
	"net"           // net.Listen, so run() can be handed a test listener
	"net/http"      // the HTTP server and routing
	"os"            // os.Interrupt
	"os/signal"     // catch Ctrl-C / SIGTERM
	"runtime/debug" // Stack, so a recovered panic is still diagnosable
	"strconv"       // Retry-After, which is seconds as a string
	"sync"          // Mutex, to guard each shard's aggs map
	"syscall"       // SIGTERM
	"time"
	"unsafe" // Sizeof, a compile-time constant, for cache-line padding
)

// Event is the in-memory form of one metric observation.
//
// The `json:"..."` struct tags no longer drive decoding - parseEvent reads
// the three wire keys directly (§30) - but they are not decoration. They
// still describe the contract, they are what encoding/json uses when a test
// or the load generator writes an event out, and they are what the
// differential test compares this parser against. Changing one changes the
// wire format, exactly as it did before.
type Event struct {
	Name  string  `json:"name"`  // metric name, e.g. "cpu.load"
	Value float64 `json:"value"` // measured value; float so decimals work
	TS    int64   `json:"ts"`    // unix timestamp in milliseconds
	Type  string  `json:"type"`  // metric kind, e.g. "gauge" / "counter"
}

// Agg holds the running aggregates for one metric: four summary numbers
// plus a bounded sketch of the distribution behind them. Still no events -
// h is a histogram of bucket counts, not samples (§23).
type Agg struct {
	Count int     // how many events seen
	Sum   float64 // sum of all values (Sum/Count = average)
	Min   float64 // smallest value seen
	Max   float64 // largest value seen

	// h answers percentile queries. Unexported and a pointer: Agg is copied
	// by value in places (mergeBuckets returns one), and a copy must never
	// be handed a histogram some other Agg is still writing into.
	h *hist
}

// shardState is one independently-locked slice of the store. Bundling the
// lock with the map it guards keeps "what's protected by what" obvious.
type shardState struct {
	mu   sync.Mutex
	aggs map[string]map[int64]*Agg
}

// cacheLine is the coherence granularity on x86-64 and arm64: the unit cores
// trade ownership of. A mutex is 8 bytes and a map header 8, so four
// unpadded shards would share one line, and a core taking shard 0's lock
// would invalidate that line for cores working on shards 1-3 - false
// sharing, where independent locks contend anyway through the cache.
const cacheLine = 64

// shard is shardState padded out to own a full cache line. Embedding keeps
// sh.mu and sh.aggs reading exactly as before, and unsafe.Sizeof is a
// compile-time constant, so adding a field either re-pads automatically or,
// past 64 bytes, fails to compile on a negative array length rather than
// silently reintroducing the false sharing.
type shard struct {
	shardState
	_ [cacheLine - unsafe.Sizeof(shardState{})]byte
}

// Store owns all metric state, split across independently-locked shards by
// metric name (§24). A metric's buckets always live in one shard, so a write
// takes exactly one lock and writes to different metrics don't queue behind
// each other - which is the ceiling §5 measured.
type Store struct {
	shards [shardCount]shard

	// counts is the server's own telemetry (§31). It lives on the Store
	// because the ingest and sweep paths are where the numbers happen, and
	// threading a separate object through both would be the same coupling
	// with more parameters.
	counts counters
}

// newStore returns a ready-to-use Store with every shard's map initialized.
func newStore() *Store {
	s := &Store{}
	for i := range s.shards {
		s.shards[i].aggs = make(map[string]map[int64]*Agg)
	}
	return s
}

// shardFor returns the shard owning a metric. Callers lock it themselves:
// the lock scope belongs to the operation, not to the lookup.
func (s *Store) shardFor(metric string) *shard {
	return &s.shards[shardIndex(metric)]
}

const (
	bucketWidth = 10 * time.Second         // width of one time bucket
	numBuckets  = 6                        // buckets kept per metric
	window      = numBuckets * bucketWidth // 60s: default /stats window and
	//                                        how far back buckets are retained

	// maxIngestBody caps the /ingest request body. One event is a few
	// hundred bytes; anything past this is a mistake or an attack.
	maxIngestBody = 4 << 10 // 4 KiB
)

// record folds one event into its metric's running aggregate.
//   - the bucket is chosen by ev.TS (event time, unix ms), so an event that
//     arrives late still lands in the bucket for when it happened.
//   - now (wall clock, passed in) drives eviction — that's about memory
//     pressure, not event time, so it stays on the real clock.
//
// handleIngest validates ev.TS is in [now-window, now+bucketWidth] before
// calling here; a direct caller that skips that just gets its out-of-range
// bucket evicted on the spot. See DESIGN.md §16.
// record folds an event in without charging any client's budget. Internal
// callers and tests use it; the global cardinality cap (§27) still applies,
// so it is a call with no *client* quota rather than one with no quota.
func (s *Store) record(now time.Time, ev Event) error {
	return s.recordFor(now, nil, ev)
}

// recordFor is record, charging c for any metric name it creates.
func (s *Store) recordFor(now time.Time, c *client, ev Event) error {
	bucket := time.UnixMilli(ev.TS).Truncate(bucketWidth).Unix()

	// One metric lives in exactly one shard, so an ingest takes one lock and
	// never blocks a write to a differently-named metric.
	sh := s.shardFor(ev.Name)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	series := sh.aggs[ev.Name]
	if series == nil {
		// Creating a name is the only place cardinality can grow, so it is
		// the only place worth checking - an event for a metric that
		// already exists never even reads the count. That keeps the cap off
		// the hot path entirely (§27).
		//
		// Existing metrics keep working however full the shard is. That is
		// the property the whole limit is for: a client inventing names
		// must not be able to degrade the metrics that were already there.
		// The reserved namespace is exempt (§31). Self-metrics must not be
		// the first thing to fail when the store fills, because a full
		// store is exactly when somebody needs to see that it is full. The
		// set is fixed by self.go rather than by any client, so exempting
		// it cannot let anything grow without bound.
		if !reserved(ev.Name) && len(sh.aggs) >= maxMetricsPerShard {
			return errCardinality
		}

		// The caller's own budget, checked after the global cap rather than
		// before it. A client must not be charged for a name it could not
		// have created anyway, and when the store is genuinely full that is
		// the reason worth reporting - it is the one the client cannot fix
		// by slowing down (§29).
		if !c.allowNewName(now) {
			return errClientNames
		}

		series = make(map[int64]*Agg)
		sh.aggs[ev.Name] = series
	}

	a := series[bucket]
	if a == nil {
		a = &Agg{Min: ev.Value, Max: ev.Value, h: &hist{}}
		series[bucket] = a
	}
	if a.h == nil {
		// An Agg built elsewhere (a test seeding a bucket) may have no
		// histogram yet. Create it rather than panicking; the summary
		// numbers stay correct either way.
		a.h = &hist{}
	}

	a.Count++
	a.Sum += ev.Value
	if ev.Value < a.Min {
		a.Min = ev.Value
	}
	if ev.Value > a.Max {
		a.Max = ev.Value
	}
	a.h.add(ev.Value)

	// Drop buckets that have aged out of the retention window, so a
	// long-lived metric's map stays bounded to ~numBuckets entries. A
	// metric that goes silent is not reached here at all, which is what the
	// sweeper is for (§10, §27).
	evict(series, windowStart(now, window))
	return nil
}

// admit is the single definition of "the server took this event": the
// contract check and the capacity check, in that order. Both ingest paths
// go through it so they cannot drift into two different ideas of what is
// acceptable.
//
// Order matters. A malformed event is refused on its own merits before it
// can consume a cardinality slot, so a client sending garbage cannot fill
// the store with names that were never going to be valid.
func (s *Store) admit(now time.Time, ev Event) error {
	return s.admitFor(now, nil, ev)
}

// admitFor is admit on behalf of a specific caller, so the name budget lands
// on the client that actually introduced the name.
//
// Validation still comes first, which now protects two budgets rather than
// one: a malformed event consumes neither a cardinality slot nor any of the
// caller's name allowance, so a client sending garbage cannot spend its own
// quota on names that were never going to be valid either.
func (s *Store) admitFor(now time.Time, c *client, ev Event) error {
	if err := validateEvent(now, ev); err != nil {
		return err
	}
	return s.recordFor(now, c, ev)
}

// retryableIngestError reports whether a refusal may succeed later.
//
// The two capacity limits are retryable and every contract violation is not,
// which is the distinction a client actually needs: resending a malformed
// event wastes both sides' time forever, while resending a refused name
// works as soon as there is room or the epoch rolls.
func retryableIngestError(err error) bool {
	return errors.Is(err, errCardinality) || errors.Is(err, errClientNames)
}

// writeIngestError renders a refusal from admit as an HTTP response.
//
// Cardinality is the one refusal here that is about the server rather than
// the request, and the one that may stop being true, so it gets its own
// status and a Retry-After. 400 would tell a client its request was
// malformed, which it wasn't; 503 is already this server's answer for "too
// many requests at once" (§26), and merging two unrelated conditions into
// one code would leave an operator unable to tell overload from a naming
// bug. 429 says what is actually true - you are asking for more than you
// are allowed - and carries backoff guidance.
//
// Retry-After is the window rather than the sweep interval: a slot frees
// when some other metric ages out entirely, not merely when a sweep runs.
func writeIngestError(w http.ResponseWriter, err error) {
	if retryableIngestError(err) {
		// The store's own window, which is the longer of the two waits and
		// therefore the safe one to quote for either limit.
		w.Header().Set("Retry-After", strconv.Itoa(int(window.Seconds())))
		http.Error(w, err.Error(), http.StatusTooManyRequests)
		return
	}
	http.Error(w, err.Error(), http.StatusBadRequest)
}

// evict deletes buckets older than cutoff from a metric's series.
// Deleting keys during a range loop is safe in Go. Caller must hold the
// Store lock. (Pure map surgery - stays a free function, no Store needed.)
func evict(series map[int64]*Agg, cutoff int64) {
	for bucket := range series {
		if bucket < cutoff {
			delete(series, bucket)
		}
	}
}

// mergeBuckets folds a metric's per-bucket Aggs into one combined Agg,
// ignoring any bucket whose key (its start time, unix seconds) is older
// than cutoff. Pass cutoff <= 0 to include every bucket.
// The bool is false when no bucket qualifies. Caller must hold the Store
// lock. (Pure read over the passed map - stays a free function.)
func mergeBuckets(series map[int64]*Agg, cutoff int64) (Agg, bool) {
	// A fresh histogram, never a reference to one of the buckets': the
	// returned Agg escapes this function while those buckets stay live and
	// keep being written to. Aliasing here would let a query's result
	// mutate under the caller, and would corrupt the store if the caller
	// merged into it.
	merged := Agg{h: &hist{}}

	first := true
	for bucket, a := range series {
		if bucket < cutoff {
			continue // outside the window
		}
		merged.Count += a.Count
		merged.Sum += a.Sum
		if first || a.Min < merged.Min {
			merged.Min = a.Min
		}
		if first || a.Max > merged.Max {
			merged.Max = a.Max
		}
		merged.h.merge(a.h) // exact: bucket counts add
		first = false
	}
	return merged, !first
}

// aggFor merges one metric's buckets at or after cutoff, taking the lock
// itself. The alerter needs a per-metric read because each rule carries its
// own window, and so its own cutoff.
//
// Sharding makes this strictly cheaper: it now locks only the shard owning
// this metric, so an alerter tick no longer blocks ingest for every other
// metric in the store.
func (s *Store) aggFor(name string, cutoff int64) (Agg, bool) {
	sh := s.shardFor(name)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return mergeBuckets(sh.aggs[name], cutoff)
}

// windowStart returns the bucket key marking the start of a window of
// size d ending at now: any bucket with a key >= this is inside the window.
func windowStart(now time.Time, d time.Duration) int64 {
	return now.Add(-d).Truncate(bucketWidth).Unix()
}

// allow wraps a handler so it only runs for one HTTP method. Anything
// else gets 405 with an Allow header, which the HTTP spec requires on a
// 405 response. This is the decorator pattern: a func that takes a
// handler and returns a wrapped one.
func allow(method string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

// handleHealth: GET /health - liveness check, proves the server is up.
func handleHealth(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "ok")
}

// validateEvent checks one decoded event against the ingest contract: the
// two fields that have no sane zero value, and the accepted ts range.
//
// It is a free function returning an error, rather than a handler writing a
// status, because the batch path needs the same verdict for each event in a
// stream with no response of its own to write it to. Every failure here is
// the caller getting the contract wrong, so every one of them is a 400 -
// which is why the error carries a message and not a status code.
func validateEvent(now time.Time, ev Event) error {
	// Unmarshal only checks syntax, not meaning. Name and TS have no sane
	// zero-value default, so they are checked explicitly. See DESIGN.md §12.
	if ev.Name == "" {
		return errors.New("name is required")
	}
	// A cap on how many names exist is worth little without a cap on how
	// big one can be: the batch body limit is 1 MiB, so without this a
	// client could fill its shard budget with names of a megabyte each and
	// spend gigabytes doing it (§27). Real metric names are well under a
	// hundred bytes; this is generous and still bounds the total.
	if len(ev.Name) > maxMetricNameLen {
		return fmt.Errorf("name is %d bytes, over the %d-byte limit",
			len(ev.Name), maxMetricNameLen)
	}
	// The reserved namespace belongs to the server (§31). A client that
	// could write into it could forge the one signal an operator reaches
	// for during an incident - filling metricflow.requests.shed with zeroes
	// would make a shedding server look calm.
	if reserved(ev.Name) {
		return fmt.Errorf("%q is reserved for the server's own metrics", selfPrefix)
	}
	if ev.TS == 0 {
		return errors.New("ts is required (unix milliseconds)")
	}

	// ev.TS picks the event's time bucket (see record / DESIGN.md §16).
	// Too old -> its bucket is already evicted, nothing to add to.
	// Too far future -> it would sit in a bucket that stays visible for
	// however long the clock is wrong. Either way, reject rather than
	// silently mis-record; one bucketWidth of future slack covers normal
	// client/server clock skew.
	ts := time.UnixMilli(ev.TS)
	switch {
	case ts.Before(now.Add(-window)):
		return fmt.Errorf("event too old (ts before now-%s)", window)
	case ts.After(now.Add(bucketWidth)):
		return fmt.Errorf("ts too far in the future (after now+%s)", bucketWidth)
	}
	return nil
}

// handleIngest: POST /ingest - accept one metric event as a JSON body.
func (s *Store) handleIngest(w http.ResponseWriter, r *http.Request) {
	// Read the whole body, but refuse to buffer an unbounded amount -
	// MaxBytesReader makes ReadAll fail once the limit is passed.
	r.Body = http.MaxBytesReader(w, r.Body, maxIngestBody)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "could not read body", http.StatusBadRequest)
		return
	}

	// Parse the raw bytes into a structured Event (§30). Only malformed
	// JSON fails here; missing fields are NOT an error, they stay at their
	// zero values and validateEvent judges them (§12).
	var ev Event
	n, err := parseEvent(body, &ev)
	if err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	// parseEvent reads one value and stops. This endpoint takes exactly one
	// event, so anything after it but whitespace is a malformed request -
	// which is what json.Unmarshal used to enforce by requiring the whole
	// body to be a single value.
	if len(bytes.TrimSpace(body[n:])) != 0 {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	now := time.Now()
	c, _ := clientFrom(r.Context())
	if err := s.admitFor(now, c, ev); err != nil {
		s.counts.addRejected(1)
		writeIngestError(w, err)
		return
	}
	s.counts.addAccepted(1)

	fmt.Fprintln(w, "got it")
}

// MetricStats is one metric's aggregate in a /stats response. Avg is
// derived (Sum/Count) and served raw; the caller rounds for display.
//
// The percentiles come from the histogram (§23) and carry its accuracy
// guarantee: each is within 1% of the true value. They are the numbers that
// actually describe a distribution - an average of 0.6 says nothing about
// whether the worst request took 2ms or 2 seconds.
type MetricStats struct {
	Count int     `json:"count"`
	Avg   float64 `json:"avg"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P99   float64 `json:"p99"`
}

// StatsResponse is the JSON body of GET /stats. Window echoes back the
// window actually applied, so the caller knows what it's looking at.
//
// The three fields after it exist because the response is now bounded
// (§28), and a bounded answer that does not say it is bounded is simply a
// wrong answer. Matched counts every metric name the query covered, before
// the limit; Truncated says some were not computed; Next is the name to
// continue from.
//
// Matched counts *names*, not metrics with data. Whether a metric has
// anything inside the window is only known after merging it, which is
// precisely the work the limit exists to avoid doing for everything - so
// len(Metrics) can be smaller than both Matched and the limit.
type StatsResponse struct {
	Window    string                 `json:"window"`
	Metrics   map[string]MetricStats `json:"metrics"`
	Matched   int                    `json:"matched"`
	Truncated bool                   `json:"truncated"`
	Next      string                 `json:"next,omitempty"`
}

// routes builds the request multiplexer. Separate from main so tests can
// exercise the wiring (path -> handler, method gating, shedding) directly.
//
// /health is deliberately outside the limiter, and it is the one exemption
// worth arguing for. A health check that gets shed makes an overloaded
// server look like a dead one, so whatever is watching it - an orchestrator,
// a load balancer - responds by killing or depooling the instance. That
// turns a server which was still serving most of its traffic into one
// serving none, and moves its load onto its equally-loaded neighbours. The
// check costs nothing to answer, so exempting it is close to free; being
// wrong about it is not.
func routes(s *Store, a *Alerter, l *limiter, cs *clients) http.Handler {
	mux := http.NewServeMux()

	// Shed-able: real work, and the traffic a firehose actually consists of.
	limited := map[string]http.HandlerFunc{
		"/ingest":       allow(http.MethodPost, s.handleIngest),
		"/ingest/batch": allow(http.MethodPost, s.handleIngestBatch),
		"/stats":        allow(http.MethodGet, s.handleStats),
		"/alerts":       allow(http.MethodGet, a.handleAlerts),
	}
	// identify wraps the limiter rather than the other way round, because
	// the limiter needs to know who the caller is before deciding whether
	// to admit them (§29).
	for path, h := range limited {
		mux.Handle(path, cs.identify(l.limit(h)))
	}

	// /health is outside identify as well as outside the limiter, and for
	// the same reason (§26): a malformed X-Client-ID from a sidecar would
	// otherwise make health checks 400, and an orchestrator would kill a
	// server that was serving everything else perfectly well.
	mux.HandleFunc("/health", allow(http.MethodGet, handleHealth))

	// Outermost, so it covers /health and the limiter as well as the
	// handlers - a route that is exempt from shedding should not also be
	// exempt from having its panics turned into an answer.
	return recoverPanic(mux)
}

// defaultRules is this program's alert configuration. Rules live in code on
// purpose: a POST /rules CRUD surface would add a lot of endpoint and
// nothing to the aggregation story. Note there is no "metric went silent"
// rule here - a metric with no events has no aggregate to compare, so that
// case surfaces as StateNoData rather than as a count rule (§18).
func defaultRules() []Rule {
	return []Rule{
		// Sustained load: the average over half a minute, and it has to
		// stay bad for a couple of evaluations before it's worth saying.
		{Name: "cpu-hot", Metric: "cpu.load", Stat: StatAvg, Op: OpGT, Value: 0.9,
			Window: 30 * time.Second, For: 20 * time.Second},
		// A single spike the average would smooth away. No For: a spike is
		// instantaneous by nature, so waiting for it to persist would mean
		// never reporting the thing this rule exists to catch.
		{Name: "cpu-spike", Metric: "cpu.load", Stat: StatMax, Op: OpGT, Value: 0.99},
		// Latency wants a percentile, not a max. A max rule fires on one
		// unlucky request; p99 fires when a real fraction of users are
		// affected, which is the thing worth waking someone for (§23).
		{Name: "slow-requests", Metric: "http.latency_ms", Stat: StatP99, Op: OpGT, Value: 500,
			For: 20 * time.Second},
	}
}

// panicBackoff is how long a supervised loop waits before restarting after
// a panic. Without it, a loop that panics before reaching its ticker would
// spin as fast as the CPU allows, turning one bug into a busy loop and a
// flood of identical log lines.
const panicBackoff = time.Second

// supervise runs fn until ctx is cancelled, restarting it if it panics,
// waiting `backoff` between restarts.
//
// A panic in a background goroutine is a different problem from a panic in a
// handler, and much worse. net/http recovers the latter: it logs, drops that
// one connection, and the server carries on. A bare goroutine has no such
// net, so a panic in the alerter or the sweeper takes the whole process with
// it - and §2 keeps every aggregate in memory, so process death is total
// data loss. Losing the sweeper degrades memory slowly and losing the
// alerter stops notifications; losing the process throws away the last
// minute of every metric instantly, which is the only thing the server
// actually holds.
//
// That asymmetry is why this restarts rather than exiting, and why it does
// not stop after N attempts. A loop that panics every tick logs every tick,
// which is noisy and impossible to miss - much better than stopping
// silently, because an alerter that has died looks exactly like one with
// nothing to report.
//
// The backoff is a parameter for the same reason Alerter.Run takes its
// interval and run() takes its listener: a test needs the behaviour, not the
// wait. Callers pass panicBackoff.
func supervise(ctx context.Context, name string, backoff time.Duration, fn func(context.Context)) {
	for ctx.Err() == nil {
		if !ranAndPanicked(ctx, name, backoff, fn) {
			return // returned on its own, which means ctx is done
		}

		// Wait out the backoff, but never past cancellation - run() waits
		// on these loops before the process exits, so sleeping through a
		// shutdown would stall it.
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

// ranAndPanicked calls fn and reports whether it panicked rather than
// returning. The stack is logged at the point of recovery, because by the
// time supervise sees the result it is gone.
func ranAndPanicked(ctx context.Context, name string, backoff time.Duration, fn func(context.Context)) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			log.Printf("PANIC in %s: %v\nrestarting in %s\n%s",
				name, r, backoff, debug.Stack())
		}
	}()

	fn(ctx)
	return false
}

// run serves on ln until ctx is cancelled, then drains in-flight requests
// (up to 5s) and returns. Split from main so a test can drive the whole
// lifecycle with a cancellable context instead of a real signal.
func run(ctx context.Context, ln net.Listener) error {
	store := newStore()

	// Bad rules are a programming error, so fail before serving a single
	// request rather than discovering it on the first tick.
	alerter, err := newAlerter(time.Now(), store, defaultRules())
	if err != nil {
		return fmt.Errorf("alert rules: %w", err)
	}

	// The alerter shares the server's shutdown signal. alertDone lets us
	// wait for it to actually stop, so the process never exits mid-evaluation.
	alertDone := make(chan struct{})
	go func() {
		defer close(alertDone)
		supervise(ctx, "alerter", panicBackoff, func(ctx context.Context) {
			alerter.Run(ctx, evalInterval)
		})
	}()

	// The sweeper reclaims metrics nobody writes to any more (§27). It
	// shares the same shutdown signal for the same reason the alerter does:
	// it holds shard locks, so the process must not exit mid-sweep.
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		supervise(ctx, "sweeper", panicBackoff, func(ctx context.Context) {
			store.Sweep(ctx, sweepInterval)
		})
	}()

	// Before serving anything, like the alert rules above: a limit the
	// operator got wrong should stop the process, not be silently ignored.
	limit, err := inFlightLimit()
	if err != nil {
		return err
	}
	log.Printf("serving at most %d requests at once", limit)

	lim, cls := newLimiter(limit), newClients()

	// The server records its own telemetry into its own store (§31), so
	// /stats, the alerting rules and retention all apply to it unchanged.
	reporter := newSelfReporter(store, lim, cls, &store.counts)
	selfDone := make(chan struct{})
	go func() {
		defer close(selfDone)
		supervise(ctx, "self-reporter", panicBackoff, func(ctx context.Context) {
			reporter.Run(ctx, selfInterval)
		})
	}()

	srv := newServer(routes(store, alerter, lim, cls))

	errc := make(chan error, 1)
	go func() {
		log.Printf("listening on %s", ln.Addr())
		errc <- srv.Serve(ln)
	}()

	select {
	case err := <-errc:
		return err // Serve fell over before we were asked to stop
	case <-ctx.Done():
		log.Println("shutting down, draining in-flight requests...")
	}

	// Give open requests up to 5s to finish before dropping them.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdownErr := srv.Shutdown(shutdownCtx)

	// Every background loop watches ctx, which is already cancelled, so
	// these return promptly.
	<-alertDone
	<-sweepDone
	<-selfDone
	return shutdownErr
}

func main() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	// ctx is cancelled on the first Ctrl-C / SIGTERM; stop() then restores
	// default handling so a second Ctrl-C kills immediately.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, ln); err != nil {
		log.Fatal(err)
	}
	log.Println("stopped cleanly")
}
