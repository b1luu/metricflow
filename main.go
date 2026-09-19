package main

import (
	"context"       // shutdown deadline + signal-cancelled context
	"encoding/json" // decode JSON request bodies into Go values
	"errors"        // errors.As, to classify a read failure
	"fmt"           // formatted printing (stdout + http responses)
	"io"            // io.ReadAll, to slurp the request body
	"log"           // server lifecycle messages
	"net"           // net.Listen, so run() can be handed a test listener
	"net/http"      // the HTTP server and routing
	"os"            // os.Interrupt
	"os/signal"     // catch Ctrl-C / SIGTERM
	"sync"          // Mutex, to guard each shard's aggs map
	"syscall"       // SIGTERM
	"time"
	"unsafe" // Sizeof, a compile-time constant, for cache-line padding
)

// Event is the in-memory form of one metric observation.
// The `json:"..."` struct tags map lowercase wire keys to these
// capitalized (exported) fields, which is what json.Unmarshal needs
// to be able to write into them.
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
func (s *Store) record(now time.Time, ev Event) {
	bucket := time.UnixMilli(ev.TS).Truncate(bucketWidth).Unix()

	// One metric lives in exactly one shard, so an ingest takes one lock and
	// never blocks a write to a differently-named metric.
	sh := s.shardFor(ev.Name)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	series := sh.aggs[ev.Name]
	if series == nil {
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
	// metric that goes silent keeps its last buckets until it resumes -
	// acceptable, see DESIGN.md §10.
	evict(series, windowStart(now, window))
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

	// Parse the raw bytes into a structured Event.
	// &ev passes the address so Unmarshal fills in *our* ev, not a copy.
	var ev Event
	if err := json.Unmarshal(body, &ev); err != nil {
		// Only fires on malformed JSON. Missing fields are NOT an
		// error - they stay at their zero values (0, "").
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	now := time.Now()
	if err := validateEvent(now, ev); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.record(now, ev)
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
type StatsResponse struct {
	Window  string                 `json:"window"`
	Metrics map[string]MetricStats `json:"metrics"`
}

// handleStats: GET /stats - per-metric aggregate over a time window, as JSON.
// Optional ?window=30s (Go duration syntax); defaults to the full
// retention window, and may not exceed it (older data is already evicted).
func (s *Store) handleStats(w http.ResponseWriter, r *http.Request) {
	win := window
	if q := r.URL.Query().Get("window"); q != "" {
		d, err := time.ParseDuration(q)
		if err != nil || d <= 0 {
			http.Error(w, "invalid window (use e.g. 30s, 1m)", http.StatusBadRequest)
			return
		}
		if d > window {
			http.Error(w, fmt.Sprintf("window exceeds retention (%s)", window), http.StatusBadRequest)
			return
		}
		win = d
	}

	cutoff := windowStart(time.Now(), win)
	resp := StatsResponse{Window: win.String(), Metrics: map[string]MetricStats{}}

	// One shard at a time, rather than one lock over everything.
	//
	// This is the trade sharding makes: the response is no longer a single
	// instant across all metrics - a fast metric in shard 0 may be read a
	// few microseconds before one in shard 31, and a write can land between
	// them. Each metric's own numbers remain internally consistent, which is
	// what a metrics query actually needs; a globally atomic snapshot would
	// mean holding every shard lock at once and handing ingest back the
	// exact stall this slice removes (§24).
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for name, series := range sh.aggs {
			m, ok := mergeBuckets(series, cutoff)
			if !ok {
				continue // no data inside the window
			}
			// One pass for all three: asking separately would sort this
			// metric's bucket keys three times.
			p := m.h.quantiles(0.50, 0.90, 0.99)

			resp.Metrics[name] = MetricStats{
				Count: m.Count,
				Avg:   m.Sum / float64(m.Count),
				Min:   m.Min,
				Max:   m.Max,
				P50:   p[0],
				P90:   p[1],
				P99:   p[2],
			}
		}
		sh.mu.Unlock()
	}

	w.Header().Set("Content-Type", "application/json")
	// Nothing useful to do if this fails: the client hung up mid-read, or
	// the socket broke. Status and headers are already sent. Ignore it.
	_ = json.NewEncoder(w).Encode(resp)
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
func routes(s *Store, a *Alerter, l *limiter) http.Handler {
	mux := http.NewServeMux()

	// Shed-able: real work, and the traffic a firehose actually consists of.
	limited := map[string]http.HandlerFunc{
		"/ingest":       allow(http.MethodPost, s.handleIngest),
		"/ingest/batch": allow(http.MethodPost, s.handleIngestBatch),
		"/stats":        allow(http.MethodGet, s.handleStats),
		"/alerts":       allow(http.MethodGet, a.handleAlerts),
	}
	for path, h := range limited {
		mux.Handle(path, l.limit(h))
	}

	mux.HandleFunc("/health", allow(http.MethodGet, handleHealth))
	return mux
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
		alerter.Run(ctx, evalInterval)
	}()

	// The sweeper reclaims metrics nobody writes to any more (§27). It
	// shares the same shutdown signal for the same reason the alerter does:
	// it holds shard locks, so the process must not exit mid-sweep.
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		store.Sweep(ctx, sweepInterval)
	}()

	// Before serving anything, like the alert rules above: a limit the
	// operator got wrong should stop the process, not be silently ignored.
	limit, err := inFlightLimit()
	if err != nil {
		return err
	}
	log.Printf("serving at most %d requests at once", limit)

	srv := newServer(routes(store, alerter, newLimiter(limit)))

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

	// Both background loops watch ctx, which is already cancelled, so these
	// return promptly.
	<-alertDone
	<-sweepDone
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
