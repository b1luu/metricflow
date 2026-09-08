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
	"sync"          // Mutex, to guard the Store's aggs map
	"syscall"       // SIGTERM
	"time"
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

// Agg holds the running aggregates for one metric. Four small numbers,
// no matter how many events arrive - we store the conclusion, not the events.
type Agg struct {
	Count int     // how many events seen
	Sum   float64 // sum of all values (Sum/Count = average)
	Min   float64 // smallest value seen
	Max   float64 // largest value seen
}

// Store owns all metric state behind one mutex. Bundling the lock with the
// map it guards keeps "what's protected by what" obvious, and lets each test
// (and, later, each shard) hold its own independent state instead of sharing
// one global.
type Store struct {
	mu   sync.Mutex
	aggs map[string]map[int64]*Agg
}

// newStore returns a ready-to-use Store with its map initialized.
func newStore() *Store {
	return &Store{aggs: make(map[string]map[int64]*Agg)}
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

	s.mu.Lock()
	defer s.mu.Unlock()

	series := s.aggs[ev.Name]
	if series == nil {
		series = make(map[int64]*Agg)
		s.aggs[ev.Name] = series
	}

	a := series[bucket]
	if a == nil {
		a = &Agg{Min: ev.Value, Max: ev.Value}
		series[bucket] = a
	}

	a.Count++
	a.Sum += ev.Value
	if ev.Value < a.Min {
		a.Min = ev.Value
	}
	if ev.Value > a.Max {
		a.Max = ev.Value
	}

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
	var merged Agg
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
		first = false
	}
	return merged, !first
}

// aggFor merges one metric's buckets at or after cutoff, taking the lock
// itself. The alerter needs a per-metric read because each rule carries its
// own window (and so its own cutoff); handleStats keeps its own loop because
// it wants every metric at one cutoff under a single lock.
func (s *Store) aggFor(name string, cutoff int64) (Agg, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return mergeBuckets(s.aggs[name], cutoff)
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

	// Unmarshal only checks syntax, not meaning. Name and TS have no sane
	// zero-value default, so they are checked explicitly. See DESIGN.md §12.
	if ev.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	if ev.TS == 0 {
		http.Error(w, "ts is required (unix milliseconds)", http.StatusBadRequest)
		return
	}

	// ev.TS picks the event's time bucket (see record / DESIGN.md §16).
	// Too old -> its bucket is already evicted, nothing to add to.
	// Too far future -> it would sit in a bucket that stays visible for
	// however long the clock is wrong. Either way, reject rather than
	// silently mis-record; one bucketWidth of future slack covers normal
	// client/server clock skew.
	now := time.Now()
	ts := time.UnixMilli(ev.TS)
	switch {
	case ts.Before(now.Add(-window)):
		http.Error(w, fmt.Sprintf("event too old (ts before now-%s)", window), http.StatusBadRequest)
		return
	case ts.After(now.Add(bucketWidth)):
		http.Error(w, fmt.Sprintf("ts too far in the future (after now+%s)", bucketWidth), http.StatusBadRequest)
		return
	}

	s.record(now, ev)
	fmt.Fprintln(w, "got it")
}

// MetricStats is one metric's aggregate in a /stats response. Avg is
// derived (Sum/Count) and served raw; the caller rounds for display.
type MetricStats struct {
	Count int     `json:"count"`
	Avg   float64 `json:"avg"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
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

	s.mu.Lock()
	for name, series := range s.aggs {
		m, ok := mergeBuckets(series, cutoff)
		if !ok {
			continue // no data inside the window
		}
		resp.Metrics[name] = MetricStats{
			Count: m.Count,
			Avg:   m.Sum / float64(m.Count),
			Min:   m.Min,
			Max:   m.Max,
		}
	}
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	// Nothing useful to do if this fails: the client hung up mid-read, or
	// the socket broke. Status and headers are already sent. Ignore it.
	_ = json.NewEncoder(w).Encode(resp)
}

// routes builds the request multiplexer for a Store. Separate from main so
// tests can exercise the wiring (path -> handler, method gating) directly.
func routes(s *Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", allow(http.MethodGet, handleHealth))
	mux.HandleFunc("/ingest", allow(http.MethodPost, s.handleIngest))
	mux.HandleFunc("/stats", allow(http.MethodGet, s.handleStats))
	return mux
}

// defaultRules is this program's alert configuration. Rules live in code on
// purpose: a POST /rules CRUD surface would add a lot of endpoint and
// nothing to the aggregation story. Note there is no "metric went silent"
// rule here - a metric with no events has no aggregate to compare, so that
// case surfaces as StateNoData rather than as a count rule (§18).
func defaultRules() []Rule {
	return []Rule{
		// Sustained load: the average over half a minute.
		{Name: "cpu-hot", Metric: "cpu.load", Stat: StatAvg, Op: OpGT, Value: 0.9, Window: 30 * time.Second},
		// A single spike the average would smooth away.
		{Name: "cpu-spike", Metric: "cpu.load", Stat: StatMax, Op: OpGT, Value: 0.99},
		{Name: "slow-requests", Metric: "http.latency_ms", Stat: StatMax, Op: OpGT, Value: 500},
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

	srv := &http.Server{Handler: routes(store)}

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

	<-alertDone // ctx is already cancelled, so this returns promptly
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
