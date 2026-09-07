package main

import (
	"context"       // shutdown deadline + signal-cancelled context
	"encoding/json" // decode JSON request bodies into Go values
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
)

// record folds one event into its metric's running aggregate. now is the
// operation's timestamp, passed in (not read here) so one call uses one
// consistent clock reading and tests can drive it directly.
// Using ev.TS (event time) instead would mean handling out-of-order,
// duplicate, and future-dated events — deferred to the event-time
// milestone. See DESIGN.md §9.
func (s *Store) record(now time.Time, ev Event) {
	bucket := now.Truncate(bucketWidth).Unix()

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
	// Read the whole request body into a []byte.
	body, err := io.ReadAll(r.Body)
	if err != nil {
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

	// TS drives which time bucket the event lands in (once §16 wiring is
	// done). An event older than the retention window can't be bucketed -
	// its bucket is already gone - so reject it now rather than silently
	// dropping it later. See DESIGN.md §16.
	now := time.Now()
	if time.UnixMilli(ev.TS).Before(now.Add(-window)) {
		http.Error(w, fmt.Sprintf("event too old (ts outside the %s window)", window), http.StatusBadRequest)
		return
	}

	fmt.Printf("parsed event: name=%s value=%.2f type=%s ts=%d\n",
		ev.Name, ev.Value, ev.Type, ev.TS)

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
	json.NewEncoder(w).Encode(resp)
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

// run serves on ln until ctx is cancelled, then drains in-flight requests
// (up to 5s) and returns. Split from main so a test can drive the whole
// lifecycle with a cancellable context instead of a real signal.
func run(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{Handler: routes(newStore())}

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
	return srv.Shutdown(shutdownCtx)
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
