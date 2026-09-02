package main

import (
	"encoding/json" // decode JSON request bodies into Go values
	"fmt"           // formatted printing (stdout + http responses)
	"io"            // io.ReadAll, to slurp the request body
	"net/http"      // the HTTP server and routing
	"sync"          // Mutex, to guard the shared aggs map
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

const (
	bucketWidth = 10 * time.Second // width of one time bucket
	numBuckets  = 6                // 6 * 10s = 60s window
)

// aggs maps metric name -> its Agg. *Agg (pointer) so we fetch the real
// struct and modify it in place; a value map would hand back a copy.
// mu guards aggs - HTTP handlers run concurrently on separate goroutines.
var (
	mu   sync.Mutex
	aggs = make(map[string]map[int64]*Agg)
)

// record folds one event into its metric's running aggregate.
// Pulled out of the HTTP handler so it can be unit-tested on its own.
func record(ev Event) {
	// Bucket by server receive time. Using ev.TS (event time) would mean
	// handling out-of-order, duplicate, and future-dated events — deferred
	// to the event-time milestone. See DESIGN.md §9.
	bucket := time.Now().Truncate(bucketWidth).Unix()

	mu.Lock()
	defer mu.Unlock()

	series := aggs[ev.Name]
	if series == nil {
		series = make(map[int64]*Agg)
		aggs[ev.Name] = series
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
}

// mergeBuckets folds a metric's per-bucket Aggs into one combined Agg.
// The bool is false when there's no data. Caller must hold mu.
func mergeBuckets(series map[int64]*Agg) (Agg, bool) {
	var merged Agg
	first := true
	for _, a := range series {
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

// handleHealth: GET /health - liveness check, proves the server is up.
func handleHealth(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "ok")
}

// handleIngest: POST /ingest - accept one metric event as a JSON body.
func handleIngest(w http.ResponseWriter, r *http.Request) {
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

	fmt.Printf("parsed event: name=%s value=%.2f type=%s ts=%d\n",
		ev.Name, ev.Value, ev.Type, ev.TS)

	record(ev)
	fmt.Fprintln(w, "got it")
}

// handleStats: GET /stats - per-metric aggregate: count, average, min, max.
func handleStats(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()

	for name, series := range aggs {
		m, ok := mergeBuckets(series)
		if !ok {
			continue
		}
		avg := m.Sum / float64(m.Count)
		fmt.Fprintf(w, "%s: count=%d avg=%.2f min=%.2f max=%.2f\n",
			name, m.Count, avg, m.Min, m.Max)
	}
}

func main() {
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/ingest", handleIngest)
	http.HandleFunc("/stats", handleStats)

	// Register routes above, THEN start the server - ListenAndServe
	// blocks forever, so anything after it would never run.
	fmt.Println("Listening on :8080")
	http.ListenAndServe(":8080", nil)
}
