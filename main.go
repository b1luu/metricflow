package main

import (
	"encoding/json" // decode JSON request bodies into Go values
	"fmt"           // formatted printing (stdout + http responses)
	"io"            // io.ReadAll, to slurp the request body
	"net/http"      // the HTTP server and routing
	"sync"          // Mutex, to guard the shared aggs map
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

// aggs maps metric name -> its Agg. *Agg (pointer) so we fetch the real
// struct and modify it in place; a value map would hand back a copy.
// mu guards aggs - HTTP handlers run concurrently on separate goroutines.
var (
	mu   sync.Mutex
	aggs = make(map[string]*Agg)
)

// record folds one event into its metric's running aggregate.
// Pulled out of the HTTP handler so it can be unit-tested on its own.
func record(ev Event) {
	mu.Lock()
	defer mu.Unlock()

	a := aggs[ev.Name]
	if a == nil {
		// First sighting of this metric: create its Agg and seed
		// Min/Max with this value so the first comparison is correct.
		// (Leaving them at 0 would break Min for positive-only metrics
		// and Max for all-negative ones.)
		a = &Agg{Min: ev.Value, Max: ev.Value}
		aggs[ev.Name] = a
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

	for name, a := range aggs {
		// Count is int, Sum is float64; Go won't divide across types,
		// so convert Count explicitly.
		avg := a.Sum / float64(a.Count)
		fmt.Fprintf(w, "%s: count=%d avg=%.2f min=%.2f max=%.2f\n",
			name, a.Count, avg, a.Min, a.Max)
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
