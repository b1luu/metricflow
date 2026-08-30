package main

import (
	"encoding/json" // decode JSON request bodies into Go values
	"fmt"           // formatted printing (stdout + http responses)
	"io"            // io.ReadAll, to slurp the request body
	"net/http"      // the HTTP server and routing
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

var totalEvents int

func main() {
	// GET /health - liveness check, just proves the server is up.
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	// POST /ingest - accept one metric event as a JSON body.
	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		// Read the whole request body into a []byte.
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "could not read body", http.StatusBadRequest)
			return
		}

		// Parse the raw bytes into a structured Event.
		// &ev passes the address so Unmarshal fills in *our* ev,
		// not a throwaway copy.
		var ev Event
		if err := json.Unmarshal(body, &ev); err != nil {
			// Only fires on malformed JSON. Missing fields are NOT an
			// error - they stay at their zero values (0, "").
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		// Prove the parse worked by printing the individual fields.
		fmt.Printf("parsed event: name=%s value=%.2f type=%s ts=%d\n",
			ev.Name, ev.Value, ev.Type, ev.TS)
		fmt.Fprintln(w, "got it")
		totalEvents++
	})
	http.HandleFunc("/count", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "total events: %d\n", totalEvents)
	})

	// Register routes above, THEN start the server - ListenAndServe
	// blocks forever, so anything after it would never run.
	fmt.Println("Listening on :8080")
	http.ListenAndServe(":8080", nil)
}
