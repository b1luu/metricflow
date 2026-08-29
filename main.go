package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Event struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	TS    int64   `json:"ts"`
	Type  string  `json:"type"`
}

func main() {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "could not read body", http.StatusBadRequest)
			return
		}
		var ev Event
		if err := json.Unmarshal(body, &ev); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		fmt.Printf("parsed event: name=%s value=%.2f type=%s ts=%d\n",
			ev.Name, ev.Value, ev.Type, ev.TS)
		fmt.Fprintln(w, "got it")
	})

	fmt.Println("Listening on :8080")
	http.ListenAndServe(":8080", nil)
}
