package main

import (
	"fmt"
	"io"
	"net/http"
)

func main() {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	fmt.Println("Listening on :8080")
	http.ListenAndServe(":8080", nil)
	
	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "could not read body", http.StatusBadRequest)
			return
		}
		fmt.Println("received:", string(body))
		fmt.Fprintln(w, "got it")
	})
}


