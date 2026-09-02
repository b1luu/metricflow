package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// resetAggs wipes shared state so each test starts clean.
// Tests in the same package can touch package-level vars directly.
func resetAggs() {
	mu.Lock()
	defer mu.Unlock()
	aggs = make(map[string]map[int64]*Agg)
}

// --- unit test: the aggregation logic, no HTTP involved ---

func TestRecordAggregates(t *testing.T) {
	resetAggs()

	for _, v := range []float64{0.8, 0.9, 0.3} {
		record(Event{Name: "cpu.load", Value: v})
	}

	// Events may land in one or two buckets depending on timing;
	// mergeBuckets recombines them so the assertions hold either way.
	a, ok := mergeBuckets(aggs["cpu.load"])
	if !ok {
		t.Fatal("expected data for cpu.load, got none")
	}
	if a.Count != 3 {
		t.Errorf("Count = %d, want 3", a.Count)
	}
	if a.Sum != 2.0 {
		t.Errorf("Sum = %v, want 2.0", a.Sum)
	}
	if a.Min != 0.3 {
		t.Errorf("Min = %v, want 0.3", a.Min)
	}
	if a.Max != 0.9 {
		t.Errorf("Max = %v, want 0.9", a.Max)
	}
}

func TestRecordSeedsMinMaxFromFirstValue(t *testing.T) {
	resetAggs()

	// All values are positive; a buggy seed of 0 would make Min stay 0.
	record(Event{Name: "latency", Value: 5})
	record(Event{Name: "latency", Value: 8})

	a, ok := mergeBuckets(aggs["latency"])
	if !ok {
		t.Fatal("expected data for latency, got none")
	}
	if a.Min != 5 {
		t.Errorf("Min = %v, want 5 (seeded from first value, not 0)", a.Min)
	}
}

func TestRecordKeepsMetricsSeparate(t *testing.T) {
	resetAggs()

	record(Event{Name: "cpu.load", Value: 1})
	record(Event{Name: "memory.used", Value: 512})

	cpu, cpuOK := mergeBuckets(aggs["cpu.load"])
	mem, memOK := mergeBuckets(aggs["memory.used"])
	if !cpuOK || !memOK || cpu.Count != 1 || mem.Count != 1 {
		t.Errorf("metrics bled into each other: %+v", aggs)
	}
}

// --- handler tests: exercise the HTTP layer with httptest ---

func TestIngestHandlerValid(t *testing.T) {
	resetAggs()

	body := `{"name":"cpu.load","value":0.8,"type":"gauge","ts":1735000000123}`
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handleIngest(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	a, ok := mergeBuckets(aggs["cpu.load"])
	if !ok || a.Count != 1 {
		t.Errorf("event was not recorded: %+v", aggs)
	}
}

func TestIngestHandlerInvalidJSON(t *testing.T) {
	resetAggs()

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"name":`))
	rec := httptest.NewRecorder()

	handleIngest(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if len(aggs) != 0 {
		t.Errorf("nothing should have been recorded, got %+v", aggs)
	}
}

func TestStatsHandlerOutput(t *testing.T) {
	resetAggs()
	record(Event{Name: "cpu.load", Value: 0.8})
	record(Event{Name: "cpu.load", Value: 0.4})

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec := httptest.NewRecorder()

	handleStats(rec, req)

	out := rec.Body.String()
	want := "cpu.load: count=2 avg=0.60 min=0.40 max=0.80"
	if !strings.Contains(out, want) {
		t.Errorf("stats output = %q, want it to contain %q", out, want)
	}
}

// --- health ---

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != "ok" {
		t.Errorf("body = %q, want %q", got, "ok")
	}
}

// --- documented edge behaviors ---

// A body missing fields is NOT rejected (DESIGN.md §7) - the absent fields
// stay at their zero values and the event still records.
func TestRecordMissingFieldsUsesZeroValues(t *testing.T) {
	resetAggs()

	body := `{"name":"cpu.load"}` // no value, ts, or type
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handleIngest(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (missing fields are allowed)", rec.Code)
	}
	a, ok := mergeBuckets(aggs["cpu.load"])
	if !ok || a.Count != 1 || a.Sum != 0 {
		t.Errorf("got %+v, want Count=1 Sum=0 (value defaulted to 0)", a)
	}
}

// Min/Max must track real extremes even when every value is negative -
// a seed of 0 would leave Max wrongly at 0 (DESIGN.md §4).
func TestRecordAllNegativeValues(t *testing.T) {
	resetAggs()

	for _, v := range []float64{-5, -2, -9} {
		record(Event{Name: "temp.delta", Value: v})
	}

	a, ok := mergeBuckets(aggs["temp.delta"])
	if !ok {
		t.Fatal("expected data for temp.delta, got none")
	}
	if a.Min != -9 || a.Max != -2 {
		t.Errorf("Min/Max = %v/%v, want -9/-2", a.Min, a.Max)
	}
}

// --- mergeBuckets in isolation ---

func TestMergeBucketsEmpty(t *testing.T) {
	if _, ok := mergeBuckets(nil); ok {
		t.Error("mergeBuckets(nil) ok = true, want false")
	}
	if _, ok := mergeBuckets(map[int64]*Agg{}); ok {
		t.Error("mergeBuckets(empty) ok = true, want false")
	}
}

// The real point of Change 1: an aggregate spread across several buckets
// must recombine correctly. Buckets are injected directly so the test
// doesn't depend on wall-clock timing.
func TestMergeBucketsAcrossBuckets(t *testing.T) {
	series := map[int64]*Agg{
		100: {Count: 2, Sum: 3, Min: 1, Max: 2},
		110: {Count: 1, Sum: 10, Min: 10, Max: 10},
		120: {Count: 3, Sum: -6, Min: -4, Max: 0},
	}

	m, ok := mergeBuckets(series)
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if m.Count != 6 {
		t.Errorf("Count = %d, want 6", m.Count)
	}
	if m.Sum != 7 {
		t.Errorf("Sum = %v, want 7", m.Sum)
	}
	if m.Min != -4 {
		t.Errorf("Min = %v, want -4 (smallest across all buckets)", m.Min)
	}
	if m.Max != 10 {
		t.Errorf("Max = %v, want 10 (largest across all buckets)", m.Max)
	}
}

// --- concurrency: the project's core claim ---

// N goroutines hammering record() on the same metric must not lose a
// single increment. Without the mutex this either miscounts (lost a.Count++)
// or panics on concurrent map writes; with it, the total is exact.
// Run with `go test -race` for true data-race detection.
func TestRecordConcurrent(t *testing.T) {
	resetAggs()

	const goroutines, perGoroutine = 50, 200

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				record(Event{Name: "cpu.load", Value: 1})
			}
		}()
	}
	wg.Wait()

	want := goroutines * perGoroutine
	m, ok := mergeBuckets(aggs["cpu.load"])
	if !ok || m.Count != want {
		t.Errorf("Count = %d, want %d (lost updates - mutex not protecting?)", m.Count, want)
	}
}
