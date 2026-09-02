package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
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
