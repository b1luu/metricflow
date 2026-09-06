package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"
)

// resetAggs wipes shared state so each test starts clean.
// Tests in the same package can touch package-level vars directly.
func resetAggs() {
	mu.Lock()
	defer mu.Unlock()
	aggs = make(map[string]map[int64]*Agg)
}

// mergeAll folds every bucket for a metric, ignoring the time window.
// Used by tests that check record()'s output regardless of wall-clock timing.
func mergeAll(name string) (Agg, bool) {
	return mergeBuckets(aggs[name], 0)
}

// seedBucket injects a bucket straight into a metric's series, so a test
// can place data at a chosen age without waiting on the clock.
func seedBucket(name string, key int64, a *Agg) {
	mu.Lock()
	defer mu.Unlock()
	if aggs[name] == nil {
		aggs[name] = map[int64]*Agg{}
	}
	aggs[name][key] = a
}

// bucketAt returns the bucket key for "d ago" (d >= 0).
func bucketAt(d time.Duration) int64 {
	return time.Now().Add(-d).Truncate(bucketWidth).Unix()
}

// --- unit test: the aggregation logic, no HTTP involved ---

func TestRecordAggregates(t *testing.T) {
	resetAggs()

	for _, v := range []float64{0.8, 0.9, 0.3} {
		record(Event{Name: "cpu.load", Value: v})
	}

	// Events may land in one or two buckets depending on timing;
	// mergeBuckets recombines them so the assertions hold either way.
	a, ok := mergeAll("cpu.load")
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

	a, ok := mergeAll("latency")
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

	cpu, cpuOK := mergeAll("cpu.load")
	mem, memOK := mergeAll("memory.used")
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
	a, ok := mergeAll("cpu.load")
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

// The io.ReadAll error branch: a request body that fails mid-read.
func TestIngestHandlerBodyReadError(t *testing.T) {
	resetAggs()

	body := iotest.ErrReader(errors.New("connection reset"))
	req := httptest.NewRequest(http.MethodPost, "/ingest", body)
	rec := httptest.NewRecorder()

	handleIngest(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if len(aggs) != 0 {
		t.Errorf("nothing should have been recorded, got %+v", aggs)
	}
}

// An empty body is not valid JSON ("unexpected end of JSON input").
func TestIngestHandlerEmptyBody(t *testing.T) {
	resetAggs()

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(""))
	rec := httptest.NewRecorder()

	handleIngest(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

// "null" is valid JSON and unmarshals to a zero Event - which the name
// check must then reject, not the JSON check.
func TestIngestHandlerNullBody(t *testing.T) {
	resetAggs()

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader("null"))
	rec := httptest.NewRecorder()

	handleIngest(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "name is required") {
		t.Errorf("body = %q, want the name-required error (not the JSON error)", rec.Body.String())
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

// A valid ?window= is accepted and (for freshly recorded events) returns
// the same data as the default window.
func TestStatsHandlerAcceptsWindowParam(t *testing.T) {
	resetAggs()
	record(Event{Name: "cpu.load", Value: 1})

	req := httptest.NewRequest(http.MethodGet, "/stats?window=30s", nil)
	rec := httptest.NewRecorder()

	handleStats(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "cpu.load: count=1") {
		t.Errorf("body = %q, want it to contain cpu.load count=1", rec.Body.String())
	}
}

func TestStatsHandlerRejectsBadWindow(t *testing.T) {
	resetAggs()

	for _, q := range []string{"window=abc", "window=0s", "window=-5s", "window=10m"} {
		req := httptest.NewRequest(http.MethodGet, "/stats?"+q, nil)
		rec := httptest.NewRecorder()

		handleStats(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", q, rec.Code)
		}
	}
}

// --- windowing through the handler (deterministic, no sleeping) ---

// A bucket older than the default window must not show up in /stats,
// even though it's still physically in the map until the next write evicts it.
func TestStatsHandlerExcludesStaleBuckets(t *testing.T) {
	resetAggs()
	seedBucket("cpu.load", bucketAt(0), &Agg{Count: 2, Sum: 4, Min: 2, Max: 2})
	seedBucket("cpu.load", bucketAt(2*time.Minute), &Agg{Count: 9, Sum: 900, Min: 100, Max: 100})

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec := httptest.NewRecorder()
	handleStats(rec, req)

	want := "cpu.load: count=2 avg=2.00 min=2.00 max=2.00"
	if !strings.Contains(rec.Body.String(), want) {
		t.Errorf("stale bucket leaked into stats: %q, want %q", rec.Body.String(), want)
	}
}

// A shorter ?window= must actually narrow the result, dropping buckets
// that the default window would have included.
func TestStatsHandlerWindowParamNarrows(t *testing.T) {
	resetAggs()
	seedBucket("m", bucketAt(0), &Agg{Count: 1, Sum: 1, Min: 1, Max: 1})
	seedBucket("m", bucketAt(30*time.Second), &Agg{Count: 1, Sum: 5, Min: 5, Max: 5})

	get := func(query string) string {
		req := httptest.NewRequest(http.MethodGet, "/stats"+query, nil)
		rec := httptest.NewRecorder()
		handleStats(rec, req)
		return rec.Body.String()
	}

	if got := get(""); !strings.Contains(got, "m: count=2") {
		t.Errorf("default window: got %q, want count=2", got)
	}
	if got := get("?window=15s"); !strings.Contains(got, "m: count=1 avg=1.00") {
		t.Errorf("15s window: got %q, want count=1 (30s-old bucket excluded)", got)
	}
}

// --- /stats shape: empty and multi-metric ---

func TestStatsHandlerEmpty(t *testing.T) {
	resetAggs()

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec := httptest.NewRecorder()
	handleStats(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
}

func TestStatsHandlerMultipleMetrics(t *testing.T) {
	resetAggs()
	record(Event{Name: "cpu.load", Value: 1})
	record(Event{Name: "memory.used", Value: 512})

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec := httptest.NewRecorder()
	handleStats(rec, req)

	out := rec.Body.String()
	// Map iteration order is random, so check for each line rather than
	// a fixed full-body string.
	for _, want := range []string{"cpu.load: count=1", "memory.used: count=1"} {
		if !strings.Contains(out, want) {
			t.Errorf("output %q missing %q", out, want)
		}
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

// A body missing non-Name fields is NOT rejected (DESIGN.md §7) - the
// absent fields stay at their zero values and the event still records.
func TestRecordMissingFieldsUsesZeroValues(t *testing.T) {
	resetAggs()

	body := `{"name":"cpu.load"}` // no value, ts, or type
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handleIngest(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (missing fields are allowed)", rec.Code)
	}
	a, ok := mergeAll("cpu.load")
	if !ok || a.Count != 1 || a.Sum != 0 {
		t.Errorf("got %+v, want Count=1 Sum=0 (value defaulted to 0)", a)
	}
}

// Name is the one field with no sane zero-value default (DESIGN.md §12) -
// an event with no name, or an explicitly empty one, is rejected.
func TestIngestHandlerRejectsMissingName(t *testing.T) {
	resetAggs()

	for _, body := range []string{`{"value":1}`, `{"name":"","value":1}`} {
		req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
		rec := httptest.NewRecorder()

		handleIngest(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400", body, rec.Code)
		}
	}
	if len(aggs) != 0 {
		t.Errorf("nothing should have been recorded, got %+v", aggs)
	}
}

// Min/Max must track real extremes even when every value is negative -
// a seed of 0 would leave Max wrongly at 0 (DESIGN.md §4).
func TestRecordAllNegativeValues(t *testing.T) {
	resetAggs()

	for _, v := range []float64{-5, -2, -9} {
		record(Event{Name: "temp.delta", Value: v})
	}

	a, ok := mergeAll("temp.delta")
	if !ok {
		t.Fatal("expected data for temp.delta, got none")
	}
	if a.Min != -9 || a.Max != -2 {
		t.Errorf("Min/Max = %v/%v, want -9/-2", a.Min, a.Max)
	}
}

// --- mergeBuckets in isolation ---

func TestMergeBucketsEmpty(t *testing.T) {
	if _, ok := mergeBuckets(nil, 0); ok {
		t.Error("mergeBuckets(nil) ok = true, want false")
	}
	if _, ok := mergeBuckets(map[int64]*Agg{}, 0); ok {
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

	m, ok := mergeBuckets(series, 0) // cutoff 0 -> every bucket
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

// Change 2: buckets older than the cutoff are excluded from the merge.
func TestMergeBucketsRespectsCutoff(t *testing.T) {
	series := map[int64]*Agg{
		100: {Count: 5, Sum: 50, Min: 1, Max: 20}, // before cutoff - ignored
		110: {Count: 2, Sum: 6, Min: 2, Max: 4},
		120: {Count: 1, Sum: 9, Min: 9, Max: 9},
	}

	m, ok := mergeBuckets(series, 110)
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if m.Count != 3 {
		t.Errorf("Count = %d, want 3 (bucket 100 excluded)", m.Count)
	}
	if m.Sum != 15 {
		t.Errorf("Sum = %v, want 15", m.Sum)
	}
	if m.Min != 2 || m.Max != 9 {
		t.Errorf("Min/Max = %v/%v, want 2/9 (bucket 100's 1 and 20 excluded)", m.Min, m.Max)
	}
}

// When every bucket is older than the cutoff, the metric has no data
// in the window and ok is false.
func TestMergeBucketsAllStale(t *testing.T) {
	series := map[int64]*Agg{
		100: {Count: 5, Sum: 50, Min: 1, Max: 20},
		110: {Count: 2, Sum: 6, Min: 2, Max: 4},
	}
	if _, ok := mergeBuckets(series, 200); ok {
		t.Error("ok = true, want false (all buckets stale)")
	}
}

// --- eviction (Change 3) ---

// evict removes buckets older than cutoff and leaves the rest untouched.
// A bucket exactly at the cutoff is kept (the test uses < , not <=).
func TestEvict(t *testing.T) {
	series := map[int64]*Agg{
		100: {Count: 1}, // older than cutoff - dropped
		109: {Count: 1}, // older than cutoff - dropped
		110: {Count: 1}, // exactly at cutoff - kept
		120: {Count: 1}, // newer - kept
	}

	evict(series, 110)

	if _, ok := series[100]; ok {
		t.Error("bucket 100 still present, should be evicted")
	}
	if _, ok := series[109]; ok {
		t.Error("bucket 109 still present, should be evicted")
	}
	if _, ok := series[110]; !ok {
		t.Error("bucket 110 evicted, should be kept (at cutoff)")
	}
	if _, ok := series[120]; !ok {
		t.Error("bucket 120 evicted, should be kept")
	}
}

// record() itself evicts aged-out buckets for the metric it touches.
// An ancient bucket is injected, then one live event is recorded; the
// ancient bucket should be gone and only the current one should remain.
func TestRecordEvictsStaleBuckets(t *testing.T) {
	resetAggs()

	mu.Lock()
	aggs["cpu.load"] = map[int64]*Agg{
		1000: {Count: 99, Sum: 99, Min: 1, Max: 1}, // ancient
	}
	mu.Unlock()

	record(Event{Name: "cpu.load", Value: 0.5})

	mu.Lock()
	defer mu.Unlock()
	if _, ok := aggs["cpu.load"][1000]; ok {
		t.Error("ancient bucket 1000 survived a record() call")
	}
	if got := len(aggs["cpu.load"]); got != 1 {
		t.Errorf("series has %d buckets, want 1 (just the current one)", got)
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
	m, ok := mergeAll("cpu.load")
	if !ok || m.Count != want {
		t.Errorf("Count = %d, want %d (lost updates - mutex not protecting?)", m.Count, want)
	}
}

// Readers (handleStats) running against writers (record) must not panic.
// Go's runtime detects concurrent map iteration + write and crashes the
// process even without -race, so this fails hard if handleStats drops the
// lock. The final count must still be exact.
func TestRecordAndStatsConcurrent(t *testing.T) {
	resetAggs()

	const writers, perWriter, readers, perReader = 20, 100, 5, 200

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				record(Event{Name: "cpu.load", Value: 1})
			}
		}()
	}
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perReader; j++ {
				rec := httptest.NewRecorder()
				handleStats(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
			}
		}()
	}
	wg.Wait()

	want := writers * perWriter
	m, ok := mergeAll("cpu.load")
	if !ok || m.Count != want {
		t.Errorf("Count = %d, want %d", m.Count, want)
	}
}

// --- small helpers, edge cases ---

// windowStart must return a bucketWidth-aligned key no older than
// (now - d - one bucket).
func TestWindowStartAligned(t *testing.T) {
	d := 30 * time.Second
	got := windowStart(d)

	widthSec := int64(bucketWidth / time.Second)
	if got%widthSec != 0 {
		t.Errorf("windowStart(%s) = %d, not aligned to %ds", d, got, widthSec)
	}

	oldest := time.Now().Add(-d - bucketWidth).Unix()
	newest := time.Now().Unix()
	if got < oldest || got > newest {
		t.Errorf("windowStart(%s) = %d, outside [%d, %d]", d, got, oldest, newest)
	}
}

// evict on a nil or empty series is a no-op, not a panic.
func TestEvictEmpty(t *testing.T) {
	evict(nil, 100)
	evict(map[int64]*Agg{}, 100)
}

// The average in /stats output is rounded to 2 decimals by %.2f.
func TestStatsHandlerAverageRounding(t *testing.T) {
	resetAggs()
	for _, v := range []float64{1, 2, 2} { // sum 5 / 3 = 1.666...
		record(Event{Name: "cpu.load", Value: v})
	}

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec := httptest.NewRecorder()
	handleStats(rec, req)

	if !strings.Contains(rec.Body.String(), "avg=1.67") {
		t.Errorf("body = %q, want avg=1.67 (5/3 rounded)", rec.Body.String())
	}
}

// --- method enforcement ---

// A wrong method gets 405 plus the Allow header the HTTP spec requires.
func TestAllowRejectsWrongMethod(t *testing.T) {
	cases := []struct {
		name          string
		handler       http.HandlerFunc
		allowed, sent string
	}{
		{"health", handleHealth, http.MethodGet, http.MethodPost},
		{"ingest", handleIngest, http.MethodPost, http.MethodGet},
		{"stats", handleStats, http.MethodGet, http.MethodDelete},
	}

	for _, c := range cases {
		h := allow(c.allowed, c.handler)
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(c.sent, "/"+c.name, nil))

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: status = %d, want 405", c.name, rec.Code)
		}
		if got := rec.Header().Get("Allow"); got != c.allowed {
			t.Errorf("%s: Allow = %q, want %q", c.name, got, c.allowed)
		}
	}
}

// The allowed method passes straight through to the wrapped handler.
func TestAllowPassesThroughCorrectMethod(t *testing.T) {
	called := false
	h := allow(http.MethodGet, func(http.ResponseWriter, *http.Request) { called = true })

	h(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))

	if !called {
		t.Error("wrapped handler was not called for the allowed method")
	}
}
