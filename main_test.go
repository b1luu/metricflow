package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"
)

// mergeAll folds every bucket for a metric, ignoring the time window.
// Used by tests that check record()'s output regardless of wall-clock timing.
func mergeAll(s *Store, name string) (Agg, bool) {
	return mergeBuckets(s.aggs[name], 0)
}

// seedBucket injects a bucket straight into a metric's series, so a test
// can place data at a chosen age without waiting on the clock.
func seedBucket(s *Store, name string, key int64, a *Agg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.aggs[name] == nil {
		s.aggs[name] = map[int64]*Agg{}
	}
	s.aggs[name][key] = a
}

// bucketAt returns the bucket key for "d ago" (d >= 0).
func bucketAt(d time.Duration) int64 {
	return time.Now().Add(-d).Truncate(bucketWidth).Unix()
}

// getStats calls s.handleStats with the given query ("" or "?window=30s")
// and decodes the JSON response, failing the test on a non-200 or bad JSON.
func getStats(t *testing.T, s *Store, query string) StatsResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleStats(rec, httptest.NewRequest(http.MethodGet, "/stats"+query, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("stats %q: status = %d, want 200", query, rec.Code)
	}
	var resp StatsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("stats %q: invalid JSON: %v (body %q)", query, err, rec.Body.String())
	}
	return resp
}

// --- unit test: the aggregation logic, no HTTP involved ---

func TestRecordAggregates(t *testing.T) {
	s := newStore()

	for _, v := range []float64{0.8, 0.9, 0.3} {
		s.record(time.Now(), Event{Name: "cpu.load", Value: v})
	}

	// Events may land in one or two buckets depending on timing;
	// mergeBuckets recombines them so the assertions hold either way.
	a, ok := mergeAll(s, "cpu.load")
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
	s := newStore()

	// All values are positive; a buggy seed of 0 would make Min stay 0.
	s.record(time.Now(), Event{Name: "latency", Value: 5})
	s.record(time.Now(), Event{Name: "latency", Value: 8})

	a, ok := mergeAll(s, "latency")
	if !ok {
		t.Fatal("expected data for latency, got none")
	}
	if a.Min != 5 {
		t.Errorf("Min = %v, want 5 (seeded from first value, not 0)", a.Min)
	}
}

func TestRecordKeepsMetricsSeparate(t *testing.T) {
	s := newStore()

	s.record(time.Now(), Event{Name: "cpu.load", Value: 1})
	s.record(time.Now(), Event{Name: "memory.used", Value: 512})

	cpu, cpuOK := mergeAll(s, "cpu.load")
	mem, memOK := mergeAll(s, "memory.used")
	if !cpuOK || !memOK || cpu.Count != 1 || mem.Count != 1 {
		t.Errorf("metrics bled into each other: %+v", s.aggs)
	}
}

// --- handler tests: exercise the HTTP layer with httptest ---

func TestIngestHandlerValid(t *testing.T) {
	s := newStore()

	body := `{"name":"cpu.load","value":0.8,"type":"gauge","ts":1735000000123}`
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleIngest(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	a, ok := mergeAll(s, "cpu.load")
	if !ok || a.Count != 1 {
		t.Errorf("event was not recorded: %+v", s.aggs)
	}
}

func TestIngestHandlerInvalidJSON(t *testing.T) {
	s := newStore()

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`{"name":`))
	rec := httptest.NewRecorder()

	s.handleIngest(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if len(s.aggs) != 0 {
		t.Errorf("nothing should have been recorded, got %+v", s.aggs)
	}
}

// The io.ReadAll error branch: a request body that fails mid-read.
func TestIngestHandlerBodyReadError(t *testing.T) {
	s := newStore()

	body := iotest.ErrReader(errors.New("connection reset"))
	req := httptest.NewRequest(http.MethodPost, "/ingest", body)
	rec := httptest.NewRecorder()

	s.handleIngest(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if len(s.aggs) != 0 {
		t.Errorf("nothing should have been recorded, got %+v", s.aggs)
	}
}

// An empty body is not valid JSON ("unexpected end of JSON input").
func TestIngestHandlerEmptyBody(t *testing.T) {
	s := newStore()

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(""))
	rec := httptest.NewRecorder()

	s.handleIngest(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

// "null" is valid JSON and unmarshals to a zero Event - which the name
// check must then reject, not the JSON check.
func TestIngestHandlerNullBody(t *testing.T) {
	s := newStore()

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader("null"))
	rec := httptest.NewRecorder()

	s.handleIngest(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "name is required") {
		t.Errorf("body = %q, want the name-required error (not the JSON error)", rec.Body.String())
	}
}

func TestStatsHandlerOutput(t *testing.T) {
	s := newStore()
	s.record(time.Now(), Event{Name: "cpu.load", Value: 0.8})
	s.record(time.Now(), Event{Name: "cpu.load", Value: 0.4})

	m := getStats(t, s, "").Metrics["cpu.load"]
	if m.Count != 2 || m.Min != 0.4 || m.Max != 0.8 {
		t.Errorf("got %+v, want count=2 min=0.4 max=0.8", m)
	}
	if d := m.Avg - 0.6; d < -1e-9 || d > 1e-9 {
		t.Errorf("avg = %v, want ~0.6", m.Avg)
	}
}

func TestStatsHandlerContentType(t *testing.T) {
	s := newStore()
	rec := httptest.NewRecorder()
	s.handleStats(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

// A valid ?window= is accepted, echoed back, and returns the metric.
func TestStatsHandlerAcceptsWindowParam(t *testing.T) {
	s := newStore()
	s.record(time.Now(), Event{Name: "cpu.load", Value: 1})

	resp := getStats(t, s, "?window=30s")
	if resp.Window != "30s" {
		t.Errorf("Window = %q, want %q", resp.Window, "30s")
	}
	if resp.Metrics["cpu.load"].Count != 1 {
		t.Errorf("cpu.load count = %d, want 1", resp.Metrics["cpu.load"].Count)
	}
}

func TestStatsHandlerRejectsBadWindow(t *testing.T) {
	s := newStore()

	for _, q := range []string{"window=abc", "window=0s", "window=-5s", "window=10m"} {
		req := httptest.NewRequest(http.MethodGet, "/stats?"+q, nil)
		rec := httptest.NewRecorder()

		s.handleStats(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", q, rec.Code)
		}
	}
}

// --- windowing through the handler (deterministic, no sleeping) ---

// A bucket older than the default window must not show up in /stats,
// even though it's still physically in the map until the next write evicts it.
func TestStatsHandlerExcludesStaleBuckets(t *testing.T) {
	s := newStore()
	seedBucket(s, "cpu.load", bucketAt(0), &Agg{Count: 2, Sum: 4, Min: 2, Max: 2})
	seedBucket(s, "cpu.load", bucketAt(2*time.Minute), &Agg{Count: 9, Sum: 900, Min: 100, Max: 100})

	m := getStats(t, s, "").Metrics["cpu.load"]
	if m.Count != 2 || m.Avg != 2 || m.Min != 2 || m.Max != 2 {
		t.Errorf("stale bucket leaked: got %+v, want count=2 avg/min/max=2", m)
	}
}

// A metric whose every bucket is out of the window (not yet evicted,
// since eviction is on-write) is omitted from /stats entirely.
func TestStatsHandlerOmitsFullyStaleMetric(t *testing.T) {
	s := newStore()
	seedBucket(s, "cpu.load", bucketAt(3*time.Minute), &Agg{Count: 9, Sum: 900, Min: 100, Max: 100})

	resp := getStats(t, s, "")
	if _, present := resp.Metrics["cpu.load"]; present {
		t.Errorf("fully-stale metric appeared in stats: %+v", resp.Metrics)
	}
}

// A shorter ?window= must actually narrow the result, dropping buckets
// that the default window would have included.
func TestStatsHandlerWindowParamNarrows(t *testing.T) {
	s := newStore()
	seedBucket(s, "m", bucketAt(0), &Agg{Count: 1, Sum: 1, Min: 1, Max: 1})
	seedBucket(s, "m", bucketAt(30*time.Second), &Agg{Count: 1, Sum: 5, Min: 5, Max: 5})

	if got := getStats(t, s, "").Metrics["m"].Count; got != 2 {
		t.Errorf("default window: count = %d, want 2", got)
	}
	narrowed := getStats(t, s, "?window=15s").Metrics["m"]
	if narrowed.Count != 1 || narrowed.Avg != 1 {
		t.Errorf("15s window: got %+v, want count=1 avg=1 (30s-old bucket excluded)", narrowed)
	}
}

// --- /stats shape: empty and multi-metric ---

func TestStatsHandlerEmpty(t *testing.T) {
	s := newStore()

	resp := getStats(t, s, "")
	if len(resp.Metrics) != 0 {
		t.Errorf("Metrics = %+v, want empty", resp.Metrics)
	}
}

func TestStatsHandlerMultipleMetrics(t *testing.T) {
	s := newStore()
	s.record(time.Now(), Event{Name: "cpu.load", Value: 1})
	s.record(time.Now(), Event{Name: "memory.used", Value: 512})

	m := getStats(t, s, "").Metrics
	if m["cpu.load"].Count != 1 || m["memory.used"].Count != 1 {
		t.Errorf("got %+v, want both metrics at count=1", m)
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
	s := newStore()

	body := `{"name":"cpu.load"}` // no value, ts, or type
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleIngest(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (missing fields are allowed)", rec.Code)
	}
	a, ok := mergeAll(s, "cpu.load")
	if !ok || a.Count != 1 || a.Sum != 0 {
		t.Errorf("got %+v, want Count=1 Sum=0 (value defaulted to 0)", a)
	}
}

// Name is the one field with no sane zero-value default (DESIGN.md §12) -
// an event with no name, or an explicitly empty one, is rejected.
func TestIngestHandlerRejectsMissingName(t *testing.T) {
	s := newStore()

	for _, body := range []string{`{"value":1}`, `{"name":"","value":1}`} {
		req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
		rec := httptest.NewRecorder()

		s.handleIngest(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400", body, rec.Code)
		}
	}
	if len(s.aggs) != 0 {
		t.Errorf("nothing should have been recorded, got %+v", s.aggs)
	}
}

// Min/Max must track real extremes even when every value is negative -
// a seed of 0 would leave Max wrongly at 0 (DESIGN.md §4).
func TestRecordAllNegativeValues(t *testing.T) {
	s := newStore()

	for _, v := range []float64{-5, -2, -9} {
		s.record(time.Now(), Event{Name: "temp.delta", Value: v})
	}

	a, ok := mergeAll(s, "temp.delta")
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
	s := newStore()

	s.mu.Lock()
	s.aggs["cpu.load"] = map[int64]*Agg{
		1000: {Count: 99, Sum: 99, Min: 1, Max: 1}, // ancient
	}
	s.mu.Unlock()

	s.record(time.Now(), Event{Name: "cpu.load", Value: 0.5})

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.aggs["cpu.load"][1000]; ok {
		t.Error("ancient bucket 1000 survived a record() call")
	}
	if got := len(s.aggs["cpu.load"]); got != 1 {
		t.Errorf("series has %d buckets, want 1 (just the current one)", got)
	}
}

// The end-to-end windowing story, driven by a synthetic clock: as time
// advances, older events roll out of the query window, and a write
// physically evicts the buckets that have aged past retention.
// record() and windowStart() take `now` as a parameter, so this needs no
// real sleeping and no global clock.
func TestWindowRollsAsClockAdvances(t *testing.T) {
	s := newStore()
	base := time.Now().Truncate(bucketWidth)

	countAt := func(now time.Time) (int, bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		m, ok := mergeBuckets(s.aggs["cpu.load"], windowStart(now, window))
		return m.Count, ok
	}

	// t=0: two events land in the base bucket.
	s.record(base, Event{Name: "cpu.load", Value: 1})
	s.record(base, Event{Name: "cpu.load", Value: 3})
	if c, ok := countAt(base); !ok || c != 2 {
		t.Fatalf("t=0: count=%d ok=%v, want 2", c, ok)
	}

	// t=30s: a third event, new bucket, all three still in the 60s window.
	t30 := base.Add(30 * time.Second)
	s.record(t30, Event{Name: "cpu.load", Value: 5})
	if c, _ := countAt(t30); c != 3 {
		t.Errorf("t=30s: count=%d, want 3", c)
	}

	// t=75s: the base bucket (starts at t=0) is now past the 60s window
	// once the cutoff is truncated to a bucket boundary -> excluded from
	// the query, even though no write has evicted it yet.
	t75 := base.Add(75 * time.Second)
	if c, ok := countAt(t75); !ok || c != 1 {
		t.Errorf("t=75s: count=%d ok=%v, want 1 (only the t=30s event)", c, ok)
	}

	// A write at t=75s evicts the stale base bucket from the map.
	s.record(t75, Event{Name: "cpu.load", Value: 9})
	s.mu.Lock()
	_, stale := s.aggs["cpu.load"][base.Unix()]
	s.mu.Unlock()
	if stale {
		t.Error("t=75s write did not evict the aged-out base bucket")
	}

	// t=150s: every remaining bucket is older than 60s -> no data.
	if _, ok := countAt(base.Add(150 * time.Second)); ok {
		t.Error("t=150s: ok=true, want false (all data aged out)")
	}
}

// --- concurrency: the project's core claim ---

// N goroutines hammering record() on the same metric must not lose a
// single increment. Without the mutex this either miscounts (lost a.Count++)
// or panics on concurrent map writes; with it, the total is exact.
// Run with `go test -race` for true data-race detection.
func TestRecordConcurrent(t *testing.T) {
	s := newStore()

	const goroutines, perGoroutine = 50, 200

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				s.record(time.Now(), Event{Name: "cpu.load", Value: 1})
			}
		}()
	}
	wg.Wait()

	want := goroutines * perGoroutine
	m, ok := mergeAll(s, "cpu.load")
	if !ok || m.Count != want {
		t.Errorf("Count = %d, want %d (lost updates - mutex not protecting?)", m.Count, want)
	}
}

// Readers (handleStats) running against writers (record) must not panic.
// Go's runtime detects concurrent map iteration + write and crashes the
// process even without -race, so this fails hard if handleStats drops the
// lock. The final count must still be exact.
func TestRecordAndStatsConcurrent(t *testing.T) {
	s := newStore()

	const writers, perWriter, readers, perReader = 20, 100, 5, 200

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				s.record(time.Now(), Event{Name: "cpu.load", Value: 1})
			}
		}()
	}
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perReader; j++ {
				rec := httptest.NewRecorder()
				s.handleStats(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
			}
		}()
	}
	wg.Wait()

	want := writers * perWriter
	m, ok := mergeAll(s, "cpu.load")
	if !ok || m.Count != want {
		t.Errorf("Count = %d, want %d", m.Count, want)
	}
}

// --- small helpers, edge cases ---

// windowStart must return a bucketWidth-aligned key exactly d back from
// now (rounded down to a bucket boundary).
func TestWindowStartAligned(t *testing.T) {
	now := time.Now()
	d := 30 * time.Second
	got := windowStart(now, d)

	widthSec := int64(bucketWidth / time.Second)
	if got%widthSec != 0 {
		t.Errorf("windowStart = %d, not aligned to %ds", got, widthSec)
	}
	if want := now.Add(-d).Truncate(bucketWidth).Unix(); got != want {
		t.Errorf("windowStart = %d, want %d", got, want)
	}
}

// evict on a nil or empty series is a no-op, not a panic.
func TestEvictEmpty(t *testing.T) {
	evict(nil, 100)
	evict(map[int64]*Agg{}, 100)
}

// Avg is served raw (exact Sum/Count), not rounded - the caller formats it.
func TestStatsHandlerAvgIsRaw(t *testing.T) {
	s := newStore()
	for _, v := range []float64{1, 2, 2} { // sum 5 / 3 = 1.666...
		s.record(time.Now(), Event{Name: "cpu.load", Value: v})
	}

	avg := getStats(t, s, "").Metrics["cpu.load"].Avg
	if want := 5.0 / 3.0; avg != want {
		t.Errorf("avg = %v, want %v (exact, unrounded)", avg, want)
	}
}

// --- method enforcement ---

// A wrong method gets 405 plus the Allow header the HTTP spec requires.
func TestAllowRejectsWrongMethod(t *testing.T) {
	s := newStore()
	cases := []struct {
		name          string
		handler       http.HandlerFunc
		allowed, sent string
	}{
		{"health", handleHealth, http.MethodGet, http.MethodPost},
		{"ingest", s.handleIngest, http.MethodPost, http.MethodGet},
		{"stats", s.handleStats, http.MethodGet, http.MethodDelete},
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

// --- server wiring & shutdown ---

// routes() maps each path to its handler with the right method gate.
func TestRoutesWireHandlers(t *testing.T) {
	h := routes(newStore())

	cases := []struct {
		method, path string
		wantStatus   int
	}{
		{http.MethodGet, "/health", http.StatusOK},
		{http.MethodGet, "/stats", http.StatusOK},
		{http.MethodPost, "/ingest", http.StatusBadRequest}, // empty body, but it reached the handler
		{http.MethodGet, "/ingest", http.StatusMethodNotAllowed},
		{http.MethodPost, "/stats", http.StatusMethodNotAllowed},
		{http.MethodGet, "/nope", http.StatusNotFound},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(c.method, c.path, nil))
		if rec.Code != c.wantStatus {
			t.Errorf("%s %s: status = %d, want %d", c.method, c.path, rec.Code, c.wantStatus)
		}
	}
}

// run serves while its context is live and returns cleanly once cancelled -
// the whole lifecycle main() drives on a signal.
func TestRunServesThenStopsOnCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, ln) }()

	// It's actually serving.
	resp, err := http.Get("http://" + ln.Addr().String() + "/health")
	if err != nil {
		t.Fatalf("server not reachable: %v", err)
	}
	resp.Body.Close()

	cancel() // simulate the signal

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run returned %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return after context cancel")
	}
}
