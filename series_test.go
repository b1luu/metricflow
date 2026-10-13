package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// getSeries calls handleSeries and decodes the response.
func getSeries(t *testing.T, s *Store, query string) SeriesResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleSeries(rec, httptest.NewRequest(http.MethodGet, "/series"+query, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("series %q: status = %d, want 200 (body %q)", query, rec.Code, rec.Body.String())
	}
	var resp SeriesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("series %q: invalid JSON: %v", query, err)
	}
	return resp
}

// recordAt puts a value in the bucket containing at.
func recordAt(t *testing.T, s *Store, name string, v float64, at time.Time) {
	t.Helper()
	if err := s.record(time.Now(), Event{Name: name, Value: v, TS: at.UnixMilli()}); err != nil {
		t.Fatal(err)
	}
}

// The reason this endpoint exists. Two metrics with the same average over
// the window, one steady and one spiky: /stats cannot tell them apart on
// avg, and the shape that distinguishes them was in the store all along.
func TestSeriesShowsShapeThatStatsFlattens(t *testing.T) {
	s := newStore()
	now := time.Now()

	for i := 0; i < numBuckets; i++ {
		at := now.Add(-time.Duration(i) * bucketWidth)
		recordAt(t, s, "steady", 0.4, at)
		spike := 0.0
		if i == 1 {
			spike = 2.4
		}
		recordAt(t, s, "spiky", spike, at)
	}

	merged := getStats(t, s, "?name=steady&name=spiky")
	if merged.Metrics["steady"].Avg != merged.Metrics["spiky"].Avg {
		t.Fatalf("setup: the averages differ (%v vs %v); the point is that they match",
			merged.Metrics["steady"].Avg, merged.Metrics["spiky"].Avg)
	}

	series := getSeries(t, s, "?name=steady&name=spiky")
	steady, spiky := series.Series["steady"], series.Series["spiky"]
	if len(steady) != numBuckets || len(spiky) != numBuckets {
		t.Fatalf("got %d and %d points, want %d each", len(steady), len(spiky), numBuckets)
	}

	var steadyMax, spikyMax float64
	for i := range steady {
		if steady[i].Avg > steadyMax {
			steadyMax = steady[i].Avg
		}
		if spiky[i].Avg > spikyMax {
			spikyMax = spiky[i].Avg
		}
	}
	if spikyMax <= steadyMax {
		t.Errorf("per-bucket peaks are %v and %v; the series did not separate them",
			spikyMax, steadyMax)
	}
}

// Points are ascending by start, because a series is read as a line and a
// Go map's iteration order is not one.
func TestSeriesPointsAreInTimeOrder(t *testing.T) {
	s := newStore()
	now := time.Now()
	for i := 0; i < numBuckets; i++ {
		recordAt(t, s, "m", float64(i+1), now.Add(-time.Duration(i)*bucketWidth))
	}

	points := getSeries(t, s, "?name=m").Series["m"]
	if len(points) < 2 {
		t.Fatalf("got %d points, want several", len(points))
	}
	for i := 1; i < len(points); i++ {
		if points[i].Start <= points[i-1].Start {
			t.Fatalf("point %d starts at %d, not after %d", i, points[i].Start, points[i-1].Start)
		}
	}
}

// The per-bucket numbers must account for exactly what the merged view
// reports. If they disagree, one of the two is lying about the same data.
func TestSeriesAccountsForEverythingStatsReports(t *testing.T) {
	s := newStore()
	now := time.Now()
	for i := 0; i < numBuckets; i++ {
		at := now.Add(-time.Duration(i) * bucketWidth)
		for v := 0; v < 15; v++ {
			recordAt(t, s, "m", float64((v*7)%23)+1, at)
		}
	}

	merged := getStats(t, s, "?name=m").Metrics["m"]
	points := getSeries(t, s, "?name=m").Series["m"]
	if len(points) == 0 {
		t.Fatal("no points")
	}

	total := 0
	min, max := points[0].Min, points[0].Max
	for _, p := range points {
		total += p.Count
		if p.Min < min {
			min = p.Min
		}
		if p.Max > max {
			max = p.Max
		}
	}
	if total != merged.Count {
		t.Errorf("buckets hold %d events, /stats reports %d", total, merged.Count)
	}
	if min != merged.Min || max != merged.Max {
		t.Errorf("bucket min/max = %v/%v, /stats = %v/%v", min, max, merged.Min, merged.Max)
	}
}

// Buckets outside the window are not returned, and a shorter ?window=
// narrows the series the same way it narrows /stats.
func TestSeriesRespectsTheWindow(t *testing.T) {
	s := newStore()
	now := time.Now()
	for i := 0; i < numBuckets; i++ {
		recordAt(t, s, "m", 1, now.Add(-time.Duration(i)*bucketWidth))
	}

	full := getSeries(t, s, "?name=m").Series["m"]
	narrow := getSeries(t, s, "?name=m&window=20s").Series["m"]

	if len(narrow) >= len(full) {
		t.Errorf("a 20s window returned %d points and the full window %d; "+
			"the window did not narrow anything", len(narrow), len(full))
	}
	if len(narrow) == 0 {
		t.Error("a 20s window returned no points at all")
	}
}

// A metric with nothing live is absent rather than an empty array: a gap in
// a chart is not a run of zeroes, and a client has to be able to tell.
func TestSeriesOmitsAMetricWithNoLiveData(t *testing.T) {
	s := newStore()
	recordNow(s, "present", 1)

	resp := getSeries(t, s, "?name=present&name=never.existed")
	if _, ok := resp.Series["never.existed"]; ok {
		t.Error("a metric that was never there came back as an empty series")
	}
	if len(resp.Series["present"]) == 0 {
		t.Error("the metric that does exist has no points")
	}
}

// ?name= is required. A series over a prefix scan would be the most
// expensive thing this server can be asked for - every matching metric,
// every bucket, a sort each - and nobody plots a thousand lines.
func TestSeriesRequiresNames(t *testing.T) {
	s := newStore()
	recordNow(s, "svc.one", 1)

	for _, raw := range []string{"", "?prefix=svc.", "?window=30s", "?name=a&prefix=b"} {
		rec := httptest.NewRecorder()
		s.handleSeries(rec, httptest.NewRequest(http.MethodGet, "/series"+raw, nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%q: status = %d, want 400", raw, rec.Code)
		}
	}
}

// The response says what resolution it is reporting at, so a caller plotting
// the points knows how wide each one is instead of assuming.
func TestSeriesReportsItsResolution(t *testing.T) {
	s := newStore()
	recordNow(s, "m", 1)

	resp := getSeries(t, s, "?name=m")
	if resp.Bucket != bucketWidth.String() {
		t.Errorf("bucket = %q, want %q", resp.Bucket, bucketWidth.String())
	}
	if resp.Window != window.String() {
		t.Errorf("window = %q, want %q", resp.Window, window.String())
	}
}

// The histogram handed out must be a copy, and this has to be checked on
// seriesFor rather than through the handler.
//
// Agg is copied by value, so Count/Sum/Min/Max are safe however it is
// written; the only shared thing is the *hist pointer. The handler computes
// quantiles immediately, so a test through it compares numbers already
// materialised and would pass even against a version that aliased - which
// the first attempt at this test did.
func TestSeriesForCopiesTheHistogram(t *testing.T) {
	s := newStore()
	now := time.Now()
	for v := 0; v < 20; v++ {
		recordAt(t, s, "m", 1, now)
	}

	got := s.seriesFor("m", 0)
	if len(got) != 1 {
		t.Fatalf("setup: got %d buckets, want 1", len(got))
	}
	before := got[0].agg.h.quantiles(0.99)[0]

	// Move the store a long way, in the same bucket.
	for v := 0; v < 500; v++ {
		recordAt(t, s, "m", 900, now)
	}

	after := got[0].agg.h.quantiles(0.99)[0]
	if before != after {
		t.Errorf("p99 of an already-returned bucket went from %v to %v; "+
			"the histogram aliases the store", before, after)
	}

	// And the store really did move, or nothing above was proven.
	if again := s.seriesFor("m", 0); again[0].agg.h.quantiles(0.99)[0] == before {
		t.Fatal("setup: the writes did not change the store")
	}
}

// The same property under the race detector, which is the form the aliasing
// bug would actually take: quantiles sort outside the shard lock, so a
// shared histogram is a write racing a sort rather than merely a stale
// number. CI runs this with -race (§22); this machine cannot.
func TestSeriesWhileTheStoreIsBeingWritten(t *testing.T) {
	s := newStore()
	now := time.Now()
	recordAt(t, s, "m", 1, now)

	done := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					_ = s.record(now, Event{Name: "m", Value: 7, TS: now.UnixMilli()})
				}
			}
		}()
	}

	for i := 0; i < 200; i++ {
		if pts := getSeries(t, s, "?name=m").Series["m"]; len(pts) == 0 {
			close(done)
			wg.Wait()
			t.Fatal("a series came back empty while the metric was being written")
		}
	}
	close(done)
	wg.Wait()
}

// Charged per bucket, because a bucket is a sort of its own - and with no
// walk, exactly as a named /stats earns (§37).
func TestSeriesIsChargedPerBucketAndNotForAWalk(t *testing.T) {
	s := storeWithMetrics(2000)
	now := time.Now()
	for i := 0; i < numBuckets; i++ {
		recordAt(t, s, "svc.metric.42", 1, now.Add(-time.Duration(i)*bucketWidth))
	}

	points := len(getSeries(t, s, "?name=svc.metric.42").Series["svc.metric.42"])
	if points == 0 {
		t.Fatal("setup: no points")
	}

	c := &client{}
	req := httptest.NewRequest(http.MethodGet, "/series?name=svc.metric.42", nil)
	req = req.WithContext(withClient(req.Context(), c, "tester"))
	rec := httptest.NewRecorder()
	s.handleSeries(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.querySpent != int64(points) {
		t.Errorf("charged %d for %d buckets; a series pays per bucket and nothing "+
			"for a walk it did not do", c.querySpent, points)
	}
}

// Method gating, like every other route (§13).
func TestSeriesRejectsNonGET(t *testing.T) {
	s := newStore()
	a, err := newAlerter(time.Now(), s, defaultRules())
	if err != nil {
		t.Fatal(err)
	}
	h := routes(s, a, newLimiter(maxInFlight), newClients())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/series?name=m", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /series: status = %d, want 405", rec.Code)
	}
}

// Every point must sit on the grid the response claims (§42).
//
// §39 added the "bucket" field so a client plotting these would not have to
// assume how wide a point is. That field is a promise, and a restored
// snapshot from a different bucketWidth used to break it: the series came
// back with points at irregular intervals, some off the grid entirely,
// under a header still claiming they were all the current width.
func TestEverySeriesPointSitsOnTheGridTheResponseClaims(t *testing.T) {
	s := newStore()
	now := time.Now()
	for i := 0; i < numBuckets; i++ {
		recordAt(t, s, "m", float64(i+1), now.Add(-time.Duration(i)*bucketWidth))
	}

	resp := getSeries(t, s, "?name=m")

	claimed, err := time.ParseDuration(resp.Bucket)
	if err != nil {
		t.Fatalf("the response claims a bucket width of %q, which is not a duration", resp.Bucket)
	}
	secs := int64(claimed / time.Second)
	if secs <= 0 {
		t.Fatalf("claimed bucket width %s is not usable", claimed)
	}

	points := resp.Series["m"]
	if len(points) == 0 {
		t.Fatal("no points")
	}
	for _, p := range points {
		if p.Start%secs != 0 {
			t.Errorf("point at %d is not a multiple of the %s the response claims",
				p.Start, claimed)
		}
	}
	// And consecutive points are exactly one claimed width apart, which is
	// the other half of what makes the field meaningful.
	for i := 1; i < len(points); i++ {
		if gap := points[i].Start - points[i-1].Start; gap != secs {
			t.Errorf("points %d and %d are %ds apart, not the %s claimed",
				i-1, i, gap, claimed)
		}
	}
}
