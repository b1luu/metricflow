package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseFlagsDefaults(t *testing.T) {
	c, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.target == "" || c.workers < 1 || c.duration <= 0 || c.metrics < 1 {
		t.Errorf("defaults are not runnable: %+v", c)
	}
	if !c.verify {
		t.Error("verify should default on: the cross-check is the point")
	}
}

func TestParseFlagsOverrides(t *testing.T) {
	c, err := parseFlags([]string{
		"-target", "http://example:9999",
		"-workers", "32",
		"-duration", "2s",
		"-metrics", "7",
		"-bad", "0.25",
		"-batch", "50",
		"-verify=false",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := config{
		target: "http://example:9999", workers: 32,
		duration: 2 * time.Second, metrics: 7, badFrac: 0.25, batch: 50, verify: false,
	}
	if c != want {
		t.Errorf("got %+v, want %+v", c, want)
	}
}

func TestParseFlagsRejectsNonsense(t *testing.T) {
	cases := []struct {
		name, wantMsg string
		args          []string
	}{
		{"no workers", "workers", []string{"-workers", "0"}},
		{"zero duration", "duration", []string{"-duration", "0"}},
		{"no metrics", "metrics", []string{"-metrics", "0"}},
		{"bad fraction over one", "bad", []string{"-bad", "1.5"}},
		{"negative bad fraction", "bad", []string{"-bad", "-0.1"}},
		{"empty target", "target", []string{"-target", ""}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseFlags(c.args)
			if err == nil {
				t.Fatalf("parseFlags(%v) = nil error, want one", c.args)
			}
			if !strings.Contains(err.Error(), c.wantMsg) {
				t.Errorf("error = %q, want it to mention %q", err, c.wantMsg)
			}
		})
	}
}

// --- sending ---

func TestSendUntilHitsARealServer(t *testing.T) {
	var got atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ingest" || r.Method != http.MethodPost {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"ts"`) {
			t.Errorf("body missing ts: %s", body)
		}
		got.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config{target: srv.URL, workers: 1, duration: 50 * time.Millisecond, metrics: 1}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	tal := sendUntil(ctx, newClient(1), cfg, 0)

	if tal.byStatus[http.StatusOK] == 0 {
		t.Fatalf("no successful requests: %+v", tal)
	}
	if tal.failed != 0 {
		t.Errorf("failed = %d, want 0", tal.failed)
	}
	if int64(tal.byStatus[http.StatusOK]) != got.Load() {
		t.Errorf("client counted %d, server saw %d", tal.byStatus[http.StatusOK], got.Load())
	}
}

func TestRunLoadStopsAtDuration(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config{target: srv.URL, workers: 4, duration: 50 * time.Millisecond, metrics: 2}

	start := time.Now()
	total, _ := runLoad(context.Background(), cfg)
	elapsed := time.Since(start)

	if total.sent() == 0 {
		t.Fatal("no requests sent")
	}
	if elapsed > 5*time.Second {
		t.Errorf("runLoad took %s, want it bounded by the duration", elapsed)
	}
}

// A tally is merged from every worker; the totals must add up.
func TestTallyMerge(t *testing.T) {
	a := newTally()
	a.byStatus[200] = 3
	a.failed = 1

	b := newTally()
	b.byStatus[200] = 2
	b.byStatus[400] = 5

	a.merge(b)

	if a.byStatus[200] != 5 || a.byStatus[400] != 5 || a.failed != 1 {
		t.Errorf("merged = %+v", a)
	}
	if a.sent() != 11 { // 5 + 5 + 1 failed
		t.Errorf("sent() = %d, want 11", a.sent())
	}
}

func TestTallyReport(t *testing.T) {
	tal := newTally()
	tal.byStatus[400] = 25
	tal.byStatus[200] = 75
	tal.good = 75 // the 400s were meant to be rejected: no mismatch
	tal.failed = 0
	tal.lat.add(5 * time.Millisecond)

	var buf bytes.Buffer
	tal.report(&buf, 2*time.Second, 0)
	out := buf.String()

	for _, want := range []string{
		"100 requests", // total
		"2s",           // elapsed
		"50 req/s",     // 100 / 2s
		"200",          // status lines present
		"75.0%",
		"400",
		"25.0%",
		"p50", "p99", "max", // percentiles actually reach the output
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report missing %q:\n%s", want, out)
		}
	}

	// Codes are sorted so two runs diff cleanly; map order would not be.
	if strings.Index(out, "200") > strings.Index(out, "400") {
		t.Errorf("status codes not in ascending order:\n%s", out)
	}
	// No transport errors, so no err line.
	if strings.Contains(out, "no response") {
		t.Errorf("reported an err line with failed=0:\n%s", out)
	}
}

func TestTallyReportShowsTransportFailures(t *testing.T) {
	tal := newTally()
	tal.byStatus[200] = 1
	tal.failed = 3

	var buf bytes.Buffer
	tal.report(&buf, time.Second, 0)

	if !strings.Contains(buf.String(), "no response") {
		t.Errorf("failures not reported:\n%s", buf.String())
	}
}

// An empty run must not divide by zero.
func TestTallyReportEmpty(t *testing.T) {
	var buf bytes.Buffer
	newTally().report(&buf, 0, 0)
	if buf.Len() == 0 {
		t.Error("report wrote nothing")
	}
}

// --- latency histogram ---

func TestLatencyQuantiles(t *testing.T) {
	l := newLatencies()
	// 100 samples: 1µs..100µs, one each. The q-th percentile is then q*100 µs.
	for i := 1; i <= 100; i++ {
		l.add(time.Duration(i) * time.Microsecond)
	}

	cases := []struct {
		q    float64
		want time.Duration
	}{
		{0.50, 51 * time.Microsecond},
		{0.90, 91 * time.Microsecond},
		{0.99, 100 * time.Microsecond},
	}
	for _, c := range cases {
		if got := l.quantile(c.q); got != c.want {
			t.Errorf("quantile(%.2f) = %s, want %s", c.q, got, c.want)
		}
	}
	if l.max != 100*time.Microsecond {
		t.Errorf("max = %s, want 100µs", l.max)
	}
	if l.count != 100 {
		t.Errorf("count = %d, want 100", l.count)
	}
}

// Samples past the cap are counted, bounded at the cap by quantile, but must
// still show their true size through max - a long tail is never hidden.
func TestLatencyOverCap(t *testing.T) {
	l := newLatencies()
	l.add(time.Millisecond)
	l.add(latencyCap + time.Second)

	if l.over != 1 {
		t.Errorf("over = %d, want 1", l.over)
	}
	if got := l.quantile(0.99); got != latencyCap {
		t.Errorf("quantile past the cap = %s, want %s", got, latencyCap)
	}
	if want := latencyCap + time.Second; l.max != want {
		t.Errorf("max = %s, want %s (the tail must stay visible)", l.max, want)
	}
}

func TestLatencyMergeAndEmpty(t *testing.T) {
	if got := newLatencies().quantile(0.5); got != 0 {
		t.Errorf("empty quantile = %s, want 0", got)
	}

	a, b := newLatencies(), newLatencies()
	a.add(10 * time.Microsecond)
	b.add(20 * time.Microsecond)
	b.add(latencyCap * 2)
	a.merge(b)

	if a.count != 3 || a.over != 1 {
		t.Errorf("after merge count=%d over=%d, want 3 and 1", a.count, a.over)
	}
	if a.max != latencyCap*2 {
		t.Errorf("merged max = %s, want %s", a.max, latencyCap*2)
	}
}

// Recording a sample must not allocate: an allocation inside the request
// loop would add jitter to the thing being measured.
func TestLatencyAddDoesNotAllocate(t *testing.T) {
	l := newLatencies()
	got := testing.AllocsPerRun(1000, func() { l.add(42 * time.Microsecond) })
	if got != 0 {
		t.Errorf("add allocated %v times per call, want 0", got)
	}
}

// A valid request refused, or a broken one accepted, must be shouted about
// rather than buried in the status table.
func TestTallyReportFlagsMismatch(t *testing.T) {
	tal := newTally()
	tal.byStatus[200] = 9
	tal.good = 10 // one valid request was refused
	tal.lat.add(time.Millisecond)

	var buf bytes.Buffer
	tal.report(&buf, time.Second, 0)

	if !strings.Contains(buf.String(), "MISMATCH") {
		t.Errorf("mismatch not reported:\n%s", buf.String())
	}
}

// --- bad-request injection ---

// The injected failures must stay genuinely distinct. If two collapsed into
// the same shape, -bad would exercise fewer of the server's reject branches
// than it claims to - a silent weakening of the whole point of the flag.
func TestBadBodiesAreDistinctShapes(t *testing.T) {
	now := time.Now()
	seen := map[string]int{}

	for i, build := range badBodies {
		body := build("cpu.load", now)
		if body == "" {
			t.Errorf("badBodies[%d] produced an empty body", i)
		}
		if prev, dup := seen[body]; dup {
			t.Errorf("badBodies[%d] duplicates [%d]: %.50s", i, prev, body)
		}
		seen[body] = i
	}

	// One per reject branch the server documents: malformed JSON, no name,
	// no ts, ts too old, ts too far future, body too large.
	if len(badBodies) != 6 {
		t.Errorf("%d bad shapes, want 6 - one per server reject branch", len(badBodies))
	}
}

// The bad fraction is a deterministic quota, so a run is reproducible and
// two runs with the same flags are comparable.
func TestBadFractionIsHonoured(t *testing.T) {
	var mu sync.Mutex
	var total, rejected int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		total++
		// Our valid bodies always carry all three fields.
		if !strings.Contains(string(body), `"name"`) ||
			!strings.Contains(string(body), `"ts"`) ||
			len(body) > 4096 {
			rejected++
			mu.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config{target: srv.URL, workers: 1, duration: 100 * time.Millisecond,
		metrics: 1, badFrac: 0.5}
	tal, _ := runLoad(context.Background(), cfg)

	if tal.sent() < 10 {
		t.Fatalf("only %d requests; too few to judge the ratio", tal.sent())
	}
	frac := float64(tal.sent()-tal.good) / float64(tal.sent())
	if frac < 0.4 || frac > 0.6 {
		t.Errorf("bad fraction = %.2f, want ~0.50 (sent %d, good %d)", frac, tal.sent(), tal.good)
	}
}

// --- end-to-end verification ---

// statsServer is a stand-in for MetricFlow's /stats, reporting whatever
// counts the test dictates.
func statsServer(t *testing.T, window string, counts map[string]int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		type m struct {
			Count int `json:"count"`
		}
		body := struct {
			Window  string       `json:"window"`
			Metrics map[string]m `json:"metrics"`
		}{Window: window, Metrics: map[string]m{}}
		for name, c := range counts {
			body.Metrics[name] = m{Count: c}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}))
}

func TestVerifyAgreesWhenCountsMatch(t *testing.T) {
	cfg := config{metrics: 2, runID: "abc", duration: time.Second}
	srv := statsServer(t, "1m0s", map[string]int{
		cfg.metricName(0): 60,
		cfg.metricName(1): 40,
	})
	defer srv.Close()
	cfg.target = srv.URL

	tal := newTally()
	tal.accepted = 100

	var buf bytes.Buffer
	if err := verify(newClient(1), cfg, tal, time.Second, &buf); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !strings.Contains(buf.String(), "OK") {
		t.Errorf("output = %q, want an OK line", buf.String())
	}
}

// The whole reason this exists: a server that drops an accepted event must
// be caught, and caught as an error so CI can gate on it.
func TestVerifyCatchesADroppedEvent(t *testing.T) {
	cfg := config{metrics: 1, runID: "abc", duration: time.Second}
	srv := statsServer(t, "1m0s", map[string]int{cfg.metricName(0): 99})
	defer srv.Close()
	cfg.target = srv.URL

	tal := newTally()
	tal.accepted = 100 // the server said it took 100 events

	err := verify(newClient(1), cfg, tal, time.Second, io.Discard)
	if err == nil {
		t.Fatal("verify accepted a 99/100 mismatch")
	}
	if !strings.Contains(err.Error(), "100") || !strings.Contains(err.Error(), "99") {
		t.Errorf("error = %q, want both counts named", err)
	}
}

// Only this run's metrics are counted. A warm server holding an earlier
// run's data must not inflate the total.
func TestVerifyIgnoresOtherMetrics(t *testing.T) {
	cfg := config{metrics: 1, runID: "run2", duration: time.Second}
	srv := statsServer(t, "1m0s", map[string]int{
		cfg.metricName(0): 10,
		"metric.run1.0":   9999, // a previous run, still inside the window
		"something.else":  5,
	})
	defer srv.Close()
	cfg.target = srv.URL

	tal := newTally()
	tal.accepted = 10

	if err := verify(newClient(1), cfg, tal, time.Second, io.Discard); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// A run at least as long as the server's window has already had its earliest
// events evicted. Asserting equality then would be the harness lying, so it
// declines to judge - and says why.
func TestVerifySkipsWhenRunOutlastsWindow(t *testing.T) {
	cfg := config{metrics: 1, runID: "abc"}
	srv := statsServer(t, "1m0s", map[string]int{cfg.metricName(0): 1})
	defer srv.Close()
	cfg.target = srv.URL

	tal := newTally()
	tal.accepted = 999999 // wildly different, and that's fine

	var buf bytes.Buffer
	if err := verify(newClient(1), cfg, tal, 2*time.Minute, &buf); err != nil {
		t.Fatalf("verify should decline, not fail: %v", err)
	}
	if !strings.Contains(buf.String(), "skipped") {
		t.Errorf("output = %q, want it to say it skipped and why", buf.String())
	}
}

func TestVerifyReportsAnUnreachableServer(t *testing.T) {
	cfg := config{target: "http://127.0.0.1:1", metrics: 1, duration: time.Second}
	err := verify(newClient(1), cfg, newTally(), time.Second, io.Discard)
	if err == nil {
		t.Fatal("verify succeeded against a dead server")
	}
}

// --- clock honesty ---

// The report must not state a figure the clock could not have resolved.
func TestShowRefusesUnresolvableFigures(t *testing.T) {
	const res = 500 * time.Microsecond

	if got := show(0, res); got != "<500µs" {
		t.Errorf("show(0, %s) = %q, want %q - 0s would be a claim, not a measurement", res, got, "<500µs")
	}
	if got := show(100*time.Microsecond, res); got != "<500µs" {
		t.Errorf("show(100µs, %s) = %q, want it flagged as unresolvable", res, got)
	}
	// At or above the resolution the figure is real and printed as-is.
	if got := show(2*time.Millisecond, res); got != "2ms" {
		t.Errorf("show(2ms, %s) = %q, want %q", res, got, "2ms")
	}
	// With a perfect clock, nothing is suppressed.
	if got := show(0, 0); got != "0s" {
		t.Errorf("show(0, 0) = %q, want %q", got, "0s")
	}
}

func TestReportStatesClockResolution(t *testing.T) {
	tal := newTally()
	tal.byStatus[200] = 1
	tal.good = 1
	tal.lat.add(0) // finished inside a tick

	var buf bytes.Buffer
	tal.report(&buf, time.Second, 500*time.Microsecond)
	out := buf.String()

	if !strings.Contains(out, "clock resolution") {
		t.Errorf("report hides its own resolution:\n%s", out)
	}
	if strings.Contains(out, "p50 0s") {
		t.Errorf("report claims an unresolvable p50 of 0s:\n%s", out)
	}
}

// The probe must return something plausible rather than hanging or zero.
func TestClockResolutionIsMeasurable(t *testing.T) {
	got := clockResolution()
	if got <= 0 {
		t.Fatalf("clockResolution() = %s, want a positive tick", got)
	}
	if got > 100*time.Millisecond {
		t.Errorf("clockResolution() = %s, implausibly coarse", got)
	}
	t.Logf("clock resolution on this machine: %s", got)
}

// --- batch mode ---

func TestParseFlagsRejectsABatchBelowOne(t *testing.T) {
	if _, err := parseFlags([]string{"-batch", "0"}); err == nil {
		t.Error("parseFlags accepted -batch 0")
	}
	if _, err := parseFlags([]string{"-batch", "-5"}); err == nil {
		t.Error("parseFlags accepted a negative batch")
	}
}

// In batch mode the generator posts NDJSON to a different endpoint and takes
// the accepted count from the reply rather than from the status code. Both
// halves matter: a 200 no longer means "one event accepted", so counting
// statuses would compare a request count against an event count later on.
func TestSendUntilBatchesToTheBatchEndpoint(t *testing.T) {
	const batch = 10

	var (
		events   atomic.Int64
		requests atomic.Int64
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ingest/batch" || r.Method != http.MethodPost {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)

		// Count the events the way the real server does - one JSON value
		// per line - so a generator that sent one blob would be caught.
		n := 0
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			if strings.TrimSpace(line) != "" {
				n++
			}
		}
		if n != batch {
			t.Errorf("request carried %d events, want %d", n, batch)
		}
		events.Add(int64(n))
		requests.Add(1)

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"accepted":%d,"rejected":0,"errors":[]}`, n)
	}))
	defer srv.Close()

	cfg := config{
		target: srv.URL, workers: 1, duration: 50 * time.Millisecond,
		metrics: 1, batch: batch,
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	tal := sendUntil(ctx, newClient(1), cfg, 0)

	if requests.Load() == 0 {
		t.Fatalf("no requests reached the server: %+v", tal)
	}
	if int64(tal.events) != events.Load() {
		t.Errorf("client counted %d events, server saw %d", tal.events, events.Load())
	}
	if int64(tal.accepted) != events.Load() {
		t.Errorf("accepted = %d, want the %d the replies reported", tal.accepted, events.Load())
	}
	// The requests/events distinction is the whole point of the mode.
	if tal.events != tal.byStatus[http.StatusOK]*batch {
		t.Errorf("events = %d, want %d requests x %d", tal.events, tal.byStatus[http.StatusOK], batch)
	}
}

// A partially-rejected batch is still a 200, so the accepted count has to
// come from the body. Counting 200s would credit the server with events it
// explicitly refused.
func TestSendUntilReadsPartialRejectsFromTheBody(t *testing.T) {
	const (
		batch    = 10
		accepted = 6
	)

	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"accepted":%d,"rejected":%d,"errors":[]}`, accepted, batch-accepted)
	}))
	defer srv.Close()

	cfg := config{
		target: srv.URL, workers: 1, duration: 50 * time.Millisecond,
		metrics: 1, batch: batch,
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	tal := sendUntil(ctx, newClient(1), cfg, 0)

	n := requests.Load()
	if n == 0 {
		t.Fatal("no requests reached the server")
	}
	if int64(tal.accepted) != n*accepted {
		t.Errorf("accepted = %d, want %d - it must come from the body, not the status",
			tal.accepted, n*accepted)
	}
	if int64(tal.rejected) != n*(batch-accepted) {
		t.Errorf("rejected = %d, want %d", tal.rejected, n*(batch-accepted))
	}
}

// The bad shapes usable inside a batch must be semantically invalid but
// syntactically fine. Malformed JSON ends the whole stream rather than
// costing one event, and an oversized body is refused by the single-event
// size cap rather than by anything about the event - so counting either as
// a per-event reject would make the generator disagree with a correct
// server.
func TestBadEventBodiesAreParseableJSON(t *testing.T) {
	now := time.Now()
	for i, build := range badEventBodies {
		body := build("cpu.load", now)

		var into map[string]any
		if err := json.Unmarshal([]byte(body), &into); err != nil {
			t.Errorf("badEventBodies[%d] = %q is not valid JSON: %v", i, body, err)
		}
		if len(body) > 4<<10 {
			t.Errorf("badEventBodies[%d] is %d bytes; big enough to trip a size cap "+
				"rather than the contract", i, len(body))
		}
	}

	// And the single-event set must still carry the two shapes that only
	// make sense there, or its own reject path stops being covered.
	if len(badBodies) != len(badEventBodies)+2 {
		t.Errorf("badBodies has %d shapes, want badEventBodies + 2", len(badBodies))
	}
}

// The report distinguishes requests from events only when they differ -
// in single-event mode the extra line would say the same thing twice.
func TestTallyReportShowsEventsOnlyWhenBatching(t *testing.T) {
	single := newTally()
	single.byStatus[200] = 100
	single.events = 100
	single.accepted = 100
	single.good = 100

	var buf bytes.Buffer
	single.report(&buf, time.Second, 0)
	if strings.Contains(buf.String(), "events/s") {
		t.Errorf("single-event report has an events line:\n%s", buf.String())
	}

	batched := newTally()
	batched.byStatus[200] = 10
	batched.events = 1000
	batched.accepted = 900
	batched.rejected = 100
	batched.good = 900

	buf.Reset()
	batched.report(&buf, time.Second, 0)
	out := buf.String()
	for _, want := range []string{"1000 events", "events/s", "900 accepted", "100 rejected"} {
		if !strings.Contains(out, want) {
			t.Errorf("batched report missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "MISMATCH") {
		t.Errorf("report cried mismatch on a consistent tally:\n%s", out)
	}
}

// --- load shedding ---

// A 503 is the server defending itself, not judging the events. Counting
// those events as "good" would make the generator report a MISMATCH every
// time the server behaved correctly under load - the harness crying wolf at
// the exact moment its output matters most.
func TestSendUntilTreatsSheddingAsNeitherGoodNorAccepted(t *testing.T) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "server at capacity", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	cfg := config{
		target: srv.URL, workers: 1, duration: 50 * time.Millisecond,
		metrics: 1, batch: 5,
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	tal := sendUntil(ctx, newClient(1), cfg, 0)

	if requests.Load() == 0 {
		t.Fatal("no requests reached the server")
	}
	if tal.good != 0 {
		t.Errorf("good = %d, want 0 - the server never looked at these events", tal.good)
	}
	if tal.accepted != 0 {
		t.Errorf("accepted = %d, want 0", tal.accepted)
	}
	if tal.shedEv != tal.events {
		t.Errorf("shedEv = %d, want all %d events", tal.shedEv, tal.events)
	}

	// And the report must not accuse the server of anything.
	var buf bytes.Buffer
	tal.report(&buf, time.Second, 0)
	if strings.Contains(buf.String(), "MISMATCH") {
		t.Errorf("report cried mismatch over a server that only shed:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "shed") {
		t.Errorf("report does not mention shedding:\n%s", buf.String())
	}
}

// A request that got no response is the one case where the server does not
// owe exact equality: it may have recorded the events and failed on the way
// back, which is at-least-once and correct. verify widens by exactly that
// many events, and no further.
func TestVerifyAllowsAtLeastOnceForUnansweredRequests(t *testing.T) {
	cfg := config{metrics: 1, runID: "abc", duration: time.Second}

	cases := []struct {
		name      string
		recorded  int
		accepted  int
		unknown   int
		wantError bool
	}{
		{"inside the window", 105, 100, 10, false},
		{"at the top of the window", 110, 100, 10, false},
		{"exactly the accepted count", 100, 100, 10, false},
		{"beyond the window", 111, 100, 10, true},
		{"below the accepted count", 99, 100, 10, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := statsServer(t, "1m0s", map[string]int{cfg.metricName(0): c.recorded})
			defer srv.Close()
			cfg := cfg
			cfg.target = srv.URL

			tal := newTally()
			tal.accepted = c.accepted
			tal.unknownEv = c.unknown

			err := verify(newClient(1), cfg, tal, time.Second, io.Discard)
			if c.wantError && err == nil {
				t.Errorf("recorded %d against %d accepted and %d unknown: accepted, want an error",
					c.recorded, c.accepted, c.unknown)
			}
			if !c.wantError && err != nil {
				t.Errorf("recorded %d against %d accepted and %d unknown: %v",
					c.recorded, c.accepted, c.unknown, err)
			}
		})
	}
}

// With no unanswered requests the check stays exact - the widening above
// must not quietly become the general rule.
func TestVerifyStaysExactWithoutUnansweredRequests(t *testing.T) {
	cfg := config{metrics: 1, runID: "abc", duration: time.Second}
	srv := statsServer(t, "1m0s", map[string]int{cfg.metricName(0): 101})
	defer srv.Close()
	cfg.target = srv.URL

	tal := newTally()
	tal.accepted = 100

	if err := verify(newClient(1), cfg, tal, time.Second, io.Discard); err == nil {
		t.Error("verify accepted a one-event discrepancy with nothing unresolved")
	}
}

func TestParseFlagsHasExpectShedOffByDefault(t *testing.T) {
	c, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.wantShed {
		t.Error("expect-shed defaults on; a normal run would fail for not overloading the server")
	}

	c, err = parseFlags([]string{"-expect-shed"})
	if err != nil {
		t.Fatal(err)
	}
	if !c.wantShed {
		t.Error("-expect-shed did not set the flag")
	}
}

// --- cardinality mode ---

func TestParseFlagsValidatesCardinality(t *testing.T) {
	if _, err := parseFlags([]string{"-cardinality", "-1"}); err == nil {
		t.Error("parseFlags accepted a negative cardinality")
	}
	c, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.cardinality != 0 || c.wantCard {
		t.Errorf("cardinality defaults to %d / expect=%v, want 0 / false - a normal run "+
			"should not flood the store", c.cardinality, c.wantCard)
	}
}

// The generated names must be distinct, bounded by -cardinality, and carry
// the run ID - the last one is what lets verify find them by prefix and
// ignore an earlier run's leftovers.
func TestCardinalityNamesAreDistinctBoundedAndScoped(t *testing.T) {
	cfg := config{metrics: 4, runID: "abc", cardinality: 7}

	seen := map[string]bool{}
	for n := 0; n < 100; n++ {
		name := cfg.cardinalityName(0, n)
		seen[name] = true
		if !strings.HasPrefix(name, "metric.abc.") {
			t.Fatalf("name %q does not carry the run ID; verify would miss it", name)
		}
	}
	if len(seen) != cfg.cardinality {
		t.Errorf("worker 0 used %d distinct names, want %d", len(seen), cfg.cardinality)
	}

	// Different workers must not collide, or the flood is smaller than asked.
	if cfg.cardinalityName(0, 0) == cfg.cardinalityName(1, 0) {
		t.Error("two workers generated the same name")
	}
}

// A 429 is the server refusing a metric name, not losing the events. They
// are counted as good (the server judged them) and separately as refused,
// and the report reconciles the two rather than crying mismatch.
func TestSendUntilCountsCardinalityRefusalsSeparately(t *testing.T) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Retry-After", "60")
		http.Error(w, "cardinality limit reached", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	cfg := config{
		target: srv.URL, workers: 1, duration: 50 * time.Millisecond,
		metrics: 1, batch: 4, cardinality: 1000,
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	tal := sendUntil(ctx, newClient(1), cfg, 0)

	if requests.Load() == 0 {
		t.Fatal("no requests reached the server")
	}
	if tal.cardEv != tal.events {
		t.Errorf("cardEv = %d, want all %d events", tal.cardEv, tal.events)
	}
	if tal.good != tal.events {
		t.Errorf("good = %d, want all %d - the server did judge these", tal.good, tal.events)
	}
	if tal.accepted != 0 {
		t.Errorf("accepted = %d, want 0", tal.accepted)
	}

	var buf bytes.Buffer
	tal.report(&buf, time.Second, 0)
	if strings.Contains(buf.String(), "MISMATCH") {
		t.Errorf("report cried mismatch over a server enforcing its own limit:\n%s", buf.String())
	}
	// The report says a server limit turned these away without claiming to
	// know which one: 429 now covers both the store being full and this
	// client's own name allowance, and the difference is in the message
	// rather than the protocol.
	if !strings.Contains(buf.String(), "refused") {
		t.Errorf("report does not mention the refusals:\n%s", buf.String())
	}
}

// In cardinality mode the names are not enumerable from the worker index, so
// verify sums by run-ID prefix instead. It must still ignore other runs.
func TestVerifySumsByPrefixInCardinalityMode(t *testing.T) {
	cfg := config{metrics: 1, runID: "run2", duration: time.Second, cardinality: 10}
	srv := statsServer(t, "1m0s", map[string]int{
		"metric.run2.0.card.0": 40,
		"metric.run2.0.card.1": 60,
		"metric.run1.0.card.0": 9999, // an earlier run, still in the window
		"something.else":       5,
	})
	defer srv.Close()
	cfg.target = srv.URL

	tal := newTally()
	tal.accepted = 100

	if err := verify(newClient(1), cfg, tal, time.Second, io.Discard); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// --- paging through /stats ---

// pagedStatsServer serves a metric set the way the real server now does:
// sorted, at most pageSize per response, with truncated and next set. A
// verify that does not page sees only the first slice of it.
func pagedStatsServer(t *testing.T, window string, counts map[string]int, pageSize int) *httptest.Server {
	t.Helper()

	names := make([]string, 0, len(counts))
	for n := range counts {
		names = append(names, n)
	}
	sort.Strings(names)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := r.URL.Query().Get("prefix")
		after := r.URL.Query().Get("after")

		matched := make([]string, 0, len(names))
		for _, n := range names {
			if strings.HasPrefix(n, prefix) && n > after {
				matched = append(matched, n)
			}
		}

		page := matched
		if len(page) > pageSize {
			page = page[:pageSize]
		}

		type metric struct {
			Count int     `json:"count"`
			Avg   float64 `json:"avg"`
			Min   float64 `json:"min"`
			Max   float64 `json:"max"`
		}
		out := struct {
			Window    string            `json:"window"`
			Metrics   map[string]metric `json:"metrics"`
			Matched   int               `json:"matched"`
			Truncated bool              `json:"truncated"`
			Next      string            `json:"next,omitempty"`
		}{Window: window, Metrics: map[string]metric{}, Matched: len(matched)}

		for _, n := range page {
			out.Metrics[n] = metric{Count: counts[n]}
		}
		if len(matched) > len(page) {
			out.Truncated = true
			out.Next = page[len(page)-1]
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}))
	return srv
}

// The regression this guards: /stats is bounded now, so a verify that reads
// one page counts a fraction of the store and reports every other event as
// lost. Against a real server that was 56000 of 1790472.
func TestVerifyPagesThroughABoundedStats(t *testing.T) {
	const (
		metrics   = 250
		perMetric = 40
		pageSize  = 17 // not a divisor of metrics
	)

	cfg := config{metrics: 1, runID: "abc", duration: time.Second, cardinality: 10}

	counts := map[string]int{}
	for i := 0; i < metrics; i++ {
		counts[fmt.Sprintf("metric.abc.0.card.%04d", i)] = perMetric
	}
	// Another run's data, still inside the window, must stay excluded.
	counts["metric.zzz.0.card.0000"] = 999999

	srv := pagedStatsServer(t, "1m0s", counts, pageSize)
	defer srv.Close()
	cfg.target = srv.URL

	tal := newTally()
	tal.accepted = metrics * perMetric

	if err := verify(newClient(1), cfg, tal, time.Second, io.Discard); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// A server whose cursor does not advance would hang a paging client
// forever. A harness that can hang is worse than one that fails.
func TestVerifyGivesUpOnACursorThatDoesNotAdvance(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Always truncated, always the same cursor.
		fmt.Fprint(w, `{"window":"1m0s","metrics":{},"matched":10,"truncated":true,"next":"stuck"}`)
	}))
	defer srv.Close()

	cfg := config{metrics: 1, runID: "abc", duration: time.Second, target: srv.URL}
	tal := newTally()
	tal.accepted = 1

	err := verify(newClient(1), cfg, tal, time.Second, io.Discard)
	if err == nil {
		t.Fatal("verify followed a stuck cursor without complaining")
	}
	if !strings.Contains(err.Error(), "cursor did not advance") {
		t.Errorf("error = %q, want it to name the stuck cursor", err)
	}
}

// Filtering by prefix is applied client-side as well as sent, because a
// server that ignored the parameter is exactly the bug this is here to
// catch - verification must not rest on the thing under test cooperating.
func TestVerifyFiltersByPrefixEvenIfTheServerDoesNot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Ignores prefix entirely and hands back everything it has.
		fmt.Fprint(w, `{"window":"1m0s","metrics":{
			"metric.abc.0":{"count":100},
			"metric.other.0":{"count":500},
			"unrelated":{"count":9000}
		},"matched":3,"truncated":false}`)
	}))
	defer srv.Close()

	cfg := config{metrics: 1, runID: "abc", duration: time.Second, target: srv.URL}
	tal := newTally()
	tal.accepted = 100

	if err := verify(newClient(1), cfg, tal, time.Second, io.Discard); err != nil {
		t.Fatalf("verify counted metrics outside this run: %v", err)
	}
}

// --- client identity ---

func TestParseFlagsClientIDDefaultsToNone(t *testing.T) {
	c, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.clientID != "" {
		t.Errorf("clientID = %q, want empty - a run that says nothing should send nothing",
			c.clientID)
	}

	c, err = parseFlags([]string{"-client", "svc.api"})
	if err != nil {
		t.Fatal(err)
	}
	if c.clientID != "svc.api" {
		t.Errorf("clientID = %q, want %q", c.clientID, "svc.api")
	}
}

// Every request the generator makes must carry the identity, ingest and
// verification alike. A run that identified itself for writes but not for
// reads would be budgeted as two different clients, and its own verification
// would be competing with it.
func TestTheClientIDIsSentOnEveryRequest(t *testing.T) {
	var (
		mu   sync.Mutex
		seen = map[string]map[string]bool{} // path -> ids seen on it
	)
	track := func(r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if seen[r.URL.Path] == nil {
			seen[r.URL.Path] = map[string]bool{}
		}
		seen[r.URL.Path][r.Header.Get(clientIDHeader)] = true
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		track(r)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/stats") {
			fmt.Fprint(w, `{"window":"1m0s","metrics":{},"matched":0,"truncated":false}`)
			return
		}
		fmt.Fprint(w, `{"accepted":1,"rejected":0,"errors":[]}`)
	}))
	defer srv.Close()

	cfg := config{
		target: srv.URL, workers: 1, duration: 50 * time.Millisecond,
		metrics: 1, batch: 2, clientID: "svc.api", runID: "abc", verify: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()
	if tal := sendUntil(ctx, newClient(1), cfg, 0); tal.events == 0 {
		t.Fatal("no events were sent")
	}
	if _, _, err := fetchRecorded(newClient(1), cfg); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, path := range []string{"/ingest/batch", "/stats"} {
		ids := seen[path]
		if len(ids) == 0 {
			t.Errorf("%s was never requested", path)
			continue
		}
		if !ids["svc.api"] || len(ids) != 1 {
			t.Errorf("%s saw client IDs %v, want only %q", path, ids, "svc.api")
		}
	}
}

// With no -client, nothing is sent and the server's own default applies -
// which is the behaviour every earlier run had, and must stay the default so
// existing invocations are unchanged.
func TestNoClientIDMeansNoHeader(t *testing.T) {
	var (
		got  string
		seen bool
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, seen = r.Header.Get(clientIDHeader), true
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config{target: srv.URL, workers: 1, duration: 50 * time.Millisecond, metrics: 1}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()
	sendUntil(ctx, newClient(1), cfg, 0)

	if !seen {
		t.Fatal("no request reached the server")
	}
	if got != "" {
		t.Errorf("sent %s = %q with no -client set", clientIDHeader, got)
	}
}
