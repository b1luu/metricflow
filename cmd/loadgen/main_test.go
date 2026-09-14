package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
		"-verify=false",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := config{
		target: "http://example:9999", workers: 32,
		duration: 2 * time.Second, metrics: 7, badFrac: 0.25, verify: false,
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
	tal.failed = 0

	var buf bytes.Buffer
	tal.report(&buf, 2*time.Second)
	out := buf.String()

	for _, want := range []string{
		"100 requests", // total
		"2s",           // elapsed
		"50 req/s",     // 100 / 2s
		"200",          // status lines present
		"75.0%",
		"400",
		"25.0%",
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
	tal.report(&buf, time.Second)

	if !strings.Contains(buf.String(), "no response") {
		t.Errorf("failures not reported:\n%s", buf.String())
	}
}

// An empty run must not divide by zero.
func TestTallyReportEmpty(t *testing.T) {
	var buf bytes.Buffer
	newTally().report(&buf, 0)
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
