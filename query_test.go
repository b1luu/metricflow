package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

// --- parsing ---

func statsQueryFor(t *testing.T, raw string) (statsQuery, error) {
	t.Helper()
	return parseStatsQuery(httptest.NewRequest(http.MethodGet, "/stats"+raw, nil))
}

// An unset parameter must land on the bound, not on "unlimited" - the whole
// point is that a caller who asks for nothing in particular still gets a
// bounded answer.
func TestParseStatsQueryDefaults(t *testing.T) {
	q, err := statsQueryFor(t, "")
	if err != nil {
		t.Fatal(err)
	}
	if q.limit != maxStatsLimit {
		t.Errorf("limit = %d, want the maximum %d", q.limit, maxStatsLimit)
	}
	if q.window != window {
		t.Errorf("window = %s, want %s", q.window, window)
	}
	if q.prefix != "" || q.after != "" {
		t.Errorf("prefix/after = %q/%q, want both empty", q.prefix, q.after)
	}
}

// limit may only narrow, exactly as window may only narrow retention (§11),
// and an out-of-range value is rejected rather than clamped: a caller who
// asks for 50000 and silently receives 1000 has wrong data with a 200 on it.
func TestParseStatsQueryLimit(t *testing.T) {
	cases := []struct {
		raw     string
		want    int
		wantErr string
	}{
		{raw: "?limit=1", want: 1},
		{raw: "?limit=250", want: 250},
		{raw: fmt.Sprintf("?limit=%d", maxStatsLimit), want: maxStatsLimit},
		{raw: fmt.Sprintf("?limit=%d", maxStatsLimit+1), wantErr: "exceeds the maximum"},
		{raw: "?limit=999999", wantErr: "exceeds the maximum"},
		{raw: "?limit=0", wantErr: "at least 1"},
		{raw: "?limit=-5", wantErr: "at least 1"},
		{raw: "?limit=lots", wantErr: "invalid limit"},
		{raw: "?limit=1.5", wantErr: "invalid limit"},
		{raw: "?limit=", want: maxStatsLimit}, // absent, not invalid
	}

	for _, c := range cases {
		t.Run(c.raw, func(t *testing.T) {
			q, err := statsQueryFor(t, c.raw)
			if c.wantErr != "" {
				if err == nil {
					t.Fatalf("accepted %q as limit %d, want an error", c.raw, q.limit)
				}
				if !strings.Contains(err.Error(), c.wantErr) {
					t.Errorf("error = %q, want it to contain %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("%q: %v", c.raw, err)
			}
			if q.limit != c.want {
				t.Errorf("limit = %d, want %d", q.limit, c.want)
			}
		})
	}
}

func TestParseStatsQueryPrefixAndAfter(t *testing.T) {
	q, err := statsQueryFor(t, "?prefix=svc.api.&after=svc.api.b")
	if err != nil {
		t.Fatal(err)
	}
	if q.prefix != "svc.api." || q.after != "svc.api.b" {
		t.Errorf("prefix/after = %q/%q, want %q/%q", q.prefix, q.after, "svc.api.", "svc.api.b")
	}
}

// The window rules from §11 have to survive the move into parseStatsQuery.
func TestParseStatsQueryKeepsTheWindowRules(t *testing.T) {
	if _, err := statsQueryFor(t, "?window=30s"); err != nil {
		t.Errorf("a valid window was rejected: %v", err)
	}
	for _, raw := range []string{"?window=nonsense", "?window=0s", "?window=-1s", "?window=2h"} {
		if _, err := statsQueryFor(t, raw); err == nil {
			t.Errorf("%q was accepted", raw)
		}
	}
}

// --- selection ---

// seedNames records one event for each name so the store holds exactly them.
func seedNames(t *testing.T, s *Store, names ...string) {
	t.Helper()
	now := time.Now()
	for _, n := range names {
		if err := s.record(now, Event{Name: n, Value: 1, TS: now.UnixMilli()}); err != nil {
			t.Fatalf("%s: %v", n, err)
		}
	}
}

// Map iteration order in Go is deliberately random, and the names are spread
// across 32 shards, so an unsorted selection would return a different subset
// on every call. Sorting is what makes a truncated response reproducible and
// what lets `after` page without a server-side cursor.
func TestSelectNamesIsSortedAndDeterministic(t *testing.T) {
	s := newStore()
	seedNames(t, s, "c.metric", "a.metric", "b.metric", "d.metric")

	q := statsQuery{window: window, limit: maxStatsLimit}
	first, matched := s.selectNames(q)

	if matched != 4 {
		t.Errorf("matched = %d, want 4", matched)
	}
	if !slices.IsSorted(first) {
		t.Errorf("names are not sorted: %v", first)
	}
	for i := 0; i < 20; i++ {
		again, _ := s.selectNames(q)
		if !slices.Equal(first, again) {
			t.Fatalf("selection is not stable: %v then %v", first, again)
		}
	}
}

func TestSelectNamesHonoursPrefix(t *testing.T) {
	s := newStore()
	seedNames(t, s,
		"svc.api.latency", "svc.api.errors", "svc.db.latency", "other.metric")

	names, matched := s.selectNames(statsQuery{window: window, limit: maxStatsLimit, prefix: "svc.api."})
	want := []string{"svc.api.errors", "svc.api.latency"}
	if !slices.Equal(names, want) {
		t.Errorf("names = %v, want %v", names, want)
	}
	if matched != 2 {
		t.Errorf("matched = %d, want 2 - matched counts what the prefix covered, not the store", matched)
	}
}

// `after` is strictly greater, so paging from the last name returned cannot
// hand that same name back.
func TestSelectNamesAfterIsExclusive(t *testing.T) {
	s := newStore()
	seedNames(t, s, "a", "b", "c")

	names, _ := s.selectNames(statsQuery{window: window, limit: maxStatsLimit, after: "b"})
	if !slices.Equal(names, []string{"c"}) {
		t.Errorf("names = %v, want [c]", names)
	}
}

// The limit truncates the answer; matched still reports the whole truth, so
// a caller can tell it is seeing part of something larger.
func TestSelectNamesTruncatesButMatchedCountsEverything(t *testing.T) {
	const total = 50

	s := newStore()
	names := make([]string, total)
	for i := range names {
		names[i] = fmt.Sprintf("m.%03d", i)
	}
	seedNames(t, s, names...)

	got, matched := s.selectNames(statsQuery{window: window, limit: 10})
	if len(got) != 10 {
		t.Errorf("returned %d names, want 10", len(got))
	}
	if matched != total {
		t.Errorf("matched = %d, want %d", matched, total)
	}
	// And it is the *first* ten by name, not ten arbitrary ones.
	if !slices.Equal(got, names[:10]) {
		t.Errorf("got %v, want the first ten %v", got, names[:10])
	}
}

func TestSelectNamesOnAnEmptyStore(t *testing.T) {
	s := newStore()
	names, matched := s.selectNames(statsQuery{window: window, limit: maxStatsLimit})
	if len(names) != 0 || matched != 0 {
		t.Errorf("got %v / matched %d, want empty", names, matched)
	}
}

// --- the handler ---

// An unbounded response would be a lie about a bounded one: a client that
// cannot tell it got a partial answer will act as though metrics that exist
// do not.
func TestStatsReportsTruncation(t *testing.T) {
	const total = 25

	s := newStore()
	for i := 0; i < total; i++ {
		recordNow(s, fmt.Sprintf("m.%03d", i), 1)
	}

	resp := getStats(t, s, "?limit=10")
	if len(resp.Metrics) != 10 {
		t.Errorf("returned %d metrics, want 10", len(resp.Metrics))
	}
	if resp.Matched != total {
		t.Errorf("matched = %d, want %d", resp.Matched, total)
	}
	if !resp.Truncated {
		t.Error("truncated = false on a response that left 15 metrics out")
	}
	if resp.Next != "m.009" {
		t.Errorf("next = %q, want %q", resp.Next, "m.009")
	}
}

// And the opposite: a complete answer must not claim to be partial, or every
// client pages forever.
func TestStatsDoesNotClaimTruncationWhenComplete(t *testing.T) {
	s := newStore()
	recordNow(s, "only.metric", 1)

	resp := getStats(t, s, "")
	if resp.Truncated {
		t.Error("truncated = true on a complete response")
	}
	if resp.Next != "" {
		t.Errorf("next = %q, want empty on a complete response", resp.Next)
	}
	if resp.Matched != 1 {
		t.Errorf("matched = %d, want 1", resp.Matched)
	}
}

func TestStatsPrefixNarrows(t *testing.T) {
	s := newStore()
	recordNow(s, "svc.api.latency", 10)
	recordNow(s, "svc.api.errors", 2)
	recordNow(s, "svc.db.latency", 30)

	resp := getStats(t, s, "?prefix=svc.api.")
	if len(resp.Metrics) != 2 {
		t.Errorf("returned %d metrics, want 2: %v", len(resp.Metrics), resp.Metrics)
	}
	if _, ok := resp.Metrics["svc.db.latency"]; ok {
		t.Error("a metric outside the prefix was returned")
	}
	if resp.Matched != 2 {
		t.Errorf("matched = %d, want 2", resp.Matched)
	}
}

// The property that makes pagination worth having: walking with `after`
// visits every metric exactly once. A page size that does not divide the
// total is the case an off-by-one hides in, so it is chosen not to.
func TestStatsPaginationCoversEveryMetricExactlyOnce(t *testing.T) {
	const (
		total    = 47
		pageSize = 7 // deliberately not a divisor of total
	)

	s := newStore()
	want := map[string]bool{}
	for i := 0; i < total; i++ {
		name := fmt.Sprintf("page.%03d", i)
		want[name] = true
		recordNow(s, name, float64(i))
	}

	seen := map[string]int{}
	after := ""
	for pages := 0; ; pages++ {
		if pages > total {
			t.Fatal("pagination did not terminate")
		}

		resp := getStats(t, s, fmt.Sprintf("?limit=%d&after=%s", pageSize, after))
		for name := range resp.Metrics {
			seen[name]++
		}
		if !resp.Truncated {
			break
		}
		if resp.Next == "" {
			t.Fatal("truncated response carried no next cursor; the client cannot continue")
		}
		if resp.Next <= after {
			t.Fatalf("next %q did not advance past %q; this would loop forever", resp.Next, after)
		}
		after = resp.Next
	}

	for name := range want {
		switch seen[name] {
		case 1: // exactly right
		case 0:
			t.Errorf("%s was never returned", name)
		default:
			t.Errorf("%s was returned %d times", name, seen[name])
		}
	}
	if len(seen) != total {
		t.Errorf("saw %d distinct metrics, want %d", len(seen), total)
	}
}

// A metric whose buckets have all aged out is skipped, but it still matched
// the query and still consumed a slot of the limit. That is a real corner:
// a page can come back with fewer metrics than its limit without being the
// last page, and `next` has to be the last name *considered* or paging would
// stall on the gap.
func TestStatsPagesPastMetricsWithNoDataInTheWindow(t *testing.T) {
	s := newStore()

	// Three live metrics either side of two that are entirely stale.
	recordNow(s, "p.1", 1)
	recordNow(s, "p.2", 1)
	seedBucket(s, "p.3", bucketAt(window+time.Minute), &Agg{Count: 9, Sum: 9, Min: 1, Max: 1})
	seedBucket(s, "p.4", bucketAt(window+time.Minute), &Agg{Count: 9, Sum: 9, Min: 1, Max: 1})
	recordNow(s, "p.5", 1)

	resp := getStats(t, s, "?limit=4")

	if resp.Matched != 5 {
		t.Errorf("matched = %d, want 5 - matched counts names, not metrics with data", resp.Matched)
	}
	if len(resp.Metrics) != 2 {
		t.Errorf("returned %d metrics, want 2 (p.1 and p.2; p.3 and p.4 are stale)", len(resp.Metrics))
	}
	if !resp.Truncated {
		t.Error("truncated = false, but p.5 was never considered")
	}
	if resp.Next != "p.4" {
		t.Errorf("next = %q, want %q - the last name considered, not the last returned", resp.Next, "p.4")
	}

	// Following that cursor must reach p.5 rather than stalling.
	rest := getStats(t, s, "?limit=4&after="+resp.Next)
	if _, ok := rest.Metrics["p.5"]; !ok {
		t.Errorf("following next=%q did not reach p.5: %v", resp.Next, rest.Metrics)
	}
}

// Rejections must be rejections, not silently clamped successes.
func TestStatsRejectsAnOutOfRangeLimit(t *testing.T) {
	s := newStore()
	recordNow(s, "a.metric", 1)

	for _, raw := range []string{
		fmt.Sprintf("?limit=%d", maxStatsLimit+1),
		"?limit=0",
		"?limit=-1",
		"?limit=abc",
	} {
		rec := httptest.NewRecorder()
		s.handleStats(rec, httptest.NewRequest(http.MethodGet, "/stats"+raw, nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", raw, rec.Code)
		}
	}
}

// The bound has to hold without being asked for. A caller sending no
// parameters at all must still get at most maxStatsLimit metrics computed -
// that is the difference between a limit and a suggestion.
func TestStatsIsBoundedWithNoParameters(t *testing.T) {
	const total = maxStatsLimit + 250

	s := newStore()
	now := time.Now()
	for i := 0; i < total; i++ {
		ev := Event{Name: fmt.Sprintf("bounded.%05d", i), Value: 1, TS: now.UnixMilli()}
		if err := s.record(now, ev); err != nil {
			t.Fatalf("metric %d: %v", i, err)
		}
	}

	resp := getStats(t, s, "")

	if len(resp.Metrics) != maxStatsLimit {
		t.Errorf("an unparameterised /stats returned %d metrics, want the cap %d",
			len(resp.Metrics), maxStatsLimit)
	}
	if resp.Matched != total {
		t.Errorf("matched = %d, want %d", resp.Matched, total)
	}
	if !resp.Truncated {
		t.Error("truncated = false while 250 metrics were left out")
	}
}

// --- bounded selection ---

// The bounded selector replaces "collect everything, sort, truncate". It has
// to agree with that exactly, so the test *is* that comparison, over random
// input including duplicates of the hard cases: limits larger than the
// input, limits of one, and inputs already in order or in reverse.
func TestNameSelectorAgreesWithSortAndTruncate(t *testing.T) {
	naive := func(in []string, limit int) []string {
		out := slices.Clone(in)
		slices.Sort(out)
		if len(out) > limit {
			out = out[:limit]
		}
		return out
	}

	// A deterministic pseudo-random order, so a failure is reproducible.
	shuffled := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("name.%06d", (i*7919+104729)%n)
		}
		return out
	}
	ascending := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("name.%06d", i)
		}
		return out
	}
	descending := func(n int) []string {
		out := ascending(n)
		slices.Reverse(out)
		return out
	}

	shapes := map[string]func(int) []string{
		"shuffled":   shuffled,
		"ascending":  ascending,
		"descending": descending,
	}

	for shape, build := range shapes {
		for _, n := range []int{0, 1, 2, 7, 999, 1000, 1001, 5000} {
			for _, limit := range []int{1, 2, 10, 999, 1000} {
				name := fmt.Sprintf("%s/n=%d/limit=%d", shape, n, limit)
				t.Run(name, func(t *testing.T) {
					in := build(n)

					sel := newNameSelector(limit)
					for _, s := range in {
						sel.add(s)
					}
					got := sel.result()

					if want := naive(in, limit); !slices.Equal(got, want) {
						t.Errorf("selector gave %d names, sort-and-truncate gave %d; first difference at %s",
							len(got), len(want), firstDiff(got, want))
					}
				})
			}
		}
	}
}

func firstDiff(a, b []string) string {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return fmt.Sprintf("index %d: %q vs %q", i, a[i], b[i])
		}
	}
	return fmt.Sprintf("length %d vs %d", len(a), len(b))
}

// The bound is on memory, and it is the reason the selector exists: without
// it a query against a full store gathers one string header per metric
// simply to discard almost all of them.
func TestNameSelectorNeverHoldsMoreThanTwiceItsLimit(t *testing.T) {
	const limit = 10

	sel := newNameSelector(limit)
	for i := 0; i < 10_000; i++ {
		sel.add(fmt.Sprintf("name.%06d", i))
		if len(sel.names) > 2*limit {
			t.Fatalf("after %d names the selector holds %d, more than twice its limit of %d",
				i+1, len(sel.names), limit)
		}
		if cap(sel.names) > 2*limit {
			t.Fatalf("after %d names the backing array grew to %d, past twice the limit",
				i+1, cap(sel.names))
		}
	}
	if len(sel.result()) != limit {
		t.Errorf("result holds %d names, want %d", len(sel.result()), limit)
	}
}

// matched counts every name the query covered, even the ones the selector
// threw away immediately - that is what tells a caller it is seeing part of
// something larger.
func TestSelectNamesCountsMatchesItDiscards(t *testing.T) {
	const total = 2000

	s := newStore()
	now := time.Now()
	for i := 0; i < total; i++ {
		ev := Event{Name: fmt.Sprintf("discard.%05d", i), Value: 1, TS: now.UnixMilli()}
		if err := s.record(now, ev); err != nil {
			t.Fatalf("metric %d: %v", i, err)
		}
	}

	names, matched := s.selectNames(statsQuery{window: window, limit: 5})
	if len(names) != 5 {
		t.Errorf("kept %d names, want 5", len(names))
	}
	if matched != total {
		t.Errorf("matched = %d, want %d - discarded names still matched", matched, total)
	}
}
