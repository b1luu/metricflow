package main

// Bounding the cost of a query.
//
// Every limit before this one bounded an input: body size (§17, §25), how
// long a request may take and how many may run at once (§26), how many
// metric names may exist (§27). None of them bounded the work a single
// request could ask the server to do.
//
// §27 is what made that urgent rather than theoretical. Capping cardinality
// bounded memory, but it also made the ceiling reachable and stable: an
// attacker can push the store to exactly shardCount * maxMetricsPerShard
// names and leave it there. Every /stats then merged all of them and sorted
// a histogram per metric for three quantiles, measured at 77 ms of CPU and
// 62 MB of allocation for one ~30-byte GET - and §26's limiter will admit
// 256 of those at once. The cardinality cap traded a memory exhaustion for a
// CPU amplification, and the amplification was the larger of the two.
//
// So a query is now selected, not dumped: the walk over names stays O(cardinality)
// because "which metrics exist" cannot be answered for less, but the
// expensive per-metric work is bounded by a limit the caller may narrow and
// cannot raise.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// maxStatsLimit bounds how many metrics one /stats response will compute.
//
// It is both the default and the ceiling: ?limit= may only narrow it, in
// exactly the way ?window= may only narrow retention (§11). A limit a caller
// could raise would not be a limit, and this endpoint is unauthenticated.
//
// 1000 is generous for the thing /stats is actually for - a dashboard, or an
// agent scraping a service's own metrics - while being 32x below the
// cardinality ceiling, so the bound is what decides the cost rather than the
// store's size.
const maxStatsLimit = 1000

// statsQuery is a parsed, validated GET /stats.
type statsQuery struct {
	window time.Duration
	prefix string // only names starting with this
	after  string // only names strictly greater than this (keyset paging)
	limit  int    // at most this many metrics get merged

	// names is the exact set a caller asked for, from ?name=. When it is
	// non-empty the store is not walked at all (§37) - the shard a name
	// lives in is a function of the name, so a known name is a lookup
	// rather than a search.
	names []string
}

// parseStatsQuery validates the query string, rejecting rather than
// clamping. A caller who asks for 50000 metrics and silently receives 1000
// has been given wrong data with a 200 attached; §11 made the same call for
// an over-long window and for the same reason.
func parseStatsQuery(r *http.Request) (statsQuery, error) {
	q := statsQuery{window: window, limit: maxStatsLimit}
	v := r.URL.Query()

	if s := v.Get("window"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d <= 0 {
			return q, fmt.Errorf("invalid window (use e.g. 30s, 1m)")
		}
		if d > window {
			return q, fmt.Errorf("window exceeds retention (%s)", window)
		}
		q.window = d
	}

	if s := v.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			return q, fmt.Errorf("invalid limit (want a positive integer)")
		}
		if n < 1 {
			return q, fmt.Errorf("limit must be at least 1, got %d", n)
		}
		if n > maxStatsLimit {
			return q, fmt.Errorf("limit exceeds the maximum of %d", maxStatsLimit)
		}
		q.limit = n
	}

	q.prefix = v.Get("prefix")
	q.after = v.Get("after")

	if names, ok := v["name"]; ok {
		// Mutually exclusive with the scanning parameters, and an error
		// rather than a silent precedence rule. ?name=a&prefix=b has two
		// readable meanings and no obvious one, and §11 already decided
		// that a query the server cannot answer as asked is a 400 rather
		// than a 200 over something else.
		if q.prefix != "" || q.after != "" {
			return q, fmt.Errorf("name cannot be combined with prefix or after")
		}
		// Deduplicated, so matched counts metrics rather than parameters,
		// and sorted so a repeated request is byte-identical.
		seen := make(map[string]struct{}, len(names))
		for _, n := range names {
			if n == "" {
				return q, fmt.Errorf("name must not be empty")
			}
			seen[n] = struct{}{}
		}
		if len(seen) > maxStatsLimit {
			return q, fmt.Errorf("asked for %d distinct names, over the maximum of %d",
				len(seen), maxStatsLimit)
		}
		q.names = make([]string, 0, len(seen))
		for n := range seen {
			q.names = append(q.names, n)
		}
		slices.Sort(q.names)
	}

	return q, nil
}

// lookupNames resolves an exact ?name= set without walking the store.
//
// This is the whole point of §37. The shard a metric lives in is a pure
// function of its name (§24), so a caller who already knows the name needs
// one lock and one map lookup - not a scan of every name in the store, which
// is what the prefix path has to do and what a single-metric query was
// paying 323 µs for.
//
// Names that do not exist are simply absent from the result, which is the
// same answer /stats gives for a metric whose buckets have all aged out.
// Asking about something that was never there is not an error.
func (s *Store) lookupNames(q statsQuery) (names []string, matched int) {
	names = make([]string, 0, len(q.names))
	for _, name := range q.names {
		sh := s.shardFor(name)
		sh.mu.Lock()
		_, ok := sh.aggs[name]
		sh.mu.Unlock()
		if ok {
			names = append(names, name)
			matched++
		}
	}
	return names, matched
}

// matches reports whether a metric name is in scope for this query.
func (q statsQuery) matches(name string) bool {
	return strings.HasPrefix(name, q.prefix) && name > q.after
}

// nameSelector keeps the smallest `limit` names it is shown, while never
// holding more than twice that many.
//
// The obvious implementation - collect every matching name, sort, truncate -
// is correct but allocates in proportion to the store: at full cardinality
// that was 2.6 MB of names gathered to keep 1000 of them, which is the same
// "one small request, unbounded work" shape §28 set out to remove, just
// moved from CPU to memory.
//
// Once `limit` names are in hand, any name at or past the largest of them
// cannot make the final cut, so it is rejected by a single string compare
// and never stored. The cutoff falls quickly, so nearly every name after the
// first few hundred costs one comparison and nothing else.
type nameSelector struct {
	limit  int
	names  []string
	cutoff string // largest name currently kept; meaningful only when full
	full   bool
}

func newNameSelector(limit int) *nameSelector {
	// Room for one compaction's worth of overshoot, allocated once.
	return &nameSelector{limit: limit, names: make([]string, 0, 2*limit)}
}

func (ns *nameSelector) add(name string) {
	// Names are unique across the store - a metric lives in exactly one
	// shard - so an equal name is the same name, and skipping it is free.
	if ns.full && name >= ns.cutoff {
		return
	}
	ns.names = append(ns.names, name)
	if len(ns.names) >= 2*ns.limit {
		ns.compact()
	}
}

func (ns *nameSelector) compact() {
	slices.Sort(ns.names)
	if len(ns.names) > ns.limit {
		ns.names = ns.names[:ns.limit]
	}
	if len(ns.names) == ns.limit {
		ns.full = true
		ns.cutoff = ns.names[ns.limit-1]
	}
}

// result returns the kept names, sorted.
func (ns *nameSelector) result() []string {
	ns.compact()
	return ns.names
}

// selectNames returns the names this query covers, sorted and truncated to
// the limit - and separately how many matched before truncation.
//
// Only names are looked at here, never aggregates. That is the whole point:
// holding a shard lock to test and maybe keep a string is cheap and brief,
// where the old code held it through a bucket merge and a histogram sort for
// every metric in the shard.
//
// Sorted order is not decoration. It makes a truncated response
// reproducible - map order is random, so an unsorted selection would return
// a different subset every call - and it is what lets `after` page through
// the store with no cursor the server has to remember.
func (s *Store) selectNames(q statsQuery) (names []string, matched, walked int) {
	sel := newNameSelector(q.limit)

	for i := range s.shards {
		sh := &s.shards[i]

		sh.mu.Lock()
		for name := range sh.aggs {
			// Counted whether or not it matches: the walk happens either
			// way, and it is the term that makes a prefix matching nothing
			// cost something rather than nothing (§35).
			walked++
			if q.matches(name) {
				// Counted before the selector decides, because matched is
				// the whole truth about the query and the selector only
				// keeps the part that fits.
				matched++
				sel.add(name)
			}
		}
		sh.mu.Unlock()
	}

	return sel.result(), matched, walked
}

// handleStats: GET /stats - per-metric aggregate over a time window, as JSON.
//
// Optional ?window= (Go duration, capped at retention), ?prefix= to narrow
// by name, ?limit= to narrow how many metrics are computed, and ?after= to
// continue from a previous page.
//
// Three phases, and the split is the hardening. Phase one collects names
// under each shard lock in turn. Phase two sorts and selects, holding no
// lock at all. Phase three merges each selected metric under its own shard
// lock and computes its quantiles *outside* that lock - mergeBuckets always
// returns a freshly allocated histogram (§23), so the sort is over a private
// copy and no other writer is waiting on it.
//
// The lock-hold consequence is the part that matters to ingest: the old code
// held one shard's lock through ~1024 merges and sorts at full cardinality.
// Now the longest hold is a single metric's merge.
func (s *Store) handleStats(w http.ResponseWriter, r *http.Request) {
	q, err := parseStatsQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	now := time.Now()

	// The caller's budget is checked before the query runs and charged
	// after it, because what a query cost is not known until it has run
	// (§35). A client can therefore always overshoot by one query, which
	// is bounded by §28's per-request limit and is the price of pricing
	// work accurately rather than guessing at it beforehand.
	c, _ := clientFrom(r.Context())
	if !c.allowQuery(now) {
		s.counts.addQueryRefused()
		// The epoch, not a second. The budget refills on the epoch
		// boundary, so a compliant client told to wait one second simply
		// spends nine more refusals discovering that - the same reasoning
		// batch.go and the cardinality refusal already use.
		w.Header().Set("Retry-After", strconv.Itoa(int(queryEpoch.Seconds())))
		http.Error(w, fmt.Sprintf(
			"this client is over its query budget (%d metric-equivalents per %s)",
			maxQueryCostPerEpoch, queryEpoch), http.StatusTooManyRequests)
		return
	}

	// An exact ?name= set costs no walk at all, which is the difference
	// between a dashboard polling six known metrics for six units and
	// paying six hundred for the privilege of six full-store scans (§37).
	var (
		names   []string
		matched int
		walked  int
	)
	if len(q.names) > 0 {
		names, matched = s.lookupNames(q)
	} else {
		names, matched, walked = s.selectNames(q)
	}
	c.chargeQuery(now, queryCost(walked, len(names)))

	cutoff := windowStart(now, q.window)

	resp := StatsResponse{
		Window:  q.window.String(),
		Metrics: make(map[string]MetricStats, len(names)),
		Matched: matched,
		// A named query returns exactly what was asked for, so there is
		// nothing to page through and no cursor to hand back.
		Truncated: len(q.names) == 0 && matched > len(names),
	}
	if resp.Truncated && len(names) > 0 {
		// The last name considered, not the last one returned with data: a
		// metric whose buckets have all aged out is skipped below, and
		// paging from the last *returned* name would hand it back forever.
		resp.Next = names[len(names)-1]
	}

	for _, name := range names {
		m, ok := s.aggFor(name, cutoff)
		if !ok {
			continue // nothing inside the window
		}
		// Outside the lock, over a private histogram. One pass for all
		// three: asking separately would sort the same buckets three times.
		p := m.h.quantiles(0.50, 0.90, 0.99)

		resp.Metrics[name] = MetricStats{
			Count: m.Count,
			Avg:   m.Sum / float64(m.Count),
			Min:   m.Min,
			Max:   m.Max,
			P50:   p[0],
			P90:   p[1],
			P99:   p[2],
		}
	}

	w.Header().Set("Content-Type", "application/json")
	// Nothing useful to do if this fails: the client hung up mid-read, or
	// the socket broke. Status and headers are already sent. Ignore it.
	_ = json.NewEncoder(w).Encode(resp)
}
