// Command loadgen drives a running MetricFlow server over real HTTP.
//
// It complements the in-process benchmarks rather than replacing them
// (DESIGN §19): benchmarks are the reproducible measurement of the engine,
// this measures the whole path - sockets, net/http, JSON - and can make one
// claim they cannot, because it crosses a real network boundary: the number
// of events the server *recorded* equals the number of requests it *accepted*.
//
// It is deliberately a separate main package with no access to the server's
// internals. A load generator that linked against the thing it measures
// could accidentally test the wrong side of the wire.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type config struct {
	target   string        // base URL of the server
	workers  int           // concurrent request senders
	duration time.Duration // how long to send for
	metrics  int           // how many distinct metric names to spread across
	badFrac  float64       // fraction of requests that should be rejectable
	verify   bool          // cross-check the server's count afterwards
	runID    string        // makes this run's metric names unique
}

// metricName is the metric a given worker reports under. The run ID keeps
// two runs against the same warm server from sharing metric names - without
// it the second run would find the first run's events still inside the
// window and verification would count them twice.
func (c config) metricName(worker int) string {
	n := worker % c.metrics
	if c.runID == "" {
		return fmt.Sprintf("metric.%d", n)
	}
	return fmt.Sprintf("metric.%s.%d", c.runID, n)
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)

	var c config
	fs.StringVar(&c.target, "target", "http://localhost:8080", "base URL of the MetricFlow server")
	fs.IntVar(&c.workers, "workers", 8, "concurrent request senders")
	fs.DurationVar(&c.duration, "duration", 10*time.Second, "how long to send for")
	fs.IntVar(&c.metrics, "metrics", 4, "number of distinct metric names to spread load across")
	fs.Float64Var(&c.badFrac, "bad", 0, "fraction of requests made deliberately invalid (0..1)")
	fs.BoolVar(&c.verify, "verify", true, "after the run, check the server recorded exactly what it accepted")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	switch {
	case c.target == "":
		return config{}, fmt.Errorf("-target is required")
	case c.workers < 1:
		return config{}, fmt.Errorf("-workers must be >= 1, got %d", c.workers)
	case c.duration <= 0:
		return config{}, fmt.Errorf("-duration must be > 0, got %s", c.duration)
	case c.metrics < 1:
		return config{}, fmt.Errorf("-metrics must be >= 1, got %d", c.metrics)
	case c.badFrac < 0 || c.badFrac > 1:
		return config{}, fmt.Errorf("-bad must be between 0 and 1, got %v", c.badFrac)
	}
	return c, nil
}

// newClient returns an HTTP client tuned for load generation.
//
// The default transport keeps only two idle connections per host, so beyond
// two workers every request would open a fresh socket and throw it away -
// the run would be measuring TCP handshakes and TIME_WAIT pressure rather
// than the server. Raising the idle pool to match the worker count keeps one
// warm connection per worker.
func newClient(workers int) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = workers * 2
	t.MaxIdleConnsPerHost = workers * 2
	return &http.Client{Transport: t, Timeout: 10 * time.Second}
}

// Latency is kept as a fixed histogram rather than a slice of every sample.
// Two reasons, and the second is the important one:
//
//   - Memory stays flat however long the run goes.
//   - Recording a sample allocates nothing. Appending to a growing slice
//     inside the request loop would allocate and copy, adding jitter to the
//     very thing being measured.
//
// It is the same trade the server itself makes with Agg (§1): keep the
// conclusion, not the events. The cost is resolution - one microsecond, and
// anything past latencyCap is only known to be "at least that".
const (
	latencyRes     = time.Microsecond
	latencyCap     = 100 * time.Millisecond
	latencyBuckets = int(latencyCap / latencyRes)
)

type latencies struct {
	bucket []int32 // index i holds samples in [i, i+1) * latencyRes
	over   int     // samples >= latencyCap
	count  int
	max    time.Duration
}

func newLatencies() *latencies {
	return &latencies{bucket: make([]int32, latencyBuckets)}
}

func (l *latencies) add(d time.Duration) {
	l.count++
	if d > l.max {
		l.max = d
	}
	if i := int(d / latencyRes); i >= 0 && i < len(l.bucket) {
		l.bucket[i]++
	} else {
		l.over++
	}
}

func (l *latencies) merge(o *latencies) {
	for i, n := range o.bucket {
		l.bucket[i] += n
	}
	l.over += o.over
	l.count += o.count
	if o.max > l.max {
		l.max = o.max
	}
}

// quantile returns the duration below which q of the samples fall. Samples
// past latencyCap can only be reported as latencyCap - the histogram
// genuinely does not know where they landed, and max is printed alongside
// so a long tail is never hidden.
func (l *latencies) quantile(q float64) time.Duration {
	if l.count == 0 {
		return 0
	}
	rank := int(q * float64(l.count))
	seen := 0
	for i, n := range l.bucket {
		seen += int(n)
		if seen > rank {
			return time.Duration(i) * latencyRes
		}
	}
	return latencyCap
}

// tally is one worker's view of a run. Workers accumulate locally and the
// totals are merged at the end, so the hot loop needs no shared state.
type tally struct {
	byStatus map[int]int // HTTP responses, by status code
	failed   int         // transport errors: no response at all
	good     int         // requests the generator intended to be valid
	lat      *latencies
}

func newTally() tally { return tally{byStatus: map[int]int{}, lat: newLatencies()} }

func (t *tally) merge(o tally) {
	for code, n := range o.byStatus {
		t.byStatus[code] += n
	}
	t.failed += o.failed
	t.good += o.good
	t.lat.merge(o.lat)
}

func (t tally) sent() int {
	n := t.failed
	for _, c := range t.byStatus {
		n += c
	}
	return n
}

// eventBody builds one valid /ingest payload.
func eventBody(metric string, value float64, now time.Time) string {
	return fmt.Sprintf(`{"name":%q,"value":%v,"ts":%d}`, metric, value, now.UnixMilli())
}

// badBodies are the rejectable shapes the server must refuse, one per
// validation branch. Cycling them exercises the whole reject path instead of
// hammering a single kind.
//
// They reuse the same metric name as the good traffic on purpose: anything
// that leaked through would inflate that metric's count rather than hide
// under a name of its own, so the verification step catches it.
//
// Note the sizes and bounds here are chosen to be *obviously* out of range,
// not read from the server's constants - loadgen is an external client and
// doesn't get to peek at them.
var badBodies = []func(metric string, now time.Time) string{
	func(string, time.Time) string {
		return `{"name":` // truncated JSON
	},
	func(_ string, now time.Time) string {
		return fmt.Sprintf(`{"value":1,"ts":%d}`, now.UnixMilli()) // no name
	},
	func(m string, _ time.Time) string {
		return fmt.Sprintf(`{"name":%q,"value":1}`, m) // no ts
	},
	func(m string, now time.Time) string {
		return eventBody(m, 1, now.Add(-time.Hour)) // ts long expired
	},
	func(m string, now time.Time) string {
		return eventBody(m, 1, now.Add(time.Hour)) // ts far in the future
	},
	func(m string, now time.Time) string {
		return fmt.Sprintf(`{"name":%q,"value":1,"ts":%d,"type":%q}`,
			m, now.UnixMilli(), strings.Repeat("x", 16<<10)) // oversized
	},
}

// sendUntil fires requests as fast as it can until ctx is done. Each worker
// sticks to one metric name so the load spreads across the store's map the
// way real reporters would.
func sendUntil(ctx context.Context, client *http.Client, cfg config, workerID int) tally {
	t := newTally()
	metric := cfg.metricName(workerID)
	url := strings.TrimSuffix(cfg.target, "/") + "/ingest"

	nBad := 0
	for i := 0; ctx.Err() == nil; i++ {
		now := time.Now()

		// A greedy quota rather than randomness: send a bad request
		// whenever we're below the requested fraction. Two runs with the
		// same flags then send the same mix, which makes them comparable.
		var body string
		if float64(nBad) < cfg.badFrac*float64(i+1) {
			body = badBodies[nBad%len(badBodies)](metric, now)
			nBad++
		} else {
			body = eventBody(metric, float64(i%10+1), now)
			t.good++
		}

		// Deliberately NOT http.NewRequestWithContext(ctx, ...): ctx ends
		// the run, and binding it to the request would cancel whatever is
		// in flight at that instant. The server may already have recorded
		// such an event while the client scores it as a failure - which
		// would make our own accepted count wrong, and the end-to-end
		// check in verify() meaningless. The deadline stops us *issuing*
		// requests; outstanding ones are allowed to finish. Client.Timeout
		// still bounds a genuinely hung server.
		req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
		if err != nil {
			t.failed++
			continue
		}

		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			t.failed++
			continue
		}
		t.lat.add(time.Since(start))

		// Drain and close, always. An undrained body keeps its connection
		// out of the idle pool, so the next request opens a new socket and
		// the tuned transport above buys nothing.
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		t.byStatus[resp.StatusCode]++
	}
	return t
}

// runLoad drives cfg.workers senders for cfg.duration. The elapsed time is
// measured rather than assumed to equal cfg.duration: workers overrun the
// deadline slightly by design (see sendUntil), and dividing by the wrong
// denominator would quietly overstate throughput.
func runLoad(ctx context.Context, cfg config) (tally, time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, cfg.duration)
	defer cancel()

	client := newClient(cfg.workers)
	results := make(chan tally, cfg.workers)

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			results <- sendUntil(ctx, client, cfg, id)
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(results)

	total := newTally()
	for t := range results {
		total.merge(t)
	}
	return total, elapsed
}

// report writes a human-readable summary. Status codes are sorted so two
// runs are diffable; map order would make them gratuitously different.
func (t tally) report(w io.Writer, elapsed time.Duration) {
	sent := t.sent()
	fmt.Fprintf(w, "\n%d requests in %s", sent, elapsed.Round(time.Millisecond))
	if secs := elapsed.Seconds(); secs > 0 {
		fmt.Fprintf(w, "  (%.0f req/s)", float64(sent)/secs)
	}
	fmt.Fprintln(w)

	pct := func(n int) float64 {
		if sent == 0 {
			return 0
		}
		return 100 * float64(n) / float64(sent)
	}

	codes := make([]int, 0, len(t.byStatus))
	for c := range t.byStatus {
		codes = append(codes, c)
	}
	sort.Ints(codes)

	for _, c := range codes {
		n := t.byStatus[c]
		fmt.Fprintf(w, "  %3d %9d  %5.1f%%\n", c, n, pct(n))
	}
	if t.failed > 0 {
		fmt.Fprintf(w, "  err %9d  %5.1f%%  (no response)\n", t.failed, pct(t.failed))
	}

	if t.lat.count > 0 {
		fmt.Fprintf(w, "latency  p50 %s  p90 %s  p99 %s  max %s\n",
			round(t.lat.quantile(0.50)), round(t.lat.quantile(0.90)),
			round(t.lat.quantile(0.99)), round(t.lat.max))
	}

	// The generator knows which requests it meant to be valid, so it can
	// check the server agreed. A mismatch either way is a real finding: a
	// good request refused, or a deliberately broken one waved through.
	if ok := t.byStatus[http.StatusOK]; ok != t.good {
		fmt.Fprintf(w, "MISMATCH: sent %d valid requests but %d were accepted\n", t.good, ok)
	}
}

// round trims percentile output to the histogram's actual resolution; more
// digits than a microsecond would be invented.
func round(d time.Duration) time.Duration { return d.Round(time.Microsecond) }

// statsResponse mirrors GET /stats. Declared here rather than shared with
// the server: this is the wire contract, and a client that reuses the
// server's own struct cannot notice if that contract changes.
type statsResponse struct {
	Window  string `json:"window"`
	Metrics map[string]struct {
		Count int     `json:"count"`
		Avg   float64 `json:"avg"`
		Min   float64 `json:"min"`
		Max   float64 `json:"max"`
	} `json:"metrics"`
}

// verify is the claim the in-process benchmarks cannot make: after a real
// run over a real socket, the server recorded exactly as many events as it
// told us it accepted. Returns an error if they disagree.
func verify(client *http.Client, cfg config, total tally, elapsed time.Duration, w io.Writer) error {
	resp, err := client.Get(strings.TrimSuffix(cfg.target, "/") + "/stats")
	if err != nil {
		return fmt.Errorf("fetching /stats: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("/stats returned %d", resp.StatusCode)
	}

	var stats statsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return fmt.Errorf("decoding /stats: %w", err)
	}

	// The server reports the window it applied, so we can tell whether this
	// comparison is even sound rather than hardcoding its retention. A run
	// longer than the window has already had its early events evicted -
	// the server would be *correct* to report fewer, so asserting equality
	// would be the harness lying, not the server.
	win, err := time.ParseDuration(stats.Window)
	if err != nil {
		return fmt.Errorf("unparseable window %q from /stats: %w", stats.Window, err)
	}
	if elapsed >= win {
		fmt.Fprintf(w, "verify: skipped - the run (%s) is at least as long as the server's "+
			"%s window, so the earliest events have already aged out\n",
			elapsed.Round(time.Millisecond), win)
		return nil
	}

	recorded := 0
	for i := 0; i < cfg.metrics; i++ {
		recorded += stats.Metrics[cfg.metricName(i)].Count
	}

	accepted := total.byStatus[http.StatusOK]
	if recorded != accepted {
		return fmt.Errorf("server accepted %d events but recorded %d (difference %d)",
			accepted, recorded, accepted-recorded)
	}

	fmt.Fprintf(w, "verify: OK - %d accepted, %d recorded\n", accepted, recorded)
	return nil
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(2)
	}

	// A per-run ID scopes this run's metric names, so verification against
	// a server that is already warm from an earlier run can't count that
	// run's still-in-window events as ours.
	cfg.runID = strconv.FormatInt(time.Now().UnixNano()%1e9, 36)

	fmt.Printf("target=%s workers=%d duration=%s metrics=%d bad=%.0f%% verify=%v run=%s\n",
		cfg.target, cfg.workers, cfg.duration, cfg.metrics, cfg.badFrac*100, cfg.verify, cfg.runID)

	total, elapsed := runLoad(context.Background(), cfg)
	total.report(os.Stdout, elapsed)

	if cfg.verify {
		if err := verify(newClient(1), cfg, total, elapsed, os.Stdout); err != nil {
			// A mismatch is a real finding, not a warning: exit non-zero so
			// this is usable as a CI gate.
			fmt.Fprintln(os.Stderr, "verify:", err)
			os.Exit(1)
		}
	}
}
