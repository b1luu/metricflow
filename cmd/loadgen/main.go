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
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
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
	lat      *latencies
}

func newTally() tally { return tally{byStatus: map[int]int{}, lat: newLatencies()} }

func (t *tally) merge(o tally) {
	for code, n := range o.byStatus {
		t.byStatus[code] += n
	}
	t.failed += o.failed
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

// sendUntil fires requests as fast as it can until ctx is done. Each worker
// sticks to one metric name so the load spreads across the store's map the
// way real reporters would.
func sendUntil(ctx context.Context, client *http.Client, cfg config, workerID int) tally {
	t := newTally()
	metric := fmt.Sprintf("metric.%d", workerID%cfg.metrics)
	url := strings.TrimSuffix(cfg.target, "/") + "/ingest"

	for i := 0; ctx.Err() == nil; i++ {
		body := eventBody(metric, float64(i%10+1), time.Now())

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
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(2)
	}

	fmt.Printf("target=%s workers=%d duration=%s metrics=%d bad=%.0f%% verify=%v\n",
		cfg.target, cfg.workers, cfg.duration, cfg.metrics, cfg.badFrac*100, cfg.verify)

	total, elapsed := runLoad(context.Background(), cfg)
	total.report(os.Stdout, elapsed)
}
