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
	target      string        // base URL of the server
	workers     int           // concurrent request senders
	duration    time.Duration // how long to send for
	metrics     int           // how many distinct metric names to spread across
	badFrac     float64       // fraction of events that should be rejectable
	batch       int           // events per request; 1 uses the single-event endpoint
	verify      bool          // cross-check the server's count afterwards
	wantShed    bool          // fail unless the server shed at least one request
	cardinality int           // distinct names per worker; 0 keeps one name per worker
	wantCard    bool          // fail unless the server refused a name for cardinality
	runID       string        // makes this run's metric names unique
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

// cardinalityName is the nth name a worker uses when -cardinality is on.
//
// The names are still enumerable - worker count times -cardinality of them,
// all carrying the run ID - which is what keeps verify() exact. A generator
// that invented truly unbounded names would flood the store just as well and
// then have no way to ask what happened to them, so the harness would lose
// the one claim it exists to make.
func (c config) cardinalityName(worker, n int) string {
	return fmt.Sprintf("%s.card.%d", c.metricName(worker), n%c.cardinality)
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)

	var c config
	fs.StringVar(&c.target, "target", "http://localhost:8080", "base URL of the MetricFlow server")
	fs.IntVar(&c.workers, "workers", 8, "concurrent request senders")
	fs.DurationVar(&c.duration, "duration", 10*time.Second, "how long to send for")
	fs.IntVar(&c.metrics, "metrics", 4, "number of distinct metric names to spread load across")
	fs.Float64Var(&c.badFrac, "bad", 0, "fraction of events made deliberately invalid (0..1)")
	fs.IntVar(&c.batch, "batch", 1, "events per request; 1 posts to /ingest, more posts NDJSON to /ingest/batch")
	fs.BoolVar(&c.verify, "verify", true, "after the run, check the server recorded exactly what it accepted")
	fs.BoolVar(&c.wantShed, "expect-shed", false, "fail unless the server shed at least one request (503)")
	fs.IntVar(&c.cardinality, "cardinality", 0, "distinct metric names per worker; 0 uses one, higher floods the store")
	fs.BoolVar(&c.wantCard, "expect-cardinality", false, "fail unless the server refused a metric name for cardinality")

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
	case c.batch < 1:
		return config{}, fmt.Errorf("-batch must be >= 1, got %d", c.batch)
	case c.cardinality < 0:
		return config{}, fmt.Errorf("-cardinality must be >= 0, got %d", c.cardinality)
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

// clockResolution measures the smallest change time.Now() can actually
// observe, by spinning until it moves.
//
// This is not paranoia. On this development machine (Windows) the clock
// ticks every ~530µs, which is far coarser than a loopback request: over
// half of them complete inside a single tick and are timed as exactly zero.
// A report that prints "p50 0s" without saying so claims an impossibly fast
// server, when what it has really measured is the limit of its own
// instrument. Same principle as StateNoData in the alerter (§18) - say "I
// can't tell" rather than something confident and wrong.
func clockResolution() time.Duration {
	var worst time.Duration
	for i := 0; i < 5; i++ {
		start := time.Now()
		var d time.Duration
		for d == 0 {
			d = time.Since(start)
		}
		if d > worst {
			worst = d
		}
	}
	return worst
}

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
// tally counts in two units, and the distinction is the whole reason the
// batch mode needed changes here. byStatus, failed and lat count *requests*.
// events, good, accepted and rejected count *events* - which stop being the
// same thing the moment one request carries a hundred of them.
type tally struct {
	byStatus  map[int]int // HTTP responses, by status code
	failed    int         // transport errors: no response at all
	events    int         // events put on the wire
	good      int         // intended-valid events in requests the server answered
	accepted  int         // events the server said it accepted
	rejected  int         // events the server said it rejected
	shedEv    int         // events in requests refused for capacity (503)
	unknownEv int         // events in requests that got no response at all
	cardEv    int         // valid events the server refused for cardinality
	lat       *latencies
}

func newTally() tally { return tally{byStatus: map[int]int{}, lat: newLatencies()} }

func (t *tally) merge(o tally) {
	for code, n := range o.byStatus {
		t.byStatus[code] += n
	}
	t.failed += o.failed
	t.events += o.events
	t.good += o.good
	t.accepted += o.accepted
	t.rejected += o.rejected
	t.shedEv += o.shedEv
	t.unknownEv += o.unknownEv
	t.cardEv += o.cardEv
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
// badEventBodies are events that parse as JSON but break the ingest
// contract. These are the only shapes usable *inside* a batch, and the
// distinction is real rather than bookkeeping:
//
//   - Malformed JSON is not a per-event failure in a batch. The decoder
//     cannot resync past it, so it ends the stream and takes the rest of
//     the batch with it - a different experiment, covered by unit tests.
//   - An oversized body is rejected by the single-event endpoint's 4 KiB
//     cap, not by anything about the event. The same bytes inside a batch
//     are a perfectly valid event, and counting them as "bad" would make
//     the generator disagree with a server that was entirely right.
var badEventBodies = []func(metric string, now time.Time) string{
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
}

// badBodies adds the two shapes that only mean anything when the whole
// request is one event.
var badBodies = append([]func(metric string, now time.Time) string{
	func(string, time.Time) string {
		return `{"name":` // truncated JSON
	},
	func(m string, now time.Time) string {
		return fmt.Sprintf(`{"name":%q,"value":1,"ts":%d,"type":%q}`,
			m, now.UnixMilli(), strings.Repeat("x", 16<<10)) // oversized
	},
}, badEventBodies...)

// batchResponse is the reply from /ingest/batch. Redeclared here rather
// than imported for the same reason statsResponse is: this is the wire
// contract, and a generator that shares types with the server it measures
// cannot notice the server changing them.
type batchResponse struct {
	Accepted int    `json:"accepted"`
	Rejected int    `json:"rejected"`
	Fatal    string `json:"fatal"`
}

// sendUntil fires requests as fast as it can until ctx is done. Each worker
// sticks to one metric name so the load spreads across the store's map the
// way real reporters would.
func sendUntil(ctx context.Context, client *http.Client, cfg config, workerID int) tally {
	t := newTally()
	metric := cfg.metricName(workerID)

	// parseFlags rejects a batch below 1, but config is also built directly
	// by tests, and a zero there would send bodies containing no events at
	// all - forever, and silently. Treat it as the single-event default.
	perRequest := max(1, cfg.batch)

	batching := perRequest > 1
	url := strings.TrimSuffix(cfg.target, "/") + "/ingest"
	bad := badBodies
	if batching {
		url += "/batch"
		bad = badEventBodies
	}

	// nextBody builds one request. The bad quota is applied per *event*, so
	// a batch carries a realistic mix rather than being wholly good or
	// wholly bad - which is also the only way the partial-failure path gets
	// exercised at all.
	//
	// A greedy quota rather than randomness: emit a bad event whenever we
	// are below the requested fraction. Two runs with the same flags then
	// send the same mix, which makes them comparable.
	nBad := 0
	nextBody := func(now time.Time) (string, int) {
		var b strings.Builder
		good := 0
		for j := 0; j < perRequest; j++ {
			t.events++

			// With -cardinality on, every event gets a different name, so
			// the store fills with names rather than with data. The bad
			// shapes keep the worker's base name: they are about the
			// contract, not about cardinality, and mixing the two would
			// make a rejection impossible to attribute.
			name := metric
			if cfg.cardinality > 0 {
				name = cfg.cardinalityName(workerID, t.events)
			}

			if float64(nBad) < cfg.badFrac*float64(t.events) {
				b.WriteString(bad[nBad%len(bad)](metric, now))
				nBad++
			} else {
				b.WriteString(eventBody(name, float64(t.events%10+1), now))
				good++
			}
			// Harmless for the single-event endpoint, which trims trailing
			// whitespace, and required between events in a batch.
			b.WriteByte('\n')
		}
		return b.String(), good
	}

	for ctx.Err() == nil {
		now := time.Now()
		body, good := nextBody(now)

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
			t.unknownEv += perRequest
			continue
		}

		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			// No response means we cannot know what the server did with
			// these events - it may have recorded them and failed on the
			// way back. Counted apart from good rather than guessed at;
			// verify() widens its check by exactly this much.
			t.failed++
			t.unknownEv += perRequest
			continue
		}
		t.lat.add(time.Since(start))

		// A 503 is the server shedding for capacity, not judging these
		// events. It never looked at them, so they belong in neither good
		// nor accepted - counting them as good would make the generator
		// report a MISMATCH every time the server correctly defended
		// itself.
		if resp.StatusCode == http.StatusServiceUnavailable {
			t.shedEv += perRequest
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			t.byStatus[resp.StatusCode]++
			continue
		}

		// A 429 is the server refusing a metric *name* for cardinality.
		// Like a 503 it is not a judgement on the values, but unlike a 503
		// the server did answer on the merits, so the events are counted
		// as good and separately as refused - and the report checks that
		// accepted plus refused accounts for all of them.
		if resp.StatusCode == http.StatusTooManyRequests {
			t.good += good
			t.cardEv += good
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			t.byStatus[resp.StatusCode]++
			continue
		}

		// The server answered on the merits, so its verdict is comparable
		// to what we meant to send.
		t.good += good

		// How many events the server took is read from the reply in batch
		// mode, because a 200 no longer means "one event accepted" - a
		// partially-rejected batch is still a 200. Counting statuses there
		// would make verify() compare a request count against an event
		// count and call the server broken.
		if batching {
			var br batchResponse
			if err := json.NewDecoder(resp.Body).Decode(&br); err == nil {
				t.accepted += br.Accepted
				t.rejected += br.Rejected

				// Rejects beyond the ones we deliberately made bad are the
				// server refusing events we meant to be valid, which in a
				// batch means cardinality - it is the only refusal of a
				// well-formed event this server has.
				if extra := br.Rejected - (perRequest - good); extra > 0 {
					t.cardEv += extra
				}
			}
		} else if resp.StatusCode == http.StatusOK {
			t.accepted++
		}

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
func (t tally) report(w io.Writer, elapsed, clockRes time.Duration) {
	sent := t.sent()
	fmt.Fprintf(w, "\n%d requests in %s", sent, elapsed.Round(time.Millisecond))
	if secs := elapsed.Seconds(); secs > 0 {
		fmt.Fprintf(w, "  (%.0f req/s)", float64(sent)/secs)
	}
	fmt.Fprintln(w)

	// Only worth printing when they differ - in single-event mode one
	// request is one event and the line would say the same thing twice.
	if t.events != sent {
		fmt.Fprintf(w, "%d events", t.events)
		if secs := elapsed.Seconds(); secs > 0 {
			fmt.Fprintf(w, "  (%.0f events/s)", float64(t.events)/secs)
		}
		fmt.Fprintf(w, "  %d accepted, %d rejected\n", t.accepted, t.rejected)
	}

	// Shedding is the server working, not failing, so it gets its own line
	// rather than hiding inside the status histogram.
	if t.shedEv > 0 {
		fmt.Fprintf(w, "shed  %d events in %d requests refused for capacity (503)\n",
			t.shedEv, t.byStatus[http.StatusServiceUnavailable])
	}
	if t.unknownEv > 0 {
		fmt.Fprintf(w, "unknown  %d events in %d requests that got no response\n",
			t.unknownEv, t.failed)
	}
	if t.cardEv > 0 {
		fmt.Fprintf(w, "cardinality  %d valid events refused because the store was full\n",
			t.cardEv)
	}

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
			show(t.lat.quantile(0.50), clockRes), show(t.lat.quantile(0.90), clockRes),
			show(t.lat.quantile(0.99), clockRes), show(t.lat.max, clockRes))

		if clockRes > 0 {
			fmt.Fprintf(w, "         (clock resolution %s - faster than that is "+
				"unresolvable, shown as <%s)\n", round(clockRes), round(clockRes))
		}
	}

	// The generator knows which requests it meant to be valid, so it can
	// check the server agreed. A mismatch either way is a real finding: a
	// good request refused, or a deliberately broken one waved through.
	// Cardinality refusals are the server correctly enforcing a limit, not
	// losing anything, so they count toward the reconciliation rather than
	// against it. Everything the server looked at is either accepted or
	// refused for a reason we can name.
	if t.accepted+t.cardEv != t.good {
		fmt.Fprintf(w, "MISMATCH: sent %d valid events; %d accepted and %d refused for cardinality\n",
			t.good, t.accepted, t.cardEv)
	}
}

// round trims percentile output to the histogram's actual resolution; more
// digits than a microsecond would be invented.
func round(d time.Duration) time.Duration { return d.Round(time.Microsecond) }

// show renders a measured duration, refusing to state a figure the clock
// could not have resolved. "<531µs" is the honest reading of a request that
// finished inside one tick; "0s" would be a claim.
func show(d, clockRes time.Duration) string {
	if clockRes > 0 && d < clockRes {
		return "<" + round(clockRes).String()
	}
	return round(d).String()
}

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
	if cfg.cardinality > 0 {
		// In cardinality mode the names are workers x -cardinality of them,
		// and most were refused and never created, so enumerating them all
		// to look each one up would be mostly misses. Every name this run
		// generates carries the run ID, so summing by that prefix finds
		// exactly this run's metrics and no others - which is the property
		// the run ID was introduced for in the first place.
		prefix := fmt.Sprintf("metric.%s.", cfg.runID)
		for name, m := range stats.Metrics {
			if strings.HasPrefix(name, prefix) {
				recorded += m.Count
			}
		}
	} else {
		for i := 0; i < cfg.metrics; i++ {
			recorded += stats.Metrics[cfg.metricName(i)].Count
		}
	}

	// total.accepted is the server's own answer in both modes: a count of
	// 200s when one request is one event, and the sum of the batch replies'
	// accepted fields when it is not.
	accepted := total.accepted

	// Requests that got no response are the one case where exact equality
	// is not the server's to honour: it may have recorded those events and
	// then failed on the way back, which is at-least-once and correct. So
	// the check widens by exactly that many events - and no further, which
	// is what still makes it a gate.
	if total.unknownEv > 0 {
		if recorded < accepted || recorded > accepted+total.unknownEv {
			return fmt.Errorf("server accepted %d events and recorded %d; "+
				"with %d events in requests that got no response, anything in [%d,%d] "+
				"would be consistent",
				accepted, recorded, total.unknownEv, accepted, accepted+total.unknownEv)
		}
		fmt.Fprintf(w, "verify: OK - %d accepted, %d recorded "+
			"(%d events unresolved: no response, so at-least-once applies)\n",
			accepted, recorded, total.unknownEv)
		return nil
	}

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

	// Probe before the run: the report must be able to say which of its own
	// numbers the clock was too coarse to resolve.
	clockRes := clockResolution()

	total, elapsed := runLoad(context.Background(), cfg)
	total.report(os.Stdout, elapsed, clockRes)

	if cfg.verify {
		if err := verify(newClient(1), cfg, total, elapsed, os.Stdout); err != nil {
			// A mismatch is a real finding, not a warning: exit non-zero so
			// this is usable as a CI gate.
			fmt.Fprintln(os.Stderr, "verify:", err)
			os.Exit(1)
		}
	}

	// Without this, a run meant to exercise load shedding passes just as
	// happily against a server with no limiter at all - the strongest
	// possible way for that gate to be worthless.
	if cfg.wantCard && total.cardEv == 0 {
		fmt.Fprintf(os.Stderr, "expect-cardinality: the server never refused a metric name; "+
			"either it has no cardinality limit or %d names per worker did not reach it\n",
			cfg.cardinality)
		os.Exit(1)
	}

	if cfg.wantShed && total.byStatus[http.StatusServiceUnavailable] == 0 {
		fmt.Fprintf(os.Stderr, "expect-shed: the server never shed a request; "+
			"either it is not limiting concurrency or %d workers did not saturate it\n",
			cfg.workers)
		os.Exit(1)
	}
}
