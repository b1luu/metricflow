package main

// Load harness, part one: benchmarks.
//
// These exist to produce evidence, not just numbers. In particular the pair
// BenchmarkRecordParallelSameMetric / ...DistinctMetrics is designed to
// *demonstrate* the claim in DESIGN §5 - that one global mutex is the
// throughput ceiling - rather than assert it. Writes to different metrics
// touch disjoint data, so with a sharded lock the distinct-metric case would
// scale with cores; through one lock the two run at the same speed.
//
// Run:
//
//	go test -run=^$ -bench=. -benchmem
//	go test -run=^$ -bench=Record -cpuprofile=cpu.out

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// discardWriter is a minimal http.ResponseWriter that keeps the status code
// and throws the body away. httptest.NewRecorder buffers every response, so
// reusing one across a benchmark measures its buffer growth as much as the
// handler; this measures the handler.
type discardWriter struct {
	header http.Header
	code   int
}

func (d *discardWriter) Header() http.Header {
	if d.header == nil {
		d.header = http.Header{}
	}
	return d.header
}

func (d *discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (d *discardWriter) WriteHeader(code int)        { d.code = code }

// ingestBody builds a valid /ingest payload stamped at now.
func ingestBody(name string, value float64, now time.Time) string {
	return fmt.Sprintf(`{"name":%q,"value":%v,"ts":%d}`, name, value, now.UnixMilli())
}

// --- the aggregation core ---

// BenchmarkRecord is the floor: one goroutine, one metric, no HTTP. now is
// hoisted out of the loop so no clock read lands in the measurement (and so
// nothing is ever evicted - this measures the steady-state update path).
func BenchmarkRecord(b *testing.B) {
	s := newStore()
	now := time.Now()
	ev := Event{Name: "cpu.load", Value: 1, TS: now.UnixMilli()}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.record(now, ev)
	}
}

// Every goroutine writes the same metric, so they contend on both the store
// lock and the same *Agg. This is the worst case.
func BenchmarkRecordParallelSameMetric(b *testing.B) {
	s := newStore()
	now := time.Now()
	ev := Event{Name: "cpu.load", Value: 1, TS: now.UnixMilli()}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.record(now, ev)
		}
	})
}

// Every goroutine writes its *own* metric, so the data they touch is
// disjoint - the only thing shared is the single store mutex. Compare this
// against SameMetric: if the numbers are alike, the lock is the ceiling and
// not the per-metric contention. That is the measurement DESIGN §5 predicts,
// and the one that would justify sharding the lock by metric name.
func BenchmarkRecordParallelDistinctMetrics(b *testing.B) {
	s := newStore()
	now := time.Now()
	var next atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ev := Event{
			Name:  fmt.Sprintf("metric.%d", next.Add(1)),
			Value: 1,
			TS:    now.UnixMilli(),
		}
		for pb.Next() {
			s.record(now, ev)
		}
	})
}

// --- the HTTP path ---

// BenchmarkIngestHandler measures read + parse + validate + record.
//
// The body's ts is refreshed every 1024 iterations: the handler rejects a ts
// older than the retention window, so a single fixed timestamp would
// silently start measuring the reject path on a long -benchtime. Amortised
// over 1024 iterations the clock read and Sprintf are noise. The status
// check in the loop makes the failure loud rather than silent either way.
func BenchmarkIngestHandler(b *testing.B) {
	s := newStore()
	body := ingestBody("cpu.load", 1, time.Now())
	w := &discardWriter{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%1024 == 0 {
			b.StopTimer()
			body = ingestBody("cpu.load", 1, time.Now())
			b.StartTimer()
		}

		w.code = http.StatusOK
		req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
		s.handleIngest(w, req)

		if w.code != http.StatusOK {
			b.Fatalf("iteration %d: status = %d, want 200", i, w.code)
		}
	}
}

// --- the query path ---

// BenchmarkStats measures a full /stats response over a realistic store:
// every metric holds a full window of buckets, so the merge does real work.
//
// The data is recorded rather than seeded. Seeding Aggs directly leaves them
// with no histogram, and the percentile work - the merge of bucket maps and
// the quantile walk - would be skipped entirely, so the benchmark would
// quietly stop measuring most of what /stats now does.
func BenchmarkStats(b *testing.B) {
	const (
		metrics       = 100
		perTimeBucket = 50
	)

	s := newStore()
	now := time.Now()
	for m := 0; m < metrics; m++ {
		name := fmt.Sprintf("metric.%d", m)
		for i := 0; i < numBuckets; i++ {
			at := now.Add(-time.Duration(i) * bucketWidth)
			for j := 0; j < perTimeBucket; j++ {
				s.record(at, Event{
					Name:  name,
					Value: float64(j%40 + 1), // ~40 distinct histogram buckets
					TS:    at.UnixMilli(),
				})
			}
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	w := &discardWriter{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.handleStats(w, req)
	}
}

// --- alerting ---

// BenchmarkEvaluateAll shows what one alerter tick costs. It should scale
// with the rule count, not the event rate - that is the whole reason
// evaluation runs on a ticker instead of inside record (§18).
func BenchmarkEvaluateAll(b *testing.B) {
	const rules = 50

	s := newStore()
	rs := make([]Rule, 0, rules)
	for i := 0; i < rules; i++ {
		name := fmt.Sprintf("metric.%d", i)
		seedBucket(s, name, bucketAt(0), &Agg{Count: 10, Sum: 50, Min: 1, Max: 9})
		rs = append(rs, Rule{
			Name: name, Metric: name,
			Stat: StatAvg, Op: OpGT, Value: 1000, // well above the data: stays ok
		})
	}

	a, err := newAlerter(time.Now(), s, rs)
	if err != nil {
		b.Fatal(err)
	}
	now := time.Now()

	// Settle every rule into ok first. The very first evaluation is a
	// transition (nodata -> ok) and would put 50 log writes inside the
	// measurement; after this, the timed loop is pure steady state, which
	// is what an ordinary tick actually costs.
	old := log.Writer()
	log.SetOutput(io.Discard)
	a.evaluateAll(now)
	log.SetOutput(old)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.evaluateAll(now)
	}
}

// --- the distribution (§23) ---

// What recording a value into the histogram costs. This is the tax the
// percentiles slice put on every single ingest, so it should stay small and
// stay allocation-free once the buckets exist.
func BenchmarkHistAdd(b *testing.B) {
	var h hist
	h.add(1) // create the map outside the measurement

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.add(float64(i%1000 + 1))
	}
}

// One quantile: what an alert rule pays per evaluation.
func BenchmarkHistQuantile(b *testing.B) {
	var h hist
	for i := 1; i <= 100000; i++ {
		h.add(float64(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.quantile(0.99)
	}
}

// Three quantiles in one pass, which is what /stats does. Compare against
// three times BenchmarkHistQuantile: the gap is the bucket sort this avoids.
func BenchmarkHistQuantilesTogether(b *testing.B) {
	var h hist
	for i := 1; i <= 100000; i++ {
		h.add(float64(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.quantiles(0.50, 0.90, 0.99)
	}
}

// --- the batch path ---

// BenchmarkIngestBatch measures the whole HTTP batch path at several batch
// sizes and reports ns/event, so the sizes are directly comparable to each
// other and to BenchmarkIngestHandler - which is the same measurement at a
// batch size of one, through the single-event endpoint.
//
// The question is where per-request cost stops dominating. At size 1 a
// batch is strictly worse than /ingest: identical request overhead, plus a
// JSON response body that /ingest does not write. Every size after that is
// the amortisation, and the point at which the curve flattens is the point
// at which the store, not the network, is the thing being measured again.
//
// Events all share one metric name on purpose. That is the *worst* case for
// the sharded store - every event in the batch takes the same lock - so a
// win here is not an artifact of the events spreading across shards.
func BenchmarkIngestBatch(b *testing.B) {
	for _, size := range []int{1, 10, 100, 1000, maxBatchEvents} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			s := newStore()
			w := &discardWriter{}
			body := ndjson(validEvents("cpu.load", size)...)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Same trap as BenchmarkIngestHandler: the accepted-ts
				// range is only 60 s wide, so a body built once would
				// silently start measuring the reject path on a long run.
				if i%1024 == 0 {
					b.StopTimer()
					body = ndjson(validEvents("cpu.load", size)...)
					b.StartTimer()
				}

				w.code = http.StatusOK
				s.handleIngestBatch(w, httptest.NewRequest(
					http.MethodPost, "/ingest/batch", strings.NewReader(body)))

				if w.code != http.StatusOK {
					b.Fatalf("iteration %d: status = %d, want 200", i, w.code)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/event")
		})
	}
}

// The same batch sizes with every event on a *different* metric, so the
// events spread across shards instead of queueing on one. Compared against
// BenchmarkIngestBatch this separates the two things batching could be
// buying: amortised request overhead, which both cases get, and reduced
// lock contention, which only this case gets.
func BenchmarkIngestBatchDistinctMetrics(b *testing.B) {
	for _, size := range []int{100, 1000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			s := newStore()
			w := &discardWriter{}

			build := func() string {
				evs := validEvents("cpu.load", size)
				for i := range evs {
					evs[i].Name = fmt.Sprintf("svc.metric.%d", i)
				}
				return ndjson(evs...)
			}
			body := build()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%1024 == 0 {
					b.StopTimer()
					body = build()
					b.StartTimer()
				}

				w.code = http.StatusOK
				s.handleIngestBatch(w, httptest.NewRequest(
					http.MethodPost, "/ingest/batch", strings.NewReader(body)))

				if w.code != http.StatusOK {
					b.Fatalf("iteration %d: status = %d, want 200", i, w.code)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/event")
		})
	}
}

// BenchmarkBatchDecodeOnly is the same stream without the store: decode and
// validate every event, record none. It splits a batch's per-event cost into
// the part JSON parsing owns and the part the store owns, which is what says
// whether optimising the locking below it could pay for itself at all.
func BenchmarkBatchDecodeOnly(b *testing.B) {
	const size = 1000

	body := ndjson(validEvents("cpu.load", size)...)
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dec := json.NewDecoder(strings.NewReader(body))
		for {
			var ev Event
			if err := dec.Decode(&ev); err != nil {
				break
			}
			if err := validateEvent(now, ev); err != nil {
				b.Fatalf("iteration %d: %v", i, err)
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/event")
}

// --- cardinality and sweeping ---

// The cap is checked only when a name is new, so an event for a metric that
// already exists should not pay for it at all. BenchmarkRecord is the
// comparison - this is the same measurement on the path that does create.
func BenchmarkRecordNewMetric(b *testing.B) {
	now := time.Now()
	ts := now.UnixMilli()

	// Names are built up front so the benchmark measures record, not
	// Sprintf. The store is rebuilt whenever the cap is reached, since past
	// that point record would be measuring the refusal path instead.
	names := make([]string, 4096)
	for i := range names {
		names[i] = fmt.Sprintf("new.metric.%d", i)
	}

	s := newStore()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%len(names) == 0 {
			b.StopTimer()
			s = newStore() // fresh budget
			b.StartTimer()
		}
		if err := s.record(now, Event{Name: names[i%len(names)], Value: 1, TS: ts}); err != nil {
			b.Fatalf("iteration %d: %v", i, err)
		}
	}
}

// Refusing has to be cheap too. A client flooding new names is exactly the
// case where the server is doing the most work per event and getting the
// least for it, so the refusal is the hot path under attack.
func BenchmarkRecordRefusedByCap(b *testing.B) {
	now := time.Now()
	ts := now.UnixMilli()

	// Fill one shard, then aim everything at it.
	s := newStore()
	full := make([]string, 0, maxMetricsPerShard)
	var overflow string
	for i := 0; len(full) <= maxMetricsPerShard; i++ {
		name := fmt.Sprintf("card.%d", i)
		if shardIndex(name) != 0 {
			continue
		}
		if len(full) == maxMetricsPerShard {
			overflow = name
			break
		}
		full = append(full, name)
		if err := s.record(now, Event{Name: name, Value: 1, TS: ts}); err != nil {
			b.Fatal(err)
		}
	}

	ev := Event{Name: overflow, Value: 1, TS: ts}
	if err := s.record(now, ev); err == nil {
		b.Fatal("the shard is not full; this would measure the accept path")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.record(now, ev); err == nil {
			b.Fatalf("iteration %d was accepted", i)
		}
	}
}

// A sweep that reclaims nothing is the common case - it runs every
// bucketWidth whether or not anything has gone idle, so its cost is a
// standing tax on a healthy server.
func BenchmarkSweepNothingToReclaim(b *testing.B) {
	const metrics = 2000

	s := newStore()
	now := time.Now()
	for i := 0; i < metrics; i++ {
		ev := Event{Name: fmt.Sprintf("live.metric.%d", i), Value: 1, TS: now.UnixMilli()}
		if err := s.record(now, ev); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got, _ := s.sweep(now); got != 0 {
			b.Fatalf("iteration %d reclaimed %d metrics from a live store", i, got)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*metrics), "ns/metric")
}

// And the case it exists for: a store full of names written once and
// abandoned. The store is rebuilt each iteration, outside the timer, because
// a sweep empties it.
func BenchmarkSweepReclaimingEverything(b *testing.B) {
	const metrics = 2000

	now := time.Now()
	old := now.Add(-window - time.Minute)
	names := make([]string, metrics)
	for i := range names {
		names[i] = fmt.Sprintf("idle.metric.%d", i)
	}

	fill := func() *Store {
		s := newStore()
		for _, n := range names {
			if err := s.record(old, Event{Name: n, Value: 1, TS: old.UnixMilli()}); err != nil {
				b.Fatal(err)
			}
		}
		return s
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := fill()
		b.StartTimer()

		if got, _ := s.sweep(now); got != metrics {
			b.Fatalf("iteration %d reclaimed %d of %d metrics", i, got, metrics)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*metrics), "ns/metric")
}

// --- the query path under cardinality ---

// storeWithMetrics fills a store with n metrics, each holding 20 values so
// every one has a real histogram to merge and sort - the work /stats does.
// Refusals are tolerated: past the cardinality cap (§27) the store is as
// full as it will get, which is exactly the case being measured.
func storeWithMetrics(n int) *Store {
	s := newStore()
	now := time.Now()
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("svc.metric.%d", i)
		for v := 0; v < 20; v++ {
			_ = s.record(now, Event{Name: name, Value: float64(v%17) + 1, TS: now.UnixMilli()})
		}
	}
	return s
}

// BenchmarkStatsByCardinality is the measurement §28 exists for: how the
// cost of one /stats request scales with how many metrics the store holds.
//
// Before the limit this was linear all the way to the cardinality ceiling -
// 77 ms and 62 MB for a single ~30-byte GET, which §26's limiter would admit
// 256 of at once. It should now flatten once the store passes maxStatsLimit,
// because past that point the request computes a fixed number of metrics
// however many exist.
func BenchmarkStatsByCardinality(b *testing.B) {
	for _, metrics := range []int{100, 1000, 10000, shardCount * maxMetricsPerShard} {
		b.Run(fmt.Sprintf("metrics=%d", metrics), func(b *testing.B) {
			s := storeWithMetrics(metrics)
			w := &discardWriter{}
			req := httptest.NewRequest(http.MethodGet, "/stats", nil)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.handleStats(w, req)
			}
		})
	}
}

// A narrow query must not pay for the store's size beyond the unavoidable
// walk: ?prefix= matching one metric should cost a walk and one merge, not
// a thousand merges.
func BenchmarkStatsNarrowPrefix(b *testing.B) {
	s := storeWithMetrics(shardCount * maxMetricsPerShard)
	w := &discardWriter{}
	req := httptest.NewRequest(http.MethodGet, "/stats?prefix=svc.metric.42&limit=1", nil)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.handleStats(w, req)
	}
}

// selectNames on its own, so the walk-and-sort floor is visible separately
// from the per-metric merge work the limit bounds.
func BenchmarkSelectNames(b *testing.B) {
	s := storeWithMetrics(shardCount * maxMetricsPerShard)
	q := statsQuery{window: window, limit: maxStatsLimit}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, matched, _ := s.selectNames(q); matched == 0 {
			b.Fatal("selected nothing")
		}
	}
}

// --- per-client limits ---

// The client budget is only consulted when a metric name is created, so an
// event for a metric that already exists must cost exactly what it did
// before per-client limits existed. BenchmarkRecord is the comparison.
func BenchmarkRecordForExistingMetric(b *testing.B) {
	s := newStore()
	c := &client{}
	now := time.Now()
	ev := Event{Name: "cpu.load", Value: 1, TS: now.UnixMilli()}

	if err := s.recordFor(now, c, ev); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.recordFor(now, c, ev); err != nil {
			b.Fatalf("iteration %d: %v", i, err)
		}
	}
}

// The identity is resolved once per request, so its cost is per request
// rather than per event - which at a batch of 10000 is a rounding error, but
// at one event per request it is not, and is worth knowing either way.
func BenchmarkIdentify(b *testing.B) {
	cs := newClients()
	h := cs.identify(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	w := &discardWriter{}
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	req.Header.Set(clientHeader, "svc.api-01_west")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.ServeHTTP(w, req)
	}
}

// Acquiring and releasing a client's share sits in front of every request,
// including the ones the global limiter then sheds, so it has to be trivial.
func BenchmarkClientAcquireRelease(b *testing.B) {
	c := &client{}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if c.acquire() {
				c.release()
			}
		}
	})
}

// And the refusal path: a client hammering past its share should cost the
// server almost nothing, for the same reason §26's shed had to be cheap.
func BenchmarkClientRefusedByItsShare(b *testing.B) {
	c := &client{}
	for i := 0; i < perClientInFlight; i++ {
		if !c.acquire() {
			b.Fatalf("setup: slot %d was refused", i+1)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if c.acquire() {
			b.Fatalf("iteration %d was admitted past the share", i)
		}
	}
}

// --- the read path while the write path is busy ---

// Every query benchmark above measures a store nobody is writing to, which
// is the one condition this server never actually runs in. "Fast queries
// out" is a third of what this project claims, and until now the only
// evidence for it came from a quiet store.
//
// The contention is structural rather than incidental. selectNames takes
// each shard's lock in turn and walks every name under it, and ingest needs
// that same lock to record a single event - so a query is not a reader
// politely sharing with writers, it is a writer's peer holding one shard
// shut for as long as the walk takes.

// storeNames returns every metric name in the store, so load can be aimed
// at metrics that already exist. Creating names instead would measure §27's
// cardinality cap refusing them, which is a different benchmark.
func storeNames(s *Store) []string {
	var names []string
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for name := range sh.aggs {
			names = append(names, name)
		}
		sh.mu.Unlock()
	}
	sort.Strings(names)
	return names
}

// ingestLoad runs writers goroutines recording into s until stop is called,
// and reports the rate they actually achieved - the load a query was
// measured under, rather than the load it was asked for.
//
// The timestamp is fixed at the start on purpose: a moving clock would roll
// buckets mid-benchmark and mix eviction into a measurement about locks.
func ingestLoad(s *Store, writers int, names []string) (stop func() float64) {
	if writers == 0 || len(names) == 0 {
		return func() float64 { return 0 }
	}

	var (
		done     = make(chan struct{})
		wg       sync.WaitGroup
		recorded atomic.Int64
	)
	now := time.Now()
	ts := now.UnixMilli()
	started := time.Now()

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			i, n := seed, int64(0)
			for {
				select {
				case <-done:
					recorded.Add(n)
					return
				default:
				}
				// A batch between channel checks: the select itself is
				// cheap but not free, and checking it once per event would
				// be measuring the check rather than the store.
				for k := 0; k < 64; k++ {
					name := names[i%len(names)]
					i++
					if s.record(now, Event{Name: name, Value: float64(i%17) + 1, TS: ts}) == nil {
						n++
					}
				}
			}
		}(w * 997)
	}

	return func() float64 {
		close(done)
		wg.Wait()
		return float64(recorded.Load()) / time.Since(started).Seconds()
	}
}

// What one /stats costs, and what it costs the firehose, at several levels
// of concurrent ingest. writers=0 is the quiet-store number every other
// query benchmark reports, kept here so the comparison is in one place.
func BenchmarkStatsUnderIngest(b *testing.B) {
	for _, writers := range []int{0, 1, 4, runtime.GOMAXPROCS(0)} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			s := storeWithMetrics(2000)
			names := storeNames(s)

			w := &discardWriter{}
			req := httptest.NewRequest(http.MethodGet, "/stats", nil)
			lat := make([]time.Duration, 0, b.N)

			stop := ingestLoad(s, writers, names)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				t0 := time.Now()
				s.handleStats(w, req)
				lat = append(lat, time.Since(t0))
			}
			b.StopTimer()

			rate := stop()

			// The tail is the number that matters for a query path. A mean
			// that looks fine while the p99 is ten times worse is exactly
			// what lock contention produces.
			sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
			b.ReportMetric(float64(lat[len(lat)/2].Microseconds()), "p50_us")
			b.ReportMetric(float64(lat[len(lat)*99/100].Microseconds()), "p99_us")
			b.ReportMetric(float64(lat[len(lat)-1].Microseconds()), "max_us")
			b.ReportMetric(rate/1e6, "ingest_Mev/s")
		})
	}
}

// The same contention from the other side: what a query costs ingest.
// BenchmarkRecordForExistingMetric is the uncontended comparison.
func BenchmarkRecordUnderStats(b *testing.B) {
	for _, queriers := range []int{0, 1} {
		b.Run(fmt.Sprintf("queriers=%d", queriers), func(b *testing.B) {
			s := storeWithMetrics(2000)
			names := storeNames(s)
			now := time.Now()
			ts := now.UnixMilli()

			done := make(chan struct{})
			var wg sync.WaitGroup
			for q := 0; q < queriers; q++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					w := &discardWriter{}
					req := httptest.NewRequest(http.MethodGet, "/stats", nil)
					for {
						select {
						case <-done:
							return
						default:
							s.handleStats(w, req)
						}
					}
				}()
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				name := names[i%len(names)]
				_ = s.record(now, Event{Name: name, Value: float64(i%17) + 1, TS: ts})
			}
			b.StopTimer()

			close(done)
			wg.Wait()
		})
	}
}

// --- what a query costs, as a number a budget could charge ---

// §29 wanted to charge a client for the metrics a query computed rather
// than the requests it made, and said it needed "a notion of cost the
// server does not have yet". These two benchmarks are that notion, derived
// rather than invented.
//
// A /stats has two terms. selectNames walks every name in the store no
// matter how few it returns, and then each selected metric is merged and
// sorted. The walk is the floor a query cannot get under; the merges are
// what a caller actually chooses by asking for more metrics.

// The walk alone: a prefix that matches nothing scans every name and merges
// none, so this is the fixed cost of asking at all.
func BenchmarkQueryWalkOnly(b *testing.B) {
	for _, metrics := range []int{1000, 4000, 16000, shardCount * maxMetricsPerShard} {
		b.Run(fmt.Sprintf("metrics=%d", metrics), func(b *testing.B) {
			s := storeWithMetrics(metrics)
			w := &discardWriter{}
			req := httptest.NewRequest(http.MethodGet, "/stats?prefix=nothing.matches.this", nil)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.handleStats(w, req)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(metrics), "ns/name")
		})
	}
}

// The per-metric term: one store, varying how many metrics the caller asks
// to have computed. The difference between limit=1 and limit=1000 is the
// work a cost-weighted budget exists to charge for.
func BenchmarkQueryByMetricsComputed(b *testing.B) {
	s := storeWithMetrics(shardCount * maxMetricsPerShard)
	for _, limit := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
			w := &discardWriter{}
			req := httptest.NewRequest(http.MethodGet,
				fmt.Sprintf("/stats?prefix=svc.metric.&limit=%d", limit), nil)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.handleStats(w, req)
			}
		})
	}
}

// An exact ?name= lookup against a full store, next to the prefix query that
// returns the same one metric. The prefix path has to walk every name,
// because it cannot know there is no longer name sharing that prefix; the
// named path knows which shard to open before it starts (§37).
func BenchmarkStatsByName(b *testing.B) {
	s := storeWithMetrics(shardCount * maxMetricsPerShard)
	w := &discardWriter{}
	req := httptest.NewRequest(http.MethodGet, "/stats?name=svc.metric.42", nil)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.handleStats(w, req)
	}
}

// Several known metrics in one request, which is what a dashboard actually
// does. Still no walk, so the cost is linear in what was asked for rather
// than in what the store happens to hold.
func BenchmarkStatsBySixNames(b *testing.B) {
	s := storeWithMetrics(shardCount * maxMetricsPerShard)
	w := &discardWriter{}
	q := "/stats?name=svc.metric.1&name=svc.metric.900&name=svc.metric.4000" +
		"&name=svc.metric.11000&name=svc.metric.20000&name=svc.metric.31000"
	req := httptest.NewRequest(http.MethodGet, q, nil)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.handleStats(w, req)
	}
}
