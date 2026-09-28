package main

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- the reserved namespace is closed to clients ---

// Without this the one signal an operator reaches for during an incident is
// the one an incident can forge: a service writing zeroes into
// metricflow.requests.shed would make a shedding server look calm.
func TestClientsCannotWriteTheReservedNamespace(t *testing.T) {
	s := newStore()
	now := time.Now()

	names := []string{
		selfPrefix + "requests.shed",
		selfPrefix + "anything.at.all",
		selfPrefix,
	}

	for _, name := range names {
		ev := Event{Name: name, Value: 0, TS: now.UnixMilli()}

		if err := validateEvent(now, ev); err == nil {
			t.Errorf("%q passed validation", name)
		}
		if err := s.admitFor(now, &client{}, ev); err == nil {
			t.Errorf("%q was admitted", name)
		}
	}

	if got := metricCount(s); got != 0 {
		t.Errorf("store holds %d metrics after only reserved names were offered", got)
	}

	// A name that merely resembles the prefix is ordinary traffic.
	for _, ok := range []string{"metricflow", "metricflowish.metric", "my.metricflow.copy"} {
		ev := Event{Name: ok, Value: 1, TS: now.UnixMilli()}
		if err := validateEvent(now, ev); err != nil {
			t.Errorf("%q was rejected as reserved: %v", ok, err)
		}
	}
}

// Over HTTP it is a 400 and permanent: no amount of retrying will make the
// name acceptable, so it must not look like the retryable refusals (§27, §29).
func TestReservedNamesAreRejectedOverHTTP(t *testing.T) {
	s := newStore()
	body := fmt.Sprintf(`{"name":%q,"value":1,"ts":%d}`,
		selfPrefix+"requests.shed", time.Now().UnixMilli())

	rec := postIngest(s, body)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "reserved") {
		t.Errorf("body = %q, want it to say the name is reserved", rec.Body.String())
	}

	// And in a batch it is a per-event rejection that is *not* retryable.
	_, resp := postBatch(t, s, body+"\n")
	if resp.Rejected != 1 || len(resp.Errors) != 1 {
		t.Fatalf("accepted/rejected = %d/%d with %d errors, want 0/1 and 1",
			resp.Accepted, resp.Rejected, len(resp.Errors))
	}
	if resp.Errors[0].Retryable {
		t.Error("a reserved name is marked retryable; a client would resend it forever")
	}
}

// --- the reserved namespace survives a full store ---

// Self-metrics must not be the first thing to fail when the store fills,
// because a full store is exactly when somebody needs to see that it is full.
func TestSelfMetricsAreRecordedEvenWhenTheStoreIsFull(t *testing.T) {
	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	// Fill one shard completely, then confirm it really is full.
	names := namesForShard(t, 0, maxMetricsPerShard+1)
	for _, name := range names[:maxMetricsPerShard] {
		if err := s.record(now, Event{Name: name, Value: 1, TS: ts}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.record(now, Event{Name: names[maxMetricsPerShard], Value: 1, TS: ts}); err == nil {
		t.Fatal("setup: the shard is not full")
	}

	// A reserved name that hashes to that same full shard still gets in.
	reservedOnFullShard := ""
	for i := 0; reservedOnFullShard == ""; i++ {
		n := fmt.Sprintf("%sprobe.%d", selfPrefix, i)
		if shardIndex(n) == 0 {
			reservedOnFullShard = n
		}
	}
	if err := s.record(now, Event{Name: reservedOnFullShard, Value: 7, TS: ts}); err != nil {
		t.Fatalf("a reserved name was refused on a full shard: %v", err)
	}
	got, ok := mergeAll(s, reservedOnFullShard)
	if !ok || got.Count != 1 || got.Max != 7 {
		t.Errorf("%s = %+v (ok=%v), want one observation of 7", reservedOnFullShard, got, ok)
	}
}

// --- what the reporter records ---

func newTestReporter(s *Store) (*selfReporter, *limiter, *clients) {
	l := newLimiter(maxInFlight)
	cs := newClients()
	return newSelfReporter(s, l, cs, &s.counts, nil), l, cs
}

// Every metric the server claims to publish must actually appear, or an
// operator builds a dashboard on a name that is never written.
func TestSelfReporterRecordsEveryMetric(t *testing.T) {
	s := newStore()
	r, _, _ := newTestReporter(s)

	r.sample(time.Now())

	want := []string{
		selfShed, selfThrottled, selfAccepted, selfRejected, selfSweptMetrics,
		selfClients, selfStoreMetrics, selfStoreBuckets, selfGoroutines,
	}
	for _, name := range want {
		if _, ok := mergeAll(s, name); !ok {
			t.Errorf("%s was not recorded", name)
		}
	}

	// And every metric in the store is one of them: the reporter must not
	// invent names nobody documented.
	for name := range storeDump(s) {
		if !reserved(name) {
			t.Errorf("the reporter wrote %q, which is outside the reserved namespace", name)
		}
	}
	if got := metricCount(s); got != len(want) {
		t.Errorf("store holds %d metrics, want the %d published", got, len(want))
	}
}

// The counters have to track the thing they are named after, or they are
// decoration. Checked through the real counter paths rather than by writing
// the numbers directly.
func TestSelfMetricsFollowTheServersActualCounters(t *testing.T) {
	s := newStore()
	r, l, cs := newTestReporter(s)

	// Some real activity: two clients, some shedding, some throttling.
	cs.get("one")
	cs.get("two")
	l.shed.Add(5)
	l.throttled.Add(3)
	s.counts.addAccepted(100)
	s.counts.addRejected(7)
	s.counts.addSwept(2)

	r.sample(time.Now())

	cases := []struct {
		name string
		want float64
	}{
		{selfShed, 5},
		{selfThrottled, 3},
		{selfAccepted, 100},
		{selfRejected, 7},
		{selfSweptMetrics, 2},
		{selfClients, 2},
	}
	for _, c := range cases {
		got, ok := mergeAll(s, c.name)
		if !ok {
			t.Errorf("%s was not recorded", c.name)
			continue
		}
		if got.Max != c.want {
			t.Errorf("%s = %v, want %v", c.name, got.Max, c.want)
		}
	}

	// The store gauges are read before the sample writes itself, so the
	// first one honestly reports an empty store. A second sample sees the
	// first one's series - which is the only self-reference here, a fixed
	// offset of however many metrics this file publishes.
	r.sample(time.Now())

	metrics, buckets := s.size()
	if got, _ := mergeAll(s, selfStoreMetrics); got.Max <= 0 || got.Max > float64(metrics) {
		t.Errorf("%s = %v, want something in (0,%d]", selfStoreMetrics, got.Max, metrics)
	}
	if got, _ := mergeAll(s, selfStoreBuckets); got.Max <= 0 || got.Max > float64(buckets) {
		t.Errorf("%s = %v, want something in (0,%d]", selfStoreBuckets, got.Max, buckets)
	}
}

// Counters are cumulative, so the increase across a window is max - min.
// That is the contract §31 documents, and it only holds if successive
// samples land in the same series rather than replacing each other.
func TestCumulativeCountersReadBackAsAnIncrease(t *testing.T) {
	s := newStore()
	r, l, _ := newTestReporter(s)

	now := time.Now()
	l.shed.Add(10)
	r.sample(now)
	l.shed.Add(15)
	r.sample(now)
	l.shed.Add(5)
	r.sample(now)

	got, ok := mergeAll(s, selfShed)
	if !ok {
		t.Fatal("nothing recorded")
	}
	if got.Count != 3 {
		t.Errorf("Count = %d, want 3 samples", got.Count)
	}
	if got.Min != 10 || got.Max != 30 {
		t.Errorf("min/max = %v/%v, want 10/30", got.Min, got.Max)
	}
	if inc := got.Max - got.Min; inc != 20 {
		t.Errorf("max - min = %v, want 20 - the increase across the samples", inc)
	}
}

// Self-metrics are recorded with no client, so they must not spend anyone's
// name allowance (§29) - least of all during the incident that makes them
// interesting.
func TestSelfMetricsSpendNoClientAllowance(t *testing.T) {
	s := newStore()
	r, _, _ := newTestReporter(s)

	c := &client{}
	now := time.Now()

	for i := 0; i < 20; i++ {
		r.sample(now)
	}

	// The client's whole allowance is untouched.
	for i := 0; i < maxNewNamesPerEpoch; i++ {
		ev := Event{Name: fmt.Sprintf("client.%04d", i), Value: 1, TS: now.UnixMilli()}
		if err := s.recordFor(now, c, ev); err != nil {
			t.Fatalf("client name %d was refused after %d self-samples: %v", i+1, 20, err)
		}
	}
}

// --- the loop ---

func TestSelfReporterRunsAndStopsOnCancel(t *testing.T) {
	s := newStore()
	r, _, _ := newTestReporter(s)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Run(ctx, time.Millisecond)
	}()

	deadline := time.After(5 * time.Second)
	for {
		got, ok := mergeAll(s, selfGoroutines)
		if ok && got.Count >= 3 {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatalf("the reporter took fewer than 3 samples (got %+v)", got)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the reporter did not stop when its context was cancelled")
	}
}

// The first sample is taken immediately, so a server asked about itself in
// its first second has something to say rather than an empty answer.
func TestSelfReporterSamplesBeforeItsFirstTick(t *testing.T) {
	s := newStore()
	r, _, _ := newTestReporter(s)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		// An interval long enough that a tick cannot be what produced the
		// sample below.
		r.Run(ctx, time.Hour)
	}()

	deadline := time.After(5 * time.Second)
	for {
		if _, ok := mergeAll(s, selfGoroutines); ok {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("no sample before the first tick")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()
	<-done
}

// --- through the query path ---

// The whole argument for recording these as metrics is that everything else
// applies to them unchanged. ?prefix= is the proof.
func TestSelfMetricsAreQueryableLikeAnyOther(t *testing.T) {
	s := newStore()
	r, _, _ := newTestReporter(s)

	recordNow(s, "svc.api.latency", 12)
	r.sample(time.Now())

	all := getStats(t, s, "")
	if _, ok := all.Metrics["svc.api.latency"]; !ok {
		t.Error("the ordinary metric is missing from an unfiltered query")
	}
	if _, ok := all.Metrics[selfGoroutines]; !ok {
		t.Error("the server's own metrics are missing from an unfiltered query")
	}

	// And they narrow like anything else.
	self := getStats(t, s, "?prefix="+selfPrefix)
	if len(self.Metrics) == 0 {
		t.Fatal("?prefix= returned no self-metrics")
	}
	for name := range self.Metrics {
		if !reserved(name) {
			t.Errorf("%q came back from a reserved-prefix query", name)
		}
	}
	if _, ok := self.Metrics["svc.api.latency"]; ok {
		t.Error("an ordinary metric came back from a reserved-prefix query")
	}
}

// --- alerting on the server itself ---

// A cumulative counter cannot be alerted on with max: it only rises, so a
// rule on it fires once and stays firing for the life of the process. That
// is the gap §31 created by choosing cumulative counters, and StatIncrease
// is what closes it.
func TestStatIncreaseReadsACounterAsItsRise(t *testing.T) {
	a := Agg{Count: 3, Sum: 60, Min: 10, Max: 30}

	cases := []struct {
		stat Stat
		want float64
	}{
		{StatIncrease, 20},
		{StatMax, 30},
		{StatMin, 10},
	}
	for _, c := range cases {
		if got := (Rule{Stat: c.stat}).statValue(a); got != c.want {
			t.Errorf("%s = %v, want %v", c.stat, got, c.want)
		}
	}

	// A counter that has not moved must read as zero rather than as its
	// height, or every rule fires the moment the process has done anything.
	flat := Agg{Count: 5, Sum: 5000, Min: 1000, Max: 1000}
	if got := (Rule{Stat: StatIncrease}).statValue(flat); got != 0 {
		t.Errorf("a flat counter read as an increase of %v, want 0", got)
	}
}

func TestStatIncreaseIsAValidStat(t *testing.T) {
	r := Rule{Name: "r", Metric: "m", Stat: StatIncrease, Op: OpGT, Value: 1}
	if err := r.Validate(); err != nil {
		t.Errorf("a rule using %s was rejected: %v", StatIncrease, err)
	}
}

// The argument for recording self-telemetry as metrics is that everything
// else reaches it unchanged. The alerting layer is the strongest case: these
// are ordinary rules over the ordinary store, and they had to work with no
// changes to the alerter at all.
func TestTheServerCanAlertOnItself(t *testing.T) {
	s := newStore()
	r, l, _ := newTestReporter(s)

	rules := []Rule{
		{Name: "server-shedding", Metric: selfShed, Stat: StatIncrease, Op: OpGT, Value: 0},
	}
	now := time.Now()
	a, err := newAlerter(now, s, rules)
	if err != nil {
		t.Fatal(err)
	}

	// Quiet: a baseline sample, and nothing shed.
	r.sample(now)
	_ = captureLog(t, func() { a.evaluateAll(now) })
	if got := a.Snapshot()[0].State; got != StateOK {
		t.Errorf("state = %s with nothing shed, want %s", got, StateOK)
	}

	// Now the server starts shedding, and says so about itself.
	l.shed.Add(42)
	r.sample(now)
	_ = captureLog(t, func() { a.evaluateAll(now) })

	got := a.Snapshot()[0]
	if got.State != StateFiring {
		t.Errorf("state = %s after 42 shed requests, want %s", got.State, StateFiring)
	}
	if got.Value != 42 {
		t.Errorf("value = %v, want 42 - the increase across the window", got.Value)
	}
}

// The rules the server ships with have to be valid, or it refuses to start
// (§18) - and these are the ones nobody would notice were broken, because
// they only matter during an incident.
func TestTheSelfHealthRulesAreValidAndWired(t *testing.T) {
	rules := defaultRules()

	byName := map[string]Rule{}
	for _, r := range rules {
		if err := r.Validate(); err != nil {
			t.Errorf("default rule %q is invalid: %v", r.Name, err)
		}
		byName[r.Name] = r
	}

	for _, name := range []string{"server-shedding", "cardinality-pressure"} {
		r, ok := byName[name]
		if !ok {
			t.Errorf("default rules do not include %q", name)
			continue
		}
		if !reserved(r.Metric) {
			t.Errorf("%q watches %q, which is not a self-metric", name, r.Metric)
		}
	}

	// The cardinality alert has to arrive before the store is full, not
	// when it already is: by then legitimate new metrics are being refused.
	if r := byName["cardinality-pressure"]; r.Value >= shardCount*maxMetricsPerShard {
		t.Errorf("cardinality-pressure fires at %v, which is at or past the ceiling of %d",
			r.Value, shardCount*maxMetricsPerShard)
	}
}

// --- under concurrency ---

// The reporter writes into the store it is sampling, while ingest writes,
// /stats reads and the sweeper deletes. It takes the same locks as everything
// else, and this is the test that says so - Go panics on concurrent map
// access even without -race, so a dropped lock anywhere here fails loudly.
//
// The invariant checked is the one a counter must never break: it may lag,
// but it may never go backwards. A reader that saw a counter fall would
// conclude the server had restarted.
func TestSelfReportingUnderConcurrentEverything(t *testing.T) {
	const (
		writers   = 8
		perWriter = 4000
		readers   = 3
	)

	s := newStore()
	r, l, cs := newTestReporter(s)

	var (
		wgWork   sync.WaitGroup
		wgBg     sync.WaitGroup
		done     atomic.Bool
		failures = make(chan string, 16)
	)
	fail := func(msg string) {
		select {
		case failures <- msg:
		default:
		}
	}

	// Ingest, plus the counters an ingest path would move.
	//
	// Half the traffic goes to a stable set of names and half invents a new
	// one each time, stamped old enough for the sweeper to reclaim it. That
	// churn is the point: writing to an existing metric only touches its
	// own series, so a test that reused a handful of names would never have
	// two goroutines writing the shard's own map at once - which is exactly
	// the race size() would lose. An earlier version did that and passed
	// with size()'s lock removed.
	old := time.Now().Add(-window - time.Minute)
	for w := 0; w < writers; w++ {
		wgWork.Add(1)
		go func(w int) {
			defer wgWork.Done()
			for i := 0; i < perWriter; i++ {
				now := time.Now()
				if i%2 == 0 {
					_ = s.record(now, Event{
						Name: fmt.Sprintf("svc.stable.%d", i%32), Value: 1, TS: now.UnixMilli()})
				} else {
					_ = s.record(old, Event{
						Name: fmt.Sprintf("svc.churn.%d.%d", w, i), Value: 1, TS: old.UnixMilli()})
				}
				s.counts.addAccepted(1)
				l.shed.Add(1)
				cs.get(fmt.Sprintf("client-%d", i%8))
			}
		}(w)
	}

	// The reporter, hammered far harder than once a second, and running
	// until the writers are done rather than for a fixed count. A fixed
	// count finishes before the writers get going and the two never
	// overlap, which would leave this asserting nothing about concurrency -
	// verified by removing size()'s lock and watching the test still pass.
	var sampled atomic.Int64
	wgBg.Add(1)
	go func() {
		defer wgBg.Done()
		for !done.Load() {
			r.sample(time.Now())
			sampled.Add(1)
		}
	}()

	// Readers, watching for a counter that goes backwards.
	for i := 0; i < readers; i++ {
		wgBg.Add(1)
		go func() {
			defer wgBg.Done()
			var highest float64
			for !done.Load() {
				resp, err := statsVia(s, "?prefix="+selfPrefix)
				if err != nil {
					fail(err.Error())
					return
				}
				m, ok := resp.Metrics[selfAccepted]
				if !ok {
					continue // not sampled yet
				}
				if m.Max < highest {
					fail(fmt.Sprintf("%s fell from %v to %v; a counter must never go backwards",
						selfAccepted, highest, m.Max))
				}
				highest = m.Max
			}
		}()
	}

	// And the sweeper, deleting whatever has gone idle underneath it all.
	wgBg.Add(1)
	go func() {
		defer wgBg.Done()
		for !done.Load() {
			s.sweep(time.Now())
		}
	}()

	wgWork.Wait()
	done.Store(true)
	wgBg.Wait()
	close(failures)

	for msg := range failures {
		t.Error(msg)
	}
	if sampled.Load() == 0 {
		t.Fatal("the reporter never sampled; nothing here overlapped")
	}

	// The self-metrics survived: they are written continuously, so no sweep
	// can reclaim them.
	for _, name := range []string{selfAccepted, selfShed, selfGoroutines} {
		got, ok := mergeAll(s, name)
		if !ok {
			t.Errorf("%s was swept away despite being written throughout", name)
			continue
		}
		if got.Count == 0 {
			t.Errorf("%s holds no observations", name)
		}
	}

	// And the final sample agrees with the counter it is reporting.
	got, ok := mergeAll(s, selfAccepted)
	if !ok {
		t.Fatal("no accepted-events samples")
	}
	if total := float64(s.counts.accepted.Load()); got.Max > total {
		t.Errorf("%s reported %v, past the counter's actual %v", selfAccepted, got.Max, total)
	}
}

// --- persistence (§33) ---

// snapshotters for the tests below: one that has written successfully, and
// one that cannot write at all.
func writtenSnapshotter(t *testing.T, at time.Time) *snapshotter {
	t.Helper()
	sn := newSnapshotter(newStore(), filepath.Join(t.TempDir(), "snap.bin"))
	if err := sn.write(at); err != nil {
		t.Fatal(err)
	}
	return sn
}

func brokenSnapshotter(t *testing.T) *snapshotter {
	t.Helper()
	return newSnapshotter(newStore(), filepath.Join(t.TempDir(), "no-such-dir", "snap.bin"))
}

// With persistence off, the snapshot metrics must be absent rather than
// zero. A zero age reads as "just snapshotted", which is the healthiest
// value there is - an alert on staleness would be permanently satisfied by
// a server that has never written a snapshot in its life.
func TestPersistenceMetricsAreAbsentWhenPersistenceIsOff(t *testing.T) {
	s := newStore()
	r, _, _ := newTestReporter(s) // built with a nil snapshotter

	r.sample(time.Now())

	for _, name := range []string{selfSnapAge, selfSnapBytes, selfSnapWriteFail, selfSnapLoadFail} {
		if _, ok := mergeAll(s, name); ok {
			t.Errorf("%s was published by a server with persistence off", name)
		}
	}
}

// And with it on, all four appear.
func TestPersistenceMetricsAppearWhenPersistenceIsOn(t *testing.T) {
	now := time.Now()
	s := newStore()
	r, _, _ := newTestReporter(s)
	r.snap = writtenSnapshotter(t, now)

	r.sample(now)

	for _, name := range []string{selfSnapAge, selfSnapBytes, selfSnapWriteFail, selfSnapLoadFail} {
		if _, ok := mergeAll(s, name); !ok {
			t.Errorf("%s was not published", name)
		}
	}

	// The size has to be the real one, not a placeholder.
	if got, _ := mergeAll(s, selfSnapBytes); got.Max <= 0 {
		t.Errorf("%s = %v, want the size of the file just written", selfSnapBytes, got.Max)
	}
}

// Age is the signal write failures cannot give. A snapshotter whose loop has
// stopped produces no failures and no snapshots, so the failure counter sits
// at zero looking healthy; only a growing age notices that nothing is
// happening. This proves age measures time since the last write rather than
// being a constant.
func TestSnapshotAgeGrowsWhileNothingIsWritten(t *testing.T) {
	wrote := time.Now()
	s := newStore()
	r, _, _ := newTestReporter(s)
	r.snap = writtenSnapshotter(t, wrote)

	r.sample(wrote.Add(5 * time.Second))
	r.sample(wrote.Add(30 * time.Second))

	got, ok := mergeAll(s, selfSnapAge)
	if !ok {
		t.Fatalf("%s was not published", selfSnapAge)
	}
	if got.Min < 4 || got.Min > 6 {
		t.Errorf("earliest age = %v, want about 5s", got.Min)
	}
	if got.Max < 29 || got.Max > 31 {
		t.Errorf("latest age = %v, want about 30s", got.Max)
	}
}

// A snapshotter that has never managed a write publishes its failures and no
// age. Gating the failure count on a first success would hide the single
// worst state persistence can be in: every write so far has failed.
func TestSnapshotWriteFailuresArePublishedBeforeAnySuccess(t *testing.T) {
	now := time.Now()
	s := newStore()
	r, _, _ := newTestReporter(s)
	r.snap = brokenSnapshotter(t)

	if err := r.snap.write(now); err == nil {
		t.Fatal("setup: the unwritable path accepted a write")
	}

	r.sample(now)

	got, ok := mergeAll(s, selfSnapWriteFail)
	if !ok {
		t.Fatalf("%s was not published", selfSnapWriteFail)
	}
	if got.Max != 1 {
		t.Errorf("%s = %v, want 1", selfSnapWriteFail, got.Max)
	}

	// No age, because there is no last successful write to be an age since.
	if _, ok := mergeAll(s, selfSnapAge); ok {
		t.Errorf("%s was published by a snapshotter that has never written one", selfSnapAge)
	}
}
