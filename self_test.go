package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
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
	return newSelfReporter(s, l, cs, &s.counts), l, cs
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
