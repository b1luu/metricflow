package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Ingesting an event for a metric that already exists must allocate
// nothing.
//
// This was the last per-event allocation: the parser built a string for the
// name, and the store already had that exact name as a map key. A batch of
// ten thousand events allocated ten thousand and fifty-one times; it now
// allocates fifteen, regardless of how many events are in it.
func TestIngestAllocatesNothingPerEvent(t *testing.T) {
	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()
	name := []byte("svc.checkout.latency_ms")

	// Create it once, so the run below is the steady-state path.
	if err := s.admitForName(now, nil, name, Event{Value: 1, TS: ts}); err != nil {
		t.Fatal(err)
	}

	ev := Event{Value: 0.8, TS: ts}
	got := testing.AllocsPerRun(500, func() {
		if err := s.admitForName(now, nil, name, ev); err != nil {
			t.Fatal(err)
		}
	})
	if got != 0 {
		t.Errorf("ingesting an existing metric allocated %v times, want 0", got)
	}
}

// And the parse half of it on its own, so a regression can be located.
func TestParseEventNameDoesNotAllocate(t *testing.T) {
	body := []byte(`{"name":"svc.checkout.latency_ms","value":0.8,"ts":1760000000000}`)

	var (
		ev   Event
		name []byte
	)
	got := testing.AllocsPerRun(500, func() {
		if _, err := parseEventName(body, &ev, &name); err != nil {
			t.Fatal(err)
		}
	})
	if got != 0 {
		t.Errorf("parsing an event allocated %v times, want 0", got)
	}
	if string(name) != "svc.checkout.latency_ms" {
		t.Errorf("name = %q, want the one in the body", name)
	}

	// The string-building form still allocates, which is the difference
	// the hot path exists to avoid.
	withString := testing.AllocsPerRun(500, func() {
		if _, err := parseEvent(body, &ev); err != nil {
			t.Fatal(err)
		}
	})
	if withString == 0 {
		t.Error("parseEvent allocated nothing; then there was nothing to save " +
			"and this whole exercise measured the wrong thing")
	}
}

// The store must own its keys. The bytes handed to recordForName point into
// a pooled read buffer, so if the map kept that slice instead of a copy,
// the next request's data would silently rename a metric.
//
// This is the failure the change risks, and the one a test has to rule out:
// nothing about the server's behaviour would look wrong until two unrelated
// metrics merged.
func TestTheStoreCopiesTheNameItIsGiven(t *testing.T) {
	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	// A buffer the caller will reuse, exactly as the batch reader does.
	buf := []byte("svc.first.metric")
	if err := s.admitForName(now, nil, buf, Event{Value: 1, TS: ts}); err != nil {
		t.Fatal(err)
	}

	// Overwrite it in place with a different name of the same length.
	copy(buf, []byte("svc.OTHER.metric"))

	if _, ok := mergeAll(s, "svc.first.metric"); !ok {
		t.Error("svc.first.metric vanished when the caller's buffer was reused; " +
			"the store kept the slice instead of copying it")
	}
	if _, ok := mergeAll(s, "svc.OTHER.metric"); ok {
		t.Error("overwriting the caller's buffer renamed a metric already in the store")
	}
}

// Creating a metric still allocates, once, because a map has to own its
// key. The saving is per event, not per metric.
func TestANewMetricStillAllocatesItsName(t *testing.T) {
	s := newStore()
	now := time.Now()
	ts := now.UnixMilli()

	allocs := testing.AllocsPerRun(100, func() {
		// A fresh name every run, so every run inserts.
		name := []byte(fmt.Sprintf("svc.new.%d", time.Now().UnixNano()))
		_ = s.admitForName(now, nil, name, Event{Value: 1, TS: ts})
	})
	if allocs == 0 {
		t.Error("creating a metric allocated nothing; the map cannot be owning its key")
	}
}

// The two name representations must give the same answers, or the hot path
// and everything else disagree about the ingest contract.
func TestBothNameFormsAgree(t *testing.T) {
	now := time.Now()
	ts := now.UnixMilli()

	names := []string{
		"", "cpu.load", selfPrefix + "requests.shed",
		string(make([]byte, maxMetricNameLen+1)),
	}
	for _, n := range names {
		ev := Event{Name: n, Value: 1, TS: ts}

		fromString := validateEvent(now, ev)
		fromBytes := validateEventName(now, []byte(n), ev)

		switch {
		case (fromString == nil) != (fromBytes == nil):
			t.Errorf("name %q: string form says %v, bytes form says %v", n, fromString, fromBytes)
		case fromString != nil && fromString.Error() != fromBytes.Error():
			t.Errorf("name %q: errors differ: %q vs %q", n, fromString, fromBytes)
		}
	}

	// And the hash, which decides which shard a metric lives in. The two
	// must agree or a metric would be written to one shard and looked up
	// in another.
	for _, n := range []string{"a", "cpu.load", "svc.checkout.latency_ms", ""} {
		if shardIndex(n) != shardIndex([]byte(n)) {
			t.Errorf("name %q hashes to shard %d as a string and %d as bytes",
				n, shardIndex(n), shardIndex([]byte(n)))
		}
	}
}

// reserved() is the other check the two forms share, and the one with
// security weight: a client that could write the reserved namespace could
// forge the signals an operator trusts during an incident (§31).
func TestReservedAgreesAcrossBothForms(t *testing.T) {
	cases := []string{
		"", "m", selfPrefix, selfPrefix + "x", "metricflo", "metricflow",
		"metricflow", "cpu.load", "zmetricflow.x",
	}
	for _, n := range cases {
		if reserved(n) != reserved([]byte(n)) {
			t.Errorf("reserved(%q) = %v as a string, %v as bytes",
				n, reserved(n), reserved([]byte(n)))
		}
	}
	// And it still means what it did.
	if !reserved(selfShed) {
		t.Errorf("%q is not reserved", selfShed)
	}
	if reserved("cpu.load") {
		t.Error("cpu.load was treated as reserved")
	}
}

// The claim that actually holds: allocations per request do not grow with
// the number of events in it.
//
// This is stronger than measuring one event. A per-event string conversion
// can be stack-allocated when the name is short, which AllocsPerRun does
// not count - so a single-event check passes against a version that
// reintroduces the conversion. Scaling does not lie: ten thousand events
// used to cost ten thousand and fifty-one allocations.
func TestBatchAllocationsDoNotScaleWithEventCount(t *testing.T) {
	// A batch body of n events, all for names that will already exist.
	batchBody := func(n int) string {
		var sb strings.Builder
		ts := time.Now().UnixMilli()
		for i := 0; i < n; i++ {
			fmt.Fprintf(&sb, `{"name":"svc.metric.%d","value":%d,"ts":%d}`, i%8, i%17, ts)
			sb.WriteByte('\n')
		}
		return sb.String()
	}

	allocsFor := func(events int) float64 {
		s := newStore()
		body := batchBody(events)

		// Warm: the first request creates the metric names.
		postBatch(t, s, body)

		return testing.AllocsPerRun(20, func() {
			postBatch(t, s, body)
		})
	}

	small := allocsFor(10)
	large := allocsFor(1000)

	// A hundred of slack for buffer growth and the response, against the
	// ~990 extra a per-event allocation would cost.
	if large > small+100 {
		t.Errorf("10 events allocate %v and 1000 allocate %v; allocation is "+
			"scaling with the event count", small, large)
	}
}
