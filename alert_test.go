package main

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
	"time"
)

// captureLog redirects the standard logger for the duration of fn.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	old := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(old)
	fn()
	return buf.String()
}

// sample is the aggregate used across the stat tests:
// avg = 10/4 = 2.5, max = 6, min = 1, count = 4.
var sample = Agg{Count: 4, Sum: 10, Min: 1, Max: 6}

func TestRuleStatValue(t *testing.T) {
	cases := []struct {
		stat Stat
		want float64
	}{
		{StatAvg, 2.5},
		{StatMax, 6},
		{StatMin, 1},
		{StatCount, 4},
	}
	for _, c := range cases {
		r := Rule{Stat: c.stat}
		if got := r.statValue(sample); got != c.want {
			t.Errorf("stat %q = %v, want %v", c.stat, got, c.want)
		}
	}

	// Unreachable for a validated rule, but must degrade quietly rather
	// than panic if one ever slips through.
	if got := (Rule{Stat: "median"}).statValue(sample); got != 0 {
		t.Errorf("unknown stat = %v, want 0", got)
	}
}

// Every operator, checked on both sides of the threshold and exactly on it -
// the boundary is where >= and > actually differ.
func TestRuleBreached(t *testing.T) {
	cases := []struct {
		op    Op
		v     float64
		want  bool
		label string
	}{
		{OpGT, 6, true, "above"},
		{OpGT, 5, false, "equal"},
		{OpGT, 4, false, "below"},

		{OpGTE, 6, true, "above"},
		{OpGTE, 5, true, "equal"},
		{OpGTE, 4, false, "below"},

		{OpLT, 4, true, "below"},
		{OpLT, 5, false, "equal"},
		{OpLT, 6, false, "above"},

		{OpLTE, 4, true, "below"},
		{OpLTE, 5, true, "equal"},
		{OpLTE, 6, false, "above"},
	}
	for _, c := range cases {
		r := Rule{Op: c.op, Value: 5}
		if got := r.breached(c.v); got != c.want {
			t.Errorf("%v %s 5 (%s) = %v, want %v", c.v, c.op, c.label, got, c.want)
		}
	}

	// Same defensive path: an unknown op never fires rather than panicking.
	if (Rule{Op: "~", Value: 5}).breached(99) {
		t.Error("unknown op breached = true, want false")
	}
}

func TestEvaluate(t *testing.T) {
	cases := []struct {
		name      string
		rule      Rule
		agg       Agg
		ok        bool
		wantState State
		wantValue float64
	}{
		{
			name: "avg over threshold fires",
			rule: Rule{Stat: StatAvg, Op: OpGT, Value: 2},
			agg:  sample, ok: true,
			wantState: StateFiring, wantValue: 2.5,
		},
		{
			name: "avg under threshold is ok",
			rule: Rule{Stat: StatAvg, Op: OpGT, Value: 3},
			agg:  sample, ok: true,
			wantState: StateOK, wantValue: 2.5,
		},
		{
			name: "max spots a spike the avg hides",
			rule: Rule{Stat: StatMax, Op: OpGT, Value: 5},
			agg:  sample, ok: true,
			wantState: StateFiring, wantValue: 6,
		},
		{
			name: "count below threshold fires (metric gone quiet)",
			rule: Rule{Stat: StatCount, Op: OpLT, Value: 10},
			agg:  sample, ok: true,
			wantState: StateFiring, wantValue: 4,
		},
		{
			name: "min under threshold fires",
			rule: Rule{Stat: StatMin, Op: OpLT, Value: 2},
			agg:  sample, ok: true,
			wantState: StateFiring, wantValue: 1,
		},
		{
			name: "no bucket in window is nodata, not ok",
			rule: Rule{Stat: StatAvg, Op: OpGT, Value: 2},
			agg:  Agg{}, ok: false,
			wantState: StateNoData, wantValue: 0,
		},
		{
			// Guards the avg divide-by-zero: an empty Agg must never be
			// compared, even if the caller says ok.
			name: "zero count is nodata even when ok",
			rule: Rule{Stat: StatAvg, Op: OpGT, Value: 2},
			agg:  Agg{}, ok: true,
			wantState: StateNoData, wantValue: 0,
		},
		{
			name: "threshold exactly met is ok under >",
			rule: Rule{Stat: StatMax, Op: OpGT, Value: 6},
			agg:  sample, ok: true,
			wantState: StateOK, wantValue: 6,
		},
		{
			name: "threshold exactly met fires under >=",
			rule: Rule{Stat: StatMax, Op: OpGTE, Value: 6},
			agg:  sample, ok: true,
			wantState: StateFiring, wantValue: 6,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			state, value := evaluate(c.rule, c.agg, c.ok)
			if state != c.wantState {
				t.Errorf("state = %q, want %q", state, c.wantState)
			}
			if value != c.wantValue {
				t.Errorf("value = %v, want %v", value, c.wantValue)
			}
		})
	}
}

func TestRuleLookback(t *testing.T) {
	if got := (Rule{}).lookback(); got != window {
		t.Errorf("zero Window = %s, want the default %s", got, window)
	}
	if got := (Rule{Window: 30 * time.Second}).lookback(); got != 30*time.Second {
		t.Errorf("explicit Window = %s, want 30s", got)
	}
}

func TestRuleValidate(t *testing.T) {
	valid := Rule{Name: "hot cpu", Metric: "cpu.load", Stat: StatAvg, Op: OpGT, Value: 0.9}

	if err := valid.Validate(); err != nil {
		t.Fatalf("valid rule rejected: %v", err)
	}
	// A zero Window is valid - it means "use the default".
	if err := (Rule{Name: "n", Metric: "m", Stat: StatMax, Op: OpLTE}).Validate(); err != nil {
		t.Errorf("zero Window rejected: %v", err)
	}
	// The full retention window is the boundary, and is allowed.
	edge := valid
	edge.Window = window
	if err := edge.Validate(); err != nil {
		t.Errorf("Window == retention rejected: %v", err)
	}

	bad := []struct {
		name    string
		mutate  func(*Rule)
		wantMsg string
	}{
		{"no name", func(r *Rule) { r.Name = "" }, "name is required"},
		{"no metric", func(r *Rule) { r.Metric = "" }, "metric is required"},
		{"unknown stat", func(r *Rule) { r.Stat = "median" }, "unknown stat"},
		{"unknown op", func(r *Rule) { r.Op = "~" }, "unknown op"},
		{"negative window", func(r *Rule) { r.Window = -time.Second }, "window"},
		{"window past retention", func(r *Rule) { r.Window = window + time.Second }, "window"},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			r := valid
			c.mutate(&r)
			err := r.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil, want an error")
			}
			if !strings.Contains(err.Error(), c.wantMsg) {
				t.Errorf("error = %q, want it to mention %q", err, c.wantMsg)
			}
		})
	}
}

// --- Alerter: construction ---

func TestNewAlerterRejectsInvalidRule(t *testing.T) {
	_, err := newAlerter(time.Now(), newStore(), []Rule{
		{Name: "bad", Metric: "cpu.load", Stat: "median", Op: OpGT},
	})
	if err == nil {
		t.Fatal("newAlerter accepted an invalid rule")
	}
	if !strings.Contains(err.Error(), "unknown stat") {
		t.Errorf("error = %q, want it to mention the bad stat", err)
	}
}

// Rule name keys the state map, so duplicates must fail loudly rather than
// let two rules quietly share one state.
func TestNewAlerterRejectsDuplicateName(t *testing.T) {
	_, err := newAlerter(time.Now(), newStore(), []Rule{
		{Name: "dupe", Metric: "a", Stat: StatAvg, Op: OpGT, Value: 1},
		{Name: "dupe", Metric: "b", Stat: StatMax, Op: OpLT, Value: 2},
	})
	if err == nil {
		t.Fatal("newAlerter accepted duplicate rule names")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("error = %q, want it to mention the duplicate", err)
	}
}

// Every rule has a state before the first tick, and it is nodata - nothing
// has been observed yet, which is the truthful answer.
func TestNewAlerterSeedsNoData(t *testing.T) {
	now := time.Now()
	a, err := newAlerter(now, newStore(), []Rule{
		{Name: "hot", Metric: "cpu.load", Stat: StatAvg, Op: OpGT, Value: 5},
	})
	if err != nil {
		t.Fatal(err)
	}

	got := a.Snapshot()
	if len(got) != 1 {
		t.Fatalf("Snapshot() has %d entries, want 1", len(got))
	}
	if got[0].State != StateNoData {
		t.Errorf("initial State = %q, want %q", got[0].State, StateNoData)
	}
	if !got[0].Since.Equal(now) {
		t.Errorf("initial Since = %v, want %v", got[0].Since, now)
	}
	if got[0].Metric != "cpu.load" {
		t.Errorf("Metric = %q, want cpu.load", got[0].Metric)
	}
}

// --- Alerter: the state machine ---

// Walks every transition edge and pins the two timing rules: Since moves
// only when the state changes, Value refreshes on every evaluation.
func TestAlerterTransitions(t *testing.T) {
	s := newStore()
	t0 := time.Now()

	a, err := newAlerter(t0, s, []Rule{
		{Name: "hot", Metric: "cpu.load", Stat: StatAvg, Op: OpGT, Value: 5},
	})
	if err != nil {
		t.Fatal(err)
	}

	setSeries := func(buckets map[int64]*Agg) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.aggs["cpu.load"] = buckets
	}
	check := func(step string, at time.Time, wantState State, wantValue float64, wantSince time.Time) {
		t.Helper()
		a.evaluateAll(at)
		st := a.Snapshot()[0]
		if st.State != wantState {
			t.Errorf("%s: State = %q, want %q", step, st.State, wantState)
		}
		if st.Value != wantValue {
			t.Errorf("%s: Value = %v, want %v", step, st.Value, wantValue)
		}
		if !st.Since.Equal(wantSince) {
			t.Errorf("%s: Since = %v, want %v", step, st.Since, wantSince)
		}
	}

	// Empty store: still nodata, so Since must not move off construction time.
	check("empty store", t0.Add(1*time.Second), StateNoData, 0, t0)

	// avg 2 -> under the threshold.
	setSeries(map[int64]*Agg{bucketAt(0): {Count: 1, Sum: 2, Min: 2, Max: 2}})
	t2 := t0.Add(2 * time.Second)
	check("nodata -> ok", t2, StateOK, 2, t2)

	// Same data again: no transition, so Since holds at t2.
	check("ok holds", t0.Add(3*time.Second), StateOK, 2, t2)

	// avg (2+20)/2 = 11 -> over the threshold.
	setSeries(map[int64]*Agg{
		bucketAt(0):                {Count: 1, Sum: 2, Min: 2, Max: 2},
		bucketAt(10 * time.Second): {Count: 1, Sum: 20, Min: 20, Max: 20},
	})
	t4 := t0.Add(4 * time.Second)
	check("ok -> firing", t4, StateFiring, 11, t4)

	// Back under the threshold.
	setSeries(map[int64]*Agg{bucketAt(0): {Count: 1, Sum: 1, Min: 1, Max: 1}})
	t5 := t0.Add(5 * time.Second)
	check("firing -> ok", t5, StateOK, 1, t5)

	// Metric goes silent entirely.
	setSeries(nil)
	t6 := t0.Add(6 * time.Second)
	check("ok -> nodata", t6, StateNoData, 0, t6)
}

// Each rule carries its own window, so two rules on the same metric can
// legitimately disagree - which is why the alerter reads per rule rather
// than taking one shared snapshot.
func TestAlerterRespectsPerRuleWindow(t *testing.T) {
	s := newStore()
	seedBucket(s, "cpu.load", bucketAt(0), &Agg{Count: 1, Sum: 1, Min: 1, Max: 1})
	seedBucket(s, "cpu.load", bucketAt(40*time.Second), &Agg{Count: 1, Sum: 1, Min: 1, Max: 1})

	a, err := newAlerter(time.Now(), s, []Rule{
		{Name: "wide", Metric: "cpu.load", Stat: StatCount, Op: OpGT, Value: 1}, // default 60s
		{Name: "narrow", Metric: "cpu.load", Stat: StatCount, Op: OpGT, Value: 1, Window: 20 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}

	a.evaluateAll(time.Now())

	byName := map[string]AlertState{}
	for _, st := range a.Snapshot() {
		byName[st.Rule] = st
	}
	if got := byName["wide"]; got.State != StateFiring || got.Value != 2 {
		t.Errorf("wide (60s): %+v, want firing with count 2", got)
	}
	if got := byName["narrow"]; got.State != StateOK || got.Value != 1 {
		t.Errorf("narrow (20s): %+v, want ok with count 1 (older bucket excluded)", got)
	}
}

// --- Alerter: logging and snapshots ---

// Steady state is silent: a firing rule logs once, not on every tick.
func TestAlerterLogsOnlyOnTransition(t *testing.T) {
	s := newStore()
	seedBucket(s, "cpu.load", bucketAt(0), &Agg{Count: 1, Sum: 9, Min: 9, Max: 9})

	a, err := newAlerter(time.Now(), s, []Rule{
		{Name: "hot", Metric: "cpu.load", Stat: StatAvg, Op: OpGT, Value: 5},
	})
	if err != nil {
		t.Fatal(err)
	}

	first := captureLog(t, func() { a.evaluateAll(time.Now()) })
	if !strings.Contains(first, "firing") || !strings.Contains(first, "hot") {
		t.Errorf("transition log = %q, want it to name the rule and the new state", first)
	}

	again := captureLog(t, func() { a.evaluateAll(time.Now()) })
	if again != "" {
		t.Errorf("unchanged state logged %q, want silence", again)
	}
}

func TestAlerterSnapshotIsSortedAndCopied(t *testing.T) {
	a, err := newAlerter(time.Now(), newStore(), []Rule{
		{Name: "zeta", Metric: "m", Stat: StatAvg, Op: OpGT, Value: 1},
		{Name: "alpha", Metric: "m", Stat: StatAvg, Op: OpGT, Value: 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	got := a.Snapshot()
	if len(got) != 2 || got[0].Rule != "alpha" || got[1].Rule != "zeta" {
		t.Fatalf("Snapshot() = %+v, want alpha then zeta", got)
	}

	// Mutating the copy must not reach the alerter's own state.
	got[0].State = StateFiring
	if a.Snapshot()[0].State == StateFiring {
		t.Error("Snapshot() handed out a live pointer, not a copy")
	}
}

// --- Alerter: the ticker loop ---

func TestAlerterRunStopsOnContextCancel(t *testing.T) {
	a, err := newAlerter(time.Now(), newStore(), nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.Run(ctx, time.Hour) }()

	cancel() // must not wait for the (one hour) tick

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

// The ticker actually drives evaluateAll - not just starts and stops.
// A millisecond interval keeps it fast; evaluateAll itself is tested
// directly elsewhere.
func TestAlerterRunEvaluatesOnTick(t *testing.T) {
	s := newStore()
	seedBucket(s, "cpu.load", bucketAt(0), &Agg{Count: 1, Sum: 9, Min: 9, Max: 9})

	a, err := newAlerter(time.Now(), s, []Rule{
		{Name: "hot", Metric: "cpu.load", Stat: StatAvg, Op: OpGT, Value: 5},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.Run(ctx, time.Millisecond) }()
	// Stop the loop and wait for it, so no goroutine outlives this test.
	defer func() { cancel(); <-done }()

	deadline := time.After(3 * time.Second)
	for {
		if a.Snapshot()[0].State == StateFiring {
			return // the ticker evaluated
		}
		select {
		case <-deadline:
			t.Fatalf("rule never fired; state = %+v", a.Snapshot()[0])
		case <-time.After(time.Millisecond):
		}
	}
}

// The rules this program actually ships with must load. Catches a typo in
// defaultRules at test time instead of at startup.
func TestDefaultRulesAreValid(t *testing.T) {
	rules := defaultRules()
	if len(rules) == 0 {
		t.Fatal("defaultRules() is empty")
	}
	if _, err := newAlerter(time.Now(), newStore(), rules); err != nil {
		t.Fatalf("defaultRules() rejected: %v", err)
	}
}
