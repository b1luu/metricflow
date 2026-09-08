package main

import (
	"strings"
	"testing"
	"time"
)

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
