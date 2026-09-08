package main

// Alerting: rules that watch a metric's windowed aggregate and report a
// state. This file holds the pure, side-effect-free core - the rule shape
// and the evaluation function. Timing, transitions, and the HTTP surface
// live with the Alerter (next slice). See DESIGN.md §18.

import (
	"errors"
	"fmt"
	"time"
)

// Stat names which aggregate a rule compares. Different stats catch very
// different failures: max spots spikes, avg spots sustained load, count
// spots a metric that has gone quiet.
type Stat string

const (
	StatAvg   Stat = "avg"
	StatMax   Stat = "max"
	StatMin   Stat = "min"
	StatCount Stat = "count"
)

// Op is the comparison a rule applies to its stat.
type Op string

const (
	OpGT  Op = ">"
	OpGTE Op = ">="
	OpLT  Op = "<"
	OpLTE Op = "<="
)

// State is where a rule currently stands.
type State string

const (
	StateOK     State = "ok"
	StateFiring State = "firing"
	// StateNoData means the metric had no observations in the rule's
	// window, so there is nothing to compare. Deliberately not folded into
	// StateOK: a service that stopped reporting must not read as healthy.
	StateNoData State = "nodata"
)

// Rule is one alerting predicate: "<metric>'s <stat> over <window> <op> <value>".
type Rule struct {
	Name   string        // unique label; keys the rule's alert state
	Metric string        // metric name to watch
	Stat   Stat          // which aggregate to compare
	Op     Op            // how to compare it
	Value  float64       // threshold
	Window time.Duration // lookback; zero means the full retention window
}

// lookback is the rule's effective window. Zero means "the default", so a
// rule that doesn't care can leave the field out.
func (r Rule) lookback() time.Duration {
	if r.Window <= 0 {
		return window
	}
	return r.Window
}

// Validate reports why a rule is unusable, or nil. Rules are defined in
// code and checked once at startup, so an invalid rule is a programming
// error that should surface immediately rather than evaluate to nonsense.
func (r Rule) Validate() error {
	if r.Name == "" {
		return errors.New("rule name is required")
	}
	if r.Metric == "" {
		return fmt.Errorf("rule %q: metric is required", r.Name)
	}

	switch r.Stat {
	case StatAvg, StatMax, StatMin, StatCount:
	default:
		return fmt.Errorf("rule %q: unknown stat %q", r.Name, r.Stat)
	}

	switch r.Op {
	case OpGT, OpGTE, OpLT, OpLTE:
	default:
		return fmt.Errorf("rule %q: unknown op %q", r.Name, r.Op)
	}

	// Beyond retention the buckets are already evicted, so the rule could
	// never be answered honestly - same limit /stats?window= enforces (§11).
	if r.Window < 0 || r.Window > window {
		return fmt.Errorf("rule %q: window %s must be >= 0 and <= %s", r.Name, r.Window, window)
	}
	return nil
}

// statValue pulls out the aggregate this rule compares. Count is widened to
// float64 so every stat goes through the same comparison path.
func (r Rule) statValue(a Agg) float64 {
	switch r.Stat {
	case StatAvg:
		return a.Sum / float64(a.Count)
	case StatMax:
		return a.Max
	case StatMin:
		return a.Min
	case StatCount:
		return float64(a.Count)
	}
	return 0 // unreachable for a validated rule
}

// breached reports whether v trips this rule's threshold.
func (r Rule) breached(v float64) bool {
	switch r.Op {
	case OpGT:
		return v > r.Value
	case OpGTE:
		return v >= r.Value
	case OpLT:
		return v < r.Value
	case OpLTE:
		return v <= r.Value
	}
	return false // unreachable for a validated rule
}

// evaluate is the pure heart of alerting: given a rule and the aggregate for
// its metric, say what state the rule is in and what value it saw.
//
// ok comes straight from mergeBuckets - false means no bucket fell inside
// the window. A zero Count is treated the same way: no observations means
// nothing to compare, not a comparison that happens to pass.
//
// No clock, no locks, no side effects. All the timing and transition
// bookkeeping lives in the Alerter, which keeps this exhaustively testable.
func evaluate(r Rule, a Agg, ok bool) (State, float64) {
	if !ok || a.Count == 0 {
		return StateNoData, 0
	}
	v := r.statValue(a)
	if r.breached(v) {
		return StateFiring, v
	}
	return StateOK, v
}
