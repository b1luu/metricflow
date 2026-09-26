package main

// Alerting: rules that watch a metric's windowed aggregate and report a
// state. This file holds the pure, side-effect-free core - the rule shape
// and the evaluation function. Timing, transitions, and the HTTP surface
// live with the Alerter (next slice). See DESIGN.md §18.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
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
	// Percentiles (§23). These are usually the right choice for a latency
	// rule: max fires on a single unlucky request, avg is dragged around by
	// the bulk and describes neither mode, but "p99 over 500ms" is the
	// question an on-call engineer actually asks.
	StatP50 Stat = "p50"
	StatP90 Stat = "p90"
	StatP99 Stat = "p99"

	// StatIncrease is max - min across the window, which is what a
	// cumulative counter means (§31). The server's own telemetry is
	// recorded that way, and without this stat none of it could be alerted
	// on: max of a counter only ever rises, so a rule on it fires once and
	// stays firing for the life of the process.
	//
	// It is meaningless on a gauge, where max and min are just the extremes
	// of a fluctuating value - but so is p99 on a counter. Which stat suits
	// which metric is the rule author's business, as it already was.
	StatIncrease Stat = "increase"
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
	// StatePending means the threshold is breached but has not held for the
	// rule's For duration yet. It is deliberately pre-announcement: visible
	// at /alerts, but it produces no log line, which is the whole point of
	// For - a metric flapping across the threshold never announces anything.
	StatePending State = "pending"
	// StateNoData means the metric had no observations in the rule's
	// window, so there is nothing to compare. Deliberately not folded into
	// StateOK: a service that stopped reporting must not read as healthy.
	StateNoData State = "nodata"
)

// Rule is one alerting predicate: "<metric>'s <stat> over <window> <op> <value>",
// optionally required to hold for a while before it counts.
type Rule struct {
	Name   string        // unique label; keys the rule's alert state
	Metric string        // metric name to watch
	Stat   Stat          // which aggregate to compare
	Op     Op            // how to compare it
	Value  float64       // threshold
	Window time.Duration // lookback; zero means the full retention window
	For    time.Duration // how long the breach must hold; zero fires at once
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
	case StatAvg, StatMax, StatMin, StatCount, StatP50, StatP90, StatP99, StatIncrease:
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

	// For has no upper bound: it counts how long a breach has persisted
	// across evaluations, which is unrelated to how much data is retained.
	if r.For < 0 {
		return fmt.Errorf("rule %q: for %s must be >= 0", r.Name, r.For)
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
	case StatIncrease:
		// A counter only rises, so the span of values seen inside the
		// window is the amount it rose by. A missed sample costs accuracy
		// here but never correctness, which is why the counters are
		// recorded cumulatively rather than as deltas (§31).
		return a.Max - a.Min
	case StatMin:
		return a.Min
	case StatCount:
		return float64(a.Count)
	case StatP50:
		return a.h.quantile(0.50)
	case StatP90:
		return a.h.quantile(0.90)
	case StatP99:
		return a.h.quantile(0.99)
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

// applyFor folds the instantaneous verdict from evaluate into the durable
// state machine, holding a fresh breach in StatePending until it has lasted
// r.For. current and since are the rule's stored state.
//
// Pure, like evaluate: everything it needs is an argument, so the whole
// ok -> pending -> firing walk is table-testable without a clock or a lock.
//
//	           breach                held for For
//	ok/nodata ────────> pending ──────────────────> firing
//	    ^                  │                           │
//	    └──────────────────┴───────────────────────────┘
//	                 breach ends
func applyFor(r Rule, now time.Time, current State, since time.Time, want State) State {
	// Only a breach can be held back, and only if the rule asks for it.
	if want != StateFiring || r.For <= 0 {
		return want
	}

	switch current {
	case StateFiring:
		return StateFiring // already promoted; For is spent
	case StatePending:
		// since is when the breach began, because pending is only ever
		// entered at that moment.
		if now.Sub(since) >= r.For {
			return StateFiring
		}
		return StatePending
	default: // ok or nodata - the breach starts now
		return StatePending
	}
}

// AlertState is where one rule currently stands. Since marks when the rule
// entered this state - it only moves on a transition, so "firing for how
// long" is answerable, and it doubles as "breaching since" while pending.
type AlertState struct {
	Rule   string    `json:"rule"`
	Metric string    `json:"metric"`
	State  State     `json:"state"`
	Value  float64   `json:"value"` // observed at the last evaluation
	Since  time.Time `json:"since"`
}

// Alerter evaluates a fixed set of rules against a Store on demand.
// It reads the Store and never the reverse: aggregation knows nothing about
// alerting. Its own mutex guards only its own state.
type Alerter struct {
	mu     sync.Mutex
	states map[string]*AlertState // by rule name

	store *Store // read-only from here
	rules []Rule // fixed after construction, so needs no lock
}

// newAlerter validates every rule up front and seeds each one's state.
// Rules are defined in code, so a bad rule is a startup failure, not a
// surprise at evaluation time.
func newAlerter(now time.Time, s *Store, rules []Rule) (*Alerter, error) {
	states := make(map[string]*AlertState, len(rules))
	for _, r := range rules {
		if err := r.Validate(); err != nil {
			return nil, err
		}
		// Name keys the state map, so a duplicate would silently make two
		// rules share one state instead of failing loudly.
		if _, dup := states[r.Name]; dup {
			return nil, fmt.Errorf("duplicate rule name %q", r.Name)
		}
		states[r.Name] = &AlertState{
			Rule:   r.Name,
			Metric: r.Metric,
			State:  StateNoData, // nothing observed yet, which is the truth
			Since:  now,
		}
	}
	return &Alerter{states: states, store: s, rules: rules}, nil
}

// evaluateAll re-checks every rule against the store and logs the ones that
// changed state. Steady state is silent: only transitions are events, so a
// firing rule logs once, not on every tick.
func (a *Alerter) evaluateAll(now time.Time) {
	for _, r := range a.rules {
		// Read the store outside a.mu - it has its own lock, and nesting
		// the two would be a deadlock waiting to happen.
		agg, ok := a.store.aggFor(r.Metric, windowStart(now, r.lookback()))
		want, value := evaluate(r, agg, ok)

		a.mu.Lock()
		st := a.states[r.Name]
		from := st.State
		state := applyFor(r, now, st.State, st.Since, want)
		changed := from != state
		if changed {
			st.State = state
			st.Since = now
		}
		st.Value = value
		a.mu.Unlock()

		// Log outside the lock: writing to stdout while holding a mutex
		// would put I/O latency into every other caller's critical path.
		if changed && announce(from, state) {
			logTransition(r, from, state, value)
		}
	}
}

// announce reports whether a transition is worth notifying about.
//
// Pending is pre-announcement: entering it says nothing, and leaving it only
// matters if the breach actually survived For and became firing. Suppressing
// pending -> ok is the point of the feature - a metric flapping across the
// threshold produces no notification at all, rather than a fire/resolve pair
// every tick. The consequence to accept is that pending -> nodata is silent
// too; that state is still visible at /alerts.
func announce(from, to State) bool {
	if to == StatePending {
		return false
	}
	if from == StatePending {
		return to == StateFiring
	}
	return true
}

// logTransition is, for now, the whole notification story - and the single
// seam a webhook or pager would plug into later. There's no Notifier
// interface yet because there's only one implementation to hide behind it.
func logTransition(r Rule, from, to State, value float64) {
	if to == StateNoData {
		log.Printf("ALERT [%s] %s -> %s: %s has no data in %s",
			r.Name, from, to, r.Metric, r.lookback())
		return
	}
	log.Printf("ALERT [%s] %s -> %s: %s %s=%g (threshold %s %g)",
		r.Name, from, to, r.Metric, r.Stat, value, r.Op, r.Value)
}

// evalInterval is how often the alerter re-checks its rules: one bucket
// width, so detection lag stays inside a single bucket of the data's own
// resolution. Evaluation costs O(rules) per tick regardless of event rate,
// so this is cheap - but like §8's bucket width it's a sensible default,
// not a tuned value.
const evalInterval = bucketWidth

// Run re-evaluates every rule on a ticker until ctx is cancelled. every is a
// parameter rather than the constant so tests can drive it far faster than
// real time, the same way run() takes a listener instead of an address.
//
// The ticker's own timestamp is used as `now`: it is the honest moment the
// evaluation belongs to, and it saves a redundant clock read.
func (a *Alerter) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			a.evaluateAll(now)
		}
	}
}

// Snapshot copies out the current state of every rule, ordered by rule name
// so callers (and tests) get a stable listing rather than map order.
func (a *Alerter) Snapshot() []AlertState {
	a.mu.Lock()
	defer a.mu.Unlock()

	out := make([]AlertState, 0, len(a.states))
	for _, st := range a.states {
		out = append(out, *st) // copy: callers must not hold our pointers
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Rule < out[j].Rule })
	return out
}

// AlertsResponse is the JSON body of GET /alerts. An object rather than a
// bare array so fields can be added later without breaking every caller's
// parser.
type AlertsResponse struct {
	Alerts []AlertState `json:"alerts"`
}

// handleAlerts: GET /alerts - the current state of every configured rule.
//
// It reads a snapshot; it does not evaluate. Evaluation is the ticker's job
// (§18), which keeps this a cheap read: polling it hard costs a lock and a
// copy, never a full re-evaluation of every rule.
func (a *Alerter) handleAlerts(w http.ResponseWriter, r *http.Request) {
	// Snapshot() always returns a non-nil slice, so an empty rule set
	// encodes as [] rather than null.
	resp := AlertsResponse{Alerts: a.Snapshot()}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp) // see §17: nothing to do on failure
}
