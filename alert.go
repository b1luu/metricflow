package main

// Alerting: rules that watch a metric's windowed aggregate and report a
// state. This file holds the pure, side-effect-free core - the rule shape
// and the evaluation function. Timing, transitions, and the HTTP surface
// live with the Alerter (next slice). See DESIGN.md §18.

import (
	"context"
	"errors"
	"fmt"
	"log"
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

// AlertState is where one rule currently stands. Since marks when the rule
// entered this state - it only moves on a transition, so "firing for how
// long" is answerable (and it's the field a `for` duration will need).
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
		state, value := evaluate(r, agg, ok)

		a.mu.Lock()
		st := a.states[r.Name]
		from := st.State
		changed := from != state
		if changed {
			st.State = state
			st.Since = now
		}
		st.Value = value
		a.mu.Unlock()

		// Log outside the lock: writing to stdout while holding a mutex
		// would put I/O latency into every other caller's critical path.
		if changed {
			logTransition(r, from, state, value)
		}
	}
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
