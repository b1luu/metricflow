package main

// Alert rules from a file.
//
// §18 built the alerting engine and left the rules in `defaultRules`, a Go
// function. That makes an alerting system you cannot configure: adding a rule
// for your own metric means editing source, having a Go toolchain, and
// rebuilding the server. Every other operational choice here is already
// settable from outside - the in-flight limit (§26) and the snapshot path
// (§32) - and rules are the one that most obviously belongs to whoever is
// running the thing rather than to whoever wrote it.

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// rulesPath is the file to load rules from; empty means use the built-in
// set, which keeps a server started with no configuration behaving exactly
// as it did before this existed.
func rulesPath() string { return os.Getenv("METRICFLOW_RULES") }

// maxRulesBytes bounds the file. Rules are evaluated every interval against
// every metric they name, so a file nobody meant to write is cheaper to
// refuse than to run.
const maxRulesBytes = 1 << 20 // 1 MiB

// jsonRule is the wire form of a Rule, and it exists because the two are
// not the same shape.
//
// Window and For are time.Duration, which encoding/json treats as the int64
// it is - so the honest JSON for half a minute would be 30000000000. A
// config file people are expected to write by hand has to say "30s", and
// that means a type whose fields are strings and a conversion step.
//
// Keeping it separate from Rule rather than putting UnmarshalJSON on Rule
// itself is the same call cmd/loadgen makes about statsResponse: the wire
// contract and the domain type drift for different reasons, and a change to
// one should not silently be a change to the other.
type jsonRule struct {
	Name   string  `json:"name"`
	Metric string  `json:"metric"`
	Stat   string  `json:"stat"`
	Op     string  `json:"op"`
	Value  float64 `json:"value"`
	Window string  `json:"window"`
	For    string  `json:"for"`
}

// rule converts one wire rule, leaving every other check to Rule.Validate -
// which already rejects an unknown stat, an unknown op, a window past
// retention and a negative For, and is reached by newAlerter for rules from
// a file exactly as it is for the built-in ones.
func (jr jsonRule) rule() (Rule, error) {
	r := Rule{
		Name:   jr.Name,
		Metric: jr.Metric,
		Stat:   Stat(jr.Stat),
		Op:     Op(jr.Op),
		Value:  jr.Value,
	}

	var err error
	if r.Window, err = parseRuleDuration(jr.Name, "window", jr.Window); err != nil {
		return Rule{}, err
	}
	if r.For, err = parseRuleDuration(jr.Name, "for", jr.For); err != nil {
		return Rule{}, err
	}
	return r, nil
}

// parseRuleDuration reads a Go duration, treating absent as zero.
//
// Zero is meaningful for both fields rather than being a missing value: an
// absent window means the full retention window, and an absent For means
// fire on the first breach. So "" is accepted and a malformed string is not,
// which is the distinction §7 drew for event fields.
func parseRuleDuration(rule, field, s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("rule %q: %s %q is not a duration (use e.g. 30s, 1m)",
			rule, field, s)
	}
	return d, nil
}

// loadRules reads and converts a rules file.
//
// Unknown fields are refused. Without that, "windwo": "30s" would be
// discarded in silence and the rule would run with a zero window - which
// means the full retention window, a legal value, so nothing downstream
// could notice. A typo that quietly widens an alert's lookback is precisely
// the kind of misconfiguration §18 refuses to start on.
func loadRules(path string) ([]Rule, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	if info.Size() > maxRulesBytes {
		return nil, fmt.Errorf("%s is %d bytes, over the %d-byte limit",
			path, info.Size(), maxRulesBytes)
	}

	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()

	var wire []jsonRule
	if err := dec.Decode(&wire); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	// A second value in the file is a file that means something other than
	// what it says, so it is refused rather than half-read.
	if dec.More() {
		return nil, fmt.Errorf("parsing %s: unexpected data after the rule list", path)
	}

	rules := make([]Rule, 0, len(wire))
	for _, jr := range wire {
		r, err := jr.rule()
		if err != nil {
			return nil, err
		}
		rules = append(rules, r)
	}
	return rules, nil
}

// watchesItself reports whether any rule looks at the server's own metrics.
//
// A rules file replaces the built-in set rather than adding to it, which is
// predictable but has one trap: the two rules that watch the server itself
// (§31) disappear with everything else, and nothing about the server's
// behaviour would tell you. This is what makes that sayable at startup.
func watchesItself(rules []Rule) bool {
	for _, r := range rules {
		if strings.HasPrefix(r.Metric, selfPrefix) {
			return true
		}
	}
	return false
}
