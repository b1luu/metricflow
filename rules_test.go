package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeRules puts a rules file in a temp dir and returns its path.
func writeRules(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rules.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// The whole point: a rule an operator wrote, loaded and usable without a Go
// toolchain anywhere in sight.
func TestRulesLoadFromAFile(t *testing.T) {
	path := writeRules(t, `[
	  {"name":"disk-full","metric":"disk.used_pct","stat":"max","op":">",
	   "value":90,"window":"30s","for":"1m"},
	  {"name":"slow","metric":"http.latency_ms","stat":"p99","op":">","value":500}
	]`)

	rules, err := loadRules(path)
	if err != nil {
		t.Fatalf("loading a valid file: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("loaded %d rules, want 2", len(rules))
	}

	got := rules[0]
	want := Rule{
		Name: "disk-full", Metric: "disk.used_pct", Stat: StatMax, Op: OpGT,
		Value: 90, Window: 30 * time.Second, For: time.Minute,
	}
	if got != want {
		t.Errorf("rule[0] = %+v, want %+v", got, want)
	}

	// An absent window and For are zero, which the engine reads as "the
	// full retention window" and "fire at once" rather than as missing.
	if rules[1].Window != 0 || rules[1].For != 0 {
		t.Errorf("rule[1] window = %s, for = %s; both should be zero when absent",
			rules[1].Window, rules[1].For)
	}
}

// Durations are written the way a human writes them. The struct field is an
// int64 of nanoseconds, so without the conversion step half a minute would
// have to be spelled 30000000000 in the file.
func TestRuleDurationsAreWrittenAsDurations(t *testing.T) {
	path := writeRules(t, `[{"name":"r","metric":"m","stat":"avg","op":">",
	  "value":1,"window":"90s","for":"2m30s"}]`)

	rules, err := loadRules(path)
	if err != nil {
		t.Fatal(err)
	}
	if rules[0].Window != 90*time.Second {
		t.Errorf("window = %s, want 90s", rules[0].Window)
	}
	if rules[0].For != 150*time.Second {
		t.Errorf("for = %s, want 2m30s", rules[0].For)
	}

	// And a duration that is not one is refused rather than silently zero.
	bad := writeRules(t, `[{"name":"r","metric":"m","stat":"avg","op":">",
	  "value":1,"window":"half a minute"}]`)
	if _, err := loadRules(bad); err == nil {
		t.Error("a malformed duration loaded without complaint")
	}
}

// The reason DisallowUnknownFields is set. A mistyped field name would
// otherwise be discarded in silence, leaving a rule with a zero window -
// which is a legal value meaning the full retention window, so nothing
// downstream could ever notice the typo.
func TestAMistypedFieldIsRefusedNotIgnored(t *testing.T) {
	path := writeRules(t, `[{"name":"r","metric":"m","stat":"avg","op":">",
	  "value":1,"windwo":"30s"}]`)

	_, err := loadRules(path)
	if err == nil {
		t.Fatal("a mistyped field name was accepted; the rule would run with " +
			"the full retention window and nothing would say so")
	}
	if !strings.Contains(err.Error(), "windwo") {
		t.Errorf("error = %q; it should name the field it did not recognise", err)
	}
}

// Rules from a file reach the same validation as rules from code - that is
// the reason jsonRule converts rather than validating anything itself.
func TestRulesFromAFileAreValidatedLikeAnyOther(t *testing.T) {
	cases := []struct {
		why  string
		body string
	}{
		{"unknown stat", `[{"name":"r","metric":"m","stat":"median","op":">","value":1}]`},
		{"unknown op", `[{"name":"r","metric":"m","stat":"avg","op":"=~","value":1}]`},
		{"no name", `[{"metric":"m","stat":"avg","op":">","value":1}]`},
		{"no metric", `[{"name":"r","stat":"avg","op":">","value":1}]`},
		{"window past retention", `[{"name":"r","metric":"m","stat":"avg","op":">",
		   "value":1,"window":"10m"}]`},
		{"negative for", `[{"name":"r","metric":"m","stat":"avg","op":">",
		   "value":1,"for":"-5s"}]`},
		{"duplicate names", `[{"name":"r","metric":"a","stat":"avg","op":">","value":1},
		   {"name":"r","metric":"b","stat":"avg","op":">","value":1}]`},
	}

	for _, c := range cases {
		path := writeRules(t, c.body)
		rules, err := loadRules(path)
		if err != nil {
			continue // rejected at conversion, which is just as good
		}
		// Otherwise it has to be rejected by the alerter, which is the
		// path the running server takes.
		if _, err := newAlerter(time.Now(), newStore(), rules); err == nil {
			t.Errorf("%s: loaded and built an alerter without complaint", c.why)
		}
	}
}

// Malformed JSON, trailing data and an oversized file are all refused.
func TestAnUnusableRulesFileIsRefused(t *testing.T) {
	cases := map[string]string{
		"not json":      `{"this": is not valid`,
		"not a list":    `{"name":"r","metric":"m","stat":"avg","op":">","value":1}`,
		"trailing data": `[] [{"name":"r"}]`,
		"trailing junk": `[{"name":"r","metric":"m","stat":"avg","op":">","value":1}] oops`,
	}
	for why, body := range cases {
		if _, err := loadRules(writeRules(t, body)); err == nil {
			t.Errorf("%s: accepted", why)
		}
	}

	if _, err := loadRules(filepath.Join(t.TempDir(), "does-not-exist.json")); err == nil {
		t.Error("a missing rules file was accepted")
	}

	big := writeRules(t, "["+strings.Repeat(`{"name":"x","metric":"m","stat":"avg","op":">","value":1},`,
		40000)+"]")
	if _, err := loadRules(big); err == nil {
		t.Error("a file over the size limit was accepted")
	}
}

// An empty list is allowed - a deployment may genuinely want no alerting -
// but the caller has to be able to tell, which is what makes the warning in
// run() possible.
func TestAnEmptyRuleListLoadsAsEmpty(t *testing.T) {
	rules, err := loadRules(writeRules(t, `[]`))
	if err != nil {
		t.Fatalf("an empty list should load: %v", err)
	}
	if len(rules) != 0 {
		t.Errorf("loaded %d rules from an empty list", len(rules))
	}
}

// A file replaces the built-in set, so §31's self-watching rules vanish with
// it. watchesItself is what lets startup say so.
func TestWatchesItselfDetectsTheReservedNamespace(t *testing.T) {
	if watchesItself([]Rule{{Metric: "cpu.load"}, {Metric: "disk.used_pct"}}) {
		t.Error("claimed self-watching for rules that only watch client metrics")
	}
	if !watchesItself([]Rule{{Metric: "cpu.load"}, {Metric: selfShed}}) {
		t.Error("did not notice a rule on " + selfShed)
	}
	// The built-in set must keep passing its own check, or the warning
	// would fire on a server nobody configured.
	if !watchesItself(defaultRules()) {
		t.Error("the built-in rules do not watch the server itself")
	}
}

// --- the wiring ---

// The loader being right is not the same as the server using it. A running
// server configured with a rules file must serve those rules and not the
// built-in ones.
func TestAConfiguredServerServesItsOwnRules(t *testing.T) {
	path := writeRules(t, `[
	  {"name":"operator-wrote-this","metric":"disk.used_pct","stat":"max",
	   "op":">","value":90},
	  {"name":"watches-the-server","metric":"metricflow.requests.shed",
	   "stat":"increase","op":">","value":0}
	]`)
	t.Setenv("METRICFLOW_RULES", path)

	addr, stop := runUntil(t, filepath.Join(t.TempDir(), "snap.bin"))
	defer stop()

	resp, err := testClient.Get("http://" + addr + "/alerts")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var got AlertsResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}

	names := map[string]bool{}
	for _, a := range got.Alerts {
		names[a.Rule] = true
	}

	for _, want := range []string{"operator-wrote-this", "watches-the-server"} {
		if !names[want] {
			t.Errorf("%q is not in /alerts; the server is not using the file", want)
		}
	}
	// And the built-in set is gone, because a file replaces rather than extends.
	for _, gone := range []string{"cpu-hot", "cpu-spike", "slow-requests"} {
		if names[gone] {
			t.Errorf("built-in rule %q survived a configured rules file", gone)
		}
	}
	if len(got.Alerts) != 2 {
		t.Errorf("/alerts reports %d rules, want the 2 in the file", len(got.Alerts))
	}
}

// A bad rules file stops startup. It is a configuration error, found before
// anything has been served, and an operator who mistyped a threshold should
// not learn about it from the alert that never fired.
func TestABadRulesFileStopsStartup(t *testing.T) {
	t.Setenv("METRICFLOW_RULES", writeRules(t, `[{"name":"r","metric":"m",
	  "stat":"median","op":">","value":1}]`))
	t.Setenv("METRICFLOW_SNAPSHOT", "")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errc := make(chan error, 1)
	go func() { errc <- run(ctx, ln) }()

	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("the server started with an unusable rules file")
		}
		if !strings.Contains(err.Error(), "alert rules") {
			t.Errorf("error = %q; it should say the rules were the problem", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the server neither started nor failed")
	}
}

// With no file configured, nothing changes: the built-in rules are used, so
// a server nobody configured behaves exactly as it did before this existed.
func TestNoRulesFileMeansTheBuiltInRules(t *testing.T) {
	t.Setenv("METRICFLOW_RULES", "")

	addr, stop := runUntil(t, filepath.Join(t.TempDir(), "snap.bin"))
	defer stop()

	resp, err := testClient.Get("http://" + addr + "/alerts")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var got AlertsResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.Alerts) != len(defaultRules()) {
		t.Errorf("/alerts reports %d rules, want the %d built-in ones",
			len(got.Alerts), len(defaultRules()))
	}
}
