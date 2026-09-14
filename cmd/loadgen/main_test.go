package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseFlagsDefaults(t *testing.T) {
	c, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.target == "" || c.workers < 1 || c.duration <= 0 || c.metrics < 1 {
		t.Errorf("defaults are not runnable: %+v", c)
	}
	if !c.verify {
		t.Error("verify should default on: the cross-check is the point")
	}
}

func TestParseFlagsOverrides(t *testing.T) {
	c, err := parseFlags([]string{
		"-target", "http://example:9999",
		"-workers", "32",
		"-duration", "2s",
		"-metrics", "7",
		"-bad", "0.25",
		"-verify=false",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := config{
		target: "http://example:9999", workers: 32,
		duration: 2 * time.Second, metrics: 7, badFrac: 0.25, verify: false,
	}
	if c != want {
		t.Errorf("got %+v, want %+v", c, want)
	}
}

func TestParseFlagsRejectsNonsense(t *testing.T) {
	cases := []struct {
		name, wantMsg string
		args          []string
	}{
		{"no workers", "workers", []string{"-workers", "0"}},
		{"zero duration", "duration", []string{"-duration", "0"}},
		{"no metrics", "metrics", []string{"-metrics", "0"}},
		{"bad fraction over one", "bad", []string{"-bad", "1.5"}},
		{"negative bad fraction", "bad", []string{"-bad", "-0.1"}},
		{"empty target", "target", []string{"-target", ""}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseFlags(c.args)
			if err == nil {
				t.Fatalf("parseFlags(%v) = nil error, want one", c.args)
			}
			if !strings.Contains(err.Error(), c.wantMsg) {
				t.Errorf("error = %q, want it to mention %q", err, c.wantMsg)
			}
		})
	}
}
