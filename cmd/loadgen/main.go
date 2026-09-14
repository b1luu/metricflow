// Command loadgen drives a running MetricFlow server over real HTTP.
//
// It complements the in-process benchmarks rather than replacing them
// (DESIGN §19): benchmarks are the reproducible measurement of the engine,
// this measures the whole path - sockets, net/http, JSON - and can make one
// claim they cannot, because it crosses a real network boundary: the number
// of events the server *recorded* equals the number of requests it *accepted*.
//
// It is deliberately a separate main package with no access to the server's
// internals. A load generator that linked against the thing it measures
// could accidentally test the wrong side of the wire.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

type config struct {
	target   string        // base URL of the server
	workers  int           // concurrent request senders
	duration time.Duration // how long to send for
	metrics  int           // how many distinct metric names to spread across
	badFrac  float64       // fraction of requests that should be rejectable
	verify   bool          // cross-check the server's count afterwards
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)

	var c config
	fs.StringVar(&c.target, "target", "http://localhost:8080", "base URL of the MetricFlow server")
	fs.IntVar(&c.workers, "workers", 8, "concurrent request senders")
	fs.DurationVar(&c.duration, "duration", 10*time.Second, "how long to send for")
	fs.IntVar(&c.metrics, "metrics", 4, "number of distinct metric names to spread load across")
	fs.Float64Var(&c.badFrac, "bad", 0, "fraction of requests made deliberately invalid (0..1)")
	fs.BoolVar(&c.verify, "verify", true, "after the run, check the server recorded exactly what it accepted")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	switch {
	case c.target == "":
		return config{}, fmt.Errorf("-target is required")
	case c.workers < 1:
		return config{}, fmt.Errorf("-workers must be >= 1, got %d", c.workers)
	case c.duration <= 0:
		return config{}, fmt.Errorf("-duration must be > 0, got %s", c.duration)
	case c.metrics < 1:
		return config{}, fmt.Errorf("-metrics must be >= 1, got %d", c.metrics)
	case c.badFrac < 0 || c.badFrac > 1:
		return config{}, fmt.Errorf("-bad must be between 0 and 1, got %v", c.badFrac)
	}
	return c, nil
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(2)
	}

	fmt.Printf("target=%s workers=%d duration=%s metrics=%d bad=%.0f%% verify=%v\n",
		cfg.target, cfg.workers, cfg.duration, cfg.metrics, cfg.badFrac*100, cfg.verify)
}
