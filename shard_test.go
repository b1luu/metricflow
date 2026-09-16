package main

import (
	"fmt"
	"testing"
)

// The one property the whole scheme rests on: a metric always resolves to the
// same shard. If it didn't, a metric's buckets would scatter across shards and
// no single lock would cover them.
func TestShardIndexIsDeterministic(t *testing.T) {
	names := []string{"cpu.load", "http.latency_ms", "", "a", "metric.with.dots.and-dashes"}
	for _, n := range names {
		first := shardIndex(n)
		for i := 0; i < 100; i++ {
			if got := shardIndex(n); got != first {
				t.Fatalf("shardIndex(%q) returned %d then %d", n, first, got)
			}
		}
	}
}

func TestShardIndexIsInRange(t *testing.T) {
	for i := 0; i < 5000; i++ {
		n := fmt.Sprintf("metric.%d", i)
		if got := shardIndex(n); got < 0 || got >= shardCount {
			t.Fatalf("shardIndex(%q) = %d, outside [0,%d)", n, got, shardCount)
		}
	}
}

// shardCount must stay a power of two: the index uses a mask, which is only
// equivalent to a modulo when it is.
func TestShardCountIsAPowerOfTwo(t *testing.T) {
	if shardCount <= 0 || shardCount&(shardCount-1) != 0 {
		t.Fatalf("shardCount = %d, must be a positive power of two", shardCount)
	}
}

// Names that differ only in their last character are the realistic case -
// metric.0, metric.1, ... - and are exactly where a weak hash would pile
// everything into one shard and rebuild the bottleneck being removed.
func TestShardIndexSpreadsSimilarNames(t *testing.T) {
	const names = 2000

	counts := make([]int, shardCount)
	for i := 0; i < names; i++ {
		counts[shardIndex(fmt.Sprintf("metric.%d", i))]++
	}

	ideal := names / shardCount
	for i, c := range counts {
		if c == 0 {
			t.Errorf("shard %d got nothing; the hash is not spreading", i)
		}
		// Generous: this checks for pathology, not statistical perfection.
		if c > ideal*2 {
			t.Errorf("shard %d holds %d of %d names (ideal ~%d) - too lumpy",
				i, c, names, ideal)
		}
	}
}

// Realistic metric names share long prefixes, which is the other way a hash
// can collapse. FNV-1a mixes every byte, so the suffix still matters.
func TestShardIndexSpreadsSharedPrefixes(t *testing.T) {
	names := []string{
		"service.api.request.latency_ms",
		"service.api.request.count",
		"service.api.request.errors",
		"service.api.response.latency_ms",
		"service.worker.queue.depth",
		"service.worker.queue.latency_ms",
		"service.db.query.latency_ms",
		"service.db.query.errors",
	}

	seen := map[int]bool{}
	for _, n := range names {
		seen[shardIndex(n)] = true
	}
	// Eight names should not all collide; even a few collisions are fine.
	if len(seen) < 4 {
		t.Errorf("%d names landed in only %d shards", len(names), len(seen))
	}
}

// The hash runs on the ingest hot path, where record() is allocation-free.
// hash/fnv would have allocated here; this must not.
func TestFnv32DoesNotAllocate(t *testing.T) {
	name := "http.latency_ms"
	if got := testing.AllocsPerRun(1000, func() { _ = shardIndex(name) }); got != 0 {
		t.Errorf("shardIndex allocated %v times per call, want 0", got)
	}
}

// A known-answer test pins the hash itself, so an "optimisation" that
// silently changes the mapping shows up here rather than as mysterious
// cross-shard behaviour.
func TestFnv32KnownValues(t *testing.T) {
	cases := []struct {
		in   string
		want uint32
	}{
		{"", 2166136261},  // the offset basis, untouched
		{"a", 0xe40c292c}, // the canonical FNV-1a test vector
		{"foobar", 0xbf9cf968},
	}
	for _, c := range cases {
		if got := fnv32(c.in); got != c.want {
			t.Errorf("fnv32(%q) = %#x, want %#x", c.in, got, c.want)
		}
	}
}
