package main

// Sharding the store's lock.
//
// DESIGN §5 measured the ceiling and named the fix: one mutex over the whole
// map serialises every ingest, so writes to *different* metrics - which touch
// entirely disjoint data - still queue behind each other. Splitting the map
// into independently-locked shards keyed by metric name lets those proceed in
// parallel.
//
// The shard is chosen by hashing the metric name, so a given metric always
// lands in the same shard and its buckets stay together under one lock.

// shardCount is how many independently-locked pieces the store is split into.
//
// A power of two, so the shard index is a mask rather than a modulo. Fixed
// rather than derived from runtime.NumCPU(): a constant keeps tests and
// benchmarks reproducible across machines, and over-sharding is cheap (an
// unused shard is an empty map and 64 bytes) while under-sharding is exactly
// the problem being solved. 32 leaves headroom above typical core counts.
const shardCount = 32

// fnv32 is FNV-1a over the metric name.
//
// Hand-rolled rather than hash/fnv because that returns an interface whose
// Write takes a []byte - converting the string would allocate, and this runs
// on the ingest hot path where record() is currently allocation-free (§23).
// FNV-1a is chosen for being cheap and well-mixed in the low bits, which is
// what a mask-based shard index reads.
func fnv32(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}

// shardIndex maps a metric name to its shard. Deterministic: the same name
// must always resolve to the same shard, or a metric's buckets would scatter
// and no single lock would cover them.
func shardIndex(metric string) int {
	return int(fnv32(metric) & (shardCount - 1))
}
