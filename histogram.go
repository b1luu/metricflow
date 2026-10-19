package main

// Percentiles. DESIGN §1 has always said the store keeps conclusions rather
// than events, and named the price: no percentiles, because those need the
// distribution. This file buys them back without keeping the events.
//
// The technique is log-bucketing (the idea behind DDSketch): bucket a value
// by the logarithm of its magnitude, so bucket widths grow with the value
// and the *relative* error stays bounded everywhere. A linear histogram
// can't do that - loadgen's (§19) works only because request latency has a
// known, narrow range. A general metrics engine sees cpu.load near 1 and
// latency near 10000 in the same process, and a bucket width that suits one
// is useless for the other.

import (
	"math"
	"slices"
)

// relAccuracy is the relative error the bucketing guarantees: a reported
// quantile is within 1% of the true value, at any magnitude. Smaller is
// more accurate and uses more buckets; 1% is far finer than anything an
// alert threshold cares about.
const relAccuracy = 0.01

// gamma is the ratio between consecutive bucket boundaries. Chosen so the
// worst-case relative error of a bucket's representative value is exactly
// relAccuracy - see bucketValue.
var gamma = (1 + relAccuracy) / (1 - relAccuracy)

// logGamma is precomputed: bucketIndex runs on the ingest hot path, and a
// division is cheaper than a second math.Log.
var logGamma = math.Log(gamma)

// minMagnitude is the smallest magnitude that gets its own bucket. Below
// this, log-bucketing would need unboundedly many buckets to describe
// values that are indistinguishable from zero for any real metric, so they
// are counted as zero instead.
const minMagnitude = 1e-9

// bucketIndex maps a positive magnitude to its bucket. Values in
// (gamma^(i-1), gamma^i] share bucket i, so the buckets get wider as the
// values get bigger and the relative error stays flat.
func bucketIndex(magnitude float64) int32 {
	return int32(math.Ceil(math.Log(magnitude) / logGamma))
}

// bucketValue is the magnitude reported for bucket i: the representative
// that minimises worst-case relative error across the bucket.
//
// For a bucket covering (gamma^(i-1), gamma^i], choosing 2*gamma^i/(gamma+1)
// puts the error at exactly (gamma-1)/(gamma+1) = relAccuracy at both edges,
// which is the best any single representative can do.
func bucketValue(i int32) float64 {
	return 2 * math.Pow(gamma, float64(i)) / (gamma + 1)
}

// hist is a sparse, mergeable distribution.
//
// Sparse rather than a fixed array because a real metric's values sit in a
// narrow band: cpu.load occupies a few dozen buckets, not the ~1400 that
// spanning 1e-9..1e9 at 1% accuracy would need. Memory therefore follows
// what actually arrived, not what theoretically could.
//
// Negatives get their own map keyed by magnitude, rather than being
// rejected. §4 already establishes that all-negative metrics are a case
// this project supports, and log-bucketing has no answer for a negative
// input otherwise.
type hist struct {
	pos   map[int32]int64 // buckets for values >= minMagnitude
	neg   map[int32]int64 // buckets for values <= -minMagnitude, keyed by |v|
	zeros int64           // |v| < minMagnitude
	count int64
}

// add records one observation. The maps are created on first use, so a
// metric that never reports a negative never allocates a negative map.
// maxHistBuckets caps how many buckets one sign of one histogram may hold
// (§47).
//
// Without it, the only bound is the float64 range: bucketIndex spans
// -1036 to 35488, so 36 525 buckets per sign are reachable by a client
// reporting values across the whole range. §27 capped the number of metric
// names and treated that as the memory bound, but what a name costs
// depends on how widely its values are spread - a thirty-fold range the
// cap has no opinion about.
//
// 1024 covers gamma^1024, about nine orders of magnitude. The number was
// chosen by asking what real metrics need rather than for roundness:
// microseconds to ten seconds is 806 buckets, milliseconds to seconds is
// 346. 512 was the first attempt and a test caught it - a 1us..10s latency
// metric hit the cap exactly, which is precisely the data this has to keep
// exact.
//
// It bounds the pathological case rather than making the store small: a
// client spreading values across the whole float64 range could otherwise
// reach 36 525 buckets in one histogram. Full-store memory is still
// dominated by how many metrics exist times how wide their values are
// (§47), and the lever for that is §27's cardinality cap.
const maxHistBuckets = 1024

func (h *hist) add(v float64) {
	h.count++
	switch {
	case v >= minMagnitude:
		if h.pos == nil {
			h.pos = make(map[int32]int64)
		}
		h.pos[h.fit(h.pos, bucketIndex(v))]++
	case v <= -minMagnitude:
		if h.neg == nil {
			h.neg = make(map[int32]int64)
		}
		h.neg[h.fit(h.neg, bucketIndex(-v))]++
	default:
		h.zeros++
	}
}

// fit returns the bucket i belongs in, collapsing the lowest buckets first
// if the map is full.
//
// Which end to sacrifice is the whole decision. Percentiles are asked for
// to find out how bad the slow end is - §18's rules watch p99 and §23
// exists because a max fires on one unlucky request while a p99 describes
// a real fraction of users. Collapsing the *highest* buckets would blur
// exactly the number everything else is built to read. Collapsing the
// lowest blurs the fastest requests, which nobody alerts on.
//
// This is DDSketch's collapsingLowest, and it keeps the promise §23 makes
// where it matters: every bucket above the floor is still accurate to 1%,
// and the floor only rises for a metric spanning more than 30 000 to one.
func (h *hist) fit(buckets map[int32]int64, i int32) int32 {
	if _, exists := buckets[i]; exists || len(buckets) < maxHistBuckets {
		return i
	}

	// Full, and i is new. Find the two lowest and fold one into the other,
	// which frees the slot i is about to take.
	lowest, second := int32(0), int32(0)
	first := true
	for b := range buckets {
		switch {
		case first:
			lowest, second, first = b, b, false
		case b < lowest:
			lowest, second = b, lowest
		case b < second || second == lowest:
			second = b
		}
	}

	// A value below everything already held becomes the new floor, so the
	// floor is where it belongs rather than above the data.
	if i < lowest {
		return lowest
	}
	buckets[second] += buckets[lowest]
	delete(buckets, lowest)
	return i
}

// merge folds o into h. Merging is exact - bucket counts simply add - which
// is the property that lets a windowed query combine several time buckets
// without any loss beyond the bucketing already applied (§8).
func (h *hist) merge(o *hist) {
	if o == nil {
		return
	}
	for i, n := range o.pos {
		if h.pos == nil {
			h.pos = make(map[int32]int64, len(o.pos))
		}
		h.pos[i] += n
	}
	for i, n := range o.neg {
		if h.neg == nil {
			h.neg = make(map[int32]int64, len(o.neg))
		}
		h.neg[i] += n
	}
	h.zeros += o.zeros
	h.count += o.count
}

// quantile returns the value below which q of the observations fall, within
// relAccuracy.
func (h *hist) quantile(q float64) float64 {
	return h.quantiles(q)[0]
}

// quantiles answers several quantiles in one pass. qs must be ascending.
//
// This exists because /stats asks for three at once, and answering them
// separately would sort the bucket keys three times per metric. The cost is
// dominated by that sort, so the pair BenchmarkHistQuantile /
// BenchmarkHistQuantilesTogether measures 31µs for one and 30µs for three -
// three quantiles for the price of one.
//
// The walk is in value order: negatives from most negative up (so
// descending magnitude), then zeros, then positives ascending.
func (h *hist) quantiles(qs ...float64) []float64 {
	out := make([]float64, len(qs))
	if h == nil || h.count == 0 || len(qs) == 0 {
		return out
	}

	ranks := make([]int64, len(qs))
	for i, q := range qs {
		r := int64(q * float64(h.count))
		if r >= h.count {
			r = h.count - 1 // q >= 1 asks for the largest observation
		}
		if r < 0 {
			r = 0
		}
		ranks[i] = r
	}

	var seen int64
	next := 0 // the next quantile still waiting for an answer

	// emit fills in every pending quantile that this bucket satisfies.
	// Because qs ascend, once one is satisfied the earlier ones already are.
	emit := func(v float64) {
		for next < len(qs) && seen > ranks[next] {
			out[next] = v
			next++
		}
	}

	negIdx := sortedBuckets(h.neg)
	for i := len(negIdx) - 1; i >= 0; i-- {
		seen += h.neg[negIdx[i]]
		if emit(-bucketValue(negIdx[i])); next == len(qs) {
			return out
		}
	}

	seen += h.zeros
	if emit(0); next == len(qs) {
		return out
	}

	for _, i := range sortedBuckets(h.pos) {
		seen += h.pos[i]
		if emit(bucketValue(i)); next == len(qs) {
			return out
		}
	}

	return out
}

// sortedBuckets returns a map's bucket indices in ascending order. Map
// iteration order is random, and a quantile walked in random order would be
// a different (wrong) number every call.
func sortedBuckets(m map[int32]int64) []int32 {
	if len(m) == 0 {
		return nil
	}
	out := make([]int32, 0, len(m))
	for i := range m {
		out = append(out, i)
	}
	// slices.Sort rather than sort.Slice: sort.Slice takes the slice as an
	// interface and builds a reflect-based swapper, which allocates twice
	// per call. That is invisible on one histogram and is not invisible on
	// a /stats over a thousand metrics, where it was a fifth of every
	// object the query path allocated.
	slices.Sort(out)
	return out
}
