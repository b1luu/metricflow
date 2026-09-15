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

import "math"

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
