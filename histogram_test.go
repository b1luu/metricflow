package main

import (
	"math"
	"testing"
)

// The whole guarantee in one test: whatever magnitude you feed in, the
// value that comes back out is within relAccuracy of it. This is what
// justifies log-bucketing over the linear histogram loadgen uses - it holds
// at 0.001 and at 1e9 alike, which no fixed bucket width can manage.
func TestBucketRelativeErrorIsBounded(t *testing.T) {
	magnitudes := []float64{
		1e-9, 1e-6, 0.001, 0.05, 0.5, 1, 1.5, 9.99,
		42, 100, 512.5, 9999, 1e6, 1.23e9,
	}
	for _, v := range magnitudes {
		got := bucketValue(bucketIndex(v))
		relErr := math.Abs(got-v) / v
		if relErr > relAccuracy {
			t.Errorf("value %g -> bucket %d -> %g: relative error %.4f exceeds %.4f",
				v, bucketIndex(v), got, relErr, relAccuracy)
		}
	}
}

// Bucketing must be monotonic, or quantiles computed by walking buckets in
// index order would be walking them out of value order.
func TestBucketIndexIsMonotonic(t *testing.T) {
	prev := bucketIndex(minMagnitude)
	for v := minMagnitude * 2; v < 1e9; v *= 1.7 {
		got := bucketIndex(v)
		if got < prev {
			t.Fatalf("bucketIndex(%g) = %d, went backwards from %d", v, got, prev)
		}
		prev = got
	}
}

// Values close together should share a bucket (that's the compression);
// values far apart must not (that's the accuracy).
func TestBucketIndexSeparatesByMagnitude(t *testing.T) {
	if bucketIndex(100) != bucketIndex(100.5) {
		t.Errorf("100 and 100.5 are within 1%% but landed in different buckets")
	}
	if bucketIndex(100) == bucketIndex(200) {
		t.Errorf("100 and 200 collapsed into one bucket; accuracy would be 100%%")
	}
}

// bucketValue must invert bucketIndex: feeding a bucket's own representative
// back in has to return the same bucket, or merging and re-reading would
// drift a little further every time.
func TestBucketValueRoundTrips(t *testing.T) {
	for i := int32(-400); i <= 400; i += 7 {
		v := bucketValue(i)
		if got := bucketIndex(v); got != i {
			t.Errorf("bucket %d -> value %g -> bucket %d", i, v, got)
		}
	}
}
