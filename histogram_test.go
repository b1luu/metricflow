package main

import (
	"math"
	"testing"
	"time"
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

// --- hist: storage and quantiles ---

// within reports whether got is inside the accuracy guarantee of want.
func within(got, want float64) bool {
	if want == 0 {
		return got == 0
	}
	return math.Abs(got-want)/math.Abs(want) <= relAccuracy
}

func TestHistQuantilesOverAUniformRange(t *testing.T) {
	var h hist
	for i := 1; i <= 1000; i++ {
		h.add(float64(i))
	}

	cases := []struct{ q, want float64 }{
		{0.00, 1},
		{0.50, 501},
		{0.90, 901},
		{0.99, 991},
		{1.00, 1000},
	}
	for _, c := range cases {
		if got := h.quantile(c.q); !within(got, c.want) {
			t.Errorf("quantile(%.2f) = %g, want %g within %.0f%%",
				c.q, got, c.want, relAccuracy*100)
		}
	}
	if h.count != 1000 {
		t.Errorf("count = %d, want 1000", h.count)
	}
}

// The reason for log buckets: accuracy has to hold at every magnitude, not
// just the one the buckets were sized for.
func TestHistAccuracyAcrossMagnitudes(t *testing.T) {
	for _, scale := range []float64{1e-6, 0.01, 1, 1000, 1e8} {
		var h hist
		for i := 1; i <= 100; i++ {
			h.add(scale * float64(i))
		}
		want := scale * 51
		if got := h.quantile(0.5); !within(got, want) {
			t.Errorf("scale %g: median = %g, want %g", scale, got, want)
		}
	}
}

// Negative values are a supported case (§4), so they need real ordering:
// the median of -100..-1 is a negative number near the middle, not zero.
func TestHistHandlesNegatives(t *testing.T) {
	var h hist
	for i := 1; i <= 100; i++ {
		h.add(-float64(i))
	}

	// Ascending order is -100 .. -1, so the 51st value is -50.
	if got := h.quantile(0.5); !within(got, -50) {
		t.Errorf("median = %g, want about -50", got)
	}
	if got := h.quantile(0); !within(got, -100) {
		t.Errorf("min = %g, want about -100", got)
	}
	if got := h.quantile(1); !within(got, -1) {
		t.Errorf("max = %g, want about -1", got)
	}
	if h.pos != nil {
		t.Error("an all-negative metric allocated a positive bucket map")
	}
}

// Mixed signs must sort across the zero boundary correctly.
func TestHistOrdersNegativesZerosAndPositives(t *testing.T) {
	var h hist
	h.add(-10)
	h.add(0)
	h.add(10)

	if got := h.quantile(0); !within(got, -10) {
		t.Errorf("lowest = %g, want -10", got)
	}
	if got := h.quantile(0.5); got != 0 {
		t.Errorf("median = %g, want exactly 0", got)
	}
	if got := h.quantile(1); !within(got, 10) {
		t.Errorf("highest = %g, want 10", got)
	}
}

// Anything indistinguishable from zero is counted as zero rather than given
// a bucket, or the index range would be unbounded below.
func TestHistTreatsTinyValuesAsZero(t *testing.T) {
	var h hist
	h.add(0)
	h.add(minMagnitude / 100)
	h.add(-minMagnitude / 100)

	if h.zeros != 3 {
		t.Errorf("zeros = %d, want 3", h.zeros)
	}
	if h.pos != nil || h.neg != nil {
		t.Error("tiny values allocated magnitude buckets")
	}
}

// Merging is what lets a windowed query combine time buckets (§8), so it
// must be exact and must not depend on which side started empty.
func TestHistMergeIsExact(t *testing.T) {
	var a, b hist
	for i := 1; i <= 500; i++ {
		a.add(float64(i))
	}
	for i := 501; i <= 1000; i++ {
		b.add(float64(i))
	}
	a.merge(&b)

	var whole hist
	for i := 1; i <= 1000; i++ {
		whole.add(float64(i))
	}

	if a.count != whole.count {
		t.Fatalf("merged count = %d, want %d", a.count, whole.count)
	}
	for _, q := range []float64{0, 0.25, 0.5, 0.9, 0.99, 1} {
		if got, want := a.quantile(q), whole.quantile(q); got != want {
			t.Errorf("q%.2f: merged %g != whole %g", q, got, want)
		}
	}
}

func TestHistMergeIntoEmptyAndNil(t *testing.T) {
	var src hist
	src.add(5)
	src.add(-5)
	src.add(0)

	var dst hist
	dst.merge(&src)
	if dst.count != 3 {
		t.Errorf("merge into empty: count = %d, want 3", dst.count)
	}

	dst.merge(nil) // must not panic
	if dst.count != 3 {
		t.Errorf("merging nil changed count to %d", dst.count)
	}
}

func TestHistEmptyQuantileIsZero(t *testing.T) {
	var h hist
	if got := h.quantile(0.5); got != 0 {
		t.Errorf("empty quantile = %g, want 0", got)
	}
	var nilHist *hist
	if got := nilHist.quantile(0.5); got != 0 {
		t.Errorf("nil quantile = %g, want 0", got)
	}
}

// A skewed distribution is the realistic case - most requests fast, a few
// slow - and it's where a median alone misleads.
func TestHistCapturesATail(t *testing.T) {
	var h hist
	for i := 0; i < 990; i++ {
		h.add(10) // the bulk
	}
	for i := 0; i < 10; i++ {
		h.add(5000) // the tail
	}

	if got := h.quantile(0.5); !within(got, 10) {
		t.Errorf("median = %g, want 10 (the bulk)", got)
	}
	if got := h.quantile(0.999); !within(got, 5000) {
		t.Errorf("p99.9 = %g, want 5000 (the tail the average would hide)", got)
	}
}

// --- record populates the distribution ---

func TestRecordBuildsADistribution(t *testing.T) {
	s := newStore()
	now := time.Now()
	for i := 1; i <= 1000; i++ {
		s.record(now, Event{Name: "latency", Value: float64(i), TS: now.UnixMilli()})
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.aggs["latency"][now.Truncate(bucketWidth).Unix()]

	if a.h == nil {
		t.Fatal("record did not build a histogram")
	}
	if a.h.count != int64(a.Count) {
		t.Errorf("histogram count %d != Agg.Count %d", a.h.count, a.Count)
	}
	if got := a.h.quantile(0.99); !within(got, 991) {
		t.Errorf("p99 = %g, want about 991", got)
	}
}

// A bucket seeded without a histogram must not panic when a real event
// lands in it - the summary numbers stay correct either way.
func TestRecordSurvivesASeededBucketWithNoHistogram(t *testing.T) {
	s := newStore()
	now := time.Now()
	key := now.Truncate(bucketWidth).Unix()

	seedBucket(s, "cpu.load", key, &Agg{Count: 1, Sum: 5, Min: 5, Max: 5}) // no h

	s.record(now, Event{Name: "cpu.load", Value: 7, TS: now.UnixMilli()})

	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.aggs["cpu.load"][key]
	if a.Count != 2 || a.Sum != 12 || a.Max != 7 {
		t.Errorf("summary numbers wrong after seeding: %+v", a)
	}
	if a.h == nil || a.h.count != 1 {
		t.Errorf("histogram should hold only the recorded event, got %+v", a.h)
	}
}

// --- merging across time buckets ---

// The windowed query has to combine several 10s buckets (§8). Percentiles
// over the merge must match percentiles over the same data recorded into a
// single bucket, or the window boundary would change the answer.
func TestMergeBucketsCombinesDistributions(t *testing.T) {
	s := newStore()
	base := time.Now().Truncate(bucketWidth)

	// 300 values spread across three consecutive time buckets.
	for i := 1; i <= 300; i++ {
		at := base.Add(time.Duration((i-1)/100) * bucketWidth)
		s.record(at, Event{Name: "latency", Value: float64(i), TS: at.UnixMilli()})
	}

	s.mu.Lock()
	got, ok := mergeBuckets(s.aggs["latency"], 0)
	s.mu.Unlock()
	if !ok {
		t.Fatal("no data")
	}

	var whole hist
	for i := 1; i <= 300; i++ {
		whole.add(float64(i))
	}

	if got.h.count != 300 {
		t.Errorf("merged count = %d, want 300", got.h.count)
	}
	for _, q := range []float64{0, 0.5, 0.9, 0.99, 1} {
		if a, b := got.h.quantile(q), whole.quantile(q); a != b {
			t.Errorf("q%.2f across buckets = %g, single bucket = %g", q, a, b)
		}
	}
}

// The aliasing hazard the pointer field exists to guard. A merge result
// escapes while the store's buckets stay live; if it shared their
// histogram, writing to the result would corrupt the store.
func TestMergeBucketsDoesNotAliasStoredHistograms(t *testing.T) {
	s := newStore()
	now := time.Now()
	s.record(now, Event{Name: "cpu.load", Value: 1, TS: now.UnixMilli()})

	s.mu.Lock()
	stored := s.aggs["cpu.load"][now.Truncate(bucketWidth).Unix()]
	got, _ := mergeBuckets(s.aggs["cpu.load"], 0)
	s.mu.Unlock()

	if got.h == stored.h {
		t.Fatal("merge returned the stored histogram itself, not a copy")
	}

	// Mutating the result must leave the store untouched.
	got.h.add(999)
	if stored.h.count != 1 {
		t.Errorf("writing to the merge result changed the store: count = %d, want 1",
			stored.h.count)
	}
}

// Merging must tolerate buckets that carry no histogram at all.
func TestMergeBucketsHandlesHistogramlessBuckets(t *testing.T) {
	s := newStore()
	seedBucket(s, "m", bucketAt(0), &Agg{Count: 5, Sum: 25, Min: 5, Max: 5}) // no h

	s.mu.Lock()
	got, ok := mergeBuckets(s.aggs["m"], 0)
	s.mu.Unlock()

	if !ok || got.Count != 5 {
		t.Fatalf("summary merge broke: %+v ok=%v", got, ok)
	}
	if got.h == nil || got.h.count != 0 {
		t.Errorf("expected an empty histogram, got %+v", got.h)
	}
}
