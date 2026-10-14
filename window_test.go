package main

import (
	"testing"
	"time"
)

// A window can only be honoured to the nearest bucket, and the response has
// to say which way it rounded.
//
// windowStart truncates downwards, so a request for a minute reaches back
// between 60 and 69 seconds depending on where in the current bucket it
// lands, and returns seven buckets rather than the six numBuckets implies.
// That is the right direction to err - the alternative drops a bucket that
// is partly inside the window, losing recent data from an answer about the
// recent past - but a response that claimed "1m0s" and delivered 69 seconds
// was telling the caller something untrue.
func TestCoveredIsAtLeastTheWindowAndUnderOneBucketMore(t *testing.T) {
	for _, offset := range []time.Duration{0, time.Second, 5 * time.Second, bucketWidth - time.Second} {
		now := time.Now().Truncate(bucketWidth).Add(offset)

		for _, want := range []time.Duration{window, 30 * time.Second, bucketWidth} {
			got := covered(now, windowStart(now, want))

			if got < want {
				t.Errorf("offset %s, window %s: covered %s, which is less than asked - "+
					"recent data was dropped", offset, want, got)
			}
			if got >= want+bucketWidth {
				t.Errorf("offset %s, window %s: covered %s, a whole bucket more than "+
					"asked", offset, want, got)
			}
		}
	}
}

// The number a caller actually sees. Asking for a minute reports a window
// of 1m0s and a coverage between 1m0s and 1m9s, and the second one is the
// truth about the answer.
func TestStatsReportsWhatItCovered(t *testing.T) {
	s := newStore()
	recordNow(s, "m", 1)

	resp := getStats(t, s, "")

	if resp.Window != window.String() {
		t.Errorf("window = %q, want %q", resp.Window, window)
	}
	gotCovers, err := time.ParseDuration(resp.Covers)
	if err != nil {
		t.Fatalf("covers = %q, which is not a duration", resp.Covers)
	}
	if gotCovers < window || gotCovers >= window+bucketWidth {
		t.Errorf("covers = %s, want between %s and %s", gotCovers, window, window+bucketWidth)
	}
}

// And /series, where it matters most: a client plotting the points can
// divide covers by bucket and know how many to expect, which window alone
// would get wrong.
// wellIntoABucket waits until the clock is at least d past a bucket
// boundary, so a test can tell "covers was computed from the cutoff" apart
// from "covers is the requested window echoed back".
//
// Those two are identical whenever a request lands exactly on a boundary,
// which is why a range check cannot separate them and why this waits
// instead. The wait is usually none at all and never more than d.
func wellIntoABucket(d time.Duration) {
	for {
		offset := time.Since(time.Now().Truncate(bucketWidth))
		if offset >= d {
			return
		}
		time.Sleep(d - offset)
	}
}

func TestSeriesReportsWhatItCovered(t *testing.T) {
	wellIntoABucket(3 * time.Second)

	s := newStore()
	now := time.Now()
	for i := 0; i < numBuckets+2; i++ {
		recordAt(t, s, "m", 1, now.Add(-time.Duration(i)*bucketWidth))
	}

	resp := getSeries(t, s, "?name=m")

	covers, err := time.ParseDuration(resp.Covers)
	if err != nil {
		t.Fatalf("covers = %q, which is not a duration", resp.Covers)
	}
	if covers < window || covers >= window+bucketWidth {
		t.Errorf("covers = %s, want between %s and %s", covers, window, window+bucketWidth)
	}

	// The point count has to agree with what the response claims to cover,
	// which is the whole reason the field is there: dividing the *window*
	// by the bucket gives the wrong answer.
	points := resp.Series["m"]
	bucket, err := time.ParseDuration(resp.Bucket)
	if err != nil {
		t.Fatalf("bucket = %q, which is not a duration", resp.Bucket)
	}
	if wantPoints := int(covers/bucket) + 1; len(points) != wantPoints {
		t.Errorf("%d points for %s covered at %s resolution, want %d",
			len(points), covers, bucket, wantPoints)
	}

	// The check that matters, and the only one that catches a response
	// echoing the requested window back as if it were the coverage: the
	// oldest point must start exactly covers ago. A range check cannot
	// catch that, because covering exactly the window is legitimate when
	// the request lands on a bucket boundary - so the lie is indentical to
	// the truth one time in ten.
	age := time.Since(time.Unix(points[0].Start, 0))
	if drift := age - covers; drift < -2*time.Second || drift > 2*time.Second {
		t.Errorf("the oldest point is %s old but the response claims to cover %s "+
			"(off by %s); covers is not describing the data returned",
			age.Round(time.Second), covers, drift.Round(time.Second))
	}
}

// A shorter window narrows coverage too, so the field tracks the request
// rather than being a constant dressed up as a measurement.
func TestCoveredTracksTheRequestedWindow(t *testing.T) {
	s := newStore()
	now := time.Now()
	for i := 0; i < numBuckets; i++ {
		recordAt(t, s, "m", 1, now.Add(-time.Duration(i)*bucketWidth))
	}

	full, err := time.ParseDuration(getStats(t, s, "").Covers)
	if err != nil {
		t.Fatal(err)
	}
	narrow, err := time.ParseDuration(getStats(t, s, "?window=20s").Covers)
	if err != nil {
		t.Fatal(err)
	}

	if narrow >= full {
		t.Errorf("a 20s window covered %s and the full window %s", narrow, full)
	}
	if narrow < 20*time.Second {
		t.Errorf("a 20s window covered only %s", narrow)
	}
}
