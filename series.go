package main

// Per-bucket reads: the time dimension the store already keeps.
//
// §8 chopped time into six ten-second buckets so a window query could sum
// the ones that overlap it. Every read path since has done exactly that and
// nothing else: mergeBuckets folds the buckets into one Agg, and the shape
// of the time inside the window is discarded on the way out.
//
// So a metric whose average is 0.4 over the minute reads identically whether
// it sat at 0.4 throughout or was idle for fifty seconds and pinned at 2.4
// for ten. The data to tell those apart is in the store - this is the
// endpoint that stops throwing it away.

import (
	"encoding/json"
	"net/http"
	"slices"
	"time"
)

// BucketStats is one time bucket of one metric. Start is the bucket's start
// time in unix seconds, which is also its key in the store.
type BucketStats struct {
	Start int64 `json:"start"`
	MetricStats
}

// SeriesResponse is the JSON body of GET /series.
//
// Bucket echoes the server's resolution back, so a caller plotting these
// knows how wide each point is rather than having to assume. Window echoes
// the retention actually applied, exactly as /stats does.
type SeriesResponse struct {
	Window string                   `json:"window"`
	Bucket string                   `json:"bucket"`
	Series map[string][]BucketStats `json:"series"`
}

// bucketAgg is one bucket copied out from under the shard lock.
type bucketAgg struct {
	start int64
	agg   Agg
}

// seriesFor copies a metric's live buckets, each with a private histogram.
//
// The private histogram is the whole reason this is not a loop over
// sh.aggs[name] in the handler. Quantiles sort a histogram's buckets, and
// doing that outside the lock over a histogram the store is still writing to
// is a data race; doing it inside the lock holds a shard shut for as many
// sorts as the metric has buckets. So each bucket is merged into a fresh
// Agg under the lock - the same trade mergeBuckets makes (§23, §28), applied
// once per bucket rather than once per metric.
func (s *Store) seriesFor(name string, cutoff int64) []bucketAgg {
	sh := s.shardFor(name)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	series := sh.aggs[name]
	out := make([]bucketAgg, 0, len(series))
	for start, a := range series {
		if start < cutoff {
			continue // outside the window
		}
		copied := Agg{Count: a.Count, Sum: a.Sum, Min: a.Min, Max: a.Max, h: &hist{}}
		copied.h.merge(a.h)
		out = append(out, bucketAgg{start: start, agg: copied})
	}
	return out
}

// handleSeries answers GET /series.
//
// It takes ?name= only, which is a deliberate bound rather than an omission.
// A series over a prefix scan would be the most expensive query this server
// can be asked for - every matching metric, every bucket, a sort each - and
// nobody plots a thousand lines. Requiring names makes the cost a function
// of what was asked for instead of what the store happens to hold, and
// reuses §37's lookup so there is no walk at all.
func (s *Store) handleSeries(w http.ResponseWriter, r *http.Request) {
	q, err := parseStatsQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(q.names) == 0 {
		http.Error(w, "series requires at least one ?name=", http.StatusBadRequest)
		return
	}

	now := time.Now()

	c, _ := clientFrom(r.Context())
	if s.refuseOverBudget(w, c, now) {
		return
	}

	cutoff := windowStart(now, q.window)
	resp := SeriesResponse{
		Window: q.window.String(),
		Bucket: bucketWidth.String(),
		Series: make(map[string][]BucketStats, len(q.names)),
	}

	// Charged per bucket, not per metric. A bucket is a sort of its own, so
	// a metric with six of them costs about six times one with a single
	// bucket, and §35's whole point is that the bill tracks the work.
	computed := 0

	for _, name := range q.names {
		buckets := s.seriesFor(name, cutoff)
		if len(buckets) == 0 {
			continue // absent, exactly as /stats treats a metric with no live data
		}
		computed += len(buckets)

		// Ascending, because a series is read as a line and a map's order
		// is not one.
		slices.SortFunc(buckets, func(a, b bucketAgg) int {
			switch {
			case a.start < b.start:
				return -1
			case a.start > b.start:
				return 1
			}
			return 0
		})

		points := make([]BucketStats, 0, len(buckets))
		for _, b := range buckets {
			// Outside the lock, over the private copy taken above.
			p := b.agg.h.quantiles(0.50, 0.90, 0.99)
			points = append(points, BucketStats{
				Start: b.start,
				MetricStats: MetricStats{
					Count: b.agg.Count,
					Avg:   b.agg.Sum / float64(b.agg.Count),
					Min:   b.agg.Min,
					Max:   b.agg.Max,
					P50:   p[0],
					P90:   p[1],
					P99:   p[2],
				},
			})
		}
		resp.Series[name] = points
	}

	// No walk, so the walk term is zero - the same arithmetic §37 earned
	// for a named /stats.
	c.chargeQuery(now, queryCost(0, computed))

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
