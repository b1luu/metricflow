package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- round trip ---

// sameAgg compares two aggregates exactly, histogram included. Percentiles
// are the reason the histogram is in the file at all, so a round trip that
// lost it would look fine on count and avg and be wrong where it matters.
func sameAgg(t *testing.T, where string, got, want *Agg) {
	t.Helper()

	if got.Count != want.Count || got.Sum != want.Sum ||
		got.Min != want.Min || got.Max != want.Max {
		t.Errorf("%s: summary = %+v, want %+v", where, *got, *want)
	}

	gh, wh := got.h, want.h
	if wh == nil {
		return
	}
	if gh == nil {
		t.Errorf("%s: histogram was lost", where)
		return
	}
	if gh.zeros != wh.zeros || gh.count != wh.count {
		t.Errorf("%s: hist zeros/count = %d/%d, want %d/%d",
			where, gh.zeros, gh.count, wh.zeros, wh.count)
	}
	for _, side := range []struct {
		name      string
		got, want map[int32]int64
	}{{"pos", gh.pos, wh.pos}, {"neg", gh.neg, wh.neg}} {
		if len(side.got) != len(side.want) {
			t.Errorf("%s: %s has %d buckets, want %d",
				where, side.name, len(side.got), len(side.want))
			continue
		}
		for k, v := range side.want {
			if side.got[k] != v {
				t.Errorf("%s: %s[%d] = %d, want %d", where, side.name, k, side.got[k], v)
			}
		}
	}
}

// A store built the way a running server builds one - through record, so the
// histograms are real - must come back identical.
func TestSnapshotRoundTripsAStore(t *testing.T) {
	s := newStore()
	now := time.Now()

	// Values chosen to reach every part of the histogram: positive,
	// negative, and small enough to land in the zero bucket (§23).
	for i := 0; i < 500; i++ {
		name := fmt.Sprintf("svc.metric.%d", i%40)
		for _, v := range []float64{float64(i%17) + 0.5, -float64(i % 9), 0, 1e-12} {
			if err := s.record(now, Event{Name: name, Value: v, TS: now.UnixMilli()}); err != nil {
				t.Fatal(err)
			}
		}
	}

	before := storeDump(s)
	data := s.snapshot(now)

	metrics, written, err := decodeSnapshot(data)
	if err != nil {
		t.Fatalf("decoding what we just encoded: %v", err)
	}
	if got := written.UnixMilli(); got != now.UnixMilli() {
		t.Errorf("written-at = %d, want %d", got, now.UnixMilli())
	}
	if len(metrics) != len(before) {
		t.Fatalf("snapshot holds %d metrics, store had %d", len(metrics), len(before))
	}

	restored := newStore()
	loaded, dropped, _ := restored.load(metrics, now)
	if loaded != len(before) || dropped != 0 {
		t.Fatalf("loaded %d / dropped %d, want %d / 0", loaded, dropped, len(before))
	}

	after := storeDump(restored)
	for name, series := range before {
		got, ok := after[name]
		if !ok {
			t.Errorf("%s was lost", name)
			continue
		}
		if len(got) != len(series) {
			t.Errorf("%s: %d buckets, want %d", name, len(got), len(series))
			continue
		}
		for key, a := range series {
			g, ok := got[key]
			if !ok {
				t.Errorf("%s: bucket %d was lost", name, key)
				continue
			}
			sameAgg(t, fmt.Sprintf("%s bucket %d", name, key), g, a)
		}
	}
}

// The percentiles have to survive, not just the summary numbers - they are
// the reason the histogram is in the file.
func TestSnapshotPreservesPercentiles(t *testing.T) {
	s := newStore()
	now := time.Now()

	// 990 fast requests and 10 slow ones: the distribution §23 was built
	// for, where avg and p99 disagree completely.
	for i := 0; i < 990; i++ {
		recordNow(s, "http.latency_ms", 20)
	}
	for i := 0; i < 10; i++ {
		recordNow(s, "http.latency_ms", 900)
	}

	want, ok := mergeAll(s, "http.latency_ms")
	if !ok {
		t.Fatal("nothing recorded")
	}
	wantQ := want.h.quantiles(0.50, 0.90, 0.99)

	metrics, _, err := decodeSnapshot(s.snapshot(now))
	if err != nil {
		t.Fatal(err)
	}
	restored := newStore()
	restored.load(metrics, now)

	got, ok := mergeAll(restored, "http.latency_ms")
	if !ok {
		t.Fatal("the metric did not survive")
	}
	gotQ := got.h.quantiles(0.50, 0.90, 0.99)

	for i, q := range []string{"p50", "p90", "p99"} {
		if gotQ[i] != wantQ[i] {
			t.Errorf("%s = %v after a round trip, want %v", q, gotQ[i], wantQ[i])
		}
	}
	// And the one that makes the point: p99 must still see the slow tail.
	if gotQ[2] < 500 {
		t.Errorf("p99 = %v after restore; the slow tail was lost", gotQ[2])
	}
}

func TestSnapshotOfAnEmptyStore(t *testing.T) {
	s := newStore()
	now := time.Now()

	metrics, _, err := decodeSnapshot(s.snapshot(now))
	if err != nil {
		t.Fatalf("an empty snapshot did not decode: %v", err)
	}
	if len(metrics) != 0 {
		t.Errorf("an empty store produced %d metrics", len(metrics))
	}
}

// --- what is deliberately left out ---

// Self-metrics describe one process, and the process that loads the file is
// a different one. Carrying a predecessor's counters forward would make a
// counter appear to fall the moment the new process starts its own at zero -
// the one thing a counter must never do (§31).
func TestSnapshotExcludesTheReservedNamespace(t *testing.T) {
	s := newStore()
	now := time.Now()

	recordNow(s, "svc.real", 1)
	if err := s.record(now, Event{Name: selfShed, Value: 99, TS: now.UnixMilli()}); err != nil {
		t.Fatal(err)
	}

	metrics, _, err := decodeSnapshot(s.snapshot(now))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range metrics {
		if reserved(m.name) {
			t.Errorf("%s was written to the snapshot", m.name)
		}
	}
	if len(metrics) != 1 {
		t.Errorf("snapshot holds %d metrics, want only the real one", len(metrics))
	}

	// And a file that does contain one was not written by this server, so
	// loading it must refuse rather than trust it.
	forged := []snapshotMetric{{
		name:    selfShed,
		buckets: map[int64]*Agg{bucketAt(0): {Count: 1, Sum: 0, Min: 0, Max: 0}},
	}}
	restored := newStore()
	if loaded, dropped, _ := restored.load(forged, now); loaded != 0 || dropped != 1 {
		t.Errorf("loaded %d / dropped %d a reserved name, want 0 / 1", loaded, dropped)
	}
}

// Buckets that aged out while the server was down are not restored. That is
// not data loss, it is what retention means (§10).
func TestLoadDropsWhatAgedOutWhileDown(t *testing.T) {
	s := newStore()
	now := time.Now()

	seedBucket(s, "half.stale", bucketAt(window+time.Minute), &Agg{Count: 3, Sum: 3, Min: 1, Max: 1})
	seedBucket(s, "half.stale", bucketAt(0), &Agg{Count: 7, Sum: 7, Min: 1, Max: 1})
	seedBucket(s, "all.stale", bucketAt(window+time.Minute), &Agg{Count: 9, Sum: 9, Min: 1, Max: 1})

	// Snapshotting already drops the stale ones, so the file is smaller too.
	metrics, _, err := decodeSnapshot(s.snapshot(now))
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 || metrics[0].name != "half.stale" {
		t.Fatalf("snapshot holds %d metrics (%+v), want only half.stale", len(metrics), metrics)
	}
	if len(metrics[0].buckets) != 1 {
		t.Errorf("half.stale kept %d buckets, want only the live one", len(metrics[0].buckets))
	}

	restored := newStore()
	restored.load(metrics, now)
	got, ok := mergeAll(restored, "half.stale")
	if !ok || got.Count != 7 {
		t.Errorf("half.stale = %+v (ok=%v), want Count=7", got, ok)
	}
	if _, ok := mergeAll(restored, "all.stale"); ok {
		t.Error("an entirely stale metric was restored")
	}
}

// A snapshot older than the whole window restores nothing, and that is the
// correct outcome rather than a failure.
func TestAnEntirelyStaleSnapshotLoadsNothing(t *testing.T) {
	s := newStore()
	then := time.Now().Add(-2 * window)

	if err := s.record(then, Event{Name: "old.metric", Value: 1, TS: then.UnixMilli()}); err != nil {
		t.Fatal(err)
	}

	metrics, _, err := decodeSnapshot(s.snapshot(then))
	if err != nil {
		t.Fatal(err)
	}

	restored := newStore()
	loaded, dropped, _ := restored.load(metrics, time.Now())
	if loaded != 0 {
		t.Errorf("loaded %d metrics from a snapshot older than the window", loaded)
	}
	if dropped != len(metrics) {
		t.Errorf("dropped %d of %d", dropped, len(metrics))
	}
}

// The cardinality cap still applies (§27): a snapshot taken under a larger
// cap, or restored onto a server configured with a smaller one, must not be
// able to exceed it just because the data used to fit.
func TestLoadRespectsTheCardinalityCap(t *testing.T) {
	now := time.Now()

	metrics := make([]snapshotMetric, 0, maxMetricsPerShard+50)
	for _, name := range namesForShard(t, 0, maxMetricsPerShard+50) {
		metrics = append(metrics, snapshotMetric{
			name:    name,
			buckets: map[int64]*Agg{bucketAt(0): {Count: 1, Sum: 1, Min: 1, Max: 1}},
		})
	}

	s := newStore()
	loaded, dropped, _ := s.load(metrics, now)

	if loaded != maxMetricsPerShard {
		t.Errorf("loaded %d metrics onto one shard, want the cap %d", loaded, maxMetricsPerShard)
	}
	if dropped != 50 {
		t.Errorf("dropped %d, want 50", dropped)
	}
	if got := metricCount(s); got != maxMetricsPerShard {
		t.Errorf("store holds %d metrics, past the cap", got)
	}
}

// --- a damaged file ---

// A snapshot that has been corrupted must be refused, not misread. Every
// single-byte change has to be caught, or a flipped bit becomes a wrong
// number an operator has no way to question.
func TestEveryCorruptionIsRejected(t *testing.T) {
	s := newStore()
	now := time.Now()
	recordNow(s, "svc.metric", 12.5)
	recordNow(s, "other.metric", -3)

	good := s.snapshot(now)
	if _, _, err := decodeSnapshot(good); err != nil {
		t.Fatalf("the undamaged snapshot did not decode: %v", err)
	}

	// Flip a bit in every byte in turn.
	for i := range good {
		bad := append([]byte(nil), good...)
		bad[i] ^= 0x01

		if _, _, err := decodeSnapshot(bad); err == nil {
			t.Fatalf("a snapshot with byte %d of %d flipped was accepted", i, len(good))
		}
	}

	// And every truncation.
	for n := 0; n < len(good); n++ {
		if _, _, err := decodeSnapshot(good[:n]); err == nil {
			t.Fatalf("a snapshot truncated to %d of %d bytes was accepted", n, len(good))
		}
	}
}

func TestSnapshotHeaderIsChecked(t *testing.T) {
	s := newStore()
	recordNow(s, "svc.metric", 1)
	good := s.snapshot(time.Now())

	cases := map[string][]byte{
		"empty":             {},
		"too short":         []byte("MF"),
		"wrong magic":       append([]byte("NOTASNAP"), good[len(snapMagic):]...),
		"header only":       []byte(snapMagic),
		"plausible garbage": make([]byte, 512),
	}
	for name, in := range cases {
		if _, _, err := decodeSnapshot(in); err == nil {
			t.Errorf("%s was accepted as a snapshot", name)
		}
	}
}

// A length prefix is the dangerous field in any binary format: a damaged one
// claiming four billion buckets must be an error rather than an allocation.
// The checksum would normally catch it, so it is recomputed here - this is
// the case where an attacker, or a bug in the writer, produces a consistent
// file that is still nonsense.
func TestAbsurdLengthsAreRefusedRatherThanAllocated(t *testing.T) {
	s := newStore()
	now := time.Now()
	recordNow(s, "svc.metric", 1)
	good := s.snapshot(now)

	// The metric count sits right after the magic and the timestamp.
	countAt := len(snapMagic) + 8

	for _, claim := range []uint32{1 << 20, 1 << 28, math.MaxUint32} {
		bad := append([]byte(nil), good...)
		binary.LittleEndian.PutUint32(bad[countAt:], claim)

		// Re-checksum, so this tests the bounds checks rather than the CRC.
		body := bad[:len(bad)-4]
		binary.LittleEndian.PutUint32(bad[len(bad)-4:], crc32.Checksum(body, crcTable))

		_, _, err := decodeSnapshot(bad)
		if err == nil {
			t.Errorf("a snapshot claiming %d metrics was accepted", claim)
			continue
		}
		if !strings.Contains(err.Error(), "remaining bytes") {
			t.Errorf("claiming %d metrics gave %q, want a bounds complaint", claim, err)
		}
	}
}

// Trailing bytes mean the file is not what it says it is, even with a valid
// checksum - two snapshots concatenated, say.
func TestTrailingBytesAreRejected(t *testing.T) {
	s := newStore()
	recordNow(s, "svc.metric", 1)
	good := s.snapshot(time.Now())

	bad := append(append([]byte(nil), good[:len(good)-4]...), 0, 0, 0)
	bad = append(bad, make([]byte, 4)...)
	binary.LittleEndian.PutUint32(bad[len(bad)-4:], crc32.Checksum(bad[:len(bad)-4], crcTable))

	if _, _, err := decodeSnapshot(bad); err == nil {
		t.Error("a snapshot with trailing bytes was accepted")
	}
}

// --- on disk ---

func TestWriteAndReadSnapshotFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.bin")

	s := newStore()
	now := time.Now()
	for i := 0; i < 50; i++ {
		recordNow(s, fmt.Sprintf("svc.metric.%d", i), float64(i))
	}

	if err := s.writeSnapshot(path, now); err != nil {
		t.Fatal(err)
	}

	restored := newStore()
	loaded, dropped, buckets, err := restored.readSnapshot(path, now)
	if err != nil {
		t.Fatal(err)
	}
	if loaded != 50 || dropped != 0 || buckets != 50 {
		t.Errorf("loaded %d / dropped %d / %d buckets, want 50 / 0 / 50", loaded, dropped, buckets)
	}
	if got := metricCount(restored); got != 50 {
		t.Errorf("restored store holds %d metrics, want 50", got)
	}
}

// A first start has nothing to restore, and that must not be an error -
// persistence should start working rather than need setting up.
func TestReadingAMissingSnapshotIsNotAnError(t *testing.T) {
	s := newStore()
	path := filepath.Join(t.TempDir(), "does-not-exist.bin")

	loaded, dropped, buckets, err := s.readSnapshot(path, time.Now())
	if err != nil {
		t.Fatalf("a missing snapshot was an error: %v", err)
	}
	if loaded != 0 || dropped != 0 || buckets != 0 {
		t.Errorf("got %d / %d / %d from a missing file", loaded, dropped, buckets)
	}
}

// A corrupt file on disk is an error rather than a silent empty start: an
// operator who has persistence configured should be told it failed, not left
// wondering where the data went.
func TestReadingACorruptSnapshotIsAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.bin")
	if err := os.WriteFile(path, []byte("this is not a snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}

	s := newStore()
	if _, _, _, err := s.readSnapshot(path, time.Now()); err == nil {
		t.Error("a corrupt snapshot file loaded without complaint")
	}
}

// Writing must replace the previous snapshot rather than append to it or
// leave a partial file where a good one was.
func TestWritingReplacesThePreviousSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.bin")
	now := time.Now()

	first := newStore()
	recordNow(first, "first.metric", 1)
	if err := first.writeSnapshot(path, now); err != nil {
		t.Fatal(err)
	}

	second := newStore()
	recordNow(second, "second.metric", 2)
	if err := second.writeSnapshot(path, now); err != nil {
		t.Fatal(err)
	}

	restored := newStore()
	if _, _, _, err := restored.readSnapshot(path, now); err != nil {
		t.Fatal(err)
	}
	if _, ok := mergeAll(restored, "second.metric"); !ok {
		t.Error("the second snapshot was not the one on disk")
	}
	if _, ok := mergeAll(restored, "first.metric"); ok {
		t.Error("the first snapshot's data is still there; the write appended rather than replaced")
	}

	// And no temporary files were left behind.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "snap.bin" {
			t.Errorf("left %q behind in the snapshot directory", e.Name())
		}
	}
}

func BenchmarkSnapshot(b *testing.B) {
	s := storeWithMetrics(2000)
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if len(s.snapshot(now)) == 0 {
			b.Fatal("empty snapshot")
		}
	}
}

func BenchmarkDecodeSnapshot(b *testing.B) {
	s := storeWithMetrics(2000)
	data := s.snapshot(time.Now())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := decodeSnapshot(data); err != nil {
			b.Fatal(err)
		}
	}
}

// --- the whole lifecycle ---

// runUntil starts a server on its own listener and returns a stop function.
func runUntil(t *testing.T, snapPath string) (addr string, stop func() error) {
	t.Helper()
	t.Setenv("METRICFLOW_SNAPSHOT", snapPath)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- run(ctx, ln) }()

	return ln.Addr().String(), func() error {
		cancel()
		select {
		case err := <-errc:
			return err
		case <-time.After(20 * time.Second):
			return errors.New("the server did not shut down")
		}
	}
}

func postEvent(t *testing.T, addr, name string, value float64) {
	t.Helper()
	body := fmt.Sprintf(`{"name":%q,"value":%v,"ts":%d}`, name, value, time.Now().UnixMilli())

	resp, err := http.Post("http://"+addr+"/ingest", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ingest returned %d", resp.StatusCode)
	}
}

func fetchStats(t *testing.T, addr, query string) StatsResponse {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/stats" + query)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var out StatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// The claim §32 exists to make: a planned restart loses nothing. Driven
// through the real lifecycle - a real listener, real HTTP, a real shutdown -
// because every part of this is in run() rather than in the store.
func TestDataSurvivesAGracefulRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.bin")

	addr, stop := runUntil(t, path)
	for i := 0; i < 100; i++ {
		postEvent(t, addr, "svc.api.latency", float64(10+i%5))
	}
	postEvent(t, addr, "svc.api.errors", 3)

	before := fetchStats(t, addr, "")
	if err := stop(); err != nil {
		t.Fatalf("shutting down: %v", err)
	}

	// A different process, as far as the store is concerned.
	addr2, stop2 := runUntil(t, path)
	defer stop2()

	after := fetchStats(t, addr2, "")

	for _, name := range []string{"svc.api.latency", "svc.api.errors"} {
		b, ok := before.Metrics[name]
		if !ok {
			t.Fatalf("%s was missing before the restart", name)
		}
		a, ok := after.Metrics[name]
		if !ok {
			t.Errorf("%s did not survive the restart", name)
			continue
		}
		if a.Count != b.Count || a.Avg != b.Avg || a.Min != b.Min || a.Max != b.Max {
			t.Errorf("%s: count/avg = %d/%v after, %d/%v before",
				name, a.Count, a.Avg, b.Count, b.Avg)
		}
		// The percentiles are the part a lossy format would flatten.
		if a.P99 != b.P99 {
			t.Errorf("%s: p99 = %v after, %v before", name, a.P99, b.P99)
		}
	}
}

// Self-metrics are the exception, and must be: they describe a process, and
// this is a new one (§31).
func TestSelfMetricsDoNotSurviveARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.bin")

	addr, stop := runUntil(t, path)
	postEvent(t, addr, "svc.real", 1)

	// Wait for the reporter's first sample to be visible.
	deadline := time.After(10 * time.Second)
	for len(fetchStats(t, addr, "?prefix="+selfPrefix).Metrics) == 0 {
		select {
		case <-deadline:
			t.Fatal("no self-metrics before the restart")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	// The file must not contain them at all.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	metrics, _, err := decodeSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range metrics {
		if reserved(m.name) {
			t.Errorf("%s was persisted", m.name)
		}
	}

	// And the real metric did survive, or this test proves nothing about
	// the exclusion being selective.
	addr2, stop2 := runUntil(t, path)
	defer stop2()
	if _, ok := fetchStats(t, addr2, "").Metrics["svc.real"]; !ok {
		t.Error("svc.real did not survive; the snapshot carried nothing")
	}
}

// Persistence is off unless asked for, so a server that was never configured
// for it behaves exactly as it did before §32 - including leaving no files
// behind.
func TestPersistenceIsOffByDefault(t *testing.T) {
	dir := t.TempDir()

	addr, stop := runUntil(t, "")
	postEvent(t, addr, "svc.api", 1)
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("an unconfigured server wrote %d files", len(entries))
	}
}

// A path the operator got wrong is found at startup rather than at the first
// tick, and is fatal: it is certainly wrong, and nothing is lost by
// refusing to start.
func TestAnUnwritableSnapshotPathStopsStartup(t *testing.T) {
	t.Setenv("METRICFLOW_SNAPSHOT", filepath.Join(t.TempDir(), "no-such-dir", "snap.bin"))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errc := make(chan error, 1)
	go func() { errc <- run(ctx, ln) }()

	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("the server started with an unwritable snapshot path")
		}
		if !strings.Contains(err.Error(), "snap.bin") {
			t.Errorf("error = %q, want it to name the path", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the server neither started nor failed")
	}
}

// A corrupt file is fatal too, for the same reason: an operator who
// configured persistence should be told it failed, not left to wonder where
// the data went.
func TestACorruptSnapshotStopsStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.bin")
	if err := os.WriteFile(path, []byte("definitely not a snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("METRICFLOW_SNAPSHOT", path)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errc := make(chan error, 1)
	go func() { errc <- run(ctx, ln) }()

	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("the server started from a corrupt snapshot")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the server neither started nor failed")
	}
}
