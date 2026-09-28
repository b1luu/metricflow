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
	"sync"
	"sync/atomic"
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

	if err := newSnapshotter(s, path).write(now); err != nil {
		t.Fatal(err)
	}

	restored := newStore()
	res := newSnapshotter(restored, path).restore(now)
	loaded, dropped, buckets := res.loaded, res.dropped, res.buckets
	if len(res.problems) != 0 {
		t.Fatalf("problems restoring: %v", res.problems)
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

	res := newSnapshotter(s, path).restore(time.Now())
	if len(res.problems) != 0 {
		t.Errorf("a missing snapshot produced problems: %v", res.problems)
	}
	if res.loaded != 0 || res.dropped != 0 || res.buckets != 0 {
		t.Errorf("got %d / %d / %d from a missing file", res.loaded, res.dropped, res.buckets)
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
	res := newSnapshotter(s, path).restore(time.Now())
	if len(res.problems) == 0 {
		t.Error("a corrupt snapshot file loaded without complaint")
	}
	if res.from != "" {
		t.Errorf("a corrupt snapshot was loaded from %q", res.from)
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
	if err := newSnapshotter(first, path).write(now); err != nil {
		t.Fatal(err)
	}

	second := newStore()
	recordNow(second, "second.metric", 2)
	if err := newSnapshotter(second, path).write(now); err != nil {
		t.Fatal(err)
	}

	restored := newStore()
	if res := newSnapshotter(restored, path).restore(now); res.from == "" {
		t.Fatalf("nothing restored: %v", res.problems)
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
		// snap.bin.prev is expected once a second write has rotated one.
		if e.Name() != "snap.bin" && e.Name() != "snap.bin.prev" {
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

	// A server that refuses to start returns before it ever calls Serve,
	// which leaves the listener open with nobody accepting - so a request to
	// it connects and then waits forever rather than being refused. Caught
	// here, because otherwise every caller hangs until its client gives up,
	// and a test that takes ten minutes to fail is a test nobody runs.
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-errc:
		cancel()
		ln.Close()
		t.Fatalf("the server failed to start: %v", err)
	default:
	}

	deadline := time.After(20 * time.Second)
	for {
		resp, err := testClient.Get("http://" + ln.Addr().String() + "/health")
		if err == nil {
			resp.Body.Close()
			break
		}
		select {
		case <-deadline:
			cancel()
			ln.Close()
			t.Fatal("the server never became healthy")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

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

// testClient has a timeout, so a server that stops answering fails a test in
// seconds rather than hanging it. http.DefaultClient has none.
var testClient = &http.Client{Timeout: 10 * time.Second}

func postEvent(t *testing.T, addr, name string, value float64) {
	t.Helper()
	body := fmt.Sprintf(`{"name":%q,"value":%v,"ts":%d}`, name, value, time.Now().UnixMilli())

	resp, err := testClient.Post("http://"+addr+"/ingest", "application/json", strings.NewReader(body))
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
	resp, err := testClient.Get("http://" + addr + "/stats" + query)
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

// --- under concurrency ---

// A snapshot is taken from a live server, so it is read while ingest writes,
// the sweeper deletes and /stats walks. It takes the same per-shard locks as
// everything else (§24), and this says so rather than assuming it.
//
// Two things are checked. Every snapshot must decode - a torn write would
// produce a file that fails its own checksum or its own bounds - and every
// metric inside one must be internally consistent, because each metric's
// buckets are read under a single lock hold.
//
// Consistency is checkable because every event for a metric carries that
// metric's own constant value, so min, max and sum/count must all equal it.
// A metric captured half-updated would not satisfy that.
func TestSnapshottingWhileEverythingElseRuns(t *testing.T) {
	const (
		writers   = 8
		perWriter = 3000
		snappers  = 2
	)

	s := newStore()

	// Stable metrics, each with its own constant value.
	value := map[string]float64{}
	for i := 0; i < 24; i++ {
		value[fmt.Sprintf("stable.%02d", i)] = float64(i + 1)
	}

	var (
		wgWork   sync.WaitGroup
		wgBg     sync.WaitGroup
		done     atomic.Bool
		taken    atomic.Int64
		failures = make(chan string, 32)
	)
	fail := func(msg string) {
		select {
		case failures <- msg:
		default:
		}
	}

	// Writers: half to the stable set, half creating names the sweeper will
	// reclaim, so the shards' own maps are under constant write. Reusing a
	// handful of names would leave the snapshot walk with nothing to race
	// against - the lesson §31's concurrency test had to learn.
	stale := time.Now().Add(-window - time.Minute)
	names := make([]string, 0, len(value))
	for n := range value {
		names = append(names, n)
	}

	for w := 0; w < writers; w++ {
		wgWork.Add(1)
		go func(w int) {
			defer wgWork.Done()
			for i := 0; i < perWriter; i++ {
				now := time.Now()
				if i%2 == 0 {
					n := names[i%len(names)]
					_ = s.record(now, Event{Name: n, Value: value[n], TS: now.UnixMilli()})
				} else {
					_ = s.record(stale, Event{
						Name:  fmt.Sprintf("churn.%d.%d", w, i),
						Value: 1, TS: stale.UnixMilli()})
				}
			}
		}(w)
	}

	for sn := 0; sn < snappers; sn++ {
		wgBg.Add(1)
		go func() {
			defer wgBg.Done()
			for !done.Load() {
				data := s.snapshot(time.Now())

				metrics, _, err := decodeSnapshot(data)
				if err != nil {
					fail(fmt.Sprintf("a snapshot taken under load did not decode: %v", err))
					return
				}
				taken.Add(1)

				for _, m := range metrics {
					v, ok := value[m.name]
					if !ok {
						continue // one of the churn metrics
					}
					for key, a := range m.buckets {
						if a.Count <= 0 {
							fail(fmt.Sprintf("%s bucket %d has Count=%d", m.name, key, a.Count))
							continue
						}
						if a.Min != v || a.Max != v || a.Sum != v*float64(a.Count) {
							fail(fmt.Sprintf(
								"%s bucket %d is torn: count=%d sum=%v min=%v max=%v, every value is %v",
								m.name, key, a.Count, a.Sum, a.Min, a.Max, v))
						}
					}
				}
			}
		}()
	}

	wgBg.Add(1)
	go func() {
		defer wgBg.Done()
		for !done.Load() {
			s.sweep(time.Now())
		}
	}()

	wgWork.Wait()
	done.Store(true)
	wgBg.Wait()
	close(failures)

	for msg := range failures {
		t.Error(msg)
	}
	if taken.Load() == 0 {
		t.Fatal("no snapshot completed; nothing here overlapped")
	}
}

// Writing to disk under load has the same requirement, plus one more: the
// file left behind must be a whole snapshot rather than whichever bytes the
// last writer happened to flush.
func TestWritingSnapshotsToDiskUnderLoad(t *testing.T) {
	const (
		writers   = 8
		perWriter = 2000
	)

	path := filepath.Join(t.TempDir(), "snap.bin")
	s := newStore()
	sn := newSnapshotter(s, path)

	var (
		wgWork sync.WaitGroup
		wgBg   sync.WaitGroup
		done   atomic.Bool
		wrote  atomic.Int64
		errs   = make(chan error, 8)
	)

	stale := time.Now().Add(-window - time.Minute)
	for w := 0; w < writers; w++ {
		wgWork.Add(1)
		go func(w int) {
			defer wgWork.Done()
			for i := 0; i < perWriter; i++ {
				now := time.Now()
				if i%2 == 0 {
					_ = s.record(now, Event{
						Name: fmt.Sprintf("live.%02d", i%16), Value: 2, TS: now.UnixMilli()})
				} else {
					_ = s.record(stale, Event{
						Name:  fmt.Sprintf("churn.%d.%d", w, i),
						Value: 1, TS: stale.UnixMilli()})
				}
			}
		}(w)
	}

	// Writer and reader of the same file at once, which is what a restart
	// racing a snapshot would look like.
	wgBg.Add(1)
	go func() {
		defer wgBg.Done()
		for !done.Load() {
			if err := sn.write(time.Now()); err != nil {
				select {
				case errs <- err:
				default:
				}
				return
			}
			wrote.Add(1)

			// Read it straight back: the rename is what guarantees this
			// sees a whole file rather than a partial one.
			restored := newStore()
			if res := newSnapshotter(restored, path).restore(time.Now()); len(res.problems) > 0 {
				select {
				case errs <- fmt.Errorf("reading back a snapshot written under load: %v", res.problems):
				default:
				}
				return
			}
		}
	}()

	wgWork.Wait()
	done.Store(true)
	wgBg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
	if wrote.Load() == 0 {
		t.Fatal("no snapshot was written")
	}

	// One more now that everything has stopped, and assert on that rather
	// than on whatever the loop last managed. The writers finish in a
	// couple of milliseconds while a write-and-read-back takes longer, so
	// the loop can legitimately get exactly one snapshot in - the empty one
	// it took before any writer had recorded anything. Asserting on that
	// was a flake, and it failed on the second run rather than the first.
	if err := sn.write(time.Now()); err != nil {
		t.Fatal(err)
	}

	restored := newStore()
	res := newSnapshotter(restored, path).restore(time.Now())
	if len(res.problems) > 0 {
		t.Fatalf("problems: %v", res.problems)
	}
	if res.loaded == 0 {
		t.Error("a snapshot of the final state held nothing")
	}
}

// --- keeping the previous generation ---

func snapshotOf(t *testing.T, names ...string) *Store {
	t.Helper()
	s := newStore()
	for _, n := range names {
		recordNow(s, n, 1)
	}
	return s
}

// The second write keeps the first, which is the whole point: a snapshot that
// turns out to be unreadable must not be the only copy.
func TestWritingRotatesThePreviousGeneration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.bin")
	now := time.Now()

	sn := newSnapshotter(snapshotOf(t, "first.metric"), path)
	if err := sn.write(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sn.prev()); !errors.Is(err, os.ErrNotExist) {
		t.Error("the first write created a previous generation out of nothing")
	}

	// A second write through the same snapshotter promotes the first file.
	sn.store = snapshotOf(t, "second.metric")
	if err := sn.write(now); err != nil {
		t.Fatal(err)
	}

	current := newStore()
	if res := newSnapshotter(current, path).restore(now); res.from != path {
		t.Fatalf("restored from %q, want the current file", res.from)
	}
	if _, ok := mergeAll(current, "second.metric"); !ok {
		t.Error("the current file does not hold the newest data")
	}

	// And the previous generation holds what the current one replaced.
	prev := newStore()
	prevSnap := newSnapshotter(prev, sn.prev())
	if res := prevSnap.restore(now); res.from == "" {
		t.Fatalf("the previous generation is not loadable: %v", res.problems)
	}
	if _, ok := mergeAll(prev, "first.metric"); !ok {
		t.Error("the previous generation does not hold the older data")
	}
}

// The fallback §32 asked for: a corrupt current file must not cost the data,
// because there is a copy behind it.
func TestRestoreFallsBackToThePreviousGeneration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.bin")
	now := time.Now()

	sn := newSnapshotter(snapshotOf(t, "older.metric"), path)
	if err := sn.write(now); err != nil {
		t.Fatal(err)
	}
	sn.store = snapshotOf(t, "newer.metric")
	if err := sn.write(now); err != nil {
		t.Fatal(err)
	}

	// Damage the current file, leaving the previous one intact.
	if err := os.WriteFile(path, []byte("shredded"), 0o600); err != nil {
		t.Fatal(err)
	}

	restored := newStore()
	res := newSnapshotter(restored, path).restore(now)

	if res.from != sn.prev() {
		t.Fatalf("restored from %q, want the previous generation", res.from)
	}
	if len(res.problems) != 1 {
		t.Errorf("problems = %v, want exactly one - the corrupt current file", res.problems)
	}
	if _, ok := mergeAll(restored, "older.metric"); !ok {
		t.Error("the fallback did not bring the older data back")
	}
	// The data that was lost is the newest, which is the cost of the
	// fallback and worth being explicit about.
	if _, ok := mergeAll(restored, "newer.metric"); ok {
		t.Error("the corrupt current file was loaded after all")
	}
}

// The subtle one. On startup the file on disk may be the corrupt one that
// just failed to load; rotating it would overwrite a good previous
// generation with a known-bad file, turning one damaged copy into two. Only
// a file this process wrote is ever promoted.
func TestAKnownBadCurrentFileIsNeverPromoted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.bin")
	now := time.Now()

	// Build a good previous generation the way a running server would.
	sn := newSnapshotter(snapshotOf(t, "precious.metric"), path)
	if err := sn.write(now); err != nil {
		t.Fatal(err)
	}
	sn.store = snapshotOf(t, "newer.metric")
	if err := sn.write(now); err != nil {
		t.Fatal(err)
	}
	if _, ok := mergeAll(mustRestore(t, sn.prev(), now), "precious.metric"); !ok {
		t.Fatal("setup: the previous generation is not what was expected")
	}

	// Now a restart: the current file is corrupt, and a fresh snapshotter
	// has no memory of having written anything.
	if err := os.WriteFile(path, []byte("shredded"), 0o600); err != nil {
		t.Fatal(err)
	}

	restarted := newStore()
	fresh := newSnapshotter(restarted, path)
	if res := fresh.restore(now); res.from != fresh.prev() {
		t.Fatalf("restored from %q, want the previous generation", res.from)
	}

	// The startup write must replace the corrupt file without promoting it.
	if err := fresh.write(now); err != nil {
		t.Fatal(err)
	}

	if _, ok := mergeAll(mustRestore(t, fresh.prev(), now), "precious.metric"); !ok {
		t.Error("the corrupt current file was rotated over the good previous one")
	}
	if _, ok := mergeAll(mustRestore(t, path, now), "precious.metric"); !ok {
		t.Error("the new current file does not hold what was restored")
	}
}

func mustRestore(t *testing.T, path string, now time.Time) *Store {
	t.Helper()
	s := newStore()
	res := newSnapshotter(s, path).restore(now)
	if res.from == "" {
		t.Fatalf("%s is not loadable: %v", path, res.problems)
	}
	return s
}

// Both copies gone is the only case with nothing to fall back to, and it has
// to be reported rather than looking like a clean first start.
func TestBothGenerationsUnusableIsReported(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.bin")

	for _, p := range []string{path, path + ".prev"} {
		if err := os.WriteFile(p, []byte("shredded"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	res := newSnapshotter(newStore(), path).restore(time.Now())
	if res.from != "" {
		t.Errorf("restored from %q, want nothing", res.from)
	}
	if len(res.problems) != 2 {
		t.Errorf("problems = %v, want one per unusable file", res.problems)
	}
}

// A first start has neither file, which is not a problem and must not be
// reported as one.
func TestNeitherGenerationPresentIsSilent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.bin")

	res := newSnapshotter(newStore(), path).restore(time.Now())
	if res.from != "" || len(res.problems) != 0 {
		t.Errorf("from = %q, problems = %v; want a silent empty start", res.from, res.problems)
	}
}

// A crash between the two renames leaves no current file and a good previous
// one. That window is real, so the fallback has to cover it.
func TestACrashBetweenRenamesIsRecoverable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.bin")
	now := time.Now()

	sn := newSnapshotter(snapshotOf(t, "survivor.metric"), path)
	if err := sn.write(now); err != nil {
		t.Fatal(err)
	}
	sn.store = snapshotOf(t, "newer.metric")
	if err := sn.write(now); err != nil {
		t.Fatal(err)
	}

	// The state a crash between the renames leaves behind.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	restored := newStore()
	res := newSnapshotter(restored, path).restore(now)
	if res.from != sn.prev() {
		t.Fatalf("restored from %q, want the previous generation", res.from)
	}
	if len(res.problems) != 0 {
		t.Errorf("problems = %v; a missing current file is not an error", res.problems)
	}
	if _, ok := mergeAll(restored, "survivor.metric"); !ok {
		t.Error("the previous generation did not carry the data")
	}
}
