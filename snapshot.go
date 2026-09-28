package main

// Surviving a restart.
//
// §2 chose in-memory state with no persistence, and every section since has
// been written on top of that: a restart loses the last minute of every
// metric. §31 sharpened it into a sentence - the server cannot observe its
// own death, because the evidence dies with it.
//
// What to persist follows from §1 rather than from taste. That section chose
// to store the conclusion rather than the events, and persistence inherits
// the choice: a write-ahead log of events would contradict it outright, and
// at the 5.8 M events/sec §30 measured it would be absurd - megabytes a
// second of disk to reconstruct numbers the server already has. The
// aggregates *are* the conclusion, they are small, and they are what a
// restart needs back.
//
// So: a periodic snapshot of the aggregates, and a load at startup. The cost
// is bounded and stated rather than hidden - a crash loses at most one
// snapshot interval, and a graceful shutdown loses nothing because it takes
// a final one on the way out.
//
// Three things the format has to get right, all of them about a file that
// might be damaged rather than about speed. Snapshots are written once an
// interval, so nothing here is on a hot path.
//
//   - A half-written file must never be loaded. Written to a temporary path,
//     synced, then renamed, so a reader sees either the old snapshot or the
//     new one.
//   - A corrupt file must be refused, not misread. Magic bytes, a version and
//     a checksum, all verified before a single aggregate is decoded.
//   - A damaged file must not be able to crash the process or exhaust it.
//     Every length prefix is checked against the bytes actually remaining, so
//     a flipped byte claiming four billion buckets is an error rather than an
//     allocation.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// snapMagic identifies the file and its layout. The version is part of
	// the magic rather than a separate field so a format change cannot be
	// mistaken for corruption, or corruption for a format change.
	snapMagic = "MFSNAP\x00\x01"

	// maxSnapshotBytes bounds what will be read back. The theoretical
	// maximum is a full store - 32 shards x 1024 metrics x 6 buckets with
	// their histograms, around 40 MB - so this is generous, and it is here
	// to stop a corrupt or hostile file from being read into memory at all.
	maxSnapshotBytes = 256 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// snapshotInterval is how often the store is written out, and therefore how
// much a crash costs: at most one interval of aggregates.
//
// Tied to bucketWidth because that is already the granularity everything
// else moves at, and because it puts the loss below the resolution of a
// single /stats bucket - a crash cannot lose a whole bucket's worth of an
// answer without also losing the bucket.
const snapshotInterval = bucketWidth

// snapshotPath is the file to persist to, from METRICFLOW_SNAPSHOT.
//
// Empty means off, which is the default and keeps a server that was never
// configured for persistence behaving exactly as it did before.
func snapshotPath() string { return os.Getenv("METRICFLOW_SNAPSHOT") }

// snapshot serialises every live bucket in the store.
//
// The reserved namespace is deliberately excluded (§31). Self-metrics
// describe one process, and the process that loads this file is a different
// one: carrying a predecessor's counters forward would make a counter appear
// to fall the moment the new process starts its own from zero, which is the
// one thing a counter must never do.
func (s *Store) snapshot(now time.Time) []byte {
	cutoff := windowStart(now, window)

	var b []byte
	b = append(b, snapMagic...)
	b = binary.LittleEndian.AppendUint64(b, uint64(now.UnixMilli()))

	// The metric count is patched in once known, so the store's locks are
	// never held while counting separately.
	countAt := len(b)
	b = binary.LittleEndian.AppendUint32(b, 0)

	metrics := uint32(0)
	for i := range s.shards {
		sh := &s.shards[i]

		sh.mu.Lock()
		for name, series := range sh.aggs {
			if reserved(name) {
				continue
			}
			live := 0
			for key := range series {
				if key >= cutoff {
					live++
				}
			}
			if live == 0 {
				continue // nothing inside the window is worth carrying
			}

			metrics++
			b = appendString(b, name)
			b = binary.LittleEndian.AppendUint32(b, uint32(live))
			for key, a := range series {
				if key >= cutoff {
					b = appendBucket(b, key, a)
				}
			}
		}
		sh.mu.Unlock()
	}
	binary.LittleEndian.PutUint32(b[countAt:], metrics)

	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crcTable))
}

func appendString(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func appendBucket(b []byte, key int64, a *Agg) []byte {
	b = binary.LittleEndian.AppendUint64(b, uint64(key))
	b = binary.LittleEndian.AppendUint64(b, uint64(a.Count))
	b = binary.LittleEndian.AppendUint64(b, math.Float64bits(a.Sum))
	b = binary.LittleEndian.AppendUint64(b, math.Float64bits(a.Min))
	b = binary.LittleEndian.AppendUint64(b, math.Float64bits(a.Max))

	// A bucket built by a test may have no histogram (§23), so the nil case
	// is written as an empty one rather than skipped - the reader should not
	// have to know which shape produced the file.
	h := a.h
	if h == nil {
		h = &hist{}
	}
	b = binary.LittleEndian.AppendUint64(b, uint64(h.zeros))
	b = binary.LittleEndian.AppendUint64(b, uint64(h.count))
	b = appendHistBuckets(b, h.pos)
	return appendHistBuckets(b, h.neg)
}

func appendHistBuckets(b []byte, m map[int32]int64) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m)))
	for k, v := range m {
		b = binary.LittleEndian.AppendUint32(b, uint32(k))
		b = binary.LittleEndian.AppendUint64(b, uint64(v))
	}
	return b
}

// reader walks a snapshot, refusing to read past its end.
//
// Every read is bounds-checked rather than trusted. The checksum already
// rejects a damaged file, but a file with a valid checksum can still have
// been written by something else entirely, and "we wrote it" is not a
// property the process reading it can verify.
type reader struct {
	b []byte
	i int
}

var errTruncated = errors.New("snapshot ends mid-record")

func (r *reader) take(n int) ([]byte, error) {
	if n < 0 || len(r.b)-r.i < n {
		return nil, errTruncated
	}
	out := r.b[r.i : r.i+n]
	r.i += n
	return out, nil
}

func (r *reader) uint16() (uint16, error) {
	b, err := r.take(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}

func (r *reader) uint32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (r *reader) uint64() (uint64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *reader) float64() (float64, error) {
	v, err := r.uint64()
	return math.Float64frombits(v), err
}

// snapshotMetric is one metric's worth of decoded buckets.
type snapshotMetric struct {
	name    string
	buckets map[int64]*Agg
}

// decodeSnapshot verifies and parses a snapshot, returning the metrics it
// holds and the time it was written.
func decodeSnapshot(b []byte) ([]snapshotMetric, time.Time, error) {
	var zero time.Time

	if len(b) > maxSnapshotBytes {
		return nil, zero, fmt.Errorf("snapshot is %d bytes, over the %d-byte limit",
			len(b), maxSnapshotBytes)
	}
	if len(b) < len(snapMagic)+4 {
		return nil, zero, errors.New("snapshot is too short to be one")
	}
	if string(b[:len(snapMagic)]) != snapMagic {
		return nil, zero, errors.New("not a snapshot, or written by a different version")
	}

	// Checked before anything is decoded, so the decoder below only ever
	// sees bytes that at least arrived intact.
	body, want := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if got := crc32.Checksum(body, crcTable); got != want {
		return nil, zero, fmt.Errorf("snapshot checksum is %08x, want %08x", got, want)
	}

	r := &reader{b: body, i: len(snapMagic)}

	ms, err := r.uint64()
	if err != nil {
		return nil, zero, err
	}
	written := time.UnixMilli(int64(ms))

	n, err := r.uint32()
	if err != nil {
		return nil, zero, err
	}
	// Sized against the bytes actually left rather than against the count
	// the file claims: the smallest possible metric is a few bytes, so a
	// count far larger than the file could hold is a corrupt one.
	if int(n) > len(body)-r.i {
		return nil, zero, fmt.Errorf("snapshot claims %d metrics in %d remaining bytes",
			n, len(body)-r.i)
	}

	out := make([]snapshotMetric, 0, n)
	for i := uint32(0); i < n; i++ {
		m, err := readMetric(r)
		if err != nil {
			return nil, zero, err
		}
		out = append(out, m)
	}
	if r.i != len(body) {
		return nil, zero, fmt.Errorf("snapshot has %d trailing bytes", len(body)-r.i)
	}
	return out, written, nil
}

func readMetric(r *reader) (snapshotMetric, error) {
	var m snapshotMetric

	nameLen, err := r.uint16()
	if err != nil {
		return m, err
	}
	name, err := r.take(int(nameLen))
	if err != nil {
		return m, err
	}
	m.name = string(name)

	count, err := r.uint32()
	if err != nil {
		return m, err
	}
	if int(count) > len(r.b)-r.i {
		return m, fmt.Errorf("metric %q claims %d buckets in %d remaining bytes",
			m.name, count, len(r.b)-r.i)
	}

	m.buckets = make(map[int64]*Agg, count)
	for i := uint32(0); i < count; i++ {
		key, a, err := readBucket(r)
		if err != nil {
			return m, err
		}
		m.buckets[key] = a
	}
	return m, nil
}

func readBucket(r *reader) (int64, *Agg, error) {
	key, err := r.uint64()
	if err != nil {
		return 0, nil, err
	}
	count, err := r.uint64()
	if err != nil {
		return 0, nil, err
	}
	sum, err := r.float64()
	if err != nil {
		return 0, nil, err
	}
	min, err := r.float64()
	if err != nil {
		return 0, nil, err
	}
	max, err := r.float64()
	if err != nil {
		return 0, nil, err
	}

	h := &hist{}
	zeros, err := r.uint64()
	if err != nil {
		return 0, nil, err
	}
	hcount, err := r.uint64()
	if err != nil {
		return 0, nil, err
	}
	h.zeros, h.count = int64(zeros), int64(hcount)

	if h.pos, err = readHistBuckets(r); err != nil {
		return 0, nil, err
	}
	if h.neg, err = readHistBuckets(r); err != nil {
		return 0, nil, err
	}

	return int64(key), &Agg{
		Count: int(count), Sum: sum, Min: min, Max: max, h: h,
	}, nil
}

func readHistBuckets(r *reader) (map[int32]int64, error) {
	n, err := r.uint32()
	if err != nil {
		return nil, err
	}
	// Each entry is twelve bytes, so a count that could not fit is corrupt.
	// Checked before allocating, which is the whole point.
	if int(n)*12 > len(r.b)-r.i {
		return nil, fmt.Errorf("histogram claims %d buckets in %d remaining bytes",
			n, len(r.b)-r.i)
	}
	if n == 0 {
		// Left nil rather than allocated, matching a histogram that never
		// saw a value of that sign (§23).
		return nil, nil
	}

	m := make(map[int32]int64, n)
	for i := uint32(0); i < n; i++ {
		k, err := r.uint32()
		if err != nil {
			return nil, err
		}
		v, err := r.uint64()
		if err != nil {
			return nil, err
		}
		m[int32(k)] = int64(v)
	}
	return m, nil
}

// load inserts a decoded snapshot into the store, dropping anything that has
// aged out since it was written.
//
// It reports what it took and what it left behind, because both are things
// an operator wants in the log: a restart that silently restored a third of
// its metrics would look exactly like one that restored all of them.
func (s *Store) load(metrics []snapshotMetric, now time.Time) (loaded, dropped, buckets int) {
	cutoff := windowStart(now, window)

	for _, m := range metrics {
		// A snapshot from long enough ago is entirely stale, which is not
		// an error - it is what retention means.
		live := make(map[int64]*Agg, len(m.buckets))
		for key, a := range m.buckets {
			if key >= cutoff {
				live[key] = a
			}
		}
		if len(live) == 0 {
			dropped++
			continue
		}
		// The reserved namespace is never written, so a file containing it
		// was not written by this server. Refused rather than trusted.
		if reserved(m.name) {
			dropped++
			continue
		}

		sh := s.shardFor(m.name)
		sh.mu.Lock()
		// The cap still applies (§27). A snapshot taken under a larger cap,
		// or restored onto a server configured with a smaller one, must not
		// be able to exceed it just because the data used to fit.
		if _, exists := sh.aggs[m.name]; !exists && len(sh.aggs) >= maxMetricsPerShard {
			sh.mu.Unlock()
			dropped++
			continue
		}
		sh.aggs[m.name] = live
		sh.mu.Unlock()

		loaded++
		buckets += len(live)
	}
	return loaded, dropped, buckets
}

// snapshotter owns one snapshot file, the generation behind it, and the
// numbers §33 publishes about both.
//
// It exists because rotation needs memory. Whether the file currently on
// disk is one this process wrote decides whether it is safe to rotate into
// the previous slot, and nothing stateless can answer that.
type snapshotter struct {
	store *Store
	path  string

	mu        sync.Mutex
	wroteOnce bool      // this process has replaced the current file at least once
	lastOK    time.Time // when the last write succeeded
	lastBytes int       // and how big it was
	failures  atomic.Int64
}

func newSnapshotter(s *Store, path string) *snapshotter {
	return &snapshotter{store: s, path: path}
}

// prev is where the generation before the current one lives.
func (sn *snapshotter) prev() string { return sn.path + ".prev" }

// write writes the store out, keeping the file it replaces.
//
// Temp, sync, rotate, rename. The rotation is the addition §32 asked for:
// the current file becomes the previous one before the new file takes its
// place, so a snapshot that turns out to be unreadable is not the only copy.
//
// The first write of a process never rotates, and that is the subtle part.
// On startup the file on disk may be the corrupt one that just failed to
// load - rotating it would overwrite a perfectly good previous generation
// with a known-bad file, turning one damaged copy into two. Only a file this
// process wrote, and therefore knows to be good, is ever promoted.
//
// A crash between the two renames leaves no current file and a good previous
// one, which restore treats as a fallback rather than as a failure.
func (sn *snapshotter) write(now time.Time) error {
	data := sn.store.snapshot(now)

	dir := filepath.Dir(sn.path)
	tmp, err := os.CreateTemp(dir, filepath.Base(sn.path)+".tmp*")
	if err != nil {
		sn.failures.Add(1)
		return fmt.Errorf("creating a temporary snapshot: %w", err)
	}
	tmpName := tmp.Name()

	// Any failure from here leaves the temporary file behind, so it is
	// removed on every path that does not rename it away.
	committed := false
	defer func() {
		tmp.Close()
		if !committed {
			os.Remove(tmpName)
			sn.failures.Add(1)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("writing the snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("syncing the snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing the snapshot: %w", err)
	}

	sn.mu.Lock()
	defer sn.mu.Unlock()

	if sn.wroteOnce {
		if _, err := os.Stat(sn.path); err == nil {
			if err := os.Rename(sn.path, sn.prev()); err != nil {
				return fmt.Errorf("rotating the previous snapshot: %w", err)
			}
		}
	}
	if err := os.Rename(tmpName, sn.path); err != nil {
		return fmt.Errorf("replacing the snapshot: %w", err)
	}
	syncDir(dir)

	committed = true
	sn.wroteOnce = true
	sn.lastOK, sn.lastBytes = now, len(data)
	return nil
}

// syncDir flushes a directory so the renames above survive a crash.
//
// Without it the file's contents are durable but the rename that installed
// them need not be, and a crash can leave a name pointing at nothing. Windows
// has no equivalent and rejects a Sync on a directory handle, so a failure is
// ignored rather than propagated - the alternative is failing every snapshot
// on a platform where the call simply does not apply.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	defer d.Close()
	_ = d.Sync()
}

// restoreResult is what a startup restore did, and what it could not do.
type restoreResult struct {
	from     string // the file actually loaded; empty if none was usable
	loaded   int
	dropped  int
	buckets  int
	problems []string // why the newer candidates were passed over
}

// restore loads the newest usable snapshot, falling back to the generation
// before it.
//
// A missing file is not a problem: the first start of a server has nothing to
// restore, and treating that as a failure would make persistence something
// you set up rather than something that starts working.
//
// A file that is present and unusable *is* a problem, and it is reported
// rather than swallowed - but it is not the end of the attempt, which is the
// whole point of keeping a second copy.
func (sn *snapshotter) restore(now time.Time) restoreResult {
	var res restoreResult

	for _, candidate := range []string{sn.path, sn.prev()} {
		data, err := os.ReadFile(candidate)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			res.problems = append(res.problems,
				fmt.Sprintf("%s could not be read: %v", candidate, err))
			continue
		}

		metrics, _, err := decodeSnapshot(data)
		if err != nil {
			res.problems = append(res.problems,
				fmt.Sprintf("%s is unusable: %v", candidate, err))
			continue
		}

		res.from = candidate
		res.loaded, res.dropped, res.buckets = sn.store.load(metrics, now)
		return res
	}
	return res
}

// Run writes the store every `every` until ctx is cancelled.
//
// A write failure here is logged and the loop carries on, which is the
// opposite of how a bad path is treated at startup, and the asymmetry is
// deliberate. A configuration that is wrong before the server has served
// anything is certainly wrong and costs nothing to refuse. A disk that fills
// at three in the morning is a different thing: killing a working server to
// protest it would throw away the very data persistence exists to protect, so
// it stays up, stays loud, and counts the failures where §31 can see them.
func (sn *snapshotter) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-t.C:
			if err := sn.write(tick); err != nil {
				log.Printf("snapshot failed: %v", err)
			}
		}
	}
}
