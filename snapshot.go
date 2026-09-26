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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
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

// writeSnapshot writes the store to path, atomically.
//
// Temp file, sync, rename. Without the sync the rename can land before the
// contents do and a crash leaves an empty file where a good snapshot used to
// be; without the rename a crash mid-write leaves a half-file that the next
// start would refuse, throwing away a perfectly good older one.
//
// The rename is atomic on POSIX. On Windows os.Rename uses MoveFileEx with
// MOVEFILE_REPLACE_EXISTING, which replaces atomically as far as any reader
// is concerned - the guarantee this needs is that nobody sees a partial
// file, not that the operation is a single disk write.
func (s *Store) writeSnapshot(path string, now time.Time) error {
	data := s.snapshot(now)

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp*")
	if err != nil {
		return fmt.Errorf("creating a temporary snapshot: %w", err)
	}
	tmpName := tmp.Name()

	// Any failure from here on leaves the temporary file behind, so it is
	// removed on every path that does not rename it.
	defer func() {
		tmp.Close()
		os.Remove(tmpName) // a no-op once the rename has succeeded
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
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replacing the snapshot: %w", err)
	}
	return nil
}

// readSnapshot loads a snapshot from disk into the store.
//
// A missing file is not an error: the first start of a server has nothing to
// restore, and treating that as a failure would make persistence something
// you had to set up rather than something that just starts working.
func (s *Store) readSnapshot(path string, now time.Time) (loaded, dropped, buckets int, err error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, 0, nil
	}
	if err != nil {
		return 0, 0, 0, fmt.Errorf("reading the snapshot: %w", err)
	}

	metrics, _, err := decodeSnapshot(data)
	if err != nil {
		return 0, 0, 0, err
	}

	loaded, dropped, buckets = s.load(metrics, now)
	return loaded, dropped, buckets, nil
}
