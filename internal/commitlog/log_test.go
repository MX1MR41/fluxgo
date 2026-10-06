package commitlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		MaxSegmentBytes: 1024,
		MaxLogBytes:     0, // disabled unless a test opts in
		FileSync:        false,
	}
}

func openTestLog(t *testing.T, cfg Config) (*Log, string) {
	t.Helper()
	dir := t.TempDir()
	l, err := Open(dir, "test_0", cfg, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return l, dir
}

func TestAppendReadRoundTrip(t *testing.T) {
	l, _ := openTestLog(t, testConfig(t))
	defer l.Close()

	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("message-%d", i))
		off, err := l.Append(msg)
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if off != uint64(i) {
			t.Fatalf("Append %d: got offset %d", i, off)
		}
	}
	for i := 0; i < 100; i++ {
		rec, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if want := fmt.Sprintf("message-%d", i); string(rec) != want {
			t.Fatalf("Read %d: got %q, want %q", i, rec, want)
		}
	}
	if got, want := l.HighestOffset(), uint64(100); got != want {
		t.Fatalf("HighestOffset = %d, want %d", got, want)
	}
}

func TestReadPastEndAndEmptyMessage(t *testing.T) {
	l, _ := openTestLog(t, testConfig(t))
	defer l.Close()

	if _, err := l.Read(0); !errors.Is(err, ErrReadPastEnd) {
		t.Fatalf("Read on empty log: got %v, want ErrReadPastEnd", err)
	}
	if _, err := l.Append(nil); err != nil {
		t.Fatalf("Append empty record: %v", err)
	}
	rec, err := l.Read(0)
	if err != nil || len(rec) != 0 {
		t.Fatalf("Read empty record: rec=%q err=%v", rec, err)
	}
}

func TestSegmentRollover(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxSegmentBytes = 64
	l, _ := openTestLog(t, cfg)
	defer l.Close()

	const n = 50
	for i := 0; i < n; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("record-%02d", i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if l.SegmentCount() < 2 {
		t.Fatalf("expected multiple segments, got %d", l.SegmentCount())
	}
	// All records must remain readable across segment boundaries.
	for i := 0; i < n; i++ {
		rec, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read %d after rollover: %v", i, err)
		}
		if want := fmt.Sprintf("record-%02d", i); string(rec) != want {
			t.Fatalf("Read %d: got %q, want %q", i, rec, want)
		}
	}
}

// TestReopenContinuesAppending is the regression test for the V1 bug where
// the index file was opened without O_APPEND, so appends after a restart
// overwrote existing index entries and corrupted the segment.
func TestReopenContinuesAppending(t *testing.T) {
	cfg := testConfig(t)
	l, dir := openTestLog(t, cfg)

	for i := 0; i < 10; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("before-%d", i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	l2, err := Open(dir, "test_0", cfg, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()

	if got := l2.HighestOffset(); got != 10 {
		t.Fatalf("HighestOffset after reopen = %d, want 10", got)
	}
	for i := 10; i < 20; i++ {
		off, err := l2.Append([]byte(fmt.Sprintf("after-%d", i)))
		if err != nil {
			t.Fatalf("Append after reopen: %v", err)
		}
		if off != uint64(i) {
			t.Fatalf("Append after reopen: got offset %d, want %d", off, i)
		}
	}
	for i := 0; i < 20; i++ {
		rec, err := l2.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read %d after reopen: %v", i, err)
		}
		var want string
		if i < 10 {
			want = fmt.Sprintf("before-%d", i)
		} else {
			want = fmt.Sprintf("after-%d", i)
		}
		if string(rec) != want {
			t.Fatalf("Read %d: got %q, want %q", i, rec, want)
		}
	}
}

// TestRecoveryAdoptsUnindexedTail simulates a crash after the record was
// written to the .log file but before its index entry landed. The log is the
// source of truth, so the complete record is adopted into the rebuilt index
// rather than thrown away.
func TestRecoveryAdoptsUnindexedTail(t *testing.T) {
	cfg := testConfig(t)
	l, dir := openTestLog(t, cfg)

	for i := 0; i < 5; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate the torn write: append a complete record to the .log without
	// touching the index.
	logPath := filepath.Join(dir, "00000000000000000000.log")
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	ghost := []byte("ghost-record")
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(ghost)))
	if _, err := f.Write(append(lenBuf[:], ghost...)); err != nil {
		t.Fatalf("write ghost: %v", err)
	}
	f.Close()

	l2, err := Open(dir, "test_0", cfg, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()

	if got := l2.HighestOffset(); got != 6 {
		t.Fatalf("HighestOffset after recovery = %d, want 6 (ghost adopted)", got)
	}
	rec, err := l2.Read(5)
	if err != nil || string(rec) != "ghost-record" {
		t.Fatalf("Read 5 (ghost): rec=%q err=%v", rec, err)
	}
	// New appends continue after the adopted record and stay readable.
	if off, err := l2.Append([]byte("real-6")); err != nil || off != 6 {
		t.Fatalf("Append after recovery: off=%d err=%v", off, err)
	}
	rec, err = l2.Read(6)
	if err != nil || string(rec) != "real-6" {
		t.Fatalf("Read 6 after recovery: rec=%q err=%v", rec, err)
	}
}

// TestRecoveryRebuildsIndex simulates a crash that left the index truncated.
func TestRecoveryRebuildsIndex(t *testing.T) {
	cfg := testConfig(t)
	l, dir := openTestLog(t, cfg)

	const n = 8
	for i := 0; i < n; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Damage the index: chop it down to the first 3 entries.
	indexPath := filepath.Join(dir, "00000000000000000000.index")
	if err := os.Truncate(indexPath, 3*indexEntryWidth); err != nil {
		t.Fatalf("truncate index: %v", err)
	}

	l2, err := Open(dir, "test_0", cfg, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()

	if got := l2.HighestOffset(); got != n {
		t.Fatalf("HighestOffset after index rebuild = %d, want %d", got, n)
	}
	for i := 0; i < n; i++ {
		rec, err := l2.Read(uint64(i))
		if err != nil || string(rec) != fmt.Sprintf("msg-%d", i) {
			t.Fatalf("Read %d after rebuild: rec=%q err=%v", i, rec, err)
		}
	}
}

// TestRecoveryTruncatesPartialRecord simulates a crash in the middle of
// writing a record's payload.
func TestRecoveryTruncatesPartialRecord(t *testing.T) {
	cfg := testConfig(t)
	l, dir := openTestLog(t, cfg)

	for i := 0; i < 3; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("ok-%d", i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append a length prefix with only half of the promised payload.
	logPath := filepath.Join(dir, "00000000000000000000.log")
	f, _ := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0)
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], 100)
	f.Write(lenBuf[:])
	f.Write([]byte("only-half"))
	f.Close()

	l2, err := Open(dir, "test_0", cfg, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()

	if got := l2.HighestOffset(); got != 3 {
		t.Fatalf("HighestOffset = %d, want 3", got)
	}
	for i := 0; i < 3; i++ {
		rec, err := l2.Read(uint64(i))
		if err != nil || string(rec) != fmt.Sprintf("ok-%d", i) {
			t.Fatalf("Read %d: rec=%q err=%v", i, rec, err)
		}
	}
}

func TestRetentionDeletesOldSegments(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxSegmentBytes = 128
	cfg.MaxLogBytes = 400
	l, _ := openTestLog(t, cfg)
	defer l.Close()

	// ~17 bytes per record; segments roll every ~7 records.
	for i := 0; i < 100; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("record-%03d", i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if l.Size() > cfg.MaxLogBytes+cfg.MaxSegmentBytes {
		t.Fatalf("total size %d exceeds retention bound %d", l.Size(), cfg.MaxLogBytes)
	}
	low, high := l.LowestOffset(), l.HighestOffset()
	if low == 0 {
		t.Fatalf("expected old segments to be deleted, lowest offset still 0")
	}
	if high != 100 {
		t.Fatalf("HighestOffset = %d, want 100", high)
	}
	if _, err := l.Read(0); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("Read deleted offset: got %v, want ErrOffsetOutOfRange", err)
	}
	if _, err := l.Read(low); err != nil {
		t.Fatalf("Read lowest retained offset %d: %v", low, err)
	}
	if _, err := l.Read(99); err != nil {
		t.Fatalf("Read newest offset: %v", err)
	}
	// The active segment must survive even if it alone exceeds MaxLogBytes.
	if l.SegmentCount() < 1 {
		t.Fatalf("no segments left")
	}
}

func TestReadBatch(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxSegmentBytes = 100 // force several segments
	l, _ := openTestLog(t, cfg)
	defer l.Close()

	const n = 30
	for i := 0; i < n; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("batch-%02d", i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Read everything in one batch, across segment boundaries.
	recs, err := l.ReadBatch(0, n, 0)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(recs) != n {
		t.Fatalf("ReadBatch returned %d records, want %d", len(recs), n)
	}
	for i, rec := range recs {
		if want := fmt.Sprintf("batch-%02d", i); string(rec) != want {
			t.Fatalf("record %d: got %q, want %q", i, rec, want)
		}
	}

	// Limit by count.
	recs, err = l.ReadBatch(5, 3, 0)
	if err != nil || len(recs) != 3 || string(recs[0]) != "batch-05" {
		t.Fatalf("ReadBatch count limit: n=%d err=%v first=%q", len(recs), err, recs[0])
	}

	// Limit by bytes: each record is 8+8=16 bytes on disk; 40 bytes fits 2.
	recs, err = l.ReadBatch(0, n, 40)
	if err != nil {
		t.Fatalf("ReadBatch bytes limit: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("ReadBatch bytes limit: got %d records, want 2", len(recs))
	}

	// maxBytes smaller than one record still returns that record.
	recs, err = l.ReadBatch(0, n, 1)
	if err != nil || len(recs) != 1 {
		t.Fatalf("ReadBatch tiny limit: n=%d err=%v", len(recs), err)
	}
}

func TestClosedLog(t *testing.T) {
	l, _ := openTestLog(t, testConfig(t))
	l.Append([]byte("x"))
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := l.Append([]byte("y")); !errors.Is(err, ErrLogClosed) {
		t.Fatalf("Append after close: got %v, want ErrLogClosed", err)
	}
	if _, err := l.Read(0); !errors.Is(err, ErrLogClosed) {
		t.Fatalf("Read after close: got %v, want ErrLogClosed", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestConcurrentAppendRead(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxSegmentBytes = 256
	l, _ := openTestLog(t, cfg)
	defer l.Close()

	const writers = 8
	const perWriter = 50
	done := make(chan error, writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			for i := 0; i < perWriter; i++ {
				if _, err := l.Append([]byte(fmt.Sprintf("w%d-%d", w, i))); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}(w)
	}
	for w := 0; w < writers; w++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent append: %v", err)
		}
	}
	if got, want := l.HighestOffset(), uint64(writers*perWriter); got != want {
		t.Fatalf("HighestOffset = %d, want %d", got, want)
	}
	// Spot check every record is readable.
	for i := uint64(0); i < uint64(writers*perWriter); i++ {
		if _, err := l.Read(i); err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
	}
}

func TestOrphanIndexFileIsIgnored(t *testing.T) {
	dir := t.TempDir()
	// An index with no matching .log must not create a phantom segment.
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000007.index"), make([]byte, 16), 0o666); err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir, "test_0", testConfig(t), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()
	if got := l.HighestOffset(); got != 0 {
		t.Fatalf("HighestOffset = %d, want 0 for log with orphan index", got)
	}
}
