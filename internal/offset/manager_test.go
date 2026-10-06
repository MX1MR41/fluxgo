package offset

import (
	"errors"
	"testing"
)

func TestCommitFetchRoundTrip(t *testing.T) {
	m, err := NewManager(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	if _, err := m.Fetch("g1", "orders", 0); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Fetch before commit: got %v, want ErrOffsetNotFound", err)
	}

	if err := m.Commit("g1", "orders", 0, 42); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	off, err := m.Fetch("g1", "orders", 0)
	if err != nil || off != 42 {
		t.Fatalf("Fetch: off=%d err=%v, want 42", off, err)
	}

	// Overwrite.
	if err := m.Commit("g1", "orders", 0, 100); err != nil {
		t.Fatalf("Commit overwrite: %v", err)
	}
	if off, _ := m.Fetch("g1", "orders", 0); off != 100 {
		t.Fatalf("Fetch after overwrite: off=%d, want 100", off)
	}

	// Groups and partitions are independent.
	if _, err := m.Fetch("g2", "orders", 0); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Fetch other group: got %v, want ErrOffsetNotFound", err)
	}
	if _, err := m.Fetch("g1", "orders", 1); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Fetch other partition: got %v, want ErrOffsetNotFound", err)
	}
	if _, err := m.Fetch("g1", "other-topic", 0); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Fetch other topic: got %v, want ErrOffsetNotFound", err)
	}
}

func TestPersistenceAcrossManagers(t *testing.T) {
	dir := t.TempDir()
	m1, _ := NewManager(dir, nil)
	if err := m1.Commit("g", "t", 3, 7); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	m2, _ := NewManager(dir, nil)
	off, err := m2.Fetch("g", "t", 3)
	if err != nil || off != 7 {
		t.Fatalf("Fetch with new manager: off=%d err=%v, want 7", off, err)
	}
}

func TestInvalidNamesRejected(t *testing.T) {
	m, _ := NewManager(t.TempDir(), nil)
	for _, bad := range []string{"", "..", "../x", "a/b", "a\\b"} {
		if err := m.Commit(bad, "t", 0, 1); err == nil {
			t.Fatalf("Commit with group %q succeeded", bad)
		}
		if err := m.Commit("g", bad, 0, 1); err == nil {
			t.Fatalf("Commit with topic %q succeeded", bad)
		}
		if _, err := m.Fetch(bad, "t", 0); err == nil {
			t.Fatalf("Fetch with group %q succeeded", bad)
		}
	}
}
