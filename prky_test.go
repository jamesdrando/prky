package prky

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// newTestStore creates a new Store with a temporary WAL file and no compaction.
// It registers t.Cleanup to ensure Close is always called.
func newTestStore(t *testing.T) *Store {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	s, err := NewStore(path, 0 /* no compaction */)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	return s
}

// TestDefaultMaxValueBytes_PutWithinLimit ensures that by default,
// a value at or below the configured limit is accepted.
func TestDefaultMaxValueBytes_PutWithinLimit(t *testing.T) {
	s := newTestStore(t)

	// Default expected soft limit: 128 KiB
	const defaultLimit = 128 * 1024

	value := make([]byte, defaultLimit) // exactly at the limit
	if err := s.Put("ok", value, 0); err != nil {
		t.Fatalf("Put() within default limit returned error: %v", err)
	}

	// Sanity check: value should be retrievable
	_, got, err := s.Get("ok")
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if len(got) != len(value) {
		t.Fatalf("Get() value length = %d, want %d", len(got), len(value))
	}
}

// TestDefaultMaxValueBytes_PutExceedsLimit ensures that by default,
// a value larger than the configured limit is rejected with ErrValueTooLarge.
func TestDefaultMaxValueBytes_PutExceedsLimit(t *testing.T) {
	s := newTestStore(t)

	const defaultLimit = 128 * 1024
	value := make([]byte, defaultLimit+1)

	err := s.Put("too_big", value, 0)
	if err == nil {
		t.Fatalf("Put() over default limit = %d bytes, expected error, got nil", len(value))
	}
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("Put() error = %v, want ErrValueTooLarge", err)
	}
}

// TestSetMaxValueBytes_TighterLimit verifies that tightening the limit
// causes larger values to be rejected while still allowing smaller ones.
func TestSetMaxValueBytes_TighterLimit(t *testing.T) {
	s := newTestStore(t)

	// Make the limit small for this test.
	const newLimit = 1024 // 1 KiB

	if err := s.SetMaxValueBytes(newLimit); err != nil {
		t.Fatalf("SetMaxValueBytes(%d) error = %v", newLimit, err)
	}

	small := make([]byte, newLimit)
	if err := s.Put("small", small, 0); err != nil {
		t.Fatalf("Put() with size %d (<= limit) returned error: %v", len(small), err)
	}

	tooBig := make([]byte, newLimit+1)
	err := s.Put("too_big", tooBig, 0)
	if err == nil {
		t.Fatalf("Put() with size %d (> limit) expected error, got nil", len(tooBig))
	}
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("Put() error = %v, want ErrValueTooLarge", err)
	}
}

// TestSetMaxValueBytes_InvalidValues ensures invalid configuration values are rejected.
func TestSetMaxValueBytes_InvalidValues(t *testing.T) {
	s := newTestStore(t)

	tests := []struct {
		name string
		val  int
	}{
		{"Zero", 0},
		{"Negative", -1},
		{"AboveHardLimit", s.hardValueLimit + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.SetMaxValueBytes(tt.val)
			if err == nil {
				t.Fatalf("SetMaxValueBytes(%d) expected error, got nil", tt.val)
			}
		})
	}
}

// TestSetMaxValueBytes_DoesNotAffectExistingData verifies that lowering the limit
// does not affect already stored values (reads should still work).
func TestSetMaxValueBytes_DoesNotAffectExistingData(t *testing.T) {
	s := newTestStore(t)

	// Start with a generous limit to allow a "large" value.
	const initialLimit = 64 * 1024
	if err := s.SetMaxValueBytes(initialLimit); err != nil {
		t.Fatalf("SetMaxValueBytes(%d) error = %v", initialLimit, err)
	}

	// Store a value near that limit.
	val := make([]byte, initialLimit-128)
	if err := s.Put("existing", val, 0); err != nil {
		t.Fatalf("Put() initial large value error = %v", err)
	}

	// Lower the limit significantly.
	const newLimit = 1024
	if err := s.SetMaxValueBytes(newLimit); err != nil {
		t.Fatalf("SetMaxValueBytes(%d) error = %v", newLimit, err)
	}

	// Existing value should still be readable.
	_, got, err := s.Get("existing")
	if err != nil {
		t.Fatalf("Get() after lowering limit returned error: %v", err)
	}
	if len(got) != len(val) {
		t.Fatalf("Get() length after lowering limit = %d, want %d", len(got), len(val))
	}

	// But new values above the new limit should be rejected.
	tooBig := make([]byte, newLimit+1)
	err = s.Put("new_too_big", tooBig, 0)
	if err == nil {
		t.Fatalf("Put() with size %d (> new limit) expected error, got nil", len(tooBig))
	}
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("Put() error = %v, want ErrValueTooLarge", err)
	}
}

// TestValueLimit_ConcurrentPuts ensures that the value limit is enforced
// even under concurrent write load.
func TestValueLimit_ConcurrentPuts(t *testing.T) {
	s := newTestStore(t)

	// Use a moderate limit.
	const limit = 8 * 1024
	if err := s.SetMaxValueBytes(limit); err != nil {
		t.Fatalf("SetMaxValueBytes(%d) error = %v", limit, err)
	}

	small := make([]byte, limit)
	big := make([]byte, limit+1)

	const writers = 8
	errCh := make(chan error, writers*2)

	// Spin up some concurrent writers.
	for i := 0; i < writers; i++ {
		go func(id int) {
			// Small should always succeed.
			if err := s.Put(
				// Make key unique per writer
				fmt.Sprintf("small-%d", id),
				small,
				0,
			); err != nil {
				errCh <- fmt.Errorf("writer %d: small Put() error: %w", id, err)
				return
			}

			// Big should always fail.
			err := s.Put(
				fmt.Sprintf("big-%d", id),
				big,
				0,
			)
			if err == nil {
				errCh <- fmt.Errorf("writer %d: big Put() unexpectedly succeeded", id)
				return
			}
			if !errors.Is(err, ErrValueTooLarge) {
				errCh <- fmt.Errorf("writer %d: big Put() error = %v, want ErrValueTooLarge", id, err)
				return
			}

			errCh <- nil
		}(i)
	}

	// Collect results with a timeout to avoid hanging tests.
	timeout := time.After(5 * time.Second)
	for i := 0; i < writers; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("concurrent writer error: %v", err)
			}
		case <-timeout:
			t.Fatalf("timeout waiting for concurrent writers")
		}
	}
}
