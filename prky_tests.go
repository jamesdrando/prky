package prky

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

// TestData is a simple struct we'll use for testing.
type TestData struct {
	Name string
	Age  int
}

// tempStore is a helper function that creates a new Store
// in a temporary WAL file for testing. It returns the store,
// the path to the WAL, and a cleanup function to call via defer.
func tempStore(t *testing.T, compactionInterval time.Duration) (store *Store, path string, cleanup func()) {
	t.Helper()

	// Create a temp file
	f, err := os.CreateTemp("", "prky_test_*.wal")
	if err != nil {
		t.Fatalf("Failed to create temp WAL file: %v", err)
	}
	path = f.Name()
	f.Close() // Close it so NewStore can open it

	store, err = NewStore(path, compactionInterval)
	if err != nil {
		os.Remove(path) //nolint:errcheck
		t.Fatalf("Failed to create new store: %v", err)
	}

	// Teardown function
	cleanup = func() {
		if err := store.Close(); err != nil {
			t.Errorf("Error closing store: %v", err)
		}
		os.Remove(path)              //nolint:errcheck
		os.Remove(path + ".compact") //nolint:errcheck
		os.Remove(path + ".old")     //nolint:errcheck
	}

	return store, path, cleanup
}

// TestPutGet tests the basic Put and Get functionality.
func TestPutGet(t *testing.T) {
	t.Parallel()
	store, _, cleanup := tempStore(t, 0)
	defer cleanup()

	key := "alice"
	putData := TestData{Name: "Alice", Age: 30}
	var getData TestData

	// 1. Test Put on a new key
	err := store.Put(key, putData, 0) // Expect version 0
	if err != nil {
		t.Fatalf("Expected no error on initial Put, got %v", err)
	}

	// 2. Test Get on the new key
	version, err := store.Get(key, &getData)
	if err != nil {
		t.Fatalf("Expected no error on Get, got %v", err)
	}

	if version != 1 {
		t.Errorf("Expected version 1 after initial Put, got %d", version)
	}
	if getData != putData {
		t.Errorf("Get data mismatch. Got %+v, expected %+v", getData, putData)
	}

	// 3. Test Get on a non-existent key
	_, err = store.Get("bob", &getData)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound for non-existent key, got %v", err)
	}
}

// TestPutOptimisticLock tests the version-mismatch logic.
func TestPutOptimisticLock(t *testing.T) {
	t.Parallel()
	store, _, cleanup := tempStore(t, 0)
	defer cleanup()

	key := "optimistic"
	data := TestData{Name: "Test", Age: 1}

	// 1. Initial Put, v0 -> v1
	err := store.Put(key, data, 0)
	if err != nil {
		t.Fatalf("Expected no error on initial Put, got %v", err)
	}

	// 2. Try to Put again, expecting v0 (should fail)
	err = store.Put(key, data, 0)
	if err != ErrVersionMismatch {
		t.Errorf("Expected ErrVersionMismatch, got %v", err)
	}

	// 3. Try to Put again, expecting v1 (should succeed)
	data.Age = 2
	err = store.Put(key, data, 1)
	if err != nil {
		t.Fatalf("Expected no error on v1 Put, got %v", err)
	}

	// 4. Verify the new data and version
	var getData TestData
	version, err := store.Get(key, &getData)
	if err != nil {
		t.Fatalf("Expected no error on final Get, got %v", err)
	}
	if version != 2 {
		t.Errorf("Expected version 2, got %d", version)
	}
	if getData.Age != 2 {
		t.Errorf("Expected data.Age to be 2, got %d", getData.Age)
	}
}

// TestDelete tests the Delete functionality.
func TestDelete(t *testing.T) {
	t.Parallel()
	store, _, cleanup := tempStore(t, 0)
	defer cleanup()

	key := "to_delete"
	data := TestData{Name: "Delete Me", Age: 99}

	// 1. Put the key, v0 -> v1
	err := store.Put(key, data, 0)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Try to Delete with wrong version
	err = store.Delete(key, 0)
	if err != ErrVersionMismatch {
		t.Errorf("Expected ErrVersionMismatch, got %v", err)
	}

	// 3. Try to Delete a non-existent key
	err = store.Delete("non-existent", 0)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}

	// 4. Delete with correct version
	err = store.Delete(key, 1)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 5. Verify Get fails with ErrNotFound
	var getData TestData
	_, err = store.Get(key, &getData)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound after Delete, got %v", err)
	}
}

// TestPersistence checks if data is correctly reloaded from the WAL.
func TestPersistence(t *testing.T) {
	t.Parallel()
	store, path, cleanup := tempStore(t, 0)

	// 1. Perform a series of operations
	// key1: v0 -> v1 (A) -> v2 (B)
	// key2: v0 -> v1 (C) -> v2 (Deleted)
	// key3: v0 -> v1 (D)
	putA := TestData{Name: "A", Age: 1}
	putB := TestData{Name: "B", Age: 2}
	putC := TestData{Name: "C", Age: 3}
	putD := TestData{Name: "D", Age: 4}

	store.Put("key1", putA, 0) //nolint:errcheck
	store.Put("key2", putC, 0) //nolint:errcheck
	store.Put("key1", putB, 1) //nolint:errcheck
	store.Delete("key2", 1)    //nolint:errcheck
	store.Put("key3", putD, 0) //nolint:errcheck

	// 2. Close the store
	cleanup() // This calls Close() and *keeps* the file

	// 3. Re-open the store from the same WAL file
	store2, err := NewStore(path, 0)
	if err != nil {
		t.Fatalf("Failed to reopen store: %v", err)
	}
	defer store2.Close()  //nolint:errcheck
	defer os.Remove(path) //nolint:errcheck

	// 4. Verify the state was replayed correctly
	var getData TestData

	// Check key1 (should be 'B', v2)
	version, err := store2.Get("key1", &getData)
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}
	if version != 2 {
		t.Errorf("key1: Expected version 2, got %d", version)
	}
	if getData != putB {
		t.Errorf("key1: Data mismatch. Got %+v, expected %+v", getData, putB)
	}

	// Check key2 (should be not found)
	_, err = store2.Get("key2", &getData)
	if err != ErrNotFound {
		t.Errorf("key2: Expected ErrNotFound, got %v", err)
	}

	// Check key3 (should be 'D', v1)
	version, err = store2.Get("key3", &getData)
	if err != nil {
		t.Fatalf("Get key3 failed: %v", err)
	}
	if version != 1 {
		t.Errorf("key3: Expected version 1, got %d", version)
	}
	if getData != putD {
		t.Errorf("key3: Data mismatch. Got %+v, expected %+v", getData, putD)
	}
}

// TestCompaction checks if the log is correctly compacted.
func TestCompaction(t *testing.T) {
	t.Parallel()
	store, path, cleanup := tempStore(t, 0) // No background compaction

	// 1. Create a "dirty" log
	store.Put("key1", TestData{"A", 1}, 0) // v1
	store.Put("key1", TestData{"B", 2}, 1) // v2 (live)
	store.Put("key2", TestData{"C", 3}, 0) // v1
	store.Delete("key2", 1)                // v2 (dead)
	store.Put("key3", TestData{"D", 4}, 0) // v1 (live)

	// 2. Manually trigger compaction
	if err := store.Compact(); err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// 3. Close and reopen to force replay from the *new* WAL
	cleanup()
	store2, err := NewStore(path, 0)
	if err != nil {
		t.Fatalf("Failed to reopen store after compaction: %v", err)
	}
	defer store2.Close()  //nolint:errcheck
	defer os.Remove(path) //nolint:errcheck

	// 4. Verify state (same as persistence test)
	var getData TestData
	// Check key1 (should be 'B', v2)
	version, err := store2.Get("key1", &getData)
	if err != nil || version != 2 || getData.Name != "B" {
		t.Errorf("key1 state incorrect after compaction. err=%v, v=%d, data=%+v", err, version, getData)
	}
	// Check key2 (should be not found)
	_, err = store2.Get("key2", &getData)
	if err != ErrNotFound {
		t.Errorf("key2: Expected ErrNotFound after compaction, got %v", err)
	}
	// Check key3 (should be 'D', v1)
	version, err = store2.Get("key3", &getData)
	if err != nil || version != 1 || getData.Name != "D" {
		t.Errorf("key3 state incorrect after compaction. err=%v, v=%d, data=%+v", err, version, getData)
	}
}

// TestCompactionBackground tests that the background loop runs
// and that Close() correctly waits for it.
func TestCompactionBackground(t *testing.T) {
	t.Parallel()
	// Use a short interval to ensure compaction runs
	store, _, cleanup := tempStore(t, 20*time.Millisecond)

	// Do a bunch of writes
	store.Put("key1", TestData{"A", 1}, 0) // v1
	store.Put("key1", TestData{"B", 2}, 1) // v2 (live)
	store.Put("key2", TestData{"C", 3}, 0) // v1
	store.Delete("key2", 1)                // v2 (dead)
	store.Put("key3", TestData{"D", 4}, 0) // v1 (live)

	// Wait long enough for at least one compaction
	time.Sleep(50 * time.Millisecond)

	// Add more data *after* initial compaction
	store.Put("key3", TestData{"E", 5}, 1) // v2 (live)

	// Wait again
	time.Sleep(50 * time.Millisecond)

	// Cleanup will call Close(), which *must* wait for the
	// compaction goroutine to finish. This test passing
	// (i.e., not leaking a goroutine) is the success.
	cleanup()
}

// TestConcurrency performs many parallel read-modify-write ops
// to check for race conditions.
func TestConcurrency(t *testing.T) {
	t.Parallel()
	store, _, cleanup := tempStore(t, 0) // No compaction
	defer cleanup()

	const numGoroutines = 100
	const numKeys = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			// Each goroutine increments a random counter
			key := fmt.Sprintf("key-%d", rand.Intn(numKeys))
			var val int

			// Loop until we successfully write
			for {
				version, err := store.Get(key, &val)
				if err == ErrNotFound {
					version = 0
					val = 0
				} else if err != nil {
					t.Errorf("Unexpected Get error: %v", err)
					return
				}

				// Our "write" is to increment the value
				err = store.Put(key, val+1, version)
				if err == ErrVersionMismatch {
					continue // Expected, retry
				} else if err != nil {
					t.Errorf("Unexpected Put error: %v", err)
					return
				}
				break // Success!
			}
		}()
	}
	wg.Wait()

	// Verify the final state
	// The *sum* of all counter values should be 100.
	totalSum := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		var val int
		_, err := store.Get(key, &val)
		if err != nil && err != ErrNotFound {
			t.Fatalf("Final Get error for key %s: %v", key, err)
		}
		totalSum += val
	}

	if totalSum != numGoroutines {
		t.Errorf("Expected final sum to be %d, got %d", numGoroutines, totalSum)
	}
}

// TestConcurrencyWithCompaction is the deadlock stress test.
// It runs the same logic as TestConcurrency, but with a
// very fast background compaction loop.
func TestConcurrencyWithCompaction(t *testing.T) {
	t.Parallel()
	// Fast compaction to stress the locks
	store, _, cleanup := tempStore(t, 5*time.Millisecond)
	defer cleanup()

	const numGoroutines = 200 // More goroutines
	const numOps = 5          // Each goroutine does 5 ops
	const numKeys = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// We need to check for errors inside the goroutines
	errChan := make(chan error, numGoroutines*numOps)

	runLogic := func() {
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				key := fmt.Sprintf("key-%d", rand.Intn(numKeys))
				var val int
				for j := 0; j < numOps; j++ { // Each goroutine does 5 ops
					for { // Retry loop
						version, err := store.Get(key, &val)
						if err == ErrNotFound {
							version = 0
							val = 0
						} else if err != nil {
							errChan <- fmt.Errorf("Get error: %w", err)
							return
						}
						err = store.Put(key, val+1, version)
						if err == ErrVersionMismatch {
							continue
						} else if err != nil {
							errChan <- fmt.Errorf("Put error: %w", err)
							return
						}
						break // Success
					}
				}
			}()
		}
		wg.Wait()
		close(errChan)
	}

	// Run this logic with a timeout, in case of deadlock
	done := make(chan struct{})
	go func() {
		runLogic()
		close(done)
	}()

	select {
	case <-done:
		// Test finished.
	case <-time.After(15 * time.Second): // 15-second timeout
		t.Fatal("Test timed out, potential deadlock!")
	}

	// Check if any goroutines reported an error
	if err := <-errChan; err != nil {
		t.Fatalf("Goroutine failed: %v", err)
	}

	// Check final sum
	totalSum := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		var val int
		_, err := store.Get(key, &val)
		if err != nil && err != ErrNotFound {
			t.Fatalf("Final Get error for key %s: %v", key, err)
		}
		totalSum += val
	}

	expectedSum := numGoroutines * numOps
	if totalSum != expectedSum {
		t.Errorf("Expected final sum to be %d, got %d", expectedSum, totalSum)
	}
}
