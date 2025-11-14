package prky

import (
	"bytes"
	"crypto/rand"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/big"
	"os"
	"sync"
	"time" // Added for compaction
)

// ## 1. Public Errors
var (
	ErrNotFound        = errors.New("key not found")
	ErrVersionMismatch = errors.New("version mismatch (optimistic lock failed)")
	errCorruptLog      = errors.New("corrupt log file detected")
)

// ## 2. Internal Data Structures

const (
	numShards = 256
)

// internalEntry is the in-memory representation of a value.
type internalEntry struct {
	Version int64
	Value   []byte
}

// logEntry is the on-disk representation written to the WAL.
type logEntry struct {
	Key      string
	Value    []byte
	Version  int64
	Checksum uint32
	Deleted  bool
}

// shard contains a single map and its dedicated lock.
type shard struct {
	mux  sync.RWMutex
	data map[string]internalEntry
}

// Store is the main key-value store struct.
type Store struct {
	shards   [numShards]*shard
	wal      *os.File       // The Write-Ahead Log file
	walMux   sync.Mutex     // Protects all access to the WAL file
	walEnc   *gob.Encoder   // A reusable gob encoder for the WAL
	walPath  string         // Path to the WAL file
	wg       sync.WaitGroup // To wait for background goroutines on Close
	quit     chan struct{}  // To signal compaction to stop
	hashSeed uint64         // Seed for hash function
}

// ## 3. Constructor and Initialization

// NewStore creates or loads a key-value store from a WAL file.
// It also takes a 'compactionInterval'. If > 0, it will
// automatically compact the log file in the background.
func NewStore(path string, compactionInterval time.Duration) (*Store, error) {
	walFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	seedVal, err := rand.Int(rand.Reader, new(big.Int).SetUint64(0xFFFFFFFFFFFFFFFF))
	if err != nil {
		return nil, fmt.Errorf("failed to generate hash seed: %w", err)
	}

	s := &Store{
		wal:      walFile,
		walEnc:   gob.NewEncoder(walFile),
		walPath:  path,
		quit:     make(chan struct{}), // For signaling compaction goroutine
		hashSeed: seedVal.Uint64(),
	}

	for i := 0; i < numShards; i++ {
		s.shards[i] = &shard{
			data: make(map[string]internalEntry),
		}
	}

	if err := s.replayLog(); err != nil {
		walFile.Close()
		return nil, err
	}

	if _, err := s.wal.Seek(0, io.SeekEnd); err != nil {
		walFile.Close()
		return nil, err
	}

	// --- Start background compaction loop ---
	if compactionInterval > 0 {
		s.wg.Add(1)
		go s.compactionLoop(compactionInterval)
	}

	return s, nil
}

// replayLog reads the entire WAL, populating the in-memory state.
// It now correctly handles delete tombstones.
func (s *Store) replayLog() error {
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return err
	}

	dec := gob.NewDecoder(s.wal)
	for {
		var entry logEntry
		err := dec.Decode(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return errCorruptLog
		}

		// Verify Checksum
		if s.checksum(entry.Value) != entry.Checksum {
			return errCorruptLog
		}

		shard := s.getShard(entry.Key)

		if entry.Deleted {
			// If it's a delete, remove it from memory.
			// We only care if the delete version is newer.
			if current, ok := shard.data[entry.Key]; ok && entry.Version > current.Version {
				delete(shard.data, entry.Key)
			}
		} else {
			// It's a Put. Only update if newer.
			if current, ok := shard.data[entry.Key]; !ok || entry.Version > current.Version {
				shard.data[entry.Key] = internalEntry{
					Version: entry.Version,
					Value:   entry.Value,
				}
			}
		}
	}
	return nil
}

// ## 4. Public API: Get, Put, Delete, Close

// Get retrieves a value by key.
func (s *Store) Get(key string, target interface{}) (version int64, err error) {
	shard := s.getShard(key)

	// --- Critical section: Read lock ---
	shard.mux.RLock()
	entry, ok := shard.data[key]
	if !ok {
		shard.mux.RUnlock() // Don't forget to unlock on failure!
		return 0, ErrNotFound
	}

	// Copy the value and version, then release the lock
	valueCopy := make([]byte, len(entry.Value))
	copy(valueCopy, entry.Value)
	version = entry.Version
	shard.mux.RUnlock()
	// --- End critical section ---

	// Decode outside the lock (to minimize lock hold time)
	buf := bytes.NewBuffer(valueCopy)
	if err := gob.NewDecoder(buf).Decode(target); err != nil {
		return 0, err
	}
	return version, nil
}

// Put adds or updates a key with new data using optimistic locking.
func (s *Store) Put(key string, data interface{}, expectedVersion int64) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(data); err != nil {
		return err
	}
	valueBytes := buf.Bytes()

	shard := s.getShard(key)
	shard.mux.Lock()
	defer shard.mux.Unlock()

	// --- Start of Atomic Transaction ---
	currentVersion := int64(0)
	if entry, ok := shard.data[key]; ok {
		currentVersion = entry.Version
	}

	if currentVersion != expectedVersion {
		return ErrVersionMismatch
	}

	newVersion := currentVersion + 1
	log := logEntry{
		Key:      key,
		Value:    valueBytes,
		Version:  newVersion,
		Checksum: s.checksum(valueBytes),
		Deleted:  false, // MODIFIED: Explicitly set Deleted to false
	}

	// Write to WAL (Durability & Atomicity)
	s.walMux.Lock()
	err := s.walEnc.Encode(log)
	if err != nil {
		s.walMux.Unlock()
		return err
	}
	if err := s.wal.Sync(); err != nil {
		s.walMux.Unlock()
		return err
	}
	s.walMux.Unlock()

	// Update in-memory map
	shard.data[key] = internalEntry{
		Version: newVersion,
		Value:   valueBytes,
	}

	return nil
}

// Delete removes a key from the store using optimistic locking.
// It requires the 'expectedVersion' to match the current version.
// It writes a "tombstone" record to the WAL for durability.
func (s *Store) Delete(key string, expectedVersion int64) error {
	shard := s.getShard(key)
	shard.mux.Lock()
	defer shard.mux.Unlock()

	// --- Start of Atomic Transaction ---

	// 1. Check version
	currentVersion := int64(0)
	if entry, ok := shard.data[key]; ok {
		currentVersion = entry.Version
	} else {
		// Key doesn't exist, so we can't delete it.
		return ErrNotFound
	}

	if currentVersion != expectedVersion {
		return ErrVersionMismatch
	}

	// 2. Prepare tombstone log entry
	newVersion := currentVersion + 1
	log := logEntry{
		Key:      key,
		Value:    nil, // No value for a delete
		Version:  newVersion,
		Checksum: s.checksum(nil), // Checksum of nil
		Deleted:  true,
	}

	// 3. Write to WAL (Durability & Atomicity)
	s.walMux.Lock()
	err := s.walEnc.Encode(log)
	if err != nil {
		s.walMux.Unlock()
		return err
	}
	if err := s.wal.Sync(); err != nil {
		s.walMux.Unlock()
		return err
	}
	s.walMux.Unlock()

	// 4. Update in-memory map
	delete(shard.data, key)

	return nil
}

// Close gracefully shuts down the database.
// It now signals the compaction loop to stop and waits for it.
func (s *Store) Close() error {
	// --- MODIFIED: Signal background goroutines to stop ---
	if s.quit != nil {
		close(s.quit) // Signal compaction to stop
		s.wg.Wait()   // Wait for it to finish
	}

	s.walMux.Lock()
	defer s.walMux.Unlock()
	return s.wal.Close()
}

// ## 5. Compaction

// compactionLoop runs in a background goroutine.
func (s *Store) compactionLoop(interval time.Duration) {
	defer s.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.Compact(); err != nil {
				// In a real-world scenario, log this error
				fmt.Fprintf(os.Stderr, "kvstore: compaction failed: %v\n", err)
			}
		case <-s.quit:
			return // Exit loop on Close()
		}
	}
}

// unlockAllShards is a helper to unlock all shards during compaction.
func (s *Store) unlockAllShards() {
	for i := len(s.shards) - 1; i >= 0; i-- {
		s.shards[i].mux.Unlock()
	}
}

// Compact performs a "Stop-the-World" compaction.
// It locks the entire database, writes a new WAL with only
// live data, and atomically swaps it with the old one.
func (s *Store) Compact() error {
	// 1. Open a new temporary WAL file
	compactPath := s.walPath + ".compact"
	compactFile, err := os.OpenFile(compactPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create compact file: %w", err)
	}
	compactEnc := gob.NewEncoder(compactFile)

	// --- 2. Stop The World ---
	// Acquire locks in a consistent order to prevent deadlock.
	for _, shard := range s.shards {
		shard.mux.Lock()
	}
	s.walMux.Lock()

	// 3. Write snapshot of live data to new WAL
	for _, shard := range s.shards {
		for key, entry := range shard.data {
			// We only write live (non-deleted) entries
			log := logEntry{
				Key:      key,
				Value:    entry.Value,
				Version:  entry.Version,
				Checksum: s.checksum(entry.Value),
				Deleted:  false,
			}
			if err := compactEnc.Encode(log); err != nil {
				// On error, abort compaction and unlock
				compactFile.Close()
				os.Remove(compactPath)
				s.unlockAllShards()
				s.walMux.Unlock()
				return fmt.Errorf("failed to write to compact file: %w", err)
			}
		}
	}

	// 4. Force new WAL to disk
	if err := compactFile.Sync(); err != nil {
		compactFile.Close()
		os.Remove(compactPath)
		s.unlockAllShards()
		s.walMux.Unlock()
		return fmt.Errorf("failed to sync compact file: %w", err)
	}
	compactFile.Close() // Close file handle before renaming

	// 5. Atomically swap WAL files
	oldWalPath := s.walPath + ".old"
	if err := s.wal.Close(); err != nil {
		// This is tricky. We've written the new log but can't close the old.
		// We'll proceed, but log the error.
		fmt.Fprintf(os.Stderr, "kvstore: failed to close old WAL: %v\n", err)
	}

	// Rename old log
	if err := os.Rename(s.walPath, oldWalPath); err != nil {
		// This is bad. We can't swap. Try to reopen old wal.
		s.wal, _ = os.OpenFile(s.walPath, os.O_RDWR|os.O_APPEND, 0644)
		s.walEnc = gob.NewEncoder(s.wal)
		s.unlockAllShards()
		s.walMux.Unlock()
		return fmt.Errorf("failed to rename old WAL: %w", err)
	}

	// Rename new log to be the active log
	if err := os.Rename(compactPath, s.walPath); err != nil {
		// This is catastrophic. Try to rename old log back.
		os.Rename(oldWalPath, s.walPath)
		s.wal, _ = os.OpenFile(s.walPath, os.O_RDWR|os.O_APPEND, 0644)
		s.walEnc = gob.NewEncoder(s.wal)
		s.unlockAllShards()
		s.walMux.Unlock()
		return fmt.Errorf("failed to rename new WAL: %w", err)
	}

	// 6. Open the new WAL file for appending
	newWalFile, err := os.OpenFile(s.walPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		// This is also catastrophic.
		s.unlockAllShards()
		s.walMux.Unlock()
		return fmt.Errorf("failed to reopen new WAL: %w", err)
	}

	s.wal = newWalFile
	s.walEnc = gob.NewEncoder(s.wal)

	// --- 7. Restart The World ---
	s.walMux.Unlock()
	s.unlockAllShards()

	// 8. Cleanup old file
	os.Remove(oldWalPath)

	return nil
}

// ## 6. Internal Helpers

// checksum is a helper to compute the CRC32 checksum.
func (s *Store) checksum(data []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(data) // Write(nil) is valid
	return h.Sum32()
}

// getShard maps a key to its designated shard.
func (s *Store) getShard(key string) *shard {
	hash := fnv1a(key, s.hashSeed)
	index := hash % uint64(numShards)
	return s.shards[index]
}

// simple, non-cryptographic hash function.
func fnv1a(s string, seed uint64) uint64 {
	const prime = 1099511628211

	hash := seed // Start with the random seed to prevent HashDoS attacks

	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime
	}
	return hash
}
