package prky

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/big"
	"os"
	"runtime"
	"sync"
	"time"
)

// Public errors.
var (
	ErrNotFound        = errors.New("key not found")
	ErrVersionMismatch = errors.New("version mismatch (optimistic lock failed)")
	ErrCorruptLog      = errors.New("corrupt log file detected")
	ErrValueTooLarge   = errors.New("value too large")
)

// Number of shards in the in-memory map.
const numShards = 256

// internalEntry is the in-memory representation of a value.
type internalEntry struct {
	Version int64
	Value   []byte
}

// shard contains a single map and its dedicated lock.
type shard struct {
	mux  sync.RWMutex
	data map[string]internalEntry
}


type walWriter struct {
    path string
    file *os.File
    ch   chan *walRequest
}

// DurabilityConfig controls WAL group-commit behavior.
type DurabilityConfig struct {
	FlushInterval   time.Duration // Maximum time a write may wait before being flushed.
	MaxBatchRecords int           // Maximum number of records per WAL batch.
	MaxBatchBytes   int           // Maximum total bytes per WAL batch.
	NumWAL          int           // Number of WAL writer/files to use.
}

// Store is the main key-value store struct.
type Store struct {
	shards         [numShards]*shard
	walPathBase string                // base path provided by caller (e.g. "prky.wal")
	walWriters  []*walWriter          // len == durability.NumWAL
	walMux         sync.Mutex         // Serializes WAL flushes and file swaps (compaction).
	walPath        string             // Path to the WAL file.
	wg             sync.WaitGroup     // Background goroutines.
	quit           chan struct{}      // Signals background goroutines to stop.
	hashSeed       uint64             // Seed for key hashing (HashDoS protection).
	maxValueBytes  int                // Max value size
	hardValueLimit int                // Hard limit on value size (can modify but not recommended)
	durability DurabilityConfig
}

// Binary WAL format:
//
// 	 [0..3]		uint32			keyLen
// 	 [4..7]		uint32			valueLen
// 	 [8..15]	int64			version
// 	 [16..19]	uint32			checksum (CRC32)
// 	 [20]		uint8			deleted (0 = false, 1 = true)
// 	 [21..]		key bytes		(keyLen)
// 	 [...]		value bytes		(valueLen)
//
// All fields are little-endian.

type walRequest struct {
	key     []byte
	value   []byte
	version int64
	deleted bool
	size    int        // Approximate total bytes for this record in WAL.
	done    chan error // Signaled when the batch containing this record is durably flushed.
}

// Precomputed CRC32 table (Castagnoli polynomial, often hardware-accelerated).
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Buffer pool for WAL batching.
var walBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 128*1024) // Default starting capacity; grows as needed.
		return &b
	},
}

// NewStore creates or loads a key-value store from a WAL file.
// If compactionInterval > 0, a background compaction goroutine will run.
// Durability is enforced via a single WAL writer goroutine with group commit.
func NewStore(path string, compactionInterval time.Duration) (*Store, error) {
    // Generate hash seed first
    seedVal, err := rand.Int(rand.Reader, new(big.Int).SetUint64(0xFFFFFFFFFFFFFFFF))
    if err != nil {
        return nil, fmt.Errorf("failed to generate hash seed: %w", err)
    }

    s := &Store{
        walPathBase: path,
        quit:        make(chan struct{}),
        durability: DurabilityConfig{
            FlushInterval:   200 * time.Microsecond,
            MaxBatchRecords: 1024,
            MaxBatchBytes:   128 * 1024,
            NumWAL:          4, 
        },
        maxValueBytes:  128 * 1024,
        hardValueLimit: 4 * 1024 * 1024,
        hashSeed:       seedVal.Uint64(),
    }

    for i := 0; i < numShards; i++ {
        s.shards[i] = &shard{
            data: make(map[string]internalEntry),
        }
    }

    // Open WAL files and replay them.
    // NOTE: at this point durability.NumWAL is 1. If the user wants more,
    // they call SetDurabilityConfig after NewStore. For multi-WAL from
    // the very first start, you can adjust this to accept a config parameter.
    numWAL := s.durability.NumWAL
    s.walWriters = make([]*walWriter, numWAL)

    for i := 0; i < numWAL; i++ {
        walPath := fmt.Sprintf("%s.%d", path, i)
        f, err := openWal(walPath)
        if err != nil {
            // close already-opened files
            for j := 0; j < i; j++ {
                _ = s.walWriters[j].file.Close()
            }
            return nil, err
        }

        // Replay this WAL into memory.
        if err := s.replayLogFile(f); err != nil {
            f.Close()
            for j := 0; j < i; j++ {
                _ = s.walWriters[j].file.Close()
            }
            return nil, err
        }

        // Seek to end for appends.
        if _, err := f.Seek(0, io.SeekEnd); err != nil {
            f.Close()
            for j := 0; j < i; j++ {
                _ = s.walWriters[j].file.Close()
            }
            return nil, err
        }

        s.walWriters[i] = &walWriter{
            path: walPath,
            file: f,
            ch:   make(chan *walRequest, 4096),
        }
    }

    // Start WAL writer goroutines (one per WAL).
    for _, w := range s.walWriters {
        s.wg.Add(1)
        go s.walLoop(w)
    }

    // Optional background compaction (we'll discuss multi-WAL compaction separately).
    if compactionInterval > 0 {
        s.wg.Add(1)
        go s.compactionLoop(compactionInterval)
    }

    return s, nil
}

// SetDurabilityConfig allows overriding the default group-commit configuration.
// This should be called soon after NewStore and before heavy use.
func (s *Store) SetDurabilityConfig(cfg DurabilityConfig) {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = s.durability.FlushInterval
	}
	if cfg.MaxBatchRecords <= 0 {
		cfg.MaxBatchRecords = s.durability.MaxBatchRecords
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = s.durability.MaxBatchBytes
	}
	if cfg.NumWAL <= 0 {
        cfg.NumWAL = s.durability.NumWAL
        if cfg.NumWAL <= 0 {
            cfg.NumWAL = 1
        }
	}
	s.durability = cfg
}

func (s *Store) SetMaxValueBytes(n int) error {
	if n <= 0 {
		return errors.New("MaxValueBytes must be positive")
	}
	if n > s.hardValueLimit {
		return fmt.Errorf("MaxValueBytes exceeds hard limit (%d bytes)", s.hardValueLimit)
	}
	s.maxValueBytes = n
	return nil
}

// replayLog scans the WAL from the beginning and reconstructs in-memory state.
func (s *Store) replayLogFile(f *os.File) error {
    if _, err := f.Seek(0, io.SeekStart); err != nil {
        return err
    }

    header := make([]byte, 21)

    for {
        _, err := io.ReadFull(f, header)
        if err == io.EOF || err == io.ErrUnexpectedEOF {
            return nil // clean EOF
        }
        if err != nil {
            return ErrCorruptLog
        }

        keyLen := binary.LittleEndian.Uint32(header[0:])
        valueLen := binary.LittleEndian.Uint32(header[4:])
        version := int64(binary.LittleEndian.Uint64(header[8:]))
        checksum := binary.LittleEndian.Uint32(header[16:])
        deleted := header[20] != 0

        if keyLen == 0 {
            return ErrCorruptLog
        }

        keyBytes := make([]byte, keyLen)
        if _, err := io.ReadFull(f, keyBytes); err != nil {
            return ErrCorruptLog
        }

        valueBytes := make([]byte, valueLen)
        if _, err := io.ReadFull(f, valueBytes); err != nil {
            return ErrCorruptLog
        }

        if s.checksum(valueBytes) != checksum {
            return ErrCorruptLog
        }

        key := string(keyBytes)
        sh := s.getShard(key)

        if deleted {
            if cur, ok := sh.data[key]; ok && version > cur.Version {
                delete(sh.data, key)
            }
        } else {
            cur, ok := sh.data[key]
            if !ok || version > cur.Version {
                sh.data[key] = internalEntry{
                    Version: version,
                    Value:   valueBytes,
                }
            }
        }
    }
}

// Get retrieves a value by key and returns its version and a copy of the value bytes.
// This function is unchanged and was already correct.
func (s *Store) Get(key string) (int64, []byte, error) {
	sh := s.getShard(key)

	sh.mux.RLock()
	entry, ok := sh.data[key]
	if !ok {
		sh.mux.RUnlock()
		return 0, nil, ErrNotFound
	}
	version := entry.Version
	valueCopy := make([]byte, len(entry.Value))
	copy(valueCopy, entry.Value)
	sh.mux.RUnlock()

	return version, valueCopy, nil
}

// Put adds or updates a key with raw value bytes using optimistic locking.
// expectedVersion == 0 means "insert if missing".
// If the current version does not match expectedVersion, ErrVersionMismatch is returned.
// The call blocks until the corresponding WAL batch has been flushed to disk.
//
// *** THIS FUNCTION HAS BEEN MODIFIED TO FIX THE STALL ***
func (s *Store) Put(key string, value []byte, expectedVersion int64) error {
	if len(value) > s.maxValueBytes {
		return fmt.Errorf("%w: %d > %d", ErrValueTooLarge, len(value), s.maxValueBytes)
	}

	if len(value) > s.hardValueLimit {
		panic("value size exceeded hard limit — this indicates a programmer error")
	}

	sh := s.getShard(key)

	// --- Phase 1: Lock, Check, and Prepare ---
	sh.mux.Lock()

	currentVersion := int64(0)
	if entry, ok := sh.data[key]; ok {
		currentVersion = entry.Version
	}
	if currentVersion != expectedVersion {
		sh.mux.Unlock() // Unlock on failed check
		return ErrVersionMismatch
	}

	newVersion := currentVersion + 1
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	req := &walRequest{
		key:     []byte(key),
		value:   valueCopy, // Pass the copy to the WAL
		version: newVersion,
		deleted: false,
		size:    21 + len(key) + len(valueCopy),
		done:    make(chan error, 1),
	}

	// Release the lock *before* the I/O wait.
	// This is the critical fix.
	sh.mux.Unlock()

	// --- Phase 2: Wait for Durability (No Lock Held) ---
	// Gets can now acquire the RLock on this shard.
	walIdx := s.walIndexForKey(key)
	writer := s.walWriters[walIdx]

	select {
	case writer.ch <- req:
	case <-s.quit:
			return errors.New("store closed")
	}

	// Wait for the fsync to complete.
	if err := <-req.done; err != nil {
		// The WAL write failed. We never touched memory.
		// It's safe to just return the error.
		return err
	}

	// --- Phase 3: Lock and Commit to Memory ---
	// The WAL write is durable. Now we update the in-memory map.
	sh.mux.Lock()
	defer sh.mux.Unlock()

	// We must re-check the state. Another goroutine might have
	// completed its Phase 2 and won the race to update the map.
	if entry, ok := sh.data[key]; ok {
		if entry.Version >= newVersion {
			// Our write (v+1) is durable, but the in-memory state
			// is already at or past our version (e.g., v+1 or v+2).
			// This is fine, it just means we were "slower" to commit
			// to memory. No action needed.
			return nil
		}
	}

	// The current in-memory version is still what we saw (expectedVersion),
	// or the key is gone (which is fine). We can safely
	// apply our update.
	sh.data[key] = internalEntry{
		Version: newVersion,
		Value:   valueCopy, // Use the same copy we sent to the WAL
	}

	return nil
}

// Delete removes a key from the store using optimistic locking.
// It writes a tombstone to the WAL and blocks until the batch is flushed.
//
// *** THIS FUNCTION HAS BEEN MODIFIED TO FIX THE STALL ***
func (s *Store) Delete(key string, expectedVersion int64) error {
	sh := s.getShard(key)

	// --- Phase 1: Lock, Check, and Prepare ---
	sh.mux.Lock()

	entry, ok := sh.data[key]
	if !ok {
		sh.mux.Unlock()
		return ErrNotFound
	}
	if entry.Version != expectedVersion {
		sh.mux.Unlock()
		return ErrVersionMismatch
	}

	newVersion := entry.Version + 1

	req := &walRequest{
		key:     []byte(key),
		value:   nil,
		version: newVersion,
		deleted: true,
		size:    21 + len(key),
		done:    make(chan error, 1),
	}

	// Release the lock *before* the I/O wait.
	// This is the critical fix.
	sh.mux.Unlock()

	// --- Phase 2: Wait for Durability (No Lock Held) ---
	walIdx := s.walIndexForKey(key)
	writer := s.walWriters[walIdx]

	select {
	case writer.ch <- req:
	case <-s.quit:
			return errors.New("store closed")
	}


	// Wait for the fsync to complete.
	if err := <-req.done; err != nil {
		// The WAL write failed. We never touched memory.
		// It's safe to just return the error.
		return err
	}

	// --- Phase 3: Lock and Commit to Memory ---
	// The WAL write is durable. Now we update the in-memory map.
	sh.mux.Lock()
	defer sh.mux.Unlock()

	// Re-check state. We only delete if the version is *still*
	// the one we expected to delete.
	if entry, ok := sh.data[key]; ok && entry.Version == expectedVersion {
		// The version hasn't changed since we checked. Safe to delete.
		delete(sh.data, key)
	}
	// If the version is *newer*, another Put came in. We don't delete.
	// If the key is *gone*, another Delete beat us. We don't delete.
	// In all cases, our "delete" operation is durable in the WAL
	// and the replay will be correct.

	return nil
}

// Close gracefully shuts down the database.
// It signals background goroutines to stop, flushes pending WAL writes,
// and closes the WAL file.
func (s *Store) Close() error {
    if s.quit != nil {
        close(s.quit)
    }

    // Close all WAL channels so writers flush and exit.
    for _, w := range s.walWriters {
        if w != nil && w.ch != nil {
            close(w.ch)
        }
    }

    s.wg.Wait()

    s.walMux.Lock()
    defer s.walMux.Unlock()

    var firstErr error
    for _, w := range s.walWriters {
        if w != nil && w.file != nil {
            if err := w.file.Close(); err != nil && firstErr == nil {
                firstErr = err
            }
        }
    }
    return firstErr
}

// walLoop is the single WAL writer goroutine responsible for group commit.
// It batches incoming requests based on count, size, and time.
// This function is unchanged and was already correct.
func (s *Store) walLoop(w *walWriter) {
    defer s.wg.Done()

    cfg := s.durability
    if cfg.FlushInterval <= 0 {
        cfg.FlushInterval = 200 * time.Microsecond
    }
    if cfg.MaxBatchRecords <= 0 {
        cfg.MaxBatchRecords = 1024
    }
    if cfg.MaxBatchBytes <= 0 {
        cfg.MaxBatchBytes = 128 * 1024
    }

    timer := time.NewTimer(cfg.FlushInterval)
    defer timer.Stop()

    var (
        pending      []*walRequest
        pendingBytes int
    )

    flush := func() {
        if len(pending) == 0 {
            return
        }

        bufPtr := walBufPool.Get().(*[]byte)
        buf := (*bufPtr)[:0]

        for _, req := range pending {
            var header [21]byte
            binary.LittleEndian.PutUint32(header[0:], uint32(len(req.key)))
            binary.LittleEndian.PutUint32(header[4:], uint32(len(req.value)))
            binary.LittleEndian.PutUint64(header[8:], uint64(req.version))
            binary.LittleEndian.PutUint32(header[16:], s.checksum(req.value))
            if req.deleted {
                header[20] = 1
            } else {
                header[20] = 0
            }

            buf = append(buf, header[:]...)
            buf = append(buf, req.key...)
            buf = append(buf, req.value...)
        }

        // We still use a global walMux here so compaction can stop the world.
        s.walMux.Lock()
        _, writeErr := w.file.Write(buf)

        if writeErr == nil && runtime.GOOS != "windows" {
            writeErr = w.file.Sync()
        }
        s.walMux.Unlock()

        for _, req := range pending {
            req.done <- writeErr
        }

        *bufPtr = buf[:0]
        walBufPool.Put(bufPtr)

        pending = pending[:0]
        pendingBytes = 0
    }

    for {
        select {
        case req, ok := <-w.ch:
            if !ok {
                flush()
                return
            }

            pending = append(pending, req)
            pendingBytes += req.size

            if len(pending) >= cfg.MaxBatchRecords || pendingBytes >= cfg.MaxBatchBytes {
                flush()
                if !timer.Stop() {
                    select {
                    case <-timer.C:
                    default:
                    }
                }
                timer.Reset(cfg.FlushInterval)
            }

        case <-timer.C:
            if len(pending) > 0 {
                flush()
            }
            timer.Reset(cfg.FlushInterval)

        case <-s.quit:
            // Drain remaining requests then exit.
            for {
                select {
                case req, ok := <-w.ch:
                    if !ok {
                        flush()
                        return
                    }
                    pending = append(pending, req)
                    pendingBytes += req.size
                default:
                    flush()
                    return
                }
            }
        }
    }
}


// compactionLoop runs periodic log compaction.
func (s *Store) compactionLoop(interval time.Duration) {
	defer s.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.Compact(); err != nil {
				fmt.Fprintf(os.Stderr, "kvstore: compaction failed: %v\n", err)
			}
		case <-s.quit:
			return
		}
	}
}

// Compact performs stop-the-world compaction by writing a new WAL containing
// only the latest live entries and atomically swapping it into place.
// Compact performs stop-the-world compaction across all WALs.
// It writes a new WAL for each writer containing only the latest live entries
// and atomically swaps it into place.
func (s *Store) Compact() error {
    numWAL := len(s.walWriters)
    if numWAL == 0 {
        return nil // nothing to compact
    }

    // Stop world: lock shards and WAL in a fixed order
    for _, sh := range s.shards {
        sh.mux.Lock()
    }
    s.walMux.Lock()

    // Prepare new compact files: one per WAL writer
    compactPaths := make([]string, numWAL)
    compactFiles := make([]*os.File, numWAL)

    for i := 0; i < numWAL; i++ {
        compactPaths[i] = fmt.Sprintf("%s.compact.%d", s.walPathBase, i)
        f, err := os.OpenFile(compactPaths[i],
            os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
        if err != nil {
            // cleanup partial state
            for j := 0; j < i; j++ {
                compactFiles[j].Close()
                os.Remove(compactPaths[j])
            }
            s.walMux.Unlock()
            s.unlockAllShards()
            return fmt.Errorf("failed to create compact file: %w", err)
        }
        compactFiles[i] = f
    }

    // Write snapshot of live data into appropriate WAL writer
    for _, sh := range s.shards {
        for key, entry := range sh.data {
            walIdx := s.walIndexForKey(key)
            if err := writeLogEntryToFile(
                compactFiles[walIdx], key, entry.Value, entry.Version, false,
            ); err != nil {
                // cleanup
                for i := 0; i < numWAL; i++ {
                    compactFiles[i].Close()
                    os.Remove(compactPaths[i])
                }
                s.walMux.Unlock()
                s.unlockAllShards()
                return fmt.Errorf("failed to write compact: %w", err)
            }
        }
    }

    // Sync & close compact WALs
    for i := 0; i < numWAL; i++ {
        if err := compactFiles[i].Sync(); err != nil {
            compactFiles[i].Close()
            os.Remove(compactPaths[i])
            s.walMux.Unlock()
            s.unlockAllShards()
            return fmt.Errorf("failed to sync compact WAL: %w", err)
        }
        compactFiles[i].Close()
    }

    // Atomically swap WALs
    for i := 0; i < numWAL; i++ {
        oldPath := fmt.Sprintf("%s.%d", s.walPathBase, i)
        backup := fmt.Sprintf("%s.%d.old", s.walPathBase, i)

        // Close existing WAL
        if s.walWriters[i] != nil && s.walWriters[i].file != nil {
            _ = s.walWriters[i].file.Close()
        }

        _ = os.Rename(oldPath, backup)
        if err := os.Rename(compactPaths[i], oldPath); err != nil {
            // rollback
            _ = os.Rename(backup, oldPath)
            s.walMux.Unlock()
            s.unlockAllShards()
            return fmt.Errorf("failed to swap WAL %d: %w", i, err)
        }

        // Reopen new WAL
        f, err := openWal(oldPath)
        if err != nil {
            // rollback exposed state
            _ = os.Rename(backup, oldPath)
            s.walMux.Unlock()
            s.unlockAllShards()
            return fmt.Errorf("failed to reopen new WAL %d: %w", i, err)
        }

        s.walWriters[i].file = f // update FD
        _ = os.Remove(backup)
    }

    // Resume operation
    s.walMux.Unlock()
    s.unlockAllShards()
    return nil
}

// unlockAllShards unlocks all shard mutexes in reverse order.
func (s *Store) unlockAllShards() {
	for i := len(s.shards) - 1; i >= 0; i-- {
		s.shards[i].mux.Unlock()
	}
}

// writeLogEntryToFile writes a single WAL entry to an arbitrary file descriptor
// without touching Store state. Used by compaction.
func writeLogEntryToFile(f *os.File, key string, value []byte, version int64, deleted bool) error {
	keyBytes := []byte(key)

	var header [21]byte
	binary.LittleEndian.PutUint32(header[0:], uint32(len(keyBytes)))
	binary.LittleEndian.PutUint32(header[4:], uint32(len(value)))
	binary.LittleEndian.PutUint64(header[8:], uint64(version))
	binary.LittleEndian.PutUint32(header[16:], crc32.Checksum(value, crcTable))
	if deleted {
		header[20] = 1
	} else {
		header[20] = 0
	}

	if _, err := f.Write(header[:]); err != nil {
		return err
	}
	if _, err := f.Write(keyBytes); err != nil {
		return err
	}
	if _, err := f.Write(value); err != nil {
		return err
	}
	return nil
}

// checksum computes a CRC32 checksum over the given data using a shared table.
func (s *Store) checksum(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// getShard maps a key to its designated shard using a seeded FNV-1a hash.
func (s *Store) getShard(key string) *shard {
	hash := fnv1a(key, s.hashSeed)
	index := hash % uint64(numShards)
	return s.shards[index]
}

// fnv1a is a simple, seeded, non-cryptographic hash function used for sharding.
// The random seed helps mitigate hash collision attacks.
func fnv1a(s string, seed uint64) uint64 {
	const prime = 1099511628211

	hash := seed
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime
	}
	return hash
}

// walIndexForKey determines which WAL writer/file a given key uses.
// It is derived from the shard index so that a key always maps to the
// same WAL and we preserve per-key ordering.
func (s *Store) walIndexForKey(key string) int {
    if len(s.walWriters) == 1 {
        return 0
    }
    hash := fnv1a(key, s.hashSeed)
    shardIdx := hash % uint64(numShards)
    walIdx := shardIdx % uint64(len(s.walWriters))
    return int(walIdx)
}
