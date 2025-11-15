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
	"sync"
	"time"
)

// Public errors.
var (
	ErrNotFound        = errors.New("key not found")
	ErrVersionMismatch = errors.New("version mismatch (optimistic lock failed)")
	errCorruptLog      = errors.New("corrupt log file detected")
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

// DurabilityConfig controls WAL group-commit behavior.
type DurabilityConfig struct {
	FlushInterval   time.Duration // Maximum time a write may wait before being flushed.
	MaxBatchRecords int           // Maximum number of records per WAL batch.
	MaxBatchBytes   int           // Maximum total bytes per WAL batch.
}

// Store is the main key-value store struct.
type Store struct {
	shards   [numShards]*shard
	wal      *os.File       // Write-ahead log file.
	walMux   sync.Mutex     // Serializes WAL flushes and file swaps (compaction).
	walPath  string         // Path to the WAL file.
	wg       sync.WaitGroup // Background goroutines.
	quit     chan struct{}  // Signals background goroutines to stop.
	hashSeed uint64         // Seed for key hashing (HashDoS protection).

	// WAL writer (group commit) infrastructure.
	walCh      chan *walRequest
	durability DurabilityConfig
}

// Binary WAL format:
//
//   [0..3]   uint32  keyLen
//   [4..7]   uint32  valueLen
//   [8..15]  int64   version
//   [16..19] uint32  checksum (CRC32)
//   [20]     uint8   deleted (0 = false, 1 = true)
//   [21..]   key bytes (keyLen)
//   [...]    value bytes (valueLen)
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
	walFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	seedVal, err := rand.Int(rand.Reader, new(big.Int).SetUint64(0xFFFFFFFFFFFFFFFF))
	if err != nil {
		walFile.Close()
		return nil, fmt.Errorf("failed to generate hash seed: %w", err)
	}

	s := &Store{
		wal:     walFile,
		walPath: path,
		quit:    make(chan struct{}),
		durability: DurabilityConfig{
			FlushInterval:   200 * time.Microsecond,
			MaxBatchRecords: 1024,
			MaxBatchBytes:   128 * 1024,
		},
		walCh: make(chan *walRequest, 4096),
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

	// WAL writer goroutine (group commit).
	s.wg.Add(1)
	go s.walLoop()

	// Optional background compaction.
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
	s.durability = cfg
}

// replayLog scans the WAL from the beginning and reconstructs in-memory state.
func (s *Store) replayLog() error {
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return err
	}

	header := make([]byte, 21)

	for {
		_, err := io.ReadFull(s.wal, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Clean EOF: no more complete records.
			return nil
		}
		if err != nil {
			return errCorruptLog
		}

		keyLen := binary.LittleEndian.Uint32(header[0:])
		valueLen := binary.LittleEndian.Uint32(header[4:])
		version := int64(binary.LittleEndian.Uint64(header[8:]))
		checksum := binary.LittleEndian.Uint32(header[16:])
		deleted := header[20] != 0

		if keyLen == 0 {
			return errCorruptLog
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(s.wal, keyBytes); err != nil {
			return errCorruptLog
		}

		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(s.wal, valueBytes); err != nil {
			return errCorruptLog
		}

		if s.checksum(valueBytes) != checksum {
			return errCorruptLog
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
func (s *Store) Put(key string, value []byte, expectedVersion int64) error {
	sh := s.getShard(key)
	sh.mux.Lock()
	defer sh.mux.Unlock()

	currentVersion := int64(0)
	if entry, ok := sh.data[key]; ok {
		currentVersion = entry.Version
	}
	if currentVersion != expectedVersion {
		return ErrVersionMismatch
	}

	newVersion := currentVersion + 1
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	req := &walRequest{
		key:     []byte(key),
		value:   valueCopy,
		version: newVersion,
		deleted: false,
		size:    21 + len(key) + len(valueCopy),
		done:    make(chan error, 1),
	}

	// Enqueue WAL write and wait for durability.
	select {
	case s.walCh <- req:
	case <-s.quit:
		return errors.New("store closed")
	}

	if err := <-req.done; err != nil {
		return err
	}

	// Only update in-memory state after the WAL is durable.
	sh.data[key] = internalEntry{
		Version: newVersion,
		Value:   valueCopy,
	}

	return nil
}

// Delete removes a key from the store using optimistic locking.
// It writes a tombstone to the WAL and blocks until the batch is flushed.
func (s *Store) Delete(key string, expectedVersion int64) error {
	sh := s.getShard(key)
	sh.mux.Lock()
	defer sh.mux.Unlock()

	entry, ok := sh.data[key]
	if !ok {
		return ErrNotFound
	}
	if entry.Version != expectedVersion {
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

	// Enqueue WAL write and wait for durability.
	select {
	case s.walCh <- req:
	case <-s.quit:
		return errors.New("store closed")
	}

	if err := <-req.done; err != nil {
		return err
	}

	delete(sh.data, key)
	return nil
}

// Close gracefully shuts down the database.
// It signals background goroutines to stop, flushes pending WAL writes,
// and closes the WAL file.
func (s *Store) Close() error {
	if s.quit != nil {
		close(s.quit)
	}
	if s.walCh != nil {
		close(s.walCh)
	}

	s.wg.Wait()

	s.walMux.Lock()
	defer s.walMux.Unlock()
	if s.wal != nil {
		return s.wal.Close()
	}
	return nil
}

// walLoop is the single WAL writer goroutine responsible for group commit.
// It batches incoming requests based on count, size, and time.
func (s *Store) walLoop() {
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

		// Build a single buffer for the entire batch.
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

		s.walMux.Lock()
		_, writeErr := s.wal.Write(buf)
		if writeErr == nil {
			writeErr = s.wal.Sync()
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
		case req, ok := <-s.walCh:
			if !ok {
				// Flush any remaining entries before exit.
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
			// Drain any remaining requests, then flush, then exit.
			for {
				select {
				case req, ok := <-s.walCh:
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
func (s *Store) Compact() error {
	compactPath := s.walPath + ".compact"
	compactFile, err := os.OpenFile(compactPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create compact file: %w", err)
	}

	// Stop-the-world: lock shards and WAL in a fixed order.
	for _, sh := range s.shards {
		sh.mux.Lock()
	}
	s.walMux.Lock()

	// Write a snapshot of live data.
	for _, sh := range s.shards {
		for key, entry := range sh.data {
			if err := writeLogEntryToFile(compactFile, key, entry.Value, entry.Version, false); err != nil {
				compactFile.Close()
				os.Remove(compactPath)
				s.unlockAllShards()
				s.walMux.Unlock()
				return fmt.Errorf("failed to write to compact file: %w", err)
			}
		}
	}

	if err := compactFile.Sync(); err != nil {
		compactFile.Close()
		os.Remove(compactPath)
		s.unlockAllShards()
		s.walMux.Unlock()
		return fmt.Errorf("failed to sync compact file: %w", err)
	}
	compactFile.Close()

	oldWalPath := s.walPath + ".old"
	if err := s.wal.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "kvstore: failed to close old WAL: %v\n", err)
	}

	if err := os.Rename(s.walPath, oldWalPath); err != nil {
		s.wal, _ = os.OpenFile(s.walPath, os.O_RDWR|os.O_APPEND, 0644)
		s.unlockAllShards()
		s.walMux.Unlock()
		return fmt.Errorf("failed to rename old WAL: %w", err)
	}

	if err := os.Rename(compactPath, s.walPath); err != nil {
		_ = os.Rename(oldWalPath, s.walPath)
		s.wal, _ = os.OpenFile(s.walPath, os.O_RDWR|os.O_APPEND, 0644)
		s.unlockAllShards()
		s.walMux.Unlock()
		return fmt.Errorf("failed to rename new WAL: %w", err)
	}

	newWalFile, err := os.OpenFile(s.walPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		s.unlockAllShards()
		s.walMux.Unlock()
		return fmt.Errorf("failed to reopen new WAL: %w", err)
	}
	s.wal = newWalFile

	s.walMux.Unlock()
	s.unlockAllShards()

	_ = os.Remove(oldWalPath)
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