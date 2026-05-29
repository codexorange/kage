package storage

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// partitionDirRe matches directories created by BrokerStore: "<topic>-<partition>".
// The topic name may contain hyphens, so we anchor on a trailing "-<digits>" suffix.
var partitionDirRe = regexp.MustCompile(`^(.+)-(\d+)$`)

// TopicPartition identifies a single Kafka topic-partition.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// BrokerStore manages one *PartitionStore per topic-partition.
// Each partition's data lives in a subdirectory of rootDir named
// "<topic>-<partition>" (e.g. "sensor-data-0").
//
// BrokerStore is safe for concurrent use.
type BrokerStore struct {
	mu      sync.RWMutex
	rootDir string
	cfg     SegmentConfig
	ctx     context.Context
	logger  *slog.Logger
	stores  map[string]*PartitionStore // key: partitionKey(topic, partition)
}

// partitionKey returns the map key for a topic-partition pair.
func partitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

// partitionDir returns the filesystem path for a topic-partition store.
func (bs *BrokerStore) partitionDir(topic string, partition int32) string {
	return filepath.Join(bs.rootDir, partitionKey(topic, partition))
}

// OpenBrokerStore opens an existing or creates a new BrokerStore rooted at
// rootDir. It scans rootDir for subdirectories matching the "<topic>-<partition>"
// naming convention and loads each as a PartitionStore.
func OpenBrokerStore(ctx context.Context, rootDir string, cfg SegmentConfig, logger *slog.Logger) (*BrokerStore, error) {
	bs := &BrokerStore{
		rootDir: rootDir,
		cfg:     cfg,
		ctx:     ctx,
		logger:  logger,
		stores:  make(map[string]*PartitionStore),
	}

	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, fmt.Errorf("storage: broker store scan %q: %w", rootDir, err)
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		m := partitionDirRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		topic := m[1]
		partNum, err := strconv.ParseInt(m[2], 10, 32)
		if err != nil {
			continue
		}
		partition := int32(partNum)

		dir := filepath.Join(rootDir, e.Name())
		ps, err := OpenPartitionStore(ctx, dir, cfg, logger)
		if err != nil {
			return nil, fmt.Errorf("storage: broker store load %q: %w", dir, err)
		}
		bs.stores[partitionKey(topic, partition)] = ps
		logger.Info("broker store: loaded partition", "topic", topic, "partition", partition)
	}

	if cfg.Retention > 0 {
		go bs.runLogCleaner(ctx)
	}

	return bs, nil
}

// GetOrCreatePartition returns the PartitionStore for the given topic-partition,
// creating the subdirectory and a new store if it does not yet exist.
func (bs *BrokerStore) GetOrCreatePartition(topic string, partition int32) (*PartitionStore, error) {
	key := partitionKey(topic, partition)

	// Fast path: already exists.
	bs.mu.RLock()
	ps, ok := bs.stores[key]
	bs.mu.RUnlock()
	if ok {
		return ps, nil
	}

	// Slow path: create under write lock (double-checked).
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if ps, ok = bs.stores[key]; ok {
		return ps, nil
	}

	dir := bs.partitionDir(topic, partition)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: create partition dir %q: %w", dir, err)
	}

	ps, err := OpenPartitionStore(bs.ctx, dir, bs.cfg, bs.logger)
	if err != nil {
		return nil, fmt.Errorf("storage: open partition %q/%d: %w", topic, partition, err)
	}
	bs.stores[key] = ps
	bs.logger.Info("broker store: created partition", "topic", topic, "partition", partition)
	return ps, nil
}

// Topics returns a snapshot of all known topic-partitions.
func (bs *BrokerStore) Topics() []TopicPartition {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	out := make([]TopicPartition, 0, len(bs.stores))
	for _, ps := range bs.stores {
		// Reconstruct topic/partition from the store's directory name.
		dirName := filepath.Base(ps.dir)
		m := partitionDirRe.FindStringSubmatch(dirName)
		if m == nil {
			continue
		}
		partNum, err := strconv.ParseInt(m[2], 10, 32)
		if err != nil {
			continue
		}
		out = append(out, TopicPartition{Topic: m[1], Partition: int32(partNum)})
	}
	return out
}

// runLogCleaner runs a background goroutine that wakes every 5 minutes and
// calls CleanOldSegments on every known partition. It exits when ctx is done.
func (bs *BrokerStore) runLogCleaner(ctx context.Context) {
	const cleanerInterval = 5 * time.Minute
	ticker := time.NewTicker(cleanerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, tp := range bs.Topics() {
				bs.mu.RLock()
				ps, ok := bs.stores[partitionKey(tp.Topic, tp.Partition)]
				bs.mu.RUnlock()
				if !ok {
					continue
				}
				n, err := ps.CleanOldSegments(bs.cfg.Retention)
				if err != nil {
					bs.logger.Error("log cleaner: partition sweep failed",
						"topic", tp.Topic,
						"partition", tp.Partition,
						"error", err,
					)
					continue
				}
				if n > 0 {
					bs.logger.Info("log cleaner: deleted expired segments",
						"topic", tp.Topic,
						"partition", tp.Partition,
						"deleted", n,
					)
				}
			}
		}
	}
}

// Close flushes and closes all managed PartitionStores.
func (bs *BrokerStore) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	var firstErr error
	for key, ps := range bs.stores {
		if err := ps.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("storage: close partition %q: %w", key, err)
		}
	}
	return firstErr
}
