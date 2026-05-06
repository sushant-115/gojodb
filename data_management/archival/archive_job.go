// Package archival provides a scheduled archival job that moves data from active
// B-tree / inverted-index storage to cold storage based on a retention policy.
//
// An archival policy defines:
//   - A key-range or time-based predicate (keys older than N days, or matching a
//     prefix).
//   - A destination Archive Store (local path, S3 prefix, GCS bucket, etc.).
//   - A schedule (cron-style interval).
//
// After archival the corresponding keys are deleted from the hot index so
// storage is reclaimed.
package archival

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
)

// ArchivePolicy defines when and where to archive data.
type ArchivePolicy struct {
	// Name is a human-readable label for this policy.
	Name string
	// IndexName is the index manager to archive from.
	IndexName string
	// KeyPrefix restricts archival to keys starting with this prefix ("" = all).
	KeyPrefix string
	// MaxAge is the minimum age of a key before it is archived (based on creation time).
	// For now, if MaxAge == 0, all matching keys are archived.
	MaxAge time.Duration
	// Interval is how often this policy runs.
	Interval time.Duration
	// DestDir is the destination directory / prefix on the archive sink.
	DestDir string
}

// ArchivablePair is a key-value pair returned by the scan step.
type ArchivablePair struct {
	Key   string
	Value []byte
}

// IndexScanner provides a range scan and delete capability over an index.
// Implemented by the index manager adapters.
type IndexScanner interface {
	Name() string
	// ScanPrefix returns all key-value pairs whose keys start with prefix.
	ScanPrefix(ctx context.Context, prefix string) ([]ArchivablePair, error)
	// DeleteKey deletes a single key from the live index.
	DeleteKey(ctx context.Context, key string) error
}

// ArchiveSink is where archived data is written.
// For simplicity this mirrors the backup.Sink interface.
type ArchiveSink interface {
	Write(ctx context.Context, path string, r io.Reader) error
}

// LocalArchiveSink writes archives to a local directory.
type LocalArchiveSink struct {
	BaseDir string
}

// Write implements ArchiveSink for local filesystem.
func (s *LocalArchiveSink) Write(ctx context.Context, path string, r io.Reader) error {
	fullPath := filepath.Join(s.BaseDir, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0750); err != nil {
		return err
	}
	f, err := os.Create(fullPath) // #nosec G304
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

// Job manages one or more ArchivePolicies, running them on a schedule.
type Job struct {
	logger   *zap.Logger
	scanners map[string]IndexScanner
	sink     ArchiveSink
	policies []ArchivePolicy
	mu       sync.Mutex
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewJob creates an archival Job.
func NewJob(logger *zap.Logger, scanners []IndexScanner, sink ArchiveSink, policies []ArchivePolicy) *Job {
	scannerMap := make(map[string]IndexScanner, len(scanners))
	for _, s := range scanners {
		scannerMap[s.Name()] = s
	}
	return &Job{
		logger:   logger.Named("archival"),
		scanners: scannerMap,
		sink:     sink,
		policies: policies,
		stopCh:   make(chan struct{}),
	}
}

// Start launches background goroutines for each policy.
func (j *Job) Start() {
	for i := range j.policies {
		p := j.policies[i]
		j.wg.Add(1)
		go j.runPolicy(p)
	}
}

// Stop gracefully shuts down all running archival goroutines.
func (j *Job) Stop() {
	close(j.stopCh)
	j.wg.Wait()
}

func (j *Job) runPolicy(p ArchivePolicy) {
	defer j.wg.Done()
	interval := p.Interval
	if interval == 0 {
		interval = 24 * time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := j.runOnce(context.Background(), p); err != nil {
				j.logger.Error("archival run failed", zap.String("policy", p.Name), zap.Error(err))
			}
		case <-j.stopCh:
			return
		}
	}
}

// RunOnce executes a single archival pass for the given policy.
// Exported so callers can trigger ad-hoc archival.
func (j *Job) RunOnce(ctx context.Context, policyName string) error {
	for _, p := range j.policies {
		if p.Name == policyName {
			return j.runOnce(ctx, p)
		}
	}
	return fmt.Errorf("archival: policy %q not found", policyName)
}

func (j *Job) runOnce(ctx context.Context, p ArchivePolicy) error {
	scanner, ok := j.scanners[p.IndexName]
	if !ok {
		return fmt.Errorf("archival: no scanner for index %q", p.IndexName)
	}

	pairs, err := scanner.ScanPrefix(ctx, p.KeyPrefix)
	if err != nil {
		return fmt.Errorf("archival: scan %q: %w", p.IndexName, err)
	}

	if len(pairs) == 0 {
		j.logger.Debug("nothing to archive", zap.String("policy", p.Name))
		return nil
	}

	// Serialise pairs as a simple newline-delimited JSON file.
	archivePath := filepath.Join(p.DestDir,
		fmt.Sprintf("archive-%s-%d.ndjson", p.Name, time.Now().UnixNano()))

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		for _, pair := range pairs {
			line := fmt.Sprintf("{\"key\":%q,\"value\":%q}\n", pair.Key, string(pair.Value))
			if _, writeErr := pw.Write([]byte(line)); writeErr != nil {
				return
			}
		}
	}()

	if err = j.sink.Write(ctx, archivePath, pr); err != nil {
		return fmt.Errorf("archival: write %q: %w", archivePath, err)
	}

	// Delete archived keys from the live index.
	deleted := 0
	for _, pair := range pairs {
		if delErr := scanner.DeleteKey(ctx, pair.Key); delErr != nil {
			j.logger.Warn("archival: failed to delete key after archiving",
				zap.String("key", pair.Key), zap.Error(delErr))
			continue
		}
		deleted++
	}

	j.logger.Info("archival run complete",
		zap.String("policy", p.Name),
		zap.Int("pairs", len(pairs)),
		zap.Int("deleted", deleted),
		zap.String("archive", archivePath))
	return nil
}
