// Package schema_migrator provides versioned schema migration support for
// GojoDB.  Each migration is a named, ordered operation that transforms the
// database schema or data layout.  Migrations are idempotent: running them
// twice leaves the database in the same state as running them once.
package schema_migrator

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Direction int

const (
	Up   Direction = iota
	Down Direction = iota
)

type Migration struct {
	Version int64
	Name    string
	Up      func(ctx context.Context, exec Executor) error
	Down    func(ctx context.Context, exec Executor) error
}

type Executor interface {
	Put(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	Scan(ctx context.Context, prefix string) ([]string, error)
}

type appliedRecord struct {
	Version   int64
	Name      string
	AppliedAt time.Time
}

type MigrationStatus struct {
	Migration Migration
	Applied   bool
	AppliedAt time.Time
}

type Migrator struct {
	mu         sync.Mutex
	migrations []Migration
}

func NewMigrator() *Migrator {
	return &Migrator{}
}

func (m *Migrator) Register(mig Migration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, existing := range m.migrations {
		if existing.Version == mig.Version {
			panic(fmt.Sprintf("schema_migrator: duplicate migration version %d", mig.Version))
		}
	}
	m.migrations = append(m.migrations, mig)
}

func (m *Migrator) Migrate(ctx context.Context, exec Executor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	applied, err := m.loadApplied(ctx, exec)
	if err != nil {
		return err
	}
	for _, mig := range m.sorted() {
		if _, ok := applied[mig.Version]; ok {
			continue
		}
		if err := m.run(ctx, exec, mig, Up); err != nil {
			return fmt.Errorf("schema_migrator: migration %d (%s) up: %w", mig.Version, mig.Name, err)
		}
		if err := m.markApplied(ctx, exec, mig); err != nil {
			return fmt.Errorf("schema_migrator: mark applied %d: %w", mig.Version, err)
		}
	}
	return nil
}

func (m *Migrator) Rollback(ctx context.Context, exec Executor, n int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	applied, err := m.loadApplied(ctx, exec)
	if err != nil {
		return err
	}
	sorted := m.sorted()
	reversed := make([]Migration, len(sorted))
	copy(reversed, sorted)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	rolled := 0
	for _, mig := range reversed {
		if rolled >= n {
			break
		}
		if _, ok := applied[mig.Version]; !ok {
			continue
		}
		if err := m.run(ctx, exec, mig, Down); err != nil {
			return fmt.Errorf("schema_migrator: migration %d (%s) down: %w", mig.Version, mig.Name, err)
		}
		if err := m.markReverted(ctx, exec, mig.Version); err != nil {
			return fmt.Errorf("schema_migrator: mark reverted %d: %w", mig.Version, err)
		}
		rolled++
	}
	return nil
}

func (m *Migrator) Status(ctx context.Context, exec Executor) ([]MigrationStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	applied, err := m.loadApplied(ctx, exec)
	if err != nil {
		return nil, err
	}
	var out []MigrationStatus
	for _, mig := range m.sorted() {
		s := MigrationStatus{Migration: mig}
		if rec, ok := applied[mig.Version]; ok {
			s.Applied = true
			s.AppliedAt = rec.AppliedAt
		}
		out = append(out, s)
	}
	return out, nil
}

func (m *Migrator) run(ctx context.Context, exec Executor, mig Migration, dir Direction) error {
	if dir == Up {
		if mig.Up == nil {
			return nil
		}
		return mig.Up(ctx, exec)
	}
	if mig.Down == nil {
		return fmt.Errorf("no Down function defined for migration %d", mig.Version)
	}
	return mig.Down(ctx, exec)
}

func (m *Migrator) sorted() []Migration {
	cp := make([]Migration, len(m.migrations))
	copy(cp, m.migrations)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Version < cp[j].Version })
	return cp
}

func (m *Migrator) recordKey(version int64) string {
	return fmt.Sprintf("__schema_migration/%d", version)
}

func (m *Migrator) markApplied(ctx context.Context, exec Executor, mig Migration) error {
	data := []byte(fmt.Sprintf("%d|%s|%s", mig.Version, mig.Name, time.Now().Format(time.RFC3339)))
	return exec.Put(ctx, m.recordKey(mig.Version), data)
}

func (m *Migrator) markReverted(ctx context.Context, exec Executor, version int64) error {
	return exec.Delete(ctx, m.recordKey(version))
}

func (m *Migrator) loadApplied(ctx context.Context, exec Executor) (map[int64]appliedRecord, error) {
	keys, err := exec.Scan(ctx, "__schema_migration/")
	if err != nil {
		return nil, err
	}
	applied := make(map[int64]appliedRecord, len(keys))
	for _, key := range keys {
		raw, err := exec.Get(ctx, key)
		if err != nil {
			continue
		}
		var rec appliedRecord
		_, _ = fmt.Sscanf(string(raw), "%d|", &rec.Version)
		applied[rec.Version] = rec
	}
	return applied, nil
}
