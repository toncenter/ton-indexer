package crud

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type hotColdSplit struct {
	Lt    uint64
	Seqno uint64
	Utime uint64
}

func (h hotColdSplit) getSplit(key string) uint64 {
	switch key {
	case "lt":
		return h.Lt
	case "seqno":
		return h.Seqno
	case "utime":
		return h.Utime
	default:
		return h.Lt
	}
}

type splitEntry struct {
	val       hotColdSplit
	expiresAt time.Time
}

type SplitProvider struct {
	hot *pgxpool.Pool
	ttl time.Duration

	entry     atomic.Pointer[splitEntry]
	refreshMu sync.Mutex
}

const publishedSplitQuery = `select lt, seqno, utime from _ton_hot_cold_split where id = 1`

func newSplitProvider(hot *pgxpool.Pool, ttl time.Duration) *SplitProvider {
	return &SplitProvider{hot: hot, ttl: ttl}
}

// Split returns the shared routing boundary
func (s *SplitProvider) Split(ctx context.Context) (hotColdSplit, error) {

	now := time.Now()
	entry := s.entry.Load()
	if entry != nil && now.Before(entry.expiresAt) {
		return entry.val, nil
	}

	if entry != nil {
		// Another goroutine is refreshing, leave update to it, return current values
		if !s.refreshMu.TryLock() {
			// Reload in case the refresh completed
			return s.entry.Load().val, nil
		}
	} else {
		s.refreshMu.Lock()
	}
	defer s.refreshMu.Unlock()

	// Another goroutine may have refreshed while we waited for refreshMu.
	now = time.Now()
	old := s.entry.Load()
	if old != nil && now.Before(old.expiresAt) {
		return old.val, nil
	}

	val, err := querySplit(ctx, s.hot)
	if err != nil {
		if old != nil {
			log.Printf(
				"hot/cold: split refresh failed, serving stale boundary: %v",
				err,
			)
			return old.val, nil
		}
		return hotColdSplit{}, err
	}

	s.entry.Store(&splitEntry{
		val:       val,
		expiresAt: time.Now().Add(s.ttl),
	})

	return val, nil
}

func querySplit(ctx context.Context, hot *pgxpool.Pool) (hotColdSplit, error) {
	var lt, seqno, utime int64
	err := hot.QueryRow(ctx, publishedSplitQuery).Scan(&lt, &seqno, &utime)
	if err != nil {
		return hotColdSplit{}, fmt.Errorf("published split read failed: %w", err)
	}
	return hotColdSplit{Lt: uint64(lt), Seqno: uint64(seqno), Utime: uint64(utime)}, nil
}
