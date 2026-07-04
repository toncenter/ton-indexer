package crud

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Watermark written by the index worker inside the data replication stream:
// on any node it is an exact lower bound of the applied data. Polling it is
// the only ordered freshness signal: kvrocks does not wake blocked stream
// readers on replicas, and pushed notifications may overtake the data anyway.
const kvrocksWatermarkKey = kvrocksKeyPrefix + ":progress:finalized_mc_seqno"

const (
	kvrocksReplicaCooldown       = 5 * time.Second
	kvrocksReplicaStatsPeriod    = 60 * time.Second
	kvrocksReplicaDiscoverPeriod = 30 * time.Second
	kvrocksReplicaDiscoverBudget = 2 * time.Second
)

type kvrocksReplica struct {
	addr      string
	client    *redis.Client
	watermark atomic.Int64
}

// A replica holds every kvrocks write of blocks up to its watermark, so a
// missing key can only be replication lag when the requested data comes from
// a block above it. Unknown seqno (0) is treated as possibly lagging.
func (r *kvrocksReplica) mayLagBehind(maxKnownMcSeqno uint32) bool {
	if maxKnownMcSeqno == 0 {
		return true
	}
	w := r.watermark.Load()
	return w <= 0 || int64(maxKnownMcSeqno) > w
}

// kvrocksReplicaSet discovers replicas via sentinel and keeps one "current"
// fresh replica to serve reads: a replica is fresh when its watermark is at
// most StalenessBlocks behind the most advanced node. When no replica is
// fresh, reads fall back to the master.
type kvrocksReplicaSet struct {
	cfg       KvrocksConfig
	master    redis.UniversalClient
	sentinels []*redis.SentinelClient

	current         atomic.Pointer[kvrocksReplica]
	masterWatermark atomic.Int64

	mu        sync.Mutex
	replicas  map[string]*kvrocksReplica
	downUntil map[string]time.Time
	freshDesc string
	rrIndex   int

	replicaReads atomic.Uint64
	masterReads  atomic.Uint64
	refilledKeys atomic.Uint64
	fallbacks    atomic.Uint64

	stop chan struct{}
	done chan struct{}
}

func newKvrocksReplicaSet(cfg KvrocksConfig, master redis.UniversalClient) *kvrocksReplicaSet {
	rs := &kvrocksReplicaSet{
		cfg:       cfg,
		master:    master,
		replicas:  map[string]*kvrocksReplica{},
		downUntil: map[string]time.Time{},
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	for _, addr := range cfg.SentinelAddrs {
		rs.sentinels = append(rs.sentinels, redis.NewSentinelClient(&redis.Options{Addr: addr}))
	}
	rs.masterWatermark.Store(-1)
	go rs.run()
	return rs
}

func (rs *kvrocksReplicaSet) run() {
	defer close(rs.done)
	refresh := time.NewTicker(rs.cfg.ReplicaRefresh)
	defer refresh.Stop()
	discover := time.NewTicker(kvrocksReplicaDiscoverPeriod)
	defer discover.Stop()
	stats := time.NewTicker(kvrocksReplicaStatsPeriod)
	defer stats.Stop()

	rs.discoverAndReconcile()
	rs.refresh()
	for {
		select {
		case <-rs.stop:
			return
		case <-refresh.C:
			rs.refresh()
		case <-discover.C:
			rs.discoverAndReconcile()
		case <-stats.C:
			rs.logStats()
		}
	}
}

// refresh polls the watermark of every node outside the mutex and then picks
// the current fresh replica.
func (rs *kvrocksReplicaSet) refresh() {
	ctx, cancel := context.WithTimeout(context.Background(), rs.cfg.ReplicaRefresh)
	defer cancel()

	if w, ok := kvrocksNodeWatermark(ctx, rs.master); ok {
		rs.masterWatermark.Store(w)
	}
	replicas := rs.replicaSnapshot()
	for _, replica := range replicas {
		if w, ok := kvrocksNodeWatermark(ctx, replica.client); ok {
			replica.watermark.Store(w)
		} else {
			replica.watermark.Store(-1)
		}
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()

	maxWatermark := rs.masterWatermark.Load()
	for _, replica := range replicas {
		if w := replica.watermark.Load(); w > maxWatermark {
			maxWatermark = w
		}
	}

	now := time.Now()
	fresh := make([]*kvrocksReplica, 0, len(replicas))
	desc := make([]string, 0, len(replicas))
	for _, replica := range replicas {
		w := replica.watermark.Load()
		if w <= 0 {
			desc = append(desc, replica.addr+":no-watermark")
			continue
		}
		lag := maxWatermark - w
		if now.Before(rs.downUntil[replica.addr]) {
			desc = append(desc, fmt.Sprintf("%s:cooldown", replica.addr))
			continue
		}
		if lag > rs.cfg.StalenessBlocks {
			desc = append(desc, fmt.Sprintf("%s:lag=%d", replica.addr, lag))
			continue
		}
		fresh = append(fresh, replica)
		desc = append(desc, fmt.Sprintf("%s:fresh(lag=%d)", replica.addr, lag))
	}

	if len(fresh) == 0 {
		rs.current.Store(nil)
	} else {
		sort.Slice(fresh, func(i, j int) bool { return fresh[i].addr < fresh[j].addr })
		rs.rrIndex++
		rs.current.Store(fresh[rs.rrIndex%len(fresh)])
	}

	sort.Strings(desc)
	state := strings.Join(desc, " ")
	if state != rs.freshDesc {
		log.Printf("kvrocks replicas: %s (max watermark %d)", state, maxWatermark)
		rs.freshDesc = state
	}
}

func (rs *kvrocksReplicaSet) replicaSnapshot() []*kvrocksReplica {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	replicas := make([]*kvrocksReplica, 0, len(rs.replicas))
	for _, replica := range rs.replicas {
		replicas = append(replicas, replica)
	}
	return replicas
}

// discoverAndReconcile queries sentinel outside the mutex and applies the
// replica set diff under it.
func (rs *kvrocksReplicaSet) discoverAndReconcile() {
	ctx, cancel := context.WithTimeout(context.Background(), kvrocksReplicaDiscoverBudget)
	defer cancel()

	addrs := rs.discoverReplicas(ctx)
	if addrs == nil {
		return
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()
	seen := map[string]struct{}{}
	for _, addr := range addrs {
		seen[addr] = struct{}{}
		if _, ok := rs.replicas[addr]; ok {
			continue
		}
		replica := &kvrocksReplica{
			addr: addr,
			client: redis.NewClient(&redis.Options{
				Addr:     addr,
				Username: rs.cfg.Username,
				Password: rs.cfg.Password,
				DB:       rs.cfg.DB,
			}),
		}
		replica.watermark.Store(-1)
		rs.replicas[addr] = replica
	}
	for addr, replica := range rs.replicas {
		if _, ok := seen[addr]; ok {
			continue
		}
		_ = replica.client.Close()
		delete(rs.replicas, addr)
		delete(rs.downUntil, addr)
	}
}

func (rs *kvrocksReplicaSet) discoverReplicas(ctx context.Context) []string {
	for _, sentinel := range rs.sentinels {
		entries, err := sentinel.Replicas(ctx, rs.cfg.SentinelMasterName).Result()
		if err != nil {
			continue
		}
		addrs := make([]string, 0, len(entries))
		for _, entry := range entries {
			flags := entry["flags"]
			if strings.Contains(flags, "s_down") || strings.Contains(flags, "o_down") || strings.Contains(flags, "disconnected") {
				continue
			}
			if entry["ip"] == "" || entry["port"] == "" {
				continue
			}
			addrs = append(addrs, entry["ip"]+":"+entry["port"])
		}
		return addrs
	}
	return nil
}

func kvrocksNodeWatermark(ctx context.Context, c redis.Cmdable) (int64, bool) {
	value, err := c.Get(ctx, kvrocksWatermarkKey).Result()
	if err != nil {
		return 0, false
	}
	watermark, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return watermark, true
}

func (rs *kvrocksReplicaSet) pick() *kvrocksReplica {
	if rs == nil {
		return nil
	}
	return rs.current.Load()
}

func (rs *kvrocksReplicaSet) markDown(replica *kvrocksReplica, err error) {
	rs.fallbacks.Add(1)
	rs.mu.Lock()
	rs.downUntil[replica.addr] = time.Now().Add(kvrocksReplicaCooldown)
	rs.mu.Unlock()
	if current := rs.current.Load(); current != nil && current.addr == replica.addr {
		rs.current.CompareAndSwap(current, nil)
	}
	log.Printf("kvrocks replica %s excluded for %s: %v", replica.addr, kvrocksReplicaCooldown, err)
}

func (rs *kvrocksReplicaSet) close() {
	if rs == nil {
		return
	}
	close(rs.stop)
	<-rs.done
	rs.mu.Lock()
	for _, replica := range rs.replicas {
		_ = replica.client.Close()
	}
	rs.replicas = map[string]*kvrocksReplica{}
	rs.mu.Unlock()
	for _, sentinel := range rs.sentinels {
		_ = sentinel.Close()
	}
}

func (rs *kvrocksReplicaSet) logStats() {
	replica := rs.replicaReads.Swap(0)
	master := rs.masterReads.Swap(0)
	refilled := rs.refilledKeys.Swap(0)
	fallbacks := rs.fallbacks.Swap(0)
	if replica+master+refilled+fallbacks == 0 {
		return
	}
	log.Printf("kvrocks reads: replica=%d master=%d refilled_keys=%d fallbacks=%d", replica, master, refilled, fallbacks)
}

// kvrocksReadTarget is the immutable client choice of a snapshot; fallback
// swaps the whole target atomically, so concurrent reads within one request
// are race-free.
type kvrocksReadTarget struct {
	client  redis.UniversalClient
	replica *kvrocksReplica
}

type kvrocksReadSnapshot struct {
	store  *KvrocksStore
	target atomic.Pointer[kvrocksReadTarget]
}

type kvrocksReadSnapshotContextKey struct{}

type kvrocksReadOptions struct {
	maxKnownMcSeqno uint32
}

type kvrocksReadOptionsContextKey struct{}

func kvrocksSnapshotFromContext(ctx context.Context) (*kvrocksReadSnapshot, bool) {
	snapshot, ok := ctx.Value(kvrocksReadSnapshotContextKey{}).(*kvrocksReadSnapshot)
	return snapshot, ok && snapshot != nil && snapshot.target.Load() != nil
}

func kvrocksReadOptionsFromContext(ctx context.Context) kvrocksReadOptions {
	options, _ := ctx.Value(kvrocksReadOptionsContextKey{}).(kvrocksReadOptions)
	return options
}

func kvrocksContextWithMaxKnownMcSeqno(ctx context.Context, maxKnownMcSeqno uint32) context.Context {
	if maxKnownMcSeqno == 0 {
		return ctx
	}
	options := kvrocksReadOptionsFromContext(ctx)
	if maxKnownMcSeqno <= options.maxKnownMcSeqno {
		return ctx
	}
	options.maxKnownMcSeqno = maxKnownMcSeqno
	return context.WithValue(ctx, kvrocksReadOptionsContextKey{}, options)
}

// pinReadSnapshot pins the node choice for all kvrocks reads sharing the
// returned context. Use it at multi-command request boundaries; single-command
// getters inherit an existing snapshot from ctx but do not need to create one.
func (s *KvrocksStore) pinReadSnapshot(ctx context.Context) context.Context {
	if s == nil || s.client == nil || s.replicas == nil {
		return ctx
	}
	if _, ok := kvrocksSnapshotFromContext(ctx); ok {
		return ctx
	}
	snapshot := &kvrocksReadSnapshot{store: s}
	if replica := s.replicas.pick(); replica != nil {
		snapshot.target.Store(&kvrocksReadTarget{client: replica.client, replica: replica})
	} else {
		snapshot.target.Store(&kvrocksReadTarget{client: s.client})
	}
	return context.WithValue(ctx, kvrocksReadSnapshotContextKey{}, snapshot)
}

// fallbackToMaster switches the snapshot to the master for the rest of the
// request. Returns false when the snapshot already reads from the master.
func (snapshot *kvrocksReadSnapshot) fallbackToMaster(err error) bool {
	for {
		target := snapshot.target.Load()
		if target == nil || target.replica == nil {
			return false
		}
		if snapshot.target.CompareAndSwap(target, &kvrocksReadTarget{client: snapshot.store.client}) {
			snapshot.store.replicas.markDown(target.replica, err)
			return true
		}
	}
}

// kvReadWithFallback runs a read on the pinned snapshot node (or the current
// fresh replica when no snapshot is pinned) and retries on the master when a
// replica fails. A pinned snapshot is switched to the master permanently so
// later reads of the same request stay on one node.
func kvReadWithFallback[T any](ctx context.Context, s *KvrocksStore, fn func(redis.UniversalClient) (T, error)) (T, *kvrocksReplica, error) {
	countRead := func(onReplica bool) {
		if s.replicas == nil {
			return
		}
		if onReplica {
			s.replicas.replicaReads.Add(1)
		} else {
			s.replicas.masterReads.Add(1)
		}
	}

	if snapshot, ok := kvrocksSnapshotFromContext(ctx); ok {
		target := snapshot.target.Load()
		res, err := fn(target.client)
		if err == nil || err == redis.Nil {
			countRead(target.replica != nil)
			return res, target.replica, err
		}
		if ctx.Err() != nil || !snapshot.fallbackToMaster(err) {
			return res, target.replica, err
		}
		res, err = fn(snapshot.store.client)
		if err == nil || err == redis.Nil {
			countRead(false)
		}
		return res, nil, err
	}

	if replica := s.replicas.pick(); replica != nil {
		res, err := fn(replica.client)
		if err == nil || err == redis.Nil {
			countRead(true)
			return res, replica, err
		}
		if ctx.Err() != nil {
			return res, replica, err
		}
		s.replicas.markDown(replica, err)
	}
	res, err := fn(s.client)
	if err == nil || err == redis.Nil {
		countRead(false)
	}
	return res, nil, err
}
