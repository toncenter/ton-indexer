package main

import (
	"container/heap"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/sirupsen/logrus"
)

type hash [32]byte

type TraceData struct {
	RootNodeKey string
	Nodes       map[string]TraceNode
}

type TraceUpdateStats struct {
	TraceId     string
	Emulated    int
	NonEmulated int
	Total       int
	Updates     int
}

type TraceNode struct {
	Transaction Transaction `msgpack:"transaction"`
	Emulated    bool        `msgpack:"emulated"`
}

type Transaction struct {
	Account string      `msgpack:"account"`
	OutMsgs []trMessage `msgpack:"out_msgs" json:"out_msgs"`
}

type trMessage struct {
	Hash hash `msgpack:"hash" json:"hash"`
}

// SyntheticTracesTracker tracks synthetic traces by account
type SyntheticTracesTracker struct {
	mu                sync.RWMutex
	accountToTraceIds map[string]mapset.Set // account -> set of trace IDs
}

func NewSyntheticTracesTracker() *SyntheticTracesTracker {
	return &SyntheticTracesTracker{
		accountToTraceIds: make(map[string]mapset.Set),
	}
}

// AddSyntheticTrace Enables tracking for synthetic trace for account
func (stt *SyntheticTracesTracker) AddSyntheticTrace(account, traceId string) {
	stt.mu.Lock()
	defer stt.mu.Unlock()

	traceSet, exists := stt.accountToTraceIds[account]
	if !exists {
		traceSet = mapset.NewSet()
		stt.accountToTraceIds[account] = traceSet
	}
	traceSet.Add(traceId)
}

// GetSyntheticTraces returns all synthetic trace IDs for a given account
func (stt *SyntheticTracesTracker) GetSyntheticTraces(account string) []string {
	stt.mu.RLock()
	defer stt.mu.RUnlock()

	if traceSet, exists := stt.accountToTraceIds[account]; exists {
		traces := make([]string, 0, traceSet.Cardinality())
		for trace := range traceSet.Iter() {
			traces = append(traces, trace.(string))
		}
		return traces
	}
	return []string{}
}

// ClearSyntheticTraces removes all synthetic traces for an account
func (stt *SyntheticTracesTracker) ClearSyntheticTraces(account string) []string {
	stt.mu.Lock()
	defer stt.mu.Unlock()

	if traceSet, exists := stt.accountToTraceIds[account]; exists {
		traces := make([]string, 0, traceSet.Cardinality())
		for trace := range traceSet.Iter() {
			traces = append(traces, trace.(string))
		}
		delete(stt.accountToTraceIds, account)
		return traces
	}
	return []string{}
}

// RemoveTrace removes a specific trace from whatever account it's in
func (stt *SyntheticTracesTracker) RemoveTrace(traceId string) {
	stt.mu.Lock()
	defer stt.mu.Unlock()

	// Find which account has this trace and remove it
	for account, traceSet := range stt.accountToTraceIds {
		if traceSet.Contains(traceId) {
			traceSet.Remove(traceId)
			// If the set is now empty, remove the account entry
			if traceSet.Cardinality() == 0 {
				delete(stt.accountToTraceIds, account)
			}
			// We assume the trace ID is only in one account's set
			break
		}
	}
}

// AddressCollector holds a concurrency safe map of key -> set of addresses
type AddressCollector struct {
	mu   sync.RWMutex
	data map[string]mapset.Set
}

func NewAddressCollector() *AddressCollector {
	return &AddressCollector{
		data: make(map[string]mapset.Set),
	}
}

// MergeAddresses merges the given addresses into the set for the specified key
func (ac *AddressCollector) MergeAddresses(key string, addresses []string) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	setForKey, exists := ac.data[key]
	if !exists {
		setForKey = mapset.NewSet()
		ac.data[key] = setForKey
	}
	// Merge them
	for _, addr := range addresses {
		setForKey.Add(addr)
	}
}

// GetAddresses returns the set of addresses for the given key (or empty set if not found).
func (ac *AddressCollector) GetAddresses(key string) mapset.Set {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	if setForKey, exists := ac.data[key]; exists {
		return setForKey.Clone()
	}
	return mapset.NewSet()
}

// RemoveKey removes the key (and its set of addresses) entirely from the collector
func (ac *AddressCollector) RemoveKey(key string) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	delete(ac.data, key)
}

// KeyExpiry holds the Redis key name and the time it should expire.
type KeyExpiry struct {
	Key        string
	ExpiryTime time.Time
}

// ExpiryMinHeap implements a min-heap based on ExpiryTime (earliest first).
type ExpiryMinHeap []KeyExpiry

// sort.Interface
func (h ExpiryMinHeap) Len() int           { return len(h) }
func (h ExpiryMinHeap) Less(i, j int) bool { return h[i].ExpiryTime.Before(h[j].ExpiryTime) }
func (h ExpiryMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// heap.Interface
func (h *ExpiryMinHeap) Push(x interface{}) {
	*h = append(*h, x.(KeyExpiry))
}
func (h *ExpiryMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// ExpiryTracker tracks key expiries with a min-heap + map to handle refreshes.
type ExpiryTracker struct {
	mu             sync.Mutex
	heap           ExpiryMinHeap
	latestExpiries map[string]time.Time // key -> latest expiry
}

// NewExpiryTracker creates an ExpiryTracker with empty heap and map.
func NewExpiryTracker() *ExpiryTracker {
	return &ExpiryTracker{
		heap:           make(ExpiryMinHeap, 0),
		latestExpiries: make(map[string]time.Time),
	}
}

// Add inserts a KeyExpiry into the min-heap and updates the map (for refresh).
func (et *ExpiryTracker) Add(key string, expiryTime time.Time) {
	et.mu.Lock()
	defer et.mu.Unlock()

	// Update "latest" expiry in the map
	et.latestExpiries[key] = expiryTime

	// Push onto heap
	heap.Push(&et.heap, KeyExpiry{Key: key, ExpiryTime: expiryTime})
}

// GetExpired pops truly expired keys (mark-and-skip older TTLs).
func (et *ExpiryTracker) GetExpired(now time.Time) []string {
	et.mu.Lock()
	defer et.mu.Unlock()

	var expired []string

	for et.heap.Len() > 0 {
		top := et.heap[0] // earliest expiry
		if top.ExpiryTime.After(now) {
			// Not yet expired
			break
		}

		popped := heap.Pop(&et.heap).(KeyExpiry)

		latest, exists := et.latestExpiries[popped.Key]
		if !exists {
			// If not in the map, skip (unlikely, but could happen).
			continue
		}

		// If the popped expiry is older than what's in the map, it's stale.
		if popped.ExpiryTime.Before(latest) {
			continue
		}

		// Otherwise, it's the real expiry (== latest in the map) and <= now.
		expired = append(expired, popped.Key)
		delete(et.latestExpiries, popped.Key)
	}

	return expired
}

type RedisTTLTracker struct {
	redisClient   *redis.Client
	expiryTracker *ExpiryTracker

	pubsubChan <-chan *redis.Message

	logger *logrus.Logger

	defaultTraceTtl   time.Duration
	syntheticTraceTtl time.Duration
	completedTraceTtl time.Duration

	maxRetries int
	baseDelay  time.Duration
	maxDelay   time.Duration

	wg               sync.WaitGroup
	addressCollector *AddressCollector
	refaccsChan      <-chan *redis.Message

	syntheticTracesTracker *SyntheticTracesTracker
}

// NewRedisTTLTracker initializes the TTL tracker.
func NewRedisTTLTracker(
	client *redis.Client,
	pubsubChan <-chan *redis.Message,
	refaccsChan <-chan *redis.Message,
	defaultTraceTtl time.Duration,
	syntheticTraceTtl time.Duration,
	completedTraceTtl time.Duration,
	logger *logrus.Logger,
	maxRetries int,
	baseDelay time.Duration,
	maxDelay time.Duration,
) *RedisTTLTracker {
	return &RedisTTLTracker{
		redisClient:       client,
		expiryTracker:     NewExpiryTracker(),
		pubsubChan:        pubsubChan,
		logger:            logger,
		defaultTraceTtl:   defaultTraceTtl,
		syntheticTraceTtl: syntheticTraceTtl,
		completedTraceTtl: completedTraceTtl,
		refaccsChan:       refaccsChan,
		maxRetries:        maxRetries,
		baseDelay:         baseDelay,
		maxDelay:          maxDelay,
		addressCollector:  NewAddressCollector(),
		// Initialize the new emulated trace tracker
		syntheticTracesTracker: NewSyntheticTracesTracker(),
	}
}

func (rtt *RedisTTLTracker) Run(ctx context.Context) {
	rtt.wg.Add(2)
	go rtt.listenForKeys(ctx)
	go rtt.periodicExpiryCheck(ctx)
}

func (rtt *RedisTTLTracker) Stop() {
	rtt.wg.Wait()
}

// listenForKeys listens on the Pub/Sub channel for new hash keys and schedules them.
func (rtt *RedisTTLTracker) listenForKeys(ctx context.Context) {
	defer rtt.wg.Done()

	for {
		select {
		case <-ctx.Done():
			rtt.logger.Info("Stopped listening for new keys (context canceled).")
			return
		case msg, ok := <-rtt.pubsubChan:
			if !ok {
				rtt.logger.Warn("PubSub channel closed, stopping listener.")
				return
			}

			key := msg.Payload
			traceData, err := rtt.readTrace(ctx, key)
			if err != nil {
				rtt.logger.WithError(err).WithField("key", key).Error("Failed to read trace with key")
				rtt.setupTraceTtl(ctx, key, nil)
				continue
			}
			rtt.setupTraceTtl(ctx, key, traceData)
			err = rtt.handleTraceEmulationStatus(ctx, key, traceData)
			if err != nil {
				rtt.logger.WithError(err).WithField("key", key).Error("Failed to process trace node")
			}

			err = rtt.collectAndMergeAddresses(ctx, key, traceData)
			if err != nil {
				rtt.logger.WithError(err).WithField("key", key).Error("Failed to collect and merge addresses")
			}

		case msg, ok := <-rtt.refaccsChan:
			if !ok {
				rtt.logger.Warn("PubSub channel closed, stopping listener.")
				return
			}
			data := msg.Payload
			tokens := strings.Split(data, ";")
			account := tokens[0]
			traceId := tokens[1]
			rtt.addressCollector.MergeAddresses(traceId, []string{account})
			rtt.logger.WithFields(logrus.Fields{
				"key":     traceId,
				"account": account,
			}).Debug("Updated referenced account")
		}
	}
}

func (rtt *RedisTTLTracker) setupTraceTtl(ctx context.Context, hashKey string, data *TraceData) {
	ttl := rtt.defaultTraceTtl
	if data != nil {
		rootNode, exists := data.Nodes[data.RootNodeKey]
		if exists && rootNode.Emulated {
			ttl = rtt.syntheticTraceTtl
		} else {
			emulated := 0
			nonEmulated := 0
			for _, node := range data.Nodes {
				if node.Emulated {
					emulated++
				} else {
					nonEmulated++
				}
			}
			if emulated == 0 {
				ttl = rtt.completedTraceTtl
			}
		}
	}
	expiration := time.Now().Add(ttl)
	rtt.expiryTracker.Add(hashKey, expiration)
	rtt.logger.WithFields(logrus.Fields{
		"trace":     hashKey,
		"expiresAt": expiration.Format(time.RFC3339),
	}).Debug("Scheduled (or refreshed) key TTL")
}

func (rtt *RedisTTLTracker) readTrace(ctx context.Context, hashKey string) (*TraceData, error) {
	traceData, err := rtt.redisClient.HGetAll(ctx, hashKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get hash for key %s: %w", hashKey, err)
	}
	rootNodeKey, exists := traceData["root_node"]
	if !exists {
		return nil, fmt.Errorf("root_node not found in trace %s", hashKey)
	}
	nodes := make(map[string]TraceNode)
	for key, value := range traceData {
		if key == "root_node" || key == "depth_limit_exceeded" || strings.Contains(key, ":") || strings.Contains(key, "actions") {
			continue
		}
		var node TraceNode
		err := msgpack.Unmarshal([]byte(value), &node)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal node for key %s: %w", key, err)
		}
		nodes[key] = node
	}
	return &TraceData{
		RootNodeKey: rootNodeKey,
		Nodes:       nodes,
	}, nil
}

// handleTraceEmulationStatus checks if trace is synthetic and adds it to tracker, otherwise cleanup all synthetic traces for root tx account
func (rtt *RedisTTLTracker) handleTraceEmulationStatus(ctx context.Context, hashKey string, data *TraceData) error {
	if data == nil {
		return nil
	}
	rootTxNode, exists := data.Nodes[data.RootNodeKey]
	if exists {
		account := rootTxNode.Transaction.Account
		if rootTxNode.Emulated {
			rtt.syntheticTracesTracker.AddSyntheticTrace(account, hashKey)
			rtt.logger.WithFields(logrus.Fields{
				"trace":   hashKey,
				"account": account,
			}).Debug("Added synthetic trace to tracker")
		} else {
			// This is a non-emulated trace root tx, clear all synthetic traces for this account
			clearedTraces := rtt.syntheticTracesTracker.ClearSyntheticTraces(account)

			if len(clearedTraces) > 0 {
				// Expire all synthetic traces
				for _, traceId := range clearedTraces {
					if traceId == hashKey {
						continue
					}
					rtt.expiryTracker.Add(traceId, time.Now())

					rtt.logger.WithFields(logrus.Fields{
						"clearedTrace": traceId,
						"account":      account,
						"newTrace":     hashKey,
					}).Info("Cleared emulated trace due to non-emulated trace appearance")
				}
			}
		}
	}

	return nil
}

// periodicExpiryCheck checks for expired keys in the min-heap and processes them in batch.
func (rtt *RedisTTLTracker) periodicExpiryCheck(ctx context.Context) {
	defer rtt.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			rtt.logger.Info("Stopping periodic expiry check (context canceled).")
			return
		case <-ticker.C:
			rtt.handleExpiredKeys(ctx)
		}
	}
}

// handleExpiredKeys processes expired keys, removes them from relevant data structures, and cleans up related indices.
func (rtt *RedisTTLTracker) handleExpiredKeys(ctx context.Context) {
	now := time.Now()
	expiredKeys := rtt.expiryTracker.GetExpired(now)
	if len(expiredKeys) == 0 {
		return
	}

	rtt.logger.WithField("count", len(expiredKeys)).
		Infof("Found %d truly expired key(s) at %s", len(expiredKeys), now.Format(time.RFC3339))

	addressToKeys := make(map[string][]string)

	// Gather addresses from each hash, delete the hash, and store them in addressToKeys
	for _, key := range expiredKeys {
		// Remove from emulated tracker if it exists there
		rtt.syntheticTracesTracker.RemoveTrace(key)
		addresses, err := rtt.collectAddressesAndDeleteHash(ctx, key)
		if err != nil {
			// Log the error but continue
			rtt.logger.WithError(err).WithField("key", key).
				Error("Failed to delete hash or collect addresses")
			continue
		}
		// For each address, accumulate this key
		for _, addr := range addresses {
			addressToKeys[addr] = append(addressToKeys[addr], key)
		}
	}

	//  For each address, do ZRange to get members, filter out any that contain any expired key
	for addr, keys := range addressToKeys {
		err := rtt.removeKeysFromSortedSet(ctx, addr, keys)
		if err != nil {
			rtt.logger.WithError(err).WithFields(logrus.Fields{
				"address": addr,
				"keys":    keys,
			}).Error("Failed removing keys from sorted set")
		}
	}
	// Cleanup classifier action account indexes
	for addr, keys := range addressToKeys {
		err := rtt.removeKeysFromSortedSet(ctx, "_aai:"+addr, keys)
		if err != nil {
			rtt.logger.WithError(err).WithFields(logrus.Fields{
				"address": addr,
				"keys":    keys,
			}).Error("Failed removing keys from sorted set")
		}
	}
}

func (rtt *RedisTTLTracker) collectAndMergeAddresses(ctx context.Context, hashKey string, data *TraceData) error {
	if data == nil {
		return nil
	}
	var addresses []string
	for _, node := range data.Nodes {
		addresses = append(addresses, node.Transaction.Account)
	}

	rtt.addressCollector.MergeAddresses(hashKey, addresses)
	return nil
}

// collectAddressesAndDeleteHash loads addresses from the hash, attempts to delete the hash
// with a simple retry, and returns the addresses found.
func (rtt *RedisTTLTracker) collectAddressesAndDeleteHash(ctx context.Context, hashKey string) ([]string, error) {

	addressSet := rtt.addressCollector.GetAddresses(hashKey)
	addresses := make([]string, 0)
	for addr := range addressSet.Iter() {
		addresses = append(addresses, addr.(string))
	}
	rtt.addressCollector.RemoveKey(hashKey)

	if err := rtt.deleteKeyWithRetry(ctx, hashKey); err != nil {
		return addresses, fmt.Errorf("failed to delete key %s after retries: %w", hashKey, err)
	}

	return addresses, nil
}

// deleteKeyWithRetry does a simple exponential backoff for DEL key.
func (rtt *RedisTTLTracker) deleteKeyWithRetry(ctx context.Context, key string) error {
	var attempt int
	delay := rtt.baseDelay

	for {
		attempt++
		err := rtt.redisClient.Del(ctx, key).Err()
		if err == nil || errors.Is(err, redis.Nil) {
			// success or key doesn't exist
			return nil
		}

		if attempt >= rtt.maxRetries {
			return fmt.Errorf("exceeded maxRetries; last err: %w", err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		rtt.logger.WithFields(logrus.Fields{
			"key":     key,
			"attempt": attempt,
			"error":   err.Error(),
		}).Warnf("DEL key failed, retrying in %s", delay)
		time.Sleep(delay)
		delay = minDuration(delay*2, rtt.maxDelay)
	}
}

// removeKeysFromSortedSet does exactly ONE ZRange for the address,
// finds all members that contain ANY of the expired keys,
// then issues a single ZRem call for them.
func (rtt *RedisTTLTracker) removeKeysFromSortedSet(ctx context.Context, address string, expiredKeys []string) error {
	// Get all members of sorted set for that address
	members, err := rtt.redisClient.ZRange(ctx, address, 0, -1).Result()
	if err != nil {
		// If the set doesn't exist, that's not an error
		if errors.Is(err, redis.Nil) {
			return nil
		}
		return fmt.Errorf("failed to ZRange address %s: %w", address, err)
	}

	// Filter out members that contain any of the expired keys as a substring
	var toRemove []interface{}
	for _, member := range members {
		for _, k := range expiredKeys {
			if strings.Contains(member, k) {
				toRemove = append(toRemove, member)
			}
		}
	}

	if len(toRemove) == 0 {
		return nil
	}

	// Remove expired entries
	if err := rtt.redisClient.ZRem(ctx, address, toRemove...).Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return nil
		}
		return fmt.Errorf("failed to ZRem members from address %s: %w", address, err)
	}

	rtt.logger.WithFields(logrus.Fields{
		"address":     address,
		"removed":     len(toRemove),
		"expiredKeys": expiredKeys,
	}).Debug("Removed matched members in batch")

	return nil
}

// containsColon checks if a string contains a ':' character
func containsColon(s string) bool {
	return strings.Contains(s, ":")
}

// minDuration returns the lesser of two durations.
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// RedisSnapshot represents a point-in-time snapshot of Redis keys and set contents
type RedisSnapshot struct {
	Keys        map[string]struct{}        // All keys in Redis
	SetContents map[string]map[string]bool // Set key -> Set of members
}

// RedisCleanupTool detects and removes stalled/untracked data in Redis
type RedisCleanupTool struct {
	redisClient      *redis.Client
	logger           *logrus.Logger
	snapshotInterval time.Duration
	expiryTracker    *ExpiryTracker // Access to tracked keys
	cleanupMutex     sync.Mutex     // Protect cleanup operations
}

// NewRedisCleanupTool creates a new instance of the cleanup tool
func NewRedisCleanupTool(
	client *redis.Client,
	logger *logrus.Logger,
	snapshotInterval time.Duration,
	expiryTracker *ExpiryTracker,
) *RedisCleanupTool {
	return &RedisCleanupTool{
		redisClient:      client,
		logger:           logger,
		snapshotInterval: snapshotInterval,
		expiryTracker:    expiryTracker,
	}
}

// takeSnapshot captures the current state of Redis data
func (rct *RedisCleanupTool) takeSnapshot(ctx context.Context) (*RedisSnapshot, error) {
	rct.logger.Info("Taking Redis snapshot...")

	snapshot := &RedisSnapshot{
		Keys:        make(map[string]struct{}),
		SetContents: make(map[string]map[string]bool),
	}

	// Get all keys
	var cursor uint64
	var keys []string
	var err error

	for {
		keys, cursor, err = rct.redisClient.Scan(ctx, cursor, "*", 1000).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan Redis keys: %w", err)
		}

		// Add keys to snapshot
		for _, key := range keys {
			// Skip certain keys
			if strings.Contains(key, "tr_in_msg") || strings.Contains(key, "tr_root_tx") {
				continue
			}

			// Skip if key is currently being tracked by ExpiryTracker
			rct.expiryTracker.mu.Lock()
			_, tracked := rct.expiryTracker.latestExpiries[key]
			rct.expiryTracker.mu.Unlock()
			if tracked {
				continue
			}

			snapshot.Keys[key] = struct{}{}

			// For each key, check its type
			keyType, err := rct.redisClient.Type(ctx, key).Result()
			if err != nil {
				rct.logger.WithError(err).WithField("key", key).
					Warn("Failed to determine key type")
				continue
			}

			if keyType == "set" || keyType == "zset" {
				// For sets and sorted sets, get all members
				var members []string
				if keyType == "set" {
					members, err = rct.redisClient.SMembers(ctx, key).Result()
				} else {
					members, err = rct.redisClient.ZRange(ctx, key, 0, -1).Result()
				}

				if err != nil {
					rct.logger.WithError(err).WithField("key", key).
						Warn("Failed to get set/zset members")
					continue
				}

				memberMap := make(map[string]bool)
				for _, member := range members {
					memberMap[member] = true
				}
				snapshot.SetContents[key] = memberMap
			}
		}

		if cursor == 0 {
			break
		}
	}

	rct.logger.WithFields(logrus.Fields{
		"keys":        len(snapshot.Keys),
		"collections": len(snapshot.SetContents),
	}).Info("Redis snapshot completed")

	return snapshot, nil
}

// isTraceDataKey checks if the key is a trace data hash
func (rct *RedisCleanupTool) isTraceDataKey(ctx context.Context, key string) bool {
	// Check if it might be a trace hash by looking for the root_node field
	exists, err := rct.redisClient.HExists(ctx, key, "root_node").Result()
	if err != nil {
		// If error, we can't confirm it's a trace hash
		return false
	}
	return exists
}

// isKeyBeingTracked checks if a key is currently being actively tracked
func (rct *RedisCleanupTool) isKeyBeingTracked(ctx context.Context, key string) bool {
	rct.expiryTracker.mu.Lock()
	defer rct.expiryTracker.mu.Unlock()
	_, tracked := rct.expiryTracker.latestExpiries[key]
	return tracked
}

// extractTraceKeysFromMember parses a set member string to find the trace hash key
func extractTraceKeysFromMember(setKey string, member string) string {
	parts := strings.Split(member, ":")
	if len(parts) >= 2 {
		traceId := parts[0]
		if len(traceId) > 0 {
			return traceId
		}
	}
	return ""
}

// cleanup compares two snapshots and cleans up stalled data
func (rct *RedisCleanupTool) cleanup(ctx context.Context, oldSnapshot, newSnapshot *RedisSnapshot) {
	rct.cleanupMutex.Lock()
	defer rct.cleanupMutex.Unlock()

	rct.logger.Info("Starting Redis cleanup process...")

	// Find stalled keys (present in both snapshots)
	stalledKeys := make([]string, 0)
	for key := range oldSnapshot.Keys {
		if _, exists := newSnapshot.Keys[key]; exists {
			// Verify it's not currently tracked
			if !rct.isKeyBeingTracked(ctx, key) {
				// Only process trace data keys
				if rct.isTraceDataKey(ctx, key) {
					stalledKeys = append(stalledKeys, key)
				}
			}
		}
	}

	// Delete stalled hash keys
	if len(stalledKeys) > 0 {
		rct.logger.WithField("count", len(stalledKeys)).
			Info("Found stalled trace hash keys to clean up")

		for _, key := range stalledKeys {
			rct.logger.WithField("key", key).Info("Deleting stalled trace hash key")
			if err := rct.redisClient.Del(ctx, key).Err(); err != nil {
				rct.logger.WithError(err).WithField("key", key).
					Error("Failed to delete stalled key")
			}
		}
	}

	// Process stalled set members
	stalledSetItems := make(map[string][]string) // Set key -> list of stalled members

	for setKey, oldMembers := range oldSnapshot.SetContents {
		if newMembers, exists := newSnapshot.SetContents[setKey]; exists {
			// For every member in both snapshots
			for member := range oldMembers {
				if newMembers[member] {
					// Extract trace key using set-specific parsing logic
					traceKey := extractTraceKeysFromMember(setKey, member)

					if traceKey == "" {
						continue
					}

					// If trace is being tracked, don't remove the member
					if !rct.isKeyBeingTracked(ctx, traceKey) {
						if _, exists := stalledSetItems[setKey]; !exists {
							stalledSetItems[setKey] = make([]string, 0)
						}
						stalledSetItems[setKey] = append(stalledSetItems[setKey], member)
					}
				}
			}
		}
	}

	// Delete stalled set members
	for setKey, members := range stalledSetItems {
		if len(members) == 0 {
			continue
		}

		// Check if key is a set or sorted set
		keyType, err := rct.redisClient.Type(ctx, setKey).Result()
		if err != nil {
			rct.logger.WithError(err).WithField("setKey", setKey).
				Error("Failed to determine set key type")
			continue
		}

		rct.logger.WithFields(logrus.Fields{
			"setKey":      setKey,
			"memberCount": len(members),
			"keyType":     keyType,
			"isAAI":       strings.HasPrefix(setKey, "_aai:"),
		}).Info("Removing stalled members from set/zset")

		// Remove members
		args := make([]interface{}, len(members))
		for i, member := range members {
			args[i] = member
		}

		if keyType == "set" {
			err = rct.redisClient.SRem(ctx, setKey, args...).Err()
		} else if keyType == "zset" {
			err = rct.redisClient.ZRem(ctx, setKey, args...).Err()
		}

		if err != nil {
			rct.logger.WithError(err).WithField("setKey", setKey).
				Error("Failed to remove stalled members")
		}
	}

	rct.logger.WithFields(logrus.Fields{
		"hashKeysRemoved":        len(stalledKeys),
		"setsWithRemovedMembers": len(stalledSetItems),
	}).Info("Redis cleanup completed")
}

// Run executes the cleanup process periodically
func (rct *RedisCleanupTool) Run(ctx context.Context) {
	rct.logger.WithField("interval", rct.snapshotInterval).
		Info("Starting Redis cleanup tool")
	oldSnapshot, err := rct.takeSnapshot(ctx)
	if err != nil {
		rct.logger.WithError(err).Error("Failed to take initial Redis snapshot")
		return
	}

	ticker := time.NewTicker(rct.snapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			rct.logger.Info("Redis cleanup tool shutting down")
			return
		case <-ticker.C:
			newSnapshot, err := rct.takeSnapshot(ctx)
			if err != nil {
				rct.logger.WithError(err).Error("Failed to take new Redis snapshot")
				continue
			}

			rct.cleanup(ctx, oldSnapshot, newSnapshot)
			oldSnapshot = newSnapshot
		}
	}
}

func main() {
	redisAddr := flag.String("redis-dsn", "", "Redis server address")

	ttlFlag := flag.Duration("ttl", 1*time.Minute, "TTL duration for traces (e.g. '24h', '30m')")
	syntheticTtlFlag := flag.Duration("synthetic-ttl", 30*time.Second, "TTL duration for synthetic traces (e.g. '24h', '30m')")
	completedTtlFlag := flag.Duration("completed-ttl", 30*time.Second, "TTL duration for completed traces (e.g. '24h', '30m')")
	channelName := flag.String("channel", "new_trace", "Redis Pub/Sub channel name for new hash keys")

	// Retry configuration
	maxRetries := flag.Int("max-retries", 3, "Maximum number of retries for cleanup")
	baseDelay := flag.Duration("base-delay", 1*time.Second, "Initial delay for exponential backoff")
	maxDelay := flag.Duration("max-delay", 30*time.Second, "Maximum delay for exponential backoff")

	// Redis cleanup tool configuration
	cleanupIntervalFlag := flag.Duration("cleanup-interval", 30*time.Minute, "Interval between Redis cleanup runs")
	enableCleanup := flag.Bool("enable-cleanup", false, "Enable automatic Redis cleanup of stalled data")

	flag.Parse()

	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	// ctx for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// sigterm handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, canceling context...")
		cancel()
	}()

	// Init redis connection
	redisOptions, err := redis.ParseURL(*redisAddr)
	if err != nil {
		logger.WithError(err).Fatal("Failed to parse Redis DSN")
	}
	redisClient := redis.NewClient(redisOptions)

	// Subscribe to the Pub/Sub channel
	sub := redisClient.Subscribe(ctx, *channelName)
	pubsubChan := sub.Channel()

	refaccsSub := redisClient.Subscribe(ctx, "referenced_accounts")
	refaccsChan := refaccsSub.Channel()

	// Start TTL tracker
	ttlTracker := NewRedisTTLTracker(
		redisClient,
		pubsubChan,
		refaccsChan,
		*ttlFlag,
		*syntheticTtlFlag,
		*completedTtlFlag,
		logger,
		*maxRetries,
		*baseDelay,
		*maxDelay,
	)
	ttlTracker.Run(ctx)

	// Start Redis cleanup tool if enabled
	if *enableCleanup {
		logger.WithField("interval", cleanupIntervalFlag.String()).
			Info("Initializing Redis cleanup tool")
		cleanupTool := NewRedisCleanupTool(
			redisClient,
			logger,
			*cleanupIntervalFlag,
			ttlTracker.expiryTracker,
		)
		go cleanupTool.Run(ctx)
	}

	<-ctx.Done()

	// Cleanup
	if err := sub.Close(); err != nil {
		logger.WithError(err).Error("Error closing subscription")
	}

	// Stop the TTL tracker's goroutines
	ttlTracker.Stop()
	logger.Info("Shutdown complete.")
}
