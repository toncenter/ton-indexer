package main

import (
	"container/heap"
	"context"
	"errors"
	"flag"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

type hash [32]byte

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

func (rtt *RedisTTLTracker) setupTraceTtl(ctx context.Context, hashKey string, data map[string]TraceNode) {
	ttl := rtt.defaultTraceTtl
	if data != nil {
		if node, exists := data[hashKey]; exists && node.Emulated {
			ttl = rtt.syntheticTraceTtl
		} else {
			emulated := 0
			nonEmulated := 0
			for _, node := range data {
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

func (rtt *RedisTTLTracker) readTrace(ctx context.Context, hashKey string) (map[string]TraceNode, error) {
	traceData, err := rtt.redisClient.HGetAll(ctx, hashKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get hash for key %s: %w", hashKey, err)
	}
	nodes := make(map[string]TraceNode)
	for key, value := range traceData {
		if strings.Contains(key, ":") || strings.Contains(key, "actions") {
			continue
		}
		var node TraceNode
		err := msgpack.Unmarshal([]byte(value), &node)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal node for key %s: %w", key, err)
		}
		nodes[key] = node
	}
	return nodes, nil
}

// handleTraceEmulationStatus checks if trace is synthetic and adds it to tracker, otherwise cleanup all synthetic traces for root tx account
func (rtt *RedisTTLTracker) handleTraceEmulationStatus(ctx context.Context, hashKey string, data map[string]TraceNode) error {
	rootTxNode, exists := data[hashKey]
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

func (rtt *RedisTTLTracker) collectAndMergeAddresses(ctx context.Context, hashKey string, data map[string]TraceNode) error {
	var addresses []string

	for key := range data {
		node := data[key]
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

// containsColon checks if a string contains a ':' character (your scenario for addresses).
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

func main() {
	redisAddr := flag.String("redis-dsn", "", "Redis server address")

	ttlFlag := flag.Duration("ttl", 5*time.Minute, "TTL duration for traces (e.g. '24h', '30m')")
	syntheticTtlFlag := flag.Duration("synthetic-ttl", 9000*time.Second, "TTL duration for synthetic traces (e.g. '24h', '30m')")
	completedTtlFlag := flag.Duration("completed-ttl", 9000*time.Second, "TTL duration for completed traces (e.g. '24h', '30m')")
	channelName := flag.String("channel", "new_trace", "Redis Pub/Sub channel name for new hash keys")

	// Retry configuration
	maxRetries := flag.Int("max-retries", 3, "Maximum number of retries for cleanup")
	baseDelay := flag.Duration("base-delay", 1*time.Second, "Initial delay for exponential backoff")
	maxDelay := flag.Duration("max-delay", 30*time.Second, "Maximum delay for exponential backoff")

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
	<-ctx.Done()

	// Cleanup
	if err := sub.Close(); err != nil {
		logger.WithError(err).Error("Error closing subscription")
	}

	// Stop the TTL tracker's goroutines
	ttlTracker.Stop()
	logger.Info("Shutdown complete.")
}
