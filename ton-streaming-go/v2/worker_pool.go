package v2

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
)

type jobPriority uint8

const (
	normalPriority jobPriority = iota
	confirmedPriority
	finalizedPriority
	priorityCount
)

func (priority jobPriority) String() string {
	switch priority {
	case normalPriority:
		return "pending"
	case confirmedPriority:
		return "confirmed"
	case finalizedPriority:
		return "finalized"
	default:
		return "unknown"
	}
}

type keyedWorker[T any] struct {
	mu       sync.Mutex
	capacity int
	order    [priorityCount][]string
	jobs     [priorityCount]map[string]T
	wake     chan struct{}
	space    chan struct{}
}

// keyedWorkerPool has a fixed number of workers and bounded queues. workerKey
// chooses the worker; jobKey identifies an update which may be replaced while
// it is still waiting. A job already being processed is never interrupted.
type keyedWorkerPool[T any] struct {
	name      string
	workers   []keyedWorker[T]
	coalesced atomic.Uint64
	dropped   atomic.Uint64
}

func newKeyedWorkerPool[T any](ctx context.Context, name string, workerCount int, queueSizePerPriority int,
	process func(context.Context, T)) *keyedWorkerPool[T] {
	if workerCount <= 0 {
		panic("worker count must be positive")
	}
	if queueSizePerPriority <= 0 {
		panic("worker queue size must be positive")
	}

	pool := &keyedWorkerPool[T]{
		name:    name,
		workers: make([]keyedWorker[T], workerCount),
	}
	for i := range pool.workers {
		worker := &pool.workers[i]
		worker.capacity = queueSizePerPriority
		worker.wake = make(chan struct{}, 1)
		worker.space = make(chan struct{}, 1)
		for priority := jobPriority(0); priority < priorityCount; priority++ {
			worker.jobs[priority] = make(map[string]T)
		}
		go pool.work(ctx, worker, process)
	}
	return pool
}

func (pool *keyedWorkerPool[T]) Enqueue(ctx context.Context, workerKey string, jobKey string, priority jobPriority, job T) bool {
	if priority >= priorityCount {
		panic("unknown worker priority")
	}
	workerIndex := pool.workerIndexFor(workerKey)
	worker := &pool.workers[workerIndex]
	waitingForSpaceLogged := false

	for {
		worker.mu.Lock()
		if pool.replaceQueuedJob(worker, jobKey, priority, job) {
			worker.mu.Unlock()
			pool.recordCoalesced()
			pool.logCommittedQueueEvent("coalesced", workerKey, workerIndex, jobKey, priority)
			return true
		}
		if len(worker.order[priority]) < worker.capacity {
			worker.order[priority] = append(worker.order[priority], jobKey)
			worker.jobs[priority][jobKey] = job
			worker.mu.Unlock()
			signal(worker.wake)
			return true
		}
		worker.mu.Unlock()

		if priority != finalizedPriority {
			pool.recordDropped()
			pool.logCommittedQueueEvent("dropped", workerKey, workerIndex, jobKey, priority)
			return true
		}

		// Finalized work is not dropped. Waiting here can only be caused by a
		// full finalized queue; pending and confirmed queues never block it.
		if !waitingForSpaceLogged {
			pool.logCommittedQueueEvent("waiting_for_space", workerKey, workerIndex, jobKey, priority)
			waitingForSpaceLogged = true
		}
		select {
		case <-ctx.Done():
			return false
		case <-worker.space:
		}
	}
}

func (pool *keyedWorkerPool[T]) workerIndexFor(key string) int {
	return keyWorkerIndex(key, len(pool.workers))
}

func (pool *keyedWorkerPool[T]) replaceQueuedJob(worker *keyedWorker[T], jobKey string, priority jobPriority, job T) bool {
	for queuedPriority := jobPriority(0); queuedPriority < priorityCount; queuedPriority++ {
		if _, exists := worker.jobs[queuedPriority][jobKey]; !exists {
			continue
		}

		if priority < queuedPriority {
			// A late lower-finality notification cannot replace a more useful one.
			return true
		}
		if priority == queuedPriority {
			worker.jobs[queuedPriority][jobKey] = job
			return true
		}
		if len(worker.order[priority]) >= worker.capacity {
			// Let Enqueue apply the overflow policy of the target priority.
			return false
		}

		delete(worker.jobs[queuedPriority], jobKey)
		worker.order[queuedPriority] = removeQueuedKey(worker.order[queuedPriority], jobKey)
		worker.order[priority] = append(worker.order[priority], jobKey)
		worker.jobs[priority][jobKey] = job
		return true
	}
	return false
}

func removeQueuedKey(keys []string, key string) []string {
	for i, candidate := range keys {
		if candidate == key {
			return append(keys[:i], keys[i+1:]...)
		}
	}
	return keys
}

func (pool *keyedWorkerPool[T]) work(ctx context.Context, worker *keyedWorker[T], process func(context.Context, T)) {
	for {
		if job, ok := takeNextJob(worker); ok {
			process(ctx, job)
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-worker.wake:
		}
	}
}

func takeNextJob[T any](worker *keyedWorker[T]) (T, bool) {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	for priority := finalizedPriority; ; priority-- {
		if len(worker.order[priority]) > 0 {
			key := worker.order[priority][0]
			worker.order[priority] = worker.order[priority][1:]
			job := worker.jobs[priority][key]
			delete(worker.jobs[priority], key)
			signal(worker.space)
			return job, true
		}
		if priority == normalPriority {
			break
		}
	}
	var empty T
	return empty, false
}

func signal(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

func (pool *keyedWorkerPool[T]) recordCoalesced() {
	count := pool.coalesced.Add(1)
	pool.logOverloadCount("coalesced", count)
}

func (pool *keyedWorkerPool[T]) recordDropped() {
	count := pool.dropped.Add(1)
	pool.logOverloadCount("dropped", count)
}

func (pool *keyedWorkerPool[T]) logOverloadCount(action string, count uint64) {
	// Log exponentially to expose sustained overload without creating a hot path.
	if count&(count-1) == 0 {
		log.Printf("[v2] %s worker pool %s %d queued updates", pool.name, action, count)
	}
}

func (pool *keyedWorkerPool[T]) logCommittedQueueEvent(
	action string,
	workerKey string,
	workerIndex int,
	jobKey string,
	priority jobPriority,
) {
	if pool.name == "transactions" || pool.name == "actions" {
		log.Printf("[v2] external_message_hash_norm=%s stage=worker_queue_event stream=%s action=%s worker=%d job=%s priority=%s",
			workerKey, pool.name, action, workerIndex, jobKey, priority)
		return
	}
	log.Printf("[v2] key=%s stage=worker_queue_event pool=%s action=%s worker=%d job=%s priority=%s",
		workerKey, pool.name, action, workerIndex, jobKey, priority)
}

func keyWorkerIndex(key string, workerCount int) int {
	const (
		fnvOffset = uint32(2166136261)
		fnvPrime  = uint32(16777619)
	)
	hash := fnvOffset
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= fnvPrime
	}
	return int(hash % uint32(workerCount))
}
