package v2

import (
	"context"
	"testing"
	"time"
)

func TestKeyedWorkerPoolPrioritizesCommittedWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	releaseFirst := make(chan struct{})
	processed := make(chan string, 3)

	pool := newKeyedWorkerPool(ctx, "test", 1, 4,
		func(_ context.Context, job string) {
			if job == "running" {
				close(started)
				<-releaseFirst
			}
			processed <- job
		})

	pool.Enqueue(ctx, "trace", "running", normalPriority, "running")
	<-started
	pool.Enqueue(ctx, "trace", "pending", normalPriority, "pending")
	pool.Enqueue(ctx, "trace", "snapshot", finalizedPriority, "finalized")
	close(releaseFirst)

	assertNextJob(t, processed, "running")
	assertNextJob(t, processed, "finalized")
	assertNextJob(t, processed, "pending")
}

func TestKeyedWorkerPoolKeepsLatestPendingWorkWhenQueueIsFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	releaseFirst := make(chan struct{})
	processed := make(chan string, 2)

	pool := newKeyedWorkerPool(ctx, "test", 1, 1,
		func(_ context.Context, job string) {
			if job == "running" {
				close(started)
				<-releaseFirst
			}
			processed <- job
		})

	pool.Enqueue(ctx, "trace", "running", normalPriority, "running")
	<-started
	pool.Enqueue(ctx, "trace", "pending", normalPriority, "old-pending")
	pool.Enqueue(ctx, "trace", "pending", normalPriority, "latest-pending")
	close(releaseFirst)

	assertNextJob(t, processed, "running")
	assertNextJob(t, processed, "latest-pending")
}

func TestKeyedWorkerPoolReplacesWaitingConfirmedWithFinalized(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	releaseFirst := make(chan struct{})
	processed := make(chan string, 4)

	pool := newKeyedWorkerPool(ctx, "test", 1, 4,
		func(_ context.Context, job string) {
			if job == "running" {
				close(started)
				<-releaseFirst
			}
			processed <- job
		})

	pool.Enqueue(ctx, "trace", "running", normalPriority, "running")
	<-started
	pool.Enqueue(ctx, "trace", "snapshot", confirmedPriority, "confirmed")
	pool.Enqueue(ctx, "trace", "snapshot", finalizedPriority, "finalized")
	pool.Enqueue(ctx, "trace", "sentinel", normalPriority, "sentinel")
	close(releaseFirst)

	assertNextJob(t, processed, "running")
	assertNextJob(t, processed, "finalized")
	assertNextJob(t, processed, "sentinel")
}

func TestKeyedWorkerPoolDoesNotOverflowFinalizedQueueWhenPromoting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	releaseRunning := make(chan struct{})
	defer close(releaseRunning)

	pool := newKeyedWorkerPool(ctx, "test", 1, 1,
		func(_ context.Context, job string) {
			if job == "running" {
				close(started)
				<-releaseRunning
			}
		})

	pool.Enqueue(ctx, "trace", "running", normalPriority, "running")
	<-started
	pool.Enqueue(ctx, "trace", "snapshot", confirmedPriority, "confirmed")
	pool.Enqueue(ctx, "trace", "other-finalized", finalizedPriority, "other-finalized")

	canceledCtx, cancelEnqueue := context.WithCancel(ctx)
	cancelEnqueue()
	if pool.Enqueue(canceledCtx, "trace", "snapshot", finalizedPriority, "finalized") {
		t.Fatal("promotion must wait when the finalized queue is full")
	}

	worker := &pool.workers[0]
	worker.mu.Lock()
	finalizedCount := len(worker.order[finalizedPriority])
	confirmedSnapshot := worker.jobs[confirmedPriority]["snapshot"]
	worker.mu.Unlock()

	if finalizedCount != 1 {
		t.Fatalf("finalized queue exceeded its capacity: got %d jobs, want 1", finalizedCount)
	}
	if confirmedSnapshot != "confirmed" {
		t.Fatalf("waiting confirmed snapshot was changed: got %q", confirmedSnapshot)
	}
}

func assertNextJob(t *testing.T, processed <-chan string, expected string) {
	t.Helper()

	select {
	case actual := <-processed:
		if actual != expected {
			t.Fatalf("expected %q, got %q", expected, actual)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %q", expected)
	}
}
