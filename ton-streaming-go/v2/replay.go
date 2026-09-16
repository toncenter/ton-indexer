package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

type deliveryVersion struct {
	seq     uint64
	partial bool // pending-tail notifications are subsets of full snapshots
}

type clientMessage struct {
	data    []byte
	replay  bool          // replay messages use a write deadline on WebSocket
	flushed chan struct{} // internal marker, acknowledged after preceding transport writes
}

type notificationKey struct {
	kind     EventType
	entity   string
	finality indexModels.FinalityState // account states have independent confirmed/finalized keys
}

const maxReplayBufferBytes = 16 << 20
const maxReplayBufferEvents = 4096

// After replay, only the finite set of snapshot identities remains. Live
// delivery may advance these watermarks but cannot add new identities.
type replaySession struct {
	manager      *ClientManager
	client       *Client
	ctx          context.Context
	cancel       context.CancelFunc
	active       bool
	again        bool
	pending      []Notification // all mutable fields below are guarded by client.mu
	pendingBytes int
	snapshots    map[notificationKey]deliveryVersion
}

// Repeated requests share one worker. The next pass reads the then-current
// subscription; there is no saved subscription or per-request task queue.
func (manager *ClientManager) requestReplayLocked(c *Client) *replaySession {
	if c.replay != nil && c.replay.active {
		c.replay.again = true
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	s := &replaySession{manager: manager, client: c, ctx: ctx, cancel: cancel, active: true,
		snapshots: make(map[notificationKey]deliveryVersion)}
	c.replay = s
	return s
}

func notificationIdentity(n Notification) (notificationKey, deliveryVersion) {
	switch n := n.(type) {
	case *TransactionsNotification:
		return notificationKey{kind: EventTransactions, entity: string(n.TraceExternalHashNorm)}, n.version
	case *ActionsNotification:
		return notificationKey{kind: EventActions, entity: string(n.TraceExternalHashNorm)}, n.version
	case *TraceNotification:
		return notificationKey{kind: EventTrace, entity: string(n.TraceExternalHashNorm)}, n.version
	case *AccountStateNotification:
		return notificationKey{EventAccountStateChange, string(n.Account), n.Finality}, n.version
	case *JettonsNotification:
		return notificationKey{EventJettonsChange, string(n.Jetton.Address), n.Finality}, n.version
	}
	return notificationKey{}, deliveryVersion{}
}

// Check only identities discovered by replay. An unrelated live event leaves
// no history behind. Both account finalities belong to the same initial entity.
func (s *replaySession) accept(n Notification, snapshot bool) bool {
	if invalidation, ok := n.(*TraceInvalidatedNotification); ok {
		// Apply invalidations in delivery order. A recreated trace is assumed
		// to follow its invalidation; delayed notices are not reconciled.
		for _, kind := range []EventType{EventTransactions, EventActions, EventTrace} {
			key := notificationKey{kind: kind, entity: string(invalidation.TraceExternalHashNorm)}
			if _, known := s.snapshots[key]; known {
				s.snapshots[key] = deliveryVersion{}
			}
		}
		return true
	}
	key, version := notificationIdentity(n)
	previous, known := s.snapshots[key]
	if snapshot && !known {
		s.snapshots[key] = previous
		if key.kind == EventAccountStateChange || key.kind == EventJettonsChange {
			other := key
			other.finality = indexModels.FinalityStateConfirmed
			if key.finality == other.finality {
				other.finality = indexModels.FinalityStateFinalized
			}
			if _, exists := s.snapshots[other]; !exists {
				s.snapshots[other] = deliveryVersion{}
			}
		}
		known = true
	}
	if !known {
		return true
	}
	if version.seq != 0 && previous.seq != 0 && (version.seq < previous.seq || !snapshot && version.seq == previous.seq && (!previous.partial || version.partial)) {
		return false
	}
	if (key.kind == EventAccountStateChange || key.kind == EventJettonsChange) && key.finality == indexModels.FinalityStateConfirmed {
		final := key
		final.finality = indexModels.FinalityStateFinalized
		if s.snapshots[final].seq >= version.seq {
			return false
		}
	}
	s.snapshots[key] = version
	return true
}

func (s *replaySession) buffer(n Notification) error {
	data, err := json.Marshal(n)
	if err != nil {
		return err
	}
	if len(s.pending) >= maxReplayBufferEvents || s.pendingBytes+len(data) > maxReplayBufferBytes {
		return fmt.Errorf("%w: live buffer overflow during replay", errSlowConsumer)
	}
	s.pending = append(s.pending, n)
	s.pendingBytes += len(data)
	return nil
}

func (s *replaySession) send(n Notification, snapshot bool) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	c := s.client
	c.mu.Lock()
	if !c.Connected {
		c.mu.Unlock()
		return context.Canceled
	}
	var data []byte
	var err error
	if s.accept(n, snapshot) {
		if event := n.AdjustForClient(c); event != nil {
			data, err = json.Marshal(event)
		}
	}
	c.mu.Unlock()
	if err != nil || data == nil {
		return err
	}
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-c.done:
		return context.Canceled
	case c.sendChan <- clientMessage{data: data, replay: true}:
		return nil
	}
}

func (s *replaySession) run(existing bool) {
	defer s.cancel()
	var err error
	for err == nil {
		if err = s.ctx.Err(); err != nil {
			break
		}
		if existing {
			s.client.mu.Lock()
			s.again = false
			s.client.mu.Unlock()
			if err = s.load(); err != nil {
				break
			}
			existing = false
		}
		s.client.mu.Lock()
		if !s.client.Connected {
			s.client.mu.Unlock()
			return
		}
		if s.again {
			existing = true
			s.client.mu.Unlock()
			continue
		}
		pending := s.pending
		s.pending = nil
		s.pendingBytes = 0
		if len(pending) == 0 {
			s.client.mu.Unlock()
			if err = s.waitForDelivery(); err != nil {
				break
			}
			s.client.mu.Lock()
			if !s.client.Connected {
				s.client.mu.Unlock()
				return
			}
			// Live events and another replay request may arrive while the
			// sender drains. Switch modes atomically with this final check.
			if len(s.pending) != 0 || s.again {
				s.client.mu.Unlock()
				continue
			}
			s.active = false
			if len(s.snapshots) == 0 {
				s.client.replay = nil
			}
			s.client.mu.Unlock()
			return
		}
		s.client.mu.Unlock()
		for _, n := range pending {
			if err = s.send(n, false); err != nil {
				break
			}
		}
	}
	s.client.mu.Lock()
	current := s.client.Connected && s.client.replay == s
	s.client.mu.Unlock()
	if current {
		disconnectClient(s.manager, s.client, fmt.Errorf("replay failed: %w", err))
	}
}

// Do not hold client.mu while waiting: both the sender and live buffering
// must keep running. Each transport writer acknowledges this marker in order.
func (s *replaySession) waitForDelivery() error {
	flushed := make(chan struct{})
	select {
	case s.client.sendChan <- clientMessage{flushed: flushed}:
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-s.client.done:
		return context.Canceled
	}
	select {
	case <-flushed:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-s.client.done:
		return context.Canceled
	}
}
