package v2

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

// Replay reads the current Redis snapshots and uses the same notification
// builders as live delivery. It never subscribes another client or broadcasts.
func (s *replaySession) load() error {
	rdb := s.manager.rdb
	if rdb == nil {
		return fmt.Errorf("replay Redis is not configured")
	}
	// Copy only the keys needed for Redis I/O. Delivery always uses the live
	// subscription, which may change while these reads are in progress.
	traces := make(map[string]struct{})
	var indexes []string
	s.client.mu.Lock()
	for hash := range s.client.Subscription.SubscribedTraces {
		traces[string(hash)] = struct{}{}
	}
	for address := range s.client.Subscription.SubscribedAddresses {
		if _, ok := s.client.Subscription.EventTypes[EventTransactions]; ok {
			indexes = append(indexes, string(address))
		}
		if _, ok := s.client.Subscription.EventTypes[EventActions]; ok {
			indexes = append(indexes, "_aai:"+string(address))
		}
	}
	s.client.mu.Unlock()
	for _, index := range indexes {
		members, err := rdb.ZRange(s.ctx, index, 0, -1).Result()
		if err != nil {
			return err
		}
		for _, member := range members {
			if trace, _, ok := strings.Cut(member, ":"); ok {
				traces[trace] = struct{}{}
			}
		}
	}
	keys := make([]string, 0, len(traces))
	for key := range traces {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		started := time.Now().UnixNano()
		raw, err := rdb.HGetAll(s.ctx, key).Result()
		if err != nil {
			return err
		}
		if len(raw) == 0 {
			continue
		}
		seq, err := parseRedisVersion(raw["update_seq"], "update_seq")
		if err != nil {
			return fmt.Errorf("trace %s: %w", key, err)
		}
		// Read one Redis snapshot for every requested event type. Calculate its
		// finality before applying account or body-hash filters.
		if raw["streaming_actions_updated"] != "1" {
			delete(raw, "actions")
		}
		traceContext, err := decodeTraceSnapshot(raw, indexModels.HashType(key))
		if err != nil {
			return err
		}
		stage := NewTraceProcessingStage(started, rawTraceSpanName, raw, key, "replay")
		finality := indexModels.FinalityStateFinalized
		allPending := true
		for _, tx := range traceContext.GetTransactions() {
			allPending = allPending && tx.Finality == indexModels.FinalityStatePending
			if tx.Finality < finality {
				finality = tx.Finality
			}
		}
		s.client.mu.Lock()
		_, transactions := s.client.Subscription.EventTypes[EventTransactions]
		_, actions := s.client.Subscription.EventTypes[EventActions]
		trace := s.client.Subscription.InterestedInTrace(EventTrace, indexModels.HashType(key))
		minimum := s.client.Subscription.MinFinality
		book, metadata := s.client.Subscription.IncludeAddressBook, s.client.Subscription.IncludeMetadata
		s.client.mu.Unlock()
		if transactions {
			hint := transactionHint{TraceKey: indexModels.HashType(key), UpdateSeq: seq, UpdateFinality: indexModels.FinalityStateFinalized, TraceFinality: finality}
			if allPending {
				hint.UpdateFinality = indexModels.FinalityStatePending
			}
			n, addresses, err := buildTransactionNotification(traceContext, hint, stage)
			if err != nil {
				return err
			}
			if n != nil {
				if n.Finality >= minimum {
					enrichTraceNotification(s.ctx, n, addresses, book, metadata)
				}
				if err := s.send(n, true); err != nil {
					return err
				}
			}
		}
		if actions || trace {
			hint := actionsHint{TraceKey: indexModels.HashType(key), UpdateSeq: seq, TraceFinality: finality, ActionsUpdated: raw["streaming_actions_updated"] == "1"}
			n, addresses := buildActionNotification(traceContext, hint, stage)
			if actions {
				if n.Finality >= minimum {
					enrichTraceNotification(s.ctx, n, addresses, book, metadata)
				}
				if err := s.send(n, true); err != nil {
					return err
				}
			}
			if trace {
				notification, addresses, err := buildTraceNotification(traceContext, n)
				if err != nil {
					return err
				}
				if notification.Finality >= minimum {
					enrichTraceNotification(s.ctx, notification, addresses, book, metadata)
				}
				if err := s.send(notification, true); err != nil {
					return err
				}
			}
		}
		stage.Emit()
	}
	return s.loadAccounts(rdb)
}

func (s *replaySession) loadAccounts(rdb *redis.Client) error {
	var accounts []indexModels.AccountAddress
	s.client.mu.Lock()
	_, states := s.client.Subscription.EventTypes[EventAccountStateChange]
	_, jettons := s.client.Subscription.EventTypes[EventJettonsChange]
	if states || jettons {
		for address := range s.client.Subscription.SubscribedAddresses {
			accounts = append(accounts, address)
		}
	}
	s.client.mu.Unlock()
	for _, account := range accounts {
		s.client.mu.Lock()
		_, states = s.client.Subscription.EventTypes[EventAccountStateChange]
		_, jettons = s.client.Subscription.EventTypes[EventJettonsChange]
		minimum := s.client.Subscription.MinFinality
		book, metadata := s.client.Subscription.IncludeAddressBook, s.client.Subscription.IncludeMetadata
		s.client.mu.Unlock()
		var finalizedLT uint64
		for _, f := range []indexModels.FinalityState{indexModels.FinalityStateFinalized, indexModels.FinalityStateConfirmed} {
			if f < minimum {
				continue
			}
			hint := accountStateHint{Account: account, Finality: f}
			raw, err := rdb.HGetAll(s.ctx, accountStateRedisKey(hint)).Result()
			if err != nil {
				return err
			}
			if len(raw) == 0 {
				continue
			}
			hint.Lt, err = parseRedisVersion(raw["lt"], "lt")
			if err != nil {
				return err
			}
			if f == indexModels.FinalityStateFinalized {
				finalizedLT = hint.Lt
			} else if hint.Lt <= finalizedLT {
				continue
			}
			state, jetton, err := buildAccountNotifications(raw, hint, jettons)
			if err != nil {
				return err
			}
			if states {
				if err := s.send(state, true); err != nil {
					return err
				}
			}
			if jetton != nil {
				enrichJettonNotification(s.ctx, jetton, book, metadata)
				if err := s.send(jetton, true); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
