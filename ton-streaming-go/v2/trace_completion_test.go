package v2

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	models "github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

func completionSubscription(key models.HashType, minimum models.FinalityState) Subscription {
	return Subscription{
		SubscribedAddresses:  AddressSet{replayAddress(1): {}, replayAddress(2): {}},
		SubscribedTraces:     TraceSet{key: {}},
		EventTypes:           makeEventSet([]EventType{EventTransactions, EventActions, EventTrace}),
		SupportedActionTypes: []string{"ton_transfer"},
		MinFinality:          minimum,
	}
}

func completionLiveNotifications(manager *ClientManager, key models.HashType, seq uint64) []Notification {
	manager.broadcast = make(chan notificationDelivery, 4)
	ProcessTransactionHint(context.Background(), manager.rdb, transactionHint{
		TraceKey: key, UpdateSeq: seq, UpdateFinality: models.FinalityStateFinalized,
		TraceFinality: models.FinalityStateFinalized, Accounts: []models.AccountAddress{replayAddress(1), replayAddress(2)},
	}, manager, "test")
	ProcessActionHint(context.Background(), manager.rdb, actionsHint{
		TraceKey: key, UpdateSeq: seq, UpdateFinality: models.FinalityStateFinalized,
		TraceFinality: models.FinalityStateFinalized, ActionsUpdated: true,
	}, manager, "test")
	var result []Notification
	for len(manager.broadcast) > 0 {
		result = append(result, (<-manager.broadcast).notification)
	}
	return result
}

func TestFinalizedStreamerCompletionFiltersLiveAndReplay(t *testing.T) {
	rdb := replayRedis(t)
	key, _, _ := seedReplayTrace(t, rdb, models.FinalityStateFinalized)
	for _, complete := range []string{"", "0", "1"} {
		for _, minimum := range []models.FinalityState{models.FinalityStatePending, models.FinalityStateConfirmed, models.FinalityStateFinalized} {
			t.Run("complete="+complete+"/min="+minimum.String(), func(t *testing.T) {
				if complete == "" {
					if err := rdb.HDel(context.Background(), key.String(), "trace_complete").Err(); err != nil {
						t.Fatal(err)
					}
				} else if err := rdb.HSet(context.Background(), key.String(), "trace_complete", complete).Err(); err != nil {
					t.Fatal(err)
				}
				manager := NewClientManager(rdb)
				client, session := replayClient(manager, completionSubscription(key, minimum))
				replayed := runReplay(t, session, true)
				want := 3
				if complete == "0" && minimum == models.FinalityStateFinalized {
					want = 0
				}
				if len(replayed) != want {
					t.Fatalf("replay: wanted %d events, got %v", want, replayed)
				}
				var live []map[string]any
				for _, n := range completionLiveNotifications(manager, key, 7) {
					if adjusted := n.AdjustForClient(client); adjusted != nil {
						data, err := json.Marshal(adjusted)
						if err != nil {
							t.Fatal(err)
						}
						var event map[string]any
						if err := json.Unmarshal(data, &event); err != nil {
							t.Fatal(err)
						}
						live = append(live, event)
					}
				}
				if !reflect.DeepEqual(replayed, live) {
					t.Fatalf("live/replay differ: replay=%v live=%v", replayed, live)
				}
				for _, event := range live {
					if event["finality"] != "finalized" {
						t.Fatalf("transaction finality was downgraded: %v", event)
					}
					if _, exposed := event["trace_complete"]; exposed {
						t.Fatal("internal completion metadata leaked into the API")
					}
				}
			})
		}
	}
}

func TestIncompleteReplayDoesNotSuppressLaterCompletedLiveSnapshot(t *testing.T) {
	rdb := replayRedis(t)
	key, _, _ := seedReplayTrace(t, rdb, models.FinalityStateFinalized)
	if err := rdb.HSet(context.Background(), key.String(), "trace_complete", "0").Err(); err != nil {
		t.Fatal(err)
	}
	manager := NewClientManager(rdb)
	client, session := replayClient(manager, completionSubscription(key, models.FinalityStateFinalized))
	if events := runReplay(t, session, true); len(events) != 0 {
		t.Fatalf("incomplete replay was delivered: %v", events)
	}
	deliver := func(seq uint64) {
		for _, n := range completionLiveNotifications(manager, key, seq) {
			client.mu.Lock()
			err := client.acceptNotificationLocked(n)
			client.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	deliver(7)
	if events := replayEvents(t, client); len(events) != 0 {
		t.Fatalf("incomplete live update was delivered: %v", events)
	}
	if err := rdb.HSet(context.Background(), key.String(), "trace_complete", "1", "update_seq", "8").Err(); err != nil {
		t.Fatal(err)
	}
	deliver(8)
	if events := replayEvents(t, client); len(events) != 3 {
		t.Fatalf("completed live update missing after incomplete replay: %v", events)
	}
	deliver(8)
	if events := replayEvents(t, client); len(events) != 0 {
		t.Fatalf("completed update was delivered twice: %v", events)
	}
}
