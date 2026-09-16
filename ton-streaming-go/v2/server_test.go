package v2

import (
	"testing"

	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

func TestHasActionHintSubscribers(t *testing.T) {
	const (
		traceKey     = indexModels.HashType("trace")
		anotherTrace = indexModels.HashType("another-trace")
		account      = indexModels.AccountAddress("account")
		otherAccount = indexModels.AccountAddress("other-account")
	)
	actionHint := actionsHint{
		TraceKey:       traceKey,
		UpdateSeq:      1,
		UpdateFinality: indexModels.FinalityStateConfirmed,
		TraceFinality:  indexModels.FinalityStatePending,
		ActionsUpdated: true,
		ActionTypesAndAccounts: []actionRoute{{
			Type:     "ton_transfer",
			Accounts: []indexModels.AccountAddress{account},
		}},
	}

	t.Run("no clients", func(t *testing.T) {
		manager := NewClientManager()

		if hasActionHintSubscribers(manager, actionHint) {
			t.Fatal("action hint must be skipped without clients")
		}
	})

	t.Run("matching actions subscriber", func(t *testing.T) {
		manager := NewClientManager()
		addTestClient(manager, &Client{
			ID:        "actions",
			Connected: true,
			Subscription: Subscription{
				SubscribedAddresses: AddressSet{account: {}},
				EventTypes:          makeEventSet([]EventType{EventActions}),
			},
		})

		if !hasActionHintSubscribers(manager, actionHint) {
			t.Fatal("action hint is needed for a matching actions subscriber")
		}
	})

	t.Run("unrelated actions subscriber", func(t *testing.T) {
		manager := NewClientManager()
		addTestClient(manager, &Client{
			ID:        "actions",
			Connected: true,
			Subscription: Subscription{
				SubscribedAddresses: AddressSet{otherAccount: {}},
				EventTypes:          makeEventSet([]EventType{EventActions}),
			},
		})

		if hasActionHintSubscribers(manager, actionHint) {
			t.Fatal("action hint must be skipped when no route matches the subscribed address")
		}
	})

	t.Run("type filter", func(t *testing.T) {
		manager := NewClientManager()
		addTestClient(manager, &Client{
			ID:        "actions",
			Connected: true,
			Subscription: Subscription{
				SubscribedAddresses: AddressSet{account: {}},
				EventTypes:          makeEventSet([]EventType{EventActions}),
				ActionTypes:         []string{"jetton_transfer"},
			},
		})

		if hasActionHintSubscribers(manager, actionHint) {
			t.Fatal("action hint must be skipped when the requested action type cannot match")
		}
	})

	t.Run("extra currency type conversion", func(t *testing.T) {
		manager := NewClientManager()
		addTestClient(manager, &Client{
			ID:        "actions",
			Connected: true,
			Subscription: Subscription{
				SubscribedAddresses: AddressSet{account: {}},
				EventTypes:          makeEventSet([]EventType{EventActions}),
				ActionTypes:         []string{"extra_currency_transfer"},
			},
		})

		if !hasActionHintSubscribers(manager, actionHint) {
			t.Fatal("ton_transfer hint may become extra_currency_transfer after parsing")
		}
	})

	t.Run("matching trace subscriber", func(t *testing.T) {
		manager := NewClientManager()
		addTestClient(manager, &Client{
			ID:        "trace",
			Connected: true,
			Subscription: Subscription{
				SubscribedTraces: TraceSet{traceKey: {}},
				EventTypes:       makeEventSet([]EventType{EventTrace}),
			},
		})

		failedHint := actionHint
		failedHint.ActionsUpdated = false
		failedHint.ActionTypesAndAccounts = nil
		if !hasActionHintSubscribers(manager, failedHint) {
			t.Fatal("failed classification is still needed for a matching trace subscriber")
		}
		failedHint.TraceKey = anotherTrace
		if hasActionHintSubscribers(manager, failedHint) {
			t.Fatal("unsubscribed trace must be skipped")
		}
	})

	t.Run("unrelated or inactive clients", func(t *testing.T) {
		manager := NewClientManager()
		addTestClient(manager, &Client{
			ID:        "transactions",
			Connected: true,
			Subscription: Subscription{
				SubscribedAddresses: AddressSet{account: {}},
				EventTypes:          makeEventSet([]EventType{EventTransactions}),
			},
		})
		addTestClient(manager, &Client{
			ID:        "disconnected-actions",
			Connected: false,
			Subscription: Subscription{
				SubscribedAddresses: AddressSet{account: {}},
				EventTypes:          makeEventSet([]EventType{EventActions}),
			},
		})
		addTestClient(manager, &Client{
			ID:        "actions-without-addresses",
			Connected: true,
			Subscription: Subscription{
				SubscribedAddresses: make(AddressSet),
				EventTypes:          makeEventSet([]EventType{EventActions}),
			},
		})

		if hasActionHintSubscribers(manager, actionHint) {
			t.Fatal("action hint must be skipped for unrelated or disconnected clients")
		}
	})
}

func TestTraceNotificationPreservesEmptyActions(t *testing.T) {
	emptyActions := make([]*indexModels.Action, 0)
	notification := &TraceNotification{
		Type:                  EventTrace,
		Finality:              indexModels.FinalityStateConfirmed,
		TraceExternalHashNorm: "trace",
		Actions:               &emptyActions,
	}
	client := &Client{
		Connected: true,
		Subscription: Subscription{
			MinFinality:      indexModels.FinalityStateConfirmed,
			SubscribedTraces: TraceSet{"trace": {}},
			EventTypes:       makeEventSet([]EventType{EventTrace}),
		},
		TracesForPotentialInvalidation: make(map[indexModels.HashType]bool),
	}

	adjusted, ok := notification.AdjustForClient(client).(*TraceNotification)
	if !ok {
		t.Fatal("trace notification was unexpectedly filtered out")
	}
	if adjusted.Actions == nil || len(*adjusted.Actions) != 0 {
		t.Fatalf("expected an explicit empty actions list, got %#v", adjusted.Actions)
	}
}

func addTestClient(manager *ClientManager, client *Client) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	manager.clients[client.ID] = client
	manager.addSubscriptionToIndexesLocked(client.ID, &client.Subscription)
}
