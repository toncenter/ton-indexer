package v2

import (
	"testing"

	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

func TestNeedsClassifiedTrace(t *testing.T) {
	const (
		traceKey     = indexModels.HashType("trace")
		anotherTrace = indexModels.HashType("another-trace")
		account      = indexModels.AccountAddress("account")
	)

	t.Run("no clients", func(t *testing.T) {
		manager := NewClientManager()

		if manager.needsClassifiedTrace(traceKey) {
			t.Fatal("classified trace must be skipped without clients")
		}
	})

	t.Run("actions subscriber", func(t *testing.T) {
		manager := NewClientManager()
		addTestClient(manager, &Client{
			ID:        "actions",
			Connected: true,
			Subscription: Subscription{
				SubscribedAddresses: AddressSet{account: {}},
				EventTypes:          makeEventSet([]EventType{EventActions}),
			},
		})

		if !manager.needsClassifiedTrace(traceKey) {
			t.Fatal("classified trace is needed for an actions subscriber")
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

		if !manager.needsClassifiedTrace(traceKey) {
			t.Fatal("classified trace is needed for a matching trace subscriber")
		}
		if manager.needsClassifiedTrace(anotherTrace) {
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

		if manager.needsClassifiedTrace(traceKey) {
			t.Fatal("classified trace must be skipped for unrelated or disconnected clients")
		}
	})
}

func addTestClient(manager *ClientManager, client *Client) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	manager.clients[client.ID] = client
	manager.addSubscriptionToIndexesLocked(client.ID, &client.Subscription)
}
