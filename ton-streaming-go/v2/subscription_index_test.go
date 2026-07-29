package v2

import (
	"testing"

	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

func TestSubscriptionIndexRoutesByAddressAndFinality(t *testing.T) {
	const (
		account = indexModels.AccountAddress("0:AAAA")
		trace   = indexModels.HashType("trace")
	)
	manager := NewClientManager()
	client := &Client{
		ID:        "client",
		Connected: true,
		Subscription: Subscription{
			SubscribedAddresses: AddressSet{account: {}},
			SubscribedTraces:    TraceSet{trace: {}},
			EventTypes: makeEventSet([]EventType{
				EventTransactions,
				EventTrace,
			}),
			MinFinality: indexModels.FinalityStateConfirmed,
		},
	}
	addTestClient(manager, client)

	pending := manager.subscribersForAddresses(EventTransactions, []indexModels.AccountAddress{account}, indexModels.FinalityStatePending)
	if len(pending) != 0 {
		t.Fatal("confirmed-only client must not receive pending transactions")
	}

	confirmed := manager.subscribersForAddresses(EventTransactions, []indexModels.AccountAddress{account}, indexModels.FinalityStateConfirmed)
	if _, ok := confirmed[client.ID]; !ok {
		t.Fatal("matching confirmed transaction subscriber was not found")
	}

	traceSubscribers := manager.subscribersForTrace(trace, indexModels.FinalityStateFinalized)
	if _, ok := traceSubscribers[client.ID]; !ok {
		t.Fatal("matching trace subscriber was not found")
	}
}

func TestUpdatingSubscriptionReplacesOldRoutes(t *testing.T) {
	const (
		oldAccount = indexModels.AccountAddress("0:AAAA")
		newAccount = indexModels.AccountAddress("0:BBBB")
	)
	manager := NewClientManager()
	client := &Client{
		ID:        "client",
		Connected: true,
		Subscription: Subscription{
			SubscribedAddresses: AddressSet{oldAccount: {}},
			EventTypes:          makeEventSet([]EventType{EventTransactions}),
		},
	}
	addTestClient(manager, client)

	err := manager.updateSubscription(client, func(subscription *Subscription) error {
		subscription.Replace([]indexModels.AccountAddress{newAccount}, []EventType{EventAccountStateChange})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	oldTargets := manager.subscribersForAddresses(EventTransactions, []indexModels.AccountAddress{oldAccount}, indexModels.FinalityStateFinalized)
	if len(oldTargets) != 0 {
		t.Fatal("old transaction route was not removed")
	}

	newTargets := manager.subscribersForAddresses(
		EventAccountStateChange, []indexModels.AccountAddress{newAccount}, indexModels.FinalityStateFinalized)
	if _, ok := newTargets[client.ID]; !ok {
		t.Fatal("new account-state route was not added")
	}
}

func TestUpdatingDisconnectedClientDoesNotRestoreRoutes(t *testing.T) {
	const (
		oldAccount = indexModels.AccountAddress("0:AAAA")
		newAccount = indexModels.AccountAddress("0:BBBB")
	)
	manager := NewClientManager()
	client := &Client{
		ID:        "client",
		Connected: true,
		Subscription: Subscription{
			SubscribedAddresses: AddressSet{oldAccount: {}},
			EventTypes:          makeEventSet([]EventType{EventTransactions}),
		},
	}
	addTestClient(manager, client)

	manager.mu.Lock()
	client.mu.Lock()
	client.Connected = false
	manager.removeSubscriptionFromIndexesLocked(client.ID, &client.Subscription)
	delete(manager.clients, client.ID)
	client.mu.Unlock()
	manager.mu.Unlock()

	updateCalled := false
	err := manager.updateSubscription(client, func(subscription *Subscription) error {
		updateCalled = true
		subscription.Replace([]indexModels.AccountAddress{newAccount}, []EventType{EventAccountStateChange})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if updateCalled {
		t.Fatal("subscription update was applied after the client disconnected")
	}

	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if len(manager.eventSubscribers) != 0 || len(manager.addressSubscribers) != 0 || len(manager.traceSubscribers) != 0 {
		t.Fatal("disconnected client was restored in subscription indexes")
	}
}
