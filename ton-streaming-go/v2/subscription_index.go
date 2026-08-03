package v2

import indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"

type clientSet map[string]struct{}

func addClientID(set clientSet, clientID string) {
	set[clientID] = struct{}{}
}

func (manager *ClientManager) addSubscriptionToIndexesLocked(clientID string, subscription *Subscription) {
	for eventType := range subscription.EventTypes {
		if eventType == EventTrace {
			if len(subscription.SubscribedTraces) == 0 {
				continue
			}
		} else if len(subscription.SubscribedAddresses) == 0 {
			continue
		}

		eventClients := manager.eventSubscribers[eventType]
		if eventClients == nil {
			eventClients = make(clientSet)
			manager.eventSubscribers[eventType] = eventClients
		}
		addClientID(eventClients, clientID)

		if eventType == EventTrace {
			for traceKey := range subscription.SubscribedTraces {
				traceClients := manager.traceSubscribers[traceKey]
				if traceClients == nil {
					traceClients = make(clientSet)
					manager.traceSubscribers[traceKey] = traceClients
				}
				addClientID(traceClients, clientID)
			}
			continue
		}

		eventAddresses := manager.addressSubscribers[eventType]
		if eventAddresses == nil {
			eventAddresses = make(map[indexModels.AccountAddress]clientSet)
			manager.addressSubscribers[eventType] = eventAddresses
		}
		for address := range subscription.SubscribedAddresses {
			addressClients := eventAddresses[address]
			if addressClients == nil {
				addressClients = make(clientSet)
				eventAddresses[address] = addressClients
			}
			addClientID(addressClients, clientID)
		}
	}
}

func (manager *ClientManager) removeSubscriptionFromIndexesLocked(clientID string, subscription *Subscription) {
	for eventType := range subscription.EventTypes {
		if eventClients := manager.eventSubscribers[eventType]; eventClients != nil {
			delete(eventClients, clientID)
			if len(eventClients) == 0 {
				delete(manager.eventSubscribers, eventType)
			}
		}

		if eventType == EventTrace {
			for traceKey := range subscription.SubscribedTraces {
				traceClients := manager.traceSubscribers[traceKey]
				delete(traceClients, clientID)
				if len(traceClients) == 0 {
					delete(manager.traceSubscribers, traceKey)
				}
			}
			continue
		}

		eventAddresses := manager.addressSubscribers[eventType]
		for address := range subscription.SubscribedAddresses {
			addressClients := eventAddresses[address]
			delete(addressClients, clientID)
			if len(addressClients) == 0 {
				delete(eventAddresses, address)
			}
		}
		if len(eventAddresses) == 0 {
			delete(manager.addressSubscribers, eventType)
		}
	}
}

// updateSubscription changes a live client's subscription and its reverse
// indexes under one lock, so a hint can never observe a half-updated route.
func (manager *ClientManager) updateSubscription(client *Client, update func(*Subscription) error) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	client.mu.Lock()
	defer client.mu.Unlock()

	if !client.Connected {
		return nil
	}

	manager.removeSubscriptionFromIndexesLocked(client.ID, &client.Subscription)
	err := update(&client.Subscription)
	manager.addSubscriptionToIndexesLocked(client.ID, &client.Subscription)
	return err
}

func (manager *ClientManager) subscribersForAddresses(eventType EventType, addresses []indexModels.AccountAddress,
	finality indexModels.FinalityState) clientSet {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	candidates := make(clientSet)
	eventAddresses := manager.addressSubscribers[eventType]
	for _, address := range addresses {
		for clientID := range eventAddresses[address] {
			candidates[clientID] = struct{}{}
		}
	}
	return manager.filterConnectedClientsLocked(candidates, &finality)
}

func (manager *ClientManager) subscribersForTrace(traceKey indexModels.HashType, finality indexModels.FinalityState) clientSet {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	return manager.filterConnectedClientsLocked(manager.traceSubscribers[traceKey], &finality)
}

func (manager *ClientManager) subscribersForActionRoutes(routes []actionRoute, finality indexModels.FinalityState) clientSet {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	result := make(clientSet)
	eventAddresses := manager.addressSubscribers[EventActions]
	for _, route := range routes {
		for _, address := range route.Accounts {
			for clientID := range eventAddresses[address] {
				if _, found := result[clientID]; found {
					continue
				}
				client := manager.clients[clientID]
				if client == nil {
					continue
				}
				client.mu.Lock()
				eligible := client.Connected && client.Subscription.MinFinality <= finality &&
					subscriptionPotentiallyAcceptsActionType(&client.Subscription, route.Type)
				client.mu.Unlock()
				if eligible {
					result[clientID] = struct{}{}
				}
			}
		}
	}
	return result
}

func subscriptionPotentiallyAcceptsActionType(subscription *Subscription, actionType string) bool {
	if len(subscription.ActionTypes) != 0 && !containsPossibleParsedActionType(subscription.ActionTypes, actionType) {
		return false
	}
	// Live subscriptions always receive a default supported-actions set. Treat
	// an empty set conservatively so internal callers cannot create a false negative.
	return len(subscription.SupportedActionTypes) == 0 ||
		containsPossibleParsedActionType(subscription.SupportedActionTypes, actionType)
}

func containsPossibleParsedActionType(values []string, rawType string) bool {
	for _, value := range values {
		if value == rawType {
			return true
		}
		// ParseRawAction exposes either raw type as extra_currency_transfer when
		// the action contains extra currencies. The hint intentionally stays small,
		// so both results remain possible here.
		if value == "extra_currency_transfer" && (rawType == "ton_transfer" || rawType == "call_contract") {
			return true
		}
	}
	return false
}

func (manager *ClientManager) hasEventSubscribers(eventType EventType) bool {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	return len(manager.filterConnectedClientsLocked(manager.eventSubscribers[eventType], nil)) > 0
}

func (manager *ClientManager) subscribersForEvent(eventType EventType, finality indexModels.FinalityState) clientSet {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	return manager.filterConnectedClientsLocked(manager.eventSubscribers[eventType], &finality)
}

func (manager *ClientManager) hasTraceSubscribers(traceKey indexModels.HashType) bool {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	return len(manager.filterConnectedClientsLocked(manager.traceSubscribers[traceKey], nil)) > 0
}

func (manager *ClientManager) enrichmentNeeds(targets clientSet) (bool, bool) {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	var addressBook bool
	var metadata bool
	for clientID := range targets {
		client := manager.clients[clientID]
		if client == nil {
			continue
		}
		client.mu.Lock()
		if client.Connected {
			addressBook = addressBook || client.Subscription.IncludeAddressBook
			metadata = metadata || client.Subscription.IncludeMetadata
		}
		client.mu.Unlock()
		if addressBook && metadata {
			break
		}
	}
	return addressBook, metadata
}

func (manager *ClientManager) filterConnectedClientsLocked(candidates clientSet, finality *indexModels.FinalityState) clientSet {
	result := make(clientSet)
	for clientID := range candidates {
		client := manager.clients[clientID]
		if client == nil {
			continue
		}
		client.mu.Lock()
		eligible := client.Connected && (finality == nil || client.Subscription.MinFinality <= *finality)
		client.mu.Unlock()
		if eligible {
			result[clientID] = struct{}{}
		}
	}
	return result
}

func mergeClientSets(destination clientSet, source clientSet) {
	for clientID := range source {
		destination[clientID] = struct{}{}
	}
}
