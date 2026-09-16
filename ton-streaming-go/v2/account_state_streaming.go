package v2

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
	"github.com/toncenter/ton-indexer/ton-emulate-go/models"
	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/vmihailenco/msgpack/v5"
)

func SubscribeToAccountStateHints(ctx context.Context, rdb *redis.Client, manager *ClientManager, channel string) {
	pubsub := rdb.Subscribe(ctx, channel)
	defer pubsub.Close()

	log.Printf("[v2] Subscribed to Redis channel (account state hints): %s", channel)

	pool := newKeyedWorkerPool(ctx, "account states", streamingWorkerCount, streamingQueueSizePerWorker,
		func(ctx context.Context, hint accountStateHint) {
			ProcessAccountStateHint(ctx, rdb, hint, manager)
		})

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[v2] Error receiving account state hint: %v", err)
			continue
		}

		hint, err := decodeAccountStateHint(msg.Payload)
		if err != nil {
			log.Printf("[v2] Invalid account state hint: %v", err)
			continue
		}
		if !hasAccountStateSubscribers(manager, hint) {
			continue
		}
		if !pool.Enqueue(ctx, hint.Account.String(), hint.jobKey(), hint.priority(), hint) {
			return
		}
	}
}

func hasAccountStateSubscribers(manager *ClientManager, hint accountStateHint) bool {
	stateTargets := manager.subscribersForAddresses(EventAccountStateChange, []indexModels.AccountAddress{hint.Account}, hint.Finality)
	jettonTargets := manager.subscribersForEvent(EventJettonsChange, hint.Finality)
	return len(stateTargets) > 0 || len(jettonTargets) > 0
}

func ProcessAccountStateHint(ctx context.Context, rdb *redis.Client, hint accountStateHint, manager *ClientManager) {
	if !hasAccountStateSubscribers(manager, hint) {
		return
	}

	key := accountStateRedisKey(hint)
	rawState, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		log.Printf("[v2] Error fetching account state for %s: %v", hint.Account, err)
		return
	}
	if err := validateAccountStateHintVersion(rawState, hint.Lt); err != nil {
		if !errors.Is(err, errStaleStreamingHint) {
			log.Printf("[v2] Account state hint version mismatch for %s: %v", hint.Account, err)
		}
		return
	}

	withJettons := len(manager.subscribersForEvent(EventJettonsChange, hint.Finality)) > 0
	state, jetton, err := buildAccountNotifications(rawState, hint, withJettons)
	if err != nil {
		log.Printf("[v2] Error unmarshalling account state for %s: %v", hint.Account, err)
		return
	}
	targets := manager.subscribersForAddresses(EventAccountStateChange, []indexModels.AccountAddress{hint.Account}, hint.Finality)
	manager.sendNotification(state, targets)
	if jetton == nil {
		return
	}
	targets = manager.subscribersForAddresses(EventJettonsChange, []indexModels.AccountAddress{jetton.Jetton.Address, jetton.Jetton.Owner}, hint.Finality)
	book, metadata := manager.enrichmentNeeds(targets)
	enrichJettonNotification(ctx, jetton, book, metadata)
	manager.sendNotification(jetton, targets)
}

func buildAccountNotifications(raw map[string]string, hint accountStateHint, withJettons bool) (*AccountStateNotification, *JettonsNotification, error) {
	var accountState models.AccountState
	if err := msgpack.Unmarshal([]byte(raw["state"]), &accountState); err != nil {
		return nil, nil, err
	}
	version := deliveryVersion{seq: hint.Lt}
	state := &AccountStateNotification{version: version, Type: EventAccountStateChange, Finality: hint.Finality,
		Account: hint.Account, State: models.MsgPackAccountStateToIndexAccountState(accountState)}
	var jetton *JettonsNotification
	if withJettons {
		jetton = jettonNotificationFromAccountState(hint, accountState, raw["interfaces"])
		if jetton != nil {
			jetton.version = version
		}
	}
	return state, jetton, nil
}

func enrichJettonNotification(ctx context.Context, n *JettonsNotification, book, metadata bool) {
	if !book && !metadata {
		return
	}
	n.AddressBook, n.Metadata = fetchAddressBookAndMetadata(ctx,
		[]indexModels.AccountAddress{n.Jetton.Address, n.Jetton.Owner, n.Jetton.Jetton},
		[]indexModels.AccountAddress{n.Jetton.Owner, n.Jetton.Jetton}, book, metadata)
}

func accountStateRedisKey(hint accountStateHint) string {
	switch hint.Finality {
	case indexModels.FinalityStateConfirmed:
		return fmt.Sprintf("account_confirmed:%s", hint.Account)
	case indexModels.FinalityStateFinalized:
		return fmt.Sprintf("account_finalized:%s", hint.Account)
	default:
		panic("account state hint finality was not validated")
	}
}

func jettonNotificationFromAccountState(hint accountStateHint, accountState models.AccountState, interfacesData string) *JettonsNotification {
	if interfacesData == "" {
		return nil
	}

	var interfaces models.AddressInterfaces
	if err := msgpack.Unmarshal([]byte(interfacesData), &interfaces); err != nil {
		log.Printf("[v2] Error unmarshalling address interfaces for %s: %v", hint.Account, err)
		return nil
	}

	var notification *JettonsNotification
	for _, iface := range interfaces.Interfaces {
		wallet, ok := iface.Value.(*models.JettonWalletInterface)
		if !ok {
			continue
		}
		notification = &JettonsNotification{
			Type:     EventJettonsChange,
			Finality: hint.Finality,
			Jetton: MsgPackJettonWalletToModel(*wallet, int64(hint.Lt),
				models.ConvertHashToIndex(accountState.CodeHash), models.ConvertHashToIndex(accountState.DataHash)),
		}
	}
	return notification
}
