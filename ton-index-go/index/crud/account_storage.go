package crud

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ton-blockchain/tolk-abi-to-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/acton/catalog"
	"github.com/toncenter/ton-indexer/ton-index-go/index/actonapi"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

// A storage batch shares one decode budget, so its total work is bounded no
// matter how many accounts it names. A thousand ordinary accounts consume about
// 6,600 codec steps between them; a single adversarial cell can consume 8,000 on
// its own, so these ceilings separate the two by orders of magnitude while still
// letting a handful of expensive accounts through.
const maxStorageItems = 4 * tolkabi.MaxItems
const maxStorageDecodedBytes = 8 * tolkabi.MaxBOCBytes
const maxStorageBatchBytes = 8 << 20

// DecodeAccountStorage decodes each state's data cell in place with the catalog
// entry its code book names first. Accounts whose code the catalog does not know
// are left alone; a decode that fails leaves StorageError instead of Storage. The
// whole batch shares one work budget and one output budget, so naming more
// accounts buys an attacker no more decoding than naming one.
func DecodeAccountStorage(states []models.AccountStateFull) error {
	return decodeAccountStorage(states, catalog.ByCodeHash)
}

func decodeAccountStorage(states []models.AccountStateFull, lookup func(string) []*tolkabi.Contract) error {
	// Reject on raw input size first, so an oversized batch never reaches the
	// native decoder at all.
	remaining := maxStorageBatchBytes
	for i := range states {
		remaining -= bocLength(states[i].DataBoc) + bocLength(states[i].CodeBoc)
		if remaining < 0 {
			return fmt.Errorf("storage batch BOCs exceed the aggregate %d byte budget", maxStorageBatchBytes)
		}
	}
	budget := tolkabi.NewBudget(maxStorageItems, maxStorageDecodedBytes)
	for i := range states {
		state := &states[i]
		if state.CodeHash == nil {
			continue
		}
		contracts := actonapi.OrderCandidates(lookup(string(*state.CodeHash)))
		if len(contracts) == 0 || contracts[0].Storage == nil {
			continue
		}
		decoded, err := decodeStorage(budget, contracts[0].Storage, state.DataBoc)
		if errors.Is(err, tolkabi.ErrBudget) {
			return errors.New("storage batch exceeds the decode work budget; request fewer accounts")
		}
		if err != nil {
			state.StorageError = err.Error()
			continue
		}
		remaining -= len(decoded)
		if remaining < 0 {
			return fmt.Errorf("decoded storage exceeds the aggregate %d byte budget", maxStorageBatchBytes)
		}
		state.Storage = decoded
	}
	return nil
}

func decodeStorage(budget *tolkabi.Context, binding *tolkabi.Binding, boc *models.BytesType) (json.RawMessage, error) {
	switch {
	case boc == nil:
		return nil, errors.New("account data BOC unavailable")
	case binding.Unsupported != "":
		return nil, fmt.Errorf("unsupported storage: %s", binding.Unsupported)
	case binding.DecodeWith == nil:
		return nil, errors.New("native storage decoder unavailable")
	}
	root, err := tolkabi.DecodeBOC(string(*boc))
	if err != nil {
		return nil, err
	}
	value, err := binding.DecodeWith(budget, root)
	if err != nil {
		return nil, err
	}
	return json.Marshal(actonapi.CanonicalizeDecoded(value))
}

func bocLength(value *models.BytesType) int {
	if value == nil {
		return 0
	}
	return len(*value)
}
