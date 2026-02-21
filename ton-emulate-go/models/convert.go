package models

import (
	"context"
	"fmt"
	"github.com/toncenter/ton-indexer/ton-index-go/index/crud"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"
	"log"
	"maps"
	"slices"
	"strconv"
	"time"

	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"

)

func convertHashToLocal(h *indexModels.HashType) *Hash {
	if h == nil {
		return nil
	}
	hash, err := FromBase64(string(*h))
	if err != nil {
		log.Printf("failed to convert base64 string to [32]byte: %s", err.Error())
		return nil
	}
	return &hash
}

func ConvertHashToIndex(h *Hash) *indexModels.HashType {
	if h == nil {
		return nil
	}
	hash := indexModels.HashType(h.Base64())
	return &hash
}

func MsgPackAccountStateToIndexAccountState(accountState AccountState) indexModels.AccountState {
	balance := strconv.FormatUint(accountState.Balance, 10)
	return indexModels.AccountState{
		Hash:          *ConvertHashToIndex(&accountState.Hash),
		Balance:       &balance,
		AccountStatus: &accountState.AccountStatus,
		FrozenHash:    ConvertHashToIndex(accountState.FrozenHash),
		DataHash:      ConvertHashToIndex(accountState.DataHash),
		CodeHash:      ConvertHashToIndex(accountState.CodeHash),
	}
}

func convertToIndexAccountState(hash *indexModels.HashType, accountStates map[Hash]*AccountState) *indexModels.AccountState {
	if hash == nil {
		return nil
	}

	localHash := convertHashToLocal(hash)
	if localHash == nil {
		log.Println("failed to convert base64 string to [32]byte")
		return nil
	}

	accountState, ok := accountStates[*localHash]
	if !ok {
		return nil
	}
	indexAccountState := MsgPackAccountStateToIndexAccountState(*accountState)
	return &indexAccountState
}

func TransformToAPIResponse(hset map[string]string, pool *crud.DbClient,
	isTestnet bool, includeAddressBook bool, includeMetadata bool, supportedActionTypes []string) (*EmulateTraceResponse, error) {
	emulatedContext := crud.NewEmptyContext(true)
	raw_traces := make(map[string]map[string]string)
	raw_traces[hset["root_node"]] = hset
	err := emulatedContext.FillFromRawData(raw_traces)
	if err != nil {
		return nil, fmt.Errorf("failed to fill context from raw data: %w", err)
	}

	traceRows := emulatedContext.GetTraces()
	if len(traceRows) != 1 {
		return nil, fmt.Errorf("more than 1 trace in the context")
	}

	trace, err := parse.ScanTrace(traceRows[0])
	if err != nil {
		return nil, fmt.Errorf("failed to scan trace: %w", err)
	}
	txs, err := crud.QueryPendingTransactionsImpl(emulatedContext, nil, indexModels.RequestSettings{}, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get trace transactions: %w", err)
	}
	trace.Transactions = make(map[indexModels.HashType]*indexModels.Transaction)
	for idx := range txs {
		tx := &txs[idx]
		if v := tx.TraceExternalHash; v != nil {
			trace.TransactionsOrder = append(trace.TransactionsOrder, tx.Hash)
			trace.Transactions[tx.Hash] = tx
		}
	}

	traceRoot, err := parse.AssembleTraceTxsFromMap(&trace.TransactionsOrder, &trace.Transactions)
	if err != nil {
		log.Printf("failed to assemble trace transactions: %s", err.Error())
	}
	trace.Trace = traceRoot

	actions := make([]*indexModels.Action, 0)
	trace.Actions = &actions
	rawActions := make([]indexModels.RawAction, 0)
	for _, row := range emulatedContext.GetActions(supportedActionTypes) {
		if loc, err := parse.ScanRawAction(row); err == nil {
			rawActions = append(rawActions, *loc)
		} else {
			return nil, fmt.Errorf("failed to scan raw action: %w", err)
		}
	}
	addr_map := map[string]bool{}
	for idx := range rawActions {
		rawAction := &rawActions[idx]
		parse.CollectAddressesFromAction(&addr_map, rawAction)

		action, err := parse.ParseRawAction(rawAction)
		if err != nil {
			return nil, fmt.Errorf("failed to parse raw action: %w", err)
		}
		*trace.Actions = append(*trace.Actions, action)
	}

	var accountStates map[Hash]*AccountState
	err = msgpack.Unmarshal([]byte(hset["account_states"]), &accountStates)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal account states: %w", err)
	}

	// iterate transactions and fill account states
	for _, tx := range trace.Transactions {
		if tx.AccountStateBefore != nil {
			tx.AccountStateBefore = convertToIndexAccountState(&tx.AccountStateHashBefore, accountStates)
		}
		if tx.AccountStateAfter != nil {
			tx.AccountStateAfter = convertToIndexAccountState(&tx.AccountStateHashAfter, accountStates)
		}
		addr_map[string(tx.Account)] = true
	}

	var book *indexModels.AddressBook = nil
	var metadata *indexModels.Metadata = nil
	if includeAddressBook || includeMetadata {
		conn, err := pool.Pool.Acquire(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to acquire connection: %w", err)
		}
		defer conn.Release()

		settings := indexModels.RequestSettings{
			Timeout:   3 * time.Second,
			IsTestnet: isTestnet,
		}
		addr_list := slices.Collect(maps.Keys(addr_map))

		if includeAddressBook {
			bookVal, err := crud.QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, fmt.Errorf("failed to query address book: %w", err)
			}
			book = &bookVal
		}

		if includeMetadata {
			metadataVal, err := crud.QueryMetadataImpl(addr_list, conn, settings)
			if err != nil {
				return nil, fmt.Errorf("failed to query metadata: %w", err)
			}

			// append raw interfaces data to metadata (e.g. token is being deployed in emulated trace)
			for _, addr := range addr_list {
				interfacesMsgPacked, hasInterfaces := hset[addr]
				if !hasInterfaces {
					continue
				}
				err = appendRawInterfacesDataToMetadata(addr, interfacesMsgPacked, &metadataVal)
				if err != nil {
					return nil, fmt.Errorf("failed to convert raw interfaces to metadata: %w", err)
				}
			}

			metadata = &metadataVal
		}
	}

	mcBlockSeqno, err := strconv.ParseUint(hset["mc_block_seqno"], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to convert mc_block_seqno to int: %w", err)
	}

	var codeCellsPointer *map[Hash]string
	if codeCells, ok := hset["code_cells"]; ok {
		var codeCellsMap map[Hash]string
		err = msgpack.Unmarshal([]byte(codeCells), &codeCellsMap)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal code cells: %w", err)
		}
		codeCellsPointer = &codeCellsMap
	}

	var dataCellsPointer *map[Hash]string
	if dataCells, ok := hset["data_cells"]; ok {
		var dataCellsMap map[Hash]string
		err = msgpack.Unmarshal([]byte(dataCells), &dataCellsMap)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal data cells: %w", err)
		}
		dataCellsPointer = &dataCellsMap
	}

	var depthLimitExceeded bool
	if depthLimit, ok := hset["depth_limit_exceeded"]; ok {
		depthLimitExceeded, err = strconv.ParseBool(depthLimit)
		if err != nil {
			return nil, fmt.Errorf("failed to convert depth_limit_exceeded to bool: %w", err)
		}
	}

	response := EmulateTraceResponse{
		McBlockSeqno: uint32(mcBlockSeqno),
		Trace:        *trace.Trace,
		Transactions: trace.Transactions,
		Actions:      trace.Actions,
		CodeCells:    codeCellsPointer,
		DataCells:    dataCellsPointer,
		AddressBook:  book,
		Metadata:     metadata,
		RandSeed:     hset["rand_seed"],
		IsIncomplete: depthLimitExceeded,
	}
	return &response, nil
}
