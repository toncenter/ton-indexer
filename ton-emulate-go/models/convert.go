package models

import (
	"context"
	"fmt"
	"log"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/index"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

func convertHashToLocal(h *index.HashType) *Hash {
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

func convertHashToIndex(h *Hash) *index.HashType {
	if h == nil {
		return nil
	}
	hash := index.HashType(h.Base64())
	return &hash
}

func convertToIndexAccountState(hash *index.HashType, accountStates map[Hash]*AccountState) *index.AccountState {
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

	balance := strconv.FormatUint(accountState.Balance, 10)
	return &index.AccountState{
		Hash:          *convertHashToIndex(&accountState.Hash),
		Balance:       &balance,
		AccountStatus: &accountState.AccountStatus,
		FrozenHash:    convertHashToIndex(accountState.FrozenHash),
		DataHash:      convertHashToIndex(accountState.DataHash),
		CodeHash:      convertHashToIndex(accountState.CodeHash),
	}
}

func TransformToAPIResponse(hset map[string]string, pool *index.DbClient,
	isTestnet bool, includeAddressBook bool, includeMetadata bool, supportedActionTypes []string) (*EmulateTraceResponse, error) {
	emulatedContext := index.NewEmptyContext(true)
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

	trace, err := index.ScanTrace(traceRows[0])
	if err != nil {
		return nil, fmt.Errorf("failed to scan trace: %w", err)
	}
	txs, err := index.QueryPendingTransactionsImpl(emulatedContext, nil, index.RequestSettings{}, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get trace transactions: %w", err)
	}
	trace.Transactions = make(map[index.HashType]*index.Transaction)
	for idx := range txs {
		tx := &txs[idx]
		if v := tx.TraceExternalHash; v != nil {
			trace.TransactionsOrder = append(trace.TransactionsOrder, tx.Hash)
			trace.Transactions[tx.Hash] = tx
		}
	}

	traceRoot, err := index.AssembleTraceTxsFromMap(&trace.TransactionsOrder, &trace.Transactions)
	if err != nil {
		log.Printf("failed to assemble trace transactions: %s", err.Error())
	}
	trace.Trace = traceRoot

	actions := make([]*index.Action, 0)
	trace.Actions = &actions
	rawActions := make([]index.RawAction, 0)
	for _, row := range emulatedContext.GetActions(supportedActionTypes) {
		if loc, err := index.ScanRawAction(row); err == nil {
			rawActions = append(rawActions, *loc)
		} else {
			return nil, fmt.Errorf("failed to scan raw action: %w", err)
		}
	}
	addr_map := map[string]bool{}
	for idx := range rawActions {
		rawAction := &rawActions[idx]
		index.CollectAddressesFromAction(&addr_map, rawAction)

		action, err := index.ParseRawAction(rawAction)
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

	var book *index.AddressBook = nil
	var metadata *index.Metadata = nil
	if includeAddressBook || includeMetadata {
		conn, err := pool.Pool.Acquire(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to acquire connection: %w", err)
		}
		defer conn.Release()

		settings := index.RequestSettings{
			Timeout:   3 * time.Second,
			IsTestnet: isTestnet,
		}
		addr_list := slices.Collect(maps.Keys(addr_map))

		if includeAddressBook {
			bookVal, err := index.QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, fmt.Errorf("failed to query address book: %w", err)
			}
			book = &bookVal
		}

		if includeMetadata {
			metadataVal, err := index.QueryMetadataImpl(addr_list, conn, settings)
			if err != nil {
				return nil, fmt.Errorf("failed to query metadata: %w", err)
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
