package models

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"

	tonindexgo "github.com/toncenter/ton-indexer/ton-index-go/index"
	emulated "github.com/toncenter/ton-indexer/ton-index-go/index/emulated"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

func ConvertHsetToTraceNodeShort(hset map[string]string) (*TraceNodeShort, map[Hash]*Transaction, error) {
	trace_id := hset["root_node"]
	rootNodeBytes := []byte(hset[trace_id])
	if len(rootNodeBytes) == 0 {
		return nil, nil, fmt.Errorf("root node not found")
	}

	txMap := make(map[Hash]*Transaction)
	node, err := convertNode(hset, rootNodeBytes, txMap)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert root node: %w", err)
	}
	return node, txMap, nil
}

func convertNode(hset map[string]string, nodeBytes []byte, txMap map[Hash]*Transaction) (*TraceNodeShort, error) {
	var node TraceNode
	err := msgpack.Unmarshal(nodeBytes, &node)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal node: %w", err)
	}

	txMap[node.Transaction.Hash] = &node.Transaction

	short := &TraceNodeShort{
		TransactionHash: node.Transaction.Hash,
		InMsgHash:       node.Transaction.InMsg.Hash,
		Children:        make([]*TraceNodeShort, 0, len(node.Transaction.OutMsgs)),
	}

	for _, outMsg := range node.Transaction.OutMsgs {
		// Get child node by out message hash
		msgKey := base64.StdEncoding.EncodeToString(outMsg.Hash[:])
		if childBytes, exists := hset[msgKey]; exists {
			childNode := []byte(childBytes)
			nodeShort, err := convertNode(hset, childNode, txMap)
			if err != nil {
				return nil, fmt.Errorf("failed to convert child node: %w", err)
			}
			short.Children = append(short.Children, nodeShort)
		}
	}

	return short, nil
}

func TransformToAPIResponse(hset map[string]string) (*EmulateTraceResponse, error) {
	shortTrace, txMap, err := ConvertHsetToTraceNodeShort(hset)
	if err != nil {
		return nil, err
	}

	var accountStates map[Hash]*AccountState
	err = msgpack.Unmarshal([]byte(hset["account_states"]), &accountStates)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal account states: %w", err)
	}

	var actionsPointer *[]tonindexgo.Action = nil
	actionsData, ok := hset["actions"]
	if ok {
		actions := make([]emulated.Action, 0)
		err = msgpack.Unmarshal([]byte(actionsData), &actions)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal actions: %w", err)
		}

		goActions := make([]tonindexgo.Action, 0, len(actions))

		for _, a := range actions {
			// convert trace id from base64 to hex, because in ton-trace-task-emulator we use hex
			// todo: unify trace id format
			traceIdBytes, err := base64.StdEncoding.DecodeString(a.TraceId)
			if err != nil {
				return nil, fmt.Errorf("failed to decode trace id: %w", err)
			}
			a.TraceId = hex.EncodeToString(traceIdBytes)
			row, err := a.GetActionRow()
			if err != nil {
				return nil, fmt.Errorf("failed to get action row: %w", err)
			}
			pgx_row := emulated.NewRow(&row)

			rawAction, err := tonindexgo.ScanRawAction(pgx_row)
			if err != nil {
				return nil, fmt.Errorf("failed to scan raw action: %w", err)
			}

			goAction, err := tonindexgo.ParseRawAction(rawAction)
			if err != nil {
				return nil, fmt.Errorf("failed to parse action: %w", err)
			}
			goActions = append(goActions, *goAction)
		}
		actionsPointer = &goActions
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

	response := EmulateTraceResponse{
		McBlockSeqno:  uint32(mcBlockSeqno),
		Trace:         *shortTrace,
		Transactions:  txMap,
		AccountStates: accountStates,
		Actions:       actionsPointer,
		CodeCells:     codeCellsPointer,
		DataCells:     dataCellsPointer,
		RandSeed:      hset["rand_seed"],
	}
	return &response, nil
}
