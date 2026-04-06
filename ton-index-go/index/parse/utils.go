package parse

import (
	"fmt"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

func AssembleTraceTxsFromMap(tx_order *[]models.HashType, txs *map[models.HashType]*models.Transaction) (*models.TraceNode, error) {
	nodes := map[models.HashType]*models.TraceNode{}
	warning := ``
	var root *models.TraceNode = nil
	for _, tx_hash := range *tx_order {
		tx := (*txs)[tx_hash]
		var in_msg_hash models.HashType
		if in_msg := tx.InMsg; in_msg != nil {
			in_msg_hash = in_msg.MsgHash
		}
		node := models.TraceNode{TransactionHash: tx_hash, InMsgHash: in_msg_hash}
		if len(tx.OutMsgs) == 0 {
			node.Children = make([]*models.TraceNode, 0)
		}
		for _, msg := range tx.OutMsgs {
			nodes[msg.MsgHash] = &node
		}
		if parent, ok := nodes[in_msg_hash]; ok {
			delete(nodes, in_msg_hash)
			parent.Children = append(parent.Children, &node)
		} else if root == nil {
			root = &node
		} else {
			warning = "missing node in trace found"
		}
	}
	if len(warning) > 0 {
		return root, fmt.Errorf("%s", warning)
	}
	return root, nil
}
