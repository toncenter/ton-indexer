package parse

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"

	"github.com/jackc/pgx/v5"
)

func ScanMultisigOrder(row pgx.Row) (*MultisigOrder, error) {
	var order MultisigOrder

	err := row.Scan(
		&order.Address,
		&order.MultisigAddress,
		&order.OrderSeqno,
		&order.Threshold,
		&order.SentForExecution,
		&order.ApprovalsMask,
		&order.ApprovalsNum,
		&order.ExpirationDate,
		&order.OrderBoc,
		&order.Signers,
		&order.LastTransactionLt,
		&order.CodeHash,
		&order.DataHash,
	)
	if err != nil {
		return nil, err
	}

	return &order, nil
}

func ScanMultisig(row pgx.Row) (*Multisig, error) {
	var multisig Multisig

	err := row.Scan(
		&multisig.Address,
		&multisig.NextOrderSeqno,
		&multisig.Threshold,
		&multisig.Signers,
		&multisig.Proposers,
		&multisig.LastTransactionLt,
		&multisig.CodeHash,
		&multisig.DataHash,
	)
	if err != nil {
		return nil, err
	}

	multisig.Orders = []MultisigOrder{}

	return &multisig, nil
}
