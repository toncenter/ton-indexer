package parse

import (
	"github.com/jackc/pgx/v5"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

func ScanMultisigOrder(row pgx.Row) (*models.MultisigOrder, error) {
	var order models.MultisigOrder

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

func ScanMultisig(row pgx.Row) (*models.Multisig, error) {
	var multisig models.Multisig

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

	multisig.Orders = []models.MultisigOrder{}

	return &multisig, nil
}
