package parse

import (
	"github.com/jackc/pgx/v5"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

func ScanJettonMaster(row pgx.Row) (*models.JettonMaster, error) {
	var res models.JettonMaster
	err := row.Scan(&res.Address, &res.TotalSupply, &res.Mintable, &res.AdminAddress,
		&res.JettonContent, &res.JettonWalletCodeHash, &res.CodeHash, &res.DataHash,
		&res.LastTransactionLt)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanJettonWallet(row pgx.Row) (*models.JettonWallet, error) {
	var res models.JettonWallet
	var mintless_into models.JettonWalletMintlessInfo
	err := row.Scan(&res.Address, &res.Balance, &res.Owner, &res.Jetton, &res.LastTransactionLt,
		&res.CodeHash, &res.DataHash, &mintless_into.IsClaimed, &mintless_into.Amount,
		&mintless_into.StartFrom, &mintless_into.ExpireAt, &mintless_into.CustomPayloadApiUri)
	if mintless_into.IsClaimed != nil && !*mintless_into.IsClaimed && mintless_into.Amount != nil {
		res.MintlessInfo = &mintless_into
	}
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanJettonTransfer(row pgx.Row) (*models.JettonTransfer, error) {
	var res models.JettonTransfer
	err := row.Scan(&res.TransactionHash, &res.TransactionLt, &res.TransactionNow, &res.TransactionAborted,
		&res.QueryId, &res.Amount, &res.Source, &res.Destination, &res.SourceWallet, &res.JettonMaster,
		&res.ResponseDestination, &res.CustomPayload, &res.ForwardTonAmount, &res.ForwardPayload, &res.TraceId)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanJettonBurn(row pgx.Row) (*models.JettonBurn, error) {
	var res models.JettonBurn
	err := row.Scan(&res.TransactionHash, &res.TransactionLt, &res.TransactionNow, &res.TransactionAborted,
		&res.QueryId, &res.Owner, &res.JettonWallet, &res.JettonMaster, &res.Amount,
		&res.ResponseDestination, &res.CustomPayload, &res.TraceId)
	if err != nil {
		return nil, err
	}
	return &res, nil
}
