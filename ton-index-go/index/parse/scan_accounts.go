package parse

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"strings"

	"github.com/jackc/pgx/v5"
)

func ScanAccountState(row pgx.Row) (*AccountState, error) {
	var acst AccountState
	err := row.Scan(&acst.Hash, &acst.Account, &acst.Balance, &acst.BalanceExtraCurrencies,
		&acst.AccountStatus, &acst.FrozenHash, &acst.DataHash, &acst.CodeHash)
	if err != nil {
		return nil, err
	}
	return &acst, nil
}

func ScanAccountStateFull(row pgx.Row) (*AccountStateFull, error) {
	var acst AccountStateFull
	err := row.Scan(&acst.AccountAddress, &acst.Hash, &acst.Balance, &acst.BalanceExtraCurrencies,
		&acst.AccountStatus, &acst.FrozenHash, &acst.LastTransactionHash, &acst.LastTransactionLt,
		&acst.DataHash, &acst.CodeHash, &acst.DataBoc, &acst.CodeBoc, &acst.ContractMethods)
	if err != nil {
		return nil, err
	}
	trimQuotes(acst.CodeBoc)
	trimQuotes(acst.DataBoc)

	if acst.ContractMethods != nil {
		// it's temporary, soon 'll be fixed in the database
		deduplicateUint32Slice(acst.ContractMethods)
	}

	return &acst, nil
}

func ScanAccountBalance(row pgx.Row) (*AccountBalance, error) {
	var acst AccountBalance
	err := row.Scan(&acst.Account, &acst.Balance)
	if err != nil {
		return nil, err
	}
	return &acst, nil
}

func trimQuotes(s *string) {
	if s != nil {
		*s = strings.Trim(*s, "'")
	}
}

func deduplicateUint32Slice(slice *[]uint32) {
	if slice == nil || len(*slice) == 0 {
		return
	}
	seen := make(map[uint32]bool)
	result := make([]uint32, 0, len(*slice))
	for _, v := range *slice {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}
	*slice = result
}
