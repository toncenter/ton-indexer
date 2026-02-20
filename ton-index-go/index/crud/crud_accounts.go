package crud

import (
	"github.com/toncenter/ton-indexer/ton-index-go/index/detect"
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"

	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func buildAccountStatesQuery(account_req AccountRequest, settings RequestSettings) (string, error) {
	clmn_query_default := `A.account, A.hash, A.balance, A.balance_extra_currencies, A.account_status, A.frozen_hash, A.last_trans_hash, A.last_trans_lt, A.data_hash, A.code_hash, `
	clmn_query := clmn_query_default + `A.data_boc, A.code_boc, C.methods`
	from_query := `latest_account_states as A left join contract_methods as C on A.code_hash = C.code_hash`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``

	// build query
	if v := account_req.AccountAddress; v != nil {
		f_str := ``
		f_str = filterByArray("A.account", v)
		if len(f_str) > 0 {
			filter_list = append(filter_list, f_str)
		}
	}
	if v := account_req.CodeHash; v != nil {
		f_str := ``
		f_str = filterByArray("A.code_hash", v)
		if len(f_str) > 0 {
			filter_list = append(filter_list, f_str)
		}
	}
	if v := account_req.IncludeBOC; v != nil && !*v {
		clmn_query = clmn_query_default + `NULL, NULL, C.methods`
	}

	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += ` limit 1000`
	// log.Println(query)
	return query, nil
}

// query implementation functions

func queryAccountStatesImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]AccountState, error) {
	acsts := []AccountState{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			if acst, err := parse.ScanAccountState(rows); err == nil {
				acsts = append(acsts, *acst)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return acsts, nil
}

func queryAccountStateFullImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]AccountStateFull, error) {
	acsts := []AccountStateFull{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			if acst, err := parse.ScanAccountStateFull(rows); err == nil {
				acsts = append(acsts, *acst)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
		if err := detect.MarkAccountStates(acsts); err != nil {
			methodIds := make([]string, len(acsts))
			for i, t := range acsts {
				if t.ContractMethods != nil {
					methodIds[i] = strings.Join(strings.Fields(fmt.Sprint(*t.ContractMethods)), ",")
				} else {
					methodIds[i] = "nil"
				}
			}
			log.Printf("Error marking account states with method ids %v: %v", methodIds, err)
		}
	}
	return acsts, nil
}

func queryTopAccountBalancesImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]AccountBalance, error) {
	acsts := []AccountBalance{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()

		for rows.Next() {
			if acst, err := parse.ScanAccountBalance(rows); err == nil {
				acsts = append(acsts, *acst)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return acsts, nil
}

func (db *DbClient) QueryAccountStates(
	account_req AccountRequest,
	settings RequestSettings,
) ([]AccountStateFull, AddressBook, Metadata, error) {
	query, err := buildAccountStatesQuery(account_req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query)
	}
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryAccountStateFullImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, t := range res {
		if t.AccountAddress != nil {
			addr_list = append(addr_list, string(*t.AccountAddress))
		}
	}
	if len(addr_list) > 0 {
		if !settings.NoAddressBook {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if !settings.NoMetadata {
			metadata, err = QueryMetadataImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
	}

	return res, book, metadata, nil
}

func (db *DbClient) QueryWalletStates(
	account_req AccountRequest,
	settings RequestSettings,
) ([]WalletState, AddressBook, Metadata, error) {
	states, book, metadata, err := db.QueryAccountStates(account_req, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	res := []WalletState{}
	for _, state := range states {
		loc, err := parse.ParseWalletState(state)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
		res = append(res, *loc)
	}
	return res, book, metadata, nil
}

func (db *DbClient) QueryTopAccountBalances(lim_req LimitRequest, settings RequestSettings) ([]AccountBalance, error) {
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return nil, err
	}
	query := `select account, balance from latest_account_states order by balance desc` + limit_query

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryTopAccountBalancesImpl(query, conn, settings)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	return res, nil
}
