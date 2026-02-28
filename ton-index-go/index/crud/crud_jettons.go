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
	"github.com/lib/pq"
)

func buildJettonMastersQuery(jetton_req JettonMasterRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := ` J.address, J.total_supply, J.mintable, J.admin_address, J.jetton_content, 
		J.jetton_wallet_code_hash, J.code_hash, J.data_hash, J.last_transaction_lt`
	from_query := ` jetton_masters as J`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by id asc`
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := jetton_req.MasterAddress; v != nil {
		filter_str := filterByArray("J.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := jetton_req.AdminAddress; v != nil {
		filter_str := filterByArray("J.admin_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildJettonWalletsQuery(jetton_req JettonWalletRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := `J.address, J.balance, J.owner, J.jetton, J.last_transaction_lt, J.code_hash, J.data_hash, 
		J.mintless_is_claimed, J.mintless_amount, J.mintless_start_from, J.mintless_expire_at, MJM.custom_payload_api_uri`
	from_query := `jetton_wallets as J left join mintless_jetton_masters as MJM on J.jetton = MJM.address`
	filter_list := []string{}
	filter_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	sort_column := "J.id"
	sort_order := "asc"
	if v := lim_req.Sort; v != nil {
		sort_column = "J.balance"
		sort_order, err = getSortOrder(*v)
		if err != nil {
			return "", err
		}

	}
	orderby_query := fmt.Sprintf(` order by %s %s`, sort_column, sort_order)

	if v := jetton_req.Address; v != nil {
		filter_str := filterByArray("J.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ` order by J.address asc`
	}
	if v := jetton_req.OwnerAddress; v != nil {
		filter_str := filterByArray("J.owner", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = fmt.Sprintf(` order by J.owner, %s %s`, sort_column, sort_order)
	}
	if v := jetton_req.JettonAddress; v != nil {
		if len(jetton_req.JettonAddress) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("J.jetton = '%s'", v[0]))
			orderby_query = fmt.Sprintf(` order by J.jetton, %s %s`, sort_column, sort_order)
		} else if len(jetton_req.JettonAddress) > 1 {
			filter_str := filterByArray("J.jetton", v)
			if len(filter_str) > 0 {
				filter_list = append(filter_list, filter_str)
			}
		}
	}
	if v := jetton_req.ExcludeZeroBalance; v != nil && *v {
		filter_list = append(filter_list, "J.balance + coalesce(mintless_amount, 0) > 0")
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildJettonTransfersQuery(transfer_req JettonTransferRequest, utime_req UtimeRequest,
	lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := `T.tx_hash, T.tx_lt, T.tx_now, T.tx_aborted, T.query_id,
		T.amount, T.source, T.destination, T.jetton_wallet_address, T.jetton_master_address, T.response_destination, T.custom_payload,
		T.forward_ton_amount, T.forward_payload, T.trace_id`
	from_query := `jetton_transfers as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	filter_list = append(filter_list, "T.tx_aborted is false")

	if v := transfer_req.OwnerAddress; v != nil {
		if v1 := transfer_req.Direction; v1 != nil {
			f_str := ``
			if *v1 == "in" {
				f_str = filterByArray("T.destination", v)
			} else {
				f_str = filterByArray("T.source", v)
			}
			if len(f_str) > 0 {
				filter_list = append(filter_list, f_str)
			}
		} else {
			f1_str := filterByArray("T.source", v)
			f2_str := filterByArray("T.destination", v)
			if len(f1_str) > 0 {
				filter_list = append(filter_list, fmt.Sprintf("(%s or %s)", f1_str, f2_str))
			}
		}
	}
	if v := transfer_req.JettonWallet; v != nil {
		filter_str := filterByArray("T.jetton_wallet_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := transfer_req.JettonMaster; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.jetton_master_address = '%s'", *v))
	}

	order_col := "T.tx_lt"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now >= %d", *v))
		order_col = "T.tx_now"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now <= %d", *v))
		order_col = "T.tx_now"
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt <= %d", *v))
	}
	if lim_req.Sort == nil {
		lim_req.Sort = new(SortType)
		*lim_req.Sort = "desc"
	}
	if lim_req.Sort != nil {
		sort_order, err := getSortOrder(*lim_req.Sort)
		if err != nil {
			return "", err
		}
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, sort_order)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query)
	return query, nil
}

func buildJettonBurnsQuery(burn_req JettonBurnRequest, utime_req UtimeRequest,
	lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := `T.tx_hash, T.tx_lt, T.tx_now, T.tx_aborted, T.query_id,
		T.owner, T.jetton_wallet_address, T.jetton_master_address, T.amount, T.response_destination, T.custom_payload, T.trace_id`
	from_query := `jetton_burns as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := burn_req.OwnerAddress; v != nil {
		f_str := ``
		f_str = filterByArray("T.owner", v)
		if len(f_str) > 0 {
			filter_list = append(filter_list, f_str)
		}
	}
	if v := burn_req.JettonWallet; v != nil {
		filter_str := filterByArray("T.jetton_wallet_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := burn_req.JettonMaster; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.jetton_master_address = '%s'", *v))
	}

	order_col := "T.tx_lt"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now >= %d", *v))
		order_col = "T.tx_now"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now <= %d", *v))
		order_col = "T.tx_now"
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt <= %d", *v))
	}
	if lim_req.Sort == nil {
		lim_req.Sort = new(SortType)
		*lim_req.Sort = "desc"
	}
	if lim_req.Sort != nil {
		sort_order, err := getSortOrder(*lim_req.Sort)
		if err != nil {
			return "", err
		}
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, sort_order)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query)
	return query, nil
}

func queryJettonWalletsTokenInfo(addr_list []string, conn *pgxpool.Conn, ctx context.Context) (map[string]TokenInfo, []string, error) {
	if len(addr_list) == 0 {
		return map[string]TokenInfo{}, []string{}, nil
	}

	query := `SELECT jw.address, jw.owner, jw.balance, jw.jetton 
			  FROM jetton_wallets jw 
			  WHERE jw.address = ANY($1)`

	rows, err := conn.Query(ctx, query, pq.Array(addr_list))
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	token_info_map := make(map[string]TokenInfo)
	additional_addresses := make(map[string]bool) // Use map to avoid duplicates
	addr_set := make(map[string]bool)

	// Convert addr_list to set for quick lookup
	for _, addr := range addr_list {
		addr_set[addr] = true
	}
	valid := true
	token_type := "jetton_wallets"
	for rows.Next() {
		var jetton_wallet_address, jetton_wallet_owner, jetton_walet_jetton string
		var balance *string

		err := rows.Scan(&jetton_wallet_address, &jetton_wallet_owner, &balance, &jetton_walet_jetton)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}

		extra := map[string]interface{}{
			"owner":  jetton_wallet_owner,
			"jetton": jetton_walet_jetton,
		}
		if balance != nil {
			extra["balance"] = *balance
		}

		token_info := TokenInfo{
			Address: jetton_wallet_address,
			Type:    &token_type,
			Extra:   extra,
			Indexed: true,
			Valid:   &valid,
		}

		token_info_map[jetton_wallet_address] = token_info

		// Add jetton address to additional addresses if not already in addr_list
		if !addr_set[jetton_walet_jetton] {
			additional_addresses[jetton_walet_jetton] = true
		}
	}

	if rows.Err() != nil {
		return nil, nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}

	additional_addr_slice := make([]string, 0, len(additional_addresses))
	for addr := range additional_addresses {
		additional_addr_slice = append(additional_addr_slice, addr)
	}

	return token_info_map, additional_addr_slice, nil
}

func queryJettonMastersImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonMaster, error) {
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

	res := []JettonMaster{}
	for rows.Next() {
		if loc, err := parse.ScanJettonMaster(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return res, nil
}

func queryJettonWalletsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonWallet, error) {
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

	res := []JettonWallet{}
	for rows.Next() {
		if loc, err := parse.ScanJettonWallet(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return res, nil
}

func queryJettonTransfersImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonTransfer, error) {
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

	res := []JettonTransfer{}
	for rows.Next() {
		if loc, err := parse.ScanJettonTransfer(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}

	if err := detect.MarkJettonTransfers(res); err != nil {
		hashes := make([]string, len(res))
		for i, t := range res {
			hashes[i] = string(t.TransactionHash)
		}
		log.Printf("Error marking jetton transfers with hashes %v: %v", hashes, err)
	}
	return res, nil
}

func queryJettonBurnsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonBurn, error) {
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

	res := []JettonBurn{}
	for rows.Next() {
		if loc, err := parse.ScanJettonBurn(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	if err := detect.MarkJettonBurns(res); err != nil {
		hashes := make([]string, len(res))
		for i, t := range res {
			hashes[i] = string(t.TransactionHash)
		}
		log.Printf("Error marking jetton burns with hashes %v: %v", hashes, err)
	}
	return res, nil
}

func (db *DbClient) QueryJettonMasters(
	jetton_req JettonMasterRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonMaster, AddressBook, Metadata, error) {
	query, err := buildJettonMastersQuery(jetton_req, lim_req, settings)
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

	res, err := queryJettonMastersImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.Address))
		if t.AdminAddress != nil {
			addr_list = append(addr_list, string(*t.AdminAddress))
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

func (db *DbClient) QueryJettonWallets(
	jetton_req JettonWalletRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonWallet, AddressBook, Metadata, error) {
	query, err := buildJettonWalletsQuery(jetton_req, lim_req, settings)
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

	res, err := queryJettonWalletsImpl(query, conn, settings)
	if err != nil {
		log.Println(query)
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.Address))
		addr_list = append(addr_list, string(t.Owner))
		addr_list = append(addr_list, string(t.Jetton))
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

func (db *DbClient) QueryJettonTransfers(
	transfer_req JettonTransferRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonTransfer, AddressBook, Metadata, error) {
	query, err := buildJettonTransfersQuery(transfer_req, utime_req, lt_req, lim_req, settings)
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

	res, err := queryJettonTransfersImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.Source))
		addr_list = append(addr_list, string(t.Destination))
		addr_list = append(addr_list, string(t.SourceWallet))
		addr_list = append(addr_list, string(t.JettonMaster))
		if t.ResponseDestination != nil {
			addr_list = append(addr_list, string(*t.ResponseDestination))
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

func (db *DbClient) QueryJettonBurns(
	transfer_req JettonBurnRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonBurn, AddressBook, Metadata, error) {
	query, err := buildJettonBurnsQuery(transfer_req, utime_req, lt_req, lim_req, settings)
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

	res, err := queryJettonBurnsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.Owner))
		addr_list = append(addr_list, string(t.JettonWallet))
		addr_list = append(addr_list, string(t.JettonMaster))
		if t.ResponseDestination != nil {
			addr_list = append(addr_list, string(*t.ResponseDestination))
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
