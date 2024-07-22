package index

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xssnick/tonutils-go/address"
)

// address utils
func getAccountAddressFriendly(account string, code_hash *string, account_status *string, is_testnet bool) string {
	addr, err := address.ParseRawAddr(strings.Trim(account, " "))
	if err != nil {
		return "addr_none"
	}
	bouncable := true
	if code_hash == nil {
		bouncable = false
	}
	if account_status != nil && *account_status == "uninit" {
		bouncable = false
	}
	if code_hash != nil && WalletsHashMap[*code_hash] {
		bouncable = false
	}
	addr.SetBounce(bouncable)
	addr.SetTestnetOnly(is_testnet)
	return addr.String()
}

// query builders
func limitQuery(lim LimitRequest) string {
	query := ``
	if lim.Limit == nil {
		// set default value
		lim.Limit = new(int32)
		*lim.Limit = 1000
	}
	if lim.Limit != nil {
		query += fmt.Sprintf(" limit %d", *lim.Limit)
	}
	if lim.Offset != nil {
		query += fmt.Sprintf(" offset %d", *lim.Offset)
	}
	return query
}

func buildBlocksQuery(
	blk_req BlockRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
) (string, error) {
	query := `select blocks.* from blocks`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query := limitQuery(lim_req)

	// filters
	if v := blk_req.Workchain; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("workchain = %d", *v))
	}
	if v := blk_req.Shard; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("shard = %d", *v))
	}
	if v := blk_req.Seqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("seqno = %d", *v))
	}
	if v := blk_req.McSeqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("mc_block_seqno = %d", *v))
	}

	order_col := "gen_utime"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("gen_utime >= %d", *v))
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("gen_utime <= %d", *v))
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("start_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("start_lt <= %d", *v))
	}
	if v := lim_req.Sort; v != nil {
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, *v)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}

	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildTransactionsQuery(
	blk_req BlockRequest,
	tx_req TransactionRequest,
	msg_req MessageRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
) (string, error) {
	query := `select T.* from`
	from_query := ` transactions as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query := limitQuery(lim_req)

	// filters
	if v := blk_req.Workchain; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_workchain = %d", *v))
	}
	if v := blk_req.Shard; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_shard = %d", *v))
	}
	if v := blk_req.Seqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_seqno = %d", *v))
	}
	if v := blk_req.McSeqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.mc_block_seqno = %d", *v))
	}

	if v := tx_req.Account; v != nil {
		if len(v) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("T.account = '%s'", v[0]))
		} else if len(v) > 1 {
			vv := []string{}
			for _, x := range v {
				if len(x) > 0 {
					vv = append(vv, fmt.Sprintf("'%s'", x))
				}
			}
			vv_str := strings.Join(vv, ",")
			filter_list = append(filter_list, fmt.Sprintf("T.account in (%s)", vv_str))
		}
	}
	if v := tx_req.Hash; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.hash = '%s'", *v))
	}
	if v := tx_req.Lt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.lt = %d", *v))
	}

	order_col := "T.lt"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.now >= %d", *v))
		order_col = "T.now"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.now <= %d", *v))
		order_col = "T.now"
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.lt <= %d", *v))
	}
	if lim_req.Sort != nil {
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, *lim_req.Sort)
	}

	// transaction by message
	by_msg := false
	if v := msg_req.Direction; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.direction = '%s'", *v))
	}
	if v := msg_req.MessageHash; v != nil {
		by_msg = true
		if len(v) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("M.msg_hash = '%s'", v[0]))
		} else if len(v) > 1 {
			vv := []string{}
			for _, x := range v {
				if len(x) > 0 {
					vv = append(vv, fmt.Sprintf("'%s'", x))
				}
			}
			vv_str := strings.Join(vv, ",")
			filter_list = append(filter_list, fmt.Sprintf("M.msg_hash in (%s)", vv_str))
		}
	}
	if v := msg_req.Source; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.source = '%s'", *v))
	}
	if v := msg_req.Destination; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.destination = '%s'", *v))
	}
	if v := msg_req.BodyHash; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.body_hash = '%s'", *v))
	}
	if v := msg_req.Opcode; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.opcode = %d and M.direction = 'in'", *v))
	}
	if by_msg {
		from_query = " messages as M join transactions as T on M.tx_hash = T.hash and M.tx_lt = T.lt"
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query += from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildMessagesQuery(
	msg_req MessageRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
) (string, error) {
	all_columns := ` M.*, B.*, I.*`
	clmn_query := ` distinct on (M.msg_hash)` + all_columns
	from_query := ` messages as M 
		left join message_contents as B on M.body_hash = B.hash 
		left join message_contents as I on M.init_state_hash = I.hash`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query := limitQuery(lim_req)

	if v := msg_req.Direction; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.direction = '%s'", *v))
		clmn_query = all_columns
	}
	if v := msg_req.Source; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.source = '%s'", *v))
	}
	if v := msg_req.Destination; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.destination = '%s'", *v))
	}
	if v := msg_req.Opcode; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.opcode = %d", *v))
	}
	if v := msg_req.MessageHash; v != nil {
		if len(v) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("M.msg_hash = '%s'", v[0]))
		} else if len(v) > 1 {
			vv := []string{}
			for _, x := range v {
				if len(x) > 0 {
					vv = append(vv, fmt.Sprintf("'%s'", x))
				}
			}
			vv_str := strings.Join(vv, ",")
			filter_list = append(filter_list, fmt.Sprintf("M.msg_hash in (%s)", vv_str))
		}
	}
	if v := msg_req.BodyHash; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.body_hash = '%s'", *v))
	}

	order_col := "M.created_lt"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_at >= %d", *v))
		order_col = "M.created_at"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_at <= %d", *v))
		order_col = "M.created_at"
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_lt <= %d", *v))
	}
	if lim_req.Sort != nil {
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, *lim_req.Sort)
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
	log.Println(query) // TODO: remove debug
	return query, nil
}

func filterByArray[T any](clmn string, v []T) string {
	filter_list := []string{}
	for _, x := range v {
		t := reflect.ValueOf(x)
		switch t.Kind() {
		case reflect.String:
			if t.Len() > 0 {
				filter_list = append(filter_list, fmt.Sprintf("'%s'", t.String()))
			}
		default:
			filter_list = append(filter_list, fmt.Sprintf("'%v'", x))
		}
	}
	if len(filter_list) == 1 {
		return fmt.Sprintf("%s = %s", clmn, filter_list[0])
	}
	if len(filter_list) > 1 {
		vv_str := strings.Join(filter_list, ",")
		return fmt.Sprintf("%s in (%s)", clmn, vv_str)
	}
	return ``
}

func buildNFTCollectionsQuery(nft_req NFTCollectionRequest, lim_req LimitRequest) (string, error) {
	clmn_query := ` N.address, N.next_item_index, N.owner_address, N.collection_content, 
				    N.data_hash, N.code_hash, N.last_transaction_lt`
	from_query := ` nft_collections as N`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by id asc`
	limit_query := limitQuery(lim_req)

	if v := nft_req.CollectionAddress; v != nil {
		filter_str := filterByArray("N.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := nft_req.OwnerAddress; v != nil {
		filter_str := filterByArray("N.owner_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
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

func buildNFTItemsQuery(nft_req NFTItemRequest, lim_req LimitRequest) (string, error) {
	clmn_query := ` N.address, N.init, N.index, N.collection_address, N.owner_address, N.content, 
					N.last_transaction_lt, N.code_hash, N.data_hash,
					C.address, C.next_item_index, C.owner_address, C.collection_content, 
				    C.data_hash, C.code_hash, C.last_transaction_lt`
	from_query := ` nft_items as N left join nft_collections as C on N.collection_address = C.address`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by N.id asc`
	limit_query := limitQuery(lim_req)

	if v := nft_req.Address; v != nil {
		filter_str := filterByArray("N.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := nft_req.OwnerAddress; v != nil {
		filter_str := filterByArray("N.owner_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ` order by N.owner_address, N.collection_address, N.index`
	}
	if v := nft_req.CollectionAddress; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("N.collection_address = '%s'", *v))
		orderby_query = ` order by collection_address, index`
	}
	if v := nft_req.Index; v != nil {
		if nft_req.CollectionAddress == nil {
			return ``, errors.New("index parameter is not allowed without the collection_address")
		}
		filter_str := filterByArray("N.index", v)
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

func buildNFTTransfersQuery(transfer_req NFTTransferRequest, utime_req UtimeRequest,
	lt_req LtRequest, lim_req LimitRequest) (string, error) {
	clmn_query := ` T.*`
	from_query := ` nft_transfers as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query := limitQuery(lim_req)

	if v := transfer_req.OwnerAddress; v != nil {
		if v1 := transfer_req.Direction; v1 != nil {
			f_str := ``
			if *v1 == "in" {
				f_str = filterByArray("T.new_owner", v)
			} else {
				f_str = filterByArray("T.old_owner", v)
			}
			if len(f_str) > 0 {
				filter_list = append(filter_list, f_str)
			}
		} else {
			f1_str := filterByArray("T.old_owner", v)
			f2_str := filterByArray("T.new_owner", v)
			if len(f1_str) > 0 {
				filter_list = append(filter_list, fmt.Sprintf("(%s or %s)", f1_str, f2_str))
			}
		}
	}
	if v := transfer_req.ItemAddress; v != nil {
		filter_str := filterByArray("T.nft_item_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := transfer_req.CollectionAddress; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.nft_collection_address = '%s'", *v))
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
		*lim_req.Sort = "desc"
	}
	if lim_req.Sort != nil {
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, *lim_req.Sort)
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
	log.Println(query)
	return query, nil
}

func buildJettonMastersQuery(jetton_req JettonMasterRequest, lim_req LimitRequest) (string, error) {
	clmn_query := ` J.address, J.total_supply, J.mintable, J.admin_address, J.jetton_content, 
		J.jetton_wallet_code_hash, J.code_hash, J.data_hash, J.last_transaction_lt`
	from_query := ` jetton_masters as J`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by id asc`
	limit_query := limitQuery(lim_req)

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

func buildJettonWalletsQuery(jetton_req JettonWalletRequest, lim_req LimitRequest) (string, error) {
	clmn_query := `J.address, J.balance, J.owner, J.jetton, J.last_transaction_lt, J.code_hash, J.data_hash`
	from_query := `jetton_wallets as J`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by id asc`
	limit_query := limitQuery(lim_req)

	if v := jetton_req.Address; v != nil {
		filter_str := filterByArray("J.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := jetton_req.OwnerAddress; v != nil {
		filter_str := filterByArray("J.owner", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := jetton_req.JettonAddress; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("J.jetton = '%s'", *v))
		orderby_query = ``
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
	lt_req LtRequest, lim_req LimitRequest) (string, error) {
	clmn_query := `T.*`
	from_query := `jetton_transfers as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query := limitQuery(lim_req)

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
		*lim_req.Sort = "desc"
	}
	if lim_req.Sort != nil {
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, *lim_req.Sort)
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
	log.Println(query)
	return query, nil
}

func buildJettonBurnsQuery(burn_req JettonBurnRequest, utime_req UtimeRequest,
	lt_req LtRequest, lim_req LimitRequest) (string, error) {
	clmn_query := `T.*`
	from_query := `jetton_burns as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query := limitQuery(lim_req)

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
		*lim_req.Sort = "desc"
	}
	if lim_req.Sort != nil {
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, *lim_req.Sort)
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
	log.Println(query)
	return query, nil
}

func buildAccountStatesQuery(account_req AccountRequest, lim_req LimitRequest) (string, error) {
	clmn_query := `A.account, A.hash, A.balance, A.account_status, A.frozen_hash, A.last_trans_hash, A.last_trans_lt, A.data_hash, A.code_hash, A.data_boc, A.code_boc`
	from_query := `latest_account_states as A`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query := limitQuery(lim_req)

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

	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	log.Println(query)
	return query, nil
}

// query implementation functions
func queryBlocksImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]Block, error) {
	// blocks
	blks := []Block{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		default:
		}
		defer rows.Close()

		for rows.Next() {
			if blk, err := ScanBlock(rows); err == nil {
				blks = append(blks, *blk)
			} else {
				return nil, err
			}
		}
	}
	return blks, nil
}

func queryMessagesImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]Message, error) {
	msgs := []Message{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		default:
		}
		defer rows.Close()

		for rows.Next() {
			msg, err := ScanMessageWithContent(rows)
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, *msg)
		}
	}
	return msgs, nil
}

func queryTransactionsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]Transaction, error) {
	// transactions
	txs := []Transaction{}
	txs_map := map[HashType]int{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		default:
		}
		defer rows.Close()

		for rows.Next() {
			if tx, err := ScanTransaction(rows); err == nil {
				txs = append(txs, *tx)
				txs_map[tx.Hash] = len(txs) - 1
			} else {
				return nil, err
			}
		}
	}

	acst_list := []string{}
	hash_list := []string{}
	for _, t := range txs {
		hash_list = append(hash_list, fmt.Sprintf("'%s'", t.Hash))
		acst_list = append(acst_list, fmt.Sprintf("'%s'", t.AccountStateHashBefore))
		acst_list = append(acst_list, fmt.Sprintf("'%s'", t.AccountStateHashAfter))
	}
	// account states
	if len(txs) == 0 {
		return txs, nil
	}
	if len(acst_list) > 0 {
		acst_list_str := strings.Join(acst_list, ",")
		query = fmt.Sprintf("select * from account_states where hash in (%s)", acst_list_str)

		acsts, err := queryAccountStatesImpl(query, conn, settings)
		if err != nil {
			return nil, err
		}
		acsts_map := make(map[HashType]*AccountState)
		for _, a := range acsts {
			acsts_map[a.Hash] = &a
		}
		for idx := range txs {
			if v, ok := acsts_map[txs[idx].AccountStateHashBefore]; ok {
				txs[idx].AccountStateBefore = v
			}
			if v, ok := acsts_map[txs[idx].AccountStateHashAfter]; ok {
				txs[idx].AccountStateAfter = v
			}
		}
	}

	// messages
	if len(hash_list) > 0 {
		hash_list_str := strings.Join(hash_list, ",")
		query = fmt.Sprintf(`select M.*, B.*, I.* from messages as M 
			left join message_contents as B on M.body_hash = B.hash 
			left join message_contents as I on M.init_state_hash = I.hash
			where M.tx_hash in (%s)`, hash_list_str)

		msgs, err := queryMessagesImpl(query, conn, settings)
		if err != nil {
			return nil, err
		}

		for _, msg := range msgs {
			if msg.Direction == "in" {
				txs[txs_map[msg.TxHash]].InMsg = &msg
			} else {
				txs[txs_map[msg.TxHash]].OutMsgs = append(txs[txs_map[msg.TxHash]].OutMsgs, &msg)
			}
		}
	}

	// sort messages
	for idx, _ := range txs {
		sort.SliceStable(txs[idx].OutMsgs, func(i, j int) bool {
			if txs[idx].OutMsgs[i].CreatedLt == nil {
				return true
			}
			if txs[idx].OutMsgs[j].CreatedLt == nil {
				return false
			}
			return *txs[idx].OutMsgs[i].CreatedLt < *txs[idx].OutMsgs[j].CreatedLt
		})
	}
	return txs, nil
}

func queryAddressBookImpl(addr_list []string, conn *pgxpool.Conn, settings RequestSettings) (AddressBook, error) {
	book := AddressBook{}
	{
		addr_list_str := strings.Join(addr_list, ",")
		query := fmt.Sprintf("select account, account_friendly, code_hash, account_status from latest_account_states where account in (%s)", addr_list_str)

		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		default:
		}
		defer rows.Close()

		for rows.Next() {
			var account string
			var account_friendly *string
			var code_hash *string
			var account_status *string
			if err := rows.Scan(&account, &account_friendly, &code_hash, &account_status); err == nil {
				addr_str := getAccountAddressFriendly(account, code_hash, account_status, settings.IsTestnet)
				book[strings.Trim(account, " ")] = AddressBookRow{UserFriendly: &addr_str}
			} else {
				return nil, err
			}
		}
	}
	return book, nil
}

func queryAccountStatesImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]AccountState, error) {
	acsts := []AccountState{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		default:
		}
		defer rows.Close()

		for rows.Next() {
			if acst, err := ScanAccountState(rows); err == nil {
				acsts = append(acsts, *acst)
			} else {
				return nil, err
			}
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
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		default:
		}
		defer rows.Close()

		for rows.Next() {
			if acst, err := ScanAccountStateFull(rows); err == nil {
				acsts = append(acsts, *acst)
			} else {
				return nil, err
			}
		}
	}
	return acsts, nil
}

func queryNFTCollectionsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]NFTCollection, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	default:
	}
	defer rows.Close()

	nfts := []NFTCollection{}
	for rows.Next() {
		if nft, err := ScanNFTCollection(rows); err == nil {
			nfts = append(nfts, *nft)
		} else {
			return nil, err
		}
	}

	return nfts, nil
}

func queryNFTItemsWithCollectionsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]NFTItem, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	default:
	}
	defer rows.Close()

	nfts := []NFTItem{}
	for rows.Next() {
		if nft, err := ScanNFTItemWithCollection(rows); err == nil {
			nfts = append(nfts, *nft)
		} else {
			return nil, err
		}
	}
	return nfts, nil
}

func queryNFTTransfersImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]NFTTransfer, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	default:
	}
	defer rows.Close()

	res := []NFTTransfer{}
	for rows.Next() {
		if loc, err := ScanNFTTransfer(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, err
		}
	}
	return res, nil
}

func queryJettonMastersImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonMaster, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	default:
	}
	defer rows.Close()

	res := []JettonMaster{}
	for rows.Next() {
		if loc, err := ScanJettonMaster(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, err
		}
	}
	return res, nil
}

func queryJettonWalletsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonWallet, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	default:
	}
	defer rows.Close()

	res := []JettonWallet{}
	for rows.Next() {
		if loc, err := ScanJettonWallet(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, err
		}
	}
	return res, nil
}

func queryJettonTransfersImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonTransfer, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	default:
	}
	defer rows.Close()

	res := []JettonTransfer{}
	for rows.Next() {
		if loc, err := ScanJettonTransfer(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, err
		}
	}
	return res, nil
}

func queryJettonBurnsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonBurn, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	default:
	}
	defer rows.Close()

	res := []JettonBurn{}
	for rows.Next() {
		if loc, err := ScanJettonBurn(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, err
		}
	}
	return res, nil
}

// Exported methods
func (db *DbClient) QueryMasterchainInfo(settings RequestSettings) (*MasterchainInfo, error) {
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	res := conn.QueryRow(ctx, "select * from blocks where workchain = -1 order by seqno desc limit 1")
	last, err := ScanBlock(res)
	if err != nil {
		return nil, err
	}

	res = conn.QueryRow(ctx, "select * from blocks where workchain = -1 order by seqno asc limit 1")
	first, err := ScanBlock(res)
	if err != nil {
		return nil, err
	}

	info := MasterchainInfo{last, first}
	return &info, nil
}

func (db *DbClient) QueryBlocks(
	blk_req BlockRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Block, error) {
	query, err := buildBlocksQuery(blk_req, utime_req, lt_req, lim_req)
	if err != nil {
		return nil, err
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	return queryBlocksImpl(query, conn, settings)
}

func (db *DbClient) QueryShards(
	seqno int,
	settings RequestSettings,
) ([]Block, error) {
	query := fmt.Sprintf(`select B.* from shard_state as S join blocks as B 
		on S.workchain = B.workchain and S.shard = B.shard and S.seqno = B.seqno 
		where mc_seqno = %d 
		order by S.mc_seqno, S.workchain, S.shard, S.seqno`, seqno)
	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	return queryBlocksImpl(query, conn, settings)
}

func (db *DbClient) QueryAddressBook(
	addr_list []string,
	settings RequestSettings,
) (AddressBook, error) {
	raw_addr_list := []string{}
	raw_addr_map := map[string]string{}
	for _, addr := range addr_list {
		addr_loc := AccountAddressConverter(addr)
		if addr_loc.IsValid() {
			if v, ok := addr_loc.Interface().(string); ok {
				raw_addr_list = append(raw_addr_list, fmt.Sprintf("'%s'", v))
				raw_addr_map[addr] = v
			}
		} else {
			raw_addr_map[addr] = ""
		}
	}
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	book, err := queryAddressBookImpl(raw_addr_list, conn, settings)
	if err != nil {
		return nil, err
	}

	new_addr_book := AddressBook{}
	for k, v := range raw_addr_map {
		if vv, ok := book[v]; ok {
			new_addr_book[k] = vv
		} else {
			new_addr_book[k] = AddressBookRow{nil}
		}

	}
	return new_addr_book, nil
}

func (db *DbClient) QueryTransactions(
	blk_req BlockRequest,
	tx_req TransactionRequest,
	msg_req MessageRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Transaction, AddressBook, error) {
	query, err := buildTransactionsQuery(blk_req, tx_req, msg_req, utime_req, lt_req, lim_req)
	if err != nil {
		return nil, nil, err
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()

	txs, err := queryTransactionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, err
	}

	var book AddressBook = nil
	addr_list := []string{}
	for _, t := range txs {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Account))
		if t.InMsg != nil {
			if t.InMsg.Source != nil {
				addr_list = append(addr_list, fmt.Sprintf("'%s'", *t.InMsg.Source))
			}
		}
		for _, m := range t.OutMsgs {
			if m.Destination != nil {
				addr_list = append(addr_list, fmt.Sprintf("'%s'", *m.Destination))
			}
		}
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, err
		}
	}
	return txs, book, nil
}

func (db *DbClient) QueryMessages(
	msg_req MessageRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Message, AddressBook, error) {
	query, err := buildMessagesQuery(msg_req, utime_req, lt_req, lim_req)
	if err != nil {
		return nil, nil, err
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()

	msgs, err := queryMessagesImpl(query, conn, settings)
	if err != nil {
		return nil, nil, err
	}

	var book AddressBook = nil
	addr_list := []string{}
	for _, m := range msgs {
		if m.Source != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *m.Source))
		}
		if m.Destination != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *m.Destination))
		}
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, err
		}
	}
	return msgs, book, nil
}

func (db *DbClient) QueryNFTCollections(
	nft_req NFTCollectionRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]NFTCollection, AddressBook, error) {
	query, err := buildNFTCollectionsQuery(nft_req, lim_req)
	if err != nil {
		return nil, nil, err
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()

	res, err := queryNFTCollectionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, err
	}

	var book AddressBook = nil
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Address))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.OwnerAddress))
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, err
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryNFTItems(
	nft_req NFTItemRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]NFTItem, AddressBook, error) {
	query, err := buildNFTItemsQuery(nft_req, lim_req)
	if err != nil {
		return nil, nil, err
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()

	res, err := queryNFTItemsWithCollectionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, err
	}

	var book AddressBook = nil
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Address))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.OwnerAddress))
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, err
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryNFTTransfers(
	transfer_req NFTTransferRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]NFTTransfer, AddressBook, error) {
	query, err := buildNFTTransfersQuery(transfer_req, utime_req, lt_req, lim_req)
	if err != nil {
		return nil, nil, err
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()

	res, err := queryNFTTransfersImpl(query, conn, settings)
	if err != nil {
		return nil, nil, err
	}

	var book AddressBook = nil
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.NftItemAddress))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.NftCollectionAddress))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.OldOwner))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.NewOwner))
		if t.ResponseDestination != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *t.ResponseDestination))
		}
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, err
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryJettonMasters(
	jetton_req JettonMasterRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonMaster, AddressBook, error) {
	query, err := buildJettonMastersQuery(jetton_req, lim_req)
	if err != nil {
		return nil, nil, err
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()

	res, err := queryJettonMastersImpl(query, conn, settings)
	if err != nil {
		return nil, nil, err
	}

	var book AddressBook = nil
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Address))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.AdminAddress))
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, err
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryJettonWallets(
	jetton_req JettonWalletRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonWallet, AddressBook, error) {
	query, err := buildJettonWalletsQuery(jetton_req, lim_req)
	if err != nil {
		return nil, nil, err
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()

	res, err := queryJettonWalletsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, err
	}

	var book AddressBook = nil
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Address))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Owner))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Jetton))
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, err
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryJettonTransfers(
	transfer_req JettonTransferRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonTransfer, AddressBook, error) {
	query, err := buildJettonTransfersQuery(transfer_req, utime_req, lt_req, lim_req)
	if err != nil {
		return nil, nil, err
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()

	res, err := queryJettonTransfersImpl(query, conn, settings)
	if err != nil {
		return nil, nil, err
	}

	var book AddressBook = nil
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Source))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Destination))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.SourceWallet))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.JettonMaster))
		if t.ResponseDestination != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *t.ResponseDestination))
		}
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, err
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryJettonBurns(
	transfer_req JettonBurnRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonBurn, AddressBook, error) {
	query, err := buildJettonBurnsQuery(transfer_req, utime_req, lt_req, lim_req)
	if err != nil {
		return nil, nil, err
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()

	res, err := queryJettonBurnsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, err
	}

	var book AddressBook = nil
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Owner))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.JettonWallet))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.JettonMaster))
		if t.ResponseDestination != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *t.ResponseDestination))
		}
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, err
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryAccountStates(
	account_req AccountRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]AccountStateFull, AddressBook, error) {
	query, err := buildAccountStatesQuery(account_req, lim_req)
	log.Println(query)
	if err != nil {
		return nil, nil, err
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()

	res, err := queryAccountStateFullImpl(query, conn, settings)
	if err != nil {
		return nil, nil, err
	}

	var book AddressBook = nil
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.AccountAddress))
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, err
		}
	}
	return res, book, nil
}
