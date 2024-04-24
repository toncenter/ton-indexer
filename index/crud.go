package index

import (
	"context"
	"fmt"
	"log"
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
		filter_list = append(filter_list, fmt.Sprintf(" M.direction = '%s'", *v))
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
		filter_list = append(filter_list, fmt.Sprintf(" M.source = '%s'", *v))
	}
	if v := msg_req.Destination; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf(" M.destination = '%s'", *v))
	}
	if v := msg_req.BodyHash; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf(" M.body_hash = '%s'", *v))
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
	clmn_query := ` distinct on (M.msg_hash) M.*`
	from_query := ` messages as M`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query := limitQuery(lim_req)

	if v := msg_req.Direction; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.direction = '%s'", *v))
		clmn_query = ` M.*`
	}
	if v := msg_req.Source; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.source = '%s'", *v))
	}
	if v := msg_req.Destination; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.destination = '%s'", *v))
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
		defer rows.Close()

		for rows.Next() {
			msg, err := ScanMessage(rows)
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, *msg)
		}
	}

	// message contents
	content_list := []string{}
	for _, m := range msgs {
		if m.BodyHash != nil {
			content_list = append(content_list, fmt.Sprintf("'%s'", *m.BodyHash))
		}
		if m.InitStateHash != nil {
			content_list = append(content_list, fmt.Sprintf("'%s'", *m.InitStateHash))
		}
	}
	if len(content_list) > 0 {
		content_list_str := strings.Join(content_list, ",")
		query = fmt.Sprintf("select * from message_contents where hash in (%s)", content_list_str)

		contents := map[string]*MessageContent{}
		{
			ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
			defer cancel_ctx()
			rows, err := conn.Query(ctx, query)
			if err != nil {
				return nil, err
			}
			defer rows.Close()

			for rows.Next() {
				if cc, err := ScanMessageContent(rows); err == nil {
					contents[cc.Hash] = cc
				} else {
					log.Printf("Failed reading content: %v\n", err)
				}
			}
		}
		for idx := range msgs {
			m := &msgs[idx]
			if m.BodyHash != nil {
				m.MessageContent = contents[*m.BodyHash]
			}
			if m.InitStateHash != nil {
				m.InitState = contents[*m.InitStateHash]
			}
		}
	}
	return msgs, nil
}

func queryTransactionsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]Transaction, error) {
	// transactions
	txs := []Transaction{}
	txs_map := map[string]int{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, err
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
		acsts_map := make(map[string]*AccountState)
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
		query = fmt.Sprintf("select * from messages where tx_hash in (%s)", hash_list_str)

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

// Methods
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
