package crud

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"

	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func buildBlocksQuery(
	req BlocksRequest,
	settings RequestSettings,
) (string, error) {
	utime_req := req.GetUtimeParams()
	lt_req := req.GetLtParams()
	lim_req := req.GetLimitParams()

	query := `select blocks.* from blocks`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	// filters
	if v := req.Workchain; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("workchain = %d", *v))
	}
	if v := req.Shard; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("shard = %d", *v))
	}
	if v := req.Seqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("seqno = %d", *v))
	}
	if v := req.RootHash; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("root_hash = '%s'", v.FilterString()))
	}
	if v := req.FileHash; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("file_hash = '%s'", v.FilterString()))
	}
	if v := req.McSeqno; v != nil {
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
		sort_order, err := getSortOrder(*v)
		if err != nil {
			return "", err
		}
		orderby_query = fmt.Sprintf(" order by %s %s, -workchain %s, shard %s, seqno %s",
			order_col, sort_order, sort_order, sort_order, sort_order)
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

func queryBlocksImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]Block, error) {
	// blocks
	blks := []Block{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		default:
		}
		defer rows.Close()

		for rows.Next() {
			if blk, err := parse.ScanBlock(rows); err == nil {
				blks = append(blks, *blk)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
	}
	return blks, nil
}

func queryBlockExists(seqno int32, conn *pgxpool.Conn, settings RequestSettings) (bool, error) {
	query := fmt.Sprintf(`select seqno from blocks where workchain = -1 and shard = -9223372036854775808 and seqno = %d`, seqno)
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return false, IndexError{Code: 500, Message: err.Error()}
	}

	seqnos := []int32{}
	for rows.Next() {
		var s int32
		if err := rows.Scan(&s); err != nil {
			return false, err
		}
		seqnos = append(seqnos, s)
	}
	if rows.Err() != nil {
		return false, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return len(seqnos) > 0, nil
}

func (db *DbClient) QueryMasterchainInfo(settings RequestSettings) (*MasterchainInfo, error) {
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	res := conn.QueryRow(ctx, "select * from blocks where workchain = -1 order by seqno desc limit 1")
	last, err := parse.ScanBlock(res)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	res = conn.QueryRow(ctx, "select * from blocks where workchain = -1 order by seqno asc limit 1")
	first, err := parse.ScanBlock(res)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	info := MasterchainInfo{Last: last, First: first}
	return &info, nil
}

func (db *DbClient) QueryBlocks(
	req BlocksRequest,
	settings RequestSettings,
) ([]Block, error) {
	query, err := buildBlocksQuery(req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query)
	}
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()
	return queryBlocksImpl(query, conn, settings)
}

func (db *DbClient) QueryShards(
	req ShardsRequest,
	settings RequestSettings,
) ([]Block, error) {
	query := fmt.Sprintf(`select B.* from shard_state as S join blocks as B 
		on S.workchain = B.workchain and S.shard = B.shard and S.seqno = B.seqno 
		where mc_seqno = %d 
		order by S.mc_seqno, S.workchain, S.shard, S.seqno`, req.Seqno)
	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()
	return queryBlocksImpl(query, conn, settings)
}
