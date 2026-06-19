package crud

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"
)

// blocksQueryParts holds the shared pieces of a blocks listing query
type blocksQueryParts struct {
	filterList []string
	orderBy    string
}

func buildBlocksParts(req models.BlocksRequest, sortOrder string) blocksQueryParts {
	utime_req := req.GetUtimeParams()
	lt_req := req.GetLtParams()
	filter_list := []string{}

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

	orderBy := ""
	if sortOrder != "" {
		orderBy = fmt.Sprintf(" order by gen_utime %s, -workchain %s, shard %s, seqno %s",
			sortOrder, sortOrder, sortOrder, sortOrder)
	}
	return blocksQueryParts{filterList: filter_list, orderBy: orderBy}
}

// blocksBoundaryFilters appends this leg's [floor, ceil) boundary on the sort
// axis (gen_utime, never null).
func blocksBoundaryFilters(p blocksQueryParts, floor, ceil *uint64) []string {
	filters := append([]string{}, p.filterList...)
	if floor != nil {
		filters = append(filters, fmt.Sprintf("gen_utime >= %d", *floor))
	}
	if ceil != nil {
		filters = append(filters, fmt.Sprintf("gen_utime < %d", *ceil))
	}
	return filters
}

func buildBlocksOffsetQuery(p blocksQueryParts, floor, ceil *uint64, offset, limit int) string {
	filter_query := ``
	if filters := blocksBoundaryFilters(p, floor, ceil); len(filters) > 0 {
		filter_query = ` where ` + strings.Join(filters, " and ")
	}
	limit_query := fmt.Sprintf(" limit %d offset %d", max(1, limit), max(0, offset))
	return `select blocks.* from blocks` + filter_query + p.orderBy + limit_query
}

func buildBlocksCountQuery(p blocksQueryParts, floor, ceil *uint64) string {
	filter_query := ``
	if filters := blocksBoundaryFilters(p, floor, ceil); len(filters) > 0 {
		filter_query = ` where ` + strings.Join(filters, " and ")
	}
	return `select count(*) from blocks` + filter_query
}

// query implementation functions
func queryBlocksImpl(query string, conn *pgxpool.Conn, settings models.RequestSettings) ([]models.Block, error) {
	// blocks
	blks := []models.Block{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
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
				return nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
	}
	return blks, nil
}

func queryBlockExists(seqno int32, conn *pgxpool.Conn, settings models.RequestSettings) (bool, error) {
	query := fmt.Sprintf(`select seqno from blocks where workchain = -1 and shard = -9223372036854775808 and seqno = %d`, seqno)
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return false, models.IndexError{Code: 500, Message: err.Error()}
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
		return false, models.IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return len(seqnos) > 0, nil
}

// Exported methods
func (db *DbClient) QueryMasterchainInfo(settings models.RequestSettings) (*models.MasterchainInfo, error) {
	fc, release, err := db.acquireFed(context.Background())
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer release()

	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()

	const lastQuery = "select * from blocks where workchain = -1 order by seqno desc limit 1"
	const firstQuery = "select * from blocks where workchain = -1 order by seqno asc limit 1"

	// newest masterchain block lives on hot, oldest on cold (standalone: same pool)
	hotConn, err := fc.hot()
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	last, err := parse.ScanBlock(hotConn.QueryRow(ctx, lastQuery))
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	coldConn, err := fc.cold()
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	first, err := parse.ScanBlock(coldConn.QueryRow(ctx, firstQuery))
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	info := models.MasterchainInfo{Last: last, First: first}
	return &info, nil
}

func (db *DbClient) QueryBlocks(
	req models.BlocksRequest,
	settings models.RequestSettings,
) ([]models.Block, error) {
	lim_req := req.GetLimitParams()
	sortOrder := "" // "" => no ORDER BY (preserves the original arbitrary order for unsorted listings)
	if v := lim_req.Sort; v != nil {
		var serr error
		sortOrder, serr = getSortOrder(*v)
		if serr != nil {
			return nil, serr
		}
	}
	offset := 0
	if lim_req.Offset != nil {
		offset = int(max(0, *lim_req.Offset))
	}
	limit := int32(settings.DefaultLimit)
	if lim_req.Limit != nil {
		limit = max(1, *lim_req.Limit)
		if limit > int32(settings.MaxLimit) {
			return nil, models.IndexError{Code: 422, Message: fmt.Sprintf("limit is not allowed: %d > %d", limit, settings.MaxLimit)}
		}
	}

	fc, release, err := db.acquireFed(context.Background())
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer release()

	// A federated listing must be ordered to merge hot+cold
	effSort := sortOrder
	if fc.federated && effSort == "" {
		effSort = "desc"
	}
	parts := buildBlocksParts(req, effSort)
	fetch := func(query string, conn *pgxpool.Conn) ([]models.Block, error) {
		if settings.DebugRequest {
			log.Println("Debug query:", query)
		}
		return queryBlocksImpl(query, conn, settings)
	}

	// A specific masterchain round lives wholly in one partition.
	if req.McSeqno != nil {
		conn, cerr := fc.connForSeqno(uint64(*req.McSeqno))
		if cerr != nil {
			return nil, models.IndexError{Code: 500, Message: cerr.Error()}
		}
		return fetch(buildBlocksOffsetQuery(parts, nil, nil, offset, int(limit)), conn)
	}

	return cascadePageOffset(fc, effSort, offset, int(limit),
		func(floor, ceil *uint64, off, lim int) (string, error) {
			return buildBlocksOffsetQuery(parts, floor, ceil, off, lim), nil
		},
		func(floor, ceil *uint64) string { return buildBlocksCountQuery(parts, floor, ceil) },
		fetch,
		func(query string, conn *pgxpool.Conn) (int, error) { return queryCount(query, conn, settings) },
		"utime",
	)
}

func (db *DbClient) QueryShards(
	req models.ShardsRequest,
	settings models.RequestSettings,
) ([]models.Block, error) {
	query := fmt.Sprintf(`select B.* from shard_state as S join blocks as B
		on S.workchain = B.workchain and S.shard = B.shard and S.seqno = B.seqno
		where mc_seqno = %d
		order by S.mc_seqno, S.workchain, S.shard, S.seqno`, req.Seqno)

	// a masterchain round and its shards live wholly in one partition
	fc, release, err := db.acquireFed(context.Background())
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer release()
	conn, err := fc.connForSeqno(uint64(req.Seqno))
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	return queryBlocksImpl(query, conn, settings)
}
