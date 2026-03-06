package parse

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

func ParseBlockId(str string) (*BlockId, error) {
	str = strings.Trim(str, "()")
	parts := strings.Split(str, ",")
	var workchain int64
	var shard int64
	var seqno int64
	var err error
	if workchain, err = strconv.ParseInt(parts[0], 10, 32); err != nil {
		return nil, err
	}
	if shard, err = strconv.ParseInt(parts[1], 10, 64); err != nil {
		return nil, err
	}
	if seqno, err = strconv.ParseInt(parts[2], 10, 32); err != nil {
		return nil, err
	}
	return &BlockId{Workchain: int32(workchain), Shard: ShardId(shard), Seqno: int32(seqno)}, nil
}

func ParseBlockIdList(str string) ([]BlockId, error) {
	str = strings.Trim(str, "{}")

	var result []BlockId
	var start int
	for i, r := range str {
		switch r {
		case '(':
			start = i
		case ')':
			loc, err := ParseBlockId(str[start : i+1])
			if err != nil {
				return nil, err
			}
			result = append(result, *loc)
		}
	}
	return result, nil
}

func ScanBlock(row pgx.Row) (*Block, error) {
	var blk Block
	var prev_blocks_str string
	err := row.Scan(&blk.Workchain, &blk.Shard, &blk.Seqno, &blk.RootHash,
		&blk.FileHash, &blk.MasterchainBlockRef.Workchain,
		&blk.MasterchainBlockRef.Shard, &blk.MasterchainBlockRef.Seqno,
		&blk.GlobalId, &blk.Version, &blk.AfterMerge,
		&blk.BeforeSplit, &blk.AfterSplit, &blk.WantMerge, &blk.WantSplit,
		&blk.KeyBlock, &blk.VertSeqnoIncr, &blk.Flags, &blk.GenUtime,
		&blk.StartLt, &blk.EndLt, &blk.ValidatorListHashShort,
		&blk.GenCatchainSeqno, &blk.MinRefMcSeqno, &blk.PrevKeyBlockSeqno,
		&blk.VertSeqno, &blk.MasterRefSeqno, &blk.RandSeed, &blk.CreatedBy,
		&blk.TxCount, &prev_blocks_str)
	if err != nil {
		return nil, err
	}

	if prev_blocks, err := ParseBlockIdList(prev_blocks_str); err != nil {
		return nil, err
	} else {
		blk.PrevBlocks = prev_blocks
	}
	return &blk, nil
}
