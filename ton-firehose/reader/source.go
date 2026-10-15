package reader

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	pbton "github.com/toncenter/ton-indexer/ton-firehose/pb/sf/ton/type/v1"
	"github.com/toncenter/ton-indexer/ton-index-go/index/crud"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const BlockTypeURL = "type.googleapis.com/sf.ton.type.v1.Block"

// Used as the parent id of the first streamable block, whose parent
// (the zerostate or a block below the indexed history) is not in the database.
var unknownParentId = strings.Repeat("0", 64)

// BlockSource yields finalized masterchain rounds as Firehose blocks.
type BlockSource interface {
	// Head returns the highest masterchain seqno that is complete in the source.
	Head(ctx context.Context) (uint64, error)
	Block(ctx context.Context, mcSeqno uint64) (*pbbstream.Block, error)
}

type Config struct {
	PgDsn           string
	PgHotDsn        string
	MaxConns        int
	HotColdSplitTTL time.Duration
	// Nil when message contents and account states are in Postgres.
	Kvrocks      *crud.KvrocksConfig
	QueryTimeout time.Duration
	// Transactions are read in pages of this size.
	PageSize             int
	FirstStreamableBlock uint64
}

// Source reads blocks through the ton-index-go crud layer, so a Firehose
// block carries exactly what API v3 returns for the same masterchain seqno,
// including hot/cold routing and Kvrocks-held payloads.
type Source struct {
	db                   *crud.DbClient
	settings             models.RequestSettings
	pageSize             int
	firstStreamableBlock uint64
}

var _ BlockSource = (*Source)(nil)

func NewSource(cfg Config) (*Source, error) {
	var kvrocks *crud.KvrocksStore
	if cfg.Kvrocks != nil {
		var err error
		kvrocks, err = crud.NewKvrocksStore(*cfg.Kvrocks)
		if err != nil {
			return nil, fmt.Errorf("connecting to kvrocks: %w", err)
		}
	}
	db, err := crud.NewDbClient(cfg.PgDsn, cfg.MaxConns, 0, kvrocks)
	if err != nil {
		return nil, err
	}
	if cfg.PgHotDsn != "" {
		if err := db.AttachHotPool(cfg.PgHotDsn, cfg.MaxConns, 0, cfg.HotColdSplitTTL); err != nil {
			return nil, fmt.Errorf("connecting to hot postgres: %w", err)
		}
	}
	return &Source{
		db: db,
		settings: models.RequestSettings{
			Timeout:       cfg.QueryTimeout,
			DefaultLimit:  cfg.PageSize,
			MaxLimit:      cfg.PageSize,
			NoAddressBook: true,
			NoMetadata:    true,
			// Decoded bodies and opcodes are API-only fields, not in the block.
			NoDecode: true,
		},
		pageSize:             cfg.PageSize,
		firstStreamableBlock: cfg.FirstStreamableBlock,
	}, nil
}

func (s *Source) Close() {
	if s.db.Kvrocks != nil {
		s.db.Kvrocks.Close()
	}
	if s.db.HotPool != nil {
		s.db.HotPool.Close()
	}
	s.db.Pool.Close()
}

// Head is the Postgres finalized_mc_seqno, capped by the Kvrocks watermark:
// after a failover Postgres may be re-indexed ahead of Kvrocks.
func (s *Source) Head(ctx context.Context) (uint64, error) {
	// The worker keeps its progress in the database it writes to.
	pool := s.db.Pool
	if s.db.HotPool != nil {
		pool = s.db.HotPool
	}
	var head int64
	err := pool.QueryRow(ctx, "select finalized_mc_seqno from _ton_indexer_progress where id = 1").Scan(&head)
	if err != nil {
		return 0, fmt.Errorf("reading finalized_mc_seqno: %w", err)
	}
	if s.db.Kvrocks != nil {
		watermark, ok, err := s.db.Kvrocks.FinalizedMcSeqno(ctx)
		if err != nil {
			return 0, fmt.Errorf("reading kvrocks watermark: %w", err)
		}
		if !ok {
			return 0, fmt.Errorf("kvrocks watermark is not initialized")
		}
		head = min(head, watermark)
	}
	return uint64(max(head, 0)), nil
}

func (s *Source) Block(ctx context.Context, mcSeqno uint64) (*pbbstream.Block, error) {
	blk, err := s.TonBlock(mcSeqno)
	if err != nil {
		return nil, err
	}
	parentId, err := s.parentId(mcSeqno)
	if err != nil {
		return nil, err
	}
	return toBstreamBlock(blk, parentId)
}

// TonBlock reads masterchain round mcSeqno the way /api/v3/masterchainBlockShards,
// /api/v3/masterchainBlockShardState and /api/v3/transactionsByMasterchainBlock do.
func (s *Source) TonBlock(mcSeqno uint64) (*pbton.Block, error) {
	seqno := int32(mcSeqno)
	blocksReq := models.BlocksRequest{McSeqno: &seqno}
	limit := int32(s.pageSize)
	blocksReq.Limit = &limit
	blocks, err := s.db.QueryBlocks(blocksReq, s.settings)
	if err != nil {
		return nil, fmt.Errorf("querying blocks of %d: %w", mcSeqno, err)
	}
	if len(blocks) >= s.pageSize {
		return nil, fmt.Errorf("masterchain block %d has at least %d blocks, raise the page size", mcSeqno, len(blocks))
	}
	shards, err := s.db.QueryShards(models.ShardsRequest{Seqno: seqno}, s.settings)
	if err != nil {
		return nil, fmt.Errorf("querying shard state of %d: %w", mcSeqno, err)
	}
	txs, err := s.transactions(seqno)
	if err != nil {
		return nil, fmt.Errorf("querying transactions of %d: %w", mcSeqno, err)
	}
	return buildBlock(uint32(mcSeqno), blocks, shards, txs)
}

func (s *Source) transactions(mcSeqno int32) ([]models.Transaction, error) {
	sort := models.ASC
	limit := int32(s.pageSize)
	var all []models.Transaction
	for {
		offset := int32(len(all))
		req := models.TransactionsRequest{McSeqno: &mcSeqno}
		req.Sort = &sort
		req.Limit = &limit
		req.Offset = &offset
		page, _, err := s.db.QueryTransactions(req, s.settings)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		if len(page) < s.pageSize {
			return all, nil
		}
	}
}

func (s *Source) parentId(mcSeqno uint64) (string, error) {
	if mcSeqno == 0 || mcSeqno == s.firstStreamableBlock {
		return unknownParentId, nil
	}
	parent := int32(mcSeqno - 1)
	workchain := int32(-1)
	blocks, err := s.db.QueryBlocks(models.BlocksRequest{McSeqno: &parent, Workchain: &workchain}, s.settings)
	if err != nil {
		return "", fmt.Errorf("querying masterchain block %d: %w", parent, err)
	}
	if len(blocks) != 1 {
		return "", fmt.Errorf("masterchain block %d not found", parent)
	}
	c := &converter{}
	rootHash := c.hash(blocks[0].RootHash)
	if c.err != nil {
		return "", c.err
	}
	return hex.EncodeToString(rootHash), nil
}

// toBstreamBlock wraps a TON block with the Firehose metadata: number is the
// masterchain seqno, id is its root hash in lowercase hex, and every block is
// final as soon as it is committed to the masterchain.
func toBstreamBlock(blk *pbton.Block, parentId string) (*pbbstream.Block, error) {
	mc := blk.MasterchainBlock
	// Deterministic: the same round always serializes to the same bytes.
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(blk)
	if err != nil {
		return nil, fmt.Errorf("marshalling block %d: %w", mc.Seqno, err)
	}
	num := uint64(mc.Seqno)
	parentNum := uint64(0)
	if num > 0 {
		parentNum = num - 1
	}
	return &pbbstream.Block{
		Number:    num,
		Id:        hex.EncodeToString(mc.RootHash),
		ParentId:  parentId,
		ParentNum: parentNum,
		LibNum:    num,
		Timestamp: timestamppb.New(time.Unix(int64(mc.GenUtime), 0)),
		Payload:   &anypb.Any{TypeUrl: BlockTypeURL, Value: payload},
	}, nil
}
