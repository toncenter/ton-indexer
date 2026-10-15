package reader

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"

	pbton "github.com/toncenter/ton-indexer/ton-firehose/pb/sf/ton/type/v1"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

// buildBlock assembles the masterchain round mcSeqno from the API v3 models
// returned for it: blocks with this mc_seqno, its shard state and transactions.
func buildBlock(mcSeqno uint32, blocks []models.Block, shards []models.Block, txs []models.Transaction) (*pbton.Block, error) {
	c := &converter{}
	out := &pbton.Block{}
	txCount := uint32(0)
	for i := range blocks {
		b := c.blockHeader(&blocks[i])
		txCount += b.TxCount
		if b.Workchain == -1 {
			if b.Seqno != mcSeqno {
				return nil, fmt.Errorf("masterchain block %d listed for mc_seqno %d", b.Seqno, mcSeqno)
			}
			out.MasterchainBlock = b
		} else {
			out.ShardBlocks = append(out.ShardBlocks, b)
		}
	}
	if out.MasterchainBlock == nil {
		return nil, fmt.Errorf("masterchain block %d not found", mcSeqno)
	}
	sort.Slice(out.ShardBlocks, func(i, j int) bool {
		return blockIdLess(blockHeaderId(out.ShardBlocks[i]), blockHeaderId(out.ShardBlocks[j]))
	})

	for i := range shards {
		out.Shards = append(out.Shards, blockHeaderId(c.blockHeader(&shards[i])))
	}
	sort.Slice(out.Shards, func(i, j int) bool { return blockIdLess(out.Shards[i], out.Shards[j]) })

	for i := range txs {
		if uint32(txs[i].McSeqno) != mcSeqno {
			return nil, fmt.Errorf("transaction %s has mc_seqno %d, expected %d", txs[i].Hash, txs[i].McSeqno, mcSeqno)
		}
		out.Transactions = append(out.Transactions, c.transaction(&txs[i]))
	}
	if c.err != nil {
		return nil, fmt.Errorf("masterchain block %d: %w", mcSeqno, c.err)
	}
	// A short read would be published as a final block and never corrected.
	if uint32(len(out.Transactions)) != txCount {
		return nil, fmt.Errorf("masterchain block %d: read %d transactions, blocks report %d", mcSeqno, len(out.Transactions), txCount)
	}
	return out, nil
}

func blockHeaderId(b *pbton.BlockHeader) *pbton.BlockId {
	return &pbton.BlockId{Workchain: b.Workchain, Shard: b.Shard, Seqno: b.Seqno}
}

func blockIdLess(a, b *pbton.BlockId) bool {
	if a.Workchain != b.Workchain {
		return a.Workchain < b.Workchain
	}
	if a.Shard != b.Shard {
		return a.Shard < b.Shard
	}
	return a.Seqno < b.Seqno
}

// converter keeps the first conversion error so that the field mappings
// below stay plain assignments.
type converter struct {
	err error
}

func (c *converter) fail(format string, args ...any) {
	if c.err == nil {
		c.err = fmt.Errorf(format, args...)
	}
}

// API hashes and BOCs are standard base64.
func (c *converter) base64(v string) []byte {
	b, err := base64.StdEncoding.DecodeString(v)
	if err != nil {
		c.fail("decoding base64 %q: %w", v, err)
	}
	return b
}

func (c *converter) hash(h models.HashType) []byte {
	return c.base64(string(h))
}

func (c *converter) hashPtr(h *models.HashType) []byte {
	if h == nil {
		return nil
	}
	return c.hash(*h)
}

func (c *converter) currencies(m map[string]string) map[uint32]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[uint32]string, len(m))
	for k, v := range m {
		id, err := strconv.ParseUint(k, 10, 32)
		if err != nil {
			c.fail("parsing extra currency id %q: %w", k, err)
		}
		out[uint32(id)] = v
	}
	return out
}

type integer interface {
	~int32 | ~int64 | ~uint32 | ~uint64
}

func convPtr[U, T integer](v *T) *U {
	if v == nil {
		return nil
	}
	u := U(*v)
	return &u
}

func addressPtr(a *models.AccountAddress) *string {
	if a == nil {
		return nil
	}
	s := string(*a)
	return &s
}

func (c *converter) blockId(b models.BlockId) *pbton.BlockId {
	return &pbton.BlockId{Workchain: b.Workchain, Shard: uint64(b.Shard), Seqno: uint32(b.Seqno)}
}

func (c *converter) blockHeader(b *models.Block) *pbton.BlockHeader {
	out := &pbton.BlockHeader{
		Workchain:              b.Workchain,
		Shard:                  uint64(b.Shard),
		Seqno:                  uint32(b.Seqno),
		RootHash:               c.hash(b.RootHash),
		FileHash:               c.hash(b.FileHash),
		GlobalId:               b.GlobalId,
		Version:                uint32(b.Version),
		AfterMerge:             b.AfterMerge,
		BeforeSplit:            b.BeforeSplit,
		AfterSplit:             b.AfterSplit,
		WantMerge:              b.WantMerge,
		WantSplit:              b.WantSplit,
		KeyBlock:               b.KeyBlock,
		VertSeqnoIncr:          b.VertSeqnoIncr,
		Flags:                  uint32(b.Flags),
		GenUtime:               uint32(b.GenUtime),
		StartLt:                uint64(b.StartLt),
		EndLt:                  uint64(b.EndLt),
		ValidatorListHashShort: uint32(b.ValidatorListHashShort),
		GenCatchainSeqno:       uint32(b.GenCatchainSeqno),
		MinRefMcSeqno:          uint32(b.MinRefMcSeqno),
		PrevKeyBlockSeqno:      uint32(b.PrevKeyBlockSeqno),
		VertSeqno:              uint32(b.VertSeqno),
		MasterRefSeqno:         uint32(b.MasterRefSeqno),
		RandSeed:               c.hash(b.RandSeed),
		CreatedBy:              c.hash(b.CreatedBy),
		TxCount:                uint32(b.TxCount),
		MasterchainBlockRef:    c.blockId(b.MasterchainBlockRef),
	}
	for _, p := range b.PrevBlocks {
		out.PrevBlocks = append(out.PrevBlocks, c.blockId(p))
	}
	return out
}

func (c *converter) transaction(t *models.Transaction) *pbton.Transaction {
	out := &pbton.Transaction{
		Account:                  string(t.Account),
		Hash:                     c.hash(t.Hash),
		Lt:                       uint64(t.Lt),
		Now:                      uint32(t.Now),
		McBlockSeqno:             uint32(t.McSeqno),
		TraceId:                  c.hashPtr(t.TraceId),
		PrevTransHash:            c.hash(t.PrevTransHash),
		PrevTransLt:              uint64(t.PrevTransLt),
		OrigStatus:               t.OrigStatus,
		EndStatus:                t.EndStatus,
		TotalFees:                uint64(t.TotalFees),
		TotalFeesExtraCurrencies: c.currencies(t.TotalFeesExtraCurrencies),
		Description:              c.descr(&t.Descr),
		BlockRef:                 c.blockId(t.BlockRef),
		AccountStateBefore:       c.accountState(t.AccountStateBefore),
		AccountStateAfter:        c.accountState(t.AccountStateAfter),
	}
	if t.InMsg != nil {
		out.InMsg = c.message(t.InMsg)
	}
	for _, m := range t.OutMsgs {
		out.OutMsgs = append(out.OutMsgs, c.message(m))
	}
	return out
}

func (c *converter) accountState(s *models.AccountState) *pbton.AccountState {
	if s == nil {
		return nil
	}
	out := &pbton.AccountState{
		Hash:            c.hash(s.Hash),
		ExtraCurrencies: c.currencies(s.BalanceExtraCurrencies),
		AccountStatus:   s.AccountStatus,
		FrozenHash:      c.hashPtr(s.FrozenHash),
		DataHash:        c.hashPtr(s.DataHash),
		CodeHash:        c.hashPtr(s.CodeHash),
	}
	if s.Balance != nil {
		balance, err := strconv.ParseUint(*s.Balance, 10, 64)
		if err != nil {
			c.fail("parsing balance of account state %s: %w", s.Hash, err)
		}
		out.Balance = &balance
	}
	return out
}

func (c *converter) message(m *models.Message) *pbton.Message {
	return &pbton.Message{
		Hash:                 c.hash(m.MsgHash),
		Source:               addressPtr(m.Source),
		Destination:          addressPtr(m.Destination),
		Value:                convPtr[uint64](m.Value),
		ValueExtraCurrencies: c.currencies(m.ValueExtraCurrencies),
		FwdFee:               m.FwdFee,
		IhrFee:               m.IhrFee,
		ExtraFlags:           m.ExtraFlags,
		CreatedLt:            m.CreatedLt,
		CreatedAt:            m.CreatedAt,
		Opcode:               convPtr[uint32](m.Opcode),
		IhrDisabled:          m.IhrDisabled,
		Bounce:               m.Bounce,
		Bounced:              m.Bounced,
		ImportFee:            m.ImportFee,
		MessageContent:       c.content(m, "body", m.BodyHash, m.MessageContent),
		InitState:            c.content(m, "init_state", m.InitStateHash, m.InitState),
		HashNorm:             c.hashPtr(m.MsgHashNorm),
	}
}

// content fails on a referenced but unloaded BOC: blocks are immutable once
// published, so a missing payload must stop the reader rather than ship.
func (c *converter) content(m *models.Message, what string, hash *models.HashType, content *models.MessageContent) *pbton.MessageContent {
	if hash == nil {
		return nil
	}
	if content == nil || content.Body == nil {
		c.fail("message %s: %s %s is not found", m.MsgHash, what, *hash)
		return nil
	}
	return &pbton.MessageContent{Hash: c.hash(*hash), Body: c.base64(string(*content.Body))}
}

func (c *converter) descr(d *models.TransactionDescr) *pbton.TransactionDescr {
	out := &pbton.TransactionDescr{
		Type:        d.Type,
		Aborted:     d.Aborted,
		Destroyed:   d.Destroyed,
		CreditFirst: d.CreditFirst,
		IsTock:      d.IsTock,
		Installed:   d.Installed,
	}
	if p := d.StoragePh; p != nil {
		out.StoragePh = &pbton.StoragePhase{
			StorageFeesCollected: convPtr[uint64](p.StorageFeesCollected),
			StorageFeesDue:       convPtr[uint64](p.StorageFeesDue),
			StatusChange:         p.StatusChange,
		}
	}
	if p := d.CreditPh; p != nil {
		out.CreditPh = &pbton.CreditPhase{
			DueFeesCollected:      convPtr[uint64](p.DueFeesCollected),
			Credit:                convPtr[uint64](p.Credit),
			CreditExtraCurrencies: c.currencies(p.CreditExtraCurrencies),
		}
	}
	if p := d.ComputePh; p != nil {
		out.ComputePh = &pbton.ComputePhase{
			Skipped:          p.IsSkipped,
			Reason:           p.Reason,
			Success:          p.Success,
			MsgStateUsed:     p.MsgStateUsed,
			AccountActivated: p.AccountActivated,
			GasFees:          convPtr[uint64](p.GasFees),
			GasUsed:          convPtr[uint64](p.GasUsed),
			GasLimit:         convPtr[uint64](p.GasLimit),
			GasCredit:        convPtr[uint64](p.GasCredit),
			Mode:             p.Mode,
			ExitCode:         p.ExitCode,
			ExitArg:          p.ExitArg,
			VmSteps:          p.VmSteps,
			VmInitStateHash:  c.hashPtr(p.VmInitStateHash),
			VmFinalStateHash: c.hashPtr(p.VmFinalStateHash),
		}
	}
	if p := d.Action; p != nil {
		out.Action = &pbton.ActionPhase{
			Success:         p.Success,
			Valid:           p.Valid,
			NoFunds:         p.NoFunds,
			StatusChange:    p.StatusChange,
			TotalFwdFees:    convPtr[uint64](p.TotalFwdFees),
			TotalActionFees: convPtr[uint64](p.TotalActionFees),
			ResultCode:      p.ResultCode,
			ResultArg:       p.ResultArg,
			TotActions:      convPtr[uint32](p.TotActions),
			SpecActions:     convPtr[uint32](p.SpecActions),
			SkippedActions:  convPtr[uint32](p.SkippedActions),
			MsgsCreated:     convPtr[uint32](p.MsgsCreated),
			ActionListHash:  c.hashPtr(p.ActionListHash),
			TotMsgSize:      msgSize(p.TotMsgSize),
		}
	}
	if p := d.Bounce; p != nil {
		out.Bounce = &pbton.BouncePhase{
			Type:       p.Type,
			MsgSize:    msgSize(p.MsgSize),
			ReqFwdFees: convPtr[uint64](p.ReqFwdFees),
			MsgFees:    convPtr[uint64](p.MsgFees),
			FwdFees:    convPtr[uint64](p.FwdFees),
		}
	}
	if p := d.SplitInfo; p != nil {
		out.SplitInfo = &pbton.SplitInfo{
			CurShardPfxLen: convPtr[uint32](p.CurShardPfxLen),
			AccSplitDepth:  convPtr[uint32](p.AccSplitDepth),
			ThisAddr:       c.hashPtr(p.ThisAddr),
			SiblingAddr:    c.hashPtr(p.SiblingAddr),
		}
	}
	return out
}

func msgSize(s *models.MsgSize) *pbton.MsgSize {
	if s == nil {
		return nil
	}
	return &pbton.MsgSize{Cells: convPtr[uint64](s.Cells), Bits: convPtr[uint64](s.Bits)}
}
