package parse

import (
	"github.com/jackc/pgx/v5"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"strconv"
	"strings"
)

// Parsing
func ParseBlockId(str string) (*models.BlockId, error) {
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
	return &models.BlockId{int32(workchain), models.ShardId(shard), int32(seqno)}, nil
}

func ParseBlockIdList(str string) ([]models.BlockId, error) {
	str = strings.Trim(str, "{}")

	var result []models.BlockId
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

// query to model
func ScanBlock(row pgx.Row) (*models.Block, error) {
	var blk models.Block
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

func ScanMessageWithContent(row pgx.Row) (*models.Message, error) {
	var m models.Message
	var body models.MessageContent
	var init_state models.MessageContent

	err := row.Scan(&m.TxHash, &m.TxLt, &m.MsgHash, &m.Direction, &m.TraceId, &m.Source, &m.Destination,
		&m.Value, &m.ValueExtraCurrencies, &m.FwdFee, &m.IhrFee, &m.ExtraFlags, &m.CreatedLt, &m.CreatedAt, &m.Opcode,
		&m.IhrDisabled, &m.Bounce, &m.Bounced, &m.ImportFee, &m.BodyHash, &m.InitStateHash, &m.MsgHashNorm,
		&m.InMsgTxHash, &m.OutMsgTxHash, &body.Hash, &body.Body, &init_state.Hash, &init_state.Body)
	if body.Hash != nil {
		m.MessageContent = &body
	}
	if init_state.Hash != nil {
		m.InitState = &init_state
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func ScanMessageContent(row pgx.Row) (*models.MessageContent, error) {
	var mc models.MessageContent
	err := row.Scan(&mc.Hash, &mc.Body)
	if err != nil {
		return nil, err
	}
	return &mc, nil
}

func ScanTrace(row pgx.Row) (*models.Trace, error) {
	var trace models.Trace
	err := row.Scan(&trace.TraceId, &trace.ExternalHash, &trace.McSeqnoStart, &trace.McSeqnoEnd,
		&trace.StartLt, &trace.StartUtime, &trace.EndLt, &trace.EndUtime,
		&trace.TraceMeta.TraceState, &trace.TraceMeta.Messages, &trace.TraceMeta.Transactions,
		&trace.TraceMeta.PendingMessages, &trace.TraceMeta.ClassificationState)

	if err != nil {
		return nil, err
	}
	return &trace, nil
}
func ScanTransaction(row pgx.Row) (*models.Transaction, error) {
	var t models.Transaction
	t.OutMsgs = []*models.Message{}

	var st models.StoragePhase
	var cr models.CreditPhase
	var co models.ComputePhase
	var ac models.ActionPhase
	var bo models.BouncePhase
	var sp models.SplitInfo
	var ms1 models.MsgSize
	var ms2 models.MsgSize

	err := row.Scan(&t.Account, &t.Hash, &t.Lt, &t.Workchain, &t.Shard, &t.Seqno,
		&t.McSeqno, &t.TraceId, &t.PrevTransHash, &t.PrevTransLt, &t.Now,
		&t.OrigStatus, &t.EndStatus, &t.TotalFees, &t.TotalFeesExtraCurrencies, &t.AccountStateHashBefore,
		&t.AccountStateHashAfter, &t.Descr.Type, &t.Descr.Aborted, &t.Descr.Destroyed,
		&t.Descr.CreditFirst, &t.Descr.IsTock, &t.Descr.Installed,
		&st.StorageFeesCollected, &st.StorageFeesDue, &st.StatusChange,
		&cr.DueFeesCollected, &cr.Credit, &cr.CreditExtraCurrencies,
		&co.IsSkipped, &co.Reason, &co.Success, &co.MsgStateUsed,
		&co.AccountActivated, &co.GasFees, &co.GasUsed, &co.GasLimit,
		&co.GasCredit, &co.Mode, &co.ExitCode, &co.ExitArg,
		&co.VmSteps, &co.VmInitStateHash, &co.VmFinalStateHash,
		&ac.Success, &ac.Valid, &ac.NoFunds, &ac.StatusChange, &ac.TotalFwdFees,
		&ac.TotalActionFees, &ac.ResultCode, &ac.ResultArg, &ac.TotActions,
		&ac.SpecActions, &ac.SkippedActions, &ac.MsgsCreated, &ac.ActionListHash,
		&ms1.Cells, &ms1.Bits,
		&bo.Type, &ms2.Cells, &ms2.Bits,
		&bo.ReqFwdFees, &bo.MsgFees, &bo.FwdFees,
		&sp.CurShardPfxLen, &sp.AccSplitDepth, &sp.ThisAddr, &sp.SiblingAddr, &t.Emulated, &t.Finality)

	if err != nil {
		return nil, err
	}

	// fix for incorrectly inserted data - gas_fees and gas_used were swapped
	if co.GasFees != nil && co.GasUsed != nil && *co.GasFees < *co.GasUsed {
		co.GasFees, co.GasUsed = co.GasUsed, co.GasFees
	}

	t.BlockRef = models.BlockId{t.Workchain, t.Shard, t.Seqno}
	t.AccountStateAfter = &models.AccountState{Hash: t.AccountStateHashAfter}
	t.AccountStateBefore = &models.AccountState{Hash: t.AccountStateHashBefore}

	if ms1.Cells != nil {
		ac.TotMsgSize = &ms1
	}

	if ms2.Cells != nil {
		bo.MsgSize = &ms2
	}

	if st.StatusChange != nil {
		t.Descr.StoragePh = &st
	}
	if cr.DueFeesCollected != nil || cr.Credit != nil || cr.CreditExtraCurrencies != nil {
		t.Descr.CreditPh = &cr
	}
	if co.IsSkipped != nil {
		t.Descr.ComputePh = &co
	}
	if ac.Success != nil {
		t.Descr.Action = &ac
	}
	if bo.Type != nil {
		t.Descr.Bounce = &bo
	}
	if sp.CurShardPfxLen != nil {
		t.Descr.SplitInfo = &sp
	}

	return &t, nil
}
