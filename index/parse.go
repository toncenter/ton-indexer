package index

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// json marshaling and unmarshaling
func (s *ShardId) MarshalJSON() ([]byte, error) {
	res := fmt.Sprintf("\"%X\"", uint64(*s))
	return []byte(res), nil
}

func (a *AccountAddress) MarshalJSON() ([]byte, error) {
	return []byte("\"" + strings.Trim(string(*a), " ") + "\""), nil
}

func (h *HexInt) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"0x%X\"", *h)), nil
}

// Parsing
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
	return &BlockId{int32(workchain), ShardId(shard), int32(seqno)}, nil
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

// query to model
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

func ScanTransaction(row pgx.Row) (*Transaction, error) {
	var t Transaction
	t.OutMsgs = []*Message{}

	var st StoragePhase
	var cr CreditPhase
	var co ComputePhase
	var ac ActionPhase
	var bo BouncePhase
	var sp SplitInfo
	var ms1 MsgSize
	var ms2 MsgSize

	err := row.Scan(&t.Account, &t.Hash, &t.Lt, &t.Workchain, &t.Shard, &t.Seqno,
		&t.McSeqno, &t.TraceId, &t.PrevTransHash, &t.PrevTransLt, &t.Now,
		&t.OrigStatus, &t.EndStatus, &t.TotalFees, &t.AccountStateHashBefore,
		&t.AccountStateHashAfter, &t.Descr.Type, &t.Descr.Aborted, &t.Descr.Destroyed,
		&t.Descr.CreditFirst, &t.Descr.IsTock, &t.Descr.Installed,
		&st.StorageFeesCollected, &st.StorageFeesDue, &st.StatusChange,
		&cr.DueFeesCollected, &cr.Credit,
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
		&sp.CurShardPfxLen, &sp.AccSplitDepth, &sp.ThisAddr, &sp.SiblingAddr)

	if ms1.Cells != nil {
		ac.TotMsgSize = &ms1
	}

	if ms2.Cells != nil {
		bo.MsgSize = &ms2
	}

	if st.StatusChange != nil {
		t.Descr.StoragePh = &st
	}
	if cr.DueFeesCollected != nil {
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

	if err != nil {
		return nil, err
	}
	return &t, nil
}

func ScanMessage(row pgx.Row) (*Message, error) {
	var m Message
	err := row.Scan(&m.TxHash, &m.TxLt, &m.Hash, &m.Direction, &m.Source, &m.Destination,
		&m.Value, &m.FwdFee, &m.IhrFee, &m.CreatedLt, &m.CreatedAt, &m.Opcode,
		&m.IhrDisabled, &m.Bounce, &m.Bounced, &m.ImportFee, &m.BodyHash, &m.InitStateHash)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func ScanMessageContent(row pgx.Row) (*MessageContent, error) {
	var mc MessageContent
	var mcd *string
	err := row.Scan(&mc.Hash, &mc.Body, &mcd)
	if err != nil {
		return nil, err
	}
	if mcd != nil {
		mc.Decoded = &DecodedContent{Type: "text_comment", Comment: *mcd}
	}
	return &mc, nil
}
