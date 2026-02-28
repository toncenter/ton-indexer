package parse

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"

	"github.com/jackc/pgx/v5"
)

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

	t.BlockRef = BlockId{Workchain: t.Workchain, Shard: t.Shard, Seqno: t.Seqno}
	t.AccountStateAfter = &AccountState{Hash: t.AccountStateHashAfter}
	t.AccountStateBefore = &AccountState{Hash: t.AccountStateHashBefore}

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
