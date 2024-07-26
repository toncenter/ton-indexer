package index

import (
	b64 "encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// json marshaling and unmarshaling
func (v *ShardId) String() string {
	return fmt.Sprintf("%X", uint64(*v))
}

func (v *ShardId) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

func (v *AccountAddress) String() string {
	return strings.Trim(string(*v), " ")
}

func (v *AccountAddress) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

func (v *HexInt) String() string {
	return fmt.Sprintf("0x%x", uint32(*v))
}

func (v *HexInt) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

func (v *OpcodeType) String() string {
	return fmt.Sprintf("0x%08x", uint32(*v))
}

func (v *OpcodeType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

// converters
func HashConverter(value string) reflect.Value {
	if len(value) == 64 {
		if res, err := hex.DecodeString(value); err == nil {
			return reflect.ValueOf(HashType(b64.StdEncoding.EncodeToString(res)))
		} else {
			return reflect.Value{}
		}
	}
	if len(value) == 44 {
		if res, err := b64.StdEncoding.DecodeString(value); err == nil {
			return reflect.ValueOf(HashType(b64.StdEncoding.EncodeToString(res)))
		} else if res, err := b64.URLEncoding.DecodeString(value); err == nil {
			return reflect.ValueOf(HashType(b64.StdEncoding.EncodeToString(res)))
		} else {
			return reflect.Value{}
		}
	}
	return reflect.Value{}
}

func AccountAddressConverter(value string) reflect.Value {
	addr, err := address.ParseAddr(value)
	if err != nil {
		addr, err = address.ParseRawAddr(value)
	}
	if err != nil {
		return reflect.Value{}
	}
	addr_str := fmt.Sprintf("%d:%s", addr.Workchain(), strings.ToUpper(hex.EncodeToString(addr.Data())))
	return reflect.ValueOf(addr_str)
}

func AccountAddressNullableConverter(value string) reflect.Value {
	if value == "null" {
		return reflect.ValueOf(value)
	}
	return AccountAddressConverter(value)
}

func ShardIdConverter(value string) reflect.Value {
	if shard, err := strconv.ParseUint(value, 16, 64); err == nil {
		return reflect.ValueOf(ShardId(shard))
	}
	if shard, err := strconv.ParseInt(value, 10, 64); err == nil {
		return reflect.ValueOf(ShardId(shard))
	}
	return reflect.Value{}
}

func OpcodeTypeConverter(value string) reflect.Value {
	value = strings.TrimPrefix(value, "0x")
	if res, err := strconv.ParseUint(value, 16, 32); err == nil {
		return reflect.ValueOf(OpcodeType(res))
	}
	if res, err := strconv.ParseInt(value, 10, 32); err == nil {
		return reflect.ValueOf(OpcodeType(res))
	}
	return reflect.Value{}
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
	t.BlockRef = BlockId{t.Workchain, t.Shard, t.Seqno}
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
	err := row.Scan(&m.TxHash, &m.TxLt, &m.MsgHash, &m.Direction, &m.TraceId, &m.Source, &m.Destination,
		&m.Value, &m.FwdFee, &m.IhrFee, &m.CreatedLt, &m.CreatedAt, &m.Opcode,
		&m.IhrDisabled, &m.Bounce, &m.Bounced, &m.ImportFee, &m.BodyHash, &m.InitStateHash)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (mc *MessageContent) TryDecodeBody() error {
	if mc.Body == nil {
		return errors.New("empty MessageContent")
	}
	if boc, err := b64.StdEncoding.DecodeString(*mc.Body); err == nil {
		if c, err := cell.FromBOC(boc); err == nil {
			l := c.BeginParse()
			if val, err := l.LoadUInt(32); err == nil && val == 0 {
				str, _ := l.LoadStringSnake()
				mc.Decoded = &DecodedContent{Type: "text_comment", Comment: str}
			}
		}
	}
	return nil
}

func ScanMessageWithContent(row pgx.Row) (*Message, error) {
	var m Message
	var body MessageContent
	var init_state MessageContent

	err := row.Scan(&m.TxHash, &m.TxLt, &m.MsgHash, &m.Direction, &m.TraceId, &m.Source, &m.Destination,
		&m.Value, &m.FwdFee, &m.IhrFee, &m.CreatedLt, &m.CreatedAt, &m.Opcode,
		&m.IhrDisabled, &m.Bounce, &m.Bounced, &m.ImportFee, &m.BodyHash, &m.InitStateHash,
		&body.Hash, &body.Body, &init_state.Hash, &init_state.Body)
	if body.Hash != nil {
		body.TryDecodeBody()
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

func ScanMessageContent(row pgx.Row) (*MessageContent, error) {
	var mc MessageContent
	err := row.Scan(&mc.Hash, &mc.Body)
	mc.TryDecodeBody()
	if err != nil {
		return nil, err
	}
	return &mc, nil
}

func ScanAccountState(row pgx.Row) (*AccountState, error) {
	var acst AccountState
	err := row.Scan(&acst.Hash, &acst.Account, &acst.Balance, &acst.AccountStatus,
		&acst.FrozenHash, &acst.DataHash, &acst.CodeHash)
	if err != nil {
		return nil, err
	}
	return &acst, nil
}

func ScanAccountStateFull(row pgx.Row) (*AccountStateFull, error) {
	var acst AccountStateFull
	err := row.Scan(&acst.AccountAddress, &acst.Hash, &acst.Balance, &acst.AccountStatus,
		&acst.FrozenHash, &acst.LastTransactionHash, &acst.LastTransactionLt, &acst.DataHash,
		&acst.CodeHash, &acst.DataBoc, &acst.CodeBoc)
	if err != nil {
		return nil, err
	}
	return &acst, nil
}

func ScanNFTCollection(row pgx.Row) (*NFTCollection, error) {
	var res NFTCollection
	err := row.Scan(&res.Address, &res.NextItemIndex, &res.OwnerAddress, &res.CollectionContent,
		&res.DataHash, &res.CodeHash, &res.LastTransactionLt)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanNFTItem(row pgx.Row) (*NFTItem, error) {
	var res NFTItem
	err := row.Scan(&res.Address, &res.Init, &res.Index, &res.CollectionAddress,
		&res.OwnerAddress, &res.Content, &res.LastTransactionLt, &res.CodeHash, &res.DataHash)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanNFTItemWithCollection(row pgx.Row) (*NFTItem, error) {
	var res NFTItem
	var col NFTCollection

	err := row.Scan(&res.Address, &res.Init, &res.Index, &res.CollectionAddress,
		&res.OwnerAddress, &res.Content, &res.LastTransactionLt, &res.CodeHash, &res.DataHash,
		&col.Address, &col.NextItemIndex, &col.OwnerAddress, &col.CollectionContent,
		&col.DataHash, &col.CodeHash, &col.LastTransactionLt)
	res.Collection = &col
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanNFTTransfer(row pgx.Row) (*NFTTransfer, error) {
	var res NFTTransfer
	err := row.Scan(&res.TransactionHash, &res.TransactionLt, &res.TransactionNow, &res.TransactionAborted,
		&res.QueryId, &res.NftItemAddress, &res.NftItemIndex, &res.NftCollectionAddress,
		&res.OldOwner, &res.NewOwner, &res.ResponseDestination, &res.CustomPayload,
		&res.ForwardAmount, &res.ForwardPayload, &res.TraceId)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanJettonMaster(row pgx.Row) (*JettonMaster, error) {
	var res JettonMaster
	err := row.Scan(&res.Address, &res.TotalSupply, &res.Mintable, &res.AdminAddress,
		&res.JettonContent, &res.JettonWalletCodeHash, &res.CodeHash, &res.DataHash,
		&res.LastTransactionLt)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanJettonWallet(row pgx.Row) (*JettonWallet, error) {
	var res JettonWallet
	err := row.Scan(&res.Address, &res.Balance, &res.Owner, &res.Jetton, &res.LastTransactionLt,
		&res.CodeHash, &res.DataHash)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanJettonTransfer(row pgx.Row) (*JettonTransfer, error) {
	var res JettonTransfer
	err := row.Scan(&res.TransactionHash, &res.TransactionLt, &res.TransactionNow, &res.TransactionAborted,
		&res.QueryId, &res.Amount, &res.Source, &res.Destination, &res.SourceWallet, &res.JettonMaster,
		&res.ResponseDestination, &res.CustomPayload, &res.ForwardTonAmount, &res.ForwardPayload, &res.TraceId)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanJettonBurn(row pgx.Row) (*JettonBurn, error) {
	var res JettonBurn
	err := row.Scan(&res.TransactionHash, &res.TransactionLt, &res.TransactionNow, &res.TransactionAborted,
		&res.QueryId, &res.Owner, &res.JettonWallet, &res.JettonMaster, &res.Amount,
		&res.ResponseDestination, &res.CustomPayload, &res.TraceId)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanAction(row pgx.Row) (*Action, error) {
	var act Action
	err := row.Scan(&act.TraceId, &act.ActionId, &act.StartLt, &act.EndLt, &act.StartUtime, &act.EndUtime,
		&act.Source, &act.SourceSecondary, &act.Destination, &act.DestinationSecondary,
		&act.Asset, &act.AssetSecondary, &act.Asset2, &act.Asset2Secondary,
		&act.Opcode, &act.TxHashes, &act.Type, &act.Value, &act.Success,
		&act.TonTransferContent, &act.TonTransferEncrypted,
		&act.JettonTransferResponseAddress, &act.JettonTransferForwardAmount, &act.JettonTransferQueryId,
		&act.NFTTransferIsPurchase, &act.NFTTransferPrice, &act.NFTTransferQueryId,
		&act.JettonSwapDex, &act.JettonSwapAmountIn, &act.JettonSwapAmountOut, &act.JettonSwapPeerSwaps,
		&act.ChangeDNSRecordKey, &act.ChangeDNSRecordValueSchema, &act.ChangeDNSRecordValue, &act.ChangeDNSRecordFlags)

	if err != nil {
		return nil, err
	}
	return &act, nil
}
