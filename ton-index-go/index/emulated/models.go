package emulated

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type AccountStatus int

const (
	AccountStatusUninit AccountStatus = iota
	AccountStatusFrozen
	AccountStatusActive
	AccountStatusNonexist
)

func (d AccountStatus) Str() (string, error) {
	switch d {
	case AccountStatusUninit:
		return "uninit", nil
	case AccountStatusFrozen:
		return "frozen", nil
	case AccountStatusActive:
		return "active", nil
	case AccountStatusNonexist:
		return "nonexist", nil
	default:
		return "", fmt.Errorf("unknown account status: %d", d)
	}
}

type AccStatusChange int

const (
	AccStatusUnchanged AccStatusChange = iota
	AccStatusFrozen
	AccStatusDeleted
)

func (d AccStatusChange) Str() (string, error) {
	switch d {
	case AccStatusUnchanged:
		return "unchanged", nil
	case AccStatusFrozen:
		return "frozen", nil
	case AccStatusDeleted:
		return "deleted", nil
	default:
		return "", fmt.Errorf("unknown account status change: %d", d)
	}
}

type ComputeSkipReason int

const (
	ComputeSkipNoState ComputeSkipReason = iota
	ComputeSkipBadState
	ComputeSkipNoGas
	ComputeSkipSuspended
)

func (d ComputeSkipReason) Str() (string, error) {
	switch d {
	case ComputeSkipNoState:
		return "no_state", nil
	case ComputeSkipBadState:
		return "bad_state", nil
	case ComputeSkipNoGas:
		return "no_gas", nil
	case ComputeSkipSuspended:
		return "suspended", nil
	default:
		return "", fmt.Errorf("unknown compute skip reason: %d", d)
	}
}

type trStoragePhase struct {
	StorageFeesCollected *int64          `msgpack:"storage_fees_collected" json:"storage_fees_collected"`
	StorageFeesDue       *int64          `msgpack:"storage_fees_due" json:"storage_fees_due"`
	StatusChange         AccStatusChange `msgpack:"status_change" json:"status_change"`
}

type trCreditPhase struct {
	DueFeesCollected *int64 `msgpack:"due_fees_collected" json:"due_fees_collected"`
	Credit           *int64 `msgpack:"credit" json:"credit"`
}

type trComputePhaseSkipped struct {
	Reason ComputeSkipReason `msgpack:"reason" json:"reason"`
}

type trComputePhaseVm struct {
	Success          bool   `msgpack:"success" json:"success"`
	MsgStateUsed     bool   `msgpack:"msg_state_used" json:"msg_state_used"`
	AccountActivated bool   `msgpack:"account_activated" json:"account_activated"`
	GasFees          *int64 `msgpack:"gas_fees" json:"gas_fees"`
	GasUsed          *int64 `msgpack:"gas_used" json:"gas_used"`
	GasLimit         *int64 `msgpack:"gas_limit" json:"gas_limit"`
	GasCredit        *int64 `msgpack:"gas_credit" json:"gas_credit"`
	Mode             *int16 `msgpack:"mode" json:"mode"`
	ExitCode         int32  `msgpack:"exit_code" json:"exit_code"`
	ExitArg          *int32 `msgpack:"exit_arg" json:"exit_arg"`
	VmSteps          *int64 `msgpack:"vm_steps" json:"vm_steps"`
	VmInitStateHash  hash   `msgpack:"vm_init_state_hash" json:"vm_init_state_hash"`
	VmFinalStateHash hash   `msgpack:"vm_final_state_hash" json:"vm_final_state_hash"`
}

type storageUsedShort struct {
	Cells *int64 `msgpack:"cells" json:"cells"`
	Bits  *int64 `msgpack:"bits" json:"bits"`
}

type trActionPhase struct {
	Success         bool             `msgpack:"success" json:"success"`
	Valid           bool             `msgpack:"valid" json:"valid"`
	NoFunds         bool             `msgpack:"no_funds" json:"no_funds"`
	StatusChange    AccStatusChange  `msgpack:"status_change" json:"status_change"`
	TotalFwdFees    *int64           `msgpack:"total_fwd_fees" json:"total_fwd_fees"`
	TotalActionFees *int64           `msgpack:"total_action_fees" json:"total_action_fees"`
	ResultCode      *int32           `msgpack:"result_code" json:"result_code"`
	ResultArg       *int32           `msgpack:"result_arg" json:"result_arg"`
	TotActions      *int32           `msgpack:"tot_actions" json:"tot_actions"`
	SpecActions     *int32           `msgpack:"spec_actions" json:"spec_actions"`
	SkippedActions  *int32           `msgpack:"skipped_actions" json:"skipped_actions"`
	MsgsCreated     *int32           `msgpack:"msgs_created" json:"msgs_created"`
	ActionListHash  hash             `msgpack:"action_list_hash" json:"action_list_hash"`
	TotMsgSize      storageUsedShort `msgpack:"tot_msg_size" json:"tot_msg_size"`
}

type trBouncePhaseNegfunds struct {
	Dummy bool `msgpack:"dummy" json:"dummy"`
}

type trBouncePhaseNofunds struct {
	MsgSize    storageUsedShort `msgpack:"msg_size" json:"msg_size"`
	ReqFwdFees *int64           `msgpack:"req_fwd_fees" json:"req_fwd_fees"`
}

type trBouncePhaseOk struct {
	MsgSize storageUsedShort `msgpack:"msg_size" json:"msg_size"`
	MsgFees *int64           `msgpack:"msg_fees" json:"msg_fees"`
	FwdFees *int64           `msgpack:"fwd_fees" json:"fwd_fees"`
}

type trMessage struct {
	Hash         hash    `msgpack:"hash" json:"hash"`
	Source       *string `msgpack:"source" json:"source"`
	Destination  *string `msgpack:"destination" json:"destination"`
	Value        *int64  `msgpack:"value" json:"value"`
	FwdFee       *int64  `msgpack:"fwd_fee" json:"fwd_fee"`
	IhrFee       *int64  `msgpack:"ihr_fee" json:"ihr_fee"`
	CreatedLt    *int64  `msgpack:"created_lt" json:"created_lt"`
	CreatedAt    *int64  `msgpack:"created_at" json:"created_at"`
	Opcode       *int32  `msgpack:"opcode" json:"opcode"`
	IhrDisabled  *bool   `msgpack:"ihr_disabled" json:"ihr_disabled"`
	Bounce       *bool   `msgpack:"bounce" json:"bounce"`
	Bounced      *bool   `msgpack:"bounced" json:"bounced"`
	ImportFee    *int64  `msgpack:"import_fee" json:"import_fee"`
	BodyBoc      string  `msgpack:"body_boc" json:"body_boc"`
	InitStateBoc *string `msgpack:"init_state_boc" json:"init_state_boc"`
}

type transactionDescr struct {
	CreditFirst bool            `msgpack:"credit_first" json:"credit_first"`
	StoragePh   trStoragePhase  `msgpack:"storage_ph" json:"storage_ph"`
	CreditPh    trCreditPhase   `msgpack:"credit_ph" json:"credit_ph"`
	ComputePh   computePhaseVar `msgpack:"compute_ph" json:"compute_ph"`
	Action      *trActionPhase  `msgpack:"action" json:"action"`
	Aborted     bool            `msgpack:"aborted" json:"aborted"`
	Bounce      *BouncePhaseVar `msgpack:"bounce" json:"bounce"`
	Destroyed   bool            `msgpack:"destroyed" json:"destroyed"`
}

type transaction struct {
	Hash                   hash             `msgpack:"hash" json:"-"`
	Account                string           `msgpack:"account" json:"account"`
	Lt                     uint64           `msgpack:"lt" json:"lt,string"`
	PrevTransHash          hash             `msgpack:"prev_trans_hash" json:"prev_trans_hash"`
	PrevTransLt            int64            `msgpack:"prev_trans_lt" json:"prev_trans_lt,string"`
	Now                    int32            `msgpack:"now" json:"now"`
	OrigStatus             AccountStatus    `msgpack:"orig_status" json:"orig_status"`
	EndStatus              AccountStatus    `msgpack:"end_status" json:"end_status"`
	InMsg                  *trMessage       `msgpack:"in_msg" json:"in_msg"`
	OutMsgs                []trMessage      `msgpack:"out_msgs" json:"out_msgs"`
	TotalFees              *int64           `msgpack:"total_fees" json:"total_fees"`
	AccountStateHashBefore hash             `msgpack:"account_state_hash_before" json:"account_state_hash_before"`
	AccountStateHashAfter  hash             `msgpack:"account_state_hash_after" json:"account_state_hash_after"`
	Description            transactionDescr `msgpack:"description" json:"description"`
}

type actionTonTransferDetails struct {
	Content   *string `msgpack:"content"`
	Encrypted bool    `msgpack:"encrypted"`
}

type actionJettonTransferDetails struct {
	ResponseDestination *string `msgpack:"response_destination"`
	ForwardAmount       *string `msgpack:"forward_amount"`
	QueryId             *string `msgpack:"query_id"`
	CustomPayload       *string `msgpack:"custom_payload"`
	ForwardPayload      *string `msgpack:"forward_payload"`
	Comment             *string `msgpack:"comment"`
	IsEncryptedComment  *bool   `msgpack:"is_encrypted_comment"`
}

type actionNftTransferDetails struct {
	IsPurchase          bool    `msgpack:"is_purchase"`
	Price               *string `msgpack:"price"`
	QueryId             *string `msgpack:"query_id"`
	CustomPayload       *string `msgpack:"custom_payload"`
	ForwardPayload      *string `msgpack:"forward_payload"`
	ForwardAmount       *string `msgpack:"forward_amount"`
	ResponseDestination *string `msgpack:"response_destination"`
	NftItemIndex        *string `msgpack:"nft_item_index"`
}

type actionDexTransferDetails struct {
	Amount                  *string `msgpack:"amount"`
	Asset                   *string `msgpack:"asset"`
	Source                  *string `msgpack:"source"`
	Destination             *string `msgpack:"destination"`
	SourceJettonWallet      *string `msgpack:"source_jetton_wallet"`
	DestinationJettonWallet *string `msgpack:"destination_jetton_wallet"`
}

type actionPeerSwapDetails struct {
	AssetIn   *string `msgpack:"asset_in"`
	AssetOut  *string `msgpack:"asset_out"`
	AmountIn  *string `msgpack:"amount_in"`
	AmountOut *string `msgpack:"amount_out"`
}

type actionJettonSwapDetails struct {
	Dex                 *string                  `msgpack:"dex"`
	Sender              *string                  `msgpack:"sender"`
	DexIncomingTransfer actionDexTransferDetails `msgpack:"dex_incoming_transfer"`
	DexOutgoingTransfer actionDexTransferDetails `msgpack:"dex_outgoing_transfer"`
	PeerSwaps           []actionPeerSwapDetails  `msgpack:"peer_swaps"`
}

type actionChangeDnsRecordDetails struct {
	Key         *string `msgpack:"key"`
	ValueSchema *string `msgpack:"value_schema"`
	Value       *string `msgpack:"value"`
	Flags       *string `msgpack:"flags"`
}

type actionNftMintDetails struct {
	NftItemIndex *string `msgpack:"nft_item_index"`
}

type actionDexDepositLiquidityData struct {
	Dex               *string `msgpack:"dex"`
	Amount1           *string `msgpack:"amount1"`
	Amount2           *string `msgpack:"amount2"`
	Asset1            *string `msgpack:"asset1"`
	Asset2            *string `msgpack:"asset2"`
	UserJettonWallet1 *string `msgpack:"user_jetton_wallet_1"`
	UserJettonWallet2 *string `msgpack:"user_jetton_wallet_2"`
	LpTokensMinted    *string `msgpack:"lp_tokens_minted"`
}

type actionDexWithdrawLiquidityData struct {
	Dex               *string `msgpack:"dex"`
	Amount1           *string `msgpack:"amount1"`
	Amount2           *string `msgpack:"amount2"`
	Asset1Out         *string `msgpack:"asset1_out"`
	Asset2Out         *string `msgpack:"asset2_out"`
	UserJettonWallet1 *string `msgpack:"user_jetton_wallet_1"`
	UserJettonWallet2 *string `msgpack:"user_jetton_wallet_2"`
	DexJettonWallet1  *string `msgpack:"dex_jetton_wallet_1"`
	DexJettonWallet2  *string `msgpack:"dex_jetton_wallet_2"`
	LpTokensBurnt     *string `msgpack:"lp_tokens_burnt"`
}

type actionStakingData struct {
	Provider *string `msgpack:"provider"`
	TsNft    *string `msgpack:"ts_nft"`
}

type Action struct {
	ActionId                 string                          `msgpack:"action_id"`
	Type                     string                          `msgpack:"type"`
	TraceId                  string                          `msgpack:"trace_id"`
	TxHashes                 []string                        `msgpack:"tx_hashes"`
	Value                    *string                         `msgpack:"value"`
	Amount                   *string                         `msgpack:"amount"`
	StartLt                  *int64                          `msgpack:"start_lt"`
	EndLt                    *int64                          `msgpack:"end_lt"`
	StartUtime               *int64                          `msgpack:"start_utime"`
	EndUtime                 *int64                          `msgpack:"end_utime"`
	Source                   *string                         `msgpack:"source"`
	SourceSecondary          *string                         `msgpack:"source_secondary"`
	Destination              *string                         `msgpack:"destination"`
	DestinationSecondary     *string                         `msgpack:"destination_secondary"`
	Asset                    *string                         `msgpack:"asset"`
	AssetSecondary           *string                         `msgpack:"asset_secondary"`
	Asset2                   *string                         `msgpack:"asset2"`
	Asset2Secondary          *string                         `msgpack:"asset2_secondary"`
	Opcode                   *uint32                         `msgpack:"opcode"`
	Success                  bool                            `msgpack:"success"`
	TonTransferData          *actionTonTransferDetails       `msgpack:"ton_transfer_data"`
	JettonTransferData       *actionJettonTransferDetails    `msgpack:"jetton_transfer_data"`
	NftTransferData          *actionNftTransferDetails       `msgpack:"nft_transfer_data"`
	JettonSwapData           *actionJettonSwapDetails        `msgpack:"jetton_swap_data"`
	ChangeDnsRecordData      *actionChangeDnsRecordDetails   `msgpack:"change_dns_record_data"`
	NftMintData              *actionNftMintDetails           `msgpack:"nft_mint_data"`
	DexDepositLiquidityData  *actionDexDepositLiquidityData  `msgpack:"dex_deposit_liquidity_data"`
	DexWithdrawLiquidityData *actionDexWithdrawLiquidityData `msgpack:"dex_withdraw_liquidity_data"`
	StakingData              *actionStakingData              `msgpack:"staking_data"`
}

type Trace struct {
	TraceId    string
	Nodes      []traceNode
	Classified bool
	Actions    []Action
}
type traceNode struct {
	Transaction transaction `msgpack:"transaction"`
	Emulated    bool        `msgpack:"emulated"`
	TraceId     string
	Key         string
}

type computePhaseVar struct {
	Type uint8
	Data interface{} // Can be trComputePhaseSkipped or trComputePhaseVm
}

var _ msgpack.CustomDecoder = (*computePhaseVar)(nil)

func (bpv *computePhaseVar) DecodeMsgpack(dec *msgpack.Decoder) error {
	length, err := dec.DecodeArrayLen()
	if err != nil {
		return err
	}
	if length != 2 {
		return fmt.Errorf("invalid variant array length: %d", length)
	}

	index, err := dec.DecodeUint8()
	if err != nil {
		return err
	}

	switch index {
	case 0:
		var a trComputePhaseSkipped
		err = dec.Decode(&a)
		bpv.Data = a
	case 1:
		var b trComputePhaseVm
		err = dec.Decode(&b)
		bpv.Data = b
	default:
		return fmt.Errorf("unknown variant index: %d", index)
	}

	bpv.Type = index
	return err
}

type BouncePhaseVar struct {
	Type uint8
	Data interface{} // Can be trBouncePhaseNegfunds, trBouncePhaseNofunds or trBouncePhaseOk
}

var _ msgpack.CustomDecoder = (*BouncePhaseVar)(nil)

func (s *BouncePhaseVar) DecodeMsgpack(dec *msgpack.Decoder) error {
	length, err := dec.DecodeArrayLen()
	if err != nil {
		return err
	}
	if length != 2 {
		return fmt.Errorf("invalid variant array length: %d", length)
	}

	fmt.Println(length)

	index, err := dec.DecodeUint8()
	if err != nil {
		return err
	}

	switch index {
	case 0:
		var a trBouncePhaseNegfunds
		err = dec.Decode(&a)
		s.Data = a
	case 1:
		var b trBouncePhaseNofunds
		err = dec.Decode(&b)
		s.Data = b
	case 2:
		var c trBouncePhaseOk
		err = dec.Decode(&c)
		s.Data = c
	default:
		return fmt.Errorf("unknown variant index: %d", index)
	}

	s.Type = index
	return err
}

type AccountState struct {
	Hash          hash    `msgpack:"hash" json:"-"`
	Balance       uint64  `msgpack:"balance" json:"balance"`
	AccountStatus string  `msgpack:"account_status" json:"account_status"`
	FrozenHash    *hash   `msgpack:"frozen_hash" json:"frozen_hash"`
	CodeHash      *hash   `msgpack:"code_hash" json:"code_hash"`
	DataBoc       *string `msgpack:"data_boc" json:"data_boc"`
	DataHash      *hash   `msgpack:"data_hash" json:"data_hash"`
	LastTransHash *hash   `msgpack:"last_trans_hash" json:"last_trans_hash"`
	LastTransLt   *uint64 `msgpack:"last_trans_lt" json:"last_trans_lt"`
	Timestamp     *uint32 `msgpack:"timestamp" json:"timestamp"`
}

type hash [32]byte

func (h hash) MarshalText() (data []byte, err error) {
	return []byte(base64.StdEncoding.EncodeToString(h[:])), nil
}

// MarshalJSON implements json.Marshaler interface
func (h hash) MarshalJSON() ([]byte, error) {
	return json.Marshal(base64.StdEncoding.EncodeToString(h[:]))
}

// EncodeMsgpack implements msgpack.CustomEncoder interface
func (h hash) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.EncodeBytes(h[:])
}

// DecodeMsgpack implements msgpack.CustomDecoder interface
func (h *hash) DecodeMsgpack(dec *msgpack.Decoder) error {
	bytes, err := dec.DecodeBytes()
	if err != nil {
		return err
	}

	if len(bytes) != 32 {
		return fmt.Errorf("invalid hash length: expected 32 bytes, got %d", len(bytes))
	}

	copy(h[:], bytes)
	return nil
}
func ConvertHSet(traceHash map[string]string, traceId string) (Trace, error) {

	queue := make([]string, 0)

	queue = append(queue, traceId)
	txs := make([]traceNode, 0)
	actions := make([]Action, 0)
	if actionsBytes, exists := traceHash["actions"]; exists {
		err := msgpack.Unmarshal([]byte(actionsBytes), &actions)
		if err != nil {
			return Trace{}, fmt.Errorf("failed to unmarshal actions: %w", err)
		}
	}
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		var node traceNode
		nodeData, exists := traceHash[key]
		if !exists {
			return Trace{}, fmt.Errorf("key %s not found in trace", key)
		}
		nodeBytes := []byte(nodeData)
		err := msgpack.Unmarshal(nodeBytes, &node)
		node.Key = key
		node.TraceId = traceId
		txs = append(txs, node)

		if err != nil {
			return Trace{}, fmt.Errorf("failed to unmarshal node: %w", err)
		}
		for _, outMsg := range node.Transaction.OutMsgs {
			// bytes to hex string
			nextKey := base64.StdEncoding.EncodeToString(outMsg.Hash[:])
			if _, exists := traceHash[nextKey]; exists {
				queue = append(queue, nextKey)
			}
		}
	}
	_, has_actions := traceHash["actions"]
	return Trace{
		TraceId:    traceId,
		Nodes:      txs,
		Classified: has_actions,
		Actions:    actions,
	}, nil
}

func (h hash) Hex() *string {
	str := hex.EncodeToString(h[:])
	return &str
}

func (n *traceNode) GetTransactionRow() (TransactionRow, error) {
	origStatus, err := n.Transaction.OrigStatus.Str()
	if err != nil {
		return TransactionRow{}, err
	}
	endStatus, err := n.Transaction.EndStatus.Str()
	if err != nil {
		return TransactionRow{}, err
	}
	ord_val := "ord"
	is_tock := false
	storageStatusChange, err := n.Transaction.Description.StoragePh.StatusChange.Str()
	if err != nil {
		return TransactionRow{}, err
	}

	txRow := TransactionRow{
		Account:                  n.Transaction.Account,
		Hash:                     base64.StdEncoding.EncodeToString(n.Transaction.Hash[:]),
		Lt:                       int64(n.Transaction.Lt),
		BlockWorkchain:           nil,
		BlockShard:               nil,
		BlockSeqno:               nil,
		McBlockSeqno:             nil,
		TraceID:                  &n.TraceId,
		PrevTransHash:            n.Transaction.PrevTransHash.Hex(),
		PrevTransLt:              &n.Transaction.PrevTransLt,
		Now:                      &n.Transaction.Now,
		OrigStatus:               &origStatus,
		EndStatus:                &endStatus,
		TotalFees:                n.Transaction.TotalFees,
		TotalFeesExtraCurrencies: nil,
		AccountStateHashBefore:   n.Transaction.AccountStateHashBefore.Hex(),
		AccountStateHashAfter:    n.Transaction.AccountStateHashAfter.Hex(),
		Descr:                    &ord_val,
		Aborted:                  &n.Transaction.Description.Aborted,
		Destroyed:                &n.Transaction.Description.Destroyed,
		CreditFirst:              &n.Transaction.Description.CreditFirst,
		IsTock:                   &is_tock,
		Installed:                &is_tock,
		StorageFeesCollected:     n.Transaction.Description.StoragePh.StorageFeesCollected,
		StorageFeesDue:           n.Transaction.Description.StoragePh.StorageFeesDue,
		StorageStatusChange:      &storageStatusChange,
		CreditDueFeesCollected:   n.Transaction.Description.CreditPh.DueFeesCollected,
		Credit:                   n.Transaction.Description.CreditPh.Credit,
		CreditExtraCurrencies:    nil,
		Emulated:                 n.Emulated,
	}
	if n.Transaction.Description.ComputePh.Type == 0 {
		txRow.ComputeSkipped = new(bool)
		*txRow.ComputeSkipped = true
		reason, err := n.Transaction.Description.ComputePh.Data.(trComputePhaseSkipped).Reason.Str()
		if err != nil {
			return TransactionRow{}, err
		}
		txRow.SkippedReason = &reason
	} else {
		txRow.ComputeSkipped = new(bool)
		*txRow.ComputeSkipped = false
		vm := n.Transaction.Description.ComputePh.Data.(trComputePhaseVm)
		txRow.ComputeSuccess = &vm.Success
		txRow.ComputeMsgStateUsed = &vm.MsgStateUsed
		txRow.ComputeAccountActivated = &vm.AccountActivated
		txRow.ComputeGasFees = vm.GasFees
		txRow.ComputeGasUsed = vm.GasUsed
		txRow.ComputeGasLimit = vm.GasLimit
		txRow.ComputeGasCredit = vm.GasCredit
		txRow.ComputeMode = vm.Mode
		txRow.ComputeExitCode = &vm.ExitCode
		txRow.ComputeExitArg = vm.ExitArg
		txRow.ComputeVmSteps = vm.VmSteps
		txRow.ComputeVmInitStateHash = vm.VmInitStateHash.Hex()
		txRow.ComputeVmFinalStateHash = vm.VmFinalStateHash.Hex()
	}

	if n.Transaction.Description.Action != nil {
		actionStatusChange, err := n.Transaction.Description.Action.StatusChange.Str()
		if err != nil {
			return TransactionRow{}, err
		}
		txRow.ActionSuccess = &n.Transaction.Description.Action.Success
		txRow.ActionValid = &n.Transaction.Description.Action.Valid
		txRow.ActionNoFunds = &n.Transaction.Description.Action.NoFunds
		txRow.ActionStatusChange = &actionStatusChange
		txRow.ActionTotalFwdFees = n.Transaction.Description.Action.TotalFwdFees
		txRow.ActionTotalActionFees = n.Transaction.Description.Action.TotalActionFees
		txRow.ActionResultCode = n.Transaction.Description.Action.ResultCode
		txRow.ActionResultArg = n.Transaction.Description.Action.ResultArg
		txRow.ActionTotActions = n.Transaction.Description.Action.TotActions
		txRow.ActionSpecActions = n.Transaction.Description.Action.SpecActions
		txRow.ActionSkippedActions = n.Transaction.Description.Action.SkippedActions
		txRow.ActionMsgsCreated = n.Transaction.Description.Action.MsgsCreated
		txRow.ActionActionListHash = n.Transaction.Description.Action.ActionListHash.Hex()
		txRow.ActionTotMsgSizeCells = n.Transaction.Description.Action.TotMsgSize.Cells
		txRow.ActionTotMsgSizeBits = n.Transaction.Description.Action.TotMsgSize.Bits
	}

	if n.Transaction.Description.Bounce != nil {
		if n.Transaction.Description.Bounce.Type == 0 {
			txRow.Bounce = new(string)
			*txRow.Bounce = "negfunds"
		} else if n.Transaction.Description.Bounce.Type == 1 {
			txRow.Bounce = new(string)
			*txRow.Bounce = "nofunds"
			phase := n.Transaction.Description.Bounce.Data.(trBouncePhaseNofunds)
			txRow.BounceMsgSizeCells = phase.MsgSize.Cells
			txRow.BounceMsgSizeBits = phase.MsgSize.Bits
			txRow.BounceReqFwdFees = phase.ReqFwdFees
		} else {
			txRow.Bounce = new(string)
			*txRow.Bounce = "ok"
			phase := n.Transaction.Description.Bounce.Data.(trBouncePhaseOk)
			txRow.BounceMsgSizeCells = phase.MsgSize.Cells
			txRow.BounceMsgSizeBits = phase.MsgSize.Bits
			txRow.BounceMsgFees = phase.MsgFees
			txRow.BounceFwdFees = phase.FwdFees
		}
	}
	return txRow, nil
}
func calcHash(boc string) (string, error) {
	decodedBody, err := base64.StdEncoding.DecodeString(boc)
	if err != nil {
		return "", err
	}
	msg_cell, err := cell.FromBOC(decodedBody)
	if err != nil {
		return "", err
	}
	hash := msg_cell.Hash()
	// bytes to
	hash_b64 := base64.StdEncoding.EncodeToString(hash[:])
	return hash_b64, nil
}

func (m *trMessage) GetMessageRow(traceId string, direction string, txLt int64, txHash string) (row MessageRow, body *MessageContentRow, initState *MessageContentRow, err error) {
	bodyHash, err := calcHash(m.BodyBoc)
	if err != nil {
		return MessageRow{}, nil, nil, err
	}
	body = &MessageContentRow{
		Hash: bodyHash,
		Body: &m.BodyBoc,
	}
	var initStateHash *string = nil
	if m.InitStateBoc != nil {
		h, err := calcHash(*m.InitStateBoc)
		initStateHash = &h
		if err != nil {
			return MessageRow{}, nil, nil, err
		}
		initState = &MessageContentRow{
			Hash: bodyHash,
			Body: m.InitStateBoc,
		}
	}
	msgRow := MessageRow{
		TxHash:               txHash,
		TxLt:                 txLt,
		MsgHash:              base64.StdEncoding.EncodeToString(m.Hash[:]),
		Direction:            direction,
		TraceID:              &traceId,
		Source:               m.Source,
		Destination:          m.Destination,
		Value:                m.Value,
		ValueExtraCurrencies: nil,
		FwdFee:               m.FwdFee,
		IhrFee:               m.IhrFee,
		CreatedLt:            m.CreatedLt,
		CreatedAt:            m.CreatedAt,
		Opcode:               m.Opcode,
		IhrDisabled:          m.IhrDisabled,
		Bounce:               m.Bounce,
		Bounced:              m.Bounced,
		ImportFee:            m.ImportFee,
		BodyHash:             &bodyHash,
		InitStateHash:        initStateHash,
	}
	return msgRow, body, initState, nil
}

func (n *traceNode) GetMessages() ([]MessageRow, map[string]MessageContentRow, map[string]MessageContentRow, error) {
	messageContents := make(map[string]MessageContentRow)
	initStates := make(map[string]MessageContentRow)
	messages := make([]MessageRow, 0)

	for _, outMsg := range n.Transaction.OutMsgs {
		msgRow, body, initState, err := outMsg.GetMessageRow(n.TraceId, "out", int64(n.Transaction.Lt),
			base64.StdEncoding.EncodeToString(n.Transaction.Hash[:]))
		if err != nil {
			return nil, nil, nil, err
		}
		messages = append(messages, msgRow)
		if body != nil {
			messageContents[msgRow.MsgHash] = *body
		}
		if initState != nil {
			initStates[*msgRow.InitStateHash] = *initState
		}
	}
	inMsgRow, body, initState, err := n.Transaction.InMsg.GetMessageRow(n.TraceId, "in", int64(n.Transaction.Lt),
		base64.StdEncoding.EncodeToString(n.Transaction.Hash[:]))
	if err != nil {
		return nil, nil, nil, err
	}
	messages = append(messages, inMsgRow)
	if body != nil {
		messageContents[inMsgRow.MsgHash] = *body
	}
	if initState != nil {
		initStates[*inMsgRow.InitStateHash] = *initState
	}
	return messages, messageContents, nil, nil
}

func (a *Action) GetActionRow() (ActionRow, error) {
	row := ActionRow{
		ActionId:             a.ActionId,
		Type:                 a.Type,
		TraceId:              a.TraceId,
		TxHashes:             a.TxHashes,
		Value:                a.Value,
		Amount:               a.Amount,
		StartLt:              *a.StartLt,
		EndLt:                *a.EndLt,
		StartUtime:           *a.StartUtime,
		EndUtime:             *a.EndUtime,
		Source:               a.Source,
		SourceSecondary:      a.SourceSecondary,
		Destination:          a.Destination,
		DestinationSecondary: a.DestinationSecondary,
		Asset:                a.Asset,
		AssetSecondary:       a.AssetSecondary,
		Asset2:               a.Asset2,
		Asset2Secondary:      a.Asset2Secondary,
		Opcode:               a.Opcode,
		Success:              a.Success,
		TraceExternalHash:    &a.TraceId,
	}
	if a.TonTransferData != nil {
		row.TonTransferContent = a.TonTransferData.Content
		row.TonTransferEncrypted = &a.TonTransferData.Encrypted
	}
	if a.JettonTransferData != nil {
		row.JettonTransferResponseDestination = a.JettonTransferData.ResponseDestination
		row.JettonTransferForwardAmount = a.JettonTransferData.ForwardAmount
		row.JettonTransferQueryId = a.JettonTransferData.QueryId
		row.JettonTransferCustomPayload = a.JettonTransferData.CustomPayload
		row.JettonTransferForwardPayload = a.JettonTransferData.ForwardPayload
		row.JettonTransferComment = a.JettonTransferData.Comment
		row.JettonTransferIsEncryptedComment = a.JettonTransferData.IsEncryptedComment
	}
	if a.NftTransferData != nil {
		row.NFTTransferIsPurchase = &a.NftTransferData.IsPurchase
		row.NFTTransferPrice = a.NftTransferData.Price
		row.NFTTransferQueryId = a.NftTransferData.QueryId
		row.NFTTransferCustomPayload = a.NftTransferData.CustomPayload
		row.NFTTransferForwardPayload = a.NftTransferData.ForwardPayload
		row.NFTTransferForwardAmount = a.NftTransferData.ForwardAmount
		row.NFTTransferResponseDestination = a.NftTransferData.ResponseDestination
		row.NFTTransferNFTItemIndex = a.NftTransferData.NftItemIndex
	}
	if a.JettonSwapData != nil {
		row.JettonSwapDex = a.JettonSwapData.Dex
		row.JettonSwapSender = a.JettonSwapData.Sender
		row.JettonSwapDexIncomingTransferAmount = a.JettonSwapData.DexIncomingTransfer.Amount
		row.JettonSwapDexIncomingTransferAsset = a.JettonSwapData.DexIncomingTransfer.Asset
		row.JettonSwapDexIncomingTransferSource = a.JettonSwapData.DexIncomingTransfer.Source
		row.JettonSwapDexIncomingTransferDestination = a.JettonSwapData.DexIncomingTransfer.Destination
		row.JettonSwapDexIncomingTransferSourceJettonWallet = a.JettonSwapData.DexIncomingTransfer.SourceJettonWallet
		row.JettonSwapDexIncomingTransferDestinationJettonWallet = a.JettonSwapData.DexIncomingTransfer.DestinationJettonWallet
		row.JettonSwapDexOutgoingTransferAmount = a.JettonSwapData.DexOutgoingTransfer.Amount
		row.JettonSwapDexOutgoingTransferAsset = a.JettonSwapData.DexOutgoingTransfer.Asset
		row.JettonSwapDexOutgoingTransferSource = a.JettonSwapData.DexOutgoingTransfer.Source
		row.JettonSwapDexOutgoingTransferDestination = a.JettonSwapData.DexOutgoingTransfer.Destination
		row.JettonSwapDexOutgoingTransferSourceJettonWallet = a.JettonSwapData.DexOutgoingTransfer.SourceJettonWallet
		row.JettonSwapDexOutgoingTransferDestinationJettonWallet = a.JettonSwapData.DexOutgoingTransfer.DestinationJettonWallet

		row.JettonSwapPeerSwaps = a.JettonSwapData.PeerSwaps
	}
	if a.ChangeDnsRecordData != nil {
		var dnsRecordsFlag *int64
		if a.ChangeDnsRecordData.Flags != nil {
			v, err := strconv.ParseInt(*a.ChangeDnsRecordData.Flags, 10, 64)
			if err != nil {
				return ActionRow{}, err
			}
			dnsRecordsFlag = &v
		}
		row.ChangeDNSRecordKey = a.ChangeDnsRecordData.Key
		row.ChangeDNSRecordValueSchema = a.ChangeDnsRecordData.ValueSchema
		row.ChangeDNSRecordValue = a.ChangeDnsRecordData.Value
		row.ChangeDNSRecordFlags = dnsRecordsFlag
	}
	if a.NftMintData != nil {
		row.NFTMintNFTItemIndex = a.NftMintData.NftItemIndex
	}
	if a.DexDepositLiquidityData != nil {
		row.DexDepositLiquidityDataDex = a.DexDepositLiquidityData.Dex
		row.DexDepositLiquidityDataAmount1 = a.DexDepositLiquidityData.Amount1
		row.DexDepositLiquidityDataAmount2 = a.DexDepositLiquidityData.Amount2
		row.DexDepositLiquidityDataAsset1 = a.DexDepositLiquidityData.Asset1
		row.DexDepositLiquidityDataAsset2 = a.DexDepositLiquidityData.Asset2
		row.DexDepositLiquidityDataUserJettonWallet1 = a.DexDepositLiquidityData.UserJettonWallet1
		row.DexDepositLiquidityDataUserJettonWallet2 = a.DexDepositLiquidityData.UserJettonWallet2
		row.DexDepositLiquidityDataLpTokensMinted = a.DexDepositLiquidityData.LpTokensMinted
	}
	if a.DexWithdrawLiquidityData != nil {
		row.DexWithdrawLiquidityDataDex = a.DexWithdrawLiquidityData.Dex
		row.DexWithdrawLiquidityDataAmount1 = a.DexWithdrawLiquidityData.Amount1
		row.DexWithdrawLiquidityDataAmount2 = a.DexWithdrawLiquidityData.Amount2
		row.DexWithdrawLiquidityDataAsset1Out = a.DexWithdrawLiquidityData.Asset1Out
		row.DexWithdrawLiquidityDataAsset2Out = a.DexWithdrawLiquidityData.Asset2Out
		row.DexWithdrawLiquidityDataUserJettonWallet1 = a.DexWithdrawLiquidityData.UserJettonWallet1
		row.DexWithdrawLiquidityDataUserJettonWallet2 = a.DexWithdrawLiquidityData.UserJettonWallet2
		row.DexWithdrawLiquidityDataDexJettonWallet1 = a.DexWithdrawLiquidityData.DexJettonWallet1
		row.DexWithdrawLiquidityDataDexJettonWallet2 = a.DexWithdrawLiquidityData.DexJettonWallet2
		row.DexWithdrawLiquidityDataLpTokensBurnt = a.DexWithdrawLiquidityData.LpTokensBurnt
	}
	if a.StakingData != nil {
		row.StakingDataProvider = a.StakingData.Provider
		row.StakingDataTsNft = a.StakingData.TsNft
	}
	return row, nil
}
