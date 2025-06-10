package emulated

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/toncenter/ton-indexer/ton-index-go/index/models"

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
	StorageFeesCollected uint64          `msgpack:"storage_fees_collected" json:"storage_fees_collected"`
	StorageFeesDue       *uint64         `msgpack:"storage_fees_due" json:"storage_fees_due"`
	StatusChange         AccStatusChange `msgpack:"status_change" json:"status_change"`
}

type trCreditPhase struct {
	DueFeesCollected *uint64 `msgpack:"due_fees_collected" json:"due_fees_collected"`
	Credit           uint64  `msgpack:"credit" json:"credit"`
}

type trComputePhaseSkipped struct {
	Reason ComputeSkipReason `msgpack:"reason" json:"reason"`
}

type trComputePhaseVm struct {
	Success          bool    `msgpack:"success" json:"success"`
	MsgStateUsed     bool    `msgpack:"msg_state_used" json:"msg_state_used"`
	AccountActivated bool    `msgpack:"account_activated" json:"account_activated"`
	GasFees          uint64  `msgpack:"gas_fees" json:"gas_fees"`
	GasUsed          uint64  `msgpack:"gas_used" json:"gas_used"`
	GasLimit         uint64  `msgpack:"gas_limit" json:"gas_limit"`
	GasCredit        *uint64 `msgpack:"gas_credit" json:"gas_credit"`
	Mode             int8    `msgpack:"mode" json:"mode"`
	ExitCode         int32   `msgpack:"exit_code" json:"exit_code"`
	ExitArg          *int32  `msgpack:"exit_arg" json:"exit_arg"`
	VmSteps          uint32  `msgpack:"vm_steps" json:"vm_steps"`
	VmInitStateHash  hash    `msgpack:"vm_init_state_hash" json:"vm_init_state_hash"`
	VmFinalStateHash hash    `msgpack:"vm_final_state_hash" json:"vm_final_state_hash"`
}

type storageUsedShort struct {
	Cells int64 `msgpack:"cells" json:"cells"`
	Bits  int64 `msgpack:"bits" json:"bits"`
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
	ReqFwdFees uint64           `msgpack:"req_fwd_fees" json:"req_fwd_fees"`
}

type trBouncePhaseOk struct {
	MsgSize storageUsedShort `msgpack:"msg_size" json:"msg_size"`
	MsgFees uint64           `msgpack:"msg_fees" json:"msg_fees"`
	FwdFees uint64           `msgpack:"fwd_fees" json:"fwd_fees"`
}

type trMessage struct {
	Hash         hash    `msgpack:"hash" json:"hash"`
	Source       *string `msgpack:"source" json:"source"`
	Destination  *string `msgpack:"destination" json:"destination"`
	Value        *uint64 `msgpack:"value" json:"value"`
	FwdFee       *uint64 `msgpack:"fwd_fee" json:"fwd_fee"`
	IhrFee       *uint64 `msgpack:"ihr_fee" json:"ihr_fee"`
	CreatedLt    *uint64 `msgpack:"created_lt" json:"created_lt"`
	CreatedAt    *uint32 `msgpack:"created_at" json:"created_at"`
	Opcode       *int32  `msgpack:"opcode" json:"opcode"`
	IhrDisabled  *bool   `msgpack:"ihr_disabled" json:"ihr_disabled"`
	Bounce       *bool   `msgpack:"bounce" json:"bounce"`
	Bounced      *bool   `msgpack:"bounced" json:"bounced"`
	ImportFee    *uint64 `msgpack:"import_fee" json:"import_fee"`
	BodyBoc      string  `msgpack:"body_boc" json:"body_boc"`
	InitStateBoc *string `msgpack:"init_state_boc" json:"init_state_boc"`
	HashNorm     *hash   `msgpack:"hash_norm" json:"hash_norm"`
}

type transactionDescr struct {
	CreditFirst bool            `msgpack:"credit_first" json:"credit_first"`
	StoragePh   *trStoragePhase `msgpack:"storage_ph" json:"storage_ph"`
	CreditPh    *trCreditPhase  `msgpack:"credit_ph" json:"credit_ph"`
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
	PrevTransLt            uint64           `msgpack:"prev_trans_lt" json:"prev_trans_lt,string"`
	Now                    uint32           `msgpack:"now" json:"now"`
	OrigStatus             AccountStatus    `msgpack:"orig_status" json:"orig_status"`
	EndStatus              AccountStatus    `msgpack:"end_status" json:"end_status"`
	InMsg                  *trMessage       `msgpack:"in_msg" json:"in_msg"`
	OutMsgs                []trMessage      `msgpack:"out_msgs" json:"out_msgs"`
	TotalFees              uint64           `msgpack:"total_fees" json:"total_fees"`
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

type actionVaultExcess struct {
	Asset  *string `msgpack:"asset"`
	Amount *string `msgpack:"amount"`
}

type actionJettonSwapDetails struct {
	Dex                 *string                  `msgpack:"dex"`
	Sender              *string                  `msgpack:"sender"`
	DexIncomingTransfer actionDexTransferDetails `msgpack:"dex_incoming_transfer"`
	DexOutgoingTransfer actionDexTransferDetails `msgpack:"dex_outgoing_transfer"`
	PeerSwaps           []actionPeerSwapDetails  `msgpack:"peer_swaps"`
	MinOutAmount        *string                  `msgpack:"min_out_amount"`
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
	Dex               *string             `msgpack:"dex"`
	Amount1           *string             `msgpack:"amount1"`
	Amount2           *string             `msgpack:"amount2"`
	Asset1            *string             `msgpack:"asset1"`
	Asset2            *string             `msgpack:"asset2"`
	UserJettonWallet1 *string             `msgpack:"user_jetton_wallet_1"`
	UserJettonWallet2 *string             `msgpack:"user_jetton_wallet_2"`
	LpTokensMinted    *string             `msgpack:"lp_tokens_minted"`
	TargetAsset1      *string             `msgpack:"target_asset_1"`
	TargetAsset2      *string             `msgpack:"target_asset_2"`
	TargetAmount1     *string             `msgpack:"target_amount_1"`
	TargetAmount2     *string             `msgpack:"target_amount_2"`
	VaultExcesses     []actionVaultExcess `msgpack:"vault_excesses"`
	TickLower         *string             `msgpack:"tick_lower"`
	TickUpper         *string             `msgpack:"tick_upper"`
	NFTIndex          *string             `msgpack:"nft_index"`
	NFTAddress        *string             `msgpack:"nft_address"`
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
	BurnedNFTIndex    *string `msgpack:"burned_nft_index"`
	BurnedNFTAddress  *string `msgpack:"burned_nft_address"`
	TickLower         *string `msgpack:"tick_lower"`
	TickUpper         *string `msgpack:"tick_upper"`
}

type actionStakingData struct {
	Provider     *string `msgpack:"provider"`
	TsNft        *string `msgpack:"ts_nft"`
	TokensBurnt  *string `msgpack:"tokens_burnt"`
	TokensMinted *string `msgpack:"tokens_minted"`
}

type actionToncoDeployPoolDetails struct {
	Jetton0RouterWallet *string `msgpack:"jetton0_router_wallet"`
	Jetton1RouterWallet *string `msgpack:"jetton1_router_wallet"`
	Jetton0Minter       *string `msgpack:"jetton0_minter"`
	Jetton1Minter       *string `msgpack:"jetton1_minter"`
	TickSpacing         *string `msgpack:"tick_spacing"`
	InitialPriceX96     *string `msgpack:"initial_price_x96"`
	ProtocolFee         *string `msgpack:"protocol_fee"`
	LpFeeBase           *string `msgpack:"lp_fee_base"`
	LpFeeCurrent        *string `msgpack:"lp_fee_current"`
	PoolActive          *bool   `msgpack:"pool_active"`
}

type actionMultisigCreateOrderDetails struct {
	QueryId           *string `msgpack:"query_id"`
	OrderSeqno        *string `msgpack:"order_seqno"`
	IsCreatedBySigner *bool   `msgpack:"is_created_by_signer"`
	IsSignedByCreator *bool   `msgpack:"is_signed_by_creator"`
	CreatorIndex      *int64  `msgpack:"creator_index"`
	ExpirationDate    *int64  `msgpack:"expiration_date"`
	OrderBoc          *string `msgpack:"order_boc"`
}

type actionMultisigApproveDetails struct {
	SignerIndex *int64 `msgpack:"signer_index"`
	ExitCode    *int32 `msgpack:"exit_code"`
}

type actionMultisigExecuteDetails struct {
	QueryId        *string `msgpack:"query_id"`
	OrderSeqno     *string `msgpack:"order_seqno"`
	ExpirationDate *int64  `msgpack:"expiration_date"`
	ApprovalsNum   *int64  `msgpack:"approvals_num"`
	SignersHash    *string `msgpack:"signers_hash"`
	OrderBoc       *string `msgpack:"order_boc"`
}

type actionVestingSendMessageDetails struct {
	QueryId    *string `msgpack:"query_id"`
	MessageBoc *string `msgpack:"message_boc"`
}

type actionVestingAddWhitelistDetails struct {
	QueryId       *string  `msgpack:"query_id"`
	AccountsAdded []string `msgpack:"accounts_added"`
}

type actionEvaaSupplyDetails struct {
	SenderJettonWallet    *string `msgpack:"sender_jetton_wallet"`
	RecipientJettonWallet *string `msgpack:"recipient_jetton_wallet"`
	MasterJettonWallet    *string `msgpack:"master_jetton_wallet"`
	Master                *string `msgpack:"master"`
	AssetId               *string `msgpack:"asset_id"`
	IsTon                 *bool   `msgpack:"is_ton"`
}

type actionEvaaWithdrawDetails struct {
	RecipientJettonWallet *string `msgpack:"recipient_jetton_wallet"`
	MasterJettonWallet    *string `msgpack:"master_jetton_wallet"`
	Master                *string `msgpack:"master"`
	FailReason            *string `msgpack:"fail_reason"`
	AssetId               *string `msgpack:"asset_id"`
}

type actionEvaaLiquidateDetails struct {
	FailReason *string `msgpack:"fail_reason"`
	DebtAmount *string `msgpack:"debt_amount"`
	AssetId    *string `msgpack:"asset_id"`
}

type actionJvaultClaimDetails struct {
	ClaimedJettons []string `msgpack:"claimed_jettons"`
	ClaimedAmounts []string `msgpack:"claimed_amounts"`
}

type actionJvaultStakeDetails struct {
	Period             *int64  `msgpack:"period"`
	MintedStakeJettons *string `msgpack:"minted_stake_jettons"`
	StakeWallet        *string `msgpack:"stake_wallet"`
}

type Action struct {
	ActionId                 string                            `msgpack:"action_id"`
	Type                     string                            `msgpack:"type"`
	TraceId                  *string                           `msgpack:"trace_id"`
	TraceExternalHash        string                            `msgpack:"trace_external_hash"`
	TxHashes                 []string                          `msgpack:"tx_hashes"`
	Value                    *string                           `msgpack:"value"`
	Amount                   *string                           `msgpack:"amount"`
	StartLt                  *uint64                           `msgpack:"start_lt"`
	EndLt                    *uint64                           `msgpack:"end_lt"`
	StartUtime               *uint32                           `msgpack:"start_utime"`
	EndUtime                 *uint32                           `msgpack:"end_utime"`
	TraceEndLt               *uint64                           `msgpack:"trace_end_lt"`
	TraceEndUtime            *uint32                           `msgpack:"trace_end_utime"`
	TraceStartLt             *uint64                           `msgpack:"trace_start_lt"`
	TraceMcSeqnoEnd          *uint32                           `msgpack:"trace_mc_seqno_end"`
	Source                   *string                           `msgpack:"source"`
	SourceSecondary          *string                           `msgpack:"source_secondary"`
	Destination              *string                           `msgpack:"destination"`
	DestinationSecondary     *string                           `msgpack:"destination_secondary"`
	Asset                    *string                           `msgpack:"asset"`
	AssetSecondary           *string                           `msgpack:"asset_secondary"`
	Asset2                   *string                           `msgpack:"asset2"`
	Asset2Secondary          *string                           `msgpack:"asset2_secondary"`
	Opcode                   *uint32                           `msgpack:"opcode"`
	Success                  bool                              `msgpack:"success"`
	TonTransferData          *actionTonTransferDetails         `msgpack:"ton_transfer_data"`
	AncestorType             []string                          `msgpack:"ancestor_type"`
	ParentActionId           *string                           `msgpack:"parent_action_id"`
	JettonTransferData       *actionJettonTransferDetails      `msgpack:"jetton_transfer_data"`
	NftTransferData          *actionNftTransferDetails         `msgpack:"nft_transfer_data"`
	JettonSwapData           *actionJettonSwapDetails          `msgpack:"jetton_swap_data"`
	ChangeDnsRecordData      *actionChangeDnsRecordDetails     `msgpack:"change_dns_record_data"`
	NftMintData              *actionNftMintDetails             `msgpack:"nft_mint_data"`
	DexDepositLiquidityData  *actionDexDepositLiquidityData    `msgpack:"dex_deposit_liquidity_data"`
	DexWithdrawLiquidityData *actionDexWithdrawLiquidityData   `msgpack:"dex_withdraw_liquidity_data"`
	StakingData              *actionStakingData                `msgpack:"staking_data"`
	ToncoDeployPoolData      *actionToncoDeployPoolDetails     `msgpack:"tonco_deploy_pool_data"`
	MultisigCreateOrderData  *actionMultisigCreateOrderDetails `msgpack:"multisig_create_order_data"`
	MultisigApproveData      *actionMultisigApproveDetails     `msgpack:"multisig_approve_data"`
	MultisigExecuteData      *actionMultisigExecuteDetails     `msgpack:"multisig_execute_data"`
	VestingSendMessageData   *actionVestingSendMessageDetails  `msgpack:"vesting_send_message_data"`
	VestingAddWhitelistData  *actionVestingAddWhitelistDetails `msgpack:"vesting_add_whitelist_data"`
	EvaaSupplyData           *actionEvaaSupplyDetails          `msgpack:"evaa_supply_data"`
	EvaaWithdrawData         *actionEvaaWithdrawDetails        `msgpack:"evaa_withdraw_data"`
	EvaaLiquidateData        *actionEvaaLiquidateDetails       `msgpack:"evaa_liquidate_data"`
	JvaultClaimData          *actionJvaultClaimDetails         `msgpack:"jvault_claim_data"`
	JvaultStakeData          *actionJvaultStakeDetails         `msgpack:"jvault_stake_data"`
}

type blockId struct {
	Workchain int32  `msgpack:"workchain"`
	Shard     uint64 `msgpack:"shard"`
	Seqno     uint32 `msgpack:"seqno"`
}
type Trace struct {
	TraceId      *string
	ExternalHash string
	Nodes        []TraceNode
	Classified   bool
	Actions      []Action
}
type TraceNode struct {
	Transaction  transaction `msgpack:"transaction"`
	Emulated     bool        `msgpack:"emulated"`
	BlockId      blockId     `msgpack:"block_id"`
	McBlockSeqno uint32      `msgpack:"mc_block_seqno"`
	TraceId      *string
	Key          string
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
func ConvertHSet(traceHash map[string]string, traceKey string) (Trace, error) {

	queue := make([]string, 0)

	rootNodeId, exists := traceHash["root_node"]
	if !exists {
		return Trace{}, fmt.Errorf("root_node not found in trace %s", traceKey)
	}
	queue = append(queue, rootNodeId)
	txs := make([]TraceNode, 0)
	actions := make([]Action, 0)
	var endLt uint64 = 0
	var endUtime uint32 = 0
	var mcSeqnoEnd uint32 = 0
	var traceId *string
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		var node TraceNode
		nodeData, exists := traceHash[key]
		if !exists {
			return Trace{}, fmt.Errorf("key %s not found in trace", key)
		}
		nodeBytes := []byte(nodeData)
		err := msgpack.Unmarshal(nodeBytes, &node)
		node.Key = key
		if key == rootNodeId && !node.Emulated {
			id := base64.StdEncoding.EncodeToString(node.Transaction.Hash[:])
			traceId = &id
		}

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
		if endLt < node.Transaction.Lt {
			endLt = node.Transaction.Lt
		}
		if endUtime < node.Transaction.Now {
			endUtime = node.Transaction.Now
		}
		if mcSeqnoEnd < node.McBlockSeqno {
			mcSeqnoEnd = node.McBlockSeqno
		}
	}
	if actionsBytes, exists := traceHash["actions"]; exists {
		err := msgpack.Unmarshal([]byte(actionsBytes), &actions)
		for i := range actions {
			actions[i].TraceEndUtime = &endUtime
			actions[i].TraceEndLt = &endLt
			actions[i].TraceMcSeqnoEnd = &mcSeqnoEnd
			actions[i].TraceExternalHash = rootNodeId
			actions[i].TraceId = traceId
		}
		if err != nil {
			return Trace{}, fmt.Errorf("failed to unmarshal actions: %w", err)
		}
	}
	_, has_actions := traceHash["actions"]
	return Trace{
		TraceId:      traceId,
		ExternalHash: rootNodeId,
		Nodes:        txs,
		Classified:   has_actions,
		Actions:      actions,
	}, nil
}

func (h hash) Base64Ptr() *string {
	str := base64.StdEncoding.EncodeToString(h[:])
	return &str
}

func (n *TraceNode) GetTransaction() (models.Transaction, error) {
	origStatus, err := n.Transaction.OrigStatus.Str()
	if err != nil {
		return models.Transaction{}, err
	}
	endStatus, err := n.Transaction.EndStatus.Str()
	if err != nil {
		return models.Transaction{}, err
	}

	// Convert hash to HashType
	hash := models.HashType(base64.StdEncoding.EncodeToString(n.Transaction.Hash[:]))
	prevTransHash := models.HashType(base64.StdEncoding.EncodeToString(n.Transaction.PrevTransHash[:]))
	accountStateHashBefore := models.HashType(base64.StdEncoding.EncodeToString(n.Transaction.AccountStateHashBefore[:]))
	accountStateHashAfter := models.HashType(base64.StdEncoding.EncodeToString(n.Transaction.AccountStateHashAfter[:]))

	// Convert TraceId
	var traceId *models.HashType
	if n.TraceId != nil {
		t := models.HashType(*n.TraceId)
		traceId = &t
	}

	// Build transaction description
	descr := models.TransactionDescr{
		Type:        "ord",
		Aborted:     &n.Transaction.Description.Aborted,
		Destroyed:   &n.Transaction.Description.Destroyed,
		CreditFirst: &n.Transaction.Description.CreditFirst,
	}

	isTock := false
	descr.IsTock = &isTock
	descr.Installed = &isTock

	// Storage phase
	if n.Transaction.Description.StoragePh != nil {
		storageStatusChange, err := n.Transaction.Description.StoragePh.StatusChange.Str()
		if err != nil {
			return models.Transaction{}, err
		}
		storageFeesCollected := int64(n.Transaction.Description.StoragePh.StorageFeesCollected)
		var storageFeesDue *int64
		if n.Transaction.Description.StoragePh.StorageFeesDue != nil {
			val := int64(*n.Transaction.Description.StoragePh.StorageFeesDue)
			storageFeesDue = &val
		}
		descr.StoragePh = &models.StoragePhase{
			StorageFeesCollected: &storageFeesCollected,
			StorageFeesDue:       storageFeesDue,
			StatusChange:         &storageStatusChange,
		}
	}

	// Credit phase
	if n.Transaction.Description.CreditPh != nil {
		var dueFeesCollected *int64
		if n.Transaction.Description.CreditPh.DueFeesCollected != nil {
			val := int64(*n.Transaction.Description.CreditPh.DueFeesCollected)
			dueFeesCollected = &val
		}
		credit := int64(n.Transaction.Description.CreditPh.Credit)
		descr.CreditPh = &models.CreditPhase{
			DueFeesCollected:      dueFeesCollected,
			Credit:                &credit,
			CreditExtraCurrencies: map[string]string{},
		}
	}

	// Compute phase
	descr.ComputePh = &models.ComputePhase{}
	if n.Transaction.Description.ComputePh.Type == 0 {
		// Skipped
		isSkipped := true
		descr.ComputePh.IsSkipped = &isSkipped
		reason, err := n.Transaction.Description.ComputePh.Data.(trComputePhaseSkipped).Reason.Str()
		if err != nil {
			return models.Transaction{}, err
		}
		descr.ComputePh.Reason = &reason
	} else {
		// VM phase
		isSkipped := false
		descr.ComputePh.IsSkipped = &isSkipped
		vm := n.Transaction.Description.ComputePh.Data.(trComputePhaseVm)
		descr.ComputePh.Success = &vm.Success
		descr.ComputePh.MsgStateUsed = &vm.MsgStateUsed
		descr.ComputePh.AccountActivated = &vm.AccountActivated
		gasFees := int64(vm.GasFees)
		gasUsed := int64(vm.GasUsed)
		gasLimit := int64(vm.GasLimit)
		descr.ComputePh.GasFees = &gasFees
		descr.ComputePh.GasUsed = &gasUsed
		descr.ComputePh.GasLimit = &gasLimit
		if vm.GasCredit != nil {
			gasCredit := int64(*vm.GasCredit)
			descr.ComputePh.GasCredit = &gasCredit
		}
		mode := int32(vm.Mode)
		descr.ComputePh.Mode = &mode
		descr.ComputePh.ExitCode = &vm.ExitCode
		descr.ComputePh.ExitArg = vm.ExitArg
		descr.ComputePh.VmSteps = &vm.VmSteps
		vmInitStateHash := models.HashType(base64.StdEncoding.EncodeToString(vm.VmInitStateHash[:]))
		vmFinalStateHash := models.HashType(base64.StdEncoding.EncodeToString(vm.VmFinalStateHash[:]))
		descr.ComputePh.VmInitStateHash = &vmInitStateHash
		descr.ComputePh.VmFinalStateHash = &vmFinalStateHash
	}

	// Action phase
	if n.Transaction.Description.Action != nil {
		actionStatusChange, err := n.Transaction.Description.Action.StatusChange.Str()
		if err != nil {
			return models.Transaction{}, err
		}
		actionListHash := models.HashType(base64.StdEncoding.EncodeToString(n.Transaction.Description.Action.ActionListHash[:]))

		descr.Action = &models.ActionPhase{
			Success:         &n.Transaction.Description.Action.Success,
			Valid:           &n.Transaction.Description.Action.Valid,
			NoFunds:         &n.Transaction.Description.Action.NoFunds,
			StatusChange:    &actionStatusChange,
			TotalFwdFees:    n.Transaction.Description.Action.TotalFwdFees,
			TotalActionFees: n.Transaction.Description.Action.TotalActionFees,
			ResultCode:      n.Transaction.Description.Action.ResultCode,
			ResultArg:       n.Transaction.Description.Action.ResultArg,
			TotActions:      n.Transaction.Description.Action.TotActions,
			SpecActions:     n.Transaction.Description.Action.SpecActions,
			SkippedActions:  n.Transaction.Description.Action.SkippedActions,
			MsgsCreated:     n.Transaction.Description.Action.MsgsCreated,
			ActionListHash:  &actionListHash,
			TotMsgSize: &models.MsgSize{
				Cells: &n.Transaction.Description.Action.TotMsgSize.Cells,
				Bits:  &n.Transaction.Description.Action.TotMsgSize.Bits,
			},
		}
	}

	// Bounce phase
	if n.Transaction.Description.Bounce != nil {
		descr.Bounce = &models.BouncePhase{}
		if n.Transaction.Description.Bounce.Type == 0 {
			bounceType := "negfunds"
			descr.Bounce.Type = &bounceType
		} else if n.Transaction.Description.Bounce.Type == 1 {
			bounceType := "nofunds"
			descr.Bounce.Type = &bounceType
			phase := n.Transaction.Description.Bounce.Data.(trBouncePhaseNofunds)
			msgSizeCells := int64(phase.MsgSize.Cells)
			msgSizeBits := int64(phase.MsgSize.Bits)
			reqFwdFees := int64(phase.ReqFwdFees)
			descr.Bounce.MsgSize = &models.MsgSize{
				Cells: &msgSizeCells,
				Bits:  &msgSizeBits,
			}
			descr.Bounce.ReqFwdFees = &reqFwdFees
		} else {
			bounceType := "ok"
			descr.Bounce.Type = &bounceType
			phase := n.Transaction.Description.Bounce.Data.(trBouncePhaseOk)
			msgSizeCells := int64(phase.MsgSize.Cells)
			msgSizeBits := int64(phase.MsgSize.Bits)
			msgFees := int64(phase.MsgFees)
			fwdFees := int64(phase.FwdFees)
			descr.Bounce.MsgSize = &models.MsgSize{
				Cells: &msgSizeCells,
				Bits:  &msgSizeBits,
			}
			descr.Bounce.MsgFees = &msgFees
			descr.Bounce.FwdFees = &fwdFees
		}
	}

	transaction := models.Transaction{
		Account:                  models.AccountAddress(n.Transaction.Account),
		Hash:                     hash,
		Lt:                       int64(n.Transaction.Lt),
		Now:                      int32(n.Transaction.Now),
		Workchain:                n.BlockId.Workchain,
		Shard:                    models.ShardId(n.BlockId.Shard),
		Seqno:                    int32(n.BlockId.Seqno),
		McSeqno:                  int32(n.McBlockSeqno),
		TraceId:                  traceId,
		PrevTransHash:            prevTransHash,
		PrevTransLt:              int64(n.Transaction.PrevTransLt),
		OrigStatus:               origStatus,
		EndStatus:                endStatus,
		TotalFees:                int64(n.Transaction.TotalFees),
		TotalFeesExtraCurrencies: map[string]string{},
		AccountStateHashBefore:   accountStateHashBefore,
		AccountStateHashAfter:    accountStateHashAfter,
		Descr:                    descr,
		BlockRef: models.BlockId{
			Workchain: n.BlockId.Workchain,
			Shard:     models.ShardId(n.BlockId.Shard),
			Seqno:     int32(n.BlockId.Seqno),
		},
		InMsg:    nil,
		OutMsgs:  nil,
		Emulated: n.Emulated,
	}

	return transaction, nil
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

func (m *trMessage) GetMessage(traceId string, direction string, txLt uint64, txHash string) (row models.Message, body *models.MessageContent, initState *models.MessageContent, err error) {
	bodyHash, err := calcHash(m.BodyBoc)
	if err != nil {
		return models.Message{}, nil, nil, err
	}

	body = &models.MessageContent{
		Hash: (*models.HashType)(&bodyHash),
		Body: &m.BodyBoc,
	}

	var initStateHashPtr *models.HashType
	if m.InitStateBoc != nil {
		initStateHashStr, err := calcHash(*m.InitStateBoc)
		if err != nil {
			return models.Message{}, nil, nil, err
		}
		initStateHash := models.HashType(initStateHashStr)
		initStateHashPtr = &initStateHash
		initState = &models.MessageContent{
			Hash: &initStateHash,
			Body: m.InitStateBoc,
		}
	}

	var value *int64
	if m.Value != nil {
		val := int64(*m.Value)
		value = &val
	}

	var opcode *models.OpcodeType
	if m.Opcode != nil {
		val := models.OpcodeType(*m.Opcode)
		opcode = &val
	}

	var hashNorm *models.HashType
	if m.HashNorm != nil {
		hashNormStr := base64.StdEncoding.EncodeToString((*m.HashNorm)[:])
		hashNormType := models.HashType(hashNormStr)
		hashNorm = &hashNormType
	}

	var traceIdPtr *models.HashType
	if traceId != "" {
		traceIdType := models.HashType(traceId)
		traceIdPtr = &traceIdType
	}

	txHashType := models.HashType(txHash)

	msgHashStr := base64.StdEncoding.EncodeToString(m.Hash[:])
	msgHash := models.HashType(msgHashStr)

	bodyHashType := models.HashType(bodyHash)

	message := models.Message{
		TxHash:               txHashType,
		TxLt:                 int64(txLt),
		MsgHash:              msgHash,
		Direction:            direction,
		TraceId:              traceIdPtr,
		Source:               (*models.AccountAddress)(m.Source),
		Destination:          (*models.AccountAddress)(m.Destination),
		Value:                value,
		ValueExtraCurrencies: map[string]string{},
		FwdFee:               m.FwdFee,
		IhrFee:               m.IhrFee,
		CreatedLt:            m.CreatedLt,
		CreatedAt:            m.CreatedAt,
		Opcode:               opcode,
		IhrDisabled:          m.IhrDisabled,
		Bounce:               m.Bounce,
		Bounced:              m.Bounced,
		ImportFee:            m.ImportFee,
		BodyHash:             &bodyHashType,
		InitStateHash:        initStateHashPtr,
		MessageContent:       body,
		InitState:            initState,
		MsgHashNorm:          hashNorm,
	}

	if direction == "in" {
		message.InMsgTxHash = &txHashType
	} else {
		message.OutMsgTxHash = &txHashType
	}

	return message, body, initState, nil
}

func (n *TraceNode) GetMessages() ([]models.Message, map[string]models.MessageContent, map[string]models.MessageContent, error) {
	messageContents := make(map[string]models.MessageContent)
	initStates := make(map[string]models.MessageContent)
	messages := make([]models.Message, 0)

	for _, outMsg := range n.Transaction.OutMsgs {
		msgRow, body, initState, err := outMsg.GetMessage(n.Key, "out", n.Transaction.Lt,
			base64.StdEncoding.EncodeToString(n.Transaction.Hash[:]))
		if err != nil {
			return nil, nil, nil, err
		}
		messages = append(messages, msgRow)
		if body != nil {
			messageContents[string(msgRow.MsgHash)] = *body
		}
		if initState != nil {
			initStates[string(*msgRow.InitStateHash)] = *initState
		}
	}
	inMsgRow, body, initState, err := n.Transaction.InMsg.GetMessage(n.Key, "in", n.Transaction.Lt,
		base64.StdEncoding.EncodeToString(n.Transaction.Hash[:]))
	if err != nil {
		return nil, nil, nil, err
	}
	messages = append(messages, inMsgRow)
	if body != nil {
		messageContents[string(inMsgRow.MsgHash)] = *body
	}
	if initState != nil {
		initStates[string(*inMsgRow.InitStateHash)] = *initState
	}
	return messages, messageContents, nil, nil
}

func (a *Action) ToRawAction() (*models.RawAction, error) {
	rawAction := &models.RawAction{
		ActionId:          models.HashType(a.ActionId),
		Type:              a.Type,
		TraceId:           (*models.HashType)(a.TraceId),
		TxHashes:          make([]models.HashType, len(a.TxHashes)),
		Value:             a.Value,
		Amount:            a.Amount,
		TraceExternalHash: (*models.HashType)(&a.TraceExternalHash),
		Success:           &a.Success,
		AncestorType:      a.AncestorType,
		ParentActionId:    a.ParentActionId,
	}

	// Convert TxHashes
	for i, hash := range a.TxHashes {
		rawAction.TxHashes[i] = models.HashType(hash)
	}

	// Handle pointer conversions with nil checks
	if a.StartLt != nil {
		rawAction.StartLt = int64(*a.StartLt)
	}
	if a.EndLt != nil {
		rawAction.EndLt = int64(*a.EndLt)
	}
	if a.StartUtime != nil {
		rawAction.StartUtime = int64(*a.StartUtime)
	}
	if a.EndUtime != nil {
		rawAction.EndUtime = int64(*a.EndUtime)
	}
	if a.TraceEndLt != nil {
		rawAction.TraceEndLt = int64(*a.TraceEndLt)
	}
	if a.TraceEndUtime != nil {
		rawAction.TraceEndUtime = int64(*a.TraceEndUtime)
	}
	if a.TraceMcSeqnoEnd != nil {
		rawAction.TraceMcSeqnoEnd = int32(*a.TraceMcSeqnoEnd)
	}
	if a.Opcode != nil {
		opcode := models.OpcodeType(*a.Opcode)
		rawAction.Opcode = &opcode
	}

	// Convert address fields
	rawAction.Source = (*models.AccountAddress)(a.Source)
	rawAction.SourceSecondary = (*models.AccountAddress)(a.SourceSecondary)
	rawAction.Destination = (*models.AccountAddress)(a.Destination)
	rawAction.DestinationSecondary = (*models.AccountAddress)(a.DestinationSecondary)
	rawAction.Asset = (*models.AccountAddress)(a.Asset)
	rawAction.AssetSecondary = (*models.AccountAddress)(a.AssetSecondary)
	rawAction.Asset2 = (*models.AccountAddress)(a.Asset2)
	rawAction.Asset2Secondary = (*models.AccountAddress)(a.Asset2Secondary)

	// Convert TonTransferData
	if a.TonTransferData != nil {
		rawAction.TonTransferContent = a.TonTransferData.Content
		rawAction.TonTransferEncrypted = &a.TonTransferData.Encrypted
	}

	// Convert JettonTransferData
	if a.JettonTransferData != nil {
		rawAction.JettonTransferResponseDestination = (*models.AccountAddress)(a.JettonTransferData.ResponseDestination)
		rawAction.JettonTransferForwardAmount = a.JettonTransferData.ForwardAmount
		rawAction.JettonTransferQueryId = a.JettonTransferData.QueryId
		rawAction.JettonTransferCustomPayload = a.JettonTransferData.CustomPayload
		rawAction.JettonTransferForwardPayload = a.JettonTransferData.ForwardPayload
		rawAction.JettonTransferComment = a.JettonTransferData.Comment
		rawAction.JettonTransferIsEncryptedComment = a.JettonTransferData.IsEncryptedComment
	}

	// Convert NftTransferData
	if a.NftTransferData != nil {
		rawAction.NFTTransferIsPurchase = &a.NftTransferData.IsPurchase
		rawAction.NFTTransferPrice = a.NftTransferData.Price
		rawAction.NFTTransferQueryId = a.NftTransferData.QueryId
		rawAction.NFTTransferCustomPayload = a.NftTransferData.CustomPayload
		rawAction.NFTTransferForwardPayload = a.NftTransferData.ForwardPayload
		rawAction.NFTTransferForwardAmount = a.NftTransferData.ForwardAmount
		rawAction.NFTTransferResponseDestination = (*models.AccountAddress)(a.NftTransferData.ResponseDestination)
		rawAction.NFTTransferNFTItemIndex = a.NftTransferData.NftItemIndex
	}

	// Convert JettonSwapData
	if a.JettonSwapData != nil {
		rawAction.JettonSwapDex = a.JettonSwapData.Dex
		rawAction.JettonSwapSender = (*models.AccountAddress)(a.JettonSwapData.Sender)

		// Convert DexIncomingTransfer
		rawAction.JettonSwapDexIncomingTransferAmount = a.JettonSwapData.DexIncomingTransfer.Amount
		rawAction.JettonSwapDexIncomingTransferAsset = (*models.AccountAddress)(a.JettonSwapData.DexIncomingTransfer.Asset)
		rawAction.JettonSwapDexIncomingTransferSource = (*models.AccountAddress)(a.JettonSwapData.DexIncomingTransfer.Source)
		rawAction.JettonSwapDexIncomingTransferDestination = (*models.AccountAddress)(a.JettonSwapData.DexIncomingTransfer.Destination)
		rawAction.JettonSwapDexIncomingTransferSourceJettonWallet = (*models.AccountAddress)(a.JettonSwapData.DexIncomingTransfer.SourceJettonWallet)
		rawAction.JettonSwapDexIncomingTransferDestinationJettonWallet = (*models.AccountAddress)(a.JettonSwapData.DexIncomingTransfer.DestinationJettonWallet)

		// Convert DexOutgoingTransfer
		rawAction.JettonSwapDexOutgoingTransferAmount = a.JettonSwapData.DexOutgoingTransfer.Amount
		rawAction.JettonSwapDexOutgoingTransferAsset = (*models.AccountAddress)(a.JettonSwapData.DexOutgoingTransfer.Asset)
		rawAction.JettonSwapDexOutgoingTransferSource = (*models.AccountAddress)(a.JettonSwapData.DexOutgoingTransfer.Source)
		rawAction.JettonSwapDexOutgoingTransferDestination = (*models.AccountAddress)(a.JettonSwapData.DexOutgoingTransfer.Destination)
		rawAction.JettonSwapDexOutgoingTransferSourceJettonWallet = (*models.AccountAddress)(a.JettonSwapData.DexOutgoingTransfer.SourceJettonWallet)
		rawAction.JettonSwapDexOutgoingTransferDestinationJettonWallet = (*models.AccountAddress)(a.JettonSwapData.DexOutgoingTransfer.DestinationJettonWallet)

		rawAction.JettonSwapMinOutAmount = a.JettonSwapData.MinOutAmount

		// Convert PeerSwaps
		rawAction.JettonSwapPeerSwaps = make([]models.RawActionJettonSwapPeerSwap, len(a.JettonSwapData.PeerSwaps))
		for i, peerSwap := range a.JettonSwapData.PeerSwaps {
			rawAction.JettonSwapPeerSwaps[i] = models.RawActionJettonSwapPeerSwap{
				AssetIn:   (*models.AccountAddress)(peerSwap.AssetIn),
				AssetOut:  (*models.AccountAddress)(peerSwap.AssetOut),
				AmountIn:  peerSwap.AmountIn,
				AmountOut: peerSwap.AmountOut,
			}
		}
	}

	// Convert ChangeDnsRecordData
	if a.ChangeDnsRecordData != nil {
		var dnsRecordsFlag *int64
		if a.ChangeDnsRecordData.Flags != nil {
			v, err := strconv.ParseInt(*a.ChangeDnsRecordData.Flags, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse DNS record flags: %w", err)
			}
			dnsRecordsFlag = &v
		}
		rawAction.ChangeDNSRecordKey = a.ChangeDnsRecordData.Key
		rawAction.ChangeDNSRecordValueSchema = a.ChangeDnsRecordData.ValueSchema
		rawAction.ChangeDNSRecordValue = a.ChangeDnsRecordData.Value
		rawAction.ChangeDNSRecordFlags = dnsRecordsFlag
	}

	// Convert NftMintData
	if a.NftMintData != nil {
		rawAction.NFTMintNFTItemIndex = a.NftMintData.NftItemIndex
	}

	// Convert DexDepositLiquidityData
	if a.DexDepositLiquidityData != nil {
		rawAction.DexDepositLiquidityDataDex = a.DexDepositLiquidityData.Dex
		rawAction.DexDepositLiquidityDataAmount1 = a.DexDepositLiquidityData.Amount1
		rawAction.DexDepositLiquidityDataAmount2 = a.DexDepositLiquidityData.Amount2
		rawAction.DexDepositLiquidityDataAsset1 = (*models.AccountAddress)(a.DexDepositLiquidityData.Asset1)
		rawAction.DexDepositLiquidityDataAsset2 = (*models.AccountAddress)(a.DexDepositLiquidityData.Asset2)
		rawAction.DexDepositLiquidityDataUserJettonWallet1 = (*models.AccountAddress)(a.DexDepositLiquidityData.UserJettonWallet1)
		rawAction.DexDepositLiquidityDataUserJettonWallet2 = (*models.AccountAddress)(a.DexDepositLiquidityData.UserJettonWallet2)
		rawAction.DexDepositLiquidityDataLpTokensMinted = a.DexDepositLiquidityData.LpTokensMinted
		rawAction.DexDepositLiquidityDataTargetAsset1 = (*models.AccountAddress)(a.DexDepositLiquidityData.TargetAsset1)
		rawAction.DexDepositLiquidityDataTargetAsset2 = (*models.AccountAddress)(a.DexDepositLiquidityData.TargetAsset2)
		rawAction.DexDepositLiquidityDataTargetAmount1 = a.DexDepositLiquidityData.TargetAmount1
		rawAction.DexDepositLiquidityDataTargetAmount2 = a.DexDepositLiquidityData.TargetAmount2
		rawAction.DexDepositLiquidityDataTickLower = a.DexDepositLiquidityData.TickLower
		rawAction.DexDepositLiquidityDataTickUpper = a.DexDepositLiquidityData.TickUpper
		rawAction.DexDepositLiquidityDataNFTIndex = a.DexDepositLiquidityData.NFTIndex
		rawAction.DexDepositLiquidityDataNFTAddress = (*models.AccountAddress)(a.DexDepositLiquidityData.NFTAddress)

		// Convert VaultExcesses
		rawAction.DexDepositLiquidityDataVaultExcesses = make([]models.RawActionVaultExcessEntry, len(a.DexDepositLiquidityData.VaultExcesses))
		for i, excess := range a.DexDepositLiquidityData.VaultExcesses {
			rawAction.DexDepositLiquidityDataVaultExcesses[i] = models.RawActionVaultExcessEntry{
				Asset:  (*models.AccountAddress)(excess.Asset),
				Amount: excess.Amount,
			}
		}
	}

	// Convert DexWithdrawLiquidityData
	if a.DexWithdrawLiquidityData != nil {
		rawAction.DexWithdrawLiquidityDataDex = a.DexWithdrawLiquidityData.Dex
		rawAction.DexWithdrawLiquidityDataAmount1 = a.DexWithdrawLiquidityData.Amount1
		rawAction.DexWithdrawLiquidityDataAmount2 = a.DexWithdrawLiquidityData.Amount2
		rawAction.DexWithdrawLiquidityDataAsset1Out = (*models.AccountAddress)(a.DexWithdrawLiquidityData.Asset1Out)
		rawAction.DexWithdrawLiquidityDataAsset2Out = (*models.AccountAddress)(a.DexWithdrawLiquidityData.Asset2Out)
		rawAction.DexWithdrawLiquidityDataUserJettonWallet1 = (*models.AccountAddress)(a.DexWithdrawLiquidityData.UserJettonWallet1)
		rawAction.DexWithdrawLiquidityDataUserJettonWallet2 = (*models.AccountAddress)(a.DexWithdrawLiquidityData.UserJettonWallet2)
		rawAction.DexWithdrawLiquidityDataDexJettonWallet1 = (*models.AccountAddress)(a.DexWithdrawLiquidityData.DexJettonWallet1)
		rawAction.DexWithdrawLiquidityDataDexJettonWallet2 = (*models.AccountAddress)(a.DexWithdrawLiquidityData.DexJettonWallet2)
		rawAction.DexWithdrawLiquidityDataLpTokensBurnt = a.DexWithdrawLiquidityData.LpTokensBurnt
		rawAction.DexWithdrawLiquidityDataBurnedNFTIndex = a.DexWithdrawLiquidityData.BurnedNFTIndex
		rawAction.DexWithdrawLiquidityDataBurnedNFTAddress = (*models.AccountAddress)(a.DexWithdrawLiquidityData.BurnedNFTAddress)
		rawAction.DexWithdrawLiquidityDataTickLower = a.DexWithdrawLiquidityData.TickLower
		rawAction.DexWithdrawLiquidityDataTickUpper = a.DexWithdrawLiquidityData.TickUpper
	}

	// Convert StakingData
	if a.StakingData != nil {
		rawAction.StakingDataProvider = a.StakingData.Provider
		rawAction.StakingDataTsNft = (*models.AccountAddress)(a.StakingData.TsNft)
		rawAction.StakingDataTokensBurnt = a.StakingData.TokensBurnt
		rawAction.StakingDataTokensMinted = a.StakingData.TokensMinted
	}

	// Convert ToncoDeployPoolData
	if a.ToncoDeployPoolData != nil {
		rawAction.ToncoDeployPoolJetton0RouterWallet = (*models.AccountAddress)(a.ToncoDeployPoolData.Jetton0RouterWallet)
		rawAction.ToncoDeployPoolJetton1RouterWallet = (*models.AccountAddress)(a.ToncoDeployPoolData.Jetton1RouterWallet)
		rawAction.ToncoDeployPoolJetton0Minter = (*models.AccountAddress)(a.ToncoDeployPoolData.Jetton0Minter)
		rawAction.ToncoDeployPoolJetton1Minter = (*models.AccountAddress)(a.ToncoDeployPoolData.Jetton1Minter)
		rawAction.ToncoDeployPoolTickSpacing = a.ToncoDeployPoolData.TickSpacing
		rawAction.ToncoDeployPoolInitialPriceX96 = a.ToncoDeployPoolData.InitialPriceX96
		rawAction.ToncoDeployPoolProtocolFee = a.ToncoDeployPoolData.ProtocolFee
		rawAction.ToncoDeployPoolLpFeeBase = a.ToncoDeployPoolData.LpFeeBase
		rawAction.ToncoDeployPoolLpFeeCurrent = a.ToncoDeployPoolData.LpFeeCurrent
		rawAction.ToncoDeployPoolPoolActive = a.ToncoDeployPoolData.PoolActive
	}

	// Convert MultisigCreateOrderData
	if a.MultisigCreateOrderData != nil {
		rawAction.MultisigCreateOrderQueryId = a.MultisigCreateOrderData.QueryId
		rawAction.MultisigCreateOrderOrderSeqno = a.MultisigCreateOrderData.OrderSeqno
		rawAction.MultisigCreateOrderIsCreatedBySigner = a.MultisigCreateOrderData.IsCreatedBySigner
		rawAction.MultisigCreateOrderIsSignedByCreator = a.MultisigCreateOrderData.IsSignedByCreator
		rawAction.MultisigCreateOrderCreatorIndex = a.MultisigCreateOrderData.CreatorIndex
		rawAction.MultisigCreateOrderExpirationDate = a.MultisigCreateOrderData.ExpirationDate
		rawAction.MultisigCreateOrderOrderBoc = a.MultisigCreateOrderData.OrderBoc
	}

	// Convert MultisigApproveData
	if a.MultisigApproveData != nil {
		rawAction.MultisigApproveSignerIndex = a.MultisigApproveData.SignerIndex
		rawAction.MultisigApproveExitCode = a.MultisigApproveData.ExitCode
	}

	// Convert MultisigExecuteData
	if a.MultisigExecuteData != nil {
		rawAction.MultisigExecuteQueryId = a.MultisigExecuteData.QueryId
		rawAction.MultisigExecuteOrderSeqno = a.MultisigExecuteData.OrderSeqno
		rawAction.MultisigExecuteExpirationDate = a.MultisigExecuteData.ExpirationDate
		rawAction.MultisigExecuteApprovalsNum = a.MultisigExecuteData.ApprovalsNum
		rawAction.MultisigExecuteSignersHash = a.MultisigExecuteData.SignersHash
		rawAction.MultisigExecuteOrderBoc = a.MultisigExecuteData.OrderBoc
	}

	// Convert VestingSendMessageData
	if a.VestingSendMessageData != nil {
		rawAction.VestingSendMessageQueryId = a.VestingSendMessageData.QueryId
		rawAction.VestingSendMessageMessageBoc = a.VestingSendMessageData.MessageBoc
	}

	// Convert VestingAddWhitelistData
	if a.VestingAddWhitelistData != nil {
		rawAction.VestingAddWhitelistQueryId = a.VestingAddWhitelistData.QueryId
		rawAction.VestingAddWhitelistAccountsAdded = make([]models.AccountAddress, len(a.VestingAddWhitelistData.AccountsAdded))
		for i, addr := range a.VestingAddWhitelistData.AccountsAdded {
			rawAction.VestingAddWhitelistAccountsAdded[i] = models.AccountAddress(addr)
		}
	}

	// Convert EvaaSupplyData
	if a.EvaaSupplyData != nil {
		rawAction.EvaaSupplySenderJettonWallet = (*models.AccountAddress)(a.EvaaSupplyData.SenderJettonWallet)
		rawAction.EvaaSupplyRecipientJettonWallet = (*models.AccountAddress)(a.EvaaSupplyData.RecipientJettonWallet)
		rawAction.EvaaSupplyMasterJettonWallet = (*models.AccountAddress)(a.EvaaSupplyData.MasterJettonWallet)
		rawAction.EvaaSupplyMaster = (*models.AccountAddress)(a.EvaaSupplyData.Master)
		rawAction.EvaaSupplyAssetId = a.EvaaSupplyData.AssetId
		rawAction.EvaaSupplyIsTon = a.EvaaSupplyData.IsTon
	}

	// Convert EvaaWithdrawData
	if a.EvaaWithdrawData != nil {
		rawAction.EvaaWithdrawRecipientJettonWallet = (*models.AccountAddress)(a.EvaaWithdrawData.RecipientJettonWallet)
		rawAction.EvaaWithdrawMasterJettonWallet = (*models.AccountAddress)(a.EvaaWithdrawData.MasterJettonWallet)
		rawAction.EvaaWithdrawMaster = (*models.AccountAddress)(a.EvaaWithdrawData.Master)
		rawAction.EvaaWithdrawFailReason = a.EvaaWithdrawData.FailReason
		rawAction.EvaaWithdrawAssetId = a.EvaaWithdrawData.AssetId
	}

	// Convert EvaaLiquidateData
	if a.EvaaLiquidateData != nil {
		rawAction.EvaaLiquidateFailReason = a.EvaaLiquidateData.FailReason
		rawAction.EvaaLiquidateDebtAmount = a.EvaaLiquidateData.DebtAmount
		rawAction.EvaaLiquidateAssetId = a.EvaaLiquidateData.AssetId
	}

	// Convert JvaultClaimData
	if a.JvaultClaimData != nil {
		rawAction.JvaultClaimClaimedJettons = make([]models.AccountAddress, len(a.JvaultClaimData.ClaimedJettons))
		for i, jetton := range a.JvaultClaimData.ClaimedJettons {
			rawAction.JvaultClaimClaimedJettons[i] = models.AccountAddress(jetton)
		}
		rawAction.JvaultClaimClaimedAmounts = a.JvaultClaimData.ClaimedAmounts
	}

	// Convert JvaultStakeData
	if a.JvaultStakeData != nil {
		rawAction.JvaultStakePeriod = a.JvaultStakeData.Period
		rawAction.JvaultStakeMintedStakeJettons = a.JvaultStakeData.MintedStakeJettons
		rawAction.JvaultStakeStakeWallet = (*models.AccountAddress)(a.JvaultStakeData.StakeWallet)
	}

	return rawAction, nil
}
