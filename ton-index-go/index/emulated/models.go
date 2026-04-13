package emulated

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
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
	Cells uint64 `msgpack:"cells" json:"cells"`
	Bits  uint64 `msgpack:"bits" json:"bits"`
}

type trActionPhase struct {
	Success         bool             `msgpack:"success" json:"success"`
	Valid           bool             `msgpack:"valid" json:"valid"`
	NoFunds         bool             `msgpack:"no_funds" json:"no_funds"`
	StatusChange    AccStatusChange  `msgpack:"status_change" json:"status_change"`
	TotalFwdFees    *uint64          `msgpack:"total_fwd_fees" json:"total_fwd_fees"`
	TotalActionFees *uint64          `msgpack:"total_action_fees" json:"total_action_fees"`
	ResultCode      *int32           `msgpack:"result_code" json:"result_code"`
	ResultArg       *int32           `msgpack:"result_arg" json:"result_arg"`
	TotActions      *uint16          `msgpack:"tot_actions" json:"tot_actions"`
	SpecActions     *uint16          `msgpack:"spec_actions" json:"spec_actions"`
	SkippedActions  *uint16          `msgpack:"skipped_actions" json:"skipped_actions"`
	MsgsCreated     *uint16          `msgpack:"msgs_created" json:"msgs_created"`
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
	ExtraFlags   *string `msgpack:"extra_flags" json:"extra_flags"`
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
	IsPurchase             bool    `msgpack:"is_purchase"`
	Price                  *string `msgpack:"price"`
	QueryId                *string `msgpack:"query_id"`
	CustomPayload          *string `msgpack:"custom_payload"`
	ForwardPayload         *string `msgpack:"forward_payload"`
	ForwardAmount          *string `msgpack:"forward_amount"`
	ResponseDestination    *string `msgpack:"response_destination"`
	NftItemIndex           *string `msgpack:"nft_item_index"`
	Marketplace            *string `msgpack:"marketplace"`
	RealPrevOwner          *string `msgpack:"real_prev_owner"`
	MarketplaceAddress     *string `msgpack:"marketplace_address"`
	PayoutAmount           *string `msgpack:"payout_amount"`
	PayoutCommentEncrypted *bool   `msgpack:"payout_comment_encrypted"`
	PayoutCommentEncoded   *bool   `msgpack:"payout_comment_encoded"`
	PayoutComment          *string `msgpack:"payout_comment"`
	RoyaltyAmount          *string `msgpack:"royalty_amount"`
}

type actionNftListingDetails struct {
	NftItemIndex          *string `msgpack:"nft_item_index"`
	FullPrice             *string `msgpack:"full_price"`
	MarketplaceFee        *string `msgpack:"marketplace_fee"`
	RoyaltyAmount         *string `msgpack:"royalty_amount"`
	MarketplaceFeeFactor  *string `msgpack:"mp_fee_factor"`
	MarketplaceFeeBase    *string `msgpack:"mp_fee_base"`
	RoyaltyFeeBase        *string `msgpack:"royalty_fee_base"`
	MaxBid                *string `msgpack:"max_bid"`
	MinBid                *string `msgpack:"min_bid"`
	MarketplaceFeeAddress *string `msgpack:"marketplace_fee_address"`
	RoyaltyAddress        *string `msgpack:"royalty_address"`
	Marketplace           *string `msgpack:"marketplace"`
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

type actionCoffeeCreatePoolDetails struct {
	Amount1             *string `msgpack:"amount_1"`
	Amount2             *string `msgpack:"amount_2"`
	Initiator1          *string `msgpack:"initiator_1"`
	Initiator2          *string `msgpack:"initiator_2"`
	ProvidedAsset       *string `msgpack:"provided_asset"`
	LpTokensMinted      *string `msgpack:"lp_tokens_minted"`
	PoolCreatorContract *string `msgpack:"pool_creator_contract"`
}

type actionCoffeeStakingDepositDetails struct {
	MintedItemAddress *string `msgpack:"minted_item_address"`
	MintedItemIndex   *string `msgpack:"minted_item_index"`
}

type actionCoffeeStakingWithdrawDetails struct {
	NftAddress *string `msgpack:"nft_address"`
	NftIndex   *string `msgpack:"nft_index"`
	Points     *string `msgpack:"points"`
}

type actionLayerzeroSendDetails struct {
	SendRequestId *uint64 `msgpack:"send_request_id"`
	MsglibManager *string `msgpack:"msglib_manager"`
	Msglib        *string `msgpack:"msglib"`
	Uln           *string `msgpack:"uln"`
	NativeFee     *uint64 `msgpack:"native_fee"`
	ZroFee        *uint64 `msgpack:"zro_fee"`
	Endpoint      *string `msgpack:"endpoint"`
	Channel       *string `msgpack:"channel"`
}

type actionLayerzeroPacketDetails struct {
	SrcOapp *string `msgpack:"src_oapp"`
	DstOapp *string `msgpack:"dst_oapp"`
	SrcEid  *int32  `msgpack:"src_eid"`
	DstEid  *int32  `msgpack:"dst_eid"`
	Nonce   *int64  `msgpack:"nonce"`
	Guid    *string `msgpack:"guid"`
	Message *string `msgpack:"message"`
}

type actionLayerzeroDvnVerifyDetails struct {
	Nonce         *int64  `msgpack:"nonce"`
	Status        *string `msgpack:"status"` // "succeeded" | "nonce_out_of_range" | "dvn_not_configured" | "unknown_<code>"
	Dvn           *string `msgpack:"dvn"`
	Proxy         *string `msgpack:"proxy"`
	Uln           *string `msgpack:"uln"`
	UlnConnection *string `msgpack:"uln_connection"`
}

type actionCocoonWorkerPayoutDetails struct {
	PayoutType   *string `msgpack:"payout_type"`
	QueryId      *string `msgpack:"query_id"`
	NewTokens    *string `msgpack:"new_tokens"`
	WorkerState  *int64  `msgpack:"worker_state"`
	WorkerTokens *string `msgpack:"worker_tokens"`
}

type actionCocoonProxyPayoutDetails struct {
	QueryId *string `msgpack:"query_id"`
}

type actionCocoonProxyChargeDetails struct {
	QueryId         *string `msgpack:"query_id"`
	NewTokensUsed   *string `msgpack:"new_tokens_used"`
	ExpectedAddress *string `msgpack:"expected_address"`
}

type actionCocoonClientTopUpDetails struct {
	QueryId *string `msgpack:"query_id"`
}

type actionCocoonRegisterProxyDetails struct {
	QueryId *string `msgpack:"query_id"`
}

type actionCocoonUnregisterProxyDetails struct {
	QueryId *string `msgpack:"query_id"`
	Seqno   *int64  `msgpack:"seqno"`
}

type actionCocoonClientRegisterDetails struct {
	QueryId *string `msgpack:"query_id"`
	Nonce   *string `msgpack:"nonce"`
}

type actionCocoonClientChangeSecretHashDetails struct {
	QueryId       *string `msgpack:"query_id"`
	NewSecretHash *string `msgpack:"new_secret_hash"`
}

type actionCocoonClientRequestRefundDetails struct {
	QueryId   *string `msgpack:"query_id"`
	ViaWallet *bool   `msgpack:"via_wallet"`
}

type actionCocoonGrantRefundDetails struct {
	QueryId         *string `msgpack:"query_id"`
	NewTokensUsed   *string `msgpack:"new_tokens_used"`
	ExpectedAddress *string `msgpack:"expected_address"`
}

type actionCocoonClientIncreaseStakeDetails struct {
	QueryId  *string `msgpack:"query_id"`
	NewStake *string `msgpack:"new_stake"`
}

type actionCocoonClientWithdrawDetails struct {
	QueryId        *string `msgpack:"query_id"`
	WithdrawAmount *string `msgpack:"withdraw_amount"`
}

type Action struct {
	ActionId                         string                                     `msgpack:"action_id"`
	Type                             string                                     `msgpack:"type"`
	TraceId                          *string                                    `msgpack:"trace_id"`
	TraceExternalHash                string                                     `msgpack:"trace_external_hash"`
	TraceExternalHashNorm            *string                                    `msgpack:"trace_external_hash_norm"`
	TxHashes                         []string                                   `msgpack:"tx_hashes"`
	Value                            *string                                    `msgpack:"value"`
	Amount                           *string                                    `msgpack:"amount"`
	StartLt                          *uint64                                    `msgpack:"start_lt"`
	EndLt                            *uint64                                    `msgpack:"end_lt"`
	StartUtime                       *uint32                                    `msgpack:"start_utime"`
	EndUtime                         *uint32                                    `msgpack:"end_utime"`
	TraceEndLt                       *uint64                                    `msgpack:"trace_end_lt"`
	TraceEndUtime                    *uint32                                    `msgpack:"trace_end_utime"`
	TraceStartLt                     *uint64                                    `msgpack:"trace_start_lt"`
	TraceMcSeqnoEnd                  *uint32                                    `msgpack:"trace_mc_seqno_end"`
	Source                           *string                                    `msgpack:"source"`
	SourceSecondary                  *string                                    `msgpack:"source_secondary"`
	Destination                      *string                                    `msgpack:"destination"`
	DestinationSecondary             *string                                    `msgpack:"destination_secondary"`
	Asset                            *string                                    `msgpack:"asset"`
	AssetSecondary                   *string                                    `msgpack:"asset_secondary"`
	Asset2                           *string                                    `msgpack:"asset2"`
	Asset2Secondary                  *string                                    `msgpack:"asset2_secondary"`
	Opcode                           *uint32                                    `msgpack:"opcode"`
	Success                          bool                                       `msgpack:"success"`
	Finality                         models.FinalityState                       `msgpack:"finality"`
	TonTransferData                  *actionTonTransferDetails                  `msgpack:"ton_transfer_data"`
	AncestorType                     []string                                   `msgpack:"ancestor_type"`
	ParentActionId                   *string                                    `msgpack:"parent_action_id"`
	Accounts                         []string                                   `msgpack:"accounts"`
	JettonTransferData               *actionJettonTransferDetails               `msgpack:"jetton_transfer_data"`
	NftTransferData                  *actionNftTransferDetails                  `msgpack:"nft_transfer_data"`
	NftListingData                   *actionNftListingDetails                   `msgpack:"nft_listing_data"`
	JettonSwapData                   *actionJettonSwapDetails                   `msgpack:"jetton_swap_data"`
	ChangeDnsRecordData              *actionChangeDnsRecordDetails              `msgpack:"change_dns_record_data"`
	NftMintData                      *actionNftMintDetails                      `msgpack:"nft_mint_data"`
	DexDepositLiquidityData          *actionDexDepositLiquidityData             `msgpack:"dex_deposit_liquidity_data"`
	DexWithdrawLiquidityData         *actionDexWithdrawLiquidityData            `msgpack:"dex_withdraw_liquidity_data"`
	StakingData                      *actionStakingData                         `msgpack:"staking_data"`
	ToncoDeployPoolData              *actionToncoDeployPoolDetails              `msgpack:"tonco_deploy_pool_data"`
	MultisigCreateOrderData          *actionMultisigCreateOrderDetails          `msgpack:"multisig_create_order_data"`
	MultisigApproveData              *actionMultisigApproveDetails              `msgpack:"multisig_approve_data"`
	MultisigExecuteData              *actionMultisigExecuteDetails              `msgpack:"multisig_execute_data"`
	VestingSendMessageData           *actionVestingSendMessageDetails           `msgpack:"vesting_send_message_data"`
	VestingAddWhitelistData          *actionVestingAddWhitelistDetails          `msgpack:"vesting_add_whitelist_data"`
	EvaaSupplyData                   *actionEvaaSupplyDetails                   `msgpack:"evaa_supply_data"`
	EvaaWithdrawData                 *actionEvaaWithdrawDetails                 `msgpack:"evaa_withdraw_data"`
	EvaaLiquidateData                *actionEvaaLiquidateDetails                `msgpack:"evaa_liquidate_data"`
	JvaultClaimData                  *actionJvaultClaimDetails                  `msgpack:"jvault_claim_data"`
	JvaultStakeData                  *actionJvaultStakeDetails                  `msgpack:"jvault_stake_data"`
	CoffeeCreatePoolData             *actionCoffeeCreatePoolDetails             `msgpack:"coffee_create_pool_data"`
	CoffeeStakingDepositData         *actionCoffeeStakingDepositDetails         `msgpack:"coffee_staking_deposit_data"`
	CoffeeStakingWithdrawData        *actionCoffeeStakingWithdrawDetails        `msgpack:"coffee_staking_withdraw_data"`
	LayerzeroSendData                *actionLayerzeroSendDetails                `msgpack:"layerzero_send_data"`
	LayerzeroPacketData              *actionLayerzeroPacketDetails              `msgpack:"layerzero_packet_data"`
	LayerzeroDvnVerifyData           *actionLayerzeroDvnVerifyDetails           `msgpack:"layerzero_dvn_verify_data"`
	CocoonWorkerPayoutData           *actionCocoonWorkerPayoutDetails           `msgpack:"cocoon_worker_payout_data"`
	CocoonProxyPayoutData            *actionCocoonProxyPayoutDetails            `msgpack:"cocoon_proxy_payout_data"`
	CocoonProxyChargeData            *actionCocoonProxyChargeDetails            `msgpack:"cocoon_proxy_charge_data"`
	CocoonClientTopUpData            *actionCocoonClientTopUpDetails            `msgpack:"cocoon_client_top_up_data"`
	CocoonRegisterProxyData          *actionCocoonRegisterProxyDetails          `msgpack:"cocoon_register_proxy_data"`
	CocoonUnregisterProxyData        *actionCocoonUnregisterProxyDetails        `msgpack:"cocoon_unregister_proxy_data"`
	CocoonClientRegisterData         *actionCocoonClientRegisterDetails         `msgpack:"cocoon_client_register_data"`
	CocoonClientChangeSecretHashData *actionCocoonClientChangeSecretHashDetails `msgpack:"cocoon_client_change_secret_hash_data"`
	CocoonClientRequestRefundData    *actionCocoonClientRequestRefundDetails    `msgpack:"cocoon_client_request_refund_data"`
	CocoonGrantRefundData            *actionCocoonGrantRefundDetails            `msgpack:"cocoon_grant_refund_data"`
	CocoonClientIncreaseStakeData    *actionCocoonClientIncreaseStakeDetails    `msgpack:"cocoon_client_increase_stake_data"`
	CocoonClientWithdrawData         *actionCocoonClientWithdrawDetails         `msgpack:"cocoon_client_withdraw_data"`
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
	Transaction   transaction          `msgpack:"transaction"`
	Emulated      bool                 `msgpack:"emulated"`
	BlockId       blockId              `msgpack:"block_id"`
	McBlockSeqno  uint32               `msgpack:"mc_block_seqno"`
	FinalityState models.FinalityState `msgpack:"finality"`
	TraceId       *string
	Key           string
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

func decompressGzip(data []byte) ([]byte, error) {
	if len(data) < 2 {
		return data, nil // too short for gzip
	}
	// check if gzip, else return data unchanged
	if data[0] == 0x1f && data[1] == 0x8b {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return io.ReadAll(reader)
	} else {
		return data, nil
	}
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
		decompressed, err := decompressGzip([]byte(actionsBytes))
		if err != nil {
			return Trace{}, fmt.Errorf("failed to decompress actions: %w", err)
		}
		err = msgpack.Unmarshal(decompressed, &actions)
		if err != nil {
			return Trace{}, fmt.Errorf("failed to unmarshal actions: %w", err)
		}
		for i := range actions {
			actions[i].TraceEndUtime = &endUtime
			actions[i].TraceEndLt = &endLt
			actions[i].TraceMcSeqnoEnd = &mcSeqnoEnd
			actions[i].TraceExternalHash = rootNodeId
			actions[i].TraceId = traceId
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

func ptrInt64FromUint64(v *uint64) *int64 {
	if v == nil {
		return nil
	}
	i := int64(*v)
	return &i
}

func ptrInt32FromUint16(v *uint16) *int32 {
	if v == nil {
		return nil
	}
	i := int32(*v)
	return &i
}

func ptrAccountAddress(s *string) *models.AccountAddress {
	return (*models.AccountAddress)(s)
}

func ptrHashType(s *string) *models.HashType {
	return (*models.HashType)(s)
}

func ptrOpcodeFromUint32(v *uint32) *models.OpcodeType {
	if v == nil {
		return nil
	}
	o := models.OpcodeType(int64(*v))
	return &o
}

func ptrOpcodeFromInt32(v *int32) *models.OpcodeType {
	if v == nil {
		return nil
	}
	o := models.OpcodeType(int64(*v))
	return &o
}

func int64FromPtrUint64(v *uint64) int64 {
	if v == nil {
		return 0
	}
	return int64(*v)
}

func int64FromPtrUint32(v *uint32) int64 {
	if v == nil {
		return 0
	}
	return int64(*v)
}

func int32FromPtrUint32(v *uint32) int32 {
	if v == nil {
		return 0
	}
	return int32(*v)
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

func (a *Action) ToRawAction() (*models.RawAction, error) {
	var traceExternalHashNorm *models.HashType
	if a.TraceExternalHashNorm != nil && *a.TraceExternalHashNorm != a.TraceExternalHash {
		traceExternalHashNorm = (*models.HashType)(a.TraceExternalHashNorm)
	}

	// Convert TxHashes []string -> []models.HashType
	txHashes := make([]models.HashType, len(a.TxHashes))
	for i, h := range a.TxHashes {
		txHashes[i] = models.HashType(h)
	}
	success := a.Success

	traceExternalHash := models.HashType(a.TraceExternalHash)

	raw := &models.RawAction{
		TraceId:               ptrHashType(a.TraceId),
		ActionId:              models.HashType(a.ActionId),
		StartLt:               int64FromPtrUint64(a.StartLt),
		EndLt:                 int64FromPtrUint64(a.EndLt),
		StartUtime:            int64FromPtrUint32(a.StartUtime),
		EndUtime:              int64FromPtrUint32(a.EndUtime),
		TraceEndLt:            int64FromPtrUint64(a.TraceEndLt),
		TraceEndUtime:         int64FromPtrUint32(a.TraceEndUtime),
		TraceMcSeqnoEnd:       int32FromPtrUint32(a.TraceMcSeqnoEnd),
		Source:                ptrAccountAddress(a.Source),
		SourceSecondary:       ptrAccountAddress(a.SourceSecondary),
		Destination:           ptrAccountAddress(a.Destination),
		DestinationSecondary:  ptrAccountAddress(a.DestinationSecondary),
		Asset:                 ptrAccountAddress(a.Asset),
		AssetSecondary:        ptrAccountAddress(a.AssetSecondary),
		Asset2:                ptrAccountAddress(a.Asset2),
		Asset2Secondary:       ptrAccountAddress(a.Asset2Secondary),
		Opcode:                ptrOpcodeFromUint32(a.Opcode),
		TxHashes:              txHashes,
		Type:                  a.Type,
		Value:                 a.Value,
		Amount:                a.Amount,
		Success:               &success,
		Finality:              a.Finality,
		TraceExternalHash:     &traceExternalHash,
		TraceExternalHashNorm: traceExternalHashNorm,
		ExtraCurrencies:       map[string]string{},
		AncestorType:          a.AncestorType,
		Accounts:              a.Accounts,
	}

	if a.TonTransferData != nil {
		raw.TonTransferContent = a.TonTransferData.Content
		raw.TonTransferEncrypted = &a.TonTransferData.Encrypted
	}
	if a.JettonTransferData != nil {
		raw.JettonTransferResponseDestination = ptrAccountAddress(a.JettonTransferData.ResponseDestination)
		raw.JettonTransferForwardAmount = a.JettonTransferData.ForwardAmount
		raw.JettonTransferQueryId = a.JettonTransferData.QueryId
		raw.JettonTransferCustomPayload = a.JettonTransferData.CustomPayload
		raw.JettonTransferForwardPayload = a.JettonTransferData.ForwardPayload
		raw.JettonTransferComment = a.JettonTransferData.Comment
		raw.JettonTransferIsEncryptedComment = a.JettonTransferData.IsEncryptedComment
	}
	if a.NftTransferData != nil {
		raw.NFTTransferIsPurchase = &a.NftTransferData.IsPurchase
		raw.NFTTransferPrice = a.NftTransferData.Price
		raw.NFTTransferQueryId = a.NftTransferData.QueryId
		raw.NFTTransferCustomPayload = a.NftTransferData.CustomPayload
		raw.NFTTransferForwardPayload = a.NftTransferData.ForwardPayload
		raw.NFTTransferForwardAmount = a.NftTransferData.ForwardAmount
		raw.NFTTransferResponseDestination = ptrAccountAddress(a.NftTransferData.ResponseDestination)
		raw.NFTTransferNFTItemIndex = a.NftTransferData.NftItemIndex
		raw.NFTTransferMarketplace = a.NftTransferData.Marketplace
		raw.NFTTransferRealPrevOwner = ptrAccountAddress(a.NftTransferData.RealPrevOwner)
		raw.NFTTransferMarketplaceAddress = ptrAccountAddress(a.NftTransferData.MarketplaceAddress)
		raw.NFTTransferPayoutAmount = a.NftTransferData.PayoutAmount
		raw.NFTTransferPayoutCommentEncrypted = a.NftTransferData.PayoutCommentEncrypted
		raw.NFTTransferPayoutCommentEncoded = a.NftTransferData.PayoutCommentEncoded
		raw.NFTTransferPayoutComment = a.NftTransferData.PayoutComment
		raw.NFTTransferRoyaltyAmount = a.NftTransferData.RoyaltyAmount
	}
	if a.NftListingData != nil {
		raw.NFTListingNFTItemIndex = a.NftListingData.NftItemIndex
		raw.NFTListingFullPrice = a.NftListingData.FullPrice
		raw.NFTListingMarketplaceFee = a.NftListingData.MarketplaceFee
		raw.NFTListingRoyaltyAmount = a.NftListingData.RoyaltyAmount
		raw.NFTListingMarketplaceFeeFactor = a.NftListingData.MarketplaceFeeFactor
		raw.NFTListingMarketplaceFeeBase = a.NftListingData.MarketplaceFeeBase
		raw.NFTListingRoyaltyFeeBase = a.NftListingData.RoyaltyFeeBase
		raw.NFTListingMaxBid = a.NftListingData.MaxBid
		raw.NFTListingMinBid = a.NftListingData.MinBid
		raw.NFTListingMarketplaceFeeAddress = ptrAccountAddress(a.NftListingData.MarketplaceFeeAddress)
		raw.NFTListingRoyaltyAddress = ptrAccountAddress(a.NftListingData.RoyaltyAddress)
		raw.NFTListingMarketplace = a.NftListingData.Marketplace
	}
	if a.JettonSwapData != nil {
		raw.JettonSwapDex = a.JettonSwapData.Dex
		raw.JettonSwapSender = ptrAccountAddress(a.JettonSwapData.Sender)
		raw.JettonSwapDexIncomingTransferAmount = a.JettonSwapData.DexIncomingTransfer.Amount
		raw.JettonSwapDexIncomingTransferAsset = ptrAccountAddress(a.JettonSwapData.DexIncomingTransfer.Asset)
		raw.JettonSwapDexIncomingTransferSource = ptrAccountAddress(a.JettonSwapData.DexIncomingTransfer.Source)
		raw.JettonSwapDexIncomingTransferDestination = ptrAccountAddress(a.JettonSwapData.DexIncomingTransfer.Destination)
		raw.JettonSwapDexIncomingTransferSourceJettonWallet = ptrAccountAddress(a.JettonSwapData.DexIncomingTransfer.SourceJettonWallet)
		raw.JettonSwapDexIncomingTransferDestinationJettonWallet = ptrAccountAddress(a.JettonSwapData.DexIncomingTransfer.DestinationJettonWallet)
		raw.JettonSwapDexOutgoingTransferAmount = a.JettonSwapData.DexOutgoingTransfer.Amount
		raw.JettonSwapDexOutgoingTransferAsset = ptrAccountAddress(a.JettonSwapData.DexOutgoingTransfer.Asset)
		raw.JettonSwapDexOutgoingTransferSource = ptrAccountAddress(a.JettonSwapData.DexOutgoingTransfer.Source)
		raw.JettonSwapDexOutgoingTransferDestination = ptrAccountAddress(a.JettonSwapData.DexOutgoingTransfer.Destination)
		raw.JettonSwapDexOutgoingTransferSourceJettonWallet = ptrAccountAddress(a.JettonSwapData.DexOutgoingTransfer.SourceJettonWallet)
		raw.JettonSwapDexOutgoingTransferDestinationJettonWallet = ptrAccountAddress(a.JettonSwapData.DexOutgoingTransfer.DestinationJettonWallet)

		if len(a.JettonSwapData.PeerSwaps) > 0 {
			raw.JettonSwapPeerSwaps = make([]models.RawActionJettonSwapPeerSwap, len(a.JettonSwapData.PeerSwaps))
			for i, ps := range a.JettonSwapData.PeerSwaps {
				raw.JettonSwapPeerSwaps[i] = models.RawActionJettonSwapPeerSwap{
					AssetIn:   ptrAccountAddress(ps.AssetIn),
					AmountIn:  ps.AmountIn,
					AssetOut:  ptrAccountAddress(ps.AssetOut),
					AmountOut: ps.AmountOut,
				}
			}
		}
		raw.JettonSwapMinOutAmount = a.JettonSwapData.MinOutAmount
	}
	if a.ChangeDnsRecordData != nil {
		var dnsRecordsFlag *int64
		if a.ChangeDnsRecordData.Flags != nil {
			v, err := strconv.ParseInt(*a.ChangeDnsRecordData.Flags, 10, 64)
			if err != nil {
				return nil, err
			}
			dnsRecordsFlag = &v
		}
		raw.ChangeDNSRecordKey = a.ChangeDnsRecordData.Key
		raw.ChangeDNSRecordValueSchema = a.ChangeDnsRecordData.ValueSchema
		raw.ChangeDNSRecordValue = a.ChangeDnsRecordData.Value
		raw.ChangeDNSRecordFlags = dnsRecordsFlag
	}
	if a.NftMintData != nil {
		raw.NFTMintNFTItemIndex = a.NftMintData.NftItemIndex
	}
	if a.DexDepositLiquidityData != nil {
		raw.DexDepositLiquidityDataDex = a.DexDepositLiquidityData.Dex
		raw.DexDepositLiquidityDataAmount1 = a.DexDepositLiquidityData.Amount1
		raw.DexDepositLiquidityDataAmount2 = a.DexDepositLiquidityData.Amount2
		raw.DexDepositLiquidityDataAsset1 = ptrAccountAddress(a.DexDepositLiquidityData.Asset1)
		raw.DexDepositLiquidityDataAsset2 = ptrAccountAddress(a.DexDepositLiquidityData.Asset2)
		raw.DexDepositLiquidityDataUserJettonWallet1 = ptrAccountAddress(a.DexDepositLiquidityData.UserJettonWallet1)
		raw.DexDepositLiquidityDataUserJettonWallet2 = ptrAccountAddress(a.DexDepositLiquidityData.UserJettonWallet2)
		raw.DexDepositLiquidityDataLpTokensMinted = a.DexDepositLiquidityData.LpTokensMinted
		raw.DexDepositLiquidityDataTargetAsset1 = ptrAccountAddress(a.DexDepositLiquidityData.TargetAsset1)
		raw.DexDepositLiquidityDataTargetAsset2 = ptrAccountAddress(a.DexDepositLiquidityData.TargetAsset2)
		raw.DexDepositLiquidityDataTargetAmount1 = a.DexDepositLiquidityData.TargetAmount1
		raw.DexDepositLiquidityDataTargetAmount2 = a.DexDepositLiquidityData.TargetAmount2
		if len(a.DexDepositLiquidityData.VaultExcesses) > 0 {
			raw.DexDepositLiquidityDataVaultExcesses = make([]models.RawActionVaultExcessEntry, len(a.DexDepositLiquidityData.VaultExcesses))
			for i, ve := range a.DexDepositLiquidityData.VaultExcesses {
				raw.DexDepositLiquidityDataVaultExcesses[i] = models.RawActionVaultExcessEntry{
					Asset:  ptrAccountAddress(ve.Asset),
					Amount: ve.Amount,
				}
			}
		}
		raw.DexDepositLiquidityDataTickLower = a.DexDepositLiquidityData.TickLower
		raw.DexDepositLiquidityDataTickUpper = a.DexDepositLiquidityData.TickUpper
		raw.DexDepositLiquidityDataNFTIndex = a.DexDepositLiquidityData.NFTIndex
		raw.DexDepositLiquidityDataNFTAddress = ptrAccountAddress(a.DexDepositLiquidityData.NFTAddress)
	}
	if a.DexWithdrawLiquidityData != nil {
		raw.DexWithdrawLiquidityDataDex = a.DexWithdrawLiquidityData.Dex
		raw.DexWithdrawLiquidityDataAmount1 = a.DexWithdrawLiquidityData.Amount1
		raw.DexWithdrawLiquidityDataAmount2 = a.DexWithdrawLiquidityData.Amount2
		raw.DexWithdrawLiquidityDataAsset1Out = ptrAccountAddress(a.DexWithdrawLiquidityData.Asset1Out)
		raw.DexWithdrawLiquidityDataAsset2Out = ptrAccountAddress(a.DexWithdrawLiquidityData.Asset2Out)
		raw.DexWithdrawLiquidityDataUserJettonWallet1 = ptrAccountAddress(a.DexWithdrawLiquidityData.UserJettonWallet1)
		raw.DexWithdrawLiquidityDataUserJettonWallet2 = ptrAccountAddress(a.DexWithdrawLiquidityData.UserJettonWallet2)
		raw.DexWithdrawLiquidityDataDexJettonWallet1 = ptrAccountAddress(a.DexWithdrawLiquidityData.DexJettonWallet1)
		raw.DexWithdrawLiquidityDataDexJettonWallet2 = ptrAccountAddress(a.DexWithdrawLiquidityData.DexJettonWallet2)
		raw.DexWithdrawLiquidityDataLpTokensBurnt = a.DexWithdrawLiquidityData.LpTokensBurnt
		raw.DexWithdrawLiquidityDataBurnedNFTIndex = a.DexWithdrawLiquidityData.BurnedNFTIndex
		raw.DexWithdrawLiquidityDataBurnedNFTAddress = ptrAccountAddress(a.DexWithdrawLiquidityData.BurnedNFTAddress)
		raw.DexWithdrawLiquidityDataTickLower = a.DexWithdrawLiquidityData.TickLower
		raw.DexWithdrawLiquidityDataTickUpper = a.DexWithdrawLiquidityData.TickUpper
	}
	if a.StakingData != nil {
		raw.StakingDataProvider = a.StakingData.Provider
		raw.StakingDataTsNft = ptrAccountAddress(a.StakingData.TsNft)
		raw.StakingDataTokensBurnt = a.StakingData.TokensBurnt
		raw.StakingDataTokensMinted = a.StakingData.TokensMinted
	}
	if a.ToncoDeployPoolData != nil {
		raw.ToncoDeployPoolJetton0RouterWallet = ptrAccountAddress(a.ToncoDeployPoolData.Jetton0RouterWallet)
		raw.ToncoDeployPoolJetton1RouterWallet = ptrAccountAddress(a.ToncoDeployPoolData.Jetton1RouterWallet)
		raw.ToncoDeployPoolJetton0Minter = ptrAccountAddress(a.ToncoDeployPoolData.Jetton0Minter)
		raw.ToncoDeployPoolJetton1Minter = ptrAccountAddress(a.ToncoDeployPoolData.Jetton1Minter)
		raw.ToncoDeployPoolTickSpacing = a.ToncoDeployPoolData.TickSpacing
		raw.ToncoDeployPoolInitialPriceX96 = a.ToncoDeployPoolData.InitialPriceX96
		raw.ToncoDeployPoolProtocolFee = a.ToncoDeployPoolData.ProtocolFee
		raw.ToncoDeployPoolLpFeeBase = a.ToncoDeployPoolData.LpFeeBase
		raw.ToncoDeployPoolLpFeeCurrent = a.ToncoDeployPoolData.LpFeeCurrent
		raw.ToncoDeployPoolPoolActive = a.ToncoDeployPoolData.PoolActive
	}
	if a.MultisigCreateOrderData != nil {
		raw.MultisigCreateOrderQueryId = a.MultisigCreateOrderData.QueryId
		raw.MultisigCreateOrderOrderSeqno = a.MultisigCreateOrderData.OrderSeqno
		raw.MultisigCreateOrderIsCreatedBySigner = a.MultisigCreateOrderData.IsCreatedBySigner
		raw.MultisigCreateOrderIsSignedByCreator = a.MultisigCreateOrderData.IsSignedByCreator
		raw.MultisigCreateOrderCreatorIndex = a.MultisigCreateOrderData.CreatorIndex
		raw.MultisigCreateOrderExpirationDate = a.MultisigCreateOrderData.ExpirationDate
		raw.MultisigCreateOrderOrderBoc = a.MultisigCreateOrderData.OrderBoc
	}
	if a.MultisigApproveData != nil {
		raw.MultisigApproveSignerIndex = a.MultisigApproveData.SignerIndex
		raw.MultisigApproveExitCode = a.MultisigApproveData.ExitCode
	}
	if a.MultisigExecuteData != nil {
		raw.MultisigExecuteQueryId = a.MultisigExecuteData.QueryId
		raw.MultisigExecuteOrderSeqno = a.MultisigExecuteData.OrderSeqno
		raw.MultisigExecuteExpirationDate = a.MultisigExecuteData.ExpirationDate
		raw.MultisigExecuteApprovalsNum = a.MultisigExecuteData.ApprovalsNum
		raw.MultisigExecuteSignersHash = a.MultisigExecuteData.SignersHash
		raw.MultisigExecuteOrderBoc = a.MultisigExecuteData.OrderBoc
	}
	if a.VestingSendMessageData != nil {
		raw.VestingSendMessageQueryId = a.VestingSendMessageData.QueryId
		raw.VestingSendMessageMessageBoc = a.VestingSendMessageData.MessageBoc
	}
	if a.VestingAddWhitelistData != nil {
		raw.VestingAddWhitelistQueryId = a.VestingAddWhitelistData.QueryId
		if len(a.VestingAddWhitelistData.AccountsAdded) > 0 {
			accts := make([]models.AccountAddress, len(a.VestingAddWhitelistData.AccountsAdded))
			for i, s := range a.VestingAddWhitelistData.AccountsAdded {
				accts[i] = models.AccountAddress(s)
			}
			raw.VestingAddWhitelistAccountsAdded = accts
		}
	}
	if a.EvaaSupplyData != nil {
		raw.EvaaSupplySenderJettonWallet = ptrAccountAddress(a.EvaaSupplyData.SenderJettonWallet)
		raw.EvaaSupplyRecipientJettonWallet = ptrAccountAddress(a.EvaaSupplyData.RecipientJettonWallet)
		raw.EvaaSupplyMasterJettonWallet = ptrAccountAddress(a.EvaaSupplyData.MasterJettonWallet)
		raw.EvaaSupplyMaster = ptrAccountAddress(a.EvaaSupplyData.Master)
		raw.EvaaSupplyAssetId = a.EvaaSupplyData.AssetId
		raw.EvaaSupplyIsTon = a.EvaaSupplyData.IsTon
	}
	if a.EvaaWithdrawData != nil {
		raw.EvaaWithdrawRecipientJettonWallet = ptrAccountAddress(a.EvaaWithdrawData.RecipientJettonWallet)
		raw.EvaaWithdrawMasterJettonWallet = ptrAccountAddress(a.EvaaWithdrawData.MasterJettonWallet)
		raw.EvaaWithdrawMaster = ptrAccountAddress(a.EvaaWithdrawData.Master)
		raw.EvaaWithdrawFailReason = a.EvaaWithdrawData.FailReason
		raw.EvaaWithdrawAssetId = a.EvaaWithdrawData.AssetId
	}
	if a.EvaaLiquidateData != nil {
		raw.EvaaLiquidateFailReason = a.EvaaLiquidateData.FailReason
		raw.EvaaLiquidateDebtAmount = a.EvaaLiquidateData.DebtAmount
		raw.EvaaLiquidateAssetId = a.EvaaLiquidateData.AssetId
	}
	if a.JvaultClaimData != nil {
		if len(a.JvaultClaimData.ClaimedJettons) > 0 {
			cj := make([]models.AccountAddress, len(a.JvaultClaimData.ClaimedJettons))
			for i, s := range a.JvaultClaimData.ClaimedJettons {
				cj[i] = models.AccountAddress(s)
			}
			raw.JvaultClaimClaimedJettons = cj
		}
		raw.JvaultClaimClaimedAmounts = a.JvaultClaimData.ClaimedAmounts
	}
	if a.JvaultStakeData != nil {
		raw.JvaultStakePeriod = a.JvaultStakeData.Period
		raw.JvaultStakeMintedStakeJettons = a.JvaultStakeData.MintedStakeJettons
		raw.JvaultStakeStakeWallet = ptrAccountAddress(a.JvaultStakeData.StakeWallet)
	}
	if a.CoffeeCreatePoolData != nil {
		raw.CoffeeCreatePoolAmount1 = a.CoffeeCreatePoolData.Amount1
		raw.CoffeeCreatePoolAmount2 = a.CoffeeCreatePoolData.Amount2
		raw.CoffeeCreatePoolInitiator1 = ptrAccountAddress(a.CoffeeCreatePoolData.Initiator1)
		raw.CoffeeCreatePoolInitiator2 = ptrAccountAddress(a.CoffeeCreatePoolData.Initiator2)
		raw.CoffeeCreatePoolProvidedAsset = ptrAccountAddress(a.CoffeeCreatePoolData.ProvidedAsset)
		raw.CoffeeCreatePoolLpTokensMinted = a.CoffeeCreatePoolData.LpTokensMinted
		raw.CoffeeCreatePoolPoolCreatorContract = ptrAccountAddress(a.CoffeeCreatePoolData.PoolCreatorContract)
	}
	if a.CoffeeStakingDepositData != nil {
		raw.CoffeeStakingDepositMintedItemAddress = ptrAccountAddress(a.CoffeeStakingDepositData.MintedItemAddress)
		raw.CoffeeStakingDepositMintedItemIndex = a.CoffeeStakingDepositData.MintedItemIndex
	}
	if a.CoffeeStakingWithdrawData != nil {
		raw.CoffeeStakingWithdrawNftAddress = ptrAccountAddress(a.CoffeeStakingWithdrawData.NftAddress)
		raw.CoffeeStakingWithdrawNftIndex = a.CoffeeStakingWithdrawData.NftIndex
		raw.CoffeeStakingWithdrawPoints = a.CoffeeStakingWithdrawData.Points
	}
	if a.LayerzeroSendData != nil {
		raw.LayerzeroSendSendRequestId = a.LayerzeroSendData.SendRequestId
		raw.LayerzeroSendMsglibManager = a.LayerzeroSendData.MsglibManager
		raw.LayerzeroSendMsglib = a.LayerzeroSendData.Msglib
		raw.LayerzeroSendUln = ptrAccountAddress(a.LayerzeroSendData.Uln)
		raw.LayerzeroSendNativeFee = a.LayerzeroSendData.NativeFee
		raw.LayerzeroSendZroFee = a.LayerzeroSendData.ZroFee
		raw.LayerzeroSendEndpoint = ptrAccountAddress(a.LayerzeroSendData.Endpoint)
		raw.LayerzeroSendChannel = ptrAccountAddress(a.LayerzeroSendData.Channel)
	}
	if a.LayerzeroPacketData != nil {
		raw.LayerzeroPacketSrcOapp = a.LayerzeroPacketData.SrcOapp
		raw.LayerzeroPacketDstOapp = a.LayerzeroPacketData.DstOapp
		raw.LayerzeroPacketSrcEid = a.LayerzeroPacketData.SrcEid
		raw.LayerzeroPacketDstEid = a.LayerzeroPacketData.DstEid
		raw.LayerzeroPacketNonce = a.LayerzeroPacketData.Nonce
		raw.LayerzeroPacketGuid = a.LayerzeroPacketData.Guid
		raw.LayerzeroPacketMessage = a.LayerzeroPacketData.Message
	}
	if a.LayerzeroDvnVerifyData != nil {
		raw.LayerzeroDvnVerifyNonce = a.LayerzeroDvnVerifyData.Nonce
		raw.LayerzeroDvnVerifyStatus = a.LayerzeroDvnVerifyData.Status
		raw.LayerzeroDvnVerifyDvn = ptrAccountAddress(a.LayerzeroDvnVerifyData.Dvn)
		raw.LayerzeroDvnVerifyProxy = ptrAccountAddress(a.LayerzeroDvnVerifyData.Proxy)
		raw.LayerzeroDvnVerifyUln = ptrAccountAddress(a.LayerzeroDvnVerifyData.Uln)
		raw.LayerzeroDvnVerifyUlnConnection = ptrAccountAddress(a.LayerzeroDvnVerifyData.UlnConnection)
	}
	if a.CocoonWorkerPayoutData != nil {
		raw.CocoonWorkerPayoutPayoutType = a.CocoonWorkerPayoutData.PayoutType
		raw.CocoonWorkerPayoutQueryId = a.CocoonWorkerPayoutData.QueryId
		raw.CocoonWorkerPayoutNewTokens = a.CocoonWorkerPayoutData.NewTokens
		raw.CocoonWorkerPayoutWorkerState = a.CocoonWorkerPayoutData.WorkerState
		raw.CocoonWorkerPayoutWorkerTokens = a.CocoonWorkerPayoutData.WorkerTokens
	}
	if a.CocoonProxyPayoutData != nil {
		raw.CocoonProxyPayoutQueryId = a.CocoonProxyPayoutData.QueryId
	}
	if a.CocoonProxyChargeData != nil {
		raw.CocoonProxyChargeQueryId = a.CocoonProxyChargeData.QueryId
		raw.CocoonProxyChargeNewTokensUsed = a.CocoonProxyChargeData.NewTokensUsed
		raw.CocoonProxyChargeExpectedAddress = a.CocoonProxyChargeData.ExpectedAddress
	}
	if a.CocoonClientTopUpData != nil {
		raw.CocoonClientTopUpQueryId = a.CocoonClientTopUpData.QueryId
	}
	if a.CocoonRegisterProxyData != nil {
		raw.CocoonRegisterProxyQueryId = a.CocoonRegisterProxyData.QueryId
	}
	if a.CocoonUnregisterProxyData != nil {
		raw.CocoonUnregisterProxyQueryId = a.CocoonUnregisterProxyData.QueryId
		raw.CocoonUnregisterProxySeqno = a.CocoonUnregisterProxyData.Seqno
	}
	if a.CocoonClientRegisterData != nil {
		raw.CocoonClientRegisterQueryId = a.CocoonClientRegisterData.QueryId
		raw.CocoonClientRegisterNonce = a.CocoonClientRegisterData.Nonce
	}
	if a.CocoonClientChangeSecretHashData != nil {
		raw.CocoonClientChangeSecretHashQueryId = a.CocoonClientChangeSecretHashData.QueryId
		raw.CocoonClientChangeSecretHashNewSecretHash = a.CocoonClientChangeSecretHashData.NewSecretHash
	}
	if a.CocoonClientRequestRefundData != nil {
		raw.CocoonClientRequestRefundQueryId = a.CocoonClientRequestRefundData.QueryId
		raw.CocoonClientRequestRefundViaWallet = a.CocoonClientRequestRefundData.ViaWallet
	}
	if a.CocoonGrantRefundData != nil {
		raw.CocoonGrantRefundQueryId = a.CocoonGrantRefundData.QueryId
		raw.CocoonGrantRefundNewTokensUsed = a.CocoonGrantRefundData.NewTokensUsed
		raw.CocoonGrantRefundExpectedAddress = a.CocoonGrantRefundData.ExpectedAddress
	}
	if a.CocoonClientIncreaseStakeData != nil {
		raw.CocoonClientIncreaseStakeQueryId = a.CocoonClientIncreaseStakeData.QueryId
		raw.CocoonClientIncreaseStakeNewStake = a.CocoonClientIncreaseStakeData.NewStake
	}
	if a.CocoonClientWithdrawData != nil {
		raw.CocoonClientWithdrawQueryId = a.CocoonClientWithdrawData.QueryId
		raw.CocoonClientWithdrawWithdrawAmount = a.CocoonClientWithdrawData.WithdrawAmount
	}
	return raw, nil
}

func (n *TraceNode) ToTransaction() (*models.Transaction, error) {
	origStatus, err := n.Transaction.OrigStatus.Str()
	if err != nil {
		return nil, err
	}
	endStatus, err := n.Transaction.EndStatus.Str()
	if err != nil {
		return nil, err
	}

	ordVal := "ord"
	isTock := false

	t := &models.Transaction{
		Account:                  models.AccountAddress(n.Transaction.Account),
		Hash:                     models.HashType(*n.Transaction.Hash.Base64Ptr()),
		Lt:                       int64(n.Transaction.Lt),
		Now:                      int32(n.Transaction.Now),
		Workchain:                n.BlockId.Workchain,
		Shard:                    models.ShardId(int64(n.BlockId.Shard)),
		Seqno:                    int32(n.BlockId.Seqno),
		McSeqno:                  int32(n.McBlockSeqno),
		TraceId:                  ptrHashType(n.TraceId),
		PrevTransHash:            models.HashType(*n.Transaction.PrevTransHash.Base64Ptr()),
		PrevTransLt:              int64(n.Transaction.PrevTransLt),
		OrigStatus:               origStatus,
		EndStatus:                endStatus,
		TotalFees:                int64(n.Transaction.TotalFees),
		TotalFeesExtraCurrencies: map[string]string{},
		AccountStateHashBefore:   models.HashType(*n.Transaction.AccountStateHashBefore.Base64Ptr()),
		AccountStateHashAfter:    models.HashType(*n.Transaction.AccountStateHashAfter.Base64Ptr()),
		Descr: models.TransactionDescr{
			Type:        ordVal,
			Aborted:     &n.Transaction.Description.Aborted,
			Destroyed:   &n.Transaction.Description.Destroyed,
			CreditFirst: &n.Transaction.Description.CreditFirst,
			IsTock:      &isTock,
			Installed:   &isTock,
		},
		OutMsgs:  []*models.Message{},
		Emulated: n.Emulated,
		Finality: n.FinalityState,
	}

	t.BlockRef = models.BlockId{
		Workchain: n.BlockId.Workchain,
		Shard:     models.ShardId(int64(n.BlockId.Shard)),
		Seqno:     int32(n.BlockId.Seqno),
	}
	t.AccountStateBefore = &models.AccountState{Hash: t.AccountStateHashBefore}
	t.AccountStateAfter = &models.AccountState{Hash: t.AccountStateHashAfter}

	var st models.StoragePhase
	if n.Transaction.Description.StoragePh != nil {
		statusChange, err := n.Transaction.Description.StoragePh.StatusChange.Str()
		if err != nil {
			return nil, err
		}
		collected := int64(n.Transaction.Description.StoragePh.StorageFeesCollected)
		st.StorageFeesCollected = &collected
		st.StorageFeesDue = ptrInt64FromUint64(n.Transaction.Description.StoragePh.StorageFeesDue)
		st.StatusChange = &statusChange
		t.Descr.StoragePh = &st
	}

	var cr models.CreditPhase
	if n.Transaction.Description.CreditPh != nil {
		cr.DueFeesCollected = ptrInt64FromUint64(n.Transaction.Description.CreditPh.DueFeesCollected)
		credit := int64(n.Transaction.Description.CreditPh.Credit)
		cr.Credit = &credit
		cr.CreditExtraCurrencies = map[string]string{}
		t.Descr.CreditPh = &cr
	}

	var co models.ComputePhase
	if n.Transaction.Description.ComputePh.Type == 0 {
		isSkipped := true
		co.IsSkipped = &isSkipped
		reason, err := n.Transaction.Description.ComputePh.Data.(trComputePhaseSkipped).Reason.Str()
		if err != nil {
			return nil, err
		}
		co.Reason = &reason
		t.Descr.ComputePh = &co
	} else {
		isSkipped := false
		co.IsSkipped = &isSkipped
		vm := n.Transaction.Description.ComputePh.Data.(trComputePhaseVm)
		co.Success = &vm.Success
		co.MsgStateUsed = &vm.MsgStateUsed
		co.AccountActivated = &vm.AccountActivated
		gasFees := int64(vm.GasFees)
		co.GasFees = &gasFees
		gasUsed := int64(vm.GasUsed)
		co.GasUsed = &gasUsed
		gasLimit := int64(vm.GasLimit)
		co.GasLimit = &gasLimit
		co.GasCredit = ptrInt64FromUint64(vm.GasCredit)
		mode := int32(vm.Mode)
		co.Mode = &mode
		exitCode := int32(vm.ExitCode)
		co.ExitCode = &exitCode
		co.ExitArg = vm.ExitArg
		vmSteps := uint32(vm.VmSteps)
		co.VmSteps = &vmSteps
		co.VmInitStateHash = ptrHashType(vm.VmInitStateHash.Base64Ptr())
		co.VmFinalStateHash = ptrHashType(vm.VmFinalStateHash.Base64Ptr())
		t.Descr.ComputePh = &co
	}

	if n.Transaction.Description.Action != nil {
		var ap models.ActionPhase
		statusChange, err := n.Transaction.Description.Action.StatusChange.Str()
		if err != nil {
			return nil, err
		}
		ap.Success = &n.Transaction.Description.Action.Success
		ap.Valid = &n.Transaction.Description.Action.Valid
		ap.NoFunds = &n.Transaction.Description.Action.NoFunds
		ap.StatusChange = &statusChange
		ap.TotalFwdFees = ptrInt64FromUint64(n.Transaction.Description.Action.TotalFwdFees)
		ap.TotalActionFees = ptrInt64FromUint64(n.Transaction.Description.Action.TotalActionFees)
		ap.ResultCode = n.Transaction.Description.Action.ResultCode
		ap.ResultArg = n.Transaction.Description.Action.ResultArg
		ap.TotActions = ptrInt32FromUint16(n.Transaction.Description.Action.TotActions)
		ap.SpecActions = ptrInt32FromUint16(n.Transaction.Description.Action.SpecActions)
		ap.SkippedActions = ptrInt32FromUint16(n.Transaction.Description.Action.SkippedActions)
		ap.MsgsCreated = ptrInt32FromUint16(n.Transaction.Description.Action.MsgsCreated)
		ap.ActionListHash = ptrHashType(n.Transaction.Description.Action.ActionListHash.Base64Ptr())
		cells := int64(n.Transaction.Description.Action.TotMsgSize.Cells)
		bits := int64(n.Transaction.Description.Action.TotMsgSize.Bits)
		ap.TotMsgSize = &models.MsgSize{Cells: &cells, Bits: &bits}
		t.Descr.Action = &ap
	}

	if n.Transaction.Description.Bounce != nil {
		var bp models.BouncePhase
		if n.Transaction.Description.Bounce.Type == 0 {
			btype := "negfunds"
			bp.Type = &btype
		} else if n.Transaction.Description.Bounce.Type == 1 {
			btype := "nofunds"
			bp.Type = &btype
			phase := n.Transaction.Description.Bounce.Data.(trBouncePhaseNofunds)
			cells := int64(phase.MsgSize.Cells)
			bits := int64(phase.MsgSize.Bits)
			bp.MsgSize = &models.MsgSize{Cells: &cells, Bits: &bits}
			reqFwdFees := int64(phase.ReqFwdFees)
			bp.ReqFwdFees = &reqFwdFees
		} else {
			btype := "ok"
			bp.Type = &btype
			phase := n.Transaction.Description.Bounce.Data.(trBouncePhaseOk)
			cells := int64(phase.MsgSize.Cells)
			bits := int64(phase.MsgSize.Bits)
			bp.MsgSize = &models.MsgSize{Cells: &cells, Bits: &bits}
			msgFees := int64(phase.MsgFees)
			bp.MsgFees = &msgFees
			fwdFees := int64(phase.FwdFees)
			bp.FwdFees = &fwdFees
		}
		t.Descr.Bounce = &bp
	}

	return t, nil
}

func (m *trMessage) toMessage(traceId string, direction string, txLt uint64, txHash string) (*models.Message, error) {
	bodyHash, err := calcHash(m.BodyBoc)
	if err != nil {
		return nil, err
	}

	var initStateHash *models.HashType
	var initStateBoc *string
	if m.InitStateBoc != nil {
		h, err := calcHash(*m.InitStateBoc)
		if err != nil {
			return nil, err
		}
		ht := models.HashType(h)
		initStateHash = &ht
		initStateBoc = m.InitStateBoc
	}

	var hashNorm *models.HashType
	if m.HashNorm != nil {
		n := base64.StdEncoding.EncodeToString((*m.HashNorm)[:])
		ht := models.HashType(n)
		hashNorm = &ht
	}

	bodyHashType := models.HashType(bodyHash)
	traceIdHash := models.HashType(traceId)
	msg := &models.Message{
		TxHash:               models.HashType(txHash),
		TxLt:                 int64(txLt),
		MsgHash:              models.HashType(base64.StdEncoding.EncodeToString(m.Hash[:])),
		Direction:            direction,
		TraceId:              &traceIdHash,
		Source:               ptrAccountAddress(m.Source),
		Destination:          ptrAccountAddress(m.Destination),
		Value:                ptrInt64FromUint64(m.Value),
		ValueExtraCurrencies: map[string]string{},
		FwdFee:               m.FwdFee,
		IhrFee:               m.IhrFee,
		ExtraFlags:           m.ExtraFlags,
		CreatedLt:            m.CreatedLt,
		CreatedAt:            m.CreatedAt,
		Opcode:               ptrOpcodeFromInt32(m.Opcode),
		IhrDisabled:          m.IhrDisabled,
		Bounce:               m.Bounce,
		Bounced:              m.Bounced,
		ImportFee:            m.ImportFee,
		BodyHash:             &bodyHashType,
		MessageContent:       &models.MessageContent{Hash: &bodyHashType, Body: &m.BodyBoc},
		InitStateHash:        initStateHash,
		MsgHashNorm:          hashNorm,
	}
	if initStateBoc != nil {
		msg.InitState = &models.MessageContent{Hash: initStateHash, Body: initStateBoc}
	}

	return msg, nil
}

func (n *TraceNode) ToMessages() ([]*models.Message, error) {
	msgs := make([]*models.Message, 0, len(n.Transaction.OutMsgs)+1)
	txHash := base64.StdEncoding.EncodeToString(n.Transaction.Hash[:])

	for _, outMsg := range n.Transaction.OutMsgs {
		msg, err := outMsg.toMessage(n.Key, "out", n.Transaction.Lt, txHash)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}

	if n.Transaction.InMsg != nil {
		inMsg, err := n.Transaction.InMsg.toMessage(n.Key, "in", n.Transaction.Lt, txHash)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, inMsg)
	}

	return msgs, nil
}
