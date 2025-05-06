package index

import (
	"fmt"
)

type ShardId int64                 // @name ShardId
type AccountAddress string         // @name AccountAddress
type AccountAddressNullable string // @name AccountAddressNullable
type HashType string               // @name HashType
type HexInt int64                  // @name HexInt
type OpcodeType int64              // @name OpcodeType

var WalletsHashMap = map[string]bool{
	"oM/CxIruFqJx8s/AtzgtgXVs7LEBfQd/qqs7tgL2how=": true,
	"1JAvzJ+tdGmPqONTIgpo2g3PcuMryy657gQhfBfTBiw=": true,
	"WHzHie/xyE9G7DeX5F/ICaFP9a4k8eDHpqmcydyQYf8=": true,
	"XJpeaMEI4YchoHxC+ZVr+zmtd+xtYktgxXbsiO7mUyk=": true,
	"/pUw0yQ4Uwg+8u8LTCkIwKv2+hwx6iQ6rKpb+MfXU/E=": true,
	"thBBpYp5gLlG6PueGY48kE0keZ/6NldOpCUcQaVm9YE=": true,
	"hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=": true,
	"ZN1UgFUixb6KnbWc6gEFzPDQh4bKeb64y3nogKjXMi0=": true,
	"/rX/aCDi/w2Ug+fg1iyBfYRniftK5YDIeIZtlZ2r1cA=": true,
	"89fKU0k97trCizgZhqhJQDy6w9LFhHea8IEGWvCsS5M=": true,
	"IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8=": true,
}

// errors
type IndexError struct {
	Code    int    `json:"-"`
	Message string `json:"error"`
}

func (e IndexError) Error() string {
	return e.Message
}

// models
type AddressBookRow struct {
	UserFriendly *string `json:"user_friendly"`
	Domain       *string `json:"domain"`
} // @name AddressBookRow

type AddressBook map[string]AddressBookRow // @name AddressBook
type Metadata map[string]AddressMetadata   // @name Metadata

type BackgroundTask struct {
	Type  string
	Retry int
	Data  map[string]interface{}
}

type AddressMetadata struct {
	IsIndexed bool        `json:"is_indexed"`
	TokenInfo []TokenInfo `json:"token_info"`
} // @name AddressMetadata

type TokenInfo struct {
	Address     string                 `json:"-"`
	Valid       *bool                  `json:"-"`
	Indexed     bool                   `json:"-"`
	Type        *string                `json:"type,omitempty"`
	Name        *string                `json:"name,omitempty"`
	Symbol      *string                `json:"symbol,omitempty"`
	Description *string                `json:"description,omitempty"`
	Image       *string                `json:"image,omitempty"`
	Extra       map[string]interface{} `json:"extra,omitempty"`
} // @name TokenInfo

type BlockId struct {
	Workchain int32   `json:"workchain"`
	Shard     ShardId `json:"shard,string"`
	Seqno     int32   `json:"seqno"`
} // @name BlockId

type AccountState struct {
	Hash                   HashType          `json:"hash"`
	Account                *AccountAddress   `json:"-"`
	Balance                *string           `json:"balance"`
	BalanceExtraCurrencies map[string]string `json:"extra_currencies"`
	AccountStatus          *string           `json:"account_status"`
	FrozenHash             *HashType         `json:"frozen_hash"`
	DataHash               *HashType         `json:"data_hash"`
	CodeHash               *HashType         `json:"code_hash"`
	DataBoc                *string           `json:"data_boc,omitempty"`
	CodeBoc                *string           `json:"code_boc,omitempty"`
} // @name AccountState

type AccountBalance struct {
	Account AccountAddress `json:"account"`
	Balance string         `json:"balance"`
} // @name AccountBalance

type AccountStateFull struct {
	AccountAddress         *AccountAddress   `json:"address"`
	Hash                   HashType          `json:"account_state_hash"`
	Balance                *string           `json:"balance"`
	BalanceExtraCurrencies map[string]string `json:"extra_currencies"`
	AccountStatus          *string           `json:"status"`
	FrozenHash             *HashType         `json:"frozen_hash,omitempty"`
	LastTransactionHash    *HashType         `json:"last_transaction_hash"`
	LastTransactionLt      *int64            `json:"last_transaction_lt,string"`
	DataHash               *HashType         `json:"data_hash,omitempty"`
	CodeHash               *HashType         `json:"code_hash,omitempty"`
	DataBoc                *string           `json:"data_boc,omitempty"`
	CodeBoc                *string           `json:"code_boc,omitempty"`
} // @name AccountStateFull

type WalletState struct {
	AccountAddress         AccountAddress    `json:"address"`
	IsWallet               bool              `json:"is_wallet"`
	WalletType             *string           `json:"wallet_type,omitempty"`
	Seqno                  *int64            `json:"seqno,omitempty"`
	WalletId               *int64            `json:"wallet_id,omitempty"`
	Balance                *string           `json:"balance,omitempty"`
	BalanceExtraCurrencies map[string]string `json:"extra_currencies,omitempty"`
	IsSignatureAllowed     *bool             `json:"is_signature_allowed,omitempty"`
	AccountStatus          *string           `json:"status,omitempty"`
	CodeHash               *HashType         `json:"code_hash,omitempty"`
	LastTransactionHash    *HashType         `json:"last_transaction_hash"`
	LastTransactionLt      *int64            `json:"last_transaction_lt,string"`
} // @name WalletState

type Block struct {
	Workchain              int32     `json:"workchain"`
	Shard                  ShardId   `json:"shard,string"`
	Seqno                  int32     `json:"seqno"`
	RootHash               HashType  `json:"root_hash"`
	FileHash               HashType  `json:"file_hash"`
	GlobalId               int32     `json:"global_id"`
	Version                int64     `json:"version"`
	AfterMerge             bool      `json:"after_merge"`
	BeforeSplit            bool      `json:"before_split"`
	AfterSplit             bool      `json:"after_split"`
	WantMerge              bool      `json:"want_merge"`
	WantSplit              bool      `json:"want_split"`
	KeyBlock               bool      `json:"key_block"`
	VertSeqnoIncr          bool      `json:"vert_seqno_incr"`
	Flags                  int32     `json:"flags"`
	GenUtime               int64     `json:"gen_utime,string"`
	StartLt                int64     `json:"start_lt,string"`
	EndLt                  int64     `json:"end_lt,string"`
	ValidatorListHashShort int32     `json:"validator_list_hash_short"`
	GenCatchainSeqno       int32     `json:"gen_catchain_seqno"`
	MinRefMcSeqno          int32     `json:"min_ref_mc_seqno"`
	PrevKeyBlockSeqno      int32     `json:"prev_key_block_seqno"`
	VertSeqno              int32     `json:"vert_seqno"`
	MasterRefSeqno         int32     `json:"master_ref_seqno"`
	RandSeed               HashType  `json:"rand_seed"`
	CreatedBy              HashType  `json:"created_by"`
	TxCount                int64     `json:"tx_count"`
	MasterchainBlockRef    BlockId   `json:"masterchain_block_ref"`
	PrevBlocks             []BlockId `json:"prev_blocks"`
} // @name Block

type DecodedContent struct {
	Type    string `json:"type"`
	Comment string `json:"comment"`
} // @name DecodedContent

type MessageContent struct {
	Hash    *HashType       `json:"hash"`
	Body    *string         `json:"body"`
	Decoded *DecodedContent `json:"decoded"`
} // @name MessageContent

type Message struct {
	TxHash               HashType          `json:"-"`
	TxLt                 int64             `json:"-"`
	MsgHash              HashType          `json:"hash"`
	Direction            string            `json:"-"`
	TraceId              *HashType         `json:"-"`
	Source               *AccountAddress   `json:"source"`
	Destination          *AccountAddress   `json:"destination"`
	Value                *int64            `json:"value,string"`
	ValueExtraCurrencies map[string]string `json:"value_extra_currencies"`
	FwdFee               *uint64           `json:"fwd_fee,string"`
	IhrFee               *uint64           `json:"ihr_fee,string"`
	CreatedLt            *uint64           `json:"created_lt,string"`
	CreatedAt            *uint32           `json:"created_at,string"`
	Opcode               *OpcodeType       `json:"opcode"`
	IhrDisabled          *bool             `json:"ihr_disabled"`
	Bounce               *bool             `json:"bounce"`
	Bounced              *bool             `json:"bounced"`
	ImportFee            *uint64           `json:"import_fee,string"`
	BodyHash             *HashType         `json:"-"`
	InitStateHash        *HashType         `json:"-"`
	InMsgTxHash          *HashType         `json:"in_msg_tx_hash,omitempty"`
	OutMsgTxHash         *HashType         `json:"out_msg_tx_hash,omitempty"`
	MessageContent       *MessageContent   `json:"message_content"`
	InitState            *MessageContent   `json:"init_state"`
	MsgHashNorm          *HashType         `json:"hash_norm,omitempty"`
} // @name Message

type MsgSize struct {
	Cells *int64 `json:"cells,string"`
	Bits  *int64 `json:"bits,string"`
} // @name MsgSize

type StoragePhase struct {
	StorageFeesCollected *int64  `json:"storage_fees_collected,string,omitempty"`
	StorageFeesDue       *int64  `json:"storage_fees_due,string,omitempty"`
	StatusChange         *string `json:"status_change,omitempty"`
} // @name StoragePhase

type CreditPhase struct {
	DueFeesCollected      *int64            `json:"due_fees_collected,string,omitempty"`
	Credit                *int64            `json:"credit,string,omitempty"`
	CreditExtraCurrencies map[string]string `json:"credit_extra_currencies,omitempty"`
} // @name CreditPhase

type ComputePhase struct {
	IsSkipped        *bool     `json:"skipped,omitempty"`
	Reason           *string   `json:"reason,omitempty"`
	Success          *bool     `json:"success,omitempty"`
	MsgStateUsed     *bool     `json:"msg_state_used,omitempty"`
	AccountActivated *bool     `json:"account_activated,omitempty"`
	GasFees          *int64    `json:"gas_fees,string,omitempty"`
	GasUsed          *int64    `json:"gas_used,string,omitempty"`
	GasLimit         *int64    `json:"gas_limit,string,omitempty"`
	GasCredit        *int64    `json:"gas_credit,string,omitempty"`
	Mode             *int32    `json:"mode,omitempty"`
	ExitCode         *int32    `json:"exit_code,omitempty"`
	ExitArg          *int32    `json:"exit_arg,omitempty"`
	VmSteps          *uint32   `json:"vm_steps,omitempty"`
	VmInitStateHash  *HashType `json:"vm_init_state_hash,omitempty"`
	VmFinalStateHash *HashType `json:"vm_final_state_hash,omitempty"`
} // @name ComputePhase

type ActionPhase struct {
	Success         *bool     `json:"success,omitempty"`
	Valid           *bool     `json:"valid,omitempty"`
	NoFunds         *bool     `json:"no_funds,omitempty"`
	StatusChange    *string   `json:"status_change,omitempty"`
	TotalFwdFees    *int64    `json:"total_fwd_fees,string,omitempty"`
	TotalActionFees *int64    `json:"total_action_fees,string,omitempty"`
	ResultCode      *int32    `json:"result_code,omitempty"`
	ResultArg       *int32    `json:"result_arg,omitempty"`
	TotActions      *int32    `json:"tot_actions,omitempty"`
	SpecActions     *int32    `json:"spec_actions,omitempty"`
	SkippedActions  *int32    `json:"skipped_actions,omitempty"`
	MsgsCreated     *int32    `json:"msgs_created,omitempty"`
	ActionListHash  *HashType `json:"action_list_hash,omitempty"`
	TotMsgSize      *MsgSize  `json:"tot_msg_size,omitempty"`
} // @name ActionPhase

type BouncePhase struct {
	Type       *string  `json:"type"`
	MsgSize    *MsgSize `json:"msg_size,omitempty"`
	ReqFwdFees *int64   `json:"req_fwd_fees,string,omitempty"`
	MsgFees    *int64   `json:"msg_fees,string,omitempty"`
	FwdFees    *int64   `json:"fwd_fees,string,omitempty"`
} // @name BouncePhase

type SplitInfo struct {
	CurShardPfxLen *int32          `json:"cur_shard_pfx_len,omitempty"`
	AccSplitDepth  *int32          `json:"acc_split_depth,omitempty"`
	ThisAddr       *AccountAddress `json:"this_addr,omitempty"`
	SiblingAddr    *AccountAddress `json:"sibling_addr,omitempty"`
} // @name SplitInfo

type TransactionDescr struct {
	Type        string        `json:"type"`
	Aborted     *bool         `json:"aborted,omitempty"`
	Destroyed   *bool         `json:"destroyed,omitempty"`
	CreditFirst *bool         `json:"credit_first,omitempty"`
	IsTock      *bool         `json:"is_tock,omitempty"`
	Installed   *bool         `json:"installed,omitempty"`
	StoragePh   *StoragePhase `json:"storage_ph,omitempty"`
	CreditPh    *CreditPhase  `json:"credit_ph,omitempty"`
	ComputePh   *ComputePhase `json:"compute_ph,omitempty"`
	Action      *ActionPhase  `json:"action,omitempty"`
	Bounce      *BouncePhase  `json:"bounce,omitempty"`
	SplitInfo   *SplitInfo    `json:"split_info,omitempty"`
} // @name TransactionDescr

type Transaction struct {
	Account                  AccountAddress    `json:"account"`
	Hash                     HashType          `json:"hash"`
	Lt                       int64             `json:"lt,string"`
	Now                      int32             `json:"now"`
	Workchain                int32             `json:"-"`
	Shard                    ShardId           `json:"-"`
	Seqno                    int32             `json:"-"`
	McSeqno                  int32             `json:"mc_block_seqno"`
	TraceId                  *HashType         `json:"trace_id,omitempty"`
	TraceExternalHash        *HashType         `json:"trace_external_hash,omitempty"`
	PrevTransHash            HashType          `json:"prev_trans_hash"`
	PrevTransLt              int64             `json:"prev_trans_lt,string"`
	OrigStatus               string            `json:"orig_status"`
	EndStatus                string            `json:"end_status"`
	TotalFees                int64             `json:"total_fees,string"`
	TotalFeesExtraCurrencies map[string]string `json:"total_fees_extra_currencies"`
	AccountStateHashBefore   HashType          `json:"-"`
	AccountStateHashAfter    HashType          `json:"-"`
	Descr                    TransactionDescr  `json:"description"`
	BlockRef                 BlockId           `json:"block_ref"`
	InMsg                    *Message          `json:"in_msg"`
	OutMsgs                  []*Message        `json:"out_msgs"`
	AccountStateBefore       *AccountState     `json:"account_state_before"`
	AccountStateAfter        *AccountState     `json:"account_state_after"`
	Emulated                 bool              `json:"emulated"`
} // @name Transaction

// nfts
type JsonType map[string]interface{}

type NFTCollection struct {
	Address           AccountAddress         `json:"address"`
	OwnerAddress      *AccountAddress        `json:"owner_address"`
	LastTransactionLt int64                  `json:"last_transaction_lt,string"`
	NextItemIndex     string                 `json:"next_item_index"`
	CollectionContent map[string]interface{} `json:"collection_content"`
	DataHash          HashType               `json:"data_hash"`
	CodeHash          HashType               `json:"code_hash"`
	CodeBoc           string                 `json:"-"`
	DataBoc           string                 `json:"-"`
} // @name NFTCollection

type NFTCollectionNullable struct {
	Address           *AccountAddress
	OwnerAddress      *AccountAddress
	LastTransactionLt *int64
	NextItemIndex     *string
	CollectionContent map[string]interface{}
	DataHash          *HashType
	CodeHash          *HashType
	CodeBoc           *string
	DataBoc           *string
}

type NFTItem struct {
	Address           AccountAddress         `json:"address"`
	Init              bool                   `json:"init"`
	Index             string                 `json:"index"`
	CollectionAddress *AccountAddress        `json:"collection_address"`
	OwnerAddress      *AccountAddress        `json:"owner_address"`
	Content           map[string]interface{} `json:"content"`
	LastTransactionLt int64                  `json:"last_transaction_lt,string"`
	CodeHash          HashType               `json:"code_hash"`
	DataHash          HashType               `json:"data_hash"`
	Collection        *NFTCollection         `json:"collection"`
} // @name NFTItem

type NFTTransfer struct {
	QueryId              string          `json:"query_id"`
	NftItemAddress       AccountAddress  `json:"nft_address"`
	NftItemIndex         string          `json:"-"`
	NftCollectionAddress AccountAddress  `json:"nft_collection"`
	TransactionHash      HashType        `json:"transaction_hash"`
	TransactionLt        int64           `json:"transaction_lt,string"`
	TransactionNow       int64           `json:"transaction_now"`
	TransactionAborted   bool            `json:"transaction_aborted"`
	OldOwner             AccountAddress  `json:"old_owner"`
	NewOwner             AccountAddress  `json:"new_owner"`
	ResponseDestination  *AccountAddress `json:"response_destination"`
	CustomPayload        *string         `json:"custom_payload"`
	ForwardAmount        *string         `json:"forward_amount"`
	ForwardPayload       *string         `json:"forward_payload"`
	TraceId              *HashType       `json:"trace_id"`
} // @name NFTTransfer

// jettons
type JettonMaster struct {
	Address              AccountAddress         `json:"address"`
	TotalSupply          string                 `json:"total_supply"`
	Mintable             bool                   `json:"mintable"`
	AdminAddress         *AccountAddress        `json:"admin_address"`
	JettonContent        map[string]interface{} `json:"jetton_content"`
	JettonWalletCodeHash HashType               `json:"jetton_wallet_code_hash"`
	CodeHash             HashType               `json:"code_hash"`
	DataHash             HashType               `json:"data_hash"`
	LastTransactionLt    int64                  `json:"last_transaction_lt,string"`
	CodeBoc              string                 `json:"-"`
	DataBoc              string                 `json:"-"`
} // @name JettonMaster

type JettonWalletMintlessInfo struct {
	IsClaimed           *bool    `json:"-"`
	Amount              *string  `json:"amount"`
	StartFrom           *int64   `json:"start_from"`
	ExpireAt            *int64   `json:"expire_at"`
	CustomPayloadApiUri []string `json:"custom_payload_api_uri"`
} // @name JettonWalletMintlessInfo

type JettonWallet struct {
	Address           AccountAddress            `json:"address"`
	Balance           string                    `json:"balance"`
	Owner             AccountAddress            `json:"owner"`
	Jetton            AccountAddress            `json:"jetton"`
	LastTransactionLt int64                     `json:"last_transaction_lt,string"`
	CodeHash          *HashType                 `json:"code_hash"`
	DataHash          *HashType                 `json:"data_hash"`
	MintlessInfo      *JettonWalletMintlessInfo `json:"mintless_info,omitempty"`
} // @name JettonWallet

type JettonTransfer struct {
	QueryId             string          `json:"query_id"`
	Source              AccountAddress  `json:"source"`
	Destination         AccountAddress  `json:"destination"`
	Amount              string          `json:"amount"`
	SourceWallet        AccountAddress  `json:"source_wallet"`
	JettonMaster        AccountAddress  `json:"jetton_master"`
	TransactionHash     HashType        `json:"transaction_hash"`
	TransactionLt       int64           `json:"transaction_lt,string"`
	TransactionNow      int64           `json:"transaction_now"`
	TransactionAborted  bool            `json:"transaction_aborted"`
	ResponseDestination *AccountAddress `json:"response_destination"`
	CustomPayload       *string         `json:"custom_payload"`
	ForwardTonAmount    *string         `json:"forward_ton_amount"`
	ForwardPayload      *string         `json:"forward_payload"`
	TraceId             *HashType       `json:"trace_id"`
} // @name JettonTransfer

type JettonBurn struct {
	QueryId             string          `json:"query_id"`
	Owner               AccountAddress  `json:"owner"`
	JettonWallet        AccountAddress  `json:"jetton_wallet"`
	JettonMaster        AccountAddress  `json:"jetton_master"`
	TransactionHash     HashType        `json:"transaction_hash"`
	TransactionLt       int64           `json:"transaction_lt,string"`
	TransactionNow      int64           `json:"transaction_now"`
	TransactionAborted  bool            `json:"transaction_aborted"`
	Amount              string          `json:"amount"`
	ResponseDestination *AccountAddress `json:"response_destination"`
	CustomPayload       *string         `json:"custom_payload"`
	TraceId             *HashType       `json:"trace_id"`
} // @name JettonBurn

type RawActionJettonSwapPeerSwap struct {
	AssetIn   *AccountAddress
	AmountIn  *string
	AssetOut  *AccountAddress
	AmountOut *string
}

func (p *RawActionJettonSwapPeerSwap) ScanNull() error {
	return fmt.Errorf("cannot scan NULL into RawActionJettonSwapPeerSwap")
}

func (p *RawActionJettonSwapPeerSwap) ScanIndex(i int) any {
	switch i {
	case 0:
		return &p.AssetIn
	case 1:
		return &p.AmountIn
	case 2:
		return &p.AssetOut
	case 3:
		return &p.AmountOut
	default:
		panic("invalid index")
	}
}

// traces
type RawAction struct {
	TraceId                                              *HashType
	ActionId                                             HashType
	StartLt                                              int64
	EndLt                                                int64
	StartUtime                                           int64
	EndUtime                                             int64
	TraceEndLt                                           int64
	TraceEndUtime                                        int64
	TraceMcSeqnoEnd                                      int32
	Source                                               *AccountAddress
	SourceSecondary                                      *AccountAddress
	Destination                                          *AccountAddress
	DestinationSecondary                                 *AccountAddress
	Asset                                                *AccountAddress
	AssetSecondary                                       *AccountAddress
	Asset2                                               *AccountAddress
	Asset2Secondary                                      *AccountAddress
	Opcode                                               *OpcodeType
	TxHashes                                             []HashType
	Type                                                 string
	TonTransferContent                                   *string
	TonTransferEncrypted                                 *bool
	Value                                                *string
	Amount                                               *string
	JettonTransferResponseDestination                    *AccountAddress
	JettonTransferForwardAmount                          *string
	JettonTransferQueryId                                *string
	JettonTransferCustomPayload                          *string
	JettonTransferForwardPayload                         *string
	JettonTransferComment                                *string
	JettonTransferIsEncryptedComment                     *bool
	NFTTransferIsPurchase                                *bool
	NFTTransferPrice                                     *string
	NFTTransferQueryId                                   *string
	NFTTransferCustomPayload                             *string
	NFTTransferForwardPayload                            *string
	NFTTransferForwardAmount                             *string
	NFTTransferResponseDestination                       *AccountAddress
	NFTTransferNFTItemIndex                              *string
	JettonSwapDex                                        *string
	JettonSwapSender                                     *AccountAddress
	JettonSwapDexIncomingTransferAmount                  *string
	JettonSwapDexIncomingTransferAsset                   *AccountAddress
	JettonSwapDexIncomingTransferSource                  *AccountAddress
	JettonSwapDexIncomingTransferDestination             *AccountAddress
	JettonSwapDexIncomingTransferSourceJettonWallet      *AccountAddress
	JettonSwapDexIncomingTransferDestinationJettonWallet *AccountAddress
	JettonSwapDexOutgoingTransferAmount                  *string
	JettonSwapDexOutgoingTransferAsset                   *AccountAddress
	JettonSwapDexOutgoingTransferSource                  *AccountAddress
	JettonSwapDexOutgoingTransferDestination             *AccountAddress
	JettonSwapDexOutgoingTransferSourceJettonWallet      *AccountAddress
	JettonSwapDexOutgoingTransferDestinationJettonWallet *AccountAddress
	JettonSwapPeerSwaps                                  []RawActionJettonSwapPeerSwap
	ChangeDNSRecordKey                                   *string
	ChangeDNSRecordValueSchema                           *string
	ChangeDNSRecordValue                                 *string
	ChangeDNSRecordFlags                                 *int64
	NFTMintNFTItemIndex                                  *string
	DexWithdrawLiquidityDataDex                          *string
	DexWithdrawLiquidityDataAmount1                      *string
	DexWithdrawLiquidityDataAmount2                      *string
	DexWithdrawLiquidityDataAsset1Out                    *AccountAddress
	DexWithdrawLiquidityDataAsset2Out                    *AccountAddress
	DexWithdrawLiquidityDataUserJettonWallet1            *AccountAddress
	DexWithdrawLiquidityDataUserJettonWallet2            *AccountAddress
	DexWithdrawLiquidityDataDexJettonWallet1             *AccountAddress
	DexWithdrawLiquidityDataDexJettonWallet2             *AccountAddress
	DexWithdrawLiquidityDataLpTokensBurnt                *string
	DexDepositLiquidityDataDex                           *string
	DexDepositLiquidityDataAmount1                       *string
	DexDepositLiquidityDataAmount2                       *string
	DexDepositLiquidityDataAsset1                        *AccountAddress
	DexDepositLiquidityDataAsset2                        *AccountAddress
	DexDepositLiquidityDataUserJettonWallet1             *AccountAddress
	DexDepositLiquidityDataUserJettonWallet2             *AccountAddress
	DexDepositLiquidityDataLpTokensMinted                *string
	StakingDataProvider                                  *string
	StakingDataTsNft                                     *AccountAddress
	StakingDataTokensBurnt                               *string
	StakingDataTokensMinted                              *string
	Success                                              *bool
	TraceExternalHash                                    *HashType
	ExtraCurrencies                                      map[string]string
	MultisigCreateOrderQueryId                           *string
	MultisigCreateOrderOrderSeqno                        *string
	MultisigCreateOrderIsCreatedBySigner                 *bool
	MultisigCreateOrderIsSignedByCreator                 *bool
	MultisigCreateOrderCreatorIndex                      *int64
	MultisigCreateOrderExpirationDate                    *int64
	MultisigCreateOrderOrderBoc                          *string
	MultisigApproveSignerIndex                           *int64
	MultisigApproveExitCode                              *int32
	MultisigExecuteQueryId                               *string
	MultisigExecuteOrderSeqno                            *string
	MultisigExecuteExpirationDate                        *int64
	MultisigExecuteApprovalsNum                          *int64
	MultisigExecuteSignersHash                           *string
	MultisigExecuteOrderBoc                              *string
	VestingSendMessageQueryId                            *string
	VestingSendMessageMessageBoc                         *string
	VestingAddWhitelistQueryId                           *string
	VestingAddWhitelistAccountsAdded                     []AccountAddress
	EvaaSupplySenderJettonWallet                         *AccountAddress
	EvaaSupplyRecipientJettonWallet                      *AccountAddress
	EvaaSupplyMasterJettonWallet                         *AccountAddress
	EvaaSupplyMaster                                     *AccountAddress
	EvaaSupplyAssetId                                    *string
	EvaaSupplyIsTon                                      *bool
	EvaaWithdrawRecipientJettonWallet                    *AccountAddress
	EvaaWithdrawMasterJettonWallet                       *AccountAddress
	EvaaWithdrawMaster                                   *AccountAddress
	EvaaWithdrawFailReason                               *string
	EvaaWithdrawAssetId                                  *string
	EvaaLiquidateFailReason                              *string
	EvaaLiquidateDebtAmount                              *string
	EvaaLiquidateAssetId                                 *string
	JvaultClaimClaimedJettons                            []AccountAddress
	JvaultClaimClaimedAmounts                            []string
	JvaultStakePeriod                                    *int64
	JvaultStakeMintedStakeJettons                        *string
	JvaultStakeStakeWallet                               *AccountAddress
	JvaultExitCode                                       *int64
} // @name RawAction

type ActionDetailsCallContract struct {
	OpCode          *OpcodeType        `json:"opcode,omitempty"`
	Source          *AccountAddress    `json:"source,omitempty"`
	Destination     *AccountAddress    `json:"destination,omitempty"`
	Value           *string            `json:"value,omitempty"`
	ExtraCurrencies *map[string]string `json:"extra_currencies,omitempty"`
}

type ActionDetailsContractDeploy struct {
	OpCode      *OpcodeType     `json:"opcode,omitempty"`
	Source      *AccountAddress `json:"source,omitempty"`
	Destination *AccountAddress `json:"destination,omitempty"`
	Value       *string         `json:"value,omitempty"`
}

type ActionDetailsTonTransfer struct {
	Source          *AccountAddress    `json:"source"`
	Destination     *AccountAddress    `json:"destination"`
	Value           *string            `json:"value"`
	ExtraCurrencies *map[string]string `json:"value_extra_currencies,omitempty"`
	Comment         *string            `json:"comment"`
	Encrypted       *bool              `json:"encrypted"`
}

type ActionDetailsAuctionBid struct {
	Amount        *string         `json:"amount"`
	Bidder        *AccountAddress `json:"bidder"`
	Auction       *AccountAddress `json:"auction"`
	NftItem       *AccountAddress `json:"nft_item"`
	NftCollection *AccountAddress `json:"nft_collection"`
	NftItemIndex  *string         `json:"nft_item_index"`
}

type ActionDetailsChangeDnsValue struct {
	SumType                *string `json:"sum_type"`
	DnsSmcAddress          *string `json:"dns_smc_address"`
	DnsAdnlAddress         *string `json:"dns_adnl_address"`
	DnsText                *string `json:"dns_text"`
	DnsNextResolverAddress *string `json:"dns_next_resolver_address"`
	DnsStorageAddress      *string `json:"dns_storage_address"`
	Flags                  *int64  `json:"flags"`
}

type ActionDetailsChangeDns struct {
	Key           *string                     `json:"key"`
	Value         ActionDetailsChangeDnsValue `json:"value"`
	Source        *AccountAddress             `json:"source"`
	Asset         *AccountAddress             `json:"asset"`
	NFTCollection *AccountAddress             `json:"nft_collection"`
}

type ActionDetailsDeleteDns struct {
	Key           *string         `json:"hash"`
	Source        *AccountAddress `json:"source"`
	Asset         *AccountAddress `json:"asset"`
	NFTCollection *AccountAddress `json:"nft_collection"`
}

type ActionDetailsRenewDns struct {
	Source        *AccountAddress `json:"source"`
	Asset         *AccountAddress `json:"asset"`
	NFTCollection *AccountAddress `json:"nft_collection"`
}

type ActionDetailsElectionDeposit struct {
	StakeHolder *AccountAddress `json:"stake_holder"`
	Amount      *string         `json:"amount,omitempty"`
}

type ActionDetailsElectionRecover struct {
	StakeHolder *AccountAddress `json:"stake_holder"`
	Amount      *string         `json:"amount,omitempty"`
}

type ActionDetailsJettonBurn struct {
	Owner             *AccountAddress `json:"owner"`
	OwnerJettonWallet *AccountAddress `json:"owner_jetton_wallet"`
	Asset             *AccountAddress `json:"asset"`
	Amount            *string         `json:"amount"`
}

type ActionDetailsJettonSwapTransfer struct {
	Asset                   *AccountAddress `json:"asset"`
	Source                  *AccountAddress `json:"source"`
	Destination             *AccountAddress `json:"destination"`
	SourceJettonWallet      *AccountAddress `json:"source_jetton_wallet"`
	DestinationJettonWallet *AccountAddress `json:"destination_jetton_wallet"`
	Amount                  *string         `json:"amount"`
}

type ActionDetailsJettonSwapPeerSwap struct {
	AssetIn   *AccountAddress `json:"asset_in"`
	AmountIn  *string         `json:"amount_in"`
	AssetOut  *AccountAddress `json:"asset_out"`
	AmountOut *string         `json:"amount_out"`
}

type ActionDetailsJettonSwap struct {
	Dex                 *string                           `json:"dex"`
	Sender              *AccountAddress                   `json:"sender"`
	AssetIn             *AccountAddress                   `json:"asset_in"`
	AssetOut            *AccountAddress                   `json:"asset_out"`
	DexIncomingTransfer *ActionDetailsJettonSwapTransfer  `json:"dex_incoming_transfer"`
	DexOutgoingTransfer *ActionDetailsJettonSwapTransfer  `json:"dex_outgoing_transfer"`
	PeerSwaps           []ActionDetailsJettonSwapPeerSwap `json:"peer_swaps"`
}

type ActionDetailsJettonTransfer struct {
	Asset                *AccountAddress `json:"asset"`
	Sender               *AccountAddress `json:"sender"`
	Receiver             *AccountAddress `json:"receiver"`
	SenderJettonWallet   *AccountAddress `json:"sender_jetton_wallet"`
	ReceiverJettonWallet *AccountAddress `json:"receiver_jetton_wallet"`
	Amount               *string         `json:"amount"`
	Comment              *string         `json:"comment"`
	IsEncryptedComment   *bool           `json:"is_encrypted_comment"`
	QueryId              *string         `json:"query_id"`
	ResponseDestination  *AccountAddress `json:"response_destination"`
	CustomPayload        *string         `json:"custom_payload"`
	ForwardPayload       *string         `json:"forward_payload"`
	ForwardAmount        *string         `json:"forward_amount"`
}

type ActionDetailsJettonMint struct {
	Asset                *AccountAddress `json:"asset"`
	Receiver             *AccountAddress `json:"receiver"`
	ReceiverJettonWallet *AccountAddress `json:"receiver_jetton_wallet"`
	Amount               *string         `json:"amount"`
	TonAmount            *string         `json:"ton_amount"`
}

type ActionDetailsNftMint struct {
	Owner         *AccountAddress `json:"owner,omitempty"`
	NftItem       *AccountAddress `json:"nft_item"`
	NftCollection *AccountAddress `json:"nft_collection"`
	NftItemIndex  *string         `json:"nft_item_index"`
}

type ActionDetailsNftTransfer struct {
	NftCollection       *AccountAddress `json:"nft_collection"`
	NftItem             *AccountAddress `json:"nft_item"`
	NftItemIndex        *string         `json:"nft_item_index"`
	OldOwner            *AccountAddress `json:"old_owner,omitempty"`
	NewOwner            *AccountAddress `json:"new_owner"`
	IsPurchase          *bool           `json:"is_purchase"`
	Price               *string         `json:"price,omitempty"`
	QueryId             *string         `json:"query_id"`
	ResponseDestination *AccountAddress `json:"response_destination"`
	CustomPayload       *string         `json:"custom_payload"`
	ForwardPayload      *string         `json:"forward_payload"`
	ForwardAmount       *string         `json:"forward_amount"`
	Comment             *string         `json:"comment"`
	IsEncryptedComment  *bool           `json:"is_encrypted_comment"`
}

type ActionDetailsTickTock struct {
	Account *AccountAddress `json:"account,omitempty"`
}

type ActionDetailsSubscribe struct {
	Subscriber   *AccountAddress `json:"subscriber"`
	Beneficiary  *AccountAddress `json:"beneficiary,omitempty"`
	Subscription *AccountAddress `json:"subscription"`
	Amount       *string         `json:"amount"`
}

type ActionDetailsUnsubscribe struct {
	Subscriber   *AccountAddress `json:"subscriber"`
	Beneficiary  *AccountAddress `json:"beneficiary,omitempty"`
	Subscription *AccountAddress `json:"subscription"`
	Amount       *string         `json:"amount,omitempty"`
}

type ActionDetailsWtonMint struct {
	Amount   *string         `json:"amount"`
	Receiver *AccountAddress `json:"receiver"`
}

type ActionDetailsDexDepositLiquidity struct {
	Dex                  *string         `json:"dex"`
	Amount1              *string         `json:"amount_1"`
	Amount2              *string         `json:"amount_2"`
	Asset1               *AccountAddress `json:"asset_1"`
	Asset2               *AccountAddress `json:"asset_2"`
	UserJettonWallet1    *AccountAddress `json:"user_jetton_wallet_1"`
	UserJettonWallet2    *AccountAddress `json:"user_jetton_wallet_2"`
	Source               *AccountAddress `json:"source"`
	Pool                 *AccountAddress `json:"pool"`
	DestinationLiquidity *AccountAddress `json:"destination_liquidity"`
	LpTokensMinted       *string         `json:"lp_tokens_minted"`
}

type ActionDetailsDexWithdrawLiquidity struct {
	Dex                  *string         `json:"dex"`
	Amount1              *string         `json:"amount_1"`
	Amount2              *string         `json:"amount_2"`
	Asset1               *AccountAddress `json:"asset_1"`
	Asset2               *AccountAddress `json:"asset_2"`
	UserJettonWallet1    *AccountAddress `json:"user_jetton_wallet_1"`
	UserJettonWallet2    *AccountAddress `json:"user_jetton_wallet_2"`
	LpTokensBurnt        *string         `json:"lp_tokens_burnt"`
	IsRefund             *bool           `json:"is_refund"`
	Source               *AccountAddress `json:"source"`
	Pool                 *AccountAddress `json:"pool"`
	DestinationLiquidity *AccountAddress `json:"destination_liquidity"`
}

type ActionDetailsStakeDeposit struct {
	Provider     *string         `json:"provider"`
	StakeHolder  *AccountAddress `json:"stake_holder"`
	Pool         *AccountAddress `json:"pool"`
	Amount       *string         `json:"amount"`
	TokensMinted *string         `json:"tokens_minted"`
	Asset        *AccountAddress `json:"asset"`
}

type ActionDetailsWithdrawStake struct {
	Provider    *string         `json:"provider"`
	StakeHolder *AccountAddress `json:"stake_holder"`
	Pool        *AccountAddress `json:"pool"`
	Amount      *string         `json:"amount"`
	PayoutNft   *AccountAddress `json:"payout_nft"`
	TokensBurnt *string         `json:"tokens_burnt"`
	Asset       *AccountAddress `json:"asset"`
}

type ActionDetailsWithdrawStakeRequest struct {
	Provider    *string         `json:"provider"`
	StakeHolder *AccountAddress `json:"stake_holder"`
	Pool        *AccountAddress `json:"pool"`
	PayoutNft   *AccountAddress `json:"payout_nft"`
	Asset       *AccountAddress `json:"asset"`
	TokensBurnt *string         `json:"tokens_burnt"`
}

type Action struct {
	TraceId           *HashType   `json:"trace_id"`
	ActionId          HashType    `json:"action_id"`
	StartLt           int64       `json:"start_lt,string"`
	EndLt             int64       `json:"end_lt,string"`
	StartUtime        int64       `json:"start_utime"`
	EndUtime          int64       `json:"end_utime"`
	TraceEndLt        int64       `json:"trace_end_lt,string"`
	TraceEndUtime     int64       `json:"trace_end_utime"`
	TraceMcSeqnoEnd   int32       `json:"trace_mc_seqno_end"`
	TxHashes          []HashType  `json:"transactions"`
	Success           *bool       `json:"success"`
	Type              string      `json:"type"`
	Details           interface{} `json:"details"`
	RawAction         *RawAction  `json:"raw_action,omitempty" swaggerignore:"true"`
	TraceExternalHash *HashType   `json:"trace_external_hash,omitempty"`
} // @name Action

type TraceMeta struct {
	TraceState          string `json:"trace_state"`
	Messages            int64  `json:"messages"`
	Transactions        int64  `json:"transactions"`
	PendingMessages     int64  `json:"pending_messages"`
	ClassificationState string `json:"classification_state"`
} // @name TraceMeta

type TraceNode struct {
	TransactionHash HashType     `json:"tx_hash,omitempty"`
	InMsgHash       HashType     `json:"in_msg_hash,omitempty"`
	Transaction     *Transaction `json:"transaction,omitempty"`
	InMsg           *Message     `json:"in_msg,omitempty"`
	Children        []*TraceNode `json:"children"`
} // @name TraceNode

type Trace struct {
	TraceId           *HashType                 `json:"trace_id"`
	ExternalHash      *HashType                 `json:"external_hash"`
	McSeqnoStart      HashType                  `json:"mc_seqno_start"`
	McSeqnoEnd        HashType                  `json:"mc_seqno_end"`
	StartLt           uint64                    `json:"start_lt,string"`
	StartUtime        uint32                    `json:"start_utime"`
	EndLt             *uint64                   `json:"end_lt,string"`
	EndUtime          *uint32                   `json:"end_utime"`
	TraceMeta         TraceMeta                 `json:"trace_info"`
	IsIncomplete      bool                      `json:"is_incomplete"`
	Warning           string                    `json:"warning,omitempty"`
	Actions           *[]*Action                `json:"actions,omitempty"`
	Trace             *TraceNode                `json:"trace,omitempty"`
	TransactionsOrder []HashType                `json:"transactions_order,omitempty"`
	Transactions      map[HashType]*Transaction `json:"transactions,omitempty"`
} // @name Trace

type DNSRecord struct {
	NftItemAddress AccountAddress  `json:"nft_item_address"`
	NftItemOwner   *AccountAddress `json:"nft_item_owner"`
	Domain         string          `json:"domain"`
	NextResolver   *AccountAddress `json:"dns_next_resolver"`
	Wallet         *AccountAddress `json:"dns_wallet"`
	SiteAdnl       *string         `json:"dns_site_adnl"`
	StorageBagID   *string         `json:"dns_storage_bag_id"`
} // @name DNSRecord

// proxied models
type V2AddressInformation struct {
	Balance             string  `json:"balance"`
	Code                *string `json:"code"`
	Data                *string `json:"data"`
	LastTransactionLt   *string `json:"last_transaction_lt"`
	LastTransactionHash *string `json:"last_transaction_hash"`
	FrozenHash          *string `json:"frozen_hash"`
	Status              string  `json:"status"`
} // @name V2AddressInformation

type V2WalletInformation struct {
	Balance             string  `json:"balance"`
	WalletType          *string `json:"wallet_type,omitempty"`
	Seqno               *int64  `json:"seqno,omitempty"`
	WalletId            *int64  `json:"wallet_id,omitempty"`
	LastTransactionLt   string  `json:"last_transaction_lt"`
	LastTransactionHash string  `json:"last_transaction_hash"`
	Status              string  `json:"status"`
} // @name V2WalletInformation

type V2SendMessageResult struct {
	MessageHash     *HashType `json:"message_hash,omitempty"`
	MessageHashNorm *HashType `json:"message_hash_norm,omitempty"`
} //@name V2SendMessageResult

type V2StackEntity struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
} // @name V2StackEntity

type V2RunGetMethodResult struct {
	GasUsed  int64       `json:"gas_used"`
	ExitCode int64       `json:"exit_code"`
	Stack    interface{} `json:"stack"`
} // @name V2RunGetMethodResult

type V2EstimatedFee struct {
	InFwdFee   uint64 `json:"in_fwd_fee"`
	StorageFee uint64 `json:"storage_fee"`
	GasFee     uint64 `json:"gas_fee"`
	FwdFee     uint64 `json:"fwd_fee"`
} // @name V2EstimatedFee

type V2EstimateFeeResult struct {
	SourceFees      V2EstimatedFee   `json:"source_fees"`
	DestinationFees []V2EstimatedFee `json:"destination_fees"`
} // @name V2EstimateFeeResult

type BalanceChangesResult struct {
	Ton     map[AccountAddress]int64                     `json:"changes"`
	Fees    map[AccountAddress]int64                     `json:"fees"`
	Jettons map[AccountAddress]map[AccountAddress]string `json:"jettons"`
}

// converters
func AddressInformationFromV3(state AccountStateFull) (*V2AddressInformation, error) {
	var info V2AddressInformation
	if state.Balance == nil {
		return nil, IndexError{Code: 500, Message: "balance is none"}
	}
	info.Balance = *state.Balance
	info.Code = state.CodeBoc
	info.Data = state.DataBoc
	info.LastTransactionHash = (*string)(state.LastTransactionHash)
	if state.LastTransactionLt != nil {
		info.LastTransactionLt = new(string)
		*info.LastTransactionLt = fmt.Sprintf("%d", *state.LastTransactionLt)
	}
	if state.AccountStatus == nil {
		return nil, IndexError{Code: 500, Message: "status is none"}
	}
	info.Status = *state.AccountStatus
	return &info, nil
}

func WalletInformationFromV3(state WalletState) (*V2WalletInformation, error) {
	var info V2WalletInformation
	if !state.IsWallet && !(state.AccountStatus != nil && *state.AccountStatus == "uninit") {
		return nil, nil
	}

	if state.Balance == nil {
		return nil, IndexError{Code: 500, Message: "balance is none"}
	}
	info.Balance = *state.Balance

	info.WalletType = state.WalletType
	info.Seqno = state.Seqno
	info.WalletId = state.WalletId

	if state.LastTransactionHash == nil {
		return nil, IndexError{Code: 500, Message: "last_transaction_hash is none"}
	}
	info.LastTransactionHash = string(*state.LastTransactionHash)
	if state.LastTransactionLt == nil {
		return nil, IndexError{Code: 500, Message: "last_transaction_lt is none"}
	}
	info.LastTransactionLt = fmt.Sprintf("%d", *state.LastTransactionLt)

	if state.AccountStatus == nil {
		return nil, IndexError{Code: 500, Message: "status is none"}
	}
	info.Status = *state.AccountStatus
	return &info, nil
}

// Multisig action details structs
type ActionDetailsMultisigCreateOrder struct {
	QueryId           *string         `json:"query_id"`
	OrderSeqno        *string         `json:"order_seqno"`
	IsCreatedBySigner *bool           `json:"is_created_by_signer"`
	IsSignedByCreator *bool           `json:"is_signed_by_creator"`
	CreatorIndex      *int64          `json:"creator_index"`
	ExpirationDate    *int64          `json:"expiration_date"`
	OrderBoc          *string         `json:"order_boc"`
	Source            *AccountAddress `json:"source"`
	Destination       *AccountAddress `json:"destination"`
	DestinationOrder  *AccountAddress `json:"destination_order"`
}

type ActionDetailsMultisigApprove struct {
	SignerIndex *int64          `json:"signer_index"`
	ExitCode    *int32          `json:"exit_code"`
	Source      *AccountAddress `json:"source"`
	Destination *AccountAddress `json:"destination"`
}

type ActionDetailsMultisigExecute struct {
	QueryId        *string         `json:"query_id"`
	OrderSeqno     *string         `json:"order_seqno"`
	ExpirationDate *int64          `json:"expiration_date"`
	ApprovalsNum   *int64          `json:"approvals_num"`
	SignersHash    *string         `json:"signers_hash"`
	OrderBoc       *string         `json:"order_boc"`
	Source         *AccountAddress `json:"source"`
	Destination    *AccountAddress `json:"destination"`
}

// Vesting action details structs
type ActionDetailsVestingSendMessage struct {
	QueryId     *string         `json:"query_id"`
	MessageBoc  *string         `json:"message_boc"`
	Source      *AccountAddress `json:"source"`
	Vesting     *AccountAddress `json:"vesting"`
	Destination *AccountAddress `json:"destination"`
	Amount      *string         `json:"amount"`
}

type ActionDetailsVestingAddWhitelist struct {
	QueryId       *string          `json:"query_id"`
	AccountsAdded []AccountAddress `json:"accounts_added"`
	Source        *AccountAddress  `json:"source"`
	Vesting       *AccountAddress  `json:"vesting"`
}

// EVAA action details structs
type ActionDetailsEvaaSupply struct {
	SenderJettonWallet    *AccountAddress `json:"sender_jetton_wallet"`
	RecipientJettonWallet *AccountAddress `json:"recipient_jetton_wallet"`
	MasterJettonWallet    *AccountAddress `json:"master_jetton_wallet"`
	Master                *AccountAddress `json:"master"`
	AssetId               *string         `json:"asset_id"`
	IsTon                 *bool           `json:"is_ton"`
	Source                *AccountAddress `json:"source"`
	SourceWallet          *AccountAddress `json:"source_wallet"`
	Recipient             *AccountAddress `json:"recipient"`
	RecipientContract     *AccountAddress `json:"recipient_contract"`
	Asset                 *AccountAddress `json:"asset"`
	Amount                *string         `json:"amount"`
}

type ActionDetailsEvaaWithdraw struct {
	RecipientJettonWallet *AccountAddress `json:"recipient_jetton_wallet"`
	MasterJettonWallet    *AccountAddress `json:"master_jetton_wallet"`
	Master                *AccountAddress `json:"master"`
	FailReason            *string         `json:"fail_reason"`
	AssetId               *string         `json:"asset_id"`
	Source                *AccountAddress `json:"source"`
	Recipient             *AccountAddress `json:"recipient"`
	OwnerContract         *AccountAddress `json:"owner_contract"`
	Asset                 *AccountAddress `json:"asset"`
	Amount                *string         `json:"amount"`
}

type ActionDetailsEvaaLiquidate struct {
	FailReason       *string         `json:"fail_reason"`
	DebtAmount       *string         `json:"debt_amount"`
	Source           *AccountAddress `json:"source"`
	Borrower         *AccountAddress `json:"borrower"`
	BorrowerContract *AccountAddress `json:"borrower_contract"`
	Collateral       *AccountAddress `json:"collateral"`
	AssetId          *string         `json:"asset_id"`
	Amount           *string         `json:"amount"`
}

// JVault action details structs
type JettonAmountPair struct {
	Jetton *AccountAddress `json:"jetton"`
	Amount *string         `json:"amount"`
}

type ActionDetailsJvaultClaim struct {
	ClaimedRewards []JettonAmountPair `json:"claimed_rewards"`
	Source         *AccountAddress    `json:"source"`
	StakeWallet    *AccountAddress    `json:"stake_wallet"`
	Pool           *AccountAddress    `json:"pool"`
}

type ActionDetailsJvaultStake struct {
	Period             *int64          `json:"period"`
	MintedStakeJettons *string         `json:"minted_stake_jettons"`
	StakeWallet        *AccountAddress `json:"stake_wallet"`
	Source             *AccountAddress `json:"source"`
	SourceJettonWallet *AccountAddress `json:"source_jetton_wallet"`
	Asset              *AccountAddress `json:"asset"`
	Pool               *AccountAddress `json:"pool"`
	Amount             *string         `json:"amount"`
}

type ActionDetailsJvaultUnstake struct {
	Source      *AccountAddress `json:"source"`
	StakeWallet *AccountAddress `json:"stake_wallet"`
	Pool        *AccountAddress `json:"pool"`
	Amount      *string         `json:"amount"`
	ExitCode    *int64          `json:"exit_code"`
}

type ActionDetailsNftDiscovery struct {
	Source        *AccountAddress `json:"source"`
	NftItem       *AccountAddress `json:"nft_item"`
	NftCollection *AccountAddress `json:"nft_collection"`
	NftItemIndex  *string         `json:"nft_item_index"`
}
