package models

import "time"

type UtimeType uint64 // @name UtimeType

// settings
type RequestSettings struct {
	Timeout              time.Duration
	IsTestnet            bool
	V2Endpoint           string
	V2ApiKey             string
	DefaultLimit         int
	MaxLimit             int
	MaxTraceTransactions int
	DebugRequest         bool
	NoAddressBook        bool
	NoMetadata           bool
	UseCache             bool
}

type LimitParams struct {
	Limit  *int32    `query:"limit"`
	Offset *int32    `query:"offset"`
	Sort   *SortType `query:"sort"`
}

func (l LimitParams) GetLimitParams() LimitParams {
	return l
}

type ILimitParams interface {
	GetLimitParams() LimitParams
}

type UtimeParams struct {
	StartUtime *UtimeType `query:"start_utime"`
	EndUtime   *UtimeType `query:"end_utime"`
}

func (u UtimeParams) GetUtimeParams() UtimeParams {
	return u
}

type IUtimeParams interface {
	GetUtimeParams() UtimeParams
}

type LtParams struct {
	StartLt *uint64 `query:"start_lt"`
	EndLt   *uint64 `query:"end_lt"`
}

func (l LtParams) GetLtParams() LtParams {
	return l
}

type ULtParams interface {
	GetLtParams() LtParams
}

type TopAccountsByBalanceRequest struct {
	LimitParams
}

// requests
type SortType string

const (
	DESC SortType = "desc"
	ASC  SortType = "asc"
)

type BlocksRequest struct {
	Workchain *int32    `query:"workchain"`
	Shard     *ShardId  `query:"shard"`
	Seqno     *int32    `query:"seqno"`
	RootHash  *HashType `query:"root_hash"`
	FileHash  *HashType `query:"file_hash"`
	McSeqno   *int32    `query:"mc_seqno"`
	UtimeParams
	LtParams
	LimitParams
}

type ShardsRequest struct {
	Seqno int32 `query:"seqno"`
}

type ShardsDiffRequest struct {
	Seqno int32 `query:"seqno"`
	LimitParams
}

type AddressBookRequest struct {
	Address []string `query:"address"`
}

type TransactionsRequest struct {
	Workchain      *int32           `query:"workchain"` // blocks
	Shard          *ShardId         `query:"shard"`
	Seqno          *int32           `query:"seqno"`
	McSeqno        *int32           `query:"mc_seqno"`
	Account        []AccountAddress `query:"account"` // accounts
	ExcludeAccount []AccountAddress `query:"exclude_account"`
	Hash           []HashType       `query:"hash"`
	Lt             *uint64          `query:"lt"`
	Direction      *string          `query:"direction"` // messages
	MessageHash    []HashType       `query:"msg_hash"`
	Source         *AccountAddress  `query:"source"`
	Destination    *AccountAddress  `query:"destination"`
	BodyHash       *HashType        `query:"body_hash"`
	Opcode         *OpcodeType      `query:"opcode"`
	UtimeParams
	LtParams
	LimitParams
}

type TransactionsByMasterchainSeqnoRequest struct {
	Seqno int32 `query:"seqno"`
	LimitParams
}

type PendingTransactionRequest struct {
	Account []AccountAddress `query:"account"`
	TraceId []HashType       `query:"trace_id"`
}

type AdjacentTransactionRequest struct {
	Hash      HashType `query:"hash"`
	Direction *string  `query:"direction"`
}

type MessageRequest struct {
	Direction         *string         `query:"direction"`
	ExcludeExternals  *bool           `query:"exclude_externals"`
	OnlyExternals     *bool           `query:"only_externals"`
	LegacyMessageHash []HashType      `query:"hash"`
	MessageHash       []HashType      `query:"msg_hash"`
	Source            *AccountAddress `query:"source"`
	Destination       *AccountAddress `query:"destination"`
	BodyHash          *HashType       `query:"body_hash"`
	Opcode            *OpcodeType     `query:"opcode"`
	UtimeParams
	LtParams
	LimitParams
}

type NFTCollectionRequest struct {
	CollectionAddress []AccountAddress `query:"collection_address"`
	OwnerAddress      []AccountAddress `query:"owner_address"`
	LimitParams
}

type NFTItemRequest struct {
	Address                 []AccountAddress `query:"address"`
	OwnerAddress            []AccountAddress `query:"owner_address"`
	CollectionAddress       []AccountAddress `query:"collection_address"`
	Index                   []string         `query:"index"`
	IncludeOnSale           *bool            `query:"include_on_sale"`
	SortByLastTransactionLt *bool            `query:"sort_by_last_transaction_lt"`
	LimitParams
}

type NFTTransferRequest struct {
	OwnerAddress      []AccountAddress `query:"owner_address"`
	ItemAddress       []AccountAddress `query:"item_address"`
	CollectionAddress *AccountAddress  `query:"collection_address"`
	Direction         *string          `query:"direction"`
	UtimeParams
	LtParams
	LimitParams
}

type NFTSalesRequest struct {
	Address []AccountAddress `query:"address"`
	LimitParams
}

type JettonMasterRequest struct {
	MasterAddress []AccountAddress `query:"address"`
	AdminAddress  []AccountAddress `query:"admin_address"`
	LimitParams
}

type JettonWalletRequest struct {
	Address            []AccountAddress `query:"address"`
	OwnerAddress       []AccountAddress `query:"owner_address"`
	JettonAddress      []AccountAddress `query:"jetton_address"`
	ExcludeZeroBalance *bool            `query:"exclude_zero_balance"`
	LimitParams
}

type JettonTransferRequest struct {
	OwnerAddress []AccountAddress `query:"owner_address"`
	JettonWallet []AccountAddress `query:"jetton_wallet"`
	JettonMaster *AccountAddress  `query:"jetton_master"`
	Direction    *string          `query:"direction"`
	UtimeParams
	LtParams
	LimitParams
}

type JettonBurnRequest struct {
	OwnerAddress []AccountAddress `query:"owner_address"`
	JettonWallet []AccountAddress `query:"jetton_wallet"`
	JettonMaster *AccountAddress  `query:"jetton_master"`
	UtimeParams
	LtParams
	LimitParams
}
type AccountRequest struct {
	AccountAddress []AccountAddress `query:"address"`
	CodeHash       []HashType       `query:"code_hash"`
	IncludeBOC     *bool            `query:"include_boc"`
}

type ActionRequest struct {
	AccountAddress       *AccountAddress `query:"account"`
	TransactionHash      []HashType      `query:"tx_hash"`
	MessageHash          []HashType      `query:"msg_hash"`
	TraceId              []HashType      `query:"trace_id"`
	ActionId             []HashType      `query:"action_id"`
	McSeqno              *int32          `query:"mc_seqno"`
	IncludeActionTypes   []string        `query:"action_type"`
	ExcludeActionTypes   []string        `query:"exclude_action_type"`
	SupportedActionTypes []string        `query:"supported_action_types"`
	IncludeAccounts      *bool           `query:"include_accounts"`
	IncludeTransactions  *bool           `query:"include_transactions"`
	UtimeParams
	LtParams
	LimitParams
}

type BalanceChangesRequest struct {
	TraceId  *HashType `query:"trace_id"`
	ActionId *HashType `query:"action_id"`
}

type TracesRequest struct {
	IncludeActions       bool            `query:"include_actions"`
	AccountAddress       *AccountAddress `query:"account"`
	TraceId              []HashType      `query:"trace_id"`
	TransactionHash      []HashType      `query:"tx_hash"`
	MessageHash          []HashType      `query:"msg_hash"`
	McSeqno              *int32          `query:"mc_seqno"`
	SupportedActionTypes []string        `query:"supported_action_types"`
	UtimeParams
	LtParams
	LimitParams
}

type PendingTracesRequest struct {
	AccountAddress       *AccountAddress `query:"account"`
	ExtMsgHash           []HashType      `query:"ext_msg_hash"`
	SupportedActionTypes []string        `query:"supported_action_types"`
}

type PendingActionsRequest struct {
	AccountAddress       *AccountAddress `query:"account"`
	ExtMsgHash           []HashType      `query:"ext_msg_hash"`
	SupportedActionTypes []string        `query:"supported_action_types"`
	IncludeTransactions  *bool           `query:"include_transactions"`
}

type DNSRecordsRequest struct {
	WalletAddress *AccountAddress `query:"wallet"`
	Domain        *string         `query:"domain"`
	LimitParams
}

type MultisigRequest struct {
	Address       []AccountAddress `query:"address"`
	WalletAddress []AccountAddress `query:"wallet_address"`
	IncludeOrders *bool            `query:"include_orders"`
	LimitParams
}

type MultisigOrderRequest struct {
	Address         []AccountAddress `query:"address"`
	MultisigAddress []AccountAddress `query:"multisig_address"`
	ParseActions    *bool            `query:"parse_actions"`
	LimitParams
}

type VestingContractsRequest struct {
	ContractAddress []AccountAddress `query:"contract_address"`
	WalletAddress   []AccountAddress `query:"wallet_address"`
	CheckWhitelist  *bool            `query:"check_whitelist"`
	LimitParams
}

type TestRequest struct {
	Hash  []HashType       `query:"my_hash"`
	Addr  []AccountAddress `query:"my_addr"`
	Shard []ShardId        `query:"my_shard"`
}

// api/v2 requests
type V2AccountRequest struct {
	AccountAddress AccountAddress `query:"address"`
	UseV2          *bool          `query:"use_v2"`
}

type V2SendMessageRequest struct {
	BOC string `json:"boc"`
} // @name V2SendMessageRequest

type V2RunGetMethodRequest struct {
	Address AccountAddress  `json:"address"`
	Method  string          `json:"method"`
	Stack   []V2StackEntity `json:"stack"`
} // @name V2RunGetMethodRequest

type V2EstimateFeeRequest struct {
	Address      AccountAddress `json:"address"`
	Body         string         `json:"body"`
	InitCode     string         `json:"init_code"`
	InitData     string         `json:"init_data"`
	IgnoreChksig bool           `json:"ignore_chksig"`
} // @name V2EstimateFeeRequest

type DecodeRequest struct {
	Opcodes []string `query:"opcodes" json:"opcodes"`
	Bodies  []string `query:"bodies" json:"bodies"`
} // @name DecodeRequest
