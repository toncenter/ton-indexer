package index

import (
	"time"
)

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
}

// requests
type BlockRequest struct {
	Workchain *int32   `query:"workchain"`
	Shard     *ShardId `query:"shard"`
	Seqno     *int32   `query:"seqno"`
	McSeqno   *int32   `query:"mc_seqno"`
}

type AddressBookRequest struct {
	Address []string `query:"address"`
}

type TransactionRequest struct {
	Account        []AccountAddress `query:"account"`
	ExcludeAccount []AccountAddress `query:"exclude_account"`
	Hash           *HashType        `query:"hash"`
	Lt             *uint64          `query:"lt"`
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
	Direction        *string                 `query:"direction"`
	ExcludeExternals *bool                   `query:"exclude_externals"`
	OnlyExternals    *bool                   `query:"only_externals"`
	MessageHash      []HashType              `query:"msg_hash"`
	Source           *AccountAddressNullable `query:"source"`
	Destination      *AccountAddressNullable `query:"destination"`
	BodyHash         *HashType               `query:"body_hash"`
	Opcode           *OpcodeType             `query:"opcode"`
}

type NFTCollectionRequest struct {
	CollectionAddress []AccountAddress `query:"collection_address"`
	OwnerAddress      []AccountAddress `query:"owner_address"`
}

type NFTItemRequest struct {
	Address           []AccountAddress `query:"address"`
	OwnerAddress      []AccountAddress `query:"owner_address"`
	CollectionAddress []AccountAddress `query:"collection_address"`
	Index             []string         `query:"index"`
}

type NFTTransferRequest struct {
	OwnerAddress      []AccountAddress `query:"owner_address"`
	ItemAddress       []AccountAddress `query:"item_address"`
	CollectionAddress *AccountAddress  `query:"collection_address"`
	Direction         *string          `query:"direction"`
}

type JettonMasterRequest struct {
	MasterAddress []AccountAddress `query:"address"`
	AdminAddress  []AccountAddress `query:"admin_address"`
}

type JettonWalletRequest struct {
	Address            []AccountAddress `query:"address"`
	OwnerAddress       []AccountAddress `query:"owner_address"`
	JettonAddress      []AccountAddress `query:"jetton_address"`
	ExcludeZeroBalance *bool            `query:"exclude_zero_balance"`
}

type JettonTransferRequest struct {
	OwnerAddress []AccountAddress `query:"owner_address"`
	JettonWallet []AccountAddress `query:"jetton_wallet"`
	JettonMaster *AccountAddress  `query:"jetton_master"`
	Direction    *string          `query:"direction"`
}

type JettonBurnRequest struct {
	OwnerAddress []AccountAddress `query:"owner_address"`
	JettonWallet []AccountAddress `query:"jetton_wallet"`
	JettonMaster *AccountAddress  `query:"jetton_master"`
}

type UtimeRequest struct {
	StartUtime *UtimeType `query:"start_utime"`
	EndUtime   *UtimeType `query:"end_utime"`
}

type LtRequest struct {
	StartLt *uint64 `query:"start_lt"`
	EndLt   *uint64 `query:"end_lt"`
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
}

type BalanceChangesRequest struct {
	TraceId  *string `query:"trace_id"`
	ActionId *string `query:"action_id"`
}

type TracesRequest struct {
	IncludeActions       bool            `query:"include_actions"`
	AccountAddress       *AccountAddress `query:"account"`
	TraceId              []HashType      `query:"trace_id"`
	TransactionHash      []HashType      `query:"tx_hash"`
	MessageHash          []HashType      `query:"msg_hash"`
	McSeqno              *int32          `query:"mc_seqno"`
	SupportedActionTypes []string        `query:"supported_action_types"`
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
}

type DNSRecordsRequest struct {
	WalletAddress *AccountAddress `query:"wallet"`
}

type SortType string

const (
	DESC SortType = "desc"
	ASC  SortType = "asc"
)

type LimitRequest struct {
	Limit  *int32    `query:"limit"`
	Offset *int32    `query:"offset"`
	Sort   *SortType `query:"sort"`
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
