package index

import (
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
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
	Workchain *int32          `query:"workchain"`
	Shard     *models.ShardId `query:"shard"`
	Seqno     *int32          `query:"seqno"`
	McSeqno   *int32          `query:"mc_seqno"`
}

type AddressBookRequest struct {
	Address []string `query:"address"`
}

type TransactionRequest struct {
	Account        []models.AccountAddress `query:"account"`
	ExcludeAccount []models.AccountAddress `query:"exclude_account"`
	Hash           *models.HashType        `query:"hash"`
	Lt             *uint64                 `query:"lt"`
}

type PendingTransactionRequest struct {
	Account []models.AccountAddress `query:"account"`
	TraceId []models.HashType       `query:"trace_id"`
}

type AdjacentTransactionRequest struct {
	Hash      models.HashType `query:"hash"`
	Direction *string         `query:"direction"`
}

type MessageRequest struct {
	Direction        *string                        `query:"direction"`
	ExcludeExternals *bool                          `query:"exclude_externals"`
	OnlyExternals    *bool                          `query:"only_externals"`
	MessageHash      []models.HashType              `query:"msg_hash"`
	Source           *models.AccountAddressNullable `query:"source"`
	Destination      *models.AccountAddressNullable `query:"destination"`
	BodyHash         *models.HashType               `query:"body_hash"`
	Opcode           *models.OpcodeType             `query:"opcode"`
}

type NFTCollectionRequest struct {
	CollectionAddress []models.AccountAddress `query:"collection_address"`
	OwnerAddress      []models.AccountAddress `query:"owner_address"`
}

type NFTItemRequest struct {
	Address           []models.AccountAddress `query:"address"`
	OwnerAddress      []models.AccountAddress `query:"owner_address"`
	CollectionAddress []models.AccountAddress `query:"collection_address"`
	Index             []string                `query:"index"`
}

type NFTTransferRequest struct {
	OwnerAddress      []models.AccountAddress `query:"owner_address"`
	ItemAddress       []models.AccountAddress `query:"item_address"`
	CollectionAddress *models.AccountAddress  `query:"collection_address"`
	Direction         *string                 `query:"direction"`
}

type JettonMasterRequest struct {
	MasterAddress []models.AccountAddress `query:"address"`
	AdminAddress  []models.AccountAddress `query:"admin_address"`
}

type JettonWalletRequest struct {
	Address            []models.AccountAddress `query:"address"`
	OwnerAddress       []models.AccountAddress `query:"owner_address"`
	JettonAddress      []models.AccountAddress `query:"jetton_address"`
	ExcludeZeroBalance *bool                   `query:"exclude_zero_balance"`
}

type JettonTransferRequest struct {
	OwnerAddress []models.AccountAddress `query:"owner_address"`
	JettonWallet []models.AccountAddress `query:"jetton_wallet"`
	JettonMaster *models.AccountAddress  `query:"jetton_master"`
	Direction    *string                 `query:"direction"`
}

type JettonBurnRequest struct {
	OwnerAddress []models.AccountAddress `query:"owner_address"`
	JettonWallet []models.AccountAddress `query:"jetton_wallet"`
	JettonMaster *models.AccountAddress  `query:"jetton_master"`
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
	AccountAddress []models.AccountAddress `query:"address"`
	CodeHash       []models.HashType       `query:"code_hash"`
	IncludeBOC     *bool                   `query:"include_boc"`
}

type ActionRequest struct {
	AccountAddress       *models.AccountAddress `query:"account"`
	TransactionHash      []models.HashType      `query:"tx_hash"`
	MessageHash          []models.HashType      `query:"msg_hash"`
	TraceId              []models.HashType      `query:"trace_id"`
	ActionId             []models.HashType      `query:"action_id"`
	McSeqno              *int32                 `query:"mc_seqno"`
	IncludeActionTypes   []string               `query:"action_type"`
	ExcludeActionTypes   []string               `query:"exclude_action_type"`
	SupportedActionTypes []string               `query:"supported_action_types"`
}

type BalanceChangesRequest struct {
	TraceId  *string `query:"trace_id"`
	ActionId *string `query:"action_id"`
}

type TracesRequest struct {
	IncludeActions       bool                   `query:"include_actions"`
	AccountAddress       *models.AccountAddress `query:"account"`
	TraceId              []models.HashType      `query:"trace_id"`
	TransactionHash      []models.HashType      `query:"tx_hash"`
	MessageHash          []models.HashType      `query:"msg_hash"`
	McSeqno              *int32                 `query:"mc_seqno"`
	SupportedActionTypes []string               `query:"supported_action_types"`
}

type PendingTracesRequest struct {
	AccountAddress       *models.AccountAddress `query:"account"`
	ExtMsgHash           []models.HashType      `query:"ext_msg_hash"`
	SupportedActionTypes []string               `query:"supported_action_types"`
}

type PendingActionsRequest struct {
	AccountAddress       *models.AccountAddress `query:"account"`
	ExtMsgHash           []models.HashType      `query:"ext_msg_hash"`
	SupportedActionTypes []string               `query:"supported_action_types"`
}

type DNSRecordsRequest struct {
	WalletAddress *models.AccountAddress `query:"wallet"`
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
	Hash  []models.HashType       `query:"my_hash"`
	Addr  []models.AccountAddress `query:"my_addr"`
	Shard []models.ShardId        `query:"my_shard"`
}

// api/v2 requests
type V2AccountRequest struct {
	AccountAddress models.AccountAddress `query:"address"`
	UseV2          *bool                 `query:"use_v2"`
}

type V2SendMessageRequest struct {
	BOC string `json:"boc"`
} // @name V2SendMessageRequest

type V2RunGetMethodRequest struct {
	Address models.AccountAddress  `json:"address"`
	Method  string                 `json:"method"`
	Stack   []models.V2StackEntity `json:"stack"`
} // @name V2RunGetMethodRequest

type V2EstimateFeeRequest struct {
	Address      models.AccountAddress `json:"address"`
	Body         string                `json:"body"`
	InitCode     string                `json:"init_code"`
	InitData     string                `json:"init_data"`
	IgnoreChksig bool                  `json:"ignore_chksig"`
} // @name V2EstimateFeeRequest
