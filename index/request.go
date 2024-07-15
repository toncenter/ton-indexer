package index

import (
	"time"
)

// settings
type RequestSettings struct {
	Timeout   time.Duration
	IsTestnet bool
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

type MessageRequest struct {
	Direction   *string         `query:"direction"`
	MessageHash []HashType      `query:"msg_hash"`
	Source      *AccountAddress `query:"source"`
	Destination *AccountAddress `query:"destination"`
	BodyHash    *HashType       `query:"body_hash"`
	Opcode      *OpcodeType     `query:"opcode"`
}

type NFTCollectionRequest struct {
	CollectionAddress []AccountAddress `query:"collection_address"`
	OwnerAddress      []AccountAddress `query:"owner_address"`
}

type NFTItemRequest struct {
	Address           []AccountAddress `query:"address"`
	OwnerAddress      []AccountAddress `query:"owner_address"`
	CollectionAddress *AccountAddress  `query:"collection_address"`
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
	Address       []AccountAddress `query:"address"`
	OwnerAddress  []AccountAddress `query:"owner_address"`
	JettonAddress *AccountAddress  `query:"jetton_address"`
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
	StartUtime *uint32 `query:"start_utime"`
	EndUtime   *uint32 `query:"end_utime"`
}

type LtRequest struct {
	StartLt *uint64 `query:"start_lt"`
	EndLt   *uint64 `query:"end_lt"`
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
