package models

import (
	"encoding/json"

	"github.com/toncenter/ton-indexer/ton-index-go/index/emulated"
)

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
	ContractMethods        *[]uint32         `json:"contract_methods"`
	Interfaces             *[]string         `json:"interfaces"`
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

type MessageContent struct {
	Hash    *HashType        `json:"hash"`
	Body    *string          `json:"body"`
	Decoded *json.RawMessage `json:"decoded" swaggertype:"object"`
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
	ExtraFlags           *string           `json:"extra_flags"`
	CreatedLt            *uint64           `json:"created_lt,string"`
	CreatedAt            *uint32           `json:"created_at,string"`
	Opcode               *OpcodeType       `json:"opcode"`
	DecodedOpcode        *string           `json:"decoded_opcode"`
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
	Account                  AccountAddress         `json:"account"`
	Hash                     HashType               `json:"hash"`
	Lt                       int64                  `json:"lt,string"`
	Now                      int32                  `json:"now"`
	Workchain                int32                  `json:"-"`
	Shard                    ShardId                `json:"-"`
	Seqno                    int32                  `json:"-"`
	McSeqno                  int32                  `json:"mc_block_seqno"`
	TraceId                  *HashType              `json:"trace_id,omitempty"`
	TraceExternalHash        *HashType              `json:"trace_external_hash,omitempty"`
	PrevTransHash            HashType               `json:"prev_trans_hash"`
	PrevTransLt              int64                  `json:"prev_trans_lt,string"`
	OrigStatus               string                 `json:"orig_status"`
	EndStatus                string                 `json:"end_status"`
	TotalFees                int64                  `json:"total_fees,string"`
	TotalFeesExtraCurrencies map[string]string      `json:"total_fees_extra_currencies"`
	AccountStateHashBefore   HashType               `json:"-"`
	AccountStateHashAfter    HashType               `json:"-"`
	Descr                    TransactionDescr       `json:"description"`
	BlockRef                 BlockId                `json:"block_ref"`
	InMsg                    *Message               `json:"in_msg"`
	OutMsgs                  []*Message             `json:"out_msgs"`
	AccountStateBefore       *AccountState          `json:"account_state_before"`
	AccountStateAfter        *AccountState          `json:"account_state_after"`
	Emulated                 bool                   `json:"emulated"`
	Finality                 emulated.FinalityState `json:"finality"`
} // @name Transaction
