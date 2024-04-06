package index

type ShardId int64
type AccountAddress string
type HexInt int64

type BlockId struct {
	Workchain int32   `json:"workchain"`
	Shard     ShardId `json:"shard,string"`
	Seqno     int32   `json:"seqno"`
}

type Block struct {
	Workchain              int32     `json:"workchain"`
	Shard                  ShardId   `json:"shard,string"`
	Seqno                  int32     `json:"seqno"`
	RootHash               string    `json:"root_hash"`
	FileHash               string    `json:"file_hash"`
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
	RandSeed               string    `json:"rand_seed"`
	CreatedBy              string    `json:"created_by"`
	TxCount                int64     `json:"tx_count"`
	MasterchainBlockRef    BlockId   `json:"masterchain_block_ref"`
	PrevBlocks             []BlockId `json:"prev_blocks"`
}

type DecodedContent struct {
	Type    string `json:"type"`
	Comment string `json:"comment"`
}

type MessageContent struct {
	Hash    string          `json:"hash"`
	Body    string          `json:"body"`
	Decoded *DecodedContent `json:"decoded"`
}

type Message struct {
	TxHash         string          `json:"-"`
	TxLt           string          `json:"-"`
	Hash           string          `json:"hash"`
	Direction      string          `json:"-"`
	Source         *AccountAddress `json:"source"`
	Destination    *AccountAddress `json:"destination"`
	Value          *int64          `json:"value"`
	FwdFee         *uint64         `json:"fwd_fee"`
	IhrFee         *uint64         `json:"ihr_fee"`
	CreatedLt      *uint64         `json:"created_lt"`
	CreatedAt      *uint32         `json:"created_at"`
	Opcode         *HexInt         `json:"opcode"`
	IhrDisabled    *bool           `json:"ihr_disabled"`
	Bounce         *bool           `json:"bounce"`
	Bounced        *bool           `json:"bounced"`
	ImportFee      *uint64         `json:"import_fee"`
	BodyHash       *string         `json:"-"`
	InitStateHash  *string         `json:"-"`
	MessageContent *MessageContent `json:"message_content"`
	InitState      *MessageContent `json:"init_state"`
}

type MsgSize struct {
	Cells *int64 `json:"cells"`
	Bits  *int64 `json:"bits"`
}

type StoragePhase struct {
	StorageFeesCollected *int64  `json:"storage_fees_collected,omitempty"`
	StorageFeesDue       *int64  `json:"storage_fees_due,omitempty"`
	StatusChange         *string `json:"status_change,omitempty"`
}

type CreditPhase struct {
	DueFeesCollected *int64 `json:"due_fees_collected,omitempty"`
	Credit           *int64 `json:"credit,omitempty"`
}

type ComputePhase struct {
	IsSkipped        *bool   `json:"skipped,omitempty"`
	Reason           *string `json:"reason,omitempty"`
	Success          *bool   `json:"success,omitempty"`
	MsgStateUsed     *bool   `json:"msg_state_used,omitempty"`
	AccountActivated *bool   `json:"account_activated,omitempty"`
	GasFees          *int64  `json:"gas_fees,omitempty"`
	GasUsed          *int64  `json:"gas_used,omitempty"`
	GasLimit         *int64  `json:"gas_limit,omitempty"`
	GasCredit        *int64  `json:"gas_credit,omitempty"`
	Mode             *int32  `json:"mode,omitempty"`
	ExitCode         *int32  `json:"exit_code,omitempty"`
	ExitArg          *int32  `json:"exit_arg,omitempty"`
	VmSteps          *uint32 `json:"vm_steps,omitempty"`
	VmInitStateHash  *string `json:"vm_init_state_hash,omitempty"`
	VmFinalStateHash *string `json:"vm_final_state_hash,omitempty"`
}

type ActionPhase struct {
	Success         *bool    `json:"success,omitempty"`
	Valid           *bool    `json:"valid,omitempty"`
	NoFunds         *bool    `json:"no_funds,omitempty"`
	StatusChange    *string  `json:"status_change,omitempty"`
	TotalFwdFees    *int64   `json:"total_fwd_fees,omitempty"`
	TotalActionFees *int64   `json:"total_action_fees,omitempty"`
	ResultCode      *int32   `json:"result_code,omitempty"`
	ResultArg       *int32   `json:"result_arg,omitempty"`
	TotActions      *int32   `json:"tot_actions,omitempty"`
	SpecActions     *int32   `json:"spec_actions,omitempty"`
	SkippedActions  *int32   `json:"skipped_actions,omitempty"`
	MsgsCreated     *int32   `json:"msgs_created,omitempty"`
	ActionListHash  *string  `json:"action_list_hash,omitempty"`
	TotMsgSize      *MsgSize `json:"tot_msg_size,omitempty"`
}

type BouncePhase struct {
	Type       *string  `json:"type"`
	MsgSize    *MsgSize `json:"msg_size,omitempty"`
	ReqFwdFees *int64   `json:"req_fwd_fees,omitempty"`
	MsgFees    *int64   `json:"msg_fees,omitempty"`
	FwdFees    *int64   `json:"fwd_fees,omitempty"`
}

type SplitInfo struct {
	CurShardPfxLen *int32          `json:"cur_shard_pfx_len,omitempty"`
	AccSplitDepth  *int32          `json:"acc_split_depth,omitempty"`
	ThisAddr       *AccountAddress `json:"this_addr,omitempty"`
	SiblingAddr    *AccountAddress `json:"sibling_addr,omitempty"`
}

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
}

type Transaction struct {
	Account                AccountAddress   `json:"account"`
	Hash                   string           `json:"hash"`
	Lt                     int64            `json:"lt"`
	Workchain              int32            `json:"block_workchain"`
	Shard                  ShardId          `json:"block_shard"`
	Seqno                  int32            `json:"block_seqno"`
	McSeqno                int32            `json:"mc_block_seqno"`
	TraceId                *string          `json:"trace_id,omitempty"`
	PrevTransHash          string           `json:"prev_trans_hash"`
	PrevTransLt            int64            `json:"prev_trans_lt"`
	Now                    int32            `json:"now"`
	OrigStatus             string           `json:"orig_status"`
	EndStatus              string           `json:"end_status"`
	TotalFees              int64            `json:"total_fees"`
	AccountStateHashBefore string           `json:"account_state_hash_before"`
	AccountStateHashAfter  string           `json:"account_state_hash_after"`
	Descr                  TransactionDescr `json:"description"`
	InMsg                  *Message         `json:"in_msg"`
	OutMsgs                []*Message       `json:"out_msgs"`
}
