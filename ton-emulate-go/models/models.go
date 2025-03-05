package models

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

type AccountStatus int

const (
	AccountStatusUninit AccountStatus = iota
	AccountStatusFrozen
	AccountStatusActive
	AccountStatusNonexist
)

func (d AccountStatus) MarshalJSON() ([]byte, error) {
	switch d {
	case AccountStatusUninit:
		return json.Marshal("uninit")
	case AccountStatusFrozen:
		return json.Marshal("frozen")
	case AccountStatusActive:
		return json.Marshal("active")
	case AccountStatusNonexist:
		return json.Marshal("nonexist")
	default:
		return nil, fmt.Errorf("unknown account status: %d", d)
	}
}

type AccStatusChange int

const (
	AccStatusUnchanged AccStatusChange = iota
	AccStatusFrozen
	AccStatusDeleted
)

func (d AccStatusChange) MarshalJSON() ([]byte, error) {
	switch d {
	case AccStatusUnchanged:
		return json.Marshal("unchanged")
	case AccStatusFrozen:
		return json.Marshal("frozen")
	case AccStatusDeleted:
		return json.Marshal("deleted")
	default:
		return nil, fmt.Errorf("unknown account status change: %d", d)
	}
}

type ComputeSkipReason int

const (
	ComputeSkipNoState ComputeSkipReason = iota
	ComputeSkipBadState
	ComputeSkipNoGas
	ComputeSkipSuspended
)

func (d ComputeSkipReason) MarshalJSON() ([]byte, error) {
	switch d {
	case ComputeSkipNoState:
		return json.Marshal("no_state")
	case ComputeSkipBadState:
		return json.Marshal("bad_state")
	case ComputeSkipNoGas:
		return json.Marshal("no_gas")
	case ComputeSkipSuspended:
		return json.Marshal("suspended")
	default:
		return nil, fmt.Errorf("unknown compute skip reason: %d", d)
	}
}

type TrStoragePhase struct {
	StorageFeesCollected uint64          `msgpack:"storage_fees_collected" json:"storage_fees_collected"`
	StorageFeesDue       *uint64         `msgpack:"storage_fees_due" json:"storage_fees_due"`
	StatusChange         AccStatusChange `msgpack:"status_change" json:"status_change"`
}

type TrCreditPhase struct {
	DueFeesCollected *uint64 `msgpack:"due_fees_collected" json:"due_fees_collected"`
	Credit           uint64  `msgpack:"credit" json:"credit"`
}

type TrComputePhaseSkipped struct {
	Reason ComputeSkipReason `msgpack:"reason" json:"reason"`
}

type TrComputePhaseVm struct {
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
	VmInitStateHash  Hash    `msgpack:"vm_init_state_hash" json:"vm_init_state_hash"`
	VmFinalStateHash Hash    `msgpack:"vm_final_state_hash" json:"vm_final_state_hash"`
}

type StorageUsedShort struct {
	Cells uint64 `msgpack:"cells" json:"cells"`
	Bits  uint64 `msgpack:"bits" json:"bits"`
}

type TrActionPhase struct {
	Success         bool             `msgpack:"success" json:"success"`
	Valid           bool             `msgpack:"valid" json:"valid"`
	NoFunds         bool             `msgpack:"no_funds" json:"no_funds"`
	StatusChange    AccStatusChange  `msgpack:"status_change" json:"status_change"`
	TotalFwdFees    *uint64          `msgpack:"total_fwd_fees" json:"total_fwd_fees"`
	TotalActionFees *uint64          `msgpack:"total_action_fees" json:"total_action_fees"`
	ResultCode      int32            `msgpack:"result_code" json:"result_code"`
	ResultArg       *int32           `msgpack:"result_arg" json:"result_arg"`
	TotActions      uint16           `msgpack:"tot_actions" json:"tot_actions"`
	SpecActions     uint16           `msgpack:"spec_actions" json:"spec_actions"`
	SkippedActions  uint16           `msgpack:"skipped_actions" json:"skipped_actions"`
	MsgsCreated     uint16           `msgpack:"msgs_created" json:"msgs_created"`
	ActionListHash  Hash             `msgpack:"action_list_hash" json:"action_list_hash"`
	TotMsgSize      StorageUsedShort `msgpack:"tot_msg_size" json:"tot_msg_size"`
}

type TrBouncePhaseNegfunds struct {
	Dummy bool `msgpack:"dummy" json:"dummy"`
}

type TrBouncePhaseNofunds struct {
	MsgSize    StorageUsedShort `msgpack:"msg_size" json:"msg_size"`
	ReqFwdFees uint64           `msgpack:"req_fwd_fees" json:"req_fwd_fees"`
}

type TrBouncePhaseOk struct {
	MsgSize StorageUsedShort `msgpack:"msg_size" json:"msg_size"`
	MsgFees uint64           `msgpack:"msg_fees" json:"msg_fees"`
	FwdFees uint64           `msgpack:"fwd_fees" json:"fwd_fees"`
}

type Message struct {
	Hash         Hash        `msgpack:"hash" json:"hash"`
	Source       *string     `msgpack:"source" json:"source"`
	Destination  *string     `msgpack:"destination" json:"destination"`
	Value        *uint64     `msgpack:"value" json:"value"`
	FwdFee       *uint64     `msgpack:"fwd_fee" json:"fwd_fee"`
	IhrFee       *uint64     `msgpack:"ihr_fee" json:"ihr_fee"`
	CreatedLt    *uint64     `msgpack:"created_lt" json:"created_lt,string"`
	CreatedAt    *uint32     `msgpack:"created_at" json:"created_at"`
	Opcode       *OpcodeType `msgpack:"opcode" json:"opcode"`
	IhrDisabled  *bool       `msgpack:"ihr_disabled" json:"ihr_disabled"`
	Bounce       *bool       `msgpack:"bounce" json:"bounce"`
	Bounced      *bool       `msgpack:"bounced" json:"bounced"`
	ImportFee    *uint64     `msgpack:"import_fee" json:"import_fee"`
	BodyBoc      string      `msgpack:"body_boc" json:"body_boc"`
	InitStateBoc *string     `msgpack:"init_state_boc" json:"init_state_boc"`
}

type OpcodeType int32 // @name OpcodeType

func (v *OpcodeType) String() string {
	return fmt.Sprintf("0x%08x", uint32(*v))
}

func (v *OpcodeType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

type TransactionDescr struct {
	CreditFirst bool            `msgpack:"credit_first" json:"credit_first"`
	StoragePh   *TrStoragePhase `msgpack:"storage_ph" json:"storage_ph"`
	CreditPh    *TrCreditPhase  `msgpack:"credit_ph" json:"credit_ph"`
	ComputePh   ComputePhaseVar `msgpack:"compute_ph" json:"compute_ph"`
	Action      *TrActionPhase  `msgpack:"action" json:"action"`
	Aborted     bool            `msgpack:"aborted" json:"aborted"`
	Bounce      *BouncePhaseVar `msgpack:"bounce" json:"bounce"`
	Destroyed   bool            `msgpack:"destroyed" json:"destroyed"`
}

type Transaction struct {
	Hash                   Hash             `msgpack:"hash" json:"-"`
	Account                string           `msgpack:"account" json:"account"`
	Lt                     uint64           `msgpack:"lt" json:"lt,string"`
	PrevTransHash          Hash             `msgpack:"prev_trans_hash" json:"prev_trans_hash"`
	PrevTransLt            uint64           `msgpack:"prev_trans_lt" json:"prev_trans_lt,string"`
	Now                    uint32           `msgpack:"now" json:"now"`
	OrigStatus             AccountStatus    `msgpack:"orig_status" json:"orig_status"`
	EndStatus              AccountStatus    `msgpack:"end_status" json:"end_status"`
	InMsg                  *Message         `msgpack:"in_msg" json:"in_msg"`
	OutMsgs                []Message        `msgpack:"out_msgs" json:"out_msgs"`
	TotalFees              uint64           `msgpack:"total_fees" json:"total_fees"`
	AccountStateHashBefore Hash             `msgpack:"account_state_hash_before" json:"account_state_hash_before"`
	AccountStateHashAfter  Hash             `msgpack:"account_state_hash_after" json:"account_state_hash_after"`
	Description            TransactionDescr `msgpack:"description" json:"description"`
}

type TraceNode struct {
	Transaction Transaction `msgpack:"transaction"`
	Emulated    bool        `msgpack:"emulated"`
}

type ComputePhaseVar struct {
	Type uint8
	Data interface{} // Can be TrComputePhaseSkipped or TrComputePhaseVm
}

func (bpv ComputePhaseVar) MarshalJSON() ([]byte, error) {
	dataBytes, err := json.Marshal(bpv.Data)
	if err != nil {
		return nil, err
	}

	var dataMap map[string]interface{}
	err = json.Unmarshal(dataBytes, &dataMap)
	if err != nil {
		return nil, err
	}

	if bpv.Type == 0 {
		dataMap["skipped"] = true
	} else {
		dataMap["skipped"] = false
	}

	return json.Marshal(dataMap)
}

var _ msgpack.CustomDecoder = (*ComputePhaseVar)(nil)

func (s *ComputePhaseVar) DecodeMsgpack(dec *msgpack.Decoder) error {
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
		var a TrComputePhaseSkipped
		err = dec.Decode(&a)
		s.Data = a
	case 1:
		var b TrComputePhaseVm
		err = dec.Decode(&b)
		s.Data = b
	default:
		return fmt.Errorf("unknown variant index: %d", index)
	}

	s.Type = index
	return err
}

type BouncePhaseVar struct {
	Type uint8
	Data interface{} // Can be TrBouncePhaseNegfunds, TrBouncePhaseNofunds or TrBouncePhaseOk
}

func (bpv BouncePhaseVar) MarshalJSON() ([]byte, error) {
	dataBytes, err := json.Marshal(bpv.Data)
	if err != nil {
		return nil, err
	}

	var dataMap map[string]interface{}
	err = json.Unmarshal(dataBytes, &dataMap)
	if err != nil {
		return nil, err
	}

	if bpv.Type == 0 {
		dataMap["type"] = "negfunds"
	} else if bpv.Type == 1 {
		dataMap["type"] = "nofunds"
	} else {
		dataMap["type"] = "ok"
	}

	return json.Marshal(dataMap)
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
		var a TrBouncePhaseNegfunds
		err = dec.Decode(&a)
		s.Data = a
	case 1:
		var b TrBouncePhaseNofunds
		err = dec.Decode(&b)
		s.Data = b
	case 2:
		var c TrBouncePhaseOk
		err = dec.Decode(&c)
		s.Data = c
	default:
		return fmt.Errorf("unknown variant index: %d", index)
	}

	s.Type = index
	return err
}

type AccountState struct {
	Hash          Hash    `msgpack:"hash" json:"-"`
	Balance       uint64  `msgpack:"balance" json:"balance"`
	AccountStatus string  `msgpack:"account_status" json:"account_status"`
	FrozenHash    *Hash   `msgpack:"frozen_hash" json:"frozen_hash"`
	CodeHash      *Hash   `msgpack:"code_hash" json:"code_hash"`
	DataHash      *Hash   `msgpack:"data_hash" json:"data_hash"`
	LastTransHash *Hash   `msgpack:"last_trans_hash" json:"last_trans_hash"`
	LastTransLt   *uint64 `msgpack:"last_trans_lt" json:"last_trans_lt,string"`
	Timestamp     *uint32 `msgpack:"timestamp" json:"timestamp"`
}

type Hash [32]byte

func (h Hash) MarshalText() (data []byte, err error) {
	return []byte(base64.StdEncoding.EncodeToString(h[:])), nil
}

// MarshalJSON implements json.Marshaler interface
func (h Hash) MarshalJSON() ([]byte, error) {
	return json.Marshal(base64.StdEncoding.EncodeToString(h[:]))
}

// EncodeMsgpack implements msgpack.CustomEncoder interface
func (h Hash) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.EncodeBytes(h[:])
}

// DecodeMsgpack implements msgpack.CustomDecoder interface
func (h *Hash) DecodeMsgpack(dec *msgpack.Decoder) error {
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
