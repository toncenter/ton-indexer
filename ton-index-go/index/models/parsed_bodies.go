package models

// ParsedBody interface for different message body types
type ParsedBody interface {
	GetType() string
}

type JettonTransferBody struct {
	QueryID          uint64                 `json:"query_id"`
	Amount           *string                `json:"amount"`
	Destination      *AccountAddress        `json:"destination"`
	Response         *AccountAddress        `json:"response"`
	CustomPayload    []byte                 `json:"custom_payload"`
	ForwardAmount    *string                `json:"forward_amount"`
	Comment          string                 `json:"comment"`
	EncryptedComment bool                   `json:"encrypted_comment"`
	ForwardPayload   []byte                 `json:"forward_payload"`
	PayloadSumType   string                 `json:"payload_sum_type"`
	StonfiSwapBody   map[string]interface{} `json:"stonfi_swap_body"`
}

func (jt *JettonTransferBody) GetType() string { return "jetton_transfer" }

type JettonBurnBody struct {
	QueryID             uint64          `json:"query_id"`
	Amount              *string         `json:"amount"`
	ResponseDestination *AccountAddress `json:"response_destination"`
}

func (jb *JettonBurnBody) GetType() string { return "jetton_burn" }

type JettonMinterBody struct {
	Opcode                uint32          `json:"opcode"`
	QueryID               uint64          `json:"query_id"`
	ToAddress             *AccountAddress `json:"to_address"`
	TonAmount             *string         `json:"ton_amount"`
	MasterMsgQueryID      uint64          `json:"master_msg_query_id"`
	MasterMsgJettonAmount *string         `json:"master_msg_jetton_amount"`
}

func (mjm *JettonMinterBody) GetType() string { return "jetton_mint" }

type JettonMintBody struct {
	QueryID      uint64          `json:"query_id"`
	ToAddress    *AccountAddress `json:"to_address"`
	TonAmount    *string         `json:"ton_amount"`
	JettonAmount *string         `json:"jetton_amount"`
}

func (mjm *JettonMintBody) GetType() string { return "jetton_mint" }

type NFTTransferBody struct {
	QueryID             uint64          `json:"query_id"`
	NewOwner            *AccountAddress `json:"new_owner"`
	ResponseDestination *AccountAddress `json:"response_destination"`
	CustomPayload       []byte          `json:"custom_payload"`
	ForwardAmount       *string         `json:"forward_amount"`
	ForwardPayload      []byte          `json:"forward_payload"`
}

func (nt *NFTTransferBody) GetType() string { return "nft_transfer" }

type TonTransferMessage struct {
	Comment   string `json:"comment"`
	Encrypted bool   `json:"encrypted"`
}

func (ttm *TonTransferMessage) GetType() string { return "ton_transfer" }

type JettonTopUpBody struct {
	QueryID uint64 `json:"query_id"`
}

func (jt *JettonTopUpBody) GetType() string { return "jetton_top_up" }

type JettonChangeAdminBody struct {
	QueryID      uint64          `json:"query_id"`
	NewAdminAddr *AccountAddress `json:"new_admin_address"`
}

func (jca *JettonChangeAdminBody) GetType() string { return "jetton_change_admin" }

type JettonClaimAdminBody struct {
	QueryID uint64 `json:"query_id"`
}

func (jca *JettonClaimAdminBody) GetType() string { return "jetton_claim_admin" }

type JettonChangeContentBody struct {
	QueryID        uint64 `json:"query_id"`
	NewMetadataUrl string `json:"new_metadata_url"`
}

func (jcc *JettonChangeContentBody) GetType() string { return "jetton_change_content" }

type JettonCallToBody struct {
	QueryID    uint64          `json:"query_id"`
	ToAddress  *AccountAddress `json:"to_address"`
	TonAmount  *string         `json:"ton_amount"`
	ActionType string          `json:"action_type"`
	ActionData []byte          `json:"action_data"`
}

func (jct *JettonCallToBody) GetType() string { return "jetton_call_to" }

type JettonSetStatusBody struct {
	QueryID   uint64 `json:"query_id"`
	NewStatus uint8  `json:"new_status"`
}

func (jss *JettonSetStatusBody) GetType() string { return "jetton_set_status" }

type SingleNominatorWithdrawBody struct {
	QueryID uint64  `json:"query_id"`
	Coins   *string `json:"coins"`
}

func (snw *SingleNominatorWithdrawBody) GetType() string { return "single_nominator_withdraw" }

type SingleNominatorChangeValidatorBody struct {
	QueryID          uint64          `json:"query_id"`
	ValidatorAddress *AccountAddress `json:"validator_address"`
}

func (sncv *SingleNominatorChangeValidatorBody) GetType() string {
	return "single_nominator_change_validator"
}

type VestingInternalTransferBody struct {
	QueryID     uint64          `json:"query_id"`
	SendMode    uint8           `json:"send_mode"`
	Destination *AccountAddress `json:"destination"`
	Value       *string         `json:"value"`
	MessageBody []byte          `json:"message_body"`
	Comment     string          `json:"comment"`
}

func (vit *VestingInternalTransferBody) GetType() string { return "vesting_internal_transfer" }

type MultisigUpdateParamsBody struct {
	NewThreshold uint8             `json:"new_threshold"`
	NewSigners   []*AccountAddress `json:"new_signers"`
	NewProposers []*AccountAddress `json:"new_proposers"`
}

func (mup *MultisigUpdateParamsBody) GetType() string { return "multisig_update_params" }

type UnknownMessage struct {
	Opcode uint32 `json:"opcode"`
	Data   []byte `json:"data"`
}

func (um *UnknownMessage) GetType() string { return "unknown" }
