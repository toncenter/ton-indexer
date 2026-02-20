package models

type MultisigOrder struct {
	Address           AccountAddress   `json:"address"`
	MultisigAddress   AccountAddress   `json:"multisig_address"`
	OrderSeqno        *string          `json:"order_seqno"`
	Threshold         *int32           `json:"threshold"`
	SentForExecution  *bool            `json:"sent_for_execution"`
	ApprovalsMask     *string          `json:"approvals_mask"`
	ApprovalsNum      *int32           `json:"approvals_num"`
	ExpirationDate    *uint64          `json:"expiration_date"`
	OrderBoc          *string          `json:"order_boc"`
	Signers           []AccountAddress `json:"signers"`
	LastTransactionLt int64            `json:"last_transaction_lt,string"`
	CodeHash          *HashType        `json:"code_hash"`
	DataHash          *HashType        `json:"data_hash"`
	Actions           []OrderAction    `json:"actions"`
} // @name MultisigOrder

type Multisig struct {
	Address           AccountAddress   `json:"address"`
	NextOrderSeqno    *string          `json:"next_order_seqno"`
	Threshold         *int32           `json:"threshold"`
	Signers           []AccountAddress `json:"signers"`
	Proposers         []AccountAddress `json:"proposers"`
	LastTransactionLt int64            `json:"last_transaction_lt,string"`
	CodeHash          *HashType        `json:"code_hash"`
	DataHash          *HashType        `json:"data_hash"`
	Orders            []MultisigOrder  `json:"orders"`
} // @name Multisig

type OrderAction struct {
	Destination    *AccountAddress `json:"destination"`
	Value          *string         `json:"value"`
	BodyRaw        []byte          `json:"body_raw"`
	Parsed         bool            `json:"parsed"`
	Error          *string         `json:"error"`
	ParsedBody     *ParsedBody     `json:"parsed_body"`
	ParsedBodyType string          `json:"parsed_body_type"`
	SendMode       uint8           `json:"send_mode"`
}
