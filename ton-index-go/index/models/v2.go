package models

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
