package models

import "fmt"

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

type BalanceChangesResult struct {
	Ton     map[AccountAddress]int64                     `json:"changes"`
	Fees    map[AccountAddress]int64                     `json:"fees"`
	Jettons map[AccountAddress]map[AccountAddress]string `json:"jettons"`
}

func AddressInformationFromV3(state AccountStateFull) (*V2AddressInformation, error) {
	var info V2AddressInformation
	if state.Balance == nil {
		return nil, IndexError{Code: 500, Message: "balance is none"}
	}
	info.Balance = *state.Balance
	info.Code = state.CodeBoc
	info.Data = state.DataBoc
	info.LastTransactionHash = (*string)(state.LastTransactionHash)
	if state.LastTransactionLt != nil {
		info.LastTransactionLt = new(string)
		*info.LastTransactionLt = fmt.Sprintf("%d", *state.LastTransactionLt)
	}
	if state.AccountStatus == nil {
		return nil, IndexError{Code: 500, Message: "status is none"}
	}
	info.Status = *state.AccountStatus
	return &info, nil
}

func WalletInformationFromV3(state WalletState) (*V2WalletInformation, error) {
	var info V2WalletInformation
	if !state.IsWallet && !(state.AccountStatus != nil && *state.AccountStatus == "uninit") {
		return nil, nil
	}

	if state.Balance == nil {
		return nil, IndexError{Code: 500, Message: "balance is none"}
	}
	info.Balance = *state.Balance

	info.WalletType = state.WalletType
	info.Seqno = state.Seqno
	info.WalletId = state.WalletId

	if state.LastTransactionHash == nil {
		return nil, IndexError{Code: 500, Message: "last_transaction_hash is none"}
	}
	info.LastTransactionHash = string(*state.LastTransactionHash)
	if state.LastTransactionLt == nil {
		return nil, IndexError{Code: 500, Message: "last_transaction_lt is none"}
	}
	info.LastTransactionLt = fmt.Sprintf("%d", *state.LastTransactionLt)

	if state.AccountStatus == nil {
		return nil, IndexError{Code: 500, Message: "status is none"}
	}
	info.Status = *state.AccountStatus
	return &info, nil
}
