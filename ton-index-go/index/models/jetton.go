package models

import (
	"encoding/json"
)

type JettonMaster struct {
	Address              AccountAddress         `json:"address"`
	TotalSupply          string                 `json:"total_supply"`
	Mintable             bool                   `json:"mintable"`
	AdminAddress         *AccountAddress        `json:"admin_address"`
	JettonContent        map[string]interface{} `json:"jetton_content"`
	JettonWalletCodeHash HashType               `json:"jetton_wallet_code_hash"`
	CodeHash             HashType               `json:"code_hash"`
	DataHash             HashType               `json:"data_hash"`
	LastTransactionLt    int64                  `json:"last_transaction_lt,string"`
	CodeBoc              string                 `json:"-"`
	DataBoc              string                 `json:"-"`
} // @name JettonMaster

type JettonWalletMintlessInfo struct {
	IsClaimed           *bool    `json:"-"`
	Amount              *string  `json:"amount"`
	StartFrom           *int64   `json:"start_from"`
	ExpireAt            *int64   `json:"expire_at"`
	CustomPayloadApiUri []string `json:"custom_payload_api_uri"`
} // @name JettonWalletMintlessInfo

type JettonWallet struct {
	Address           AccountAddress            `json:"address"`
	Balance           string                    `json:"balance"`
	Owner             AccountAddress            `json:"owner"`
	Jetton            AccountAddress            `json:"jetton"`
	LastTransactionLt int64                     `json:"last_transaction_lt,string"`
	CodeHash          *HashType                 `json:"code_hash"`
	DataHash          *HashType                 `json:"data_hash"`
	MintlessInfo      *JettonWalletMintlessInfo `json:"mintless_info,omitempty"`
} // @name JettonWallet

type JettonTransfer struct {
	QueryId               string           `json:"query_id"`
	Source                AccountAddress   `json:"source"`
	Destination           AccountAddress   `json:"destination"`
	Amount                string           `json:"amount"`
	SourceWallet          AccountAddress   `json:"source_wallet"`
	JettonMaster          AccountAddress   `json:"jetton_master"`
	TransactionHash       HashType         `json:"transaction_hash"`
	TransactionLt         int64            `json:"transaction_lt,string"`
	TransactionNow        int64            `json:"transaction_now"`
	TransactionAborted    bool             `json:"transaction_aborted"`
	ResponseDestination   *AccountAddress  `json:"response_destination"`
	CustomPayload         *BytesType       `json:"custom_payload"`
	DecodedCustomPayload  *json.RawMessage `json:"decoded_custom_payload" swaggertype:"object"`
	ForwardTonAmount      *string          `json:"forward_ton_amount"`
	ForwardPayload        *BytesType       `json:"forward_payload"`
	DecodedForwardPayload *json.RawMessage `json:"decoded_forward_payload" swaggertype:"object"`
	TraceId               *HashType        `json:"trace_id"`
} // @name JettonTransfer

type JettonBurn struct {
	QueryId              string           `json:"query_id"`
	Owner                AccountAddress   `json:"owner"`
	JettonWallet         AccountAddress   `json:"jetton_wallet"`
	JettonMaster         AccountAddress   `json:"jetton_master"`
	TransactionHash      HashType         `json:"transaction_hash"`
	TransactionLt        int64            `json:"transaction_lt,string"`
	TransactionNow       int64            `json:"transaction_now"`
	TransactionAborted   bool             `json:"transaction_aborted"`
	Amount               string           `json:"amount"`
	ResponseDestination  *AccountAddress  `json:"response_destination"`
	CustomPayload        *BytesType       `json:"custom_payload"`
	DecodedCustomPayload *json.RawMessage `json:"decoded_custom_payload" swaggertype:"object"`
	TraceId              *HashType        `json:"trace_id"`
} // @name JettonBurn
