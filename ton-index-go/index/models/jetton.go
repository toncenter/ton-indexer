package models

import (
	"encoding/json"
	"fmt"
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
	CustomPayload         *string          `json:"custom_payload"`
	DecodedCustomPayload  *json.RawMessage `json:"decoded_custom_payload" swaggertype:"object"`
	ForwardTonAmount      *string          `json:"forward_ton_amount"`
	ForwardPayload        *string          `json:"forward_payload"`
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
	CustomPayload        *string          `json:"custom_payload"`
	DecodedCustomPayload *json.RawMessage `json:"decoded_custom_payload" swaggertype:"object"`
	TraceId              *HashType        `json:"trace_id"`
} // @name JettonBurn

type RawActionJettonSwapPeerSwap struct {
	AssetIn   *AccountAddress
	AmountIn  *string
	AssetOut  *AccountAddress
	AmountOut *string
}

func (p *RawActionJettonSwapPeerSwap) ScanNull() error {
	return fmt.Errorf("cannot scan NULL into RawActionJettonSwapPeerSwap")
}

func (p *RawActionJettonSwapPeerSwap) ScanIndex(i int) any {
	switch i {
	case 0:
		return &p.AssetIn
	case 1:
		return &p.AmountIn
	case 2:
		return &p.AssetOut
	case 3:
		return &p.AmountOut
	default:
		panic("invalid index")
	}
}

type RawActionVaultExcessEntry struct {
	Asset  *AccountAddress
	Amount *string
}

func (p *RawActionVaultExcessEntry) ScanNull() error {
	return fmt.Errorf("cannot scan NULL into RawActionVaultExcessEntry")
}

func (p *RawActionVaultExcessEntry) ScanIndex(i int) any {
	switch i {
	case 0:
		return &p.Asset
	case 1:
		return &p.Amount
	default:
		panic("invalid index")
	}
}
