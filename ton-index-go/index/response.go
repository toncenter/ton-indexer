package index

import (
	"fmt"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

// responses
type MasterchainInfo struct {
	Last  *models.Block `json:"last"`
	First *models.Block `json:"first"`
} // @name MasterchainInfo

type BlocksResponse struct {
	Blocks []models.Block `json:"blocks"`
} // @name BlocksResponse

type TransactionsResponse struct {
	Transactions []models.Transaction `json:"transactions"`
	AddressBook  models.AddressBook   `json:"address_book"`
} // @name TransactionsResponse

type MessagesResponse struct {
	Messages    []models.Message   `json:"messages"`
	AddressBook models.AddressBook `json:"address_book"`
	Metadata    models.Metadata    `json:"metadata"`
} // @name MessagesResponse

type AccountStatesResponse struct {
	Accounts    []models.AccountStateFull `json:"accounts"`
	AddressBook models.AddressBook        `json:"address_book"`
	Metadata    models.Metadata           `json:"metadata"`
} // @name AccountStatesResponse

type WalletStatesResponse struct {
	Wallets     []models.WalletState `json:"wallets"`
	AddressBook models.AddressBook   `json:"address_book"`
	Metadata    models.Metadata      `json:"metadata"`
} // @name WalletStatesResponse

type NFTCollectionsResponse struct {
	Collections []models.NFTCollection `json:"nft_collections"`
	AddressBook models.AddressBook     `json:"address_book"`
	Metadata    models.Metadata        `json:"metadata"`
} // @name NFTCollectionsResponse

type NFTItemsResponse struct {
	Items       []models.NFTItem   `json:"nft_items"`
	AddressBook models.AddressBook `json:"address_book"`
	Metadata    models.Metadata    `json:"metadata"`
} // @name NFTItemsResponse

type NFTTransfersResponse struct {
	Transfers   []models.NFTTransfer `json:"nft_transfers"`
	AddressBook models.AddressBook   `json:"address_book"`
	Metadata    models.Metadata      `json:"metadata"`
} // @name NFTTransfersResponse

type JettonMastersResponse struct {
	Masters     []models.JettonMaster `json:"jetton_masters"`
	AddressBook models.AddressBook    `json:"address_book"`
	Metadata    models.Metadata       `json:"metadata"`
} // @name JettonMastersResponse

type JettonWalletsResponse struct {
	Wallets     []models.JettonWallet `json:"jetton_wallets"`
	AddressBook models.AddressBook    `json:"address_book"`
	Metadata    models.Metadata       `json:"metadata"`
} // @name JettonWalletsResponse

type JettonTransfersResponse struct {
	Transfers   []models.JettonTransfer `json:"jetton_transfers"`
	AddressBook models.AddressBook      `json:"address_book"`
	Metadata    models.Metadata         `json:"metadata"`
} // @name JettonTransfersResponse

type JettonBurnsResponse struct {
	Burns       []models.JettonBurn `json:"jetton_burns"`
	AddressBook models.AddressBook  `json:"address_book"`
	Metadata    models.Metadata     `json:"metadata"`
} // @name JettonBurnsResponse

type TracesResponse struct {
	Traces      []models.Trace     `json:"traces"`
	AddressBook models.AddressBook `json:"address_book"`
	Metadata    models.Metadata    `json:"metadata"`
} // @name TracesResponse

type DeprecatedEventsResponse struct {
	Events      []models.Trace     `json:"events"`
	AddressBook models.AddressBook `json:"address_book"`
	Metadata    models.Metadata    `json:"metadata"`
} // @name DeprecatedEventsResponse

type ActionsResponse struct {
	Actions     []models.Action    `json:"actions"`
	AddressBook models.AddressBook `json:"address_book"`
	Metadata    models.Metadata    `json:"metadata"`
} // @name ActionsResponse

type DNSRecordsResponse struct {
	Records     []models.DNSRecord `json:"records"`
	AddressBook models.AddressBook `json:"address_book"`
} // @name DNSRecordsResponse

// errors
type RequestError struct {
	Message string `json:"error"`
	Code    int    `json:"code"`
} // @name RequestError

func (r RequestError) Error() string {
	return fmt.Sprintf("Error %d: %s", r.Code, r.Message)
}
