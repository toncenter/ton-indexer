package index

import "fmt"

// responses
type MasterchainInfo struct {
	Last  *Block `json:"last"`
	First *Block `json:"first"`
} // @name MasterchainInfo

type BlocksResponse struct {
	Blocks []Block `json:"blocks"`
} // @name BlocksResponse

type TransactionsResponse struct {
	Transactions []Transaction `json:"transactions"`
	AddressBook  AddressBook   `json:"address_book"`
} // @name TransactionsResponse

type MessagesResponse struct {
	Messages    []Message   `json:"messages"`
	AddressBook AddressBook `json:"address_book"`
	Metadata    Metadata    `json:"metadata"`
} // @name MessagesResponse

type AccountStatesResponse struct {
	Accounts    []AccountStateFull `json:"accounts"`
	AddressBook AddressBook        `json:"address_book"`
	Metadata    Metadata           `json:"metadata"`
} // @name AccountStatesResponse

type WalletStatesResponse struct {
	Wallets     []WalletState `json:"wallets"`
	AddressBook AddressBook   `json:"address_book"`
	Metadata    Metadata      `json:"metadata"`
} // @name WalletStatesResponse

type NFTCollectionsResponse struct {
	Collections []NFTCollection `json:"nft_collections"`
	AddressBook AddressBook     `json:"address_book"`
	Metadata    Metadata        `json:"metadata"`
} // @name NFTCollectionsResponse

type NFTItemsResponse struct {
	Items       []NFTItem   `json:"nft_items"`
	AddressBook AddressBook `json:"address_book"`
	Metadata    Metadata    `json:"metadata"`
} // @name NFTItemsResponse

type NFTTransfersResponse struct {
	Transfers   []NFTTransfer `json:"nft_transfers"`
	AddressBook AddressBook   `json:"address_book"`
	Metadata    Metadata      `json:"metadata"`
} // @name NFTTransfersResponse

type JettonMastersResponse struct {
	Masters     []JettonMaster `json:"jetton_masters"`
	AddressBook AddressBook    `json:"address_book"`
	Metadata    Metadata       `json:"metadata"`
} // @name JettonMastersResponse

type JettonWalletsResponse struct {
	Wallets     []JettonWallet `json:"jetton_wallets"`
	AddressBook AddressBook    `json:"address_book"`
	Metadata    Metadata       `json:"metadata"`
} // @name JettonWalletsResponse

type JettonTransfersResponse struct {
	Transfers   []JettonTransfer `json:"jetton_transfers"`
	AddressBook AddressBook      `json:"address_book"`
	Metadata    Metadata         `json:"metadata"`
} // @name JettonTransfersResponse

type JettonBurnsResponse struct {
	Burns       []JettonBurn `json:"jetton_burns"`
	AddressBook AddressBook  `json:"address_book"`
	Metadata    Metadata     `json:"metadata"`
} // @name JettonBurnsResponse

type TracesResponse struct {
	Traces      []Trace     `json:"traces"`
	AddressBook AddressBook `json:"address_book"`
	Metadata    Metadata    `json:"metadata"`
} // @name TracesResponse

type DeprecatedEventsResponse struct {
	Events      []Trace     `json:"events"`
	AddressBook AddressBook `json:"address_book"`
	Metadata    Metadata    `json:"metadata"`
} // @name DeprecatedEventsResponse

type ActionsResponse struct {
	Actions     []Action    `json:"actions"`
	AddressBook AddressBook `json:"address_book"`
	Metadata    Metadata    `json:"metadata"`
} // @name ActionsResponse

type DNSRecordsResponse struct {
	Records     []DNSRecord `json:"records"`
	AddressBook AddressBook `json:"address_book"`
} // @name DNSRecordsResponse

// errors
type RequestError struct {
	Message string `json:"error"`
	Code    int    `json:"code"`
} // @name RequestError

func (r RequestError) Error() string {
	return fmt.Sprintf("Error %d: %s", r.Code, r.Message)
}
