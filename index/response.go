package index

import "fmt"

// responses
type MasterchainInfo struct {
	Last  *Block `json:"last"`
	First *Block `json:"first"`
}

type BlocksResponse struct {
	Blocks []Block `json:"blocks"`
}

type TransactionsResponse struct {
	Transactions []Transaction `json:"transactions"`
	AddressBook  AddressBook   `json:"address_book"`
}

type MessagesResponse struct {
	Messages    []Message   `json:"messages"`
	AddressBook AddressBook `json:"address_book"`
}

// errors
type RequestError struct {
	Message string `json:"error"`
	Code    int    `json:"code"`
}

func (r RequestError) Error() string {
	return fmt.Sprintf("Error %d: %s", r.Code, r.Message)
}
