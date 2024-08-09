package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/pprof"
	"github.com/gofiber/swagger"
	_ "github.com/kdimentionaltree/ton-index-go/docs"
	"github.com/kdimentionaltree/ton-index-go/index"
)

type Settings struct {
	PgDsn        string
	MaxConns     int
	MinConns     int
	Bind         string
	InstanceName string
	Prefork      bool
	Debug        bool
	Request      index.RequestSettings
}

func onlyOneOf(flags ...bool) bool {
	res := 0
	for _, v := range flags {
		if v {
			res += 1
		}
	}
	return res <= 1
}

var pool *index.DbClient
var settings Settings

//	@title			TON Index (Go)
//	@version		1.1.0
//	@description	TON Index collects data from a full node to PostgreSQL database and provides convenient API to an indexed blockchain.
//  @query.collection.format multi

//	@securitydefinitions.apikey APIKeyHeader
//	@in		header
//	@name	X-Api-Key
//	@securitydefinitions.apikey APIKeyQuery
//	@in		query
//	@name	api_key

// @summary		Get Masterchain Info
// @description	Get first and last indexed block
// @id	api_v3_get_masterchain_info
// @tags	blockchain
// @Accept       json
// @Produce      json
// @success		200	{object}	index.MasterchainInfo
// @failure		400	{object}	index.RequestError
// @router			/api/v3/masterchainInfo [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetMasterchainInfo(c *fiber.Ctx) error {
	info, err := pool.QueryMasterchainInfo(settings.Request)
	if err != nil {
		return err
	}
	return c.JSON(&info)
}

// @summary Get blocks
// @description Returns blocks by specified filters.
// @id api_v3_get_blocks
// @tags blockchain
// @Accept       json
// @Produce      json
// @success		200	{object}	index.BlocksResponse
// @failure		400	{object}	index.RequestError
// @param	workchain query int32 false "Block workchain."
// @param	shard query string false "Block shard id. Must be sent with *workchain*. Example: `8000000000000000`."
// @param	seqno query int32 false "Block block seqno. Must be sent with *workchain* and *shard*."
// @param	mc_seqno query int32 false "Masterchain block seqno"
// @param start_utime query int32 false "Query blocks with generation UTC timestamp **after** given timestamp." minimum(0)
// @param end_utime query int32 false "Query blocks with generation UTC timestamp **before** given timestamp." minimum(0)
// @param start_lt query int64 false "Query blocks with `lt >= start_lt`." minimum(0)
// @param end_lt query int64 false "Query blocks with `lt <= end_lt`." minimum(0)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// @param sort query string false "Sort results by UTC timestamp." Enums(asc, desc) default(desc)
// @router			/api/v3/blocks [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetBlocks(c *fiber.Ctx) error {
	blk_req := index.BlockRequest{}
	utime_req := index.UtimeRequest{}
	lt_req := index.LtRequest{}
	lim_req := index.LimitRequest{}

	if err := c.QueryParser(&blk_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&utime_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lt_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	blks, err := pool.QueryBlocks(blk_req, utime_req, lt_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(blks) == 0 {
	// 	return index.IndexError{Code: 404, Message: "blocks not found"}
	// }

	blk_resp := index.BlocksResponse{Blocks: blks}
	return c.JSON(blk_resp)
}

// @summary Get masterchain block shard state
// @description Get masterchain block shard state. Same as /api/v2/shards.
// @id api_v3_get_masterchainBlockShardState
// @tags blockchain
// @Accept       json
// @Produce      json
// @success		200	{object}	index.TransactionsResponse
// @failure		400	{object}	index.RequestError
// @param	seqno query int32 true "Masterchain block seqno."
// @router			/api/v3/masterchainBlockShardState [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetShards(c *fiber.Ctx) error {
	seqno := c.QueryInt("seqno")
	blks, err := pool.QueryShards(seqno, settings.Request)
	if err != nil {
		return err
	}
	if len(blks) == 0 {
		return index.IndexError{Code: 404, Message: "blocks not found"}
	}

	blk_resp := index.BlocksResponse{Blocks: blks}
	return c.JSON(blk_resp)
}

// @summary Get masterchain block shard state
//
//	@description Returns all worchain blocks, that appeared after previous masterchain block.
//
//		 **Note:** this method is not equivalent with [/api/v2/shards](https://toncenter.com/api/v2/#/blocks/get_shards_shards_get).
//
// @id api_v3_get_masterchainBlockShards
// @tags blockchain
// @Accept       json
// @Produce      json
// @success		200	{object}	index.TransactionsResponse
// @failure		400	{object}	index.RequestError
// @param	seqno query int32 true "Masterchain block seqno."
// @router			/api/v3/masterchainBlockShards [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetShardsDiff(c *fiber.Ctx) error {
	seqno := c.QueryInt("seqno")
	blk_req := index.BlockRequest{}
	blk_req.McSeqno = new(int32)
	*blk_req.McSeqno = int32(seqno)
	blks, err := pool.QueryBlocks(blk_req, index.UtimeRequest{}, index.LtRequest{}, index.LimitRequest{}, settings.Request)
	if err != nil {
		return err
	}
	if len(blks) == 0 {
		return index.IndexError{Code: 404, Message: "blocks not found"}
	}

	blk_resp := index.BlocksResponse{Blocks: blks}
	return c.JSON(blk_resp)
}

// @summary Get transactions
// @description Get transactions by specified filter.
// @id api_v3_get_transactions
// @tags blockchain
// @Accept       json
// @Produce      json
// @success		200	{object}	index.TransactionsResponse
// @failure		400	{object}	index.RequestError
// @param	workchain query int32 false "Block workchain."
// @param	shard query string false "Block shard id. Must be sent with *workchain*. Example: `8000000000000000`."
// @param	seqno query int32 false "Block block seqno. Must be sent with *workchain* and *shard*."
// @param	mc_seqno query int32 false "Masterchain block seqno."
// @param 	account	query []string false "List of account addresses to get transactions. Can be sent in hex, base64 or base64url form." collectionFormat(multi)
// @param exclude_account query []string false "Exclude transactions on specified account addresses." collectionFormat(multi)
// @param hash query string false "Transaction hash."
// @param lt query int64 false "Transaction lt."
// @param start_utime query int32 false "Query transactions with generation UTC timestamp **after** given timestamp." minimum(0)
// @param end_utime query int32 false "Query transactions with generation UTC timestamp **before** given timestamp." minimum(0)
// @param start_lt query int64 false "Query transactions with `lt >= start_lt`." minimum(0)
// @param end_lt query int64 false "Query transactions with `lt <= end_lt`." minimum(0)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// @param sort query string false "Sort transactions by lt." Enums(asc, desc) default(desc)
// @router			/api/v3/transactions [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetTransactions(c *fiber.Ctx) error {
	blk_req := index.BlockRequest{}
	tx_req := index.TransactionRequest{}
	utime_req := index.UtimeRequest{}
	lt_req := index.LtRequest{}
	lim_req := index.LimitRequest{}

	if err := c.QueryParser(&blk_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&tx_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&utime_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lt_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	txs, book, err := pool.QueryTransactions(
		blk_req, tx_req, index.MessageRequest{},
		utime_req, lt_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(txs) == 0 {
	// 	return index.IndexError{Code: 404, Message: "transactions not found"}
	// }

	txs_resp := index.TransactionsResponse{Transactions: txs, AddressBook: book}
	return c.JSON(txs_resp)
}

// @summary Get Adjacent Transactions
// @description Get parent and/or children for specified transaction.
// @id api_v3_get_adjacent_transactions
// @tags blockchain
// @Accept       json
// @Produce      json
// @success		200	{object}	index.TransactionsResponse
// @failure		400	{object}	index.RequestError
// @param hash query string false "Transaction hash."
// @param direction query string false "Direction of message." Enums(in, out)
// @router			/api/v3/adjacentTransactions [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetAdjacentTransactions(c *fiber.Ctx) error {
	req := index.AdjacentTransactionRequest{}

	if err := c.QueryParser(&req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	txs, book, err := pool.QueryAdjacentTransactions(req, settings.Request)
	if err != nil {
		return err
	}
	// if len(txs) == 0 {
	// 	return index.IndexError{Code: 404, Message: "transactions not found"}
	// }

	txs_resp := index.TransactionsResponse{Transactions: txs, AddressBook: book}
	return c.JSON(txs_resp)
}

// @summary Get transactions by Masterchain block
// @description Returns transactions from masterchain block and from all shards.
// @id api_v3_get_transactions_by_masterchain_block
// @tags blockchain
// @Accept       json
// @Produce      json
// @success		200	{object}	index.TransactionsResponse
// @failure		400	{object}	index.RequestError
// @param	seqno query int32 true "Masterchain block seqno."
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// @param sort query string false "Sort transactions by lt." Enums(asc, desc) default(desc)
// @router			/api/v3/transactionsByMasterchainBlock [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetTransactionsByMasterchainBlock(c *fiber.Ctx) error {
	seqno := int32(c.QueryInt("seqno"))
	lim_req := index.LimitRequest{}
	blk_req := index.BlockRequest{McSeqno: &seqno}

	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	txs, book, err := pool.QueryTransactions(
		blk_req, index.TransactionRequest{}, index.MessageRequest{}, index.UtimeRequest{},
		index.LtRequest{}, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(txs) == 0 {
	// 	return index.IndexError{Code: 404, Message: "transactions not found"}
	// }

	txs_resp := index.TransactionsResponse{Transactions: txs, AddressBook: book}
	return c.JSON(txs_resp)
}

// @summary Get transactions by message
//
//	@description Get transactions whose inbound/outbound message has the specified hash. \
//	This endpoint returns list of Transaction objectssince collisions of message hashes can occur.
//
// @id api_v3_get_transactions_by_message
// @tags blockchain
// @Accept       json
// @Produce      json
// @success		200	{object}	index.TransactionsResponse
// @failure		400	{object}	index.RequestError
// @param msg_hash query string false "Message hash. Acceptable in hex, base64 and base64url forms."
// @param body_hash query string false "Hash of message body."
// @param opcode query string false "Opcode of message in hex or signed 32-bit decimal form."
// @param direction query string false "Direction of message." Enums(in, out)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// // @param sort query string false "Sort transactions by lt." Enums(asc, desc) default(desc)
// @router			/api/v3/transactionsByMessage [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetTransactionsByMessage(c *fiber.Ctx) error {
	msg_req := index.MessageRequest{}
	lim_req := index.LimitRequest{}

	if err := c.QueryParser(&msg_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if msg_req.BodyHash == nil && msg_req.MessageHash == nil && msg_req.Opcode == nil {
		return index.IndexError{Code: 422, Message: "at least one of msg_hash, body_hash, opcode should be specified"}
	}

	txs, book, err := pool.QueryTransactions(
		index.BlockRequest{}, index.TransactionRequest{}, msg_req,
		index.UtimeRequest{}, index.LtRequest{}, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(txs) == 0 {
	// 	return index.IndexError{Code: 404, Message: "transactions not found"}
	// }

	txs_resp := index.TransactionsResponse{Transactions: txs, AddressBook: book}
	return c.JSON(txs_resp)
}

// @summary Get messages
//
//	@description Get messages by specified filters.
//
// @id api_v3_get_messages
// @tags blockchain
// @Accept       json
// @Produce      json
// @success		200	{object}	index.MessagesResponse
// @failure		400	{object}	index.RequestError
// @param msg_hash query []string false "Message hash. Acceptable in hex, base64 and base64url forms." collectionFormat(multi)
// @param body_hash query string false "Hash of message body."
// @param source query string false "The source account address. Can be sent in hex, base64 or base64url form. Use value `null` to get external messages."
// @param destination query string false "The destination account address. Can be sent in hex, base64 or base64url form. Use value `null` to get log messages."
// @param opcode query string false "Opcode of message in hex or signed 32-bit decimal form."
// @param direction query string false "Direction of message." Enums(in, out)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// // @param sort query string false "Sort transactions by lt." Enums(asc, desc) default(desc)
// @router			/api/v3/messages [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetMessages(c *fiber.Ctx) error {
	msg_req := index.MessageRequest{}
	utime_req := index.UtimeRequest{}
	lt_req := index.LtRequest{}
	lim_req := index.LimitRequest{}

	if err := c.QueryParser(&msg_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	hash_str := c.Query("hash")
	if len(hash_str) > 0 && msg_req.MessageHash == nil {
		hash_val := index.HashConverter(hash_str)
		if hash_val.IsValid() {
			if hash, ok := hash_val.Interface().(index.HashType); ok {
				msg_req.MessageHash = []index.HashType{hash}
			}
		}
	}
	if err := c.QueryParser(&utime_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lt_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	msgs, book, err := pool.QueryMessages(msg_req, utime_req, lt_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(msgs) == 0 {
	// 	return index.IndexError{Code: 404, Message: "messages not found"}
	// }

	msgs_resp := index.MessagesResponse{Messages: msgs, AddressBook: book}
	return c.JSON(msgs_resp)
}

// @summary Address Book
//
// @description Query address book
//
// @id api_v3_get_address_book
// @tags accounts
// @Accept json
// @Produce json
// @success 200 {object} index.AddressBook
// @failure 400 {object} index.RequestError
// @param address query []string true "List of addresses in any form to get address book. Max: 1024." collectionFormat(multi)
// @router /api/v3/addressBook [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetAddressBook(c *fiber.Ctx) error {
	var addr_book_req index.AddressBookRequest
	if err := c.QueryParser(&addr_book_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if len(addr_book_req.Address) == 0 {
		return index.IndexError{Code: 422, Message: "at least 1 address required"}
	}
	book, err := pool.QueryAddressBook(addr_book_req.Address, settings.Request)
	if err != nil {
		return err
	}
	return c.JSON(book)
}

// @summary Get Account States
//
// @description Query account states
//
// @id api_v3_get_account_states
// @tags accounts
// @Accept json
// @Produce json
// @success 200 {object} index.AccountStatesResponse
// @failure 400 {object} index.RequestError
// @param address query []string true "List of addresses in any form to get address book. Max: 1024." collectionFormat(multi)
// // @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// // @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// @router /api/v3/accountStates [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetAccountStates(c *fiber.Ctx) error {
	var account_req index.AccountRequest
	var lim_req index.LimitRequest

	if err := c.QueryParser(&account_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	if len(account_req.AccountAddress) == 0 {
		return index.IndexError{Code: 422, Message: "address of account is required"}
	}

	res, book, err := pool.QueryAccountStates(account_req, lim_req, settings.Request)
	if err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	// if len(res) == 0 {
	// 	return index.IndexError{Code: 404, Message: "account states not found"}
	// }

	resp := index.AccountStatesResponse{Accounts: res, AddressBook: book}
	return c.JSON(resp)
}

// @summary Get Wallet States
//
// @description Query wallet information
//
// @id api_v3_get_wallet_states
// @tags accounts
// @Accept json
// @Produce json
// @success 200 {object} index.WalletStatesResponse
// @failure 400 {object} index.RequestError
// @param address query []string true "List of addresses in any form to get address book. Max: 1024." collectionFormat(multi)
// @router /api/v3/walletStates [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetWalletStates(c *fiber.Ctx) error {
	var account_req index.AccountRequest
	var lim_req index.LimitRequest
	if err := c.QueryParser(&account_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	if len(account_req.AccountAddress) == 0 {
		return index.IndexError{Code: 422, Message: "address of account is required"}
	}

	res, book, err := pool.QueryWalletStates(account_req, lim_req, settings.Request)
	if err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	resp := index.WalletStatesResponse{Wallets: res, AddressBook: book}
	return c.JSON(resp)
}

// @summary Get NFT collections
//
// @description Get NFT collections by specified filters
//
// @id api_v3_get_nft_collections
// @tags nfts
// @Accept json
// @Produce json
// @success 200 {object} index.NFTCollectionsResponse
// @failure 400 {object} index.RequestError
// @param collection_address query []string false "Collection address in any form. Max: 1024." collectionFormat(multi)
// @param owner_address query []string false "Address of collection owner in any form. Max: 1024." collectionFormat(multi)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// @router /api/v3/nft/collections [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetNFTCollections(c *fiber.Ctx) error {
	var nft_req index.NFTCollectionRequest
	var lim_req index.LimitRequest

	if err := c.QueryParser(&nft_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	res, book, err := pool.QueryNFTCollections(nft_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(res) == 0 {
	// 	return index.IndexError{Code: 404, Message: "nft collections not found"}
	// }
	resp := index.NFTCollectionsResponse{Collections: res, AddressBook: book}
	return c.JSON(resp)
}

// @summary Get NFT items
//
// @description Get NFT items by specified filters
//
// @id api_v3_get_nft_items
// @tags nfts
// @Accept json
// @Produce json
// @success 200 {object} index.NFTItemsResponse
// @failure 400 {object} index.RequestError
// @param address query []string false "NFT item address in any form. Max: 1000." collectionFormat(multi)
// @param owner_address query []string false "Address of NFT item owner in any form. Max: 1000." collectionFormat(multi)
// @param collection_address query string false "Collection address in any form."
// @param index query []string false "Index of item for given collection. Max: 1000." collectionFormat(multi)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// @router /api/v3/nft/items [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetNFTItems(c *fiber.Ctx) error {
	var nft_req index.NFTItemRequest
	var lim_req index.LimitRequest

	if err := c.QueryParser(&nft_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	res, book, err := pool.QueryNFTItems(nft_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(res) == 0 {
	// 	return index.IndexError{Code: 404, Message: "nft items not found"}
	// }
	resp := index.NFTItemsResponse{Items: res, AddressBook: book}
	return c.JSON(resp)
}

// @summary Get NFT Transfers
//
// @description Get transfers of NFT items by specified filters
//
// @id api_v3_get_nft_transfers
// @tags nfts
// @Accept json
// @Produce json
// @success 200 {object} index.NFTTransfersResponse
// @failure 400 {object} index.RequestError
// @param owner_address query []string false "Address of NFT owner in any form. Max 1000" collectionFormat(multi)
// @param item_address query []string false "Address of NFT item in any form. Max: 1000." collectionFormat(multi)
// @param collection_address query string false "Collection address in any form."
// @param direction query string false "Direction of transfer." Enums(in, out)
// @param start_utime query int32 false "Query transactions with generation UTC timestamp **after** given timestamp." minimum(0)
// @param end_utime query int32 false "Query transactions with generation UTC timestamp **before** given timestamp." minimum(0)
// @param start_lt query int64 false "Query transactions with `lt >= start_lt`." minimum(0)
// @param end_lt query int64 false "Query transactions with `lt <= end_lt`." minimum(0)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// @param sort query string false "Sort transactions by lt." Enums(asc, desc) default(desc)
// @router /api/v3/nft/transfers [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetNFTTransfers(c *fiber.Ctx) error {
	transfer_req := index.NFTTransferRequest{}
	utime_req := index.UtimeRequest{}
	lt_req := index.LtRequest{}
	lim_req := index.LimitRequest{}

	if err := c.QueryParser(&transfer_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if addr_str := c.Query("address"); len(addr_str) > 0 && transfer_req.OwnerAddress == nil {
		addr_val := index.AccountAddressConverter(addr_str)
		if addr_val.IsValid() {
			if addr, ok := addr_val.Interface().(index.AccountAddress); ok {
				transfer_req.OwnerAddress = []index.AccountAddress{addr}
			}
		}
	}
	if transfer_req.Direction != nil && *transfer_req.Direction == "both" {
		transfer_req.Direction = nil
	}
	if err := c.QueryParser(&utime_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lt_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	res, book, err := pool.QueryNFTTransfers(transfer_req, utime_req, lt_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(res) == 0 {
	// 	return index.IndexError{Code: 404, Message: "nft transfers not found"}
	// }

	resp := index.NFTTransfersResponse{Transfers: res, AddressBook: book}
	return c.JSON(resp)
}

// @summary Get Top Accounts By Balance
// @description Get list of accounts sorted descending by balance.
// @id api_v3_get_top_accounts_by_balance
// @tags stats
// @Accept       json
// @Produce      json
// @success		200	{object}	[]index.AccountBalance
// @failure		400	{object}	index.RequestError
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// @router			/api/v3/topAccountsByBalance [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetTopAccountsByBalance(c *fiber.Ctx) error {
	lim_req := index.LimitRequest{}

	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	res, err := pool.QueryTopAccountBalances(lim_req, settings.Request)
	if err != nil {
		return err
	}
	return c.JSON(res)
}

// @summary Get Jetton Masters
//
// @description Get Jetton masters by specified filters
//
// @id api_v3_get_jetton_masters
// @tags jettons
// @Accept json
// @Produce json
// @success 200 {object} index.JettonMastersResponse
// @failure 400 {object} index.RequestError
// @param address query []string false "Jetton Master address in any form. Max: 1024." collectionFormat(multi)
// @param admin_address query []string false "Address of Jetton Master's admin in any form. Max: 1024." collectionFormat(multi)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// @router /api/v3/jetton/masters [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetJettonMasters(c *fiber.Ctx) error {
	var jetton_req index.JettonMasterRequest
	var lim_req index.LimitRequest

	if err := c.QueryParser(&jetton_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	res, book, err := pool.QueryJettonMasters(jetton_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(res) == 0 {
	// 	return index.IndexError{Code: 404, Message: "jetton masters not found"}
	// }
	resp := index.JettonMastersResponse{Masters: res, AddressBook: book}
	return c.JSON(resp)
}

// @summary Get Jetton Wallets
//
// @description Get Jetton wallets by specified filters
//
// @id api_v3_get_jetton_wallets
// @tags jettons
// @Accept json
// @Produce json
// @success 200 {object} index.JettonWalletsResponse
// @failure 400 {object} index.RequestError
// @param address query []string false "Jetton wallet address in any form. Max: 1000." collectionFormat(multi)
// @param owner_address query []string false "Address of Jetton wallet's owner in any form. Max: 1000." collectionFormat(multi)
// @param jetton_address query string false "Jetton Master in any form."
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// @router /api/v3/jetton/wallets [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetJettonWallets(c *fiber.Ctx) error {
	var jetton_req index.JettonWalletRequest
	var lim_req index.LimitRequest

	if err := c.QueryParser(&jetton_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	res, book, err := pool.QueryJettonWallets(jetton_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(res) == 0 {
	// 	return index.IndexError{Code: 404, Message: "jetton wallets not found"}
	// }
	resp := index.JettonWalletsResponse{Wallets: res, AddressBook: book}
	return c.JSON(resp)
}

// @summary Get Jetton Transfers
//
// @description Get Jetton transfers by specified filters
//
// @id api_v3_get_jetton_transfers
// @tags jettons
// @Accept json
// @Produce json
// @success 200 {object} index.JettonTransfersResponse
// @failure 400 {object} index.RequestError
// @param address query []string false "Address of jetton wallet owner in any form. Max 1000" collectionFormat(multi)
// @param jetton_wallet query []string false "Jetton wallet address in any form. Max: 1000." collectionFormat(multi)
// @param jetton_master query string false "Jetton master address in any form."
// @param direction query string false "Direction of transfer." Enums(in, out)
// @param start_utime query int32 false "Query transactions with generation UTC timestamp **after** given timestamp." minimum(0)
// @param end_utime query int32 false "Query transactions with generation UTC timestamp **before** given timestamp." minimum(0)
// @param start_lt query int64 false "Query transactions with `lt >= start_lt`." minimum(0)
// @param end_lt query int64 false "Query transactions with `lt <= end_lt`." minimum(0)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// @param sort query string false "Sort transactions by lt." Enums(asc, desc) default(desc)
// @router /api/v3/jetton/transfers [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetJettonTransfers(c *fiber.Ctx) error {
	transfer_req := index.JettonTransferRequest{}
	utime_req := index.UtimeRequest{}
	lt_req := index.LtRequest{}
	lim_req := index.LimitRequest{}

	if err := c.QueryParser(&transfer_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if addr_str := c.Query("address"); len(addr_str) > 0 && transfer_req.OwnerAddress == nil {
		addr_val := index.AccountAddressConverter(addr_str)
		if addr_val.IsValid() {
			if addr, ok := addr_val.Interface().(index.AccountAddress); ok {
				transfer_req.OwnerAddress = []index.AccountAddress{addr}
			}
		}
	}
	if transfer_req.Direction != nil && *transfer_req.Direction == "both" {
		transfer_req.Direction = nil
	}
	if err := c.QueryParser(&utime_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lt_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	res, book, err := pool.QueryJettonTransfers(transfer_req, utime_req, lt_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(res) == 0 {
	// 	return index.IndexError{Code: 404, Message: "jetton transfers not found"}
	// }

	resp := index.JettonTransfersResponse{Transfers: res, AddressBook: book}
	return c.JSON(resp)
}

// @summary Get Jetton Burns
//
// @description Get Jetton burns by specified filters
//
// @id api_v3_get_jetton_burns
// @tags jettons
// @Accept json
// @Produce json
// @success 200 {object} index.JettonBurnsResponse
// @failure 400 {object} index.RequestError
// @param address query []string false "Address of jetton wallet owner in any form. Max 1000" collectionFormat(multi)
// @param jetton_wallet query []string false "Jetton wallet address in any form. Max: 1000." collectionFormat(multi)
// @param jetton_master query string false "Jetton master address in any form."
// @param start_utime query int32 false "Query transactions with generation UTC timestamp **after** given timestamp." minimum(0)
// @param end_utime query int32 false "Query transactions with generation UTC timestamp **before** given timestamp." minimum(0)
// @param start_lt query int64 false "Query transactions with `lt >= start_lt`." minimum(0)
// @param end_lt query int64 false "Query transactions with `lt <= end_lt`." minimum(0)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// @param sort query string false "Sort transactions by lt." Enums(asc, desc) default(desc)
// @router /api/v3/jetton/burns [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetJettonBurns(c *fiber.Ctx) error {
	burn_req := index.JettonBurnRequest{}
	utime_req := index.UtimeRequest{}
	lt_req := index.LtRequest{}
	lim_req := index.LimitRequest{}

	if err := c.QueryParser(&burn_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if addr_str := c.Query("address"); len(addr_str) > 0 && burn_req.OwnerAddress == nil {
		addr_val := index.AccountAddressConverter(addr_str)
		if addr_val.IsValid() {
			if addr, ok := addr_val.Interface().(index.AccountAddress); ok {
				burn_req.OwnerAddress = []index.AccountAddress{addr}
			}
		}
	}
	if err := c.QueryParser(&utime_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lt_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	res, book, err := pool.QueryJettonBurns(burn_req, utime_req, lt_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(res) == 0 {
	// 	return index.IndexError{Code: 404, Message: "jetton burns not found"}
	// }

	resp := index.JettonBurnsResponse{Burns: res, AddressBook: book}
	return c.JSON(resp)
}

// @summary Get Events
// @description Get events by specified filter.
// @id api_v3_get_events
// @tags events
// @Accept       json
// @Produce      json
// @success		200	{object}	index.EventsResponse
// @failure		400	{object}	index.RequestError
// @param account query string false "List of account addresses to get transactions. Can be sent in hex, base64 or base64url form."
// @param tx_hash query []string false "Find event by transaction hash."
// @param msg_hash query []string false "Find event by message hash."
// @param start_utime query int32 false "Query events, which was finished **after** given timestamp." minimum(0)
// @param end_utime query int32 false "Query events, which was finished **before** given timestamp." minimum(0)
// @param start_lt query int64 false "Query events with `end_lt >= start_lt`." minimum(0)
// @param end_lt query int64 false "Query events with `end_lt <= end_lt`." minimum(0)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(1000) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// @param sort query string false "Sort events by lt." Enums(asc, desc) default(desc)
// @router			/api/v3/events [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetEvents(c *fiber.Ctx) error {
	event_req := index.EventRequest{}
	utime_req := index.UtimeRequest{}
	lt_req := index.LtRequest{}
	lim_req := index.LimitRequest{}

	if err := c.QueryParser(&event_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&utime_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lt_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	if !onlyOneOf(event_req.AccountAddress != nil, event_req.TraceId != nil, len(event_req.TransactionHash) > 0, len(event_req.MessageHash) > 0) {
		return index.IndexError{Code: 422, Message: "only one of account, trace_id, tx_hash, msg_hash should be specified"}
	}

	res, book, err := pool.QueryEvents(event_req, utime_req, lt_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(txs) == 0 {
	// 	return index.IndexError{Code: 404, Message: "transactions not found"}
	// }

	txs_resp := index.EventsResponse{Events: res, AddressBook: book}
	return c.JSON(txs_resp)
}

// @summary Get Actions
// @description Get actions by specified filter.
// @id api_v3_get_actions
// @tags events
// @Accept       json
// @Produce      json
// @success		200	{object}	index.ActionsResponse
// @failure		400	{object}	index.RequestError
// @param action_id	query []string false "Find actions by the action_id." collectionFormat(multi)
// @param trace_id	query []string false "Find actions by the trace_id." collectionFormat(multi)
// @router			/api/v3/actions [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetActions(c *fiber.Ctx) error {
	var act_req index.ActionRequest
	var lim_req index.LimitRequest

	if err := c.QueryParser(&act_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	if err := c.QueryParser(&lim_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	res, book, err := pool.QueryActions(act_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	// if len(res) == 0 {
	// 	return index.IndexError{Code: 404, Message: "actions not found"}
	// }

	resp := index.ActionsResponse{Actions: res, AddressBook: book}
	return c.Status(200).JSON(resp)
}

// @summary Get Wallet Information
//
// @description Get wallet smart contract information. The following wallets are supported: `v1r1`, `v1r2`, `v1r3`, `v2r1`, `v2r2`, `v3r1`, `v3r2`, `v4r1`, `v4r2`, `v5beta`, `v5r1`. In case the account is not a wallet error code 409 is returned.
//
// @id api_v3_get_wallet_information
// @tags api/v2
// @Accept json
// @Produce json
// @success 200 {object} index.V2WalletInformation
// @failure 400 {object} index.RequestError
// @param address query string true "Account address in any form."
// @param use_v2 query bool false "Use method from api/v2. Not recommended" default(true)
// @router /api/v3/walletInformation [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetV2WalletInformation(c *fiber.Ctx) error {
	var acc_req index.V2AccountRequest
	if err := c.QueryParser(&acc_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	if len(acc_req.AccountAddress) == 0 {
		return index.IndexError{Code: 401, Message: "address of account is required"}
	}

	use_v2 := false
	use_fallback := false
	if acc_req.UseV2 != nil {
		use_v2 = *acc_req.UseV2
	}
	var res *index.V2WalletInformation
	if !use_v2 {
		account_req := index.AccountRequest{AccountAddress: []index.AccountAddress{acc_req.AccountAddress}}
		loc, _, err := pool.QueryWalletStates(account_req, index.LimitRequest{}, settings.Request)
		if err != nil {
			return err
		}
		if len(loc) == 0 {
			res = new(index.V2WalletInformation)
			res.Balance = "0"
			res.LastTransactionHash = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
			res.LastTransactionLt = "0"
			res.Status = "uninitialized"
		} else {
			info, err := index.WalletInformationFromV3(loc[0])
			if err != nil {
				use_fallback = true
				// return err
			} else if info == nil {
				use_fallback = true
				// return index.IndexError{Code: 409, Message: "not a wallet"}
			} else {
				res = info
			}
		}
	}

	if use_v2 || use_fallback {
		loc, err := index.GetV2WalletInformation(acc_req, settings.Request)
		if err != nil {
			return err
		}
		res = loc
	}

	return c.Status(200).JSON(res)
}

// @summary Get Address Information
//
// @description Get smart contract information.
//
// @id api_v3_get_v2_addressInformation
// @tags api/v2
// @Accept json
// @Produce json
// @success 200 {object} index.V2AddressInformation
// @failure 400 {object} index.RequestError
// @param address query string true "Account address in any form."
// @param use_v2 query bool false "Use method from api/v2. Not recommended" default(true)
// @router /api/v3/addressInformation [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetV2AddressInformation(c *fiber.Ctx) error {
	var acc_req index.V2AccountRequest
	if err := c.QueryParser(&acc_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if acc_req.UseV2 == nil {
		acc_req.UseV2 = new(bool)
		*acc_req.UseV2 = true
	}
	if len(acc_req.AccountAddress) == 0 {
		return index.IndexError{Code: 401, Message: "address of account is required"}
	}

	var res *index.V2AddressInformation
	if acc_req.UseV2 == nil || *acc_req.UseV2 {
		loc, err := index.GetV2AddressInformation(acc_req, settings.Request)
		if err != nil {
			return err
		}
		res = loc
	} else {
		account_req := index.AccountRequest{AccountAddress: []index.AccountAddress{acc_req.AccountAddress}}
		loc, _, err := pool.QueryAccountStates(account_req, index.LimitRequest{}, settings.Request)
		if err != nil {
			return err
		}
		if len(loc) == 0 {
			res = new(index.V2AddressInformation)
			res.Balance = "0"
			res.LastTransactionHash = new(string)
			*res.LastTransactionHash = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
			res.LastTransactionLt = new(string)
			*res.LastTransactionLt = "0"
			res.Status = "uninitialized"
		} else {
			info, err := index.AddressInformationFromV3(loc[0])
			if err != nil {
				return err
			}
			res = info
		}
	}

	return c.Status(200).JSON(res)
}

// @summary Send Message
//
// @description Send an external message to the TON network.
//
// @id api_v3_post_v2_message
// @tags api/v2
// @Accept json
// @Produce json
// @success 200 {object} index.V2SendMessageResult
// @failure 400 {object} index.RequestError
// @param boc body index.V2SendMessageRequest true "Message in boc base64 format."
// @router /api/v3/message [post]
// @security		APIKeyHeader
// @security		APIKeyQuery
func PostV2SendMessage(c *fiber.Ctx) error {
	var req index.V2SendMessageRequest
	if err := c.BodyParser(&req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if len(req.BOC) == 0 {
		return index.IndexError{Code: 401, Message: "boc is required"}
	}

	res, err := index.PostMessage(req, settings.Request)
	if err != nil {
		return err
	}

	return c.Status(200).JSON(res)
}

// @summary Estimate Fee
//
// @description Estimate fees required for query processing. Fields body, init-code and init-data accepted in serialized format (b64-encoded).
//
// @id api_v3_post_v2_estimate_fee
// @tags api/v2
// @Accept json
// @Produce json
// @success 200 {object} index.V2EstimateFeeResult
// @failure 400 {object} index.RequestError
// @param request body index.V2EstimateFeeRequest true "Estimate fee request."
// @router /api/v3/estimateFee [post]
// @security		APIKeyHeader
// @security		APIKeyQuery
func PostV2EstimateFee(c *fiber.Ctx) error {
	var req index.V2EstimateFeeRequest
	if err := c.BodyParser(&req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}

	res, err := index.PostEstimateFee(req, settings.Request)
	if err != nil {
		return err
	}

	return c.Status(200).JSON(res)
}

// @summary Run Get-Method
// @description Run get method of smart contract. Stack supports only `num`, `cell` and `slice` types:
// @description ```
// @description [
// @description 	{
// @description 		"type": "num",
// @description 		"value": "0x12a"
// @description 	},
// @description 	{
// @description 		"type": "cell",
// @description 		"value": "te6..." // base64 encoded boc with cell
// @description 	},
// @description 	{
// @description 		"type": "slice",
// @description 		"value": "te6..." // base64 encoded boc with slice
// @description 	}
// @description ]
// @description ```
//
// @id api_v3_post_v2_rungetmethod
// @tags api/v2
// @Accept json
// @Produce json
// @success 200 {object} index.V2RunGetMethodRequest
// @failure 400 {object} index.RequestError
// @param request body index.V2RunGetMethodRequest true "Run Get-method request"
// @router /api/v3/runGetMethod [post]
// @security		APIKeyHeader
// @security		APIKeyQuery
func PostV2RunGetMethod(c *fiber.Ctx) error {
	var req index.V2RunGetMethodRequest
	if err := c.BodyParser(&req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	if len(req.Address) == 0 {
		return index.IndexError{Code: 401, Message: "address is required"}
	}

	if len(req.Method) == 0 {
		return index.IndexError{Code: 401, Message: "method is required"}
	}

	res, err := index.PostRunGetMethod(req, settings.Request)
	if err != nil {
		return err
	}

	return c.Status(200).JSON(res)
}

// // @summary Test method
// //
// //	@description Test method
// //
// // @id api_v3_get_test_method
// // @tags _debug
// // @Accept       json
// // @Produce      json
// // @success		200	{object}	index.MessagesResponse
// // @failure		400	{object}	index.RequestError
// // @param my_hash query []string false "Hash" collectionFormat(multi)
// // @param my_addr query []string false "Address" collectionFormat(multi)
// // @param my_shard query []string false "ShardId" collectionFormat(multi)
// // @router			/api/v3/__testMethod [get]
// // @security		APIKeyHeader
// // @security		APIKeyQuery
func GetTestMethod(c *fiber.Ctx) error {
	var test_req index.TestRequest
	if err := c.QueryParser(&test_req); err != nil {
		return index.IndexError{Code: 422, Message: err.Error()}
	}
	return c.Status(200).JSON(test_req)
}

func HealthCheck(c *fiber.Ctx) error {
	return c.Status(200).SendString("OK")
}

func ExtractParam(ctx *fiber.Ctx, header string, query string) (string, bool) {
	result := ``
	found := false
	if val := ctx.GetReqHeaders()[header]; len(val) > 0 {
		result = val[0]
		found = true
	}
	if val, ok := ctx.Queries()[query]; len(query) > 0 && ok {
		result = val
		found = true
	}
	return result, found
}

func ErrorHandlerFunc(ctx *fiber.Ctx, err error) error {
	api_key, _ := ExtractParam(ctx, "X-Api-Key", "api_key")
	ip := ctx.IP()
	if ips := ctx.IPs(); len(ips) > 0 {
		ip = ips[0]
	}

	switch e := err.(type) {
	case index.IndexError:
		if e.Code != 404 {
			log.Printf("Code: %d Path: %s IP: %s API Key: %s Queries: %v Body: %s Error: %s",
				e.Code, ctx.Path(), ip, api_key, ctx.Queries(), string(ctx.Body()), err.Error())
		}
		return ctx.Status(e.Code).JSON(e)
	default:
		log.Printf("Path: %s IP: %s API Key: %s Queries: %v Body: %s Error: %s", ctx.Path(), ip, api_key, ctx.Queries(), string(ctx.Body()), err.Error())
		resp := map[string]string{}
		resp["error"] = fmt.Sprintf("internal server error: %s", err.Error())
		return ctx.Status(fiber.StatusInternalServerError).JSON(resp)
	}
}

func test() {
	// addr_str := "0QAvlUF6KtTT7R9/kmxOPULEMd+zbtVBZigkorOlGqWtzVky"
	// addr, err := address.ParseAddr(addr_str)
	// log.Println(addr, err)
}

func main() {
	test()
	var timeout_ms int

	flag.StringVar(&settings.PgDsn, "pg", "postgresql://localhost:5432", "PostgreSQL connection string")
	flag.StringVar(&settings.Request.V2Endpoint, "v2", "", "TON HTTP API endpoint for proxied methods")
	flag.StringVar(&settings.Request.V2ApiKey, "v2-apikey", "", "API key for TON HTTP API endpoint")
	flag.IntVar(&settings.MaxConns, "maxconns", 100, "PostgreSQL max connections")
	flag.IntVar(&settings.MinConns, "minconns", 0, "PostgreSQL min connections")
	flag.StringVar(&settings.Bind, "bind", ":8000", "Bind address")
	flag.StringVar(&settings.InstanceName, "name", "Go", "Instance name to show in Swagger UI")
	flag.BoolVar(&settings.Prefork, "prefork", false, "Prefork workers")
	flag.BoolVar(&settings.Request.IsTestnet, "testnet", false, "Use testnet address book")
	flag.BoolVar(&settings.Debug, "debug", false, "Run service in debug mode")
	flag.IntVar(&timeout_ms, "query-timeout", 3000, "Query timeout in milliseconds")
	flag.IntVar(&settings.Request.DefaultLimit, "default-limit", 100, "Default value for limit")
	flag.IntVar(&settings.Request.MaxLimit, "max-limit", 1000, "Maximum value for limit")
	settings.Request.Timeout = time.Duration(timeout_ms) * time.Millisecond
	flag.Parse()

	var err error
	pool, err = index.NewDbClient(settings.PgDsn, settings.MaxConns, settings.MinConns)
	if err != nil {
		log.Fatal(err)
		os.Exit(63)
	}
	// web server
	config := fiber.Config{
		AppName:      "TON Index API",
		Concurrency:  256 * 1024,
		Prefork:      settings.Prefork,
		ErrorHandler: ErrorHandlerFunc,
	}
	app := fiber.New(config)

	// converters
	fiber.SetParserDecoder(fiber.ParserConfig{
		IgnoreUnknownKeys: true,
		ParserType: []fiber.ParserType{
			{Customtype: index.HashType(""), Converter: index.HashConverter},
			{Customtype: index.AccountAddress(""), Converter: index.AccountAddressConverter},
			{Customtype: index.AccountAddressNullable(""), Converter: index.AccountAddressNullableConverter},
			{Customtype: index.ShardId(0), Converter: index.ShardIdConverter},
			{Customtype: index.OpcodeType(0), Converter: index.OpcodeTypeConverter},
			{Customtype: index.UtimeType(0), Converter: index.UtimeTypeConverter},
		},
		ZeroEmpty: true,
	})

	// endpoints
	app.Use("/api/v3/", func(c *fiber.Ctx) error {
		c.Accepts("application/json")
		start := time.Now()
		err := c.Next()
		stop := time.Now()
		c.Append("Server-timing", fmt.Sprintf("app;dur=%v", stop.Sub(start).String()))
		return err
	})
	if settings.Debug {
		app.Use(pprof.New())
	}
	// app.Use("/api/v3", logger.New(logger.Config{Format: "[${ip}]:${port} ${status} - ${method} ${path}\n"}))

	// healthcheck
	app.Get("/healthcheck", HealthCheck)
	// app.Get("/metrics", monitor.New(monitor.Config{Title: "TON Index API monitor"}))

	// masterchain info
	app.Get("/api/v3/masterchainInfo", GetMasterchainInfo)
	app.Get("/api/v3/masterchainBlockShardState", GetShards)
	app.Get("/api/v3/masterchainBlockShards", GetShardsDiff)

	// blocks
	app.Get("/api/v3/blocks", GetBlocks)

	// transactions
	app.Get("/api/v3/transactions", GetTransactions)
	app.Get("/api/v3/transactionsByMasterchainBlock", GetTransactionsByMasterchainBlock)
	app.Get("/api/v3/transactionsByMessage", GetTransactionsByMessage)
	app.Get("/api/v3/adjacentTransactions", GetAdjacentTransactions)

	// messages
	app.Get("/api/v3/messages", GetMessages)

	// stats
	app.Get("/api/v3/topAccountsByBalance", GetTopAccountsByBalance)

	// account methods
	app.Get("/api/v3/addressBook", GetAddressBook)
	app.Get("/api/v3/accountStates", GetAccountStates)
	app.Get("/api/v3/walletStates", GetWalletStates)

	// nfts
	app.Get("/api/v3/nft/collections", GetNFTCollections)
	app.Get("/api/v3/nft/items", GetNFTItems)
	app.Get("/api/v3/nft/transfers", GetNFTTransfers)

	// jettons
	app.Get("/api/v3/jetton/masters", GetJettonMasters)
	app.Get("/api/v3/jetton/wallets", GetJettonWallets)
	app.Get("/api/v3/jetton/transfers", GetJettonTransfers)
	app.Get("/api/v3/jetton/burns", GetJettonBurns)

	// events
	app.Get("/api/v3/actions", GetActions)
	app.Get("/api/v3/events", GetEvents)

	// api/v2 proxied
	app.Get("/api/v3/addressInformation", GetV2AddressInformation)
	app.Get("/api/v3/account", GetV2AddressInformation)
	app.Get("/api/v3/walletInformation", GetV2WalletInformation)
	app.Get("/api/v3/wallet", GetV2WalletInformation)
	app.Post("/api/v3/message", PostV2SendMessage)
	app.Post("/api/v3/runGetMethod", PostV2RunGetMethod)
	app.Post("/api/v3/estimateFee", PostV2EstimateFee)

	// test
	app.Get("/api/v3/__testMethod", GetTestMethod)

	// swagger
	var swagger_config = swagger.Config{
		Title:           "TON Index (" + settings.InstanceName + ") - Swagger UI",
		Layout:          "BaseLayout",
		DeepLinking:     true,
		TryItOutEnabled: true,
	}
	app.Get("/api/v3/*", swagger.New(swagger_config))
	app.Static("/", "./static")
	err = app.Listen(settings.Bind)
	log.Fatal(err)
}
