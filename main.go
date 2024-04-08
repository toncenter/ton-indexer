package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/swagger"
	_ "github.com/kdimentionaltree/ton-index-go/docs"
	"github.com/kdimentionaltree/ton-index-go/index"
)

type Settings struct {
	PgDsn   string
	Bind    string
	Prefork bool
	Request index.RequestSettings
}

var pool *index.DbClient
var settings Settings

//	@title			TON Index (Go)
//	@version		1.1.0
//	@description	TON Index collects data from a full node to PostgreSQL database and provides convenient API to an indexed blockchain.

//	@securitydefinitions.apikey APIKeyHeader
//	@in		header
//	@name	X-Api-Key
//	@securitydefinitions.apikey APIKeyQuery
//	@in		query
//	@name	api_key

// @summary		Get Masterchain Info
// @description	Get first and last indexed block
// @id	api_v3_get_masterchain_info
// @tags	default
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
// @tags default
// @Accept       json
// @Produce      json
// @success		200	{object}	index.BlocksResponse
// @failure		400	{object}	index.RequestError
// @param	workchain query int32 false "Block workchain."
// @param	shard query int64 false "Block shard id. Must be sent with *workchain*. Example: `8000000000000000`."
// @param	seqno query int32 false "Block block seqno. Must be sent with *workchain* and *shard*."
// @param	mc_seqno query int32 false "Masterchain block seqno"
// @param start_utime query int32 false "Query blocks with generation UTC timestamp **after** given timestamp." minimum(0)
// @param end_utime query int32 false "Query blocks with generation UTC timestamp **before** given timestamp." minimum(0)
// @param start_lt query int64 false "Query blocks with `lt >= start_lt`." minimum(0)
// @param end_lt query int64 false "Query blocks with `lt <= end_lt`." minimum(0)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(500) default(10)
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
		return err
	}
	if err := c.QueryParser(&utime_req); err != nil {
		return err
	}
	if err := c.QueryParser(&lt_req); err != nil {
		return err
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return err
	}

	blks, err := pool.QueryBlocks(blk_req, utime_req, lt_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	if len(blks) == 0 {
		return c.Status(fiber.StatusNotFound).JSON(index.RequestError{Message: "Blocks not found", Code: fiber.StatusNotFound})
	}

	blk_resp := index.BlocksResponse{Blocks: blks}
	return c.JSON(blk_resp)
}

// @summary Get masterchain block shard state
// @description Get masterchain block shard state. Same as /api/v2/shards.
// @id api_v3_get_masterchainBlockShardState
// @tags default
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
		return c.Status(fiber.StatusNotFound).JSON(index.RequestError{Message: "Blocks not found", Code: fiber.StatusNotFound})
	}

	blk_resp := index.BlocksResponse{Blocks: blks}
	return c.JSON(blk_resp)
}

// @summary Get masterchain block shard state
//
//	@description Returns all worchain blocks, that appeared after previous masterchain block. \
//		\
//		 **Note:** this method is not equivalent with [/api/v2/shards](https://toncenter.com/api/v2/#/blocks/get_shards_shards_get).
//
// @id api_v3_get_masterchainBlockShards
// @tags default
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
		return c.Status(fiber.StatusNotFound).JSON(index.RequestError{Message: "Blocks not found", Code: fiber.StatusNotFound})
	}

	blk_resp := index.BlocksResponse{Blocks: blks}
	return c.JSON(blk_resp)
}

// @summary Get transactions
// @description Get transactions by specified filter.
// @id api_v3_get_transactions
// @tags default
// @Accept       json
// @Produce      json
// @success		200	{object}	index.TransactionsResponse
// @failure		400	{object}	index.RequestError
// @param	workchain query int32 false "Block workchain."
// @param	shard query int64 false "Block shard id. Must be sent with *workchain*. Example: `8000000000000000`."
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
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(500) default(10)
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
		return err
	}
	if err := c.QueryParser(&tx_req); err != nil {
		return err
	}
	if err := c.QueryParser(&utime_req); err != nil {
		return err
	}
	if err := c.QueryParser(&lt_req); err != nil {
		return err
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return err
	}

	txs, err := pool.QueryTransactions(
		blk_req, tx_req, index.MessageRequest{},
		utime_req, lt_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	if len(txs) == 0 {
		return c.Status(fiber.StatusNotFound).JSON(index.RequestError{Message: "Transactions not found", Code: fiber.StatusNotFound})
	}

	txs_resp := index.TransactionsResponse{Transactions: txs}
	return c.JSON(txs_resp)
}

// @summary Get transactions by Masterchain block
// @description Returns transactions from masterchain block and from all shards.
// @id api_v3_get_transactions_by_masterchain_block
// @tags default
// @Accept       json
// @Produce      json
// @success		200	{object}	index.TransactionsResponse
// @failure		400	{object}	index.RequestError
// @param	seqno query int32 true "Masterchain block seqno."
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(500) default(10)
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
		return err
	}

	txs, err := pool.QueryTransactions(
		blk_req, index.TransactionRequest{}, index.MessageRequest{}, index.UtimeRequest{},
		index.LtRequest{}, lim_req, settings.Request)
	if err != nil {
		return err
	}
	if len(txs) == 0 {
		return c.Status(fiber.StatusNotFound).JSON(index.RequestError{Message: "Transactions not found", Code: fiber.StatusNotFound})
	}

	txs_resp := index.TransactionsResponse{Transactions: txs}
	return c.JSON(txs_resp)
}

// @summary Get transactions by message
//
//	@description Get transactions whose inbound/outbound message has the specified hash. \
//	This endpoint returns list of Transaction objectssince collisions of message hashes can occur.
//
// @id api_v3_get_transactions_by_message
// @tags default
// @Accept       json
// @Produce      json
// @success		200	{object}	index.TransactionsResponse
// @failure		400	{object}	index.RequestError
// @param msg_hash query string false "Message hash. Acceptable in hex, base64 and base64url forms."
// @param body_hash query string false "Hash of message body."
// @param direction query string false "Direction of message." Enums(in, out)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(500) default(10)
// @param offset query int32 false "Skip first N rows. Use with *limit* to batch read." minimum(0) default(0)
// // @param sort query string false "Sort transactions by lt." Enums(asc, desc) default(desc)
// @router			/api/v3/transactionsByMessage [get]
// @security		APIKeyHeader
// @security		APIKeyQuery
func GetTransactionsByMessage(c *fiber.Ctx) error {
	msg_req := index.MessageRequest{}
	lim_req := index.LimitRequest{}

	if err := c.QueryParser(&msg_req); err != nil {
		return err
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return err
	}
	if msg_req.BodyHash == nil && msg_req.MessageHash == nil {
		return c.Status(fiber.StatusBadRequest).JSON(index.RequestError{Message: "Either msg_hash or body_hash should be specified", Code: fiber.StatusBadRequest})
	}

	txs, err := pool.QueryTransactions(
		index.BlockRequest{}, index.TransactionRequest{}, msg_req,
		index.UtimeRequest{}, index.LtRequest{}, lim_req, settings.Request)
	if err != nil {
		return err
	}
	if len(txs) == 0 {
		return c.Status(fiber.StatusNotFound).JSON(index.RequestError{Message: "Transactions not found", Code: fiber.StatusNotFound})
	}

	txs_resp := index.TransactionsResponse{Transactions: txs}
	return c.JSON(txs_resp)
}

// @summary Get messages
//
//	@description Get messages by specified filters.
//
// @id api_v3_get_messages
// @tags default
// @Accept       json
// @Produce      json
// @success		200	{object}	index.MessagesResponse
// @failure		400	{object}	index.RequestError
// @param msg_hash query string false "Message hash. Acceptable in hex, base64 and base64url forms."
// @param body_hash query string false "Hash of message body."
// @param source query string false "The source account address. Can be sent in hex, base64 or base64url form. Use value `null` to get external messages."
// @param destination query string false "The destination account address. Can be sent in hex, base64 or base64url form. Use value `null` to get log messages."
// @param direction query string false "Direction of message." Enums(in, out)
// @param limit query int32 false "Limit number of queried rows. Use with *offset* to batch read." minimum(1) maximum(500) default(10)
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

	hash := c.Query("hash")
	if err := c.QueryParser(&msg_req); err != nil {
		return err
	}
	if len(hash) > 0 && msg_req.MessageHash == nil {
		msg_req.MessageHash = []string{hash}
	}
	if err := c.QueryParser(&utime_req); err != nil {
		return err
	}
	if err := c.QueryParser(&lt_req); err != nil {
		return err
	}
	if err := c.QueryParser(&lim_req); err != nil {
		return err
	}

	msgs, err := pool.QueryMessages(msg_req, utime_req, lt_req, lim_req, settings.Request)
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return c.Status(fiber.StatusNotFound).JSON(index.RequestError{Message: "Messages not found", Code: fiber.StatusNotFound})
	}

	msgs_resp := index.MessagesResponse{Messages: msgs}
	return c.JSON(msgs_resp)
}

func test() {
	log.Println("Test OK")
}

func main() {
	test()
	settings.Request.Timeout = 3000 * time.Millisecond

	flag.StringVar(&settings.PgDsn, "pg", "postgresql://localhost:5432", "PostgreSQL connection string")
	flag.StringVar(&settings.Bind, "bind", ":8000", "Bind address")
	flag.BoolVar(&settings.Prefork, "prefork", false, "Prefork workers")
	flag.Parse()

	var err error
	pool, err = index.NewDbClient(settings.PgDsn)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	// web server
	config := fiber.Config{
		AppName:     "TON Index API",
		Concurrency: 256 * 1024,
		Prefork:     settings.Prefork,
	}
	app := fiber.New(config)

	app.Use("/api/v3/", func(c *fiber.Ctx) error {
		c.Accepts("application/json")
		err := c.Next()
		if err != nil {
			log.Println("Error:", err)
			return c.Status(fiber.StatusInternalServerError).JSON(err.Error())
		}
		return nil
	})

	// masterchain info
	app.Get("/api/v3/masterchainInfo", GetMasterchainInfo)
	app.Get("/api/v3/masterchainBlockShardState", GetShards)

	// blocks
	app.Get("/api/v3/blocks", GetBlocks)

	// transactions
	app.Get("/api/v3/transactions", GetTransactions)
	app.Get("/api/v3/transactionsByMasterchainBlock", GetTransactionsByMasterchainBlock)
	app.Get("/api/v3/transactionsByMessage", GetTransactionsByMessage)

	// messages
	app.Get("/api/v3/messages", GetMessages)

	// swagger
	app.Get("/api/v3/*", swagger.New(swagger.Config{}))
	err = app.Listen(settings.Bind)
	log.Fatal(err)
}
