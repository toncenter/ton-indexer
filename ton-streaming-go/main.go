package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/swagger"
	"github.com/gofiber/websocket/v2"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"

	"github.com/toncenter/ton-indexer/ton-index-go/index"
	"github.com/toncenter/ton-indexer/ton-index-go/index/emulated"
	_ "github.com/toncenter/ton-indexer/ton-streaming-go/docs"
)

// Command-line flags
var (
	redisAddr               = flag.String("redis", "localhost:6379", "Redis server dsn")
	tracesChannel           = flag.String("traces-channel", "new_trace", "Redis channel for blockchain events")
	commitedTxsChannel      = flag.String("commited-txs-channel", "new_commited_txs", "Redis channel for committed transactions")
	classifiedTracesChannel = flag.String("classified-traces-channel", "classified_trace", "Redis channel for classified traces")
	serverPort              = flag.Int("port", 8085, "Server port")
	prefork                 = flag.Bool("prefork", false, "Use prefork")
	testnet                 = flag.Bool("testnet", false, "Use testnet")
	pg                      = flag.String("pg", "", "PostgreSQL connection string")
	imgProxyBaseUrl         = flag.String("imgproxy-baseurl", "", "Image proxy base URL")
)

// Subscription represents a client's subscription to blockchain events
type Subscription struct {
	Addresses          []string
	Types              []string
	IncludeAddressBook bool
	IncludeMetadata    bool
}

func (s *Subscription) InterestedInType(eventType string) bool {
	// Check if the client is subscribed to this event type
	for _, t := range s.Types {
		if t == eventType {
			return true
		}
	}
	return false
}

func (s *Subscription) InterestedIn(eventType string, eventAddresses []string) bool {
	// Check if the client is subscribed to this event type
	if !s.InterestedInType(eventType) {
		return false
	}

	// Check if the client is subscribed to this address
	for _, subsrcAddr := range s.Addresses {
		for _, eventAddr := range eventAddresses {
			if subsrcAddr == eventAddr {
				return true
			}
		}
	}

	return false
}

// SubscriptionRequest represents a subscription/unsubscription request
type SubscriptionRequest struct {
	Operation            string   `json:"operation"`
	Addresses            []string `json:"addresses"`
	Types                []string `json:"types"`
	SupportedActionTypes []string `json:"supported_action_types"`
	IncludeAddressBook   bool     `json:"include_address_book,omitempty"`
	IncludeMetadata      bool     `json:"include_metadata,omitempty"`
}

// BlockchainEvent represents an event from the blockchain
type BlockchainEvent struct {
	Type        string             `json:"type"`
	Data        any                `json:"data"`
	AddressBook *index.AddressBook `json:"address_book,omitempty"`
	Metadata    *index.Metadata    `json:"metadata,omitempty"`
}

// Client represents a connected client
type Client struct {
	ID           string
	Connected    bool
	Subscription *Subscription
	SendEvent    func([]byte) error
	sendChan     chan []byte
	mu           sync.Mutex
}

func (c *Client) startSender(manager *ClientManager) {
	go func() {
		for msg := range c.sendChan {
			c.mu.Lock()
			if !c.Connected {
				c.mu.Unlock()
				break
			}
			err := c.SendEvent(msg)
			c.mu.Unlock()
			if err != nil {
				manager.unregister <- c
				break
			}
		}
	}()
}

// ClientManager manages all connected clients
type ClientManager struct {
	clients    map[string]*Client
	register   chan *Client
	unregister chan *Client
	broadcast  chan Notification
	mu         sync.RWMutex
}

// NewClientManager creates a new client manager
func NewClientManager() *ClientManager {
	return &ClientManager{
		clients:    make(map[string]*Client),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan Notification),
	}
}

func (manager *ClientManager) shouldFetchAddressBookAndMetadata(eventType string, addressesToNotify []string) (bool, bool) {
	shouldFetchAddressBook := false
	shouldFetchMetadata := false

	for _, client := range manager.clients {
		client.mu.Lock()
		if client.Connected && client.Subscription != nil &&
			client.Subscription.InterestedIn(eventType, addressesToNotify) {
			shouldFetchAddressBook = shouldFetchAddressBook || client.Subscription.IncludeAddressBook
			shouldFetchMetadata = shouldFetchMetadata || client.Subscription.IncludeMetadata
		}
		client.mu.Unlock()
		if shouldFetchAddressBook && shouldFetchMetadata {
			break
		}
	}

	return shouldFetchAddressBook, shouldFetchMetadata
}

// Run starts the client manager
func (manager *ClientManager) Run() {
	for {
		select {
		case client := <-manager.register:
			manager.mu.Lock()
			client.sendChan = make(chan []byte, 1024*1024) // 1MB buffer
			manager.clients[client.ID] = client
			manager.mu.Unlock()
			client.startSender(manager)
			log.Printf("Client %s connected", client.ID)
		case client := <-manager.unregister:
			manager.mu.Lock()
			if _, ok := manager.clients[client.ID]; ok {
				delete(manager.clients, client.ID)
				log.Printf("Client %s disconnected", client.ID)
			}
			manager.mu.Unlock()
		case notification := <-manager.broadcast:
			manager.mu.RLock()
			for _, client := range manager.clients {
				client.mu.Lock()
				if client.Connected && client.Subscription != nil {
					if event := notification.AdjustForClient(client); event != nil {
						msgBytes, err := json.Marshal(event)
						if err != nil {
							log.Printf("Error marshalling event: %v", err)
							client.mu.Unlock()
							continue
						}
						select {
						case client.sendChan <- msgBytes:
						default:
							log.Printf("Client %s send buffer full, dropping event", client.ID)
						}
					}
				}
				client.mu.Unlock()
			}
			manager.mu.RUnlock()
		}
	}
}

// fetchAddressBookAndMetadata fetches address book and metadata for a list of addresses
func fetchAddressBookAndMetadata(ctx context.Context, addresses []string, includeAddressBook bool, includeMetadata bool) (*index.AddressBook, *index.Metadata) {
	var addressBook *index.AddressBook
	var metadata *index.Metadata

	if dbClient == nil {
		return nil, nil
	}

	conn, err := dbClient.Pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection: %v", err)
		return nil, nil
	}
	defer conn.Release()

	settings := index.RequestSettings{
		Timeout:   3 * time.Second,
		IsTestnet: *testnet,
	}

	if includeAddressBook {
		book, err := index.QueryAddressBookImpl(addresses, conn, settings)
		if err != nil {
			log.Printf("Error querying address book: %v", err)
		} else {
			addressBook = &book
		}
	}

	if includeMetadata {
		meta, err := index.QueryMetadataImpl(addresses, conn, settings)
		if err != nil {
			log.Printf("Error querying metadata: %v", err)
		} else {
			// Apply imgproxy base URL if provided
			if *imgProxyBaseUrl != "" {
				index.SubstituteImgproxyBaseUrl(&meta, *imgProxyBaseUrl)
			}
			metadata = &meta
		}
	}

	return addressBook, metadata
}

type Notification interface {
	AdjustForClient(client *Client) any
}

type ActionsNotification struct {
	Type                  string             `json:"type"`
	TraceExternalHashNorm string             `json:"trace_external_hash_norm"`
	Actions               []*index.Action    `json:"actions"`
	ActionAddresses       [][]string         `json:"-"`
	AddressBook           *index.AddressBook `json:"address_book,omitempty"`
	Metadata              *index.Metadata    `json:"metadata,omitempty"`
}

var _ Notification = (*ActionsNotification)(nil)

func (n *ActionsNotification) AdjustForClient(client *Client) any {
	if !client.Subscription.InterestedInType(n.Type) {
		return nil
	}

	var adjustedActions []*index.Action
	var adjustedActionAddresses [][]string
	var adjustedAddressBook *index.AddressBook
	var adjustedMetadata *index.Metadata
	if n.AddressBook != nil {
		adjustedAddressBook = &index.AddressBook{}
	}
	if n.Metadata != nil {
		adjustedMetadata = &index.Metadata{}
	}
	var allAddresses = map[string]bool{}
	for idx, action := range n.Actions {
		actionAddresses := n.ActionAddresses[idx]
		includeAction := false
		for _, addr := range actionAddresses {
			if slices.Contains(client.Subscription.Addresses, addr) {
				adjustedActions = append(adjustedActions, action)
				adjustedActionAddresses = append(adjustedActionAddresses, actionAddresses)
				includeAction = true
				break
			}
		}
		if includeAction {
			for _, addr := range actionAddresses {
				allAddresses[addr] = true
				if adjustedAddressBook != nil {
					if addrBookEntry, ok := (*n.AddressBook)[addr]; ok {
						(*adjustedAddressBook)[addr] = addrBookEntry
					}
				}
				if adjustedMetadata != nil {
					if metaEntry, ok := (*n.Metadata)[addr]; ok {
						(*adjustedMetadata)[addr] = metaEntry
					}
				}
			}
		}
	}
	if len(adjustedActions) == 0 {
		return nil
	}

	return &ActionsNotification{
		Type:                  n.Type,
		TraceExternalHashNorm: n.TraceExternalHashNorm,
		Actions:               adjustedActions,
		ActionAddresses:       adjustedActionAddresses,
		AddressBook:           adjustedAddressBook,
		Metadata:              adjustedMetadata,
	}
}

func SubscribeToClassifiedTraces(ctx context.Context, rdb *redis.Client, manager *ClientManager) {
	pubsub := rdb.Subscribe(ctx, *classifiedTracesChannel)
	defer pubsub.Close()

	log.Printf("Subscribed to Redis channel: %s", *classifiedTracesChannel)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("Error receiving message from Redis: %v", err)
			continue
		}

		traceExternalHashNorm := msg.Payload
		ProcessNewClassifiedTrace(ctx, rdb, traceExternalHashNorm, manager)
	}
}

func ProcessNewClassifiedTrace(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, manager *ClientManager) {
	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	raw_traces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("Error loading raw traces: %v", err)
		return
	}

	emulatedContext := index.NewEmptyContext(true)
	if err := emulatedContext.FillFromRawData(raw_traces); err != nil {
		log.Printf("Error filling context from raw data: %v", err)
		return
	}
	if emulatedContext.GetTraceCount() > 1 {
		log.Printf("More than 1 trace in the context")
		return
	}
	if emulatedContext.GetTraceCount() == 0 {
		log.Printf("No traces in the context")
		return
	}

	traceIsCommited := true
	for _, row := range emulatedContext.GetTransactions() {
		if tx, err := index.ScanTransaction(row); err == nil {
			if tx.Emulated {
				traceIsCommited = false
			}
		} else {
			log.Printf("Error scanning transaction: %v", err)
			traceIsCommited = false // if we can't scan the transaction, we can't assume trace is committed
		}
	}

	var actions = []*index.Action{}
	var actionsAddresses = [][]string{}
	for _, row := range emulatedContext.GetActions(index.ExpandActionTypeShortcuts([]string{"v1"})) {
		var rawAction *index.RawAction
		if loc, err := index.ScanRawAction(row); err == nil {
			rawAction = loc
		} else {
			log.Printf("Error scanning raw action: %v", err)
			continue
		}

		actionAddrMap := map[string]bool{}
		index.CollectAddressesFromAction(&actionAddrMap, rawAction)

		action, err := index.ParseRawAction(rawAction)
		if err != nil {
			log.Printf("Error parsing raw action: %v", err)
			continue
		}
		actionAddresses := []string{}
		for addr := range actionAddrMap {
			actionAddresses = append(actionAddresses, addr)
		}
		actions = append(actions, action)
		actionsAddresses = append(actionsAddresses, actionAddresses)
	}

	var addressBook *index.AddressBook
	var metadata *index.Metadata
	allAddresses := []string{}
	for _, actionAddr := range actionsAddresses {
		allAddresses = append(allAddresses, actionAddr...)
	}
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata("pending_actions", allAddresses)
	if shouldFetchAddressBook || shouldFetchMetadata {
		addressBook, metadata = fetchAddressBookAndMetadata(
			ctx,
			allAddresses,
			shouldFetchAddressBook,
			shouldFetchMetadata,
		)
	}

	manager.broadcast <- &ActionsNotification{
		Type:                  "pending_actions",
		TraceExternalHashNorm: traceExternalHashNorm,
		Actions:               actions,
		ActionAddresses:       actionsAddresses,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}

	if traceIsCommited {
		manager.broadcast <- &ActionsNotification{
			Type:                  "actions",
			TraceExternalHashNorm: traceExternalHashNorm,
			Actions:               actions,
			ActionAddresses:       actionsAddresses,
			AddressBook:           addressBook,
			Metadata:              metadata,
		}
	}
}

// SubscribeToTraces subscribes to blockchain events from Redis
func SubscribeToTraces(ctx context.Context, rdb *redis.Client, manager *ClientManager) {
	pubsub := rdb.Subscribe(ctx, *tracesChannel)
	defer pubsub.Close()

	log.Printf("Subscribed to Redis channel: %s", *tracesChannel)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("Error receiving message from Redis: %v", err)
			continue
		}

		traceExternalHashNorm := msg.Payload
		ProcessNewTrace(ctx, rdb, traceExternalHashNorm, manager)
	}
}

type TransactionsNotification struct {
	Type                  string              `json:"type"`
	TraceExternalHashNorm string              `json:"trace_external_hash_norm"`
	Transactions          []index.Transaction `json:"transactions"`
	AddressBook           *index.AddressBook  `json:"address_book,omitempty"`
	Metadata              *index.Metadata     `json:"metadata,omitempty"`
}

var _ Notification = (*TransactionsNotification)(nil)

func (n *TransactionsNotification) AdjustForClient(client *Client) any {
	if !client.Subscription.InterestedInType(n.Type) {
		return nil
	}

	var adjustedTransactions []index.Transaction
	var adjustedAddressBook *index.AddressBook
	var adjustedMetadata *index.Metadata
	if n.AddressBook != nil {
		adjustedAddressBook = &index.AddressBook{}
	}
	if n.Metadata != nil {
		adjustedMetadata = &index.Metadata{}
	}

	var allAddresses = map[string]bool{}
	for _, tx := range n.Transactions {
		includeTransaction := false
		account := string(tx.Account)

		// Check if the transaction account is in the subscribed addresses
		if slices.Contains(client.Subscription.Addresses, account) {
			includeTransaction = true
		}

		if includeTransaction {
			adjustedTransactions = append(adjustedTransactions, tx)
			allAddresses[account] = true

			// Include source from in message if exists
			if tx.InMsg != nil && tx.InMsg.Source != nil {
				allAddresses[string(*tx.InMsg.Source)] = true
			}

			// Include destinations from out messages if exist
			for _, outMsg := range tx.OutMsgs {
				if outMsg.Destination != nil {
					allAddresses[string(*outMsg.Destination)] = true
				}
			}
		}
	}

	if len(adjustedTransactions) == 0 {
		return nil
	}

	// Update address book and metadata for all addresses
	for addr := range allAddresses {
		if adjustedAddressBook != nil {
			if addrBookEntry, ok := (*n.AddressBook)[addr]; ok {
				(*adjustedAddressBook)[addr] = addrBookEntry
			}
		}
		if adjustedMetadata != nil {
			if metaEntry, ok := (*n.Metadata)[addr]; ok {
				(*adjustedMetadata)[addr] = metaEntry
			}
		}
	}

	return &TransactionsNotification{
		Type:                  n.Type,
		TraceExternalHashNorm: n.TraceExternalHashNorm,
		Transactions:          adjustedTransactions,
		AddressBook:           adjustedAddressBook,
		Metadata:              adjustedMetadata,
	}
}

func ProcessNewTrace(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, manager *ClientManager) {
	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	raw_traces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("Error loading raw traces: %v", err)
		return
	}

	emulatedContext := index.NewEmptyContext(true)
	err = emulatedContext.FillFromRawData(raw_traces)
	if err != nil {
		log.Printf("Error filling context from raw data: %v", err)
		return
	}
	if emulatedContext.GetTraceCount() > 1 {
		log.Printf("More than 1 trace in the context")
		return
	}
	if emulatedContext.GetTraceCount() == 0 {
		log.Printf("No traces in the context")
		return
	}

	var txs []index.Transaction
	txs_map := map[index.HashType]int{}
	{
		rows := emulatedContext.GetTransactions()
		for _, row := range rows {
			if tx, err := index.ScanTransaction(row); err == nil {
				txs = append(txs, *tx)
				txs_map[tx.Hash] = len(txs) - 1
			} else {
				log.Printf("Error scanning transaction: %v", err)
			}
		}
	}

	allAddresses := []string{}
	var tx_hashes []string
	for _, t := range txs {
		tx_hashes = append(tx_hashes, string(t.Hash))
		allAddresses = append(allAddresses, string(t.Account))
	}
	if len(tx_hashes) > 0 {
		rows := emulatedContext.GetMessages(tx_hashes)
		for _, row := range rows {
			msg, err := index.ScanMessageWithContent(row)
			if err != nil {
				log.Printf("Error scanning message: %v", err)
				continue
			}
			if msg.Direction == "in" {
				txs[txs_map[msg.TxHash]].InMsg = msg
				if msg.Source != nil {
					allAddresses = append(allAddresses, string(*msg.Source))
				}
			} else {
				txs[txs_map[msg.TxHash]].OutMsgs = append(txs[txs_map[msg.TxHash]].OutMsgs, msg)
				if msg.Destination != nil {
					allAddresses = append(allAddresses, string(*msg.Destination))
				}
			}
		}
	}

	// sort messages
	for idx := range txs {
		sort.SliceStable(txs[idx].OutMsgs, func(i, j int) bool {
			if txs[idx].OutMsgs[i].CreatedLt == nil {
				return true
			}
			if txs[idx].OutMsgs[j].CreatedLt == nil {
				return false
			}
			return *txs[idx].OutMsgs[i].CreatedLt < *txs[idx].OutMsgs[j].CreatedLt
		})
	}

	var addressBook *index.AddressBook
	var metadata *index.Metadata
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata("pending_transactions", allAddresses)
	if shouldFetchAddressBook || shouldFetchMetadata {
		addressBook, metadata = fetchAddressBookAndMetadata(
			ctx,
			allAddresses,
			shouldFetchAddressBook,
			shouldFetchMetadata,
		)
	}

	manager.broadcast <- &TransactionsNotification{
		TraceExternalHashNorm: traceExternalHashNorm,
		Transactions:          txs,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}

}

func SubscribeToCommittedTransactions(ctx context.Context, rdb *redis.Client, manager *ClientManager) {
	pubsub := rdb.Subscribe(ctx, *commitedTxsChannel)
	defer pubsub.Close()

	log.Printf("Subscribed to Redis channel: %s", *commitedTxsChannel)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("Error receiving message from Redis: %v", err)
			continue
		}

		log.Printf("Received message from Redis: %s", msg.Payload)

		parts := strings.Split(msg.Payload, ":")
		if len(parts) != 2 {
			log.Printf("Invalid message format: %s", msg.Payload)
			continue
		}
		traceExternalHashNorm := parts[0]
		txHashes := strings.Split(parts[1], ",")

		if len(txHashes) == 0 {
			log.Printf("No transaction hashes found in commited txs channel message: %s", msg.Payload)
			continue
		}
		ProcessNewCommitedTxs(ctx, rdb, traceExternalHashNorm, txHashes, manager)
	}
}

func ProcessNewCommitedTxs(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, txHashes []string, manager *ClientManager) {
	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	raw_traces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("Error loading raw traces: %v", err)
		return
	}

	emulatedContext := index.NewEmptyContext(true)
	err = emulatedContext.FillFromRawData(raw_traces)
	if err != nil {
		log.Printf("Error filling context from raw data: %v", err)
		return
	}
	if emulatedContext.GetTraceCount() > 1 {
		log.Printf("More than 1 trace in the context")
		return
	}
	if emulatedContext.GetTraceCount() == 0 {
		log.Printf("No traces in the context")
		return
	}
	rows := emulatedContext.GetTransactionsByTraceIdAndHash(traceExternalHashNorm, txHashes)
	if len(rows) == 0 {
		log.Printf("No transactions found for trace %s (%d hashes)", traceExternalHashNorm, len(txHashes))
		return
	}

	var txs []index.Transaction
	txs_map := map[index.HashType]int{}
	{
		rows := emulatedContext.GetTransactions()
		for _, row := range rows {
			if tx, err := index.ScanTransaction(row); err == nil {
				txs = append(txs, *tx)
				txs_map[tx.Hash] = len(txs) - 1
			} else {
				log.Printf("Error scanning transaction: %v", err)
			}
		}
	}

	allAddresses := []string{}
	var tx_hashes []string
	for _, t := range txs {
		tx_hashes = append(tx_hashes, string(t.Hash))
		allAddresses = append(allAddresses, string(t.Account))
	}
	if len(tx_hashes) > 0 {
		rows := emulatedContext.GetMessages(tx_hashes)
		for _, row := range rows {
			msg, err := index.ScanMessageWithContent(row)
			if err != nil {
				log.Printf("Error scanning message: %v", err)
				continue
			}
			if msg.Direction == "in" {
				txs[txs_map[msg.TxHash]].InMsg = msg
				if msg.Source != nil {
					allAddresses = append(allAddresses, string(*msg.Source))
				}
			} else {
				txs[txs_map[msg.TxHash]].OutMsgs = append(txs[txs_map[msg.TxHash]].OutMsgs, msg)
				if msg.Destination != nil {
					allAddresses = append(allAddresses, string(*msg.Destination))
				}
			}
		}
	}

	// sort messages
	for idx := range txs {
		sort.SliceStable(txs[idx].OutMsgs, func(i, j int) bool {
			if txs[idx].OutMsgs[i].CreatedLt == nil {
				return true
			}
			if txs[idx].OutMsgs[j].CreatedLt == nil {
				return false
			}
			return *txs[idx].OutMsgs[i].CreatedLt < *txs[idx].OutMsgs[j].CreatedLt
		})
	}

	var addressBook *index.AddressBook
	var metadata *index.Metadata
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata("pending_transactions", allAddresses)
	if shouldFetchAddressBook || shouldFetchMetadata {
		addressBook, metadata = fetchAddressBookAndMetadata(
			ctx,
			allAddresses,
			shouldFetchAddressBook,
			shouldFetchMetadata,
		)
	}

	manager.broadcast <- &TransactionsNotification{
		Type:         "transactions",
		Transactions: txs,
		AddressBook:  addressBook,
		Metadata:     metadata,
	}
}

// @title TON Streaming API
// @version 0.0.1
// @description TON Streaming API provides real-time blockchain events via SSE, WebSocket, and WebTransport.
// @basePath /api/streaming/

// SSEHandler handles Server-Sent Events connections
// @Summary Subscribe to blockchain events via SSE
// @Schemes
// @Description Subscribe to real-time blockchain events using Server-Sent Events.
// @Tags streaming
// @Accept json
// @Produce text/event-stream
// @Router /v1/sse [post]
func SSEHandler(manager *ClientManager) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// 1) Parse + validate subscription *before* starting the stream
		var subReq SubscriptionRequest
		if err := c.BodyParser(&subReq); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid subscription request"})
		}
		if len(subReq.Addresses) == 0 || len(subReq.Types) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Addresses and types are required"})
		}
		for _, t := range subReq.Types {
			switch t {
			case "pending_actions", "actions", "pending_transactions", "transactions":
			default:
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": fmt.Sprintf("Invalid event type: %s", t)})
			}
		}

		// 2) Create client + channel
		clientID := fmt.Sprintf("%s-%s", c.IP(), time.Now().Format(time.RFC3339Nano))
		eventCh := make(chan []byte, 16)
		client := &Client{
			ID:           clientID,
			Connected:    true,
			Subscription: &Subscription{Addresses: subReq.Addresses, Types: subReq.Types, IncludeAddressBook: subReq.IncludeAddressBook, IncludeMetadata: subReq.IncludeMetadata},
			// when manager wants to send something, it will push into eventCh
			SendEvent: func(data []byte) error {
				select {
				case eventCh <- data:
					return nil
				default:
					// drop if buffer full
					return nil
				}
			},
		}

		// 3) Register
		manager.register <- client

		// 4) Set SSE headers
		c.Set("Content-Type", "text/event-stream")
		c.Set("Cache-Control", "no-cache")
		c.Set("Connection", "keep-alive")
		c.Set("Transfer-Encoding", "chunked")

		// 5) Hijack to a streaming writer
		c.Status(fiber.StatusOK).Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
			// send an initial "connected" event
			fmt.Fprintf(w, "event: connected\ndata: {\"status\":\"connected\"}\n\n")
			w.Flush()
			log.Printf("Client %s connected via SSE", clientID)

			keepAlive := time.NewTicker(30 * time.Second)
			defer keepAlive.Stop()

			for {
				select {
				case data := <-eventCh:
					// actual SSE payload
					_, err := fmt.Fprintf(w, "event: event\ndata: %s\n\n", data)
					if err != nil {
						// write error → client gone
						client.Connected = false
						manager.unregister <- client
						return
					}
					if err := w.Flush(); err != nil {
						client.Connected = false
						manager.unregister <- client
						return
					}

				case <-keepAlive.C:
					// comment‐style keepalive
					if _, err := w.WriteString(": keepalive\n\n"); err != nil {
						client.Connected = false
						manager.unregister <- client
						return
					}
					if err := w.Flush(); err != nil {
						client.Connected = false
						manager.unregister <- client
						return
					}
				}
			}
		}))
		return nil
	}
}

// WebSocketHandler handles WebSocket connections
func WebSocketHandler(manager *ClientManager) func(*websocket.Conn) {
	return func(c *websocket.Conn) {
		// Create a new client
		clientID := c.RemoteAddr().String()
		client := &Client{
			ID:        clientID,
			Connected: true,
			SendEvent: func(data []byte) error {
				return c.WriteMessage(websocket.TextMessage, data)
			},
		}

		// Register the client
		manager.register <- client

		// Handle WebSocket messages
		var (
			msg []byte
			err error
		)

		for {
			if _, msg, err = c.ReadMessage(); err != nil {
				log.Println("read:", err)
				break
			}
			log.Printf("recv: %s", msg)

			// Parse the message
			var subReq SubscriptionRequest
			if err := json.Unmarshal(msg, &subReq); err != nil {
				log.Printf("Error parsing subscription request: %v", err)
				c.WriteMessage(websocket.TextMessage, []byte(`{"error":"Invalid subscription request"}`))
				continue
			}

			// Handle subscription/unsubscription
			switch subReq.Operation {
			case "subscribe":
				// Validate subscription
				if len(subReq.Addresses) == 0 || len(subReq.Types) == 0 {
					c.WriteMessage(websocket.TextMessage, []byte(`{"error":"Addresses and types are required"}`))
					continue
				}
				var addressesRaw []string
				validAddresses := true
				for _, addr := range subReq.Addresses {
					// Convert address to Raw form using AccountAddressConverter
					rawAddrValue := index.AccountAddressConverter(addr)
					if !rawAddrValue.IsValid() {
						c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"error":"Invalid address: %s"}`, addr)))
						validAddresses = false
						break
					}
					rawAddr := rawAddrValue.Interface().(index.AccountAddress)
					addressesRaw = append(addressesRaw, string(rawAddr))
				}
				if !validAddresses {
					continue
				}

				// Validate event types
				validTypes := true
				for _, t := range subReq.Types {
					if t != "pending_actions" && t != "actions" && t != "pending_transactions" && t != "transactions" {
						c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"error":"Invalid event type: %s"}`, t)))
						validTypes = false
						break
					}
				}
				if !validTypes {
					continue
				}

				// Update client subscription
				client.mu.Lock()
				client.Subscription = &Subscription{
					Addresses:          addressesRaw,
					Types:              subReq.Types,
					IncludeAddressBook: subReq.IncludeAddressBook,
					IncludeMetadata:    subReq.IncludeMetadata,
				}
				client.mu.Unlock()

				// Send confirmation
				c.WriteMessage(websocket.TextMessage, []byte(`{"status":"subscribed"}`))

			default:
				c.WriteMessage(websocket.TextMessage, []byte(`{"error":"Invalid operation"}`))
			}
		}

		// Unregister the client when the connection is closed
		client.Connected = false
		manager.unregister <- client
	}
}

// Global database client
var dbClient *index.DbClient

func main() {
	flag.Parse()

	// Initialize Redis client
	options, err := redis.ParseURL(*redisAddr)
	if err != nil {
		log.Fatalf("Error parsing Redis URL: %v", err)
	}
	rdb := redis.NewClient(options)
	ctx := context.Background()

	// Initialize PostgreSQL client if connection string is provided
	if *pg != "" {
		log.Printf("Connecting to PostgreSQL: %s", *pg)
		dbClient, err = index.NewDbClient(*pg, 100, 0)
		if err != nil {
			log.Printf("Failed to connect to PostgreSQL: %v", err)
			log.Printf("AddressBook and Metadata will not be available")
		} else {
			log.Printf("Connected to PostgreSQL successfully")
		}
	} else {
		log.Printf("PostgreSQL connection string is not provided")
		log.Printf("AddressBook and Metadata will not be available")
	}

	// Initialize client manager
	manager := NewClientManager()
	go manager.Run()

	// Subscribe to blockchain events
	go SubscribeToTraces(ctx, rdb, manager)
	go SubscribeToCommittedTransactions(ctx, rdb, manager)
	go SubscribeToClassifiedTraces(ctx, rdb, manager)

	// Initialize Fiber app
	app := fiber.New(fiber.Config{
		AppName:     "TON Streaming API",
		Prefork:     *prefork,
		ReadTimeout: 5 * time.Second,
	})

	// Middleware
	app.Use(logger.New())

	// API routes
	api := app.Group("/api/streaming")

	// SSE endpoint
	api.Post("/v1/sse", SSEHandler(manager))

	// WebSocket endpoint
	api.Use("/v1/ws", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			c.Locals("allowed", true)
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})
	api.Get("/v1/ws", websocket.New(WebSocketHandler(manager)))

	// Swagger documentation
	api.Get("/*", swagger.New(swagger.Config{
		Title:       "TON Streaming API - Swagger UI",
		DeepLinking: true,
	}))

	// Start server
	log.Printf("Starting server on port %d", *serverPort)
	log.Fatal(app.Listen(fmt.Sprintf(":%d", *serverPort)))
}
