package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/swagger"
	"github.com/gofiber/websocket/v2"
	"github.com/redis/go-redis/v9"

	"github.com/toncenter/ton-indexer/ton-index-go/index"
	"github.com/toncenter/ton-indexer/ton-index-go/index/emulated"
	_ "github.com/toncenter/ton-indexer/ton-streaming-go/docs"
)

// Command-line flags
var (
	redisAddr          = flag.String("redis", "localhost:6379", "Redis server dsn")
	tracesChannel      = flag.String("traces-channel", "new_trace", "Redis channel for blockchain events")
	commitedTxsChannel = flag.String("commited-txs-channel", "new_commited_tx", "Redis channel for committed transactions")
	serverPort         = flag.Int("port", 8085, "Server port")
	prefork            = flag.Bool("prefork", false, "Use prefork")
)

// Subscription represents a client's subscription to blockchain events
type Subscription struct {
	Addresses []string
	Types     []string
}

// SubscriptionRequest represents a subscription/unsubscription request
type SubscriptionRequest struct {
	Operation string   `json:"operation"`
	Addresses []string `json:"addresses"`
	Types     []string `json:"types"`
}

// BlockchainEvent represents an event from the blockchain
type BlockchainEvent struct {
	Type    string `json:"type"`
	Address string `json:"address"`
	Data    any    `json:"data"`
}

// Client represents a connected client
type Client struct {
	ID           string
	Connected    bool
	Subscription *Subscription
	SendEvent    func([]byte) error
	mu           sync.Mutex
}

// ClientManager manages all connected clients
type ClientManager struct {
	clients    map[string]*Client
	register   chan *Client
	unregister chan *Client
	broadcast  chan *BlockchainEvent
	mu         sync.RWMutex
}

// NewClientManager creates a new client manager
func NewClientManager() *ClientManager {
	return &ClientManager{
		clients:    make(map[string]*Client),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan *BlockchainEvent),
	}
}

// Run starts the client manager
func (manager *ClientManager) Run() {
	for {
		select {
		case client := <-manager.register:
			manager.mu.Lock()
			manager.clients[client.ID] = client
			manager.mu.Unlock()
			log.Printf("Client %s connected", client.ID)
		case client := <-manager.unregister:
			manager.mu.Lock()
			if _, ok := manager.clients[client.ID]; ok {
				delete(manager.clients, client.ID)
				log.Printf("Client %s disconnected", client.ID)
			}
			manager.mu.Unlock()
		case event := <-manager.broadcast:

			// Broadcast to clients based on their subscriptions
			manager.mu.RLock()
			for _, client := range manager.clients {
				client.mu.Lock()
				if client.Connected && client.Subscription != nil {
					// Check if the client is subscribed to this event
					if shouldSendEvent(client.Subscription, event) {
						// Send the event to the client
						if client.SendEvent != nil {
							msgBytes, err := json.Marshal(event)
							if err != nil {
								log.Printf("Error marshalling event: %v", err)
								continue
							}
							if err := client.SendEvent(msgBytes); err != nil {
								log.Printf("Error sending event to client %s: %v", client.ID, err)
							}
							log.Printf("Sent event to client %s: %d bytes", client.ID, len(msgBytes))
						}
					}
				}
				client.mu.Unlock()
			}
			manager.mu.RUnlock()
		}
	}
}

// shouldSendEvent checks if an event should be sent to a client based on their subscription
func shouldSendEvent(sub *Subscription, event *BlockchainEvent) bool {
	// Check if the client is subscribed to this event type
	typeMatch := false
	for _, t := range sub.Types {
		if t == event.Type {
			typeMatch = true
			break
		}
	}
	if !typeMatch {
		return false
	}

	// Check if the client is subscribed to this address
	for _, addr := range sub.Addresses {
		if addr == event.Address {
			return true
		}
	}

	return false
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

type PendingTransactionsNotification struct {
	TraceExternalHashNorm string              `json:"trace_external_hash_norm"`
	Transactions          []index.Transaction `json:"transactions"`
}

func ProcessNewTrace(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, manager *ClientManager) {
	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	raw_traces, err := repository.LoadRawTracesByExtMsg([]string{traceExternalHashNorm})
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
	if len(emulatedContext.GetTraces()) != 1 {
		log.Printf("More than 1 trace in the context")
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

	var tx_hashes []string
	for _, t := range txs {
		tx_hashes = append(tx_hashes, string(t.Hash))
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
			} else {
				txs[txs_map[msg.TxHash]].OutMsgs = append(txs[txs_map[msg.TxHash]].OutMsgs, msg)
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

	txsByAccount := map[string][]index.Transaction{}
	for _, tx := range txs {
		if !tx.Emulated {
			continue
		}
		txsByAccount[string(tx.Account)] = append(txsByAccount[string(tx.Account)], tx)
	}
	for account, txs := range txsByAccount {
		notification := &PendingTransactionsNotification{
			TraceExternalHashNorm: traceExternalHashNorm,
			Transactions:          txs,
		}
		event := &BlockchainEvent{
			Type:    "pending_transactions",
			Address: account,
			Data:    notification,
		}
		manager.broadcast <- event

		// log.Printf("Broadcasting %d pending transactions for account %s", len(txs), account)
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
		txInMsgHash := parts[1]

		ProcessNewCommitedTx(ctx, rdb, traceExternalHashNorm, txInMsgHash, manager)
	}
}

func ProcessNewCommitedTx(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, txInMsgHash string, manager *ClientManager) {
	txRaw := rdb.HGet(ctx, traceExternalHashNorm, txInMsgHash)
	if txRaw.Err() != nil {
		log.Printf("Error loading transaction: %v", txRaw.Err())
		return
	}

	var node emulated.TraceNode
	if err := json.Unmarshal([]byte(txRaw.Val()), &node); err != nil {
		log.Printf("Error unmarshalling transaction: %v", err)
		return
	}
	txRow, err := node.GetTransactionRow()
	if err != nil {
		log.Printf("Error getting transaction row: %v", err)
		return
	}
	tx, err := index.ScanTransaction(emulated.NewRow(&txRow))
	if err != nil {
		log.Printf("Error scanning transaction: %v", err)
		return
	}

	event := &BlockchainEvent{
		Type:    "transactions",
		Address: string(tx.Account),
		Data:    tx,
	}
	manager.broadcast <- event

	// log.Printf("Broadcasting committed transaction for account %s", string(tx.Account))
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
		c.Set("Content-Type", "text/event-stream")
		c.Set("Cache-Control", "no-cache")
		c.Set("Connection", "keep-alive")
		c.Set("Transfer-Encoding", "chunked")

		// Parse subscription from request body
		var subReq SubscriptionRequest
		if err := c.BodyParser(&subReq); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid subscription request",
			})
		}

		// Validate subscription
		if len(subReq.Addresses) == 0 || len(subReq.Types) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Addresses and types are required",
			})
		}

		// Validate event types
		for _, t := range subReq.Types {
			if t != "pending_actions" && t != "actions" && t != "pending_transactions" && t != "transactions" {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": fmt.Sprintf("Invalid event type: %s", t),
				})
			}
		}

		// Create a new client with the subscription
		clientID := c.IP() + "-" + time.Now().String()
		client := &Client{
			ID:        clientID,
			Connected: true,
			Subscription: &Subscription{
				Addresses: subReq.Addresses,
				Types:     subReq.Types,
			},
			SendEvent: func(data []byte) error {
				_, err := c.Write([]byte(fmt.Sprintf("event: event\ndata: %s\n\n", data)))
				return err
			},
		}

		// Register the client
		manager.register <- client

		// Send connected event
		c.Write([]byte("event: connected\ndata: {\"status\":\"connected\"}\n\n"))

		// Keep the connection open
		for {
			select {
			case <-c.Context().Done():
				// Unregister the client when the connection is closed
				client.Connected = false
				manager.unregister <- client
				return nil
			case <-time.After(time.Second * 30):
				// Send a keepalive message every 30 seconds
				if _, err := c.Write([]byte(": keepalive\n\n")); err != nil {
					client.Connected = false
					manager.unregister <- client
					return nil
				}
			}
		}
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
					Addresses: addressesRaw,
					Types:     subReq.Types,
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

func main() {
	flag.Parse()

	// Initialize Redis client
	options, err := redis.ParseURL(*redisAddr)
	if err != nil {
		log.Fatalf("Error parsing Redis URL: %v", err)
	}
	rdb := redis.NewClient(options)
	ctx := context.Background()

	// Initialize client manager
	manager := NewClientManager()
	go manager.Run()

	// Subscribe to blockchain events
	go SubscribeToTraces(ctx, rdb, manager)
	go SubscribeToCommittedTransactions(ctx, rdb, manager)

	// Initialize Fiber app
	app := fiber.New(fiber.Config{
		AppName:     "TON Streaming API",
		Prefork:     *prefork,
		ReadTimeout: 5 * time.Second,
	})

	// Middleware
	app.Use(logger.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowHeaders: "Origin, Content-Type, Accept",
	}))

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
