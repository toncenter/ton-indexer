package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/gofiber/websocket/v2"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"

	"github.com/toncenter/ton-indexer/ton-index-go/index"
	"github.com/toncenter/ton-indexer/ton-index-go/index/emulated"
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

type EventType string

const (
	Transactions        EventType = "transactions"
	Actions             EventType = "actions"
	PendingTransactions EventType = "pending_transactions"
	PendingActions      EventType = "pending_actions"
)

// Subscription represents a client's subscription to blockchain events
type Subscription struct {
	SubscribedAddresses map[string][]EventType
	IncludeAddressBook  bool
	IncludeMetadata     bool
}

func (s *Subscription) AddSubscribedAddresses(addresses map[string][]EventType) {
	for addr, eventTypes := range addresses {
		if _, ok := s.SubscribedAddresses[addr]; !ok {
			s.SubscribedAddresses[addr] = eventTypes
		} else {
			s.SubscribedAddresses[addr] = append(s.SubscribedAddresses[addr], eventTypes...)
		}
	}
}

func (s *Subscription) Unsubscribe(addresses []string) {
	for _, addr := range addresses {
		delete(s.SubscribedAddresses, addr)
	}
}

func (s *Subscription) InterestedIn(eventType EventType, eventAddresses []string) bool {
	for _, eventAddr := range eventAddresses {
		if _, ok := s.SubscribedAddresses[eventAddr]; ok {
			if slices.Contains(s.SubscribedAddresses[eventAddr], eventType) {
				return true
			}
		}
	}

	return false
}

// SubscriptionRequest represents a subscription/unsubscription request
type SubscriptionRequest struct {
	Id                   *string      `json:"id"`
	Operation            string       `json:"operation"`
	Addresses            *[]string    `json:"addresses"`
	Types                *[]EventType `json:"types"`
	SupportedActionTypes *[]string    `json:"supported_action_types"`
	IncludeAddressBook   *bool        `json:"include_address_book,omitempty"`
	IncludeMetadata      *bool        `json:"include_metadata,omitempty"`
}

// BlockchainEvent represents an event from the blockchain
type BlockchainEvent struct {
	Type        EventType          `json:"type"`
	Data        any                `json:"data"`
	AddressBook *index.AddressBook `json:"address_book,omitempty"`
	Metadata    *index.Metadata    `json:"metadata,omitempty"`
}

// Client represents a connected client
type Client struct {
	ID           string
	Connected    bool
	Subscription Subscription
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

func (manager *ClientManager) shouldFetchAddressBookAndMetadata(eventTypes []EventType, addressesToNotify []string) (bool, bool) {
	shouldFetchAddressBook := false
	shouldFetchMetadata := false

	for _, client := range manager.clients {
		for _, eventType := range eventTypes {
			client.mu.Lock()
			if client.Connected && client.Subscription.InterestedIn(eventType, addressesToNotify) {
				shouldFetchAddressBook = shouldFetchAddressBook || client.Subscription.IncludeAddressBook
				shouldFetchMetadata = shouldFetchMetadata || client.Subscription.IncludeMetadata
			}
			client.mu.Unlock()
			if shouldFetchAddressBook && shouldFetchMetadata {
				break
			}
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
				if client.Connected {
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
	Type                  EventType          `json:"type"`
	TraceExternalHashNorm string             `json:"trace_external_hash_norm"`
	Actions               []*index.Action    `json:"actions"`
	ActionAddresses       [][]string         `json:"-"`
	AddressBook           *index.AddressBook `json:"address_book,omitempty"`
	Metadata              *index.Metadata    `json:"metadata,omitempty"`
}

var _ Notification = (*ActionsNotification)(nil)

func (n *ActionsNotification) AdjustForClient(client *Client) any {
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

		if client.Subscription.InterestedIn(n.Type, actionAddresses) {
			adjustedActions = append(adjustedActions, action)
			adjustedActionAddresses = append(adjustedActionAddresses, actionAddresses)

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

	emulatedContext := index.NewEmptyContext(false)
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

	// sort actions by EndLt and StartLt descending
	idx := make([]int, len(actions))
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(i, j int) bool {
		if actions[idx[i]].EndLt == actions[idx[j]].EndLt {
			return actions[idx[i]].StartLt < actions[idx[j]].StartLt
		}
		return actions[idx[i]].EndLt < actions[idx[j]].EndLt
	})

	sortedActions := make([]*index.Action, len(actions))
	sortedActionsAddresses := make([][]string, len(actionsAddresses))
	for i, k := range idx {
		sortedActions[i] = actions[k]
		sortedActionsAddresses[i] = actionsAddresses[k]
	}

	var addressBook *index.AddressBook
	var metadata *index.Metadata
	allAddresses := []string{}
	for _, actionAddr := range sortedActionsAddresses {
		allAddresses = append(allAddresses, actionAddr...)
	}
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata([]EventType{PendingActions, Actions}, allAddresses)
	if shouldFetchAddressBook || shouldFetchMetadata {
		addressBook, metadata = fetchAddressBookAndMetadata(
			ctx,
			allAddresses,
			shouldFetchAddressBook,
			shouldFetchMetadata,
		)
	}

	manager.broadcast <- &ActionsNotification{
		Type:                  PendingActions,
		TraceExternalHashNorm: traceExternalHashNorm,
		Actions:               sortedActions,
		ActionAddresses:       sortedActionsAddresses,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}

	if traceIsCommited {
		manager.broadcast <- &ActionsNotification{
			Type:                  Actions,
			TraceExternalHashNorm: traceExternalHashNorm,
			Actions:               sortedActions,
			ActionAddresses:       sortedActionsAddresses,
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
	Type                  EventType           `json:"type"`
	TraceExternalHashNorm string              `json:"trace_external_hash_norm"`
	Transactions          []index.Transaction `json:"transactions"`
	AddressBook           *index.AddressBook  `json:"address_book,omitempty"`
	Metadata              *index.Metadata     `json:"metadata,omitempty"`
}

var _ Notification = (*TransactionsNotification)(nil)

func (n *TransactionsNotification) AdjustForClient(client *Client) any {
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
		account := string(tx.Account)

		if client.Subscription.InterestedIn(n.Type, []string{account}) {
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

	emulatedContext := index.NewEmptyContext(false)
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
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata([]EventType{PendingTransactions}, allAddresses)
	if shouldFetchAddressBook || shouldFetchMetadata {
		addressBook, metadata = fetchAddressBookAndMetadata(
			ctx,
			allAddresses,
			shouldFetchAddressBook,
			shouldFetchMetadata,
		)
	}

	// Sort transactions by Lt descending
	sort.Slice(txs, func(i, j int) bool {
		return txs[i].Lt > txs[j].Lt
	})

	manager.broadcast <- &TransactionsNotification{
		Type:                  PendingTransactions,
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

		// log.Printf("Received message from Redis: %s", msg.Payload)

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

	emulatedContext := index.NewEmptyContext(false)
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
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata([]EventType{Transactions}, allAddresses)
	if shouldFetchAddressBook || shouldFetchMetadata {
		addressBook, metadata = fetchAddressBookAndMetadata(
			ctx,
			allAddresses,
			shouldFetchAddressBook,
			shouldFetchMetadata,
		)
	}

	// Sort transactions by Lt descending
	sort.Slice(txs, func(i, j int) bool {
		return txs[i].Lt > txs[j].Lt
	})

	manager.broadcast <- &TransactionsNotification{
		Type:                  Transactions,
		TraceExternalHashNorm: traceExternalHashNorm,
		Transactions:          txs,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}
}

type ErrorResponse struct {
	Id    *string `json:"id,omitempty"`
	Error string  `json:"error"`
}

type StatusResponse struct {
	Id     *string `json:"id,omitempty"`
	Status string  `json:"status"`
}

var validEventTypes = map[EventType]struct{}{
	PendingActions:      {},
	Actions:             {},
	PendingTransactions: {},
	Transactions:        {},
}

// ValidateSubscription performs all syntactic checks in one place and returns the
// address→event‑types map that goes straight into the Subscription struct.
func ValidateSubscription(req *SubscriptionRequest) (map[string][]EventType, error) {
	if req.Operation != "subscribe" {
		return nil, fmt.Errorf("invalid operation: %s", req.Operation)
	}

	if req.Addresses == nil || len(*req.Addresses) == 0 || req.Types == nil || len(*req.Types) == 0 {
		return nil, errors.New("addresses and types are required")
	}

	// convert addresses once; capacity = len(req.Addresses) avoids reallocs
	addrMap := make(map[string][]EventType, len(*req.Addresses))
	for _, a := range *req.Addresses {
		cnv, err := convertAddress(a)
		if err != nil {
			return nil, err
		}
		addrMap[cnv] = *req.Types
	}

	for _, t := range *req.Types {
		if _, ok := validEventTypes[t]; !ok {
			return nil, fmt.Errorf("invalid event type: %s", t)
		}
	}
	return addrMap, nil
}

// convertAddress keeps the reflection ugliness in one place.
func convertAddress(s string) (string, error) {
	raw := index.AccountAddressConverter(s)
	if !raw.IsValid() {
		return "", fmt.Errorf("invalid address: %s", s)
	}
	return string(raw.Interface().(index.AccountAddress)), nil
}

// writeSSE marshals v and writes a single Server‑Sent‑Event frame.
func writeSSE(w *bufio.Writer, event string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeSSEBytes(w, event, data)
}

func writeSSEBytes(w *bufio.Writer, event string, payload []byte) error {
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, payload); err != nil {
		return err
	}
	return w.Flush()
}

// sendWSJSONErr centralises websocket error frames.
func sendWSJSONErr(c *websocket.Conn, id *string, err error) {
	if msg, e := json.Marshal(ErrorResponse{Id: id, Error: err.Error()}); e == nil {
		_ = c.WriteMessage(websocket.TextMessage, msg)
	} else {
		log.Printf("marshal error response: %v", e)
	}
}

// ────────────────────────────────────────────────────────────────────────────────
// HTTP‑SSE handler
// ────────────────────────────────────────────────────────────────────────────────

func SSEHandler(manager *ClientManager) fiber.Handler {
	return func(c *fiber.Ctx) error {
		var req SubscriptionRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{Error: fmt.Sprintf("invalid subscription request: %v", err)})
		}
		addrMap, err := ValidateSubscription(&req)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{Id: req.Id, Error: err.Error()})
		}

		clientID := fmt.Sprintf("%s-%s", c.IP(), time.Now().Format(time.RFC3339Nano))
		eventCh := make(chan []byte, 16)
		client := &Client{
			ID:        clientID,
			Connected: true,
			Subscription: Subscription{
				SubscribedAddresses: addrMap,
				IncludeAddressBook:  req.IncludeAddressBook != nil && *req.IncludeAddressBook,
				IncludeMetadata:     req.IncludeMetadata != nil && *req.IncludeMetadata,
			},
			SendEvent: func(b []byte) error {
				select {
				case eventCh <- b:
					return nil // buffered write
				default:
					return nil // drop if buffer full
				}
			},
		}
		manager.register <- client

		// 3) SSE plumbing
		c.Set("Content-Type", "text/event-stream")
		c.Set("Cache-Control", "no-cache")
		c.Set("Connection", "keep-alive")
		c.Set("Transfer-Encoding", "chunked")

		c.Status(fiber.StatusOK).Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
			if err := writeSSE(w, "connected", StatusResponse{Id: req.Id, Status: "subscribed"}); err != nil {
				log.Printf("write connected frame: %v", err)
				return
			}
			log.Printf("Client %s connected via SSE", clientID)

			keepAlive := time.NewTicker(15 * time.Second)
			defer keepAlive.Stop()

			for {
				select {
				case data := <-eventCh:
					if err := writeSSEBytes(w, "event", data); err != nil {
						client.Connected = false
						manager.unregister <- client
						return
					}
					_ = w.Flush()
				case <-keepAlive.C:
					if _, err := w.WriteString(": keepalive\n\n"); err != nil {
						client.Connected = false
						manager.unregister <- client
						return
					}
					_ = w.Flush()
				}
			}
		}))
		return nil
	}
}

// ────────────────────────────────────────────────────────────────────────────────
// WebSocket handler
// ────────────────────────────────────────────────────────────────────────────────

func WebSocketHandler(manager *ClientManager) func(*websocket.Conn) {
	return func(c *websocket.Conn) {
		clientID := c.RemoteAddr().String()
		client := &Client{
			ID:        clientID,
			Connected: true,
			Subscription: Subscription{
				SubscribedAddresses: make(map[string][]EventType),
				IncludeAddressBook:  false,
				IncludeMetadata:     false,
			},
			SendEvent: func(b []byte) error { return c.WriteMessage(websocket.TextMessage, b) },
		}
		manager.register <- client
		defer func() {
			client.Connected = false
			manager.unregister <- client
		}()

		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				log.Printf("read: %v", err)
				return
			}

			var req SubscriptionRequest
			if err := json.Unmarshal(msg, &req); err != nil {
				sendWSJSONErr(c, nil, fmt.Errorf("invalid subscription request: %v", err))
				continue
			}

			if req.Operation == "ping" {
				ack, _ := json.Marshal(StatusResponse{Id: req.Id, Status: "pong"})
				_ = c.WriteMessage(websocket.TextMessage, ack)
				continue
			}

			if req.Operation == "unsubscribe" {
				if req.Addresses == nil || len(*req.Addresses) == 0 {
					sendWSJSONErr(c, req.Id, fmt.Errorf("addresses are required"))
					continue
				}
				cnvAddrs := make([]string, len(*req.Addresses))
				addrsValid := true
				for i, a := range *req.Addresses {
					cnvAddrs[i], err = convertAddress(a)
					if err != nil {
						addrsValid = false
						sendWSJSONErr(c, req.Id, err)
						break
					}
				}
				if !addrsValid {
					continue
				}

				client.mu.Lock()
				client.Subscription.Unsubscribe(cnvAddrs)
				client.mu.Unlock()
				ack, _ := json.Marshal(StatusResponse{Id: req.Id, Status: "unsubscribed"})
				_ = c.WriteMessage(websocket.TextMessage, ack)
				continue
			}

			addrMap, err := ValidateSubscription(&req)
			if err != nil {
				sendWSJSONErr(c, req.Id, err)
				continue
			}

			// apply subscription atomically
			client.mu.Lock()
			client.Subscription.AddSubscribedAddresses(addrMap)
			if req.IncludeAddressBook != nil {
				client.Subscription.IncludeAddressBook = *req.IncludeAddressBook
			}
			if req.IncludeMetadata != nil {
				client.Subscription.IncludeMetadata = *req.IncludeMetadata
			}
			client.mu.Unlock()

			ack, _ := json.Marshal(StatusResponse{Id: req.Id, Status: "subscribed"})
			_ = c.WriteMessage(websocket.TextMessage, ack)
		}
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
		ProxyHeader: fiber.HeaderXForwardedFor,
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

	// Start server
	log.Printf("Starting server on port %d", *serverPort)
	log.Fatal(app.Listen(fmt.Sprintf(":%d", *serverPort)))
}
