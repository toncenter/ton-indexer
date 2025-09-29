package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/websocket/v2"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/toncenter/ton-indexer/ton-emulate-go/models"
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
	TraceInvalidated    EventType = "trace_invalidated"
	AccountStateChanged EventType = "account_state_change"
	JettonsChanged      EventType = "jettons_change"
)

// RateLimitConfig holds rate limiting configuration for a client
type RateLimitConfig struct {
	MaxParallelConnections int
	MaxSubscribedAddresses int
}

// RateLimiter manages rate limiting for clients
type RateLimiter struct {
	mu      sync.RWMutex
	clients map[string]*ClientRateLimit
}

// ClientRateLimit tracks rate limiting for a specific client
type ClientRateLimit struct {
	limitingKey       string
	activeConnections map[string]bool // clientID -> true
	config            RateLimitConfig
	mu                sync.Mutex
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter() *RateLimiter {
	return &RateLimiter{
		clients: make(map[string]*ClientRateLimit),
	}
}

// RegisterConnection registers a new connection for rate limiting
func (rl *RateLimiter) RegisterConnection(limitingKey string, clientID string, config RateLimitConfig) error {
	if limitingKey == "" {
		// No rate limiting if no key provided
		return nil
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	clientLimit, exists := rl.clients[limitingKey]
	if !exists {
		clientLimit = &ClientRateLimit{
			limitingKey:       limitingKey,
			activeConnections: make(map[string]bool),
			config:            config,
		}
		rl.clients[limitingKey] = clientLimit
	}

	clientLimit.mu.Lock()
	defer clientLimit.mu.Unlock()

	// Check if we've reached the connection limit
	if config.MaxParallelConnections != -1 && len(clientLimit.activeConnections) >= config.MaxParallelConnections {
		return fmt.Errorf("connection limit reached: %d active connections", config.MaxParallelConnections)
	}

	clientLimit.activeConnections[clientID] = true
	return nil
}

// UnregisterConnection removes a connection from rate limiting
func (rl *RateLimiter) UnregisterConnection(limitingKey string, clientID string) {
	if limitingKey == "" {
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if clientLimit, exists := rl.clients[limitingKey]; exists {
		clientLimit.mu.Lock()
		delete(clientLimit.activeConnections, clientID)

		// Clean up if no active connections
		if len(clientLimit.activeConnections) == 0 {
			delete(rl.clients, limitingKey)
		}
		clientLimit.mu.Unlock()
	}
}

// GetAddressLimit returns the maximum number of addresses for a client
func (rl *RateLimiter) GetAddressLimit(limitingKey string) int {
	if limitingKey == "" {
		return 0 // No limit
	}

	rl.mu.RLock()
	defer rl.mu.RUnlock()

	if clientLimit, exists := rl.clients[limitingKey]; exists {
		return clientLimit.config.MaxSubscribedAddresses
	}
	return 0
}

type eventSet map[EventType]struct{}
type AddressSubs map[string]eventSet

type Subscription struct {
	SubscribedAddresses  AddressSubs
	ActionTypes          []string
	SupportedActionTypes []string
	IncludeAddressBook   bool
	IncludeMetadata      bool
}

func makeEventSet(types []EventType) eventSet {
	s := make(eventSet, len(types))
	for _, t := range types {
		s[t] = struct{}{}
	}
	return s
}

func (s *Subscription) Add(addrs map[string][]EventType) {
	if s.SubscribedAddresses == nil {
		s.SubscribedAddresses = make(AddressSubs)
	}
	for addr, types := range addrs {
		set, ok := s.SubscribedAddresses[addr]
		if !ok {
			s.SubscribedAddresses[addr] = makeEventSet(types)
			continue
		}
		for _, t := range types {
			set[t] = struct{}{}
		}
	}
}

func (s *Subscription) Replace(newAddrs map[string][]EventType) {
	newMap := make(AddressSubs, len(newAddrs))
	for addr, types := range newAddrs {
		newMap[addr] = makeEventSet(types)
	}

	s.SubscribedAddresses = newMap
}

func (s *Subscription) Unsubscribe(addresses []string) {
	for _, addr := range addresses {
		delete(s.SubscribedAddresses, addr)
	}
}

func (s *Subscription) InterestedIn(eventType EventType, eventAddresses []string) bool {
	for _, a := range eventAddresses {
		if set, ok := s.SubscribedAddresses[a]; ok {
			if _, has := set[eventType]; has {
				return true
			}
		}
	}
	return false
}

type Operation string

const (
	OpPing            Operation = "ping"
	OpSubscribe       Operation = "subscribe"        // merge
	OpSetSubscription Operation = "set_subscription" // replace
	OpUnsubscribe     Operation = "unsubscribe"
	OpConfigure       Operation = "configure"
)

type Envelope struct {
	Id        *string   `json:"id"`
	Operation Operation `json:"operation"`
	// Keep the rest raw so we can decode per-op without ambiguity:
	// unmarshal once into Envelope, then unmarshal again into op-specific struct.
}

// subscribe (merge) — adds event types to addresses (existing ones preserved)
type SubscribeRequest struct {
	Addresses []string    `json:"addresses"`
	Types     []EventType `json:"types"`
}

// set_subscription (replace) — authoritative snapshot of the full subscription
type SetSubscriptionRequest struct {
	Subscriptions map[string][]EventType `json:"subscriptions"`
}

// unsubscribe — remove a list of addresses entirely
type UnsubscribeRequest struct {
	Addresses []string `json:"addresses"`
}

// configure — toggle globals setting for this connection
type ConfigureRequest struct {
	IncludeAddressBook   *bool    `json:"include_address_book"`
	IncludeMetadata      *bool    `json:"include_metadata"`
	SupportedActionTypes []string `json:"supported_action_types"`
	ActionTypes          []string `json:"action_types"`
}

// SSERequest represents a subscription/unsubscription request
type SSERequest struct {
	Id                   *string      `json:"id"`
	Operation            *string      `json:"operation"`
	Addresses            *[]string    `json:"addresses"`
	Types                *[]EventType `json:"types"`
	ActionTypes          []string     `json:"action_types"`
	SupportedActionTypes []string     `json:"supported_action_types"`
	IncludeAddressBook   *bool        `json:"include_address_book"`
	IncludeMetadata      *bool        `json:"include_metadata"`
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
	ID                             string
	LimitingKey                    string // Added for rate limiting
	Connected                      bool
	Subscription                   Subscription
	TracesForPotentialInvalidation map[string]bool // traceExternalHashNorm -> true
	SendEvent                      func([]byte) error
	sendChan                       chan []byte
	mu                             sync.Mutex
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
	clients     map[string]*Client
	register    chan *Client
	unregister  chan *Client
	broadcast   chan Notification
	rateLimiter *RateLimiter // Added for rate limiting
	mu          sync.RWMutex
}

// NewClientManager creates a new client manager
func NewClientManager() *ClientManager {
	return &ClientManager{
		clients:     make(map[string]*Client),
		register:    make(chan *Client),
		unregister:  make(chan *Client),
		broadcast:   make(chan Notification),
		rateLimiter: NewRateLimiter(),
	}
}

func (manager *ClientManager) shouldFetchAddressBookAndMetadata(eventTypes []EventType, addressesToNotify []string) (bool, bool) {
	shouldFetchAddressBook := false
	shouldFetchMetadata := false

	manager.mu.RLock()
	clients := make([]*Client, 0, len(manager.clients))
	for _, c := range manager.clients {
		clients = append(clients, c)
	}
	manager.mu.RUnlock()

	for _, client := range clients {
		client.mu.Lock()
		if client.Connected {
			for _, eventType := range eventTypes {
				if client.Subscription.InterestedIn(eventType, addressesToNotify) {
					shouldFetchAddressBook = shouldFetchAddressBook || client.Subscription.IncludeAddressBook
					shouldFetchMetadata = shouldFetchMetadata || client.Subscription.IncludeMetadata
				}
			}
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
			client.sendChan = make(chan []byte, 64)
			manager.clients[client.ID] = client
			manager.mu.Unlock()
			client.startSender(manager)
			log.Printf("Client %s connected", client.ID)
		case client := <-manager.unregister:
			manager.mu.Lock()
			if _, ok := manager.clients[client.ID]; ok {
				delete(manager.clients, client.ID)
				// Unregister from rate limiter
				manager.rateLimiter.UnregisterConnection(client.LimitingKey, client.ID)
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
func fetchAddressBookAndMetadata(ctx context.Context, addrBookAddresses []string, metadataAddresses []string, includeAddressBook bool, includeMetadata bool) (*index.AddressBook, *index.Metadata) {
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
		book, err := index.QueryAddressBookImpl(addrBookAddresses, conn, settings)
		if err != nil {
			log.Printf("Error querying address book: %v", err)
		} else {
			addressBook = &book
		}
	}

	if includeMetadata {
		meta, err := index.QueryMetadataImpl(metadataAddresses, conn, settings)
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

type TraceInvalidatedNotification struct {
	Type                  EventType `json:"type"`
	TraceExternalHashNorm string    `json:"trace_external_hash_norm"`
}

var _ Notification = (*TraceInvalidatedNotification)(nil)

func (n *TraceInvalidatedNotification) AdjustForClient(client *Client) any {
	if subscribed := client.TracesForPotentialInvalidation[n.TraceExternalHashNorm]; subscribed {
		delete(client.TracesForPotentialInvalidation, n.TraceExternalHashNorm)
		return n
	}
	return nil
}

type ActionsNotification struct {
	Type                  EventType          `json:"type"`
	TraceExternalHashNorm string             `json:"trace_external_hash_norm"`
	Actions               []*index.Action    `json:"actions"`
	ActionAddresses       [][]string         `json:"-"` // contains all addresses mentioned in actions for addressbook, it's not same as index.Action.Accounts
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
	supportedActionsSet := mapset.NewSet(client.Subscription.SupportedActionTypes...)
	filterActionsSet := mapset.NewSet(client.Subscription.ActionTypes...)
	for idx, action := range n.Actions {
		if client.Subscription.InterestedIn(n.Type, action.Accounts) {
			if !filterActionsSet.IsEmpty() && !filterActionsSet.ContainsAny(action.Type) {
				continue
			}

			if supportedActionsSet.ContainsAny(action.AncestorType...) {
				continue
			}
			if !supportedActionsSet.ContainsAny(action.Type) {
				continue
			}

			adjustedActions = append(adjustedActions, action)
			adjustedActionAddresses = append(adjustedActionAddresses, n.ActionAddresses[idx])

			for _, addr := range n.ActionAddresses[idx] {
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

	switch n.Type {
	case PendingActions:
		client.TracesForPotentialInvalidation[n.TraceExternalHashNorm] = true
	case Actions:
		delete(client.TracesForPotentialInvalidation, n.TraceExternalHashNorm)
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
		go ProcessNewClassifiedTrace(ctx, rdb, traceExternalHashNorm, manager)
	}
}

func ProcessNewClassifiedTrace(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, manager *ClientManager) {
	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	raw_traces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("Error loading raw traces: %v, trace key: %s", err, traceExternalHashNorm)
		return
	}

	emulatedContext := index.NewEmptyContext(false)
	err = emulatedContext.FillFromRawData(raw_traces)
	if err != nil {
		log.Printf("Error filling context from raw data: %v, trace key: %s", err, traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() > 1 {
		log.Printf("More than 1 trace in the context, trace key: %s", traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() == 0 {
		log.Printf("No traces in the context, trace key: %s", traceExternalHashNorm)
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

	for _, row := range emulatedContext.GetAllActions() { // GetAllActions returns actions in ascending order
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
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata([]EventType{PendingActions, Actions}, allAddresses)
	if shouldFetchAddressBook || shouldFetchMetadata {
		addressBook, metadata = fetchAddressBookAndMetadata(
			ctx,
			allAddresses,
			allAddresses,
			shouldFetchAddressBook,
			shouldFetchMetadata,
		)
	}

	manager.broadcast <- &ActionsNotification{
		Type:                  PendingActions,
		TraceExternalHashNorm: traceExternalHashNorm,
		Actions:               actions,
		ActionAddresses:       actionsAddresses,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}

	if traceIsCommited {
		manager.broadcast <- &ActionsNotification{
			Type:                  Actions,
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
		go ProcessNewTrace(ctx, rdb, traceExternalHashNorm, manager)
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

	switch n.Type {
	case PendingTransactions:
		client.TracesForPotentialInvalidation[n.TraceExternalHashNorm] = true
	case Transactions:
		delete(client.TracesForPotentialInvalidation, n.TraceExternalHashNorm)
	}

	return &TransactionsNotification{
		Type:                  n.Type,
		TraceExternalHashNorm: n.TraceExternalHashNorm,
		Transactions:          adjustedTransactions,
		AddressBook:           adjustedAddressBook,
		Metadata:              adjustedMetadata,
	}
}

type AccountStateNotification struct {
	Type    EventType          `json:"type"`
	Account string             `json:"account"`
	State   index.AccountState `json:"state"`
}

var _ Notification = (*AccountStateNotification)(nil)

func (n *AccountStateNotification) AdjustForClient(client *Client) any {
	if client.Subscription.InterestedIn(n.Type, []string{n.Account}) {
		return n
	}
	return nil
}

type JettonsNotification struct {
	Type        EventType          `json:"type"`
	Jetton      index.JettonWallet `json:"jetton"`
	AddressBook *index.AddressBook `json:"address_book,omitempty"`
	Metadata    *index.Metadata    `json:"metadata,omitempty"`
}

var _ Notification = (*JettonsNotification)(nil)

func (n *JettonsNotification) AdjustForClient(client *Client) any {
	if client.Subscription.InterestedIn(n.Type, []string{n.Jetton.Address.String()}) ||
		client.Subscription.InterestedIn(n.Type, []string{n.Jetton.Owner.String()}) {
		return n
	}
	return nil
}

func ProcessNewTrace(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, manager *ClientManager) {
	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	raw_traces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("Error loading raw traces: %v, trace key: %s", err, traceExternalHashNorm)
		return
	}

	emulatedContext := index.NewEmptyContext(false)
	err = emulatedContext.FillFromRawData(raw_traces)
	if err != nil {
		log.Printf("Error filling context from raw data: %v, trace key: %s", err, traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() > 1 {
		log.Printf("More than 1 trace in the context, trace key: %s", traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() == 0 {
		log.Printf("No traces in the context, trace key: %s", traceExternalHashNorm)
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
		go ProcessNewCommitedTxs(ctx, rdb, traceExternalHashNorm, txHashes, manager)
	}
}

func ProcessNewCommitedTxs(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, txHashes []string, manager *ClientManager) {
	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	raw_traces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("Error loading raw traces: %v, trace key: %s", err, traceExternalHashNorm)
		return
	}

	emulatedContext := index.NewEmptyContext(false)
	err = emulatedContext.FillFromRawData(raw_traces)
	if err != nil {
		log.Printf("Error filling context from raw data: %v, trace key: %s", err, traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() > 1 {
		log.Printf("More than 1 trace in the context, trace key: %s", traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() == 0 {
		log.Printf("No traces in the context, trace key: %s", traceExternalHashNorm)
		return
	}
	rows := emulatedContext.GetTransactionsByTraceIdAndHash(traceExternalHashNorm, txHashes)
	if len(rows) == 0 {
		log.Printf("No transactions found for trace %s (%d hashes)", traceExternalHashNorm, len(txHashes))
		return
	}

	var txs []index.Transaction
	txs_map := map[index.HashType]int{}
	for _, row := range rows {
		if tx, err := index.ScanTransaction(row); err == nil {
			txs = append(txs, *tx)
			txs_map[tx.Hash] = len(txs) - 1
		} else {
			log.Printf("Error scanning transaction: %v", err)
		}
	}

	txsAddresses := []string{}
	var tx_hashes []string
	for _, t := range txs {
		tx_hashes = append(tx_hashes, string(t.Hash))
		txsAddresses = append(txsAddresses, string(t.Account))
	}

	go ProcessNewAccountStates(ctx, rdb, txsAddresses, manager)

	allAddresses := txsAddresses
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

// TODO: cleanup processedAccountStatesLts map periodically to avoid memory leak
var processedAccountStatesLts = map[string]uint64{}
var processedAccountStatesLtsMutex sync.RWMutex

func ProcessNewAccountStates(ctx context.Context, rdb *redis.Client, addresses []string, manager *ClientManager) {
	for _, addr := range addresses {
		key := fmt.Sprintf("acct:%s", addr)
		acctData, err := rdb.HGetAll(ctx, key).Result()
		if err != nil {
			log.Printf("Error fetching account state for %s: %v", addr, err)
			continue
		}
		var accountState models.AccountState
		err = msgpack.Unmarshal([]byte(acctData["state"]), &accountState)
		if err != nil {
			log.Printf("Error unmarshalling account state for %s: %v (%s)", addr, err, acctData["state"])
			continue
		}
		if accountState.LastTransLt != nil {
			shouldNotify := false
			processedAccountStatesLtsMutex.Lock()
			cur := processedAccountStatesLts[addr]

			if *accountState.LastTransLt > cur {
				processedAccountStatesLts[addr] = *accountState.LastTransLt
				shouldNotify = true
			}
			processedAccountStatesLtsMutex.Unlock()

			if !shouldNotify {
				continue // already processed same or newer LT
			}
		}

		manager.broadcast <- &AccountStateNotification{
			Type:    AccountStateChanged,
			Account: addr,
			State:   models.MsgPackAccountStateToIndexAccountState(accountState),
		}

		interfacesData := acctData["interfaces"]
		if interfacesData == "" {
			continue
		}
		var addrInterfaces models.AddressInterfaces
		err = msgpack.Unmarshal([]byte(interfacesData), &addrInterfaces)
		if err != nil {
			log.Printf("Error unmarshalling address interfaces for %s: %v (%s)", addr, err, interfacesData)
			continue
		}

		var notification *JettonsNotification

		for _, iface := range addrInterfaces.Interfaces {
			switch val := iface.Value.(type) {
			case *models.JettonWalletInterface:
				notification = &JettonsNotification{
					Type:   JettonsChanged,
					Jetton: MsgPackJettonWalletToModel(*val, int64(*accountState.LastTransLt), models.ConvertHashToIndex(accountState.CodeHash), models.ConvertHashToIndex(accountState.DataHash)),
				}
			}
		}
		if notification == nil {
			continue
		}

		addrBookAddresses := []string{notification.Jetton.Address.String(), notification.Jetton.Owner.String(), notification.Jetton.Jetton.String()}
		metadataAddresses := []string{notification.Jetton.Owner.String(), notification.Jetton.Jetton.String()}
		shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata([]EventType{JettonsChanged}, addrBookAddresses)
		if shouldFetchAddressBook || shouldFetchMetadata {
			notification.AddressBook, notification.Metadata = fetchAddressBookAndMetadata(
				ctx,
				addrBookAddresses,
				metadataAddresses,
				shouldFetchAddressBook,
				shouldFetchMetadata,
			)
		}
		manager.broadcast <- notification
	}
}

func MsgPackJettonWalletToModel(j models.JettonWalletInterface, lastTransLt int64, codeHash *index.HashType, dataHash *index.HashType) index.JettonWallet {
	return index.JettonWallet{
		Address:           index.AccountAddress(j.Address),
		Balance:           j.Balance,
		Owner:             index.AccountAddress(j.Owner),
		Jetton:            index.AccountAddress(j.Jetton),
		LastTransactionLt: lastTransLt,
		CodeHash:          codeHash,
		DataHash:          dataHash,
	}
}

func SubscribeToInvalidatedTraces(ctx context.Context, rdb *redis.Client, manager *ClientManager) {
	pubsub := rdb.Subscribe(ctx, "invalidated_traces")
	defer pubsub.Close()

	log.Printf("Subscribed to Redis channel: invalidated_traces")

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("Error receiving message from Redis: %v", err)
			continue
		}

		traceExternalHashNorm := msg.Payload

		// Notify clients about the invalidated trace
		manager.broadcast <- &TraceInvalidatedNotification{
			Type:                  TraceInvalidated,
			TraceExternalHashNorm: traceExternalHashNorm,
		}
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
	AccountStateChanged: {},
	JettonsChanged:      {},
}

// ParseRateLimitHeaders extracts rate limiting configuration from headers map
func ParseRateLimitHeaders(headers map[string][]string) (string, RateLimitConfig) {
	var limitingKey string
	config := RateLimitConfig{}

	// Get limiting key
	if values, ok := headers["X-Limiting-Key"]; ok && len(values) > 0 {
		limitingKey = values[0]
	}

	// Get max parallel connections
	if values, ok := headers["X-Max-Parallel-Connections"]; ok && len(values) > 0 {
		if maxConn, err := strconv.Atoi(values[0]); err == nil {
			config.MaxParallelConnections = maxConn
		}
	}

	// Get max subscribed addresses
	if values, ok := headers["X-Max-Subscribed-Addr"]; ok && len(values) > 0 {
		if maxAddr, err := strconv.Atoi(values[0]); err == nil {
			config.MaxSubscribedAddresses = maxAddr
		}
	}

	return limitingKey, config
}

func ValidateSSERequest(req *SSERequest) (map[string][]EventType, error) {
	if req.Addresses == nil || len(*req.Addresses) == 0 {
		return nil, fmt.Errorf("addresses are required for subscribe operation")
	}
	if req.Types == nil || len(*req.Types) == 0 {
		return nil, fmt.Errorf("types are required for subscribe operation")
	}

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

// checkAddressLimit checks if adding new addresses would exceed the limit
func checkAddressLimit(client *Client, newAddresses int, rateLimiter *RateLimiter, toOverwrite bool) error {
	if client.LimitingKey == "" {
		return nil // No rate limiting
	}

	maxAddresses := rateLimiter.GetAddressLimit(client.LimitingKey)
	if maxAddresses <= 0 {
		return nil // No limit
	}

	currentCount := len(client.Subscription.SubscribedAddresses)
	if toOverwrite {
		currentCount = 0 // Overwriting, so current count is 0
	}
	if currentCount+newAddresses > maxAddresses {
		return fmt.Errorf("address limit exceeded: current %d + new %d > max %d",
			currentCount, newAddresses, maxAddresses)
	}

	return nil
}

// ────────────────────────────────────────────────────────────────────────────────
// HTTP‑SSE handler
// ────────────────────────────────────────────────────────────────────────────────

func SSEHandler(manager *ClientManager) fiber.Handler {
	return func(c *fiber.Ctx) error {
		var req SSERequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{Error: fmt.Sprintf("invalid subscription request: %v", err)})
		}
		addrMap, err := ValidateSSERequest(&req)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{Id: req.Id, Error: err.Error()})
		}

		if len(req.SupportedActionTypes) == 0 {
			if val, ok := c.GetReqHeaders()["X-Actions-Version"]; ok && len(val) > 0 {
				req.SupportedActionTypes = val
			} else {
				req.SupportedActionTypes = []string{"latest"}
			}
		}

		// Parse rate limiting headers
		limitingKey, rateLimitConfig := ParseRateLimitHeaders(c.GetReqHeaders())

		clientID := fmt.Sprintf("%s-%s", c.IP(), time.Now().Format(time.RFC3339Nano))

		// Check rate limits before creating client
		if limitingKey != "" {
			// Check connection limit
			if err := manager.rateLimiter.RegisterConnection(limitingKey, clientID, rateLimitConfig); err != nil {
				return c.Status(fiber.StatusTooManyRequests).JSON(ErrorResponse{
					Id:    req.Id,
					Error: err.Error(),
				})
			}

			// Check address limit
			if rateLimitConfig.MaxSubscribedAddresses != -1 && len(addrMap) > rateLimitConfig.MaxSubscribedAddresses {
				manager.rateLimiter.UnregisterConnection(limitingKey, clientID)
				return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{
					Id:    req.Id,
					Error: fmt.Sprintf("too many addresses: %d > max %d", len(addrMap), rateLimitConfig.MaxSubscribedAddresses),
				})
			}
		}

		eventCh := make(chan []byte, 16)

		client := &Client{
			ID:          clientID,
			LimitingKey: limitingKey,
			Connected:   true,
			Subscription: Subscription{
				IncludeAddressBook:   req.IncludeAddressBook != nil && *req.IncludeAddressBook,
				IncludeMetadata:      req.IncludeMetadata != nil && *req.IncludeMetadata,
				ActionTypes:          req.ActionTypes,
				SupportedActionTypes: index.ExpandActionTypeShortcuts(req.SupportedActionTypes),
			},
			TracesForPotentialInvalidation: make(map[string]bool),
			SendEvent: func(b []byte) error {
				select {
				case eventCh <- b:
					return nil // buffered write
				default:
					return nil // drop if buffer full
				}
			},
		}
		client.Subscription.Add(addrMap)
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
						client.mu.Lock()
						client.Connected = false
						client.mu.Unlock()
						manager.unregister <- client
						return
					}
					_ = w.Flush()
				case <-keepAlive.C:
					if _, err := w.WriteString(": keepalive\n\n"); err != nil {
						client.mu.Lock()
						client.Connected = false
						client.mu.Unlock()
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
		// Get rate limiting info from headers (passed through websocket upgrade)
		headers := make(map[string][]string)
		headers["X-Limiting-Key"] = []string{c.Headers("X-Limiting-Key")}
		headers["X-Max-Parallel-Connections"] = []string{c.Headers("X-Max-Parallel-Connections")}
		headers["X-Max-Subscribed-Addr"] = []string{c.Headers("X-Max-Subscribed-Addr")}
		headers["X-Actions-Version"] = []string{c.Headers("X-Actions-Version", "latest")}
		limitingKey, rateLimitConfig := ParseRateLimitHeaders(headers)

		clientID := fmt.Sprintf("%s-%s", c.RemoteAddr(), time.Now().Format(time.RFC3339Nano))

		// Check connection limit
		if limitingKey != "" {
			if err := manager.rateLimiter.RegisterConnection(limitingKey, clientID, rateLimitConfig); err != nil {
				sendWSJSONErr(c, nil, err)
				c.Close()
				return
			}
		}

		client := &Client{
			ID:          clientID,
			LimitingKey: limitingKey,
			Connected:   true,
			Subscription: Subscription{
				SubscribedAddresses:  make(AddressSubs),
				SupportedActionTypes: index.ExpandActionTypeShortcuts(headers["X-Actions-Version"]),
				IncludeAddressBook:   false,
				IncludeMetadata:      false,
			},
			TracesForPotentialInvalidation: make(map[string]bool),
			SendEvent:                      func(b []byte) error { return c.WriteMessage(websocket.TextMessage, b) },
		}
		manager.register <- client
		defer func() {
			client.mu.Lock()
			client.Connected = false
			client.mu.Unlock()
			manager.unregister <- client
		}()

		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				log.Printf("read: %v", err)
				return
			}

			var env Envelope
			if err := json.Unmarshal(msg, &env); err != nil {
				sendWSJSONErr(c, nil, fmt.Errorf("invalid subscription request: %v", err))
				continue
			}
			switch env.Operation {
			case OpPing:
				ack, _ := json.Marshal(StatusResponse{Id: env.Id, Status: "pong"})
				_ = c.WriteMessage(websocket.TextMessage, ack)

			case OpUnsubscribe:
				var req UnsubscribeRequest
				if err := json.Unmarshal(msg, &req); err != nil {
					sendWSJSONErr(c, env.Id, fmt.Errorf("invalid unsubscribe request: %v", err))
					continue
				}
				if len(req.Addresses) == 0 {
					sendWSJSONErr(c, env.Id, fmt.Errorf("addresses are required"))
					continue
				}
				cnvAddrs := make([]string, len(req.Addresses))
				addrsValid := true
				for i, a := range req.Addresses {
					cnvAddrs[i], err = convertAddress(a)
					if err != nil {
						addrsValid = false
						sendWSJSONErr(c, env.Id, err)
						break
					}
				}
				if !addrsValid {
					continue
				}

				client.mu.Lock()
				client.Subscription.Unsubscribe(cnvAddrs)
				client.mu.Unlock()
				ack, _ := json.Marshal(StatusResponse{Id: env.Id, Status: "unsubscribed"})
				_ = c.WriteMessage(websocket.TextMessage, ack)

			case OpSubscribe:
				var req SubscribeRequest
				if err := json.Unmarshal(msg, &req); err != nil {
					sendWSJSONErr(c, env.Id, fmt.Errorf("invalid subscribe request: %v", err))
					continue
				}

				addrMap := make(map[string][]EventType, len(req.Addresses))
				var validationError error
				for _, a := range req.Addresses {
					cnv, err := convertAddress(a)
					if err != nil {
						validationError = fmt.Errorf("invalid address: %s", a)
						break
					}
					for _, t := range req.Types {
						if _, ok := validEventTypes[t]; !ok {
							validationError = fmt.Errorf("invalid event type: %s", t)
							break
						}
					}
					if validationError != nil {
						break
					}
					addrMap[cnv] = req.Types
				}
				if validationError != nil {
					sendWSJSONErr(c, env.Id, validationError)
					continue
				}

				// Check address limit
				client.mu.Lock()
				err = checkAddressLimit(client, len(addrMap), manager.rateLimiter, false)
				if err != nil {
					client.mu.Unlock()
					sendWSJSONErr(c, env.Id, err)
					continue
				}
				client.Subscription.Add(addrMap)
				client.mu.Unlock()

				ack, _ := json.Marshal(StatusResponse{Id: env.Id, Status: "subscribed"})
				_ = c.WriteMessage(websocket.TextMessage, ack)

			case OpSetSubscription:
				var req SetSubscriptionRequest
				if err := json.Unmarshal(msg, &req); err != nil {
					sendWSJSONErr(c, env.Id, fmt.Errorf("invalid set_subscription: %v", err))
					continue
				}

				addrMap := make(map[string][]EventType, len(req.Subscriptions))
				var validationError error
				for a, et := range req.Subscriptions {
					cnv, err := convertAddress(a)
					if err != nil {
						validationError = err
						break
					}
					for _, t := range et {
						if _, ok := validEventTypes[t]; !ok {
							validationError = fmt.Errorf("invalid event type: %s", t)
							break
						}
					}
					if validationError != nil {
						break
					}
					addrMap[cnv] = et
				}
				if validationError != nil {
					sendWSJSONErr(c, env.Id, validationError)
					continue
				}

				// Rate-limit against the *new* snapshot size
				client.mu.Lock()
				if err := checkAddressLimit(client, len(addrMap), manager.rateLimiter, true); err != nil {
					client.mu.Unlock()
					sendWSJSONErr(c, env.Id, err)
					continue
				}

				client.Subscription.Replace(addrMap)
				client.mu.Unlock()

				ack, _ := json.Marshal(StatusResponse{Id: env.Id, Status: "subscription_set"})
				_ = c.WriteMessage(websocket.TextMessage, ack)

			case OpConfigure:
				var req ConfigureRequest
				if err := json.Unmarshal(msg, &req); err != nil {
					sendWSJSONErr(c, env.Id, fmt.Errorf("invalid configure request: %v", err))
					continue
				}

				client.mu.Lock()
				if req.IncludeAddressBook != nil {
					client.Subscription.IncludeAddressBook = *req.IncludeAddressBook
				}
				if req.IncludeMetadata != nil {
					client.Subscription.IncludeMetadata = *req.IncludeMetadata
				}
				if len(req.SupportedActionTypes) > 0 {
					client.Subscription.SupportedActionTypes = index.ExpandActionTypeShortcuts(req.SupportedActionTypes)
				}
				if len(req.ActionTypes) > 0 {
					client.Subscription.ActionTypes = req.ActionTypes
				}
				client.mu.Unlock()

				ack, _ := json.Marshal(StatusResponse{Id: env.Id, Status: "configured"})
				_ = c.WriteMessage(websocket.TextMessage, ack)
				continue

			default:
				sendWSJSONErr(c, env.Id, fmt.Errorf("unknown operation: %s", env.Operation))
			}
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
	go SubscribeToInvalidatedTraces(ctx, rdb, manager)

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
