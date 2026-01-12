package v2

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/toncenter/ton-indexer/ton-emulate-go/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index"
	"github.com/toncenter/ton-indexer/ton-index-go/index/emulated"
)

////////////////////////////////////////////////////////////////////////////////
// Config
////////////////////////////////////////////////////////////////////////////////

// Config provides runtime dependencies for v2 handlers.
type Config struct {
	DBClient        *index.DbClient
	Testnet         bool
	ImgProxyBaseURL string
}

var config Config

// InitConfig registers configuration to be used by v2 handlers.
func InitConfig(cfg Config) {
	config = cfg
}

////////////////////////////////////////////////////////////////////////////////
// Finality & Event types
////////////////////////////////////////////////////////////////////////////////

func defaultMinFinality() emulated.FinalityState {
	// Safer default: only finalized events unless explicitly requested otherwise.
	return emulated.FinalityStateFinalized
}

type EventType string

const (
	EventTransactions       EventType = "transactions"
	EventActions            EventType = "actions"
	EventAccountStateChange EventType = "account_state_change"
	EventJettonsChange      EventType = "jettons_change"
	EventTraceInvalidated   EventType = "trace_invalidated" // internal; not subscribable
)

var validEventTypes = map[EventType]struct{}{
	EventTransactions:       {},
	EventActions:            {},
	EventAccountStateChange: {},
	EventJettonsChange:      {},
}

const (
	jettonTransferNotificationOpcode       index.OpcodeType = 0x7362d09c
	nftOwnershipAssignedNotificationOpcode index.OpcodeType = 0x05138d91
)

////////////////////////////////////////////////////////////////////////////////
// Rate limiting
////////////////////////////////////////////////////////////////////////////////

type RateLimitConfig struct {
	MaxParallelConnections int
	MaxSubscribedAddresses int
}

type ClientRateLimit struct {
	limitingKey       string
	activeConnections map[string]bool // clientID -> true
	config            RateLimitConfig
	mu                sync.Mutex
}

type RateLimiter struct {
	mu      sync.RWMutex
	clients map[string]*ClientRateLimit
}

func NewRateLimiter() *RateLimiter {
	return &RateLimiter{
		clients: make(map[string]*ClientRateLimit),
	}
}

func (rl *RateLimiter) RegisterConnection(limitingKey string, clientID string, config RateLimitConfig) error {
	if limitingKey == "" {
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

	if config.MaxParallelConnections != -1 &&
		len(clientLimit.activeConnections) >= config.MaxParallelConnections {
		return fmt.Errorf("connection limit reached: %d active connections", config.MaxParallelConnections)
	}

	clientLimit.activeConnections[clientID] = true
	return nil
}

func (rl *RateLimiter) UnregisterConnection(limitingKey string, clientID string) {
	if limitingKey == "" {
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if clientLimit, exists := rl.clients[limitingKey]; exists {
		clientLimit.mu.Lock()
		delete(clientLimit.activeConnections, clientID)
		if len(clientLimit.activeConnections) == 0 {
			delete(rl.clients, limitingKey)
		}
		clientLimit.mu.Unlock()
	}
}

func (rl *RateLimiter) GetAddressLimit(limitingKey string) int {
	if limitingKey == "" {
		return 0
	}

	rl.mu.RLock()
	defer rl.mu.RUnlock()

	if clientLimit, exists := rl.clients[limitingKey]; exists {
		return clientLimit.config.MaxSubscribedAddresses
	}
	return 0
}

////////////////////////////////////////////////////////////////////////////////
// Subscription model
////////////////////////////////////////////////////////////////////////////////

type eventSet map[EventType]struct{}
type AddressSet map[string]struct{}

type Subscription struct {
	SubscribedAddresses  AddressSet
	EventTypes           eventSet
	ActionTypes          []string
	SupportedActionTypes []string
	IncludeAddressBook   bool
	IncludeMetadata      bool
	MinFinality          emulated.FinalityState
}

func makeEventSet(types []EventType) eventSet {
	s := make(eventSet, len(types))
	for _, t := range types {
		s[t] = struct{}{}
	}
	return s
}

func makeAddressSet(addrs []string) AddressSet {
	s := make(AddressSet, len(addrs))
	for _, addr := range addrs {
		s[addr] = struct{}{}
	}
	return s
}

func (s *Subscription) Replace(addresses []string, eventTypes []EventType) {
	s.SubscribedAddresses = makeAddressSet(addresses)
	s.EventTypes = makeEventSet(eventTypes)
}

func (s *Subscription) Unsubscribe(addresses []string) {
	for _, addr := range addresses {
		delete(s.SubscribedAddresses, addr)
	}
}

func (s *Subscription) InterestedIn(eventType EventType, eventAddresses []string) bool {
	if s.EventTypes == nil {
		return false
	}
	if _, ok := s.EventTypes[eventType]; !ok {
		return false
	}
	for _, a := range eventAddresses {
		if _, ok := s.SubscribedAddresses[a]; ok {
			return true
		}
	}
	return false
}

////////////////////////////////////////////////////////////////////////////////
// Client / manager
////////////////////////////////////////////////////////////////////////////////

type Notification interface {
	AdjustForClient(client *Client) any
}

type Client struct {
	ID                             string
	LimitingKey                    string
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

type ClientManager struct {
	clients     map[string]*Client
	register    chan *Client
	unregister  chan *Client
	broadcast   chan Notification
	rateLimiter *RateLimiter
	mu          sync.RWMutex
}

func NewClientManager() *ClientManager {
	return &ClientManager{
		clients:     make(map[string]*Client),
		register:    make(chan *Client),
		unregister:  make(chan *Client),
		broadcast:   make(chan Notification),
		rateLimiter: NewRateLimiter(),
	}
}

// shouldFetchAddressBookAndMetadata figures out if at least one client
// - is interested in any of the given event types for given addresses
// - AND has IncludeAddressBook / IncludeMetadata true
// AND will actually receive this event with given finality.
func (manager *ClientManager) shouldFetchAddressBookAndMetadata(eventTypes []EventType, eventFinality emulated.FinalityState, addressesToNotify []string) (bool, bool) {
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
		if client.Connected && client.Subscription.MinFinality <= eventFinality {
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

func (manager *ClientManager) Run() {
	for {
		select {
		case client := <-manager.register:
			manager.mu.Lock()
			client.sendChan = make(chan []byte, 64)
			manager.clients[client.ID] = client
			manager.mu.Unlock()
			client.startSender(manager)
			log.Printf("[v2] Client %s connected", client.ID)

		case client := <-manager.unregister:
			manager.mu.Lock()
			if _, ok := manager.clients[client.ID]; ok {
				delete(manager.clients, client.ID)
				manager.rateLimiter.UnregisterConnection(client.LimitingKey, client.ID)
				log.Printf("[v2] Client %s disconnected", client.ID)
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
							log.Printf("[v2] Error marshalling event: %v", err)
							client.mu.Unlock()
							continue
						}
						select {
						case client.sendChan <- msgBytes:
						default:
							log.Printf("[v2] Client %s send buffer full, dropping event", client.ID)
						}
					}
				}
				client.mu.Unlock()
			}
			manager.mu.RUnlock()
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// Address book / metadata fetching
////////////////////////////////////////////////////////////////////////////////

func fetchAddressBookAndMetadata(ctx context.Context, addrBookAddresses []string, metadataAddresses []string, includeAddressBook bool, includeMetadata bool) (*index.AddressBook, *index.Metadata) {
	var addressBook *index.AddressBook
	var metadata *index.Metadata

	if config.DBClient == nil {
		return nil, nil
	}

	conn, err := config.DBClient.Pool.Acquire(ctx)
	if err != nil {
		log.Printf("[v2] Error acquiring DB connection: %v", err)
		return nil, nil
	}
	defer conn.Release()

	settings := index.RequestSettings{
		Timeout:   3 * time.Second,
		IsTestnet: config.Testnet,
	}

	if includeAddressBook {
		book, err := index.QueryAddressBookImpl(addrBookAddresses, conn, settings)
		if err != nil {
			log.Printf("[v2] Error querying address book: %v", err)
		} else {
			addressBook = &book
		}
	}

	if includeMetadata {
		meta, err := index.QueryMetadataImpl(metadataAddresses, conn, settings)
		if err != nil {
			log.Printf("[v2] Error querying metadata: %v", err)
		} else {
			if config.ImgProxyBaseURL != "" {
				index.SubstituteImgproxyBaseUrl(&meta, config.ImgProxyBaseURL)
			}
			metadata = &meta
		}
	}

	return addressBook, metadata
}

////////////////////////////////////////////////////////////////////////////////
// Notifications
////////////////////////////////////////////////////////////////////////////////

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
	Type                  EventType              `json:"type"` // always "actions"
	Finality              emulated.FinalityState `json:"finality,string"`
	TraceExternalHashNorm string                 `json:"trace_external_hash_norm"`
	Actions               []*index.Action        `json:"actions"`
	ActionAddresses       [][]string             `json:"-"` // used internally
	AddressBook           *index.AddressBook     `json:"address_book,omitempty"`
	Metadata              *index.Metadata        `json:"metadata,omitempty"`
}

var _ Notification = (*ActionsNotification)(nil)

func (n *ActionsNotification) AdjustForClient(client *Client) any {
	// Finality filter
	if n.Finality < client.Subscription.MinFinality {
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
	allAddresses := map[string]bool{}

	supportedActionsSet := mapset.NewSet(client.Subscription.SupportedActionTypes...)
	filterActionsSet := mapset.NewSet(client.Subscription.ActionTypes...)

	for idx, action := range n.Actions {
		if client.Subscription.InterestedIn(EventActions, action.Accounts) {
			// Filter by requested action types (if any)
			if !filterActionsSet.IsEmpty() && !filterActionsSet.ContainsAny(action.Type) {
				continue
			}

			// Filter by "supported" action types/versions
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

	// Manage invalidation tracking
	switch n.Finality {
	case emulated.FinalityStatePending, emulated.FinalityStateConfirmed, emulated.FinalityStateSigned:
		client.TracesForPotentialInvalidation[n.TraceExternalHashNorm] = true
	case emulated.FinalityStateFinalized:
		delete(client.TracesForPotentialInvalidation, n.TraceExternalHashNorm)
	}

	return &ActionsNotification{
		Type:                  n.Type,
		Finality:              n.Finality,
		TraceExternalHashNorm: n.TraceExternalHashNorm,
		Actions:               adjustedActions,
		ActionAddresses:       adjustedActionAddresses,
		AddressBook:           adjustedAddressBook,
		Metadata:              adjustedMetadata,
	}
}

type TransactionsNotification struct {
	Type                  EventType              `json:"type"` // always "transactions"
	Finality              emulated.FinalityState `json:"finality"`
	TraceExternalHashNorm string                 `json:"trace_external_hash_norm"`
	Transactions          []index.Transaction    `json:"transactions"`
	AddressBook           *index.AddressBook     `json:"address_book,omitempty"`
	Metadata              *index.Metadata        `json:"metadata,omitempty"`
}

var _ Notification = (*TransactionsNotification)(nil)

func (n *TransactionsNotification) AdjustForClient(client *Client) any {
	// Finality filter
	if n.Finality < client.Subscription.MinFinality {
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

	allAddresses := map[string]bool{}
	for _, tx := range n.Transactions {
		account := string(tx.Account)

		if client.Subscription.InterestedIn(EventTransactions, []string{account}) {
			adjustedTransactions = append(adjustedTransactions, tx)
			allAddresses[account] = true

			if tx.InMsg != nil && tx.InMsg.Source != nil {
				allAddresses[string(*tx.InMsg.Source)] = true
			}
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

	switch n.Finality {
	case emulated.FinalityStatePending, emulated.FinalityStateConfirmed, emulated.FinalityStateSigned:
		client.TracesForPotentialInvalidation[n.TraceExternalHashNorm] = true
	case emulated.FinalityStateFinalized:
		delete(client.TracesForPotentialInvalidation, n.TraceExternalHashNorm)
	}

	return &TransactionsNotification{
		Type:                  n.Type,
		Finality:              n.Finality,
		TraceExternalHashNorm: n.TraceExternalHashNorm,
		Transactions:          adjustedTransactions,
		AddressBook:           adjustedAddressBook,
		Metadata:              adjustedMetadata,
	}
}

type AccountStateNotification struct {
	Type     EventType              `json:"type"`
	Finality emulated.FinalityState `json:"finality"` // confirmed / signed / finalized
	Account  string                 `json:"account"`
	State    index.AccountState     `json:"state"`
}

var _ Notification = (*AccountStateNotification)(nil)

func (n *AccountStateNotification) AdjustForClient(client *Client) any {
	if n.Account == "0:DAE153A74D894BBC32748198CD626E4F5DF4A69AD2FA56CE80FC2644B5708D20" {
		log.Printf("[v2] AdjustForClient for special account %s with finality %d", n.Account, n.Finality)
	}
	if n.Finality < client.Subscription.MinFinality {
		return nil
	}
	if client.Subscription.InterestedIn(EventAccountStateChange, []string{n.Account}) {
		return n
	}
	return nil
}

type JettonsNotification struct {
	Type        EventType              `json:"type"`
	Finality    emulated.FinalityState `json:"finality"` // confirmed / signed / finalized
	Jetton      index.JettonWallet     `json:"jetton"`
	AddressBook *index.AddressBook     `json:"address_book,omitempty"`
	Metadata    *index.Metadata        `json:"metadata,omitempty"`
}

var _ Notification = (*JettonsNotification)(nil)

func (n *JettonsNotification) AdjustForClient(client *Client) any {
	if n.Finality < client.Subscription.MinFinality {
		return nil
	}
	if client.Subscription.InterestedIn(EventJettonsChange, []string{n.Jetton.Address.String()}) ||
		client.Subscription.InterestedIn(EventJettonsChange, []string{n.Jetton.Owner.String()}) {
		return n
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// Redis subscription: traces (pending)
////////////////////////////////////////////////////////////////////////////////

func SubscribeToTraces(ctx context.Context, rdb *redis.Client, manager *ClientManager, channel string) {
	pubsub := rdb.Subscribe(ctx, channel)
	defer pubsub.Close()

	log.Printf("[v2] Subscribed to Redis channel (pending traces): %s", channel)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("[v2] Error receiving pending trace message: %v", err)
			continue
		}
		traceExternalHashNorm := msg.Payload
		go ProcessNewTrace(ctx, rdb, traceExternalHashNorm, manager)
	}
}

// pending (emulated) trace
func ProcessNewTrace(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, manager *ClientManager) {
	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	rawTraces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("[v2] Error loading raw traces (pending): %v, trace key: %s", err, traceExternalHashNorm)
		return
	}

	emulatedContext := index.NewEmptyContext(false)
	err = emulatedContext.FillFromRawData(rawTraces)
	if err != nil {
		log.Printf("[v2] Error filling context from raw data (pending): %v, trace key: %s", err, traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() > 1 {
		log.Printf("[v2] More than 1 trace in context (pending), trace key: %s", traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() == 0 {
		log.Printf("[v2] No traces in context (pending), trace key: %s", traceExternalHashNorm)
		return
	}

	var txs []index.Transaction
	txsMap := map[index.HashType]int{}
	{
		rows := emulatedContext.GetTransactions()
		for _, row := range rows {
			if tx, err := index.ScanTransaction(row); err == nil {
				txs = append(txs, *tx)
				txsMap[tx.Hash] = len(txs) - 1
			} else {
				log.Printf("[v2] Error scanning transaction (pending): %v", err)
			}
		}
	}

	allAddresses := []string{}
	var txHashes []string
	for _, t := range txs {
		txHashes = append(txHashes, string(t.Hash))
		allAddresses = append(allAddresses, string(t.Account))
	}

	if len(txHashes) > 0 {
		rows := emulatedContext.GetMessages(txHashes)
		msgPtrs := make([]*index.Message, 0, len(rows))
		for _, row := range rows {
			msg, err := index.ScanMessageWithContent(row)
			msgPtrs = append(msgPtrs, msg)
			if err != nil {
				log.Printf("[v2] Error scanning message (pending): %v", err)
				continue
			}
			if msg.Direction == "in" {
				txs[txsMap[msg.TxHash]].InMsg = msg
				if msg.Source != nil {
					allAddresses = append(allAddresses, string(*msg.Source))
				}
			} else {
				txs[txsMap[msg.TxHash]].OutMsgs = append(txs[txsMap[msg.TxHash]].OutMsgs, msg)
				if msg.Destination != nil {
					allAddresses = append(allAddresses, string(*msg.Destination))
				}
			}
		}
		if err := index.MarkMessagesByPtr(msgPtrs); err != nil {
			hashes := make([]string, len(msgPtrs))
			for i, msg := range msgPtrs {
				hashes[i] = string(msg.MsgHash)
			}
			log.Printf("[v2] Error marking messages (pending) with hashes %v: %v", hashes, err)
		}
	}

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
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata(
		[]EventType{EventTransactions},
		emulated.FinalityStatePending,
		allAddresses,
	)
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
		Type:                  EventTransactions,
		Finality:              emulated.FinalityStatePending,
		TraceExternalHashNorm: traceExternalHashNorm,
		Transactions:          txs,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}
}

////////////////////////////////////////////////////////////////////////////////
// Redis subscription: confirmed & finalized txs
////////////////////////////////////////////////////////////////////////////////

// SubscribeToSignedTransactions listens for txs that reached "signed" state.
// Channel format is assumed to be: "<trace_external_hash_norm>:txhash1,txhash2,..."
func SubscribeToSignedTransactions(ctx context.Context, rdb *redis.Client, manager *ClientManager, channel string) {
	pubsub := rdb.Subscribe(ctx, channel)
	defer pubsub.Close()

	log.Printf("[v2] Subscribed to Redis channel (signed txs): %s", channel)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("[v2] Error receiving signed txs message: %v", err)
			continue
		}

		parts := strings.Split(msg.Payload, ":")
		if len(parts) != 2 {
			log.Printf("[v2] Invalid signed txs message format: %s", msg.Payload)
			continue
		}
		traceExternalHashNorm := parts[0]
		txHashes := strings.Split(parts[1], ",")
		if len(txHashes) == 0 {
			log.Printf("[v2] No transaction hashes found in signed txs message: %s", msg.Payload)
			continue
		}

		go ProcessNewSignedTxs(ctx, rdb, traceExternalHashNorm, txHashes, manager)
	}
}

// SubscribeToConfirmedTransactions listens for txs that reached "confirmed" state.
// Channel format is assumed to be: "<trace_external_hash_norm>:txhash1,txhash2,..."
func SubscribeToConfirmedTransactions(ctx context.Context, rdb *redis.Client, manager *ClientManager, channel string) {
	pubsub := rdb.Subscribe(ctx, channel)
	defer pubsub.Close()

	log.Printf("[v2] Subscribed to Redis channel (confirmed txs): %s", channel)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("[v2] Error receiving confirmed txs message: %v", err)
			continue
		}

		parts := strings.Split(msg.Payload, ":")
		if len(parts) != 2 {
			log.Printf("[v2] Invalid confirmed txs message format: %s", msg.Payload)
			continue
		}
		traceExternalHashNorm := parts[0]
		txHashes := strings.Split(parts[1], ",")
		if len(txHashes) == 0 {
			log.Printf("[v2] No transaction hashes found in confirmed txs message: %s", msg.Payload)
			continue
		}

		go ProcessNewConfirmedTxs(ctx, rdb, traceExternalHashNorm, txHashes, manager)
	}
}

// SubscribeToFinalizedTransactions listens for txs that reached "finalized" state
// (this replaces the old "committed" notion).
// Channel format is assumed to be: "<trace_external_hash_norm>:txhash1,txhash2,..."
func SubscribeToFinalizedTransactions(ctx context.Context, rdb *redis.Client, manager *ClientManager, channel string) {
	pubsub := rdb.Subscribe(ctx, channel)
	defer pubsub.Close()

	log.Printf("[v2] Subscribed to Redis channel (finalized txs): %s", channel)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("[v2] Error receiving finalized txs message: %v", err)
			continue
		}

		parts := strings.Split(msg.Payload, ":")
		if len(parts) != 2 {
			log.Printf("[v2] Invalid finalized txs message format: %s", msg.Payload)
			continue
		}
		traceExternalHashNorm := parts[0]
		txHashes := strings.Split(parts[1], ",")
		if len(txHashes) == 0 {
			log.Printf("[v2] No transaction hashes found in finalized txs message: %s", msg.Payload)
			continue
		}

		go ProcessNewFinalizedTxs(ctx, rdb, traceExternalHashNorm, txHashes, manager)
	}
}

// confirmed txs
func ProcessNewConfirmedTxs(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, txHashes []string, manager *ClientManager) {
	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	rawTraces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("[v2] Error loading raw traces (confirmed): %v, trace key: %s", err, traceExternalHashNorm)
		return
	}

	emulatedContext := index.NewEmptyContext(false)
	err = emulatedContext.FillFromRawData(rawTraces)
	if err != nil {
		log.Printf("[v2] Error filling context from raw data (confirmed): %v, trace key: %s", err, traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() > 1 {
		log.Printf("[v2] More than 1 trace in context (confirmed), trace key: %s", traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() == 0 {
		log.Printf("[v2] No traces in context (confirmed), trace key: %s", traceExternalHashNorm)
		return
	}

	rows := emulatedContext.GetTransactionsByTraceIdAndHash(traceExternalHashNorm, txHashes)
	if len(rows) == 0 {
		log.Printf("[v2] No transactions found for trace %s (confirmed, %d hashes)", traceExternalHashNorm, len(txHashes))
		return
	}

	var txs []index.Transaction
	txsMap := map[index.HashType]int{}
	for _, row := range rows {
		if tx, err := index.ScanTransaction(row); err == nil {
			txs = append(txs, *tx)
			txsMap[tx.Hash] = len(txs) - 1
		} else {
			log.Printf("[v2] Error scanning transaction (confirmed): %v", err)
		}
	}

	txsAddresses := []string{}
	var hashes []string
	for _, t := range txs {
		hashes = append(hashes, string(t.Hash))
		txsAddresses = append(txsAddresses, string(t.Account))
	}

	allAddresses := txsAddresses
	if len(hashes) > 0 {
		rows := emulatedContext.GetMessages(hashes)
		for _, row := range rows {
			msg, err := index.ScanMessageWithContent(row)
			if err != nil {
				log.Printf("[v2] Error scanning message (confirmed): %v", err)
				continue
			}
			if msg.Direction == "in" {
				txs[txsMap[msg.TxHash]].InMsg = msg
				if msg.Source != nil {
					allAddresses = append(allAddresses, string(*msg.Source))
				}
			} else {
				txs[txsMap[msg.TxHash]].OutMsgs = append(txs[txsMap[msg.TxHash]].OutMsgs, msg)
				if msg.Destination != nil {
					allAddresses = append(allAddresses, string(*msg.Destination))
				}
			}
		}
	}

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
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata(
		[]EventType{EventTransactions},
		emulated.FinalityStateConfirmed,
		allAddresses,
	)
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
		Type:                  EventTransactions,
		Finality:              emulated.FinalityStateConfirmed,
		TraceExternalHashNorm: traceExternalHashNorm,
		Transactions:          txs,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}
}

// signed txs
func ProcessNewSignedTxs(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, txHashes []string, manager *ClientManager) {
	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	rawTraces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("[v2] Error loading raw traces (signed): %v, trace key: %s", err, traceExternalHashNorm)
		return
	}

	emulatedContext := index.NewEmptyContext(false)
	err = emulatedContext.FillFromRawData(rawTraces)
	if err != nil {
		log.Printf("[v2] Error filling context from raw data (signed): %v, trace key: %s", err, traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() > 1 {
		log.Printf("[v2] More than 1 trace in context (signed), trace key: %s", traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() == 0 {
		log.Printf("[v2] No traces in context (signed), trace key: %s", traceExternalHashNorm)
		return
	}

	rows := emulatedContext.GetTransactionsByTraceIdAndHash(traceExternalHashNorm, txHashes)
	if len(rows) == 0 {
		log.Printf("[v2] No transactions found for trace %s (signed, %d hashes)", traceExternalHashNorm, len(txHashes))
		return
	}

	var txs []index.Transaction
	txsMap := map[index.HashType]int{}
	for _, row := range rows {
		if tx, err := index.ScanTransaction(row); err == nil {
			txs = append(txs, *tx)
			txsMap[tx.Hash] = len(txs) - 1
		} else {
			log.Printf("[v2] Error scanning transaction (signed): %v", err)
		}
	}

	txsAddresses := []string{}
	var hashes []string
	for _, t := range txs {
		hashes = append(hashes, string(t.Hash))
		txsAddresses = append(txsAddresses, string(t.Account))
	}

	allAddresses := txsAddresses
	if len(hashes) > 0 {
		rows := emulatedContext.GetMessages(hashes)
		for _, row := range rows {
			msg, err := index.ScanMessageWithContent(row)
			if err != nil {
				log.Printf("[v2] Error scanning message (signed): %v", err)
				continue
			}
			if msg.Direction == "in" {
				txs[txsMap[msg.TxHash]].InMsg = msg
				if msg.Source != nil {
					allAddresses = append(allAddresses, string(*msg.Source))
				}
			} else {
				txs[txsMap[msg.TxHash]].OutMsgs = append(txs[txsMap[msg.TxHash]].OutMsgs, msg)
				if msg.Destination != nil {
					allAddresses = append(allAddresses, string(*msg.Destination))
				}
			}
		}
	}

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
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata(
		[]EventType{EventTransactions},
		emulated.FinalityStateSigned,
		allAddresses,
	)
	if shouldFetchAddressBook || shouldFetchMetadata {
		addressBook, metadata = fetchAddressBookAndMetadata(
			ctx,
			allAddresses,
			allAddresses,
			shouldFetchAddressBook,
			shouldFetchMetadata,
		)
	}

	sort.Slice(txs, func(i, j int) bool {
		return txs[i].Lt > txs[j].Lt
	})

	manager.broadcast <- &TransactionsNotification{
		Type:                  EventTransactions,
		Finality:              emulated.FinalityStateSigned,
		TraceExternalHashNorm: traceExternalHashNorm,
		Transactions:          txs,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}
}

// finalized txs (old "committed")
func ProcessNewFinalizedTxs(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, txHashes []string, manager *ClientManager) {
	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	rawTraces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("[v2] Error loading raw traces (finalized): %v, trace key: %s", err, traceExternalHashNorm)
		return
	}

	emulatedContext := index.NewEmptyContext(false)
	err = emulatedContext.FillFromRawData(rawTraces)
	if err != nil {
		log.Printf("[v2] Error filling context from raw data (finalized): %v, trace key: %s", err, traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() > 1 {
		log.Printf("[v2] More than 1 trace in context (finalized), trace key: %s", traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() == 0 {
		log.Printf("[v2] No traces in context (finalized), trace key: %s", traceExternalHashNorm)
		return
	}
	rows := emulatedContext.GetTransactionsByTraceIdAndHash(traceExternalHashNorm, txHashes)
	if len(rows) == 0 {
		log.Printf("[v2] No transactions found for trace %s (finalized, %d hashes)", traceExternalHashNorm, len(txHashes))
		return
	}

	var txs []index.Transaction
	txsMap := map[index.HashType]int{}
	for _, row := range rows {
		if tx, err := index.ScanTransaction(row); err == nil {
			txs = append(txs, *tx)
			txsMap[tx.Hash] = len(txs) - 1
		} else {
			log.Printf("[v2] Error scanning transaction (finalized): %v", err)
		}
	}

	txsAddresses := []string{}
	var hashes []string
	for _, t := range txs {
		hashes = append(hashes, string(t.Hash))
		txsAddresses = append(txsAddresses, string(t.Account))
	}

	allAddresses := txsAddresses
	if len(hashes) > 0 {
		rows := emulatedContext.GetMessages(hashes)
		for _, row := range rows {
			msg, err := index.ScanMessageWithContent(row)
			if err != nil {
				log.Printf("[v2] Error scanning message (finalized): %v", err)
				continue
			}
			if msg.Direction == "in" {
				txs[txsMap[msg.TxHash]].InMsg = msg
				if msg.Source != nil {
					allAddresses = append(allAddresses, string(*msg.Source))
				}
			} else {
				txs[txsMap[msg.TxHash]].OutMsgs = append(txs[txsMap[msg.TxHash]].OutMsgs, msg)
				if msg.Destination != nil {
					allAddresses = append(allAddresses, string(*msg.Destination))
				}
			}
		}
	}

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
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata(
		[]EventType{EventTransactions},
		emulated.FinalityStateFinalized,
		allAddresses,
	)
	if shouldFetchAddressBook || shouldFetchMetadata {
		addressBook, metadata = fetchAddressBookAndMetadata(
			ctx,
			allAddresses,
			allAddresses,
			shouldFetchAddressBook,
			shouldFetchMetadata,
		)
	}

	sort.Slice(txs, func(i, j int) bool {
		return txs[i].Lt > txs[j].Lt
	})

	manager.broadcast <- &TransactionsNotification{
		Type:                  EventTransactions,
		Finality:              emulated.FinalityStateFinalized,
		TraceExternalHashNorm: traceExternalHashNorm,
		Transactions:          txs,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}
}

////////////////////////////////////////////////////////////////////////////////
// Account state updates
////////////////////////////////////////////////////////////////////////////////

func SubscribeToAccountStateUpdates(ctx context.Context, rdb *redis.Client, manager *ClientManager, channel string) {
	pubsub := rdb.Subscribe(ctx, channel)
	defer pubsub.Close()

	log.Printf("[v2] Subscribed to Redis channel (account state updates): %s", channel)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("[v2] Error receiving account state update message: %v", err)
			continue
		}

		finality, addr, err := parseAccountStateChannelPayload(msg.Payload)
		if err != nil {
			log.Printf("[v2] Invalid account state payload %q: %v", msg.Payload, err)
			continue
		}

		go ProcessNewAccountStates(ctx, rdb, addr, finality, manager)
	}
}

func parseAccountStateChannelPayload(payload string) (emulated.FinalityState, string, error) {
	parts := strings.SplitN(payload, ":", 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("unexpected payload format")
	}

	var finality emulated.FinalityState
	switch parts[0] {
	case "account_confirmed":
		finality = emulated.FinalityStateConfirmed
	case "account_signed":
		finality = emulated.FinalityStateSigned
	case "account_finalized":
		finality = emulated.FinalityStateFinalized
	default:
		return 0, "", fmt.Errorf("unknown prefix %q", parts[0])
	}

	if parts[1] == "" {
		return 0, "", fmt.Errorf("missing address")
	}

	return finality, parts[1], nil
}

////////////////////////////////////////////////////////////////////////////////
// Classified traces (actions)
////////////////////////////////////////////////////////////////////////////////

func SubscribeToClassifiedTraces(ctx context.Context, rdb *redis.Client, manager *ClientManager, channel string) {
	pubsub := rdb.Subscribe(ctx, channel)
	defer pubsub.Close()

	log.Printf("[v2] Subscribed to Redis channel (classified traces): %s", channel)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("[v2] Error receiving classified trace message: %v", err)
			continue
		}

		traceExternalHashNorm := msg.Payload
		go ProcessNewClassifiedTrace(ctx, rdb, traceExternalHashNorm, manager)
	}
}

func ProcessNewClassifiedTrace(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, manager *ClientManager) {
	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	rawTraces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("[v2] Error loading raw traces (classified): %v, trace key: %s", err, traceExternalHashNorm)
		return
	}

	emulatedContext := index.NewEmptyContext(false)
	err = emulatedContext.FillFromRawData(rawTraces)
	if err != nil {
		log.Printf("[v2] Error filling context from raw data (classified): %v, trace key: %s", err, traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() > 1 {
		log.Printf("[v2] More than 1 trace in context (classified), trace key: %s", traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() == 0 {
		log.Printf("[v2] No traces in context (classified), trace key: %s", traceExternalHashNorm)
		return
	}

	// Finality of trace is the minimum finality of its txs
	// NFT and jetton transfer notifications do not affect finality if there are no outgoing messages
	minFinality := emulated.FinalityStateFinalized
	{
		txRows := emulatedContext.GetTransactions()
		txFinality := make(map[string]emulated.FinalityState, len(txRows))
		txHashes := make([]string, 0, len(txRows))
		for _, row := range txRows {
			if tx, err := index.ScanTransaction(row); err == nil {
				txHash := string(tx.Hash)
				txHashes = append(txHashes, txHash)
				txFinality[txHash] = tx.Finality
			} else {
				log.Printf("[v2] Error scanning transaction (classified): %v", err)
			}
		}

		inOpcodes := make(map[string]index.OpcodeType, len(txFinality))
		outMsgCounts := make(map[string]int, len(txFinality))
		if len(txHashes) > 0 {
			for _, row := range emulatedContext.GetMessages(txHashes) {
				msg, err := index.ScanMessageWithContent(row)
				if err != nil {
					log.Printf("[v2] Error scanning message (classified): %v", err)
					continue
				}
				txHash := string(msg.TxHash)
				if msg.Direction == "in" {
					if msg.Opcode != nil {
						inOpcodes[txHash] = *msg.Opcode
					}
					continue
				}
				outMsgCounts[txHash]++
			}
		}

		for txHash, finality := range txFinality {
			if outMsgCounts[txHash] == 0 {
				if opcode, ok := inOpcodes[txHash]; ok {
					if opcode == jettonTransferNotificationOpcode || opcode == nftOwnershipAssignedNotificationOpcode {
						continue
					}
				}
			}
			if finality < minFinality {
				minFinality = finality
			}
		}
	}

	var actions []*index.Action
	var actionsAddresses [][]string

	for _, row := range emulatedContext.GetAllActions() { // ascending order
		var rawAction *index.RawAction
		if loc, err := index.ScanRawAction(row); err == nil {
			rawAction = loc
		} else {
			log.Printf("[v2] Error scanning raw action: %v", err)
			continue
		}

		actionAddrMap := map[string]bool{}
		index.CollectAddressesFromAction(&actionAddrMap, rawAction)

		action, err := index.ParseRawAction(rawAction)
		if err != nil {
			log.Printf("[v2] Error parsing raw action: %v", err)
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
	var allAddresses []string
	for _, aa := range actionsAddresses {
		allAddresses = append(allAddresses, aa...)
	}
	// For now, we emit "pending" actions and optionally a "finalized" snapshot.
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata(
		[]EventType{EventActions},
		emulated.FinalityStatePending,
		allAddresses,
	)
	if shouldFetchAddressBook || shouldFetchMetadata {
		addressBook, metadata = fetchAddressBookAndMetadata(
			ctx,
			allAddresses,
			allAddresses,
			shouldFetchAddressBook,
			shouldFetchMetadata,
		)
	}

	// Pending actions
	manager.broadcast <- &ActionsNotification{
		Type:                  EventActions,
		Finality:              minFinality,
		TraceExternalHashNorm: traceExternalHashNorm,
		Actions:               actions,
		ActionAddresses:       actionsAddresses,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}
}

////////////////////////////////////////////////////////////////////////////////
// Invalidated traces
////////////////////////////////////////////////////////////////////////////////

func SubscribeToInvalidatedTraces(ctx context.Context, rdb *redis.Client, manager *ClientManager, channel string) {
	pubsub := rdb.Subscribe(ctx, channel)
	defer pubsub.Close()

	log.Printf("[v2] Subscribed to Redis channel (invalidated traces): %s", channel)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("[v2] Error receiving invalidated trace message: %v", err)
			continue
		}

		traceExternalHashNorm := msg.Payload
		manager.broadcast <- &TraceInvalidatedNotification{
			Type:                  EventTraceInvalidated,
			TraceExternalHashNorm: traceExternalHashNorm,
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// Account state & jettons (confirmed, signed & finalized, no pending)
////////////////////////////////////////////////////////////////////////////////

func ProcessNewAccountStates(ctx context.Context, rdb *redis.Client, addr string, finality emulated.FinalityState, manager *ClientManager) {
	var key string
	switch finality {
	case emulated.FinalityStateConfirmed:
		key = fmt.Sprintf("account_confirmed:%s", addr)
	case emulated.FinalityStateSigned:
		key = fmt.Sprintf("account_signed:%s", addr)
	case emulated.FinalityStateFinalized:
		key = fmt.Sprintf("account_finalized:%s", addr)
	default:
		log.Printf("[v2] Unsupported finality %d for account state processing for %s", finality, addr)
		return
	}
	acctData, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		log.Printf("[v2] Error fetching account state for %s: %v", addr, err)
		return
	}
	var accountState models.AccountState
	err = msgpack.Unmarshal([]byte(acctData["state"]), &accountState)
	if err != nil {
		log.Printf("[v2] Error unmarshalling account state finality %v for %s: %v (%s)", finality, addr, err, acctData["state"])
		return
	}

	manager.broadcast <- &AccountStateNotification{
		Type:     EventAccountStateChange,
		Finality: finality,
		Account:  addr,
		State:    models.MsgPackAccountStateToIndexAccountState(accountState),
	}

	interfacesData := acctData["interfaces"]
	if interfacesData == "" {
		return
	}
	var addrInterfaces models.AddressInterfaces
	err = msgpack.Unmarshal([]byte(interfacesData), &addrInterfaces)
	if err != nil {
		log.Printf("[v2] Error unmarshalling address interfaces for %s: %v (%s)", addr, err, interfacesData)
		return
	}

	var notification *JettonsNotification

	for _, iface := range addrInterfaces.Interfaces {
		switch val := iface.Value.(type) {
		case *models.JettonWalletInterface:
			notification = &JettonsNotification{
				Type:     EventJettonsChange,
				Finality: finality,
				Jetton:   MsgPackJettonWalletToModel(*val, int64(*accountState.LastTransLt), models.ConvertHashToIndex(accountState.CodeHash), models.ConvertHashToIndex(accountState.DataHash)),
			}
		}
	}
	if notification == nil {
		return
	}

	addrBookAddresses := []string{notification.Jetton.Address.String(), notification.Jetton.Owner.String(), notification.Jetton.Jetton.String()}
	metadataAddresses := []string{notification.Jetton.Owner.String(), notification.Jetton.Jetton.String()}
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata(
		[]EventType{EventJettonsChange},
		finality,
		addrBookAddresses,
	)
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

////////////////////////////////////////////////////////////////////////////////
// Common HTTP / WS helpers
////////////////////////////////////////////////////////////////////////////////

type ErrorResponse struct {
	Id    *string `json:"id,omitempty"`
	Error string  `json:"error"`
}

type StatusResponse struct {
	Id     *string `json:"id,omitempty"`
	Status string  `json:"status"`
}

func ParseRateLimitHeaders(headers map[string][]string) (string, RateLimitConfig) {
	var limitingKey string
	config := RateLimitConfig{}

	if values, ok := headers["X-Limiting-Key"]; ok && len(values) > 0 {
		limitingKey = values[0]
	}
	if values, ok := headers["X-Max-Parallel-Connections"]; ok && len(values) > 0 {
		if maxConn, err := strconv.Atoi(values[0]); err == nil {
			config.MaxParallelConnections = maxConn
		}
	}
	if values, ok := headers["X-Max-Subscribed-Addr"]; ok && len(values) > 0 {
		if maxAddr, err := strconv.Atoi(values[0]); err == nil {
			config.MaxSubscribedAddresses = maxAddr
		}
	}

	return limitingKey, config
}

func convertAddress(s string) (string, error) {
	raw := index.AccountAddressConverter(s)
	if !raw.IsValid() {
		return "", fmt.Errorf("invalid address: %s", s)
	}
	return string(raw.Interface().(index.AccountAddress)), nil
}

func validateAddressesAndTypes(addresses []string, types []EventType) ([]string, error) {
	for _, t := range types {
		if _, ok := validEventTypes[t]; !ok {
			return nil, fmt.Errorf("invalid event type: %s", t)
		}
	}

	uniqueAddrs := make([]string, 0, len(addresses))
	addrsSet := make(map[string]struct{}, len(addresses))
	for _, a := range addresses {
		cnv, err := convertAddress(a)
		if err != nil {
			return nil, err
		}
		if _, exists := addrsSet[cnv]; exists {
			continue
		}
		addrsSet[cnv] = struct{}{}
		uniqueAddrs = append(uniqueAddrs, cnv)
	}

	return uniqueAddrs, nil
}

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

func sendWSJSONErr(c *websocket.Conn, id *string, err error) {
	if msg, e := json.Marshal(ErrorResponse{Id: id, Error: err.Error()}); e == nil {
		_ = c.WriteMessage(websocket.TextMessage, msg)
	} else {
		log.Printf("[v2] marshal error response: %v", e)
	}
}

func checkAddressLimit(client *Client, newAddresses int, rateLimiter *RateLimiter, toOverwrite bool) error {
	if client.LimitingKey == "" {
		return nil
	}

	maxAddresses := rateLimiter.GetAddressLimit(client.LimitingKey)
	if maxAddresses <= 0 {
		return nil
	}

	currentCount := len(client.Subscription.SubscribedAddresses)
	if toOverwrite {
		currentCount = 0
	}
	if currentCount+newAddresses > maxAddresses {
		return fmt.Errorf("address limit exceeded: current %d + new %d > max %d",
			currentCount, newAddresses, maxAddresses)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////
// SSE v2
////////////////////////////////////////////////////////////////////////////////

type SSERequest struct {
	Id                   *string                 `json:"id"`
	Addresses            []string                `json:"addresses"`
	Types                []EventType             `json:"types"`
	MinFinality          *emulated.FinalityState `json:"min_finality,omitempty"`
	ActionTypes          []string                `json:"action_types"`
	SupportedActionTypes []string                `json:"supported_action_types"`
	IncludeAddressBook   *bool                   `json:"include_address_book"`
	IncludeMetadata      *bool                   `json:"include_metadata"`
}

func ValidateSSERequest(req *SSERequest) ([]string, emulated.FinalityState, error) {
	if len(req.Addresses) == 0 {
		return nil, defaultMinFinality(), fmt.Errorf("addresses are required for subscription")
	}
	if len(req.Types) == 0 {
		return nil, defaultMinFinality(), fmt.Errorf("types are required for subscription")
	}

	uniqueAddrs, err := validateAddressesAndTypes(req.Addresses, req.Types)
	if err != nil {
		return nil, defaultMinFinality(), err
	}

	minFin := defaultMinFinality()
	if req.MinFinality != nil {
		minFin = *req.MinFinality
	}

	return uniqueAddrs, minFin, nil
}

func SSEHandler(manager *ClientManager) fiber.Handler {
	return func(c *fiber.Ctx) error {
		var req SSERequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{Error: fmt.Sprintf("invalid subscription request: %v", err)})
		}
		addresses, minFinality, err := ValidateSSERequest(&req)
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

		// Rate limiting
		limitingKey, rateLimitConfig := ParseRateLimitHeaders(c.GetReqHeaders())
		clientID := fmt.Sprintf("%s-%s", c.IP(), time.Now().Format(time.RFC3339Nano))

		if limitingKey != "" {
			if err := manager.rateLimiter.RegisterConnection(limitingKey, clientID, rateLimitConfig); err != nil {
				return c.Status(fiber.StatusTooManyRequests).JSON(ErrorResponse{
					Id:    req.Id,
					Error: err.Error(),
				})
			}
			if rateLimitConfig.MaxSubscribedAddresses != -1 && len(addresses) > rateLimitConfig.MaxSubscribedAddresses {
				manager.rateLimiter.UnregisterConnection(limitingKey, clientID)
				return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{
					Id:    req.Id,
					Error: fmt.Sprintf("too many addresses: %d > max %d", len(addresses), rateLimitConfig.MaxSubscribedAddresses),
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
				MinFinality:          minFinality,
			},
			TracesForPotentialInvalidation: make(map[string]bool),
			SendEvent: func(b []byte) error {
				select {
				case eventCh <- b:
					return nil
				default:
					return nil
				}
			},
		}
		client.Subscription.Replace(addresses, req.Types)
		manager.register <- client

		c.Set("Content-Type", "text/event-stream")
		c.Set("Cache-Control", "no-cache")
		c.Set("Connection", "keep-alive")
		c.Set("Transfer-Encoding", "chunked")

		c.Status(fiber.StatusOK).Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
			if err := writeSSE(w, "connected", StatusResponse{Id: req.Id, Status: "subscribed"}); err != nil {
				log.Printf("[v2] write connected frame: %v", err)
				return
			}
			log.Printf("[v2] Client %s connected via SSE", clientID)

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

////////////////////////////////////////////////////////////////////////////////
// WebSocket v2
////////////////////////////////////////////////////////////////////////////////

type Operation string

const (
	OpPing        Operation = "ping"
	OpSubscribe   Operation = "subscribe" // replaces entire subscription snapshot
	OpUnsubscribe Operation = "unsubscribe"
)

type Envelope struct {
	Id        *string   `json:"id"`
	Operation Operation `json:"operation"`
}

type SubscribeRequest struct {
	Addresses            []string                `json:"addresses"`
	Types                []EventType             `json:"types"`
	MinFinality          *emulated.FinalityState `json:"min_finality,omitempty"`
	ActionTypes          []string                `json:"action_types,omitempty"`
	SupportedActionTypes []string                `json:"supported_action_types,omitempty"`
	IncludeAddressBook   *bool                   `json:"include_address_book,omitempty"`
	IncludeMetadata      *bool                   `json:"include_metadata,omitempty"`
}

type UnsubscribeRequest struct {
	Addresses []string `json:"addresses"`
}

func WebSocketHandler(manager *ClientManager) func(*websocket.Conn) {
	return func(c *websocket.Conn) {
		// headers from upgrade
		headers := make(map[string][]string)
		headers["X-Limiting-Key"] = []string{c.Headers("X-Limiting-Key")}
		headers["X-Max-Parallel-Connections"] = []string{c.Headers("X-Max-Parallel-Connections")}
		headers["X-Max-Subscribed-Addr"] = []string{c.Headers("X-Max-Subscribed-Addr")}
		headers["X-Actions-Version"] = []string{c.Headers("X-Actions-Version", "latest")}

		limitingKey, rateLimitConfig := ParseRateLimitHeaders(headers)
		clientID := fmt.Sprintf("%s-%s", c.RemoteAddr(), time.Now().Format(time.RFC3339Nano))

		if limitingKey != "" {
			if err := manager.rateLimiter.RegisterConnection(limitingKey, clientID, rateLimitConfig); err != nil {
				sendWSJSONErr(c, nil, err)
				_ = c.Close()
				return
			}
		}

		client := &Client{
			ID:          clientID,
			LimitingKey: limitingKey,
			Connected:   true,
			Subscription: Subscription{
				SubscribedAddresses:  make(AddressSet),
				EventTypes:           make(eventSet),
				SupportedActionTypes: index.ExpandActionTypeShortcuts(headers["X-Actions-Version"]),
				IncludeAddressBook:   false,
				IncludeMetadata:      false,
				MinFinality:          defaultMinFinality(),
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
				log.Printf("[v2] ws read: %v", err)
				return
			}

			var env Envelope
			if err := json.Unmarshal(msg, &env); err != nil {
				sendWSJSONErr(c, nil, fmt.Errorf("invalid request envelope: %v", err))
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
				if len(req.Addresses) == 0 {
					sendWSJSONErr(c, env.Id, fmt.Errorf("addresses are required"))
					continue
				}
				if len(req.Types) == 0 {
					sendWSJSONErr(c, env.Id, fmt.Errorf("types are required"))
					continue
				}

				cnvAddrs, err := validateAddressesAndTypes(req.Addresses, req.Types)
				if err != nil {
					sendWSJSONErr(c, env.Id, err)
					continue
				}

				minFin := client.Subscription.MinFinality
				if req.MinFinality != nil {
					minFin = *req.MinFinality
				}

				client.mu.Lock()
				err = checkAddressLimit(client, len(cnvAddrs), manager.rateLimiter, true)
				if err != nil {
					client.mu.Unlock()
					sendWSJSONErr(c, env.Id, err)
					continue
				}

				client.Subscription.Replace(cnvAddrs, req.Types)
				client.Subscription.MinFinality = minFin

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

				ack, _ := json.Marshal(StatusResponse{Id: env.Id, Status: "subscribed"})
				_ = c.WriteMessage(websocket.TextMessage, ack)

			default:
				sendWSJSONErr(c, env.Id, fmt.Errorf("unknown operation: %s", env.Operation))
			}
		}
	}
}
