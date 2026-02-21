package v2

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/toncenter/ton-indexer/ton-index-go/index/crud"
	"github.com/toncenter/ton-indexer/ton-index-go/index/detect"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"
	"log"
	"math/rand/v2"
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
	"github.com/toncenter/ton-indexer/ton-index-go/index/emulated"
	
)

////////////////////////////////////////////////////////////////////////////////
// Config
////////////////////////////////////////////////////////////////////////////////

// Config provides runtime dependencies for v2 handlers.
type Config struct {
	DBClient        *crud.DbClient
	Testnet         bool
	ImgProxyBaseURL string
}

var config Config

// InitConfig registers configuration to be used by v2 handlers.
func InitConfig(cfg Config) {
	config = cfg
}

////////////////////////////////////////////////////////////////////////////////
// Debug measurements
////////////////////////////////////////////////////////////////////////////////

type Measurement struct {
	Id              int64
	ExtMsgHashNorm  indexModels.HashType
	ExtMsgHash      indexModels.HashType
	TraceRootTxHash indexModels.HashType
	Timings         map[string]float64
	Extra           map[string]string
}
type MeasurementRow struct {
	Id              int64                `json:"id"`
	ExtMsgHashNorm  indexModels.HashType `json:"msg_hash_norm"`
	ExtMsgHash      indexModels.HashType `json:"msg_hash"`
	TraceRootTxHash indexModels.HashType `json:"trace_id"`
	Step            string               `json:"step"`
	Time            float64              `json:"time"`
	Extra           *map[string]string   `json:"extra"`
}

func NewMeasurement() *Measurement {
	return &Measurement{
		ExtMsgHashNorm:  "-",
		ExtMsgHash:      "-",
		TraceRootTxHash: "-",
		Timings:         make(map[string]float64),
		Extra:           make(map[string]string),
	}
}

func (m *Measurement) PrintMeasurement() {
	traceId := m.TraceRootTxHash
	if m.Id == -1 {
		traceId = "-"
	}

	for step, elapsed := range m.Timings {
		row := MeasurementRow{
			Id:              m.Id,
			ExtMsgHashNorm:  m.ExtMsgHashNorm,
			ExtMsgHash:      m.ExtMsgHash,
			TraceRootTxHash: traceId,
			Step:            step,
			Time:            elapsed,
			Extra:           &m.Extra,
		}
		json_str, _ := json.Marshal(row)
		log.Printf("[v2] MEASURE %s END", json_str)
	}
}

func (m *Measurement) MeasureStep(step string) {
	elapsed := float64(time.Now().UnixNano()) / 1e9
	m.Timings[step] = elapsed
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
	EventTrace              EventType = "trace"
	EventAccountStateChange EventType = "account_state_change"
	EventJettonsChange      EventType = "jettons_change"
	EventTraceInvalidated   EventType = "trace_invalidated" // internal; not subscribable
)

var validEventTypes = map[EventType]struct{}{
	EventTransactions:       {},
	EventActions:            {},
	EventTrace:              {},
	EventAccountStateChange: {},
	EventJettonsChange:      {},
}

const (
	jettonTransferNotificationOpcode       string = "0x7362d09c"
	nftOwnershipAssignedNotificationOpcode string = "0x05138d91"
	excessesOpcode                         string = "0xd53276db"
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
type TraceSet map[string]struct{}

type Subscription struct {
	SubscribedAddresses  AddressSet
	SubscribedTraces     TraceSet
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

func makeTraceSet(traces []string) TraceSet {
	s := make(TraceSet, len(traces))
	for _, trace := range traces {
		s[trace] = struct{}{}
	}
	return s
}

func (s *Subscription) Replace(addresses []string, eventTypes []EventType) {
	s.SubscribedAddresses = makeAddressSet(addresses)
	s.EventTypes = makeEventSet(eventTypes)
}

func (s *Subscription) ReplaceTraces(traces []string) {
	s.SubscribedTraces = makeTraceSet(traces)
}

func (s *Subscription) Unsubscribe(addresses []string) {
	for _, addr := range addresses {
		delete(s.SubscribedAddresses, addr)
	}
}

func (s *Subscription) UnsubscribeTraces(traces []string) {
	for _, trace := range traces {
		delete(s.SubscribedTraces, trace)
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

func (s *Subscription) InterestedInTrace(eventType EventType, traceExternalHashNorm string) bool {
	if s.EventTypes == nil {
		return false
	}
	if _, ok := s.EventTypes[eventType]; !ok {
		return false
	}

	if s.SubscribedTraces == nil {
		return false
	}
	_, ok := s.SubscribedTraces[traceExternalHashNorm]
	return ok
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
		register:    make(chan *Client, 128),
		unregister:  make(chan *Client, 128),
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

// shouldFetchAddressBookAndMetadataForTrace checks if any connected client
// subscribed to the trace will receive this event and needs address book or metadata.
func (manager *ClientManager) shouldFetchAddressBookAndMetadataForTrace(eventFinality emulated.FinalityState, traceExternalHashNorm string) (bool, bool) {
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
			if client.Subscription.InterestedInTrace(EventTrace, traceExternalHashNorm) {
				shouldFetchAddressBook = shouldFetchAddressBook || client.Subscription.IncludeAddressBook
				shouldFetchMetadata = shouldFetchMetadata || client.Subscription.IncludeMetadata
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

func fetchAddressBookAndMetadata(ctx context.Context, addrBookAddresses []string, metadataAddresses []string, includeAddressBook bool, includeMetadata bool) (*indexModels.AddressBook, *indexModels.Metadata) {
	var addressBook *indexModels.AddressBook
	var metadata *indexModels.Metadata

	if config.DBClient == nil {
		return nil, nil
	}

	conn, err := config.DBClient.Pool.Acquire(ctx)
	if err != nil {
		log.Printf("[v2] Error acquiring DB connection: %v", err)
		return nil, nil
	}
	defer conn.Release()

	settings := indexModels.RequestSettings{
		Timeout:   3 * time.Second,
		IsTestnet: config.Testnet,
	}

	if includeAddressBook {
		book, err := crud.QueryAddressBookImpl(addrBookAddresses, conn, settings)
		if err != nil {
			log.Printf("[v2] Error querying address book: %v", err)
		} else {
			addressBook = &book
		}
	}

	if includeMetadata {
		meta, err := crud.QueryMetadataImpl(metadataAddresses, conn, settings)
		if err != nil {
			log.Printf("[v2] Error querying metadata: %v", err)
		} else {
			if config.ImgProxyBaseURL != "" {
				crud.SubstituteImgproxyBaseUrl(&meta, config.ImgProxyBaseURL)
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
	Type                  EventType                `json:"type"` // always "actions"
	Finality              emulated.FinalityState   `json:"finality,string"`
	TraceExternalHashNorm string                   `json:"trace_external_hash_norm"`
	Actions               []*indexModels.Action    `json:"actions"`
	ActionAddresses       [][]string               `json:"-"` // used internally
	AddressBook           *indexModels.AddressBook `json:"address_book,omitempty"`
	Metadata              *indexModels.Metadata    `json:"metadata,omitempty"`
}

var _ Notification = (*ActionsNotification)(nil)

func (n *ActionsNotification) AdjustForClient(client *Client) any {
	// Finality filter
	if n.Finality < client.Subscription.MinFinality {
		return nil
	}

	var adjustedActions []*indexModels.Action
	var adjustedActionAddresses [][]string
	var adjustedAddressBook *indexModels.AddressBook
	var adjustedMetadata *indexModels.Metadata
	if n.AddressBook != nil {
		adjustedAddressBook = &indexModels.AddressBook{}
	}
	if n.Metadata != nil {
		adjustedMetadata = &indexModels.Metadata{}
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
	if n.Finality == emulated.FinalityStateFinalized {
		delete(client.TracesForPotentialInvalidation, n.TraceExternalHashNorm)
	} else {
		client.TracesForPotentialInvalidation[n.TraceExternalHashNorm] = true
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
	Type                  EventType                 `json:"type"` // always "transactions"
	Finality              emulated.FinalityState    `json:"finality"`
	TraceExternalHashNorm string                    `json:"trace_external_hash_norm"`
	Transactions          []indexModels.Transaction `json:"transactions"`
	AddressBook           *indexModels.AddressBook  `json:"address_book,omitempty"`
	Metadata              *indexModels.Metadata     `json:"metadata,omitempty"`
}

var _ Notification = (*TransactionsNotification)(nil)

func (n *TransactionsNotification) AdjustForClient(client *Client) any {
	// Finality filter
	if n.Finality < client.Subscription.MinFinality {
		return nil
	}

	var adjustedTransactions []indexModels.Transaction
	var adjustedAddressBook *indexModels.AddressBook
	var adjustedMetadata *indexModels.Metadata
	if n.AddressBook != nil {
		adjustedAddressBook = &indexModels.AddressBook{}
	}
	if n.Metadata != nil {
		adjustedMetadata = &indexModels.Metadata{}
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

	if n.Finality == emulated.FinalityStateFinalized {
		delete(client.TracesForPotentialInvalidation, n.TraceExternalHashNorm)
	} else {
		client.TracesForPotentialInvalidation[n.TraceExternalHashNorm] = true
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

type TraceNotification struct {
	Type                  EventType                                         `json:"type"` // always "trace"
	Finality              emulated.FinalityState                            `json:"finality"`
	TraceExternalHashNorm string                                            `json:"trace_external_hash_norm"`
	Trace                 indexModels.TraceNode                             `json:"trace"`
	Transactions          map[indexModels.HashType]*indexModels.Transaction `json:"transactions"`
	Actions               *[]*indexModels.Action                            `json:"actions,omitempty"`
	AddressBook           *indexModels.AddressBook                          `json:"address_book,omitempty"`
	Metadata              *indexModels.Metadata                             `json:"metadata,omitempty"`
}

var _ Notification = (*TraceNotification)(nil)

func (n *TraceNotification) AdjustForClient(client *Client) any {
	if n.Finality < client.Subscription.MinFinality {
		return nil
	}
	if !client.Subscription.InterestedInTrace(EventTrace, n.TraceExternalHashNorm) {
		return nil
	}

	var adjustedActions *[]*indexModels.Action
	if n.Actions != nil {
		supportedActionsSet := mapset.NewSet(client.Subscription.SupportedActionTypes...)
		filterActionsSet := mapset.NewSet(client.Subscription.ActionTypes...)
		filteredActions := make([]*indexModels.Action, 0, len(*n.Actions))
		for _, action := range *n.Actions {
			if !filterActionsSet.IsEmpty() && !filterActionsSet.ContainsAny(action.Type) {
				continue
			}
			if supportedActionsSet.ContainsAny(action.AncestorType...) {
				continue
			}
			if !supportedActionsSet.ContainsAny(action.Type) {
				continue
			}
			filteredActions = append(filteredActions, action)
		}
		if len(filteredActions) > 0 {
			adjustedActions = &filteredActions
		}
	}

	if n.Finality == emulated.FinalityStateFinalized {
		delete(client.TracesForPotentialInvalidation, n.TraceExternalHashNorm)
	} else {
		client.TracesForPotentialInvalidation[n.TraceExternalHashNorm] = true
	}

	return &TraceNotification{
		Type:                  n.Type,
		Finality:              n.Finality,
		TraceExternalHashNorm: n.TraceExternalHashNorm,
		Trace:                 n.Trace,
		Transactions:          n.Transactions,
		Actions:               adjustedActions,
		AddressBook:           n.AddressBook,
		Metadata:              n.Metadata,
	}
}

type AccountStateNotification struct {
	Type     EventType                `json:"type"`
	Finality emulated.FinalityState   `json:"finality"` // confirmed / finalized
	Account  string                   `json:"account"`
	State    indexModels.AccountState `json:"state"`
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
	Type        EventType                `json:"type"`
	Finality    emulated.FinalityState   `json:"finality"` // confirmed / finalized
	Jetton      indexModels.JettonWallet `json:"jetton"`
	AddressBook *indexModels.AddressBook `json:"address_book,omitempty"`
	Metadata    *indexModels.Metadata    `json:"metadata,omitempty"`
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
	m := NewMeasurement()
	m.Extra["redis_conns"] = strconv.Itoa(int(rdb.PoolStats().TotalConns - rdb.PoolStats().IdleConns))
	m.ExtMsgHashNorm = indexModels.HashType(traceExternalHashNorm)
	m.MeasureStep("process_new_trace__start")

	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	rawTraces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("[v2] Error loading raw traces (pending): %v, trace key: %s", err, traceExternalHashNorm)
		return
	}
	m.Id, err = strconv.ParseInt(rawTraces[traceExternalHashNorm]["measurement_id"], 10, 64)
	if err != nil {
		m.Id = rand.Int64()
	}
	m.ExtMsgHash = indexModels.HashType(rawTraces[traceExternalHashNorm]["root_node"])
	m.MeasureStep("process_new_trace__raw_traces_loaded")

	emulatedContext := crud.NewEmptyContext(false)
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

	var txs []indexModels.Transaction
	txsMap := map[indexModels.HashType]int{}
	{
		rows := emulatedContext.GetTransactions()
		for _, row := range rows {
			if tx, err := parse.ScanTransaction(row); err == nil {
				txs = append(txs, *tx)
				txsMap[tx.Hash] = len(txs) - 1
				if m.TraceRootTxHash == "-" && tx.TraceId != nil {
					m.TraceRootTxHash = *tx.TraceId
				}
			} else {
				log.Printf("[v2] Error scanning transaction (pending): %v", err)
			}
		}
	}
	m.MeasureStep("process_new_trace__transactions_parsed")

	allAddresses := []string{}
	var txHashes []string
	for _, t := range txs {
		txHashes = append(txHashes, string(t.Hash))
		allAddresses = append(allAddresses, string(t.Account))
	}

	if len(txHashes) > 0 {
		rows := emulatedContext.GetMessages(txHashes)
		msgPtrs := make([]*indexModels.Message, 0, len(rows))
		for _, row := range rows {
			msg, err := parse.ScanMessageWithContent(row)
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
		if err := detect.MarkMessagesByPtr(msgPtrs); err != nil {
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
	m.MeasureStep("process_new_trace__messages_marked")

	var addressBook *indexModels.AddressBook
	var metadata *indexModels.Metadata
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
	m.MeasureStep("process_new_trace__address_book_and_metadata_fetched")
	m.PrintMeasurement()

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

// ProcessNewConfirmedTxs now emits a FULL TRACE SNAPSHOT (not a fragment).
// txHashes is ignored for payload building; it only serves as a "trace changed" signal.
func ProcessNewConfirmedTxs(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, txHashes []string, manager *ClientManager) {
	processTransactionsTraceSnapshot(ctx, rdb, traceExternalHashNorm, manager, "confirmed")
}

func ProcessNewFinalizedTxs(ctx context.Context, rdb *redis.Client, traceExternalHashNorm string, txHashes []string, manager *ClientManager) {
	processTransactionsTraceSnapshot(ctx, rdb, traceExternalHashNorm, manager, "finalized")
}

// processTransactionsTraceSnapshot loads the entire trace from Redis,
// attaches messages, sorts, computes TRACE-level finality (min tx finality),
// fetches address_book/metadata only if at least one eligible client needs them,
// and broadcasts a single TransactionsNotification snapshot.
// channelHint is only used for logging/debugging.
func processTransactionsTraceSnapshot(
	ctx context.Context,
	rdb *redis.Client,
	traceExternalHashNorm string,
	manager *ClientManager,
	channelHint string,
) {
	m := NewMeasurement()
	m.Extra["redis_conns"] = strconv.Itoa(int(rdb.PoolStats().TotalConns - rdb.PoolStats().IdleConns))
	m.ExtMsgHashNorm = indexModels.HashType(traceExternalHashNorm)
	m.MeasureStep("process_new_" + channelHint + "_transactions__start")

	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	rawTraces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("[v2] Error loading raw traces (%s): %v, trace key: %s", channelHint, err, traceExternalHashNorm)
		return
	}
	m.Id, err = strconv.ParseInt(rawTraces[traceExternalHashNorm]["measurement_id"], 10, 64)
	if err != nil {
		m.Id = rand.Int64()
	}
	m.ExtMsgHash = indexModels.HashType(rawTraces[traceExternalHashNorm]["root_node"])
	m.MeasureStep("process_new_" + channelHint + "_transactions__raw_traces_loaded")

	emulatedContext := crud.NewEmptyContext(false)
	if err := emulatedContext.FillFromRawData(rawTraces); err != nil {
		log.Printf("[v2] Error filling context from raw data (%s): %v, trace key: %s", channelHint, err, traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() > 1 {
		log.Printf("[v2] More than 1 trace in context (%s), trace key: %s", channelHint, traceExternalHashNorm)
		return
	}
	if emulatedContext.GetTraceCount() == 0 {
		log.Printf("[v2] No traces in context (%s), trace key: %s", channelHint, traceExternalHashNorm)
		return
	}

	// Build FULL snapshot of all transactions in this trace.
	var txs []indexModels.Transaction
	txsMap := map[indexModels.HashType]int{} // txHash -> index in txs slice

	// Compute trace-level finality as min(tx.Finality).
	// Start from the highest state.
	traceFinality := emulated.FinalityStateFinalized

	rows := emulatedContext.GetTransactions()
	for _, row := range rows {
		tx, scanErr := parse.ScanTransaction(row)
		if scanErr != nil {
			log.Printf("[v2] Error scanning transaction (%s): %v", channelHint, scanErr)
			continue
		}

		txs = append(txs, *tx)
		txsMap[tx.Hash] = len(txs) - 1

		if m.TraceRootTxHash == "-" && tx.TraceId != nil {
			m.TraceRootTxHash = *tx.TraceId
		}

		if tx.Finality < traceFinality {
			traceFinality = tx.Finality
		}
	}
	m.MeasureStep("process_new_" + channelHint + "_transactions__transactions_parsed")

	if len(txs) == 0 {
		log.Printf("[v2] No transactions found for trace %s (%s)", traceExternalHashNorm, channelHint)
		return
	}

	// Collect tx hashes & initial addresses (accounts).
	hashes := make([]string, 0, len(txs))
	allAddresses := make([]string, 0, len(txs)*3)

	for _, t := range txs {
		hashes = append(hashes, string(t.Hash))
		allAddresses = append(allAddresses, string(t.Account))
	}

	// Attach messages for ALL transactions in snapshot.
	if len(hashes) > 0 {
		msgRows := emulatedContext.GetMessages(hashes)
		for _, row := range msgRows {
			msg, scanErr := parse.ScanMessageWithContent(row)
			if scanErr != nil {
				log.Printf("[v2] Error scanning message (%s): %v", channelHint, scanErr)
				continue
			}

			txIdx, ok := txsMap[msg.TxHash]
			if !ok {
				log.Printf("[v2] Message for unknown transaction (%s), tx hash: %s", channelHint, msg.TxHash)
				continue
			}

			if msg.Direction == "in" {
				txs[txIdx].InMsg = msg
				if msg.Source != nil {
					allAddresses = append(allAddresses, string(*msg.Source))
				}
			} else {
				txs[txIdx].OutMsgs = append(txs[txIdx].OutMsgs, msg)
				if msg.Destination != nil {
					allAddresses = append(allAddresses, string(*msg.Destination))
				}
			}
		}
	}

	// Sort OutMsgs by CreatedLt ascending
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
	m.MeasureStep("process_new_" + channelHint + "_transactions__messages_marked")

	// Sort transactions by Lt descending (as documented).
	sort.Slice(txs, func(i, j int) bool {
		return txs[i].Lt > txs[j].Lt
	})

	// Fetch address_book/metadata only if at least one eligible client needs it,
	// and only if that client will receive this event with traceFinality.
	var addressBook *indexModels.AddressBook
	var metadata *indexModels.Metadata

	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata(
		[]EventType{EventTransactions},
		traceFinality,
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

	m.MeasureStep("process_new_" + channelHint + "_transactions__address_book_and_metadata_fetched")
	m.PrintMeasurement()

	manager.broadcast <- &TransactionsNotification{
		Type:                  EventTransactions,
		Finality:              traceFinality,
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
	m := NewMeasurement()
	m.Extra["redis_conns"] = strconv.Itoa(int(rdb.PoolStats().TotalConns - rdb.PoolStats().IdleConns))
	m.ExtMsgHashNorm = indexModels.HashType(traceExternalHashNorm)
	m.MeasureStep("process_new_classified_trace__start")

	repository := &emulated.EmulatedTracesRepository{Rdb: rdb}
	rawTraces, err := repository.LoadRawTraces([]string{traceExternalHashNorm})
	if err != nil {
		log.Printf("[v2] Error loading raw traces (classified): %v, trace key: %s", err, traceExternalHashNorm)
		return
	}
	m.Id, err = strconv.ParseInt(rawTraces[traceExternalHashNorm]["measurement_id"], 10, 64)
	if err != nil {
		m.Id = rand.Int64()
	}
	m.ExtMsgHash = indexModels.HashType(rawTraces[traceExternalHashNorm]["root_node"])
	m.MeasureStep("process_new_classified_trace__raw_traces_loaded")

	emulatedContext := crud.NewEmptyContext(false)
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

	// log.Print("[v2] Processing classified trace: ", traceExternalHashNorm)

	// Finality of trace is the minimum finality of its txs, however,
	// NFT and jetton transfer notifications do not affect finality if there are no outgoing messages
	traceFinality := emulated.FinalityStateFinalized
	{
		txRows := emulatedContext.GetTransactions()
		txFinality := make(map[string]emulated.FinalityState, len(txRows))
		txHashes := make([]string, 0, len(txRows))
		for _, row := range txRows {
			if tx, err := parse.ScanTransaction(row); err == nil {
				txHash := string(tx.Hash)
				txHashes = append(txHashes, txHash)
				txFinality[txHash] = tx.Finality
				if m.TraceRootTxHash == "-" && tx.TraceId != nil {
					m.TraceRootTxHash = *tx.TraceId
				}
			} else {
				log.Printf("[v2] Error scanning transaction (classified): %v", err)
			}
		}

		inOpcodes := make(map[string]string, len(txFinality))
		outMsgCounts := make(map[string]int, len(txFinality))
		if len(txHashes) > 0 {
			for _, row := range emulatedContext.GetMessages(txHashes) {
				msg, err := parse.ScanMessageWithContent(row)
				if err != nil {
					log.Printf("[v2] Error scanning message (classified): %v", err)
					continue
				}
				txHash := string(msg.TxHash)
				if msg.Direction == "in" {
					if msg.Opcode != nil {
						inOpcodes[txHash] = (*msg.Opcode).String()
					}
					continue
				} else {
					outMsgCounts[txHash]++
				}
			}
		}

		for txHash, finality := range txFinality {
			if outMsgCounts[txHash] == 0 {
				if opcode, ok := inOpcodes[txHash]; ok {
					if opcode == jettonTransferNotificationOpcode || opcode == nftOwnershipAssignedNotificationOpcode || opcode == excessesOpcode {
						continue
					}
				}
			}
			if finality < traceFinality {
				traceFinality = finality
			}
		}
	}

	var actions []*indexModels.Action
	var actionsAddresses [][]string

	for _, row := range emulatedContext.GetAllActions() { // ascending order
		var rawAction *indexModels.RawAction
		if loc, err := parse.ScanRawAction(row); err == nil {
			rawAction = loc
		} else {
			log.Printf("[v2] Error scanning raw action: %v", err)
			continue
		}

		actionAddrMap := map[string]bool{}
		parse.CollectAddressesFromAction(&actionAddrMap, rawAction)

		action, err := parse.ParseRawAction(rawAction)
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
	m.MeasureStep("process_new_classified_trace__actions_parsed")

	var addressBook *indexModels.AddressBook
	var metadata *indexModels.Metadata
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
	m.MeasureStep("process_new_classified_trace__address_book_and_metadata_fetched")
	m.PrintMeasurement()

	log.Printf("[v2] Broadcasting classified trace actions for trace %s with finality %d: %d actions, addresses: %v", traceExternalHashNorm, traceFinality, len(actions), actionsAddresses)
	// Pending actions
	manager.broadcast <- &ActionsNotification{
		Type:                  EventActions,
		Finality:              traceFinality,
		TraceExternalHashNorm: traceExternalHashNorm,
		Actions:               actions,
		ActionAddresses:       actionsAddresses,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}

	txs, err := crud.QueryPendingTransactionsImpl(emulatedContext, nil, indexModels.RequestSettings{}, false)
	if err != nil {
		log.Printf("[v2] Error querying trace transactions (classified): %v", err)
		return
	}

	txOrder := make([]indexModels.HashType, 0, len(txs))
	for idx := range txs {
		txOrder = append(txOrder, txs[idx].Hash)
	}

	traceRoot, traceTxMap, err := buildTraceFromTransactions(txOrder, txs)
	if err != nil {
		log.Printf("[v2] Error assembling trace (classified) %s: %v", traceExternalHashNorm, err)
	}
	if traceRoot == nil {
		return
	}

	traceAddrSet := map[string]bool{}
	for idx := range txs {
		collectAddressesFromTransaction(traceAddrSet, &txs[idx])
	}
	for _, aa := range actionsAddresses {
		for _, addr := range aa {
			traceAddrSet[addr] = true
		}
	}
	traceAddresses := make([]string, 0, len(traceAddrSet))
	for addr := range traceAddrSet {
		traceAddresses = append(traceAddresses, addr)
	}

	var traceAddressBook *indexModels.AddressBook
	var traceMetadata *indexModels.Metadata
	shouldFetchTraceAddressBook, shouldFetchTraceMetadata := manager.shouldFetchAddressBookAndMetadataForTrace(
		traceFinality,
		traceExternalHashNorm,
	)
	if shouldFetchTraceAddressBook || shouldFetchTraceMetadata {
		traceAddressBook, traceMetadata = fetchAddressBookAndMetadata(
			ctx,
			traceAddresses,
			traceAddresses,
			shouldFetchTraceAddressBook,
			shouldFetchTraceMetadata,
		)
	}

	var traceActionsPtr *[]*indexModels.Action
	if len(actions) > 0 {
		traceActionsPtr = &actions
	}

	manager.broadcast <- &TraceNotification{
		Type:                  EventTrace,
		Finality:              traceFinality,
		TraceExternalHashNorm: traceExternalHashNorm,
		Trace:                 *traceRoot,
		Transactions:          traceTxMap,
		Actions:               traceActionsPtr,
		AddressBook:           traceAddressBook,
		Metadata:              traceMetadata,
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
// Account state & jettons (confirmed & finalized, no pending)
////////////////////////////////////////////////////////////////////////////////

func ProcessNewAccountStates(ctx context.Context, rdb *redis.Client, addr string, finality emulated.FinalityState, manager *ClientManager) {
	var key string
	switch finality {
	case emulated.FinalityStateConfirmed:
		key = fmt.Sprintf("account_confirmed:%s", addr)
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

func MsgPackJettonWalletToModel(j models.JettonWalletInterface, lastTransLt int64, codeHash *indexModels.HashType, dataHash *indexModels.HashType) indexModels.JettonWallet {
	return indexModels.JettonWallet{
		Address:           indexModels.AccountAddress(j.Address),
		Balance:           j.Balance,
		Owner:             indexModels.AccountAddress(j.Owner),
		Jetton:            indexModels.AccountAddress(j.Jetton),
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
	raw := indexModels.AccountAddressConverter(s)
	if !raw.IsValid() {
		return "", fmt.Errorf("invalid address: %s", s)
	}
	return string(raw.Interface().(indexModels.AccountAddress)), nil
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

func validateTraceExternalHashNorms(traces []string) ([]string, error) {
	unique := make([]string, 0, len(traces))
	seen := make(map[string]struct{}, len(traces))
	for _, trace := range traces {
		trace = strings.TrimSpace(trace)
		if trace == "" {
			return nil, fmt.Errorf("trace_external_hash_norms contains empty value")
		}
		if _, exists := seen[trace]; exists {
			continue
		}
		seen[trace] = struct{}{}
		unique = append(unique, trace)
	}
	return unique, nil
}

func hasEventType(types []EventType, target EventType) bool {
	for _, t := range types {
		if t == target {
			return true
		}
	}
	return false
}

func hasNonTraceEventTypes(types []EventType) bool {
	for _, t := range types {
		if t != EventTrace {
			return true
		}
	}
	return false
}

func collectAddressesFromTransaction(addrSet map[string]bool, tx *indexModels.Transaction) {
	addrSet[string(tx.Account)] = true
	if tx.InMsg != nil && tx.InMsg.Source != nil {
		addrSet[string(*tx.InMsg.Source)] = true
	}
	for _, outMsg := range tx.OutMsgs {
		if outMsg.Destination != nil {
			addrSet[string(*outMsg.Destination)] = true
		}
	}
}

func buildActionsFromContext(emulatedContext *crud.EmulatedTracesContext) ([]*indexModels.Action, [][]string) {
	actions := make([]*indexModels.Action, 0)
	actionsAddresses := make([][]string, 0)
	for _, row := range emulatedContext.GetAllActions() {
		rawAction, err := parse.ScanRawAction(row)
		if err != nil {
			log.Printf("[v2] Error scanning raw action: %v", err)
			continue
		}

		actionAddrMap := map[string]bool{}
		parse.CollectAddressesFromAction(&actionAddrMap, rawAction)

		action, err := parse.ParseRawAction(rawAction)
		if err != nil {
			log.Printf("[v2] Error parsing raw action: %v", err)
			continue
		}

		actionAddresses := make([]string, 0, len(actionAddrMap))
		for addr := range actionAddrMap {
			actionAddresses = append(actionAddresses, addr)
		}

		actions = append(actions, action)
		actionsAddresses = append(actionsAddresses, actionAddresses)
	}

	return actions, actionsAddresses
}

func buildTraceFromTransactions(txOrder []indexModels.HashType, txs []indexModels.Transaction) (*indexModels.TraceNode, map[indexModels.HashType]*indexModels.Transaction, error) {
	txMap := make(map[indexModels.HashType]*indexModels.Transaction, len(txs))
	for idx := range txs {
		tx := &txs[idx]
		txMap[tx.Hash] = tx
	}

	traceRoot, err := parse.AssembleTraceTxsFromMap(&txOrder, &txMap)
	if err != nil {
		return traceRoot, txMap, err
	}
	if traceRoot == nil {
		return nil, txMap, fmt.Errorf("trace root is nil")
	}
	return traceRoot, txMap, nil
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

func writeWSMessage(c *websocket.Conn, client *Client, msg []byte) error {
	if client == nil {
		return c.WriteMessage(websocket.TextMessage, msg)
	}

	client.mu.Lock()
	if !client.Connected {
		client.mu.Unlock()
		return nil
	}
	err := client.SendEvent(msg)
	client.mu.Unlock()
	return err
}

func sendWSJSONErr(c *websocket.Conn, client *Client, id *string, err error) {
	if msg, e := json.Marshal(ErrorResponse{Id: id, Error: err.Error()}); e == nil {
		_ = writeWSMessage(c, client, msg)
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
	Id                     *string                 `json:"id"`
	Addresses              []string                `json:"addresses"`
	TraceExternalHashNorms []string                `json:"trace_external_hash_norms,omitempty"`
	Types                  []EventType             `json:"types"`
	MinFinality            *emulated.FinalityState `json:"min_finality,omitempty"`
	ActionTypes            []string                `json:"action_types"`
	SupportedActionTypes   []string                `json:"supported_action_types"`
	IncludeAddressBook     *bool                   `json:"include_address_book"`
	IncludeMetadata        *bool                   `json:"include_metadata"`
}

func ValidateSSERequest(req *SSERequest) ([]string, []string, emulated.FinalityState, error) {
	if len(req.Types) == 0 {
		return nil, nil, defaultMinFinality(), fmt.Errorf("types are required for subscription")
	}

	uniqueAddrs, err := validateAddressesAndTypes(req.Addresses, req.Types)
	if err != nil {
		return nil, nil, defaultMinFinality(), err
	}

	traceExternalHashNorms, err := validateTraceExternalHashNorms(req.TraceExternalHashNorms)
	if err != nil {
		return nil, nil, defaultMinFinality(), err
	}

	hasTraceType := hasEventType(req.Types, EventTrace)
	hasAddressTypes := hasNonTraceEventTypes(req.Types)

	if len(traceExternalHashNorms) > 0 && !hasTraceType {
		return nil, nil, defaultMinFinality(), fmt.Errorf("trace_external_hash_norms requires type \"trace\"")
	}
	if hasTraceType && len(traceExternalHashNorms) == 0 {
		return nil, nil, defaultMinFinality(), fmt.Errorf("trace_external_hash_norms are required for trace subscription")
	}
	if hasAddressTypes && len(uniqueAddrs) == 0 {
		return nil, nil, defaultMinFinality(), fmt.Errorf("addresses are required for subscription")
	}

	minFin := defaultMinFinality()
	if req.MinFinality != nil {
		minFin = *req.MinFinality
	}

	return uniqueAddrs, traceExternalHashNorms, minFin, nil
}

func SSEHandler(manager *ClientManager) fiber.Handler {
	return func(c *fiber.Ctx) error {
		var req SSERequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{Error: fmt.Sprintf("invalid subscription request: %v", err)})
		}
		addresses, traceExternalHashNorms, minFinality, err := ValidateSSERequest(&req)
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
				SupportedActionTypes: indexModels.ExpandActionTypeShortcuts(req.SupportedActionTypes),
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
		client.Subscription.ReplaceTraces(traceExternalHashNorms)
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
	Addresses              []string                `json:"addresses"`
	TraceExternalHashNorms []string                `json:"trace_external_hash_norms,omitempty"`
	Types                  []EventType             `json:"types"`
	MinFinality            *emulated.FinalityState `json:"min_finality,omitempty"`
	ActionTypes            []string                `json:"action_types,omitempty"`
	SupportedActionTypes   []string                `json:"supported_action_types,omitempty"`
	IncludeAddressBook     *bool                   `json:"include_address_book,omitempty"`
	IncludeMetadata        *bool                   `json:"include_metadata,omitempty"`
}

type UnsubscribeRequest struct {
	Addresses              []string `json:"addresses"`
	TraceExternalHashNorms []string `json:"trace_external_hash_norms,omitempty"`
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
				sendWSJSONErr(c, nil, nil, err)
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
				SubscribedTraces:     make(TraceSet),
				EventTypes:           make(eventSet),
				SupportedActionTypes: indexModels.ExpandActionTypeShortcuts(headers["X-Actions-Version"]),
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
				sendWSJSONErr(c, client, nil, fmt.Errorf("invalid request envelope: %v", err))
				continue
			}

			switch env.Operation {
			case OpPing:
				ack, _ := json.Marshal(StatusResponse{Id: env.Id, Status: "pong"})
				_ = writeWSMessage(c, client, ack)

			case OpUnsubscribe:
				var req UnsubscribeRequest
				if err := json.Unmarshal(msg, &req); err != nil {
					sendWSJSONErr(c, client, env.Id, fmt.Errorf("invalid unsubscribe request: %v", err))
					continue
				}
				if len(req.Addresses) == 0 && len(req.TraceExternalHashNorms) == 0 {
					sendWSJSONErr(c, client, env.Id, fmt.Errorf("addresses or trace_external_hash_norms are required"))
					continue
				}

				var cnvAddrs []string
				if len(req.Addresses) > 0 {
					cnvAddrs = make([]string, len(req.Addresses))
					addrsValid := true
					for i, a := range req.Addresses {
						cnvAddrs[i], err = convertAddress(a)
						if err != nil {
							addrsValid = false
							sendWSJSONErr(c, client, env.Id, err)
							break
						}
					}
					if !addrsValid {
						continue
					}
				}

				traceExternalHashNorms, err := validateTraceExternalHashNorms(req.TraceExternalHashNorms)
				if err != nil {
					sendWSJSONErr(c, client, env.Id, err)
					continue
				}

				client.mu.Lock()
				if len(cnvAddrs) > 0 {
					client.Subscription.Unsubscribe(cnvAddrs)
				}
				if len(traceExternalHashNorms) > 0 {
					client.Subscription.UnsubscribeTraces(traceExternalHashNorms)
				}
				client.mu.Unlock()
				ack, _ := json.Marshal(StatusResponse{Id: env.Id, Status: "unsubscribed"})
				_ = writeWSMessage(c, client, ack)

			case OpSubscribe:
				var req SubscribeRequest
				if err := json.Unmarshal(msg, &req); err != nil {
					sendWSJSONErr(c, client, env.Id, fmt.Errorf("invalid subscribe request: %v", err))
					continue
				}
				if len(req.Types) == 0 {
					sendWSJSONErr(c, client, env.Id, fmt.Errorf("types are required"))
					continue
				}

				cnvAddrs, err := validateAddressesAndTypes(req.Addresses, req.Types)
				if err != nil {
					sendWSJSONErr(c, client, env.Id, err)
					continue
				}

				traceExternalHashNorms, err := validateTraceExternalHashNorms(req.TraceExternalHashNorms)
				if err != nil {
					sendWSJSONErr(c, client, env.Id, err)
					continue
				}

				hasTraceType := hasEventType(req.Types, EventTrace)
				hasAddressTypes := hasNonTraceEventTypes(req.Types)
				if len(traceExternalHashNorms) > 0 && !hasTraceType {
					sendWSJSONErr(c, client, env.Id, fmt.Errorf("trace_external_hash_norms requires type \"trace\""))
					continue
				}
				if hasTraceType && len(traceExternalHashNorms) == 0 {
					sendWSJSONErr(c, client, env.Id, fmt.Errorf("trace_external_hash_norms are required for trace subscription"))
					continue
				}
				if hasAddressTypes && len(cnvAddrs) == 0 {
					sendWSJSONErr(c, client, env.Id, fmt.Errorf("addresses are required"))
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
					sendWSJSONErr(c, client, env.Id, err)
					continue
				}

				client.Subscription.Replace(cnvAddrs, req.Types)
				client.Subscription.ReplaceTraces(traceExternalHashNorms)
				client.Subscription.MinFinality = minFin

				if req.IncludeAddressBook != nil {
					client.Subscription.IncludeAddressBook = *req.IncludeAddressBook
				}
				if req.IncludeMetadata != nil {
					client.Subscription.IncludeMetadata = *req.IncludeMetadata
				}
				if len(req.SupportedActionTypes) > 0 {
					client.Subscription.SupportedActionTypes = indexModels.ExpandActionTypeShortcuts(req.SupportedActionTypes)
				}
				if len(req.ActionTypes) > 0 {
					client.Subscription.ActionTypes = req.ActionTypes
				}

				client.mu.Unlock()

				ack, _ := json.Marshal(StatusResponse{Id: env.Id, Status: "subscribed"})
				_ = writeWSMessage(c, client, ack)

			default:
				sendWSJSONErr(c, client, env.Id, fmt.Errorf("unknown operation: %s", env.Operation))
			}
		}
	}
}
