package v2

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/index/crud"
	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"

	"github.com/toncenter/ton-indexer/ton-emulate-go/models"
	"github.com/toncenter/ton-indexer/ton-streaming-go/observability"
)

////////////////////////////////////////////////////////////////////////////////
// Config
////////////////////////////////////////////////////////////////////////////////

// Config provides runtime dependencies for v2 handlers.
type Config struct {
	EnrichmentReader crud.EnrichmentReader
	Testnet          bool
	ImgProxyBaseURL  string
}

var config Config

// InitConfig registers configuration to be used by v2 handlers.
func InitConfig(cfg Config) {
	config = cfg
}

////////////////////////////////////////////////////////////////////////////////
// Trace processing span state
////////////////////////////////////////////////////////////////////////////////

const (
	actionHintSpanName = "ton.streaming_api.process_action_hint"
	rawTraceSpanName   = "ton.streaming_api.process_raw_trace"

	streamingWorkerCount        = 16
	streamingQueueSizePerWorker = 64
)

type TraceProcessingStage struct {
	RootTxHash indexModels.HashType
	Span       *observability.StageSpan
}

func NewTraceProcessingStage(
	startTimeUnix int64,
	spanName string,
	rawTrace map[string]string,
	traceExternalHashNorm string,
	channel string,
) *TraceProcessingStage {
	stage := &TraceProcessingStage{
		RootTxHash: "-",
		Span:       observability.NewStage(startTimeUnix, spanName, rawTrace),
	}
	stage.Span.AddAttr("ton.redis.in.channel", channel)
	stage.Span.AddAttr("ton.trace.external_message_hash_norm", traceExternalHashNorm)
	return stage
}

func (s *TraceProcessingStage) SetRootTxHash(rootTxHash indexModels.HashType) {
	if rootTxHash == "" || rootTxHash == "-" {
		return
	}
	s.RootTxHash = rootTxHash
	if s.Span != nil {
		s.Span.AddAttr("ton.trace.root_tx_hash", string(rootTxHash))
	}
}

func (s *TraceProcessingStage) Emit() {
	if s.Span != nil {
		s.Span.Emit()
	}
}

func (s *TraceProcessingStage) EmitOtelError(errorType, message string) {
	if s.Span == nil {
		return
	}
	s.Span.MarkError(errorType, message)
	s.Span.Emit()
}

////////////////////////////////////////////////////////////////////////////////
// Finality & Event types
////////////////////////////////////////////////////////////////////////////////

func defaultMinFinality() indexModels.FinalityState {
	// Safer default: only finalized events unless explicitly requested otherwise.
	return indexModels.FinalityStateFinalized
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
type AddressSet map[indexModels.AccountAddress]struct{}
type TraceSet map[indexModels.HashType]struct{}

type Subscription struct {
	SubscribedAddresses  AddressSet
	SubscribedTraces     TraceSet
	EventTypes           eventSet
	ActionTypes          []string
	SupportedActionTypes []string
	IncludeAddressBook   bool
	IncludeMetadata      bool
	MinFinality          indexModels.FinalityState
}

func makeEventSet(types []EventType) eventSet {
	s := make(eventSet, len(types))
	for _, t := range types {
		s[t] = struct{}{}
	}
	return s
}

func makeAddressSet(addrs []indexModels.AccountAddress) AddressSet {
	s := make(AddressSet, len(addrs))
	for _, addr := range addrs {
		s[addr] = struct{}{}
	}
	return s
}

func makeTraceSet(traces []indexModels.HashType) TraceSet {
	s := make(TraceSet, len(traces))
	for _, trace := range traces {
		s[trace] = struct{}{}
	}
	return s
}

func (s *Subscription) Replace(addresses []indexModels.AccountAddress, eventTypes []EventType) {
	s.SubscribedAddresses = makeAddressSet(addresses)
	s.EventTypes = makeEventSet(eventTypes)
}

func (s *Subscription) ReplaceTraces(traces []indexModels.HashType) {
	s.SubscribedTraces = makeTraceSet(traces)
}

func (s *Subscription) Unsubscribe(addresses []indexModels.AccountAddress) {
	for _, addr := range addresses {
		delete(s.SubscribedAddresses, addr)
	}
}

func (s *Subscription) UnsubscribeTraces(traces []indexModels.HashType) {
	for _, trace := range traces {
		delete(s.SubscribedTraces, trace)
	}
}

func (s *Subscription) InterestedIn(eventType EventType, eventAddresses []indexModels.AccountAddress) bool {
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

func (s *Subscription) InterestedInTrace(eventType EventType, traceExternalHashNorm indexModels.HashType) bool {
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

type notificationDelivery struct {
	notification Notification
	targets      clientSet // nil means all connected clients
}

type Client struct {
	ID                             string
	LimitingKey                    string
	Connected                      bool
	Subscription                   Subscription
	TracesForPotentialInvalidation map[indexModels.HashType]bool // traceExternalHashNorm -> true
	SendEvent                      func([]byte) error
	sendChan                       chan []byte
	mu                             sync.Mutex
	writeMu                        sync.Mutex
}

func disconnectClient(manager *ClientManager, client *Client) {
	client.mu.Lock()
	if !client.Connected {
		client.mu.Unlock()
		return
	}
	client.Connected = false
	client.mu.Unlock()

	manager.unregister <- client
}

func (c *Client) startSender(manager *ClientManager) {
	go func() {
		for msg := range c.sendChan {
			c.writeMu.Lock()
			c.mu.Lock()
			if !c.Connected {
				c.mu.Unlock()
				c.writeMu.Unlock()
				break
			}
			c.mu.Unlock()

			err := c.SendEvent(msg)
			c.writeMu.Unlock()
			if err != nil {
				disconnectClient(manager, c)
				break
			}
		}
	}()
}

type ClientManager struct {
	clients            map[string]*Client
	eventSubscribers   map[EventType]clientSet
	addressSubscribers map[EventType]map[indexModels.AccountAddress]clientSet
	traceSubscribers   map[indexModels.HashType]clientSet
	register           chan *Client
	unregister         chan *Client
	broadcast          chan notificationDelivery
	rateLimiter        *RateLimiter
	mu                 sync.RWMutex
}

func NewClientManager() *ClientManager {
	return &ClientManager{
		clients:            make(map[string]*Client),
		eventSubscribers:   make(map[EventType]clientSet),
		addressSubscribers: make(map[EventType]map[indexModels.AccountAddress]clientSet),
		traceSubscribers:   make(map[indexModels.HashType]clientSet),
		register:           make(chan *Client, 128),
		unregister:         make(chan *Client, 128),
		broadcast:          make(chan notificationDelivery),
		rateLimiter:        NewRateLimiter(),
	}
}

// shouldFetchAddressBookAndMetadata figures out if at least one client
// - is interested in any of the given event types for given addresses
// - AND has IncludeAddressBook / IncludeMetadata true
// AND will actually receive this event with given finality.
func (manager *ClientManager) shouldFetchAddressBookAndMetadata(eventTypes []EventType, eventFinality indexModels.FinalityState, addressesToNotify []indexModels.AccountAddress) (bool, bool) {
	targets := make(clientSet)
	for _, eventType := range eventTypes {
		mergeClientSets(targets, manager.subscribersForAddresses(eventType, addressesToNotify, eventFinality))
	}
	return manager.enrichmentNeeds(targets)
}

// shouldFetchAddressBookAndMetadataForTrace checks if any connected client
// subscribed to the trace will receive this event and needs address book or metadata.
func (manager *ClientManager) shouldFetchAddressBookAndMetadataForTrace(eventFinality indexModels.FinalityState, traceExternalHashNorm indexModels.HashType) (bool, bool) {
	return manager.enrichmentNeeds(manager.subscribersForTrace(traceExternalHashNorm, eventFinality))
}

func (manager *ClientManager) sendNotification(notification Notification, targets clientSet) {
	if targets != nil && len(targets) == 0 {
		return
	}
	manager.broadcast <- notificationDelivery{
		notification: notification,
		targets:      targets,
	}
}

func (manager *ClientManager) Run() {
	for {
		select {
		case client := <-manager.register:
			manager.mu.Lock()
			client.mu.Lock()
			if !client.Connected {
				manager.removeSubscriptionFromIndexesLocked(client.ID, &client.Subscription)
				client.mu.Unlock()
				manager.rateLimiter.UnregisterConnection(client.LimitingKey, client.ID)
				manager.mu.Unlock()
				continue
			}
			client.sendChan = make(chan []byte, 64)
			manager.clients[client.ID] = client
			manager.addSubscriptionToIndexesLocked(client.ID, &client.Subscription)
			client.mu.Unlock()
			manager.mu.Unlock()
			client.startSender(manager)
			log.Printf("[v2] Client %s connected", client.ID)

		case client := <-manager.unregister:
			manager.mu.Lock()
			if _, ok := manager.clients[client.ID]; ok {
				client.mu.Lock()
				manager.removeSubscriptionFromIndexesLocked(client.ID, &client.Subscription)
				if client.sendChan != nil {
					close(client.sendChan)
				}
				client.mu.Unlock()
				delete(manager.clients, client.ID)
				manager.rateLimiter.UnregisterConnection(client.LimitingKey, client.ID)
				log.Printf("[v2] Client %s disconnected", client.ID)
			}
			manager.mu.Unlock()

		case delivery := <-manager.broadcast:
			manager.mu.RLock()
			clients := make([]*Client, 0, len(manager.clients))
			if delivery.targets == nil {
				for _, client := range manager.clients {
					clients = append(clients, client)
				}
			} else {
				for clientID := range delivery.targets {
					if client := manager.clients[clientID]; client != nil {
						clients = append(clients, client)
					}
				}
			}
			manager.mu.RUnlock()

			for _, client := range clients {
				client.mu.Lock()
				if client.Connected {
					if event := delivery.notification.AdjustForClient(client); event != nil {
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
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// Address book / metadata fetching
////////////////////////////////////////////////////////////////////////////////

func fetchAddressBookAndMetadata(_ context.Context, addrBookAddresses []indexModels.AccountAddress, metadataAddresses []indexModels.AccountAddress, includeAddressBook bool, includeMetadata bool) (*indexModels.AddressBook, *indexModels.Metadata) {
	var addressBook *indexModels.AddressBook
	var metadata *indexModels.Metadata

	if config.EnrichmentReader == nil {
		return nil, nil
	}

	settings := indexModels.RequestSettings{
		Timeout:   3 * time.Second,
		IsTestnet: config.Testnet,
	}

	if includeAddressBook {
		book, err := config.EnrichmentReader.QueryAddressBookByAddresses(addrBookAddresses, settings)
		if err != nil {
			log.Printf("[v2] Error querying address book: %v", err)
		} else {
			addressBook = &book
		}
	}

	if includeMetadata {
		meta, err := config.EnrichmentReader.QueryMetadataByAddresses(metadataAddresses, settings)
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
	Type                  EventType            `json:"type"`
	TraceExternalHashNorm indexModels.HashType `json:"trace_external_hash_norm"`
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
	Type                  EventType                      `json:"type"` // always "actions"
	Finality              indexModels.FinalityState      `json:"finality,string"`
	TraceExternalHashNorm indexModels.HashType           `json:"trace_external_hash_norm"`
	Actions               []*indexModels.Action          `json:"actions"`
	ActionAddresses       [][]indexModels.AccountAddress `json:"-"` // used internally
	AddressBook           *indexModels.AddressBook       `json:"address_book,omitempty"`
	Metadata              *indexModels.Metadata          `json:"metadata,omitempty"`
}

var _ Notification = (*ActionsNotification)(nil)

func (n *ActionsNotification) AdjustForClient(client *Client) any {
	// Finality filter
	if n.Finality < client.Subscription.MinFinality {
		return nil
	}

	var adjustedActions []*indexModels.Action
	var adjustedActionAddresses [][]indexModels.AccountAddress
	var adjustedAddressBook *indexModels.AddressBook
	var adjustedMetadata *indexModels.Metadata
	if client.Subscription.IncludeAddressBook && n.AddressBook != nil {
		adjustedAddressBook = &indexModels.AddressBook{}
	}
	if client.Subscription.IncludeMetadata && n.Metadata != nil {
		adjustedMetadata = &indexModels.Metadata{}
	}
	allAddresses := map[indexModels.AccountAddress]bool{}

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
	if n.Finality == indexModels.FinalityStateFinalized {
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
	Finality              indexModels.FinalityState `json:"finality"`
	TraceExternalHashNorm indexModels.HashType      `json:"trace_external_hash_norm"`
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
	if client.Subscription.IncludeAddressBook && n.AddressBook != nil {
		adjustedAddressBook = &indexModels.AddressBook{}
	}
	if client.Subscription.IncludeMetadata && n.Metadata != nil {
		adjustedMetadata = &indexModels.Metadata{}
	}

	allAddresses := map[indexModels.AccountAddress]bool{}
	for _, tx := range n.Transactions {
		account := tx.Account

		if client.Subscription.InterestedIn(EventTransactions, []indexModels.AccountAddress{account}) {
			adjustedTransactions = append(adjustedTransactions, tx)
			allAddresses[account] = true

			if tx.InMsg != nil && tx.InMsg.Source != nil {
				allAddresses[*tx.InMsg.Source] = true
			}
			for _, outMsg := range tx.OutMsgs {
				if outMsg.Destination != nil {
					allAddresses[*outMsg.Destination] = true
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

	if n.Finality == indexModels.FinalityStateFinalized {
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
	Finality              indexModels.FinalityState                         `json:"finality"`
	TraceExternalHashNorm indexModels.HashType                              `json:"trace_external_hash_norm"`
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
		adjustedActions = &filteredActions
	}

	if n.Finality == indexModels.FinalityStateFinalized {
		delete(client.TracesForPotentialInvalidation, n.TraceExternalHashNorm)
	} else {
		client.TracesForPotentialInvalidation[n.TraceExternalHashNorm] = true
	}

	var addressBook *indexModels.AddressBook
	if client.Subscription.IncludeAddressBook {
		addressBook = n.AddressBook
	}
	var metadata *indexModels.Metadata
	if client.Subscription.IncludeMetadata {
		metadata = n.Metadata
	}

	return &TraceNotification{
		Type:                  n.Type,
		Finality:              n.Finality,
		TraceExternalHashNorm: n.TraceExternalHashNorm,
		Trace:                 n.Trace,
		Transactions:          n.Transactions,
		Actions:               adjustedActions,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}
}

type AccountStateNotification struct {
	Type     EventType                  `json:"type"`
	Finality indexModels.FinalityState  `json:"finality"` // confirmed / finalized
	Account  indexModels.AccountAddress `json:"account"`
	State    indexModels.AccountState   `json:"state"`
}

var _ Notification = (*AccountStateNotification)(nil)

func (n *AccountStateNotification) AdjustForClient(client *Client) any {
	if n.Finality < client.Subscription.MinFinality {
		return nil
	}
	if client.Subscription.InterestedIn(EventAccountStateChange, []indexModels.AccountAddress{n.Account}) {
		return n
	}
	return nil
}

type JettonsNotification struct {
	Type        EventType                 `json:"type"`
	Finality    indexModels.FinalityState `json:"finality"` // confirmed / finalized
	Jetton      indexModels.JettonWallet  `json:"jetton"`
	AddressBook *indexModels.AddressBook  `json:"address_book,omitempty"`
	Metadata    *indexModels.Metadata     `json:"metadata,omitempty"`
}

var _ Notification = (*JettonsNotification)(nil)

func (n *JettonsNotification) AdjustForClient(client *Client) any {
	if n.Finality < client.Subscription.MinFinality {
		return nil
	}
	if client.Subscription.InterestedIn(EventJettonsChange, []indexModels.AccountAddress{n.Jetton.Address}) ||
		client.Subscription.InterestedIn(EventJettonsChange, []indexModels.AccountAddress{n.Jetton.Owner}) {
		var addressBook *indexModels.AddressBook
		if client.Subscription.IncludeAddressBook {
			addressBook = n.AddressBook
		}
		var metadata *indexModels.Metadata
		if client.Subscription.IncludeMetadata {
			metadata = n.Metadata
		}

		return &JettonsNotification{
			Type:        n.Type,
			Finality:    n.Finality,
			Jetton:      n.Jetton,
			AddressBook: addressBook,
			Metadata:    metadata,
		}
	}
	return nil
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

		traceExternalHashNorm := indexModels.HashType(msg.Payload)
		manager.sendNotification(&TraceInvalidatedNotification{
			Type:                  EventTraceInvalidated,
			TraceExternalHashNorm: traceExternalHashNorm,
		}, nil)
	}
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
func validateAddressesAndTypes(addresses []string, types []EventType) ([]indexModels.AccountAddress, error) {
	for _, t := range types {
		if _, ok := validEventTypes[t]; !ok {
			return nil, fmt.Errorf("invalid event type: %s", t)
		}
	}

	uniqueAddrs := make([]indexModels.AccountAddress, 0, len(addresses))
	addrsSet := make(map[indexModels.AccountAddress]struct{}, len(addresses))
	for _, a := range addresses {
		cnv, err := indexModels.ParseAccountAddress(a)
		if err != nil || cnv == nil || !cnv.IsAddressStd() {
			return nil, err
		}
		if _, exists := addrsSet[*cnv]; exists {
			continue
		}
		addrsSet[*cnv] = struct{}{}
		uniqueAddrs = append(uniqueAddrs, *cnv)
	}

	return uniqueAddrs, nil
}

func validateTraceExternalHashNorms(traces []string) ([]indexModels.HashType, error) {
	unique := make([]indexModels.HashType, 0, len(traces))
	seen := make(map[indexModels.HashType]struct{}, len(traces))
	for _, trace := range traces {
		traceHash, err := indexModels.ParseHashType(trace)
		if err != nil || traceHash == nil {
			return nil, fmt.Errorf("trace_external_hash_norms contains empty value")
		}
		if _, exists := seen[*traceHash]; exists {
			continue
		}
		seen[*traceHash] = struct{}{}
		unique = append(unique, *traceHash)
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

func collectAddressesFromTransaction(addrSet map[indexModels.AccountAddress]bool, tx *indexModels.Transaction) {
	addrSet[tx.Account] = true
	if tx.InMsg != nil && tx.InMsg.Source != nil {
		addrSet[*tx.InMsg.Source] = true
	}
	for _, outMsg := range tx.OutMsgs {
		if outMsg.Destination != nil {
			addrSet[*outMsg.Destination] = true
		}
	}
}

func buildActionsFromContext(emulatedContext *crud.EmulatedTracesContext) ([]*indexModels.Action, [][]indexModels.AccountAddress) {
	actions := make([]*indexModels.Action, 0)
	actionsAddresses := make([][]indexModels.AccountAddress, 0)
	for _, rawAction := range emulatedContext.GetAllActions() {
		actionAddrMap := map[indexModels.AccountAddress]bool{}
		parse.CollectAddressesFromAction(&actionAddrMap, rawAction)

		action, err := parse.ParseRawAction(rawAction)
		if err != nil {
			log.Printf("[v2] Error parsing raw action: %v", err)
			continue
		}

		actionAddresses := make([]indexModels.AccountAddress, 0, len(actionAddrMap))
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

func writeSSEComment(w *bufio.Writer, comment string) error {
	if _, err := fmt.Fprintf(w, ": %s\n\n", comment); err != nil {
		return err
	}
	return w.Flush()
}

func writeWSMessage(c *websocket.Conn, client *Client, msg []byte) error {
	if client == nil {
		return c.WriteMessage(websocket.TextMessage, msg)
	}

	client.writeMu.Lock()
	defer client.writeMu.Unlock()

	client.mu.Lock()
	if !client.Connected {
		client.mu.Unlock()
		return nil
	}
	client.mu.Unlock()
	return client.SendEvent(msg)
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
	Id                     *string                    `json:"id"`
	Addresses              []string                   `json:"addresses"`
	TraceExternalHashNorms []string                   `json:"trace_external_hash_norms,omitempty"`
	Types                  []EventType                `json:"types"`
	MinFinality            *indexModels.FinalityState `json:"min_finality,omitempty"`
	ActionTypes            []string                   `json:"action_types"`
	SupportedActionTypes   []string                   `json:"supported_action_types"`
	IncludeAddressBook     *bool                      `json:"include_address_book"`
	IncludeMetadata        *bool                      `json:"include_metadata"`
}

func ValidateSSERequest(req *SSERequest) ([]indexModels.AccountAddress, []indexModels.HashType, indexModels.FinalityState, error) {
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
			TracesForPotentialInvalidation: make(map[indexModels.HashType]bool),
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
			defer disconnectClient(manager, client)

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
						log.Printf("[v2] SSE event write failed for client %s: %v", clientID, err)
						return
					}
				case <-keepAlive.C:
					if err := writeSSEComment(w, "keepalive"); err != nil {
						log.Printf("[v2] SSE keepalive write failed for client %s: %v", clientID, err)
						return
					}
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
	Addresses              []string                   `json:"addresses"`
	TraceExternalHashNorms []string                   `json:"trace_external_hash_norms,omitempty"`
	Types                  []EventType                `json:"types"`
	MinFinality            *indexModels.FinalityState `json:"min_finality,omitempty"`
	ActionTypes            []string                   `json:"action_types,omitempty"`
	SupportedActionTypes   []string                   `json:"supported_action_types,omitempty"`
	IncludeAddressBook     *bool                      `json:"include_address_book,omitempty"`
	IncludeMetadata        *bool                      `json:"include_metadata,omitempty"`
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
			TracesForPotentialInvalidation: make(map[indexModels.HashType]bool),
			SendEvent:                      func(b []byte) error { return c.WriteMessage(websocket.TextMessage, b) },
		}
		manager.register <- client
		defer disconnectClient(manager, client)

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

				var cnvAddrs []indexModels.AccountAddress
				if len(req.Addresses) > 0 {
					cnvAddrs = make([]indexModels.AccountAddress, len(req.Addresses))
					addrsValid := true
					for i, a := range req.Addresses {
						cnv, err := indexModels.ParseAccountAddress(a)
						if err != nil || cnv == nil {
							addrsValid = false
							sendWSJSONErr(c, client, env.Id, err)
							break
						}
						if !cnv.IsAddressStd() {
							addrsValid = false
							err := indexModels.IndexError{
								Code:    422,
								Message: "address is not standard",
							}
							sendWSJSONErr(c, client, env.Id, err)
							break
						}
						cnvAddrs[i] = *cnv
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

				_ = manager.updateSubscription(client, func(subscription *Subscription) error {
					if len(cnvAddrs) > 0 {
						subscription.Unsubscribe(cnvAddrs)
					}
					if len(traceExternalHashNorms) > 0 {
						subscription.UnsubscribeTraces(traceExternalHashNorms)
					}
					return nil
				})
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

				err = manager.updateSubscription(client, func(subscription *Subscription) error {
					if err := checkAddressLimit(client, len(cnvAddrs), manager.rateLimiter, true); err != nil {
						return err
					}

					minFinality := subscription.MinFinality
					if req.MinFinality != nil {
						minFinality = *req.MinFinality
					}

					subscription.Replace(cnvAddrs, req.Types)
					subscription.ReplaceTraces(traceExternalHashNorms)
					subscription.MinFinality = minFinality

					if req.IncludeAddressBook != nil {
						subscription.IncludeAddressBook = *req.IncludeAddressBook
					}
					if req.IncludeMetadata != nil {
						subscription.IncludeMetadata = *req.IncludeMetadata
					}
					if len(req.SupportedActionTypes) > 0 {
						subscription.SupportedActionTypes = indexModels.ExpandActionTypeShortcuts(req.SupportedActionTypes)
					}
					if len(req.ActionTypes) > 0 {
						subscription.ActionTypes = req.ActionTypes
					}
					return nil
				})
				if err != nil {
					sendWSJSONErr(c, client, env.Id, err)
					continue
				}

				ack, _ := json.Marshal(StatusResponse{Id: env.Id, Status: "subscribed"})
				_ = writeWSMessage(c, client, ack)

			default:
				sendWSJSONErr(c, client, env.Id, fmt.Errorf("unknown operation: %s", env.Operation))
			}
		}
	}
}
