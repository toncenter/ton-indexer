package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"ton-metadata-cache/cache"
	"ton-metadata-cache/models"
	"ton-metadata-cache/repl"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// TTL for caches that support expiration
	cacheTTL = 1 * time.Hour
)

// Handler processes replication events and updates caches.
type Handler struct {
	cache *cache.Manager
	db    *pgxpool.Pool
}

// NewHandler creates a new event handler.
func NewHandler(cache *cache.Manager, db *pgxpool.Pool) *Handler {
	return &Handler{cache: cache, db: db}
}

// GetShortestDomain returns the shortest self-resolving domain for an owner.
// Returns empty string if owner has no self-resolving domains.
func (h *Handler) GetShortestDomain(ctx context.Context, owner string) string {
	// Get nft_item_address with lowest domain length from sorted set
	nftAddr, err := h.cache.DnsByOwner.GetLowest(ctx, owner)
	if err != nil {
		return ""
	}

	// Look up the actual domain from DnsEntries
	entry, err := h.cache.DnsEntries.Get(ctx, nftAddr)
	if err != nil || entry.Domain == nil {
		return ""
	}

	return *entry.Domain
}

// HandleEvents processes events from the replicator channel.
// Blocks until the channel is closed or context is cancelled.
func (h *Handler) HandleEvents(ctx context.Context, events <-chan repl.ChangeEvent) {
	println("EVENT")
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			if err := h.handleEvent(ctx, event); err != nil {
				log.Printf("Error handling event: %v", err)
			}
		}
	}
}

func (h *Handler) handleEvent(ctx context.Context, event repl.ChangeEvent) error {
	println(event.Table, event.Xid)
	switch event.Table {
	case "public.contract_methods":
		return h.handleContractMethods(ctx, event)
	case "public.dns_entries":
		return h.handleDnsEntries(ctx, event)
	case "public.latest_account_states":
		return h.handleAccountStates(ctx, event)
	case "public.address_metadata":
		return h.handleAddressMetadata(ctx, event)
	case "public.jetton_wallets":
		return h.handleJettonWallets(ctx, event)
	case "public.nft_items":
		return h.handleNftItems(ctx, event)
	case "public.nft_collections":
		return h.handleNftCollections(ctx, event)
	case "public.jetton_masters":
		return h.handleJettonMasters(ctx, event)
	default:
		return nil
	}
}

func (h *Handler) handleContractMethods(ctx context.Context, event repl.ChangeEvent) error {
	if event.Operation != repl.Insert && event.Operation != repl.Update {
		return nil
	}
	codeHash, ok := event.Data["code_hash"].(string)
	if !ok {
		return fmt.Errorf("missing code_hash")
	}
	methods, _ := toUint32Slice(event.Data["methods"])
	return h.cache.ContractMethods.Set(ctx, codeHash, methods, 0)
}

func (h *Handler) handleDnsEntries(ctx context.Context, event repl.ChangeEvent) error {
	if event.Operation != repl.Insert && event.Operation != repl.Update {
		return nil
	}

	entry := models.DnsEntry{
		NftItemAddress: getString(event.Data, "nft_item_address"),
	}
	if entry.NftItemAddress == "" {
		return fmt.Errorf("missing nft_item_address")
	}
	entry.NftItemOwner = getStringPtr(event.Data, "nft_item_owner")
	entry.Domain = getStringPtr(event.Data, "domain")
	entry.DnsNextResolver = getStringPtr(event.Data, "dns_next_resolver")
	entry.DnsWallet = getStringPtr(event.Data, "dns_wallet")
	entry.DnsSiteAdnl = getStringPtr(event.Data, "dns_site_adnl")
	entry.DnsStorageBagId = getStringPtr(event.Data, "dns_storage_bag_id")
	entry.LastTransactionLt = getInt64Ptr(event.Data, "last_transaction_lt")

	// For UPDATE: remove from old owner's sorted set if it was self-resolving
	if event.Operation == repl.Update {
		if oldEntry, err := h.cache.DnsEntries.Get(ctx, entry.NftItemAddress); err == nil {
			if oldEntry.NftItemOwner != nil && oldEntry.DnsWallet != nil &&
				*oldEntry.NftItemOwner == *oldEntry.DnsWallet {
				// Old entry was self-resolving, remove from old owner's set
				if err := h.cache.DnsByOwner.Remove(ctx, *oldEntry.NftItemOwner, entry.NftItemAddress); err != nil {
					log.Printf("Warning: failed to remove from old owner's dns set: %v", err)
				}
			}
		}
	}

	// Update main DNS cache
	if err := h.cache.DnsEntries.Set(ctx, entry.NftItemAddress, entry, 0); err != nil {
		return err
	}

	// Add to owner's sorted set if self-resolving (score = domain length)
	if entry.NftItemOwner != nil && entry.DnsWallet != nil && entry.Domain != nil {
		if *entry.NftItemOwner == *entry.DnsWallet {
			score := float64(len(*entry.Domain))
			if err := h.cache.DnsByOwner.Add(ctx, *entry.NftItemOwner, score, entry.NftItemAddress); err != nil {
				log.Printf("Warning: failed to add to owner's dns set: %v", err)
			}
		}
	}
	return nil
}

func (h *Handler) handleAccountStates(ctx context.Context, event repl.ChangeEvent) error {
	if event.Operation != repl.Insert && event.Operation != repl.Update {
		return nil
	}
	account := getString(event.Data, "account")
	if account == "" {
		return fmt.Errorf("missing account")
	}
	state := models.LatestAccountState{
		CodeHash: getStringPtr(event.Data, "code_hash"),
	}
	return h.cache.AccountStates.Set(ctx, account, state, cacheTTL)
}

func (h *Handler) handleAddressMetadata(ctx context.Context, event repl.ChangeEvent) error {
	addr := getString(event.Data, "address")
	typ := getString(event.Data, "type")
	if addr == "" || typ == "" {
		return fmt.Errorf("missing address or type")
	}

	data, err := h.getOrFetchAddressData(ctx, addr)
	if err != nil {
		return err
	}

	if data.Metadata == nil {
		data.Metadata = make(map[string]models.MetadataEntry)
	}

	switch event.Operation {
	case repl.Insert, repl.Update:
		data.Metadata[typ] = models.MetadataEntry{
			Exists:      true,
			Valid:       getBoolPtr(event.Data, "valid"),
			Name:        getStringPtr(event.Data, "name"),
			Symbol:      getStringPtr(event.Data, "symbol"),
			Description: getStringPtr(event.Data, "description"),
			Image:       getStringPtr(event.Data, "image"),
			Extra:       getMap(event.Data, "extra"),
		}
	case repl.Delete:
		data.Metadata[typ] = models.MetadataEntry{Exists: false}
	}

	return h.cache.AddressInfo.Set(ctx, addr, data, 0)
}

func (h *Handler) handleJettonWallets(ctx context.Context, event repl.ChangeEvent) error {
	addr := getString(event.Data, "address")
	if addr == "" {
		return fmt.Errorf("missing address")
	}

	data, err := h.getOrFetchAddressData(ctx, addr)
	if err != nil {
		return err
	}

	switch event.Operation {
	case repl.Insert, repl.Update:
		data.JettonWallet = &models.JettonWalletData{
			Owner:   getString(event.Data, "owner"),
			Jetton:  getString(event.Data, "jetton"),
			Balance: getStringPtr(event.Data, "balance"),
		}
	case repl.Delete:
		data.JettonWallet = nil
	}

	return h.cache.AddressInfo.Set(ctx, addr, data, 0)
}

func (h *Handler) handleNftItems(ctx context.Context, event repl.ChangeEvent) error {
	addr := getString(event.Data, "address")
	if addr == "" {
		return fmt.Errorf("missing address")
	}

	data, err := h.getOrFetchAddressData(ctx, addr)
	if err != nil {
		return err
	}

	switch event.Operation {
	case repl.Insert, repl.Update:
		data.NftItem = &models.NftItemData{
			Index: getStringPtr(event.Data, "index"),
		}
	case repl.Delete:
		data.NftItem = nil
	}

	return h.cache.AddressInfo.Set(ctx, addr, data, 0)
}

func (h *Handler) handleNftCollections(ctx context.Context, event repl.ChangeEvent) error {
	addr := getString(event.Data, "address")
	if addr == "" {
		return fmt.Errorf("missing address")
	}

	data, err := h.getOrFetchAddressData(ctx, addr)
	if err != nil {
		return err
	}

	switch event.Operation {
	case repl.Insert, repl.Update:
		data.NftCollection = true
	case repl.Delete:
		data.NftCollection = false
	}

	return h.cache.AddressInfo.Set(ctx, addr, data, 0)
}

func (h *Handler) handleJettonMasters(ctx context.Context, event repl.ChangeEvent) error {
	addr := getString(event.Data, "address")
	if addr == "" {
		return fmt.Errorf("missing address")
	}

	data, err := h.getOrFetchAddressData(ctx, addr)
	if err != nil {
		return err
	}

	switch event.Operation {
	case repl.Insert, repl.Update:
		data.JettonMaster = true
	case repl.Delete:
		data.JettonMaster = false
	}

	return h.cache.AddressInfo.Set(ctx, addr, data, 0)
}

// getOrFetchAddressData retrieves address data from cache, or fetches complete data from DB on cache miss.
func (h *Handler) getOrFetchAddressData(ctx context.Context, addr string) (models.AddressData, error) {
	// Try cache first
	data, err := h.cache.AddressInfo.Get(ctx, addr)
	if err == nil {
		return data, nil
	}
	if !errors.Is(err, cache.ErrNotFound) {
		return models.AddressData{}, err
	}

	// Cache miss - fetch complete data from DB
	data, err = h.fetchCompleteAddressData(ctx, addr)
	if err != nil {
		return models.AddressData{}, err
	}

	// Back-propagate to cache (no TTL for replication-sourced data)
	if err := h.cache.AddressInfo.Set(ctx, addr, data, 0); err != nil {
		log.Printf("Warning: failed to cache address data: %v", err)
	}

	return data, nil
}

// fetchCompleteAddressData fetches all data for an address from DB using optimized queries.
func (h *Handler) fetchCompleteAddressData(ctx context.Context, addr string) (models.AddressData, error) {
	data := models.AddressData{
		Metadata: make(map[string]models.MetadataEntry),
	}

	// Query 1: Jetton wallet (separate due to different columns)
	var owner, jetton string
	var balance *string
	err := h.db.QueryRow(ctx, `
		SELECT owner, jetton, balance FROM jetton_wallets WHERE address = $1
	`, addr).Scan(&owner, &jetton, &balance)
	if err == nil {
		data.JettonWallet = &models.JettonWalletData{
			Owner: owner, Jetton: jetton, Balance: balance,
		}
	}

	// Query 2: UNION ALL for nft_items, nft_collections, jetton_masters + metadata
	rows, err := h.db.Query(ctx, `
		SELECT 'nft_items' as type, m.valid, m.name, m.symbol, m.description, m.image, m.extra, n.index
		FROM nft_items n 
		LEFT JOIN address_metadata m ON n.address = m.address AND m.type = 'nft_items'
		WHERE n.address = $1
		UNION ALL
		SELECT 'nft_collections' as type, m.valid, m.name, m.symbol, m.description, m.image, m.extra, null
		FROM nft_collections c 
		LEFT JOIN address_metadata m ON c.address = m.address AND m.type = 'nft_collections'
		WHERE c.address = $1
		UNION ALL
		SELECT 'jetton_masters' as type, m.valid, m.name, m.symbol, m.description, m.image, m.extra, null
		FROM jetton_masters j 
		LEFT JOIN address_metadata m ON j.address = m.address AND m.type = 'jetton_masters'
		WHERE j.address = $1
	`, addr)
	if err != nil {
		return data, err
	}
	defer rows.Close()

	for rows.Next() {
		var typ string
		var valid *bool
		var name, symbol, description, image *string
		var extra map[string]interface{}
		var nftIndex *string

		if err := rows.Scan(&typ, &valid, &name, &symbol, &description, &image, &extra, &nftIndex); err != nil {
			continue
		}

		// Set type flag
		switch typ {
		case "nft_items":
			data.NftItem = &models.NftItemData{Index: nftIndex}
		case "nft_collections":
			data.NftCollection = true
		case "jetton_masters":
			data.JettonMaster = true
		}

		// Store metadata entry
		data.Metadata[typ] = models.MetadataEntry{
			Exists:      true,
			Valid:       valid,
			Name:        name,
			Symbol:      symbol,
			Description: description,
			Image:       image,
			Extra:       extra,
		}
	}

	return data, rows.Err()
}

// Helper functions for type conversion from event data

func getString(data map[string]interface{}, key string) string {
	if v, ok := data[key].(string); ok {
		return v
	}
	return ""
}

func getStringPtr(data map[string]interface{}, key string) *string {
	if v, ok := data[key].(string); ok {
		return &v
	}
	return nil
}

func getInt64Ptr(data map[string]interface{}, key string) *int64 {
	switch v := data[key].(type) {
	case int64:
		return &v
	case int:
		i := int64(v)
		return &i
	case float64:
		i := int64(v)
		return &i
	}
	return nil
}

func getInt32Ptr(data map[string]interface{}, key string) *int32 {
	switch v := data[key].(type) {
	case int32:
		return &v
	case int:
		i := int32(v)
		return &i
	case int64:
		i := int32(v)
		return &i
	case float64:
		i := int32(v)
		return &i
	}
	return nil
}

func getBoolPtr(data map[string]interface{}, key string) *bool {
	if v, ok := data[key].(bool); ok {
		return &v
	}
	return nil
}

func getMap(data map[string]interface{}, key string) map[string]interface{} {
	if v, ok := data[key].(map[string]interface{}); ok {
		return v
	}
	return nil
}

func toUint32Slice(v interface{}) ([]uint32, bool) {
	switch arr := v.(type) {
	case []uint32:
		return arr, true
	case []interface{}:
		result := make([]uint32, 0, len(arr))
		for _, item := range arr {
			switch n := item.(type) {
			case uint32:
				result = append(result, n)
			case int:
				result = append(result, uint32(n))
			case int64:
				result = append(result, uint32(n))
			case float64:
				result = append(result, uint32(n))
			}
		}
		return result, true
	}
	return nil, false
}
