package loader

import (
	"context"
	"errors"
	"fmt"
	"github.com/toncenter/ton-indexer/ton-index-go/index"
	"log"
	"time"

	"ton-metadata-cache/cache"
	"ton-metadata-cache/models"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const cacheTTL = 1 * time.Hour

// Loader handles loading data from PostgreSQL into Redis cache.
type Loader struct {
	pool  *pgxpool.Pool
	cache *cache.Manager
}

// New creates a new Loader instance.
func New(pool *pgxpool.Pool, cache *cache.Manager) *Loader {
	return &Loader{
		pool:  pool,
		cache: cache,
	}
}

// LoadContractMethods loads all contract methods from the database into cache.
// Uses MSet for efficient batch insertion with no TTL.
func (l *Loader) LoadContractMethods(ctx context.Context) error {
	rows, err := l.pool.Query(ctx, "SELECT code_hash, methods FROM contract_methods")
	if err != nil {
		return fmt.Errorf("query contract_methods: %w", err)
	}
	defer rows.Close()

	batch := make(map[string][]uint32, 1000)
	count := 0

	for rows.Next() {
		var codeHash string
		var methods []uint32

		if err := rows.Scan(&codeHash, &methods); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}

		batch[codeHash] = methods
		count++

		// Flush batch every 1000 items
		if len(batch) >= 1000 {
			if err := l.cache.ContractMethods.MSet(ctx, batch, 0); err != nil {
				return fmt.Errorf("mset batch: %w", err)
			}
			batch = make(map[string][]uint32, 1000)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}

	// Flush remaining items
	if len(batch) > 0 {
		if err := l.cache.ContractMethods.MSet(ctx, batch, 0); err != nil {
			return fmt.Errorf("mset final batch: %w", err)
		}
	}

	log.Printf("Loaded %d contract methods into cache", count)
	return nil
}

// LoadDnsEntries loads all DNS entries from the database into cache.
// Also builds the DnsByOwner sorted set index for self-resolving entries (owner=wallet).
// Uses keyset pagination to avoid locking the entire table.
func (l *Loader) LoadDnsEntries(ctx context.Context) error {
	const batchSize = 5000

	// ownerEntries collects self-resolving entries per owner for sorted set
	// map[owner] -> []redis.Z{score: domainLength, member: nftItemAddress}
	ownerEntries := make(map[string][]redis.Z)
	totalCount := 0
	lastKey := ""

	for {
		rows, err := l.pool.Query(ctx, `
			SELECT nft_item_address, nft_item_owner, domain, dns_next_resolver, 
			       dns_wallet, dns_site_adnl, dns_storage_bag_id, last_transaction_lt 
			FROM dns_entries
			WHERE nft_item_address > $1
			ORDER BY nft_item_address
			LIMIT $2
		`, lastKey, batchSize)
		if err != nil {
			return fmt.Errorf("query dns_entries: %w", err)
		}

		batch := make(map[string]models.DnsEntry, batchSize)
		rowCount := 0

		for rows.Next() {
			var entry models.DnsEntry
			if err := rows.Scan(
				&entry.NftItemAddress,
				&entry.NftItemOwner,
				&entry.Domain,
				&entry.DnsNextResolver,
				&entry.DnsWallet,
				&entry.DnsSiteAdnl,
				&entry.DnsStorageBagId,
				&entry.LastTransactionLt,
			); err != nil {
				rows.Close()
				return fmt.Errorf("scan row: %w", err)
			}

			batch[entry.NftItemAddress] = entry
			lastKey = entry.NftItemAddress
			rowCount++

			// Collect self-resolving entries for sorted set (owner=wallet)
			if entry.NftItemOwner != nil && entry.DnsWallet != nil && entry.Domain != nil {
				if *entry.NftItemOwner == *entry.DnsWallet {
					owner := *entry.NftItemOwner
					ownerEntries[owner] = append(ownerEntries[owner], redis.Z{
						Score:  float64(len(*entry.Domain)),
						Member: entry.NftItemAddress,
					})
				}
			}
		}

		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("iterate rows: %w", err)
		}
		rows.Close()

		// Store batch in cache
		if len(batch) > 0 {
			if err := l.cache.DnsEntries.MSet(ctx, batch, 0); err != nil {
				return fmt.Errorf("mset batch: %w", err)
			}
		}

		totalCount += rowCount

		// If we got fewer rows than batch size, we're done
		if rowCount < batchSize {
			break
		}
	}

	// Store owner->nftAddresses sorted sets
	ownerCount := 0
	for owner, entries := range ownerEntries {
		if err := l.cache.DnsByOwner.AddBatch(ctx, owner, entries); err != nil {
			return fmt.Errorf("add owner dns entries: %w", err)
		}
		ownerCount++
	}

	log.Printf("Loaded %d DNS entries into cache, %d owners indexed", totalCount, ownerCount)
	return nil
}

// LoadAll loads all preloadable caches (contract_methods, dns_entries, jetton_masters).
func (l *Loader) LoadAll(ctx context.Context) error {
	if err := l.LoadContractMethods(ctx); err != nil {
		return fmt.Errorf("load contract methods: %w", err)
	}
	if err := l.LoadDnsEntries(ctx); err != nil {
		return fmt.Errorf("load dns entries: %w", err)
	}
	if err := l.LoadJettonMasters(ctx); err != nil {
		return fmt.Errorf("load jetton masters: %w", err)
	}
	return nil
}

// LoadJettonMasters loads all jetton masters with their metadata into cache.
func (l *Loader) LoadJettonMasters(ctx context.Context) error {
	const batchSize = 5000
	lastKey := ""
	totalCount := 0

	for {
		rows, err := l.pool.Query(ctx, `
			SELECT j.address, m.valid, m.name, m.symbol, m.description, m.image, m.extra
			FROM jetton_masters j
			LEFT JOIN address_metadata m ON j.address = m.address AND m.type = 'jetton_masters'
			WHERE j.address > $1
			ORDER BY j.address
			LIMIT $2
		`, lastKey, batchSize)
		if err != nil {
			return fmt.Errorf("query jetton_masters: %w", err)
		}

		batch := make(map[string]models.AddressData, batchSize)
		rowCount := 0

		for rows.Next() {
			var addr string
			var valid *bool
			var name, symbol, description, image *string
			var extra map[string]interface{}

			if err := rows.Scan(&addr, &valid, &name, &symbol, &description, &image, &extra); err != nil {
				rows.Close()
				return fmt.Errorf("scan row: %w", err)
			}

			entry := models.MetadataEntry{
				Exists:      valid != nil,
				Valid:       valid,
				Name:        name,
				Symbol:      symbol,
				Description: description,
				Image:       image,
				Extra:       extra,
			}

			batch[addr] = models.AddressData{
				JettonMaster: true,
				Metadata:     map[string]models.MetadataEntry{"jetton_masters": entry},
			}

			lastKey = addr
			rowCount++
		}
		rows.Close()

		if len(batch) > 0 {
			if err := l.cache.AddressInfo.MSet(ctx, batch, 0); err != nil {
				return fmt.Errorf("mset batch: %w", err)
			}
		}

		totalCount += rowCount
		if rowCount < batchSize {
			break
		}
	}

	log.Printf("Loaded %d jetton masters into cache", totalCount)
	return nil
}

// AfterConnectRegisterTypes registers custom PostgreSQL types on a connection.
// Use this with pgxpool.Config.AfterConnect.
func AfterConnectRegisterTypes(ctx context.Context, conn *pgx.Conn) error {
	typeNames := []string{
		"tonaddr",
		"_tonaddr",
		"tonhash",
		"_tonhash",
		"peer_swap_details",
		"_peer_swap_details",
		"liquidity_vault_excess_details",
		"_liquidity_vault_excess_details",
	}

	for _, typeName := range typeNames {
		dataType, err := conn.LoadType(ctx, typeName)
		if err != nil {
			return fmt.Errorf("load type '%s': %w", typeName, err)
		}
		conn.TypeMap().RegisterType(dataType)
	}
	return nil
}

func (l *Loader) fetchAccountStateFromDB(ctx context.Context, account string) (*models.LatestAccountState, error) {
	var state models.LatestAccountState
	err := l.pool.QueryRow(ctx, `
		SELECT code_hash FROM latest_account_states WHERE account = $1
	`, account).Scan(&state.CodeHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query account state: %w", err)
	}
	return &state, nil
}

func (l *Loader) fetchAccountStatesFromDB(ctx context.Context, accounts []string) (map[string]models.LatestAccountState, error) {
	rows, err := l.pool.Query(ctx, `
		SELECT account, code_hash FROM latest_account_states WHERE account = ANY($1)
	`, accounts)
	if err != nil {
		return nil, fmt.Errorf("query account states: %w", err)
	}
	defer rows.Close()

	result := make(map[string]models.LatestAccountState)
	for rows.Next() {
		var account string
		var state models.LatestAccountState
		if err := rows.Scan(&account, &state.CodeHash); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		result[account] = state
	}
	return result, rows.Err()
}

// GetAddressMetadata retrieves address metadata from cache, falling back to database.
// Extends TTL on cache hit.
func (l *Loader) GetAddressMetadata(ctx context.Context, address, metaType string) (*models.AddressMetadataEntry, error) {
	key := address + ":" + metaType

	// Try cache first
	entry, err := l.cache.AddressMetadata.GetEx(ctx, key, cacheTTL)
	if err == nil {
		return &entry, nil
	}
	if !errors.Is(err, cache.ErrNotFound) {
		return nil, fmt.Errorf("cache get: %w", err)
	}

	// Fetch from database
	entryPtr, err := l.fetchAddressMetadataFromDB(ctx, address, metaType)
	if err != nil {
		return nil, err
	}
	if entryPtr == nil {
		return nil, nil
	}

	// Save to cache
	if err := l.cache.AddressMetadata.Set(ctx, key, *entryPtr, cacheTTL); err != nil {
		log.Printf("Warning: failed to cache address metadata: %v", err)
	}

	return entryPtr, nil
}

// AddressMetadataKey represents a composite key for address_metadata.
type AddressMetadataKey struct {
	Address string
	Type    string
}

// GetAddressMetadataMulti retrieves multiple address metadata entries, using cache with DB fallback.
func (l *Loader) GetAddressMetadataMulti(ctx context.Context, keys []AddressMetadataKey) (map[string]models.AddressMetadataEntry, error) {
	if len(keys) == 0 {
		return make(map[string]models.AddressMetadataEntry), nil
	}

	// Build cache keys
	cacheKeys := make([]string, len(keys))
	keyMap := make(map[string]AddressMetadataKey, len(keys))
	for i, k := range keys {
		cacheKey := k.Address + ":" + k.Type
		cacheKeys[i] = cacheKey
		keyMap[cacheKey] = k
	}

	// Try cache first
	cached, err := l.cache.AddressMetadata.MGetEx(ctx, cacheTTL, cacheKeys...)
	if err != nil {
		return nil, fmt.Errorf("cache mget: %w", err)
	}

	// Find missing keys
	var missing []AddressMetadataKey
	for cacheKey, k := range keyMap {
		if _, ok := cached[cacheKey]; !ok {
			missing = append(missing, k)
		}
	}

	if len(missing) == 0 {
		return cached, nil
	}

	// Fetch missing from database
	fromDB, err := l.fetchAddressMetadataFromDBMulti(ctx, missing)
	if err != nil {
		return nil, err
	}

	// Cache the fetched values
	if len(fromDB) > 0 {
		if err := l.cache.AddressMetadata.MSet(ctx, fromDB, cacheTTL); err != nil {
			log.Printf("Warning: failed to cache address metadata: %v", err)
		}
	}

	// Merge results
	for k, v := range fromDB {
		cached[k] = v
	}

	return cached, nil
}

func (l *Loader) fetchAddressMetadataFromDB(ctx context.Context, address, metaType string) (*models.AddressMetadataEntry, error) {
	var entry models.AddressMetadataEntry
	err := l.pool.QueryRow(ctx, `
		SELECT address, type, valid, name, description, extra, symbol, image, updated_at, expires_at
		FROM address_metadata WHERE address = $1 AND type = $2
	`, address, metaType).Scan(
		&entry.Address, &entry.Type, &entry.Valid, &entry.Name, &entry.Description,
		&entry.Extra, &entry.Symbol, &entry.Image, &entry.UpdatedAt, &entry.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query address metadata: %w", err)
	}
	return &entry, nil
}

func (l *Loader) fetchAddressMetadataFromDBMulti(ctx context.Context, keys []AddressMetadataKey) (map[string]models.AddressMetadataEntry, error) {
	if len(keys) == 0 {
		return make(map[string]models.AddressMetadataEntry), nil
	}

	// Build query with multiple (address, type) pairs
	addresses := make([]string, len(keys))
	types := make([]string, len(keys))
	for i, k := range keys {
		addresses[i] = k.Address
		types[i] = k.Type
	}

	rows, err := l.pool.Query(ctx, `
		SELECT address, type, valid, name, description, extra, symbol, image, updated_at, expires_at
		FROM address_metadata 
		WHERE (address, type) IN (SELECT unnest($1::text[]), unnest($2::text[]))
	`, addresses, types)
	if err != nil {
		return nil, fmt.Errorf("query address metadata: %w", err)
	}
	defer rows.Close()

	result := make(map[string]models.AddressMetadataEntry)
	for rows.Next() {
		var entry models.AddressMetadataEntry
		if err := rows.Scan(
			&entry.Address, &entry.Type, &entry.Valid, &entry.Name, &entry.Description,
			&entry.Extra, &entry.Symbol, &entry.Image, &entry.UpdatedAt, &entry.ExpiresAt,
		); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		key := entry.Address + ":" + entry.Type
		result[key] = entry
	}
	return result, rows.Err()
}

// QueryAddressBook retrieves address book information from cache.
// Returns account states, detected interfaces, and DNS domains for the given addresses.
func (l *Loader) QueryAddressBook(ctx context.Context, addrList []string, isTestnet bool) (index.AddressBook, error) {
	if len(addrList) == 0 {
		return make(index.AddressBook), nil
	}

	// Convert input addresses to raw account format
	accountToInputAddr := make(map[string]string, len(addrList))
	accounts := make([]string, 0, len(addrList))
	for _, addr := range addrList {
		account := accountAddressConverter(addr)
		if account != "" {
			accountToInputAddr[account] = addr
			accounts = append(accounts, account)
		}
	}

	// Get account states from cache
	accountStates, err := l.cache.AccountStates.MGet(ctx, accounts...)
	if err != nil {
		return nil, fmt.Errorf("get account states: %w", err)
	}

	// Collect unique code hashes for method lookup
	codeHashes := make([]string, 0)
	codeHashSet := make(map[string]struct{})
	for _, state := range accountStates {
		if state.CodeHash != nil && *state.CodeHash != "" {
			if _, exists := codeHashSet[*state.CodeHash]; !exists {
				codeHashSet[*state.CodeHash] = struct{}{}
				codeHashes = append(codeHashes, *state.CodeHash)
			}
		}
	}

	// Get contract methods from cache
	var contractMethods map[string][]uint32
	if len(codeHashes) > 0 {
		contractMethods, err = l.cache.ContractMethods.MGet(ctx, codeHashes...)
		if err != nil {
			return nil, fmt.Errorf("get contract methods: %w", err)
		}
	} else {
		contractMethods = make(map[string][]uint32)
	}

	// Get DNS domains: first get nft addresses from sorted sets, then look up domains
	dnsDomains := make(map[string]string)
	nftAddresses, err := l.cache.DnsByOwner.GetLowestMulti(ctx, accounts...)
	if err != nil {
		return nil, fmt.Errorf("get dns owner addresses: %w", err)
	}

	// Collect nft addresses to look up
	if len(nftAddresses) > 0 {
		nftAddrs := make([]string, 0, len(nftAddresses))
		for _, nftAddr := range nftAddresses {
			nftAddrs = append(nftAddrs, nftAddr)
		}

		// Get DNS entries to extract domains
		dnsEntries, err := l.cache.DnsEntries.MGet(ctx, nftAddrs...)
		if err != nil {
			return nil, fmt.Errorf("get dns entries: %w", err)
		}

		// Build account -> domain map
		for account, nftAddr := range nftAddresses {
			if entry, ok := dnsEntries[nftAddr]; ok && entry.Domain != nil {
				dnsDomains[account] = *entry.Domain
			}
		}
	}

	// Build the address book
	book := make(index.AddressBook, len(addrList))

	for _, addr := range addrList {
		account := accountAddressConverter(addr)
		if account == "" {
			addrStr := index.GetAccountAddressFriendly(account, nil, isTestnet)
			emptyInterfaces := []string{}
			book[addr] = index.AddressBookRow{
				UserFriendly: &addrStr,
				Domain:       nil,
				Interfaces:   &emptyInterfaces,
			}
			continue
		}

		state, hasState := accountStates[account]

		var codeHash *string
		var methods []uint32
		if hasState && state.CodeHash != nil {
			codeHash = state.CodeHash
			if m, ok := contractMethods[*state.CodeHash]; ok {
				methods = m
			}
		}

		// Detect interfaces
		var interfaces []string
		if codeHash != nil || methods != nil {
			codeHashStr := ""
			if codeHash != nil {
				codeHashStr = *codeHash
			}
			interfaces = index.DetectInterface(codeHashStr, methods)
		} else {
			interfaces = []string{}
		}

		// Get friendly address
		addrStr := index.GetAccountAddressFriendly(account, codeHash, isTestnet)

		// Get domain if available
		var domain *string
		if d, ok := dnsDomains[account]; ok {
			domain = &d
		}

		book[addr] = index.AddressBookRow{
			UserFriendly: &addrStr,
			Domain:       domain,
			Interfaces:   &interfaces,
		}
	}

	return book, nil
}

func accountAddressConverter(addr string) string {
	account := ``
	if addr_val := index.AccountAddressConverter(addr); addr_val.IsValid() {
		if addr_str, ok := addr_val.Interface().(index.AccountAddress); ok {
			account = string(addr_str)
		}
	}
	return account
}

// fetchAddressDataBatch fetches complete data for multiple addresses from DB.
func (l *Loader) fetchAddressDataBatch(ctx context.Context, addresses []string) (map[string]models.AddressData, error) {
	result := make(map[string]models.AddressData)

	// Initialize empty entries for all addresses
	for _, addr := range addresses {
		result[addr] = models.AddressData{
			Metadata: make(map[string]models.MetadataEntry),
		}
	}

	// Query 1: Jetton wallets
	rows, err := l.pool.Query(ctx, `
		SELECT address, owner, jetton, balance FROM jetton_wallets WHERE address = ANY($1)
	`, addresses)
	if err == nil {
		for rows.Next() {
			var addr, owner, jetton string
			var balance *string
			if err := rows.Scan(&addr, &owner, &jetton, &balance); err != nil {
				continue
			}
			data := result[addr]
			data.JettonWallet = &models.JettonWalletData{Owner: owner, Jetton: jetton, Balance: balance}
			result[addr] = data
		}
		rows.Close()
	}

	// Query 2: UNION ALL for types + metadata
	rows, err = l.pool.Query(ctx, `
		SELECT n.address, 'nft_items' as type, m.valid, m.name, m.symbol, m.description, m.image, m.extra, n.index
		FROM nft_items n 
		LEFT JOIN address_metadata m ON n.address = m.address AND m.type = 'nft_items'
		WHERE n.address = ANY($1)
		UNION ALL
		SELECT c.address, 'nft_collections' as type, m.valid, m.name, m.symbol, m.description, m.image, m.extra, null
		FROM nft_collections c 
		LEFT JOIN address_metadata m ON c.address = m.address AND m.type = 'nft_collections'
		WHERE c.address = ANY($1)
		UNION ALL
		SELECT j.address, 'jetton_masters' as type, m.valid, m.name, m.symbol, m.description, m.image, m.extra, null
		FROM jetton_masters j 
		LEFT JOIN address_metadata m ON j.address = m.address AND m.type = 'jetton_masters'
		WHERE j.address = ANY($1)
	`, addresses)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	for rows.Next() {
		var addr, typ string
		var valid *bool
		var name, symbol, description, image *string
		var extra map[string]interface{}
		var nftIndex *string

		if err := rows.Scan(&addr, &typ, &valid, &name, &symbol, &description, &image, &extra, &nftIndex); err != nil {
			continue
		}

		data := result[addr]
		switch typ {
		case "nft_items":
			data.NftItem = &models.NftItemData{Index: nftIndex}
		case "nft_collections":
			data.NftCollection = true
		case "jetton_masters":
			data.JettonMaster = true
		}

		data.Metadata[typ] = models.MetadataEntry{
			Exists:      true,
			Valid:       valid,
			Name:        name,
			Symbol:      symbol,
			Description: description,
			Image:       image,
			Extra:       extra,
		}
		result[addr] = data
	}

	return result, nil
}

// buildMetadataResponse converts cached data to API response format.
func (l *Loader) buildMetadataResponse(cached map[string]models.AddressData, requestedAddrs []string) map[string]index.AddressMetadata {
	result := make(map[string]index.AddressMetadata)

	for _, addr := range requestedAddrs {
		data, ok := cached[addr]
		if !ok {
			continue
		}

		var tokenInfos []index.TokenInfo

		// Jetton wallet
		if data.JettonWallet != nil {
			extra := map[string]interface{}{
				"owner":  data.JettonWallet.Owner,
				"jetton": data.JettonWallet.Jetton,
			}
			if data.JettonWallet.Balance != nil {
				extra["balance"] = *data.JettonWallet.Balance
			}
			valid := true
			typ := "jetton_wallets"
			tokenInfos = append(tokenInfos, index.TokenInfo{
				Address: addr,
				Type:    &typ,
				Extra:   extra,
				Indexed: true,
				Valid:   &valid,
			})
		}

		// NFT item
		if data.NftItem != nil {
			tokenInfos = append(tokenInfos, l.buildTokenInfo(addr, "nft_items", data.Metadata, data.NftItem.Index))
		}

		// NFT collection
		if data.NftCollection {
			tokenInfos = append(tokenInfos, l.buildTokenInfo(addr, "nft_collections", data.Metadata, nil))
		}

		// Jetton master
		if data.JettonMaster {
			tokenInfos = append(tokenInfos, l.buildTokenInfo(addr, "jetton_masters", data.Metadata, nil))
		}

		if len(tokenInfos) > 0 {
			indexed := true
			for _, ti := range tokenInfos {
				indexed = indexed && ti.Indexed
			}
			result[addr] = index.AddressMetadata{
				TokenInfo: tokenInfos,
				IsIndexed: indexed,
			}
		}
	}

	return result
}

// buildTokenInfo creates a TokenInfo from metadata.
func (l *Loader) buildTokenInfo(addr, typ string, metadata map[string]models.MetadataEntry, nftIndex *string) index.TokenInfo {
	typCopy := typ
	info := index.TokenInfo{
		Address:  addr,
		Type:     &typCopy,
		NftIndex: nftIndex,
	}

	if meta, ok := metadata[typ]; ok && meta.Exists {
		info.Indexed = true
		info.Valid = meta.Valid
		if meta.Valid != nil && *meta.Valid {
			info.Name = meta.Name
			info.Symbol = meta.Symbol
			info.Description = meta.Description
			info.Image = meta.Image
			info.Extra = meta.Extra
		}
	} else {
		info.Indexed = false
	}

	return info
}

// QueryMetadata retrieves metadata for the given addresses using cache with DB fallback.
// Returns a map of address -> AddressMetadata.
func (l *Loader) QueryMetadata(ctx context.Context, addrList []string) (map[string]index.AddressMetadata, error) {
	if len(addrList) == 0 {
		return make(map[string]index.AddressMetadata), nil
	}

	// Convert addresses to raw account format
	accounts := make([]string, 0, len(addrList))
	for _, addr := range addrList {
		account := accountAddressConverter(addr)
		if account != "" {
			accounts = append(accounts, account)
		}
	}

	// 1. Batch get from cache
	cached, err := l.cache.AddressInfo.MGet(ctx, accounts...)
	if err != nil {
		return nil, fmt.Errorf("cache mget: %w", err)
	}

	// 2. Find missing addresses
	var missing []string
	for _, addr := range accounts {
		if _, ok := cached[addr]; !ok {
			missing = append(missing, addr)
		}
	}

	// 3. Fetch missing from DB and back-propagate to cache
	if len(missing) > 0 {
		fromDB, err := l.fetchAddressDataBatch(ctx, missing)
		if err != nil {
			return nil, fmt.Errorf("fetch from db: %w", err)
		}

		// Back-propagate to cache
		if len(fromDB) > 0 {
			if err := l.cache.AddressInfo.MSet(ctx, fromDB, cacheTTL); err != nil {
				log.Printf("Warning: failed to cache fetched data: %v", err)
			}
		}

		// Merge into cached
		for k, v := range fromDB {
			cached[k] = v
		}
	}

	// 4. Collect jetton wallet master addresses for additional lookup
	masterAddrs := make(map[string]struct{})
	for _, data := range cached {
		if data.JettonWallet != nil {
			masterAddrs[data.JettonWallet.Jetton] = struct{}{}
		}
	}

	// Remove already-fetched masters
	for addr := range cached {
		delete(masterAddrs, addr)
	}

	// 5. Fetch master addresses (cache first, then DB)
	if len(masterAddrs) > 0 {
		masterList := make([]string, 0, len(masterAddrs))
		for addr := range masterAddrs {
			masterList = append(masterList, addr)
		}

		mastersCached, _ := l.cache.AddressInfo.MGet(ctx, masterList...)

		var mastersMissing []string
		for _, addr := range masterList {
			if _, ok := mastersCached[addr]; !ok {
				mastersMissing = append(mastersMissing, addr)
			}
		}

		if len(mastersMissing) > 0 {
			mastersFromDB, _ := l.fetchAddressDataBatch(ctx, mastersMissing)
			if len(mastersFromDB) > 0 {
				l.cache.AddressInfo.MSet(ctx, mastersFromDB, cacheTTL)
			}
			for k, v := range mastersFromDB {
				mastersCached[k] = v
			}
		}

		// Merge masters into cached
		for k, v := range mastersCached {
			cached[k] = v
		}
	}

	// 6. Build TokenInfo response
	return l.buildMetadataResponse(cached, accounts), nil
}
