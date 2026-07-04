package crud

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/toncenter/ton-indexer/ton-index-go/index/detect"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/services"
)

const kvrocksKeyPrefix = "ton-index:v1"
const kvrocksAddressBookStateScriptBatchSize = 200
const kvrocksNominatorPoolsBatchSize = 1000

// Address book only needs code_hash and contract methods; keep BOC-heavy account state payloads inside Kvrocks.
var kvrocksAddressBookStateScript = redis.NewScript(`
local prefix = ARGV[1]
local out = {}

for i = 2, #ARGV do
  local address = ARGV[i]
  local payload = redis.call('GET', prefix .. ':latest_account_states:' .. address .. ':payload')
  if payload then
    local row = cjson.decode(payload)
    local code_hash = row['code_hash']
    if type(code_hash) ~= 'string' then
      code_hash = ''
    end

    local methods = ''
    if code_hash ~= '' then
      local methods_payload = redis.call('GET', prefix .. ':contract_methods:' .. code_hash .. ':payload')
      if methods_payload then
        local methods_row = cjson.decode(methods_payload)
        local methods_value = methods_row['methods']
        if type(methods_value) == 'string' then
          methods = methods_value
        end
      end
    end

    table.insert(out, address)
    table.insert(out, code_hash)
    table.insert(out, methods)
  end
end

return out
`)

type KvrocksConfig struct {
	Addr               string
	SentinelAddrs      []string
	SentinelMasterName string
	Username           string
	Password           string
	DB                 int

	ReplicaReads    bool
	StalenessBlocks int64
	ReplicaRefresh  time.Duration
}

type KvrocksStore struct {
	client   redis.UniversalClient
	replicas *kvrocksReplicaSet
}

// Immutable hash-addressed payloads referenced by postgres rows: the worker
// writes them to kvrocks before the postgres commit, so a miss on a replica
// always means replication lag and the key can be refetched from the master.
var kvrocksRefillTables = map[string]struct{}{
	"message_contents": {},
	"account_states":   {},
}

func NewKvrocksStore(cfg KvrocksConfig) (*KvrocksStore, error) {
	var client redis.UniversalClient

	if len(cfg.SentinelAddrs) > 0 {
		if cfg.SentinelMasterName == "" {
			return nil, fmt.Errorf("kvrocks sentinel master name must be configured")
		}
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.SentinelMasterName,
			SentinelAddrs: cfg.SentinelAddrs,
			Username:      cfg.Username,
			Password:      cfg.Password,
			DB:            cfg.DB,
		})
	} else {
		if cfg.Addr == "" {
			return nil, fmt.Errorf("kvrocks address or sentinels must be configured")
		}
		if strings.Contains(cfg.Addr, "://") {
			options, err := redis.ParseURL(cfg.Addr)
			if err != nil {
				return nil, err
			}
			client = redis.NewClient(options)
		} else {
			client = redis.NewClient(&redis.Options{
				Addr:     cfg.Addr,
				Username: cfg.Username,
				Password: cfg.Password,
				DB:       cfg.DB,
			})
		}
	}

	if err := client.Ping(context.Background()).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	if err := kvrocksAddressBookStateScript.Load(context.Background(), client).Err(); err != nil {
		log.Printf("failed to preload kvrocks address book script: %v", err)
	}

	store := &KvrocksStore{client: client}
	if cfg.ReplicaReads && len(cfg.SentinelAddrs) > 0 {
		if cfg.ReplicaRefresh <= 0 {
			cfg.ReplicaRefresh = 300 * time.Millisecond
		}
		if cfg.StalenessBlocks < 0 {
			cfg.StalenessBlocks = 0
		}
		store.replicas = newKvrocksReplicaSet(cfg, client)
		log.Printf("kvrocks replica reads enabled: staleness <= %d blocks, refresh every %s", cfg.StalenessBlocks, cfg.ReplicaRefresh)
	}
	return store, nil
}

func (s *KvrocksStore) Close() error {
	if s == nil {
		return nil
	}
	s.replicas.close()
	if s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *KvrocksStore) key(table string, id string) string {
	return kvrocksKeyPrefix + ":" + table + ":" + strings.Trim(id, " ")
}

func (s *KvrocksStore) payloadKey(table string, id string) string {
	return s.key(table, id) + ":payload"
}

func (s *KvrocksStore) getPayloads(ctx context.Context, table string, ids []string) (map[string]string, error) {
	payloads := make(map[string]string)
	if s == nil || s.client == nil || len(ids) == 0 {
		return payloads, nil
	}

	seen := make(map[string]struct{})
	uniqueIDs := make([]string, 0, len(ids))
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.Trim(id, " ")
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		uniqueIDs = append(uniqueIDs, id)
		keys = append(keys, s.payloadKey(table, id))
	}
	if len(keys) == 0 {
		return payloads, nil
	}

	values, replica, err := kvReadWithFallback(ctx, s, func(c redis.UniversalClient) ([]interface{}, error) {
		return c.MGet(ctx, keys...).Result()
	})
	if err != nil {
		return nil, err
	}

	missing := make([]int, 0)
	for idx, value := range values {
		if value == nil {
			missing = append(missing, idx)
			continue
		}
		payload, err := kvrocksPayloadString(value)
		if err != nil {
			return nil, fmt.Errorf("%w for %s", err, uniqueIDs[idx])
		}
		payloads[uniqueIDs[idx]] = payload
	}

	// A replica may not have received the newest immutable payloads yet;
	// refetch exactly the missing keys from the master. Intermediate account
	// states (non-last transaction of an account within a block) are never
	// written to kvrocks at all, so when the replica already covers the
	// requested blocks a miss is a legitimate absence and nothing is refetched.
	readOptions := kvrocksReadOptionsFromContext(ctx)
	if _, refill := kvrocksRefillTables[table]; refill && replica != nil && len(missing) > 0 && replica.mayLagBehind(readOptions.maxKnownMcSeqno) {
		missingKeys := make([]string, len(missing))
		for i, idx := range missing {
			missingKeys[i] = keys[idx]
		}
		refillValues, err := s.client.MGet(ctx, missingKeys...).Result()
		if err != nil {
			return nil, err
		}
		if s.replicas != nil {
			s.replicas.refilledKeys.Add(uint64(len(missingKeys)))
		}
		for i, value := range refillValues {
			if value == nil {
				continue
			}
			idx := missing[i]
			payload, err := kvrocksPayloadString(value)
			if err != nil {
				return nil, fmt.Errorf("%w for %s", err, uniqueIDs[idx])
			}
			payloads[uniqueIDs[idx]] = payload
		}
	}
	return payloads, nil
}

func kvrocksPayloadString(value interface{}) (string, error) {
	switch payload := value.(type) {
	case string:
		return payload, nil
	case []byte:
		return string(payload), nil
	default:
		return "", fmt.Errorf("unexpected kvrocks payload type %T", value)
	}
}

func kvrocksPayloadDestroyed(id string, table string, payload string) (bool, error) {
	var row struct {
		Destroyed bool `json:"destroyed"`
	}
	if err := json.Unmarshal([]byte(payload), &row); err != nil {
		return false, fmt.Errorf("decode %s %s destroyed flag: %w", table, id, err)
	}
	return row.Destroyed, nil
}

func (s *KvrocksStore) indexKey(table string, name string) string {
	return kvrocksKeyPrefix + ":idx:" + table + ":" + name
}

func kvrocksLimitOffset(lim models.LimitParams, settings models.RequestSettings) (int64, int64, error) {
	limit := int32(settings.DefaultLimit)
	if lim.Limit != nil {
		limit = *lim.Limit
	}
	if limit < 1 {
		limit = 1
	}
	if limit > int32(settings.MaxLimit) {
		return 0, 0, models.IndexError{Code: 422, Message: fmt.Sprintf("limit is not allowed: %d > %d", limit, settings.MaxLimit)}
	}

	offset := int32(0)
	if lim.Offset != nil {
		offset = *lim.Offset
	}
	if offset < 0 {
		offset = 0
	}
	return int64(limit), int64(offset), nil
}

func kvrocksSortDesc(lim models.LimitParams, defaultDesc bool) (bool, error) {
	if lim.Sort == nil {
		return defaultDesc, nil
	}
	sortOrder, err := getSortOrder(*lim.Sort)
	if err != nil {
		return false, err
	}
	return sortOrder == "desc", nil
}

func (s *KvrocksStore) rangeByID(ctx context.Context, table string, indexName string, limit int64, offset int64, desc bool) ([]string, error) {
	if s == nil || s.client == nil || limit <= 0 {
		return []string{}, nil
	}
	start := offset
	stop := offset + limit - 1
	key := s.indexKey(table, indexName)
	values, _, err := kvReadWithFallback(ctx, s, func(c redis.UniversalClient) ([]string, error) {
		if desc {
			return c.ZRevRange(ctx, key, start, stop).Result()
		}
		return c.ZRange(ctx, key, start, stop).Result()
	})
	return values, err
}

func (s *KvrocksStore) rangeByLex(ctx context.Context, table string, indexName string, limit int64, offset int64, desc bool) ([]string, error) {
	if s == nil || s.client == nil || limit <= 0 {
		return []string{}, nil
	}
	key := s.indexKey(table, indexName)
	opt := &redis.ZRangeBy{Min: "-", Max: "+", Offset: offset, Count: limit}
	values, _, err := kvReadWithFallback(ctx, s, func(c redis.UniversalClient) ([]string, error) {
		if desc {
			return c.ZRevRangeByLex(ctx, key, opt).Result()
		}
		return c.ZRangeByLex(ctx, key, opt).Result()
	})
	return values, err
}

func (s *KvrocksStore) rangeByLexBounds(ctx context.Context, table string, indexName string, min string, max string, limit int64, offset int64, desc bool) ([]string, error) {
	if s == nil || s.client == nil || limit <= 0 {
		return []string{}, nil
	}
	key := s.indexKey(table, indexName)
	opt := &redis.ZRangeBy{Min: min, Max: max, Offset: offset, Count: limit}
	values, _, err := kvReadWithFallback(ctx, s, func(c redis.UniversalClient) ([]string, error) {
		if desc {
			return c.ZRevRangeByLex(ctx, key, opt).Result()
		}
		return c.ZRangeByLex(ctx, key, opt).Result()
	})
	return values, err
}

func (s *KvrocksStore) rangeByIDWithScores(ctx context.Context, table string, indexName string, limit int64, offset int64, desc bool) ([]redis.Z, error) {
	if s == nil || s.client == nil || limit <= 0 {
		return []redis.Z{}, nil
	}
	start := offset
	stop := offset + limit - 1
	key := s.indexKey(table, indexName)
	values, _, err := kvReadWithFallback(ctx, s, func(c redis.UniversalClient) ([]redis.Z, error) {
		if desc {
			return c.ZRevRangeWithScores(ctx, key, start, stop).Result()
		}
		return c.ZRangeWithScores(ctx, key, start, stop).Result()
	})
	return values, err
}

func appendUniqueString(dst []string, seen map[string]struct{}, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return dst
	}
	if _, ok := seen[value]; ok {
		return dst
	}
	seen[value] = struct{}{}
	return append(dst, value)
}

func redisMemberString(member any) string {
	switch v := member.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

type kvrocksLexIndexRange struct {
	indexName  string
	min        string
	max        string
	sortPrefix string
}

func allLexIndexRange(indexName string) kvrocksLexIndexRange {
	return kvrocksLexIndexRange{indexName: indexName, min: "-", max: "+"}
}

func prefixLexIndexRange(indexName string, prefix string) kvrocksLexIndexRange {
	return kvrocksLexIndexRange{indexName: indexName, min: "[" + prefix, max: "[" + prefix + "\xff"}
}

func prefixLexIndexRangeWithSortPrefix(indexName string, prefix string, sortPrefix string) kvrocksLexIndexRange {
	return kvrocksLexIndexRange{
		indexName:  indexName,
		min:        "[" + prefix,
		max:        "[" + prefix + "\xff",
		sortPrefix: sortPrefix,
	}
}

func allLexIndexRangeWithSortPrefix(indexName string, sortPrefix string) kvrocksLexIndexRange {
	return kvrocksLexIndexRange{indexName: indexName, min: "-", max: "+", sortPrefix: sortPrefix}
}

type kvrocksLexMember struct {
	sortValue string
	id        string
}

func (s *KvrocksStore) mergedLexIndexIDs(ctx context.Context, table string, ranges []kvrocksLexIndexRange, limit int64, offset int64, desc bool) ([]string, error) {
	need := limit + offset
	if need <= 0 || len(ranges) == 0 {
		return []string{}, nil
	}
	members := make([]kvrocksLexMember, 0, need*int64(len(ranges)))
	for _, indexRange := range ranges {
		indexMembers, err := s.rangeByLexBounds(ctx, table, indexRange.indexName, indexRange.min, indexRange.max, need, 0, desc)
		if err != nil {
			return nil, err
		}
		for _, member := range indexMembers {
			members = append(members, kvrocksLexMember{
				sortValue: indexRange.sortPrefix + member,
				id:        addressSuffixFromLexMember(member),
			})
		}
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].sortValue < members[j].sortValue
	})
	if desc {
		for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
			members[i], members[j] = members[j], members[i]
		}
	}

	seen := map[string]struct{}{}
	ids := make([]string, 0, need)
	for _, member := range members {
		ids = appendUniqueString(ids, seen, member.id)
		if int64(len(ids)) >= need {
			break
		}
	}
	if offset >= int64(len(ids)) {
		return []string{}, nil
	}
	end := offset + limit
	if end > int64(len(ids)) {
		end = int64(len(ids))
	}
	return ids[offset:end], nil
}

type kvrocksScoredID struct {
	id    string
	score float64
}

func (s *KvrocksStore) mergedIDIndexIDs(ctx context.Context, table string, indexes []string, limit int64, offset int64, desc bool) ([]string, error) {
	need := limit + offset
	if need <= 0 || len(indexes) == 0 {
		return []string{}, nil
	}
	byID := map[string]kvrocksScoredID{}
	for _, indexName := range indexes {
		rows, err := s.rangeByIDWithScores(ctx, table, indexName, need, 0, desc)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			id := redisMemberString(row.Member)
			if id == "" {
				continue
			}
			old, ok := byID[id]
			if !ok || (!desc && row.Score < old.score) || (desc && row.Score > old.score) {
				byID[id] = kvrocksScoredID{id: id, score: row.Score}
			}
		}
	}
	rows := make([]kvrocksScoredID, 0, len(byID))
	for _, row := range byID {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].score == rows[j].score {
			if desc {
				return rows[i].id > rows[j].id
			}
			return rows[i].id < rows[j].id
		}
		if desc {
			return rows[i].score > rows[j].score
		}
		return rows[i].score < rows[j].score
	})
	if offset >= int64(len(rows)) {
		return []string{}, nil
	}
	end := offset + limit
	if end > int64(len(rows)) {
		end = int64(len(rows))
	}
	ids := make([]string, 0, end-offset)
	for _, row := range rows[offset:end] {
		ids = append(ids, row.id)
	}
	return ids, nil
}

func addressesToIDs(addresses []models.AccountAddress) []string {
	ids := make([]string, 0, len(addresses))
	for _, address := range addresses {
		ids = append(ids, string(address))
	}
	return ids
}

func uniqueAccountAddresses(addresses []models.AccountAddress) []models.AccountAddress {
	seen := make(map[models.AccountAddress]struct{}, len(addresses))
	unique := make([]models.AccountAddress, 0, len(addresses))
	for _, address := range addresses {
		if (&address).IsAddressNone() {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}
		unique = append(unique, address)
	}
	return unique
}

func stringsToAddresses(ids []string) []models.AccountAddress {
	addresses := make([]models.AccountAddress, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			addresses = append(addresses, models.AccountAddress(id))
		}
	}
	return addresses
}

func addressSuffixFromLexMember(member string) string {
	if len(member) < 66 {
		return member
	}
	hashColon := len(member) - 65
	if hashColon < 0 || hashColon >= len(member) || member[hashColon] != ':' {
		return member
	}
	start := hashColon - 1
	for start >= 0 && member[start] != ':' {
		start--
	}
	return member[start+1:]
}

func addressIDsFromLexMembers(members []string) []string {
	ids := make([]string, 0, len(members))
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		ids = appendUniqueString(ids, seen, addressSuffixFromLexMember(member))
	}
	return ids
}

func padDecimalString(value string, width int) string {
	value = strings.TrimSpace(value)
	if len(value) >= width {
		return value
	}
	return strings.Repeat("0", width-len(value)) + value
}

func (s *KvrocksStore) orderedJettonMasters(ctx context.Context, ids []string) ([]models.JettonMaster, error) {
	payloads, err := s.getPayloads(ctx, "jetton_masters", ids)
	if err != nil {
		return nil, err
	}
	res := make([]models.JettonMaster, 0, len(ids))
	for _, id := range ids {
		payload, ok := payloads[id]
		if !ok {
			continue
		}
		destroyed, err := kvrocksPayloadDestroyed(id, "jetton_masters", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		var row models.JettonMaster
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode jetton_masters %s: %w", id, err)
		}
		res = append(res, row)
	}
	return res, nil
}

func (s *KvrocksStore) orderedJettonWallets(ctx context.Context, ids []string) ([]models.JettonWallet, error) {
	payloads, err := s.getPayloads(ctx, "jetton_wallets", ids)
	if err != nil {
		return nil, err
	}
	res := make([]models.JettonWallet, 0, len(ids))
	for _, id := range ids {
		payload, ok := payloads[id]
		if !ok {
			continue
		}
		destroyed, err := kvrocksPayloadDestroyed(id, "jetton_wallets", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		row, err := decodeJettonWalletPayload(id, payload)
		if err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (s *KvrocksStore) orderedNFTCollections(ctx context.Context, ids []string) ([]models.NFTCollection, error) {
	payloads, err := s.getPayloads(ctx, "nft_collections", ids)
	if err != nil {
		return nil, err
	}
	res := make([]models.NFTCollection, 0, len(ids))
	for _, id := range ids {
		payload, ok := payloads[id]
		if !ok {
			continue
		}
		destroyed, err := kvrocksPayloadDestroyed(id, "nft_collections", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		var row models.NFTCollection
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode nft_collections %s: %w", id, err)
		}
		res = append(res, row)
	}
	return res, nil
}

func (s *KvrocksStore) orderedNFTItems(ctx context.Context, ids []string) ([]models.NFTItem, error) {
	payloads, err := s.getPayloads(ctx, "nft_items", ids)
	if err != nil {
		return nil, err
	}
	res := make([]models.NFTItem, 0, len(ids))
	for _, id := range ids {
		payload, ok := payloads[id]
		if !ok {
			continue
		}
		destroyed, err := kvrocksPayloadDestroyed(id, "nft_items", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		var row models.NFTItem
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode nft_items %s: %w", id, err)
		}
		res = append(res, row)
	}
	return res, nil
}

func (s *KvrocksStore) GetMessageContents(ctx context.Context, hashes []models.HashType) (map[models.HashType]*models.MessageContent, error) {
	ids := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		ids = append(ids, string(hash))
	}
	payloads, err := s.getPayloads(ctx, "message_contents", ids)
	if err != nil {
		return nil, err
	}

	res := make(map[models.HashType]*models.MessageContent, len(payloads))
	for id, payload := range payloads {
		var row struct {
			Hash models.HashType   `json:"hash"`
			Body *models.BytesType `json:"body"`
		}
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode message_contents %s: %w", id, err)
		}
		hash := row.Hash
		res[hash] = &models.MessageContent{Hash: &hash, Body: row.Body}
	}
	return res, nil
}

func (s *KvrocksStore) GetAccountStates(ctx context.Context, hashes []models.HashType) (map[models.HashType]*models.AccountState, error) {
	ids := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		ids = append(ids, string(hash))
	}
	payloads, err := s.getPayloads(ctx, "account_states", ids)
	if err != nil {
		return nil, err
	}

	res := make(map[models.HashType]*models.AccountState, len(payloads))
	for id, payload := range payloads {
		var row struct {
			Hash                   models.HashType        `json:"hash"`
			Account                *models.AccountAddress `json:"account"`
			Balance                *string                `json:"balance"`
			BalanceExtraCurrencies map[string]string      `json:"balance_extra_currencies"`
			AccountStatus          *string                `json:"account_status"`
			FrozenHash             *models.HashType       `json:"frozen_hash"`
			DataHash               *models.HashType       `json:"data_hash"`
			CodeHash               *models.HashType       `json:"code_hash"`
		}
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode account_states %s: %w", id, err)
		}
		res[row.Hash] = &models.AccountState{
			Hash:                   row.Hash,
			Account:                row.Account,
			Balance:                row.Balance,
			BalanceExtraCurrencies: row.BalanceExtraCurrencies,
			AccountStatus:          row.AccountStatus,
			FrozenHash:             row.FrozenHash,
			DataHash:               row.DataHash,
			CodeHash:               row.CodeHash,
		}
	}
	return res, nil
}

type kvLatestAccountStatePayload struct {
	Account                models.AccountAddress `json:"account"`
	Hash                   models.HashType       `json:"hash"`
	Balance                *string               `json:"balance"`
	BalanceExtraCurrencies map[string]string     `json:"balance_extra_currencies"`
	AccountStatus          *string               `json:"account_status"`
	LastTransHash          *models.HashType      `json:"last_trans_hash"`
	LastTransLt            *jsonInt64            `json:"last_trans_lt"`
	FrozenHash             *models.HashType      `json:"frozen_hash"`
	DataHash               *models.HashType      `json:"data_hash"`
	CodeHash               *models.HashType      `json:"code_hash"`
	DataBoc                *models.BytesType     `json:"data_boc"`
	CodeBoc                *models.BytesType     `json:"code_boc"`
	ContractMethods        *[]uint32             `json:"-"`
}

func (p kvLatestAccountStatePayload) accountStateFull(includeBOC bool) models.AccountStateFull {
	account := p.Account
	dataBoc := p.DataBoc
	codeBoc := p.CodeBoc
	if !includeBOC {
		dataBoc = nil
		codeBoc = nil
	}
	return models.AccountStateFull{
		AccountAddress:         &account,
		Hash:                   p.Hash,
		Balance:                p.Balance,
		BalanceExtraCurrencies: p.BalanceExtraCurrencies,
		AccountStatus:          p.AccountStatus,
		FrozenHash:             p.FrozenHash,
		LastTransactionHash:    p.LastTransHash,
		LastTransactionLt:      p.LastTransLt.ptr(),
		DataHash:               p.DataHash,
		CodeHash:               p.CodeHash,
		DataBoc:                dataBoc,
		CodeBoc:                codeBoc,
		ContractMethods:        p.ContractMethods,
	}
}

func parseContractMethods(value string) ([]uint32, error) {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "{")
	value = strings.TrimSuffix(value, "}")
	if value == "" {
		return []uint32{}, nil
	}
	parts := strings.Split(value, ",")
	methods := make([]uint32, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return nil, err
		}
		methods = append(methods, uint32(n))
	}
	return methods, nil
}

func (s *KvrocksStore) GetContractMethods(ctx context.Context, codeHashes []models.HashType) (map[models.HashType][]uint32, error) {
	ids := make([]string, 0, len(codeHashes))
	seen := map[string]struct{}{}
	for _, hash := range codeHashes {
		ids = appendUniqueString(ids, seen, string(hash))
	}
	payloads, err := s.getPayloads(ctx, "contract_methods", ids)
	if err != nil {
		return nil, err
	}
	res := make(map[models.HashType][]uint32, len(payloads))
	for id, payload := range payloads {
		var row struct {
			CodeHash models.HashType `json:"code_hash"`
			Methods  string          `json:"methods"`
		}
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode contract_methods %s: %w", id, err)
		}
		methods, err := parseContractMethods(row.Methods)
		if err != nil {
			return nil, fmt.Errorf("decode contract_methods %s: %w", id, err)
		}
		res[row.CodeHash] = methods
	}
	return res, nil
}

type kvrocksAddressBookState struct {
	CodeHash        *models.HashType
	ContractMethods *[]uint32
}

func redisString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case nil:
		return "", nil
	default:
		return "", fmt.Errorf("unexpected redis string value type %T", value)
	}
}

func parseAddressBookStateScriptResult(raw any) (map[models.AccountAddress]*kvrocksAddressBookState, error) {
	rows, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected address book script result type %T", raw)
	}
	if len(rows)%3 != 0 {
		return nil, fmt.Errorf("unexpected address book script result length %d", len(rows))
	}

	res := make(map[models.AccountAddress]*kvrocksAddressBookState, len(rows)/3)
	for i := 0; i < len(rows); i += 3 {
		address, err := redisString(rows[i])
		if err != nil {
			return nil, err
		}
		codeHash, err := redisString(rows[i+1])
		if err != nil {
			return nil, err
		}
		methodsValue, err := redisString(rows[i+2])
		if err != nil {
			return nil, err
		}
		if address == "" {
			continue
		}

		state := kvrocksAddressBookState{}
		if codeHash != "" {
			loc := models.HashType(codeHash)
			state.CodeHash = &loc
		}
		if methodsValue != "" {
			methods, err := parseContractMethods(methodsValue)
			if err != nil {
				return nil, fmt.Errorf("decode contract_methods %s: %w", codeHash, err)
			}
			state.ContractMethods = &methods
		}
		addr := models.AccountAddress(address)
		res[addr] = &state
	}
	return res, nil
}

func (s *KvrocksStore) getAddressBookStates(ctx context.Context, addresses []models.AccountAddress) (map[models.AccountAddress]*kvrocksAddressBookState, error) {
	states := make(map[models.AccountAddress]*kvrocksAddressBookState)
	if s == nil || s.client == nil || len(addresses) == 0 {
		return states, nil
	}

	for start := 0; start < len(addresses); start += kvrocksAddressBookStateScriptBatchSize {
		end := start + kvrocksAddressBookStateScriptBatchSize
		if end > len(addresses) {
			end = len(addresses)
		}
		args := make([]any, 0, end-start+1)
		args = append(args, kvrocksKeyPrefix)
		for _, address := range addresses[start:end] {
			args = append(args, address.String())
		}

		raw, _, err := kvReadWithFallback(ctx, s, func(c redis.UniversalClient) (interface{}, error) {
			return kvrocksAddressBookStateScript.RunRO(ctx, c, nil, args...).Result()
		})
		if err != nil {
			return nil, err
		}
		batchStates, err := parseAddressBookStateScriptResult(raw)
		if err != nil {
			return nil, err
		}
		for address, state := range batchStates {
			states[address] = state
		}
	}
	return states, nil
}

func (s *KvrocksStore) getLatestAccountStatesByIDs(ctx context.Context, ids []string, includeBOC bool) ([]models.AccountStateFull, error) {
	ctx = s.pinReadSnapshot(ctx)
	payloads, err := s.getPayloads(ctx, "latest_account_states", ids)
	if err != nil {
		return nil, err
	}
	rows := make([]kvLatestAccountStatePayload, 0, len(ids))
	codeHashes := make([]models.HashType, 0, len(ids))
	seenCodeHashes := map[string]struct{}{}
	for _, id := range ids {
		payload, ok := payloads[id]
		if !ok {
			continue
		}
		var row kvLatestAccountStatePayload
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode latest_account_states %s: %w", id, err)
		}
		if row.CodeHash != nil {
			if _, ok := seenCodeHashes[string(*row.CodeHash)]; !ok {
				seenCodeHashes[string(*row.CodeHash)] = struct{}{}
				codeHashes = append(codeHashes, *row.CodeHash)
			}
		}
		rows = append(rows, row)
	}

	methodsByCodeHash, err := s.GetContractMethods(ctx, codeHashes)
	if err != nil {
		return nil, err
	}
	res := make([]models.AccountStateFull, 0, len(rows))
	for _, row := range rows {
		if row.CodeHash != nil {
			if methods, ok := methodsByCodeHash[*row.CodeHash]; ok {
				methodsCopy := append([]uint32(nil), methods...)
				row.ContractMethods = &methodsCopy
			}
		}
		res = append(res, row.accountStateFull(includeBOC))
	}
	if err := detect.MarkAccountStates(res); err != nil {
		log.Printf("Error marking account states from kvrocks: %v", err)
	}
	return res, nil
}

func (s *KvrocksStore) GetLatestAccountStates(ctx context.Context, addresses []models.AccountAddress, includeBOC bool) (map[models.AccountAddress]*models.AccountStateFull, error) {
	ids := addressesToIDs(addresses)
	states, err := s.getLatestAccountStatesByIDs(ctx, ids, includeBOC)
	if err != nil {
		return nil, err
	}
	res := make(map[models.AccountAddress]*models.AccountStateFull, len(states))
	for i := range states {
		if states[i].AccountAddress == nil {
			continue
		}
		res[*states[i].AccountAddress] = &states[i]
	}
	return res, nil
}

func (s *KvrocksStore) QueryAccountStates(ctx context.Context, accountReq models.AccountRequest, settings models.RequestSettings) ([]models.AccountStateFull, error) {
	ctx = s.pinReadSnapshot(ctx)
	includeBOC := true
	if accountReq.IncludeBOC != nil {
		includeBOC = *accountReq.IncludeBOC
	}

	ids := []string{}
	seen := map[string]struct{}{}
	if len(accountReq.AccountAddress) > 0 {
		for _, address := range accountReq.AccountAddress {
			ids = appendUniqueString(ids, seen, string(address))
		}
	} else if len(accountReq.CodeHash) > 0 {
		indexNames := make([]string, 0, len(accountReq.CodeHash))
		for _, codeHash := range accountReq.CodeHash {
			indexNames = append(indexNames, "code_hash:"+string(codeHash)+":by_account")
		}
		var err error
		ids, err = s.mergedIDIndexIDs(ctx, "latest_account_states", indexNames, 1000, 0, false)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		ids, err = s.rangeByID(ctx, "latest_account_states", "all:by_account", 1000, 0, false)
		if err != nil {
			return nil, err
		}
	}

	states, err := s.getLatestAccountStatesByIDs(ctx, ids, includeBOC)
	if err != nil {
		return nil, err
	}
	if len(accountReq.CodeHash) > 0 && len(accountReq.AccountAddress) > 0 {
		allowed := map[models.HashType]struct{}{}
		for _, hash := range accountReq.CodeHash {
			allowed[hash] = struct{}{}
		}
		filtered := states[:0]
		for _, state := range states {
			if state.CodeHash == nil {
				continue
			}
			if _, ok := allowed[*state.CodeHash]; ok {
				filtered = append(filtered, state)
			}
		}
		states = filtered
	}
	return states, nil
}

func (s *KvrocksStore) QueryTopAccountBalances(ctx context.Context, req models.TopAccountsByBalanceRequest, settings models.RequestSettings) ([]models.AccountBalance, error) {
	ctx = s.pinReadSnapshot(ctx)
	limReq := req.GetLimitParams()

	limit, offset, err := kvrocksLimitOffset(limReq, settings)
	if err != nil {
		return nil, err
	}
	members, err := s.rangeByLex(ctx, "latest_account_states", "balance", limit, offset, true)
	if err != nil {
		return nil, err
	}
	ids := addressIDsFromLexMembers(members)
	states, err := s.getLatestAccountStatesByIDs(ctx, ids, false)
	if err != nil {
		return nil, err
	}
	res := make([]models.AccountBalance, 0, len(states))
	for _, state := range states {
		if state.AccountAddress == nil || state.Balance == nil {
			continue
		}
		res = append(res, models.AccountBalance{Account: *state.AccountAddress, Balance: *state.Balance})
	}
	return res, nil
}

func (s *KvrocksStore) GetJettonWallets(ctx context.Context, addresses []models.AccountAddress) (map[models.AccountAddress]*models.JettonWallet, error) {
	ids := make([]string, 0, len(addresses))
	for _, address := range addresses {
		ids = append(ids, string(address))
	}
	payloads, err := s.getPayloads(ctx, "jetton_wallets", ids)
	if err != nil {
		return nil, err
	}

	res := make(map[models.AccountAddress]*models.JettonWallet, len(payloads))
	for id, payload := range payloads {
		destroyed, err := kvrocksPayloadDestroyed(id, "jetton_wallets", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		row, err := decodeJettonWalletPayload(id, payload)
		if err != nil {
			return nil, err
		}
		res[row.Address] = &row
	}
	return res, nil
}

func decodeJettonWalletPayload(id string, payload string) (models.JettonWallet, error) {
	var row struct {
		Address           models.AccountAddress `json:"address"`
		Balance           string                `json:"balance"`
		Owner             models.AccountAddress `json:"owner"`
		Jetton            models.AccountAddress `json:"jetton"`
		LastTransactionLt int64                 `json:"last_transaction_lt,string"`
		CodeHash          *models.HashType      `json:"code_hash"`
		DataHash          *models.HashType      `json:"data_hash"`
		MintlessIsClaimed *bool                 `json:"mintless_is_claimed"`
		MintlessAmount    *string               `json:"mintless_amount"`
		MintlessStartFrom *int64                `json:"mintless_start_from"`
		MintlessExpireAt  *int64                `json:"mintless_expire_at"`
		CustomPayloadURI  []string              `json:"custom_payload_api_uri"`
	}
	if err := json.Unmarshal([]byte(payload), &row); err != nil {
		return models.JettonWallet{}, fmt.Errorf("decode jetton_wallets %s: %w", id, err)
	}
	res := models.JettonWallet{
		Address:           row.Address,
		Balance:           row.Balance,
		Owner:             row.Owner,
		Jetton:            row.Jetton,
		LastTransactionLt: row.LastTransactionLt,
		CodeHash:          row.CodeHash,
		DataHash:          row.DataHash,
	}
	if row.MintlessIsClaimed != nil && !*row.MintlessIsClaimed && row.MintlessAmount != nil {
		res.MintlessInfo = &models.JettonWalletMintlessInfo{
			IsClaimed:           row.MintlessIsClaimed,
			Amount:              row.MintlessAmount,
			StartFrom:           row.MintlessStartFrom,
			ExpireAt:            row.MintlessExpireAt,
			CustomPayloadApiUri: row.CustomPayloadURI,
		}
	}
	return res, nil
}

func (s *KvrocksStore) GetJettonMasters(ctx context.Context, addresses []models.AccountAddress) (map[models.AccountAddress]*models.JettonMaster, error) {
	payloads, err := s.getPayloads(ctx, "jetton_masters", addressesToIDs(addresses))
	if err != nil {
		return nil, err
	}
	res := make(map[models.AccountAddress]*models.JettonMaster, len(payloads))
	for id, payload := range payloads {
		destroyed, err := kvrocksPayloadDestroyed(id, "jetton_masters", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		var row models.JettonMaster
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode jetton_masters %s: %w", id, err)
		}
		res[row.Address] = &row
	}
	return res, nil
}

func (s *KvrocksStore) QueryJettonMasters(ctx context.Context, req models.JettonMasterRequest, settings models.RequestSettings) ([]models.JettonMaster, error) {
	ctx = s.pinReadSnapshot(ctx)
	limReq := req.GetLimitParams()

	limit, offset, err := kvrocksLimitOffset(limReq, settings)
	if err != nil {
		return nil, err
	}

	ids := []string{}
	seen := map[string]struct{}{}
	if len(req.MasterAddress) > 0 {
		for _, address := range req.MasterAddress {
			ids = appendUniqueString(ids, seen, string(address))
		}
		sort.Strings(ids)
	} else if len(req.AdminAddress) > 0 {
		indexNames := make([]string, 0, len(req.AdminAddress))
		for _, admin := range req.AdminAddress {
			indexNames = append(indexNames, "admin:"+string(admin)+":by_id")
		}
		ids, err = s.mergedIDIndexIDs(ctx, "jetton_masters", indexNames, limit, offset, false)
		if err != nil {
			return nil, err
		}
	} else {
		ids, err = s.rangeByID(ctx, "jetton_masters", "all:by_id", limit, offset, false)
		if err != nil {
			return nil, err
		}
	}

	masters, err := s.orderedJettonMasters(ctx, ids)
	if err != nil {
		return nil, err
	}
	if len(req.AdminAddress) > 0 && len(req.MasterAddress) > 0 {
		allowed := map[models.AccountAddress]struct{}{}
		for _, admin := range req.AdminAddress {
			allowed[admin] = struct{}{}
		}
		filtered := masters[:0]
		for _, master := range masters {
			if master.AdminAddress == nil {
				continue
			}
			if _, ok := allowed[*master.AdminAddress]; ok {
				filtered = append(filtered, master)
			}
		}
		masters = filtered
	}
	if len(req.MasterAddress) > 0 {
		if offset >= int64(len(masters)) {
			return []models.JettonMaster{}, nil
		}
		end := offset + limit
		if end > int64(len(masters)) {
			end = int64(len(masters))
		}
		masters = masters[offset:end]
	}
	return masters, nil
}

func (s *KvrocksStore) QueryJettonWallets(ctx context.Context, req models.JettonWalletRequest, settings models.RequestSettings) ([]models.JettonWallet, error) {
	ctx = s.pinReadSnapshot(ctx)
	limReq := req.GetLimitParams()

	limit, offset, err := kvrocksLimitOffset(limReq, settings)
	if err != nil {
		return nil, err
	}
	desc, err := kvrocksSortDesc(limReq, false)
	if err != nil {
		return nil, err
	}

	ids := []string{}
	if len(req.Address) > 0 {
		seen := map[string]struct{}{}
		for _, address := range req.Address {
			ids = appendUniqueString(ids, seen, string(address))
		}
		sort.Strings(ids)
	} else {
		prefixes := []string{}
		owners := req.OwnerAddress
		jettons := req.JettonAddress
		if len(owners) == 0 && len(jettons) == 0 {
			prefixes = append(prefixes, "all")
		} else if len(owners) > 0 && len(jettons) > 0 {
			for _, owner := range owners {
				for _, jetton := range jettons {
					prefixes = append(prefixes, "owner:"+string(owner)+":jetton:"+string(jetton))
				}
			}
		} else if len(owners) > 0 {
			for _, owner := range owners {
				prefixes = append(prefixes, "owner:"+string(owner))
			}
		} else {
			for _, jetton := range jettons {
				prefixes = append(prefixes, "jetton:"+string(jetton))
			}
		}

		excludeZero := req.ExcludeZeroBalance != nil && *req.ExcludeZeroBalance
		useBalance := limReq.Sort != nil
		indexNames := make([]string, 0, len(prefixes))
		for _, prefix := range prefixes {
			indexName := prefix
			if excludeZero {
				indexName += ":nonzero"
			}
			if useBalance {
				indexName += ":by_balance"
			} else {
				indexName += ":by_id"
			}
			indexNames = append(indexNames, indexName)
		}

		if useBalance {
			ranges := make([]kvrocksLexIndexRange, 0, len(indexNames))
			for _, indexName := range indexNames {
				ranges = append(ranges, allLexIndexRange(indexName))
			}
			ids, err = s.mergedLexIndexIDs(ctx, "jetton_wallets", ranges, limit, offset, desc)
			if err != nil {
				return nil, err
			}
		} else {
			ids, err = s.mergedIDIndexIDs(ctx, "jetton_wallets", indexNames, limit, offset, false)
			if err != nil {
				return nil, err
			}
		}
	}

	wallets, err := s.orderedJettonWallets(ctx, ids)
	if err != nil {
		return nil, err
	}
	if len(req.Address) > 0 && (len(req.OwnerAddress) > 0 || len(req.JettonAddress) > 0 || (req.ExcludeZeroBalance != nil && *req.ExcludeZeroBalance)) {
		ownerAllowed := map[models.AccountAddress]struct{}{}
		jettonAllowed := map[models.AccountAddress]struct{}{}
		for _, owner := range req.OwnerAddress {
			ownerAllowed[owner] = struct{}{}
		}
		for _, jetton := range req.JettonAddress {
			jettonAllowed[jetton] = struct{}{}
		}
		filtered := wallets[:0]
		for _, wallet := range wallets {
			if len(ownerAllowed) > 0 {
				if _, ok := ownerAllowed[wallet.Owner]; !ok {
					continue
				}
			}
			if len(jettonAllowed) > 0 {
				if _, ok := jettonAllowed[wallet.Jetton]; !ok {
					continue
				}
			}
			if req.ExcludeZeroBalance != nil && *req.ExcludeZeroBalance && wallet.Balance == "0" {
				continue
			}
			filtered = append(filtered, wallet)
		}
		wallets = filtered
	}
	if len(req.Address) > 0 {
		if offset >= int64(len(wallets)) {
			return []models.JettonWallet{}, nil
		}
		end := offset + limit
		if end > int64(len(wallets)) {
			end = int64(len(wallets))
		}
		wallets = wallets[offset:end]
	}
	return wallets, nil
}

func (s *KvrocksStore) GetNFTItems(ctx context.Context, addresses []models.AccountAddress) (map[models.AccountAddress]*models.NFTItem, error) {
	ids := make([]string, 0, len(addresses))
	for _, address := range addresses {
		ids = append(ids, string(address))
	}
	payloads, err := s.getPayloads(ctx, "nft_items", ids)
	if err != nil {
		return nil, err
	}

	res := make(map[models.AccountAddress]*models.NFTItem, len(payloads))
	for id, payload := range payloads {
		destroyed, err := kvrocksPayloadDestroyed(id, "nft_items", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		var row models.NFTItem
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode nft_items %s: %w", id, err)
		}
		res[row.Address] = &row
	}
	return res, nil
}

func (s *KvrocksStore) GetNFTCollections(ctx context.Context, addresses []models.AccountAddress) (map[models.AccountAddress]*models.NFTCollection, error) {
	ids := make([]string, 0, len(addresses))
	for _, address := range addresses {
		ids = append(ids, string(address))
	}
	payloads, err := s.getPayloads(ctx, "nft_collections", ids)
	if err != nil {
		return nil, err
	}

	res := make(map[models.AccountAddress]*models.NFTCollection, len(payloads))
	for id, payload := range payloads {
		destroyed, err := kvrocksPayloadDestroyed(id, "nft_collections", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		var row models.NFTCollection
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode nft_collections %s: %w", id, err)
		}
		res[row.Address] = &row
	}
	return res, nil
}

func (s *KvrocksStore) QueryNFTCollections(ctx context.Context, req models.NFTCollectionRequest, settings models.RequestSettings) ([]models.NFTCollection, error) {
	ctx = s.pinReadSnapshot(ctx)
	limReq := req.GetLimitParams()

	limit, offset, err := kvrocksLimitOffset(limReq, settings)
	if err != nil {
		return nil, err
	}

	ids := []string{}
	seen := map[string]struct{}{}
	idsArePaged := false
	if len(req.CollectionAddress) > 0 {
		for _, address := range req.CollectionAddress {
			ids = appendUniqueString(ids, seen, string(address))
		}
		sort.Strings(ids)
	} else if len(req.OwnerAddress) > 0 {
		indexNames := make([]string, 0, len(req.OwnerAddress))
		for _, owner := range req.OwnerAddress {
			indexNames = append(indexNames, "owner:"+string(owner)+":by_id")
		}
		ids, err = s.mergedIDIndexIDs(ctx, "nft_collections", indexNames, limit, offset, false)
		if err != nil {
			return nil, err
		}
		idsArePaged = true
	} else {
		ids, err = s.rangeByID(ctx, "nft_collections", "all:by_id", limit, offset, false)
		if err != nil {
			return nil, err
		}
		return s.orderedNFTCollections(ctx, ids)
	}

	collections, err := s.orderedNFTCollections(ctx, ids)
	if err != nil {
		return nil, err
	}
	if len(req.OwnerAddress) > 0 && len(req.CollectionAddress) > 0 {
		allowed := map[models.AccountAddress]struct{}{}
		for _, owner := range req.OwnerAddress {
			allowed[owner] = struct{}{}
		}
		filtered := collections[:0]
		for _, collection := range collections {
			if collection.OwnerAddress == nil {
				continue
			}
			if _, ok := allowed[*collection.OwnerAddress]; ok {
				filtered = append(filtered, collection)
			}
		}
		collections = filtered
	}
	if !idsArePaged {
		if offset >= int64(len(collections)) {
			return []models.NFTCollection{}, nil
		}
		end := offset + limit
		if end > int64(len(collections)) {
			end = int64(len(collections))
		}
		collections = collections[offset:end]
	}
	return collections, nil
}

func (s *KvrocksStore) attachNFTItemDetails(ctx context.Context, items []models.NFTItem) error {
	if len(items) == 0 {
		return nil
	}

	collectionIDs := []models.AccountAddress{}
	collectionSeen := map[string]struct{}{}
	ownerIDs := []models.AccountAddress{}
	ownerSeen := map[string]struct{}{}
	for _, item := range items {
		if item.CollectionAddress != nil {
			id := string(*item.CollectionAddress)
			if _, ok := collectionSeen[id]; !ok {
				collectionSeen[id] = struct{}{}
				collectionIDs = append(collectionIDs, *item.CollectionAddress)
			}
		}
		if item.OwnerAddress != nil {
			id := string(*item.OwnerAddress)
			if _, ok := ownerSeen[id]; !ok {
				ownerSeen[id] = struct{}{}
				ownerIDs = append(ownerIDs, *item.OwnerAddress)
			}
		}
	}

	collections, err := s.GetNFTCollections(ctx, collectionIDs)
	if err != nil {
		return err
	}
	sales, err := s.GetNFTSales(ctx, ownerIDs)
	if err != nil {
		return err
	}
	salesByOwner := make(map[models.AccountAddress][]models.RawNFTSale)
	for _, sale := range sales {
		salesByOwner[sale.Address] = append(salesByOwner[sale.Address], sale)
	}

	for i := range items {
		if items[i].CollectionAddress != nil {
			if collection, ok := collections[*items[i].CollectionAddress]; ok {
				items[i].Collection = collection
			}
		}
		if items[i].OwnerAddress == nil {
			continue
		}
		for _, sale := range salesByOwner[*items[i].OwnerAddress] {
			if sale.NftAddress == nil || *sale.NftAddress != items[i].Address {
				continue
			}
			switch sale.Type {
			case "getgems_sale":
				address := sale.Address
				items[i].OnSale = true
				items[i].SaleContractAddress = &address
				items[i].RealOwner = sale.NftOwnerAddress
			case "getgems_auction":
				address := sale.Address
				items[i].OnSale = true
				items[i].AuctionContractAddress = &address
				items[i].RealOwner = sale.NftOwnerAddress
			}
		}
	}
	return nil
}

func (s *KvrocksStore) QueryNFTItems(ctx context.Context, req models.NFTItemRequest, settings models.RequestSettings) ([]models.NFTItem, error) {
	ctx = s.pinReadSnapshot(ctx)
	limReq := req.GetLimitParams()

	if len(req.Index) > 0 && len(req.CollectionAddress) == 0 {
		return nil, models.IndexError{Code: 422, Message: "index parameter is not allowed without the collection_address"}
	}
	limit, offset, err := kvrocksLimitOffset(limReq, settings)
	if err != nil {
		return nil, err
	}

	ids := []string{}
	seen := map[string]struct{}{}
	if len(req.Address) > 0 {
		for _, address := range req.Address {
			ids = appendUniqueString(ids, seen, string(address))
		}
		sort.Strings(ids)
	} else if len(req.Index) > 0 {
		ranges := []kvrocksLexIndexRange{}
		for _, collection := range req.CollectionAddress {
			for _, index := range req.Index {
				index = strings.TrimSpace(index)
				if index == "" {
					continue
				}
				ranges = append(ranges, prefixLexIndexRangeWithSortPrefix(
					"collection:"+string(collection)+":by_index",
					padDecimalString(index, 80)+":",
					string(collection)+":",
				))
			}
		}
		ids, err = s.mergedLexIndexIDs(ctx, "nft_items", ranges, limit+offset, 0, false)
		if err != nil {
			return nil, err
		}
	} else {
		sortByLt := req.SortByLastTransactionLt != nil && *req.SortByLastTransactionLt
		need := limit + offset
		if len(req.OwnerAddress) > 0 {
			ownerPrefix := "owner"
			if req.IncludeOnSale != nil && *req.IncludeOnSale {
				ownerPrefix = "real_owner"
			}
			ranges := []kvrocksLexIndexRange{}
			for _, owner := range req.OwnerAddress {
				if len(req.CollectionAddress) > 0 {
					for _, collection := range req.CollectionAddress {
						indexName := ownerPrefix + ":" + string(owner) + ":collection:" + string(collection)
						if sortByLt {
							indexName += ":by_last_transaction_lt"
						} else {
							indexName += ":by_index"
						}
						ranges = append(ranges, allLexIndexRange(indexName))
					}
					continue
				}
				indexName := ownerPrefix + ":" + string(owner)
				if sortByLt {
					indexName += ":by_last_transaction_lt"
				} else {
					indexName += ":by_collection_index"
				}
				ranges = append(ranges, allLexIndexRange(indexName))
			}
			ids, err = s.mergedLexIndexIDs(ctx, "nft_items", ranges, need, 0, sortByLt)
			if err != nil {
				return nil, err
			}
		} else if len(req.CollectionAddress) > 0 {
			ranges := []kvrocksLexIndexRange{}
			for _, collection := range req.CollectionAddress {
				indexName := "collection:" + string(collection)
				if sortByLt {
					indexName += ":by_last_transaction_lt"
					ranges = append(ranges, allLexIndexRange(indexName))
				} else {
					indexName += ":by_index"
					ranges = append(ranges, allLexIndexRangeWithSortPrefix(indexName, string(collection)+":"))
				}
			}
			ids, err = s.mergedLexIndexIDs(ctx, "nft_items", ranges, need, 0, sortByLt)
			if err != nil {
				return nil, err
			}
		} else if sortByLt {
			members, err := s.rangeByLex(ctx, "nft_items", "all:by_last_transaction_lt", need, 0, true)
			if err != nil {
				return nil, err
			}
			ids = addressIDsFromLexMembers(members)
		} else {
			ids, err = s.rangeByID(ctx, "nft_items", "all:by_id", limit, offset, false)
			if err != nil {
				return nil, err
			}
			items, err := s.orderedNFTItems(ctx, ids)
			if err != nil {
				return nil, err
			}
			if err := s.attachNFTItemDetails(ctx, items); err != nil {
				return nil, err
			}
			return items, nil
		}
	}

	items, err := s.orderedNFTItems(ctx, ids)
	if err != nil {
		return nil, err
	}
	items = filterNFTItems(items, req)
	if offset >= int64(len(items)) {
		return []models.NFTItem{}, nil
	}
	end := offset + limit
	if end > int64(len(items)) {
		end = int64(len(items))
	}
	items = items[offset:end]
	if err := s.attachNFTItemDetails(ctx, items); err != nil {
		return nil, err
	}
	return items, nil
}

func filterNFTItems(items []models.NFTItem, req models.NFTItemRequest) []models.NFTItem {
	if len(req.Address) == 0 && len(req.OwnerAddress) == 0 && len(req.CollectionAddress) == 0 && len(req.Index) == 0 {
		return items
	}
	addressAllowed := map[models.AccountAddress]struct{}{}
	ownerAllowed := map[models.AccountAddress]struct{}{}
	collectionAllowed := map[models.AccountAddress]struct{}{}
	indexAllowed := map[string]struct{}{}
	for _, address := range req.Address {
		addressAllowed[address] = struct{}{}
	}
	for _, owner := range req.OwnerAddress {
		ownerAllowed[owner] = struct{}{}
	}
	for _, collection := range req.CollectionAddress {
		collectionAllowed[collection] = struct{}{}
	}
	for _, index := range req.Index {
		indexAllowed[strings.TrimSpace(index)] = struct{}{}
	}
	useRealOwner := req.IncludeOnSale != nil && *req.IncludeOnSale
	filtered := items[:0]
	for _, item := range items {
		if len(addressAllowed) > 0 {
			if _, ok := addressAllowed[item.Address]; !ok {
				continue
			}
		}
		if len(ownerAllowed) > 0 {
			var owner *models.AccountAddress
			if useRealOwner {
				owner = item.RealOwner
			} else {
				owner = item.OwnerAddress
			}
			if owner == nil {
				continue
			}
			if _, ok := ownerAllowed[*owner]; !ok {
				continue
			}
		}
		if len(collectionAllowed) > 0 {
			if item.CollectionAddress == nil {
				continue
			}
			if _, ok := collectionAllowed[*item.CollectionAddress]; !ok {
				continue
			}
		}
		if len(indexAllowed) > 0 {
			if _, ok := indexAllowed[item.Index]; !ok {
				continue
			}
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func (s *KvrocksStore) orderedDNSRecords(ctx context.Context, ids []string) ([]models.DNSRecord, error) {
	payloads, err := s.getPayloads(ctx, "dns_entries", ids)
	if err != nil {
		return nil, err
	}
	return s.decodeDNSRecords(ids, payloads)
}

func (s *KvrocksStore) decodeDNSRecords(ids []string, payloads map[string]string) ([]models.DNSRecord, error) {
	res := make([]models.DNSRecord, 0, len(ids))
	for _, id := range ids {
		payload, ok := payloads[id]
		if !ok {
			continue
		}
		destroyed, err := kvrocksPayloadDestroyed(id, "dns_entries", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		var row models.DNSRecord
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode dns_entries %s: %w", id, err)
		}
		res = append(res, row)
	}
	return res, nil
}

func (s *KvrocksStore) QueryDNSRecords(ctx context.Context, req models.DNSRecordsRequest, settings models.RequestSettings) ([]models.DNSRecord, error) {
	ctx = s.pinReadSnapshot(ctx)
	limReq := req.GetLimitParams()

	limit, offset, err := kvrocksLimitOffset(limReq, settings)
	if err != nil {
		return nil, err
	}
	if req.WalletAddress == nil && req.Domain == nil {
		return nil, models.IndexError{Code: 422, Message: "wallet or domain is required"}
	}

	ids := []string{}
	if req.WalletAddress != nil {
		members, err := s.rangeByLex(ctx, "dns_entries", "wallet:"+string(*req.WalletAddress)+":by_domain", limit, offset, false)
		if err != nil {
			return nil, err
		}
		ids = addressIDsFromLexMembers(members)
	} else {
		ids, err = s.rangeByID(ctx, "dns_entries", "domain:"+*req.Domain+":by_id", limit, offset, false)
		if err != nil {
			return nil, err
		}
	}
	return s.orderedDNSRecords(ctx, ids)
}

func (s *KvrocksStore) GetNFTSales(ctx context.Context, addresses []models.AccountAddress) ([]models.RawNFTSale, error) {
	ctx = s.pinReadSnapshot(ctx)
	sales, err := s.getGetgemsSales(ctx, addresses)
	if err != nil {
		return nil, err
	}
	auctions, err := s.getGetgemsAuctions(ctx, addresses)
	if err != nil {
		return nil, err
	}
	telemint, err := s.getTelemintItems(ctx, addresses)
	if err != nil {
		return nil, err
	}

	all := append(sales, auctions...)
	all = append(all, telemint...)
	if len(all) == 0 {
		return all, nil
	}

	itemAddresses := make([]models.AccountAddress, 0, len(all))
	for _, sale := range all {
		if sale.NftAddress != nil {
			itemAddresses = append(itemAddresses, *sale.NftAddress)
		}
	}
	items, err := s.GetNFTItems(ctx, itemAddresses)
	if err != nil {
		return nil, err
	}

	collectionAddresses := make([]models.AccountAddress, 0, len(items))
	for _, item := range items {
		if item.CollectionAddress != nil {
			collectionAddresses = append(collectionAddresses, *item.CollectionAddress)
		}
	}
	collections, err := s.GetNFTCollections(ctx, collectionAddresses)
	if err != nil {
		return nil, err
	}

	for i := range all {
		if all[i].NftAddress == nil {
			continue
		}
		item, ok := items[*all[i].NftAddress]
		if !ok {
			continue
		}
		applyNFTItemToRawSale(&all[i], item)
		if item.CollectionAddress == nil {
			continue
		}
		if collection, ok := collections[*item.CollectionAddress]; ok {
			applyNFTCollectionToRawSale(&all[i], collection)
		}
	}
	return all, nil
}

func (s *KvrocksStore) getGetgemsSales(ctx context.Context, addresses []models.AccountAddress) ([]models.RawNFTSale, error) {
	ids := make([]string, 0, len(addresses))
	for _, address := range addresses {
		ids = append(ids, string(address))
	}
	payloads, err := s.getPayloads(ctx, "getgems_nft_sales", ids)
	if err != nil {
		return nil, err
	}

	res := make([]models.RawNFTSale, 0, len(payloads))
	for id, payload := range payloads {
		destroyed, err := kvrocksPayloadDestroyed(id, "getgems_nft_sales", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		var row kvGetgemsSalePayload
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode getgems_nft_sales %s: %w", id, err)
		}
		res = append(res, row.raw())
	}
	return res, nil
}

func (s *KvrocksStore) getGetgemsAuctions(ctx context.Context, addresses []models.AccountAddress) ([]models.RawNFTSale, error) {
	ids := make([]string, 0, len(addresses))
	for _, address := range addresses {
		ids = append(ids, string(address))
	}
	payloads, err := s.getPayloads(ctx, "getgems_nft_auctions", ids)
	if err != nil {
		return nil, err
	}

	res := make([]models.RawNFTSale, 0, len(payloads))
	for id, payload := range payloads {
		destroyed, err := kvrocksPayloadDestroyed(id, "getgems_nft_auctions", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		var row kvGetgemsAuctionPayload
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode getgems_nft_auctions %s: %w", id, err)
		}
		res = append(res, row.raw())
	}
	return res, nil
}

func (s *KvrocksStore) getTelemintItems(ctx context.Context, addresses []models.AccountAddress) ([]models.RawNFTSale, error) {
	ids := make([]string, 0, len(addresses))
	for _, address := range addresses {
		ids = append(ids, string(address))
	}
	payloads, err := s.getPayloads(ctx, "telemint_nft_items", ids)
	if err != nil {
		return nil, err
	}

	res := make([]models.RawNFTSale, 0, len(payloads))
	for id, payload := range payloads {
		destroyed, err := kvrocksPayloadDestroyed(id, "telemint_nft_items", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		var row kvTelemintPayload
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode telemint_nft_items %s: %w", id, err)
		}
		res = append(res, row.raw())
	}
	return res, nil
}

type jsonInt64 int64

func (v *jsonInt64) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	s := strings.Trim(string(data), `"`)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}
	*v = jsonInt64(n)
	return nil
}

func (v *jsonInt64) ptr() *int64 {
	if v == nil {
		return nil
	}
	n := int64(*v)
	return &n
}

type jsonInt32 int32

func (v *jsonInt32) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	s := strings.Trim(string(data), `"`)
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return err
	}
	*v = jsonInt32(n)
	return nil
}

func (v *jsonInt32) ptr() *int32 {
	if v == nil {
		return nil
	}
	n := int32(*v)
	return &n
}

type jsonString string

func (v *jsonString) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	if len(data) > 0 && data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		*v = jsonString(s)
		return nil
	}
	*v = jsonString(string(data))
	return nil
}

func (v *jsonString) ptr() *string {
	if v == nil {
		return nil
	}
	s := string(*v)
	return &s
}

type kvGetgemsSalePayload struct {
	Address               models.AccountAddress  `json:"address"`
	NftAddress            *models.AccountAddress `json:"nft_address"`
	NftOwnerAddress       *models.AccountAddress `json:"nft_owner_address"`
	MarketplaceAddress    *models.AccountAddress `json:"marketplace_address"`
	CreatedAt             *int64                 `json:"created_at"`
	LastTransactionLt     *jsonInt64             `json:"last_transaction_lt"`
	CodeHash              *models.HashType       `json:"code_hash"`
	DataHash              *models.HashType       `json:"data_hash"`
	IsComplete            *bool                  `json:"is_complete"`
	FullPrice             *string                `json:"full_price"`
	MarketplaceFeeAddress *models.AccountAddress `json:"marketplace_fee_address"`
	MarketplaceFee        *string                `json:"marketplace_fee"`
	RoyaltyAddress        *models.AccountAddress `json:"royalty_address"`
	RoyaltyAmount         *string                `json:"royalty_amount"`
}

func (p kvGetgemsSalePayload) raw() models.RawNFTSale {
	return models.RawNFTSale{
		Type:                  "getgems_sale",
		Address:               p.Address,
		NftAddress:            p.NftAddress,
		NftOwnerAddress:       p.NftOwnerAddress,
		MarketplaceAddress:    p.MarketplaceAddress,
		CreatedAt:             p.CreatedAt,
		LastTransactionLt:     p.LastTransactionLt.ptr(),
		CodeHash:              p.CodeHash,
		DataHash:              p.DataHash,
		IsComplete:            p.IsComplete,
		FullPrice:             p.FullPrice,
		MarketplaceFeeAddress: p.MarketplaceFeeAddress,
		MarketplaceFee:        p.MarketplaceFee,
		RoyaltyAddress:        p.RoyaltyAddress,
		RoyaltyAmount:         p.RoyaltyAmount,
	}
}

type kvGetgemsAuctionPayload struct {
	Address            models.AccountAddress  `json:"address"`
	NftAddress         *models.AccountAddress `json:"nft_addr"`
	NftOwnerAddress    *models.AccountAddress `json:"nft_owner"`
	MarketplaceAddress *models.AccountAddress `json:"mp_addr"`
	CreatedAt          *int64                 `json:"created_at"`
	LastTransactionLt  *jsonInt64             `json:"last_transaction_lt"`
	CodeHash           *models.HashType       `json:"code_hash"`
	DataHash           *models.HashType       `json:"data_hash"`
	EndFlag            *bool                  `json:"end_flag"`
	EndTime            *int64                 `json:"end_time"`
	LastBid            *string                `json:"last_bid"`
	LastMember         *models.AccountAddress `json:"last_member"`
	MinStep            *int64                 `json:"min_step"`
	MpFeeAddress       *models.AccountAddress `json:"mp_fee_addr"`
	MpFeeFactor        *int64                 `json:"mp_fee_factor"`
	MpFeeBase          *int64                 `json:"mp_fee_base"`
	RoyaltyFeeAddress  *models.AccountAddress `json:"royalty_fee_addr"`
	RoyaltyFeeFactor   *int64                 `json:"royalty_fee_factor"`
	RoyaltyFeeBase     *int64                 `json:"royalty_fee_base"`
	MaxBid             *string                `json:"max_bid"`
	MinBid             *string                `json:"min_bid"`
	LastBidAt          *int64                 `json:"last_bid_at"`
	IsCanceled         *bool                  `json:"is_canceled"`
}

func (p kvGetgemsAuctionPayload) raw() models.RawNFTSale {
	return models.RawNFTSale{
		Type:               "getgems_auction",
		Address:            p.Address,
		NftAddress:         p.NftAddress,
		NftOwnerAddress:    p.NftOwnerAddress,
		MarketplaceAddress: p.MarketplaceAddress,
		CreatedAt:          p.CreatedAt,
		LastTransactionLt:  p.LastTransactionLt.ptr(),
		CodeHash:           p.CodeHash,
		DataHash:           p.DataHash,
		EndFlag:            p.EndFlag,
		EndTime:            p.EndTime,
		LastBid:            p.LastBid,
		LastMember:         p.LastMember,
		MinStep:            p.MinStep,
		MpFeeAddress:       p.MpFeeAddress,
		MpFeeFactor:        p.MpFeeFactor,
		MpFeeBase:          p.MpFeeBase,
		RoyaltyFeeAddress:  p.RoyaltyFeeAddress,
		RoyaltyFeeFactor:   p.RoyaltyFeeFactor,
		RoyaltyFeeBase:     p.RoyaltyFeeBase,
		MaxBid:             p.MaxBid,
		MinBid:             p.MinBid,
		LastBidAt:          p.LastBidAt,
		IsCanceled:         p.IsCanceled,
	}
}

type kvTelemintPayload struct {
	Address            models.AccountAddress  `json:"address"`
	TokenName          *string                `json:"token_name"`
	BidderAddress      *models.AccountAddress `json:"bidder_address"`
	Bid                *string                `json:"bid"`
	BidTs              *jsonString            `json:"bid_ts"`
	MinBid             *string                `json:"min_bid"`
	EndTime            *jsonInt32             `json:"end_time"`
	BeneficiaryAddress *models.AccountAddress `json:"beneficiary_address"`
	InitialMinBid      *string                `json:"initial_min_bid"`
	MaxBid             *string                `json:"max_bid"`
	MinBidStep         *string                `json:"min_bid_step"`
	MinExtendTime      *jsonString            `json:"min_extend_time"`
	Duration           *jsonString            `json:"duration"`
	RoyaltyNumerator   *jsonInt32             `json:"royalty_numerator"`
	RoyaltyDenominator *jsonInt32             `json:"royalty_denominator"`
	RoyaltyDestination *models.AccountAddress `json:"royalty_destination"`
	LastTransactionLt  *jsonInt64             `json:"last_transaction_lt"`
	CodeHash           *models.HashType       `json:"code_hash"`
	DataHash           *models.HashType       `json:"data_hash"`
}

func (p kvTelemintPayload) raw() models.RawNFTSale {
	address := p.Address
	init := true
	index := p.TokenName
	return models.RawNFTSale{
		Type:                     "telemint",
		Address:                  p.Address,
		NftAddress:               &address,
		LastTransactionLt:        p.LastTransactionLt.ptr(),
		CodeHash:                 p.CodeHash,
		DataHash:                 p.DataHash,
		TokenName:                p.TokenName,
		BidderAddress:            p.BidderAddress,
		Bid:                      p.Bid,
		BidTs:                    p.BidTs.ptr(),
		TelemintMinBid:           p.MinBid,
		TelemintEndTime:          p.EndTime.ptr(),
		BeneficiaryAddress:       p.BeneficiaryAddress,
		InitialMinBid:            p.InitialMinBid,
		TelemintMaxBid:           p.MaxBid,
		MinBidStep:               p.MinBidStep,
		MinExtendTime:            p.MinExtendTime.ptr(),
		Duration:                 p.Duration.ptr(),
		RoyaltyNumerator:         p.RoyaltyNumerator.ptr(),
		RoyaltyDenominator:       p.RoyaltyDenominator.ptr(),
		RoyaltyDestination:       p.RoyaltyDestination,
		NftItemAddress:           &address,
		NftItemInit:              &init,
		NftItemIndex:             index,
		NftItemLastTransactionLt: p.LastTransactionLt.ptr(),
		NftItemCodeHash:          p.CodeHash,
		NftItemDataHash:          p.DataHash,
	}
}

func applyNFTItemToRawSale(raw *models.RawNFTSale, item *models.NFTItem) {
	raw.NftItemAddress = &item.Address
	raw.NftItemInit = &item.Init
	raw.NftItemIndex = &item.Index
	raw.NftItemCollectionAddress = item.CollectionAddress
	raw.NftItemOwnerAddress = item.OwnerAddress
	raw.NftItemContent = item.Content
	raw.NftItemLastTransactionLt = &item.LastTransactionLt
	raw.NftItemCodeHash = &item.CodeHash
	raw.NftItemDataHash = &item.DataHash
}

func applyNFTCollectionToRawSale(raw *models.RawNFTSale, collection *models.NFTCollection) {
	raw.CollectionAddress = &collection.Address
	raw.CollectionNextItemIndex = &collection.NextItemIndex
	raw.CollectionOwnerAddress = collection.OwnerAddress
	raw.CollectionContent = collection.CollectionContent
	raw.CollectionDataHash = &collection.DataHash
	raw.CollectionCodeHash = &collection.CodeHash
	raw.CollectionLastTransactionLt = &collection.LastTransactionLt
}

func (s *KvrocksStore) GetMultisigs(ctx context.Context, addresses []models.AccountAddress) (map[models.AccountAddress]*models.Multisig, error) {
	payloads, err := s.getPayloads(ctx, "multisig", addressesToIDs(addresses))
	if err != nil {
		return nil, err
	}
	res := make(map[models.AccountAddress]*models.Multisig, len(payloads))
	for id, payload := range payloads {
		destroyed, err := kvrocksPayloadDestroyed(id, "multisig", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		var row models.Multisig
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode multisig %s: %w", id, err)
		}
		res[row.Address] = &row
	}
	return res, nil
}

func (s *KvrocksStore) orderedMultisigs(ctx context.Context, ids []string) ([]models.Multisig, error) {
	payloads, err := s.getPayloads(ctx, "multisig", ids)
	if err != nil {
		return nil, err
	}
	res := make([]models.Multisig, 0, len(ids))
	for _, id := range ids {
		payload, ok := payloads[id]
		if !ok {
			continue
		}
		destroyed, err := kvrocksPayloadDestroyed(id, "multisig", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		var row models.Multisig
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode multisig %s: %w", id, err)
		}
		res = append(res, row)
	}
	return res, nil
}

func (s *KvrocksStore) GetMultisigOrders(ctx context.Context, addresses []models.AccountAddress) (map[models.AccountAddress]*models.MultisigOrder, error) {
	payloads, err := s.getPayloads(ctx, "multisig_orders", addressesToIDs(addresses))
	if err != nil {
		return nil, err
	}
	res := make(map[models.AccountAddress]*models.MultisigOrder, len(payloads))
	for id, payload := range payloads {
		destroyed, err := kvrocksPayloadDestroyed(id, "multisig_orders", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		row, err := decodeMultisigOrderPayload(id, payload)
		if err != nil {
			return nil, err
		}
		res[row.Address] = &row
	}
	return res, nil
}

func (s *KvrocksStore) orderedMultisigOrders(ctx context.Context, ids []string) ([]models.MultisigOrder, error) {
	payloads, err := s.getPayloads(ctx, "multisig_orders", ids)
	if err != nil {
		return nil, err
	}
	res := make([]models.MultisigOrder, 0, len(ids))
	for _, id := range ids {
		payload, ok := payloads[id]
		if !ok {
			continue
		}
		destroyed, err := kvrocksPayloadDestroyed(id, "multisig_orders", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		row, err := decodeMultisigOrderPayload(id, payload)
		if err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func decodeMultisigOrderPayload(id string, payload string) (models.MultisigOrder, error) {
	var row struct {
		Address           models.AccountAddress   `json:"address"`
		MultisigAddress   models.AccountAddress   `json:"multisig_address"`
		OrderSeqno        *string                 `json:"order_seqno"`
		Threshold         *int32                  `json:"threshold"`
		SentForExecution  *bool                   `json:"sent_for_execution"`
		ApprovalsMask     *string                 `json:"approvals_mask"`
		ApprovalsNum      *int32                  `json:"approvals_num"`
		ExpirationDate    *jsonString             `json:"expiration_date"`
		OrderBoc          *string                 `json:"order_boc"`
		Signers           []models.AccountAddress `json:"signers"`
		LastTransactionLt int64                   `json:"last_transaction_lt,string"`
		CodeHash          *models.HashType        `json:"code_hash"`
		DataHash          *models.HashType        `json:"data_hash"`
	}
	if err := json.Unmarshal([]byte(payload), &row); err != nil {
		return models.MultisigOrder{}, fmt.Errorf("decode multisig_orders %s: %w", id, err)
	}
	var expiration *uint64
	if row.ExpirationDate != nil {
		value, err := strconv.ParseUint(string(*row.ExpirationDate), 10, 64)
		if err != nil {
			return models.MultisigOrder{}, fmt.Errorf("decode multisig_orders %s expiration_date: %w", id, err)
		}
		expiration = &value
	}
	return models.MultisigOrder{
		Address:           row.Address,
		MultisigAddress:   row.MultisigAddress,
		OrderSeqno:        row.OrderSeqno,
		Threshold:         row.Threshold,
		SentForExecution:  row.SentForExecution,
		ApprovalsMask:     row.ApprovalsMask,
		ApprovalsNum:      row.ApprovalsNum,
		ExpirationDate:    expiration,
		OrderBoc:          row.OrderBoc,
		Signers:           row.Signers,
		LastTransactionLt: row.LastTransactionLt,
		CodeHash:          row.CodeHash,
		DataHash:          row.DataHash,
	}, nil
}

func (s *KvrocksStore) QueryMultisigs(ctx context.Context, req models.MultisigRequest, settings models.RequestSettings) ([]models.Multisig, error) {
	ctx = s.pinReadSnapshot(ctx)
	limReq := req.GetLimitParams()

	limit, offset, err := kvrocksLimitOffset(limReq, settings)
	if err != nil {
		return nil, err
	}
	desc, err := kvrocksSortDesc(limReq, true)
	if err != nil {
		return nil, err
	}

	ids := []string{}
	seen := map[string]struct{}{}
	idsArePaged := false
	if len(req.Address) > 0 {
		for _, address := range req.Address {
			ids = appendUniqueString(ids, seen, string(address))
		}
		sort.Strings(ids)
	} else if len(req.WalletAddress) > 0 {
		indexes := make([]string, 0, len(req.WalletAddress))
		for _, wallet := range req.WalletAddress {
			indexes = append(indexes, "wallet:"+string(wallet)+":by_id")
		}
		ids, err = s.mergedIDIndexIDs(ctx, "multisig", indexes, limit, offset, desc)
		if err != nil {
			return nil, err
		}
		idsArePaged = true
	} else {
		ids, err = s.rangeByID(ctx, "multisig", "all:by_id", limit, offset, desc)
		if err != nil {
			return nil, err
		}
		multisigs, err := s.orderedMultisigs(ctx, ids)
		if err != nil {
			return nil, err
		}
		if req.IncludeOrders == nil || *req.IncludeOrders {
			if err := s.attachMultisigOrders(ctx, multisigs); err != nil {
				return nil, err
			}
		}
		return multisigs, nil
	}

	multisigs, err := s.orderedMultisigs(ctx, ids)
	if err != nil {
		return nil, err
	}
	if len(req.WalletAddress) > 0 && len(req.Address) > 0 {
		allowed := map[models.AccountAddress]struct{}{}
		for _, wallet := range req.WalletAddress {
			allowed[wallet] = struct{}{}
		}
		filtered := multisigs[:0]
		for _, multisig := range multisigs {
			matched := false
			for _, signer := range multisig.Signers {
				if _, ok := allowed[signer]; ok {
					matched = true
					break
				}
			}
			if !matched {
				for _, proposer := range multisig.Proposers {
					if _, ok := allowed[proposer]; ok {
						matched = true
						break
					}
				}
			}
			if matched {
				filtered = append(filtered, multisig)
			}
		}
		multisigs = filtered
	}
	if !idsArePaged {
		if offset >= int64(len(multisigs)) {
			return []models.Multisig{}, nil
		}
		end := offset + limit
		if end > int64(len(multisigs)) {
			end = int64(len(multisigs))
		}
		multisigs = multisigs[offset:end]
	}
	if req.IncludeOrders == nil || *req.IncludeOrders {
		if err := s.attachMultisigOrders(ctx, multisigs); err != nil {
			return nil, err
		}
	}
	return multisigs, nil
}

func (s *KvrocksStore) attachMultisigOrders(ctx context.Context, multisigs []models.Multisig) error {
	for i := range multisigs {
		ids, err := s.rangeByID(ctx, "multisig_orders", "multisig:"+string(multisigs[i].Address)+":by_id", 10000, 0, false)
		if err != nil {
			return err
		}
		orders, err := s.orderedMultisigOrders(ctx, ids)
		if err != nil {
			return err
		}
		multisigs[i].Orders = orders
	}
	return nil
}

func (s *KvrocksStore) QueryMultisigOrders(ctx context.Context, req models.MultisigOrderRequest, settings models.RequestSettings) ([]models.MultisigOrder, error) {
	ctx = s.pinReadSnapshot(ctx)
	limReq := req.GetLimitParams()

	limit, offset, err := kvrocksLimitOffset(limReq, settings)
	if err != nil {
		return nil, err
	}
	desc, err := kvrocksSortDesc(limReq, true)
	if err != nil {
		return nil, err
	}

	ids := []string{}
	seen := map[string]struct{}{}
	idsArePaged := false
	if len(req.Address) > 0 {
		for _, address := range req.Address {
			ids = appendUniqueString(ids, seen, string(address))
		}
		sort.Strings(ids)
	} else if len(req.MultisigAddress) > 0 {
		indexes := make([]string, 0, len(req.MultisigAddress))
		for _, multisig := range req.MultisigAddress {
			indexes = append(indexes, "multisig:"+string(multisig)+":by_id")
		}
		ids, err = s.mergedIDIndexIDs(ctx, "multisig_orders", indexes, limit, offset, desc)
		if err != nil {
			return nil, err
		}
		idsArePaged = true
	} else {
		ids, err = s.rangeByID(ctx, "multisig_orders", "all:by_id", limit, offset, desc)
		if err != nil {
			return nil, err
		}
		return s.orderedMultisigOrders(ctx, ids)
	}

	orders, err := s.orderedMultisigOrders(ctx, ids)
	if err != nil {
		return nil, err
	}
	if len(req.MultisigAddress) > 0 && len(req.Address) > 0 {
		allowed := map[models.AccountAddress]struct{}{}
		for _, multisig := range req.MultisigAddress {
			allowed[multisig] = struct{}{}
		}
		filtered := orders[:0]
		for _, order := range orders {
			if _, ok := allowed[order.MultisigAddress]; ok {
				filtered = append(filtered, order)
			}
		}
		orders = filtered
	}
	if !idsArePaged {
		if offset >= int64(len(orders)) {
			return []models.MultisigOrder{}, nil
		}
		end := offset + limit
		if end > int64(len(orders)) {
			end = int64(len(orders))
		}
		orders = orders[offset:end]
	}
	return orders, nil
}

type kvVestingContractPayload struct {
	Address       models.AccountAddress `json:"address"`
	StartTime     *int64                `json:"vesting_start_time"`
	TotalDuration *int64                `json:"vesting_total_duration"`
	UnlockPeriod  *int64                `json:"unlock_period"`
	CliffDuration *int64                `json:"cliff_duration"`
	SenderAddress models.AccountAddress `json:"vesting_sender_address"`
	OwnerAddress  models.AccountAddress `json:"owner_address"`
	TotalAmount   *string               `json:"vesting_total_amount"`
}

func (p kvVestingContractPayload) vestingInfo() models.VestingInfo {
	address := p.Address
	sender := p.SenderAddress
	owner := p.OwnerAddress
	return models.VestingInfo{
		Address:       &address,
		StartTime:     p.StartTime,
		TotalDuration: p.TotalDuration,
		UnlockPeriod:  p.UnlockPeriod,
		CliffDuration: p.CliffDuration,
		SenderAddress: &sender,
		OwnerAddress:  &owner,
		TotalAmount:   p.TotalAmount,
		Whitelist:     []models.AccountAddress{},
	}
}

func (s *KvrocksStore) orderedVestingContracts(ctx context.Context, ids []string) ([]models.VestingInfo, error) {
	payloads, err := s.getPayloads(ctx, "vesting_contracts", ids)
	if err != nil {
		return nil, err
	}
	res := make([]models.VestingInfo, 0, len(ids))
	for _, id := range ids {
		payload, ok := payloads[id]
		if !ok {
			continue
		}
		destroyed, err := kvrocksPayloadDestroyed(id, "vesting_contracts", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		var row kvVestingContractPayload
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode vesting_contracts %s: %w", id, err)
		}
		res = append(res, row.vestingInfo())
	}
	if err := s.attachVestingWhitelist(ctx, res); err != nil {
		return nil, err
	}
	return res, nil
}

type kvNominatorPoolPayload struct {
	StakeAmountSent      string                       `json:"stake_amount_sent"`
	ValidatorAmount      string                       `json:"validator_amount"`
	ValidatorAddress     models.AccountAddress        `json:"validator_address"`
	ValidatorRewardShare int                          `json:"validator_reward_share"`
	State                int                          `json:"state"`
	NominatorsCount      int                          `json:"nominators_count"`
	MaxNominatorsCount   int                          `json:"max_nominators_count"`
	MinValidatorStake    string                       `json:"min_validator_stake"`
	MinNominatorStake    string                       `json:"min_nominator_stake"`
	ActiveNominators     []models.ActiveNominatorInfo `json:"active_nominators"`
}

func decodeNominatorPoolPayload(id string, payload string) (models.NominatorPoolInfo, error) {
	var row kvNominatorPoolPayload
	if err := json.Unmarshal([]byte(payload), &row); err != nil {
		return models.NominatorPoolInfo{}, fmt.Errorf("decode nominator_pools %s: %w", id, err)
	}
	return models.NominatorPoolInfo{
		StakeAmountSent:      row.StakeAmountSent,
		ValidatorAmount:      row.ValidatorAmount,
		ValidatorAddress:     row.ValidatorAddress,
		ValidatorRewardShare: row.ValidatorRewardShare,
		State:                row.State,
		NominatorsCount:      row.NominatorsCount,
		MaxNominatorsCount:   row.MaxNominatorsCount,
		MinValidatorStake:    row.MinValidatorStake,
		MinNominatorStake:    row.MinNominatorStake,
		ActiveNominators:     row.ActiveNominators,
	}, nil
}

func (s *KvrocksStore) GetNominatorPool(ctx context.Context, poolAddr string) (*models.NominatorPoolInfo, error) {
	payloads, err := s.getPayloads(ctx, "nominator_pools", []string{poolAddr})
	if err != nil {
		return nil, err
	}
	payload, ok := payloads[poolAddr]
	if !ok {
		return nil, nil
	}
	destroyed, err := kvrocksPayloadDestroyed(poolAddr, "nominator_pools", payload)
	if err != nil {
		return nil, err
	}
	if destroyed {
		return nil, nil
	}
	pool, err := decodeNominatorPoolPayload(poolAddr, payload)
	if err != nil {
		return nil, err
	}
	return &pool, nil
}

func (s *KvrocksStore) QueryNominatorPoolsByNominator(ctx context.Context, nominatorAddr string) ([]models.NominatorPoolPosition, error) {
	ids := make([]string, 0)
	seen := make(map[string]struct{})
	for offset := int64(0); ; {
		members, err := s.rangeByLex(ctx, "nominator_pools", "nominator:"+nominatorAddr+":by_pool",
			kvrocksNominatorPoolsBatchSize, offset, false)
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			ids = appendUniqueString(ids, seen, member)
		}
		if len(members) < kvrocksNominatorPoolsBatchSize {
			break
		}
		offset += int64(len(members))
	}

	payloads, err := s.getPayloads(ctx, "nominator_pools", ids)
	if err != nil {
		return nil, err
	}
	res := make([]models.NominatorPoolPosition, 0, len(ids))
	for _, id := range ids {
		payload, ok := payloads[id]
		if !ok {
			continue
		}
		destroyed, err := kvrocksPayloadDestroyed(id, "nominator_pools", payload)
		if err != nil {
			return nil, err
		}
		if destroyed {
			continue
		}
		pool, err := decodeNominatorPoolPayload(id, payload)
		if err != nil {
			return nil, err
		}
		for _, nominator := range pool.ActiveNominators {
			if nominator.Address == nominatorAddr {
				res = append(res, models.NominatorPoolPosition{
					PoolAddress:    models.AccountAddress(id),
					Balance:        nominator.Balance,
					PendingBalance: nominator.PendingBalance,
				})
				break
			}
		}
	}
	return res, nil
}

func (s *KvrocksStore) attachVestingWhitelist(ctx context.Context, vestings []models.VestingInfo) error {
	for i := range vestings {
		if vestings[i].Address == nil {
			continue
		}
		members, err := s.rangeByLex(ctx, "vesting_whitelist", "contract:"+string(*vestings[i].Address)+":by_wallet", 100000, 0, false)
		if err != nil {
			return err
		}
		vestings[i].Whitelist = stringsToAddresses(addressIDsFromLexMembers(members))
	}
	return nil
}

func (s *KvrocksStore) QueryVestingContracts(ctx context.Context, req models.VestingContractsRequest, settings models.RequestSettings) ([]models.VestingInfo, error) {
	ctx = s.pinReadSnapshot(ctx)
	limReq := req.GetLimitParams()
	if len(req.ContractAddress) == 0 && len(req.WalletAddress) == 0 {
		return nil, models.IndexError{Code: 422, Message: "at least one of contract_address or wallet_address is required"}
	}
	if len(req.ContractAddress) > 0 && len(req.WalletAddress) > 0 {
		return nil, models.IndexError{Code: 422, Message: "only one of contract_address or wallet_address should be specified"}
	}
	limit, offset, err := kvrocksLimitOffset(limReq, settings)
	if err != nil {
		return nil, err
	}

	ids := []string{}
	seen := map[string]struct{}{}
	idsArePaged := false
	if len(req.ContractAddress) > 0 {
		for _, address := range req.ContractAddress {
			ids = appendUniqueString(ids, seen, string(address))
		}
		sort.Strings(ids)
	} else {
		indexes := make([]string, 0, len(req.WalletAddress)*2)
		for _, wallet := range req.WalletAddress {
			indexes = append(indexes, "wallet:"+string(wallet)+":by_id")
			if req.CheckWhitelist != nil && *req.CheckWhitelist {
				indexes = append(indexes, "wallet_whitelist:"+string(wallet)+":by_id")
			}
		}
		ids, err = s.mergedIDIndexIDs(ctx, "vesting_contracts", indexes, limit, offset, false)
		if err != nil {
			return nil, err
		}
		idsArePaged = true
	}
	if !idsArePaged {
		if offset >= int64(len(ids)) {
			return []models.VestingInfo{}, nil
		}
		end := offset + limit
		if end > int64(len(ids)) {
			end = int64(len(ids))
		}
		ids = ids[offset:end]
	}
	return s.orderedVestingContracts(ctx, ids)
}

// batchPrimaryDomains resolves each address's primary domain in two round-trips
func (s *KvrocksStore) batchPrimaryDomains(ctx context.Context, addrList []models.AccountAddress) (map[models.AccountAddress]*string, error) {
	domains := map[models.AccountAddress]*string{}
	if s == nil || s.client == nil || len(addrList) == 0 {
		return domains, nil
	}

	// primary-domain ZRANGEBYLEX per address.
	memberLists, _, err := kvReadWithFallback(ctx, s, func(c redis.UniversalClient) ([][]string, error) {
		pipe := c.Pipeline()
		cmds := make([]*redis.StringSliceCmd, len(addrList))
		for i, raw := range addrList {
			key := s.indexKey("dns_entries", "owner_wallet:"+raw.String()+":by_domain")
			cmds[i] = pipe.ZRangeByLex(ctx, key, &redis.ZRangeBy{Min: "-", Max: "+", Offset: 0, Count: 1})
		}
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return nil, err
		}
		lists := make([][]string, len(addrList))
		for i := range cmds {
			members, err := cmds[i].Result()
			if err != nil {
				if err == redis.Nil {
					continue
				}
				return nil, err
			}
			lists[i] = members
		}
		return lists, nil
	})
	if err != nil {
		return nil, err
	}

	// collect ids, fetch all payloads in one MGET.
	addrIDs := make([][]string, len(addrList))
	allIDs := []string{}
	for i := range addrList {
		ids := addressIDsFromLexMembers(memberLists[i])
		addrIDs[i] = ids
		allIDs = append(allIDs, ids...)
	}
	payloads, err := s.getPayloads(ctx, "dns_entries", allIDs)
	if err != nil {
		return nil, err
	}

	// decode per address, keep the primary (first) domain.
	for i, raw := range addrList {
		if len(addrIDs[i]) == 0 {
			continue
		}
		records, err := s.decodeDNSRecords(addrIDs[i], payloads)
		if err != nil {
			return nil, err
		}
		if len(records) > 0 {
			domain := records[0].Domain
			domains[raw] = &domain
		}
	}
	return domains, nil
}

func QueryAddressBookImplKvrocks(addrList []models.AccountAddress, store *KvrocksStore, settings models.RequestSettings) (models.AddressBook, error) {
	if settings.UseCache {
		book, err := services.AddressInfoCacheClient.GetAddressBook(addrList)
		if err != nil {
			log.Println("Error getting address book from cache: ", err)
		} else {
			return book, nil
		}
	}
	book := models.AddressBook{}
	if store == nil {
		return book, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel()
	ctx = store.pinReadSnapshot(ctx)
	uniqueAddrList := uniqueAccountAddresses(addrList)
	states, err := store.getAddressBookStates(ctx, uniqueAddrList)
	if err != nil {
		return nil, err
	}
	domains, err := store.batchPrimaryDomains(ctx, uniqueAddrList)
	if err != nil {
		return nil, err
	}

	for _, raw := range addrList {
		state := states[raw]
		var codeHash *models.HashType
		interfaces := []string{}
		if state != nil {
			if state.CodeHash != nil {
				codeHashValue := *state.CodeHash
				codeHash = &codeHashValue
			}
			methods := []uint32{}
			if state.ContractMethods != nil {
				methods = *state.ContractMethods
			}
			codeHashStr := ""
			if state.CodeHash != nil {
				codeHashStr = state.CodeHash.String()
			}
			interfaces = detect.DetectInterface(codeHashStr, methods)
		}
		friendly := models.GetAccountAddressFriendly(raw, codeHash, settings.IsTestnet)
		book[raw] = models.AddressBookRow{
			UserFriendly: &friendly,
			Domain:       domains[raw],
			Interfaces:   &interfaces,
		}
	}
	return book, nil
}

func (s *KvrocksStore) queryJettonWalletsTokenInfo(ctx context.Context, addrList []models.AccountAddress) (map[models.AccountAddress]models.TokenInfo, []models.AccountAddress, error) {
	if len(addrList) == 0 {
		return map[models.AccountAddress]models.TokenInfo{}, []models.AccountAddress{}, nil
	}
	addresses := make([]models.AccountAddress, 0, len(addrList))
	addrSet := map[models.AccountAddress]bool{}
	for _, addr := range addrList {
		addrSet[addr] = true
		addresses = append(addresses, addr)
	}
	wallets, err := s.GetJettonWallets(ctx, addresses)
	if err != nil {
		return nil, nil, err
	}

	tokenInfoMap := make(map[models.AccountAddress]models.TokenInfo)
	additionalAddresses := map[models.AccountAddress]bool{}
	valid := true
	tokenType := "jetton_wallets"
	for _, wallet := range wallets {
		extra := map[string]interface{}{
			"owner":  wallet.Owner.String(),
			"jetton": wallet.Jetton.String(),
		}
		if wallet.Balance != "" {
			extra["balance"] = wallet.Balance
		}
		tokenInfoMap[wallet.Address] = models.TokenInfo{
			Address: wallet.Address,
			Type:    &tokenType,
			Extra:   extra,
			Indexed: true,
			Valid:   &valid,
		}
		if !addrSet[wallet.Jetton] {
			additionalAddresses[wallet.Jetton] = true
		}
	}

	additional := make([]models.AccountAddress, 0, len(additionalAddresses))
	for addr := range additionalAddresses {
		additional = append(additional, addr)
	}
	return tokenInfoMap, additional, nil
}

type metadataKey struct {
	address models.AccountAddress
	typ     string
}

func addressMetadataID(address models.AccountAddress, typ string) string {
	return address.String() + ":" + strings.TrimSpace(typ)
}

func (s *KvrocksStore) GetAddressMetadata(ctx context.Context, keys []metadataKey) (map[metadataKey]models.TokenInfo, error) {
	ids := make([]string, 0, len(keys))
	idToKey := make(map[string]metadataKey, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		id := addressMetadataID(key.address, key.typ)
		if id == ":" {
			continue
		}
		ids = appendUniqueString(ids, seen, id)
		idToKey[id] = key
	}
	payloads, err := s.getPayloads(ctx, "address_metadata", ids)
	if err != nil {
		return nil, err
	}
	res := make(map[metadataKey]models.TokenInfo, len(payloads))
	for id, payload := range payloads {
		key := idToKey[id]
		var row struct {
			Address     models.AccountAddress  `json:"address"`
			Type        *string                `json:"type"`
			Valid       *bool                  `json:"valid"`
			Name        *string                `json:"name"`
			Symbol      *string                `json:"symbol"`
			Description *string                `json:"description"`
			Image       *string                `json:"image"`
			Extra       map[string]interface{} `json:"extra"`
		}
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode address_metadata %s: %w", id, err)
		}
		if row.Address == "" {
			row.Address = key.address
		}
		if row.Type == nil {
			typ := key.typ
			row.Type = &typ
		}
		res[key] = models.TokenInfo{
			Address:     row.Address,
			Valid:       row.Valid,
			Indexed:     true,
			Type:        row.Type,
			Name:        row.Name,
			Symbol:      row.Symbol,
			Description: row.Description,
			Image:       row.Image,
			Extra:       row.Extra,
		}
	}
	return res, nil
}

func QueryMetadataImplKvrocks(addrList []models.AccountAddress, settings models.RequestSettings, store *KvrocksStore) (models.Metadata, error) {
	if store == nil {
		return models.Metadata{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel()
	ctx = store.pinReadSnapshot(ctx)

	tokenInfoMap := map[models.AccountAddress][]models.TokenInfo{}
	jettonWalletInfos, additionalAddrs, err := store.queryJettonWalletsTokenInfo(ctx, addrList)
	if err != nil {
		return nil, err
	}
	for addr, info := range jettonWalletInfos {
		tokenInfoMap[addr] = append(tokenInfoMap[addr], info)
	}
	addresses := addrList
	if len(additionalAddrs) > 0 {
		addresses = append(addresses, additionalAddrs...)
	}
	nftItems, err := store.GetNFTItems(ctx, addresses)
	if err != nil {
		return nil, err
	}
	nftCollections, err := store.GetNFTCollections(ctx, addresses)
	if err != nil {
		return nil, err
	}
	jettonMasters, err := store.GetJettonMasters(ctx, addresses)
	if err != nil {
		return nil, err
	}

	known := map[metadataKey]models.TokenInfo{}
	addKnown := func(addr models.AccountAddress, typ string, nftIndex *string) {
		typeCopy := typ
		known[metadataKey{address: addr, typ: typ}] = models.TokenInfo{
			Address:  addr,
			Type:     &typeCopy,
			NftIndex: nftIndex,
			Indexed:  false,
		}
	}
	for _, item := range nftItems {
		index := item.Index
		addKnown(item.Address, "nft_items", &index)
	}
	for _, collection := range nftCollections {
		addKnown(collection.Address, "nft_collections", nil)
	}
	for _, master := range jettonMasters {
		addKnown(master.Address, "jetton_masters", nil)
	}

	knownKeys := make([]metadataKey, 0, len(known))
	for key := range known {
		knownKeys = append(knownKeys, key)
	}
	metadataRows, err := store.GetAddressMetadata(ctx, knownKeys)
	if err != nil {
		return nil, err
	}
	for key, row := range metadataRows {
		if knownInfo, ok := known[key]; ok {
			row.NftIndex = knownInfo.NftIndex
			metadataRows[key] = row
		}
	}

	tasks := []models.BackgroundTask{}
	for key, info := range known {
		if row, ok := metadataRows[key]; ok {
			if row.Valid != nil && *row.Valid {
				tokenInfoMap[key.address] = append(tokenInfoMap[key.address], row)
			} else {
				tokenInfoMap[key.address] = append(tokenInfoMap[key.address], models.TokenInfo{
					Address:  key.address,
					Indexed:  true,
					Valid:    row.Valid,
					Type:     row.Type,
					NftIndex: row.NftIndex,
				})
			}
			continue
		}
		tasks = append(tasks, models.BackgroundTask{
			Type: "fetch_metadata",
			Data: map[string]interface{}{
				"address": key.address,
				"type":    key.typ,
			},
		})
		tokenInfoMap[key.address] = append(tokenInfoMap[key.address], info)
	}

	metadata := models.Metadata{}
	for addr, infos := range tokenInfoMap {
		indexed := true
		for _, info := range infos {
			indexed = indexed && info.Indexed
		}
		metadata[addr] = models.AddressMetadata{
			TokenInfo: infos,
			IsIndexed: indexed,
		}
	}

	backgroundTaskManager := services.GetBackgroundTaskManager()
	if len(tasks) > 0 && backgroundTaskManager != nil {
		backgroundTaskManager.EnqueueTasksIfPossible(tasks)
	}
	return metadata, nil
}
