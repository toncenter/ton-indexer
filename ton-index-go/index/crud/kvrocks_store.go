package crud

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

const kvrocksKeyPrefix = "ton-index:v1"

type KvrocksConfig struct {
	Addr               string
	SentinelAddrs      []string
	SentinelMasterName string
	Username           string
	Password           string
	DB                 int
}

type KvrocksStore struct {
	client redis.UniversalClient
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
	return &KvrocksStore{client: client}, nil
}

func (s *KvrocksStore) Close() error {
	if s == nil || s.client == nil {
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

	values, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	for idx, value := range values {
		if value == nil {
			continue
		}
		switch payload := value.(type) {
		case string:
			payloads[uniqueIDs[idx]] = payload
		case []byte:
			payloads[uniqueIDs[idx]] = string(payload)
		default:
			return nil, fmt.Errorf("unexpected kvrocks payload type %T for %s", value, uniqueIDs[idx])
		}
	}
	return payloads, nil
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
			Hash models.HashType `json:"hash"`
			Body *string         `json:"body"`
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
		var row models.JettonWallet
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode jetton_wallets %s: %w", id, err)
		}
		res[row.Address] = &row
	}
	return res, nil
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
		var row models.NFTCollection
		if err := json.Unmarshal([]byte(payload), &row); err != nil {
			return nil, fmt.Errorf("decode nft_collections %s: %w", id, err)
		}
		res[row.Address] = &row
	}
	return res, nil
}

func (s *KvrocksStore) GetNFTSales(ctx context.Context, addresses []models.AccountAddress) ([]models.RawNFTSale, error) {
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
