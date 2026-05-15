package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
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
	return kvrocksKeyPrefix + ":" + table + ":" + strings.TrimSpace(id)
}

func (s *KvrocksStore) payloadKey(table string, id string) string {
	return s.key(table, id) + ":payload"
}

func addressMetadataID(address string, typ string) string {
	return strings.TrimSpace(address) + ":" + strings.TrimSpace(typ)
}

func (s *KvrocksStore) getPayload(ctx context.Context, table string, id string) (string, bool, error) {
	if s == nil || s.client == nil {
		return "", false, nil
	}
	value, err := s.client.Get(ctx, s.payloadKey(table, id)).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func decodeMetadataField(payload string, field string) (map[string]interface{}, error) {
	var row map[string]json.RawMessage
	if err := json.Unmarshal([]byte(payload), &row); err != nil {
		return nil, err
	}
	raw, ok := row[field]
	if !ok || len(raw) == 0 || string(raw) == "null" {
		return map[string]interface{}{}, nil
	}
	var metadata map[string]interface{}
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return nil, err
	}
	if metadata == nil {
		metadata = map[string]interface{}{}
	}
	return metadata, nil
}

func (s *KvrocksStore) getCommonMetadata(ctx context.Context, task FetchTask) (map[string]interface{}, error) {
	var fieldName string
	switch task.Type {
	case "nft_collections":
		fieldName = "collection_content"
	case "jetton_masters":
		fieldName = "jetton_content"
	default:
		return nil, fmt.Errorf("unsupported common metadata type: %s", task.Type)
	}
	payload, ok, err := s.getPayload(ctx, task.Type, task.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %v", err)
	}
	if !ok {
		return nil, fmt.Errorf("failed to fetch metadata: %s %s not found in kvrocks", task.Type, task.Address)
	}
	metadata, err := decodeMetadataField(payload, fieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %v", err)
	}
	metadata["_type"] = task.Type
	return metadata, nil
}

func (s *KvrocksStore) getNftMetadata(ctx context.Context, task FetchTask) (map[string]interface{}, error) {
	payload, ok, err := s.getPayload(ctx, "nft_items", task.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %v", err)
	}
	if !ok {
		return nil, fmt.Errorf("failed to fetch metadata: nft_items %s not found in kvrocks", task.Address)
	}
	metadata, err := decodeMetadataField(payload, "content")
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %v", err)
	}

	if dnsPayload, ok, err := s.getPayload(ctx, "dns_entries", task.Address); err != nil {
		return nil, fmt.Errorf("failed to fetch dns metadata: %v", err)
	} else if ok {
		var dnsRow struct {
			Domain *string `json:"domain"`
		}
		if err := json.Unmarshal([]byte(dnsPayload), &dnsRow); err != nil {
			return nil, fmt.Errorf("failed to unmarshal dns metadata: %v", err)
		}
		if dnsRow.Domain != nil {
			metadata["domain"] = *dnsRow.Domain
		}
	}
	metadata["_type"] = "nft_items"
	return metadata, nil
}

func (s *KvrocksStore) GetMetadata(ctx context.Context, task FetchTask) (map[string]interface{}, error) {
	switch task.Type {
	case "nft_collections", "jetton_masters":
		return s.getCommonMetadata(ctx, task)
	case "nft_items":
		return s.getNftMetadata(ctx, task)
	}
	return nil, fmt.Errorf("unsupported task type: %s", task.Type)
}

type addressMetadataPayload struct {
	Address        string                 `json:"address"`
	Type           string                 `json:"type"`
	Valid          bool                   `json:"valid"`
	Name           *string                `json:"name,omitempty"`
	Description    *string                `json:"description,omitempty"`
	Image          *string                `json:"image,omitempty"`
	Symbol         *string                `json:"symbol,omitempty"`
	Extra          map[string]interface{} `json:"extra,omitempty"`
	UpdatedAt      int64                  `json:"updated_at"`
	ExpiresAt      int64                  `json:"expires_at"`
	ReindexAllowed *bool                  `json:"reindex_allowed,omitempty"`
}

func (s *KvrocksStore) SetAddressMetadata(ctx context.Context, payload addressMetadataPayload) error {
	if s == nil || s.client == nil {
		return nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	id := addressMetadataID(payload.Address, payload.Type)
	return s.client.Set(ctx, s.payloadKey("address_metadata", id), data, 0).Err()
}
