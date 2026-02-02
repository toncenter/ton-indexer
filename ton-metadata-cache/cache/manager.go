package cache

import (
	"ton-metadata-cache/models"

	"github.com/redis/go-redis/v9"
)

// Manager holds all typed caches for the application.
type Manager struct {
	// ContractMethods cache: code_hash -> []uint32 (no TTL, preloaded)
	ContractMethods *Cache[[]uint32]

	// DnsEntries cache: nft_item_address -> DnsEntry (no TTL, preloaded)
	DnsEntries *Cache[models.DnsEntry]

	// DnsByOwner: owner_address -> sorted set of nft_item_addresses
	// Score is domain length, so lowest score = shortest domain
	// Only contains self-resolving entries (owner=wallet)
	DnsByOwner *SortedSetCache

	// AccountStates cache: account -> LatestAccountState (1h TTL, use GetEx)
	AccountStates *Cache[models.LatestAccountState]

	// AddressMetadata cache: address:type -> AddressMetadataEntry (1h TTL, use GetEx)
	AddressMetadata *Cache[models.AddressMetadataEntry]

	// AddressInfo cache: address -> AddressData (unified metadata cache)
	AddressInfo *Cache[models.AddressData]
}

// NewManager creates a Manager with all caches configured.
func NewManager(client *redis.Client) *Manager {
	return &Manager{
		ContractMethods: New(Options[[]uint32]{
			Client:  client,
			Encoder: MsgpackEncoder[[]uint32](),
			Decoder: MsgpackDecoder[[]uint32](),
			Prefix:  "cm",
		}),
		DnsEntries: New(Options[models.DnsEntry]{
			Client:  client,
			Encoder: MsgpackEncoder[models.DnsEntry](),
			Decoder: MsgpackDecoder[models.DnsEntry](),
			Prefix:  "dns",
		}),
		DnsByOwner: NewSortedSetCache(client, "dnsown"),
		AccountStates: New(Options[models.LatestAccountState]{
			Client:  client,
			Encoder: MsgpackEncoder[models.LatestAccountState](),
			Decoder: MsgpackDecoder[models.LatestAccountState](),
			Prefix:  "acc",
		}),
		AddressMetadata: New(Options[models.AddressMetadataEntry]{
			Client:  client,
			Encoder: MsgpackEncoder[models.AddressMetadataEntry](),
			Decoder: MsgpackDecoder[models.AddressMetadataEntry](),
			Prefix:  "meta",
		}),
		AddressInfo: New(Options[models.AddressData]{
			Client:  client,
			Encoder: MsgpackEncoder[models.AddressData](),
			Decoder: MsgpackDecoder[models.AddressData](),
			Prefix:  "addr",
		}),
	}
}
