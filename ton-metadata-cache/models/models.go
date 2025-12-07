package models

import "github.com/toncenter/ton-indexer/ton-index-go/index"

// DnsEntry represents a row from dns_entries table.
// Cache key: nft_item_address
type DnsEntry struct {
	NftItemAddress    string  `msgpack:"nft_item_address"`
	NftItemOwner      *string `msgpack:"nft_item_owner,omitempty"`
	Domain            *string `msgpack:"domain,omitempty"`
	DnsNextResolver   *string `msgpack:"dns_next_resolver,omitempty"`
	DnsWallet         *string `msgpack:"dns_wallet,omitempty"`
	DnsSiteAdnl       *string `msgpack:"dns_site_adnl,omitempty"`
	DnsStorageBagId   *string `msgpack:"dns_storage_bag_id,omitempty"`
	LastTransactionLt *int64  `msgpack:"last_transaction_lt,omitempty"`
}

// LatestAccountState represents cached account state data.
// Cache key: account
// Only stores CodeHash as that's the only field used for address book queries.
type LatestAccountState struct {
	CodeHash *string `msgpack:"code_hash,omitempty"`
}

// AddressMetadataEntry represents a row from address_metadata table.
// Cache key: address:type (composite)
type AddressMetadataEntry struct {
	Address     string                 `msgpack:"address"`
	Type        string                 `msgpack:"type"`
	Valid       *bool                  `msgpack:"valid,omitempty"`
	Name        *string                `msgpack:"name,omitempty"`
	Description *string                `msgpack:"description,omitempty"`
	Extra       map[string]interface{} `msgpack:"extra,omitempty"`
	Symbol      *string                `msgpack:"symbol,omitempty"`
	Image       *string                `msgpack:"image,omitempty"`
	UpdatedAt   *int64                 `msgpack:"updated_at,omitempty"`
	ExpiresAt   *int64                 `msgpack:"expires_at,omitempty"`
}

// AddressData holds all cached info for a single address
// Cache key: addr:{address}
type AddressData struct {
	JettonWallet  *JettonWalletData        `msgpack:"jw,omitempty"`
	NftItem       *NftItemData             `msgpack:"ni,omitempty"`
	NftCollection bool                     `msgpack:"nc,omitempty"`
	JettonMaster  bool                     `msgpack:"jm,omitempty"`
	Metadata      map[string]MetadataEntry `msgpack:"meta,omitempty"`
}

// JettonWalletData holds jetton wallet specific data
type JettonWalletData struct {
	Owner   string  `msgpack:"o"`
	Jetton  string  `msgpack:"j"`
	Balance *string `msgpack:"b,omitempty"`
}

// NftItemData holds NFT item specific data
type NftItemData struct {
	Index *string `msgpack:"i,omitempty"`
}

// MetadataEntry represents address_metadata for a specific type
type MetadataEntry struct {
	Exists      bool                   `msgpack:"e"`           // false = checked, doesn't exist in DB
	Valid       *bool                  `msgpack:"v,omitempty"` // nil = not indexed, true/false = indexed
	Name        *string                `msgpack:"n,omitempty"`
	Symbol      *string                `msgpack:"s,omitempty"`
	Description *string                `msgpack:"d,omitempty"`
	Image       *string                `msgpack:"im,omitempty"`
	Extra       map[string]interface{} `msgpack:"x,omitempty"`
}

// AddressInfoRequest represents the request body for the /address_info endpoint.
type AddressInfoRequest struct {
	Addresses          []string `json:"addresses"`
	IncludeAddressBook bool     `json:"include_address_book"`
	IncludeMetadata    bool     `json:"include_metadata"`
}

// AddressInfoResponse represents the response for the /address_info endpoint.
// Uses index.AddressMetadata and index.AddressBook from ton-index-go.
type AddressInfoResponse struct {
	Metadata    map[string]index.AddressMetadata `json:"metadata,omitempty"`
	AddressBook index.AddressBook                `json:"address_book,omitempty"`
}
