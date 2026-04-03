package models

type DNSRecord struct {
	NftItemAddress AccountAddress  `json:"nft_item_address"`
	NftItemOwner   *AccountAddress `json:"nft_item_owner"`
	Domain         string          `json:"domain"`
	NextResolver   *AccountAddress `json:"dns_next_resolver"`
	Wallet         *AccountAddress `json:"dns_wallet"`
	SiteAdnl       *string         `json:"dns_site_adnl"`
	StorageBagID   *string         `json:"dns_storage_bag_id"`
} // @name DNSRecord
