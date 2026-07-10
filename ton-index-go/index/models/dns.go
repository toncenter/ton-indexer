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

type DNSAuctionRecord struct {
	NftItemAddress AccountAddress  `json:"nft_item_address"`
	Domain         string          `json:"domain"`
	MaxBidAddress  *AccountAddress `json:"max_bid_address"`
	MaxBidAmount   *string         `json:"max_bid_amount"`
	AuctionEndTime *int64          `json:"auction_end_time,string"`
	LastFillUpTime *int64          `json:"last_fill_up_time,string"`
} // @name DNSAuctionRecord

type DNSAuction struct {
	NftItemAddress AccountAddress  `json:"nft_item_address"`
	Domain         string          `json:"domain"`
	MaxBidAddress  *AccountAddress `json:"max_bid_address"`
	MaxBidAmount   *string         `json:"max_bid_amount"`
	AuctionEndTime *int64          `json:"auction_end_time,string"`
	LastFillUpTime *int64          `json:"last_fill_up_time,string"`
	Finished       bool            `json:"finished"`
	NftItem        *NFTItem        `json:"nft_item,omitempty"`
} // @name DNSAuction
