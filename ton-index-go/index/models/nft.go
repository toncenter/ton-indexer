package models

import "encoding/json"

type NFTCollection struct {
	Address           AccountAddress         `json:"address"`
	OwnerAddress      *AccountAddress        `json:"owner_address"`
	LastTransactionLt int64                  `json:"last_transaction_lt,string"`
	NextItemIndex     string                 `json:"next_item_index"`
	CollectionContent map[string]interface{} `json:"collection_content"`
	DataHash          HashType               `json:"data_hash"`
	CodeHash          HashType               `json:"code_hash"`
	CodeBoc           string                 `json:"-"`
	DataBoc           string                 `json:"-"`
} // @name NFTCollection

type NFTCollectionNullable struct {
	Address           *AccountAddress
	OwnerAddress      *AccountAddress
	LastTransactionLt *int64
	NextItemIndex     *string
	CollectionContent map[string]interface{}
	DataHash          *HashType
	CodeHash          *HashType
	CodeBoc           *string
	DataBoc           *string
}

type NFTItem struct {
	Address                AccountAddress         `json:"address"`
	Init                   bool                   `json:"init"`
	Index                  string                 `json:"index"`
	CollectionAddress      *AccountAddress        `json:"collection_address"`
	OwnerAddress           *AccountAddress        `json:"owner_address"`
	Content                map[string]interface{} `json:"content"`
	LastTransactionLt      int64                  `json:"last_transaction_lt,string"`
	CodeHash               HashType               `json:"code_hash"`
	DataHash               HashType               `json:"data_hash"`
	Collection             *NFTCollection         `json:"collection"`
	OnSale                 bool                   `json:"on_sale"`
	SaleContractAddress    *AccountAddress        `json:"sale_contract_address,omitempty"`
	AuctionContractAddress *AccountAddress        `json:"auction_contract_address,omitempty"`
	RealOwner              *AccountAddress        `json:"real_owner,omitempty"`
} // @name NFTItem

type NFTTransfer struct {
	QueryId               string           `json:"query_id"`
	NftItemAddress        AccountAddress   `json:"nft_address"`
	NftItemIndex          string           `json:"-"`
	NftCollectionAddress  AccountAddress   `json:"nft_collection"`
	TransactionHash       HashType         `json:"transaction_hash"`
	TransactionLt         int64            `json:"transaction_lt,string"`
	TransactionNow        int64            `json:"transaction_now"`
	TransactionAborted    bool             `json:"transaction_aborted"`
	OldOwner              AccountAddress   `json:"old_owner"`
	NewOwner              AccountAddress   `json:"new_owner"`
	ResponseDestination   *AccountAddress  `json:"response_destination"`
	CustomPayload         *string          `json:"custom_payload"`
	DecodedCustomPayload  *json.RawMessage `json:"decoded_custom_payload" swaggertype:"object"`
	ForwardAmount         *string          `json:"forward_amount"`
	ForwardPayload        *string          `json:"forward_payload"`
	DecodedForwardPayload *json.RawMessage `json:"decoded_forward_payload" swaggertype:"object"`
	TraceId               *HashType        `json:"trace_id"`
} // @name NFTTransfer

type NFTSaleDetailsGetgemsSale struct {
	IsComplete            *bool           `json:"is_complete,omitempty"`
	FullPrice             *string         `json:"full_price,omitempty"`
	MarketplaceFeeAddress *AccountAddress `json:"marketplace_fee_address,omitempty"`
	MarketplaceFee        *string         `json:"marketplace_fee,omitempty"`
	RoyaltyAddress        *AccountAddress `json:"royalty_address,omitempty"`
	RoyaltyAmount         *string         `json:"royalty_amount,omitempty"`
}

type NFTSaleDetailsGetgemsAuction struct {
	EndFlag           *bool           `json:"end_flag,omitempty"`
	EndTime           *int64          `json:"end_time,omitempty"`
	LastBid           *string         `json:"last_bid,omitempty"`
	LastMember        *AccountAddress `json:"last_member,omitempty"`
	MinStep           *int64          `json:"min_step,omitempty"`
	MpFeeAddress      *AccountAddress `json:"mp_fee_address,omitempty"`
	MpFeeFactor       *int64          `json:"mp_fee_factor,omitempty"`
	MpFeeBase         *int64          `json:"mp_fee_base,omitempty"`
	RoyaltyFeeAddress *AccountAddress `json:"royalty_fee_address,omitempty"`
	RoyaltyFeeFactor  *int64          `json:"royalty_fee_factor,omitempty"`
	RoyaltyFeeBase    *int64          `json:"royalty_fee_base,omitempty"`
	MaxBid            *string         `json:"max_bid,omitempty"`
	MinBid            *string         `json:"min_bid,omitempty"`
	LastBidAt         *int64          `json:"last_bid_at,omitempty"`
	IsCanceled        *bool           `json:"is_canceled,omitempty"`
}

type NFTSaleDetailsTeleitem struct {
	TokenName          *string         `json:"token_name,omitempty"`
	BidderAddress      *AccountAddress `json:"bidder_address,omitempty"`
	Bid                *string         `json:"bid,omitempty"`
	BidTs              *string         `json:"bid_ts,omitempty"`
	MinBid             *string         `json:"min_bid,omitempty"`
	EndTime            *int32          `json:"end_time,omitempty"`
	BeneficiaryAddress *AccountAddress `json:"beneficiary_address,omitempty"`
	InitialMinBid      *string         `json:"initial_min_bid,omitempty"`
	MaxBid             *string         `json:"max_bid,omitempty"`
	MinBidStep         *string         `json:"min_bid_step,omitempty"`
	MinExtendTime      *string         `json:"min_extend_time,omitempty"`
	Duration           *string         `json:"duration,omitempty"`
	RoyaltyNumerator   *int32          `json:"royalty_numerator,omitempty"`
	RoyaltyDenominator *int32          `json:"royalty_denominator,omitempty"`
	RoyaltyDestination *AccountAddress `json:"royalty_destination,omitempty"`
}

type RawNFTSale struct {
	Type               string
	Address            AccountAddress
	NftAddress         *AccountAddress
	NftOwnerAddress    *AccountAddress
	MarketplaceAddress *AccountAddress
	CreatedAt          *int64
	LastTransactionLt  *int64
	CodeHash           *HashType
	DataHash           *HashType
	// GetGems Sale fields
	IsComplete            *bool
	FullPrice             *string
	MarketplaceFeeAddress *AccountAddress
	MarketplaceFee        *string
	RoyaltyAddress        *AccountAddress
	RoyaltyAmount         *string
	// GetGems Auction fields
	EndFlag           *bool
	EndTime           *int64
	LastBid           *string
	LastMember        *AccountAddress
	MinStep           *int64
	MpFeeAddress      *AccountAddress
	MpFeeFactor       *int64
	MpFeeBase         *int64
	RoyaltyFeeAddress *AccountAddress
	RoyaltyFeeFactor  *int64
	RoyaltyFeeBase    *int64
	MaxBid            *string
	MinBid            *string
	LastBidAt         *int64
	IsCanceled        *bool
	// Telemint fields
	TokenName          *string
	BidderAddress      *AccountAddress
	Bid                *string
	BidTs              *string
	TelemintMinBid     *string
	TelemintEndTime    *int32
	BeneficiaryAddress *AccountAddress
	InitialMinBid      *string
	TelemintMaxBid     *string
	MinBidStep         *string
	MinExtendTime      *string
	Duration           *string
	RoyaltyNumerator   *int32
	RoyaltyDenominator *int32
	RoyaltyDestination *AccountAddress
	// NFT Item fields
	NftItemAddress           *AccountAddress
	NftItemInit              *bool
	NftItemIndex             *string
	NftItemCollectionAddress *AccountAddress
	NftItemOwnerAddress      *AccountAddress
	NftItemContent           map[string]interface{}
	NftItemLastTransactionLt *int64
	NftItemCodeHash          *HashType
	NftItemDataHash          *HashType
	// NFT Collection fields
	CollectionAddress           *AccountAddress
	CollectionNextItemIndex     *string
	CollectionOwnerAddress      *AccountAddress
	CollectionContent           map[string]interface{}
	CollectionDataHash          *HashType
	CollectionCodeHash          *HashType
	CollectionLastTransactionLt *int64
}

type NFTSale struct {
	Type               string          `json:"type"`
	Address            AccountAddress  `json:"address"`
	NftAddress         *AccountAddress `json:"nft_address,omitempty"`
	NftOwnerAddress    *AccountAddress `json:"nft_owner_address,omitempty"`
	MarketplaceAddress *AccountAddress `json:"marketplace_address,omitempty"`
	CreatedAt          *int64          `json:"created_at,omitempty"`
	LastTransactionLt  *int64          `json:"last_transaction_lt,string,omitempty"`
	CodeHash           *HashType       `json:"code_hash,omitempty"`
	DataHash           *HashType       `json:"data_hash,omitempty"`
	Details            interface{}     `json:"details"`
	NftItem            *NFTItem        `json:"nft_item,omitempty"`
} // @name NFTSale
