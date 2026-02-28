package parse

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

func ParseNFTSale(raw *RawNFTSale) (*NFTSale, error) {
	var sale NFTSale

	sale.Type = raw.Type
	sale.Address = raw.Address
	sale.NftAddress = raw.NftAddress
	sale.NftOwnerAddress = raw.NftOwnerAddress
	sale.MarketplaceAddress = raw.MarketplaceAddress
	sale.CreatedAt = raw.CreatedAt
	sale.LastTransactionLt = raw.LastTransactionLt
	sale.CodeHash = raw.CodeHash
	sale.DataHash = raw.DataHash

	switch raw.Type {
	case "getgems_sale":
		var details NFTSaleDetailsGetgemsSale
		details.IsComplete = raw.IsComplete
		details.FullPrice = raw.FullPrice
		details.MarketplaceFeeAddress = raw.MarketplaceFeeAddress
		details.MarketplaceFee = raw.MarketplaceFee
		details.RoyaltyAddress = raw.RoyaltyAddress
		details.RoyaltyAmount = raw.RoyaltyAmount
		sale.Details = &details
	case "getgems_auction":
		var details NFTSaleDetailsGetgemsAuction
		details.EndFlag = raw.EndFlag
		details.EndTime = raw.EndTime
		details.LastBid = raw.LastBid
		details.LastMember = raw.LastMember
		details.MinStep = raw.MinStep
		details.MpFeeAddress = raw.MpFeeAddress
		details.MpFeeFactor = raw.MpFeeFactor
		details.MpFeeBase = raw.MpFeeBase
		details.RoyaltyFeeAddress = raw.RoyaltyFeeAddress
		details.RoyaltyFeeFactor = raw.RoyaltyFeeFactor
		details.RoyaltyFeeBase = raw.RoyaltyFeeBase
		details.MaxBid = raw.MaxBid
		details.MinBid = raw.MinBid
		details.LastBidAt = raw.LastBidAt
		details.IsCanceled = raw.IsCanceled
		sale.Details = &details
	case "telemint":
		var details NFTSaleDetailsTeleitem
		details.TokenName = raw.TokenName
		details.BidderAddress = raw.BidderAddress
		details.Bid = raw.Bid
		details.BidTs = raw.BidTs
		details.MinBid = raw.TelemintMinBid
		details.EndTime = raw.TelemintEndTime
		details.BeneficiaryAddress = raw.BeneficiaryAddress
		details.InitialMinBid = raw.InitialMinBid
		details.MaxBid = raw.TelemintMaxBid
		details.MinBidStep = raw.MinBidStep
		details.MinExtendTime = raw.MinExtendTime
		details.Duration = raw.Duration
		details.RoyaltyNumerator = raw.RoyaltyNumerator
		details.RoyaltyDenominator = raw.RoyaltyDenominator
		details.RoyaltyDestination = raw.RoyaltyDestination
		sale.Details = &details
	}

	if raw.NftItemAddress != nil {
		sale.NftItem = new(NFTItem)
		sale.NftItem.Address = *raw.NftItemAddress
		if raw.NftItemInit != nil {
			sale.NftItem.Init = *raw.NftItemInit
		}
		if raw.NftItemIndex != nil {
			sale.NftItem.Index = *raw.NftItemIndex
		}
		sale.NftItem.CollectionAddress = raw.NftItemCollectionAddress
		sale.NftItem.OwnerAddress = raw.NftItemOwnerAddress
		sale.NftItem.Content = raw.NftItemContent
		if raw.NftItemLastTransactionLt != nil {
			sale.NftItem.LastTransactionLt = *raw.NftItemLastTransactionLt
		}
		if raw.NftItemCodeHash != nil {
			sale.NftItem.CodeHash = *raw.NftItemCodeHash
		}
		if raw.NftItemDataHash != nil {
			sale.NftItem.DataHash = *raw.NftItemDataHash
		}

		if raw.CollectionAddress != nil {
			sale.NftItem.Collection = new(NFTCollection)
			sale.NftItem.Collection.Address = *raw.CollectionAddress
			if raw.CollectionNextItemIndex != nil {
				sale.NftItem.Collection.NextItemIndex = *raw.CollectionNextItemIndex
			}
			sale.NftItem.Collection.OwnerAddress = raw.CollectionOwnerAddress
			sale.NftItem.Collection.CollectionContent = raw.CollectionContent
			if raw.CollectionDataHash != nil {
				sale.NftItem.Collection.DataHash = *raw.CollectionDataHash
			}
			if raw.CollectionCodeHash != nil {
				sale.NftItem.Collection.CodeHash = *raw.CollectionCodeHash
			}
			if raw.CollectionLastTransactionLt != nil {
				sale.NftItem.Collection.LastTransactionLt = *raw.CollectionLastTransactionLt
			}
		}
	}

	return &sale, nil
}
