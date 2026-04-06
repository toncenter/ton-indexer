package parse

import (
	"github.com/jackc/pgx/v5"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

func ScanNFTCollection(row pgx.Row) (*models.NFTCollection, error) {
	var res models.NFTCollection
	err := row.Scan(&res.Address, &res.NextItemIndex, &res.OwnerAddress, &res.CollectionContent,
		&res.DataHash, &res.CodeHash, &res.LastTransactionLt)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanNFTItem(row pgx.Row) (*models.NFTItem, error) {
	var res models.NFTItem
	err := row.Scan(&res.Address, &res.Init, &res.Index, &res.CollectionAddress,
		&res.OwnerAddress, &res.Content, &res.LastTransactionLt, &res.CodeHash, &res.DataHash)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanNFTItemWithCollection(row pgx.Row) (*models.NFTItem, error) {
	var res models.NFTItem
	var col models.NFTCollectionNullable
	var saleAddress, saleOwner *models.AccountAddress
	var auctionAddress, auctionOwner *models.AccountAddress

	err := row.Scan(&res.Address, &res.Init, &res.Index, &res.CollectionAddress,
		&res.OwnerAddress, &res.Content, &res.LastTransactionLt, &res.CodeHash, &res.DataHash,
		&col.Address, &col.NextItemIndex, &col.OwnerAddress, &col.CollectionContent,
		&col.DataHash, &col.CodeHash, &col.LastTransactionLt,
		&saleAddress, &saleOwner,
		&auctionAddress, &auctionOwner)

	if col.Address != nil {
		res.Collection = new(models.NFTCollection)
		res.Collection.Address = *col.Address
		res.Collection.NextItemIndex = *col.NextItemIndex
		res.Collection.OwnerAddress = col.OwnerAddress
		res.Collection.CollectionContent = col.CollectionContent
		res.Collection.DataHash = *col.DataHash
		res.Collection.CodeHash = *col.CodeHash
		res.Collection.LastTransactionLt = *col.LastTransactionLt
	}

	// Handle sale/auction data
	if saleAddress != nil {
		res.OnSale = true
		res.SaleContractAddress = saleAddress
		if saleOwner != nil {
			res.RealOwner = saleOwner
		}
	} else if auctionAddress != nil {
		res.OnSale = true
		res.AuctionContractAddress = auctionAddress
		if auctionOwner != nil {
			res.RealOwner = auctionOwner
		}
	}

	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanNFTTransfer(row pgx.Row) (*models.NFTTransfer, error) {
	var res models.NFTTransfer
	err := row.Scan(&res.TransactionHash, &res.TransactionLt, &res.TransactionNow, &res.TransactionAborted,
		&res.QueryId, &res.NftItemAddress, &res.NftItemIndex, &res.NftCollectionAddress,
		&res.OldOwner, &res.NewOwner, &res.ResponseDestination, &res.CustomPayload,
		&res.ForwardAmount, &res.ForwardPayload, &res.TraceId)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ParseNFTSale(raw *models.RawNFTSale) (*models.NFTSale, error) {
	var sale models.NFTSale

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
		var details models.NFTSaleDetailsGetgemsSale
		details.IsComplete = raw.IsComplete
		details.FullPrice = raw.FullPrice
		details.MarketplaceFeeAddress = raw.MarketplaceFeeAddress
		details.MarketplaceFee = raw.MarketplaceFee
		details.RoyaltyAddress = raw.RoyaltyAddress
		details.RoyaltyAmount = raw.RoyaltyAmount
		sale.Details = &details
	case "getgems_auction":
		var details models.NFTSaleDetailsGetgemsAuction
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
		var details models.NFTSaleDetailsTeleitem
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
		sale.NftItem = new(models.NFTItem)
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
			sale.NftItem.Collection = new(models.NFTCollection)
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
