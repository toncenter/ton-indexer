package parse

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"

	"github.com/jackc/pgx/v5"
)

func ScanNFTCollection(row pgx.Row) (*NFTCollection, error) {
	var res NFTCollection
	err := row.Scan(&res.Address, &res.NextItemIndex, &res.OwnerAddress, &res.CollectionContent,
		&res.DataHash, &res.CodeHash, &res.LastTransactionLt)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanNFTItem(row pgx.Row) (*NFTItem, error) {
	var res NFTItem
	err := row.Scan(&res.Address, &res.Init, &res.Index, &res.CollectionAddress,
		&res.OwnerAddress, &res.Content, &res.LastTransactionLt, &res.CodeHash, &res.DataHash)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func ScanNFTItemWithCollection(row pgx.Row) (*NFTItem, error) {
	var res NFTItem
	var col NFTCollectionNullable
	var saleAddress, saleOwner *AccountAddress
	var auctionAddress, auctionOwner *AccountAddress

	err := row.Scan(&res.Address, &res.Init, &res.Index, &res.CollectionAddress,
		&res.OwnerAddress, &res.Content, &res.LastTransactionLt, &res.CodeHash, &res.DataHash,
		&col.Address, &col.NextItemIndex, &col.OwnerAddress, &col.CollectionContent,
		&col.DataHash, &col.CodeHash, &col.LastTransactionLt,
		&saleAddress, &saleOwner,
		&auctionAddress, &auctionOwner)

	if col.Address != nil {
		res.Collection = new(NFTCollection)
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

func ScanNFTTransfer(row pgx.Row) (*NFTTransfer, error) {
	var res NFTTransfer
	err := row.Scan(&res.TransactionHash, &res.TransactionLt, &res.TransactionNow, &res.TransactionAborted,
		&res.QueryId, &res.NftItemAddress, &res.NftItemIndex, &res.NftCollectionAddress,
		&res.OldOwner, &res.NewOwner, &res.ResponseDestination, &res.CustomPayload,
		&res.ForwardAmount, &res.ForwardPayload, &res.TraceId)
	if err != nil {
		return nil, err
	}
	return &res, nil
}
