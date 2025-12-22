package index

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Query GetGems Sales
func queryGetgemsSalesImpl(addresses []AccountAddress, conn *pgxpool.Conn, settings RequestSettings) ([]RawNFTSale, error) {
	address_filter := filterByArray("s.address", addresses)
	if len(address_filter) == 0 {
		return []RawNFTSale{}, nil
	}

	query := fmt.Sprintf(`
		SELECT
			'getgems_sale' as sale_type,
			s.address,
			s.nft_address,
			s.nft_owner_address,
			s.marketplace_address,
			s.created_at,
			s.last_transaction_lt,
			s.code_hash,
			s.data_hash,
			s.is_complete,
			s.full_price,
			s.marketplace_fee_address,
			s.marketplace_fee,
			s.royalty_address,
			s.royalty_amount,
			N.address as nft_item_address,
			N.init as nft_item_init,
			N.index as nft_item_index,
			N.collection_address as nft_item_collection_address,
			N.owner_address as nft_item_owner_address,
			N.content as nft_item_content,
			N.last_transaction_lt as nft_item_last_transaction_lt,
			N.code_hash as nft_item_code_hash,
			N.data_hash as nft_item_data_hash,
			C.address as collection_address,
			C.next_item_index as collection_next_item_index,
			C.owner_address as collection_owner_address,
			C.collection_content as collection_content,
			C.data_hash as collection_data_hash,
			C.code_hash as collection_code_hash,
			C.last_transaction_lt as collection_last_transaction_lt
		FROM getgems_nft_sales as s
		LEFT JOIN nft_items N ON s.nft_address = N.address
		LEFT JOIN nft_collections C ON N.collection_address = C.address
		WHERE %s
	`, address_filter)

	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	results := []RawNFTSale{}
	for rows.Next() {
		var raw RawNFTSale
		raw.Type = "getgems_sale"

		err := rows.Scan(
			&raw.Type,
			&raw.Address,
			&raw.NftAddress,
			&raw.NftOwnerAddress,
			&raw.MarketplaceAddress,
			&raw.CreatedAt,
			&raw.LastTransactionLt,
			&raw.CodeHash,
			&raw.DataHash,
			&raw.IsComplete,
			&raw.FullPrice,
			&raw.MarketplaceFeeAddress,
			&raw.MarketplaceFee,
			&raw.RoyaltyAddress,
			&raw.RoyaltyAmount,
			&raw.NftItemAddress,
			&raw.NftItemInit,
			&raw.NftItemIndex,
			&raw.NftItemCollectionAddress,
			&raw.NftItemOwnerAddress,
			&raw.NftItemContent,
			&raw.NftItemLastTransactionLt,
			&raw.NftItemCodeHash,
			&raw.NftItemDataHash,
			&raw.CollectionAddress,
			&raw.CollectionNextItemIndex,
			&raw.CollectionOwnerAddress,
			&raw.CollectionContent,
			&raw.CollectionDataHash,
			&raw.CollectionCodeHash,
			&raw.CollectionLastTransactionLt,
		)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		results = append(results, raw)
	}

	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}

	return results, nil
}

// Query GetGems Auctions
func queryGetgemsAuctionsImpl(addresses []AccountAddress, conn *pgxpool.Conn, settings RequestSettings) ([]RawNFTSale, error) {
	address_filter := filterByArray("a.address", addresses)
	if len(address_filter) == 0 {
		return []RawNFTSale{}, nil
	}

	query := fmt.Sprintf(`
		SELECT
			'getgems_auction' as sale_type,
			a.address,
			a.nft_addr as nft_address,
			a.nft_owner as nft_owner_address,
			a.mp_addr as marketplace_address,
			a.created_at,
			a.last_transaction_lt,
			a.code_hash,
			a.data_hash,
			a.end_flag,
			a.end_time,
			a.last_bid,
			a.last_member,
			a.min_step,
			a.mp_fee_addr,
			a.mp_fee_factor,
			a.mp_fee_base,
			a.royalty_fee_addr,
			a.royalty_fee_factor,
			a.royalty_fee_base,
			a.max_bid,
			a.min_bid,
			a.last_bid_at,
			a.is_canceled,
			N.address as nft_item_address,
			N.init as nft_item_init,
			N.index as nft_item_index,
			N.collection_address as nft_item_collection_address,
			N.owner_address as nft_item_owner_address,
			N.content as nft_item_content,
			N.last_transaction_lt as nft_item_last_transaction_lt,
			N.code_hash as nft_item_code_hash,
			N.data_hash as nft_item_data_hash,
			C.address as collection_address,
			C.next_item_index as collection_next_item_index,
			C.owner_address as collection_owner_address,
			C.collection_content as collection_content,
			C.data_hash as collection_data_hash,
			C.code_hash as collection_code_hash,
			C.last_transaction_lt as collection_last_transaction_lt
		FROM getgems_nft_auctions as a
		LEFT JOIN nft_items N ON a.nft_addr = N.address
		LEFT JOIN nft_collections C ON N.collection_address = C.address
		WHERE %s
	`, address_filter)

	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	results := []RawNFTSale{}
	for rows.Next() {
		var raw RawNFTSale
		raw.Type = "getgems_auction"

		err := rows.Scan(
			&raw.Type,
			&raw.Address,
			&raw.NftAddress,
			&raw.NftOwnerAddress,
			&raw.MarketplaceAddress,
			&raw.CreatedAt,
			&raw.LastTransactionLt,
			&raw.CodeHash,
			&raw.DataHash,
			&raw.EndFlag,
			&raw.EndTime,
			&raw.LastBid,
			&raw.LastMember,
			&raw.MinStep,
			&raw.MpFeeAddress,
			&raw.MpFeeFactor,
			&raw.MpFeeBase,
			&raw.RoyaltyFeeAddress,
			&raw.RoyaltyFeeFactor,
			&raw.RoyaltyFeeBase,
			&raw.MaxBid,
			&raw.MinBid,
			&raw.LastBidAt,
			&raw.IsCanceled,
			&raw.NftItemAddress,
			&raw.NftItemInit,
			&raw.NftItemIndex,
			&raw.NftItemCollectionAddress,
			&raw.NftItemOwnerAddress,
			&raw.NftItemContent,
			&raw.NftItemLastTransactionLt,
			&raw.NftItemCodeHash,
			&raw.NftItemDataHash,
			&raw.CollectionAddress,
			&raw.CollectionNextItemIndex,
			&raw.CollectionOwnerAddress,
			&raw.CollectionContent,
			&raw.CollectionDataHash,
			&raw.CollectionCodeHash,
			&raw.CollectionLastTransactionLt,
		)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		results = append(results, raw)
	}

	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}

	return results, nil
}

// Query Telemint NFTs
func queryTelemintImpl(addresses []AccountAddress, conn *pgxpool.Conn, settings RequestSettings) ([]RawNFTSale, error) {
	address_filter := filterByArray("t.address", addresses)
	if len(address_filter) == 0 {
		return []RawNFTSale{}, nil
	}

	query := fmt.Sprintf(`
		SELECT
			'telemint' as sale_type,
			t.address,
			t.token_name,
			t.bidder_address,
			t.bid,
			t.bid_ts,
			t.min_bid,
			t.end_time,
			t.beneficiary_address,
			t.initial_min_bid,
			t.max_bid,
			t.min_bid_step,
			t.min_extend_time,
			t.duration,
			t.royalty_numerator,
			t.royalty_denominator,
			t.royalty_destination,
			t.last_transaction_lt,
			t.code_hash,
			t.data_hash
		FROM telemint_nft_items as t
		WHERE %s
	`, address_filter)

	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	results := []RawNFTSale{}
	for rows.Next() {
		var raw RawNFTSale
		raw.Type = "teleitem"

		err := rows.Scan(
			&raw.Type,
			&raw.Address,
			&raw.TokenName,
			&raw.BidderAddress,
			&raw.Bid,
			&raw.BidTs,
			&raw.TelemintMinBid,
			&raw.TelemintEndTime,
			&raw.BeneficiaryAddress,
			&raw.InitialMinBid,
			&raw.TelemintMaxBid,
			&raw.MinBidStep,
			&raw.MinExtendTime,
			&raw.Duration,
			&raw.RoyaltyNumerator,
			&raw.RoyaltyDenominator,
			&raw.RoyaltyDestination,
			&raw.LastTransactionLt,
			&raw.CodeHash,
			&raw.DataHash,
		)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}

		raw.NftAddress = &raw.Address

		raw.NftItemAddress = &raw.Address
		if raw.TokenName != nil {
			raw.NftItemInit = new(bool)
			*raw.NftItemInit = true
			raw.NftItemIndex = raw.TokenName
		}
		raw.NftItemLastTransactionLt = raw.LastTransactionLt
		raw.NftItemCodeHash = raw.CodeHash
		raw.NftItemDataHash = raw.DataHash

		results = append(results, raw)
	}

	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}

	return results, nil
}

func queryNFTSalesImpl(addresses []AccountAddress, conn *pgxpool.Conn, settings RequestSettings) ([]NFTSale, error) {
	// Query all three sources
	sales, err := queryGetgemsSalesImpl(addresses, conn, settings)
	if err != nil {
		return nil, err
	}

	auctions, err := queryGetgemsAuctionsImpl(addresses, conn, settings)
	if err != nil {
		return nil, err
	}

	telemint, err := queryTelemintImpl(addresses, conn, settings)
	if err != nil {
		return nil, err
	}

	// Combine all results
	allRaw := append(sales, auctions...)
	allRaw = append(allRaw, telemint...)

	// Parse to NFTSale
	results := []NFTSale{}
	for _, raw := range allRaw {
		if sale, err := ParseNFTSale(&raw); err == nil {
			results = append(results, *sale)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}

	return results, nil
}

func (db *DbClient) QueryNFTSales(
	sales_req NFTSalesRequest,
	settings RequestSettings,
) ([]NFTSale, AddressBook, Metadata, error) {
	if len(sales_req.Address) == 0 {
		return nil, nil, nil, IndexError{Code: 422, Message: "at least 1 address required"}
	}

	if len(sales_req.Address) > 1000 {
		return nil, nil, nil, IndexError{Code: 422, Message: "maximum 1000 addresses allowed"}
	}

	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryNFTSalesImpl(sales_req.Address, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}

	// Collect all addresses for AddressBook and Metadata
	for _, sale := range res {
		addr_list = append(addr_list, string(sale.Address))
		if sale.NftAddress != nil {
			addr_list = append(addr_list, string(*sale.NftAddress))
		}
		if sale.NftOwnerAddress != nil {
			addr_list = append(addr_list, string(*sale.NftOwnerAddress))
		}
		if sale.MarketplaceAddress != nil {
			addr_list = append(addr_list, string(*sale.MarketplaceAddress))
		}

		// Collect addresses from Details based on type
		switch sale.Type {
		case "getgems_sale":
			if details, ok := sale.Details.(*NFTSaleDetailsGetgemsSale); ok {
				if details.MarketplaceFeeAddress != nil {
					addr_list = append(addr_list, string(*details.MarketplaceFeeAddress))
				}
				if details.RoyaltyAddress != nil {
					addr_list = append(addr_list, string(*details.RoyaltyAddress))
				}
			}
		case "getgems_auction":
			if details, ok := sale.Details.(*NFTSaleDetailsGetgemsAuction); ok {
				if details.LastMember != nil {
					addr_list = append(addr_list, string(*details.LastMember))
				}
				if details.MpFeeAddress != nil {
					addr_list = append(addr_list, string(*details.MpFeeAddress))
				}
				if details.RoyaltyFeeAddress != nil {
					addr_list = append(addr_list, string(*details.RoyaltyFeeAddress))
				}
			}
		case "telemint":
			if details, ok := sale.Details.(*NFTSaleDetailsTeleitem); ok {
				if details.BidderAddress != nil {
					addr_list = append(addr_list, string(*details.BidderAddress))
				}
				if details.BeneficiaryAddress != nil {
					addr_list = append(addr_list, string(*details.BeneficiaryAddress))
				}
				if details.RoyaltyDestination != nil {
					addr_list = append(addr_list, string(*details.RoyaltyDestination))
				}
			}
		}

		// Add NFT item and collection addresses
		if sale.NftItem != nil {
			addr_list = append(addr_list, string(sale.NftItem.Address))
			if sale.NftItem.CollectionAddress != nil {
				addr_list = append(addr_list, string(*sale.NftItem.CollectionAddress))
			}
			if sale.NftItem.OwnerAddress != nil {
				addr_list = append(addr_list, string(*sale.NftItem.OwnerAddress))
			}
		}
	}

	if len(addr_list) > 0 {
		if !settings.NoAddressBook {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if !settings.NoMetadata {
			metadata, err = QueryMetadataImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
	}

	return res, book, metadata, nil
}
