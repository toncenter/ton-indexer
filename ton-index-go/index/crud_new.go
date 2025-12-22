package index

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

func buildNFTSalesQuery(sales_req NFTSalesRequest, settings RequestSettings) (string, error) {
	if len(sales_req.Address) == 0 {
		return "", IndexError{Code: 422, Message: "at least 1 address required"}
	}

	address_filter_sales := filterByArray("s.address", sales_req.Address)
	address_filter_auctions := filterByArray("a.address", sales_req.Address)
	address_filter_telemint := filterByArray("t.address", sales_req.Address)

	if len(address_filter_sales) == 0 || len(address_filter_auctions) == 0 || len(address_filter_telemint) == 0 {
		return "", IndexError{Code: 422, Message: "invalid addresses provided"}
	}

	// GetGems Sales query
	sales_clmn_query := `
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
		NULL::boolean as end_flag,
		NULL::bigint as end_time,
		NULL::numeric as last_bid,
		NULL::tonaddr as last_member,
		NULL::bigint as min_step,
		NULL::tonaddr as mp_fee_address,
		NULL::bigint as mp_fee_factor,
		NULL::bigint as mp_fee_base,
		NULL::tonaddr as royalty_fee_address,
		NULL::bigint as royalty_fee_factor,
		NULL::bigint as royalty_fee_base,
		NULL::numeric as max_bid,
		NULL::numeric as min_bid,
		NULL::bigint as last_bid_at,
		NULL::boolean as is_canceled,
		NULL::varchar as token_name,
		NULL::tonaddr as bidder_address,
		NULL::numeric as bid,
		NULL::numeric as bid_ts,
		NULL::numeric as telemint_min_bid,
		NULL::integer as telemint_end_time,
		NULL::tonaddr as beneficiary_address,
		NULL::numeric as initial_min_bid,
		NULL::numeric as telemint_max_bid,
		NULL::numeric as min_bid_step,
		NULL::numeric as min_extend_time,
		NULL::numeric as duration,
		NULL::integer as royalty_numerator,
		NULL::integer as royalty_denominator,
		NULL::tonaddr as royalty_destination,
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
	`

	sales_from_query := `
		getgems_nft_sales as s
		LEFT JOIN nft_items N ON s.nft_address = N.address
		LEFT JOIN nft_collections C ON N.collection_address = C.address
	`

	// GetGems Auctions query
	auctions_clmn_query := `
		'getgems_auction' as sale_type,
		a.address,
		a.nft_addr as nft_address,
		a.nft_owner as nft_owner_address,
		a.mp_addr as marketplace_address,
		a.created_at,
		a.last_transaction_lt,
		a.code_hash,
		a.data_hash,
		NULL::boolean as is_complete,
		NULL::numeric as full_price,
		NULL::tonaddr as marketplace_fee_address,
		NULL::numeric as marketplace_fee,
		NULL::tonaddr as royalty_address,
		NULL::numeric as royalty_amount,
		a.end_flag,
		a.end_time,
		a.last_bid,
		a.last_member,
		a.min_step,
		a.mp_fee_addr as mp_fee_address,
		a.mp_fee_factor,
		a.mp_fee_base,
		a.royalty_fee_addr as royalty_fee_address,
		a.royalty_fee_factor,
		a.royalty_fee_base,
		a.max_bid,
		a.min_bid,
		a.last_bid_at,
		a.is_canceled,
		NULL::varchar as token_name,
		NULL::tonaddr as bidder_address,
		NULL::numeric as bid,
		NULL::numeric as bid_ts,
		NULL::numeric as telemint_min_bid,
		NULL::integer as telemint_end_time,
		NULL::tonaddr as beneficiary_address,
		NULL::numeric as initial_min_bid,
		NULL::numeric as telemint_max_bid,
		NULL::numeric as min_bid_step,
		NULL::numeric as min_extend_time,
		NULL::numeric as duration,
		NULL::integer as royalty_numerator,
		NULL::integer as royalty_denominator,
		NULL::tonaddr as royalty_destination,
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
	`

	auctions_from_query := `
		getgems_nft_auctions as a
		LEFT JOIN nft_items N ON a.nft_addr = N.address
		LEFT JOIN nft_collections C ON N.collection_address = C.address
	`

	// Telemint query
	telemint_clmn_query := `
		'telemint' as sale_type,
		t.address,
		NULL::tonaddr as nft_address,
		NULL::tonaddr as nft_owner_address,
		NULL::tonaddr as marketplace_address,
		NULL::bigint as created_at,
		t.last_transaction_lt,
		t.code_hash,
		t.data_hash,
		NULL::boolean as is_complete,
		NULL::numeric as full_price,
		NULL::tonaddr as marketplace_fee_address,
		NULL::numeric as marketplace_fee,
		NULL::tonaddr as royalty_address,
		NULL::numeric as royalty_amount,
		NULL::boolean as end_flag,
		NULL::bigint as end_time,
		NULL::numeric as last_bid,
		NULL::tonaddr as last_member,
		NULL::bigint as min_step,
		NULL::tonaddr as mp_fee_address,
		NULL::bigint as mp_fee_factor,
		NULL::bigint as mp_fee_base,
		NULL::tonaddr as royalty_fee_address,
		NULL::bigint as royalty_fee_factor,
		NULL::bigint as royalty_fee_base,
		NULL::numeric as max_bid,
		NULL::numeric as min_bid,
		NULL::bigint as last_bid_at,
		NULL::boolean as is_canceled,
		t.token_name,
		t.bidder_address,
		t.bid,
		t.bid_ts,
		t.min_bid as telemint_min_bid,
		t.end_time as telemint_end_time,
		t.beneficiary_address,
		t.initial_min_bid,
		t.max_bid as telemint_max_bid,
		t.min_bid_step,
		t.min_extend_time,
		t.duration,
		t.royalty_numerator,
		t.royalty_denominator,
		t.royalty_destination,
		NULL::tonaddr as nft_item_address,
		NULL::boolean as nft_item_init,
		NULL::varchar as nft_item_index,
		NULL::tonaddr as nft_item_collection_address,
		NULL::tonaddr as nft_item_owner_address,
		NULL::jsonb as nft_item_content,
		NULL::bigint as nft_item_last_transaction_lt,
		NULL::tonhash as nft_item_code_hash,
		NULL::tonhash as nft_item_data_hash,
		NULL::tonaddr as collection_address,
		NULL::varchar as collection_next_item_index,
		NULL::tonaddr as collection_owner_address,
		NULL::jsonb as collection_content,
		NULL::tonhash as collection_data_hash,
		NULL::tonhash as collection_code_hash,
		NULL::bigint as collection_last_transaction_lt
	`

	telemint_from_query := `
		telemint_nft_items as t
	`

	// Build complete query with UNION ALL
	query := fmt.Sprintf(`
		SELECT %s
		FROM %s
		WHERE %s
		UNION ALL
		SELECT %s
		FROM %s
		WHERE %s
		UNION ALL
		SELECT %s
		FROM %s
		WHERE %s
	`, sales_clmn_query, sales_from_query, address_filter_sales,
		auctions_clmn_query, auctions_from_query, address_filter_auctions,
		telemint_clmn_query, telemint_from_query, address_filter_telemint)

	return query, nil
}

func queryNFTSalesImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]NFTSale, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	sales := []NFTSale{}
	for rows.Next() {
		if raw_sale, err := ScanRawNFTSale(rows); err == nil {
			if sale, err := ParseNFTSale(raw_sale); err == nil {
				sales = append(sales, *sale)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return sales, nil
}

func (db *DbClient) QueryNFTSales(
	sales_req NFTSalesRequest,
	settings RequestSettings,
) ([]NFTSale, AddressBook, Metadata, error) {
	query, err := buildNFTSalesQuery(sales_req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query)
	}
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryNFTSalesImpl(query, conn, settings)
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
			if details, ok := sale.Details.(*NFTSaleDetailsTelemint); ok {
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
