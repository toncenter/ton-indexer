package index

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)


func buildNFTSalesQuery(sales_req NFTSalesRequest, settings RequestSettings) (string, error) {
	if len(sales_req.Address) == 0 {
		return "", IndexError{Code: 422, Message: "at least 1 address required"}
	}

	address_filter_sales := filterByArray("s.address", sales_req.Address)
	address_filter_auctions := filterByArray("a.address", sales_req.Address)

	if len(address_filter_sales) == 0 || len(address_filter_auctions) == 0 {
		return "", IndexError{Code: 422, Message: "invalid addresses provided"}
	}

	sales_clmn_query := `
		'sale' as sale_type,
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

	// Columns for auctions query
	auctions_clmn_query := `
		'auction' as sale_type,
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

	query := fmt.Sprintf(`
		SELECT %s
		FROM %s
		WHERE %s
		UNION ALL
		SELECT %s
		FROM %s
		WHERE %s
	`, sales_clmn_query, sales_from_query, address_filter_sales,
		auctions_clmn_query, auctions_from_query, address_filter_auctions)

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
		if sale, err := ScanNFTSale(rows); err == nil {
			sales = append(sales, *sale)
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
		if sale.MarketplaceFeeAddress != nil {
			addr_list = append(addr_list, string(*sale.MarketplaceFeeAddress))
		}
		if sale.RoyaltyAddress != nil {
			addr_list = append(addr_list, string(*sale.RoyaltyAddress))
		}
		if sale.MpFeeAddress != nil {
			addr_list = append(addr_list, string(*sale.MpFeeAddress))
		}
		if sale.RoyaltyFeeAddress != nil {
			addr_list = append(addr_list, string(*sale.RoyaltyFeeAddress))
		}
		if sale.LastMember != nil {
			addr_list = append(addr_list, string(*sale.LastMember))
		}
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
