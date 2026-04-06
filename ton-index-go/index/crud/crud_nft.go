package crud

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/toncenter/ton-indexer/ton-index-go/index/detect"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"
	"log"
	"strings"
)

func buildNFTCollectionsQuery(nft_req models.NFTCollectionRequest, lim_req models.LimitRequest, settings models.RequestSettings) (string, error) {
	clmn_query := ` N.address, N.next_item_index, N.owner_address, N.collection_content, 
				    N.data_hash, N.code_hash, N.last_transaction_lt`
	from_query := ` nft_collections as N`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by id asc`
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := nft_req.CollectionAddress; v != nil {
		filter_str := filterByArray("N.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := nft_req.OwnerAddress; v != nil {
		filter_str := filterByArray("N.owner_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildNFTItemsQuery(nft_req models.NFTItemRequest, lim_req models.LimitRequest, settings models.RequestSettings) (string, []any, error) {
	args := []any{}
	clmn_query := ` N.address, N.init, N.index, N.collection_address, N.owner_address, N.content, 
                N.last_transaction_lt, N.code_hash, N.data_hash,
                C.address, C.next_item_index, C.owner_address, C.collection_content, 
                C.data_hash, C.code_hash, C.last_transaction_lt,
                S.address, S.nft_owner_address,
                A.address, A.nft_owner`
	from_query := ` nft_items as N 
                left join nft_collections as C on N.collection_address = C.address
                left join getgems_nft_sales as S on N.owner_address = S.address and N.address = S.nft_address
                left join getgems_nft_auctions as A on N.owner_address = A.address and N.address = A.nft_addr`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by N.id asc`
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", nil, err
	}

	if v := nft_req.Address; v != nil {
		filter_str := filterByArray("N.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := nft_req.OwnerAddress; v != nil {
		var filter_str string
		if nft_req.IncludeOnSale != nil && *nft_req.IncludeOnSale {
			filter_str = filterByArray("N.real_owner", v)
		} else {
			filter_str = filterByArray("N.owner_address", v)
		}
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ` order by N.owner_address, N.collection_address, N.index`
	}
	if v := nft_req.CollectionAddress; v != nil {
		if len(nft_req.CollectionAddress) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("N.collection_address = '%s'", v[0]))
			orderby_query = ` order by N.collection_address, N.index`
		} else if len(nft_req.CollectionAddress) > 1 {
			filter_str := filterByArray("N.collection_address", v)
			if len(filter_str) > 0 {
				filter_list = append(filter_list, filter_str)
			}
		}
	}
	if v := nft_req.Index; len(v) > 0 {
		if nft_req.CollectionAddress == nil {
			return ``, nil, models.IndexError{Code: 422, Message: "index parameter is not allowed without the collection_address"}
		}
		filtered := v[:0]
		for _, s := range v {
			if s != "" {
				filtered = append(filtered, s)
			}
		}
		if len(filtered) > 0 {
			args = append(args, filtered)
			filter_list = append(filter_list, fmt.Sprintf("N.index = ANY($%d)", len(args)))
		}
	}
	if v := nft_req.SortByLastTransactionLt; v != nil && *v {
		orderby_query = ` order by N.last_transaction_lt desc`
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, args, nil
}

func buildNFTTransfersQuery(transfer_req models.NFTTransferRequest, utime_req models.UtimeRequest,
	lt_req models.LtRequest, lim_req models.LimitRequest, settings models.RequestSettings) (string, error) {
	clmn_query := ` T.tx_hash, T.tx_lt, T.tx_now, T.tx_aborted, T.query_id,
		T.nft_item_address, T.nft_item_index, T.nft_collection_address, T.old_owner, T.new_owner, T.response_destination, T.custom_payload,
		T.forward_amount, T.forward_payload, T.trace_id`
	from_query := ` nft_transfers as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := transfer_req.OwnerAddress; v != nil {
		if v1 := transfer_req.Direction; v1 != nil {
			f_str := ``
			if *v1 == "in" {
				f_str = filterByArray("T.new_owner", v)
			} else {
				f_str = filterByArray("T.old_owner", v)
			}
			if len(f_str) > 0 {
				filter_list = append(filter_list, f_str)
			}
		} else {
			f1_str := filterByArray("T.old_owner", v)
			f2_str := filterByArray("T.new_owner", v)
			if len(f1_str) > 0 {
				filter_list = append(filter_list, fmt.Sprintf("(%s or %s)", f1_str, f2_str))
			}
		}
	}
	if v := transfer_req.ItemAddress; v != nil {
		filter_str := filterByArray("T.nft_item_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := transfer_req.CollectionAddress; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.nft_collection_address = '%s'", *v))
	}

	order_col := "T.tx_lt"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now >= %d", *v))
		order_col = "T.tx_now"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now <= %d", *v))
		order_col = "T.tx_now"
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt <= %d", *v))
	}
	if lim_req.Sort == nil {
		lim_req.Sort = new(models.SortType)
		*lim_req.Sort = "desc"
	}
	if lim_req.Sort != nil {
		sort_order, err := getSortOrder(*lim_req.Sort)
		if err != nil {
			return "", err
		}
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, sort_order)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query)
	return query, nil
}

func queryNFTCollectionsImpl(query string, conn *pgxpool.Conn, settings models.RequestSettings) ([]models.NFTCollection, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	nfts := []models.NFTCollection{}
	for rows.Next() {
		if nft, err := parse.ScanNFTCollection(rows); err == nil {
			nfts = append(nfts, *nft)
		} else {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, models.IndexError{Code: 500, Message: rows.Err().Error()}
	}

	return nfts, nil
}

func queryNFTItemsWithCollectionsImpl(query string, conn *pgxpool.Conn, settings models.RequestSettings, args ...any) ([]models.NFTItem, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	nfts := []models.NFTItem{}
	for rows.Next() {
		if nft, err := parse.ScanNFTItemWithCollection(rows); err == nil {
			nfts = append(nfts, *nft)
		} else {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, models.IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return nfts, nil
}

func queryNFTTransfersImpl(query string, conn *pgxpool.Conn, settings models.RequestSettings) ([]models.NFTTransfer, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	res := []models.NFTTransfer{}
	for rows.Next() {
		if loc, err := parse.ScanNFTTransfer(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, models.IndexError{Code: 500, Message: rows.Err().Error()}
	}
	if err := detect.MarkNFTTransfers(res); err != nil {
		hashes := make([]string, len(res))
		for i, t := range res {
			hashes[i] = string(t.TransactionHash)
		}
		log.Printf("Error marking nft transfers with hashes %v: %v", hashes, err)
	}

	return res, nil
}

func (db *DbClient) QueryNFTCollections(
	nft_req models.NFTCollectionRequest,
	lim_req models.LimitRequest,
	settings models.RequestSettings,
) ([]models.NFTCollection, models.AddressBook, models.Metadata, error) {
	query, err := buildNFTCollectionsQuery(nft_req, lim_req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query)
	}
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryNFTCollectionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	book := models.AddressBook{}
	metadata := models.Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.Address))
		if t.OwnerAddress != nil {
			addr_list = append(addr_list, string(*t.OwnerAddress))
		}
	}
	if len(addr_list) > 0 {
		if !settings.NoAddressBook {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
		if !settings.NoMetadata {
			metadata, err = QueryMetadataImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
	}
	return res, book, metadata, nil
}

func (db *DbClient) QueryNFTItems(
	nft_req models.NFTItemRequest,
	lim_req models.LimitRequest,
	settings models.RequestSettings,
) ([]models.NFTItem, models.AddressBook, models.Metadata, error) {
	query, args, err := buildNFTItemsQuery(nft_req, lim_req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query)
	}
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryNFTItemsWithCollectionsImpl(query, conn, settings, args...)
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	book := models.AddressBook{}
	metadata := models.Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.Address))
		if t.CollectionAddress != nil {
			addr_list = append(addr_list, string(*t.CollectionAddress))
		}
		if t.OwnerAddress != nil {
			addr_list = append(addr_list, string(*t.OwnerAddress))
		}
	}
	if len(addr_list) > 0 {
		if !settings.NoAddressBook {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
		if !settings.NoMetadata {
			metadata, err = QueryMetadataImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
	}
	return res, book, metadata, nil
}

func (db *DbClient) QueryNFTTransfers(
	transfer_req models.NFTTransferRequest,
	utime_req models.UtimeRequest,
	lt_req models.LtRequest,
	lim_req models.LimitRequest,
	settings models.RequestSettings,
) ([]models.NFTTransfer, models.AddressBook, models.Metadata, error) {
	query, err := buildNFTTransfersQuery(transfer_req, utime_req, lt_req, lim_req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query)
	}
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryNFTTransfersImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	book := models.AddressBook{}
	metadata := models.Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.NftItemAddress))
		addr_list = append(addr_list, string(t.NftCollectionAddress))
		addr_list = append(addr_list, string(t.OldOwner))
		addr_list = append(addr_list, string(t.NewOwner))
		if t.ResponseDestination != nil {
			addr_list = append(addr_list, string(*t.ResponseDestination))
		}
	}
	if len(addr_list) > 0 {
		if !settings.NoAddressBook {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
		if !settings.NoMetadata {
			metadata, err = QueryMetadataImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
	}
	return res, book, metadata, nil
}
