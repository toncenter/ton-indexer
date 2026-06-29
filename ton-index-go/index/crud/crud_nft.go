package crud

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/toncenter/ton-indexer/ton-index-go/index/detect"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"
)

func buildNFTCollectionsQuery(req models.NFTCollectionRequest, settings models.RequestSettings) (string, error) {
	lim_req := req.GetLimitParams()
	clmn_query := ` N.address, N.next_item_index, N.owner_address, N.collection_content, 
				    N.data_hash, N.code_hash, N.last_transaction_lt`
	from_query := ` nft_collections as N`
	filter_list := []string{"not N.destroyed"}
	filter_query := ``
	orderby_query := ` order by id asc`
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := req.CollectionAddress; v != nil {
		filter_str := filterByArray("N.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := req.OwnerAddress; v != nil {
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

func buildNFTItemsQuery(req models.NFTItemRequest, settings models.RequestSettings) (string, []any, error) {
	lim_req := req.GetLimitParams()
	args := []any{}
	clmn_query := ` N.address, N.init, N.index, N.collection_address, N.owner_address, N.content, 
                N.last_transaction_lt, N.code_hash, N.data_hash,
                C.address, C.next_item_index, C.owner_address, C.collection_content, 
                C.data_hash, C.code_hash, C.last_transaction_lt,
                S.address, S.nft_owner_address,
                A.address, A.nft_owner`
	from_query := ` nft_items as N 
                left join nft_collections as C on N.collection_address = C.address and not C.destroyed
                left join getgems_nft_sales as S on N.owner_address = S.address and N.address = S.nft_address and not S.destroyed
                left join getgems_nft_auctions as A on N.owner_address = A.address and N.address = A.nft_addr and not A.destroyed`
	filter_list := []string{"not N.destroyed"}
	filter_query := ``
	orderby_query := ` order by N.id asc`
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", nil, err
	}

	if v := req.Address; v != nil {
		filter_str := filterByArray("N.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := req.OwnerAddress; v != nil {
		var filter_str string
		if req.IncludeOnSale != nil && *req.IncludeOnSale {
			filter_str = filterByArray("N.real_owner", v)
		} else {
			filter_str = filterByArray("N.owner_address", v)
		}
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ` order by N.owner_address, N.collection_address, N.index`
	}
	if v := req.CollectionAddress; v != nil {
		if len(req.CollectionAddress) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("N.collection_address = '%s'", v[0].FilterString()))
			orderby_query = ` order by N.collection_address, N.index`
		} else if len(req.CollectionAddress) > 1 {
			filter_str := filterByArray("N.collection_address", v)
			if len(filter_str) > 0 {
				filter_list = append(filter_list, filter_str)
			}
		}
	}
	if v := req.Index; len(v) > 0 {
		if req.CollectionAddress == nil {
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
	if v := req.SortByLastTransactionLt; v != nil && *v {
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

const nftTransfersColumns = ` T.tx_hash, T.tx_lt, T.tx_now, T.tx_aborted, T.query_id,
	T.nft_item_address, T.nft_item_index, T.nft_collection_address, T.old_owner, T.new_owner, T.response_destination, T.custom_payload,
	T.forward_amount, T.forward_payload, T.trace_id`

// nftTransfersParts holds the shared pieces of an nft transfers listing query.
// orderByNow selects the time axis (T.tx_now) over the lt axis (T.tx_lt) for boundary + ordering.
type nftTransfersParts struct {
	fromQuery  string
	filterList []string
	orderBy    string
	orderByNow bool
}

func nftTransfersQueryParts(req models.NFTTransferRequest, sortOrder string) nftTransfersParts {
	utime_req := req.GetUtimeParams()
	lt_req := req.GetLtParams()

	from_query := ` nft_transfers as T`
	filter_list := []string{}

	if v := req.OwnerAddress; v != nil {
		if v1 := req.Direction; v1 != nil {
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
	if v := req.ItemAddress; v != nil {
		filter_str := filterByArray("T.nft_item_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := req.CollectionAddress; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.nft_collection_address = '%s'", v.FilterString()))
	}

	orderByNow := false
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now >= %d", *v))
		orderByNow = true
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now <= %d", *v))
		orderByNow = true
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt <= %d", *v))
	}

	order_col := "T.tx_lt"
	if orderByNow {
		order_col = "T.tx_now"
	}
	orderby_query := fmt.Sprintf(" order by %s %s", order_col, sortOrder)

	return nftTransfersParts{fromQuery: from_query, filterList: filter_list, orderBy: orderby_query, orderByNow: orderByNow}
}

// nftTransfersBoundaryFilters appends this leg's [floor, ceil) boundary on the
// sort axis (T.tx_lt or T.tx_now). lt/now are never null, so no null handling is needed.
func nftTransfersBoundaryFilters(p nftTransfersParts, floor, ceil *uint64) []string {
	col := "T.tx_lt"
	if p.orderByNow {
		col = "T.tx_now"
	}
	filters := append([]string{}, p.filterList...)
	if floor != nil {
		filters = append(filters, fmt.Sprintf("%s >= %d", col, *floor))
	}
	if ceil != nil {
		filters = append(filters, fmt.Sprintf("%s < %d", col, *ceil))
	}
	return filters
}

func buildNFTTransfersOffsetQuery(p nftTransfersParts, floor, ceil *uint64, offset, limit int) string {
	filter_query := ``
	if filters := nftTransfersBoundaryFilters(p, floor, ceil); len(filters) > 0 {
		filter_query = ` where ` + strings.Join(filters, " and ")
	}
	limit_query := fmt.Sprintf(" limit %d offset %d", max(1, limit), max(0, offset))
	return `select` + nftTransfersColumns + ` from` + p.fromQuery + filter_query + p.orderBy + limit_query
}

func buildNFTTransfersCountQuery(p nftTransfersParts, floor, ceil *uint64) string {
	filter_query := ``
	if filters := nftTransfersBoundaryFilters(p, floor, ceil); len(filters) > 0 {
		filter_query = ` where ` + strings.Join(filters, " and ")
	}
	return `select count(*) from` + p.fromQuery + filter_query
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
	settings models.RequestSettings,
) ([]models.NFTCollection, models.AddressBook, models.Metadata, error) {
	if db.Kvrocks != nil {
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		res, err := db.Kvrocks.QueryNFTCollections(ctx, nft_req, settings)
		if err != nil {
			return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		addr_list := []models.AccountAddress{}
		for _, t := range res {
			addr_list = append(addr_list, t.Address)
			if t.OwnerAddress != nil {
				addr_list = append(addr_list, *t.OwnerAddress)
			}
		}
		book, metadata, err := db.queryKvrocksEnrichment(addr_list, settings)
		if err != nil {
			return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		return res, book, metadata, nil
	}

	query, err := buildNFTCollectionsQuery(nft_req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query)
	}
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, releaseConn, err := acquireConnForRequest(db.Pool, settings)
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer releaseConn()

	res, err := queryNFTCollectionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	book := models.AddressBook{}
	metadata := models.Metadata{}
	addr_list := []models.AccountAddress{}
	for _, t := range res {
		addr_list = append(addr_list, t.Address)
		if t.OwnerAddress != nil {
			addr_list = append(addr_list, *t.OwnerAddress)
		}
	}
	if len(addr_list) > 0 {
		if db.Kvrocks != nil {
			releaseConn()
			book, metadata, err = db.queryKvrocksEnrichment(addr_list, settings)
			if err != nil {
				return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		} else {
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
	}
	return res, book, metadata, nil
}

func (db *DbClient) QueryNFTItems(
	nft_req models.NFTItemRequest,
	settings models.RequestSettings,
) ([]models.NFTItem, models.AddressBook, models.Metadata, error) {
	if db.Kvrocks != nil {
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		res, err := db.Kvrocks.QueryNFTItems(ctx, nft_req, settings)
		if err != nil {
			return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		addr_list := []models.AccountAddress{}
		for _, t := range res {
			addr_list = append(addr_list, t.Address)
			if t.CollectionAddress != nil {
				addr_list = append(addr_list, *t.CollectionAddress)
			}
			if t.OwnerAddress != nil {
				addr_list = append(addr_list, *t.OwnerAddress)
			}
			if t.RealOwner != nil {
				addr_list = append(addr_list, *t.RealOwner)
			}
			if t.SaleContractAddress != nil {
				addr_list = append(addr_list, *t.SaleContractAddress)
			}
			if t.AuctionContractAddress != nil {
				addr_list = append(addr_list, *t.AuctionContractAddress)
			}
		}
		book, metadata, err := db.queryKvrocksEnrichment(addr_list, settings)
		if err != nil {
			return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		return res, book, metadata, nil
	}

	query, args, err := buildNFTItemsQuery(nft_req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query)
	}
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, releaseConn, err := acquireConnForRequest(db.Pool, settings)
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer releaseConn()

	res, err := queryNFTItemsWithCollectionsImpl(query, conn, settings, args...)
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	book := models.AddressBook{}
	metadata := models.Metadata{}
	addr_list := []models.AccountAddress{}
	for _, t := range res {
		addr_list = append(addr_list, t.Address)
		if t.CollectionAddress != nil {
			addr_list = append(addr_list, *t.CollectionAddress)
		}
		if t.OwnerAddress != nil {
			addr_list = append(addr_list, *t.OwnerAddress)
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
	req models.NFTTransferRequest,
	settings models.RequestSettings,
) ([]models.NFTTransfer, models.AddressBook, models.Metadata, error) {
	lim_req := req.GetLimitParams()
	sortOrder := "desc"
	if v := lim_req.Sort; v != nil {
		var serr error
		sortOrder, serr = getSortOrder(*v)
		if serr != nil {
			return nil, nil, nil, serr
		}
	}
	limit := int32(settings.DefaultLimit)
	if lim_req.Limit != nil {
		limit = max(1, *lim_req.Limit)
		if limit > int32(settings.MaxLimit) {
			return nil, nil, nil, models.IndexError{Code: 422, Message: fmt.Sprintf("limit is not allowed: %d > %d", limit, settings.MaxLimit)}
		}
	}

	fc, release, err := db.acquireFedForRequest(settings)
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer release()

	parts := nftTransfersQueryParts(req, sortOrder)
	orderKey := "lt"
	if parts.orderByNow {
		orderKey = "utime"
	}
	offset := 0
	if lim_req.Offset != nil {
		offset = int(max(0, *lim_req.Offset))
	}
	// nft transfers are offset-paged (no lt cursor); offset 0 is just the first page.
	res, err := cascadePageOffset(fc, sortOrder, offset, int(limit),
		func(floor, ceil *uint64, off, lim int) (string, error) {
			return buildNFTTransfersOffsetQuery(parts, floor, ceil, off, lim), nil
		},
		func(floor, ceil *uint64) string { return buildNFTTransfersCountQuery(parts, floor, ceil) },
		func(query string, conn *pgxpool.Conn) ([]models.NFTTransfer, error) {
			if settings.DebugRequest {
				log.Println("Debug query:", query)
			}
			return queryNFTTransfersImpl(query, conn, settings)
		},
		func(query string, conn *pgxpool.Conn) (int, error) {
			return queryCount(query, conn, settings)
		},
		orderKey,
	)
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	book := models.AddressBook{}
	metadata := models.Metadata{}
	addr_list := []models.AccountAddress{}
	for _, t := range res {
		addr_list = append(addr_list, t.NftItemAddress)
		addr_list = append(addr_list, t.NftCollectionAddress)
		addr_list = append(addr_list, t.OldOwner)
		addr_list = append(addr_list, t.NewOwner)
		if t.ResponseDestination != nil {
			addr_list = append(addr_list, *t.ResponseDestination)
		}
	}
	if len(addr_list) > 0 {
		if db.Kvrocks != nil {
			release()
			book, metadata, err = db.queryKvrocksEnrichment(addr_list, settings)
			if err != nil {
				return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		} else {
			coldConn, cerr := fc.cold()
			if cerr != nil {
				return nil, nil, nil, models.IndexError{Code: 500, Message: cerr.Error()}
			}
			if !settings.NoAddressBook {
				book, err = QueryAddressBookImpl(addr_list, coldConn, settings)
				if err != nil {
					return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
				}
			}
			if !settings.NoMetadata {
				metadata, err = QueryMetadataImpl(addr_list, coldConn, settings)
				if err != nil {
					return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
				}
			}
		}
	}
	return res, book, metadata, nil
}
