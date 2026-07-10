package crud

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

// DNS auction cursors are the raw bidder-index member "{auction_end_time zero-padded to 20 digits}:{nft_item_address}"
// in URL-safe base64. Members are ordered by end time and then item address on both backends.
func encodeDNSAuctionCursorMember(member string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(member))
}

func encodeDNSAuctionCursor(endTime int64, item models.AccountAddress) string {
	return encodeDNSAuctionCursorMember(padDecimalString(strconv.FormatInt(endTime, 10), 20) + ":" + string(item))
}

func decodeDNSAuctionCursor(cursor string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", fmt.Errorf("invalid after cursor")
	}
	member := string(raw)
	if len(member) < 22 || member[20] != ':' {
		return "", fmt.Errorf("invalid after cursor")
	}
	for _, c := range member[:20] {
		if c < '0' || c > '9' {
			return "", fmt.Errorf("invalid after cursor")
		}
	}
	if _, err := strconv.ParseInt(member[:20], 10, 64); err != nil {
		return "", fmt.Errorf("invalid after cursor")
	}
	return member, nil
}

func (db *DbClient) QueryDNSRecords(req models.DNSRecordsRequest, settings models.RequestSettings) ([]models.DNSRecord, models.AddressBook, error) {
	lim_req := req.GetLimitParams()
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()

	if db.Kvrocks != nil {
		records, err := db.Kvrocks.QueryDNSRecords(ctx, req, settings)
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		book := models.AddressBook{}
		if !settings.NoAddressBook {
			addr_list := []models.AccountAddress{}
			for _, r := range records {
				addr_list = append(addr_list, r.NftItemAddress)
				if r.NftItemOwner != nil {
					addr_list = append(addr_list, *r.NftItemOwner)
				}
				if r.Wallet != nil {
					addr_list = append(addr_list, *r.Wallet)
				}
			}
			book, err = db.queryKvrocksAddressBook(addr_list, settings)
			if err != nil {
				return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
		return records, book, nil
	}

	conn, releaseConn, err := acquireConnForRequest(db.Pool, settings)
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer releaseConn()

	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	var query string
	var queryArg interface{}
	query = `SELECT nft_item_address, nft_item_owner, domain, dns_next_resolver, dns_wallet, dns_site_adnl, dns_storage_bag_id
        FROM dns_entries`
	if req.WalletAddress != nil {
		query += `
        WHERE dns_wallet = $1 AND NOT destroyed
		ORDER BY LENGTH(domain), domain ASC ` + limit_query
		queryArg = req.WalletAddress
	} else {
		query += `
        WHERE domain = $1 AND NOT destroyed
		ORDER BY LENGTH(domain), domain ASC ` + limit_query
		queryArg = req.Domain
	}

	rows, err := conn.Query(ctx, query, queryArg)
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	records := []models.DNSRecord{}
	for rows.Next() {
		var record models.DNSRecord
		if err := rows.Scan(&record.NftItemAddress, &record.NftItemOwner, &record.Domain,
			&record.NextResolver, &record.Wallet, &record.SiteAdnl, &record.StorageBagID); err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		records = append(records, record)
	}
	book := models.AddressBook{}
	if !settings.NoAddressBook {
		addr_list := []models.AccountAddress{}
		for _, r := range records {
			addr_list = append(addr_list, r.NftItemAddress)
			if r.NftItemOwner != nil {
				addr_list = append(addr_list, *r.NftItemOwner)
			}
			if r.Wallet != nil {
				addr_list = append(addr_list, *r.Wallet)
			}
		}
		if len(addr_list) > 0 {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
	}

	return records, book, nil
}

// Drop stale index hits whose payload no longer contains an auction.
func dnsRecordsToAuctions(records []models.DNSAuctionRecord, now int64) []models.DNSAuction {
	auctions := make([]models.DNSAuction, 0, len(records))
	for _, r := range records {
		if r.MaxBidAddress == nil || r.AuctionEndTime == nil {
			continue
		}
		auctions = append(auctions, models.DNSAuction{
			NftItemAddress: r.NftItemAddress,
			Domain:         r.Domain,
			MaxBidAddress:  r.MaxBidAddress,
			MaxBidAmount:   r.MaxBidAmount,
			AuctionEndTime: r.AuctionEndTime,
			LastFillUpTime: r.LastFillUpTime,
			Finished:       *r.AuctionEndTime <= now,
		})
	}
	return auctions
}

func dnsAuctionAddressList(auctions []models.DNSAuction) []models.AccountAddress {
	addr_list := []models.AccountAddress{}
	for _, a := range auctions {
		addr_list = append(addr_list, a.NftItemAddress)
		if a.MaxBidAddress != nil {
			addr_list = append(addr_list, *a.MaxBidAddress)
		}
	}
	return addr_list
}

// Include each item's metadata and its collection when available.
func dnsAuctionMetadataAddressList(auctions []models.DNSAuction) []models.AccountAddress {
	addr_list := []models.AccountAddress{}
	for _, a := range auctions {
		addr_list = append(addr_list, a.NftItemAddress)
		if a.NftItem != nil && a.NftItem.CollectionAddress != nil {
			addr_list = append(addr_list, *a.NftItem.CollectionAddress)
		}
	}
	return addr_list
}

func attachAuctionNFTItemsPg(auctions []models.DNSAuction, conn *pgxpool.Conn, settings models.RequestSettings) error {
	addresses := []models.AccountAddress{}
	seen := map[models.AccountAddress]struct{}{}
	for _, a := range auctions {
		if _, ok := seen[a.NftItemAddress]; ok {
			continue
		}
		seen[a.NftItemAddress] = struct{}{}
		addresses = append(addresses, a.NftItemAddress)
	}
	if len(addresses) == 0 {
		return nil
	}

	item_req := models.NFTItemRequest{Address: addresses}
	limit := int32(len(addresses))
	item_req.Limit = &limit
	query, args, err := buildNFTItemsQuery(item_req, settings)
	if err != nil {
		return models.IndexError{Code: 500, Message: err.Error()}
	}
	items, err := queryNFTItemsWithCollectionsImpl(query, conn, settings, args...)
	if err != nil {
		return err
	}
	byAddr := make(map[models.AccountAddress]*models.NFTItem, len(items))
	for i := range items {
		byAddr[items[i].Address] = &items[i]
	}
	for i := range auctions {
		if item, ok := byAddr[auctions[i].NftItemAddress]; ok {
			auctions[i].NftItem = item
		}
	}
	return nil
}

func (db *DbClient) QueryDNSAuctions(req models.DNSAuctionsRequest, settings models.RequestSettings) ([]models.DNSAuction, models.AddressBook, models.Metadata, *string, error) {
	lim_req := req.GetLimitParams()
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()

	// Validate the cursor up front so a malformed value is a 422 on both backends.
	if req.After != nil && *req.After != "" {
		if _, err := decodeDNSAuctionCursor(*req.After); err != nil {
			return nil, nil, nil, nil, models.IndexError{Code: 422, Message: err.Error()}
		}
	}

	if db.Kvrocks != nil {
		auctions, next_cursor, err := db.Kvrocks.QueryDNSAuctions(ctx, req, settings)
		if err != nil {
			return nil, nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		book := models.AddressBook{}
		if !settings.NoAddressBook {
			book, err = db.queryKvrocksAddressBook(dnsAuctionAddressList(auctions), settings)
			if err != nil {
				return nil, nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
		metadata := models.Metadata{}
		if !settings.NoMetadata {
			metadata, err = QueryMetadataImplKvrocks(dnsAuctionMetadataAddressList(auctions), settings, db.Kvrocks)
			if err != nil {
				return nil, nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
		return auctions, book, metadata, next_cursor, nil
	}

	conn, releaseConn, err := acquireConnForRequest(db.Pool, settings)
	if err != nil {
		return nil, nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer releaseConn()

	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return nil, nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	limit := int32(settings.DefaultLimit)
	if lim_req.Limit != nil {
		limit = *lim_req.Limit
	}

	now := time.Now().Unix()
	query := `SELECT nft_item_address, domain, max_bid_address, max_bid_amount::text, auction_end_time, last_fill_up_time
        FROM dns_entries
        WHERE max_bid_address = $1 AND auction_end_time IS NOT NULL AND NOT destroyed`
	args := []interface{}{req.Bidder}
	switch req.State {
	case "won":
		query += ` AND auction_end_time <= $2`
		args = append(args, now)
	case "bidding":
		query += ` AND auction_end_time > $2`
		args = append(args, now)
	}
	if req.After != nil && *req.After != "" {
		member, err := decodeDNSAuctionCursor(*req.After)
		if err != nil {
			return nil, nil, nil, nil, models.IndexError{Code: 422, Message: err.Error()}
		}
		cursorEndTime, err := strconv.ParseInt(member[:20], 10, 64)
		if err != nil {
			return nil, nil, nil, nil, models.IndexError{Code: 422, Message: "invalid after cursor"}
		}
		query += fmt.Sprintf(` AND (auction_end_time, nft_item_address::text) > ($%d, $%d)`, len(args)+1, len(args)+2)
		args = append(args, cursorEndTime, member[21:])
	}
	query += `
		ORDER BY auction_end_time ASC, nft_item_address ASC ` + limit_query

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	records := []models.DNSAuctionRecord{}
	for rows.Next() {
		var record models.DNSAuctionRecord
		if err := rows.Scan(&record.NftItemAddress, &record.Domain, &record.MaxBidAddress,
			&record.MaxBidAmount, &record.AuctionEndTime, &record.LastFillUpTime); err != nil {
			return nil, nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		records = append(records, record)
	}
	auctions := dnsRecordsToAuctions(records, now)

	// Cursor advances over the raw fetched page (pre settle-race filtering) so dropped rows can't stall pagination.
	var next_cursor *string
	if int32(len(records)) == limit && len(records) > 0 {
		last := records[len(records)-1]
		if last.AuctionEndTime != nil {
			cursor := encodeDNSAuctionCursor(*last.AuctionEndTime, last.NftItemAddress)
			next_cursor = &cursor
		}
	}

	if req.IncludeNftItems != nil && *req.IncludeNftItems {
		if err := attachAuctionNFTItemsPg(auctions, conn, settings); err != nil {
			return nil, nil, nil, nil, err
		}
	}

	book := models.AddressBook{}
	if !settings.NoAddressBook {
		addr_list := dnsAuctionAddressList(auctions)
		if len(addr_list) > 0 {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
	}

	metadata := models.Metadata{}
	if !settings.NoMetadata {
		meta_addr_list := dnsAuctionMetadataAddressList(auctions)
		if len(meta_addr_list) > 0 {
			metadata, err = QueryMetadataImpl(meta_addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
	}

	return auctions, book, metadata, next_cursor, nil
}
