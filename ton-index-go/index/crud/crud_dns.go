package crud

import (
	"context"

	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

func (db *DbClient) QueryDNSRecords(req models.DNSRecordsRequest, settings models.RequestSettings) ([]models.DNSRecord, models.AddressBook, error) {
	lim_req := req.GetLimitParams()
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

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
        WHERE dns_wallet = $1
		ORDER BY LENGTH(domain), domain ASC ` + limit_query
		queryArg = req.WalletAddress
	} else {
		query += `
        WHERE domain = $1
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
