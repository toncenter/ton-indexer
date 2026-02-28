package crud

import (
	"github.com/toncenter/ton-indexer/ton-index-go/index/detect"
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/services"

	"context"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"strings"

	"github.com/lib/pq"

	"github.com/jackc/pgx/v5/pgxpool"
)

func getSortOrder(order SortType) (string, error) {
	switch strings.ToLower(string(order)) {
	case "desc", "d":
		return "desc", nil
	case "asc", "a":
		return "asc", nil
	}
	return "", IndexError{Code: 422, Message: "wrong value for sort parameter"}
}

// query builders
func limitQuery(lim LimitRequest, settings RequestSettings) (string, error) {
	query := ``
	if lim.Limit == nil {
		// set default value
		lim.Limit = new(int32)
		*lim.Limit = int32(settings.DefaultLimit)
	}
	if lim.Limit != nil {
		limit := max(1, *lim.Limit)
		if limit > int32(settings.MaxLimit) {
			return "", IndexError{Code: 422, Message: fmt.Sprintf("limit is not allowed: %d > %d", limit, settings.MaxLimit)}
		}
		query += fmt.Sprintf(" limit %d", limit)
	}
	if lim.Offset != nil {
		offset := max(0, *lim.Offset)
		query += fmt.Sprintf(" offset %d", offset)
	}
	return query, nil
}

func filterByArray[T any](clmn string, values []T) string {
	filter_list := []string{}
	for _, x := range values {
		t := reflect.ValueOf(x)
		switch t.Kind() {
		case reflect.String:
			if t.Len() > 0 {
				filter_list = append(filter_list, fmt.Sprintf("'%s'", t.String()))
			}
		default:
			filter_list = append(filter_list, fmt.Sprintf("'%v'", x))
		}
	}
	if len(filter_list) == 1 {
		return fmt.Sprintf("%s = %s", clmn, filter_list[0])
	}
	if len(filter_list) > 1 {
		vv_str := strings.Join(filter_list, ",")
		return fmt.Sprintf("%s in (%s)", clmn, vv_str)
	}
	return ``
}

func QueryMetadataImpl(addr_list []string, conn *pgxpool.Conn, settings RequestSettings) (Metadata, error) {
	if settings.UseCache {
		metadata, err := services.GetCacheClient().GetMetadata(addr_list)
		if err != nil {
			log.Println("Error getting metadata from cache: ", err)
		} else {
			return metadata, nil
		}
	}
	token_info_map := map[string][]TokenInfo{}

	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()

	// Query jetton wallet information and get additional addresses to scan
	jetton_wallet_infos, additional_addrs, err := queryJettonWalletsTokenInfo(addr_list, conn, ctx)
	if err != nil {
		return nil, err
	}
	for addr, info := range jetton_wallet_infos {
		token_info_map[addr] = append(token_info_map[addr], info)
	}
	if len(additional_addrs) > 0 {
		addr_list = append(addr_list, additional_addrs...)
	}

	query := "select n.address, m.valid, 'nft_items' as type, m.name, m.symbol, m.description, m.image, m.extra, n.index from nft_items n left join address_metadata m on n.address = m.address and m.type = 'nft_items' where n.address = ANY($1)" +
		" union all " +
		"select c.address, m.valid, 'nft_collections' as type, m.name, m.symbol, m.description, m.image, m.extra, null as index from nft_collections c left join address_metadata m on c.address = m.address and m.type = 'nft_collections' where c.address = ANY($1)" +
		" union all " +
		"select j.address, m.valid, 'jetton_masters' as type, m.name, m.symbol, m.description, m.image, m.extra, null as index from jetton_masters j left join address_metadata m on j.address = m.address and m.type = 'jetton_masters'  where j.address = ANY($1)"

	rows, err := conn.Query(ctx, query, pq.Array(addr_list))
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	defer rows.Close()

	tasks := []BackgroundTask{}

	for rows.Next() {
		var row TokenInfo
		err := rows.Scan(&row.Address, &row.Valid, &row.Type, &row.Name, &row.Symbol, &row.Description, &row.Image, &row.Extra, &row.NftIndex)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		if row.Valid == nil {
			data := map[string]interface{}{
				"address": row.Address,
				"type":    row.Type,
			}
			tasks = append(tasks, BackgroundTask{Type: "fetch_metadata", Data: data})
			token_info_map[row.Address] = append(token_info_map[row.Address], TokenInfo{
				Address:  row.Address,
				Type:     row.Type,
				NftIndex: row.NftIndex,
				Indexed:  false,
			})
		} else {
			row.Indexed = true

			if _, ok := token_info_map[*row.Type]; !ok {
				token_info_map[row.Address] = []TokenInfo{}
			}
			if *row.Valid {
				token_info_map[row.Address] = append(token_info_map[row.Address], row)
			} else {
				token_info_map[row.Address] = append(token_info_map[row.Address], TokenInfo{
					Address:  row.Address,
					Indexed:  true,
					Valid:    row.Valid,
					Type:     row.Type,
					NftIndex: row.NftIndex,
				})
			}
		}
	}

	metadata := Metadata{}
	for addr, infos := range token_info_map {
		indexed := true
		for _, info := range infos {
			indexed = indexed && info.Indexed
		}
		metadata[addr] = AddressMetadata{
			TokenInfo: infos,
			IsIndexed: indexed,
		}
	}

	if len(tasks) > 0 && services.GetBackgroundTaskManager() != nil {
		services.GetBackgroundTaskManager().EnqueueTasksIfPossible(tasks)
	}
	return metadata, nil
}

func SubstituteImgproxyBaseUrl(metadata *Metadata, base_url string) {
	proxied_fields := []string{"_image_small", "_image_medium", "_image_big"}

	for _, addr_meta := range *metadata {
		for _, tokenInfo := range addr_meta.TokenInfo {
			if tokenInfo.Image == nil || tokenInfo.Extra == nil {
				continue
			}

			for _, field := range proxied_fields {
				if val, exists := tokenInfo.Extra[field]; exists {
					if img_url, ok := val.(string); ok && img_url != "" {
						if result, err := url.JoinPath(base_url, img_url); err == nil {
							tokenInfo.Extra[field] = result
						} else {
							log.Printf("Error joining imgproxy base URL with image URL: %v", err)
						}
					}
				}
			}
		}
	}
}

func QueryAddressBookImpl(addr_list []string, conn *pgxpool.Conn, settings RequestSettings) (AddressBook, error) {
	if settings.UseCache {
		book, err := services.GetCacheClient().GetAddressBook(addr_list)
		if err != nil {
			log.Println("Error getting address book from cache: ", err)
		} else {
			return book, nil
		}
	}
	book := AddressBook{}
	quote_addr_list := []string{}
	for _, item := range addr_list {
		quote_addr_list = append(quote_addr_list, fmt.Sprintf("'%s'", item))
	}

	// read address book first
	book_tmp := AddressBook{}
	addr_list_str := strings.Join(quote_addr_list, ",")
	{
		query := fmt.Sprintf(`SELECT las.account, las.code_hash, cm.methods
							FROM latest_account_states las
							LEFT JOIN contract_methods cm ON las.code_hash = cm.code_hash
							WHERE las.account IN (%s)`, addr_list_str)
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()

		for rows.Next() {
			var account string
			var code_hash *string
			var methods *[]uint32
			if err := rows.Scan(&account, &code_hash, &methods); err == nil {
				addr_str := GetAccountAddressFriendly(account, code_hash, settings.IsTestnet)

				// detect interfaces
				var interfaces []string
				if code_hash != nil || methods != nil {
					codeHashStr := ""
					if code_hash != nil {
						codeHashStr = *code_hash
					}
					var methodIDs []uint32
					if methods != nil {
						methodIDs = *methods
					}
					interfaces = detect.DetectInterface(codeHashStr, methodIDs)
				}

				interfacesPtr := &interfaces
				book_tmp[strings.Trim(account, " ")] = AddressBookRow{
					UserFriendly: &addr_str,
					Domain:       nil,
					Interfaces:   interfacesPtr,
				}
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}

	// read dns entries
	{
		query := fmt.Sprintf(`select distinct on(nft_item_owner) nft_item_owner, domain from dns_entries
			where nft_item_owner in (%s)
			and nft_item_owner = dns_wallet
			order by nft_item_owner, length(domain)`, addr_list_str)
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()

		for rows.Next() {
			var account string
			var domain *string
			if err := rows.Scan(&account, &domain); err == nil {
				if book_rec, ok := book_tmp[account]; ok {
					book_rec.Domain = domain
					book_tmp[account] = book_rec
				}
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}

	for _, addr := range addr_list {
		account := ``
		if addr_val := AccountAddressConverter(addr); addr_val.IsValid() {
			if addr_str, ok := addr_val.Interface().(AccountAddress); ok {
				account = string(addr_str)
			}
		}
		if rec, ok := book_tmp[account]; ok {
			book[addr] = rec
		} else {
			addr_str := GetAccountAddressFriendly(account, nil, settings.IsTestnet)
			emptyInterfaces := []string{}
			book[addr] = AddressBookRow{
				UserFriendly: &addr_str,
				Domain:       nil,
				Interfaces:   &emptyInterfaces,
			}
		}
	}
	return book, nil
}

func (db *DbClient) QueryMetadata(
	addr_list []string,
	settings RequestSettings,
) (Metadata, error) {
	raw_addr_list := []string{}
	for _, addr := range addr_list {
		addr_loc := AccountAddressConverter(addr)
		if addr_loc.IsValid() {
			if v, ok := addr_loc.Interface().(AccountAddress); ok {
				raw_addr_list = append(raw_addr_list, string(v))
			}
		}
	}
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()
	return QueryMetadataImpl(raw_addr_list, conn, settings)
}

func (db *DbClient) QueryAddressBook(
	addr_list []string,
	settings RequestSettings,
) (AddressBook, error) {
	raw_addr_list := []string{}
	raw_addr_map := map[string]string{}
	for _, addr := range addr_list {
		addr_loc := AccountAddressConverter(addr)
		if addr_loc.IsValid() {
			if v, ok := addr_loc.Interface().(AccountAddress); ok {
				raw_addr_list = append(raw_addr_list, string(v))
				raw_addr_map[addr] = string(v)
			}
		} else {
			raw_addr_map[addr] = ""
		}
	}
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()
	book, err := QueryAddressBookImpl(raw_addr_list, conn, settings)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	new_addr_book := AddressBook{}
	for k, v := range raw_addr_map {
		if vv, ok := book[v]; ok {
			new_addr_book[k] = vv
		} else {
			emptyInterfaces := []string{}
			new_addr_book[k] = AddressBookRow{
				UserFriendly: nil,
				Domain:       nil,
				Interfaces:   &emptyInterfaces,
			}
		}
	}
	return new_addr_book, nil
}
