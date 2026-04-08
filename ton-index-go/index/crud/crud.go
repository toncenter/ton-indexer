package crud

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"strings"

	"github.com/lib/pq"
	"github.com/toncenter/ton-indexer/ton-index-go/index/detect"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/services"

	"github.com/jackc/pgx/v5/pgxpool"
)

func getSortOrder(order models.SortType) (string, error) {
	switch strings.ToLower(string(order)) {
	case "desc", "d":
		return "desc", nil
	case "asc", "a":
		return "asc", nil
	}
	return "", models.IndexError{Code: 422, Message: "wrong value for sort parameter"}
}

// query builders
func limitQuery(lim models.LimitParams, settings models.RequestSettings) (string, error) {
	query := ``
	if lim.Limit == nil {
		// set default value
		lim.Limit = new(int32)
		*lim.Limit = int32(settings.DefaultLimit)
	}
	if lim.Limit != nil {
		limit := max(1, *lim.Limit)
		if limit > int32(settings.MaxLimit) {
			return "", models.IndexError{Code: 422, Message: fmt.Sprintf("limit is not allowed: %d > %d", limit, settings.MaxLimit)}
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
	filterStringIfaceType := reflect.TypeOf((*models.FilterStringInterface)(nil)).Elem()
	for _, x := range values {
		v := reflect.ValueOf(x)
		t := reflect.TypeOf(x)
		switch {
		case t.Implements(filterStringIfaceType):
			xx := v.Interface().(models.FilterStringInterface)
			filter_list = append(filter_list, fmt.Sprintf(`'%s'`, xx.FilterString()))
		case t.Kind() == reflect.String:
			if len(t.String()) > 0 {
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

func QueryMetadataImpl(addr_list []models.AccountAddress, conn *pgxpool.Conn, settings models.RequestSettings) (models.Metadata, error) {
	if settings.UseCache {
		metadata, err := services.AddressInfoCacheClient.GetMetadata(addr_list)
		if err != nil {
			log.Println("Error getting metadata from cache: ", err)
		} else {
			return metadata, nil
		}
	}
	token_info_map := map[models.AccountAddress][]models.TokenInfo{}

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
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	defer rows.Close()

	tasks := []models.BackgroundTask{}

	for rows.Next() {
		var row models.TokenInfo
		err := rows.Scan(&row.Address, &row.Valid, &row.Type, &row.Name, &row.Symbol, &row.Description, &row.Image, &row.Extra, &row.NftIndex)
		if err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		if row.Valid == nil {
			data := map[string]interface{}{
				"address": row.Address,
				"type":    row.Type,
			}
			tasks = append(tasks, models.BackgroundTask{Type: "fetch_metadata", Data: data})
			token_info_map[row.Address] = append(token_info_map[row.Address], models.TokenInfo{
				Address:  row.Address,
				Type:     row.Type,
				NftIndex: row.NftIndex,
				Indexed:  false,
			})
		} else {
			row.Indexed = true

			if _, ok := token_info_map[row.Address]; !ok {
				token_info_map[row.Address] = []models.TokenInfo{}
			}
			if *row.Valid {
				token_info_map[row.Address] = append(token_info_map[row.Address], row)
			} else {
				token_info_map[row.Address] = append(token_info_map[row.Address], models.TokenInfo{
					Address:  row.Address,
					Indexed:  true,
					Valid:    row.Valid,
					Type:     row.Type,
					NftIndex: row.NftIndex,
				})
			}
		}
	}

	metadata := models.Metadata{}
	for addr, infos := range token_info_map {
		indexed := true
		for _, info := range infos {
			indexed = indexed && info.Indexed
		}
		metadata[addr] = models.AddressMetadata{
			TokenInfo: infos,
			IsIndexed: indexed,
		}
	}

	backgroundTaskManager := services.GetBackgroundTaskManager()
	if len(tasks) > 0 && backgroundTaskManager != nil {
		backgroundTaskManager.EnqueueTasksIfPossible(tasks)
	}
	return metadata, nil
}

func SubstituteImgproxyBaseUrl(metadata *models.Metadata, base_url string) {
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

func QueryAddressBookImpl(addr_list []models.AccountAddress, conn *pgxpool.Conn, settings models.RequestSettings) (models.AddressBook, error) {
	if settings.UseCache {
		book, err := services.AddressInfoCacheClient.GetAddressBook(addr_list)
		if err != nil {
			log.Println("Error getting address book from cache: ", err)
		} else {
			return book, nil
		}
	}
	book := models.AddressBook{}
	quote_addr_list := []string{}
	for _, item := range addr_list {
		quote_addr_list = append(quote_addr_list, fmt.Sprintf("'%s'", item.FilterString()))
	}

	// read address book first
	book_tmp := models.AddressBook{}
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
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()

		for rows.Next() {
			var account models.AccountAddress
			var code_hash *models.HashType
			var methods *[]uint32
			if err := rows.Scan(&account, &code_hash, &methods); err == nil {
				addr_str := models.GetAccountAddressFriendly(account, code_hash, settings.IsTestnet)

				// detect interfaces
				var interfaces []string
				if code_hash != nil || methods != nil {
					codeHashStr := ""
					if code_hash != nil {
						codeHashStr = code_hash.String()
					}
					var methodIDs []uint32
					if methods != nil {
						methodIDs = *methods
					}
					interfaces = detect.DetectInterface(codeHashStr, methodIDs)
				}

				interfacesPtr := &interfaces
				book_tmp[account] = models.AddressBookRow{
					UserFriendly: &addr_str,
					Domain:       nil,
					Interfaces:   interfacesPtr,
				}
			} else {
				return nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, models.IndexError{Code: 500, Message: rows.Err().Error()}
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
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()

		for rows.Next() {
			var account models.AccountAddress
			var domain *string
			if err := rows.Scan(&account, &domain); err == nil {
				if book_rec, ok := book_tmp[account]; ok {
					book_rec.Domain = domain
					book_tmp[account] = book_rec
				}
			} else {
				return nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, models.IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}

	for _, addr := range addr_list {
		if rec, ok := book_tmp[addr]; ok {
			book[addr] = rec
		} else {
			addr_str := models.GetAccountAddressFriendly(addr, nil, settings.IsTestnet)
			emptyInterfaces := []string{}
			book[addr] = models.AddressBookRow{
				UserFriendly: &addr_str,
				Domain:       nil,
				Interfaces:   &emptyInterfaces,
			}
		}
	}
	return book, nil
}

func (db *DbClient) QueryMetadata(
	address_list []string,
	settings models.RequestSettings,
) (models.Metadata, error) {
	addr_list := []models.AccountAddress{}
	for _, addr := range address_list {
		addr_loc, err := models.ParseAccountAddress(addr)
		if err == nil && addr_loc != nil {
			addr_list = append(addr_list, *addr_loc)
		}
	}
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()
	return QueryMetadataImpl(addr_list, conn, settings)
}

func (db *DbClient) QueryAddressBook(
	address_list []string,
	settings models.RequestSettings,
) (models.GenericAddressBook, error) {
	addr_list := []models.AccountAddress{}
	mapping := map[string]models.AccountAddress{}
	for _, addr := range address_list {
		addr_loc, err := models.ParseAccountAddress(addr)
		if err == nil && addr_loc != nil {
			addr_list = append(addr_list, *addr_loc)
			mapping[addr] = *addr_loc
		} else {
			mapping[addr] = ""
		}
	}
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()
	book, err := QueryAddressBookImpl(addr_list, conn, settings)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	new_addr_book := models.GenericAddressBook{}
	for k, v := range mapping {
		if vv, ok := book[v]; ok {
			new_addr_book[k] = vv
		} else {
			emptyInterfaces := []string{}
			new_addr_book[k] = models.AddressBookRow{
				UserFriendly: nil,
				Domain:       nil,
				Interfaces:   &emptyInterfaces,
			}
		}
	}
	return new_addr_book, nil
}

func (db *DbClient) QueryBalanceChanges(
	req models.BalanceChangesRequest,
	settings models.RequestSettings,
) (models.BalanceChangesResult, error) {
	trace_id := req.TraceId
	if trace_id == nil && req.ActionId == nil {
		return models.BalanceChangesResult{}, models.IndexError{Code: 400, Message: "trace_id or action_id is required"}
	}

	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return models.BalanceChangesResult{}, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()

	if trace_id == nil && req.ActionId != nil {
		query := "SELECT trace_id FROM actions WHERE action_id = $1"
		err := conn.QueryRow(ctx, query, *req.ActionId).Scan(&trace_id)
		if err != nil {
			return models.BalanceChangesResult{}, models.IndexError{Code: 404, Message: "action_id not found"}
		}
	}

	if trace_id == nil {
		return models.BalanceChangesResult{}, models.IndexError{Code: 400, Message: "trace_id is required"}
	}

	trace_changes, actions_changes, err := CalculateBalanceChanges(models.HashType(*trace_id), conn)
	if err != nil {
		return models.BalanceChangesResult{}, models.IndexError{Code: 500, Message: err.Error()}
	}
	var targetChanges *BalanceChanges = trace_changes
	if req.ActionId != nil {
		if v, ok := actions_changes[models.HashType(*req.ActionId)]; ok {
			targetChanges = v
		} else {
			return models.BalanceChangesResult{}, nil
		}
	}
	jetton_changes := make(map[models.AccountAddress]map[models.AccountAddress]string)
	for accountAddress, jettons := range targetChanges.Jettons {
		jetton_changes[accountAddress] = make(map[models.AccountAddress]string)
		for jetton, balance := range jettons {
			jetton_changes[accountAddress][jetton] = balance.String()
		}
	}
	return models.BalanceChangesResult{
		Ton:     targetChanges.get_summarized_balance_changes(),
		Fees:    targetChanges.Fees,
		Jettons: jetton_changes,
	}, nil
}
