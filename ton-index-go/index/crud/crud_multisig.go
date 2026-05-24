package crud

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"
)

func buildMultisigQuery(multisig_req models.MultisigRequest, settings models.RequestSettings) (string, error) {
	lim_req := multisig_req.GetLimitParams()

	conditions := []string{"not m.destroyed"}

	if len(multisig_req.Address) > 0 {
		conditions = append(conditions, filterByArray("m.address", multisig_req.Address))
	}

	if len(multisig_req.WalletAddress) > 0 {
		var walletAddresses []string
		for _, addr := range multisig_req.WalletAddress {
			walletAddresses = append(walletAddresses, addr.FilterString())
		}

		walletAddressesStr := strings.Join(walletAddresses, ",")
		conditions = append(conditions, fmt.Sprintf("(m.signers && ARRAY[%s]::tonaddr[] OR m.proposers && ARRAY[%s]::tonaddr[])",
			walletAddressesStr, walletAddressesStr))
	}

	var whereClause string
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	sort_order := "desc"
	if lim_req.Sort != nil {
		var err error
		sort_order, err = getSortOrder(*lim_req.Sort)
		if err != nil {
			return "", err
		}
	}

	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	query := fmt.Sprintf(`
		SELECT m.address, m.next_order_seqno, m.threshold, m.signers, m.proposers,
		       m.last_transaction_lt, m.code_hash, m.data_hash
		FROM multisig m
		%s
		ORDER BY m.id %s %s`,
		whereClause, sort_order, limit_query)

	return query, nil
}

func buildMultisigOrderQuery(order_req models.MultisigOrderRequest, settings models.RequestSettings) (string, error) {
	lim_req := order_req.GetLimitParams()

	conditions := []string{"not destroyed"}

	if len(order_req.Address) > 0 {
		conditions = append(conditions, filterByArray("address", order_req.Address))
	}

	if len(order_req.MultisigAddress) > 0 {
		var multisigAddresses []string
		for _, addr := range order_req.MultisigAddress {
			multisigAddresses = append(multisigAddresses, addr.FilterString())
		}

		conditions = append(conditions, fmt.Sprintf("multisig_address IN (%s)",
			strings.Join(multisigAddresses, ",")))
	}

	var whereClause string
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	sort_order := "desc"
	if lim_req.Sort != nil {
		var err error
		sort_order, err = getSortOrder(*lim_req.Sort)
		if err != nil {
			return "", err
		}
	}

	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	query := fmt.Sprintf(`
		SELECT address, multisig_address, order_seqno, threshold, sent_for_execution, approvals_mask,
		       approvals_num, expiration_date, order_boc, signers, last_transaction_lt,
		       code_hash, data_hash
		FROM multisig_orders
		%s
		ORDER BY id %s %s`,
		whereClause, sort_order, limit_query)

	return query, nil
}

func queryMultisigImpl(query string, conn *pgxpool.Conn, settings models.RequestSettings) ([]models.Multisig, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	multisigs := []models.Multisig{}
	for rows.Next() {
		multisig, err := parse.ScanMultisig(rows)
		if err != nil {
			return nil, err
		}
		multisigs = append(multisigs, *multisig)
	}

	return multisigs, nil
}

func queryMultisigOrderImpl(query string, conn *pgxpool.Conn, settings models.RequestSettings, args ...any) ([]models.MultisigOrder, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	orders := []models.MultisigOrder{}
	for rows.Next() {
		order, err := parse.ScanMultisigOrder(rows)
		if err != nil {
			return nil, err
		}
		orders = append(orders, *order)
	}

	return orders, nil
}

func (db *DbClient) QueryMultisigs(
	multisig_req models.MultisigRequest,
	settings models.RequestSettings,
) ([]models.Multisig, models.AddressBook, error) {
	if db.Kvrocks != nil {
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		multisigs, err := db.Kvrocks.QueryMultisigs(ctx, multisig_req, settings)
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		addr_set := make(map[models.AccountAddress]bool)
		for _, multisig := range multisigs {
			addr_set[multisig.Address] = true
			for _, signer := range multisig.Signers {
				addr_set[signer] = true
			}
			for _, proposer := range multisig.Proposers {
				addr_set[proposer] = true
			}
			for _, order := range multisig.Orders {
				addr_set[order.Address] = true
				for _, signer := range order.Signers {
					addr_set[signer] = true
				}
			}
		}
		addr_list := make([]models.AccountAddress, 0, len(addr_set))
		for addr := range addr_set {
			addr_list = append(addr_list, addr)
		}
		book, err := db.queryKvrocksAddressBook(addr_list, settings)
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		return multisigs, book, nil
	}

	query, err := buildMultisigQuery(multisig_req, settings)
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	multisigs, err := queryMultisigImpl(query, conn, settings)
	if err != nil {
		log.Println(query)
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	if multisig_req.IncludeOrders == nil || *multisig_req.IncludeOrders {
		// Fetch orders for each multisig
		addresses := make([]models.AccountAddress, len(multisigs))
		for i, multisig := range multisigs {
			addresses[i] = multisig.Address
		}
		ordersQuery := fmt.Sprintf("SELECT " +
			"address, multisig_address, order_seqno, threshold, sent_for_execution, approvals_mask, approvals_num, expiration_date, " +
			"order_boc, signers, last_transaction_lt, code_hash, data_hash " +
			"FROM multisig_orders m " +
			"WHERE multisig_address = ANY($1) AND NOT m.destroyed " +
			"ORDER BY id")
		orders, err := queryMultisigOrderImpl(ordersQuery, conn, settings, addresses)
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		ordersByAddress := make(map[models.AccountAddress][]models.MultisigOrder)
		for _, order := range orders {
			ordersByAddress[order.MultisigAddress] = append(ordersByAddress[order.MultisigAddress], order)
		}
		for i := range multisigs {
			multisigs[i].Orders = ordersByAddress[multisigs[i].Address]
		}
	}

	// Collect addresses for address book
	addr_set := make(map[models.AccountAddress]bool)
	for _, multisig := range multisigs {
		addr_set[multisig.Address] = true
		for _, signer := range multisig.Signers {
			addr_set[signer] = true
		}
		for _, proposer := range multisig.Proposers {
			addr_set[proposer] = true
		}
		for _, order := range multisig.Orders {
			addr_set[order.Address] = true
			for _, signer := range order.Signers {
				addr_set[signer] = true
			}
		}
	}

	book := models.AddressBook{}
	if len(addr_set) > 0 && !settings.NoAddressBook {
		addr_list := make([]models.AccountAddress, 0, len(addr_set))
		for addr := range addr_set {
			addr_list = append(addr_list, addr)
		}
		book, err = QueryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
	}

	return multisigs, book, nil
}

func (db *DbClient) QueryMultisigOrders(
	order_req models.MultisigOrderRequest,
	settings models.RequestSettings,
) ([]models.MultisigOrder, models.AddressBook, error) {
	if db.Kvrocks != nil {
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		orders, err := db.Kvrocks.QueryMultisigOrders(ctx, order_req, settings)
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		if order_req.ParseActions != nil && *order_req.ParseActions {
			for i := range orders {
				if orders[i].OrderBoc != nil {
					orderActions, err := parse.ParseOrder(*orders[i].OrderBoc)
					if err != nil {
						log.Println("Failed to parse multisig order", orders[i].Address, err)
						orders[i].Actions = nil
					}
					orders[i].Actions = orderActions
				}
			}
		}
		addr_set := collectMultisigOrderAddresses(orders)
		addr_list := make([]models.AccountAddress, 0, len(addr_set))
		for addr := range addr_set {
			addr_list = append(addr_list, addr)
		}
		book, err := db.queryKvrocksAddressBook(addr_list, settings)
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		return orders, book, nil
	}

	query, err := buildMultisigOrderQuery(order_req, settings)
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	orders, err := queryMultisigOrderImpl(query, conn, settings)
	if err != nil {
		log.Println(query)
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	if order_req.ParseActions != nil && *order_req.ParseActions {
		for i := range orders {
			if orders[i].OrderBoc != nil {
				orderActions, err := parse.ParseOrder(*orders[i].OrderBoc)
				if err != nil {
					log.Println("Failed to parse multisig order", orders[i].Address, err)
					orders[i].Actions = nil
				}
				orders[i].Actions = orderActions
			}
		}
	}

	addr_set := collectMultisigOrderAddresses(orders)

	book := models.AddressBook{}
	if len(addr_set) > 0 && !settings.NoAddressBook {
		addr_list := make([]models.AccountAddress, 0, len(addr_set))
		for addr := range addr_set {
			addr_list = append(addr_list, addr)
		}
		book, err = QueryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
	}

	return orders, book, nil
}

func collectMultisigOrderAddresses(orders []models.MultisigOrder) map[models.AccountAddress]bool {
	addrSet := make(map[models.AccountAddress]bool)
	addAddr := func(a *models.AccountAddress) {
		if a != nil && len(*a) > 0 {
			addrSet[*a] = true
		}
	}
	for _, order := range orders {
		addrSet[order.Address] = true
		for _, signer := range order.Signers {
			addrSet[signer] = true
		}
		addAddr(&order.MultisigAddress)
		// parse actions bodies
		for _, action := range order.Actions {
			addAddr(action.Destination)
			if action.ParsedBody == nil {
				continue
			}
			body := *action.ParsedBody
			switch b := body.(type) {
			case *models.JettonTransferBody:
				addAddr(b.Destination)
				addAddr(b.Response)
				if b.StonfiSwapBody != nil {
					if jw, ok := b.StonfiSwapBody["jetton_wallet"].(*models.AccountAddress); ok {
						addAddr(jw)
					}
					if ua, ok := b.StonfiSwapBody["user_address"].(*models.AccountAddress); ok {
						addAddr(ua)
					}
				}
			case *models.NFTTransferBody:
				addAddr(b.NewOwner)
				addAddr(b.ResponseDestination)
			case *models.JettonBurnBody:
				addAddr(b.ResponseDestination)
			case *models.JettonMinterBody:
				addAddr(b.ToAddress)
			case *models.JettonChangeAdminBody:
				addAddr(b.NewAdminAddr)
			case *models.JettonCallToBody:
				addAddr(b.ToAddress)
			case *models.SingleNominatorChangeValidatorBody:
				addAddr(b.ValidatorAddress)
			case *models.VestingInternalTransferBody:
				addAddr(b.Destination)
			case *models.MultisigUpdateParamsBody:
				for _, a := range b.NewSigners {
					addAddr(a)
				}
				for _, a := range b.NewProposers {
					addAddr(a)
				}
			}
		}
	}
	return addrSet
}
