package crud

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"

	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func (db *DbClient) QueryVestingContracts(
	vesting_req VestingContractsRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]VestingInfo, AddressBook, error) {

	if len(vesting_req.ContractAddress) == 0 && len(vesting_req.WalletAddress) == 0 {
		return nil, nil, IndexError{Code: 422, Message: "at least one of contract_address or wallet_address is required"}
	}

	if len(vesting_req.ContractAddress) > 0 && len(vesting_req.WalletAddress) > 0 {
		return nil, nil, IndexError{Code: 422, Message: "only one of contract_address or wallet_address should be specified"}
	}

	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	query, err := buildVestingContractsQuery(vesting_req, lim_req, settings)
	if err != nil {
		return nil, nil, err
	}

	vestings, err := queryVestingContractsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, err
	}

	book := AddressBook{}
	if !settings.NoAddressBook {
		addr_list := []string{}
		for _, v := range vestings {
			if v.Address != nil {
				addr_list = append(addr_list, string(*v.Address))
			}
			if v.SenderAddress != nil {
				addr_list = append(addr_list, string(*v.SenderAddress))
			}
			if v.OwnerAddress != nil {
				addr_list = append(addr_list, string(*v.OwnerAddress))
			}
			for _, addr := range v.Whitelist {
				addr_list = append(addr_list, string(addr))
			}
		}

		if len(addr_list) > 0 {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
	}

	return vestings, book, nil
}

func buildVestingContractsQuery(vesting_req VestingContractsRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	clmn_query := `V.address, V.vesting_start_time, V.vesting_total_duration, V.unlock_period, V.cliff_duration,
		V.vesting_sender_address, V.owner_address, V.vesting_total_amount`

	from_query := `vesting_contracts V`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` ORDER BY V.id ASC`

	if len(vesting_req.ContractAddress) > 0 {
		f_str := filterByArray("V.address", vesting_req.ContractAddress)
		if len(f_str) > 0 {
			filter_list = append(filter_list, f_str)
		}
	}

	if len(vesting_req.WalletAddress) > 0 {
		// Build filter conditions
		wallet_conditions := []string{}

		// Always check owner and sender addresses
		wallet_owner_filter := filterByArray("V.owner_address", vesting_req.WalletAddress)
		wallet_sender_filter := filterByArray("V.vesting_sender_address", vesting_req.WalletAddress)

		if len(wallet_owner_filter) > 0 {
			wallet_conditions = append(wallet_conditions, wallet_owner_filter)
		}
		if len(wallet_sender_filter) > 0 {
			wallet_conditions = append(wallet_conditions, wallet_sender_filter)
		}

		// If check_whitelist is true, also search in whitelist using EXISTS
		if vesting_req.CheckWhitelist != nil && *vesting_req.CheckWhitelist {
			whitelist_filter := filterByArray("W.wallet_address", vesting_req.WalletAddress)
			if len(whitelist_filter) > 0 {
				whitelist_exists := fmt.Sprintf("EXISTS (SELECT 1 FROM vesting_whitelist W WHERE W.vesting_contract_address = V.address AND %s)", whitelist_filter)
				wallet_conditions = append(wallet_conditions, whitelist_exists)
			}
		}

		if len(wallet_conditions) > 0 {
			wallet_filter := "(" + strings.Join(wallet_conditions, " OR ") + ")"
			filter_list = append(filter_list, wallet_filter)
		}
	}

	if len(filter_list) > 0 {
		filter_query = ` WHERE ` + strings.Join(filter_list, " AND ")
	}

	query := `SELECT ` + clmn_query
	query += ` FROM ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query

	return query, nil
}

func queryVestingContractsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]VestingInfo, error) {
	vestings := []VestingInfo{}
	contractAddresses := []string{}

	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()

		for rows.Next() {
			vesting := VestingInfo{}

			err = rows.Scan(
				&vesting.Address,
				&vesting.StartTime,
				&vesting.TotalDuration,
				&vesting.UnlockPeriod,
				&vesting.CliffDuration,
				&vesting.SenderAddress,
				&vesting.OwnerAddress,
				&vesting.TotalAmount,
			)

			if err != nil {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}

			vesting.Whitelist = []AccountAddress{}
			vestings = append(vestings, vesting)

			if vesting.Address != nil {
				contractAddresses = append(contractAddresses, string(*vesting.Address))
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}

	// Fetch whitelist addresses separately if we have contracts
	if len(contractAddresses) > 0 {
		whitelistMap := make(map[string][]AccountAddress)

		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()

		whitelistQuery := fmt.Sprintf(`SELECT vesting_contract_address, wallet_address
			FROM vesting_whitelist
			WHERE vesting_contract_address IN (%s)
			ORDER BY vesting_contract_address, wallet_address`,
			strings.Join(func() []string {
				quoted := make([]string, len(contractAddresses))
				for i, addr := range contractAddresses {
					quoted[i] = fmt.Sprintf("'%s'", addr)
				}
				return quoted
			}(), ","))

		rows, err := conn.Query(ctx, whitelistQuery)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()

		for rows.Next() {
			var contractAddr, walletAddr string
			if err := rows.Scan(&contractAddr, &walletAddr); err != nil {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
			whitelistMap[contractAddr] = append(whitelistMap[contractAddr], AccountAddress(walletAddr))
		}

		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}

		// Assign whitelist addresses to vesting contracts
		for i := range vestings {
			if vestings[i].Address != nil {
				if whitelist, exists := whitelistMap[string(*vestings[i].Address)]; exists {
					vestings[i].Whitelist = whitelist
				}
			}
		}
	}

	return vestings, nil
}
