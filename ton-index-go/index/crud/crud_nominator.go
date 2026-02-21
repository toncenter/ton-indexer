package crud

import (
	"context"
	"fmt"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"
	"sort"
)

func (db *DbClient) GetNominatorBookings(
	nominatorAddr string,
	poolAddr string,
	fromTime, toTime *int32,
	limit int,
) ([]models.NominatorBooking, error) {
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	var bookings []models.NominatorBooking

	// query incomes
	incomeQuery := `
		SELECT tx_now, income_amount
		FROM nominator_pool_incomes
		WHERE nominator_address = $1 AND pool_address = $2
	`
	var incomeArgs []interface{}
	incomeArgs = append(incomeArgs, nominatorAddr, poolAddr)
	argIdx := 3

	if fromTime != nil {
		incomeQuery += fmt.Sprintf(" AND tx_now >= $%d", argIdx)
		incomeArgs = append(incomeArgs, *fromTime)
		argIdx++
	}
	if toTime != nil {
		incomeQuery += fmt.Sprintf(" AND tx_now <= $%d", argIdx)
		incomeArgs = append(incomeArgs, *toTime)
		argIdx++
	}

	incomeQuery += " ORDER BY tx_now ASC"

	if limit > 0 {
		incomeQuery += fmt.Sprintf(" LIMIT %d", limit)
	}

	incomeRows, err := conn.Query(context.Background(), incomeQuery, incomeArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer incomeRows.Close()

	for incomeRows.Next() {
		var utime int32
		var amount int64
		if err := incomeRows.Scan(&utime, &amount); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		bookings = append(bookings, models.NominatorBooking{
			Utime:       utime,
			BookingType: "nominator_income",
			Debit:       0,
			Credit:      amount,
		})
	}

	// query actions (deposits, withdrawals)
	actionQuery := `
		SELECT start_utime, type, amount
		FROM actions
		WHERE (source = $1 OR destination = $1)
		  AND (source = $2 OR destination = $2)
		  AND type IN ('stake_deposit', 'stake_withdrawal')
	`
	var actionArgs []interface{}
	actionArgs = append(actionArgs, nominatorAddr, poolAddr)
	argIdx = 3

	if fromTime != nil {
		actionQuery += fmt.Sprintf(" AND start_utime >= $%d", argIdx)
		actionArgs = append(actionArgs, *fromTime)
		argIdx++
	}
	if toTime != nil {
		actionQuery += fmt.Sprintf(" AND start_utime <= $%d", argIdx)
		actionArgs = append(actionArgs, *toTime)
		argIdx++
	}

	actionQuery += " ORDER BY start_utime ASC"

	if limit > 0 {
		actionQuery += fmt.Sprintf(" LIMIT %d", limit)
	}

	actionRows, err := conn.Query(context.Background(), actionQuery, actionArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer actionRows.Close()

	for actionRows.Next() {
		var utime int32
		var actionType string
		var amount *int64
		if err := actionRows.Scan(&utime, &actionType, &amount); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}

		var debit, credit int64
		var bookingType string
		if actionType == "stake_deposit" {
			bookingType = "nominator_deposit"
			debit = 0
			if amount != nil {
				credit = *amount - 1000000000 // subtract 1 TON
			} else {
				credit = 0
			}
		} else { // withdrawal
			bookingType = "nominator_withdrawal"
			if amount != nil {
				debit = *amount
			} else {
				debit = 0
			}
			credit = 0
		}

		bookings = append(bookings, models.NominatorBooking{
			Utime:       utime,
			BookingType: bookingType,
			Debit:       debit,
			Credit:      credit,
		})

		// add forward fee booking for withdrawals
		if actionType == "stake_withdrawal" {
			bookings = append(bookings, models.NominatorBooking{
				Utime:       utime,
				BookingType: "nominator_withdrawal_fwd_fee",
				Debit:       10000000,
				Credit:      0,
			})
		}
	}

	// sort by utime asc
	sort.Slice(bookings, func(i, j int) bool {
		return bookings[i].Utime < bookings[j].Utime
	})

	// apply limit to combined results
	if limit > 0 && len(bookings) > limit {
		bookings = bookings[:limit]
	}

	return bookings, nil
}

func (db *DbClient) GetNominatorEarnings(
	nominatorAddr string,
	poolAddr string,
	fromTime, toTime *int32,
	limit int,
) (*models.NominatorEarningsResponse, error) {
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	// query incomes only
	incomeQuery := `
		SELECT tx_now, income_amount, nominator_balance
		FROM nominator_pool_incomes
		WHERE nominator_address = $1 AND pool_address = $2
	`
	var incomeArgs []interface{}
	incomeArgs = append(incomeArgs, nominatorAddr, poolAddr)
	argIdx := 3

	if fromTime != nil {
		incomeQuery += fmt.Sprintf(" AND tx_now >= $%d", argIdx)
		incomeArgs = append(incomeArgs, *fromTime)
		argIdx++
	}
	if toTime != nil {
		incomeQuery += fmt.Sprintf(" AND tx_now <= $%d", argIdx)
		incomeArgs = append(incomeArgs, *toTime)
		argIdx++
	}

	incomeQuery += " ORDER BY tx_now ASC"

	if limit > 0 {
		incomeQuery += fmt.Sprintf(" LIMIT %d", limit)
	}

	incomeRows, err := conn.Query(context.Background(), incomeQuery, incomeArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer incomeRows.Close()

	earnings := []models.NominatorEarning{}
	var totalOnPeriod int64 = 0

	for incomeRows.Next() {
		var utime int32
		var income int64
		var stakeBefore int64
		if err := incomeRows.Scan(&utime, &income, &stakeBefore); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		earnings = append(earnings, models.NominatorEarning{
			Utime:       utime,
			Income:      income,
			StakeBefore: stakeBefore,
		})
		totalOnPeriod += income
	}

	return &models.NominatorEarningsResponse{
		TotalOnPeriod: totalOnPeriod,
		Earnings:      earnings,
	}, nil
}

func (db *DbClient) GetPoolBookings(
	poolAddr string,
	fromTime, toTime *int32,
	limit int,
) ([]models.PoolBooking, error) {
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	var bookings []models.PoolBooking

	// query incomes
	incomeQuery := `
		SELECT nominator_address, tx_now, income_amount
		FROM nominator_pool_incomes
		WHERE pool_address = $1
	`
	var incomeArgs []interface{}
	incomeArgs = append(incomeArgs, poolAddr)
	argIdx := 2

	if fromTime != nil {
		incomeQuery += fmt.Sprintf(" AND tx_now >= $%d", argIdx)
		incomeArgs = append(incomeArgs, *fromTime)
		argIdx++
	}
	if toTime != nil {
		incomeQuery += fmt.Sprintf(" AND tx_now <= $%d", argIdx)
		incomeArgs = append(incomeArgs, *toTime)
		argIdx++
	}

	incomeQuery += " ORDER BY tx_now ASC"

	if limit > 0 {
		incomeQuery += fmt.Sprintf(" LIMIT %d", limit)
	}

	incomeRows, err := conn.Query(context.Background(), incomeQuery, incomeArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer incomeRows.Close()

	for incomeRows.Next() {
		var nominatorAddr string
		var utime int32
		var amount int64
		if err := incomeRows.Scan(&nominatorAddr, &utime, &amount); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		bookings = append(bookings, models.PoolBooking{
			NominatorAddress: nominatorAddr,
			Utime:            utime,
			BookingType:      "nominator_income",
			Debit:            amount,
			Credit:           0,
		})
	}

	// query actions (deposits, withdrawals) - pool as destination or source
	actionQuery := `
		SELECT 
			CASE 
				WHEN source = $1 THEN destination
				ELSE source
			END as nominator_address,
			start_utime, type, amount
		FROM actions
		WHERE (source = $1 OR destination = $1)
		  AND type IN ('stake_deposit', 'stake_withdrawal')
	`
	var actionArgs []interface{}
	actionArgs = append(actionArgs, poolAddr)
	argIdx = 2

	if fromTime != nil {
		actionQuery += fmt.Sprintf(" AND start_utime >= $%d", argIdx)
		actionArgs = append(actionArgs, *fromTime)
		argIdx++
	}
	if toTime != nil {
		actionQuery += fmt.Sprintf(" AND start_utime <= $%d", argIdx)
		actionArgs = append(actionArgs, *toTime)
		argIdx++
	}

	actionQuery += " ORDER BY start_utime ASC"

	if limit > 0 {
		actionQuery += fmt.Sprintf(" LIMIT %d", limit)
	}

	actionRows, err := conn.Query(context.Background(), actionQuery, actionArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer actionRows.Close()

	for actionRows.Next() {
		var nominatorAddr string
		var utime int32
		var actionType string
		var amount *int64
		if err := actionRows.Scan(&nominatorAddr, &utime, &actionType, &amount); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}

		var debit, credit int64
		var bookingType string
		if actionType == "stake_deposit" {
			bookingType = "nominator_deposit"
			credit = 0
			if amount != nil {
				debit = *amount - 1000000000 // subtract 1 TON
			} else {
				debit = 0
			}
		} else { // withdrawal
			bookingType = "nominator_withdrawal"
			if amount != nil {
				credit = *amount
			} else {
				credit = 0
			}
			debit = 0
		}

		bookings = append(bookings, models.PoolBooking{
			NominatorAddress: nominatorAddr,
			Utime:            utime,
			BookingType:      bookingType,
			Debit:            debit,
			Credit:           credit,
		})

		// add forward fee booking for withdrawals
		if actionType == "stake_withdrawal" {
			bookings = append(bookings, models.PoolBooking{
				NominatorAddress: nominatorAddr,
				Utime:            utime,
				BookingType:      "nominator_withdrawal_fwd_fee",
				Debit:            0,
				Credit:           10000000,
			})
		}
	}

	// sort by utime asc
	sort.Slice(bookings, func(i, j int) bool {
		return bookings[i].Utime < bookings[j].Utime
	})

	// apply limit to combined results
	if limit > 0 && len(bookings) > limit {
		bookings = bookings[:limit]
	}

	return bookings, nil
}

func (db *DbClient) GetNominator(nominatorAddr string) ([]models.NominatorInPool, error) {
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	// get pools from incomes in last 24 hours
	poolsQuery := `
		SELECT DISTINCT pool_address
		FROM nominator_pool_incomes
		WHERE nominator_address = $1
		  AND tx_now > extract(epoch from now() - interval '24 hours')
		ORDER BY pool_address
	`

	poolRows, err := conn.Query(context.Background(), poolsQuery, nominatorAddr)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer poolRows.Close()

	var poolAddresses []string
	for poolRows.Next() {
		var poolAddr string
		if err := poolRows.Scan(&poolAddr); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		poolAddresses = append(poolAddresses, poolAddr)
	}

	if len(poolAddresses) == 0 {
		return []models.NominatorInPool{}, nil
	}

	// get latest account states for all pools
	statesQuery := `
		SELECT account, code_hash, data_boc
		FROM latest_account_states
		WHERE account = ANY($1)
	`

	stateRows, err := conn.Query(context.Background(), statesQuery, poolAddresses)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer stateRows.Close()

	expectedCodeHash := "mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8="
	result := []models.NominatorInPool{}

	for stateRows.Next() {
		var poolAddr string
		var codeHash string
		var dataBoc []byte

		if err := stateRows.Scan(&poolAddr, &codeHash, &dataBoc); err != nil {
			continue
		}

		// verify code hash
		if codeHash != expectedCodeHash {
			continue
		}

		// parse pool data
		poolInfo, err := parse.ParseNominatorPoolData(dataBoc)
		if err != nil {
			continue
		}

		// find nominator in active nominators
		for _, nominator := range poolInfo.ActiveNominators {
			if nominator.Address == nominatorAddr {
				result = append(result, models.NominatorInPool{
					PoolAddress:    poolAddr,
					Balance:        nominator.Balance,
					PendingBalance: nominator.PendingBalance,
				})
				break
			}
		}
	}

	return result, nil
}

func (db *DbClient) GetPool(poolAddr string) (*models.NominatorPoolInfo, error) {
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	// get latest account state
	query := `
		SELECT code_hash, data_boc
		FROM latest_account_states
		WHERE account = $1
		LIMIT 1
	`

	var codeHash string
	var dataBoc []byte
	err = conn.QueryRow(context.Background(), query, poolAddr).Scan(&codeHash, &dataBoc)
	if err != nil {
		return nil, models.IndexError{Code: 404, Message: "Pool not found"}
	}

	// verify code hash matches nominator pool
	expectedCodeHash := "mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8="
	if codeHash != expectedCodeHash {
		return nil, models.IndexError{Code: 400, Message: fmt.Sprintf("Account is not a nominator pool, code hash is %s", codeHash)}
	}

	// parse pool data
	poolInfo, err := parse.ParseNominatorPoolData(dataBoc)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: fmt.Sprintf("Failed to parse pool data: %v", err)}
	}

	// get inactive nominators from incomes table
	inactiveQuery := `
		SELECT DISTINCT nominator_address
		FROM nominator_pool_incomes
		WHERE pool_address = $1
		ORDER BY nominator_address
	`

	rows, err := conn.Query(context.Background(), inactiveQuery, poolAddr)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	activeAddresses := make(map[string]bool)
	for _, nominator := range poolInfo.ActiveNominators {
		activeAddresses[nominator.Address] = true
	}

	inactiveNominators := []string{}
	for rows.Next() {
		var nominatorAddr string
		if err := rows.Scan(&nominatorAddr); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		if !activeAddresses[nominatorAddr] {
			inactiveNominators = append(inactiveNominators, nominatorAddr)
		}
	}

	poolInfo.InactiveNominators = inactiveNominators

	return poolInfo, nil
}
