package crud

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

const zeroAmount = "0"

func amountOrZero(amount *string) string {
	if amount == nil {
		return zeroAmount
	}
	return *amount
}

func addNumericString(total *big.Int, amount string) {
	value, ok := new(big.Int).SetString(amount, 10)
	if !ok {
		return
	}
	total.Add(total, value)
}

func appendStakingUtimeFilters(query string, args []interface{}, argIdx int, column string, utimeReq models.UtimeRequest) (string, []interface{}, int, error) {
	if utimeReq.StartUtime != nil && utimeReq.EndUtime != nil && *utimeReq.StartUtime > *utimeReq.EndUtime {
		return query, args, argIdx, models.IndexError{Code: 422, Message: "start_utime must be less than or equal to end_utime"}
	}
	if utimeReq.StartUtime != nil {
		query += fmt.Sprintf(" AND %s >= $%d", column, argIdx)
		args = append(args, int64(*utimeReq.StartUtime))
		argIdx++
	}
	if utimeReq.EndUtime != nil {
		query += fmt.Sprintf(" AND %s <= $%d", column, argIdx)
		args = append(args, int64(*utimeReq.EndUtime))
		argIdx++
	}
	return query, args, argIdx, nil
}

func (db *DbClient) GetNominatorBookings(
	nominatorAddr string,
	poolAddr string,
	utimeReq models.UtimeRequest,
	settings models.RequestSettings,
) ([]models.NominatorBooking, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancelCtx()

	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	bookings := []models.NominatorBooking{}

	incomeQuery := `
		SELECT tx_now, income_amount::text
		FROM nominator_pool_incomes
		WHERE nominator_address = $1 AND pool_address = $2
	`
	incomeArgs := []interface{}{nominatorAddr, poolAddr}
	argIdx := 3

	incomeQuery, incomeArgs, _, err = appendStakingUtimeFilters(incomeQuery, incomeArgs, argIdx, "tx_now", utimeReq)
	if err != nil {
		return nil, err
	}

	incomeQuery += " ORDER BY tx_now ASC"

	incomeRows, err := conn.Query(ctx, incomeQuery, incomeArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	for incomeRows.Next() {
		var utime int32
		var amount string
		if err := incomeRows.Scan(&utime, &amount); err != nil {
			incomeRows.Close()
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		bookings = append(bookings, models.NominatorBooking{
			Utime:       utime,
			BookingType: "nominator_income",
			Debit:       zeroAmount,
			Credit:      amount,
		})
	}
	if err := incomeRows.Err(); err != nil {
		incomeRows.Close()
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	incomeRows.Close()

	actionQuery := `
		SELECT start_utime, type, amount::text
		FROM actions
		WHERE (source = $1 OR destination = $1)
		  AND (source = $2 OR destination = $2)
		  AND type IN ('stake_deposit', 'stake_withdrawal')
	`
	actionArgs := []interface{}{nominatorAddr, poolAddr}
	argIdx = 3

	actionQuery, actionArgs, _, err = appendStakingUtimeFilters(actionQuery, actionArgs, argIdx, "start_utime", utimeReq)
	if err != nil {
		return nil, err
	}

	actionQuery += " ORDER BY start_utime ASC"

	actionRows, err := conn.Query(ctx, actionQuery, actionArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	for actionRows.Next() {
		var utime int32
		var actionType string
		var amount *string
		if err := actionRows.Scan(&utime, &actionType, &amount); err != nil {
			actionRows.Close()
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}

		booking := models.NominatorBooking{
			Utime:  utime,
			Debit:  zeroAmount,
			Credit: zeroAmount,
		}
		switch actionType {
		case "stake_deposit":
			booking.BookingType = "nominator_deposit"
			booking.Credit = amountOrZero(amount)
		case "stake_withdrawal":
			booking.BookingType = "nominator_withdrawal"
			booking.Debit = amountOrZero(amount)
		default:
			continue
		}
		bookings = append(bookings, booking)
	}
	if err := actionRows.Err(); err != nil {
		actionRows.Close()
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	actionRows.Close()

	sort.Slice(bookings, func(i, j int) bool {
		return bookings[i].Utime < bookings[j].Utime
	})

	return bookings, nil
}

func (db *DbClient) GetNominatorEarnings(
	nominatorAddr string,
	poolAddr string,
	utimeReq models.UtimeRequest,
	settings models.RequestSettings,
) (*models.NominatorEarningsResponse, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancelCtx()

	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	incomeQuery := `
		SELECT tx_now, income_amount::text, nominator_balance::text
		FROM nominator_pool_incomes
		WHERE nominator_address = $1 AND pool_address = $2
	`
	incomeArgs := []interface{}{nominatorAddr, poolAddr}
	argIdx := 3

	incomeQuery, incomeArgs, _, err = appendStakingUtimeFilters(incomeQuery, incomeArgs, argIdx, "tx_now", utimeReq)
	if err != nil {
		return nil, err
	}

	incomeQuery += " ORDER BY tx_now ASC"

	incomeRows, err := conn.Query(ctx, incomeQuery, incomeArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer incomeRows.Close()

	earnings := []models.NominatorEarning{}
	totalOnPeriod := big.NewInt(0)

	for incomeRows.Next() {
		var utime int32
		var income string
		var stakeBefore string
		if err := incomeRows.Scan(&utime, &income, &stakeBefore); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		earnings = append(earnings, models.NominatorEarning{
			Utime:       utime,
			Income:      income,
			StakeBefore: stakeBefore,
		})
		addNumericString(totalOnPeriod, income)
	}
	if err := incomeRows.Err(); err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	return &models.NominatorEarningsResponse{
		TotalOnPeriod: totalOnPeriod.String(),
		Earnings:      earnings,
	}, nil
}

func (db *DbClient) GetPoolBookings(
	poolAddr string,
	utimeReq models.UtimeRequest,
	settings models.RequestSettings,
) ([]models.PoolBooking, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancelCtx()

	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	bookings := []models.PoolBooking{}

	incomeQuery := `
		SELECT nominator_address, tx_now, income_amount::text
		FROM nominator_pool_incomes
		WHERE pool_address = $1
	`
	incomeArgs := []interface{}{poolAddr}
	argIdx := 2

	incomeQuery, incomeArgs, _, err = appendStakingUtimeFilters(incomeQuery, incomeArgs, argIdx, "tx_now", utimeReq)
	if err != nil {
		return nil, err
	}

	incomeQuery += " ORDER BY tx_now ASC"

	incomeRows, err := conn.Query(ctx, incomeQuery, incomeArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	for incomeRows.Next() {
		var nominatorAddr string
		var utime int32
		var amount string
		if err := incomeRows.Scan(&nominatorAddr, &utime, &amount); err != nil {
			incomeRows.Close()
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		bookings = append(bookings, models.PoolBooking{
			NominatorAddress: nominatorAddr,
			Utime:            utime,
			BookingType:      "nominator_income",
			Debit:            amount,
			Credit:           zeroAmount,
		})
	}
	if err := incomeRows.Err(); err != nil {
		incomeRows.Close()
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	incomeRows.Close()

	actionQuery := `
		SELECT
			CASE
				WHEN source = $1 THEN destination
				ELSE source
			END as nominator_address,
			start_utime, type, amount::text
		FROM actions
		WHERE (source = $1 OR destination = $1)
		  AND type IN ('stake_deposit', 'stake_withdrawal')
	`
	actionArgs := []interface{}{poolAddr}
	argIdx = 2

	actionQuery, actionArgs, _, err = appendStakingUtimeFilters(actionQuery, actionArgs, argIdx, "start_utime", utimeReq)
	if err != nil {
		return nil, err
	}

	actionQuery += " ORDER BY start_utime ASC"

	actionRows, err := conn.Query(ctx, actionQuery, actionArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	for actionRows.Next() {
		var nominatorAddr string
		var utime int32
		var actionType string
		var amount *string
		if err := actionRows.Scan(&nominatorAddr, &utime, &actionType, &amount); err != nil {
			actionRows.Close()
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}

		booking := models.PoolBooking{
			NominatorAddress: nominatorAddr,
			Utime:            utime,
			Debit:            zeroAmount,
			Credit:           zeroAmount,
		}
		switch actionType {
		case "stake_deposit":
			booking.BookingType = "nominator_deposit"
			booking.Debit = amountOrZero(amount)
		case "stake_withdrawal":
			booking.BookingType = "nominator_withdrawal"
			booking.Credit = amountOrZero(amount)
		default:
			continue
		}
		bookings = append(bookings, booking)
	}
	if err := actionRows.Err(); err != nil {
		actionRows.Close()
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	actionRows.Close()

	sort.Slice(bookings, func(i, j int) bool {
		return bookings[i].Utime < bookings[j].Utime
	})

	return bookings, nil
}

func (db *DbClient) GetNominator(nominatorAddr string, settings models.RequestSettings) ([]models.NominatorInPool, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancelCtx()

	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	contains, err := json.Marshal([]map[string]string{{"address": nominatorAddr}})
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	query := `
		SELECT p.address, nominator->>'balance', nominator->>'pending_balance'
		FROM nominator_pools p
		CROSS JOIN LATERAL jsonb_array_elements(p.active_nominators) nominator
		WHERE p.destroyed = false
		  AND p.active_nominators @> $1::jsonb
		  AND nominator->>'address' = $2
		ORDER BY p.address
	`

	rows, err := conn.Query(ctx, query, string(contains), nominatorAddr)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	result := []models.NominatorInPool{}
	for rows.Next() {
		var item models.NominatorInPool
		if err := rows.Scan(&item.PoolAddress, &item.Balance, &item.PendingBalance); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	return result, nil
}

func (db *DbClient) GetPool(poolAddr string, settings models.RequestSettings) (*models.NominatorPoolInfo, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancelCtx()

	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	query := `
		SELECT stake_amount_sent::text, validator_amount::text, nominators_count, active_nominators
		FROM nominator_pools
		WHERE address = $1 AND destroyed = false
		LIMIT 1
	`

	poolInfo := models.NominatorPoolInfo{}
	var activeNominatorsRaw []byte
	err = conn.QueryRow(ctx, query, poolAddr).Scan(
		&poolInfo.StakeAmountSent,
		&poolInfo.ValidatorAmount,
		&poolInfo.NominatorsCount,
		&activeNominatorsRaw,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, models.IndexError{Code: 404, Message: "Pool not found"}
		}
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	if err := json.Unmarshal(activeNominatorsRaw, &poolInfo.ActiveNominators); err != nil {
		return nil, models.IndexError{Code: 500, Message: fmt.Sprintf("Failed to decode pool nominators: %v", err)}
	}

	inactiveQuery := `
		SELECT DISTINCT nominator_address
		FROM nominator_pool_incomes
		WHERE pool_address = $1
		ORDER BY nominator_address
	`

	rows, err := conn.Query(ctx, inactiveQuery, poolAddr)
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
	if err := rows.Err(); err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	poolInfo.InactiveNominators = inactiveNominators
	return &poolInfo, nil
}
