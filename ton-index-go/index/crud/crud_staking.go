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

func negateNumericString(amount string) string {
	value, ok := new(big.Int).SetString(amount, 10)
	if !ok {
		if amount == zeroAmount || amount == "" {
			return zeroAmount
		}
		return "-" + amount
	}
	if value.Sign() == 0 {
		return zeroAmount
	}
	return value.Neg(value).String()
}

func addNumericString(total *big.Int, amount string) {
	value, ok := new(big.Int).SetString(amount, 10)
	if !ok {
		return
	}
	total.Add(total, value)
}

func hashPtr(value models.HashType) *models.HashType {
	return &value
}

func int64Ptr(value int64) *int64 {
	return &value
}

func movementLt(txLt *int64, startLt *int64) int64 {
	if txLt != nil {
		return *txLt
	}
	if startLt != nil {
		return *startLt
	}
	return 0
}

func hashValue(value *models.HashType) string {
	if value == nil {
		return ""
	}
	return string(*value)
}

type stakeMovementSortKey struct {
	utime            int32
	lt               int64
	traceID          string
	txHash           string
	actionID         string
	movementType     string
	nominatorAddress string
}

func nominatorStakeMovementSortKey(movement models.NominatorStakeMovement) stakeMovementSortKey {
	return stakeMovementSortKey{
		utime:        movement.Utime,
		lt:           movementLt(movement.TxLt, movement.StartLt),
		traceID:      hashValue(movement.TraceId),
		txHash:       hashValue(movement.TxHash),
		actionID:     hashValue(movement.ActionId),
		movementType: movement.Type,
	}
}

func poolStakeMovementSortKey(movement models.PoolStakeMovement) stakeMovementSortKey {
	return stakeMovementSortKey{
		utime:            movement.Utime,
		lt:               movementLt(movement.TxLt, movement.StartLt),
		traceID:          hashValue(movement.TraceId),
		txHash:           hashValue(movement.TxHash),
		actionID:         hashValue(movement.ActionId),
		movementType:     movement.Type,
		nominatorAddress: string(movement.NominatorAddress),
	}
}

func stakeMovementLess(left stakeMovementSortKey, right stakeMovementSortKey) bool {
	if left.utime != right.utime {
		return left.utime < right.utime
	}
	if left.lt != right.lt {
		return left.lt < right.lt
	}
	if left.traceID != right.traceID {
		return left.traceID < right.traceID
	}
	if left.txHash != right.txHash {
		return left.txHash < right.txHash
	}
	if left.actionID != right.actionID {
		return left.actionID < right.actionID
	}
	if left.movementType != right.movementType {
		return left.movementType < right.movementType
	}
	return left.nominatorAddress < right.nominatorAddress
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

func (db *DbClient) GetNominatorStakeMovements(
	nominatorAddr string,
	poolAddr string,
	utimeReq models.UtimeRequest,
	settings models.RequestSettings,
) ([]models.NominatorStakeMovement, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancelCtx()

	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	movements := []models.NominatorStakeMovement{}

	incomeQuery := `
		SELECT tx_hash, tx_lt, tx_now, income_amount::text, trace_id
		FROM nominator_pool_incomes
		WHERE nominator_address = $1 AND pool_address = $2
	`
	incomeArgs := []interface{}{nominatorAddr, poolAddr}
	argIdx := 3

	incomeQuery, incomeArgs, _, err = appendStakingUtimeFilters(incomeQuery, incomeArgs, argIdx, "tx_now", utimeReq)
	if err != nil {
		return nil, err
	}

	incomeQuery += " ORDER BY tx_now ASC, tx_lt ASC, tx_hash ASC"

	incomeRows, err := conn.Query(ctx, incomeQuery, incomeArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	for incomeRows.Next() {
		var txHash models.HashType
		var txLt int64
		var utime int32
		var amount string
		var traceID *models.HashType
		if err := incomeRows.Scan(&txHash, &txLt, &utime, &amount, &traceID); err != nil {
			incomeRows.Close()
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		movements = append(movements, models.NominatorStakeMovement{
			Utime:        utime,
			Type:         "nominator_income",
			Amount:       amount,
			BalanceDelta: amount,
			TxHash:       hashPtr(txHash),
			TxLt:         int64Ptr(txLt),
			TraceId:      traceID,
		})
	}
	if err := incomeRows.Err(); err != nil {
		incomeRows.Close()
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	incomeRows.Close()

	actionQuery := `
		SELECT start_utime, type, amount::text, trace_id, action_id, start_lt
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

	actionQuery += " ORDER BY start_utime ASC, start_lt ASC, action_id ASC"

	actionRows, err := conn.Query(ctx, actionQuery, actionArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	for actionRows.Next() {
		var utime int32
		var actionType string
		var amount *string
		var traceID *models.HashType
		var actionID models.HashType
		var startLt int64
		if err := actionRows.Scan(&utime, &actionType, &amount, &traceID, &actionID, &startLt); err != nil {
			actionRows.Close()
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}

		movement := models.NominatorStakeMovement{
			Utime:    utime,
			Amount:   amountOrZero(amount),
			TraceId:  traceID,
			ActionId: hashPtr(actionID),
			StartLt:  int64Ptr(startLt),
		}
		switch actionType {
		case "stake_deposit":
			movement.Type = "nominator_deposit"
			movement.BalanceDelta = amountOrZero(amount)
		case "stake_withdrawal":
			movement.Type = "nominator_withdrawal"
			movement.BalanceDelta = negateNumericString(amountOrZero(amount))
		default:
			continue
		}
		movements = append(movements, movement)
	}
	if err := actionRows.Err(); err != nil {
		actionRows.Close()
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	actionRows.Close()

	sort.Slice(movements, func(i, j int) bool {
		return stakeMovementLess(
			nominatorStakeMovementSortKey(movements[i]),
			nominatorStakeMovementSortKey(movements[j]),
		)
	})

	return movements, nil
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
		SELECT tx_hash, tx_lt, tx_now, income_amount::text, nominator_balance::text, trace_id
		FROM nominator_pool_incomes
		WHERE nominator_address = $1 AND pool_address = $2
	`
	incomeArgs := []interface{}{nominatorAddr, poolAddr}
	argIdx := 3

	incomeQuery, incomeArgs, _, err = appendStakingUtimeFilters(incomeQuery, incomeArgs, argIdx, "tx_now", utimeReq)
	if err != nil {
		return nil, err
	}

	incomeQuery += " ORDER BY tx_now ASC, tx_lt ASC, tx_hash ASC"

	incomeRows, err := conn.Query(ctx, incomeQuery, incomeArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer incomeRows.Close()

	earnings := []models.NominatorEarning{}
	totalOnPeriod := big.NewInt(0)

	for incomeRows.Next() {
		var txHash models.HashType
		var txLt int64
		var utime int32
		var income string
		var stakeBefore string
		var traceID *models.HashType
		if err := incomeRows.Scan(&txHash, &txLt, &utime, &income, &stakeBefore, &traceID); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		earnings = append(earnings, models.NominatorEarning{
			Utime:       utime,
			Income:      income,
			StakeBefore: stakeBefore,
			TxHash:      txHash,
			TxLt:        txLt,
			TraceId:     traceID,
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

func (db *DbClient) GetPoolStakeMovements(
	poolAddr string,
	utimeReq models.UtimeRequest,
	settings models.RequestSettings,
) ([]models.PoolStakeMovement, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancelCtx()

	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	movements := []models.PoolStakeMovement{}

	incomeQuery := `
		SELECT nominator_address, tx_hash, tx_lt, tx_now, income_amount::text, trace_id
		FROM nominator_pool_incomes
		WHERE pool_address = $1
	`
	incomeArgs := []interface{}{poolAddr}
	argIdx := 2

	incomeQuery, incomeArgs, _, err = appendStakingUtimeFilters(incomeQuery, incomeArgs, argIdx, "tx_now", utimeReq)
	if err != nil {
		return nil, err
	}

	incomeQuery += " ORDER BY tx_now ASC, tx_lt ASC, tx_hash ASC, nominator_address ASC"

	incomeRows, err := conn.Query(ctx, incomeQuery, incomeArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	for incomeRows.Next() {
		var nominatorAddr models.AccountAddress
		var txHash models.HashType
		var txLt int64
		var utime int32
		var amount string
		var traceID *models.HashType
		if err := incomeRows.Scan(&nominatorAddr, &txHash, &txLt, &utime, &amount, &traceID); err != nil {
			incomeRows.Close()
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		movements = append(movements, models.PoolStakeMovement{
			NominatorAddress: nominatorAddr,
			Utime:            utime,
			Type:             "nominator_income",
			Amount:           amount,
			BalanceDelta:     amount,
			TxHash:           hashPtr(txHash),
			TxLt:             int64Ptr(txLt),
			TraceId:          traceID,
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
			start_utime, type, amount::text, trace_id, action_id, start_lt
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

	actionQuery += " ORDER BY start_utime ASC, start_lt ASC, action_id ASC"

	actionRows, err := conn.Query(ctx, actionQuery, actionArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	for actionRows.Next() {
		var nominatorAddr models.AccountAddress
		var utime int32
		var actionType string
		var amount *string
		var traceID *models.HashType
		var actionID models.HashType
		var startLt int64
		if err := actionRows.Scan(&nominatorAddr, &utime, &actionType, &amount, &traceID, &actionID, &startLt); err != nil {
			actionRows.Close()
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}

		movement := models.PoolStakeMovement{
			NominatorAddress: nominatorAddr,
			Utime:            utime,
			Amount:           amountOrZero(amount),
			TraceId:          traceID,
			ActionId:         hashPtr(actionID),
			StartLt:          int64Ptr(startLt),
		}
		switch actionType {
		case "stake_deposit":
			movement.Type = "nominator_deposit"
			movement.BalanceDelta = amountOrZero(amount)
		case "stake_withdrawal":
			movement.Type = "nominator_withdrawal"
			movement.BalanceDelta = negateNumericString(amountOrZero(amount))
		default:
			continue
		}
		movements = append(movements, movement)
	}
	if err := actionRows.Err(); err != nil {
		actionRows.Close()
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	actionRows.Close()

	sort.Slice(movements, func(i, j int) bool {
		return stakeMovementLess(
			poolStakeMovementSortKey(movements[i]),
			poolStakeMovementSortKey(movements[j]),
		)
	})

	return movements, nil
}

func (db *DbClient) GetNominatorPools(nominatorAddr string, settings models.RequestSettings) ([]models.NominatorPoolPosition, error) {
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

	result := []models.NominatorPoolPosition{}
	for rows.Next() {
		var item models.NominatorPoolPosition
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
		SELECT state, stake_amount_sent::text, validator_amount::text, validator_address,
		       validator_reward_share, max_nominators_count, min_validator_stake::text,
		       min_nominator_stake::text, nominators_count, active_nominators
		FROM nominator_pools
		WHERE address = $1 AND destroyed = false
		LIMIT 1
	`

	poolInfo := models.NominatorPoolInfo{}
	var activeNominatorsRaw []byte
	err = conn.QueryRow(ctx, query, poolAddr).Scan(
		&poolInfo.State,
		&poolInfo.StakeAmountSent,
		&poolInfo.ValidatorAmount,
		&poolInfo.ValidatorAddress,
		&poolInfo.ValidatorRewardShare,
		&poolInfo.MaxNominatorsCount,
		&poolInfo.MinValidatorStake,
		&poolInfo.MinNominatorStake,
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
	return &poolInfo, nil
}
