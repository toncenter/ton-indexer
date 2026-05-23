package crud

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

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

	eventQuery := `
		SELECT tx_hash, tx_lt, tx_now, event_index, event_type, amount::text,
		       balance_delta::text, pending_balance_delta::text, balance_before::text,
		       balance_after::text, pending_balance_before::text, pending_balance_after::text,
		       withdraw_request_before, withdraw_request_after, trace_id
		FROM nominator_pool_events
		WHERE nominator_address = $1 AND pool_address = $2
	`
	eventArgs := []interface{}{nominatorAddr, poolAddr}
	argIdx := 3

	eventQuery, eventArgs, _, err = appendStakingUtimeFilters(eventQuery, eventArgs, argIdx, "tx_now", utimeReq)
	if err != nil {
		return nil, err
	}

	eventQuery += " ORDER BY tx_now ASC, tx_lt ASC, event_index ASC"

	eventRows, err := conn.Query(ctx, eventQuery, eventArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	for eventRows.Next() {
		var movement models.NominatorStakeMovement
		if err := eventRows.Scan(
			&movement.TxHash,
			&movement.TxLt,
			&movement.Utime,
			&movement.EventIndex,
			&movement.Type,
			&movement.Amount,
			&movement.BalanceDelta,
			&movement.PendingBalanceDelta,
			&movement.BalanceBefore,
			&movement.BalanceAfter,
			&movement.PendingBalanceBefore,
			&movement.PendingBalanceAfter,
			&movement.WithdrawRequestBefore,
			&movement.WithdrawRequestAfter,
			&movement.TraceId,
		); err != nil {
			eventRows.Close()
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		movements = append(movements, movement)
	}
	if err := eventRows.Err(); err != nil {
		eventRows.Close()
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	eventRows.Close()

	return movements, nil
}

func (db *DbClient) GetNominatorRewards(
	nominatorAddr string,
	poolAddr string,
	utimeReq models.UtimeRequest,
	settings models.RequestSettings,
) (*models.NominatorRewardsResponse, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancelCtx()

	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	rewardQuery := `
		SELECT tx_hash, tx_lt, tx_now, event_index, amount::text, balance_before::text, trace_id
		FROM nominator_pool_events
		WHERE nominator_address = $1 AND pool_address = $2 AND event_type = 'reward'
	`
	rewardArgs := []interface{}{nominatorAddr, poolAddr}
	argIdx := 3

	rewardQuery, rewardArgs, _, err = appendStakingUtimeFilters(rewardQuery, rewardArgs, argIdx, "tx_now", utimeReq)
	if err != nil {
		return nil, err
	}

	rewardQuery += " ORDER BY tx_now ASC, tx_lt ASC, event_index ASC"

	rewardRows, err := conn.Query(ctx, rewardQuery, rewardArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer rewardRows.Close()

	rewards := []models.NominatorReward{}
	totalOnPeriod := big.NewInt(0)

	for rewardRows.Next() {
		var reward models.NominatorReward
		if err := rewardRows.Scan(
			&reward.TxHash,
			&reward.TxLt,
			&reward.Utime,
			&reward.EventIndex,
			&reward.Reward,
			&reward.StakeBefore,
			&reward.TraceId,
		); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		rewards = append(rewards, reward)
		addNumericString(totalOnPeriod, reward.Reward)
	}
	if err := rewardRows.Err(); err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	return &models.NominatorRewardsResponse{
		TotalOnPeriod: totalOnPeriod.String(),
		Rewards:       rewards,
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

	eventQuery := `
		SELECT nominator_address, tx_hash, tx_lt, tx_now, event_index, event_type, amount::text,
		       balance_delta::text, pending_balance_delta::text, balance_before::text,
		       balance_after::text, pending_balance_before::text, pending_balance_after::text,
		       withdraw_request_before, withdraw_request_after, trace_id
		FROM nominator_pool_events
		WHERE pool_address = $1
	`
	eventArgs := []interface{}{poolAddr}
	argIdx := 2

	eventQuery, eventArgs, _, err = appendStakingUtimeFilters(eventQuery, eventArgs, argIdx, "tx_now", utimeReq)
	if err != nil {
		return nil, err
	}

	eventQuery += " ORDER BY tx_now ASC, tx_lt ASC, event_index ASC"

	eventRows, err := conn.Query(ctx, eventQuery, eventArgs...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	for eventRows.Next() {
		var movement models.PoolStakeMovement
		if err := eventRows.Scan(
			&movement.NominatorAddress,
			&movement.TxHash,
			&movement.TxLt,
			&movement.Utime,
			&movement.EventIndex,
			&movement.Type,
			&movement.Amount,
			&movement.BalanceDelta,
			&movement.PendingBalanceDelta,
			&movement.BalanceBefore,
			&movement.BalanceAfter,
			&movement.PendingBalanceBefore,
			&movement.PendingBalanceAfter,
			&movement.WithdrawRequestBefore,
			&movement.WithdrawRequestAfter,
			&movement.TraceId,
		); err != nil {
			eventRows.Close()
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		movements = append(movements, movement)
	}
	if err := eventRows.Err(); err != nil {
		eventRows.Close()
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	eventRows.Close()

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
