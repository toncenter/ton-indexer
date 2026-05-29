package crud

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

func (db *DbClient) QueryValidatorEvents(
	req models.ValidatorEventsRequest,
	utimeReq models.UtimeParams,
	settings models.RequestSettings,
) ([]models.ValidatorEvent, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancelCtx()

	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	query := `
		SELECT tx_hash, tx_lt, tx_now, mc_seqno, trace_id, event_index, event_type,
		       stake_holder_address, validator_pubkey, adnl_addr, query_id::text,
		       amount::text, reason, metadata
		FROM validator_events
		WHERE true
	`
	args := []interface{}{}
	argIdx := 1

	if req.StakeHolderAddress != nil {
		query += fmt.Sprintf(" AND stake_holder_address = $%d", argIdx)
		args = append(args, string(*req.StakeHolderAddress))
		argIdx++
	}
	if req.ValidatorPubkey != nil {
		query += fmt.Sprintf(" AND validator_pubkey = $%d", argIdx)
		args = append(args, strings.ToUpper(*req.ValidatorPubkey))
		argIdx++
	}
	if req.EventType != nil {
		query += fmt.Sprintf(" AND event_type = $%d", argIdx)
		args = append(args, *req.EventType)
		argIdx++
	}

	query, args, _, err = appendStakingUtimeFilters(query, args, argIdx, "tx_now", utimeReq)
	if err != nil {
		return nil, err
	}
	query += " ORDER BY tx_now DESC, tx_lt DESC, event_index DESC"
	limit, err := limitQuery(req.LimitParams, settings)
	if err != nil {
		return nil, err
	}
	query += limit

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	events := []models.ValidatorEvent{}
	for rows.Next() {
		var event models.ValidatorEvent
		var metadataRaw []byte
		if err := rows.Scan(
			&event.TxHash,
			&event.TxLt,
			&event.Utime,
			&event.McSeqno,
			&event.TraceId,
			&event.EventIndex,
			&event.Type,
			&event.StakeHolderAddress,
			&event.ValidatorPubkey,
			&event.AdnlAddr,
			&event.QueryId,
			&event.Amount,
			&event.Reason,
			&metadataRaw,
		); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		if len(metadataRaw) == 0 {
			metadataRaw = []byte("{}")
		}
		event.Metadata = json.RawMessage(metadataRaw)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	return events, nil
}

func (db *DbClient) QueryValidatorElections(
	req models.ValidatorElectionsRequest,
	settings models.RequestSettings,
) ([]models.ValidatorElection, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancelCtx()

	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	query := `
		SELECT election_id, elect_close, min_stake::text, total_stake::text,
		       failed, finished, source_mc_seqno
		FROM validator_elections
		WHERE true
	`
	args := []interface{}{}
	argIdx := 1
	if req.ElectionId != nil {
		query += fmt.Sprintf(" AND election_id = $%d", argIdx)
		args = append(args, *req.ElectionId)
		argIdx++
	}
	if req.Finished != nil {
		query += fmt.Sprintf(" AND finished = $%d", argIdx)
		args = append(args, *req.Finished)
		argIdx++
	}
	participantFilters := []string{}
	if req.StakeHolderAddress != nil {
		participantFilters = append(participantFilters, fmt.Sprintf(" AND p.stake_holder_address = $%d", argIdx))
		args = append(args, string(*req.StakeHolderAddress))
		argIdx++
	}
	if req.AdnlAddress != nil {
		participantFilters = append(participantFilters, fmt.Sprintf(" AND p.adnl_addr = $%d", argIdx))
		args = append(args, strings.ToUpper(*req.AdnlAddress))
		argIdx++
	}
	if req.ValidatorPubkey != nil {
		participantFilters = append(participantFilters, fmt.Sprintf(" AND p.validator_pubkey = $%d", argIdx))
		args = append(args, strings.ToUpper(*req.ValidatorPubkey))
		argIdx++
	}
	if len(participantFilters) > 0 {
		query += fmt.Sprintf(` AND EXISTS (
			SELECT 1 FROM validator_election_participants p
			WHERE p.election_id = validator_elections.election_id%s
		)`, strings.Join(participantFilters, ""))
	}
	query += " ORDER BY election_id DESC"
	limit, err := limitQuery(req.LimitParams, settings)
	if err != nil {
		return nil, err
	}
	query += limit

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	elections := []models.ValidatorElection{}
	for rows.Next() {
		var election models.ValidatorElection
		if err := rows.Scan(
			&election.ElectionId,
			&election.ElectClose,
			&election.MinStake,
			&election.TotalStake,
			&election.Failed,
			&election.Finished,
			&election.SourceMcSeqno,
		); err != nil {
			rows.Close()
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		elections = append(elections, election)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	rows.Close()

	includeParticipants := req.ReturnParticipants != nil && *req.ReturnParticipants
	if includeParticipants {
		for i := range elections {
			participants, err := db.queryValidatorElectionParticipants(ctx, conn, elections[i].ElectionId)
			if err != nil {
				return nil, err
			}
			elections[i].Participants = participants
		}
	}
	return elections, nil
}

func (db *DbClient) queryValidatorElectionParticipants(
	ctx context.Context,
	conn *pgxpool.Conn,
	electionId int32,
) ([]models.ValidatorElectionParticipant, error) {
	rows, err := conn.Query(ctx, `
		SELECT election_id, validator_pubkey, stake::text, max_factor,
		       stake_holder_address, adnl_addr, source_mc_seqno
		FROM validator_election_participants
		WHERE election_id = $1
		ORDER BY stake DESC, validator_pubkey
	`, electionId)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	participants := []models.ValidatorElectionParticipant{}
	for rows.Next() {
		var participant models.ValidatorElectionParticipant
		if err := rows.Scan(
			&participant.ElectionId,
			&participant.ValidatorPubkey,
			&participant.Stake,
			&participant.MaxFactor,
			&participant.StakeHolderAddress,
			&participant.AdnlAddr,
			&participant.SourceMcSeqno,
		); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		participants = append(participants, participant)
	}
	if err := rows.Err(); err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	return participants, nil
}

func (db *DbClient) QueryValidatorCycles(
	req models.ValidatorCyclesRequest,
	settings models.RequestSettings,
) ([]models.ValidatorCycle, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancelCtx()

	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	query := `
		SELECT election_id, utime_since, utime_until, total, main,
		       total_weight::text, total_stake::text, source_mc_seqno,
		       validators_elected_for, elections_start_before, elections_end_before,
		       stake_held_for, max_validators, max_main_validators, min_validators,
		       min_stake::text, max_stake::text, min_total_stake::text, max_stake_factor
		FROM validator_cycles
		WHERE true
	`
	args := []interface{}{}
	argIdx := 1
	if req.CycleStart != nil {
		query += fmt.Sprintf(" AND utime_since = $%d", argIdx)
		args = append(args, *req.CycleStart)
		argIdx++
	}
	if req.ElectionId != nil {
		query += fmt.Sprintf(" AND election_id = $%d", argIdx)
		args = append(args, *req.ElectionId)
		argIdx++
	}
	if req.StakeHolderAddress != nil {
		query += fmt.Sprintf(` AND EXISTS (
			SELECT 1 FROM validator_election_participants p
			JOIN validator_cycle_members m
			  ON m.utime_since = validator_cycles.utime_since
			 AND m.validator_pubkey = p.validator_pubkey
			WHERE p.election_id = validator_cycles.election_id
			  AND p.stake_holder_address = $%d
		)`, argIdx)
		args = append(args, string(*req.StakeHolderAddress))
		argIdx++
	}
	if req.AdnlAddress != nil {
		query += fmt.Sprintf(` AND EXISTS (
			SELECT 1 FROM validator_cycle_members m
			WHERE m.utime_since = validator_cycles.utime_since
			  AND m.adnl_addr = $%d
		)`, argIdx)
		args = append(args, strings.ToUpper(*req.AdnlAddress))
		argIdx++
	}
	if req.ValidatorPubkey != nil {
		query += fmt.Sprintf(` AND EXISTS (
			SELECT 1 FROM validator_cycle_members m
			WHERE m.utime_since = validator_cycles.utime_since
			  AND m.validator_pubkey = $%d
		)`, argIdx)
		args = append(args, strings.ToUpper(*req.ValidatorPubkey))
		argIdx++
	}
	query += " ORDER BY utime_since DESC"
	limit, err := limitQuery(req.LimitParams, settings)
	if err != nil {
		return nil, err
	}
	query += limit

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	cycles := []models.ValidatorCycle{}
	for rows.Next() {
		var cycle models.ValidatorCycle
		if err := rows.Scan(
			&cycle.ElectionId,
			&cycle.CycleStart,
			&cycle.CycleEnd,
			&cycle.Total,
			&cycle.Main,
			&cycle.TotalWeight,
			&cycle.TotalStake,
			&cycle.SourceMcSeqno,
			&cycle.ValidatorsElectedFor,
			&cycle.ElectionsStartBefore,
			&cycle.ElectionsEndBefore,
			&cycle.StakeHeldFor,
			&cycle.MaxValidators,
			&cycle.MaxMainValidators,
			&cycle.MinValidators,
			&cycle.MinStake,
			&cycle.MaxStake,
			&cycle.MinTotalStake,
			&cycle.MaxStakeFactor,
		); err != nil {
			rows.Close()
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		cycles = append(cycles, cycle)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	rows.Close()

	includeValidators := req.ReturnValidators != nil && *req.ReturnValidators
	if includeValidators {
		for i := range cycles {
			validators, err := db.queryValidatorCycleValidators(ctx, conn, cycles[i].CycleStart)
			if err != nil {
				return nil, err
			}
			cycles[i].Validators = validators
		}
	}
	return cycles, nil
}

func (db *DbClient) queryValidatorCycleValidators(
	ctx context.Context,
	conn *pgxpool.Conn,
	utimeSince int32,
) ([]models.ValidatorCycleValidator, error) {
	rows, err := conn.Query(ctx, `
		SELECT utime_since, validator_index, validator_pubkey,
		       adnl_addr, weight::text, source_mc_seqno
		FROM validator_cycle_members
		WHERE utime_since = $1
		ORDER BY validator_index
	`, utimeSince)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	validators := []models.ValidatorCycleValidator{}
	for rows.Next() {
		var validator models.ValidatorCycleValidator
		if err := rows.Scan(
			&validator.CycleStart,
			&validator.ValidatorIndex,
			&validator.ValidatorPubkey,
			&validator.AdnlAddr,
			&validator.Weight,
			&validator.SourceMcSeqno,
		); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		validators = append(validators, validator)
	}
	if err := rows.Err(); err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	return validators, nil
}

func (db *DbClient) QueryValidatorComplaints(
	req models.ValidatorComplaintsRequest,
	settings models.RequestSettings,
) ([]models.ValidatorComplaint, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancelCtx()

	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	query := `
		SELECT vc.utime_since, c.complaint_hash, c.validator_pubkey, c.adnl_addr, c.description_boc,
		       c.created_at, c.severity, c.reward_address, c.paid::text, c.suggested_fine::text,
		       c.suggested_fine_part, c.voted_validators, c.vset_id, c.weight_remaining,
		       c.approved_percent, c.is_passed, c.source_mc_seqno
		FROM validator_complaints c
		JOIN validator_cycles vc ON vc.election_id = c.election_id
		WHERE true
	`
	args := []interface{}{}
	argIdx := 1
	if req.CycleStart != nil {
		query += fmt.Sprintf(" AND vc.utime_since = $%d", argIdx)
		args = append(args, *req.CycleStart)
		argIdx++
	}
	if req.ValidatorPubkey != nil {
		query += fmt.Sprintf(" AND c.validator_pubkey = $%d", argIdx)
		args = append(args, strings.ToUpper(*req.ValidatorPubkey))
		argIdx++
	}
	if req.StakeHolderAddress != nil {
		query += fmt.Sprintf(` AND EXISTS (
			SELECT 1 FROM validator_election_participants p
			WHERE p.election_id = c.election_id
			  AND p.validator_pubkey = c.validator_pubkey
			  AND p.stake_holder_address = $%d
		)`, argIdx)
		args = append(args, string(*req.StakeHolderAddress))
		argIdx++
	}
	if req.AdnlAddress != nil {
		query += fmt.Sprintf(" AND c.adnl_addr = $%d", argIdx)
		args = append(args, strings.ToUpper(*req.AdnlAddress))
		argIdx++
	}
	query += " ORDER BY vc.utime_since DESC, c.created_at DESC, c.complaint_hash"
	limit, err := limitQuery(req.LimitParams, settings)
	if err != nil {
		return nil, err
	}
	query += limit

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	complaints := []models.ValidatorComplaint{}
	for rows.Next() {
		var complaint models.ValidatorComplaint
		var votedRaw []byte
		var adnlAddr *string
		if err := rows.Scan(
			&complaint.CycleStart,
			&complaint.ComplaintHash,
			&complaint.ValidatorPubkey,
			&adnlAddr,
			&complaint.DescriptionBoc,
			&complaint.CreatedAt,
			&complaint.Severity,
			&complaint.RewardAddress,
			&complaint.Paid,
			&complaint.SuggestedFine,
			&complaint.SuggestedFinePart,
			&votedRaw,
			&complaint.VsetId,
			&complaint.WeightRemaining,
			&complaint.ApprovedPercent,
			&complaint.IsPassed,
			&complaint.SourceMcSeqno,
		); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		if len(votedRaw) == 0 {
			votedRaw = []byte("[]")
		}
		complaint.AdnlAddr = adnlAddr
		if err := json.Unmarshal(votedRaw, &complaint.VotedValidators); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		complaints = append(complaints, complaint)
	}
	if err := rows.Err(); err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	return complaints, nil
}
