package models

type NominatorPoolIncome struct {
	TxHash           HashType       `json:"tx_hash"`
	TxLt             int64          `json:"tx_lt,string"`
	TxNow            int32          `json:"tx_now"`
	McSeqno          int32          `json:"mc_seqno"`
	PoolAddress      AccountAddress `json:"pool_address"`
	NominatorAddress AccountAddress `json:"nominator_address"`
	IncomeAmount     string         `json:"income_amount"`
	NominatorBalance string         `json:"nominator_balance"`
	TraceId          *HashType      `json:"trace_id,omitempty"`
} // @name NominatorPoolIncome

type NominatorStakeMovement struct {
	Utime        int32     `json:"utime"`
	Type         string    `json:"type"` // "nominator_income", "nominator_deposit", "nominator_withdrawal"
	Amount       string    `json:"amount"`
	BalanceDelta string    `json:"balance_delta"`
	TxHash       *HashType `json:"tx_hash,omitempty"`
	TxLt         *int64    `json:"tx_lt,omitempty,string"`
	TraceId      *HashType `json:"trace_id,omitempty"`
	ActionId     *HashType `json:"action_id,omitempty"`
	StartLt      *int64    `json:"start_lt,omitempty,string"`
} // @name NominatorStakeMovement

type PoolStakeMovement struct {
	NominatorAddress AccountAddress `json:"nominator_address"`
	Utime            int32          `json:"utime"`
	Type             string         `json:"type"`
	Amount           string         `json:"amount"`
	BalanceDelta     string         `json:"balance_delta"`
	TxHash           *HashType      `json:"tx_hash,omitempty"`
	TxLt             *int64         `json:"tx_lt,omitempty,string"`
	TraceId          *HashType      `json:"trace_id,omitempty"`
	ActionId         *HashType      `json:"action_id,omitempty"`
	StartLt          *int64         `json:"start_lt,omitempty,string"`
} // @name PoolStakeMovement

type NominatorPoolInfo struct {
	StakeAmountSent      string                `json:"stake_amount_sent"`
	ValidatorAmount      string                `json:"validator_amount"`
	ValidatorAddress     AccountAddress        `json:"validator_address"`
	ValidatorRewardShare int                   `json:"validator_reward_share"`
	State                int                   `json:"state"`
	NominatorsCount      int                   `json:"nominators_count"`
	MaxNominatorsCount   int                   `json:"max_nominators_count"`
	MinValidatorStake    string                `json:"min_validator_stake"`
	MinNominatorStake    string                `json:"min_nominator_stake"`
	ActiveNominators     []ActiveNominatorInfo `json:"active_nominators"`
} // @name NominatorPoolInfo

type ActiveNominatorInfo struct {
	Address        string `json:"address"`
	Balance        string `json:"balance"`
	PendingBalance string `json:"pending_balance"`
} // @name ActiveNominatorInfo

type NominatorPoolPosition struct {
	PoolAddress    AccountAddress `json:"pool_address"`
	Balance        string         `json:"balance"`
	PendingBalance string         `json:"pending_balance"`
} // @name NominatorPoolPosition

type NominatorEarning struct {
	Utime       int32     `json:"utime"`
	Income      string    `json:"income"`
	StakeBefore string    `json:"stake_before"`
	TxHash      HashType  `json:"tx_hash"`
	TxLt        int64     `json:"tx_lt,string"`
	TraceId     *HashType `json:"trace_id,omitempty"`
} // @name NominatorEarning

type NominatorEarningsResponse struct {
	TotalOnPeriod string             `json:"total_on_period"`
	Earnings      []NominatorEarning `json:"earnings"`
} // @name NominatorEarningsResponse
