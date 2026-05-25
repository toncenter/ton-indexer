package models

type NominatorPoolEvent struct {
	TxHash                HashType       `json:"tx_hash"`
	TxLt                  int64          `json:"tx_lt,string"`
	Utime                 int32          `json:"utime"`
	McSeqno               int32          `json:"mc_seqno"`
	TraceId               *HashType      `json:"trace_id,omitempty"`
	PoolAddress           AccountAddress `json:"pool_address"`
	NominatorAddress      AccountAddress `json:"nominator_address"`
	EventIndex            int32          `json:"event_index"`
	Type                  string         `json:"type" enums:"reward,deposit,withdrawal,pending_deposit_activation,withdrawal_request"` // "reward", "deposit", "withdrawal", "pending_deposit_activation", "withdrawal_request"
	Amount                string         `json:"amount"`
	BalanceDelta          string         `json:"balance_delta"`
	PendingBalanceDelta   string         `json:"pending_balance_delta"`
	BalanceBefore         string         `json:"balance_before"`
	BalanceAfter          string         `json:"balance_after"`
	PendingBalanceBefore  string         `json:"pending_balance_before"`
	PendingBalanceAfter   string         `json:"pending_balance_after"`
	WithdrawRequestBefore bool           `json:"withdraw_request_before"`
	WithdrawRequestAfter  bool           `json:"withdraw_request_after"`
} // @name NominatorPoolEvent

type NominatorPoolEventsResponse struct {
	StartUtime UtimeType            `json:"start_utime"`
	EndUtime   UtimeType            `json:"end_utime"`
	Events     []NominatorPoolEvent `json:"events"`
} // @name NominatorPoolEventsResponse

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

type NominatorPoolsResponse struct {
	NominatorPools []NominatorPoolPosition `json:"nominator_pools"`
} // @name NominatorPoolsResponse

type NominatorReward struct {
	Utime       int32     `json:"utime"`
	Reward      string    `json:"reward"`
	StakeBefore string    `json:"stake_before"`
	TxHash      HashType  `json:"tx_hash"`
	TxLt        int64     `json:"tx_lt,string"`
	TraceId     *HashType `json:"trace_id,omitempty"`
	EventIndex  int32     `json:"event_index"`
} // @name NominatorReward

type NominatorRewardsResponse struct {
	StartUtime    UtimeType         `json:"start_utime"`
	EndUtime      UtimeType         `json:"end_utime"`
	TotalOnPeriod string            `json:"total_on_period"`
	Rewards       []NominatorReward `json:"rewards"`
} // @name NominatorRewardsResponse
