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

type NominatorBooking struct {
	Utime       int32  `json:"utime"`
	BookingType string `json:"booking_type"` // "income", "deposit", "withdrawal"
	Debit       int64  `json:"debit"`
	Credit      int64  `json:"credit"`
} // @name NominatorBooking

type PoolBooking struct {
	NominatorAddress string `json:"nominator_address"`
	Utime            int32  `json:"utime"`
	BookingType      string `json:"booking_type"`
	Debit            int64  `json:"debit"`
	Credit           int64  `json:"credit"`
} // @name PoolBooking

type NominatorPoolInfo struct {
	StakeAmountSent    int64                 `json:"stake_amount_sent"`
	ValidatorAmount    int64                 `json:"validator_amount"`
	NominatorsCount    int                   `json:"nominators_count"`
	ActiveNominators   []ActiveNominatorInfo `json:"active_nominators"`
	InactiveNominators []string              `json:"inactive_nominators"`
} // @name NominatorPoolInfo

type ActiveNominatorInfo struct {
	Address        string `json:"address"`
	Balance        int64  `json:"balance"`
	PendingBalance int64  `json:"pending_balance"`
} // @name ActiveNominatorInfo

type NominatorInPool struct {
	PoolAddress    string `json:"pool_address"`
	Balance        int64  `json:"balance"`
	PendingBalance int64  `json:"pending_balance"`
} // @name NominatorInPool

type NominatorEarning struct {
	Utime       int32 `json:"utime"`
	Income      int64 `json:"income"`
	StakeBefore int64 `json:"stake_before"`
} // @name NominatorEarning

type NominatorEarningsResponse struct {
	TotalOnPeriod int64              `json:"total_on_period"`
	Earnings      []NominatorEarning `json:"earnings"`
} // @name NominatorEarningsResponse
