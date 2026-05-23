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
	Debit       string `json:"debit"`
	Credit      string `json:"credit"`
} // @name NominatorBooking

type PoolBooking struct {
	NominatorAddress string `json:"nominator_address"`
	Utime            int32  `json:"utime"`
	BookingType      string `json:"booking_type"`
	Debit            string `json:"debit"`
	Credit           string `json:"credit"`
} // @name PoolBooking

type NominatorPoolInfo struct {
	StakeAmountSent    string                `json:"stake_amount_sent"`
	ValidatorAmount    string                `json:"validator_amount"`
	NominatorsCount    int                   `json:"nominators_count"`
	ActiveNominators   []ActiveNominatorInfo `json:"active_nominators"`
	InactiveNominators []string              `json:"inactive_nominators"`
} // @name NominatorPoolInfo

type ActiveNominatorInfo struct {
	Address        string `json:"address"`
	Balance        string `json:"balance"`
	PendingBalance string `json:"pending_balance"`
} // @name ActiveNominatorInfo

type NominatorInPool struct {
	PoolAddress    string `json:"pool_address"`
	Balance        string `json:"balance"`
	PendingBalance string `json:"pending_balance"`
} // @name NominatorInPool

type NominatorEarning struct {
	Utime       int32  `json:"utime"`
	Income      string `json:"income"`
	StakeBefore string `json:"stake_before"`
} // @name NominatorEarning

type NominatorEarningsResponse struct {
	TotalOnPeriod string             `json:"total_on_period"`
	Earnings      []NominatorEarning `json:"earnings"`
} // @name NominatorEarningsResponse
