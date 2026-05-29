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

type ValidatorEvent struct {
	TxHash             HashType       `json:"tx_hash"`
	TxLt               int64          `json:"tx_lt,string"`
	Utime              int32          `json:"utime"`
	McSeqno            int32          `json:"mc_seqno"`
	TraceId            *HashType      `json:"trace_id,omitempty"`
	Type               string         `json:"type"`
	StakeHolderAddress AccountAddress `json:"stake_holder_address"`
	ValidatorPubkey    *string        `json:"validator_pubkey,omitempty"`
	AdnlAddr           *string        `json:"adnl_addr,omitempty"`
	ElectionId         *int32         `json:"election_id,omitempty"`
	QueryId            *string        `json:"query_id,omitempty"`
	Amount             string         `json:"amount"`
	Reason             *int32         `json:"reason,omitempty"`
} // @name ValidatorEvent

type ValidatorEventsResponse struct {
	StartUtime UtimeType        `json:"start_utime"`
	EndUtime   UtimeType        `json:"end_utime"`
	Events     []ValidatorEvent `json:"events"`
} // @name ValidatorEventsResponse

type ValidatorElectionParticipant struct {
	ElectionId         int32          `json:"election_id"`
	ValidatorPubkey    string         `json:"validator_pubkey"`
	Stake              string         `json:"stake"`
	MaxFactor          int32          `json:"max_factor"`
	StakeHolderAddress AccountAddress `json:"stake_holder_address"`
	AdnlAddr           string         `json:"adnl_addr"`
	SourceMcSeqno      int32          `json:"source_mc_seqno"`
} // @name ValidatorElectionParticipant

type ValidatorElection struct {
	ElectionId    int32                          `json:"election_id"`
	ElectClose    int32                          `json:"elect_close"`
	MinStake      string                         `json:"min_stake"`
	TotalStake    string                         `json:"total_stake"`
	Failed        bool                           `json:"failed"`
	Finished      bool                           `json:"finished"`
	SourceMcSeqno int32                          `json:"source_mc_seqno"`
	Participants  []ValidatorElectionParticipant `json:"participants,omitempty"`
} // @name ValidatorElection

type ValidatorElectionsResponse struct {
	Elections []ValidatorElection `json:"elections"`
} // @name ValidatorElectionsResponse

type ValidatorCycleValidator struct {
	CycleStart      int32  `json:"cycle_start"`
	ValidatorIndex  int32  `json:"validator_index"`
	ValidatorPubkey string `json:"validator_pubkey"`
	AdnlAddr        string `json:"adnl_addr"`
	Weight          string `json:"weight"`
	SourceMcSeqno   int32  `json:"source_mc_seqno"`
} // @name ValidatorCycleValidator

type ValidatorCycle struct {
	ElectionId           *int32                    `json:"election_id,omitempty"`
	CycleStart           int32                     `json:"cycle_start"`
	CycleEnd             int32                     `json:"cycle_end"`
	Total                int32                     `json:"total"`
	Main                 int32                     `json:"main"`
	TotalWeight          string                    `json:"total_weight"`
	TotalStake           *string                   `json:"total_stake,omitempty"`
	SourceMcSeqno        int32                     `json:"source_mc_seqno"`
	ValidatorsElectedFor int32                     `json:"validators_elected_for"`
	ElectionsStartBefore int32                     `json:"elections_start_before"`
	ElectionsEndBefore   int32                     `json:"elections_end_before"`
	StakeHeldFor         int32                     `json:"stake_held_for"`
	MaxValidators        int32                     `json:"max_validators"`
	MaxMainValidators    int32                     `json:"max_main_validators"`
	MinValidators        int32                     `json:"min_validators"`
	MinStake             string                    `json:"min_stake"`
	MaxStake             string                    `json:"max_stake"`
	MinTotalStake        string                    `json:"min_total_stake"`
	MaxStakeFactor       int32                     `json:"max_stake_factor"`
	Validators           []ValidatorCycleValidator `json:"validators,omitempty"`
} // @name ValidatorCycle

type ValidatorCyclesResponse struct {
	Cycles []ValidatorCycle `json:"cycles"`
} // @name ValidatorCyclesResponse

type ValidatorComplaintVote struct {
	ValidatorIndex  int32   `json:"validator_index"`
	ValidatorPubkey *string `json:"validator_pubkey,omitempty"`
	AdnlAddr        *string `json:"adnl_addr,omitempty"`
	Weight          *string `json:"weight,omitempty"`
} // @name ValidatorComplaintVote

type ValidatorComplaint struct {
	CycleStart        int32                    `json:"cycle_start"`
	ComplaintHash     string                   `json:"complaint_hash"`
	ValidatorPubkey   string                   `json:"validator_pubkey"`
	AdnlAddr          *string                  `json:"adnl_addr"`
	DescriptionBoc    string                   `json:"description_boc"`
	CreatedAt         int32                    `json:"created_at"`
	Severity          int32                    `json:"severity"`
	RewardAddress     AccountAddress           `json:"reward_address"`
	Paid              string                   `json:"paid"`
	SuggestedFine     string                   `json:"suggested_fine"`
	SuggestedFinePart int32                    `json:"suggested_fine_part"`
	VotedValidators   []ValidatorComplaintVote `json:"voted_validators"`
	VsetId            string                   `json:"vset_id"`
	WeightRemaining   int64                    `json:"weight_remaining"`
	ApprovedPercent   float64                  `json:"approved_percent"`
	IsPassed          bool                     `json:"is_passed"`
	SourceMcSeqno     int32                    `json:"source_mc_seqno"`
} // @name ValidatorComplaint

type ValidatorComplaintsResponse struct {
	Complaints []ValidatorComplaint `json:"complaints"`
} // @name ValidatorComplaintsResponse
