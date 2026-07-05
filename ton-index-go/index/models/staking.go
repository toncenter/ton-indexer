package models

type NominatorPoolEvent struct {
	TxHash                HashType       `json:"tx_hash"`
	TxLt                  int64          `json:"tx_lt,string"`
	Utime                 int32          `json:"utime"`
	PoolAddress           AccountAddress `json:"pool_address"`
	NominatorAddress      AccountAddress `json:"nominator_address"`
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
	StartUtime  *UtimeType           `json:"start_utime,omitempty"`
	EndUtime    *UtimeType           `json:"end_utime,omitempty"`
	Events      []NominatorPoolEvent `json:"events"`
	AddressBook AddressBook          `json:"address_book"`
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
	AddressBook          AddressBook           `json:"address_book"`
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
	AddressBook    AddressBook             `json:"address_book"`
} // @name NominatorPoolsResponse

type NominatorReward struct {
	Utime       int32    `json:"utime"`
	Reward      string   `json:"reward"`
	StakeBefore string   `json:"stake_before"`
	TxHash      HashType `json:"tx_hash"`
	TxLt        int64    `json:"tx_lt,string"`
} // @name NominatorReward

type NominatorRewardsResponse struct {
	StartUtime  *UtimeType        `json:"start_utime,omitempty"`
	EndUtime    *UtimeType        `json:"end_utime,omitempty"`
	TotalOnPage string            `json:"total_on_page"`
	Rewards     []NominatorReward `json:"rewards"`
	AddressBook AddressBook       `json:"address_book"`
} // @name NominatorRewardsResponse

type NominatorPoolValidatorEvent struct {
	TxHash               HashType       `json:"tx_hash"`
	TxLt                 int64          `json:"tx_lt,string"`
	Utime                int32          `json:"utime"`
	PoolAddress          AccountAddress `json:"pool_address"`
	ValidatorAddress     AccountAddress `json:"validator_address"`
	Type                 string         `json:"type" enums:"reward,penalty,deposit,withdrawal"` // "reward", "penalty", "deposit", "withdrawal"
	Amount               string         `json:"amount"`
	BalanceDelta         string         `json:"balance_delta"`
	BalanceBefore        string         `json:"balance_before"`
	BalanceAfter         string         `json:"balance_after"`
	QueryId              *string        `json:"query_id,omitempty"`
	CycleStart           *int32         `json:"cycle_start,omitempty"`
	ValidatorRewardShare int32          `json:"validator_reward_share"`
} // @name NominatorPoolValidatorEvent

type NominatorPoolValidatorEventsResponse struct {
	StartUtime  *UtimeType                    `json:"start_utime,omitempty"`
	EndUtime    *UtimeType                    `json:"end_utime,omitempty"`
	Events      []NominatorPoolValidatorEvent `json:"events"`
	AddressBook AddressBook                   `json:"address_book"`
} // @name NominatorPoolValidatorEventsResponse

type NominatorPoolValidatorReward struct {
	Utime         int32    `json:"utime"`
	Reward        string   `json:"reward"`
	BalanceBefore string   `json:"balance_before"`
	TxHash        HashType `json:"tx_hash"`
	TxLt          int64    `json:"tx_lt,string"`
	CycleStart    *int32   `json:"cycle_start,omitempty"`
	QueryId       *string  `json:"query_id,omitempty"`
} // @name NominatorPoolValidatorReward

type NominatorPoolValidatorRewardsResponse struct {
	StartUtime  *UtimeType                     `json:"start_utime,omitempty"`
	EndUtime    *UtimeType                     `json:"end_utime,omitempty"`
	TotalOnPage string                         `json:"total_on_page"`
	Rewards     []NominatorPoolValidatorReward `json:"rewards"`
	AddressBook AddressBook                    `json:"address_book"`
} // @name NominatorPoolValidatorRewardsResponse

type ValidatorEvent struct {
	TxHash             HashType       `json:"tx_hash"`
	TxLt               int64          `json:"tx_lt,string"`
	Utime              int32          `json:"utime"`
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
	StartUtime  *UtimeType       `json:"start_utime,omitempty"`
	EndUtime    *UtimeType       `json:"end_utime,omitempty"`
	Events      []ValidatorEvent `json:"events"`
	AddressBook AddressBook      `json:"address_book"`
} // @name ValidatorEventsResponse

type ValidatorElectionParticipant struct {
	ValidatorPubkey    string         `json:"validator_pubkey"`
	Stake              string         `json:"stake"`
	MaxFactor          int32          `json:"max_factor"`
	StakeHolderAddress AccountAddress `json:"stake_holder_address"`
	AdnlAddr           string         `json:"adnl_addr"`
} // @name ValidatorElectionParticipant

type ValidatorElection struct {
	ElectionId   int32                          `json:"election_id"`
	ElectClose   int32                          `json:"elect_close"`
	MinStake     string                         `json:"min_stake"`
	TotalStake   string                         `json:"total_stake"`
	Failed       bool                           `json:"failed"`
	Finished     bool                           `json:"finished"`
	Participants []ValidatorElectionParticipant `json:"participants,omitempty"`
} // @name ValidatorElection

type ValidatorElectionsResponse struct {
	Elections   []ValidatorElection `json:"elections"`
	AddressBook AddressBook         `json:"address_book"`
} // @name ValidatorElectionsResponse

type ValidatorCycleValidator struct {
	ValidatorIndex     int32           `json:"validator_index"`
	ValidatorPubkey    string          `json:"validator_pubkey"`
	AdnlAddr           string          `json:"adnl_addr"`
	Weight             string          `json:"weight"`
	StakeHolderAddress *AccountAddress `json:"stake_holder_address,omitempty"`
	Stake              *string         `json:"stake,omitempty"`
	// MaxFactor is the participant's raw declared max stake factor (65536 = 1.0), not the effective one.
	// The elector caps it at the cycle's max_stake_factor when computing Stake, so the effective factor a
	// consumer should use to relate Stake and MaxFactor is min(MaxFactor, ValidatorCycle.MaxStakeFactor).
	MaxFactor  *int32               `json:"max_factor,omitempty"`
	Complaints []ValidatorComplaint `json:"complaints"`
} // @name ValidatorCycleValidator

type ValidatorCycle struct {
	ElectionId           *int32                    `json:"election_id,omitempty"`
	CycleStart           int32                     `json:"cycle_start"`
	CycleEnd             int32                     `json:"cycle_end"`
	Total                int32                     `json:"total"`
	Main                 int32                     `json:"main"`
	TotalWeight          string                    `json:"total_weight"`
	TotalStake           *string                   `json:"total_stake,omitempty"`
	ValidatorsElectedFor int32                     `json:"validators_elected_for"`
	ElectionsStartBefore int32                     `json:"elections_start_before"`
	ElectionsEndBefore   int32                     `json:"elections_end_before"`
	StakeHeldFor         int32                     `json:"stake_held_for"`
	MaxValidators        int32                     `json:"max_validators"`
	MaxMainValidators    int32                     `json:"max_main_validators"`
	MinValidators        int32                     `json:"min_validators"`
	MinStake             *string                   `json:"min_stake,omitempty"`
	MaxStake             *string                   `json:"max_stake,omitempty"`
	MinStakeLimit        string                    `json:"min_stake_limit"`
	MaxStakeLimit        string                    `json:"max_stake_limit"`
	MinTotalStake        string                    `json:"min_total_stake"`
	MaxStakeFactor       int32                     `json:"max_stake_factor"`
	Validators           []ValidatorCycleValidator `json:"validators,omitempty"`
} // @name ValidatorCycle

type ValidatorCyclesResponse struct {
	Cycles      []ValidatorCycle `json:"cycles"`
	AddressBook AddressBook      `json:"address_book"`
} // @name ValidatorCyclesResponse

type ValidatorComplaintVote struct {
	ValidatorIndex  int32   `json:"validator_index"`
	ValidatorPubkey *string `json:"validator_pubkey,omitempty"`
	AdnlAddr        *string `json:"adnl_addr,omitempty"`
	Weight          *string `json:"weight,omitempty"`
} // @name ValidatorComplaintVote

type ValidatorComplaint struct {
	CycleStart         int32                    `json:"cycle_start"`
	ElectionId         int32                    `json:"election_id"`
	ComplaintHash      string                   `json:"complaint_hash"`
	ValidatorPubkey    string                   `json:"validator_pubkey"`
	AdnlAddr           *string                  `json:"adnl_addr"`
	StakeHolderAddress *AccountAddress          `json:"stake_holder_address,omitempty"`
	DescriptionBoc     string                   `json:"description_boc"`
	CreatedAt          int32                    `json:"created_at"`
	Severity           int32                    `json:"severity"`
	RewardAddress      AccountAddress           `json:"reward_address"`
	Paid               string                   `json:"paid"`
	SuggestedFine      string                   `json:"suggested_fine"`
	SuggestedFinePart  int32                    `json:"suggested_fine_part"`
	VotedValidators    []ValidatorComplaintVote `json:"voted_validators"`
	VsetId             string                   `json:"vset_id"`
	WeightRemaining    int64                    `json:"weight_remaining,string"`
	ApprovedPercent    float64                  `json:"approved_percent"`
	IsPassed           bool                     `json:"is_passed"`
} // @name ValidatorComplaint

type ValidatorComplaintsResponse struct {
	Complaints  []ValidatorComplaint `json:"complaints"`
	AddressBook AddressBook          `json:"address_book"`
} // @name ValidatorComplaintsResponse
