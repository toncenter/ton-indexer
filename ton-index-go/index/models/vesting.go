package models

type VestingInfo struct {
	Address       *AccountAddress  `json:"address"`
	StartTime     *int64           `json:"start_time"`
	TotalDuration *int64           `json:"total_duration"`
	UnlockPeriod  *int64           `json:"unlock_period"`
	CliffDuration *int64           `json:"cliff_duration"`
	SenderAddress *AccountAddress  `json:"sender_address"`
	OwnerAddress  *AccountAddress  `json:"owner_address"`
	TotalAmount   *string          `json:"total_amount"`
	Whitelist     []AccountAddress `json:"whitelist"`
}
