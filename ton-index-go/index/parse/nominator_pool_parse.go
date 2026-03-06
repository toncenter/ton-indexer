package parse

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"math/big"
	"strings"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// ParseNominatorPoolData parses nominator pool storage from data BOC (base64 encoded)
func ParseNominatorPoolData(dataBoc []byte) (*models.NominatorPoolInfo, error) {
	// decode from base64
	bocBytes, err := base64.StdEncoding.DecodeString(string(dataBoc))
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	// load cell from BOC
	dataCell, err := cell.FromBOC(bocBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse BOC: %w", err)
	}

	slice := dataCell.BeginParse()

	// parse nominator pool storage according to TLB schema
	// nominator_pool_storage#_ state:uint8 nominators_count:uint16
	//   stake_amount_sent:Coins validator_amount:Coins
	//   config:^NominatorPoolConfig nominators:(HashmapE 256 Nominator)
	//   withdraw_requests:(HashmapE 256 WithdrawRequest)
	//   stake_at:uint32 saved_validator_set_hash:uint256
	//   validator_set_changes_count:uint32 validator_set_change_time:uint32
	//   stake_held_for:uint32 config_proposal_votings:^ConfigProposalVotings
	//   = NominatorPoolStorage;

	// skip state (uint8)
	_, err = slice.LoadUInt(8)
	if err != nil {
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	// load nominators_count (uint16)
	nominatorsCount, err := slice.LoadUInt(16)
	if err != nil {
		return nil, fmt.Errorf("failed to load nominators_count: %w", err)
	}

	// load stake_amount_sent (Coins)
	stakeAmountSent, err := slice.LoadBigCoins()
	if err != nil {
		return nil, fmt.Errorf("failed to load stake_amount_sent: %w", err)
	}

	// load validator_amount (Coins)
	validatorAmount, err := slice.LoadBigCoins()
	if err != nil {
		return nil, fmt.Errorf("failed to load validator_amount: %w", err)
	}

	// skip config reference
	_, err = slice.LoadRef()
	if err != nil {
		return nil, fmt.Errorf("failed to load config ref: %w", err)
	}

	// load nominators dictionary
	nominatorsDict, err := slice.LoadDict(256)
	if err != nil {
		return nil, fmt.Errorf("failed to load nominators dict: %w", err)
	}

	activeNominators := []models.ActiveNominatorInfo{}

	if nominatorsDict != nil {
		// iterate through nominators
		all, err := nominatorsDict.LoadAll()
		if err != nil {
			return nil, fmt.Errorf("failed to load all nominators: %w", err)
		}

		for _, kv := range all {
			// key is 256-bit address hash
			hashBytes, err := kv.Key.LoadSlice(256)
			if err != nil {
				continue
			}

			// format address as 0:HASH
			addressStr := fmt.Sprintf("0:%s", strings.ToUpper(hex.EncodeToString(hashBytes)))

			// parse nominator value
			// nominator#_ deposit:Coins pending_deposit:Coins withdraw:Coins = Nominator;

			deposit, err := kv.Value.LoadBigCoins()
			if err != nil {
				continue
			}

			pendingDeposit, err := kv.Value.LoadBigCoins()
			if err != nil {
				continue
			}

			activeNominators = append(activeNominators, models.ActiveNominatorInfo{
				Address:        addressStr,
				Balance:        deposit.Int64(),
				PendingBalance: pendingDeposit.Int64(),
			})
		}
	}

	return &models.NominatorPoolInfo{
		StakeAmountSent:    stakeAmountSent.Int64(),
		ValidatorAmount:    validatorAmount.Int64(),
		NominatorsCount:    int(nominatorsCount),
		ActiveNominators:   activeNominators,
		InactiveNominators: []string{}, // will be filled later
	}, nil
}

// bigIntToString converts *big.Int to string, returns "0" if nil
func bigIntToString(val *big.Int) string {
	if val == nil {
		return "0"
	}
	return val.String()
}
