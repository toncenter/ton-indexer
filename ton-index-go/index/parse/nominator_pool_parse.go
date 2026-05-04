package parse

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// ParseNominatorPoolData parses nominator pool storage from data BOC (base64 encoded)
func ParseNominatorPoolData(dataBoc []byte) (*models.NominatorPoolInfo, error) {
	bocBytes, err := base64.StdEncoding.DecodeString(string(dataBoc))
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	dataCell, err := cell.FromBOC(bocBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse BOC: %w", err)
	}

	slice := dataCell.BeginParse()

	// nominator_pool_storage#_ state:uint8 nominators_count:uint16
	//   stake_amount_sent:Coins validator_amount:Coins
	//   config:^NominatorPoolConfig nominators:(HashmapE 256 Nominator)
	//   ...

	if _, err = slice.LoadUInt(8); err != nil {
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	nominatorsCount, err := slice.LoadUInt(16)
	if err != nil {
		return nil, fmt.Errorf("failed to load nominators_count: %w", err)
	}

	stakeAmountSent, err := slice.LoadBigCoins()
	if err != nil {
		return nil, fmt.Errorf("failed to load stake_amount_sent: %w", err)
	}

	validatorAmount, err := slice.LoadBigCoins()
	if err != nil {
		return nil, fmt.Errorf("failed to load validator_amount: %w", err)
	}

	if _, err = slice.LoadRef(); err != nil {
		return nil, fmt.Errorf("failed to load config ref: %w", err)
	}

	nominatorsDict, err := slice.LoadDict(256)
	if err != nil {
		return nil, fmt.Errorf("failed to load nominators dict: %w", err)
	}

	activeNominators := []models.ActiveNominatorInfo{}

	if nominatorsDict != nil {
		all, err := nominatorsDict.LoadAll()
		if err != nil {
			return nil, fmt.Errorf("failed to load all nominators: %w", err)
		}

		for _, kv := range all {
			hashBytes, err := kv.Key.LoadSlice(256)
			if err != nil {
				continue
			}

			addressStr := fmt.Sprintf("0:%s", strings.ToUpper(hex.EncodeToString(hashBytes)))

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
		InactiveNominators: []string{},
	}, nil
}

// BigIntToString converts *big.Int to string, returns "0" if nil
func BigIntToString(val *big.Int) string {
	if val == nil {
		return "0"
	}
	return val.String()
}
