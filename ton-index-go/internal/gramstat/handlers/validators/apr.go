package validators

import (
	"math/big"
	"time"
)

// prepareAPR uses the reward of this election over its selected principal, but
// annualizes over this round AND its immediate successor: each capital tranche
// earns once per two-round cycle. No compounding, fee or penalty subtraction.
// Never bridge a missing round or extrapolate an unfinished interval.
func prepareAPR(r *StakeRound, current, next Coverage, now time.Time) {
	r.APR, r.APRCycleSeconds, r.APRNextUntil, r.APRNextHash, r.APRNextDigest = nil, nil, nil, nil, nil
	r.APRStatus = "incomplete_round"
	if current.Since != r.Since || current.Until != r.Until || current.Until <= current.Since ||
		current.Version != Version || current.Hash != r.ConfigHash || current.Hash == "" ||
		current.Digest != r.RosterDigest || current.Digest == "" ||
		current.Through < current.Until || int64(current.Until) > now.Unix() {
		return
	}
	r.APRStatus = "missing_next_round"
	if next.Since != current.Until || next.Until <= next.Since || next.Version != Version || next.Hash == "" || next.Digest == "" {
		return
	}
	r.APRStatus = "incomplete_next_round"
	if next.Through < next.Until || int64(next.Until) > now.Unix() {
		return
	}
	seconds := next.Until - current.Since
	r.APRCycleSeconds, r.APRNextUntil, r.APRNextHash, r.APRNextDigest = &seconds, &next.Until, &next.Hash, &next.Digest
	r.APRStatus = "invalid_stake"
	if r.Status == "invalid_source" || r.Status == "missing_election_link" {
		return
	}
	selected, selectedErr := stakeAmount(r.Selected)
	reward, rewardErr := stakeAmount(r.Reward)
	if selectedErr != nil || rewardErr != nil {
		return
	}
	r.APRStatus = "missing_selected"
	if selected == nil {
		return
	}
	r.APRStatus = "zero_selected"
	if selected.Sign() == 0 {
		return
	}
	r.APRStatus = "missing_reward"
	if reward == nil || r.UnfreezeTime == nil || int64(*r.UnfreezeTime) > now.Unix() || *r.UnfreezeTime < r.Until {
		return
	}
	// Integer nanoGRAM and rational arithmetic throughout; only the final percent
	// is rounded (half away from zero) to 12 decimal places for storage/display.
	numerator := new(big.Int).Mul(reward, big.NewInt(365*86400*100))
	denominator := new(big.Int).Mul(selected, new(big.Int).SetUint64(uint64(seconds)))
	value := new(big.Rat).SetFrac(numerator, denominator).FloatString(12)
	r.APR, r.APRStatus = &value, "complete"
}
