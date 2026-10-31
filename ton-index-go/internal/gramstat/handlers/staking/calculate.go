package staking

import (
	"fmt"
	"math/big"
	"strings"
	"time"
)

// Group by contract mechanism, independently of Telegram infrastructure tags.
func category(h Holder, holder string, timestamp uint32) string {
	if h.Address == "" || h.Error != "" {
		return "unknown"
	}
	switch h.Kind {
	case "single_nominator", "nominator_pool":
		return h.Kind
	case "liquid_staking_controller", "hipo_controller":
		if h.Pool != nil {
			return "liquid_staking"
		}
	}
	// First-party wsTON proxy list, first independently documented observation.
	// Older attribution needs an earlier reviewed registry interval, not a live
	// nickname projected backwards. Other whales_proxy contracts stay unknown.
	if timestamp >= 1792236000 {
		switch strings.ToLower(holder) {
		case "-1:07fdddf68a769856d336f23554c4e13ee2072505739d51682935a40f27e570b1",
			"-1:92cdc50db0fd94175d29edc1c42d39a2429dab2da4c28051e48670ab1661f0b1":
			return "liquid_staking"
		}
	}
	return "unknown"
}

func Calculate(p Point, calculated time.Time) (Row, error) {
	r := Row{Day: p.Day, AnchorMC: p.AnchorMC, Through: p.AnchorTime, Partial: p.Partial,
		Calculated: calculated.UTC().Format("2006-01-02 15:04:05.000000"), Version: Version,
		Status: "missing_history", Breakdown: map[string]string{}}
	if p.State == nil {
		return r, nil
	}
	r.StateMC = p.State.MC
	if p.State.Version != 1 {
		return r, fmt.Errorf("unsupported Elector observation version")
	}
	if !p.State.Complete {
		r.Status = "unknown_elector_state"
		return r, nil
	}
	remaining, submitted, selected := new(big.Int), new(big.Int), new(big.Int)
	groups := map[string]*big.Int{"liquid_staking": {}, "nominator_pool": {}, "single_nominator": {}, "unknown": {}}
	holders := map[string]Holder{}
	for _, h := range p.State.Holders {
		holders[strings.ToLower(h.Address)] = h
	}
	seen := map[uint32]bool{}
	for _, e := range p.State.Elections {
		if seen[e.ID] {
			return r, fmt.Errorf("duplicate election ID")
		}
		seen[e.ID] = true
		if err := validate(e); err != nil {
			return r, err
		}
		n, _ := amount(e.Total)
		remaining.Add(remaining, n)
		original, applications := initial(e, p.Origins[electionKey(e)])
		validOrigin := original != nil && validate(*original) == nil
		if validOrigin {
			originals := map[string]Position{}
			for _, q := range original.Positions {
				originals[q.Pubkey] = q
			}
			if len(originals) != len(e.Positions) {
				validOrigin = false
			}
			for _, q := range e.Positions {
				old, ok := originals[q.Pubkey]
				a, _ := amount(q.Stake)
				b, err := amount(old.Stake)
				if !ok || old.Holder != q.Holder || err != nil || a.Cmp(b) > 0 {
					validOrigin = false
					break
				}
			}
		}
		if validOrigin {
			a, err := amount(applications.Total)
			if err != nil {
				return r, err
			}
			b, _ := amount(original.Total)
			if a.Cmp(b) < 0 {
				return r, fmt.Errorf("selected stake exceeds accepted applications")
			}
			submitted.Add(submitted, a)
			selected.Add(selected, b)
		} else {
			r.UnknownOrigins++
		}
		for _, position := range e.Positions {
			value, _ := amount(position.Stake)
			group := category(holders[strings.ToLower(position.Holder)], position.Holder, p.AnchorTime)
			groups[group].Add(groups[group], value)
			r.Positions++
		}
		r.Elections++
	}
	r.Remaining = stringPtr(remaining)
	if r.UnknownOrigins == 0 {
		r.Submitted = stringPtr(submitted)
		r.Selected = stringPtr(selected)
		r.Reduction = stringPtr(new(big.Int).Sub(selected, remaining))
	}
	for k, v := range groups {
		r.Breakdown[k] = v.String()
	}
	r.TelegramUnknown = stringPtr(remaining)
	r.TelegramObserved = stringPtr(new(big.Int))
	r.Status = "complete"
	if r.UnknownOrigins > 0 || groups["unknown"].Sign() > 0 {
		r.Status = "partial_attribution"
	}
	return r, nil
}
