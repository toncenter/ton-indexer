// Package staking derives exact stake amounts from native Elector observations.
package staking

import (
	"fmt"
	"math/big"
	"strings"
)

const Version = "staking-v1"

type Position struct {
	Pubkey string  `json:"pubkey"`
	Holder string  `json:"holder"`
	Stake  string  `json:"stake"`
	Weight string  `json:"weight"`
	Banned bool    `json:"banned"`
	ADNL   *string `json:"adnl"`
}
type Election struct {
	ID         uint32     `json:"election_id"`
	Hash       string     `json:"vset_hash"`
	UnfreezeAt uint32     `json:"unfreeze_at"`
	Total      string     `json:"total_stake"`
	Positions  []Position `json:"positions"`
}
type Applications struct {
	ID           uint32     `json:"election_id"`
	Finished     bool       `json:"finished"`
	Failed       bool       `json:"failed"`
	Total        string     `json:"total_stake"`
	Participants []Position `json:"participants"`
}
type Holder struct {
	Address string  `json:"address"`
	Kind    string  `json:"kind"`
	Pool    *string `json:"pool"`
	Error   string  `json:"error"`
}
type Snapshot struct {
	Version              int           `json:"version"`
	Address              string        `json:"address"`
	DataHash             string        `json:"data_hash"`
	CodeHash             string        `json:"code_hash"`
	PreviousHash         string        `json:"previous_data_hash"`
	Complete             bool          `json:"complete"`
	PenaltyActivity      bool          `json:"penalty_activity"`
	Error                string        `json:"error"`
	ApplicationsComplete bool          `json:"applications_complete"`
	Applications         *Applications `json:"applications"`
	Elections            []Election    `json:"elections"`
	Holders              []Holder      `json:"holders"`
}
type Observation struct {
	MC   uint32
	Time uint32
	Snapshot
}
type Origin struct {
	First    Observation
	Previous *Observation
}
type Point struct {
	Day        string
	AnchorMC   uint32
	AnchorTime uint32
	Partial    uint8
	State      *Observation
	Origins    map[string]Origin
}
type Row struct {
	Day              string            `json:"day"`
	AnchorMC         uint32            `json:"anchor_mc_seqno"`
	StateMC          uint32            `json:"state_mc_seqno"`
	Through          uint32            `json:"data_through"`
	Partial          uint8             `json:"is_partial"`
	Calculated       string            `json:"calculated_at"`
	Version          string            `json:"calc_version"`
	Status           string            `json:"status"`
	Remaining        *string           `json:"remaining_nanogram"`
	Submitted        *string           `json:"submitted_nanogram"`
	Selected         *string           `json:"selected_nanogram"`
	Reduction        *string           `json:"frozen_principal_reduction_nanogram"`
	Breakdown        map[string]string `json:"breakdown"`
	Telegram         *string           `json:"telegram_nanogram"`
	TelegramObserved *string           `json:"telegram_observed_nanogram"`
	TelegramUnknown  *string           `json:"telegram_unknown_nanogram"`
	Positions        uint32            `json:"positions"`
	Elections        uint32            `json:"elections"`
	UnknownOrigins   uint32            `json:"unknown_origins"`
}

func electionKey(e Election) string { return fmt.Sprintf("%d:%s", e.ID, strings.ToUpper(e.Hash)) }
func amount(s string) (*big.Int, error) {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok || n.Sign() < 0 || n.String() != s || len(s) > 38 {
		return nil, fmt.Errorf("invalid exact stake amount %q", s)
	}
	return n, nil
}
func stringPtr(n *big.Int) *string { s := n.String(); return &s }

func validate(e Election) error {
	total, err := amount(e.Total)
	if err != nil {
		return err
	}
	sum := new(big.Int)
	seen := map[string]bool{}
	for _, p := range e.Positions {
		if p.Pubkey == "" || p.Holder == "" || seen[p.Pubkey] {
			return fmt.Errorf("duplicate or invalid frozen position")
		}
		seen[p.Pubkey] = true
		n, err := amount(p.Stake)
		if err != nil {
			return err
		}
		sum.Add(sum, n)
	}
	if sum.Cmp(total) != 0 {
		return fmt.Errorf("frozen election sum mismatch")
	}
	return nil
}

// A first observation in the middle of an election is not its original stake.
// The preceding complete state must join by native Elector data hash and not
// contain this election. Successful final applications corroborate selection.
func initial(e Election, o Origin) (*Election, *Applications) {
	s := o.First.Snapshot
	if !s.Complete || s.PenaltyActivity || o.Previous == nil || !o.Previous.Complete || s.Address != o.Previous.Address || !strings.EqualFold(s.CodeHash, o.Previous.CodeHash) ||
		s.PreviousHash == "" || !strings.EqualFold(s.PreviousHash, o.Previous.DataHash) ||
		!s.ApplicationsComplete || s.Applications == nil || !s.Applications.Finished || s.Applications.Failed || s.Applications.ID != e.ID {
		return nil, nil
	}
	for _, old := range o.Previous.Elections {
		if old.ID == e.ID {
			return nil, nil
		}
	}
	for i := range s.Elections {
		if electionKey(s.Elections[i]) == electionKey(e) {
			return &s.Elections[i], s.Applications
		}
	}
	return nil, nil
}
