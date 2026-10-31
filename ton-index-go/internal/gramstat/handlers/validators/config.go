package validators

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func verifyConfig(boc []byte, want Round) (string, error) {
	root, err := cell.FromBOC(boc)
	if err != nil {
		return "", err
	}
	var set tlb.ValidatorSetAny
	if err = tlb.LoadFromCell(&set, root.BeginParse()); err != nil {
		return "", err
	}
	var got Round
	var dict *cell.Dictionary
	var hasWeight bool
	switch v := set.Validators.(type) {
	case tlb.ValidatorSet:
		got.Since, got.Until, got.Total, got.Main, dict = v.UTimeSince, v.UTimeUntil, v.Total, v.Main, v.List
	case tlb.ValidatorSetExt:
		got.Since, got.Until, got.Total, got.Main, got.TotalWeight, dict = v.UTimeSince, v.UTimeUntil, v.Total, v.Main, v.TotalWeight, v.List
		hasWeight = true
	default:
		return "", fmt.Errorf("unsupported validator set")
	}
	if dict == nil {
		return "", fmt.Errorf("missing validator dictionary")
	}
	entries, err := dict.LoadAll()
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		idx, err := entry.Key.LoadUInt(16)
		if err != nil {
			return "", err
		}
		tag, err := entry.Value.PreloadUInt(8)
		if err != nil {
			return "", err
		}
		m := Member{Index: uint16(idx)}
		switch tag {
		case 0x73:
			var v tlb.ValidatorAddr
			if err = tlb.LoadFromCell(&v, entry.Value); err != nil {
				return "", err
			}
			m.Pubkey, m.ADNL, m.Weight = hex.EncodeToString(v.PublicKey.Key), hex.EncodeToString(v.ADNLAddr), v.Weight
		case 0x53:
			var v tlb.Validator
			if err = tlb.LoadFromCell(&v, entry.Value); err != nil {
				return "", err
			}
			m.Pubkey, m.ADNL, m.Weight = hex.EncodeToString(v.PublicKey.Key), hex.EncodeToString(make([]byte, 32)), v.Weight
		default:
			return "", fmt.Errorf("unsupported validator descriptor")
		}
		got.Members = append(got.Members, m)
	}
	if !hasWeight {
		got.TotalWeight = want.TotalWeight
	}
	if err = got.validate(); err != nil {
		return "", err
	}
	if err = want.validate(); err != nil {
		return "", err
	}
	if !bytes.Equal([]byte(got.RosterDigest), []byte(want.RosterDigest)) {
		return "", fmt.Errorf("config #34 differs from indexed roster at MC %d", want.AnchorMC)
	}
	return hex.EncodeToString(root.Hash()), nil
}
