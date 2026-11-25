package dexvolume

import "encoding/json"

// A classifier can represent a serial chain within ONE jetton_swap action.
// Use its peer_swaps only as evidence of an executed intermediate amount.
// If the endpoints or any handoff do not reconcile, keep the user boundaries
// but do not promote any intermediate leg to a measurement of the whole swap.
func peerFlow(raw []byte, in, out Leg) (Flow, error) {
	var hops []struct {
		AssetIn   string      `json:"asset_in"`
		AssetOut  string      `json:"asset_out"`
		AmountIn  json.Number `json:"amount_in"`
		AmountOut json.Number `json:"amount_out"`
	}
	if len(raw) == 0 {
		return Flow{}, nil
	}
	if err := json.Unmarshal(raw, &hops); err != nil {
		return Flow{}, err
	}
	if len(hops) < 2 || len(hops) > 64 {
		return Flow{}, nil
	}
	prev := in
	var f Flow
	for i, h := range hops {
		a := Leg{Asset: asset(h.AssetIn), Amount: h.AmountIn.String()}
		b := Leg{Asset: asset(h.AssetOut), Amount: h.AmountOut.String()}
		if a.Asset != prev.Asset || a.quantity() == nil || b.quantity() == nil ||
			(i == 0 && a.Amount != prev.Amount) || (i > 0 && !handoff(prev.quantity(), a.quantity())) {
			return Flow{}, nil
		}
		if i > 0 {
			amount := a.Amount
			if prev.quantity().Cmp(a.quantity()) < 0 {
				amount = prev.Amount
			}
			switch a.Asset {
			case Native:
				if f.Gram == "" {
					f.Gram = amount
				}
			case USDT:
				if f.USDT == "" {
					f.USDT = amount
				}
			}
		}
		prev = b
	}
	if prev.Asset != out.Asset || prev.Amount != out.Amount {
		return Flow{}, nil
	}
	return f, nil
}
