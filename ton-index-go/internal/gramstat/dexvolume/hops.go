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
		if a.Asset != prev.Asset || a.Amount != prev.Amount || a.quantity() == nil || b.quantity() == nil {
			return Flow{}, nil
		}
		if i > 0 {
			switch a.Asset {
			case Native:
				if f.Gram == "" {
					f.Gram = a.Amount
				}
			case USDT:
				if f.USDT == "" {
					f.USDT = a.Amount
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
