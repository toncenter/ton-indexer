package actonapi

import (
	"cmp"
	"slices"

	"github.com/ton-blockchain/acton/packages/abi-go"
)

// OrderCandidates sorts the catalog entries claiming one code hash so that the
// most specific comes first, and every response that must name a single contract
// for that code names the same one. Twenty-one of the catalog's twenty-two
// ambiguous hashes are one contract entered twice under two vendor names, where
// the order is immaterial; the remaining one describes the same bytes at two
// depths, and the entry that decodes more of them wins.
func OrderCandidates(contracts []*acton.Contract) []*acton.Contract {
	ordered := slices.Clone(contracts)
	slices.SortStableFunc(ordered, func(a, b *acton.Contract) int {
		if n := cmp.Compare(len(b.GetMethods), len(a.GetMethods)); n != 0 {
			return n
		}
		if n := cmp.Compare(messageBindings(b), messageBindings(a)); n != 0 {
			return n
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return ordered
}

func messageBindings(contract *acton.Contract) int {
	total := 0
	for _, bindings := range contract.Messages {
		total += len(bindings)
	}
	return total
}
