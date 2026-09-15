package actonapi

import (
	"cmp"
	"slices"

	"github.com/ton-blockchain/tolk-abi-to-go"
)

// OrderCandidates sorts the catalog entries claiming one code hash so that the
// most specific comes first, and every response that must name a single contract
// for that code names the same one. Twenty-one of the catalog's twenty-two
// ambiguous hashes are one contract entered twice under two vendor names, where
// the order is immaterial; the remaining one describes the same bytes at two
// depths, and the entry that decodes more of them wins.
func OrderCandidates(contracts []*tolkabi.Contract) []*tolkabi.Contract {
	ordered := slices.Clone(contracts)
	slices.SortStableFunc(ordered, func(a, b *tolkabi.Contract) int {
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

func messageBindings(contract *tolkabi.Contract) int {
	total := 0
	for _, bindings := range contract.Messages {
		total += len(bindings)
	}
	return total
}

// selectMethod picks the first entry of an already ordered candidate list that
// declares the requested getter, so a request needs no ABI selector of its own.
// Where several entries claim one code hash they agree on every getter they
// share: across the catalog's 333 hashes there is none whose candidates declare
// the same getter with different signatures. Candidates matched through a library
// implementation hash are a different contract, not another name for the same
// one, so the caller places them after the code-hash matches.
func selectMethod(contracts []*tolkabi.Contract, name string, id int64, byName bool) (*tolkabi.Contract, *tolkabi.GetMethod, error) {
	if len(contracts) == 0 {
		return nil, nil, Fail(404, "contract is not in the catalog")
	}
	for _, contract := range contracts {
		var found *tolkabi.GetMethod
		for i := range contract.GetMethods {
			method := &contract.GetMethods[i]
			if byName && method.Name == name || !byName && method.ID == id {
				if found != nil {
					return nil, nil, Fail(409, "ambiguous getter in catalog")
				}
				found = method
			}
		}
		if found == nil {
			continue
		}
		for i := range contract.GetMethods {
			if other := &contract.GetMethods[i]; other != found && other.ID == found.ID {
				return nil, nil, Fail(409, "ambiguous TVM method ID in catalog")
			}
		}
		return contract, found, nil
	}
	return nil, nil, Fail(422, "method is not declared by the account's catalog contract")
}
