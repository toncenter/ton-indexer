package crud

import (
	"cmp"
	"slices"

	"github.com/ton-blockchain/acton/packages/abi-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/acton/catalog"
	"github.com/toncenter/ton-indexer/ton-index-go/index/detect"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

// EnrichTraceContracts must run after account states have been populated. It uses
// only each transaction's before/after code hashes, with no I/O or getter calls.
// Pending writers currently supply no per-state code hashes, so pending traces
// get no contract_info.
func EnrichTraceContracts(traces []models.Trace) {
	enrichTraceContracts(traces, catalog.ByCodeHash)
}

func enrichTraceContracts(traces []models.Trace, lookup func(string) []*acton.Contract) {
	// Cache hits and misses across the entire response, bounded by encountered hashes.
	cache := make(map[models.HashType]*models.ContractTypeSummary)
	for i := range traces {
		trace := &traces[i]
		trace.ContractInfo = nil
		accounts := make(map[models.AccountAddress]map[models.HashType]bool)
		addTransaction := func(tx *models.Transaction) {
			if tx == nil {
				return
			}
			for _, slot := range [2]**models.AccountState{&tx.AccountStateBefore, &tx.AccountStateAfter} {
				if *slot == nil {
					continue
				}
				// Pending transaction copies share states with their source context.
				// Keep trace-only links out of later non-trace responses.
				state := **slot
				state.ContractInfoKey = nil
				*slot = &state
				if state.CodeHash == nil {
					continue
				}
				hash, err := models.ParseHashType(string(*state.CodeHash))
				if err != nil {
					continue
				}
				summary, ok := cache[*hash]
				if !ok {
					summary = &models.ContractTypeSummary{Interfaces: detect.DetectInterface(string(*hash), nil)}
					for _, contract := range lookup(string(*hash)) {
						candidate := models.ContractCandidate{ID: contract.ID, DisplayName: contract.DisplayName}
						for _, link := range contract.Links {
							candidate.Links = append(candidate.Links, models.ContractLink{Kind: link.Kind, Title: link.Title, URL: link.URL})
						}
						summary.Candidates = append(summary.Candidates, candidate)
					}
					slices.SortFunc(summary.Candidates, func(a, b models.ContractCandidate) int {
						return cmp.Compare(a.ID, b.ID)
					})
					if len(summary.Interfaces) > 0 || len(summary.Candidates) > 0 {
						summary.Match = "code_hash"
					}
					cache[*hash] = summary
				}
				if trace.ContractInfo == nil {
					trace.ContractInfo = &models.TraceContractInfo{
						CatalogRevision: catalog.Revision,
						ByCodeHash:      make(map[models.HashType]*models.ContractTypeSummary),
						Accounts:        make(map[models.AccountAddress][]models.HashType),
					}
				}
				state.ContractInfoKey = hash
				trace.ContractInfo.ByCodeHash[*hash] = summary
				if accounts[tx.Account] == nil {
					accounts[tx.Account] = make(map[models.HashType]bool)
				}
				accounts[tx.Account][*hash] = true
			}
		}
		for _, tx := range trace.Transactions {
			addTransaction(tx)
		}
		// Also support trace responses with embedded transactions instead of a map.
		nodes := []*models.TraceNode{trace.Trace}
		for len(nodes) > 0 {
			node := nodes[len(nodes)-1]
			nodes = nodes[:len(nodes)-1]
			if node == nil {
				continue
			}
			addTransaction(node.Transaction)
			nodes = append(nodes, node.Children...)
		}
		for account, hashes := range accounts {
			list := make([]models.HashType, 0, len(hashes))
			for hash := range hashes {
				list = append(list, hash)
			}
			slices.Sort(list)
			trace.ContractInfo.Accounts[account] = list
		}
	}
}
