package crud

import (
	"github.com/ton-blockchain/acton/packages/abi-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/acton/catalog"
	"github.com/toncenter/ton-indexer/ton-index-go/index/actonapi"
	"github.com/toncenter/ton-indexer/ton-index-go/index/detect"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

// TraceCodeBook describes every code hash the given traces mention. It reads only
// the account states already populated on each transaction, with no I/O and no
// getter calls, so pending traces, whose writers supply no per-state code hashes,
// contribute nothing.
func TraceCodeBook(traces []models.Trace) models.CodeBook {
	var hashes []models.HashType
	for i := range traces {
		for _, transaction := range traces[i].Transactions {
			hashes = appendCodeHashes(hashes, transaction)
		}
		// A trace response may embed its transactions in the node tree instead.
		nodes := []*models.TraceNode{traces[i].Trace}
		for len(nodes) > 0 {
			node := nodes[len(nodes)-1]
			nodes = nodes[:len(nodes)-1]
			if node == nil {
				continue
			}
			hashes = appendCodeHashes(hashes, node.Transaction)
			nodes = append(nodes, node.Children...)
		}
	}
	return CodeBook(hashes)
}

// TransactionCodeBook describes the code each transaction ran before and after.
func TransactionCodeBook(transactions []models.Transaction) models.CodeBook {
	var hashes []models.HashType
	for i := range transactions {
		hashes = appendCodeHashes(hashes, &transactions[i])
	}
	return CodeBook(hashes)
}

// CodeBook resolves what the catalog and the interface table know about each of
// the given code hashes.
func CodeBook(hashes []models.HashType) models.CodeBook {
	return codeBook(hashes, catalog.ByCodeHash)
}

func appendCodeHashes(hashes []models.HashType, transaction *models.Transaction) []models.HashType {
	if transaction == nil {
		return hashes
	}
	for _, state := range [2]*models.AccountState{transaction.AccountStateBefore, transaction.AccountStateAfter} {
		if state != nil && state.CodeHash != nil {
			hashes = append(hashes, *state.CodeHash)
		}
	}
	return hashes
}

// codeBook keys each row by the exact spelling it was given, so a client looks a
// row up with the code_hash it can see, while the catalog is still consulted once
// per distinct code. Codes that neither source recognizes are left out entirely.
func codeBook(hashes []models.HashType, lookup func(string) []*acton.Contract) models.CodeBook {
	var book models.CodeBook
	resolved := map[models.HashType]models.CodeBookRow{}
	for _, hash := range hashes {
		key, err := models.ParseHashType(string(hash))
		if err != nil {
			continue
		}
		row, cached := resolved[*key]
		if !cached {
			row.Interfaces = detect.DetectInterface(string(*key), nil)
			for _, contract := range actonapi.OrderCandidates(lookup(string(*key))) {
				entry := models.CodeContract{CatalogID: contract.ID, DisplayName: contract.DisplayName}
				for _, link := range contract.Links {
					entry.Links = append(entry.Links, models.ContractLink{Kind: link.Kind, Title: link.Title, URL: link.URL})
				}
				row.Contracts = append(row.Contracts, entry)
			}
			resolved[*key] = row
		}
		if len(row.Interfaces) == 0 && len(row.Contracts) == 0 {
			continue
		}
		if book == nil {
			book = models.CodeBook{}
		}
		book[hash] = row
	}
	return book
}

// AccountCodeBook describes the code of each of the given account states.
func AccountCodeBook(states []models.AccountStateFull) models.CodeBook {
	var hashes []models.HashType
	for i := range states {
		if states[i].CodeHash != nil {
			hashes = append(hashes, *states[i].CodeHash)
		}
	}
	return CodeBook(hashes)
}
