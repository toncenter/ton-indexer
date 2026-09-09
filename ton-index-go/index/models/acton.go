package models

type ContractLink struct {
	Kind  string `json:"kind"`
	Title string `json:"title"`
	URL   string `json:"url"`
} // @name ContractLink

// ContractCandidate is a catalog label, not proof of source verification or trust.
type ContractCandidate struct {
	ID          string         `json:"id"`
	DisplayName string         `json:"display_name"`
	Links       []ContractLink `json:"links,omitempty"`
} // @name ContractCandidate

// ContractTypeSummary contains only exact code-hash matches. All candidates are
// retained when a hash is ambiguous. An empty object means an unknown code hash.
type ContractTypeSummary struct {
	Match      string              `json:"match,omitempty"`
	Interfaces []string            `json:"interfaces,omitempty"`
	Candidates []ContractCandidate `json:"candidates,omitempty"`
} // @name ContractTypeSummary

// TraceContractInfo describes only states present in this trace, never the latest
// account state. Keys and account hash lists use padded standard base64; the
// original code_hash field is left as the caller spelled it. Index ByCodeHash with
// a state's contract_info_key. Lists are sorted and deduplicated, not
// chronological. Missing state/code hashes are omitted.
type TraceContractInfo struct {
	CatalogRevision string                            `json:"catalog_revision"`
	ByCodeHash      map[HashType]*ContractTypeSummary `json:"by_code_hash"`
	Accounts        map[AccountAddress][]HashType     `json:"accounts"`
} // @name TraceContractInfo
