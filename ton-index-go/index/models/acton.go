package models

type ContractLink struct {
	Kind  string `json:"kind"`
	Title string `json:"title"`
	URL   string `json:"url"`
} // @name ContractLink

// CodeContract is a catalog label for a code hash, not proof of source verification.
type CodeContract struct {
	CatalogID   string         `json:"catalog_id"`
	DisplayName string         `json:"display_name"`
	Links       []ContractLink `json:"links,omitempty"`
} // @name CodeContract

// CodeBookRow describes one code hash. Contracts are ordered most specific first,
// so a decoder uses the head; the rest are other catalog entries claiming the same
// bytecode. A row exists only for code the catalog knows.
type CodeBookRow struct {
	Contracts []CodeContract `json:"contracts,omitempty"`
} // @name CodeBookRow

// CodeBook maps code hashes to what is known about them. Keys are spelled exactly
// as the account states in the same response spell them, so a client indexes it
// with account_state.code_hash. It describes code and never an account: a hash
// here was seen somewhere in this response, which says nothing about which account
// currently runs it.
type CodeBook map[HashType]CodeBookRow // @name CodeBook
