// Package actonapi exposes catalog-backed ABI operations without importing the
// database or the legacy native marker. All state reads and execution are injected.
package actonapi

import (
	"context"
	"encoding/json"

	"github.com/gofiber/fiber/v2"
	"github.com/toncenter/ton-indexer/ton-index-go/index/acton"
)

const MaxBatch = 1000
const MaxBodyBytes = 1 << 20
const MaxMetadataBytes = 8 << 20
const MaxStorageAccounts = 8
const MaxStorageBatchBytes = 8 << 20

type Error struct {
	Code    int    `json:"code"`
	Message string `json:"error"`
}

func (e *Error) Error() string { return e.Message }

func Fail(code int, message string) error { return &Error{Code: code, Message: message} }

type Link struct {
	Kind  string `json:"kind"`
	Title string `json:"title"`
	URL   string `json:"url"`
}

type ContractSummary struct {
	CatalogID       string   `json:"catalog_id"`
	DisplayName     string   `json:"display_name"`
	CodeHashes      []string `json:"code_hashes"`
	KnownAddresses  []string `json:"known_addresses"`
	Links           []Link   `json:"links"`
	LinksProvenance string   `json:"links_provenance"`
	SourceVerified  bool     `json:"source_verified"`
}

type ExtendedContractABI struct {
	ContractSummary
	CompilerABI json.RawMessage `json:"compiler_abi" swaggertype:"object"`
}

type ContractsResponse struct {
	Contracts             []ContractSummary     `json:"contracts"`
	Revision              string                `json:"revision"`
	Total                 int                   `json:"total"`
	Limit                 int                   `json:"limit"`
	Offset                int                   `json:"offset"`
	TransportCapabilities []TransportCapability `json:"transport_capabilities"`
}

// AccountState is the narrow database adapter DTO. Missing rows are not_found;
// Error represents a row-specific failure. DataBOC is only requested for storage.
type AccountState struct {
	Address                                string
	Status                                 string
	StateHash, CodeHash, DataHash          *string
	LastTransactionHash, LastTransactionLT *string
	DataBOC                                *string
	Interfaces                             []string
	Error                                  string
}

type Snapshot struct {
	Address             string          `json:"address"`
	AccountStatus       string          `json:"account_status"`
	AccountStateHash    *string         `json:"account_state_hash"`
	CodeHash            *string         `json:"code_hash"`
	ImplementationHash  *string         `json:"implementation_hash,omitempty"`
	DataHash            *string         `json:"data_hash"`
	LastTransactionHash *string         `json:"last_transaction_hash"`
	LastTransactionLT   *string         `json:"last_transaction_lt"`
	Seqno               *int32          `json:"seqno,omitempty"`
	BlockID             json.RawMessage `json:"block_id,omitempty" swaggertype:"object"`
	Pinning             string          `json:"pinning"`
	ProofVerified       bool            `json:"proof_verified"`
}

type Identification struct {
	Type       string           `json:"type"`
	Provenance string           `json:"provenance"`
	Contract   *ContractSummary `json:"contract,omitempty"`
}

type StorageResult struct {
	Type    acton.TypeInfo `json:"type"`
	Decoded any            `json:"decoded"`
	Error   string         `json:"error,omitempty"`
}

type Account struct {
	Snapshot
	Status  string                   `json:"status"`
	Types   []Identification         `json:"types"`
	Storage map[string]StorageResult `json:"storage,omitempty"`
	Error   string                   `json:"error,omitempty"`
}

type AccountsRequest struct {
	Addresses      []string `json:"addresses"`
	IncludeStorage bool     `json:"include_storage"`
}

type AccountsResponse struct {
	Accounts []Account `json:"accounts"`
	Revision string    `json:"revision"`
}

type Parameter struct {
	Name    string          `json:"name"`
	Type    acton.TypeInfo  `json:"type"`
	Default json.RawMessage `json:"default,omitempty" swaggertype:"object"`
}

type GetMethod struct {
	Name        string         `json:"name"`
	ID          int64          `json:"id"`
	Parameters  []Parameter    `json:"parameters"`
	Return      acton.TypeInfo `json:"return"`
	Description string         `json:"description,omitempty"`
	Unsupported string         `json:"unsupported,omitempty"`
}

type ContractMethods struct {
	ExtendedContractABI
	GetMethods []GetMethod `json:"get_methods"`
}

type GetMethodsResponse struct {
	Contracts             []ContractMethods     `json:"contracts"`
	Account               *Account              `json:"account,omitempty"`
	Revision              string                `json:"revision"`
	TransportCapabilities []TransportCapability `json:"transport_capabilities"`
}

type DecodeRequest struct {
	ContractType string `json:"contract_type"`
	CodeHash     string `json:"code_hash"`
	Direction    string `json:"direction"`
	Body         string `json:"body"`
}

type DecodeResponse struct {
	CatalogID string         `json:"catalog_id"`
	Direction string         `json:"direction"`
	Type      acton.TypeInfo `json:"type"`
	Decoded   any            `json:"decoded"`
	Revision  string         `json:"revision"`
}

type RunRequest struct {
	Address      string          `json:"address"`
	ContractType string          `json:"contract_type,omitempty"`
	CodeHash     string          `json:"code_hash,omitempty"`
	Method       any             `json:"method" swaggertype:"string"`
	Args         json.RawMessage `json:"args,omitempty" swaggertype:"object"`
	Stack        json.RawMessage `json:"stack,omitempty" swaggertype:"array,object"`
	Seqno        *int32          `json:"seqno,omitempty"`
}

// Execution retains the untouched standard stack even if conversion or ABI
// decoding fails. StackError must prevent typed decoding, not hide VM results.
type Execution struct {
	Stack      []acton.StackValue `json:"stack"`
	RawStack   json.RawMessage    `json:"raw_stack" swaggertype:"array,object"`
	GasUsed    string             `json:"gas_used"`
	ExitCode   int32              `json:"exit_code"`
	StackError string             `json:"stack_error,omitempty"`
	Transport  string             `json:"transport"`
}

type RunResponse struct {
	Execution
	Snapshot       Snapshot  `json:"snapshot"`
	CatalogID      string    `json:"catalog_id"`
	Method         GetMethod `json:"method"`
	Identification string    `json:"identification"`
	Success        bool      `json:"success"`
	Decoded        any       `json:"decoded"`
	DecodeError    string    `json:"decode_error,omitempty"`
	Revision       string    `json:"revision"`
}

type GetterExecutor interface {
	Snapshot(context.Context, string, *int32) (*Snapshot, error)
	Run(context.Context, *Snapshot, int64, []acton.StackValue) (*Execution, error)
}

type TransportCapability struct {
	Name         string            `json:"name"`
	Endpoint     string            `json:"endpoint"`
	Default      bool              `json:"default"`
	InputTypes   []string          `json:"input_types"`
	InputAliases map[string]string `json:"input_aliases"`
	OutputTypes  []string          `json:"output_types"`
	Warnings     []string          `json:"warnings"`
}

// These are wire capabilities, not promises that every generated getter is
// executable on every upstream. In particular, Unsupported is never null.
func TransportCapabilities() []TransportCapability {
	return []TransportCapability{
		{Name: "standard", Endpoint: "runGetMethodStd", Default: true,
			InputTypes:   []string{"int", "cell", "slice", "tuple", "null"},
			InputAliases: map[string]string{"num": "int", "list": "Lisp list encoded as tuple pairs and null"},
			OutputTypes:  []string{"int", "cell", "slice", "tuple", "null"},
			Warnings: []string{
				"Requires Tonlib tvm.list semantics: a flattened Lisp list, including empty list as null. Some Rust/localnet adapters instead interpret lists as tuples and cannot execute null arguments correctly.",
				"Some upstreams serialize null, builder, NaN or continuation results as tvm.stackEntryUnsupported. Such results cannot be decoded losslessly; raw_stack and stack_error are returned.",
				"Builder, NaN and continuation inputs are not supported by the standard wire schema. Native codec availability does not guarantee transport support.",
			}},
	}
}

type Dependencies struct {
	QueryAccounts func(*fiber.Ctx, []string, bool) ([]AccountState, error)
	Executor      func(*fiber.Ctx) GetterExecutor
}
