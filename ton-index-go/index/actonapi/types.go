// Package actonapi is the HTTP surface of the Acton endpoints: request and
// response shapes, and the handlers that read them. Everything it decides with
// lives in index/acton.
package actonapi

import (
	"encoding/json"
	"github.com/toncenter/ton-indexer/ton-index-go/index/acton"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"

	"github.com/gofiber/fiber/v2"
	"github.com/ton-blockchain/tolk-abi-to-go"
)

const MaxBatch = 1000
const MaxMetadataBytes = 8 << 20

// A selected contract carries its compiler ABI, which is 12 KiB for a median
// catalog entry and 74 KiB for the largest, so the selector count is bounded far
// below the batch limits that apply to hashes and addresses elsewhere.
const MaxSelectors = 50

// ActonParameter renders its type as a name rather than as an index into a type
// table. No getter in the catalog takes a structural parameter, and a caller that
// needs the type graph reads the contract's abi, whose own get_methods carry the
// indexes.
type ActonParameter struct {
	Name string `json:"name"`
	Type string `json:"type"`
} // @name ActonParameter

type ActonGetMethod struct {
	Name        string           `json:"name"`
	MethodID    int64            `json:"method_id"`
	Parameters  []ActonParameter `json:"parameters"`
	Return      string           `json:"return"`
	Description string           `json:"description,omitempty"`
	Unsupported string           `json:"unsupported,omitempty"`
} // @name ActonGetMethod

// ActonContract carries abi only when the request named a selector: the whole
// catalog is 450 KiB of identity and getters against 4.5 MiB of type tables.
// Links are catalog assertions, not evidence of source verification. Code hashes
// are base64 and known addresses are raw, as everywhere else in v3, whatever
// spelling the catalog stores them in.
type ActonContract struct {
	CatalogID      string                `json:"catalog_id"`
	DisplayName    string                `json:"display_name"`
	CodeHashes     []string              `json:"code_hashes"`
	KnownAddresses []string              `json:"known_addresses"`
	Links          []models.ContractLink `json:"links"`
	GetMethods     []ActonGetMethod      `json:"get_methods"`
	ABI            json.RawMessage       `json:"abi,omitempty" swaggertype:"object"`
} // @name ActonContract

type ActonContractsResponse struct {
	Contracts []ActonContract `json:"contracts"`
	Total     int             `json:"total"`
	Limit     int             `json:"limit"`
	Offset    int             `json:"offset"`
} // @name ActonContractsResponse

type DecodeRequest struct {
	CatalogID string `json:"catalog_id"`
	CodeHash  string `json:"code_hash"`
	Direction string `json:"direction"`
	Body      string `json:"body"`
} // @name ActonDecodeRequest

type DecodeResponse struct {
	CatalogID string           `json:"catalog_id"`
	Direction string           `json:"direction"`
	Type      tolkabi.TypeInfo `json:"type"`
	Decoded   any              `json:"decoded"`
} // @name ActonDecodeResponse

// RunRequest names the getter and nothing about its ABI: the catalog entry is the
// one declaring that getter for the code running at the execution seqno.
type RunRequest struct {
	Address string          `json:"address"`
	Method  string          `json:"method"`
	Args    json.RawMessage `json:"args,omitempty" swaggertype:"object"`
	McSeqno *int32          `json:"mc_seqno,omitempty"`
} // @name ActonRunRequest

type RunResponse struct {
	acton.Execution
	Snapshot       acton.Snapshot `json:"snapshot"`
	CatalogID      string         `json:"catalog_id"`
	Method         ActonGetMethod `json:"method"`
	Identification string         `json:"identification"`
	Success        bool           `json:"success"`
	Decoded        any            `json:"decoded"`
	DecodeError    string         `json:"decode_error,omitempty"`
} // @name ActonRunResponse

type Dependencies struct {
	Executor func(*fiber.Ctx) acton.GetterExecutor
}
