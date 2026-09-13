package actonapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/ton-blockchain/acton/packages/abi-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/xssnick/tonutils-go/address"
)

type API struct {
	contracts []*acton.Contract
	revision  string
	byID      map[string][]*acton.Contract
	byHash    map[string][]*acton.Contract
	deps      Dependencies
}

func New(contracts []*acton.Contract, revision string, deps Dependencies) *API {
	a := &API{contracts: contracts, revision: revision, deps: deps, byID: map[string][]*acton.Contract{}, byHash: map[string][]*acton.Contract{}}
	for _, contract := range contracts {
		a.byID[contract.ID] = append(a.byID[contract.ID], contract)
		seen := map[string]bool{}
		for _, hash := range contract.CodeHashes {
			if key, err := acton.NormalizeCodeHash(hash); err == nil && !seen[key] {
				a.byHash[key] = append(a.byHash[key], contract)
				seen[key] = true
			}
		}
	}
	return a
}

// contractInfo renders one catalog entry. The compiler ABI is attached only for a
// selected entry: it is ten times the size of everything else about a contract.
func contractInfo(contract *acton.Contract, withABI bool) ActonContract {
	info := ActonContract{CatalogID: contract.ID, DisplayName: contract.DisplayName,
		CodeHashes:     append([]string{}, contract.CodeHashes...),
		KnownAddresses: append([]string{}, contract.KnownAddresses...),
		Links:          []models.ContractLink{}, GetMethods: []ActonGetMethod{}}
	for _, link := range contract.Links {
		info.Links = append(info.Links, models.ContractLink{Kind: link.Kind, Title: link.Title, URL: link.URL})
	}
	for _, method := range contract.GetMethods {
		info.GetMethods = append(info.GetMethods, methodInfo(method))
	}
	if withABI {
		info.ABI = contract.ABI
	}
	return info
}

func methodInfo(method acton.GetMethod) ActonGetMethod {
	rendered := ActonGetMethod{Name: method.Name, MethodID: method.ID, Return: method.Return.Name,
		Description: method.Description, Unsupported: method.Unsupported, Parameters: []ActonParameter{}}
	for _, parameter := range method.Parameters {
		rendered.Parameters = append(rendered.Parameters, ActonParameter{Name: parameter.Name, Type: parameter.Type.Name})
	}
	return rendered
}

func CanonicalAddress(value string) (string, error) {
	if len(value) > 128 {
		return "", Fail(422, "invalid address")
	}
	value = strings.TrimSpace(value)
	var addr *address.Address
	var err error
	if strings.Contains(value, ":") {
		addr, err = address.ParseRawAddr(value)
	} else {
		// ParseAddr checks CRC but accepts arbitrary tags and only base64url.
		// TEP-2 requires both alphabets and exactly these four flag bytes.
		if len(value) != 48 {
			return "", Fail(422, "invalid friendly address length")
		}
		data, decodeErr := base64.RawURLEncoding.Strict().DecodeString(strings.NewReplacer("+", "-", "/", "_").Replace(value))
		if decodeErr != nil || len(data) != 36 || data[0]&0x7f != 0x11 && data[0]&0x7f != 0x51 {
			return "", Fail(422, "invalid friendly address encoding or tag")
		}
		addr, err = address.ParseAddr(base64.RawURLEncoding.EncodeToString(data))
	}
	if err != nil || addr == nil || addr.Type() != address.StdAddress {
		return "", Fail(422, "invalid standard account address")
	}
	return strings.ToUpper(addr.StringRaw()), nil
}

func decodeJSON(data []byte, dst any) error {
	if len(data) > MaxBodyBytes {
		return Fail(413, "request body exceeds 1 MiB")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return Fail(422, "invalid JSON: "+err.Error())
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return Fail(422, "expected one JSON value")
	}
	return nil
}

func queryValues(c *fiber.Ctx, name string) []string {
	values := c.Context().QueryArgs().PeekMulti(name)
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, string(value))
	}
	return out
}

func (a *API) selectContracts(contractType, hash string) ([]*acton.Contract, error) {
	if (contractType == "") == (hash == "") {
		return nil, Fail(422, "provide exactly one of contract_type or code_hash")
	}
	if contractType != "" {
		return a.byID[contractType], nil
	}
	key, err := codeHashKey(hash)
	if err != nil {
		return nil, err
	}
	return a.byHash[key], nil
}

// codeHashKey rejects padding the normalizer would otherwise tolerate, so two
// spellings of one hash cannot arrive as two selectors.
func codeHashKey(hash string) (string, error) {
	if len(hash) > 66 || strings.TrimSpace(hash) != hash || strings.ContainsAny(hash, " \t\r\n\v\f") {
		return "", Fail(422, "invalid code_hash")
	}
	key, err := acton.NormalizeCodeHash(hash)
	if err != nil {
		return "", Fail(422, "invalid code_hash")
	}
	return key, nil
}

func unique(contracts []*acton.Contract) (*acton.Contract, error) {
	if len(contracts) == 0 {
		return nil, Fail(404, "contract is not in the catalog")
	}
	if len(contracts) != 1 {
		// Identical bytecode does not make two catalog entries interchangeable:
		// they can declare different getters and different storage meanings for
		// the same bits. Report the candidates so the caller can pick one with
		// contract_type instead of guessing.
		ids := make([]string, 0, len(contracts))
		for _, c := range contracts {
			ids = append(ids, c.ID)
		}
		return nil, models.IndexError{Code: 409, Message: "ambiguous catalog selection: " + strings.Join(ids, ", "), Candidates: ids}
	}
	return contracts[0], nil
}

// Contracts is the whole catalog when no selector is given, and only the named
// entries, each with its compiler ABI, when one is.
// @Summary List Acton contracts
// @Description Without a selector this is the entire pinned catalog without type tables. With code_hash or catalog_id it is the matching entries, each carrying its full compiler ABI. Results are deduplicated and keep selector order; unknown selectors match nothing. Identification is bytecode-hash matching, not source verification.
// @Tags acton
// @Produce json
// @Param code_hash query []string false "Code hashes; at most 50 selectors in total" collectionFormat(multi)
// @Param catalog_id query []string false "Catalog IDs; at most 50 selectors in total" collectionFormat(multi)
// @Param limit query int false "Page size; the whole result by default" minimum(1) maximum(1000)
// @Param offset query int false "Rows to skip" default(0) minimum(0)
// @Success 200 {object} ActonContractsResponse
// @Failure 413 {object} models.IndexError
// @Failure 422 {object} models.IndexError
// @Router /api/v3/acton/contracts [get]
// @Security APIKeyHeader
// @Security APIKeyQuery
func (a *API) Contracts(c *fiber.Ctx) error {
	hashes, ids := queryValues(c, "code_hash"), queryValues(c, "catalog_id")
	selected := len(hashes)+len(ids) > 0
	if len(hashes)+len(ids) > MaxSelectors {
		return Fail(422, "provide at most 50 code_hash and catalog_id selectors")
	}
	contracts := a.contracts
	if selected {
		contracts = nil
		seen := map[*acton.Contract]bool{}
		add := func(matches []*acton.Contract) {
			for _, contract := range OrderCandidates(matches) {
				if !seen[contract] {
					seen[contract] = true
					contracts = append(contracts, contract)
				}
			}
		}
		for _, id := range ids {
			add(a.byID[id])
		}
		for _, hash := range hashes {
			key, err := codeHashKey(hash)
			if err != nil {
				return err
			}
			add(a.byHash[key])
		}
	}
	limit := len(contracts)
	if raw := c.Query("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > MaxBatch {
			return Fail(422, "limit must be between 1 and 1000")
		}
		limit = parsed
	}
	offset, err := strconv.Atoi(c.Query("offset", "0"))
	if err != nil || offset < 0 {
		return Fail(422, "offset must be nonnegative")
	}
	start := min(offset, len(contracts))
	end := start + min(limit, len(contracts)-start)
	response := ActonContractsResponse{Contracts: []ActonContract{}, Total: len(contracts), Limit: limit, Offset: offset}
	for _, contract := range contracts[start:end] {
		response.Contracts = append(response.Contracts, contractInfo(contract, selected))
	}
	return a.sendBounded(c, response)
}

// Decode uses native generated bindings for an explicitly selected ABI.
// @Summary Decode Acton storage or message body
// @Description Select exactly one catalog type or code hash. Direction is storage, deployment_storage, incoming_messages, incoming_external, outgoing_messages, or emitted_events.
// @Tags acton
// @Accept json
// @Produce json
// @Param request body DecodeRequest true "Explicit ABI selector and BOC"
// @Success 200 {object} DecodeResponse
// @Failure 409 {object} models.IndexError
// @Failure 422 {object} models.IndexError
// @Router /api/v3/acton/decode [post]
// @Security APIKeyHeader
// @Security APIKeyQuery
func (a *API) Decode(c *fiber.Ctx) error {
	var req DecodeRequest
	if err := decodeJSON(c.Body(), &req); err != nil {
		return err
	}
	contracts, err := a.selectContracts(req.ContractType, req.CodeHash)
	if err != nil {
		return err
	}
	contract, err := unique(contracts)
	if err != nil {
		return err
	}
	if req.Body == "" {
		return Fail(422, "body BOC is required")
	}
	response := DecodeResponse{CatalogID: contract.ID, Direction: req.Direction}
	if req.Direction == "storage" || req.Direction == "deployment_storage" {
		binding := contract.Storage
		if req.Direction == "deployment_storage" {
			binding = contract.DeploymentStorage
		}
		if binding == nil {
			return Fail(422, "storage binding unavailable")
		}
		response.Type = binding.Type
		// One caller-supplied BOC, so a per-call budget rather than a shared one.
		response.Decoded, err = decodeBinding(binding, req.Body)
		if err != nil {
			return Fail(422, err.Error())
		}
	} else {
		if req.Direction == "" || len(contract.Messages[req.Direction]) == 0 {
			return Fail(422, "message direction is not in the selected ABI")
		}
		decoded, err := acton.DecodeMessage(contract, req.Direction, req.Body)
		if err != nil {
			return Fail(422, err.Error())
		}
		if decoded == nil {
			return Fail(422, "no matching message binding")
		}
		response.Type, response.Decoded = decoded.Type, decoded.Value
	}
	return a.sendBounded(c, response)
}

// RunGetMethod selects an ABI using code read at the execution seqno, never from
// the latest indexed state. An explicit contract must also match that code hash.
// @Summary Run and decode a pinned Acton getter
// @Description Requires positive seqno or resolves it once. Reads account code and executes runGetMethodStd at the same seqno; library code and implementation hashes stay distinct. Standard Tonlib null and Lisp lists are supported; builder, NaN and continuation values are not. Pinning trusts the configured upstream, not a proof. Args and raw stack are mutually exclusive. VM and decoding failures retain raw stack, gas and exit code.
// @Tags acton
// @Accept json
// @Produce json
// @Param request body RunRequest true "Address, method name or numeric TVM ID, named args or typed stack, optional seqno"
// @Success 200 {object} RunResponse
// @Failure 409 {object} models.IndexError
// @Failure 422 {object} models.IndexError
// @Failure 502 {object} models.IndexError
// @Router /api/v3/acton/runGetMethod [post]
// @Security APIKeyHeader
// @Security APIKeyQuery
func (a *API) RunGetMethod(c *fiber.Ctx) error {
	var req RunRequest
	if err := decodeJSON(c.Body(), &req); err != nil {
		return err
	}
	addr, err := CanonicalAddress(req.Address)
	if err != nil {
		return err
	}
	if req.Seqno != nil && *req.Seqno <= 0 {
		return Fail(422, "seqno must be positive")
	}
	if req.ContractType != "" && req.CodeHash != "" {
		return Fail(422, "contract_type and code_hash are mutually exclusive")
	}
	if len(req.Args) != 0 && len(req.Stack) != 0 {
		return Fail(422, "args and stack are mutually exclusive")
	}
	var explicitContract *acton.Contract
	requestHash := ""
	if req.ContractType != "" {
		selected, err := a.selectContracts(req.ContractType, req.CodeHash)
		if err != nil {
			return err
		}
		explicitContract, err = unique(selected)
		if err != nil {
			return err
		}
	} else if req.CodeHash != "" {
		if len(req.CodeHash) > 66 || strings.TrimSpace(req.CodeHash) != req.CodeHash || strings.ContainsAny(req.CodeHash, " \t\r\n\v\f") {
			return Fail(422, "invalid code_hash")
		}
		requestHash, err = acton.NormalizeCodeHash(req.CodeHash)
		if err != nil {
			return Fail(422, "invalid code_hash")
		}
	}
	var methodName string
	var methodID int64
	byName := false
	switch method := req.Method.(type) {
	case string:
		if method == "" || len(method) > 256 {
			return Fail(422, "invalid method")
		}
		methodID, err = strconv.ParseInt(method, 10, 32)
		if err != nil {
			methodName, byName = method, true
		}
	case json.Number:
		methodID, err = strconv.ParseInt(string(method), 10, 32)
		if err != nil {
			return Fail(422, "method ID must be an int32")
		}
	default:
		return Fail(422, "method must be a name or int32 ID")
	}
	args := map[string]any{}
	var stack []acton.StackValue
	if len(req.Args) != 0 {
		if err := decodeJSON(req.Args, &args); err != nil {
			return err
		}
		if args == nil {
			return Fail(422, "args must be an object")
		}
	}
	if len(req.Stack) != 0 {
		if err := decodeJSON(req.Stack, &stack); err != nil {
			return err
		}
		if stack == nil {
			return Fail(422, "stack must be an array")
		}
		if err := ValidateStack(stack); err != nil {
			return Fail(422, err.Error())
		}
	}
	if a.deps.Executor == nil {
		return Fail(503, "getter execution unavailable")
	}
	executor := a.deps.Executor(c)
	if executor == nil {
		return Fail(503, "getter execution unavailable")
	}
	snapshot, err := executor.Snapshot(c.UserContext(), addr, req.Seqno)
	if err != nil {
		return err
	}
	if snapshot == nil || snapshot.CodeHash == nil || snapshot.Seqno == nil || *snapshot.Seqno <= 0 {
		return Fail(502, "upstream did not provide pinned account code")
	}
	if snapshot.Address != addr || req.Seqno != nil && *snapshot.Seqno != *req.Seqno {
		return Fail(502, "upstream snapshot selector mismatch")
	}
	key, err := acton.NormalizeCodeHash(*snapshot.CodeHash)
	if err != nil {
		return Fail(502, "invalid upstream code hash")
	}
	contracts := a.byHash[key]
	implementationKey := ""
	if snapshot.ImplementationHash != nil {
		implementationKey, err = acton.NormalizeCodeHash(*snapshot.ImplementationHash)
		if err != nil {
			return Fail(502, "invalid upstream library implementation hash")
		}
		// Do not let two different catalog matches silently override each other.
		contracts = append([]*acton.Contract{}, contracts...)
		for _, candidate := range a.byHash[implementationKey] {
			found := false
			for _, existing := range contracts {
				if candidate == existing {
					found = true
					break
				}
			}
			if !found {
				contracts = append(contracts, candidate)
			}
		}
	}
	if requestHash != "" {
		if requestHash != key && requestHash != implementationKey {
			return Fail(409, "selected code_hash does not match account code or library implementation at execution seqno")
		}
		if selected := a.byHash[requestHash]; len(selected) > 0 {
			contracts = selected
		}
	}
	if explicitContract != nil {
		matched := false
		for _, candidate := range contracts {
			if candidate == explicitContract {
				matched = true
			}
		}
		if !matched {
			return Fail(409, "selected ABI does not match account code at execution seqno")
		}
		contracts = []*acton.Contract{explicitContract}
	}
	contract, err := unique(contracts)
	if err != nil {
		return err
	}
	identification := "exact_code_hash"
	matchedCode := false
	for _, candidate := range a.byHash[key] {
		if candidate == contract {
			matchedCode = true
		}
	}
	if !matchedCode && implementationKey != "" {
		identification = "library_reference"
	}
	var method *acton.GetMethod
	for i := range contract.GetMethods {
		m := &contract.GetMethods[i]
		if byName && m.Name == methodName || !byName && m.ID == methodID {
			if method != nil {
				return Fail(409, "ambiguous getter in catalog")
			}
			method = m
		}
	}
	if method == nil {
		return Fail(422, "method is not in selected catalog contract")
	}
	for i := range contract.GetMethods {
		other := &contract.GetMethods[i]
		if other != method && other.ID == method.ID {
			return Fail(409, "ambiguous TVM method ID in catalog")
		}
	}
	if len(req.Stack) == 0 {
		if method.Unsupported != "" {
			return Fail(422, method.Unsupported)
		}
		if method.EncodeArgs == nil {
			return Fail(422, "native argument encoder unavailable; use raw stack")
		}
		allowed := map[string]bool{}
		for _, p := range method.Parameters {
			allowed[p.Name] = true
		}
		for name := range args {
			if !allowed[name] {
				return Fail(422, "unknown argument: "+name)
			}
		}
		stack, err = method.EncodeArgs(args)
		if err != nil {
			return Fail(422, err.Error())
		}
	}
	stack, err = NormalizeStack(stack)
	if err != nil {
		return Fail(422, err.Error())
	}
	execution, err := executor.Run(c.UserContext(), snapshot, method.ID, stack)
	if err != nil {
		return err
	}
	if execution == nil {
		return Fail(502, "empty getter execution result")
	}
	if execution.StackError == "" {
		execution.Stack, err = NormalizeStack(execution.Stack)
		if err != nil {
			execution.StackError = err.Error()
		}
	}
	response := RunResponse{Execution: *execution, Snapshot: *snapshot, CatalogID: contract.ID, Method: methodInfo(*method), Identification: identification, Success: execution.ExitCode == 0 || execution.ExitCode == 1}
	switch {
	case !response.Success:
		response.DecodeError = fmt.Sprintf("VM exited with code %d", execution.ExitCode)
	case execution.StackError != "":
		response.DecodeError = execution.StackError
	case method.Unsupported != "":
		response.DecodeError = method.Unsupported
	case method.DecodeResult == nil:
		response.DecodeError = "native result decoder unavailable"
	default:
		response.Decoded, err = method.DecodeResult(execution.Stack)
		if err != nil {
			response.Decoded = nil
			response.DecodeError = err.Error()
		}
	}
	return a.sendBounded(c, response)
}

func decodeBinding(binding *acton.Binding, boc string) (any, error) {
	if binding.Unsupported != "" {
		return nil, fmt.Errorf("unsupported storage: %s", binding.Unsupported)
	}
	if binding.Decode == nil {
		return nil, errors.New("native storage decoder unavailable")
	}
	root, err := acton.DecodeBOC(boc)
	if err != nil {
		return nil, err
	}
	return binding.Decode(root)
}

// sendBounded serializes once and rejects the exact encoded size, so a caller
// that batches too much gets 413 instead of a multi-megabyte body. A GET response
// here is a pure function of the pinned catalog and the request, so its digest is
// a strong validator and a client that already holds the body revalidates for the
// cost of a header.
func (a *API) sendBounded(c *fiber.Ctx, response any) error {
	body, err := json.Marshal(response)
	if err != nil {
		return Fail(502, "response cannot be serialized")
	}
	if len(body) > MaxMetadataBytes {
		return Fail(413, "response exceeds 8 MiB; reduce the batch or page size")
	}
	c.Set("X-Acton-Catalog-Revision", a.revision)
	if c.Method() == fiber.MethodGet {
		tag := fmt.Sprintf("%q", fmt.Sprintf("%x", sha256.Sum256(body)))
		c.Set(fiber.HeaderETag, tag)
		if strings.Contains(c.Get(fiber.HeaderIfNoneMatch), tag) {
			return c.SendStatus(fiber.StatusNotModified)
		}
	}
	c.Type("json")
	return c.Send(body)
}
