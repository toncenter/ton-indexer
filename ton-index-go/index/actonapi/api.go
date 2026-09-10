package actonapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/ton-blockchain/acton/packages/abi-go"
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

func summary(c *acton.Contract) ContractSummary {
	links := make([]Link, 0, len(c.Links))
	for _, link := range c.Links {
		links = append(links, Link{Kind: link.Kind, Title: link.Title, URL: link.URL})
	}
	return ContractSummary{CatalogID: c.ID, DisplayName: c.DisplayName, CodeHashes: append([]string{}, c.CodeHashes...), KnownAddresses: append([]string{}, c.KnownAddresses...), Links: links, LinksProvenance: "catalog_asserted"}
}

func extended(c *acton.Contract) ExtendedContractABI {
	return ExtendedContractABI{ContractSummary: summary(c), CompilerABI: c.ABI}
}

func methodInfo(m acton.GetMethod) GetMethod {
	params := make([]Parameter, 0, len(m.Parameters))
	for _, p := range m.Parameters {
		params = append(params, Parameter{Name: p.Name, Type: p.Type, Default: p.Default})
	}
	return GetMethod{Name: m.Name, ID: m.ID, Parameters: params, Return: m.Return, Description: m.Description, Unsupported: m.Unsupported}
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
	if len(hash) > 66 || strings.TrimSpace(hash) != hash || strings.ContainsAny(hash, " \t\r\n\v\f") {
		return nil, Fail(422, "invalid code_hash")
	}
	key, err := acton.NormalizeCodeHash(hash)
	if err != nil {
		return nil, Fail(422, "invalid code_hash")
	}
	return a.byHash[key], nil
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
		return nil, &Error{Code: 409, Message: "ambiguous catalog selection: " + strings.Join(ids, ", "), Candidates: ids}
	}
	return contracts[0], nil
}

// Contracts lists bounded catalog summaries and never executes getters.
// @Summary List Acton contracts
// @Tags acton
// @Produce json
// @Param limit query int false "Page size (1-1000)" default(100) minimum(1) maximum(1000)
// @Param offset query int false "Rows to skip" default(0) minimum(0)
// @Success 200 {object} ContractsResponse
// @Failure 413 {object} Error
// @Failure 422 {object} Error
// @Router /api/v3/acton/contracts [get]
// @Security APIKeyHeader
// @Security APIKeyQuery
func (a *API) Contracts(c *fiber.Ctx) error {
	limit, err := strconv.Atoi(c.Query("limit", "100"))
	if err != nil || limit < 1 || limit > MaxBatch {
		return Fail(422, "limit must be between 1 and 1000")
	}
	offset, err := strconv.Atoi(c.Query("offset", "0"))
	if err != nil || offset < 0 {
		return Fail(422, "offset must be nonnegative")
	}
	start := min(offset, len(a.contracts))
	end := start + min(limit, len(a.contracts)-start)
	response := ContractsResponse{Contracts: []ContractSummary{}, Total: len(a.contracts), Limit: limit, Offset: offset, Revision: a.revision}
	for _, contract := range a.contracts[start:end] {
		response.Contracts = append(response.Contracts, summary(contract))
	}
	return sendBounded(c, response)
}

// ABI preserves the caller's hash keys. Each maps to every catalog entry
// claiming that code hash: empty for unknown, more than one when the catalog is
// ambiguous. It never picks one silently.
// @Summary Get Acton compiler ABIs by code hash
// @Tags acton
// @Produce json
// @Param code_hash query []string true "Up to 1000 code hashes; unknown values map to an empty list" collectionFormat(multi)
// @Success 200 {object} map[string][]ExtendedContractABI
// @Failure 413 {object} Error
// @Failure 422 {object} Error
// @Router /api/v3/acton/abi [get]
// @Security APIKeyHeader
// @Security APIKeyQuery
func (a *API) ABI(c *fiber.Ctx) error {
	hashes := queryValues(c, "code_hash")
	if len(hashes) == 0 || len(hashes) > MaxBatch {
		return Fail(422, "provide 1 to 1000 code_hash values")
	}
	result := make(map[string][]ExtendedContractABI, len(hashes))
	for _, hash := range hashes {
		if _, exists := result[hash]; exists {
			continue
		}
		contracts, err := a.selectContracts("", hash)
		if err != nil {
			return err
		}
		// 22 of the catalog's 333 code hashes are claimed by two entries, jetton
		// wallets among them. Return every candidate for that key rather than
		// failing the whole batch over one of its members.
		abis := make([]ExtendedContractABI, 0, len(contracts))
		for _, contract := range contracts {
			abis = append(abis, extended(contract))
		}
		result[hash] = abis
	}
	c.Set("X-Acton-Catalog-Revision", a.revision)
	return sendBounded(c, result)
}

func (a *API) accounts(c *fiber.Ctx, addresses []string, storage bool) ([]Account, error) {
	if len(addresses) == 0 || len(addresses) > MaxBatch {
		return nil, Fail(422, "provide 1 to 1000 addresses")
	}
	canonical := make([]string, 0, len(addresses))
	seen := map[string]bool{}
	for _, value := range addresses {
		addr, err := CanonicalAddress(value)
		if err != nil {
			return nil, err
		}
		if !seen[addr] {
			canonical = append(canonical, addr)
			seen[addr] = true
		}
	}
	if storage && len(canonical) > MaxStorageAccounts {
		return nil, Fail(422, "include_storage supports at most 8 unique addresses; use smaller storage batches")
	}
	if a.deps.QueryAccounts == nil {
		return nil, Fail(503, "account state query is unavailable")
	}
	rows, err := a.deps.QueryAccounts(c, canonical, storage)
	if err != nil {
		return nil, err
	}
	if len(rows) > len(canonical) {
		return nil, Fail(502, "account store returned more rows than requested")
	}
	responseBudget := MaxStorageBatchBytes - 8192
	if storage {
		for _, row := range rows {
			size := row.BOCBytes
			if row.DataBOC != nil && size < len(*row.DataBOC) {
				size = len(*row.DataBOC)
			}
			if size < 0 || size > responseBudget {
				return nil, Fail(413, "storage batch BOCs exceed aggregate 8 MiB budget")
			}
			responseBudget -= size
		}
	}
	storageDecodes := 0
	byAddress := map[string]AccountState{}
	for _, row := range rows {
		addr, err := CanonicalAddress(row.Address)
		if err != nil {
			return nil, Fail(502, "invalid address returned by account store")
		}
		if _, exists := byAddress[addr]; exists {
			return nil, Fail(502, "duplicate account state returned by account store")
		}
		byAddress[addr] = row
	}
	result := make([]Account, 0, len(canonical))
	for _, addr := range canonical {
		responseBudget -= 1024
		if responseBudget < 0 {
			return nil, Fail(413, "account response exceeds aggregate 8 MiB budget")
		}
		account := Account{Snapshot: Snapshot{Address: addr, Pinning: "indexed_account_state"}, Status: "not_found", Types: []Identification{}}
		row, found := byAddress[addr]
		if !found {
			result = append(result, account)
			continue
		}
		account.Snapshot.AccountStatus = row.Status
		account.AccountStateHash, account.CodeHash, account.DataHash = row.StateHash, row.CodeHash, row.DataHash
		account.LastTransactionHash, account.LastTransactionLT = row.LastTransactionHash, row.LastTransactionLT
		account.Status = "unknown"
		if row.Error != "" {
			account.Status = "error"
			account.Error = row.Error
			result = append(result, account)
			continue
		}
		var contracts []*acton.Contract
		if row.CodeHash != nil {
			key, err := acton.NormalizeCodeHash(*row.CodeHash)
			if err != nil {
				account.Status = "error"
				account.Error = "invalid stored code hash"
			} else {
				contracts = a.byHash[key]
			}
		}
		for _, contract := range contracts {
			info := summary(contract)
			account.Types = append(account.Types, Identification{Type: contract.ID, Provenance: "exact_code_hash", Contract: &info})
			if storage && contract.Storage != nil {
				if account.Storage == nil {
					account.Storage = map[string]StorageResult{}
				}
				decoded := StorageResult{Type: contract.Storage.Type}
				switch {
				case row.DataBOC == nil:
					decoded.Error = "account data BOC unavailable"
				case len(*row.DataBOC) > MaxBodyBytes:
					decoded.Error = "account data BOC exceeds size limit"
				default:
					if storageDecodes >= MaxStorageAccounts {
						return nil, Fail(413, "storage batch exceeds 8 native decode operations")
					}
					storageDecodes++
					// Decode the advertised current-storage binding explicitly;
					// DecodeStorage may fall back to deployment storage without
					// returning which type matched.
					value, err := decodeAccountStorage(contract.Storage, *row.DataBOC)
					if err != nil {
						decoded.Error = err.Error()
					} else {
						encoded, err := json.Marshal(value)
						if err != nil {
							return nil, Fail(502, "native storage decoder returned a non-JSON value")
						}
						responseBudget -= len(encoded)
						if responseBudget < 0 {
							return nil, Fail(413, "decoded storage exceeds aggregate 8 MiB budget")
						}
						decoded.Decoded = json.RawMessage(encoded)
					}
				}
				account.Storage[contract.ID] = decoded
			}
		}
		interfaces := map[string]bool{}
		for _, iface := range row.Interfaces {
			if !interfaces[iface] {
				account.Types = append(account.Types, Identification{Type: iface, Provenance: "public_interface"})
				interfaces[iface] = true
			}
		}
		if len(account.Types) > 0 && account.Status != "error" {
			account.Status = "identified"
		}
		result = append(result, account)
	}
	return result, nil
}

func decodeAccountStorage(binding *acton.Binding, boc string) (any, error) {
	if binding.Unsupported != "" {
		return nil, fmt.Errorf("unsupported storage: %s", binding.Unsupported)
	}
	if binding.Decode == nil {
		return nil, fmt.Errorf("native storage decoder unavailable")
	}
	root, err := acton.DecodeBOC(boc)
	if err != nil {
		return nil, err
	}
	return binding.Decode(root)
}

// Accounts returns one indexed snapshot per canonical address, in input order.
// @Summary Identify Acton accounts in a batch
// @Description One database batch. include_storage defaults to false; when true, at most 8 unique addresses and an aggregate 8 MiB BOC/decoded-output budget are allowed. Public interfaces are hints, not exact ABI or storage matches. Links are catalog assertions, not source verification.
// @Tags acton
// @Produce json
// @Param address query []string true "Up to 1000 addresses, canonically deduplicated" collectionFormat(multi)
// @Param include_storage query bool false "Decode storage for exact code matches" default(false)
// @Success 200 {object} AccountsResponse
// @Failure 413 {object} Error
// @Failure 422 {object} Error
// @Router /api/v3/acton/accounts [get]
// @Security APIKeyHeader
// @Security APIKeyQuery
func (a *API) Accounts(c *fiber.Ctx) error {
	storage, err := strconv.ParseBool(c.Query("include_storage", "false"))
	if err != nil {
		return Fail(422, "include_storage must be a boolean")
	}
	accounts, err := a.accounts(c, queryValues(c, "address"), storage)
	if err != nil {
		return err
	}
	return sendBounded(c, AccountsResponse{Accounts: accounts, Revision: a.revision})
}

// PostAccounts is the JSON batch variant of Accounts.
// @Summary Identify Acton accounts in a JSON batch
// @Description include_storage defaults to false; the response contains canonical deduplicated addresses. Storage batches allow at most 8 unique addresses with an aggregate 8 MiB BOC/decoded-output budget.
// @Tags acton
// @Accept json
// @Produce json
// @Param request body AccountsRequest true "Account batch"
// @Success 200 {object} AccountsResponse
// @Failure 413 {object} Error
// @Failure 422 {object} Error
// @Router /api/v3/acton/accounts [post]
// @Security APIKeyHeader
// @Security APIKeyQuery
func (a *API) PostAccounts(c *fiber.Ctx) error {
	var req AccountsRequest
	if err := decodeJSON(c.Body(), &req); err != nil {
		return err
	}
	accounts, err := a.accounts(c, req.Addresses, req.IncludeStorage)
	if err != nil {
		return err
	}
	return sendBounded(c, AccountsResponse{Accounts: accounts, Revision: a.revision})
}

// GetMethods enumerates metadata only. Each compiler_abi includes its type table.
// @Summary List typed Acton getters
// @Tags acton
// @Produce json
// @Param address query string false "Indexed account address; exactly one selector required"
// @Param code_hash query string false "Contract code hash"
// @Param contract_type query string false "Catalog ID"
// @Success 200 {object} GetMethodsResponse
// @Failure 413 {object} Error
// @Failure 422 {object} Error
// @Router /api/v3/acton/getMethods [get]
// @Security APIKeyHeader
// @Security APIKeyQuery
func (a *API) GetMethods(c *fiber.Ctx) error {
	addr, hash, id := c.Query("address"), c.Query("code_hash"), c.Query("contract_type")
	count := 0
	for _, name := range []string{"address", "code_hash", "contract_type"} {
		values := queryValues(c, name)
		if len(values) > 1 || len(values) == 1 && values[0] == "" {
			return Fail(422, "provide exactly one nonempty selector")
		}
		count += len(values)
	}
	if count != 1 {
		return Fail(422, "provide exactly one of address, code_hash, contract_type")
	}
	response := GetMethodsResponse{Contracts: []ContractMethods{}, Revision: a.revision}
	var contracts []*acton.Contract
	if addr != "" {
		accounts, err := a.accounts(c, []string{addr}, false)
		if err != nil {
			return err
		}
		response.Account = &accounts[0]
		if accounts[0].CodeHash != nil && accounts[0].Status != "error" {
			key, err := acton.NormalizeCodeHash(*accounts[0].CodeHash)
			if err != nil {
				return Fail(502, "invalid stored code hash")
			}
			contracts = a.byHash[key]
		}
	} else {
		var err error
		contracts, err = a.selectContracts(id, hash)
		if err != nil {
			return err
		}
	}
	for _, contract := range contracts {
		info := ContractMethods{ExtendedContractABI: extended(contract), GetMethods: []GetMethod{}}
		for _, method := range contract.GetMethods {
			info.GetMethods = append(info.GetMethods, methodInfo(method))
		}
		response.Contracts = append(response.Contracts, info)
	}
	return sendBounded(c, response)
}

// Decode uses native generated bindings for an explicitly selected ABI.
// @Summary Decode Acton storage or message body
// @Description Select exactly one catalog type or code hash. Direction is storage, deployment_storage, incoming_messages, incoming_external, outgoing_messages, or emitted_events.
// @Tags acton
// @Accept json
// @Produce json
// @Param request body DecodeRequest true "Explicit ABI selector and BOC"
// @Success 200 {object} DecodeResponse
// @Failure 409 {object} Error
// @Failure 422 {object} Error
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
	response := DecodeResponse{CatalogID: contract.ID, Direction: req.Direction, Revision: a.revision}
	if req.Direction == "storage" || req.Direction == "deployment_storage" {
		binding := contract.Storage
		if req.Direction == "deployment_storage" {
			binding = contract.DeploymentStorage
		}
		if binding == nil {
			return Fail(422, "storage binding unavailable")
		}
		response.Type = binding.Type
		response.Decoded, err = decodeAccountStorage(binding, req.Body)
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
	return c.JSON(response)
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
// @Failure 409 {object} Error
// @Failure 422 {object} Error
// @Failure 502 {object} Error
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
	response := RunResponse{Execution: *execution, Snapshot: *snapshot, CatalogID: contract.ID, Method: methodInfo(*method), Identification: identification, Success: execution.ExitCode == 0 || execution.ExitCode == 1, Revision: a.revision}
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
	return c.JSON(response)
}

// sendBounded serializes once and rejects the exact encoded size, so a caller
// that batches too much gets 413 instead of a multi-megabyte body.
func sendBounded(c *fiber.Ctx, response any) error {
	body, err := json.Marshal(response)
	if err != nil {
		return Fail(502, "response cannot be serialized")
	}
	if len(body) > MaxMetadataBytes {
		return Fail(413, "response exceeds 8 MiB; reduce the batch or page size")
	}
	c.Type("json")
	return c.Send(body)
}
