// Package accounttypes prepares mutually exclusive account-type observations.
// All population counts come from a completed code-hash snapshot.
package accounttypes

import (
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/toncenter/ton-indexer/ton-index-go/index/acton"
	"github.com/toncenter/ton-indexer/ton-index-go/index/acton/catalog"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/handlers/codehashes"
)

//go:embed account-type-rules.json
var rulesJSON []byte

const UnknownLeaf = "unknown-no-methods"

var Categories = []string{"Wallets", "Jettons", "NFTs", "NFT markets", "DeFi", "Other recognized", "Mixed", "Unknown"}

type Rule struct {
	Path      []string `json:"path"`
	Roles     []string `json:"roles"`
	Contracts []string `json:"contracts"`
}

type Definition struct {
	Path     []string `json:"path"`
	Roles    []string `json:"roles"`
	Evidence string   `json:"evidence"` // code_hash, get_methods, unknown
}

type FeaturePrefix struct {
	Prefix      string              `json:"prefix"`
	Assignments map[string]string   `json:"assignments"`
	Methods     map[string][]uint32 `json:"methods"`
}

type Features struct {
	ABIRevision    string
	Classification string
	Digest         string
	Definitions    map[string]Definition
	Contracts      map[string]string
	CodeContracts  map[string][]string
	MethodNames    map[uint32][]string
	Prefixes       []FeaturePrefix
}

func nodeID(path []string) string {
	digest := sha256.Sum256([]byte(strings.Join(path, "\x1f")))
	return hex.EncodeToString(digest[:12])
}

func canonicalHash(hash string) (string, error) {
	b, err := base64.StdEncoding.Strict().DecodeString(hash)
	if err != nil || len(b) != 32 || len(hash) != 44 {
		return "", fmt.Errorf("invalid base64 code hash")
	}
	return hash, nil
}

// BuildFeatures knows the catalog, but not account counts or project membership.
// Generic ABI annotations (Cron) supplement specific implementations, not roles.
func BuildFeatures(methods map[string][]uint32) (*Features, error) {
	var rules struct {
		Version     string   `json:"version"`
		Annotations []string `json:"annotations"`
		Groups      []Rule   `json:"groups"`
	}
	if err := json.Unmarshal(rulesJSON, &rules); err != nil {
		return nil, err
	}
	ruleHash := sha256.Sum256(rulesJSON)
	f := &Features{
		ABIRevision: catalog.Revision, Classification: rules.Version + "-getters-v1-" + hex.EncodeToString(ruleHash[:8]),
		Definitions: map[string]Definition{UnknownLeaf: {[]string{"Unknown", "No static method record"}, []string{}, "unknown"}},
		Contracts:   map[string]string{}, CodeContracts: map[string][]string{}, MethodNames: map[uint32][]string{},
	}
	byID := map[string]Rule{}
	for _, r := range rules.Groups {
		if len(r.Path) == 0 || !slices.Contains(Categories, r.Path[0]) || len(r.Roles) == 0 {
			return nil, fmt.Errorf("invalid classification rule")
		}
		r.Roles = slices.Clone(r.Roles)
		sort.Strings(r.Roles)
		if len(slices.Compact(slices.Clone(r.Roles))) != len(r.Roles) || (len(r.Roles) > 1) != (r.Path[0] == "Mixed") {
			return nil, fmt.Errorf("invalid independent roles for %v", r.Path)
		}
		if r.Path[0] == "Mixed" {
			r.Path = []string{"Mixed", strings.Join(r.Roles, " + ")}
		}
		for _, id := range r.Contracts {
			if _, exists := byID[id]; exists || catalog.ByID(id) == nil {
				return nil, fmt.Errorf("duplicate or unknown catalog rule %s", id)
			}
			byID[id] = r
		}
	}
	for _, id := range rules.Annotations {
		if catalog.ByID(id) == nil {
			return nil, fmt.Errorf("unknown ABI annotation %s", id)
		}
	}
	for _, contract := range catalog.Contracts {
		if _, ok := byID[contract.ID]; !ok && !slices.Contains(rules.Annotations, contract.ID) {
			return nil, fmt.Errorf("catalog entry has no reviewed rule: %s", contract.ID)
		}
		meta, err := json.Marshal(acton.Describe(contract))
		if err != nil {
			return nil, err
		}
		f.Contracts[contract.ID] = string(meta)
		for _, hash := range contract.CodeHashes {
			b, err := hex.DecodeString(hash)
			if err != nil || len(b) != 32 {
				return nil, fmt.Errorf("invalid catalog hash")
			}
			h := base64.StdEncoding.EncodeToString(b)
			f.CodeContracts[h] = append(f.CodeContracts[h], contract.ID)
		}
		for _, method := range contract.GetMethods {
			if method.ID >= 0 && method.ID <= math.MaxUint32 {
				id := uint32(method.ID)
				f.MethodNames[id] = append(f.MethodNames[id], method.Name)
			}
		}
	}
	for id, names := range f.MethodNames {
		sort.Strings(names)
		f.MethodNames[id] = slices.Compact(names)
	}
	known := map[string]string{}
	for hash, ids := range f.CodeContracts {
		sort.Strings(ids)
		var rule *Rule
		var labels []string
		for _, id := range ids {
			if len(ids) > 1 && slices.Contains(rules.Annotations, id) {
				continue
			}
			r := byID[id]
			if rule != nil && (!slices.Equal(rule.Path, r.Path) || !slices.Equal(rule.Roles, r.Roles)) {
				return nil, fmt.Errorf("conflicting reviewed classifications for %s: %v", hash, ids)
			}
			rule = &r
			labels = append(labels, catalog.ByID(id).DisplayName)
		}
		if rule == nil {
			return nil, fmt.Errorf("annotation-only code hash %s", hash)
		}
		sort.Strings(labels)
		path := append(slices.Clone(rule.Path), strings.Join(slices.Compact(labels), " / "))
		leaf := nodeID(path)
		f.Definitions[leaf] = Definition{path, rule.Roles, "code_hash"}
		known[hash] = leaf
	}
	for _, char := range codehashes.Alphabet {
		f.Prefixes = append(f.Prefixes, FeaturePrefix{string(char), map[string]string{}, map[string][]uint32{}})
	}
	for hash, ids := range methods {
		if _, err := canonicalHash(hash); err != nil || len(ids) > 4096 {
			return nil, fmt.Errorf("invalid static method record")
		}
		ids = slices.Clone(ids)
		slices.Sort(ids)
		ids = slices.Compact(ids)
		p := &f.Prefixes[strings.IndexByte(codehashes.Alphabet, hash[0])]
		p.Methods[hash] = ids
		if _, ok := known[hash]; !ok {
			def := infer(ids)
			leaf := nodeID(def.Path)
			f.Definitions[leaf] = def
			p.Assignments[hash] = leaf
		}
	}
	for hash, leaf := range known {
		f.Prefixes[strings.IndexByte(codehashes.Alphabet, hash[0])].Assignments[hash] = leaf
	}
	// Canonical JSON sorts map keys. Times and source counts are intentionally
	// excluded: an unchanged feature set is reused across many count snapshots.
	data, err := json.Marshal(f)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	f.Digest = hex.EncodeToString(digest[:])
	return f, nil
}

func infer(ids []uint32) Definition {
	has := func(required ...uint32) bool {
		for _, id := range required {
			if !slices.Contains(ids, id) {
				return false
			}
		}
		return true
	}
	var roles []string
	var path []string
	for _, candidate := range []struct {
		role string
		path []string
		ids  []uint32
	}{
		{"Jetton wallet", []string{"Jettons", "Wallets"}, []uint32{97026}},
		{"Jetton master", []string{"Jettons", "Masters"}, []uint32{106029, 103289}},
		{"NFT item", []string{"NFTs", "Items"}, []uint32{102351}},
		{"NFT collection", []string{"NFTs", "Collections"}, []uint32{102491, 92067, 68445}},
	} {
		if has(candidate.ids...) {
			roles = append(roles, candidate.role)
			path = candidate.path
		}
	}
	if len(roles) == 0 {
		return Definition{[]string{"Unknown", "Methods available, type unknown"}, []string{}, "unknown"}
	}
	sort.Strings(roles)
	if len(roles) > 1 {
		path = []string{"Mixed", strings.Join(roles, " + ")}
	}
	return Definition{append(path, "By get methods"), roles, "get_methods"}
}

type LeafCount struct {
	Leaf     string `json:"leaf"`
	Accounts uint64 `json:"accounts,string"`
	Hashes   uint64 `json:"hashes,string"`
}

type Node struct {
	ID       string   `json:"id"`
	Parent   string   `json:"parent"`
	Label    string   `json:"label"`
	Accounts uint64   `json:"accounts,string"`
	Hashes   uint64   `json:"hashes,string"`
	Known    uint64   `json:"known,string"`
	Inferred uint64   `json:"inferred,string"`
	Unknown  uint64   `json:"unknown,string"`
	Leaves   []string `json:"leaves"`
	Roles    []string `json:"roles,omitempty"`
	Evidence string   `json:"evidence,omitempty"`
}

func BuildTree(defs map[string]Definition, counts []LeafCount, accounts, hashes uint64) ([]Node, error) {
	nodes := map[string]*Node{"all": {ID: "all", Label: "All", Leaves: []string{}}}
	seen := map[string]bool{}
	for _, count := range counts {
		def, ok := defs[count.Leaf]
		if !ok || seen[count.Leaf] || count.Accounts == 0 || count.Hashes == 0 {
			return nil, fmt.Errorf("unknown, duplicate or empty leaf %s", count.Leaf)
		}
		seen[count.Leaf] = true
		parent := "all"
		for depth := 0; depth <= len(def.Path); depth++ {
			id := "all"
			if depth > 0 {
				id = nodeID(def.Path[:depth])
				if depth == len(def.Path) {
					id = count.Leaf
				}
				if nodes[id] == nil {
					nodes[id] = &Node{ID: id, Parent: parent, Label: def.Path[depth-1], Leaves: []string{}}
				}
			}
			n := nodes[id]
			if count.Accounts > math.MaxUint64-n.Accounts || count.Hashes > math.MaxUint64-n.Hashes {
				return nil, fmt.Errorf("account-type total overflow")
			}
			n.Accounts += count.Accounts
			n.Hashes += count.Hashes
			n.Leaves = append(n.Leaves, count.Leaf)
			switch def.Evidence {
			case "code_hash":
				n.Known += count.Accounts
			case "get_methods":
				n.Inferred += count.Accounts
			default:
				n.Unknown += count.Accounts
			}
			if depth == len(def.Path) {
				n.Roles, n.Evidence = def.Roles, def.Evidence
			}
			parent = id
		}
	}
	if nodes["all"].Accounts != accounts || nodes["all"].Hashes != hashes || accounts == 0 {
		return nil, fmt.Errorf("classified distribution disagrees with source snapshot")
	}
	result := make([]Node, 0, len(nodes))
	for _, n := range nodes {
		sort.Strings(n.Leaves)
		result = append(result, *n)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}
