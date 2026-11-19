package accounttypes

import (
	"encoding/base64"
	"encoding/hex"
	"slices"
	"testing"
)

func hash64(hexHash string) string {
	b, _ := hex.DecodeString(hexHash)
	return base64.StdEncoding.EncodeToString(b)
}

func TestReviewedAliasesAndIndependentRoles(t *testing.T) {
	f, err := BuildFeatures(nil)
	if err != nil {
		t.Fatal(err)
	}
	for hash, want := range map[string][]string{
		"192535677eed65c20ac387efe4dd7415ad9ebb9349103e87c60e592538c9dcf3": {"DeFi", "Staking", "Pools"},
		"aab1db63f193a4aecaa3e3a959fc1784f1412b191a66ae870958405ba88114c7": {"DeFi", "Trading", "Orders and escrow"},
		"bc947d16346c2fdc845b7a446382f59c86acb075e2a31dbfa956cd0c9e54678e": {"Mixed", "DEX pool + NFT collection"},
		"feb5ff6820e2ff0d9483e7e0d62c817d846789fb4ae580c878866d959dabd5c0": {"Wallets", "Ordinary"},
	} {
		key := hash64(hash)
		for _, p := range f.Prefixes {
			if leaf := p.Assignments[key]; leaf != "" && !slices.Equal(f.Definitions[leaf].Path[:len(want)], want) {
				t.Fatal(hash, f.Definitions[leaf])
			}
		}
		if len(f.CodeContracts[key]) == 0 {
			t.Fatal("missing known hash")
		}
	}
	if !slices.Equal(f.MethodNames[76407], []string{"is_plugin_installed", "version"}) {
		t.Fatal("method-name ambiguity lost")
	}
}

func TestMethodInferenceIsDisjointAndConservative(t *testing.T) {
	for _, tc := range []struct {
		ids  []uint32
		path []string
	}{
		{[]uint32{97026}, []string{"Jettons", "Wallets", "By get methods"}},
		{[]uint32{106029}, []string{"Unknown", "Methods available, type unknown"}},
		{[]uint32{106029, 103289}, []string{"Jettons", "Masters", "By get methods"}},
		{[]uint32{102351, 84760, 97667, 123660}, []string{"NFTs", "Items", "By get methods"}},
		{[]uint32{102491}, []string{"Unknown", "Methods available, type unknown"}},
		{[]uint32{102491, 92067, 68445}, []string{"NFTs", "Collections", "By get methods"}},
		{[]uint32{102351, 97026}, []string{"Mixed", "Jetton wallet + NFT item", "By get methods"}},
		{[]uint32{78748, 85143}, []string{"Unknown", "Methods available, type unknown"}},
	} {
		if got := infer(tc.ids); !slices.Equal(got.Path, tc.path) {
			t.Fatal(tc.ids, got.Path)
		}
	}
}

func TestExactTreePartitionAndStableFeatures(t *testing.T) {
	a, err := BuildFeatures(map[string][]uint32{hash64("0000000000000000000000000000000000000000000000000000000000000000"): {97026, 97026}})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := BuildFeatures(map[string][]uint32{hash64("0000000000000000000000000000000000000000000000000000000000000000"): {97026}})
	if a.Digest != b.Digest {
		t.Fatal("feature digest depends on duplicate IDs")
	}
	def := infer([]uint32{97026})
	leaf := nodeID(def.Path)
	counts := []LeafCount{{leaf, 9007199254740993, 1}, {UnknownLeaf, 7, 2}}
	nodes, err := BuildTree(a.Definitions, counts, 9007199254741000, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, parent := range nodes {
		var sum, hashes uint64
		children := 0
		for _, child := range nodes {
			if child.Parent == parent.ID {
				children++
				sum += child.Accounts
				hashes += child.Hashes
			}
		}
		if children > 0 && (sum != parent.Accounts || hashes != parent.Hashes) {
			t.Fatal("non-additive tree", parent)
		}
		if parent.Known+parent.Inferred+parent.Unknown != parent.Accounts {
			t.Fatal("evidence partition differs from total")
		}
	}
	if _, err := BuildTree(a.Definitions, counts, 9007199254740999, 3); err == nil {
		t.Fatal("accepted inconsistent source total")
	}
}
