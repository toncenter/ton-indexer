package wallets

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
)

func TestActivationVersionNamesMatchFrozenRegistryDigest(t *testing.T) {
	if len(codes) != 19 || len(versions) != len(codes) {
		t.Fatal("incomplete version mapping")
	}
	families := map[string][]string{}
	for i, code := range codes {
		families[versions[i]] = append(families[versions[i]], code)
	}
	if len(families) != 18 {
		t.Fatal("V5 beta must be one version")
	}
	names := make([]string, 0, len(families))
	for name := range families {
		names = append(names, name)
	}
	sort.Strings(names)
	fields := []string{}
	for _, name := range names {
		hashes := families[name]
		sort.Strings(hashes)
		values := []string{}
		for _, hash := range hashes {
			b, _ := json.Marshal(hash)
			values = append(values, string(b))
		}
		b, _ := json.Marshal(name)
		fields = append(fields, string(b)+": ["+strings.Join(values, ", ")+"]")
	}
	// Frozen export format: Python json.dumps(..., sort_keys=True). This verifies
	// the hash-to-version mapping, not merely membership in the wallet hash set.
	canonical := "{" + strings.Join(fields, ", ") + "}"
	if digest := fmt.Sprintf("%x", sha256.Sum256([]byte(canonical))); digest != Registry {
		t.Fatalf("version mapping changed the agreed registry: %s", digest)
	}
}
