package catalog

import (
	"os"
	"strings"
	"testing"
)

func TestPinnedCatalogAndNames(t *testing.T) {
	lock, err := os.ReadFile("catalog.lock")
	if err != nil || !strings.Contains(string(lock), "ABI_CATALOG_SHA256="+Revision) {
		t.Fatal("generated catalog differs from lock", err)
	}
	hashes := map[string]bool{}
	unsupported, punctuated := 0, false
	for _, c := range Contracts {
		for _, hash := range c.CodeHashes {
			hashes[hash] = true
		}
		for _, m := range c.GetMethods {
			punctuated = punctuated || m.Name == "processed?" && m.ID == 117746
			if m.Unsupported != "" {
				unsupported++
			}
		}
	}
	if len(Contracts) != 299 || len(hashes) != 346 || !punctuated || unsupported == 0 {
		t.Fatalf("catalog changed: contracts=%d hashes=%d getters retained=%t", len(Contracts), len(hashes), punctuated)
	}
	aliases := ByCodeHash("192535677eed65c20ac387efe4dd7415ad9ebb9349103e87c60e592538c9dcf3")
	if len(aliases) != 2 {
		t.Fatal("shared Stakee/Tonstakers code lost a candidate")
	}
}
