package detect

import (
	"slices"
	"testing"
)

func TestInterfacesByCodeHash(t *testing.T) {
	want := make(map[string][]string)
	for _, iface := range getInterfaces() {
		for _, hash := range iface.CodeHashes {
			want[hash] = append(want[hash], iface.Name)
		}
	}
	for hash, names := range want {
		slices.Sort(names)
		names = slices.Compact(names)
		got := InterfacesByCodeHash(hash)
		if !slices.Equal(got, names) {
			t.Fatalf("%s: got %v, want every match %v", hash, got, names)
		}
		got[0] = "modified response"
		if !slices.Equal(InterfacesByCodeHash(hash), names) {
			t.Fatalf("%s: caller mutated the cached interface list", hash)
		}
	}
	if got := InterfacesByCodeHash("unknown"); len(got) != 0 {
		t.Fatalf("unknown hash must not infer method-based interfaces: %v", got)
	}
}
