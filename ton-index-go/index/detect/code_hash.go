package detect

import (
	"slices"
	"sync"
)

var codeHashInterfaces = sync.OnceValue(func() map[string][]string {
	result := make(map[string][]string)
	for _, iface := range getInterfaces() {
		for _, hash := range iface.CodeHashes {
			result[hash] = append(result[hash], iface.Name)
		}
	}
	for hash, names := range result {
		slices.Sort(names)
		result[hash] = slices.Compact(names)
	}
	return result
})

// InterfacesByCodeHash returns every exact match for a padded standard-base64
// code hash. Unlike DetectInterface it never selects a first match or uses methods.
func InterfacesByCodeHash(codeHash string) []string {
	return slices.Clone(codeHashInterfaces()[codeHash])
}
