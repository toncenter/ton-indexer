package acton

import (
	"strings"

	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

// API v3 spells a hash in base64 and an address in raw form with uppercase hex,
// and a client indexes address_book and code_book with exactly those spellings.
// The Acton sources disagree: the catalog stores hex hashes and user-friendly
// addresses, and the codecs emit the lowercase raw of tonutils. Both are rewritten
// on the way out, so one value never reaches a client spelled two ways. The codecs
// keep their own spelling — their output is byte-compared against the TypeScript
// generator, and the response, not the codec, is what this API owns.

// canonicalHashes renders catalog code hashes the way every other v3 hash is
// rendered. A hash that cannot be parsed is passed through rather than dropped;
// TestCatalogSpellsHashesAndAddressesLikeV3 asserts the catalog holds none.
func CanonicalHashes(hashes []string) []string {
	out := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		parsed, err := models.ParseHashType(hash)
		if err != nil {
			out = append(out, hash)
			continue
		}
		out = append(out, string(*parsed))
	}
	return out
}

// canonicalAddresses renders catalog addresses in raw form. The friendly spelling
// also carries the bounceable and testnet flags, but those say how to send to an
// address rather than which account it is, which is all a catalog entry claims.
func CanonicalAddresses(addresses []string) []string {
	out := make([]string, 0, len(addresses))
	for _, value := range addresses {
		canonical, err := CanonicalAddress(value)
		if err != nil {
			out = append(out, value)
			continue
		}
		out = append(out, canonical)
	}
	return out
}

// CanonicalizeDecoded rewrites the addresses a codec produced into raw uppercase
// and returns the same value, rewritten in place. Only a standard address can
// look like workchain:hex: a codec renders integers in decimal, cells as base64
// BOCs, and bit strings as an object carrying their own length.
func CanonicalizeDecoded(value any) any {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			v[key] = CanonicalizeDecoded(item)
		}
	case []any:
		for i, item := range v {
			v[i] = CanonicalizeDecoded(item)
		}
	case string:
		if address, ok := UpperRawAddress(v); ok {
			return address
		}
	}
	return value
}

func UpperRawAddress(value string) (string, bool) {
	workchain, hash, found := strings.Cut(value, ":")
	if !found || len(hash) != 64 {
		return "", false
	}
	digits := strings.TrimPrefix(workchain, "-")
	if digits == "" || len(digits) > 10 {
		return "", false
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return "", false
		}
	}
	for i := 0; i < len(hash); i++ {
		c := hash[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return "", false
		}
	}
	return workchain + ":" + strings.ToUpper(hash), true
}
