// Package catalog contains the offline Acton ABI catalog and native Go bindings.
package catalog

//go:generate go run github.com/ton-blockchain/acton/packages/abi-go/cmd/tolk-abi-to-go --catalog catalog.json --output-dir . --package catalog
