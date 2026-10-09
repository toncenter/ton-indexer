package acton

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/ton-blockchain/tolk-abi-to-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

// CanonicalAddress accepts every spelling the rest of v3 accepts and returns the
// raw form it returns. A getter runs on an account, so the other address kinds
// the parser knows — none, external, variable-length — are refused here.
func CanonicalAddress(value string) (string, error) {
	parsed, err := models.ParseAccountAddress(value)
	if err != nil || !parsed.IsAddressStd() {
		return "", Fail(422, "invalid standard account address")
	}
	return string(*parsed), nil
}

// codeHashKey spells a hash the way the rest of v3 spells one, so every hex or
// base64 form of the same code collapses to a single selector.
func CodeHashKey(hash string) (string, error) {
	key, err := models.ParseHashType(hash)
	if err != nil {
		return "", Fail(422, "invalid code_hash")
	}
	return string(*key), nil
}

func DecodeJSON(data []byte, dst any) error {
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

func DecodeBinding(binding *tolkabi.Binding, boc string) (any, error) {
	if binding.Unsupported != "" {
		return nil, fmt.Errorf("unsupported storage: %s", binding.Unsupported)
	}
	if binding.Decode == nil {
		return nil, errors.New("native storage decoder unavailable")
	}
	root, err := tolkabi.DecodeBOC(boc)
	if err != nil {
		return nil, err
	}
	return binding.Decode(root)
}
