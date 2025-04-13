package models

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

type AccountState struct {
	Hash          Hash    `msgpack:"hash"`
	Balance       uint64  `msgpack:"balance"`
	AccountStatus string  `msgpack:"account_status"`
	FrozenHash    *Hash   `msgpack:"frozen_hash"`
	CodeHash      *Hash   `msgpack:"code_hash"`
	DataHash      *Hash   `msgpack:"data_hash"`
	LastTransHash *Hash   `msgpack:"last_trans_hash"`
	LastTransLt   *uint64 `msgpack:"last_trans_lt"`
	Timestamp     *uint32 `msgpack:"timestamp"`
}

type Hash [32]byte

func (h Hash) Base64() string {
	return base64.StdEncoding.EncodeToString(h[:])
}

func FromBase64(s string) (Hash, error) {
	var h Hash
	_, err := base64.StdEncoding.Decode(h[:], []byte(s))
	return h, err
}

func (h Hash) MarshalText() (data []byte, err error) {
	return []byte(base64.StdEncoding.EncodeToString(h[:])), nil
}

// MarshalJSON implements json.Marshaler interface
func (h Hash) MarshalJSON() ([]byte, error) {
	return json.Marshal(base64.StdEncoding.EncodeToString(h[:]))
}

// EncodeMsgpack implements msgpack.CustomEncoder interface
func (h Hash) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.EncodeBytes(h[:])
}

// DecodeMsgpack implements msgpack.CustomDecoder interface
func (h *Hash) DecodeMsgpack(dec *msgpack.Decoder) error {
	bytes, err := dec.DecodeBytes()
	if err != nil {
		return err
	}

	if len(bytes) != 32 {
		return fmt.Errorf("invalid hash length: expected 32 bytes, got %d", len(bytes))
	}

	copy(h[:], bytes)
	return nil
}
