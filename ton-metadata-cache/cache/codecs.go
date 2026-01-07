package cache

import (
	"github.com/vmihailenco/msgpack/v5"
)

// MsgpackEncoder returns an Encoder that marshals values to msgpack.
func MsgpackEncoder[T any]() Encoder[T] {
	return func(value T) ([]byte, error) {
		return msgpack.Marshal(value)
	}
}

// MsgpackDecoder returns a Decoder that unmarshals msgpack to values.
func MsgpackDecoder[T any]() Decoder[T] {
	return func(data []byte) (T, error) {
		var value T
		err := msgpack.Unmarshal(data, &value)
		return value, err
	}
}
