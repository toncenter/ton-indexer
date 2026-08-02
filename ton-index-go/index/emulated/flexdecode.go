package emulated

import (
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// Accept both decimal strings and native msgpack numbers in emulated trace
// payloads. Registration covers scalar integer and string fields and must run
// before msgpack memoizes its per-type decoders.
func init() {
	for _, v := range []interface{}{int(0), int8(0), int16(0), int32(0), int64(0)} {
		msgpack.Register(v, nil, decodeFlexInt)
	}
	for _, v := range []interface{}{uint(0), uint8(0), uint16(0), uint32(0), uint64(0)} {
		msgpack.Register(v, nil, decodeFlexUint)
	}
	msgpack.Register("", nil, decodeFlexString)
}

func decodeFlexInt(d *msgpack.Decoder, v reflect.Value) error {
	c, err := d.PeekCode()
	if err != nil {
		return err
	}
	if msgpcode.IsString(c) || msgpcode.IsBin(c) {
		s, err := d.DecodeString()
		if err != nil {
			return err
		}
		n, err := parseFlexInt(s)
		if err != nil {
			return err
		}
		v.SetInt(n)
		return nil
	}
	n, err := d.DecodeInt64()
	if err != nil {
		return err
	}
	v.SetInt(n)
	return nil
}

func decodeFlexUint(d *msgpack.Decoder, v reflect.Value) error {
	c, err := d.PeekCode()
	if err != nil {
		return err
	}
	if msgpcode.IsString(c) || msgpcode.IsBin(c) {
		s, err := d.DecodeString()
		if err != nil {
			return err
		}
		n, err := parseFlexUint(s)
		if err != nil {
			return err
		}
		v.SetUint(n)
		return nil
	}
	n, err := d.DecodeUint64()
	if err != nil {
		return err
	}
	v.SetUint(n)
	return nil
}

func decodeFlexString(d *msgpack.Decoder, v reflect.Value) error {
	c, err := d.PeekCode()
	if err != nil {
		return err
	}
	switch {
	case msgpcode.IsFixedNum(c), c == msgpcode.Int8, c == msgpcode.Int16, c == msgpcode.Int32, c == msgpcode.Int64:
		n, err := d.DecodeInt64()
		if err != nil {
			return err
		}
		v.SetString(strconv.FormatInt(n, 10))
	case c == msgpcode.Uint8, c == msgpcode.Uint16, c == msgpcode.Uint32, c == msgpcode.Uint64:
		n, err := d.DecodeUint64()
		if err != nil {
			return err
		}
		v.SetString(strconv.FormatUint(n, 10))
	case c == msgpcode.Float, c == msgpcode.Double:
		f, err := d.DecodeFloat64()
		if err != nil {
			return err
		}
		v.SetString(strconv.FormatFloat(f, 'g', -1, 64))
	default:
		s, err := d.DecodeString()
		if err != nil {
			return err
		}
		v.SetString(s)
	}
	return nil
}

// parseFlexInt accepts decimal integers and integral floating-point spellings.
// It rejects fractional values rather than rounding them.
func parseFlexInt(s string) (int64, error) {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	// MaxInt64 rounds to 2^63 in float64, so the upper bound is inclusive.
	// MinInt64 is exact and uses a strict lower bound.
	if err != nil || math.Trunc(f) != f || f < math.MinInt64 || f >= math.MaxInt64 {
		return 0, fmt.Errorf("msgpack: %q is not an int64", s)
	}
	return int64(f), nil
}

func parseFlexUint(s string) (uint64, error) {
	if n, err := strconv.ParseUint(s, 10, 64); err == nil {
		return n, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	// >= for the same reason as parseFlexInt: float64(MaxUint64) rounds up to
	// 2^64, and "18446744073709551615.0" parses to it.
	if err != nil || math.Trunc(f) != f || f < 0 || f >= math.MaxUint64 {
		return 0, fmt.Errorf("msgpack: %q is not a uint64", s)
	}
	return uint64(f), nil
}
