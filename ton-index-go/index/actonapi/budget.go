package actonapi

import (
	"reflect"

	"github.com/toncenter/ton-indexer/ton-index-go/index/acton"
)

// Walk the native JSON-safe value before marshaling or retaining it in a batch.
// Count repeated references repeatedly, as JSON does, and cap both traversal and
// depth. Six bytes per string byte bounds JSON escaping without allocating it.
// Native per-call Context limits still bound construction of each individual
// value; this budget prevents retaining many individually valid large values.
func consumeValueBudget(value any, remaining, nodes *int) error {
	var walk func(reflect.Value, int) error
	exceeded := func() error { return Fail(413, "decoded storage exceeds aggregate 8 MiB or item/depth budget") }
	walk = func(v reflect.Value, depth int) error {
		*nodes -= 1
		*remaining -= 16
		if *remaining < 0 || *nodes < 0 || depth > acton.MaxDepth {
			return exceeded()
		}
		if !v.IsValid() {
			return nil
		}
		switch v.Kind() {
		case reflect.Interface, reflect.Pointer:
			if !v.IsNil() {
				return walk(v.Elem(), depth+1)
			}
		case reflect.String:
			if v.Len() > *remaining/6 {
				return exceeded()
			}
			*remaining -= 6 * v.Len()
		case reflect.Slice, reflect.Array:
			if v.Len() > *nodes {
				return exceeded()
			}
			for i := 0; i < v.Len(); i++ {
				if err := walk(v.Index(i), depth+1); err != nil {
					return err
				}
			}
		case reflect.Map:
			if v.Len() > *nodes/2 {
				return exceeded()
			}
			it := v.MapRange()
			for it.Next() {
				if err := walk(it.Key(), depth+1); err != nil {
					return err
				}
				if err := walk(it.Value(), depth+1); err != nil {
					return err
				}
			}
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				field := v.Type().Field(i)
				if field.PkgPath != "" {
					continue
				}
				*remaining -= 128 + 6*len(field.Name)
				if err := walk(v.Field(i), depth+1); err != nil {
					return err
				}
			}
		case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			*remaining -= 32
		default:
			return Fail(502, "native storage decoder returned a non-JSON value")
		}
		if *remaining < 0 {
			return exceeded()
		}
		return nil
	}
	return walk(reflect.ValueOf(value), 0)
}
