package models

import (
	b64 "encoding/base64"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/xssnick/tonutils-go/address"
)

func HashConverter(value string) reflect.Value {
	if len(value) == 64 || len(value) == 66 && strings.HasPrefix(value, "0x") {
		value = strings.TrimPrefix(value, "0x")
		if res, err := hex.DecodeString(value); err == nil {
			return reflect.ValueOf(HashType(b64.StdEncoding.EncodeToString(res)))
		} else {
			return reflect.Value{}
		}
	}
	if len(value) == 44 {
		if res, err := b64.StdEncoding.DecodeString(value); err == nil {
			return reflect.ValueOf(HashType(b64.StdEncoding.EncodeToString(res)))
		} else if res, err := b64.URLEncoding.DecodeString(value); err == nil {
			return reflect.ValueOf(HashType(b64.StdEncoding.EncodeToString(res)))
		} else {
			return reflect.Value{}
		}
	}
	return reflect.Value{}
}

func AccountAddressConverter(value string) reflect.Value {
	addr, err := address.ParseAddr(value)
	if err != nil {
		value_url := strings.Replace(value, "+", "-", -1)
		value_url = strings.Replace(value_url, "/", "_", -1)
		addr, err = address.ParseAddr(value_url)
	}
	if err != nil {
		addr, err = address.ParseRawAddr(value)
	}
	if err != nil {
		return reflect.Value{}
	}
	addr_str := fmt.Sprintf("%d:%s", addr.Workchain(), strings.ToUpper(hex.EncodeToString(addr.Data())))
	return reflect.ValueOf(AccountAddress(addr_str))
}

func AccountAddressNullableConverter(value string) reflect.Value {
	if value == "null" {
		return reflect.ValueOf(value)
	}
	return AccountAddressConverter(value)
}

func ShardIdConverter(value string) reflect.Value {
	value = strings.TrimPrefix(value, "0x")
	if shard, err := strconv.ParseUint(value, 16, 64); err == nil {
		return reflect.ValueOf(ShardId(shard))
	}
	if shard, err := strconv.ParseInt(value, 10, 64); err == nil {
		return reflect.ValueOf(ShardId(shard))
	}
	return reflect.Value{}
}

func UtimeTypeConverter(value string) reflect.Value {
	if utime, err := strconv.ParseUint(value, 10, 32); err == nil {
		return reflect.ValueOf(UtimeType(utime))
	}
	if utime, err := strconv.ParseFloat(value, 64); err == nil {
		return reflect.ValueOf(UtimeType(uint64(utime)))
	}
	return reflect.Value{}
}

func OpcodeTypeConverter(value string) reflect.Value {
	value = strings.TrimPrefix(value, "0x")
	if res, err := strconv.ParseUint(value, 16, 32); err == nil {
		return reflect.ValueOf(OpcodeType(res))
	}
	if res, err := strconv.ParseInt(value, 10, 32); err == nil {
		return reflect.ValueOf(OpcodeType(res))
	}
	return reflect.Value{}
}

func GetAccountAddressFriendly(account string, code_hash *string, is_testnet bool) string {
	addr, err := address.ParseRawAddr(strings.Trim(account, " "))
	if err != nil {
		return "addr_none"
	}
	bouncable := true
	if code_hash == nil || WalletsHashMap[*code_hash] {
		bouncable = false
	}
	addr.SetBounce(bouncable)
	addr.SetTestnetOnly(is_testnet)
	return addr.String()
}
