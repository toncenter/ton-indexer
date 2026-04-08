package models

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/xssnick/tonutils-go/address"
)

// string
func (v *ShardId) String() string {
	return fmt.Sprintf("%X", uint64(*v))
}

func (v *AccountAddress) String() string {
	return string(*v)
}

func (v *AccountAddress) StringPtr() *string {
	return (*string)(v)
}

func (v *HexInt) String() string {
	return fmt.Sprintf("0x%x", uint32(*v))
}

func (v *OpcodeType) String() string {
	return fmt.Sprintf("0x%08x", uint32(*v))
}

func (v *HashType) String() string {
	return string(*v)
}

func (v *BytesType) String() string {
	return string(*v)
}

func (a *AccountAddressStruct) String() string {
	switch a.Kind {
	case AddressNone:
		return ""
	case AddressStd:
		return fmt.Sprintf("%d:%s", a.Workchain, a.Addr)
	case AddressVar:
		return "addr_var$unsupported"
	case AddressExt:
		return fmt.Sprintf("addr_ext$%d:%s", a.ExtLen, a.Addr)
	default:
		return "addr_unknown"
	}
}

// parsers
func ParseAccountAddress(value string) (*AccountAddress, error) {
	addr, err := ParseAccountAddressStruct(value)
	if err != nil {
		return nil, err
	}
	return new(AccountAddress(addr.String())), nil
}

func ParseAccountAddressStruct(value string) (*AccountAddressStruct, error) {
	value = strings.TrimSpace(value)
	// parse address none
	if value == "null" || value == "addr_none" {
		return &AccountAddressStruct{Kind: AddressNone}, nil
	}
	// parse address extern
	if strings.HasPrefix(value, "addr_ext$") {
		value = strings.TrimPrefix(value, "addr_ext$")
		parts := strings.SplitN(value, ":", 2)
		res := AccountAddressStruct{Kind: AddressExt}

		extLen, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			return nil, err
		}
		res.ExtLen = int32(extLen)

		extAddr, err := hex.DecodeString(parts[1])
		if err != nil {
			return nil, err
		}
		res.Addr = fmt.Sprintf("%X", extAddr)
		return &res, nil
	}

	// parse var address
	if strings.HasPrefix(value, "addr_var$") {
		return &AccountAddressStruct{Kind: AddressVar}, nil
	}

	// parse std address
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
		return nil, err
	}

	result := AccountAddressStruct{
		Kind:      AddressStd,
		Workchain: addr.Workchain(),
		Addr:      fmt.Sprintf("%X", addr.Data()),
	}
	return &result, nil
}

func ParseHashType(s string) (*HashType, error) {
	val, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	if len(val) != 32 {
		return nil, errors.New(fmt.Sprintf("invalid hash type 32 bytes expected, got %d", len(val)))
	}
	return new(HashType(base64.StdEncoding.EncodeToString(val))), nil
}

func MustParseHashType(s string) HashType {
	val, err := ParseHashType(s)
	if err != nil {
		panic(err)
	}
	return *val
}

// postgresql scanners
func ScanAccountAddressStruct(v []byte) (*AccountAddressStruct, error) {
	if len(v) == 0 {
		return &AccountAddressStruct{Kind: AddressNone}, nil
	}

	if len(v) < 4 {
		return nil, fmt.Errorf("invalid address: too short: %d bytes", len(v))
	}
	prefix := binary.BigEndian.Uint32(v[0:4])
	if prefix == uint32(1<<31) {
		length := int32(binary.BigEndian.Uint32(v[4:8]))
		data := v[8:]
		return &AccountAddressStruct{Kind: AddressExt, ExtLen: length, Addr: strings.ToUpper(hex.EncodeToString(data))}, nil
	}
	if len(v) != 36 {
		return nil, fmt.Errorf("invalid address: expected 36 bytes, got %d", len(v))
	}
	return &AccountAddressStruct{Kind: AddressStd, Workchain: int32(prefix), Addr: strings.ToUpper(hex.EncodeToString(v[4:36]))}, nil
}

//func (a *BytesType) ScanBytes(v []byte) error {
//	if v == nil {
//		return fmt.Errorf("HashType: NULL is not allowed")
//	}
//
//	*a = BytesType(base64.StdEncoding.EncodeToString(v))
//	return nil
//}
//
//func (h *HashType) ScanBytes(v []byte) error {
//	if v == nil {
//		return fmt.Errorf("HashType: NULL is not allowed")
//	}
//	if len(v) != 32 {
//		return fmt.Errorf("HashType: expected 32 bytes, got %d", len(v))
//	}
//
//	*h = HashType(base64.StdEncoding.EncodeToString(v))
//	return nil
//}
//
//func (a *AccountAddress) ScanBytes(v []byte) error {
//	if v == nil {
//		*a = ""
//		return nil
//	}
//	s, err := ScanAccountAddressStruct(v)
//	if err != nil {
//		return err
//	}
//
//	*a = AccountAddress(s.String())
//	return nil
//}

// postgresql bytea representation
func (a *AccountAddressStruct) BytesValue() ([]byte, error) {
	switch a.Kind {
	case AddressNone:
		return nil, nil
	case AddressExt:
		prefix := 1 << 31
		addr, err := hex.DecodeString(a.Addr)
		if err != nil {
			return nil, err
		}
		resLen := (a.ExtLen + 7) / 8
		result := make([]byte, 8+resLen)
		binary.BigEndian.PutUint32(result[:4], uint32(prefix))
		binary.BigEndian.PutUint32(result[4:8], uint32(a.ExtLen))
		copy(result[8:], addr)
		return result, nil
	case AddressStd:
		addr, err := hex.DecodeString(a.Addr)
		if err != nil {
			return nil, err
		}
		result := make([]byte, 36)
		binary.BigEndian.PutUint32(result[:4], uint32(a.Workchain))
		copy(result[4:], addr)
		return result, nil
	case AddressVar:
		return nil, fmt.Errorf("address_var is not supported")
	default:
		return nil, fmt.Errorf("unknown type of account address: %v", a.Kind)
	}
}

func (a *BytesType) BytesValue() ([]byte, error) {
	res, err := base64.StdEncoding.DecodeString(string(*a))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (a *HashType) BytesValue() ([]byte, error) {
	res, err := base64.StdEncoding.DecodeString(string(*a))
	if err != nil {
		return nil, err
	}
	return res, nil
}

// postgresql representation for filters
func (h HashType) FilterString() string {
	if len(h) == 0 {
		return "NULL"
	}
	s, err := base64.StdEncoding.DecodeString(string(h))
	if err != nil {
		log.Printf("failed to encode address: %s", h.String())
		return "NULL"
	}
	return fmt.Sprintf("\\x%X", s)
}

func (a AccountAddress) FilterString() string {
	if len(a) == 0 {
		return "NULL"
	}
	s, err := ParseAccountAddressStruct(string(a))
	if err != nil {
		log.Printf("failed to encode address: %s", a.String())
		return "NULL"
	}
	if s.Kind == AddressNone || s.Kind == AddressVar {
		return "NULL"
	}
	res, err := s.BytesValue()
	if err != nil {
		log.Printf("failed to encode address: %s", a.String())
		return "NULL"
	}
	return fmt.Sprintf("\\x%x", res)
}

// marshal
func (v *ShardId) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

func (v *AccountAddress) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

func (v *HexInt) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

func (v *HashType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

func (v *OpcodeType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

func (v *BytesType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", v.String())), nil
}

// unmarshal
func (a *AccountAddress) UnmarshalText(text []byte) error {
	parsed, err := ParseAccountAddress(string(text))
	if err != nil {
		return err
	}
	*a = *parsed
	return nil
}

func (a *HashType) UnmarshalText(text []byte) error {
	value := string(text)
	if len(value) == 64 || len(value) == 66 && strings.HasPrefix(value, "0x") {
		value = strings.TrimPrefix(value, "0x")
		if res, err := hex.DecodeString(value); err != nil {
			return err
		} else {
			*a = HashType(base64.StdEncoding.EncodeToString(res))
			return nil
		}
	}
	if len(value) == 44 {
		if res, err := base64.StdEncoding.DecodeString(value); err == nil {
			*a = HashType(base64.StdEncoding.EncodeToString(res))
			return nil
		} else if res, err := base64.URLEncoding.DecodeString(value); err == nil {
			*a = HashType(base64.StdEncoding.EncodeToString(res))
			return nil
		} else {
			return err
		}
	}
	return fmt.Errorf("invalid hash type: %s", value)
}

func (a *ShardId) UnmarshalText(text []byte) error {
	value := string(text)
	value = strings.TrimPrefix(value, "0x")
	if shard, err := strconv.ParseUint(value, 16, 64); err == nil {
		*a = ShardId(shard)
		return nil
	}
	if shard, err := strconv.ParseInt(value, 10, 64); err == nil {
		*a = ShardId(shard)
		return nil
	}
	return fmt.Errorf("invalid shard id: %s", value)
}

func (a *UtimeType) UnmarshalText(text []byte) error {
	value := string(text)
	if utime, err := strconv.ParseUint(value, 10, 32); err == nil {
		*a = UtimeType(utime)
		return nil
	}
	if utime, err := strconv.ParseFloat(value, 64); err == nil {
		*a = UtimeType(uint64(utime))
		return nil
	}
	return fmt.Errorf("invalid utime type: %s", value)
}

func (a *OpcodeType) UnmarshalText(text []byte) error {
	value := string(text)
	value = strings.TrimPrefix(value, "0x")
	if res, err := strconv.ParseUint(value, 16, 32); err == nil {
		*a = OpcodeType(res)
		return nil
	}
	if res, err := strconv.ParseInt(value, 10, 32); err == nil {
		*a = OpcodeType(res)
		return nil
	}
	return fmt.Errorf("invalid opcode type: %s", value)
}

// converters
func AddressInformationFromV3(state AccountStateFull) (*V2AddressInformation, error) {
	var info V2AddressInformation
	if state.Balance == nil {
		return nil, IndexError{Code: 500, Message: "balance is none"}
	}
	info.Balance = *state.Balance
	info.Code = (*string)(state.CodeBoc)
	info.Data = (*string)(state.DataBoc)
	info.LastTransactionHash = (*string)(state.LastTransactionHash)
	if state.LastTransactionLt != nil {
		info.LastTransactionLt = new(string)
		*info.LastTransactionLt = fmt.Sprintf("%d", *state.LastTransactionLt)
	}
	if state.AccountStatus == nil {
		return nil, IndexError{Code: 500, Message: "status is none"}
	}
	info.Status = *state.AccountStatus
	return &info, nil
}

func WalletInformationFromV3(state WalletState) (*V2WalletInformation, error) {
	var info V2WalletInformation
	if !state.IsWallet && !(state.AccountStatus != nil && *state.AccountStatus == "uninit") {
		return nil, nil
	}

	if state.Balance == nil {
		return nil, IndexError{Code: 500, Message: "balance is none"}
	}
	info.Balance = *state.Balance

	info.WalletType = state.WalletType
	info.Seqno = state.Seqno
	info.WalletId = state.WalletId

	if state.LastTransactionHash == nil {
		return nil, IndexError{Code: 500, Message: "last_transaction_hash is none"}
	}
	info.LastTransactionHash = string(*state.LastTransactionHash)
	if state.LastTransactionLt == nil {
		return nil, IndexError{Code: 500, Message: "last_transaction_lt is none"}
	}
	info.LastTransactionLt = fmt.Sprintf("%d", *state.LastTransactionLt)

	if state.AccountStatus == nil {
		return nil, IndexError{Code: 500, Message: "status is none"}
	}
	info.Status = *state.AccountStatus
	return &info, nil
}

// utils
func GetAccountAddressFriendly(account AccountAddress, code_hash *HashType, is_testnet bool) string {
	addr, err := address.ParseRawAddr(account.String())
	if err != nil {
		return "addr_none"
	}
	bouncable := true
	if code_hash == nil || WalletsHashMap[code_hash.String()] {
		bouncable = false
	}
	addr.SetBounce(bouncable)
	addr.SetTestnetOnly(is_testnet)
	return addr.String()
}

// converters
func HashConverter(value string) reflect.Value {
	if len(value) == 64 || len(value) == 66 && strings.HasPrefix(value, "0x") {
		value = strings.TrimPrefix(value, "0x")
		if res, err := hex.DecodeString(value); err == nil {
			return reflect.ValueOf(HashType(base64.StdEncoding.EncodeToString(res)))
		} else {
			return reflect.Value{}
		}
	}
	if len(value) == 44 {
		if res, err := base64.StdEncoding.DecodeString(value); err == nil {
			return reflect.ValueOf(HashType(base64.StdEncoding.EncodeToString(res)))
		} else if res, err := base64.URLEncoding.DecodeString(value); err == nil {
			return reflect.ValueOf(HashType(base64.StdEncoding.EncodeToString(res)))
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
