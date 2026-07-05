package models

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/xssnick/tonutils-go/address"
)

const (
	tonAddressMaxBits = 511
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
		return "addr_none"
	case AddressStd:
		return fmt.Sprintf("%d:%s", a.Workchain, a.Addr)
	case AddressVar:
		return fmt.Sprintf("var$%d:%d:%s", a.Workchain, a.AddrLen, a.Addr)
	case AddressExt:
		return fmt.Sprintf("ext$%d:%s", a.ExtLen, a.Addr)
	default:
		return "unsupported"
	}
}

func addressHexValue(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	default:
		return -1
	}
}

func parseAddressBitLen(value string) (int32, error) {
	bitLen, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, err
	}
	if bitLen < 0 || bitLen > tonAddressMaxBits {
		return 0, fmt.Errorf("invalid address bit length: %d", bitLen)
	}
	return int32(bitLen), nil
}

func normalizeAddressHex(value string, bitLen int32) (string, error) {
	expectedLen := int((bitLen + 3) / 4)
	if len(value) != expectedLen {
		return "", fmt.Errorf("wrong address hex length: %d != %d", len(value), expectedLen)
	}
	for i := 0; i < len(value); i++ {
		if addressHexValue(value[i]) < 0 {
			return "", fmt.Errorf("invalid address hex value: %s", value)
		}
	}
	if expectedLen > 0 {
		unusedBits := (4 - (bitLen & 3)) & 3
		if unusedBits != 0 {
			lastNibble := addressHexValue(value[expectedLen-1])
			if lastNibble&((1<<unusedBits)-1) != 0 {
				return "", fmt.Errorf("unused address bits must be zero")
			}
		}
	}
	return strings.ToUpper(value), nil
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
	if strings.HasPrefix(value, "ext$") {
		value = strings.TrimPrefix(value, "ext$")
		parts := strings.SplitN(value, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid address format: %s", value)
		}

		res := AccountAddressStruct{Kind: AddressExt}

		extLen, err := parseAddressBitLen(parts[0])
		if err != nil {
			return nil, err
		}
		res.ExtLen = extLen

		extAddr, err := normalizeAddressHex(parts[1], extLen)
		if err != nil {
			return nil, err
		}
		res.Addr = extAddr
		return &res, nil
	}

	// parse var address
	if strings.HasPrefix(value, "var$") {
		value = strings.TrimPrefix(value, "var$")
		parts := strings.SplitN(value, ":", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid address format: %s", value)
		}

		workchain, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			return nil, err
		}
		addrLen, err := parseAddressBitLen(parts[1])
		if err != nil {
			return nil, err
		}
		addrHex, err := normalizeAddressHex(parts[2], addrLen)
		if err != nil {
			return nil, err
		}
		return &AccountAddressStruct{
			Kind:      AddressVar,
			Workchain: int32(workchain),
			AddrLen:   addrLen,
			Addr:      addrHex,
		}, nil
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

func MustParseAccountAddress(s string) AccountAddress {
	val, err := ParseAccountAddress(s)
	if err != nil {
		panic(err)
	}
	return *val
}

func ParseHashType(s string) (*HashType, error) {
	val, err := ParseHashBytes(s)
	if err != nil {
		return nil, err
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

// types representation for filters
func (h HashType) FilterString() string {
	if len(h) == 0 {
		return "NULL"
	}
	return h.String()
}

func (a AccountAddress) FilterString() string {
	if len(a) == 0 {
		return "NULL"
	}
	return a.String()
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

func ParseHashBytes(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if len(value) == 66 && (strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X")) {
		value = value[2:]
	}
	if len(value) == 64 {
		bytes, err := hex.DecodeString(value)
		if err != nil {
			return nil, err
		}
		if len(bytes) == 32 {
			return bytes, nil
		}
	}
	if len(value) == 43 || len(value) == 44 {
		for _, encoding := range []*base64.Encoding{
			base64.StdEncoding,
			base64.URLEncoding,
			base64.RawStdEncoding,
			base64.RawURLEncoding,
		} {
			bytes, err := encoding.DecodeString(value)
			if err == nil && len(bytes) == 32 {
				return bytes, nil
			}
		}
	}
	return nil, fmt.Errorf("expected 32-byte hash in hex, base64, or base64url format")
}

// converters
func HashConverter(value string) reflect.Value {
	res, err := ParseHashBytes(value)
	if err != nil {
		return reflect.Value{}
	}
	return reflect.ValueOf(HashType(base64.StdEncoding.EncodeToString(res)))
}

func AccountAddressConverter(value string) reflect.Value {
	parsed, err := ParseAccountAddress(value)
	if err != nil {
		return reflect.Value{}
	}
	return reflect.ValueOf(*parsed)
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
	hasHexPrefix := strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X")
	if hasHexPrefix {
		value = value[2:]
	}
	if res, err := strconv.ParseUint(value, 16, 32); err == nil {
		return reflect.ValueOf(OpcodeType(int32(uint32(res))))
	}
	if hasHexPrefix {
		return reflect.Value{}
	}
	if res, err := strconv.ParseInt(value, 10, 32); err == nil {
		return reflect.ValueOf(OpcodeType(res))
	}
	return reflect.Value{}
}
