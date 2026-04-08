package models

type FilterStringInterface interface {
	FilterString() string
}

type AddressKind int

const (
	AddressNone AddressKind = iota
	AddressExt
	AddressStd
	AddressVar
)

type AccountAddressStruct struct {
	Kind      AddressKind
	Workchain int32
	ExtLen    int32
	Addr      string
}

type ShardId int64                 // @name ShardId
type AccountAddress string         // @name AccountAddress
type AccountAddressNullable string // @name AccountAddressNullable
type BytesType string              // @name BytesType
type HashType string               // @name HashType
type HexInt int64                  // @name HexInt
type OpcodeType int64              // @name OpcodeType

var WalletsHashMap = map[string]bool{
	"oM/CxIruFqJx8s/AtzgtgXVs7LEBfQd/qqs7tgL2how=": true,
	"1JAvzJ+tdGmPqONTIgpo2g3PcuMryy657gQhfBfTBiw=": true,
	"WHzHie/xyE9G7DeX5F/ICaFP9a4k8eDHpqmcydyQYf8=": true,
	"XJpeaMEI4YchoHxC+ZVr+zmtd+xtYktgxXbsiO7mUyk=": true,
	"/pUw0yQ4Uwg+8u8LTCkIwKv2+hwx6iQ6rKpb+MfXU/E=": true,
	"thBBpYp5gLlG6PueGY48kE0keZ/6NldOpCUcQaVm9YE=": true,
	"hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=": true,
	"ZN1UgFUixb6KnbWc6gEFzPDQh4bKeb64y3nogKjXMi0=": true,
	"/rX/aCDi/w2Ug+fg1iyBfYRniftK5YDIeIZtlZ2r1cA=": true,
	"89fKU0k97trCizgZhqhJQDy6w9LFhHea8IEGWvCsS5M=": true,
	"IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8=": true,
}

type IndexError struct {
	Code    int    `json:"-"`
	Message string `json:"error"`
}

func (e IndexError) Error() string {
	return e.Message
}

type AddressBookRow struct {
	UserFriendly *string   `json:"user_friendly"`
	Domain       *string   `json:"domain"`
	Interfaces   *[]string `json:"interfaces"`
} // @name AddressBookRow

type GenericAddressBook map[string]AddressBookRow  // @name GenericAddressBook
type AddressBook map[AccountAddress]AddressBookRow // @name AddressBook
type Metadata map[AccountAddress]AddressMetadata   // @name Metadata

type BackgroundTask struct {
	Type  string
	Retry int
	Data  map[string]interface{}
}

type AddressMetadata struct {
	IsIndexed bool        `json:"is_indexed"`
	TokenInfo []TokenInfo `json:"token_info"`
} // @name AddressMetadata

type TokenInfo struct {
	Address     AccountAddress         `json:"-"`
	Valid       *bool                  `json:"valid,omitempty"`
	Indexed     bool                   `json:"-"`
	Type        *string                `json:"type,omitempty"`
	Name        *string                `json:"name,omitempty"`
	Symbol      *string                `json:"symbol,omitempty"`
	Description *string                `json:"description,omitempty"`
	Image       *string                `json:"image,omitempty"`
	NftIndex    *string                `json:"nft_index,omitempty"`
	Extra       map[string]interface{} `json:"extra,omitempty"`
} // @name TokenInfo

type JsonType map[string]interface{}

func (a *AccountAddress) IsAddressNone() bool {
	return len(*a) == 0 || *a == "addr_none" || *a == "null"
}

func (a *AccountAddress) IsAddressStd() bool {
	s, err := ParseAccountAddressStruct(string(*a))
	if err != nil {
		return false
	}
	return s.Kind == AddressStd
}
