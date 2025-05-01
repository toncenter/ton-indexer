package models

import (
	"fmt"

	"github.com/toncenter/ton-indexer/ton-index-go/index"
	"github.com/vmihailenco/msgpack/v5"
)

type GenericInterface struct {
	Value interface{}
}

var _ msgpack.CustomDecoder = (*GenericInterface)(nil)

func (g *GenericInterface) DecodeMsgpack(dec *msgpack.Decoder) error {
	length, err := dec.DecodeArrayLen()
	if err != nil {
		return err
	}
	if length != 2 {
		return fmt.Errorf("invalid variant array length: %d", length)
	}
	index, err := dec.DecodeUint8()
	if err != nil {
		return err
	}
	switch index {
	case 0:
		var a JettonWalletInterface
		err = dec.Decode(&a)
		g.Value = &a
	case 1:
		var a JettonMasterInterface
		err = dec.Decode(&a)
		g.Value = &a
	case 2:
		var a NftItemInterface
		err = dec.Decode(&a)
		g.Value = &a
	case 3:
		var a NftCollectionInterface
		err = dec.Decode(&a)
		g.Value = &a
	default:
		var a UnsupportedInterface
		g.Value = &a
	}
	return err
}

type JettonWalletInterface struct {
	Balance string `msgpack:"balance"`
	Address string `msgpack:"address"`
	Owner   string `msgpack:"owner"`
	Jetton  string `msgpack:"jetton"`
}

type JettonMasterInterface struct {
	Address              string             `msgpack:"address"`
	TotalSupply          string             `msgpack:"total_supply"`
	Mintable             bool               `msgpack:"mintable"`
	AdminAddress         *string            `msgpack:"admin_address"`
	JettonContent        *map[string]string `msgpack:"jetton_content"`
	JettonWalletCodeHash string             `msgpack:"jetton_wallet_code_hash"`
}

type NftItemInterface struct {
	Address           string             `msgpack:"address"`
	Init              bool               `msgpack:"init"`
	Index             string             `msgpack:"index"`
	CollectionAddress *string            `msgpack:"collection_address"`
	OwnerAddress      *string            `msgpack:"owner_address"`
	Content           *map[string]string `msgpack:"content"`
}

type NftCollectionInterface struct {
	Address           string             `msgpack:"address"`
	NextItemIndex     string             `msgpack:"next_item_index"`
	OwnerAddress      *string            `msgpack:"owner_address"`
	CollectionContent *map[string]string `msgpack:"collection_content"`
}

type UnsupportedInterface struct {
}

type AddressInterfaces struct {
	Interfaces []GenericInterface `msgpack:"interfaces"`
}

func convertTokenDataToTokenInfo(tokenType string, content map[string]string) index.TokenInfo {
	var uri, name, description, image, image_data, symbol, decimals, amount_style, render_type *string
	if u, ok := content["uri"]; ok {
		uri = &u
	}
	if n, ok := content["name"]; ok {
		name = &n
	}
	if d, ok := content["description"]; ok {
		description = &d
	}
	if i, ok := content["image"]; ok {
		image = &i
	}
	if id, ok := content["image_data"]; ok {
		image_data = &id
	}
	if s, ok := content["symbol"]; ok {
		symbol = &s
	}
	if d, ok := content["decimals"]; ok {
		decimals = &d
	}
	if as, ok := content["amount_style"]; ok {
		amount_style = &as
	}
	if rt, ok := content["render_type"]; ok {
		render_type = &rt
	}
	extra := make(map[string]interface{})
	if uri != nil {
		extra["uri"] = *uri
	}
	if image_data != nil {
		extra["image_data"] = *image_data
	}
	if decimals != nil {
		extra["decimals"] = *decimals
	}
	if amount_style != nil {
		extra["amount_style"] = *amount_style
	}
	if render_type != nil {
		extra["render_type"] = *render_type
	}

	tokenInfo := index.TokenInfo{
		Type:        &tokenType,
		Name:        name,
		Symbol:      symbol,
		Description: description,
		Image:       image,
		Extra:       extra,
	}
	return tokenInfo
}

func appendRawInterfacesDataToMetadata(addr string, interfacesRaw string, metadata *index.Metadata) error {
	if _, hasMetadata := (*metadata)[addr]; hasMetadata {
		return nil
	}
	if interfacesRaw == "" {
		return nil
	}
	var addrInterfaces AddressInterfaces

	err := msgpack.Unmarshal([]byte(interfacesRaw), &addrInterfaces)
	if err != nil {
		return fmt.Errorf("failed to unmarshal interfaces wrapper: %w", err)
	}

	addressMetadata := index.AddressMetadata{
		IsIndexed: false,
		TokenInfo: []index.TokenInfo{},
	}

	for _, iface := range addrInterfaces.Interfaces {
		switch val := iface.Value.(type) {
		case *JettonMasterInterface:
			addressMetadata.TokenInfo = append(addressMetadata.TokenInfo, convertTokenDataToTokenInfo("jetton_masters", *val.JettonContent))
		case *NftItemInterface:
			addressMetadata.TokenInfo = append(addressMetadata.TokenInfo, convertTokenDataToTokenInfo("nft_items", *val.Content))
		case *NftCollectionInterface:
			addressMetadata.TokenInfo = append(addressMetadata.TokenInfo, convertTokenDataToTokenInfo("nft_collections", *val.CollectionContent))
		}
	}
	if len(addressMetadata.TokenInfo) > 0 {
		(*metadata)[addr] = addressMetadata
	}

	return nil
}
