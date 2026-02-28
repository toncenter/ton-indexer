package parse

import (
	b64 "encoding/base64"
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"log"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type walletInfoParserFunc func(string, *WalletState) error
type walletInfoParser struct {
	Name      string
	ParseFunc walletInfoParserFunc
}

func ParseWalletSeqno(data string, state *WalletState) error {
	boc, err := b64.StdEncoding.DecodeString(data)
	if err != nil {
		return err
	}
	if c, err := cell.FromBOC(boc); err == nil {
		l := c.BeginParse()
		state.Seqno = new(int64)
		*state.Seqno = int64(l.MustLoadUInt(32))
	} else {
		return err
	}
	return nil
}

func ParseWalletV3(data string, state *WalletState) error {
	boc, err := b64.StdEncoding.DecodeString(data)
	if err != nil {
		return err
	}
	if c, err := cell.FromBOC(boc); err == nil {
		l := c.BeginParse()
		state.Seqno = new(int64)
		state.WalletId = new(int64)
		*state.Seqno = int64(l.MustLoadUInt(32))
		*state.WalletId = int64(l.MustLoadUInt(32))
	} else {
		return err
	}
	return nil
}

func ParseWalletV5(data string, state *WalletState) error {
	boc, err := b64.StdEncoding.DecodeString(data)
	if err != nil {
		return err
	}
	if c, err := cell.FromBOC(boc); err == nil {
		l := c.BeginParse()
		state.IsSignatureAllowed = new(bool)
		state.Seqno = new(int64)
		state.WalletId = new(int64)
		*state.IsSignatureAllowed = l.MustLoadBoolBit()
		*state.Seqno = int64(l.MustLoadUInt(32))
		*state.WalletId = int64(l.MustLoadUInt(32))
	} else {
		return err
	}
	return nil
}

func (parser *walletInfoParser) Parse(data string, state *WalletState) error {
	state.IsWallet = true
	state.WalletType = &parser.Name
	err := parser.ParseFunc(data, state)
	if err != nil {
		return err
	}
	return nil
}

var walletParsersMap = map[string]walletInfoParser{
	"oM/CxIruFqJx8s/AtzgtgXVs7LEBfQd/qqs7tgL2how=": {Name: "wallet v1 r1", ParseFunc: ParseWalletSeqno},
	"1JAvzJ+tdGmPqONTIgpo2g3PcuMryy657gQhfBfTBiw=": {Name: "wallet v1 r2", ParseFunc: ParseWalletSeqno},
	"WHzHie/xyE9G7DeX5F/ICaFP9a4k8eDHpqmcydyQYf8=": {Name: "wallet v1 r3", ParseFunc: ParseWalletSeqno},
	"XJpeaMEI4YchoHxC+ZVr+zmtd+xtYktgxXbsiO7mUyk=": {Name: "wallet v2 r1", ParseFunc: ParseWalletSeqno},
	"/pUw0yQ4Uwg+8u8LTCkIwKv2+hwx6iQ6rKpb+MfXU/E=": {Name: "wallet v2 r2", ParseFunc: ParseWalletSeqno},
	"thBBpYp5gLlG6PueGY48kE0keZ/6NldOpCUcQaVm9YE=": {Name: "wallet v3 r1", ParseFunc: ParseWalletV3},
	"hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=": {Name: "wallet v3 r2", ParseFunc: ParseWalletV3},
	"ZN1UgFUixb6KnbWc6gEFzPDQh4bKeb64y3nogKjXMi0=": {Name: "wallet v4 r1", ParseFunc: ParseWalletV3},
	"/rX/aCDi/w2Ug+fg1iyBfYRniftK5YDIeIZtlZ2r1cA=": {Name: "wallet v4 r2", ParseFunc: ParseWalletV3},
	"89fKU0k97trCizgZhqhJQDy6w9LFhHea8IEGWvCsS5M=": {Name: "wallet v5 beta", ParseFunc: ParseWalletV5},
	"IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8=": {Name: "wallet v5 r1", ParseFunc: ParseWalletV5},
}

func ParseWalletState(state AccountStateFull) (*WalletState, error) {
	var info WalletState
	if state.AccountAddress != nil && state.DataBoc != nil {
		if parser, ok := walletParsersMap[string(*state.CodeHash)]; ok {
			if err := parser.Parse(*state.DataBoc, &info); err != nil {
				return nil, err
			}

		} else {
			// log.Println("Parser not found for:", state)
		}
	} else if state.AccountStatus != nil && *state.AccountStatus == "active" {
		log.Println("Failed to parse state:", state)
	}
	info.AccountAddress = *state.AccountAddress
	info.Balance = state.Balance
	info.BalanceExtraCurrencies = state.BalanceExtraCurrencies
	info.AccountStatus = state.AccountStatus
	info.CodeHash = state.CodeHash
	info.LastTransactionHash = state.LastTransactionHash
	info.LastTransactionLt = state.LastTransactionLt
	return &info, nil
}
