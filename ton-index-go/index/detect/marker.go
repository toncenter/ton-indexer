package detect

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

// batch request to a c++ library (stub implementation)
func MarkerRequest(opcodesList []uint32, bocBase64List []string) ([]string, []string, error) {
	return []string{}, []string{}, nil
}

func MarkMessagesByPtr(messages []*Message) error {
	return nil
}

func MarkMessages(messages []Message) error {
	return nil
}

func MarkJettonTransfers(transfers []JettonTransfer) error {
	return nil
}

func MarkJettonBurns(burns []JettonBurn) error {
	return nil
}

func MarkNFTTransfers(transfers []NFTTransfer) error {
	return nil
}
