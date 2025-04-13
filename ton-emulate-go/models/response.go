package models

import (
	"github.com/toncenter/ton-indexer/ton-index-go/index"
)

type EmulateTraceResponse struct {
	McBlockSeqno uint32                                `json:"mc_block_seqno"`
	Trace        index.TraceNode                       `json:"trace"`
	Transactions map[index.HashType]*index.Transaction `json:"transactions"`
	Actions      *[]*index.Action                      `json:"actions,omitempty"`
	CodeCells    *map[Hash]string                      `json:"code_cells,omitempty"`
	DataCells    *map[Hash]string                      `json:"data_cells,omitempty"`
	AddressBook  *index.AddressBook                    `json:"address_book,omitempty"`
	Metadata     *index.Metadata                       `json:"metadata,omitempty"`
	RandSeed     string                                `json:"rand_seed"`
	IsIncomplete bool                                  `json:"is_incomplete"`
}
