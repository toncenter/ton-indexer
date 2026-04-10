package models

import (
	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

type EmulateTraceResponse struct {
	McBlockSeqno uint32                                            `json:"mc_block_seqno"`
	Trace        indexModels.TraceNode                             `json:"trace"`
	Transactions map[indexModels.HashType]*indexModels.Transaction `json:"transactions"`
	Actions      *[]*indexModels.Action                            `json:"actions,omitempty"`
	CodeCells    *map[Hash]string                                  `json:"code_cells,omitempty"`
	DataCells    *map[Hash]string                                  `json:"data_cells,omitempty"`
	AddressBook  *indexModels.AddressBook                          `json:"address_book,omitempty"`
	Metadata     *indexModels.Metadata                             `json:"metadata,omitempty"`
	RandSeed     string                                            `json:"rand_seed"`
	IsIncomplete bool                                              `json:"is_incomplete"`
}
