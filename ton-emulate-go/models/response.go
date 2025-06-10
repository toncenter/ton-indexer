package models

import (
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

type EmulateTraceResponse struct {
	McBlockSeqno uint32                                  `json:"mc_block_seqno"`
	Trace        models.TraceNode                        `json:"trace"`
	Transactions map[models.HashType]*models.Transaction `json:"transactions"`
	Actions      *[]*models.Action                       `json:"actions,omitempty"`
	CodeCells    *map[Hash]string                        `json:"code_cells,omitempty"`
	DataCells    *map[Hash]string                        `json:"data_cells,omitempty"`
	AddressBook  *models.AddressBook                     `json:"address_book,omitempty"`
	Metadata     *models.Metadata                        `json:"metadata,omitempty"`
	RandSeed     string                                  `json:"rand_seed"`
	IsIncomplete bool                                    `json:"is_incomplete"`
}
