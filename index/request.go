package index

import "time"

// settings
type RequestSettings struct {
	Timeout time.Duration
}

// requests
type BlockRequest struct {
	Workchain *int32 `query:"workchain"`
	Shard     *int64 `query:"shard"`
	Seqno     *int32 `query:"seqno"`
	McSeqno   *int32 `query:"mc_seqno"`
}

type TransactionRequest struct {
	Account        []string `query:"account"`
	ExcludeAccount []string `query:"exclude_account"`
	Hash           *string  `query:"hash"`
	Lt             *uint64  `query:"lt"`
}

type MessageRequest struct {
	Direction   *string  `query:"direction"`
	MessageHash []string `query:"msg_hash"`
	Source      *string  `query:"source"`
	Destination *string  `query:"destination"`
	BodyHash    *string  `query:"body_hash"`
}

type UtimeRequest struct {
	StartUtime *uint32 `query:"start_utime"`
	EndUtime   *uint32 `query:"end_utime"`
}
type LtRequest struct {
	StartLt *uint64 `query:"start_lt"`
	EndLt   *uint64 `query:"end_lt"`
}

type SortType string

const (
	DESC SortType = "desc"
	ASC  SortType = "asc"
)

type LimitRequest struct {
	Limit  *int32    `query:"limit"`
	Offset *int32    `query:"offset"`
	Sort   *SortType `query:"sort"`
}
