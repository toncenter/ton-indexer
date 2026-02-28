package models

type TraceMeta struct {
	TraceState          string `json:"trace_state"`
	Messages            int64  `json:"messages"`
	Transactions        int64  `json:"transactions"`
	PendingMessages     int64  `json:"pending_messages"`
	ClassificationState string `json:"classification_state"`
} // @name TraceMeta

type TraceNode struct {
	TransactionHash HashType     `json:"tx_hash,omitempty"`
	InMsgHash       HashType     `json:"in_msg_hash,omitempty"`
	Transaction     *Transaction `json:"transaction,omitempty"`
	InMsg           *Message     `json:"in_msg,omitempty"`
	Children        []*TraceNode `json:"children"`
} // @name TraceNode

type Trace struct {
	TraceId           *HashType                 `json:"trace_id"`
	ExternalHash      *HashType                 `json:"external_hash"`
	McSeqnoStart      HashType                  `json:"mc_seqno_start"`
	McSeqnoEnd        HashType                  `json:"mc_seqno_end"`
	StartLt           uint64                    `json:"start_lt,string"`
	StartUtime        uint32                    `json:"start_utime"`
	EndLt             *uint64                   `json:"end_lt,string"`
	EndUtime          *uint32                   `json:"end_utime"`
	TraceMeta         TraceMeta                 `json:"trace_info"`
	IsIncomplete      bool                      `json:"is_incomplete"`
	Warning           string                    `json:"warning,omitempty"`
	Actions           *[]*Action                `json:"actions,omitempty"`
	Trace             *TraceNode                `json:"trace,omitempty"`
	TransactionsOrder []HashType                `json:"transactions_order,omitempty"`
	Transactions      map[HashType]*Transaction `json:"transactions,omitempty"`
} // @name Trace
