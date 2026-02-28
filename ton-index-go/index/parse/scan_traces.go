package parse

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"

	"github.com/jackc/pgx/v5"
)

func ScanTrace(row pgx.Row) (*Trace, error) {
	var trace Trace
	err := row.Scan(&trace.TraceId, &trace.ExternalHash, &trace.McSeqnoStart, &trace.McSeqnoEnd,
		&trace.StartLt, &trace.StartUtime, &trace.EndLt, &trace.EndUtime,
		&trace.TraceMeta.TraceState, &trace.TraceMeta.Messages, &trace.TraceMeta.Transactions,
		&trace.TraceMeta.PendingMessages, &trace.TraceMeta.ClassificationState)

	if err != nil {
		return nil, err
	}
	return &trace, nil
}
