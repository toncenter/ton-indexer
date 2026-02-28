package parse

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"

	"github.com/jackc/pgx/v5"
)

func ScanMessageWithContent(row pgx.Row) (*Message, error) {
	var m Message
	var body MessageContent
	var init_state MessageContent

	err := row.Scan(&m.TxHash, &m.TxLt, &m.MsgHash, &m.Direction, &m.TraceId, &m.Source, &m.Destination,
		&m.Value, &m.ValueExtraCurrencies, &m.FwdFee, &m.IhrFee, &m.ExtraFlags, &m.CreatedLt, &m.CreatedAt, &m.Opcode,
		&m.IhrDisabled, &m.Bounce, &m.Bounced, &m.ImportFee, &m.BodyHash, &m.InitStateHash, &m.MsgHashNorm,
		&m.InMsgTxHash, &m.OutMsgTxHash, &body.Hash, &body.Body, &init_state.Hash, &init_state.Body)
	if body.Hash != nil {
		m.MessageContent = &body
	}
	if init_state.Hash != nil {
		m.InitState = &init_state
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func ScanMessageContent(row pgx.Row) (*MessageContent, error) {
	var mc MessageContent
	err := row.Scan(&mc.Hash, &mc.Body)
	if err != nil {
		return nil, err
	}
	return &mc, nil
}
