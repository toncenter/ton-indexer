package repl

import (
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// decoder handles parsing WAL messages into ChangeEvents
type decoder struct {
	relations map[uint32]*pglogrepl.RelationMessageV2
	typeMap   *pgtype.Map
	tableSet  tableSet
	inStream  bool
}

// newDecoder creates a new WAL message decoder
func newDecoder(tables tableSet) *decoder {
	return &decoder{
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
		typeMap:   pgtype.NewMap(),
		tableSet:  tables,
		inStream:  false,
	}
}

// decode parses WAL data and returns a ChangeEvent if applicable
// Returns nil if the message doesn't produce a change event (e.g., begin/commit messages)
// or if the table is not in the configured watch list
func (d *decoder) decode(walData []byte) (*ChangeEvent, error) {
	logicalMsg, err := pglogrepl.ParseV2(walData, d.inStream)
	if err != nil {
		return nil, fmt.Errorf("parse logical replication message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		d.relations[msg.RelationID] = msg
		return nil, nil

	case *pglogrepl.InsertMessageV2:
		return d.handleInsert(msg)

	case *pglogrepl.UpdateMessageV2:
		return d.handleUpdate(msg)

	case *pglogrepl.DeleteMessageV2:
		return d.handleDelete(msg)
	default:
		return nil, nil
	}
}

// handleInsert processes an INSERT message
func (d *decoder) handleInsert(msg *pglogrepl.InsertMessageV2) (*ChangeEvent, error) {
	rel, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	tableName := fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
	if !d.tableSet.contains(tableName) {
		return nil, nil
	}

	data, err := d.decodeTuple(msg.Tuple, rel)
	if err != nil {
		return nil, fmt.Errorf("decode tuple: %w", err)
	}

	return &ChangeEvent{
		Table:     tableName,
		Operation: Insert,
		Data:      data,
		Timestamp: time.Now(),
		Xid:       msg.Xid,
	}, nil
}

// handleUpdate processes an UPDATE message
func (d *decoder) handleUpdate(msg *pglogrepl.UpdateMessageV2) (*ChangeEvent, error) {
	rel, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	tableName := fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
	if !d.tableSet.contains(tableName) {
		return nil, nil
	}

	data, err := d.decodeTuple(msg.NewTuple, rel)
	if err != nil {
		return nil, fmt.Errorf("decode new tuple: %w", err)
	}

	var oldData map[string]interface{}
	if msg.OldTuple != nil {
		oldData, err = d.decodeTuple(msg.OldTuple, rel)
		if err != nil {
			return nil, fmt.Errorf("decode old tuple: %w", err)
		}
	}

	return &ChangeEvent{
		Table:     tableName,
		Operation: Update,
		Data:      data,
		OldData:   oldData,
		Timestamp: time.Now(),
		Xid:       msg.Xid,
	}, nil
}

// handleDelete processes a DELETE message
func (d *decoder) handleDelete(msg *pglogrepl.DeleteMessageV2) (*ChangeEvent, error) {
	rel, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	tableName := fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
	if !d.tableSet.contains(tableName) {
		return nil, nil
	}

	var oldData map[string]interface{}
	var err error
	if msg.OldTuple != nil {
		oldData, err = d.decodeTuple(msg.OldTuple, rel)
		if err != nil {
			return nil, fmt.Errorf("decode old tuple: %w", err)
		}
	}

	return &ChangeEvent{
		Table:     tableName,
		Operation: Delete,
		OldData:   oldData,
		Timestamp: time.Now(),
		Xid:       msg.Xid,
	}, nil
}

// decodeTuple converts a tuple message into a map of column values
func (d *decoder) decodeTuple(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessageV2) (map[string]interface{}, error) {
	if tuple == nil {
		return nil, nil
	}

	values := make(map[string]interface{}, len(tuple.Columns))
	for idx, col := range tuple.Columns {
		if idx >= len(rel.Columns) {
			break
		}
		colName := rel.Columns[idx].Name

		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// TOAST value was not changed, skip
			continue
		case 't': // text
			val, err := d.decodeTextColumnData(col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, fmt.Errorf("decode column %s: %w", colName, err)
			}
			values[colName] = val
		}
	}

	return values, nil
}

// decodeTextColumnData decodes column data using the type map
func (d *decoder) decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := d.typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(d.typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
