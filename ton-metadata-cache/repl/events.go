package repl

import "time"

// Operation represents the type of database operation
type Operation string

const (
	Insert Operation = "INSERT"
	Update Operation = "UPDATE"
	Delete Operation = "DELETE"
)

// ChangeEvent represents a single change event from PostgreSQL logical replication
type ChangeEvent struct {
	// Table is the fully qualified table name in "schema.table" format
	Table string

	// Operation is the type of change (INSERT, UPDATE, DELETE)
	Operation Operation

	// Data contains the new row data (for INSERT and UPDATE)
	Data map[string]interface{}

	// OldData contains the previous row data (for UPDATE and DELETE, if REPLICA IDENTITY is set)
	OldData map[string]interface{}

	// Timestamp is when the event was received
	Timestamp time.Time

	// Xid is the transaction ID
	Xid uint32
}
