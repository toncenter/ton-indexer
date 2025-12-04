package repl

import (
	"errors"
	"time"
)

// Config holds the configuration for the Replicator
type Config struct {
	// ConnectionString is the PostgreSQL connection string for replication
	// Must include replication=database parameter
	ConnectionString string

	// SlotName is the name of the replication slot to use
	// If empty, defaults to "repl_slot"
	SlotName string

	// PublicationName is the name of the PostgreSQL publication
	// If empty, defaults to "repl_publication"
	PublicationName string

	// Tables is the list of tables to watch in "schema.table" format
	// e.g., []string{"public.users", "public.orders"}
	Tables []string

	// TemporarySlot if true, creates a temporary replication slot that is
	// automatically dropped when the connection closes
	TemporarySlot bool

	// StandbyMessageTimeout is how often to send standby status updates
	// Defaults to 10 seconds if not set
	StandbyMessageTimeout time.Duration

	// EventBufferSize is the size of the events channel buffer
	// Defaults to 100 if not set
	EventBufferSize int

	// CreatePublication if true, drops existing and creates new publication
	CreatePublication bool
}

// Validate checks the configuration and returns an error if invalid
func (c *Config) Validate() error {
	if c.ConnectionString == "" {
		return errors.New("ConnectionString is required")
	}
	if len(c.Tables) == 0 {
		return errors.New("at least one table must be specified")
	}
	return nil
}

// applyDefaults sets default values for optional configuration fields
func (c *Config) applyDefaults() {
	if c.SlotName == "" {
		c.SlotName = "repl_slot"
	}
	if c.PublicationName == "" {
		c.PublicationName = "repl_publication_test"
	}
	if c.StandbyMessageTimeout == 0 {
		c.StandbyMessageTimeout = 10 * time.Second
	}
	if c.EventBufferSize == 0 {
		c.EventBufferSize = 100
	}
}

// tableSet represents a set of tables for quick lookup
type tableSet map[string]struct{}

// buildTableSet creates a set from the Tables slice for O(1) lookups
func (c *Config) buildTableSet() tableSet {
	set := make(tableSet, len(c.Tables))
	for _, t := range c.Tables {
		set[t] = struct{}{}
	}
	return set
}

// contains checks if a table is in the set
func (ts tableSet) contains(table string) bool {
	_, ok := ts[table]
	return ok
}
