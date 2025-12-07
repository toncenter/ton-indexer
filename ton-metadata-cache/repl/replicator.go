package repl

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Replicator manages PostgreSQL logical replication and emits change events
type Replicator struct {
	config   Config
	events   chan ChangeEvent
	tableSet tableSet
	conn     *pgconn.PgConn
	decoder  *decoder
	lastMsg  atomic.Int64
}

// NewReplicator creates a new Replicator with the given configuration
func NewReplicator(cfg Config) (*Replicator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	cfg.applyDefaults()

	tableSet := cfg.buildTableSet()

	return &Replicator{
		config:   cfg,
		events:   make(chan ChangeEvent, cfg.EventBufferSize),
		tableSet: tableSet,
		decoder:  newDecoder(tableSet),
	}, nil
}

// Events returns a read-only channel that emits change events
func (r *Replicator) Events() <-chan ChangeEvent {
	return r.events
}

func (r *Replicator) TimeSinceLastMsg() time.Duration {
	lastTime := r.lastMsg.Load()
	return time.Since(time.UnixMilli(lastTime))
}

// Start begins the replication process. It blocks until the context is cancelled
// or an unrecoverable error occurs. The events channel is closed when Start returns.
func (r *Replicator) Start(ctx context.Context) error {
	defer close(r.events)

	if err := r.connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer r.conn.Close(ctx)

	if r.config.CreatePublication {
		if err := r.setupPublication(ctx); err != nil {
			return fmt.Errorf("setup publication: %w", err)
		}
	}

	if err := r.createReplicationSlot(ctx); err != nil {
		return fmt.Errorf("create replication slot: %w", err)
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, r.conn)
	if err != nil {
		return fmt.Errorf("identify system: %w", err)
	}

	if err := r.startReplication(ctx, sysident.XLogPos); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	return r.receiveMessages(ctx, sysident.XLogPos)
}

// Close stops the replicator and closes the connection
func (r *Replicator) Close() error {
	if r.conn != nil {
		return r.conn.Close(context.Background())
	}
	return nil
}

// connect establishes a connection to PostgreSQL
func (r *Replicator) connect(ctx context.Context) error {
	conn, err := pgconn.Connect(ctx, r.config.ConnectionString)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	r.conn = conn
	return nil
}

// setupPublication creates or updates the publication for the configured tables
func (r *Replicator) setupPublication(ctx context.Context) error {
	// Drop existing publication if exists
	//dropSQL := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", r.config.PublicationName)
	//result := r.conn.Exec(ctx, dropSQL)
	//if _, err := result.ReadAll(); err != nil {
	//	return fmt.Errorf("drop publication: %w", err)
	//}

	// Create publication for specific tables
	tableList := strings.Join(r.config.Tables, ", ")
	createSQL := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s;", r.config.PublicationName, tableList)
	result := r.conn.Exec(ctx, createSQL)
	if _, err := result.ReadAll(); err != nil {
		return fmt.Errorf("create publication: %w", err)
	}

	return nil
}

// createReplicationSlot creates the replication slot
func (r *Replicator) createReplicationSlot(ctx context.Context) error {
	_, err := pglogrepl.CreateReplicationSlot(
		ctx,
		r.conn,
		r.config.SlotName,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: r.config.TemporarySlot,
		},
	)
	if err != nil {
		// Check if slot already exists
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return err
	}
	return nil
}

// startReplication begins the replication stream
func (r *Replicator) startReplication(ctx context.Context, startPos pglogrepl.LSN) error {
	pluginArgs := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", r.config.PublicationName),
		"messages 'true'",
		"streaming 'true'",
	}

	return pglogrepl.StartReplication(
		ctx,
		r.conn,
		r.config.SlotName,
		startPos,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArgs,
		},
	)
}

// receiveMessages is the main loop that receives and processes replication messages
func (r *Replicator) receiveMessages(ctx context.Context, startPos pglogrepl.LSN) error {
	clientXLogPos := startPos
	nextStandbyDeadline := time.Now().Add(r.config.StandbyMessageTimeout)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Send standby status update if needed
		if time.Now().After(nextStandbyDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: clientXLogPos,
			})
			if err != nil {
				return fmt.Errorf("send standby status: %w", err)
			}
			nextStandbyDeadline = time.Now().Add(r.config.StandbyMessageTimeout)
		}

		// Receive message with timeout
		msgCtx, cancel := context.WithDeadline(ctx, nextStandbyDeadline)
		rawMsg, err := r.conn.ReceiveMessage(msgCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) || pgconn.SafeToRetry(err) {
				continue
			}
			return fmt.Errorf("receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("postgres error: %s", errMsg.Message)
		}
		r.lastMsg.Store(time.Now().UnixMilli())

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				nextStandbyDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlog data: %w", err)
			}

			event, err := r.decoder.decode(xld.WALData)
			if err != nil {
				return fmt.Errorf("decode wal data: %w", err)
			}

			if event != nil {
				select {
				case r.events <- *event:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}
		}
	}
}
