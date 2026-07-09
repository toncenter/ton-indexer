package crud

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type fedConns struct {
	db             *DbClient
	ctx            context.Context
	acquireTimeout time.Duration
	split          hotColdSplit
	federated      bool
	utimeMargin    uint64

	hotConn, coldConn *pgxpool.Conn // nil until first use
}

func (fc *fedConns) acquireContext() (context.Context, context.CancelFunc) {
	base := fc.ctx
	if base == nil {
		base = context.Background()
	}
	if fc.acquireTimeout > 0 {
		return context.WithTimeout(base, fc.acquireTimeout)
	}
	return context.WithCancel(base)
}

func (fc *fedConns) acquire(pool *pgxpool.Pool) (*pgxpool.Conn, error) {
	ctx, cancel := fc.acquireContext()
	defer cancel()
	return pool.Acquire(ctx)
}

// cold returns the default-pool connection, acquiring it on first use.
func (fc *fedConns) cold() (*pgxpool.Conn, error) {
	if fc.coldConn == nil {
		c, err := fc.acquire(fc.db.Pool)
		if err != nil {
			return nil, err
		}
		fc.coldConn = c
	}
	return fc.coldConn, nil
}

// hot returns the hot-pool connection, or aliases cold in standalone mode.
func (fc *fedConns) hot() (*pgxpool.Conn, error) {
	if !fc.federated {
		return fc.cold()
	}
	if fc.hotConn == nil {
		c, err := fc.acquire(fc.db.HotPool)
		if err != nil {
			return nil, err
		}
		fc.hotConn = c
	}
	return fc.hotConn, nil
}

func (fc *fedConns) conn(cold bool) (*pgxpool.Conn, error) {
	if cold {
		return fc.cold()
	}
	return fc.hot()
}

func (fc *fedConns) connForSeqno(mcSeqno uint64) (*pgxpool.Conn, error) {
	return fc.conn(fc.coldForSeqno(mcSeqno))
}

func (fc *fedConns) coldForSeqno(mcSeqno uint64) bool {
	return fc.federated && mcSeqno < fc.split.Seqno
}

func (db *DbClient) acquireFed(ctx context.Context) (*fedConns, func(), error) {
	fc := &fedConns{db: db, ctx: ctx, federated: db.HotPool != nil}
	if fc.federated {
		split, err := db.Split.Split(ctx)
		if err != nil {
			return nil, nil, err
		}
		fc.split = split
	}
	fc.utimeMargin = db.RouterUtimeMargin
	release := func() {
		coldConn := fc.coldConn
		hotConn := fc.hotConn
		fc.coldConn = nil
		fc.hotConn = nil
		if coldConn != nil {
			coldConn.Release()
		}
		if hotConn != nil && hotConn != coldConn {
			hotConn.Release()
		}
	}
	return fc, release, nil
}

// hotThenCold is for point lookups: try hot, then cold, and let hot rows win.
// Hot errors degrade to cold-only; cold errors stay fatal because hot is partial.
func hotThenCold[T any, K comparable](fc *fedConns, expect int,
	fetch func(conn *pgxpool.Conn) ([]T, error),
	keyOf func(*T) K,
) ([]T, error) {
	hotConn, hotErr := fc.hot()
	var rows []T
	if hotErr == nil {
		rows, hotErr = fetch(hotConn)
	}
	if hotErr == nil && (!fc.federated || (expect >= 0 && len(rows) >= expect)) {
		return rows, nil
	}
	if !fc.federated {
		return nil, hotErr // single pool: nothing else to try
	}
	coldConn, err := fc.cold()
	if err != nil {
		return nil, err
	}
	coldRows, err := fetch(coldConn)
	if err != nil {
		return nil, err
	}
	if hotErr != nil {
		return coldRows, nil
	}
	seen := make(map[K]bool, len(rows))
	for i := range rows {
		seen[keyOf(&rows[i])] = true
	}
	for i := range coldRows {
		if !seen[keyOf(&coldRows[i])] {
			rows = append(rows, coldRows[i])
		}
	}
	return rows, nil
}
