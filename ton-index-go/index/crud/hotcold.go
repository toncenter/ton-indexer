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

// hot returns the hot-pool connection, acquiring it on first use
// Standalone, hot is the default pool, so it aliases cold
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

// cascadeLeg is one side of a federated cascade: which side it is plus the
// order-axis range it owns ([floor, ceil); nil = unbounded). The connection is
// acquired by the caller only if the leg is actually fetched.
type cascadeLeg struct {
	cold        bool // true = cold side (also used for logging)
	floor, ceil *uint64
}

// cascadeLegs returns the fetch order for one page: hot first for desc, cold
// first for asc, a single unbounded leg standalone.
func (fc *fedConns) cascadeLegs(boundary uint64, sortOrder string) []cascadeLeg {
	if !fc.federated {
		return []cascadeLeg{{}} // single unbounded leg on hot() == cold()
	}
	hot := cascadeLeg{floor: &boundary}
	cold := cascadeLeg{cold: true, ceil: &boundary}
	if sortOrder == "desc" {
		return []cascadeLeg{hot, cold}
	}
	return []cascadeLeg{cold, hot}
}

// cascadePage fetches one federated page ordered by the sort axis: walk the legs
// until the page fills, then (when groupComplete) finish the tail group so the
// cursor never lands inside a partially returned key group.
func cascadePage[T any](
	fc *fedConns, sortOrder string, want int,
	buildPage func(floor, ceil *uint64, limit int32) (string, error),
	buildGroup func(key uint64) string,
	fetch func(query string, conn *pgxpool.Conn) ([]T, error),
	getOrderKey func(*T) *uint64,
	orderKey string,
	groupComplete bool,
) ([]T, error) {
	split := fc.split.getSplit(orderKey)

	// Fetch rows, stop if reached want amount of rows
	var rows []T
	for _, leg := range fc.cascadeLegs(split, sortOrder) {
		if len(rows) >= want {
			break
		}
		conn, err := fc.conn(leg.cold)
		if err != nil {
			return nil, err
		}
		q, err := buildPage(leg.floor, leg.ceil, int32(want-len(rows)))
		if err != nil {
			return nil, err
		}
		got, err := fetch(q, conn)
		if err != nil {
			return nil, err
		}
		rows = append(rows, got...)
	}
	if len(rows) == 0 {
		return rows, nil
	}

	if !groupComplete {
		return rows, nil
	}
	// Complete the tail group: rows sharing the extreme order key (min for desc, max for asc)
	var keyPtr *uint64
	for i := range rows {
		k := getOrderKey(&rows[i])
		if keyPtr == nil || (sortOrder == "desc" && *k < *keyPtr) || (sortOrder != "desc" && *k > *keyPtr) {
			keyPtr = k
		}
	}
	if keyPtr == nil {
		return rows, nil
	}
	key := *keyPtr
	groupCold := fc.federated && key < split
	owner, err := fc.conn(groupCold)
	if err != nil {
		return nil, err
	}
	group, err := fetch(buildGroup(key), owner)
	if err != nil {
		return nil, err
	}
	trimmed := make([]T, 0, len(rows)+len(group))
	for i := range rows {
		if k := getOrderKey(&rows[i]); k != nil && *k == key {
			continue
		}
		trimmed = append(trimmed, rows[i])
	}
	return append(trimmed, group...), nil
}

func cascadePageOffset[T any](
	fc *fedConns, sortOrder string, offset, limit int,
	buildOffset func(floor, ceil *uint64, offset, limit int) (string, error),
	buildCount func(floor, ceil *uint64) string,
	fetch func(query string, conn *pgxpool.Conn) ([]T, error),
	count func(query string, conn *pgxpool.Conn) (int, error),
	orderKey string,
) ([]T, error) {
	// Standalone: single pool, no boundary.
	if !fc.federated {
		cold, err := fc.cold()
		if err != nil {
			return nil, err
		}
		q, err := buildOffset(nil, nil, offset, limit)
		if err != nil {
			return nil, err
		}
		return fetch(q, cold)
	}

	// Federated: global order is [near side][far side] across the seam s.
	//   desc: [hot >= s][cold < s]    asc: [cold < s][hot >= s]
	// near owns the start of the order (where offset 0 begins); far owns the tail.
	s := fc.split.getSplit(orderKey)
	nearCold := sortOrder != "desc"
	var nearFloor, nearCeil, farFloor, farCeil *uint64
	if nearCold {
		nearCeil, farFloor = &s, &s // near=cold (< s), far=hot (>= s)
	} else {
		nearFloor, farCeil = &s, &s // near=hot (>= s), far=cold (< s)
	}

	nearConn, err := fc.conn(nearCold)
	if err != nil {
		return nil, err
	}
	nq, err := buildOffset(nearFloor, nearCeil, offset, limit)
	if err != nil {
		return nil, err
	}
	nearRows, err := fetch(nq, nearConn)
	if err != nil {
		return nil, err
	}
	if len(nearRows) == limit {
		return nearRows, nil // window entirely in the near side
	}

	farConn, err := fc.conn(!nearCold)
	if err != nil {
		return nil, err
	}
	if len(nearRows) > 0 {
		// straddle: near's tail + far's head (far portion starts at the seam)
		fq, err := buildOffset(farFloor, farCeil, 0, limit-len(nearRows))
		if err != nil {
			return nil, err
		}
		farRows, err := fetch(fq, farConn)
		if err != nil {
			return nil, err
		}
		return append(nearRows, farRows...), nil
	}

	// offset ran past the whole near side: count it, then offset into far by the remainder.
	nearCount, err := count(buildCount(nearFloor, nearCeil), nearConn)
	if err != nil {
		return nil, err
	}
	farOffset := offset - nearCount
	if farOffset < 0 {
		farOffset = 0
	}
	fq, err := buildOffset(farFloor, farCeil, farOffset, limit)
	if err != nil {
		return nil, err
	}
	return fetch(fq, farConn)
}

// hotThenCold runs a point-lookup query (by id/hash — no band) on hot first,
// then cold, merging by key with the hot copy winning. The cold leg is skipped
// when hot already returned `expect` rows (pass -1 to always consult cold).
func hotThenCold[T any, K comparable](fc *fedConns, expect int,
	fetch func(conn *pgxpool.Conn) ([]T, error),
	keyOf func(*T) K,
) ([]T, error) {
	hotConn, err := fc.hot()
	if err != nil {
		return nil, err
	}
	rows, err := fetch(hotConn)
	if err != nil {
		return nil, err
	}
	if !fc.federated || (expect >= 0 && len(rows) >= expect) {
		return rows, nil
	}
	coldConn, err := fc.cold()
	if err != nil {
		return nil, err
	}
	coldRows, err := fetch(coldConn)
	if err != nil {
		return nil, err
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
