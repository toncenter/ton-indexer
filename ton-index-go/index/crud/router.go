package crud

import (
	"github.com/jackc/pgx/v5/pgxpool"
)

// routeDecision says where to run the unchanged page query.
type routeDecision int

const (
	routeCold      routeDecision = iota // run on cold, done
	routeHot                            // run on hot, done (window entirely in hot)
	routeHotVerify                      // run on hot, verify page, rerun on cold on failure
)

// routeWindow is the request window on the active sort axis only.
type routeWindow struct {
	startLt, endLt       *uint64 // bounds on lt axis (from request LtParams)
	startUtime, endUtime *uint64 // bounds on utime axis (from request UtimeParams)
	orderByNow           bool    // sort axis is utime, not lt
	sortDesc             bool    // resolved sort order
	canHaveNullKeys      bool    // messages only: externals with NULL created_lt possible
}

// classifyRoute picks cold, hot, or hot-with-verification for a paged request.
// Utime hot checks add a small seam margin.
func classifyRoute(w routeWindow, s hotColdSplit, utimeMarginSec uint64) routeDecision {
	var start, end *uint64
	var coldFloor, hotFloor uint64
	if w.orderByNow {
		start, end = w.startUtime, w.endUtime
		coldFloor = s.Utime
		hotFloor = s.Utime + utimeMarginSec
	} else {
		start, end = w.startLt, w.endLt
		coldFloor = s.Lt
		hotFloor = s.Lt
	}

	// 1. Window entirely below the floor.
	if end != nil && *end < coldFloor {
		return routeCold
	}
	// 2. Lower bound below the floor: window may reach into cold.
	if start != nil && *start < hotFloor {
		return routeCold
	}
	// 3. Ascending with no lower bound: first page is the oldest history.
	if !w.sortDesc && start == nil {
		return routeCold
	}
	// 4. Descending page that may contain NULL sort keys (externals): the
	//    NULLS FIRST clamp needs the full historical set.
	if w.canHaveNullKeys && w.sortDesc && start == nil {
		return routeCold
	}
	// 5. Lower bound at or above the floor: whole window is in hot, a short
	//    page is a legitimate full answer.
	if start != nil && *start >= hotFloor {
		return routeHot
	}
	// 6. Descending, no lower bound: hot likely has the page; verify, rerun on
	//    cold if it crossed the floor.
	return routeHotVerify
}

// verifyHotPage accepts a hot page only when it is full, has no NULL sort keys,
// and does not cross below the split floor.
func verifyHotPage(orderKeys []*uint64, limit int, floor uint64) bool {
	if len(orderKeys) < limit {
		return false // short page: hot ran dry below the floor
	}
	for _, k := range orderKeys {
		if k == nil {
			return false // NULL sort key: position vs floor unknown
		}
	}
	if len(orderKeys) == 0 {
		return true // limit 0
	}
	last := orderKeys[len(orderKeys)-1]
	return *last >= floor
}

// resolveRouted records the chosen path before a possible cold read.
func resolveRouted(dec routeDecision, hotErr error, verified bool) (useCold, rerun bool) {
	switch dec {
	case routeCold:
		return true, false
	case routeHot:
		if hotErr != nil {
			return true, true
		}
		return false, false
	case routeHotVerify:
		if hotErr != nil || !verified {
			return true, true
		}
		return false, false
	default:
		return true, false
	}
}

// routedPage runs one standalone page query on the selected DB. Hot errors or
// failed verification rerun the same query on cold, which has full history.
func routedPage[T any](fc *fedConns, dec routeDecision,
	fetch func(conn *pgxpool.Conn) ([]T, error),
	orderKeyOf func(*T) *uint64, limit int, floor uint64) ([]T, bool, error) {

	if dec == routeCold {
		conn, err := fc.cold()
		if err != nil {
			return nil, false, err
		}
		rows, err := fetch(conn)
		return rows, true, err
	}

	// routeHot / routeHotVerify: attempt hot first.
	var rows []T
	hotConn, hotErr := fc.hot()
	if hotErr == nil {
		rows, hotErr = fetch(hotConn)
	}

	verified := true
	if dec == routeHotVerify && hotErr == nil {
		keys := make([]*uint64, len(rows))
		for i := range rows {
			keys[i] = orderKeyOf(&rows[i])
		}
		verified = verifyHotPage(keys, limit, floor)
	}

	if useCold, _ := resolveRouted(dec, hotErr, verified); !useCold {
		return rows, false, nil
	}

	coldConn, err := fc.cold()
	if err != nil {
		return nil, false, err
	}
	rows, err = fetch(coldConn)
	return rows, true, err
}
