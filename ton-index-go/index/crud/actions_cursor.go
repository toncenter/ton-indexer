package crud

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/vmihailenco/msgpack/v5"
)

// actionsCursor is the ORDER BY tuple of the last row of a page; the next page starts strictly after it.
type actionsCursor struct {
	_msgpack struct{} `msgpack:",as_array"`
	Utime    bool
	Desc     bool
	TraceEnd int64
	TraceId  [32]byte
	End      int64
	ActionId [32]byte
}

func (c actionsCursor) encode() (string, error) {
	b, err := msgpack.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b), err
}

func decodeActionsCursor(s string) (actionsCursor, error) {
	var c actionsCursor
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err == nil {
		err = msgpack.Unmarshal(b, &c)
	}
	if err != nil {
		return actionsCursor{}, models.IndexError{Code: 422, Message: "invalid cursor"}
	}
	return c, nil
}

func actionsCursorAfter(a *models.RawAction, utime, desc bool) (actionsCursor, error) {
	traceId, err := models.ParseHashBytes(string(*a.TraceId))
	if err != nil {
		return actionsCursor{}, err
	}
	actionId, err := models.ParseHashBytes(string(a.ActionId))
	if err != nil {
		return actionsCursor{}, err
	}
	c := actionsCursor{Utime: utime, Desc: desc, TraceEnd: a.TraceEndLt, TraceId: [32]byte(traceId), End: a.EndLt, ActionId: [32]byte(actionId)}
	if utime {
		c.TraceEnd, c.End = a.TraceEndUtime, a.EndUtime
	}
	return c, nil
}

// applyCursor inlines the values like the other bounds of this builder: integers and re-encoded base64.
func (p *actionsQueryParts) applyCursor(c actionsCursor) {
	op := ">"
	if c.Desc {
		op = "<"
	}
	p.filterList = append(p.filterList, fmt.Sprintf("(%s) %s (%d, '%s'::tonhash, %d, '%s'::tonhash)",
		strings.Join(p.orderCols, ", "), op, c.TraceEnd, base64.StdEncoding.EncodeToString(c.TraceId[:]),
		c.End, base64.StdEncoding.EncodeToString(c.ActionId[:])))
}

// narrowWindow bounds the router window by the cursor so pages past the split go straight to cold.
func (c actionsCursor) narrowWindow(w *routeWindow) {
	v := uint64(c.TraceEnd)
	start, end := &w.startLt, &w.endLt
	if c.Utime {
		start, end = &w.startUtime, &w.endUtime
	}
	if c.Desc {
		if *end == nil || v < **end {
			*end = &v
		}
	} else if *start == nil || v > **start {
		*start = &v
	}
}
