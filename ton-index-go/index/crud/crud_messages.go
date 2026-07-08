package crud

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/toncenter/ton-indexer/ton-index-go/index/detect"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"
)

const messagesRestColumns = `M.trace_id, M.source, M.destination, M.value,
	M.value_extra_currencies, M.fwd_fee, M.ihr_fee, M.extra_flags, M.created_lt, M.created_at, M.opcode, M.ihr_disabled, M.bounce,
	M.bounced, M.import_fee, M.body_hash, M.init_state_hash, M.msg_hash_norm`

const messagesClmnQuery = `'', 0, M.msg_hash, '', ` + messagesRestColumns + `,
	(array_agg(M.tx_hash ORDER BY M.tx_lt) FILTER (WHERE M.direction='in'))[1] as in_tx_hash,
	(array_agg(M.tx_hash ORDER BY M.tx_lt) FILTER (WHERE M.direction='out'))[1] as out_tx_hash`

const messagesGroupBy = ` group by M.msg_hash, ` + messagesRestColumns

// msgQueryParts holds the shared pieces of a messages listing query.
type msgQueryParts struct {
	filterList []string
	args       []any
	orderBy    string
	orderByNow bool
	useKvrocks bool
}

func messagesQueryParts(req models.MessageRequest, sortOrder string, useKvrocks bool) msgQueryParts {
	utime_req := req.GetUtimeParams()
	lt_req := req.GetLtParams()

	filter_list := []string{}
	args := []any{}

	if v := req.Direction; v != nil {
		args = append(args, *v)
		filter_list = append(filter_list, fmt.Sprintf("M.direction = $%d", len(args)))
	}
	if v := req.Source; v != nil {
		if v.IsAddressNone() {
			filter_list = append(filter_list, "M.source is NULL")
		} else {
			filter_list = append(filter_list, fmt.Sprintf("M.source = '%s'", v.FilterString()))
		}
	}
	if v := req.Destination; v != nil {
		if v.IsAddressNone() {
			filter_list = append(filter_list, "M.destination is NULL")
		} else {
			filter_list = append(filter_list, fmt.Sprintf("M.destination = '%s'", v.FilterString()))
		}
	}
	if v := req.Opcode; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.opcode = %d", *v))
	}
	if v := req.MessageHash; v != nil {
		filter_str := fmt.Sprintf("(%s or %s)", filterByArray("M.msg_hash", v), filterByArray("M.msg_hash_norm", v))
		filter_list = append(filter_list, filter_str)
	}
	if v := req.BodyHash; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.body_hash = '%s'", v.FilterString()))
	}

	order_col := "M.created_lt"
	orderByNow := false
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_at >= %d", *v))
		order_col = "M.created_at"
		orderByNow = true
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_at <= %d", *v))
		order_col = "M.created_at"
		orderByNow = true
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_lt <= %d", *v))
	}
	if v := req.ExcludeExternals; v != nil && *v {
		filter_list = append(filter_list, order_col+" is not NULL")
	}
	if v := req.OnlyExternals; v != nil && *v {
		filter_list = append(filter_list, order_col+" is NULL")
	}

	// With one address filter, lead ORDER BY with that address column so Postgres
	// can use the composite (address, created_lt) index. The created_at path has
	// no matching composite index.
	addressFirst := ""
	if req.Destination != nil && req.Source == nil {
		addressFirst = "M.destination"
	} else if req.Source != nil && req.Destination == nil {
		addressFirst = "M.source"
	}

	var orderby_query string
	if addressFirst != "" && !orderByNow {
		orderby_query = fmt.Sprintf(" order by %s %s, %s %s, M.msg_hash %s", addressFirst, sortOrder, order_col, sortOrder, sortOrder)
	} else {
		orderby_query = fmt.Sprintf(" order by %s %s, M.msg_hash %s", order_col, sortOrder, sortOrder)
	}
	return msgQueryParts{filterList: filter_list, args: args, orderBy: orderby_query, orderByNow: orderByNow, useKvrocks: useKvrocks}
}

// messagesPageInner builds one grouped page without hot/cold seam bounds.
func messagesPageInner(p msgQueryParts, orderLimit string) string {
	filter_query := ``
	if len(p.filterList) > 0 {
		filter_query = ` where ` + strings.Join(p.filterList, " and ")
	}
	return `select ` + messagesClmnQuery + ` from messages as M` + filter_query + messagesGroupBy + orderLimit
}

func buildMessagesOffsetQuery(p msgQueryParts, offset, limit int) string {
	limit_query := fmt.Sprintf(" limit %d offset %d", max(1, limit), max(0, offset))
	inner_query := messagesPageInner(p, p.orderBy+limit_query)
	if p.useKvrocks {
		return `select MM.*, NULL::tonhash, NULL::text, NULL::tonhash, NULL::text from (` + inner_query + `) as MM;`
	}
	return `select MM.*, B.*, I.* from (` + inner_query + `) as MM
	left join message_contents as B on MM.body_hash = B.hash
	left join message_contents as I on MM.init_state_hash = I.hash;`
}

func messagePtrs(msgs []models.Message) []*models.Message {
	ptrs := make([]*models.Message, 0, len(msgs))
	for i := range msgs {
		ptrs = append(ptrs, &msgs[i])
	}
	return ptrs
}

func attachMessageContentsFromKvrocks(parentCtx context.Context, msgs []*models.Message, store *KvrocksStore, settings models.RequestSettings) error {
	if store == nil || len(msgs) == 0 {
		return nil
	}

	hashes := make([]models.HashType, 0, len(msgs)*2)
	for _, msg := range msgs {
		if msg == nil {
			continue
		}
		if msg.BodyHash != nil {
			hashes = append(hashes, *msg.BodyHash)
		}
		if msg.InitStateHash != nil {
			hashes = append(hashes, *msg.InitStateHash)
		}
	}
	if len(hashes) == 0 {
		return nil
	}

	ctx, cancel_ctx := context.WithTimeout(parentCtx, settings.Timeout)
	defer cancel_ctx()
	contents, err := store.GetMessageContents(ctx, hashes)
	if err != nil {
		return err
	}

	for idx := range msgs {
		msg := msgs[idx]
		if msg == nil {
			continue
		}
		if msg.BodyHash != nil {
			if content, ok := contents[*msg.BodyHash]; ok {
				loc := *content
				msg.MessageContent = &loc
			}
		}
		if msg.InitStateHash != nil {
			if content, ok := contents[*msg.InitStateHash]; ok {
				loc := *content
				msg.InitState = &loc
			}
		}
	}
	return nil
}

func finalizeMessagePtrs(parentCtx context.Context, msgs []*models.Message, store *KvrocksStore, settings models.RequestSettings) error {
	if err := attachMessageContentsFromKvrocks(parentCtx, msgs, store, settings); err != nil {
		return models.IndexError{Code: 500, Message: err.Error()}
	}

	if err := detect.MarkMessagesByPtr(msgs); err != nil {
		hashes := make([]string, 0, len(msgs))
		for _, msg := range msgs {
			if msg != nil {
				hashes = append(hashes, msg.MsgHash.String())
			}
		}
		log.Printf("Error marking messages with hashes %v: %v", hashes, err)
	}
	return nil
}

func finalizeMessages(msgs []models.Message, store *KvrocksStore, settings models.RequestSettings) error {
	return finalizeMessagePtrs(store.pinReadSnapshot(context.Background()), messagePtrs(msgs), store, settings)
}

func queryMessagesImpl(query string, conn *pgxpool.Conn, settings models.RequestSettings, args ...any) ([]models.Message, error) {
	msgs := []models.Message{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query, args...)
		if err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			msg, err := parse.ScanMessageWithContent(rows)
			if err != nil {
				return nil, models.IndexError{Code: 500, Message: err.Error()}
			}
			msgs = append(msgs, *msg)
		}
		if rows.Err() != nil {
			return nil, models.IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}

	return msgs, nil
}

// messagesCanHaveNullKeys reports whether a page can contain external-in messages
func messagesCanHaveNullKeys(req models.MessageRequest) bool {
	if req.ExcludeExternals != nil && *req.ExcludeExternals {
		return false
	}
	if req.Source != nil && !req.Source.IsAddressNone() {
		return false
	}
	if req.Direction != nil && *req.Direction == "out" {
		return false
	}
	return true
}

// messageOrderKey returns nil for external messages with NULL created_lt; that
// forces cold fallback during hot-page verification.
func messageOrderKey(orderByNow bool) func(*models.Message) *uint64 {
	if orderByNow {
		return func(m *models.Message) *uint64 {
			if m.CreatedAt == nil {
				return nil
			}
			v := uint64(*m.CreatedAt)
			return &v
		}
	}
	return func(m *models.Message) *uint64 { return m.CreatedLt }
}

// queryMessagesRouted fetches one grouped page via the hot/cold router.
func queryMessagesRouted(
	fc *fedConns,
	req models.MessageRequest,
	parts msgQueryParts,
	sortOrder string,
	offset, limit int,
	fetch func(query string, conn *pgxpool.Conn) ([]models.Message, error),
) ([]models.Message, error) {
	query := buildMessagesOffsetQuery(parts, offset, limit)

	// Single pool: read cold directly, no classification.
	dec := routeCold
	var floor uint64
	if fc.federated {
		w := routeWindow{
			startLt:         req.StartLt,
			endLt:           req.EndLt,
			startUtime:      (*uint64)(req.StartUtime),
			endUtime:        (*uint64)(req.EndUtime),
			orderByNow:      parts.orderByNow,
			sortDesc:        sortOrder == "desc",
			canHaveNullKeys: messagesCanHaveNullKeys(req),
		}
		dec = classifyRoute(w, fc.split, fc.utimeMargin)

		// created_lt >= split.Lt keeps the whole grouped message above the floor.
		floor = fc.split.Lt
		if parts.orderByNow {
			floor = fc.split.Utime + fc.utimeMargin
		}
	}

	msgs, _, err := routedPage(fc, dec,
		func(conn *pgxpool.Conn) ([]models.Message, error) { return fetch(query, conn) },
		messageOrderKey(parts.orderByNow), limit, floor)
	return msgs, err
}

func (db *DbClient) QueryMessages(
	req models.MessageRequest,
	settings models.RequestSettings,
) ([]models.Message, models.AddressBook, models.Metadata, error) {
	lim_req := req.GetLimitParams()
	sortOrder := "desc"
	if v := lim_req.Sort; v != nil {
		var serr error
		sortOrder, serr = getSortOrder(*v)
		if serr != nil {
			return nil, nil, nil, serr
		}
	}
	offset := 0
	if lim_req.Offset != nil {
		offset = int(max(0, *lim_req.Offset))
	}
	limit := int32(settings.DefaultLimit)
	if lim_req.Limit != nil {
		limit = max(1, *lim_req.Limit)
		if limit > int32(settings.MaxLimit) {
			return nil, nil, nil, models.IndexError{Code: 422, Message: fmt.Sprintf("limit is not allowed: %d > %d", limit, settings.MaxLimit)}
		}
	}

	parts := messagesQueryParts(req, sortOrder, db.Kvrocks != nil)

	fc, release, err := db.acquireFedForRequest(settings)
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer release()

	fetch := func(query string, conn *pgxpool.Conn) ([]models.Message, error) {
		if settings.DebugRequest {
			log.Println("Debug query:", query)
		}
		return queryMessagesImpl(query, conn, settings, parts.args...)
	}

	msgs, err := queryMessagesRouted(fc, req, parts, sortOrder, offset, int(limit), fetch)
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	if db.Kvrocks != nil {
		release()
	}
	if err := finalizeMessages(msgs, db.Kvrocks, settings); err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	book := models.AddressBook{}
	metadata := models.Metadata{}
	addr_list := []models.AccountAddress{}
	for _, m := range msgs {
		if m.Source != nil {
			addr_list = append(addr_list, *m.Source)
		}
		if m.Destination != nil {
			addr_list = append(addr_list, *m.Destination)
		}
	}
	if len(addr_list) > 0 {
		if db.Kvrocks != nil {
			release()
			book, metadata, err = db.queryKvrocksEnrichment(addr_list, settings)
			if err != nil {
				return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		} else {
			coldConn, cerr := fc.cold()
			if cerr != nil {
				return nil, nil, nil, models.IndexError{Code: 500, Message: cerr.Error()}
			}
			if !settings.NoAddressBook {
				book, err = QueryAddressBookImpl(addr_list, coldConn, settings)
				if err != nil {
					return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
				}
			}
			if !settings.NoMetadata {
				metadata, err = QueryMetadataImpl(addr_list, coldConn, settings)
				if err != nil {
					return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
				}
			}
		}
	}

	return msgs, book, metadata, nil
}
