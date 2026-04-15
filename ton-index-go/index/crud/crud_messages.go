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

func buildMessagesQuery(
	req models.MessageRequest,
	settings models.RequestSettings,
) (string, []any, error) {
	args := []any{}
	utime_req := req.GetUtimeParams()
	lt_req := req.GetLtParams()
	lim_req := req.GetLimitParams()

	rest_columns := `M.trace_id, M.source, M.destination, M.value, 
		M.value_extra_currencies, M.fwd_fee, M.ihr_fee, M.extra_flags, M.created_lt, M.created_at, M.opcode, M.ihr_disabled, M.bounce, 
		M.bounced, M.import_fee, M.body_hash, M.init_state_hash, M.msg_hash_norm`
	clmn_query := `'', 0, M.msg_hash, '', ` + rest_columns + `, 
		max(case when M.direction='in' then M.tx_hash else null end) as in_tx_hash, 
		max(case when M.direction='out' then M.tx_hash else null end) as out_tx_hash`
	from_query := ` messages as M `
	groupby_query := ` group by M.msg_hash, ` + rest_columns
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", nil, err
	}

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
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_at >= %d", *v))
		order_col = "M.created_at"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_at <= %d", *v))
		order_col = "M.created_at"
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

	sort_order := "desc"
	if lim_req.Sort != nil {
		sort_order, err = getSortOrder(*lim_req.Sort)
		if err != nil {
			return "", nil, err
		}
	}
	orderby_query = fmt.Sprintf(" order by %s %s, M.msg_hash %s", order_col, sort_order, sort_order)

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	inner_query := `select ` + clmn_query
	inner_query += ` from ` + from_query
	inner_query += filter_query
	inner_query += groupby_query
	inner_query += orderby_query
	inner_query += limit_query
	query := `select MM.*, B.*, I.* from (` + inner_query + `) as MM 
	left join message_contents as B on MM.body_hash = B.hash
	left join message_contents as I on MM.init_state_hash = I.hash;`
	// log.Println(query) // TODO: remove debug
	return query, args, nil
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

	// decode opcodes and bodies
	if err := detect.MarkMessages(msgs); err != nil {
		hashes := make([]string, len(msgs))
		for i, msg := range msgs {
			hashes[i] = msg.MsgHash.String()
		}
		log.Printf("Error marking messages with hashes %v: %v", hashes, err)
	}

	return msgs, nil
}

func (db *DbClient) QueryMessages(
	req models.MessageRequest,
	settings models.RequestSettings,
) ([]models.Message, models.AddressBook, models.Metadata, error) {
	query, args, err := buildMessagesQuery(req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query)
	}
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	msgs, err := queryMessagesImpl(query, conn, settings, args...)
	if err != nil {
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
		if !settings.NoAddressBook {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
		if !settings.NoMetadata {
			metadata, err = QueryMetadataImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
	}

	return msgs, book, metadata, nil
}
