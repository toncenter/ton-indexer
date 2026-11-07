// Package api serves bounded, read-only Gramstat chart data.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

type Reader interface {
	Read(context.Context, string, url.Values) (json.RawMessage, error)
}

type ClickHouseReader struct{ CH *gramstat.ClickHouse }

// Each route selects a fixed query. No user SQL, identifiers, settings, or URLs
// are forwarded. Every 64-bit integer and monetary amount is a decimal string.
var queries = map[string]string{
	"chain": `SELECT day, toString(transactions) AS transactions,
toString(fees_nanoton) AS fees_nanoton, toString(minted_nanoton) AS minted_nanoton,
toString(burned_nanoton) AS burned_nanoton, toString(supply_nanoton) AS supply_nanoton,
supply_utime, is_partial, source_mc_from, source_mc_to, source_id,
formatDateTime(calculated_at, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS calculated_at
FROM gramstat.chain_daily FINAL WHERE day >= {from:Date} AND day < {to:Date} ORDER BY day`,
	"daily": `SELECT metric, day, toString(value) AS value, toString(amount_nanoton) AS amount_nanoton,
breakdown, is_partial, source_mc_seqno,
formatDateTime(calculated_at, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS calculated_at,
formatDateTime(data_through, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS data_through
FROM gramstat.daily_metrics FINAL WHERE day >= {from:Date} AND day < {to:Date} ORDER BY metric, day`,
	"stars": `SELECT day, toString(purchases) AS purchases, toString(unknown_purchases) AS unknown_purchases,
toString(stars_total) AS stars_total, toString(known_stars_total) AS known_stars_total,
is_partial, formatDateTime(calculated_at, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS calculated_at,
formatDateTime(data_through, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS data_through
FROM gramstat.telegram_stars_daily FINAL WHERE day >= {from:Date} AND day < {to:Date} ORDER BY day`,
	"rounds": `SELECT r.utime_since, r.utime_until, r.total, r.is_partial, r.data_through,
formatDateTime(r.calculated_at, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS calculated_at,
g.located, g.unknown, g.countries, g.country_known, g.breakdown,
g.status AS geo_status, g.data_through AS geo_data_through,
formatDateTime(g.calculated_at, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS geo_calculated_at
FROM gramstat.validator_rounds AS r FINAL
LEFT JOIN gramstat.validator_geo_rounds AS g FINAL
 ON r.utime_since = g.utime_since AND r.roster_digest = g.roster_digest AND r.config34_hash = g.config34_hash
WHERE r.utime_since < toUnixTimestamp(toDateTime({to:Date}, 'UTC'))
 AND r.utime_until > toUnixTimestamp(toDateTime({from:Date}, 'UTC')) ORDER BY r.utime_since`,
	"locations": `SELECT g.utime_since, g.total, g.located, g.unknown, g.data_through, g.status,
formatDateTime(g.calculated_at, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS calculated_at,
arrayMap(m -> tuple(m.country, m.city, m.latitude, m.longitude),
 arrayFilter(m -> m.status = 'located', g.members)) AS points
FROM gramstat.validator_geo_rounds AS g FINAL
INNER JOIN gramstat.validator_rounds AS r FINAL
 ON r.utime_since = g.utime_since AND r.roster_digest = g.roster_digest AND r.config34_hash = g.config34_hash
WHERE g.utime_since = {round:UInt32}`,
	"coverage": `SELECT 'chain' AS source, toString(minOrNull(day)) AS first_day,
toString(maxOrNull(day)) AS last_day,
formatDateTime(maxOrNull(calculated_at), '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS updated_at
FROM gramstat.chain_daily FINAL
UNION ALL SELECT 'actions', toString(minOrNull(day)), toString(maxOrNull(day)),
formatDateTime(maxOrNull(calculated_at), '%Y-%m-%dT%H:%i:%SZ', 'UTC')
FROM gramstat.daily_metrics FINAL
UNION ALL SELECT 'validators', toString(toDate(minOrNull(utime_since), 'UTC')),
toString(toDate(maxOrNull(utime_since), 'UTC')),
formatDateTime(maxOrNull(calculated_at), '%Y-%m-%dT%H:%i:%SZ', 'UTC')
FROM gramstat.validator_rounds FINAL`,
}

func (c ClickHouseReader) Read(ctx context.Context, name string, params url.Values) (json.RawMessage, error) {
	query, ok := queries[name]
	if !ok {
		return nil, fmt.Errorf("unknown chart query")
	}
	p := url.Values{
		"readonly": {"1"}, "max_execution_time": {"10"}, "max_threads": {"2"},
		"max_memory_usage": {"268435456"}, "max_result_rows": {"20000"},
		"max_result_bytes": {"4194304"}, "result_overflow_mode": {"throw"},
		"max_rows_to_read": {"2000000"}, "read_overflow_mode": {"throw"},
		"join_use_nulls": {"1"}, "output_format_json_quote_64bit_integers": {"1"},
	}
	for k, v := range params {
		p["param_"+k] = v
	}
	data, err := c.CH.Query(ctx, query+" FORMAT JSON", p)
	if err != nil {
		return nil, err
	}
	var result struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	if len(result.Data) == 0 || result.Data[0] != '[' {
		return nil, fmt.Errorf("missing chart rows")
	}
	return result.Data, nil
}
