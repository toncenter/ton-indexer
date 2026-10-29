package gramstat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const DailySchema = `CREATE DATABASE IF NOT EXISTS gramstat;
CREATE TABLE IF NOT EXISTS gramstat.daily_metrics (
    metric LowCardinality(String),
    day Date,
    value UInt64,
    calculated_at DateTime64(6, 'UTC'),
    data_through DateTime('UTC'),
    source_mc_seqno UInt32,
    is_partial UInt8,
    amount_nanoton Nullable(Decimal(38,0)) DEFAULT NULL,
    breakdown Map(String, UInt64) DEFAULT map()
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(day)
ORDER BY (metric, day);
`

// Apply with the schema using an administrative account when upgrading a
// deployment that predates monetary sums and categorical breakdowns.
const DailyMigration = `ALTER TABLE gramstat.daily_metrics
    ADD COLUMN IF NOT EXISTS amount_nanoton Nullable(Decimal(38,0)) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS breakdown Map(String, UInt64) DEFAULT map();
`

type DailyMetric struct {
	Metric        string            `json:"metric"`
	Day           string            `json:"day"`
	Value         uint64            `json:"value"`
	CalculatedAt  string            `json:"calculated_at"`
	DataThrough   string            `json:"data_through"`
	SourceMCSeqno uint32            `json:"source_mc_seqno"`
	IsPartial     uint8             `json:"is_partial"`
	AmountNanoton *string           `json:"amount_nanoton"`
	Breakdown     map[string]uint64 `json:"breakdown,omitempty"`
}

// ClickHouse is a small HTTP transport usable by handlers with different schemas.
// Credentials stay in headers; errors never include them or a credential-bearing URL.
type ClickHouse struct {
	endpoint *url.URL
	user     string
	password string
	client   *http.Client
}

func NewClickHouse(endpoint, user, password string) (*ClickHouse, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("GRAMSTAT_CH_URL must be an HTTP(S) endpoint without credentials or query parameters")
	}
	return &ClickHouse{endpoint: u, user: user, password: password, client: &http.Client{
		Timeout:       2 * time.Minute,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func (c *ClickHouse) Query(ctx context.Context, query string, params url.Values) ([]byte, error) {
	return c.request(ctx, query, params, nil)
}

func (c *ClickHouse) request(ctx context.Context, query string, params url.Values, body []byte) ([]byte, error) {
	u := *c.endpoint
	values := make(url.Values)
	for k, v := range params {
		values[k] = v
	}
	values.Set("query", query)
	// Buffer errors before sending HTTP 200; in particular, an insert is synchronous.
	values.Set("wait_end_of_query", "1")
	values.Set("async_insert", "0")
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.user, c.password)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.client.Do(req)
	if err != nil {
		// url.Error includes the whole query URL; retain only the underlying cause.
		if e, ok := err.(*url.Error); ok {
			err = e.Err
		}
		return nil, fmt.Errorf("ClickHouse request: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 16<<20 {
		return nil, fmt.Errorf("ClickHouse response exceeds 16 MiB")
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("X-ClickHouse-Exception-Code") != "" || bytes.HasPrefix(data, []byte("Code:")) {
		message := strings.TrimSpace(string(data))
		if len(message) > 2048 {
			message = message[:2048]
		}
		return nil, fmt.Errorf("ClickHouse HTTP %d: %s", resp.StatusCode, message)
	}
	return data, nil
}

func (c *ClickHouse) WriteDaily(ctx context.Context, rows []DailyMetric) error {
	if len(rows) == 0 {
		return nil
	}
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	_, err := c.request(ctx, "INSERT INTO gramstat.daily_metrics FORMAT JSONEachRow", nil, body.Bytes())
	return err
}

func storedDaysQuery(metrics []string, today time.Time) (string, url.Values) {
	params := url.Values{"param_today": {today.Format(time.DateOnly)}}
	placeholders := make([]string, len(metrics))
	for i, name := range metrics {
		key := fmt.Sprintf("metric%d", i)
		placeholders[i] = "{" + key + ":String}"
		params.Set("param_"+key, name)
	}
	// Date is already a JSON string. A toString(day) AS day alias would also
	// substitute into WHERE in ClickHouse, comparing String with Date.
	return `SELECT metric, day
FROM gramstat.daily_metrics FINAL
WHERE metric IN (` + strings.Join(placeholders, ",") + `) AND day < {today:Date}
  AND is_partial = 0
GROUP BY metric, day FORMAT JSONEachRow`, params
}

func (c *ClickHouse) StoredDays(ctx context.Context, metrics []string, today time.Time) (map[string][]time.Time, error) {
	query, params := storedDaysQuery(metrics, today)
	data, err := c.Query(ctx, query, params)
	if err != nil {
		return nil, err
	}
	result := make(map[string][]time.Time)
	dec := json.NewDecoder(bytes.NewReader(data))
	for {
		var row struct{ Metric, Day string }
		if err := dec.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("decode stored days: %w", err)
		}
		day, err := time.Parse(time.DateOnly, row.Day)
		if err != nil {
			return nil, err
		}
		result[row.Metric] = append(result[row.Metric], day)
	}
	return result, nil
}
