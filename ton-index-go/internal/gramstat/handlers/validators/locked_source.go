package validators

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
	"github.com/xssnick/tonutils-go/address"
)

type ContractObservation struct {
	Since        uint32  `json:"utime_since"`
	Until        uint32  `json:"utime_until"`
	Index        uint16  `json:"idx"`
	Pubkey       string  `json:"pubkey"`
	ADNL         string  `json:"adnl_addr"`
	Weight       uint64  `json:"weight"`
	Holder       string  `json:"holder"`
	ContractType *string `json:"contract_type"`
	Conflict     bool    `json:"-"`
}

type ISPObservation struct {
	RecordID string  `json:"record_id"`
	Time     uint32  `json:"observed_at"`
	ADNL     string  `json:"adnl_addr"`
	ISP      *string `json:"isp"`
	Conflict bool    `json:"-"`
}

type Attribution struct {
	Types        map[[2]uint32]ContractObservation
	ISPs         map[string]ISPObservation
	Telegram     map[string]TelegramEvidence
	Affiliations map[string]Affiliation
}

type TelegramEvidence struct {
	ADNL       string  `json:"adnl_addr"`
	First      uint32  `json:"first_telegram"`
	FirstOther *uint32 `json:"first_other"`
	RecordID   string  `json:"record_id"`
}

// One compact history read per run, not per round. Missing telemetry in a later
// round must not erase an established, uncontradicted Telegram association.
func (s SessionStats) CollectTelegram(ctx context.Context, to time.Time) (map[string]TelegramEvidence, error) {
	ch, err := gramstat.NewClickHouse(s.URL, s.User, s.Password)
	if err != nil {
		return nil, err
	}
	data, err := ch.Query(ctx, `SELECT upper(adnl_addr) AS adnl_addr,
 toUnixTimestamp(minIf(created_timestamp,geoip_isp='Telegram Messenger')) AS first_telegram,
 toUnixTimestamp(minIfOrNull(created_timestamp,geoip_isp IS NOT NULL AND geoip_isp!='Telegram Messenger')) AS first_other,
 argMinIf(record_id,tuple(created_timestamp,record_id),geoip_isp='Telegram Messenger') AS record_id
 FROM session_stats_mainnet_v2.node_addresses WHERE created_timestamp<toDateTime({until:UInt32})
 GROUP BY adnl_addr HAVING countIf(geoip_isp='Telegram Messenger')>0 FORMAT JSONEachRow`, url.Values{"param_until": {fmt.Sprint(to.Unix())}, "readonly": {"1"}, "max_threads": {"2"}, "max_execution_time": {"90"}, "max_memory_usage": {"2000000000"}})
	if err != nil {
		return nil, err
	}
	rows, err := decodeRows[TelegramEvidence](data)
	out := map[string]TelegramEvidence{}
	for _, r := range rows {
		out[r.ADNL] = r
	}
	return out, err
}

const stakeISPQuery = `SELECT record_id,toUnixTimestamp(created_timestamp) AS observed_at,
 upper(adnl_addr) AS adnl_addr,geoip_isp AS isp FROM session_stats_mainnet_v2.node_addresses
 WHERE record_id IN (SELECT record_id FROM selected_records)
 GROUP BY record_id,created_timestamp,adnl_addr,geoip_isp
 FORMAT JSONEachRow`

func rawHolder(s string) string {
	var a *address.Address
	var err error
	if strings.Contains(s, ":") {
		a, err = address.ParseRawAddr(s)
	} else {
		a, err = address.ParseAddr(s)
	}
	if err != nil || a == nil {
		return ""
	}
	return strings.ToUpper(a.StringRaw())
}

// CollectAttribution reads the round-specific contract type and the exact ISP
// record previously selected by validator_geo. record_id is the first primary
// key column, avoiding repeated scans of the entire node-address history.
func (s SessionStats) CollectAttribution(ctx context.Context, rounds []Round, geos map[uint32]GeoRound) (Attribution, error) {
	out := Attribution{Types: map[[2]uint32]ContractObservation{}, ISPs: map[string]ISPObservation{}}
	if len(rounds) == 0 {
		return out, nil
	}
	ch, err := gramstat.NewClickHouse(s.URL, s.User, s.Password)
	if err != nil {
		return out, fmt.Errorf("invalid or missing GRAMSTAT_SS_URL")
	}
	params := url.Values{"readonly": {"1"}, "max_execution_time": {"90"}, "max_threads": {"2"}, "max_rows_to_read": {"50000000"}, "max_bytes_to_read": {"4000000000"}, "max_memory_usage": {"2000000000"}, "output_format_json_quote_64bit_integers": {"0"}}
	ids := make([]string, len(rounds))
	for i, r := range rounds {
		ids[i] = strconv.FormatUint(uint64(r.Since), 10)
	}
	data, err := ch.Query(ctx, `SELECT utime_since,utime_until,idx,upper(pubkey) AS pubkey,
 upper(adnl_addr) AS adnl_addr,toUInt64(weight) AS weight,ifNull(wallet,'') AS holder,
 wallet_version AS contract_type FROM session_stats_mainnet_v2.validators
 WHERE utime_since IN (`+strings.Join(ids, ",")+`) ORDER BY utime_since,idx FORMAT JSONEachRow`, params)
	if err != nil {
		return out, err
	}
	types, err := decodeRows[ContractObservation](data)
	if err != nil {
		return out, err
	}
	for _, r := range types {
		r.Holder = rawHolder(r.Holder)
		key := [2]uint32{r.Since, uint32(r.Index)}
		if old, ok := out.Types[key]; ok {
			a, _ := json.Marshal(old)
			b, _ := json.Marshal(r)
			r.Conflict = old.Conflict || string(a) != string(b)
		}
		out.Types[key] = r
	}
	type record struct {
		ID string `json:"record_id"`
	}
	seen := map[string]bool{}
	var records []record
	for _, r := range rounds {
		for _, m := range geos[r.Since].Members {
			if m.RecordID != nil && m.OrderingConflicts == 0 && !seen[*m.RecordID] {
				seen[*m.RecordID] = true
				records = append(records, record{*m.RecordID})
			}
		}
	}
	if len(records) == 0 {
		return out, nil
	}
	input, err := encodeRows(records)
	if err != nil {
		return out, err
	}
	data, err = ch.QueryExternal(ctx, stakeISPQuery, params, "selected_records", "record_id String", input)
	if err != nil {
		return out, err
	}
	isps, err := decodeRows[ISPObservation](data)
	if err != nil {
		return out, err
	}
	for _, r := range isps {
		if old, ok := out.ISPs[r.RecordID]; ok {
			a, _ := json.Marshal(old)
			b, _ := json.Marshal(r)
			r.Conflict = old.Conflict || string(a) != string(b)
		}
		out.ISPs[r.RecordID] = r
	}
	return out, nil
}
