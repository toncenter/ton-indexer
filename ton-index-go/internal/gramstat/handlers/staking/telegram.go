package staking

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/url"
	"strings"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

type SessionStats struct{ URL, User, Password string }
type telegramInput struct {
	ID   uint32 `json:"id"`
	ADNL string `json:"adnl"`
	From uint32 `json:"from_time"`
	To   uint32 `json:"to_time"`
}
type telegramResult struct {
	ID        uint32  `json:"id"`
	Samples   uint64  `json:"samples"`
	Conflicts uint64  `json:"conflicts"`
	ISP       *string `json:"isp"`
}

const telegramSQL = `WITH observed AS (
 SELECT r.id,count() AS samples,
   uniqExact(tuple(n.created_timestamp,n.record_id,n.geoip_isp))
       -uniqExact(tuple(n.created_timestamp,n.record_id)) AS conflicts,
   argMax(tuple(n.geoip_isp),tuple(n.created_timestamp,n.record_id)) AS latest
 FROM session_stats_mainnet_v2.node_addresses n
 ALL INNER JOIN roster r ON upper(n.adnl_addr)=r.adnl
 WHERE n.created_timestamp>=toDateTime({from:UInt32},'UTC')
   AND n.created_timestamp<toDateTime({to:UInt32},'UTC')
   AND toUnixTimestamp(n.created_timestamp)>=r.from_time
   AND toUnixTimestamp(n.created_timestamp)<r.to_time
 GROUP BY r.id
)
SELECT r.id,o.samples,o.conflicts,tupleElement(o.latest,1) AS isp
FROM roster r LEFT JOIN observed o ON r.id=o.id ORDER BY r.id FORMAT JSONEachRow`

func (s SessionStats) Attribute(ctx context.Context, points []Point, rows []Row) error {
	var input []telegramInput
	type target struct {
		row   int
		stake *big.Int
	}
	var targets []target
	from, to := uint32(^uint32(0)), uint32(0)
	for i, p := range points {
		if p.State == nil || !p.State.Complete {
			continue
		}
		for _, e := range p.State.Elections {
			for _, position := range e.Positions {
				adnl := position.ADNL
				if adnl == nil {
					o := p.Origins[electionKey(e)]
					for _, old := range o.First.Elections {
						if electionKey(old) == electionKey(e) {
							for _, q := range old.Positions {
								if q.Pubkey == position.Pubkey && q.Holder == position.Holder {
									adnl = q.ADNL
								}
							}
						}
					}
				}
				if adnl == nil || *adnl == strings.Repeat("0", 64) {
					continue
				}
				lo := uint32(0)
				if p.AnchorTime > 86400 {
					lo = p.AnchorTime - 86400
				}
				hi := p.AnchorTime + 1
				from = min(from, lo)
				to = max(to, hi)
				input = append(input, telegramInput{uint32(len(input)), strings.ToUpper(*adnl), lo, hi})
				value, err := amount(position.Stake)
				if err != nil {
					return err
				}
				targets = append(targets, target{i, value})
			}
		}
	}
	if len(input) == 0 {
		for i := range rows {
			if rows[i].Remaining != nil && *rows[i].Remaining == "0" {
				zero := "0"
				rows[i].Telegram = &zero
			}
			if rows[i].Remaining != nil && *rows[i].Remaining != "0" && rows[i].Status == "complete" {
				rows[i].Status = "partial_infrastructure"
			}
		}
		return nil
	}
	ch, err := gramstat.NewClickHouse(s.URL, s.User, s.Password)
	if err != nil {
		return fmt.Errorf("invalid GRAMSTAT_SS_URL")
	}
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, r := range input {
		if err = enc.Encode(r); err != nil {
			return err
		}
	}
	params := url.Values{"readonly": {"1"}, "max_execution_time": {"90"}, "max_threads": {"2"},
		"max_rows_to_read": {"50000000"}, "max_memory_usage": {"2000000000"},
		"output_format_json_quote_64bit_integers": {"0"}, "param_from": {fmt.Sprint(from)}, "param_to": {fmt.Sprint(to)}}
	data, err := ch.QueryExternal(ctx, telegramSQL, params, "roster", "id UInt32, adnl String, from_time UInt32, to_time UInt32", body.Bytes())
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	seen := map[uint32]bool{}
	known := make([]*big.Int, len(rows))
	tg := make([]*big.Int, len(rows))
	for i := range rows {
		known[i] = new(big.Int)
		tg[i] = new(big.Int)
	}
	for {
		var r telegramResult
		err = dec.Decode(&r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if int(r.ID) >= len(targets) || seen[r.ID] {
			return fmt.Errorf("invalid Telegram attribution response")
		}
		seen[r.ID] = true
		t := targets[r.ID]
		if r.Samples > 0 && r.Conflicts == 0 && r.ISP != nil && strings.TrimSpace(*r.ISP) != "" {
			known[t.row].Add(known[t.row], t.stake)
			if *r.ISP == "Telegram Messenger" {
				tg[t.row].Add(tg[t.row], t.stake)
			}
		}
	}
	if len(seen) != len(input) {
		return fmt.Errorf("incomplete Telegram attribution response")
	}
	for i := range rows {
		if rows[i].Remaining == nil {
			continue
		}
		total, _ := amount(*rows[i].Remaining)
		unknown := new(big.Int).Sub(total, known[i])
		rows[i].TelegramObserved = stringPtr(tg[i])
		rows[i].TelegramUnknown = stringPtr(unknown)
		if unknown.Sign() == 0 {
			rows[i].Telegram = stringPtr(tg[i])
		}
		if unknown.Sign() > 0 && rows[i].Status == "complete" {
			rows[i].Status = "partial_infrastructure"
		}
	}
	return nil
}
