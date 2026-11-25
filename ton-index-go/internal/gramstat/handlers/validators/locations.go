package validators

import (
	"context"
	_ "embed"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

//go:embed locations.sql
var locationsSQL string

type SessionStats struct{ URL, User, Password string }
type rosterRow struct {
	Since  uint32 `json:"utime_since"`
	Cutoff uint32 `json:"cutoff"`
	Index  uint16 `json:"idx"`
	Pubkey string `json:"pubkey"`
	ADNL   string `json:"adnl_addr"`
}
type observation struct {
	Since uint32 `json:"utime_since"`
	Index uint16 `json:"idx"`
	GeoMember
}

func (s SessionStats) Collect(ctx context.Context, rounds []Round, asof time.Time) ([]GeoRound, error) {
	ch, err := gramstat.NewClickHouse(s.URL, s.User, s.Password)
	if err != nil {
		return nil, fmt.Errorf("invalid or missing GRAMSTAT_SS_URL")
	}
	params := url.Values{"readonly": {"1"}, "max_execution_time": {"90"}, "max_threads": {"2"},
		"max_rows_to_read": {"50000000"}, "max_bytes_to_read": {"4000000000"}, "max_memory_usage": {"2000000000"},
		"output_format_json_quote_64bit_integers": {"0"}}
	data, err := ch.Query(ctx, `SELECT coalesce(toUnixTimestamp(maxOrNull(created_timestamp)),0) AS head
FROM session_stats_mainnet_v2.node_addresses FORMAT JSONEachRow`, params)
	if err != nil {
		return nil, err
	}
	heads, err := decodeRows[struct {
		Head uint32 `json:"head"`
	}](data)
	if err != nil {
		return nil, err
	}
	if len(heads) != 1 || heads[0].Head == 0 {
		return nil, fmt.Errorf("no session-stats observation head")
	}
	head := heads[0].Head
	var roster []rosterRow
	from, to := uint32(math.MaxUint32), uint32(0)
	for _, r := range rounds {
		if err = r.validate(); err != nil {
			return nil, err
		}
		cutoff := uint32(min(int64(r.Through), asof.Unix(), int64(head)+1))
		if cutoff <= r.Since {
			return nil, fmt.Errorf("session-stats has not reached round %d", r.Since)
		}
		from = min(from, r.Since)
		to = max(to, cutoff)
		for _, m := range r.Members {
			roster = append(roster, rosterRow{r.Since, cutoff, m.Index, m.Pubkey, m.ADNL})
		}
	}
	if len(roster) == 0 {
		return nil, nil
	}
	input, err := encodeRows(roster)
	if err != nil {
		return nil, err
	}
	params.Set("param_from", strconv.FormatUint(uint64(from), 10))
	params.Set("param_to", strconv.FormatUint(uint64(to), 10))
	data, err = ch.QueryExternal(ctx, locationsSQL, params, "roster",
		"utime_since UInt32, cutoff UInt32, idx UInt16, pubkey String, adnl_addr String", input)
	if err != nil {
		return nil, err
	}
	observations, err := decodeRows[observation](data)
	if err != nil {
		return nil, err
	}
	return geoRows(rounds, observations, head, asof, time.Now())
}

func geoRows(rounds []Round, observations []observation, head uint32, asof, calculated time.Time) ([]GeoRound, error) {
	byID := make(map[[2]uint32]observation, len(observations))
	for _, o := range observations {
		key := [2]uint32{o.Since, uint32(o.Index)}
		if _, ok := byID[key]; ok {
			return nil, fmt.Errorf("duplicate location row")
		}
		byID[key] = o
	}
	result := make([]GeoRound, 0, len(rounds))
	for _, r := range rounds {
		g := GeoRound{Since: r.Since, Until: r.Until, ConfigHash: r.ConfigHash, RosterDigest: r.RosterDigest,
			Total: r.Total, CalculatedAt: timestamp(calculated), Through: uint32(min(int64(r.Through), asof.Unix(), int64(head)+1)),
			SourceThrough: head, Version: Version, Breakdown: make(map[string]uint16), Status: "complete"}
		if g.Through <= r.Since {
			return nil, fmt.Errorf("invalid geography cutoff")
		}
		if g.Through < g.Until {
			g.Partial = 1
		}
		for _, m := range r.Members {
			key := [2]uint32{r.Since, uint32(m.Index)}
			o, ok := byID[key]
			if !ok || o.Pubkey != m.Pubkey {
				return nil, fmt.Errorf("location response does not match roster")
			}
			delete(byID, key)
			p := o.GeoMember
			p.ADNL = m.ADNL
			if p.Samples == 0 {
				p = GeoMember{Pubkey: m.Pubkey, ADNL: m.ADNL, Status: "missing_observation"}
			} else {
				if p.ObservedAt == nil || p.RecordID == nil || *p.ObservedAt < r.Since || *p.ObservedAt >= g.Through {
					return nil, fmt.Errorf("observation outside round/cutoff")
				}
				g.Observed++
				p.Status = "located"
				if p.Country != nil {
					v := strings.ToUpper(strings.TrimSpace(*p.Country))
					if len(v) != 2 || v[0] < 'A' || v[0] > 'Z' || v[1] < 'A' || v[1] > 'Z' {
						p.Country = nil
					} else {
						p.Country = &v
					}
				}
				if p.City != nil && *p.City == "" {
					p.City = nil
				}
				if p.Latitude == nil || p.Longitude == nil || math.IsNaN(*p.Latitude) || math.IsNaN(*p.Longitude) || math.IsInf(*p.Latitude, 0) || math.IsInf(*p.Longitude, 0) || math.Abs(*p.Latitude) > 90 || math.Abs(*p.Longitude) > 180 || (*p.Latitude == 0 && *p.Longitude == 0) {
					p.Latitude, p.Longitude = nil, nil
					p.Status = "missing_coordinates"
				}
				// Same ordering key with conflicting payloads has no unique winner.
				if p.OrderingConflicts > 0 {
					p.Latitude, p.Longitude, p.Country, p.City = nil, nil, nil, nil
					p.Status = "conflicting_observation"
				}
				if p.Latitude != nil {
					g.Located++
				}
				if p.Country != nil {
					g.CountryKnown++
					g.Breakdown[*p.Country]++
				}
			}
			g.Members = append(g.Members, p)
		}
		if g.CountryKnown > 0 {
			n := uint16(len(g.Breakdown))
			g.Countries = &n
		}
		g.Unknown = g.Total - g.Located
		if g.Observed == 0 {
			g.Status = "missing_observations"
		} else if g.Partial != 0 || g.Located < g.Total || g.CountryKnown < g.Total {
			g.Status = "partial"
		}
		result = append(result, g)
	}
	if len(byID) != 0 {
		return nil, fmt.Errorf("unexpected extra location rows")
	}
	return result, nil
}
