package wallets

import (
	"context"
	_ "embed"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed activation_breakdown.sql
var activationBreakdownSQL string

type versionDay struct {
	Counts map[string]uint64
	Total  uint64
	MC     uint32
}

func emptyVersionDay() *versionDay {
	d := &versionDay{Counts: map[string]uint64{}}
	for _, v := range versions {
		d.Counts[v] = 0
	}
	return d
}

func versionParameter(d *versionDay) string {
	keys := make([]string, 0, len(d.Counts))
	for key := range d.Counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, "'"+key+"':"+strconv.FormatUint(d.Counts[key], 10))
	}
	return "{" + strings.Join(values, ",") + "}"
}

// All arithmetic stays UInt64. Complete maps distinguish certified zeros from
// legacy/unprepared rows. The caller compares Total to its independent counter.
func (h *Handler) activationVersions(ctx context.Context, p url.Values) (map[string]*versionDay, error) {
	var rows struct {
		Data []struct {
			Day       string `json:"day"`
			Code      string `json:"code"`
			Count     uint64 `json:"activations,string"`
			Ambiguous uint32 `json:"ambiguous"`
			MC        uint32 `json:"through_mc"`
		} `json:"data"`
	}
	if err := h.query(ctx, activationBreakdownSQL, p, &rows); err != nil {
		from, _ := strconv.ParseUint(p.Get("param_day_from"), 10, 64)
		before, _ := strconv.ParseUint(p.Get("param_before"), 10, 64)
		if before > from+3600 && (strings.Contains(err.Error(), "Code: 241.") || strings.Contains(err.Error(), "Code: 159.")) {
			// Bounded read-only retries, including the unusually busy 2024 days.
			// Split on transaction time so no first event appears in both halves.
			middle := from + (before-from)/2
			result := map[string]*versionDay{}
			for _, span := range [][2]uint64{{from, middle}, {middle, before}} {
				part := url.Values{}
				for k, v := range p {
					part[k] = append([]string(nil), v...)
				}
				set(part, "day_from", span[0])
				set(part, "before", span[1])
				days, e := h.activationVersions(ctx, part)
				if e != nil {
					return nil, e
				}
				for day, d := range days {
					if result[day] == nil {
						result[day] = emptyVersionDay()
					}
					dst := result[day]
					if d.Total > math.MaxUint64-dst.Total {
						return nil, fmt.Errorf("wallet activation count overflow")
					}
					dst.Total += d.Total
					dst.MC = max(dst.MC, d.MC)
					for key, n := range d.Counts {
						dst.Counts[key] += n
					}
				}
			}
			return result, nil
		}
		return nil, err
	}
	byCode := map[string]string{}
	for i, code := range codes {
		byCode[code] = versions[i]
	}
	result := map[string]*versionDay{}
	seen := map[string]bool{}
	from, _ := strconv.ParseInt(p.Get("param_day_from"), 10, 64)
	before, _ := strconv.ParseInt(p.Get("param_before"), 10, 64)
	end, _ := strconv.ParseUint(p.Get("param_end"), 10, 32)
	for _, row := range rows.Data {
		date, err := time.Parse(time.DateOnly, row.Day)
		key := row.Day + row.Code
		version := byCode[row.Code]
		if err != nil || date.Unix()+86400 <= from || date.Unix() >= before ||
			row.MC > uint32(end) || row.MC == 0 || version == "" || row.Ambiguous != 0 || seen[key] {
			return nil, fmt.Errorf("invalid/ambiguous wallet activation version")
		}
		seen[key] = true
		d := result[row.Day]
		if d == nil {
			d = emptyVersionDay()
			result[row.Day] = d
		}
		if row.Count > math.MaxUint64-d.Total {
			return nil, fmt.Errorf("wallet activation count overflow")
		}
		d.Total += row.Count
		d.Counts[version] += row.Count
		d.MC = max(d.MC, row.MC)
	}
	return result, nil
}

func (h *Handler) dailyActivationVersions(ctx context.Context, p url.Values, total string) (string, error) {
	days, err := h.activationVersions(ctx, p)
	if err != nil {
		return "", err
	}
	d := days[p.Get("param_day")]
	if d == nil {
		d = emptyVersionDay()
	}
	if len(days) > 1 || strconv.FormatUint(d.Total, 10) != total {
		return "", fmt.Errorf("wallet activation versions disagree with daily total %s", total)
	}
	return versionParameter(d), nil
}

// Upgrade only the compact existing publication rows. Reuse their run IDs and
// coverage proof: activity, cumulative points and monthly unions stay attached
// to the same publication. Each month's complete reconciliation precedes writes.
func (h *Handler) backfillActivationVersions(ctx context.Context, params url.Values, from, to time.Time) error {
	p := url.Values{}
	for k, v := range params {
		p[k] = append([]string(nil), v...)
	}
	set(p, "upgrade_from", uint64(from.Unix()))
	set(p, "upgrade_to", uint64(to.Unix()))
	var missing struct {
		Data []struct {
			Day   string `json:"day"`
			Total uint64 `json:"activations,string"`
			MC    uint32 `json:"through_mc"`
			Run   string `json:"run_id"`
		} `json:"data"`
	}
	if err := h.query(ctx, `SELECT day,toString(argMax(d.activations,tuple(d.through_mc,d.calculated_at,d.run_id))) AS activations,
 argMax(d.through_mc,tuple(d.through_mc,d.calculated_at,d.run_id)) AS through_mc,
 toString(argMax(d.run_id,tuple(d.through_mc,d.calculated_at,d.run_id))) AS run_id
 FROM {sink:Identifier}.wallet_days d
 WHERE source_id={source:String} AND registry={registry:String} AND history_verified=1
 AND day>=toDate(toDateTime({upgrade_from:UInt32},'UTC')) AND day<toDate(toDateTime({upgrade_to:UInt32},'UTC'))
 GROUP BY day HAVING length(argMax(d.activation_versions,tuple(d.through_mc,d.calculated_at,d.run_id)))=0
 ORDER BY day FORMAT JSON`, p, &missing); err != nil {
		return err
	}
	for i := 0; i < len(missing.Data); {
		j := i + 1
		for j < len(missing.Data) && missing.Data[j].Day[:7] == missing.Data[i].Day[:7] {
			j++
		}
		start, _ := time.Parse(time.DateOnly, missing.Data[i].Day)
		last, _ := time.Parse(time.DateOnly, missing.Data[j-1].Day)
		set(p, "day_from", uint64(start.Unix()))
		set(p, "before", uint64(last.AddDate(0, 0, 1).Unix()))
		days, err := h.activationVersions(ctx, p)
		if err != nil {
			return fmt.Errorf("activation versions %s: %w", start.Format("2006-01"), err)
		}
		for _, row := range missing.Data[i:j] {
			d := days[row.Day]
			if d == nil {
				d = emptyVersionDay()
				days[row.Day] = d
			}
			if d.Total != row.Total || d.MC > row.MC {
				return fmt.Errorf("activation versions mismatch on %s: got %d, want %d", row.Day, d.Total, row.Total)
			}
		}
		for _, row := range missing.Data[i:j] {
			p.Set("param_day", row.Day)
			p.Set("param_run", row.Run)
			p.Set("param_versions", versionParameter(days[row.Day]))
			// The original certified run is immutable. This is a new format of
			// that row, not a new publication of the large dependent datasets.
			if err := h.query(ctx, `INSERT INTO {sink:Identifier}.wallet_days
 SELECT source_id,registry,day,run_id,through_mc,complete_before,history_verified,
 proof_id,activations,opening_activated,first_mc,last_mc,now64(6),{versions:Map(String,UInt64)}
 FROM {sink:Identifier}.wallet_days
 WHERE source_id={source:String} AND registry={registry:String} AND day={day:Date} AND run_id={run:UUID}
 ORDER BY through_mc DESC,calculated_at DESC LIMIT 1`, p, nil); err != nil {
				return err
			}
		}
		if h.Log != nil {
			h.Log.Info("wallet activation versions prepared", "from", start.Format(time.DateOnly), "days", j-i)
		}
		i = j
	}
	return nil
}
