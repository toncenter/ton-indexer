package wallets

import (
	"context"
	_ "embed"
	"fmt"
	"net/url"
	"time"
)

//go:embed live_activated.sql
var liveActivatedSQL string

//go:embed live_activations_daily.sql
var liveActivationsDailySQL string

type activationPrefix struct {
	MC        uint32 `json:"through_mc"`
	Base      uint32 `json:"base_mc"`
	From      uint32 `json:"from_utime"`
	To        uint32 `json:"to_utime"`
	Activated string `json:"activated"`
}

func (h *Handler) liveActivated(ctx context.Context, params url.Values, s source) error {
	if s.Verified != 1 {
		return nil
	}
	p := url.Values{}
	for k, v := range params {
		p[k] = append([]string(nil), v...)
	}
	var state struct {
		Data []activationPrefix `json:"data"`
	}
	if err := h.query(ctx, `SELECT through_mc,base_mc,from_utime,to_utime,toString(activated) AS activated
 FROM {sink:Identifier}.wallet_activation_stream_state
 WHERE source_id={source:String} AND registry={registry:String}
 ORDER BY through_mc DESC LIMIT 1 FORMAT JSON`, p, &state); err != nil {
		return err
	}
	if len(state.Data) == 0 {
		// Seed once from an already committed cumulative point. A pause day with
		// no MCs is skipped, not mistaken for a zero opening balance.
		if err := h.query(ctx, `WITH last_day AS (
 SELECT day,run_id,last_mc FROM {sink:Identifier}.wallet_days
 WHERE source_id={source:String} AND registry={registry:String} AND history_verified=1 AND last_mc>0
 AND complete_before>=toUnixTimestamp(toDateTime(day+1,'UTC'))
 ORDER BY day DESC,through_mc DESC,calculated_at DESC,run_id DESC LIMIT 1)
 SELECT a.mc_seqno AS through_mc,a.mc_seqno AS base_mc,a.utime AS from_utime,a.utime AS to_utime,toString(a.activated) AS activated
 FROM {sink:Identifier}.wallet_activations_mc a
 WHERE a.source_id={source:String} AND a.registry={registry:String}
 AND a.day=(SELECT day FROM last_day) AND a.run_id=(SELECT run_id FROM last_day) AND a.mc_seqno=(SELECT last_mc FROM last_day)
 FORMAT JSON`, p, &state); err != nil {
			return err
		}
		if len(state.Data) == 0 {
			return nil
		} // Closed-day preparation will seed a later run.
	}
	if len(state.Data) != 1 {
		return fmt.Errorf("ambiguous wallet activation prefix")
	}
	prev := state.Data[0]
	if prev.Base == 0 || prev.MC < prev.Base || prev.MC > s.End || prev.Activated == "" {
		return fmt.Errorf("invalid/regressing wallet activation prefix")
	}
	for prev.MC < s.End {
		end := uint32(min(uint64(prev.MC)+20000, uint64(s.End)))
		set(p, "previous", uint64(prev.MC))
		set(p, "end", uint64(end))
		p.Set("param_opening", prev.Activated)
		set(p, "previous_time", uint64(prev.To))
		var block struct {
			Time uint32 `json:"utime"`
		}
		if err := h.query(ctx, `SELECT toUInt32(gen_utime) AS utime FROM {raw:Identifier}.blocks FINAL
 WHERE workchain=-1 AND shard=-9223372036854775808 AND seqno={end:UInt32} FORMAT JSONEachRow`, p, &block); err != nil {
			return err
		}
		if block.Time < prev.To {
			return fmt.Errorf("missing/regressing activation MC time")
		}
		set(p, "tip_time", uint64(block.Time))
		if err := h.query(ctx, liveActivatedSQL, p, nil); err != nil {
			return fmt.Errorf("activation stream: %w", err)
		}
		var tip struct {
			N    uint32 `json:"n"`
			Time uint32 `json:"utime"`
			Min  string `json:"minimum"`
			Max  string `json:"maximum"`
		}
		if err := h.query(ctx, `SELECT toUInt32(count()) AS n,max(utime) AS utime,toString(min(activated)) AS minimum,toString(max(activated)) AS maximum
 FROM {sink:Identifier}.wallet_activation_stream WHERE source_id={source:String} AND registry={registry:String} AND mc_seqno={end:UInt32}
 AND day=toDate(toDateTime({tip_time:UInt32},'UTC')) FORMAT JSONEachRow`, p, &tip); err != nil {
			return err
		}
		if tip.N == 0 || tip.Min != tip.Max || tip.Time != block.Time {
			return fmt.Errorf("missing/conflicting activation stream tip")
		}
		// Append checkpoint versions. ReplacingMergeTree could discard a committed
		// checkpoint in favour of an uncommitted future one after a failed insert.
		for _, step := range []uint64{3600, 86400} {
			set(p, "step", step)
			if err := h.query(ctx, `INSERT INTO {sink:Identifier}.wallet_activation_stream_checkpoints
 SELECT source_id,registry,day,{step:UInt32},intDiv(utime,{step:UInt32})*{step:UInt32},max(mc_seqno),argMax(utime,mc_seqno),argMax(activated,mc_seqno)
 FROM {sink:Identifier}.wallet_activation_stream WHERE source_id={source:String} AND registry={registry:String}
 AND mc_seqno>{previous:UInt32} AND mc_seqno<={end:UInt32}
 AND day>=toDate(toDateTime({previous_time:UInt32},'UTC')) AND day<=toDate(toDateTime({tip_time:UInt32},'UTC'))
 GROUP BY source_id,registry,day,intDiv(utime,{step:UInt32})*{step:UInt32}`, p, nil); err != nil {
				return err
			}
		}
		day := time.Unix(int64(s.Before), 0).UTC().Truncate(24 * time.Hour)
		p.Set("param_day", day.Format(time.DateOnly))
		set(p, "day_from", uint64(day.Unix()))
		set(p, "before", uint64(s.Before))
		count := "0"
		set(p, "daily_ready", 0)
		if end == s.End && day.Unix() >= int64(s.From) && day.Unix() < int64(s.Before) {
			set(p, "daily_ready", 1)
			var bounds struct {
				MC uint32 `json:"mc"`
			}
			if err := h.query(ctx, `SELECT toUInt32(min(mc_block_seqno)) AS mc FROM {raw:Identifier}.blocks
 WHERE gen_utime>={day_from:UInt32} AND gen_utime<{before:UInt32}
 AND mc_block_seqno BETWEEN {first:UInt32} AND {end:UInt32} FORMAT JSONEachRow`, p, &bounds); err != nil {
				return err
			}
			count = "0"
			if bounds.MC > 0 {
				set(p, "day_first_mc", uint64(bounds.MC))
				var daily struct {
					Count string `json:"activations"`
				}
				if err := h.query(ctx, liveActivationsDailySQL, p, &daily); err != nil {
					return err
				}
				count = daily.Count
			}
		}
		set(p, "base", uint64(prev.Base))
		set(p, "base_time", uint64(prev.From))
		set(p, "tip_time", uint64(tip.Time))
		p.Set("param_activated", tip.Min)
		p.Set("param_daily_activations", count)
		if err := h.query(ctx, `INSERT INTO {sink:Identifier}.wallet_activation_stream_state
 SELECT {source:String},{registry:String},{base:UInt32},{base_time:UInt32},{end:UInt32},{tip_time:UInt32},{activated:UInt64},
 toDate({day:String}),if({daily_ready:UInt8}=1,toNullable({daily_activations:UInt64}),NULL),{before:UInt32},now64(6)`, p, nil); err != nil {
			return err
		}
		prev.MC, prev.To, prev.Activated = end, tip.Time, tip.Min
	}
	if h.Log != nil {
		h.Log.Info("wallet activation stream advanced", "through_mc", prev.MC, "utime", prev.To, "activated", prev.Activated)
	}
	return nil
}
