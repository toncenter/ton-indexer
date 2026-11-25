// Package accounts prepares existing-account counts from a verified state baseline.
package accounts

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

//go:embed schema.sql
var Schema string

//go:embed hourly.sql
var hourlySQL string

type Handler struct {
	CH        *gramstat.ClickHouse
	MetricsDB string
	Log       *slog.Logger
}

func (*Handler) Name() string { return "accounts" }

func (h *Handler) Run(ctx context.Context, _ gramstat.Request) error {
	if h.MetricsDB == "" {
		return fmt.Errorf("GRAMSTAT_CHAIN_METRICS_DB is required")
	}
	p := url.Values{"param_metrics": {h.MetricsDB}, "max_threads": {"2"}, "max_execution_time": {"90"}, "max_memory_usage": {"2147483648"}}
	data, err := h.CH.Query(ctx, `SELECT mc_seqno,toString(uninit) AS uninit,toString(active) AS active,toString(frozen) AS frozen,baseline_id
 FROM gramstat.accounts_baseline FINAL WHERE source_database={metrics:String} AND mc_seqno=2 FORMAT JSON`, p)
	if err != nil {
		return err
	}
	var base struct {
		Data []struct {
			MC                     uint32 `json:"mc_seqno"`
			Uninit, Active, Frozen string
			ID                     string `json:"baseline_id"`
		}
	}
	if err = json.Unmarshal(data, &base); err != nil {
		return err
	}
	if len(base.Data) != 1 || base.Data[0].ID == "" {
		return fmt.Errorf("exactly one verified MC 2 Accounts baseline is required")
	}
	b := base.Data[0]
	for key, value := range map[string]string{"baseline_uninit": b.Uninit, "baseline_active": b.Active, "baseline_frozen": b.Frozen} {
		if _, err = strconv.ParseUint(value, 10, 63); err != nil {
			return fmt.Errorf("invalid %s", key)
		}
		p.Set("param_"+key, value)
	}
	p.Set("param_baseline_mc", strconv.FormatUint(uint64(b.MC), 10))
	p.Set("param_baseline_id", b.ID)
	data, err = h.CH.Query(ctx, `SELECT max(mc_seqno) AS head FROM {metrics:Identifier}.account_deltas FORMAT JSONEachRow`, p)
	if err != nil {
		return err
	}
	var progress struct {
		Head uint32 `json:"head"`
	}
	if err = json.Unmarshal(data, &progress); err != nil {
		return err
	}
	if progress.Head <= b.MC+1000 {
		return fmt.Errorf("no settled Accounts prefix")
	}
	end := progress.Head - 1000
	p.Set("param_end", strconv.FormatUint(uint64(end), 10))
	// Verify the complete prefix before allowing any INSERT blocks to be emitted.
	data, err = h.CH.Query(ctx, `SELECT toUInt32(count()) AS n,min(mc_seqno) AS first_mc,max(mc_seqno) AS last_mc
 FROM {metrics:Identifier}.account_deltas FINAL WHERE mc_seqno>{baseline_mc:UInt32} AND mc_seqno<={end:UInt32} FORMAT JSONEachRow`, p)
	if err != nil {
		return err
	}
	var proof struct {
		N     uint32 `json:"n"`
		First uint32 `json:"first_mc"`
		Last  uint32 `json:"last_mc"`
	}
	if err = json.Unmarshal(data, &proof); err != nil {
		return err
	}
	if proof.First != b.MC+1 || proof.Last != end || proof.N != end-b.MC {
		return fmt.Errorf("incomplete Accounts delta prefix")
	}
	if _, err = h.CH.Query(ctx, "INSERT INTO gramstat.accounts_hourly "+hourlySQL, p); err != nil {
		return err
	}
	if h.Log != nil {
		h.Log.Info("accounts published", "source_mc_to", end, "baseline_id", b.ID)
	}
	return nil
}
