// gramstat-dex-volume prepares closed UTC days from recognized mainnet actions.
// The default is a read-only reconciliation; --write publishes the result.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/dexvolume"
)

func run(ctx context.Context) error {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	from := flag.String("from", today.AddDate(0, 0, -1).Format(time.DateOnly), "first UTC day")
	to := flag.String("to", today.Format(time.DateOnly), "exclusive UTC day")
	write := flag.Bool("write", false, "publish complete reads to gramstat.daily_metrics")
	missing := flag.Bool("missing", false, "skip already published days, including days with incomplete attribution")
	recent := flag.Bool("recent-first", false, "process the newest requested day first")
	ssh := flag.String("price-ssh-config", "/etc/gramstat-dex/ssh_config", "restricted price reader SSH configuration")
	samples := flag.Bool("audit-routes", false, "show one boundary reconciliation per complex route shape per UTC day")
	flag.Parse()
	start, e := time.Parse(time.DateOnly, *from)
	if e != nil {
		return e
	}
	stop, e := time.Parse(time.DateOnly, *to)
	if e != nil {
		return e
	}
	if !start.Before(stop) || stop.After(today) {
		return fmt.Errorf("expected nonempty interval of closed UTC days")
	}
	if os.Getenv("GRAMSTAT_PG_DSN") == "" {
		return fmt.Errorf("missing PostgreSQL reader")
	}
	cfg, e := pgx.ParseConfig(os.Getenv("GRAMSTAT_PG_DSN"))
	if e != nil {
		return fmt.Errorf("invalid PostgreSQL reader")
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	for k, v := range map[string]string{"application_name": "gramstat-dex-volume", "default_transaction_read_only": "on", "timezone": "UTC", "statement_timeout": "45000", "lock_timeout": "1000", "max_parallel_workers_per_gather": "0", "jit": "off", "work_mem": "32MB"} {
		cfg.RuntimeParams[k] = v
	}
	pg, e := pgx.ConnectConfig(ctx, cfg)
	if e != nil {
		return fmt.Errorf("cannot connect to PostgreSQL reader")
	}
	defer pg.Close(ctx)
	var sink *gramstat.ClickHouse
	skip := map[string]bool{}
	if *write || *missing {
		sink, e = gramstat.NewClickHouse(os.Getenv("GRAMSTAT_CH_URL"), os.Getenv("GRAMSTAT_CH_USER"), os.Getenv("GRAMSTAT_CH_PASSWORD"))
		if e != nil {
			return e
		}
		if *missing {
			raw, e := sink.Query(ctx, `SELECT day FROM gramstat.daily_metrics FINAL WHERE metric={metric:String} AND day>={lo:Date} AND day<{hi:Date} FORMAT JSON`, url.Values{"param_metric": {dexvolume.Metric}, "param_lo": {*from}, "param_hi": {*to}})
			if e != nil {
				return e
			}
			var res struct{ Data []struct{ Day string } }
			if e = json.Unmarshal(raw, &res); e != nil {
				return e
			}
			for _, row := range res.Data {
				skip[row.Day] = true
			}
		}
	}
	for i := 0; i < int(stop.Sub(start)/(24*time.Hour)); i++ {
		day := start.AddDate(0, 0, i)
		if *recent {
			day = stop.AddDate(0, 0, -1-i)
		}
		if skip[day.Format(time.DateOnly)] {
			continue
		}
		t := time.Now()
		var audit func(string, []dexvolume.Swap, []dexvolume.Route, int)
		if *samples {
			seen := map[string]bool{}
			audit = func(id string, ss []dexvolume.Swap, rs []dexvolume.Route, missing int) {
				shape := "one venue"
				switch {
				case missing > 0:
					shape = "unresolved"
				case len(rs) > 1:
					shape = "independent"
				case len(rs) == 1 && rs[0].Dex == "multiple":
					shape = "multiple venues"
				}
				if seen[shape] {
					return
				}
				seen[shape] = true
				type boundary struct {
					Dex     string
					OK      bool
					In, Out dexvolume.Leg
				}
				var legs []boundary
				for _, s := range ss {
					legs = append(legs, boundary{s.Dex, s.OK, s.In, s.Out})
				}
				_ = json.NewEncoder(os.Stderr).Encode(map[string]any{"audit": shape, "trace": id, "actions": legs, "routes": rs, "unresolved": missing})
			}
		}
		d, e := dexvolume.ReadDay(ctx, pg, day, audit)
		if e != nil {
			return fmt.Errorf("%s actions: %w", day.Format(time.DateOnly), e)
		}
		read := time.Since(t)
		priceCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
		b, e := dexvolume.Prices(priceCtx, *ssh, d)
		cancel()
		if e != nil {
			return fmt.Errorf("%s prices: %w", day.Format(time.DateOnly), e)
		}
		row, e := dexvolume.Summarize(d, b, time.Now())
		if e != nil {
			return e
		}
		if *write {
			if e = sink.WriteDaily(ctx, []gramstat.DailyMetric{row}); e != nil {
				return e
			}
		}
		if e = json.NewEncoder(os.Stdout).Encode(row); e != nil {
			return e
		}
		log.Printf("%s complex=%d actions=%s elapsed=%s published=%t", row.Day, d.Complex, read.Round(time.Millisecond), time.Since(t).Round(time.Millisecond), *write)
	}
	return nil
}
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer cancel()
	if e := run(ctx); e != nil {
		log.Fatal(e)
	}
}
