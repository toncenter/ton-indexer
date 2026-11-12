// gramstat-aggregate computes analytics from indexed data and publishes to ClickHouse.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/handlers/accounts"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/handlers/actions"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/handlers/chain"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/handlers/validators"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/handlers/wallets"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("aggregation failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	handlers := flag.String("handlers", "actions", "comma-separated handler names")
	from := flag.String("from", "", "first UTC date, YYYY-MM-DD (requires --to)")
	to := flag.String("to", "", "exclusive UTC end date, YYYY-MM-DD (requires --from)")
	lookback := flag.Int("lookback-days", 3, "closed days to refresh, in addition to today")
	timeout := flag.Duration("timeout", 20*time.Minute, "timeout per handler, including catch-up")
	daemon := flag.Bool("daemon", false, "run recurring handlers until SIGTERM/SIGINT")
	interval := flag.Duration("interval", 5*time.Minute, "daemon refresh interval")
	monthlyInterval := flag.Duration("monthly-interval", 15*time.Minute, "daemon monthly-unique refresh interval")
	monthlyTimeout := flag.Duration("monthly-timeout", 5*time.Minute, "daemon monthly-unique handler timeout")
	reconcileInterval := flag.Duration("reconcile-interval", time.Hour, "daemon historical reconciliation interval")
	concurrency := flag.Int("concurrency", 3, "maximum concurrent daemon handlers")
	recentOnly := flag.Bool("recent-only", false, "one-off recent-window refresh without historical catch-up")
	actionsChunkDays := flag.Int("actions-chunk-days", 7, "actions days per source scan (1..3660; large chunks require explicit dates)")
	actionsStatementTimeout := flag.Duration("actions-statement-timeout", 5*time.Minute, "actions PostgreSQL statement timeout")
	actionsWorkMem := flag.Int("actions-work-mem-mb", 64, "actions PostgreSQL memory per sort/hash operation (1..1024 MiB)")
	actionsWorkers := flag.Int("actions-parallel-workers", 0, "actions PostgreSQL parallel scan workers (0..4)")
	actionsMonthly := flag.Bool("actions-include-monthly", false, "also prepare complete-month unique addresses in the same historical source scan")
	schema := flag.Bool("print-schema", false, "print ClickHouse DDL and exit (apply once as an administrator)")
	list := flag.Bool("list-handlers", false, "list available handlers and exit")
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if *schema {
		fmt.Print(gramstat.DailySchema)
		fmt.Print(gramstat.DailyMigration)
		fmt.Print(gramstat.StarsSchema)
		fmt.Print(gramstat.ChainSchema)
		fmt.Print(chain.Schema)
		fmt.Print(accounts.Schema)
		fmt.Print(wallets.Schema)
		fmt.Print(strings.Replace(gramstat.ChainSchema, "gramstat.chain_daily", "gramstat.chain_daily_full", 1))
		fmt.Print(validators.Schema)
		fmt.Print(validators.StakeSchema)
		fmt.Print(validators.StakeChangesSchema)
		fmt.Print(validators.LockedSchema)
		fmt.Print(validators.AffiliationSchema)
		return nil
	}
	if *list {
		fmt.Println("actions: " + strings.Join(actions.Metrics, ", "))
		fmt.Println("stars: explicit-date Stars-only rebuild through Fragment's account index")
		fmt.Println("actions_total: explicit-date initial total action type-count backfill")
		fmt.Println("actions_monthly: exact calendar-month unique jetton/NFT senders, receivers and swap users")
		fmt.Println("chain: full-history block flows and burn-corrected daily supply")
		fmt.Println("accounts: existing accounts from a verified opening state and complete deltas")
		fmt.Println("wallets: exact minute/hour/day activity and per-MC first activations")
		fmt.Println("validators: elected #34 count and roster by round")
		fmt.Println("validator_geo: session-stats locations, countries and coverage by round")
		fmt.Println("validator_stake: principal, rewards and two-round staking APR by verified round")
		fmt.Println("validator_stake_changes: holder stake changes between rounds n-2 and n")
		fmt.Println("validator_stake_locked: current plus previous selected stake, Telegram and pool-contract attribution")
		return nil
	}
	if *timeout <= 0 {
		return fmt.Errorf("--timeout must be positive")
	}
	req := gramstat.Request{AsOf: time.Now().UTC(), LookbackDays: *lookback, RecentOnly: *recentOnly}
	var err error
	if *from != "" {
		if req.From, err = time.Parse(time.DateOnly, *from); err != nil {
			return fmt.Errorf("invalid --from date")
		}
	}
	if *to != "" {
		if req.To, err = time.Parse(time.DateOnly, *to); err != nil {
			return fmt.Errorf("invalid --to date")
		}
	}
	if _, _, err := req.DailyWindow(); err != nil {
		return err
	}
	if (*daemon && (!req.From.IsZero() || *recentOnly || *actionsMonthly || *actionsChunkDays > 7)) || (*recentOnly && !req.From.IsZero()) {
		return fmt.Errorf("daemon/recent refresh cannot be combined with explicit backfill options")
	}
	if *actionsMonthly && (req.From.IsZero() || *actionsChunkDays != 3660) {
		return fmt.Errorf("--actions-include-monthly requires explicit dates and --actions-chunk-days 3660")
	}
	ch, err := gramstat.NewClickHouse(os.Getenv("GRAMSTAT_CH_URL"), os.Getenv("GRAMSTAT_CH_USER"), os.Getenv("GRAMSTAT_CH_PASSWORD"))
	if err != nil {
		return err
	}
	registry := []gramstat.Handler{
		&wallets.Handler{CH: ch, SourceID: os.Getenv("GRAMSTAT_WALLETS_SOURCE_ID"), SinkDB: os.Getenv("GRAMSTAT_WALLETS_SINK_DB"), FollowNative: os.Getenv("GRAMSTAT_WALLETS_FOLLOW_NATIVE") == "1", Log: log},
		&accounts.Handler{CH: ch, MetricsDB: os.Getenv("GRAMSTAT_CHAIN_METRICS_DB"), Log: log},
		&chain.Handler{CH: ch, RawDB: os.Getenv("GRAMSTAT_CHAIN_RAW_DB"), MetricsDB: os.Getenv("GRAMSTAT_CHAIN_METRICS_DB"), Log: log},
		&actions.Handler{Source: actions.Postgres{DSN: os.Getenv("GRAMSTAT_PG_DSN"), StatementTimeout: *actionsStatementTimeout, WorkMemMB: *actionsWorkMem, ParallelWorkers: *actionsWorkers, IncludeMonthly: *actionsMonthly}, Sink: ch, Log: log, ChunkDays: *actionsChunkDays},
		&actions.Handler{Source: actions.Postgres{DSN: os.Getenv("GRAMSTAT_PG_DSN"), StatementTimeout: *actionsStatementTimeout, WorkMemMB: *actionsWorkMem, ParallelWorkers: *actionsWorkers, StarsOnly: true}, Sink: ch, Log: log, ChunkDays: *actionsChunkDays, StarsOnly: true},
		&actions.Handler{Source: actions.Postgres{DSN: os.Getenv("GRAMSTAT_PG_DSN"), StatementTimeout: *actionsStatementTimeout, WorkMemMB: *actionsWorkMem, ParallelWorkers: *actionsWorkers, TotalOnly: true}, Sink: ch, Log: log, ChunkDays: *actionsChunkDays, TotalOnly: true},
		&actions.MonthlyHandler{Source: actions.Postgres{DSN: os.Getenv("GRAMSTAT_PG_DSN"), StatementTimeout: *actionsStatementTimeout, WorkMemMB: *actionsWorkMem, ParallelWorkers: *actionsWorkers}, Sink: ch, Log: log},
		&validators.Handler{Source: validators.Postgres{DSN: os.Getenv("GRAMSTAT_PG_DSN")},
			Verifier: validators.ConfigRPC{URL: strings.TrimRight(os.Getenv("GRAMSTAT_CONFIG_URL"), "/"), APIKey: os.Getenv("GRAMSTAT_CONFIG_API_KEY")},
			Sink:     validators.Store{CH: ch}, Log: log},
		&validators.GeoHandler{Source: validators.SessionStats{URL: os.Getenv("GRAMSTAT_SS_URL"), User: os.Getenv("GRAMSTAT_SS_USER"), Password: os.Getenv("GRAMSTAT_SS_PASSWORD")},
			Sink: validators.Store{CH: ch}, Log: log},
		&validators.StakeHandler{Source: validators.Postgres{DSN: os.Getenv("GRAMSTAT_PG_DSN")},
			Sink: validators.Store{CH: ch}, ResultsDB: os.Getenv("GRAMSTAT_STAKE_RESULTS_DB"), Log: log},
		&validators.StakeChangesHandler{Source: validators.Postgres{DSN: os.Getenv("GRAMSTAT_PG_DSN")},
			Sink: validators.Store{CH: ch}, ResultsDB: os.Getenv("GRAMSTAT_STAKE_RESULTS_DB"), Log: log},
		&validators.LockedHandler{Source: validators.Postgres{DSN: os.Getenv("GRAMSTAT_PG_DSN")},
			Attribution: validators.SessionStats{URL: os.Getenv("GRAMSTAT_SS_URL"), User: os.Getenv("GRAMSTAT_SS_USER"), Password: os.Getenv("GRAMSTAT_SS_PASSWORD")},
			Resolver:    &validators.AffiliationResolver{RPC: validators.ConfigRPC{URL: os.Getenv("GRAMSTAT_CONFIG_URL"), APIKey: os.Getenv("GRAMSTAT_CONFIG_API_KEY")}, Store: validators.Store{CH: ch}, Log: log},
			Sink:        validators.Store{CH: ch}, Log: log},
	}
	names := strings.Split(*handlers, ",")
	for i := range names {
		names[i] = strings.TrimSpace(names[i])
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *daemon {
		var specs []gramstat.Schedule
		selected := map[string]bool{}
		for _, name := range names {
			selected[name] = true
		}
		dependencies := map[string][]string{
			"validator_geo":           {"validators"},
			"validator_stake":         {"validators"},
			"validator_stake_changes": {"validator_stake"},
			"validator_stake_locked":  {"validator_geo", "validator_stake"},
		}
		for _, name := range names {
			if name == "stars" || name == "actions_total" {
				return fmt.Errorf("targeted rebuild is not a recurring handler; use actions")
			}
			s := gramstat.Schedule{Name: name, Every: *interval, Timeout: *timeout, ReconcileEvery: *reconcileInterval}
			if name == "actions_monthly" {
				s.Every, s.Timeout = *monthlyInterval, *monthlyTimeout
			}
			for _, dep := range dependencies[name] {
				if selected[dep] {
					s.After = append(s.After, dep)
				}
			}
			specs = append(specs, s)
		}
		return gramstat.Serve(ctx, log, registry, specs, *concurrency, req)
	}
	return gramstat.Run(ctx, log, registry, names, req, *timeout)
}
