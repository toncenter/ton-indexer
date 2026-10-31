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
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/handlers/actions"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/handlers/validators"
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
		fmt.Print(validators.Schema)
		return nil
	}
	if *list {
		fmt.Println("actions: " + strings.Join(actions.Metrics, ", "))
		fmt.Println("validators: elected #34 count and roster by round")
		fmt.Println("validator_geo: session-stats locations, countries and coverage by round")
		return nil
	}
	if *timeout <= 0 {
		return fmt.Errorf("--timeout must be positive")
	}
	req := gramstat.Request{AsOf: time.Now().UTC(), LookbackDays: *lookback}
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
	ch, err := gramstat.NewClickHouse(os.Getenv("GRAMSTAT_CH_URL"), os.Getenv("GRAMSTAT_CH_USER"), os.Getenv("GRAMSTAT_CH_PASSWORD"))
	if err != nil {
		return err
	}
	registry := []gramstat.Handler{
		&actions.Handler{Source: actions.Postgres{DSN: os.Getenv("GRAMSTAT_PG_DSN")}, Sink: ch, Log: log},
		&validators.Handler{Source: validators.Postgres{DSN: os.Getenv("GRAMSTAT_PG_DSN")},
			Verifier: validators.ConfigRPC{URL: strings.TrimRight(os.Getenv("GRAMSTAT_CONFIG_URL"), "/"), APIKey: os.Getenv("GRAMSTAT_CONFIG_API_KEY")},
			Sink:     validators.Store{CH: ch}, Log: log},
		&validators.GeoHandler{Source: validators.SessionStats{URL: os.Getenv("GRAMSTAT_SS_URL"), User: os.Getenv("GRAMSTAT_SS_USER"), Password: os.Getenv("GRAMSTAT_SS_PASSWORD")},
			Sink: validators.Store{CH: ch}, Log: log},
	}
	names := strings.Split(*handlers, ",")
	for i := range names {
		names[i] = strings.TrimSpace(names[i])
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return gramstat.Run(ctx, log, registry, names, req, *timeout)
}
