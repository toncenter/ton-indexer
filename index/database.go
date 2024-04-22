package index

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DbClient struct {
	Pool *pgxpool.Pool
}

func NewDbClient(dsn string) (*DbClient, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 128
	config.MinConns = 64
	config.HealthCheckPeriod = 60 * time.Second

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatalf("Failed to connect to database '%s': %v\n", dsn, err)
		os.Exit(63)
	}
	if err = pool.Ping(context.Background()); err != nil {
		log.Fatalf("Failed to ping to database '%s': %v\n", dsn, err)
		os.Exit(64)
	}
	return &DbClient{pool}, nil
}
