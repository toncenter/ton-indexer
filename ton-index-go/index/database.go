package index

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DbClient struct {
	Pool *pgxpool.Pool
}

func afterConnectRegisterTypes(ctx context.Context, conn *pgx.Conn) error {
	data_type_names := []string{
		"tonaddr",
		"_tonaddr",
		"tonhash",
		"_tonhash",
		"peer_swap_details",
		"_peer_swap_details",
	}
	for _, type_name := range data_type_names {
		data_type, err := conn.LoadType(ctx, type_name)
		if err != nil {
			return fmt.Errorf("failed to load type '%s': %v", type_name, err)
		}
		conn.TypeMap().RegisterType(data_type)
	}
	return nil
}

func NewDbClient(dsn string, maxconns int, minconns int) (*DbClient, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if maxconns > 0 {
		config.MaxConns = int32(maxconns)
	}
	if minconns > 0 {
		config.MinConns = int32(minconns)
	}
	config.HealthCheckPeriod = 60 * time.Second
	config.AfterConnect = afterConnectRegisterTypes

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
