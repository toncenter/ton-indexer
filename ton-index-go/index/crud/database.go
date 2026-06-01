package crud

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DbClient struct {
	Pool    *pgxpool.Pool
	Kvrocks *KvrocksStore
}

func afterConnectRegisterTypes(ctx context.Context, conn *pgx.Conn) error {
	text_type_names := []string{
		"tonaddr",
		"tonhash",
		"tonbytes",
	}
	for _, type_name := range text_type_names {
		var oid uint32
		err := conn.QueryRow(ctx, "select $1::text::regtype::oid", type_name).Scan(&oid)
		if err != nil {
			return fmt.Errorf("failed to load type '%s': %v", type_name, err)
		}
		conn.TypeMap().RegisterType(&pgtype.Type{Name: type_name, OID: oid, Codec: pgtype.TextCodec{}})
	}

	data_type_names := []string{
		"_tonaddr",
		"_tonhash",
		"_tonbytes",
		"peer_swap_details",
		"liquidity_vault_excess_details",
		"_peer_swap_details",
		"_liquidity_vault_excess_details",
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

type ServiceVersion struct {
	Major int32
	Minor int32
	Patch int32
}

func LoadVersion(pool *pgxpool.Pool) ServiceVersion {
	ctx := context.Background()
	query := `SELECT major, minor, patch as extra FROM ton_db_version where id = 1;`

	version := ServiceVersion{1, 2, 0}
	err := pool.QueryRow(ctx, query).Scan(&version.Major, &version.Minor, &version.Patch)
	if err != nil {
		log.Printf("Warning: Failed to load version: %v", err)
	}
	return version
}

func NewDbClient(dsn string, maxconns int, minconns int, kvrocks *KvrocksStore) (*DbClient, error) {
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
		return nil, fmt.Errorf("failed to create connection pool: %v", err)
	}
	if err = pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}
	return &DbClient{Pool: pool, Kvrocks: kvrocks}, nil
}
