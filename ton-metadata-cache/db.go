package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DbClient wraps a pgxpool.Pool for database operations.
type DbClient struct {
	Pool *pgxpool.Pool
}

// NewDbClient creates a new DbClient with the given connection parameters.
func NewDbClient(dsn string, minConns int, maxConns int) (*DbClient, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	config.MinConns = int32(minConns)
	config.MaxConns = int32(maxConns)

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	return &DbClient{Pool: pool}, nil
}

// Close closes the underlying connection pool.
func (c *DbClient) Close() {
	c.Pool.Close()
}
