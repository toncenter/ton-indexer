package services

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"

	"context"
	"errors"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

var backgroundTaskManager *TaskManager

func SetBackgroundTaskManager(tm *TaskManager) { backgroundTaskManager = tm }
func GetBackgroundTaskManager() *TaskManager   { return backgroundTaskManager }

type TaskManager struct {
	pool        *pgxpool.Pool
	taskChannel chan []BackgroundTask
}

func NewBackgroundTaskManager(pg_dsn string, channel_size int, min_conns int, max_conns int) (*TaskManager, error) {
	config, err := pgxpool.ParseConfig(pg_dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pool config: %w", err)
	}
	config.MinConns = int32(min_conns)
	config.MaxConns = int32(max_conns)

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	user := pool.Config().ConnConfig.User

	row := conn.QueryRow(context.Background(), "SELECT has_table_privilege($1, 'background_tasks', 'INSERT, UPDATE, DELETE')", user)
	var has_privilege bool
	err = row.Scan(&has_privilege)
	if err != nil {
		return nil, err
	}
	if !has_privilege {
		return nil, errors.New("user does not have required privileges on background_tasks table")
	}
	return &TaskManager{
		pool:        pool,
		taskChannel: make(chan []BackgroundTask, channel_size),
	}, nil
}
func (manager *TaskManager) Start(ctx context.Context) {
	go manager.run(ctx)
}

func (manager *TaskManager) loop(ctx context.Context) {
	tasks := <-manager.taskChannel
	conn, err := manager.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection to create tasks: %v", err)
		manager.taskChannel <- tasks
	}
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Printf("Error beginning transaction to create tasks: %v", err)
		manager.taskChannel <- tasks
	}
	tx_failed := false
	for _, task := range tasks {
		_, err := tx.Exec(ctx, "INSERT INTO background_tasks (type, data, status) "+
			"VALUES ($1, $2, 'ready') ON CONFLICT DO NOTHING", task.Type, task.Data)
		if err != nil {
			log.Printf("Error inserting task: %v", err)
			tx.Rollback(ctx)
			manager.taskChannel <- tasks
			tx_failed = true
			break
		}
	}
	if !tx_failed {
		err = tx.Commit(ctx)
		if err != nil {
			log.Printf("Error committing transaction to create tasks: %v", err)
			manager.taskChannel <- tasks
		}
	}
}

func (manager *TaskManager) run(ctx context.Context) {
	for {
		manager.loop(ctx)
	}
}

func (manager *TaskManager) EnqueueTasksIfPossible(tasks []BackgroundTask) bool {
	select {
	case manager.taskChannel <- tasks:
		return true
	default:
		return false
	}
}
