package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/websocket/v2"
	"github.com/redis/go-redis/v9"

	"github.com/toncenter/ton-indexer/ton-index-go/index"
	streamingv1 "github.com/toncenter/ton-indexer/ton-streaming-go/v1"
	streamingv2 "github.com/toncenter/ton-indexer/ton-streaming-go/v2"
)

var (
	redisAddr               = flag.String("redis", "localhost:6379", "Redis server dsn")
	tracesChannel           = flag.String("traces-channel", "new_trace", "Redis channel for blockchain events")
	commitedTxsChannel      = flag.String("commited-txs-channel", "new_finalized_txs", "Redis channel for committed transactions")
	confirmedTxsChannel     = flag.String("confirmed-txs-channel", "new_confirmed_txs", "Redis channel for confirmed transactions")
	classifiedTracesChannel = flag.String("classified-traces-channel", "classified_trace", "Redis channel for classified traces")
	redisPoolSize           = flag.Int("redis-pool-size", 0, "Connection pool size of redis client")
	redisMinIdleConns       = flag.Int("redis-min-idle-conns", 0, "Minimum amount of idle connections to keep in pool for redis client")
	redisMaxIdleConns       = flag.Int("redis-max-idle-conns", 0, "Maximum amount of idle connections to keep in pool for redis client")
	redisMaxActiveConns     = flag.Int("redis-max-active-conns", 0, "Maximum active redis connections")
	serverPort              = flag.Int("port", 8085, "Server port")
	prefork                 = flag.Bool("prefork", false, "Use prefork")
	testnet                 = flag.Bool("testnet", false, "Use testnet")
	pg                      = flag.String("pg", "", "PostgreSQL connection string")
	imgProxyBaseUrl         = flag.String("imgproxy-baseurl", "", "Image proxy base URL")
)

func main() {
	flag.Parse()

	options, err := redis.ParseURL(*redisAddr)
	if err != nil {
		log.Fatalf("Error parsing Redis URL: %v", err)
	}
	if redisPoolSize != nil {
		options.PoolSize = *redisPoolSize
	}
	if redisMinIdleConns != nil {
		options.MinIdleConns = *redisMinIdleConns
	}
	if redisMaxIdleConns != nil {
		options.MaxIdleConns = *redisMaxIdleConns
	}
	if redisMaxActiveConns != nil {
		options.MaxActiveConns = *redisMaxActiveConns
	}
	rdb := redis.NewClient(options)
	ctx := context.Background()
	go runRedisPoolStatLogger(ctx, rdb)

	var dbClient *index.DbClient
	if *pg != "" {
		log.Printf("Connecting to PostgreSQL: %s", *pg)
		dbClient, err = index.NewDbClient(*pg, 100, 0)
		if err != nil {
			log.Printf("Failed to connect to PostgreSQL: %v", err)
			log.Printf("AddressBook and Metadata will not be available")
		} else {
			log.Printf("Connected to PostgreSQL successfully")
		}
	} else {
		log.Printf("PostgreSQL connection string is not provided")
		log.Printf("AddressBook and Metadata will not be available")
	}

	streamingv1.InitConfig(streamingv1.Config{
		DBClient:        dbClient,
		Testnet:         *testnet,
		ImgProxyBaseURL: *imgProxyBaseUrl,
	})

	manager := streamingv1.NewClientManager()
	go manager.Run()

	go streamingv1.SubscribeToTraces(ctx, rdb, manager, *tracesChannel)
	go streamingv1.SubscribeToCommittedTransactions(ctx, rdb, manager, *commitedTxsChannel)
	go streamingv1.SubscribeToClassifiedTraces(ctx, rdb, manager, *classifiedTracesChannel)
	go streamingv1.SubscribeToInvalidatedTraces(ctx, rdb, manager)

	streamingv2.InitConfig(streamingv2.Config{
		DBClient:        dbClient,
		Testnet:         *testnet,
		ImgProxyBaseURL: *imgProxyBaseUrl,
	})

	v2Manager := streamingv2.NewClientManager()
	go v2Manager.Run()

	go streamingv2.SubscribeToTraces(ctx, rdb, v2Manager, *tracesChannel)
	go streamingv2.SubscribeToConfirmedTransactions(ctx, rdb, v2Manager, *confirmedTxsChannel) // "new_confirmed_txs"
	go streamingv2.SubscribeToFinalizedTransactions(ctx, rdb, v2Manager, *commitedTxsChannel)  // "new_finalized_txs"
	go streamingv2.SubscribeToClassifiedTraces(ctx, rdb, v2Manager, *classifiedTracesChannel)
	go streamingv2.SubscribeToAccountStateUpdates(ctx, rdb, v2Manager, "new_account_state")
	go streamingv2.SubscribeToInvalidatedTraces(ctx, rdb, v2Manager, "invalidated_traces")

	app := fiber.New(fiber.Config{
		AppName:     "TON Streaming API",
		Prefork:     *prefork,
		ReadTimeout: 5 * time.Second,
		ProxyHeader: fiber.HeaderXForwardedFor,
	})

	app.Use(logger.New())

	api := app.Group("/api/streaming")

	api.Post("/v1/sse", streamingv1.SSEHandler(manager))

	api.Use("/v1/ws", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			c.Locals("allowed", true)
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})
	api.Get("/v1/ws", websocket.New(streamingv1.WebSocketHandler(manager)))

	api.Post("/v2/sse", streamingv2.SSEHandler(v2Manager))

	api.Use("/v2/ws", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			c.Locals("allowed", true)
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})
	api.Get("/v2/ws", websocket.New(streamingv2.WebSocketHandler(v2Manager)))

	log.Printf("Starting server on port %d", *serverPort)
	log.Fatal(app.Listen(fmt.Sprintf(":%d", *serverPort)))
}

func runRedisPoolStatLogger(ctx context.Context, client *redis.Client) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		stats := client.PoolStats()
		log.Printf("Redis stats Hits: %d, Misses: %d, Timeouts: %d, TotalConns: %d, IdleConns: %d, StaleConns: %d\n",
			stats.Hits, stats.Misses, stats.Timeouts, stats.TotalConns, stats.IdleConns, stats.StaleConns)
	}
}
