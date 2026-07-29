package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/index/crud"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/websocket/v2"
	"github.com/redis/go-redis/v9"

	streamingv2 "github.com/toncenter/ton-indexer/ton-streaming-go/v2"
)

var (
	redisAddr                = flag.String("redis", "localhost:6379", "Redis server dsn")
	classifiedTracesChannel  = flag.String("classified-traces-channel", "classified_trace", "Redis channel for classified traces")
	transactionHintsChannel  = flag.String("transaction-hints-channel", "streaming_transactions", "Redis channel for v2 transaction hints")
	accountStateHintsChannel = flag.String("account-state-hints-channel", "streaming_account_states", "Redis channel for v2 account state hints")
	redisPoolSize            = flag.Int("redis-pool-size", 0, "Connection pool size of redis client")
	redisMinIdleConns        = flag.Int("redis-min-idle-conns", 0, "Minimum amount of idle connections to keep in pool for redis client")
	redisMaxIdleConns        = flag.Int("redis-max-idle-conns", 0, "Maximum amount of idle connections to keep in pool for redis client")
	redisMaxActiveConns      = flag.Int("redis-max-active-conns", 0, "Maximum active redis connections")
	serverPort               = flag.Int("port", 8085, "Server port")
	prefork                  = flag.Bool("prefork", false, "Use prefork")
	testnet                  = flag.Bool("testnet", false, "Use testnet")
	pg                       = flag.String("pg", "", "PostgreSQL connection string")
	kvrocksAddr              = flag.String("kvrocks", "", "Kvrocks address or Redis URL for enrichment reads")
	kvrocksSentinels         = flag.String("kvrocks-sentinels", "", "Comma-separated Kvrocks Sentinel addresses")
	kvrocksSentinelMaster    = flag.String("kvrocks-sentinel-master", "", "Kvrocks Sentinel master name")
	kvrocksUser              = flag.String("kvrocks-user", "", "Kvrocks username")
	kvrocksPassword          = flag.String("kvrocks-password", "", "Kvrocks password")
	kvrocksDB                = flag.Int("kvrocks-db", 0, "Kvrocks database number")
	kvrocksReplicaReads      = flag.Bool("kvrocks-replica-reads", false, "Read from Kvrocks replicas discovered via Sentinel")
	kvrocksStalenessBlocks   = flag.Int64("kvrocks-staleness-blocks", 5, "Max watermark lag in masterchain blocks for a Kvrocks replica to serve reads")
	kvrocksReplicaRefreshMS  = flag.Int64("kvrocks-replica-refresh-ms", 300, "Kvrocks replica discovery and freshness poll interval in milliseconds")
	imgProxyBaseUrl          = flag.String("imgproxy-baseurl", "", "Image proxy base URL")
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

	var kvrocksStore *crud.KvrocksStore
	if *kvrocksAddr != "" || *kvrocksSentinels != "" {
		kvrocksStore, err = crud.NewKvrocksStore(crud.KvrocksConfig{
			Addr:               *kvrocksAddr,
			SentinelAddrs:      crud.ParseKvrocksSentinelAddrs(*kvrocksSentinels),
			SentinelMasterName: *kvrocksSentinelMaster,
			Username:           *kvrocksUser,
			Password:           *kvrocksPassword,
			DB:                 *kvrocksDB,
			ReplicaReads:       *kvrocksReplicaReads,
			StalenessBlocks:    *kvrocksStalenessBlocks,
			ReplicaRefresh:     time.Duration(*kvrocksReplicaRefreshMS) * time.Millisecond,
		})
		if err != nil {
			log.Fatalf("Failed to connect to Kvrocks: %v", err)
		}
		defer kvrocksStore.Close()
		log.Printf("Kvrocks enrichment reads enabled")
	}

	if kvrocksStore == nil && *pg != "" {
		log.Printf("Connecting to PostgreSQL: %s", *pg)
	}
	enrichmentReader, err := crud.NewEnrichmentReader(*pg, 100, 0, kvrocksStore)
	if err != nil {
		log.Printf("Failed to connect to PostgreSQL: %v", err)
		log.Printf("AddressBook and Metadata will not be available")
		enrichmentReader = nil
	} else if kvrocksStore != nil {
		log.Printf("Using Kvrocks for AddressBook and Metadata")
	} else if *pg != "" {
		log.Printf("Connected to PostgreSQL successfully")
	} else {
		log.Printf("Neither Kvrocks nor PostgreSQL enrichment backend is configured")
		log.Printf("AddressBook and Metadata will not be available")
	}

	streamingv2.InitConfig(streamingv2.Config{
		EnrichmentReader: enrichmentReader,
		Testnet:          *testnet,
		ImgProxyBaseURL:  *imgProxyBaseUrl,
	})

	v2Manager := streamingv2.NewClientManager()
	go v2Manager.Run()

	go streamingv2.SubscribeToTransactionHints(ctx, rdb, v2Manager, *transactionHintsChannel)
	go streamingv2.SubscribeToClassifiedTraces(ctx, rdb, v2Manager, *classifiedTracesChannel)
	go streamingv2.SubscribeToAccountStateHints(ctx, rdb, v2Manager, *accountStateHintsChannel)
	go streamingv2.SubscribeToInvalidatedTraces(ctx, rdb, v2Manager, "invalidated_traces")

	app := fiber.New(fiber.Config{
		AppName:     "TON Streaming API",
		Prefork:     *prefork,
		ReadTimeout: 5 * time.Second,
		ProxyHeader: fiber.HeaderXForwardedFor,
	})

	app.Use(logger.New())

	api := app.Group("/api/streaming")

	api.Get("/healthz", healthzHandler(rdb))

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
