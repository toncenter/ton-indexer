package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ton-metadata-cache/cache"
	"ton-metadata-cache/loader"
	"ton-metadata-cache/models"
	"ton-metadata-cache/repl"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Config struct {
	PostgresDSN            string
	PostgresReplicationDSN string
	RedisURL               string
	ListenAddr             string
	IsTestnet              bool
}

func parseFlags() Config {
	cfg := Config{}

	flag.StringVar(&cfg.PostgresDSN, "pg", "", "PostgreSQL connection DSN")
	flag.StringVar(&cfg.PostgresReplicationDSN, "pg-repl", "", "PostgreSQL replication connection DSN")
	flag.StringVar(&cfg.RedisURL, "redis", "", "Redis connection URL")
	flag.StringVar(&cfg.ListenAddr, "listen", ":8000", "HTTP server listen address")
	flag.BoolVar(&cfg.IsTestnet, "testnet", false, "Enable testnet mode for address formatting")

	flag.Parse()

	return cfg
}

func main() {
	cfg := parseFlags()

	if cfg.PostgresDSN == "" {
		log.Fatal("PostgreSQL DSN is required. Use -pg flag")
	}
	if cfg.PostgresReplicationDSN == "" {
		log.Fatal("PostgreSQL replication DSN is required. Use -pg-repl flag")
	}
	if cfg.RedisURL == "" {
		log.Fatal("Redis connection string is required. Use -redis flag")
	}

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Redis client
	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Fatalf("Failed to parse Redis URL: %v", err)
	}
	redisClient := redis.NewClient(redisOpts)
	defer redisClient.Close()

	// Verify Redis connection
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Connected to Redis")

	// Initialize cache manager
	cacheManager := cache.NewManager(redisClient)

	//Initialize PostgreSQL connection pool with custom type registration
	poolConfig, err := pgxpool.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("Failed to parse PostgreSQL DSN: %v", err)
	}
	poolConfig.AfterConnect = loader.AfterConnectRegisterTypes

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("Failed to create PostgreSQL pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	//Initialize loader and preload caches in background
	ldr := loader.New(pool, cacheManager)
	go func() {
		log.Println("Starting background cache preload...")
		if err := ldr.LoadAll(ctx); err != nil {
			log.Printf("Warning: Failed to preload caches: %v", err)
		} else {
			log.Println("Background cache preload completed")
		}
	}()

	// Initialize Fiber app
	app := fiber.New()

	// POST /address_info - returns metadata and/or address book for given addresses
	app.Post("/address_info", func(c *fiber.Ctx) error {
		var req models.AddressInfoRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}

		if len(req.Addresses) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "addresses is required",
			})
		}

		response := models.AddressInfoResponse{}

		if req.IncludeMetadata {
			metadata, err := ldr.QueryMetadata(c.Context(), req.Addresses)
			if err != nil {
				log.Printf("Error querying metadata: %v", err)
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "Failed to query metadata",
				})
			}
			response.Metadata = metadata
		}

		if req.IncludeAddressBook {
			addressBook, err := ldr.QueryAddressBook(c.Context(), req.Addresses, cfg.IsTestnet)
			if err != nil {
				log.Printf("Error querying address book: %v", err)
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "Failed to query address book",
				})
			}
			response.AddressBook = addressBook
		}

		return c.JSON(response)
	})

	// Configure the replicator
	replCfg := repl.Config{
		ConnectionString: cfg.PostgresReplicationDSN,
		SlotName:         "metadata_cache_slot",
		PublicationName:  "metadata_cache_pub",
		Tables: []string{
			"public.contract_methods",
			"public.dns_entries",
			"public.latest_account_states",
			"public.address_metadata",
			"public.jetton_wallets",
			"public.nft_items",
			"public.nft_collections",
			"public.jetton_masters",
		},
		TemporarySlot: true,
	}

	// Create the replicator
	r, err := repl.NewReplicator(replCfg)

	if err != nil {
		log.Fatalf("Failed to create replicator: %v", err)
	}

	// Create event handler
	handler := NewHandler(cacheManager, pool)

	// Start consuming events in a goroutine
	go handler.HandleEvents(ctx, r.Events())

	// Start the replicator in a goroutine
	go func() {
		if err := r.Start(ctx); err != nil {
			log.Printf("Replicator error: %v", err)
			log.Fatal("Replicator error: ", err)
		}
	}()

	app.Get("/health", func(c *fiber.Ctx) error {
		ctx = context.Background()
		heatlh_err := pool.Ping(ctx)
		if heatlh_err != nil {
			c.Status(500)
			return heatlh_err
		}

		heatlh_err = redisClient.Ping(ctx).Err()
		if heatlh_err != nil {
			c.Status(500)
			return heatlh_err
		}

		if r.TimeSinceLastMsg() > 30*time.Second {
			c.Status(500)
			return errors.New("stale replication")
		}
		c.Status(200)
		return nil
	})

	// Handle shutdown signals
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		cancel()
		r.Close()
		app.Shutdown()
	}()

	log.Printf("Starting server on %s", cfg.ListenAddr)
	log.Fatal(app.Listen(cfg.ListenAddr))
}
