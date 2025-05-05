package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/swagger"
	"github.com/toncenter/ton-indexer/ton-index-go/index"

	"github.com/go-redis/redis/v8"

	"github.com/vmihailenco/msgpack/v5"

	_ "github.com/toncenter/ton-indexer/ton-emulate-go/docs"
	"github.com/toncenter/ton-indexer/ton-emulate-go/models"
)

type TraceTask struct {
	ID               string  `msgpack:"id"`
	BOC              string  `msgpack:"boc"`
	IgnoreChksig     bool    `msgpack:"ignore_chksig"`
	DetectInterfaces bool    `msgpack:"detect_interfaces"`
	IncludeCodeData  bool    `msgpack:"include_code_data"`
	McBlockSeqno     *uint32 `msgpack:"mc_block_seqno"`
}

type EmulateRequest struct {
	Boc                string  `json:"boc" example:"te6ccgEBAQEAAgAAAA=="`
	IgnoreChksig       bool    `json:"ignore_chksig" example:"false"`
	WithActions        bool    `json:"with_actions" example:"false"`
	IncludeCodeData    bool    `json:"include_code_data" example:"false"`
	IncludeAddressBook bool    `json:"include_address_book" example:"false"`
	IncludeMetadata    bool    `json:"include_metadata" example:"false"`
	McBlockSeqno       *uint32 `json:"mc_block_seqno" example:"null"`
}

// validate function for EmulateRequest
func (req EmulateRequest) Validate() error {
	if req.Boc == "" {
		return fmt.Errorf("boc is required")
	}
	_, err := base64.StdEncoding.Strict().DecodeString(req.Boc)
	if err != nil {
		return fmt.Errorf("invalid boc: %v", err)
	}
	if pool == nil && (req.IncludeAddressBook || req.IncludeMetadata) {
		return fmt.Errorf("address book and metadata are not available")
	}

	return err
}

// Command-line flags
var (
	redisAddr         = flag.String("redis", "localhost:6379", "Redis server dsn")
	emulatorQueueName = flag.String("emulator-queue", "emulatorqueue", "Redis queue name")
	classifierChannel = flag.String("classifier-channel", "classifierchannel", "Redis queue name")
	pg                = flag.String("pg", "", "PostgreSQL connection string")
	imgProxyBaseUrl   = flag.String("imgproxy-baseurl", "", "Image proxy base URL")
	serverPort        = flag.Int("port", 8080, "Server port")
	prefork           = flag.Bool("prefork", false, "Use prefork")
	testnet           = flag.Bool("testnet", false, "Use testnet")
)

var pool *index.DbClient

func generateTaskID() string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 10)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// @title TON Emulate API
// @version 0.0.1
// @description	TON Emulate API provides an endpoint to emulate transactions and traces before committing them to the blockchain.
// @basePath /api/emulate/

// EmulateTrace godoc
// @Summary Emulate trace by external message
// @Schemes
// @Description Emulate trace by external message.
// @Tags emulate
// @Accept json
// @Produce json
// @Param   request     body    EmulateRequest     true        "External Message Request"
// @Param	X-Actions-Version	header	string	false	"Supported actions version"
// @Router /v1/emulateTrace [post]
func emulateTrace(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var req EmulateRequest
	if err := c.BodyParser(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request: "+err.Error())
	}
	if err := req.Validate(); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request: "+err.Error())
	}

	var supportedActionTypes []string
	if valueStr, ok := ExtractHeader(c, "X-Actions-Version"); ok {
		supportedActionTypes = []string{valueStr}
	}
	supportedActionTypes = index.ExpandActionTypeShortcuts(supportedActionTypes)

	taskID := generateTaskID()
	task := TraceTask{
		ID:               taskID,
		BOC:              req.Boc,
		IgnoreChksig:     req.IgnoreChksig,
		DetectInterfaces: req.WithActions,
		IncludeCodeData:  req.IncludeCodeData,
		McBlockSeqno:     req.McBlockSeqno,
	}

	// Serialize the task using msgpack
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseArrayEncodedStructs(false)

	if err := enc.Encode(task); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "failed to serialize task: "+err.Error())
	}

	// Initialize Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: *redisAddr, // Redis server address
	})

	// Push the packed task to the Redis queue
	if err := rdb.LPush(ctx, *emulatorQueueName, buf.Bytes()).Err(); err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, "failed to push task to emulator queue: "+err.Error())
	}

	// Subscribe to the result channel
	pubsub := rdb.Subscribe(ctx, "emulator_channel_"+taskID)
	defer pubsub.Close()

	// Wait for the result
	msg, err := pubsub.ReceiveMessage(ctx)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, "failed to receive result from emulator channel: "+err.Error())
	}

	if msg.Payload == "error" {
		error_msg, err := rdb.Get(ctx, "emulator_error_"+taskID).Result()
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to receive error from emulator error: "+err.Error())
		}
		return fiber.NewError(fiber.StatusInternalServerError, error_msg)
	}
	if msg.Payload != "success" {
		return fiber.NewError(fiber.StatusInternalServerError, "unexpected message from emulator channel: "+msg.Payload)
	}

	if req.WithActions {
		// publish task id to classifier channel
		if err := rdb.Publish(ctx, *classifierChannel, taskID).Err(); err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to publish task id to classifier queue: "+err.Error())
		}
		// wait for the notification in channel classifier_result_channel_taskID
		pubsub := rdb.Subscribe(ctx, "classifier_result_channel_"+taskID)
		defer pubsub.Close()
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to receive result from classifier channel: "+err.Error())
		}
		if msg.Payload == "error" {
			error_msg, err := rdb.Get(ctx, "classifier_error_"+taskID).Result()
			if err != nil {
				return fiber.NewError(fiber.StatusInternalServerError, "failed to receive error from classifier error: "+err.Error())
			}
			return fiber.NewError(fiber.StatusInternalServerError, error_msg)
		}
		if msg.Payload != "success" {
			return fiber.NewError(fiber.StatusInternalServerError, "unexpected message from classifier result channel: "+msg.Payload)
		}
	}

	hset, err := rdb.HGetAll(ctx, "result_"+taskID).Result()
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, "failed to get result from Redis: "+err.Error())
	}

	result, err := models.TransformToAPIResponse(hset, pool, *testnet, req.IncludeAddressBook, req.IncludeMetadata,
		supportedActionTypes)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, "failed to transform result: "+err.Error())
	}
	if *imgProxyBaseUrl != "" && result.Metadata != nil {
		index.SubstituteImgproxyBaseUrl(result.Metadata, *imgProxyBaseUrl)
	}

	return c.Status(200).JSON(result)
}

func ExtractHeader(ctx *fiber.Ctx, header string) (string, bool) {
	if val := ctx.GetReqHeaders()[header]; len(val) > 0 {
		return val[0], true
	}
	return ``, false
}

func main() {
	flag.Parse()

	var err error
	if *pg == "" {
		log.Print("PostgreSQL connection string is not provided")
		log.Print("AddressBook and Metadata will not be available")
	} else {
		log.Print("PostgreSQL connection string: ", *pg)
		pool, err = index.NewDbClient(*pg, 100, 0)
		if err != nil {
			log.Print("failed to connect to PostgreSQL: ", err)
			log.Print("AddressBook and Metadata will not be available")
		}
	}

	config := fiber.Config{
		AppName:        "TON Index API",
		Concurrency:    256 * 1024,
		Prefork:        *prefork,
		ReadBufferSize: 1048576,
	}
	app := fiber.New(config)

	app.Use(func(c *fiber.Ctx) error {
		err := c.Next()
		if err != nil {
			// Log the error internally here if necessary

			// Return a JSON response with the error
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}

			return c.Status(code).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
		return nil
	})

	app.Use("/api/emulate/", func(c *fiber.Ctx) error {
		c.Accepts("application/json")
		start := time.Now()
		err := c.Next()
		stop := time.Now()
		c.Append("Server-timing", fmt.Sprintf("app;dur=%v", stop.Sub(start).String()))
		return err
	})

	app.Post("/api/emulate/v1/emulateTrace", emulateTrace)

	var swagger_config = swagger.Config{
		Title:           "TON Emulate API - Swagger UI",
		Layout:          "BaseLayout",
		DeepLinking:     true,
		TryItOutEnabled: true,
	}
	app.Get("/api/emulate/*", swagger.New(swagger_config))
	bind := fmt.Sprintf(":%d", *serverPort)
	err = app.Listen(bind)
	log.Fatal(err)
}
