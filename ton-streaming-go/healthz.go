package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

const (
	healthKeyTraceEmulator = "health:ton-trace-emulator"

	finalizedMaxAge    = 15 * time.Second
	confirmedMaxAge    = 15 * time.Second
	healthRedisTimeout = 2 * time.Second
)

type componentHealth struct {
	OK                       bool   `json:"ok"`
	Error                    string `json:"error,omitempty"`
	LastFinalizedMcBlockTime *int64 `json:"last_finalized_mc_block_time,omitempty"`
	FinalizedAgeSeconds      *int64 `json:"finalized_age_seconds,omitempty"`
	LastConfirmedBlockTime   *int64 `json:"last_confirmed_block_time,omitempty"`
	ConfirmedAgeSeconds      *int64 `json:"confirmed_age_seconds,omitempty"`
}

type healthzResponse struct {
	OK         bool                       `json:"ok"`
	Now        int64                      `json:"now"`
	Components map[string]componentHealth `json:"components"`
}

func parseInt64Field(values map[string]string, field string) (int64, error) {
	val, ok := values[field]
	if !ok || val == "" {
		return 0, fmt.Errorf("missing field %q", field)
	}
	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value %q", field, val)
	}
	return parsed, nil
}

func statusFromCmd(cmd *redis.MapStringStringCmd) (componentHealth, map[string]string) {
	status := componentHealth{OK: true}
	if err := cmd.Err(); err != nil {
		status.OK = false
		status.Error = err.Error()
		return status, nil
	}
	values := cmd.Val()
	if len(values) == 0 {
		status.OK = false
		status.Error = "missing health data"
		return status, nil
	}
	return status, values
}

func applyEmulatorStatus(status *componentHealth, values map[string]string, now int64) {
	if values == nil {
		return
	}
	finalized, err := parseInt64Field(values, "finalized_mc_block_time")
	if err != nil {
		status.OK = false
		status.Error = err.Error()
	} else {
		status.LastFinalizedMcBlockTime = &finalized
		finalizedAge := now - finalized
		status.FinalizedAgeSeconds = &finalizedAge
		if time.Duration(finalizedAge)*time.Second > finalizedMaxAge {
			status.OK = false
			status.Error = "finalized masterchain block is too old"
		}
	}

	confirmed, err := parseInt64Field(values, "confirmed_block_time")
	if err != nil {
		status.OK = false
		if status.Error == "" {
			status.Error = err.Error()
		}
	} else {
		status.LastConfirmedBlockTime = &confirmed
		confirmedAge := now - confirmed
		status.ConfirmedAgeSeconds = &confirmedAge
		if time.Duration(confirmedAge)*time.Second > confirmedMaxAge {
			status.OK = false
			if status.Error == "" {
				status.Error = "confirmed block is too old"
			}
		}
	}
}

func healthzHandler(rdb *redis.Client) fiber.Handler {
	return func(c *fiber.Ctx) error {
		ctx, cancel := context.WithTimeout(context.Background(), healthRedisTimeout)
		defer cancel()

		emulatorCmd := rdb.HGetAll(ctx, healthKeyTraceEmulator)

		now := time.Now().Unix()
		response := healthzResponse{
			OK:         true,
			Now:        now,
			Components: make(map[string]componentHealth),
		}

		// ton-trace-emulator
		emulatorStatus, emulatorValues := statusFromCmd(emulatorCmd)
		applyEmulatorStatus(&emulatorStatus, emulatorValues, now)
		response.OK = response.OK && emulatorStatus.OK
		response.Components["ton-trace-emulator"] = emulatorStatus

		if response.OK {
			return c.Status(fiber.StatusOK).JSON(response)
		}
		if payload, err := json.Marshal(response); err != nil {
			log.Printf("healthz returned 503; failed to marshal response for logging: %v", err)
		} else {
			log.Printf("healthz returned 503: %s", payload)
		}
		return c.Status(fiber.StatusServiceUnavailable).JSON(response)
	}
}
