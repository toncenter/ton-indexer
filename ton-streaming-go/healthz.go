package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

const (
	healthKeyTraceEmulator   = "health:ton-trace-emulator"
	healthKeyEventClassifier = "health:event-classifier"
	healthKeyTTLTracker      = "health:ton-trace-ttl-tracker"

	finalizedMaxAge    = 15 * time.Second
	confirmedMaxAge    = 15 * time.Second
	classifierMaxAge   = 15 * time.Second
	ttlTrackerMaxAge   = 15 * time.Second
	healthRedisTimeout = 2 * time.Second
)

type componentHealth struct {
	OK                       bool   `json:"ok"`
	Error                    string `json:"error,omitempty"`
	AgeSeconds               *int64 `json:"age_seconds,omitempty"`
	LastHeartbeat            *int64 `json:"last_heartbeat,omitempty"`
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

func applyHeartbeat(status *componentHealth, values map[string]string, now int64, maxAge time.Duration) {
	if values == nil {
		return
	}
	lastHeartbeat, err := parseInt64Field(values, "last_heartbeat")
	if err != nil {
		status.OK = false
		if status.Error == "" {
			status.Error = err.Error()
		}
		return
	}
	status.LastHeartbeat = &lastHeartbeat
	age := now - lastHeartbeat
	status.AgeSeconds = &age
	if time.Duration(age)*time.Second > maxAge {
		status.OK = false
		if status.Error == "" {
			status.Error = "heartbeat is too old"
		}
	}
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

		pipe := rdb.Pipeline()
		emulatorCmd := pipe.HGetAll(ctx, healthKeyTraceEmulator)
		classifierCmd := pipe.HGetAll(ctx, healthKeyEventClassifier)
		ttlTrackerCmd := pipe.HGetAll(ctx, healthKeyTTLTracker)
		_, _ = pipe.Exec(ctx)

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

		// indexer/event-classifier
		classifierStatus, classifierValues := statusFromCmd(classifierCmd)
		applyHeartbeat(&classifierStatus, classifierValues, now, classifierMaxAge)
		response.OK = response.OK && classifierStatus.OK
		response.Components["event-classifier"] = classifierStatus

		// ton-trace-ttl-tracker
		ttlTrackerStatus, ttlTrackerValues := statusFromCmd(ttlTrackerCmd)
		applyHeartbeat(&ttlTrackerStatus, ttlTrackerValues, now, ttlTrackerMaxAge)
		response.OK = response.OK && ttlTrackerStatus.OK
		response.Components["ton-trace-ttl-tracker"] = ttlTrackerStatus

		if response.OK {
			return c.Status(fiber.StatusOK).JSON(response)
		}
		return c.Status(fiber.StatusServiceUnavailable).JSON(response)
	}
}
