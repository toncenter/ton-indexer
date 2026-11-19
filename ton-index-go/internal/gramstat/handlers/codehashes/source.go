// Package codehashes collects current indexed account counts without reading
// account payloads, executing getters, or classifying contract implementations.
package codehashes

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	SourceID      = "mainnet-kvrocks-code-hashes-v1"
	Alphabet      = "+/0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	indexPrefix   = "ton-index:v1:idx:latest_account_states:code_hash:"
	indexSuffix   = ":by_account"
	progressKey   = "ton-index:v1:progress:finalized_mc_seqno"
	PageSize      = 250
	maxHashes     = 5_000_000
	SourceTimeout = 90 * time.Second
)

// Bound each read-only script to a small metadata page. Returning only the hash
// and count avoids sending the common 57-byte index key wrapper for every row.
// Lua numbers must never silently round a cardinality above 2^53-1.
const pageScript = `
local r = redis.call('SCAN', ARGV[1], 'MATCH', ARGV[2], 'COUNT', 250)
local out = {}
for i,k in ipairs(r[2]) do
  local n = redis.call('ZCARD', k)
  if n > 9007199254740991 then
    return redis.error_reply('Cardinality exceeds exact Lua integer range')
  end
  out[i] = string.sub(k, tonumber(ARGV[3])+1, -12)..':'..string.format('%.0f', n)
end
return {r[1], table.concat(out, '\n')}
`

type Prefix struct {
	Name     string            `json:"prefix"`
	Counts   map[string]uint64 `json:"counts"`
	MCBefore uint32            `json:"mc_before"`
	MCAfter  uint32            `json:"mc_after"`
}

// Snapshot is an observation over a collection interval, NOT a state at one MC.
// Each prefix stays on one connection; different prefixes may observe different
// replicas behind the endpoint.
type Snapshot struct {
	StartedAt, FinishedAt time.Time
	Prefixes              []Prefix
	Pages, ZeroHashes     uint64
}

type pageConnection interface {
	page(context.Context, string, string) (string, string, error)
	progress(context.Context) (uint32, error)
	close()
}

type RedisSource struct {
	Addr, Username, Password string
	// Test seam: production always uses a connection that cannot reconnect.
	open func(context.Context) (pageConnection, error)
}

type redisConnection struct {
	client *redis.Client
	conn   *redis.Conn
}

func (s RedisSource) connect(ctx context.Context) (pageConnection, error) {
	if s.open != nil {
		return s.open(ctx)
	}
	if _, _, err := net.SplitHostPort(s.Addr); err != nil {
		return nil, fmt.Errorf("GRAMSTAT_KVROCKS_ADDR must be host:port")
	}
	var dialed atomic.Bool
	dialer := net.Dialer{Timeout: 5 * time.Second}
	client := redis.NewClient(&redis.Options{
		Addr: s.Addr, Username: s.Username, Password: s.Password,
		Protocol: 2, MaxRetries: -1, PoolSize: 1,
		DialTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second,
		ConnMaxIdleTime: -1, ContextTimeoutEnabled: true, DisableIndentity: true,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if dialed.Swap(true) {
				return nil, fmt.Errorf("refusing to reconnect a replica-local SCAN cursor")
			}
			return dialer.DialContext(ctx, network, addr)
		},
	})
	return &redisConnection{client: client, conn: client.Conn()}, nil
}

func (c *redisConnection) close() {
	_ = c.conn.Close()
	_ = c.client.Close()
}

func (c *redisConnection) progress(ctx context.Context) (uint32, error) {
	value, err := c.conn.Get(ctx, progressKey).Result()
	if err != nil {
		return 0, fmt.Errorf("read Kvrocks progress: %w", err)
	}
	n, err := strconv.ParseUint(value, 10, 32)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("invalid Kvrocks progress watermark")
	}
	return uint32(n), nil
}

func (c *redisConnection) page(ctx context.Context, cursor, prefix string) (string, string, error) {
	values, err := c.conn.EvalRO(ctx, pageScript, nil, cursor, indexPrefix+prefix+"*", len(indexPrefix)).Slice()
	if err != nil {
		return "", "", fmt.Errorf("read code-hash index page: %w", err)
	}
	if len(values) != 2 {
		return "", "", fmt.Errorf("invalid code-hash page shape")
	}
	next, ok1 := values[0].(string)
	body, ok2 := values[1].(string)
	if !ok1 || !ok2 || len(next) == 0 || len(next) > 512 || len(body) > PageSize*66 {
		return "", "", fmt.Errorf("invalid code-hash page response")
	}
	return next, body, nil
}

func (s RedisSource) Collect(ctx context.Context) (*Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, SourceTimeout)
	defer cancel()
	snapshot := &Snapshot{StartedAt: time.Now().UTC()}
	hashes := 0
	for _, char := range Alphabet {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := s.connect(ctx)
		if err != nil {
			return nil, err
		}
		prefix, pages, zeros, err := collectPrefix(ctx, conn, string(char), maxHashes-hashes)
		conn.close()
		if err != nil {
			// A failed prefix invalidates the WHOLE run. Never resume its cursor
			// on a replacement connection or publish the other 63 prefixes.
			return nil, fmt.Errorf("code-hash prefix %q: %w", string(char), err)
		}
		hashes += len(prefix.Counts)
		snapshot.Prefixes = append(snapshot.Prefixes, prefix)
		snapshot.Pages += pages
		snapshot.ZeroHashes += zeros
	}
	snapshot.FinishedAt = time.Now().UTC()
	if _, err := snapshot.summary(); err != nil {
		return nil, err
	}
	return snapshot, ctx.Err()
}

func collectPrefix(ctx context.Context, conn pageConnection, name string, budget int) (Prefix, uint64, uint64, error) {
	p := Prefix{Name: name, Counts: make(map[string]uint64)}
	var pages, zeros uint64
	var err error
	p.MCBefore, err = conn.progress(ctx)
	if err != nil {
		return p, pages, zeros, err
	}
	cursor, previous := "0", ""
	cursors := map[string]bool{}
	scanned := 0
	for {
		if err := ctx.Err(); err != nil {
			return p, pages, zeros, err
		}
		next, body, err := conn.page(ctx, cursor, name)
		if err != nil {
			return p, pages, zeros, err
		}
		pages++
		if next == "" || len(next) > 512 || len(body) > PageSize*66 {
			return p, pages, zeros, fmt.Errorf("invalid page")
		}
		lines := []string(nil)
		if body != "" {
			lines = strings.Split(body, "\n")
		}
		if len(lines) > PageSize || (len(lines) == 0 && next != "0") {
			return p, pages, zeros, fmt.Errorf("oversized or non-advancing page")
		}
		for _, line := range lines {
			hash, raw, ok := strings.Cut(line, ":")
			decoded, e := base64.StdEncoding.Strict().DecodeString(hash)
			if !ok || e != nil || len(hash) != 44 || len(decoded) != 32 || !strings.HasPrefix(hash, name) || hash <= previous {
				return p, pages, zeros, fmt.Errorf("invalid, repeated, or out-of-order code hash")
			}
			if raw == "" || strings.Trim(raw, "0123456789") != "" {
				return p, pages, zeros, fmt.Errorf("invalid code-hash cardinality")
			}
			count, e := strconv.ParseUint(raw, 10, 64)
			if e != nil || count > 1<<53-1 {
				return p, pages, zeros, fmt.Errorf("inexact code-hash cardinality")
			}
			previous = hash
			scanned++
			if scanned > budget {
				return p, pages, zeros, fmt.Errorf("code-hash scan exceeds budget")
			}
			if count == 0 {
				// A key can disappear during a live scan; do not invent an
				// address or retain an empty family in the distribution.
				zeros++
			} else {
				p.Counts[hash] = count
			}
		}
		if next == "0" {
			break
		}
		if cursors[next] || next == cursor {
			return p, pages, zeros, fmt.Errorf("repeated SCAN cursor")
		}
		cursors[next] = true
		cursor = next
	}
	p.MCAfter, err = conn.progress(ctx)
	if err == nil && (p.MCBefore == 0 || p.MCAfter < p.MCBefore) {
		err = fmt.Errorf("Kvrocks progress regressed")
	}
	return p, pages, zeros, err
}

type totals struct {
	Hashes, Accounts uint64
	MCMin, MCMax     uint32
}

func (s *Snapshot) summary() (totals, error) {
	t := totals{MCMin: math.MaxUint32}
	if s == nil || len(s.Prefixes) != len(Alphabet) || s.StartedAt.IsZero() || s.FinishedAt.Before(s.StartedAt) {
		return t, fmt.Errorf("incomplete code-hash snapshot")
	}
	for i, p := range s.Prefixes {
		if p.Name != Alphabet[i:i+1] || p.MCBefore == 0 || p.MCAfter < p.MCBefore {
			return t, fmt.Errorf("invalid code-hash prefix coverage")
		}
		t.MCMin, t.MCMax = min(t.MCMin, p.MCBefore), max(t.MCMax, p.MCAfter)
		for hash, n := range p.Counts {
			b, err := base64.StdEncoding.Strict().DecodeString(hash)
			if err != nil || len(hash) != 44 || len(b) != 32 || hash[:1] != p.Name || n == 0 || n > math.MaxUint64-t.Accounts {
				return t, fmt.Errorf("invalid code-hash snapshot entry")
			}
			t.Hashes++
			t.Accounts += n
		}
	}
	if t.Hashes == 0 || t.Hashes > maxHashes {
		return t, fmt.Errorf("empty or oversized code-hash snapshot")
	}
	return t, nil
}
