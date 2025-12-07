package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrNotFound     = errors.New("key not found")
	ErrEncodeFailed = errors.New("failed to encode value")
	ErrDecodeFailed = errors.New("failed to decode value")
)

// Encoder converts a value of type T to a byte slice for storage in Redis.
type Encoder[T any] func(value T) ([]byte, error)

// Decoder converts a byte slice from Redis back to a value of type T.
type Decoder[T any] func(data []byte) (T, error)

// Cache is a generic cache backed by Redis.
type Cache[T any] struct {
	client  *redis.Client
	encoder Encoder[T]
	decoder Decoder[T]
	prefix  string
}

// Options contains configuration options for creating a new Cache.
type Options[T any] struct {
	Client  *redis.Client
	Encoder Encoder[T]
	Decoder Decoder[T]
	Prefix  string
}

// New creates a new generic Cache instance.
func New[T any](opts Options[T]) *Cache[T] {
	return &Cache[T]{
		client:  opts.Client,
		encoder: opts.Encoder,
		decoder: opts.Decoder,
		prefix:  opts.Prefix,
	}
}

// key returns the full Redis key with prefix applied.
func (c *Cache[T]) key(k string) string {
	if c.prefix == "" {
		return k
	}
	return c.prefix + ":" + k
}

// Set stores a value in the cache with the given key and TTL.
// Use ttl=0 for no expiration.
func (c *Cache[T]) Set(ctx context.Context, key string, value T, ttl time.Duration) error {
	data, err := c.encoder(value)
	if err != nil {
		return errors.Join(ErrEncodeFailed, err)
	}
	return c.client.Set(ctx, c.key(key), data, ttl).Err()
}

// Get retrieves a value from the cache by key.
// Returns ErrNotFound if the key does not exist.
func (c *Cache[T]) Get(ctx context.Context, key string) (T, error) {
	var zero T

	data, err := c.client.Get(ctx, c.key(key)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return zero, ErrNotFound
		}
		return zero, err
	}

	value, err := c.decoder(data)
	if err != nil {
		return zero, errors.Join(ErrDecodeFailed, err)
	}

	return value, nil
}

// GetEx retrieves a value and extends its TTL.
// Useful for keeping frequently accessed keys alive longer.
// Returns ErrNotFound if the key does not exist.
func (c *Cache[T]) GetEx(ctx context.Context, key string, ttl time.Duration) (T, error) {
	var zero T

	data, err := c.client.GetEx(ctx, c.key(key), ttl).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return zero, ErrNotFound
		}
		return zero, err
	}

	value, err := c.decoder(data)
	if err != nil {
		return zero, errors.Join(ErrDecodeFailed, err)
	}

	return value, nil
}

// Delete removes a key from the cache.
func (c *Cache[T]) Delete(ctx context.Context, key string) error {
	return c.client.Del(ctx, c.key(key)).Err()
}

// MGet retrieves multiple values from the cache.
// Returns a map of key to value for keys that exist.
func (c *Cache[T]) MGet(ctx context.Context, keys ...string) (map[string]T, error) {
	if len(keys) == 0 {
		return make(map[string]T), nil
	}

	fullKeys := make([]string, len(keys))
	for i, k := range keys {
		fullKeys[i] = c.key(k)
	}

	results, err := c.client.MGet(ctx, fullKeys...).Result()
	if err != nil {
		return nil, err
	}

	values := make(map[string]T)
	for i, result := range results {
		if result == nil {
			continue
		}

		var data []byte
		switch v := result.(type) {
		case string:
			data = []byte(v)
		case []byte:
			data = v
		default:
			continue
		}

		value, err := c.decoder(data)
		if err != nil {
			continue
		}
		values[keys[i]] = value
	}

	return values, nil
}

// MGetEx retrieves multiple values and extends their TTL.
// Uses pipelining for efficiency. Returns a map of key to value for keys that exist.
func (c *Cache[T]) MGetEx(ctx context.Context, ttl time.Duration, keys ...string) (map[string]T, error) {
	if len(keys) == 0 {
		return make(map[string]T), nil
	}

	pipe := c.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))

	for i, k := range keys {
		cmds[i] = pipe.GetEx(ctx, c.key(k), ttl)
	}

	_, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	values := make(map[string]T)
	for i, cmd := range cmds {
		data, err := cmd.Bytes()
		if err != nil {
			continue // Key doesn't exist or other error
		}

		value, err := c.decoder(data)
		if err != nil {
			continue
		}
		values[keys[i]] = value
	}

	return values, nil
}

// MSet stores multiple key-value pairs in the cache with the given TTL.
// Uses pipelining for efficiency.
func (c *Cache[T]) MSet(ctx context.Context, items map[string]T, ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}

	pipe := c.client.Pipeline()
	for k, v := range items {
		data, err := c.encoder(v)
		if err != nil {
			return errors.Join(ErrEncodeFailed, err)
		}
		pipe.Set(ctx, c.key(k), data, ttl)
	}

	_, err := pipe.Exec(ctx)
	return err
}
