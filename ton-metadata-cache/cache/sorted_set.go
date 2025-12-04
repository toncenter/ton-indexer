package cache

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// SortedSetCache provides sorted set operations backed by Redis.
// Useful for maintaining ordered collections where you need to efficiently
// find min/max elements.
type SortedSetCache struct {
	client *redis.Client
	prefix string
}

// NewSortedSetCache creates a new sorted set cache with the given prefix.
func NewSortedSetCache(client *redis.Client, prefix string) *SortedSetCache {
	return &SortedSetCache{
		client: client,
		prefix: prefix,
	}
}

func (c *SortedSetCache) key(k string) string {
	if c.prefix == "" {
		return k
	}
	return c.prefix + ":" + k
}

// Add adds a member with the given score to the sorted set.
// If the member already exists, its score is updated.
func (c *SortedSetCache) Add(ctx context.Context, key string, score float64, member string) error {
	return c.client.ZAdd(ctx, c.key(key), redis.Z{
		Score:  score,
		Member: member,
	}).Err()
}

// Remove removes a member from the sorted set.
func (c *SortedSetCache) Remove(ctx context.Context, key string, member string) error {
	return c.client.ZRem(ctx, c.key(key), member).Err()
}

// GetLowest returns the member with the lowest score.
// Returns ErrNotFound if the set is empty.
func (c *SortedSetCache) GetLowest(ctx context.Context, key string) (string, error) {
	results, err := c.client.ZRange(ctx, c.key(key), 0, 0).Result()
	if err != nil {
		return "", err
	}
	if len(results) == 0 {
		return "", ErrNotFound
	}
	return results[0], nil
}

// GetAll returns all members with their scores, ordered by score ascending.
func (c *SortedSetCache) GetAll(ctx context.Context, key string) ([]redis.Z, error) {
	return c.client.ZRangeWithScores(ctx, c.key(key), 0, -1).Result()
}

// Delete removes the entire sorted set.
func (c *SortedSetCache) Delete(ctx context.Context, key string) error {
	return c.client.Del(ctx, c.key(key)).Err()
}

// Count returns the number of members in the sorted set.
func (c *SortedSetCache) Count(ctx context.Context, key string) (int64, error) {
	return c.client.ZCard(ctx, c.key(key)).Result()
}

// AddBatch adds multiple members to a sorted set using pipelining.
// items is a slice of (score, member) pairs.
func (c *SortedSetCache) AddBatch(ctx context.Context, key string, items []redis.Z) error {
	if len(items) == 0 {
		return nil
	}
	return c.client.ZAdd(ctx, c.key(key), items...).Err()
}

// GetLowestMulti returns the member with the lowest score for each key.
// Uses pipelining for efficiency. Returns a map of key to member.
func (c *SortedSetCache) GetLowestMulti(ctx context.Context, keys ...string) (map[string]string, error) {
	if len(keys) == 0 {
		return make(map[string]string), nil
	}

	pipe := c.client.Pipeline()
	cmds := make([]*redis.StringSliceCmd, len(keys))

	for i, k := range keys {
		cmds[i] = pipe.ZRange(ctx, c.key(k), 0, 0)
	}

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	result := make(map[string]string)
	for i, cmd := range cmds {
		members, err := cmd.Result()
		if err != nil || len(members) == 0 {
			continue
		}
		result[keys[i]] = members[0]
	}

	return result, nil
}
