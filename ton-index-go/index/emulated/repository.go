package emulated

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"strings"
)

type EmulatedTracesRepository struct {
	Rdb *redis.Client
}

func NewRepository(dsn string) (*EmulatedTracesRepository, error) {
	options, err := redis.ParseURL(dsn)
	if err != nil {
		return nil, err
	}
	return &EmulatedTracesRepository{Rdb: redis.NewClient(options)}, nil
}

func (receiver *EmulatedTracesRepository) LoadRawTraces(trace_ids []string) (map[string]map[string]string, error) {
	result := make(map[string]map[string]string)
	for _, trace_id := range trace_ids {
		trace_query := receiver.Rdb.HGetAll(context.Background(), trace_id)
		trace_query_result, err := trace_query.Result()
		if err != nil {
			return nil, err
		}
		result[trace_id] = trace_query_result
	}
	return result, nil
}

func (receiver *EmulatedTracesRepository) GetTraceIdsByAccount(account string) ([]string, error) {
	// read hset from redis into keys array
	scores := receiver.Rdb.ZRevRangeWithScores(context.Background(), account, 0, -1)
	result, err := scores.Result()
	var trace_ids []string
	if err != nil {
		return nil, err
	}
	for _, z := range result {
		key := z.Member.(string)
		// split key by :
		split := strings.Split(key, ":")
		if len(split) != 2 {
			return nil, errors.New("Invalid key format")
		}
		trace_id := split[0]
		keys_query := receiver.Rdb.Keys(context.Background(), trace_id)
		key_query_result, err := keys_query.Result()
		if err != nil {
			return nil, err
		}
		if len(key_query_result) == 0 {
			return nil, nil
		}
		trace_ids = append(trace_ids, trace_id)
	}
	return trace_ids, nil
}

func (receiver *EmulatedTracesRepository) GetActionIdsByAccount(account string) (map[string][]string, error) {
	scores := receiver.Rdb.ZRevRangeWithScores(context.Background(), "_aai:"+account, 0, -1)
	result, err := scores.Result()
	actions := make(map[string][]string)
	if err != nil {
		return nil, err
	}
	for _, z := range result {
		key := z.Member.(string)
		// split key by :
		split := strings.Split(key, ":")
		if len(split) != 2 {
			return nil, errors.New("Invalid key format")
		}
		trace_id := split[0]
		action_id := split[1]
		keys_query := receiver.Rdb.Keys(context.Background(), trace_id)
		key_query_result, err := keys_query.Result()
		if err != nil {
			return nil, err
		}
		if len(key_query_result) == 0 {
			return nil, nil
		}
		if _, ok := actions[trace_id]; !ok {
			actions[trace_id] = make([]string, 0)
		}
		actions[trace_id] = append(actions[trace_id], action_id)
	}
	return actions, nil
}
