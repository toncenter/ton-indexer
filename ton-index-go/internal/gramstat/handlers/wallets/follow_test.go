package wallets

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
)

type followStore struct {
	answers []string
	queries []string
}

func (s *followStore) Query(_ context.Context, q string, _ url.Values) ([]byte, error) {
	s.queries = append(s.queries, q)
	if len(s.answers) == 0 {
		return nil, fmt.Errorf("unexpected query")
	}
	v := s.answers[0]
	s.answers = s.answers[1:]
	return []byte(v), nil
}

func TestFollowNativeAdvancement(t *testing.T) {
	for _, tc := range []struct {
		name    string
		answers []string
		valid   bool
	}{
		{"complete", []string{`{"mc":1020}`, `{"deltas":18,"blocks":18}`, `{"expected":3,"found":3,"before":1729036800}`, ``}, true},
		{"gap", []string{`{"mc":1020}`, `{"deltas":17,"blocks":18}`}, false},
		{"missing shard", []string{`{"mc":1020}`, `{"deltas":18,"blocks":18}`, `{"expected":3,"found":2,"before":1729036800}`}, false},
		{"time regression", []string{`{"mc":1020}`, `{"deltas":18,"blocks":18}`, `{"expected":3,"found":3,"before":1}`}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &followStore{answers: append([]string{}, tc.answers...)}
			h := &Handler{CH: store}
			s := source{First: 3, End: 10, From: 1728777600, Before: 1728864000, Verified: 1}
			p := url.Values{}
			err := h.advanceNative(context.Background(), p, &s)
			if (err == nil) != tc.valid {
				t.Fatal(err)
			}
			if tc.valid && (s.End != 20 || s.Before != 1729036800) {
				t.Fatal(s)
			}
			if !tc.valid && s.End != 10 {
				t.Fatal("failed proof advanced source")
			}
			if len(store.answers) != 0 {
				b, _ := json.Marshal(store.answers)
				t.Fatal(string(b))
			}
		})
	}
}
