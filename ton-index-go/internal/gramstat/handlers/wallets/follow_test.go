package wallets

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

type followStore struct {
	answers []string
	queries []string
}

type followQuery func(context.Context, string, url.Values) ([]byte, error)

func (f followQuery) Query(c context.Context, q string, p url.Values) ([]byte, error) {
	return f(c, q, p)
}

func TestFollowingStopsBeforeUnfinishedBatch(t *testing.T) {
	for _, hole := range []uint32{11, 15} {
		t.Run(fmt.Sprint(hole), func(t *testing.T) {
			inserts := 0
			store := followQuery(func(_ context.Context, q string, p url.Values) ([]byte, error) {
				switch {
				case strings.Contains(q, "max(mc_seqno)"):
					return []byte(`{"mc":1020}`), nil
				case strings.Contains(q, "AS deltas"):
					mc, _ := strconv.ParseUint(p.Get("param_end"), 10, 32)
					n := uint32(mc) - 2
					if uint32(mc) >= hole {
						n--
					}
					return []byte(fmt.Sprintf(`{"deltas":%d,"blocks":%d}`, n, uint32(mc)-2)), nil
				case strings.Contains(q, "AS expected"):
					return []byte(`{"expected":3,"found":3,"before":1729036800}`), nil
				case strings.Contains(q, "INSERT INTO"):
					inserts++
					return nil, nil
				}
				return nil, fmt.Errorf("unexpected query")
			})
			h := &Handler{CH: store}
			s := source{First: 3, End: 10, Before: 1728864000, Verified: 1}
			if err := h.advanceNative(context.Background(), url.Values{}, &s); err != nil {
				t.Fatal(err)
			}
			if s.End != hole-1 || (hole == 11 && inserts != 0) || (hole == 15 && inserts != 1) {
				t.Fatal(s.End, inserts)
			}
		})
	}
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
		{"regressed prefix", []string{`{"mc":1020}`, `{"deltas":17,"blocks":18}`, `{"deltas":7,"blocks":8}`}, false},
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
