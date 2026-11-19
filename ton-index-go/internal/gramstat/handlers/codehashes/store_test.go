package codehashes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"testing"
)

type fakeStore struct {
	calls        []string
	failAt       string
	verification string
	id           string
}

func (f *fakeStore) InsertJSON(_ context.Context, table string, data []byte) error {
	f.calls = append(f.calls, table)
	if f.failAt == table {
		return errors.New("interrupted insert")
	}
	if table == "gramstat.account_code_hash_counts" {
		dec := json.NewDecoder(bytes.NewReader(data))
		var n int
		for {
			var row struct {
				ID string `json:"snapshot_id"`
				Prefix
			}
			err := dec.Decode(&row)
			if err == io.EOF {
				break
			}
			if err != nil || row.ID == "" || (f.id != "" && f.id != row.ID) || len(row.Counts) != 1 {
				return errors.New("bad staged rows")
			}
			f.id = row.ID
			n++
		}
		if n != 64 {
			return errors.New("incomplete stage")
		}
	} else {
		var row map[string]any
		if err := json.Unmarshal(data, &row); err != nil || row["snapshot_id"] != f.id ||
			row["indexed_accounts"] != float64(640) || row["consistency"] != "live-index-scan" {
			return errors.New("bad completion marker")
		}
	}
	return nil
}
func (f *fakeStore) Query(_ context.Context, _ string, p url.Values) ([]byte, error) {
	f.calls = append(f.calls, "verify")
	if p.Get("param_snapshot") != f.id || p.Get("param_source") != SourceID || p.Get("param_day") != "2026-11-15" {
		return nil, errors.New("verification did not capture staged identity")
	}
	if f.failAt == "verify" {
		return nil, errors.New("read failure")
	}
	return []byte(f.verification), nil
}

const correctVerification = `{"prefixes":64,"distinct_prefixes":64,"hashes":"64","accounts":"640","invalid_prefixes":0}`

func TestPublishOnlyAfterCompleteReadback(t *testing.T) {
	for _, tc := range []struct {
		name, failAt, verification string
		success                    bool
	}{
		{"complete", "", correctVerification, true},
		{"insert interrupted", "gramstat.account_code_hash_counts", correctVerification, false},
		{"read interrupted", "verify", "", false},
		{"missing prefix", "", `{"prefixes":63,"distinct_prefixes":63,"hashes":"63","accounts":"630"}`, false},
		{"duplicate prefix", "", `{"prefixes":64,"distinct_prefixes":63,"hashes":"64","accounts":"640"}`, false},
		{"wrong sum", "", `{"prefixes":64,"distinct_prefixes":64,"hashes":"64","accounts":"641"}`, false},
		{"malformed readback", "", `{`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeStore{failAt: tc.failAt, verification: tc.verification}
			err := publish(context.Background(), f, testSnapshot())
			if (err == nil) != tc.success {
				t.Fatal(err)
			}
			published := len(f.calls) > 0 && f.calls[len(f.calls)-1] == "gramstat.account_code_hash_snapshots"
			if published != tc.success {
				t.Fatal("incomplete generation reached completion table", f.calls)
			}
		})
	}
}

func TestRetriesHaveIndependentGenerations(t *testing.T) {
	a := &fakeStore{failAt: "verify"}
	_ = publish(context.Background(), a, testSnapshot())
	b := &fakeStore{verification: correctVerification}
	if err := publish(context.Background(), b, testSnapshot()); err != nil {
		t.Fatal(err)
	}
	if a.id == b.id {
		t.Fatal("retry reused staged generation")
	}
}
