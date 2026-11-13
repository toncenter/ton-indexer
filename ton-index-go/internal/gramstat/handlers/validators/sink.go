package validators

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

//go:embed schema.sql
var Schema string

type Coverage struct {
	Since      uint32 `json:"utime_since"`
	Until      uint32 `json:"utime_until"`
	Through    uint32 `json:"data_through"`
	Digest     string `json:"roster_digest"`
	Hash       string `json:"config34_hash"`
	AnchorRoot string `json:"anchor_root_hash"`
	AnchorFile string `json:"anchor_file_hash"`
	AnchorMC   uint32 `json:"anchor_mc_seqno"`
	Version    string `json:"calc_version"`
}

type Store struct{ CH *gramstat.ClickHouse }

func decodeRows[T any](data []byte) ([]T, error) {
	var rows []T
	dec := json.NewDecoder(bytes.NewReader(data))
	for {
		var row T
		err := dec.Decode(&row)
		if err == io.EOF {
			return rows, nil
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
}

func encodeRows[T any](rows []T) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (s Store) Coverage(ctx context.Context, geo bool) (map[uint32]Coverage, error) {
	table, extra := "validator_rounds", ", anchor_root_hash, anchor_file_hash, anchor_mc_seqno"
	if geo {
		table, extra = "validator_geo_rounds", ""
	}
	data, err := s.CH.Query(ctx, "SELECT utime_since,utime_until,data_through,roster_digest,config34_hash,calc_version"+extra+" FROM gramstat."+table+" FINAL FORMAT JSONEachRow", nil)
	if err != nil {
		return nil, err
	}
	rows, err := decodeRows[Coverage](data)
	result := make(map[uint32]Coverage, len(rows))
	for _, r := range rows {
		result[r.Since] = r
	}
	return result, err
}

func (s Store) Rounds(ctx context.Context, ids []uint32) ([]Round, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatUint(uint64(id), 10)
	}
	data, err := s.CH.Query(ctx, "SELECT * FROM gramstat.validator_rounds FINAL WHERE utime_since IN ("+strings.Join(parts, ",")+") ORDER BY utime_since FORMAT JSONEachRow",
		url.Values{"output_format_json_named_tuples_as_objects": {"1"}, "output_format_json_quote_64bit_integers": {"0"}})
	if err != nil {
		return nil, err
	}
	return decodeRows[Round](data)
}

func (s Store) WriteRounds(ctx context.Context, rows []Round) error {
	if len(rows) == 0 {
		return nil
	}
	data, err := encodeRows(rows)
	if err != nil {
		return err
	}
	return s.CH.InsertJSON(ctx, "gramstat.validator_rounds", data)
}

func (s Store) WriteGeo(ctx context.Context, rows []GeoRound) error {
	if len(rows) == 0 {
		return nil
	}
	data, err := encodeRows(rows)
	if err != nil {
		return err
	}
	return s.CH.InsertJSON(ctx, "gramstat.validator_geo_rounds", data)
}
