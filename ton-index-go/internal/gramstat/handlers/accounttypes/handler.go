package accounttypes

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/handlers/codehashes"
)

//go:embed schema.sql
var Schema string

type Transport interface {
	Query(context.Context, string, url.Values) ([]byte, error)
	InsertJSON(context.Context, string, []byte) error
}

type Handler struct {
	CH  Transport
	Log *slog.Logger
}

func (*Handler) Name() string { return "account_types" }

type countSnapshot struct {
	ID       string `json:"snapshot_id"`
	Day      string `json:"observed_day"`
	Started  string `json:"started_at"`
	Finished string `json:"finished_at"`
	Accounts uint64 `json:"accounts,string"`
	Hashes   uint64 `json:"hashes,string"`
	MCMin    uint32 `json:"source_mc_min"`
	MCMax    uint32 `json:"source_mc_max"`
}

type featureIdentity struct {
	ID  string `json:"feature_id"`
	Day string `json:"feature_day"`
}

const latestCountsSQL = `SELECT toString(snapshot_id) AS snapshot_id,toString(observed_day) AS observed_day,
 toString(started_at) AS started_at,toString(finished_at) AS finished_at,
 toString(indexed_accounts) AS accounts,toString(hash_count) AS hashes,source_mc_min,source_mc_max
 FROM gramstat.account_code_hash_snapshots
 WHERE source_id={source:String} AND consistency='live-index-scan'
 ORDER BY finished_at DESC,snapshot_id DESC LIMIT 1 FORMAT JSONEachRow`

const methodsSQL = `SELECT hash,arrayMap(x->toUInt32(x),arraySort(raw_methods)) AS methods,variants,valid
 FROM (SELECT toString(code_hash) AS hash,any(methods) AS raw_methods,
 toUInt32(uniqExact(arraySort(methods))) AS variants,
 toUInt8(arrayAll(x->x<=4294967295,any(methods))) AS valid
 FROM mainnet_gramstat_full.contract_methods
 WHERE substring(code_hash,1,1) IN {prefixes:Array(String)}
 GROUP BY code_hash) ORDER BY hash FORMAT JSONEachRow`

const existingFeaturesSQL = `SELECT toString(feature_id) AS feature_id,toString(feature_day) AS feature_day
 FROM gramstat.account_type_feature_sets WHERE digest={digest:String}
 ORDER BY published_at DESC,feature_id DESC LIMIT 1 FORMAT JSONEachRow`

const verifyFeaturesSQL = `SELECT toUInt32(count()) AS rows,toUInt32(uniqExact(prefix)) AS prefixes,
 toString(sum(length(assignments))) AS assignment_count,toString(sum(length(methods))) AS method_count,
 toUInt32(countIf(NOT arrayAll(k->startsWith(k,prefix),mapKeys(assignments))
   OR NOT arrayAll(k->startsWith(k,prefix),mapKeys(methods)))) AS invalid
 FROM gramstat.account_type_features
 WHERE feature_id={feature:UUID} AND feature_day={feature_day:Date} FORMAT JSONEachRow`

// Only grouped counts cross the HTTP boundary. Every source code hash
// participates, including the long tail without a method record.
const aggregateSQL = `SELECT
 coalesce(nullIf(f.leaf,''),'unknown-no-methods') AS leaf,
 toString(sum(n)) AS accounts,toString(count()) AS hashes
 FROM (
   SELECT hash,n FROM gramstat.account_code_hash_counts
   ARRAY JOIN mapKeys(counts) AS hash,mapValues(counts) AS n
   WHERE source_id={source:String} AND snapshot_id={snapshot:UUID} AND observed_day={day:Date}
 ) AS c LEFT JOIN (
   SELECT hash,leaf FROM gramstat.account_type_features
   ARRAY JOIN mapKeys(assignments) AS hash,mapValues(assignments) AS leaf
   WHERE feature_id={feature:UUID} AND feature_day={feature_day:Date}
 ) AS f ON c.hash=f.hash
 GROUP BY leaf ORDER BY leaf FORMAT JSONEachRow`

const alreadyPublishedSQL = `SELECT toString(publication_id) AS id FROM gramstat.account_type_snapshots
 WHERE source_id={source:String} AND counts_snapshot_id={snapshot:UUID} AND observed_day={day:Date}
 AND feature_id={feature:UUID} LIMIT 1 FORMAT JSONEachRow`

func decodeRows[T any](data []byte) ([]T, error) {
	var rows []T
	dec := json.NewDecoder(bytes.NewReader(data))
	for {
		var row T
		if err := dec.Decode(&row); err == io.EOF {
			return rows, nil
		} else if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
}

func sqlArray(items []string) string {
	// Internal strings only: validated Base64 prefix characters.
	var b bytes.Buffer
	b.WriteByte('[')
	for i, item := range items {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "'%s'", item)
	}
	b.WriteByte(']')
	return b.String()
}

func (h *Handler) Run(ctx context.Context, req gramstat.Request) error {
	if !req.From.IsZero() || !req.To.IsZero() {
		return fmt.Errorf("account_types prepares latest observations; explicit dates are unsupported")
	}
	params := url.Values{"param_source": {codehashes.SourceID}}
	raw, err := h.CH.Query(ctx, latestCountsSQL, params)
	if err != nil {
		return err
	}
	snapshots, err := decodeRows[countSnapshot](raw)
	if err != nil {
		return err
	}
	if len(snapshots) != 1 {
		return fmt.Errorf("no unambiguous completed code-hash snapshot")
	}
	snapshot := snapshots[0]
	if snapshot.Accounts == 0 || snapshot.Hashes == 0 || snapshot.MCMin == 0 || snapshot.MCMax < snapshot.MCMin {
		return fmt.Errorf("invalid source snapshot totals")
	}
	if _, err := uuid.Parse(snapshot.ID); err != nil {
		return err
	}
	methods := map[string][]uint32{}
	methodsStarted := time.Now().UTC()
	for start := 0; start < len(codehashes.Alphabet); start += 4 {
		prefixes := []string{}
		for _, char := range codehashes.Alphabet[start : start+4] {
			prefixes = append(prefixes, string(char))
		}
		raw, err := h.CH.Query(ctx, methodsSQL, url.Values{"param_prefixes": {sqlArray(prefixes)}})
		if err != nil {
			return err
		}
		rows, err := decodeRows[struct {
			Hash     string   `json:"hash"`
			Methods  []uint32 `json:"methods"`
			Variants uint32   `json:"variants"`
			Valid    uint8    `json:"valid"`
		}](raw)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if _, err := canonicalHash(row.Hash); err != nil || !slices.Contains(prefixes, row.Hash[:1]) || row.Variants != 1 || row.Valid != 1 {
				return fmt.Errorf("invalid or conflicting static method metadata")
			}
			if _, exists := methods[row.Hash]; exists {
				return fmt.Errorf("duplicate method record")
			}
			methods[row.Hash] = row.Methods
			if len(methods) > 5_000_000 {
				return fmt.Errorf("method record budget exceeded")
			}
		}
	}
	methodsFinished := time.Now().UTC()
	features, err := BuildFeatures(methods)
	if err != nil {
		return err
	}
	identity, err := h.prepareFeatures(ctx, features, methodsStarted, methodsFinished)
	if err != nil {
		return err
	}
	params.Set("param_snapshot", snapshot.ID)
	params.Set("param_day", snapshot.Day)
	params.Set("param_feature", identity.ID)
	params.Set("param_feature_day", identity.Day)
	raw, err = h.CH.Query(ctx, alreadyPublishedSQL, params)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(raw)) > 0 {
		return nil
	}
	raw, err = h.CH.Query(ctx, aggregateSQL, params)
	if err != nil {
		return err
	}
	counts, err := decodeRows[LeafCount](raw)
	if err != nil {
		return err
	}
	nodes, err := BuildTree(features.Definitions, counts, snapshot.Accounts, snapshot.Hashes)
	if err != nil {
		return err
	}
	tree, err := json.Marshal(nodes)
	if err != nil {
		return err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	row := map[string]any{
		"publication_id": id.String(), "source_id": codehashes.SourceID, "counts_snapshot_id": snapshot.ID,
		"observed_day": snapshot.Day, "feature_id": identity.ID, "feature_day": identity.Day,
		"abi_revision": features.ABIRevision, "classification_version": features.Classification,
		"started_at": snapshot.Started, "finished_at": snapshot.Finished, "published_at": stamp(time.Now()),
		"indexed_accounts": snapshot.Accounts, "hash_count": snapshot.Hashes,
		"source_mc_min": snapshot.MCMin, "source_mc_max": snapshot.MCMax, "nodes_json": string(tree),
	}
	body, err := json.Marshal(row)
	if err != nil {
		return err
	}
	if err := h.CH.InsertJSON(ctx, "gramstat.account_type_snapshots", body); err != nil {
		return err
	}
	if h.Log != nil {
		h.Log.Info("account types published", "publication", id.String(), "counts_snapshot", snapshot.ID,
			"accounts", snapshot.Accounts, "hashes", snapshot.Hashes, "nodes", len(nodes), "feature_digest", features.Digest)
	}
	return nil
}

func stamp(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000000") }

func (h *Handler) prepareFeatures(ctx context.Context, f *Features, started, finished time.Time) (featureIdentity, error) {
	raw, err := h.CH.Query(ctx, existingFeaturesSQL, url.Values{"param_digest": {f.Digest}})
	if err != nil {
		return featureIdentity{}, err
	}
	existing, err := decodeRows[featureIdentity](raw)
	if err != nil {
		return featureIdentity{}, err
	}
	if len(existing) > 1 {
		return featureIdentity{}, fmt.Errorf("invalid feature publication lookup")
	}
	if len(existing) == 1 {
		return existing[0], nil
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return featureIdentity{}, err
	}
	identity := featureIdentity{id.String(), started.Format(time.DateOnly)}
	var assignments, methods uint64
	var batch bytes.Buffer
	encoder := json.NewEncoder(&batch)
	// Eight prefixes per bounded insert avoids a stream of tiny CH parts.
	// An interrupted generation remains invisible; a retry has a new UUID.
	for i, p := range f.Prefixes {
		row := struct {
			ID  string `json:"feature_id"`
			Day string `json:"feature_day"`
			FeaturePrefix
		}{identity.ID, identity.Day, p}
		if err := encoder.Encode(row); err != nil {
			return featureIdentity{}, err
		}
		if i%8 == 7 {
			if err := h.CH.InsertJSON(ctx, "gramstat.account_type_features", batch.Bytes()); err != nil {
				return featureIdentity{}, err
			}
			batch.Reset()
		}
		assignments += uint64(len(p.Assignments))
		methods += uint64(len(p.Methods))
	}
	raw, err = h.CH.Query(ctx, verifyFeaturesSQL, url.Values{"param_feature": {identity.ID}, "param_feature_day": {identity.Day}})
	if err != nil {
		return featureIdentity{}, err
	}
	var check struct {
		Rows, Prefixes, Invalid uint32
		Assignments             uint64 `json:"assignment_count,string"`
		Methods                 uint64 `json:"method_count,string"`
	}
	if err := json.Unmarshal(raw, &check); err != nil {
		return featureIdentity{}, err
	}
	if check.Rows != 64 || check.Prefixes != 64 || check.Invalid != 0 || check.Assignments != assignments || check.Methods != methods {
		return featureIdentity{}, fmt.Errorf("incomplete account-type feature generation")
	}
	body, err := json.Marshal(map[string]any{
		"feature_id": identity.ID, "feature_day": identity.Day, "digest": f.Digest,
		"abi_revision": f.ABIRevision, "classification_version": f.Classification,
		"methods_started_at": stamp(started), "methods_finished_at": stamp(finished), "published_at": stamp(time.Now()),
		"contracts": f.Contracts, "code_contracts": f.CodeContracts, "method_names": f.MethodNames,
	})
	if err != nil {
		return featureIdentity{}, err
	}
	if err := h.CH.InsertJSON(ctx, "gramstat.account_type_feature_sets", body); err != nil {
		return featureIdentity{}, err
	}
	return identity, nil
}
