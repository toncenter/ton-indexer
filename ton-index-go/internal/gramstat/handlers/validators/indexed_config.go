package validators

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// IndexedConfig checks the native config cell persisted with the MC block.
// No liteserver or HTTP config request is needed by the aggregator.
type IndexedConfig struct{ DSN string }

func (v IndexedConfig) Verify(ctx context.Context, r Round) (string, error) {
	cfg, err := pgx.ParseConfig(v.DSN)
	if err != nil || v.DSN == "" {
		return "", fmt.Errorf("invalid GRAMSTAT_PG_DSN")
	}
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.RuntimeParams["statement_timeout"] = "30000"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return "", err
	}
	defer conn.Close(context.Background())
	var encoded, hash, root, file string
	var timestamp uint32
	err = conn.QueryRow(ctx, `SELECT s.config_boc,s.config_hash,b.root_hash::text,b.file_hash::text,s.gen_utime
FROM public.validator_set_states s JOIN public.blocks b ON b.workchain=-1 AND b.seqno=s.mc_seqno
WHERE s.mc_seqno=$1`, r.AnchorMC).Scan(&encoded, &hash, &root, &file, &timestamp)
	if err != nil {
		return "", fmt.Errorf("native config #34 at MC %d: %w", r.AnchorMC, err)
	}
	if root != r.AnchorRoot || file != r.AnchorFile || timestamp != r.AnchorTime {
		return "", fmt.Errorf("native config MC identity mismatch")
	}
	boc, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", err
	}
	got, err := verifyConfig(boc, r)
	if err == nil && !strings.EqualFold(hash, got) {
		return "", fmt.Errorf("native config hash mismatch")
	}
	return got, err
}
