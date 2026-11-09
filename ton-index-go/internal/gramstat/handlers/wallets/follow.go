package wallets

import (
	"context"
	"fmt"
	"net/url"
)

// Native InsertBatchClickhouse writes messages, blocks and deployments before
// account_deltas. This contract must be certified in the source epoch's proof.
func (h *Handler) advanceNative(ctx context.Context, p url.Values, s *source) error {
	if s.Verified != 1 {
		return fmt.Errorf("native following requires a verified source epoch")
	}
	var head struct {
		MC uint32 `json:"mc"`
	}
	if err := h.query(ctx, `SELECT max(mc_seqno) AS mc FROM {metrics:Identifier}.account_deltas FORMAT JSONEachRow`, p, &head); err != nil {
		return err
	}
	if head.MC <= 1000 || head.MC-1000 <= s.End {
		return nil
	}
	end := head.MC - 1000
	set(p, "end", uint64(end))
	var proof struct {
		Deltas uint32 `json:"deltas"`
		Blocks uint32 `json:"blocks"`
	}
	if err := h.query(ctx, `SELECT
 (SELECT toUInt32(count()) FROM {metrics:Identifier}.account_deltas FINAL WHERE mc_seqno BETWEEN {first:UInt32} AND {end:UInt32}) AS deltas,
 (SELECT toUInt32(count()) FROM {raw:Identifier}.blocks FINAL WHERE workchain=-1 AND shard=-9223372036854775808 AND seqno BETWEEN {first:UInt32} AND {end:UInt32}) AS blocks
 FORMAT JSONEachRow`, p, &proof); err != nil {
		return err
	}
	if proof.Deltas != end-s.First+1 || proof.Blocks != proof.Deltas {
		return fmt.Errorf("native wallet prefix incomplete at MC %d", end)
	}
	var tips struct {
		Expected uint32 `json:"expected"`
		Found    uint32 `json:"found"`
		Before   uint32 `json:"before"`
	}
	if err := h.query(ctx, `SELECT
 (SELECT toUInt32(count()) FROM {raw:Identifier}.shard_state FINAL WHERE mc_seqno={end:UInt32}) AS expected,
 toUInt32(count()) AS found,toUInt32(min(gen_utime)) AS before
 FROM {raw:Identifier}.blocks FINAL
 WHERE (workchain,shard,seqno) IN (
 SELECT workchain,shard,seqno FROM {raw:Identifier}.shard_state FINAL WHERE mc_seqno={end:UInt32}) FORMAT JSONEachRow`, p, &tips); err != nil {
		return err
	}
	if tips.Expected < 2 || tips.Found != tips.Expected || tips.Before < s.Before {
		return fmt.Errorf("incomplete or regressed wallet shard-tip horizon")
	}
	set(p, "complete_from", uint64(s.From))
	set(p, "before", uint64(tips.Before))
	p.Set("param_proof", s.Proof)
	if err := h.query(ctx, `INSERT INTO {sink:Identifier}.wallet_sources
 SELECT {source:String},{raw:String},{metrics:String},{first:UInt32},{end:UInt32},
 {complete_from:UInt32},{before:UInt32},toUInt8(1),{proof:String}`, p, nil); err != nil {
		return err
	}
	s.End = end
	s.Before = tips.Before
	if h.Log != nil {
		h.Log.Info("wallet source advanced", "through_mc", end, "complete_before", s.Before)
	}
	return nil
}
