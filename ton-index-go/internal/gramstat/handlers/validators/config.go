package validators

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// ConfigRPC verifies the requested post-block #34 via TON Center v2. The saved
// PG full block ID and config-cell hash identify the observation. This API does
// not return a Merkle proof; it is a trusted RPC check, not a verified light client.
type ConfigRPC struct{ URL, APIKey string }

func (c ConfigRPC) Verify(ctx context.Context, r Round) (string, error) {
	u, err := url.Parse(c.URL)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("GRAMSTAT_CONFIG_URL must be a TON Center v2 HTTP(S) base URL without credentials")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/getConfigParam"
	u.RawQuery = url.Values{"config_id": {"34"}, "seqno": {strconv.FormatUint(uint64(r.AnchorMC), 10)}}.Encode()
	client := http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return "", err
		}
		if c.APIKey != "" {
			req.Header.Set("X-API-Key", c.APIKey)
		}
		resp, err := client.Do(req)
		if err != nil {
			if e, ok := err.(*url.Error); ok {
				err = e.Err
			}
			return "", fmt.Errorf("config RPC: %w", err)
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20+1))
		resp.Body.Close()
		if resp.StatusCode == 429 && attempt < 2 {
			timer := time.NewTimer(time.Duration(attempt+1) * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return "", ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if readErr != nil {
			return "", readErr
		}
		if resp.StatusCode != 200 || len(data) > 1<<20 {
			return "", fmt.Errorf("config RPC HTTP %d or oversized response", resp.StatusCode)
		}
		var body struct {
			OK     bool `json:"ok"`
			Result struct {
				Config struct {
					Bytes string `json:"bytes"`
				} `json:"config"`
			} `json:"result"`
		}
		if err = json.Unmarshal(data, &body); err != nil {
			return "", err
		}
		if !body.OK || body.Result.Config.Bytes == "" {
			return "", fmt.Errorf("config #34 unavailable at MC %d", r.AnchorMC)
		}
		boc, err := base64.StdEncoding.DecodeString(body.Result.Config.Bytes)
		if err != nil {
			return "", err
		}
		return verifyConfig(boc, r)
	}
	return "", fmt.Errorf("config RPC retries exhausted")
}

func verifyConfig(boc []byte, want Round) (string, error) {
	root, err := cell.FromBOC(boc)
	if err != nil {
		return "", err
	}
	var set tlb.ValidatorSetAny
	if err = tlb.LoadFromCell(&set, root.BeginParse()); err != nil {
		return "", err
	}
	var got Round
	var dict *cell.Dictionary
	var hasWeight bool
	switch v := set.Validators.(type) {
	case tlb.ValidatorSet:
		got.Since, got.Until, got.Total, got.Main, dict = v.UTimeSince, v.UTimeUntil, v.Total, v.Main, v.List
	case tlb.ValidatorSetExt:
		got.Since, got.Until, got.Total, got.Main, got.TotalWeight, dict = v.UTimeSince, v.UTimeUntil, v.Total, v.Main, v.TotalWeight, v.List
		hasWeight = true
	default:
		return "", fmt.Errorf("unsupported validator set")
	}
	if dict == nil {
		return "", fmt.Errorf("missing validator dictionary")
	}
	entries, err := dict.LoadAll()
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		idx, err := entry.Key.LoadUInt(16)
		if err != nil {
			return "", err
		}
		tag, err := entry.Value.PreloadUInt(8)
		if err != nil {
			return "", err
		}
		m := Member{Index: uint16(idx)}
		switch tag {
		case 0x73:
			var v tlb.ValidatorAddr
			if err = tlb.LoadFromCell(&v, entry.Value); err != nil {
				return "", err
			}
			m.Pubkey, m.ADNL, m.Weight = hex.EncodeToString(v.PublicKey.Key), hex.EncodeToString(v.ADNLAddr), v.Weight
		case 0x53:
			var v tlb.Validator
			if err = tlb.LoadFromCell(&v, entry.Value); err != nil {
				return "", err
			}
			m.Pubkey, m.ADNL, m.Weight = hex.EncodeToString(v.PublicKey.Key), hex.EncodeToString(make([]byte, 32)), v.Weight
		default:
			return "", fmt.Errorf("unsupported validator descriptor")
		}
		got.Members = append(got.Members, m)
	}
	if !hasWeight {
		got.TotalWeight = want.TotalWeight
	}
	if err = got.validate(); err != nil {
		return "", err
	}
	if err = want.validate(); err != nil {
		return "", err
	}
	if !bytes.Equal([]byte(got.RosterDigest), []byte(want.RosterDigest)) {
		return "", fmt.Errorf("config #34 differs from indexed roster at MC %d", want.AnchorMC)
	}
	return hex.EncodeToString(root.Hash()), nil
}
