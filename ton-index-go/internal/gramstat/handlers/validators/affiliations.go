package validators

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const AffiliationVersion = "account-identity-v3"

const controllerCodeHash = "dd3ce98db487c7585803933bffba7a57eb4e663099059d08b83db0b4ce060793"
const hipoCodeHash = "56fb96fc4b9051deecfce8b04ce3c888990ba80fe6bd07154e351506ee9907a0"
const whalesCodeHash = "6097b64a3b9db526a6b26497afeae3f224a8f639d37b6534d46df21fe0589c21"

var controllerPools = map[string]string{
	"0:A45B17F28409229B78360E3290420F13E4FE20F90D7E2BF8C4AC6703259E22FA": "Tonstakers",
	"0:F6FF877DD4CE1355B101572045F09D54C29309737EB52CA542CFA6C195F7CC5B": "Stakee",
	"0:3D1F01190FFB4E35F573EA5252D0F54A479BE2122106CA3F40B907B01905675C": "KTON",
	"0:9FE32DD92875447A6B83D031D0230C539752152B660D4A056797BE9AD2E396D5": "sGRAM",
}

const hipoTreasury = "0:8BC991CFE177BC7E9721433EFA3BEFD199485A55CFFD040A06C89AF026B71BCF"
const whalesPool = "0:744A8C6E183C79AA356DD0FFDB3C80857967452C1995A291E18E07ECD2AFB0B1"

// The deployed Hipo code cell is a library reference, not the expanded library.
// Its hash is hipoCodeHash. Initial elector is addr_none; proxy_new_stake later
// replaces that field, leaving treasury, borrower and round_since immutable.
// HipoFinance/contract@85e7777 contracts/imports/utils.fc:create_loan_address.
var hipoLoanCode = func() *cell.Cell {
	b, err := base64.StdEncoding.DecodeString("te6cckEBAQEAIwAIQgLguNIV0gwX1kyPywev5kpDFPDyb3KFUTanvMX2/n9UJlN+qwc=")
	if err != nil {
		panic(err)
	}
	c, err := cell.FromBOC(b)
	if err != nil {
		panic(err)
	}
	if hex.EncodeToString(c.Hash()) != hipoCodeHash {
		panic("invalid Hipo library reference")
	}
	return c
}()

func hipoLoanAddress(pool, borrower string, since uint32) (string, error) {
	p, err := address.ParseRawAddr(pool)
	if err != nil || p.Workchain() != 0 {
		return "", fmt.Errorf("invalid Hipo treasury")
	}
	b, err := address.ParseRawAddr(borrower)
	if err != nil || b.Workchain() != 0 || since == 0 {
		return "", fmt.Errorf("invalid Hipo borrower/round")
	}
	data := cell.BeginCell().MustStoreUInt(0, 2).MustStoreAddr(p).MustStoreAddr(b).MustStoreUInt(uint64(since), 32).EndCell()
	init := cell.BeginCell().MustStoreUInt(6, 5).MustStoreRef(hipoLoanCode).MustStoreRef(data).EndCell()
	return "-1:" + strings.ToUpper(hex.EncodeToString(init.Hash())), nil
}

func officialWhalesProxy(holder string) bool {
	return holder == "-1:07FDDDF68A769856D336F23554C4E13EE2072505739D51682935A40F27E570B1" || holder == "-1:92CDC50DB0FD94175D29EDC1C42D39A2429DAB2DA4C28051E48670AB1661F0B1"
}

// These implementations keep the parent pool/treasury in immutable identity
// data. The observation date is provenance, not an expiry date for affiliation.
// Round-specific type/holder matching still happens before this lookup is used.
type Affiliation struct {
	Holder            string `json:"holder"`
	Family            string `json:"family"`
	Protocol          string `json:"protocol"`
	Pool              string `json:"pool"`
	CodeHash          string `json:"code_hash"`
	DataHash          string `json:"data_hash"`
	Since             uint32 `json:"utime_since"`
	MC                uint32 `json:"mc_seqno"`
	Root              string `json:"root_hash"`
	File              string `json:"file_hash"`
	ResponseHash      string `json:"response_sha256"`
	FirstMC           uint32 `json:"first_mc_seqno,omitempty"`
	FirstResponseHash string `json:"first_response_sha256,omitempty"`
	Borrower          string `json:"borrower"`
	LoanSince         uint32 `json:"loan_since"`
	Version           string `json:"verification_version"`
	CheckedAt         string `json:"checked_at"`
}

type AffiliationStore interface {
	LoadAffiliations(context.Context) ([]Affiliation, error)
	WriteAffiliations(context.Context, []Affiliation) error
}

func affiliationKey(holder, family string) string { return holder + "/" + family }

func protocolFor(a Affiliation) string {
	switch a.Family {
	case "liquid_staking_controller":
		if a.CodeHash == controllerCodeHash {
			return controllerPools[a.Pool]
		}
	case "hipo_controller":
		if a.CodeHash == hipoCodeHash && a.Pool == hipoTreasury {
			if derived, err := hipoLoanAddress(a.Pool, a.Borrower, a.LoanSince); err == nil && derived == a.Holder {
				return "Hipo"
			}
		}
	case "whales_proxy":
		if a.CodeHash == whalesCodeHash && a.Pool == whalesPool && officialWhalesProxy(a.Holder) {
			return "Whales wsTON"
		}
	}
	return ""
}

func affiliationFor(since uint32, holder, family string, resolved map[string]Affiliation) (string, string) {
	key := affiliationKey(holder, family)
	a, ok := resolved[key]
	if !ok || a.Version != AffiliationVersion || a.Holder != holder || a.Family != family || (family == "hipo_controller" && a.LoanSince != since) {
		return "", ""
	}
	name := protocolFor(a)
	if name == "" {
		return "", ""
	}
	return name, fmt.Sprintf("%s:mc=%d:code=%s:response=%s", AffiliationVersion, a.MC, a.CodeHash, a.ResponseHash)
}

// InspectAffiliation verifies the trusted RPC block identity and known code
// implementation before decoding its static pool link. No current-state label
// is silently substituted for the requested historical account state.
func InspectAffiliation(raw []byte, r Round, holder, family string) (Affiliation, error) {
	a := Affiliation{Holder: rawHolder(holder), Family: family, Since: r.Since, MC: r.AnchorMC, Root: r.AnchorRoot, File: r.AnchorFile,
		Version: AffiliationVersion, CheckedAt: time.Now().UTC().Format("2006-01-02 15:04:05.000000")}
	if a.Holder == "" || r.AnchorMC == 0 || r.AnchorRoot == "" || r.AnchorFile == "" {
		return a, fmt.Errorf("missing account/anchor identity")
	}
	var body struct {
		OK     bool `json:"ok"`
		Result struct {
			State string `json:"state"`
			Code  string `json:"code"`
			Data  string `json:"data"`
			Block struct {
				Workchain int    `json:"workchain"`
				Seqno     uint32 `json:"seqno"`
				Root      string `json:"root_hash"`
				File      string `json:"file_hash"`
			} `json:"block_id"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return a, err
	}
	b := body.Result.Block
	if !body.OK || body.Result.State != "active" || b.Workchain != -1 || b.Seqno != r.AnchorMC || b.Root != r.AnchorRoot || b.File != r.AnchorFile {
		return a, fmt.Errorf("account response does not match active historical state")
	}
	load := func(s string) (*cell.Cell, error) {
		b, e := base64.StdEncoding.DecodeString(s)
		if e != nil {
			return nil, e
		}
		return cell.FromBOC(b)
	}
	code, err := load(body.Result.Code)
	if err != nil {
		return a, err
	}
	data, err := load(body.Result.Data)
	if err != nil {
		return a, err
	}
	a.CodeHash, a.DataHash = hex.EncodeToString(code.Hash()), hex.EncodeToString(data.Hash())
	hash := sha256.Sum256(raw)
	a.ResponseHash = hex.EncodeToString(hash[:])
	addr := func(s *cell.Slice) (string, error) {
		v, e := s.LoadAddr()
		if e != nil {
			return "", e
		}
		if v == nil {
			return "", fmt.Errorf("missing contract address")
		}
		return rawHolder(v.StringRaw()), nil
	}
	s := data.BeginParse()
	switch family {
	case "liquid_staking_controller":
		if a.CodeHash != controllerCodeHash {
			return a, fmt.Errorf("unreviewed controller code")
		}
		s, err = s.LoadRef()
		if err != nil {
			return a, err
		}
		if _, err = s.LoadUInt(32); err != nil {
			return a, err
		}
		if _, err = addr(s); err != nil {
			return a, err
		}
		if a.Pool, err = addr(s); err != nil {
			return a, err
		}
		if _, err = addr(s); err != nil {
			return a, err
		}
		auth, e := s.LoadRef()
		if e != nil {
			return a, e
		}
		if _, err = addr(auth); err != nil {
			return a, err
		}
		if _, err = addr(auth); err != nil {
			return a, err
		}
		if auth.BitsLeft() != 0 || auth.RefsNum() != 0 || s.BitsLeft() != 0 || s.RefsNum() != 0 {
			return a, fmt.Errorf("unexpected controller static layout")
		}
	case "hipo_controller":
		if a.CodeHash != hipoCodeHash {
			return a, fmt.Errorf("unreviewed Hipo code")
		}
		elector, e := addr(s)
		if e != nil || elector != "-1:"+strings.Repeat("3", 64) {
			return a, fmt.Errorf("unexpected Hipo elector")
		}
		if a.Pool, err = addr(s); err != nil {
			return a, err
		}
		if a.Borrower, err = addr(s); err != nil {
			return a, err
		}
		loanSince, err := s.LoadUInt(32)
		if err != nil {
			return a, err
		}
		a.LoanSince = uint32(loanSince)
		if s.BitsLeft() != 0 || s.RefsNum() != 0 {
			return a, fmt.Errorf("unexpected Hipo loan layout")
		}
		derived, err := hipoLoanAddress(a.Pool, a.Borrower, a.LoanSince)
		if err != nil || derived != a.Holder || a.LoanSince != r.Since {
			return a, fmt.Errorf("Hipo StateInit address or round mismatch")
		}
	case "whales_proxy":
		if a.CodeHash == whalesCodeHash && officialWhalesProxy(a.Holder) {
			a.Pool = whalesPool
		}
	}
	a.Protocol = protocolFor(a)
	return a, nil
}

type AffiliationResolver struct {
	RPC       ConfigRPC
	Store     AffiliationStore
	Log       *slog.Logger
	cache     map[string]Affiliation
	attempted map[string]bool
}

func (r *AffiliationResolver) Resolve(ctx context.Context, rounds []Round, members map[uint32]StakeMembers, attrs Attribution) (map[string]Affiliation, error) {
	if r.cache == nil {
		cache := map[string]Affiliation{}
		if r.Store != nil {
			records, err := r.Store.LoadAffiliations(ctx)
			if err != nil {
				return nil, err
			}
			for _, a := range records {
				if a.Version != AffiliationVersion {
					continue
				}
				if err := validateAffiliation(a); err != nil {
					return nil, err
				}
				cache[affiliationKey(a.Holder, a.Family)] = a
			}
		}
		r.cache = cache
	}
	var changed []Affiliation
	if r.attempted == nil {
		r.attempted = map[string]bool{}
	}
	for _, round := range rounds {
		if members[round.Since].Digest != round.RosterDigest {
			continue
		}
		for _, m := range round.Members {
			t, ok := attrs.Types[[2]uint32{round.Since, uint32(m.Index)}]
			if !ok || t.Conflict || t.ContractType == nil || t.Pubkey != m.Pubkey || t.ADNL != m.ADNL || t.Weight != m.Weight || t.Until != round.Until || t.Holder != rawHolder(members[round.Since].Positions[m.Pubkey].Holder) {
				continue
			}
			family := *t.ContractType
			if family != "liquid_staking_controller" && family != "hipo_controller" && !(family == "whales_proxy" && officialWhalesProxy(t.Holder)) {
				continue
			}
			key := affiliationKey(t.Holder, family)
			if _, ok := r.cache[key]; ok {
				continue
			}
			if r.attempted[key] {
				continue
			}
			r.attempted[key] = true
			a, err := r.fetch(ctx, round, t.Holder, family)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if r.Log != nil {
					r.Log.Warn("stake affiliation unresolved", "holder", t.Holder, "mc", round.AnchorMC, "error", err)
				}
				continue
			}
			changed = append(changed, a)
		}
	}
	if len(changed) > 0 && r.Store != nil {
		if err := r.Store.WriteAffiliations(ctx, changed); err != nil {
			return nil, err
		}
	}
	for _, a := range changed {
		r.cache[affiliationKey(a.Holder, a.Family)] = a
	}
	return r.cache, nil
}

func validateAffiliation(a Affiliation) error {
	if a.Version != AffiliationVersion || rawHolder(a.Holder) != a.Holder || a.Holder == "" || a.MC == 0 || len(a.ResponseHash) != 64 || len(a.CodeHash) != 64 || len(a.DataHash) != 64 || a.Root == "" || a.File == "" || protocolFor(a) != a.Protocol {
		return fmt.Errorf("invalid persisted stake affiliation")
	}
	if _, err := time.Parse("2006-01-02 15:04:05.000000", a.CheckedAt); err != nil {
		return fmt.Errorf("invalid affiliation check time")
	}
	return nil
}

func (r *AffiliationResolver) fetch(ctx context.Context, round Round, holder, family string) (Affiliation, error) {
	u, err := url.Parse(r.RPC.URL)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return Affiliation{}, fmt.Errorf("invalid affiliation RPC base URL")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/getAddressInformation"
	u.RawQuery = url.Values{"address": {holder}, "seqno": {strconv.FormatUint(uint64(round.AnchorMC), 10)}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Affiliation{}, err
	}
	if r.RPC.APIKey != "" {
		req.Header.Set("X-API-Key", r.RPC.APIKey)
	}
	client := http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		if e, ok := err.(*url.Error); ok {
			err = e.Err
		}
		return Affiliation{}, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20+1))
	if err != nil {
		return Affiliation{}, err
	}
	if response.StatusCode != 200 || len(data) > 1<<20 {
		return Affiliation{}, fmt.Errorf("account RPC HTTP %d or oversized response", response.StatusCode)
	}
	return InspectAffiliation(data, round, holder, family)
}
