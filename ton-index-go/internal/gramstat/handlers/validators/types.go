// Package validators publishes elected validator sets and session-stats locations by round.
package validators

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"time"
)

const Version = "rounds-v1"

type Member struct {
	Index  uint16 `json:"idx"`
	Pubkey string `json:"pubkey"`
	ADNL   string `json:"adnl_addr"`
	Weight uint64 `json:"weight"`
}

type Round struct {
	Since        uint32   `json:"utime_since"`
	Until        uint32   `json:"utime_until"`
	Total        uint16   `json:"total"`
	Main         uint16   `json:"main"`
	TotalWeight  uint64   `json:"total_weight"`
	SourceMC     uint32   `json:"source_mc_seqno"`
	AnchorMC     uint32   `json:"anchor_mc_seqno"`
	AnchorTime   uint32   `json:"anchor_utime"`
	AnchorRoot   string   `json:"anchor_root_hash"`
	AnchorFile   string   `json:"anchor_file_hash"`
	ConfigHash   string   `json:"config34_hash"`
	RosterDigest string   `json:"roster_digest"`
	CalculatedAt string   `json:"calculated_at"`
	Through      uint32   `json:"data_through"`
	Partial      uint8    `json:"is_partial"`
	Version      string   `json:"calc_version"`
	Members      []Member `json:"members"`
}

func timestamp(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000000") }

func normalizeHash(s string) (string, error) {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 32 {
		return "", fmt.Errorf("expected a 256-bit hex identifier")
	}
	return strings.ToUpper(s), nil
}

func (r *Round) validate() error {
	if r.Since >= r.Until || r.Total == 0 || r.Main == 0 || r.Main > r.Total || len(r.Members) != int(r.Total) {
		return fmt.Errorf("invalid interval or incomplete roster for round %d", r.Since)
	}
	sort.Slice(r.Members, func(i, j int) bool { return r.Members[i].Index < r.Members[j].Index })
	keys := make(map[string]bool)
	var sum uint64
	for i := range r.Members {
		m := &r.Members[i]
		var err error
		if m.Pubkey, err = normalizeHash(m.Pubkey); err != nil {
			return err
		}
		if m.ADNL, err = normalizeHash(m.ADNL); err != nil {
			return err
		}
		if int(m.Index) != i || keys[m.Pubkey] || m.Weight == 0 {
			return fmt.Errorf("invalid/duplicate member in round %d", r.Since)
		}
		keys[m.Pubkey] = true
		var carry uint64
		sum, carry = bits.Add64(sum, m.Weight, 0)
		if carry != 0 {
			return fmt.Errorf("weight overflow")
		}
	}
	if sum != r.TotalWeight {
		return fmt.Errorf("weight mismatch for round %d", r.Since)
	}
	// Includes every field of the set, but not mutable observation metadata.
	data, _ := json.Marshal(struct {
		Since, Until uint32
		Total, Main  uint16
		Weight       uint64
		Members      []Member
	}{
		r.Since, r.Until, r.Total, r.Main, r.TotalWeight, r.Members})
	hash := sha256.Sum256(data)
	r.RosterDigest = hex.EncodeToString(hash[:])
	return nil
}

type GeoMember struct {
	Pubkey            string   `json:"pubkey"`
	ADNL              string   `json:"adnl_addr"`
	ObservedAt        *uint32  `json:"observed_at"`
	RecordID          *string  `json:"record_id"`
	Country           *string  `json:"country"`
	City              *string  `json:"city"`
	Latitude          *float64 `json:"latitude"`
	Longitude         *float64 `json:"longitude"`
	Samples           uint64   `json:"samples"`
	IPVersions        uint32   `json:"ip_versions"`
	CountryVersions   uint32   `json:"country_versions"`
	OrderingConflicts uint32   `json:"ordering_conflicts"`
	Status            string   `json:"status"`
}

type GeoRound struct {
	Since         uint32            `json:"utime_since"`
	Until         uint32            `json:"utime_until"`
	ConfigHash    string            `json:"config34_hash"`
	RosterDigest  string            `json:"roster_digest"`
	Total         uint16            `json:"total"`
	Observed      uint16            `json:"observed"`
	Located       uint16            `json:"located"`
	CountryKnown  uint16            `json:"country_known"`
	Unknown       uint16            `json:"unknown"`
	Countries     *uint16           `json:"countries"`
	Breakdown     map[string]uint16 `json:"breakdown"`
	CalculatedAt  string            `json:"calculated_at"`
	Through       uint32            `json:"data_through"`
	SourceThrough uint32            `json:"source_through"`
	Partial       uint8             `json:"is_partial"`
	Status        string            `json:"status"`
	Version       string            `json:"calc_version"`
	Members       []GeoMember       `json:"members"`
}
