// gramstat-stake-registry verifies captured historical account states and can
// import the observations into ClickHouse. Generated observations are runtime
// data, never source files or browser modules.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/handlers/validators"
)

type capture struct {
	Family       string          `json:"family"`
	Holder       string          `json:"holder"`
	Since        uint32          `json:"utime_since"`
	MC           uint32          `json:"anchor_mc_seqno"`
	Root         string          `json:"anchor_root_hash"`
	File         string          `json:"anchor_file_hash"`
	ResponseHash string          `json:"response_sha256"`
	Body         json.RawMessage `json:"body"`
	Error        string          `json:"error"`
	Inventory    *struct {
		Checks     int   `json:"checks"`
		CapturedAt int64 `json:"captured_at"`
	} `json:"inventory"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	input := flag.String("capture", "", "JSONL account-state capture")
	publish := flag.Bool("publish", false, "publish checked observations to GRAMSTAT_CH_* ClickHouse")
	schema := flag.Bool("print-schema", false, "print observation table DDL and exit")
	flag.Parse()
	if *schema {
		fmt.Print(validators.AffiliationSchema)
		return nil
	}
	if *input == "" || flag.NArg() != 0 {
		return fmt.Errorf("--capture is required; --publish imports the checked records")
	}
	f, err := os.Open(*input)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	byHolder := map[string][]validators.Affiliation{}
	var expected, checks int
	for {
		var c capture
		if err = dec.Decode(&c); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if c.Inventory != nil {
			expected = c.Inventory.Checks
			continue
		}
		checks++
		if c.Error != "" {
			return fmt.Errorf("capture contains an RPC error for %s", c.Holder)
		}
		a, e := validators.InspectAffiliation(c.Body, validators.Round{Since: c.Since, AnchorMC: c.MC, AnchorRoot: c.Root, AnchorFile: c.File}, c.Holder, c.Family)
		if e != nil {
			return fmt.Errorf("holder %s MC %d: %w", c.Holder, c.MC, e)
		}
		// The capture records the original HTTP body digest before JSONL encoding.
		if b, err := hex.DecodeString(c.ResponseHash); err != nil || len(b) != 32 {
			return fmt.Errorf("missing original response digest")
		}
		a.ResponseHash = c.ResponseHash
		if a.Protocol == "" {
			continue
		}
		key := a.Holder + "/" + a.Family
		byHolder[key] = append(byHolder[key], a)
	}
	if expected == 0 || checks != expected {
		return fmt.Errorf("incomplete capture: %d of %d", checks, expected)
	}
	var records []validators.Affiliation
	for _, observations := range byHolder {
		sort.Slice(observations, func(i, j int) bool { return observations[i].MC < observations[j].MC })
		first, last := observations[0], observations[len(observations)-1]
		for _, a := range observations {
			if a.Pool != first.Pool || a.Protocol != first.Protocol || a.CodeHash != first.CodeHash || a.Borrower != first.Borrower || a.LoanSince != first.LoanSince {
				return fmt.Errorf("changing holder identity: %s", a.Holder)
			}
		}
		last.FirstMC, last.FirstResponseHash = first.MC, first.ResponseHash
		records = append(records, last)
	}
	sort.Slice(records, func(i, j int) bool {
		a, b := records[i], records[j]
		if a.Protocol != b.Protocol {
			return a.Protocol < b.Protocol
		}
		return a.Holder < b.Holder
	})
	counts := map[string]int{}
	for _, a := range records {
		counts[a.Protocol]++
	}
	if *publish {
		ch, err := gramstat.NewClickHouse(os.Getenv("GRAMSTAT_CH_URL"), os.Getenv("GRAMSTAT_CH_USER"), os.Getenv("GRAMSTAT_CH_PASSWORD"))
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := (validators.Store{CH: ch}).WriteAffiliations(ctx, records); err != nil {
			return err
		}
	}
	fmt.Printf("checked=%d verified_holders=%d protocols=%v published=%t\n", checks, len(records), counts, *publish)
	return nil
}
