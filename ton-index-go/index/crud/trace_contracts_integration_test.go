package crud

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

//go:embed testdata/trace_contracts.sql
var traceContractsSchema string

// Opt-in: starts isolated local servers, never connects to an existing database.
// Redis exercises the actual KvrocksStore payload/lookup path, not RocksDB itself.
func TestTraceContractsIntegration(t *testing.T) {
	if os.Getenv("TON_INDEX_TRACE_INTEGRATION") != "1" {
		t.Skip("set TON_INDEX_TRACE_INTEGRATION=1; requires initdb, pg_ctl, and redis-server")
	}
	for _, tool := range []string{"initdb", "pg_ctl", "redis-server"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatal(err)
		}
	}
	run := func(name string, args ...string) {
		t.Helper()
		if out, err := exec.Command(name, args...).CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", name, err, out)
		}
	}
	port := func() string {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		return fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
	}
	pgDir := filepath.Join(t.TempDir(), "postgres")
	pgPort := port()
	run("initdb", "-D", pgDir, "--auth=trust", "--username=postgres", "--no-locale", "--no-sync")
	run("pg_ctl", "-D", pgDir, "-l", filepath.Join(pgDir, "test.log"), "-w", "start", "-o",
		"-h 127.0.0.1 -p "+pgPort+" -c unix_socket_directories='' -c fsync=off")
	t.Cleanup(func() { run("pg_ctl", "-D", pgDir, "-m", "immediate", "-w", "stop") })
	ctx := context.Background()
	config, err := pgxpool.ParseConfig("postgres://postgres@127.0.0.1:" + pgPort + "/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	// Temporary SQL objects remain on this one connection across public CRUD calls.
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	execute := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	execute(traceContractsSchema)
	account := models.AccountAddress("0:" + strings.Repeat("1", 64))
	traceID := models.MustParseHashType(strings.Repeat("2", 64))
	beforeHash := models.MustParseHashType(strings.Repeat("3", 64))
	afterHash := models.MustParseHashType(strings.Repeat("4", 64))
	vestingHash := models.MustParseHashType(vestingCodeHash)
	const latestCodeHash = "kUmuUcHkaJcQzr94MCl7Fqz7rbNjqSClN4k+f/7sp2g="
	execute(`INSERT INTO traces VALUES ($1, $1, 1, 1, 1, 1, 1, 1, 'complete', 0, 1, 0, 'ok')`, traceID)
	execute(`INSERT INTO transactions (account, hash, lt, block_workchain, block_shard, block_seqno,
		mc_block_seqno, trace_id, prev_trans_hash, prev_trans_lt, now, orig_status, end_status,
		total_fees, total_fees_extra_currencies, account_state_hash_before, account_state_hash_after, descr)
		VALUES ($1, $2, 1, 0, 0, 1, 1, $2, $2, 0, 1, 'active', 'active', 0, '{}', $3, $4, 'ord')`,
		account, traceID, beforeHash, afterHash)
	execute(`INSERT INTO account_states VALUES ($1, $2, '1', '{}', 'active', NULL, NULL, $3),
		($4, $2, '1', '{}', 'active', NULL, NULL, $5)`, beforeHash, account, vestingHash, afterHash, walletCodeHash)
	execute(`INSERT INTO latest_account_states VALUES ($1, $2)`, account, latestCodeHash)
	settings := models.RequestSettings{Timeout: 5 * time.Second, DefaultLimit: 10, MaxLimit: 10, NoMetadata: true}
	req := models.TracesRequest{TraceId: []models.HashType{traceID}}
	db := &DbClient{Pool: pool}
	check := func(t *testing.T, traces []models.Trace, book models.AddressBook, metadata models.Metadata) {
		t.Helper()
		// This is the exact response wrapper serialized by GetTraces/GetPendingTraces.
		raw, err := json.Marshal(models.TracesResponse{Traces: traces, AddressBook: book, Metadata: metadata})
		if err != nil {
			t.Fatal(err)
		}
		var response models.TracesResponse
		if err := json.Unmarshal(raw, &response); err != nil {
			t.Fatal(err)
		}
		if len(response.Traces) != 1 || response.Traces[0].ContractInfo == nil {
			t.Fatalf("contract_info missing from route response: %s", raw)
		}
		trace := response.Traces[0]
		info := trace.ContractInfo
		wantHashes := []models.HashType{vestingHash, walletCodeHash}
		slices.Sort(wantHashes)
		if !slices.Equal(info.Accounts[account], wantHashes) || len(info.ByCodeHash) != 2 {
			t.Fatalf("account-to-version lookup lost history: %+v", info)
		}
		if wallet := info.ByCodeHash[walletCodeHash]; wallet == nil || !slices.Contains(wallet.Interfaces, "wallet_v5r1") {
			t.Fatalf("base64 interface lookup failed: %+v", wallet)
		}
		if vesting := info.ByCodeHash[vestingHash]; vesting == nil || !slices.ContainsFunc(vesting.Candidates, func(c models.ContractCandidate) bool {
			return c.ID == "Jetton Vesting.JettonVesting"
		}) {
			t.Fatalf("historical catalog lookup failed: %+v", vesting)
		}
		if info.ByCodeHash[latestCodeHash] != nil || book[account].Interfaces == nil || !slices.Contains(*book[account].Interfaces, "tg_wallet") {
			t.Fatal("latest-account interfaces and historical contract metadata were mixed")
		}
		if trace.Trace == nil || len(trace.TransactionsOrder) != 1 || trace.Trace.TransactionHash != trace.TransactionsOrder[0] {
			t.Fatal("trace assembly lost its transaction link")
		}
		tx := trace.Transactions[trace.TransactionsOrder[0]]
		if tx == nil || tx.AccountStateBefore.ContractInfoKey == nil || *tx.AccountStateBefore.ContractInfoKey != vestingHash ||
			tx.AccountStateAfter.ContractInfoKey == nil || *tx.AccountStateAfter.ContractInfoKey != walletCodeHash {
			t.Fatal("per-state links do not match historical before/after codes")
		}
	}
	for _, backend := range []string{"postgres", "kvrocks"} {
		t.Run(backend, func(t *testing.T) {
			if backend == "kvrocks" {
				redisPort := port()
				server := exec.Command("redis-server", "--bind", "127.0.0.1", "--port", redisPort, "--save", "", "--appendonly", "no")
				if err := server.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = server.Process.Signal(os.Interrupt); _ = server.Wait() })
				var store *KvrocksStore
				var err error
				for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
					store, err = NewKvrocksStore(KvrocksConfig{Addr: "127.0.0.1:" + redisPort})
					if err == nil {
						break
					}
					time.Sleep(50 * time.Millisecond)
				}
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close(); db.Kvrocks = nil })
				db.Kvrocks = store
				for hash, code := range map[models.HashType]string{beforeHash: vestingCodeHash, afterHash: walletCodeHash} {
					payload, err := json.Marshal(map[string]any{"hash": hash, "account": account, "code_hash": code, "balance": "1"})
					if err != nil {
						t.Fatal(err)
					}
					if err := store.client.Set(ctx, store.payloadKey("account_states", string(hash)), payload, 0).Err(); err != nil {
						t.Fatal(err)
					}
				}
				payload, _ := json.Marshal(map[string]any{"code_hash": latestCodeHash})
				if err := store.client.Set(ctx, store.payloadKey("latest_account_states", string(account)), payload, 0).Err(); err != nil {
					t.Fatal(err)
				}
				// Prove that the Kvrocks path does not get its states from PostgreSQL.
				execute(`ALTER TABLE account_states RENAME TO unavailable_account_states`)
			}
			traces, book, metadata, err := db.QueryTraces(req, settings)
			if err != nil {
				t.Fatal(err)
			}
			check(t, traces, book, metadata)
			conn, err := pool.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			legacy, _, err := queryTracesImpl(buildTracesOffsetQuery(req, "desc", 0, 10, false), false, nil, conn, settings, db.Kvrocks)
			conn.Release()
			if err != nil {
				t.Fatal(err)
			}
			check(t, legacy, book, metadata)
		})
	}
	t.Run("pending", func(t *testing.T) {
		pending := NewEmptyContext(true)
		externalHash := models.MustParseHashType(strings.Repeat("5", 64))
		pending.emulatedTraces["pending"] = &models.Trace{ExternalHash: &externalHash, TraceMeta: models.TraceMeta{TraceState: "pending"}}
		tx := &models.Transaction{Account: account, Hash: traceID, Emulated: true,
			AccountStateBefore: traceState(vestingCodeHash), AccountStateAfter: traceState(walletCodeHash)}
		pending.emulatedTransactions["pending"] = []*models.Transaction{tx}
		pending.txHashTraceExternalHash[string(traceID)] = string(externalHash)
		traces, book, metadata, err := db.QueryPendingTraces(settings, pending, models.PendingTracesRequest{})
		if err != nil {
			t.Fatal(err)
		}
		check(t, traces, book, metadata)
		if tx.AccountStateAfter.ContractInfoKey != nil || tx.AccountStateBefore.ContractInfoKey != nil {
			t.Fatal("pending response polluted shared context states")
		}
		txs, err := QueryPendingTransactionsImpl(pending, nil, settings, false)
		if err != nil || txs[0].AccountStateAfter.ContractInfoKey != nil {
			t.Fatal("a subsequent non-trace pending response retained a trace-only link")
		}
	})
}
