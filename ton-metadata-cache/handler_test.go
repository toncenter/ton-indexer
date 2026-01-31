package main

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"ton-metadata-cache/cache"
	"ton-metadata-cache/repl"
)

func setupTestHandler(t *testing.T) (*Handler, *miniredis.Miniredis) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	cacheManager := cache.NewManager(client)
	handler := NewHandler(cacheManager, nil) // nil db pool for tests that don't need DB

	return handler, mr
}

func TestDnsEntries_OwnerChangeCleanup(t *testing.T) {
	h, mr := setupTestHandler(t)
	defer mr.Close()
	ctx := context.Background()

	// Step 1: INSERT a self-resolving DNS entry (owner=wallet)
	insertEvent := repl.ChangeEvent{
		Table:     "public.dns_entries",
		Operation: repl.Insert,
		Data: map[string]interface{}{
			"nft_item_address": "NFT_ADDR_1",
			"nft_item_owner":   "ALICE",
			"dns_wallet":       "ALICE",
			"domain":           "alice.ton",
		},
	}

	if err := h.handleEvent(ctx, insertEvent); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Verify: ALICE should have domain "alice.ton"
	domain := h.GetShortestDomain(ctx, "ALICE")
	if domain != "alice.ton" {
		t.Errorf("expected domain 'alice.ton', got '%s'", domain)
	}

	// Step 2: UPDATE - transfer ownership from ALICE to BOB
	updateEvent := repl.ChangeEvent{
		Table:     "public.dns_entries",
		Operation: repl.Update,
		Data: map[string]interface{}{
			"nft_item_address": "NFT_ADDR_1",
			"nft_item_owner":   "BOB",
			"dns_wallet":       "BOB",
			"domain":           "alice.ton",
		},
	}

	if err := h.handleEvent(ctx, updateEvent); err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Verify: ALICE should have no domain now
	domain = h.GetShortestDomain(ctx, "ALICE")
	if domain != "" {
		t.Errorf("expected ALICE to have no domain after transfer, got '%s'", domain)
	}

	// Verify: BOB should now have the domain
	domain = h.GetShortestDomain(ctx, "BOB")
	if domain != "alice.ton" {
		t.Errorf("expected BOB's domain 'alice.ton', got '%s'", domain)
	}
}

func TestDnsEntries_SelfResolvingOnly(t *testing.T) {
	h, mr := setupTestHandler(t)
	defer mr.Close()
	ctx := context.Background()

	// INSERT a DNS entry where owner != wallet (not self-resolving)
	insertEvent := repl.ChangeEvent{
		Table:     "public.dns_entries",
		Operation: repl.Insert,
		Data: map[string]interface{}{
			"nft_item_address": "NFT_ADDR_2",
			"nft_item_owner":   "ALICE",
			"dns_wallet":       "CHARLIE", // Different from owner
			"domain":           "test.ton",
		},
	}

	if err := h.handleEvent(ctx, insertEvent); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Verify: ALICE should NOT have a domain (not self-resolving)
	domain := h.GetShortestDomain(ctx, "ALICE")
	if domain != "" {
		t.Errorf("expected no domain for ALICE when owner != wallet, got '%s'", domain)
	}

	// Verify: DnsEntries cache should still have the entry
	entry, err := h.cache.DnsEntries.Get(ctx, "NFT_ADDR_2")
	if err != nil {
		t.Fatalf("expected DNS entry to exist: %v", err)
	}
	if *entry.Domain != "test.ton" {
		t.Errorf("expected domain 'test.ton', got '%s'", *entry.Domain)
	}
}

func TestDnsEntries_ShortestDomainPreference(t *testing.T) {
	h, mr := setupTestHandler(t)
	defer mr.Close()
	ctx := context.Background()

	// INSERT first domain for ALICE: "alice.ton" (10 chars)
	event1 := repl.ChangeEvent{
		Table:     "public.dns_entries",
		Operation: repl.Insert,
		Data: map[string]interface{}{
			"nft_item_address": "NFT_ADDR_1",
			"nft_item_owner":   "ALICE",
			"dns_wallet":       "ALICE",
			"domain":           "alice.ton",
		},
	}
	if err := h.handleEvent(ctx, event1); err != nil {
		t.Fatalf("INSERT 1 failed: %v", err)
	}

	// INSERT second domain for ALICE: "a.ton" (5 chars - shorter)
	event2 := repl.ChangeEvent{
		Table:     "public.dns_entries",
		Operation: repl.Insert,
		Data: map[string]interface{}{
			"nft_item_address": "NFT_ADDR_2",
			"nft_item_owner":   "ALICE",
			"dns_wallet":       "ALICE",
			"domain":           "a.ton",
		},
	}
	if err := h.handleEvent(ctx, event2); err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
	}

	// Verify: ALICE should have the shorter domain
	domain := h.GetShortestDomain(ctx, "ALICE")
	if domain != "a.ton" {
		t.Errorf("expected shortest domain 'a.ton', got '%s'", domain)
	}

	// INSERT third domain for ALICE: "verylongalice.ton" (longer)
	event3 := repl.ChangeEvent{
		Table:     "public.dns_entries",
		Operation: repl.Insert,
		Data: map[string]interface{}{
			"nft_item_address": "NFT_ADDR_3",
			"nft_item_owner":   "ALICE",
			"dns_wallet":       "ALICE",
			"domain":           "verylongalice.ton",
		},
	}
	if err := h.handleEvent(ctx, event3); err != nil {
		t.Fatalf("INSERT 3 failed: %v", err)
	}

	// Verify: ALICE should still have the shortest domain
	domain = h.GetShortestDomain(ctx, "ALICE")
	if domain != "a.ton" {
		t.Errorf("expected domain to remain 'a.ton', got '%s'", domain)
	}
}

func TestDnsEntries_OwnerBecomesNonSelfResolving(t *testing.T) {
	h, mr := setupTestHandler(t)
	defer mr.Close()
	ctx := context.Background()

	// INSERT self-resolving entry
	insertEvent := repl.ChangeEvent{
		Table:     "public.dns_entries",
		Operation: repl.Insert,
		Data: map[string]interface{}{
			"nft_item_address": "NFT_ADDR_1",
			"nft_item_owner":   "ALICE",
			"dns_wallet":       "ALICE",
			"domain":           "alice.ton",
		},
	}
	if err := h.handleEvent(ctx, insertEvent); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Verify mapping exists
	domain := h.GetShortestDomain(ctx, "ALICE")
	if domain != "alice.ton" {
		t.Fatalf("expected domain 'alice.ton', got '%s'", domain)
	}

	// UPDATE: change wallet to someone else (no longer self-resolving)
	updateEvent := repl.ChangeEvent{
		Table:     "public.dns_entries",
		Operation: repl.Update,
		Data: map[string]interface{}{
			"nft_item_address": "NFT_ADDR_1",
			"nft_item_owner":   "ALICE",
			"dns_wallet":       "CHARLIE", // Now points to different wallet
			"domain":           "alice.ton",
		},
	}
	if err := h.handleEvent(ctx, updateEvent); err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Verify: ALICE should have no domain since entry is no longer self-resolving
	domain = h.GetShortestDomain(ctx, "ALICE")
	if domain != "" {
		t.Errorf("expected no domain when entry becomes non-self-resolving, got '%s'", domain)
	}
}

func TestDnsEntries_TransferShortestKeepsBackup(t *testing.T) {
	h, mr := setupTestHandler(t)
	defer mr.Close()
	ctx := context.Background()

	// ALICE has two domains: "a.ton" (shortest) and "alice.ton"
	event1 := repl.ChangeEvent{
		Table:     "public.dns_entries",
		Operation: repl.Insert,
		Data: map[string]interface{}{
			"nft_item_address": "NFT_ADDR_1",
			"nft_item_owner":   "ALICE",
			"dns_wallet":       "ALICE",
			"domain":           "a.ton",
		},
	}
	event2 := repl.ChangeEvent{
		Table:     "public.dns_entries",
		Operation: repl.Insert,
		Data: map[string]interface{}{
			"nft_item_address": "NFT_ADDR_2",
			"nft_item_owner":   "ALICE",
			"dns_wallet":       "ALICE",
			"domain":           "alice.ton",
		},
	}
	h.handleEvent(ctx, event1)
	h.handleEvent(ctx, event2)

	// Verify ALICE has shortest domain
	domain := h.GetShortestDomain(ctx, "ALICE")
	if domain != "a.ton" {
		t.Fatalf("expected 'a.ton', got '%s'", domain)
	}

	// Transfer "a.ton" to BOB
	transferEvent := repl.ChangeEvent{
		Table:     "public.dns_entries",
		Operation: repl.Update,
		Data: map[string]interface{}{
			"nft_item_address": "NFT_ADDR_1",
			"nft_item_owner":   "BOB",
			"dns_wallet":       "BOB",
			"domain":           "a.ton",
		},
	}
	if err := h.handleEvent(ctx, transferEvent); err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Verify: BOB now has "a.ton"
	domain = h.GetShortestDomain(ctx, "BOB")
	if domain != "a.ton" {
		t.Errorf("expected BOB to have 'a.ton', got '%s'", domain)
	}

	// KEY TEST: ALICE should now show "alice.ton" (her remaining domain)
	domain = h.GetShortestDomain(ctx, "ALICE")
	if domain != "alice.ton" {
		t.Errorf("expected ALICE to fall back to 'alice.ton', got '%s'", domain)
	}
}
