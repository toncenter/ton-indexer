package crud

import (
	"reflect"
	"testing"
)

func TestNewEnrichmentReaderWithoutBackend(t *testing.T) {
	reader, err := NewEnrichmentReader("", 1, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reader != nil {
		t.Fatalf("expected no reader, got %T", reader)
	}
}

func TestNewEnrichmentReaderPrefersKvrocks(t *testing.T) {
	store := &KvrocksStore{}
	reader, err := NewEnrichmentReader("invalid PostgreSQL DSN", 1, 0, store)
	if err != nil {
		t.Fatalf("Kvrocks selection must not initialize PostgreSQL: %v", err)
	}
	if reader != store {
		t.Fatalf("expected Kvrocks reader, got %T", reader)
	}
}

func TestParseKvrocksSentinelAddrs(t *testing.T) {
	got := ParseKvrocksSentinelAddrs(" sentinel-1:26379, ,sentinel-2:26379 ")
	want := []string{"sentinel-1:26379", "sentinel-2:26379"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected sentinel addresses: got %v, want %v", got, want)
	}
}
