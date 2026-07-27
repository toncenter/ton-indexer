package main

import "testing"

func TestIsTraceMetadataField(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want bool
	}{
		{name: "update sequence", key: "update_seq", want: true},
		{name: "trace node", key: "node_hash", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isTraceMetadataField(test.key); got != test.want {
				t.Fatalf("isTraceMetadataField(%q) = %v, want %v", test.key, got, test.want)
			}
		})
	}
}
