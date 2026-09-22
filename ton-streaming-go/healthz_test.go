package main

import "testing"

func TestFinalizedProducerHealthDoesNotRequireConfirmedBlocks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values map[string]string
		ok     bool
	}{
		{"finalized", map[string]string{"mode": "finalized", "finalized_mc_block_time": "995"}, true},
		{"stale finalized", map[string]string{"mode": "finalized", "finalized_mc_block_time": "980"}, false},
		{"legacy missing confirmed", map[string]string{"finalized_mc_block_time": "995"}, false},
		{"legacy healthy", map[string]string{"finalized_mc_block_time": "995", "confirmed_block_time": "995"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := componentHealth{OK: true}
			applyEmulatorStatus(&status, tc.values, 1000)
			if status.OK != tc.ok {
				t.Fatalf("health=%+v; want OK=%v", status, tc.ok)
			}
		})
	}
}
