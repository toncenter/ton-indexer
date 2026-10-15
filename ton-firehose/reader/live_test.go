package reader

import (
	"bufio"
	"os"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestLivePollerPrintsFireProtocol(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	// The FIRE handler prints to os.Stdout.
	stdout := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = stdout }()

	cfg := LiveConfig{
		StateDir:              t.TempDir(),
		FirstStreamableBlock:  1,
		FetchBatchSize:        4,
		PollInterval:          time.Millisecond,
		MaxBlockFetchDuration: time.Second,
	}
	poller := newLivePoller(&fakeSource{head: 1000}, cfg, zap.NewNop())

	stop := uint64(11) // exclusive
	done := make(chan error, 1)
	go func() {
		done <- poller.Run(cfg.FirstStreamableBlock, &stop, cfg.FetchBatchSize)
		w.Close()
	}()

	var lines []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if len(lines) < 2 || lines[0] != "FIRE INIT 3.0 sf.ton.type.v1.Block" {
		t.Fatalf("unexpected output start: %q", lines)
	}
	// FIRE BLOCK <num> <id> <parent_num> <parent_id> <lib_num> <timestamp_ns> <payload>
	fields := strings.Fields(lines[1])
	if len(fields) != 9 || fields[2] != "1" || fields[3] != fakeId(1) || fields[6] != "1" {
		t.Errorf("unexpected first block line %q", lines[1])
	}
	if got := len(lines) - 1; got != 10 {
		t.Errorf("printed %d blocks, want 1..10", got)
	}
}
