package codehashes

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A real RESP connection verifies that a failed page is not transparently
// retried on another replica with a cursor that belongs to the old connection.
func TestRedisCursorConnectionIsNotReplaced(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var connections atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					args, err := readTestCommand(reader)
					if err != nil {
						return
					}
					switch strings.ToUpper(args[0]) {
					case "HELLO":
						_, _ = io.WriteString(conn, "-ERR unknown command HELLO\r\n")
					case "GET":
						_, _ = io.WriteString(conn, "$3\r\n100\r\n")
					case "EVAL_RO":
						body := testHash('+', 0) + ":1"
						_, _ = fmt.Fprintf(conn, "*2\r\n$1\r\n1\r\n$%d\r\n%s\r\n", len(body), body)
						return // Fail before the next SCAN page.
					default:
						_, _ = io.WriteString(conn, "+OK\r\n")
					}
				}
			}()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	snapshot, err := (RedisSource{Addr: listener.Addr().String()}).Collect(ctx)
	if err == nil || snapshot != nil || connections.Load() != 1 {
		t.Fatal("reconnected or published interrupted prefix", snapshot, err, connections.Load())
	}
	_ = listener.Close()
	<-done
}

func readTestCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil || len(line) < 4 || line[0] != '*' {
		return nil, io.ErrUnexpectedEOF
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	args := make([]string, n)
	for i := range args {
		line, err := reader.ReadString('\n')
		if err != nil || len(line) < 4 || line[0] != '$' {
			return nil, io.ErrUnexpectedEOF
		}
		size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || size < 0 || size > 10000 {
			return nil, fmt.Errorf("invalid test command size")
		}
		value := make([]byte, size+2)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		args[i] = string(value[:size])
	}
	return args, nil
}
