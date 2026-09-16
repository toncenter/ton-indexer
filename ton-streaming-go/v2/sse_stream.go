package v2

import (
	"bufio"
	"io"

	"github.com/valyala/fasthttp/fasthttputil"
)

// sseStream connects fasthttp's response lifecycle to client cleanup. A network
// write failure closes this reader, releasing the quota and stopping the writer
// without waiting for another keepalive tick.
type sseStream struct {
	io.ReadCloser
	onClose func()
}

func (s *sseStream) Close() error {
	// Unblock a writer in Flush before cleanup, which can wait for the manager.
	err := s.ReadCloser.Close()
	s.onClose()
	return err
}

func newSSEStream(manager *ClientManager, client *Client, write func(*bufio.Writer)) *sseStream {
	// Preserve the buffered pipe used by fasthttp.NewStreamReader, while
	// initializing the transport closer before starting the writer goroutine.
	pipe := fasthttputil.NewPipeConns()
	reader, writer := pipe.Conn2(), pipe.Conn1()
	client.closeTransport = reader.Close
	stream := &sseStream{
		ReadCloser: reader,
		onClose:    func() { disconnectClient(manager, client) },
	}
	go func() {
		defer writer.Close()
		defer stream.onClose()
		buffer := bufio.NewWriter(writer)
		write(buffer)
		_ = buffer.Flush()
	}()
	return stream
}
