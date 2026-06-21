package client

import (
	"context"
	"io"
	stderrors "errors"
	"sync"
	"testing"
	"time"

	"go-micro.dev/v5/codec"
)

type blockingCodec struct {
	mu       sync.Mutex
	sendDone chan struct{}
	recvDone chan struct{}
	sendWait chan struct{}
	recvWait chan struct{}
	socket   *mockSocket
	closed   bool
}

type mockSocket struct {
	mu      sync.Mutex
	closed  bool
	closedC chan struct{}
}

func (s *mockSocket) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.closedC)
	}
	return nil
}

func newMockSocket() *mockSocket {
	return &mockSocket{closedC: make(chan struct{})}
}

func newBlockingCodec() *blockingCodec {
	return &blockingCodec{
		sendDone: make(chan struct{}, 1),
		recvDone: make(chan struct{}, 1),
		sendWait: make(chan struct{}),
		recvWait: make(chan struct{}),
	}
}

func (c *blockingCodec) ReadHeader(m *codec.Message, t codec.MessageType) error {
	c.mu.Lock()
	s := c.socket
	c.mu.Unlock()
	if s == nil {
		select {
		case <-c.recvWait:
			return io.ErrUnexpectedEOF
		case <-time.After(5 * time.Second):
			return stderrors.New("read header timeout")
		}
	}
	select {
	case <-c.recvWait:
		return io.ErrUnexpectedEOF
	case <-s.closedC:
		return io.ErrUnexpectedEOF
	case <-time.After(5 * time.Second):
		return stderrors.New("read header timeout")
	}
}

func (c *blockingCodec) ReadBody(b interface{}) error {
	c.mu.Lock()
	s := c.socket
	c.mu.Unlock()
	if s == nil {
		return nil
	}
	select {
	case <-s.closedC:
		return io.ErrUnexpectedEOF
	default:
		return nil
	}
}

func (c *blockingCodec) Write(m *codec.Message, b interface{}) error {
	c.mu.Lock()
	s := c.socket
	c.mu.Unlock()
	if s == nil {
		select {
		case <-c.sendWait:
			c.sendDone <- struct{}{}
			return io.ErrClosedPipe
		case <-time.After(5 * time.Second):
			return stderrors.New("write timeout")
		}
	}
	select {
	case <-c.sendWait:
		c.sendDone <- struct{}{}
		return io.ErrClosedPipe
	case <-s.closedC:
		c.sendDone <- struct{}{}
		return io.ErrClosedPipe
	case <-time.After(5 * time.Second):
		return stderrors.New("write timeout")
	}
}

func (c *blockingCodec) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *blockingCodec) String() string { return "blocking" }

type noopResponse struct{}

func (r *noopResponse) Codec() codec.Reader      { return nil }
func (r *noopResponse) Header() map[string]string { return nil }
func (r *noopResponse) Read() ([]byte, error)     { return nil, nil }
func (r *noopResponse) WriteHeader(h map[string]string) {}
func (r *noopResponse) Write(b []byte) error { return nil }

type noopRequest struct{}

func (r *noopRequest) Service() string              { return "noop" }
func (r *noopRequest) Method() string               { return "noop" }
func (r *noopRequest) Endpoint() string             { return "noop" }
func (r *noopRequest) ContentType() string          { return "application/protobuf" }
func (r *noopRequest) Header() map[string]string    { return nil }
func (r *noopRequest) Body() interface{}            { return nil }
func (r *noopRequest) Read() ([]byte, error)        { return nil, nil }
func (r *noopRequest) Codec() codec.Writer          { return nil }
func (r *noopRequest) Stream() bool                 { return false }

func TestStreamCtxCancelInterruptsSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cdc := newBlockingCodec()
	sock := newMockSocket()
	cdc.socket = sock

	s := &rpcStream{
		id:       "1",
		context:  ctx,
		request:  &noopRequest{},
		response: &noopResponse{},
		codec:    cdc,
		socket:   sock,
		closed:   make(chan bool),
		release:  func(err error) {},
		sendEOS:  false,
	}
	s.startCtxWatcher()

	sendErr := make(chan error, 1)
	go func() {
		sendErr <- s.Send(&struct{}{})
	}()

	time.Sleep(20 * time.Millisecond)

	cancel()

	select {
	case err := <-sendErr:
		if err == nil {
			t.Fatal("expected Send to return error after ctx cancel, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Send did not return promptly after ctx cancel")
	}

	sock.mu.Lock()
	isClosed := sock.closed
	sock.mu.Unlock()
	if !isClosed {
		t.Fatal("expected underlying socket to be closed after ctx cancel")
	}

	_ = s.Close()
}

func TestStreamCtxCancelInterruptsRecv(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cdc := newBlockingCodec()
	sock := newMockSocket()
	cdc.socket = sock

	s := &rpcStream{
		id:       "2",
		context:  ctx,
		request:  &noopRequest{},
		response: &noopResponse{},
		codec:    cdc,
		socket:   sock,
		closed:   make(chan bool),
		release:  func(err error) {},
		sendEOS:  false,
	}
	s.startCtxWatcher()

	recvErr := make(chan error, 1)
	go func() {
		recvErr <- s.Recv(&struct{}{})
	}()

	time.Sleep(20 * time.Millisecond)

	cancel()

	select {
	case err := <-recvErr:
		if err == nil {
			t.Fatal("expected Recv to return error after ctx cancel, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Recv did not return promptly after ctx cancel")
	}

	sock.mu.Lock()
	isClosed := sock.closed
	sock.mu.Unlock()
	if !isClosed {
		t.Fatal("expected underlying socket to be closed after ctx cancel")
	}

	_ = s.Close()
}

func TestStreamCloseIdempotent(t *testing.T) {
	ctx := context.Background()
	cdc := newBlockingCodec()
	sock := newMockSocket()
	cdc.socket = sock

	s := &rpcStream{
		id:       "3",
		context:  ctx,
		request:  &noopRequest{},
		response: &noopResponse{},
		codec:    cdc,
		socket:   sock,
		closed:   make(chan bool),
		release:  func(err error) {},
		sendEOS:  false,
	}

	err1 := s.Close()
	err2 := s.Close()
	if err1 != nil || err2 != nil {
		t.Fatalf("Close should be idempotent, got err1=%v err2=%v", err1, err2)
	}
}
