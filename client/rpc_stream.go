package client

import (
	"context"
	"errors"
	"io"
	"sync"

	"go-micro.dev/v5/codec"
)

type rpcStream struct {
	err      error
	request  Request
	response Response
	codec    codec.Codec
	socket   interface{ Close() error }
	context  context.Context

	closed    chan bool
	closeOnce sync.Once
	cancelOnce sync.Once

	release func(err error)
	id      string
	sync.RWMutex
	close bool

	sendEOS bool
}

func (r *rpcStream) isClosed() bool {
	select {
	case <-r.closed:
		return true
	default:
		return false
	}
}

func (r *rpcStream) startCtxWatcher() {
	go func() {
		select {
		case <-r.context.Done():
			r.cancelOnce.Do(func() {
				r.Lock()
				r.err = r.context.Err()
				r.Unlock()
				if r.socket != nil {
					_ = r.socket.Close()
				}
			})
		case <-r.closed:
		}
	}()
}

func (r *rpcStream) Context() context.Context {
	return r.context
}

func (r *rpcStream) Request() Request {
	return r.request
}

func (r *rpcStream) Response() Response {
	return r.response
}

func (r *rpcStream) Send(msg interface{}) error {
	r.RLock()
	if r.isClosed() {
		r.RUnlock()
		r.Lock()
		r.err = errShutdown
		r.Unlock()
		return errShutdown
	}
	select {
	case <-r.context.Done():
		r.RUnlock()
		r.Lock()
		r.err = r.context.Err()
		r.Unlock()
		return r.err
	default:
	}

	req := codec.Message{
		Id:       r.id,
		Target:   r.request.Service(),
		Method:   r.request.Method(),
		Endpoint: r.request.Endpoint(),
		Type:     codec.Request,
	}
	r.RUnlock()

	err := r.codec.Write(&req, msg)

	if err != nil {
		r.Lock()
		r.err = err
		r.Unlock()
		return err
	}

	return nil
}

func (r *rpcStream) Recv(msg interface{}) error {
	r.RLock()
	if r.isClosed() {
		r.RUnlock()
		r.Lock()
		r.err = errShutdown
		r.Unlock()
		return errShutdown
	}
	select {
	case <-r.context.Done():
		r.RUnlock()
		r.Lock()
		r.err = r.context.Err()
		r.Unlock()
		return r.err
	default:
	}
	r.RUnlock()

	var resp codec.Message

	err := r.codec.ReadHeader(&resp, codec.Response)
	if err != nil {
		r.Lock()
		if errors.Is(err, io.EOF) && !r.isClosed() {
			r.err = io.ErrUnexpectedEOF
			r.Unlock()
			return io.ErrUnexpectedEOF
		}
		r.err = err
		r.Unlock()
		return err
	}

	switch {
	case len(resp.Error) > 0:
		if resp.Error != lastStreamResponseError {
			r.Lock()
			r.err = serverError(resp.Error)
			r.Unlock()
		} else {
			r.Lock()
			r.err = io.EOF
			r.Unlock()
		}
		err = r.codec.ReadBody(nil)
		if err != nil {
			r.Lock()
			r.err = err
			r.Unlock()
		}
	default:
		err = r.codec.ReadBody(msg)
		if err != nil {
			r.Lock()
			r.err = err
			r.Unlock()
		}
	}

	r.RLock()
	rerr := r.err
	r.RUnlock()
	return rerr
}

func (r *rpcStream) Error() error {
	r.RLock()
	defer r.RUnlock()

	return r.err
}

func (r *rpcStream) CloseSend() error {
	return errors.New("streamer not implemented")
}

func (r *rpcStream) Close() error {
	var closeErr error
	r.closeOnce.Do(func() {
		r.Lock()
		close(r.closed)
		r.Unlock()

		if r.sendEOS {
			r.codec.Write(&codec.Message{
				Id:       r.id,
				Target:   r.request.Service(),
				Method:   r.request.Method(),
				Endpoint: r.request.Endpoint(),
				Type:     codec.Error,
				Error:    lastStreamResponseError,
			}, nil)
		}

		closeErr = r.codec.Close()

		rerr := r.Error()
		if r.close && rerr == nil {
			rerr = errors.New("connection header set to close")
		}
		r.release(rerr)
	})
	return closeErr
}
