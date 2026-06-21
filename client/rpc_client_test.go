package client

import (
	"context"
	stderrors "errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	merrors "go-micro.dev/v5/errors"
	"go-micro.dev/v5/registry"
	"go-micro.dev/v5/selector"
)

const (
	serviceName     = "test.service"
	serviceEndpoint = "Test.Endpoint"
)

func newTestRegistry() registry.Registry {
	return registry.NewMemoryRegistry(registry.Services(testData))
}

func TestCallAddress(t *testing.T) {
	var called bool
	service := serviceName
	endpoint := serviceEndpoint
	address := "10.1.10.1:8080"

	wrap := func(cf CallFunc) CallFunc {
		return func(_ context.Context, node *registry.Node, req Request, _ interface{}, _ CallOptions) error {
			called = true

			if req.Service() != service {
				return fmt.Errorf("expected service: %s got %s", service, req.Service())
			}

			if req.Endpoint() != endpoint {
				return fmt.Errorf("expected service: %s got %s", endpoint, req.Endpoint())
			}

			if node.Address != address {
				return fmt.Errorf("expected address: %s got %s", address, node.Address)
			}

			// don't do the call
			return nil
		}
	}

	r := newTestRegistry()
	c := NewClient(
		Registry(r),
		WrapCall(wrap),
	)

	if err := c.Options().Selector.Init(selector.Registry(r)); err != nil {
		t.Fatal("failed to initialize selector", err)
	}

	req := c.NewRequest(service, endpoint, nil)

	// test calling remote address
	if err := c.Call(context.Background(), req, nil, WithAddress(address)); err != nil {
		t.Fatal("call with address error", err)
	}

	if !called {
		t.Fatal("wrapper not called")
	}
}

func TestCallRetry(t *testing.T) {
	service := "test.service"
	endpoint := "Test.Endpoint"
	address := "10.1.10.1"

	var called int

	wrap := func(cf CallFunc) CallFunc {
		return func(_ context.Context, _ *registry.Node, _ Request, _ interface{}, _ CallOptions) error {
			called++
			if called == 1 {
				return merrors.InternalServerError("test.error", "retry request")
			}
			// don't do the call
			return nil
		}
	}

	r := newTestRegistry()
	c := NewClient(
		Registry(r),
		WrapCall(wrap),
		Retry(RetryAlways),
		Retries(1),
	)

	if err := c.Options().Selector.Init(selector.Registry(r)); err != nil {
		t.Fatal("failed to initialize selector", err)
	}

	req := c.NewRequest(service, endpoint, nil)

	// test calling remote address
	if err := c.Call(context.Background(), req, nil, WithAddress(address)); err != nil {
		t.Fatal("call with address error", err)
	}

	// num calls
	if called < c.Options().CallOptions.Retries+1 {
		t.Fatal("request not retried")
	}
}

func TestCallWrapper(t *testing.T) {
	var called bool
	id := "test.1"
	service := "test.service"
	endpoint := "Test.Endpoint"
	address := "10.1.10.1:8080"

	wrap := func(cf CallFunc) CallFunc {
		return func(_ context.Context, node *registry.Node, req Request, _ interface{}, _ CallOptions) error {
			called = true

			if req.Service() != service {
				return fmt.Errorf("expected service: %s got %s", service, req.Service())
			}

			if req.Endpoint() != endpoint {
				return fmt.Errorf("expected service: %s got %s", endpoint, req.Endpoint())
			}

			if node.Address != address {
				return fmt.Errorf("expected address: %s got %s", address, node.Address)
			}

			// don't do the call
			return nil
		}
	}

	r := newTestRegistry()
	c := NewClient(
		Registry(r),
		WrapCall(wrap),
	)

	if err := c.Options().Selector.Init(selector.Registry(r)); err != nil {
		t.Fatal("failed to initialize selector", err)
	}

	err := r.Register(&registry.Service{
		Name:    service,
		Version: "latest",
		Nodes: []*registry.Node{
			{
				Id:      id,
				Address: address,
				Metadata: map[string]string{
					"protocol": "mucp",
				},
			},
		},
	})
	if err != nil {
		t.Fatal("failed to register service", err)
	}

	req := c.NewRequest(service, endpoint, nil)
	if err := c.Call(context.Background(), req, nil); err != nil {
		t.Fatal("call wrapper error", err)
	}

	if !called {
		t.Fatal("wrapper not called")
	}
}

func TestCallCtxCancelNoExtraCalls(t *testing.T) {
	service := "test.service.cancel"
	endpoint := "Test.Endpoint"

	address := "10.1.10.1:8080"
	r := newTestRegistry()
	err := r.Register(&registry.Service{
		Name:    service,
		Version: "latest",
		Nodes: []*registry.Node{
			{Id: service + "-1", Address: address},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var callCount int32
	release := make(chan struct{})
	var mu sync.Mutex

	wrap := func(cf CallFunc) CallFunc {
		return func(ctx context.Context, _ *registry.Node, _ Request, _ interface{}, _ CallOptions) error {
			atomic.AddInt32(&callCount, 1)
			mu.Lock()
			releaseCh := release
			mu.Unlock()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseCh:
				return nil
			case <-time.After(10 * time.Second):
				return stderrors.New("timeout waiting in wrapper")
			}
		}
	}

	c := NewClient(
		Registry(r),
		WrapCall(wrap),
		Retries(3),
		Retry(RetryAlways),
		RequestTimeout(200*time.Millisecond),
	)
	if err := c.Options().Selector.Init(selector.Registry(r)); err != nil {
		t.Fatal(err)
	}

	beforeGoroutines := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())

	req := c.NewRequest(service, endpoint, nil)

	done := make(chan error, 1)
	go func() {
		done <- c.Call(ctx, req, nil)
	}()

	time.Sleep(20 * time.Millisecond)

	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected Call to return error after cancel, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Call did not return within reasonable time after cancel")
	}

	mu.Lock()
	close(release)
	mu.Unlock()

	time.Sleep(200 * time.Millisecond)

	afterGoroutines := runtime.NumGoroutine()
	leaked := afterGoroutines - beforeGoroutines
	if leaked > 2 {
		t.Errorf("too many leaked goroutines: before=%d after=%d leaked=%d",
			beforeGoroutines, afterGoroutines, leaked)
	}
}

func TestCallConnectionTimeoutZeroNotOverridden(t *testing.T) {
	service := "test.service.timeout"
	endpoint := "Test.Endpoint"
	address := "10.1.10.1:8081"
	r := newTestRegistry()
	err := r.Register(&registry.Service{
		Name:    service,
		Version: "latest",
		Nodes: []*registry.Node{
			{Id: service + "-1", Address: address},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var timeoutHeader string
	var mu sync.Mutex

	wrap := func(cf CallFunc) CallFunc {
		return func(ctx context.Context, node *registry.Node, req Request, resp interface{}, opts CallOptions) error {
			mu.Lock()
			if node != nil && req != nil {
				timeoutHeader = fmt.Sprintf("%v", opts.ConnectionTimeout)
			}
			mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			return nil
		}
	}

	c := NewClient(
		Registry(r),
		WrapCall(wrap),
	)
	if err := c.Options().Selector.Init(selector.Registry(r)); err != nil {
		t.Fatal(err)
	}

	req := c.NewRequest(service, endpoint, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_ = c.Call(ctx, req, nil, WithConnectionTimeout(0), WithAddress(address))

	mu.Lock()
	got := timeoutHeader
	mu.Unlock()
	if got != "0s" {
		t.Errorf("WithConnectionTimeout(0) should remain as 0 duration (no Timeout header in real call), got opts.ConnectionTimeout=%s", got)
	}
}
