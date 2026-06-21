package service

import (
	"context"
	"strings"
	"testing"

	"github.com/pkg/errors"

	"go-micro.dev/v5/registry"
	"go-micro.dev/v5/server"
)

func TestServiceStartRegisterCheckFailureDeregisters(t *testing.T) {
	r := registry.NewMemoryRegistry()

	svcName := "test.svc.start.fail"

	expectedErr := errors.New("register check failed intentionally")

	srv := server.NewServer(
		server.Name(svcName),
		server.Registry(r),
		server.RegisterCheck(func(ctx context.Context) error {
			return expectedErr
		}),
	)

	srvOpts := srv.Options()
	addr := srvOpts.Address
	if len(addr) == 0 {
		addr = "127.0.0.1:0"
	}
	node := &registry.Node{
		Id:      srvOpts.Name + "-" + srvOpts.Id,
		Address: addr,
	}
	regSvc := &registry.Service{
		Name:    srvOpts.Name,
		Version: srvOpts.Version,
		Nodes:   []*registry.Node{node},
	}
	if err := r.Register(regSvc); err != nil {
		t.Fatal(err)
	}

	svc := New(
		Name(svcName),
		Registry(r),
		Server(srv),
	)

	_, err := r.GetService(svcName)
	if err != nil {
		t.Fatalf("Expected service to be registered before Start, got err: %v", err)
	}

	startErr := svc.Start()
	if startErr == nil {
		t.Fatal("Expected Start to return error, got nil")
	}

	startErrStr := startErr.Error()
	cause := errors.Cause(startErr)
	if cause != expectedErr && cause.Error() != expectedErr.Error() {
		containsSubStr := false
		for _, n := range []string{expectedErr.Error(), "register check failed", "server start failed"} {
			if strings.Contains(startErrStr, n) {
				containsSubStr = true
				break
			}
		}
		if !containsSubStr {
			t.Fatalf("Expected Start error to wrap %v, got %v (cause: %v)", expectedErr, startErr, cause)
		}
	}

	_, err = r.GetService(svcName)
	if err == nil {
		t.Fatalf("Expected service to be deregistered after Start failure, but GetService still succeeded")
	}
	if !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("Expected ErrNotFound, got %v", err)
	}
}

func TestServiceStartFailurePreservesOriginalError(t *testing.T) {
	r := registry.NewMemoryRegistry()

	svcName := "test.svc.error.wrap"

	origErr := errors.New("boom! startup failed")

	sOpts := []server.Option{
		server.Name(svcName),
		server.Registry(r),
		server.RegisterCheck(func(ctx context.Context) error {
			return origErr
		}),
	}

	svc := New(
		Name(svcName),
		Registry(r),
		Server(server.NewServer(sOpts...)),
	)

	err := svc.Start()
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	cause := errors.Cause(err)
	if cause != origErr && cause.Error() != origErr.Error() {
		containsSubStr := false
		errStr := err.Error()
		needles := []string{origErr.Error(), "register check failed", "server start failed"}
		for _, n := range needles {
			if strings.Contains(errStr, n) {
				containsSubStr = true
				break
			}
		}
		if !containsSubStr {
			t.Fatalf("Original error %q not found in error chain: %v (cause=%v)", origErr, err, cause)
		}
	}
}
