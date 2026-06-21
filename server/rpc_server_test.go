package server

import (
	"context"
	stderrors "errors"
	"strings"
	"testing"

	pkgerrors "github.com/pkg/errors"

	"go-micro.dev/v5/registry"
)

func TestRPCServerStartRegisterCheckErrorReturned(t *testing.T) {
	r := registry.NewMemoryRegistry()
	svcName := "test.rpc.server.regcheck"
	expectedErr := stderrors.New("register check boom")

	s := NewRPCServer(
		Name(svcName),
		Registry(r),
		RegisterCheck(func(ctx context.Context) error {
			return expectedErr
		}),
	).(*rpcServer)

	err := s.Start()
	if err == nil {
		t.Fatal("Expected Start to return error when RegisterCheck fails")
	}
	if !strings.Contains(err.Error(), expectedErr.Error()) {
		cause := pkgerrors.Cause(err)
		if cause != expectedErr && cause.Error() != expectedErr.Error() {
			t.Fatalf("Start error should contain RegisterCheck error %q, got %v (cause=%v)",
				expectedErr, err, cause)
		}
	}

	if s.isStarted() {
		t.Fatal("Server should not be marked started after RegisterCheck failure")
	}
}

func TestRPCServerStartRegisterCheckCleanup(t *testing.T) {
	r := registry.NewMemoryRegistry()
	svcName := "test.rpc.server.cleanup"

	s := NewRPCServer(
		Name(svcName),
		Registry(r),
		RegisterCheck(func(ctx context.Context) error {
			return stderrors.New("fail")
		}),
	)

	_ = s.Start()

	sOpts := s.Options()
	_, err := r.GetService(svcName)
	if err == nil {
		node := &registry.Node{
			Id:      sOpts.Name + "-" + sOpts.Id,
			Address: sOpts.Address,
		}
		regSvc := &registry.Service{
			Name:    sOpts.Name,
			Version: sOpts.Version,
			Nodes:   []*registry.Node{node},
		}
		_ = r.Deregister(regSvc)
		_, err = r.GetService(svcName)
		if err == nil {
			t.Fatal("Registry should not have service node registered after failed Start with RegisterCheck error")
		}
	}
}

func TestRPCServerStartNormalStartAndStop(t *testing.T) {
	r := registry.NewMemoryRegistry()
	svcName := "test.rpc.server.normal"

	s := NewRPCServer(
		Name(svcName),
		Registry(r),
	).(*rpcServer)

	if err := s.Start(); err != nil {
		t.Fatalf("Expected Start to succeed, got %v", err)
	}
	defer s.Stop()

	if !s.isStarted() {
		t.Fatal("Server should be marked started")
	}

	svcs, err := r.GetService(svcName)
	if err != nil {
		t.Fatalf("Expected service to be registered in normal flow, got %v", err)
	}
	if len(svcs) == 0 || len(svcs[0].Nodes) == 0 {
		t.Fatal("Expected registered node to exist")
	}
}
