package server

import (
	"testing"
	"time"

	"go-micro.dev/v5/registry"
)

func TestStartFailureRollback(t *testing.T) {
	reg := registry.NewMemoryRegistry()

	svc := &registry.Service{
		Name:    "testsvc",
		Version: "1.0.0",
		Nodes: []*registry.Node{
			{Id: "testnode-1", Address: "127.0.0.1:0"},
		},
	}

	if err := reg.Register(svc); err != nil {
		t.Fatal(err)
	}

	services, err := reg.GetService("testsvc")
	if err != nil {
		t.Fatal(err)
	}
	if len(services) == 0 {
		t.Fatal("expected service to be registered")
	}

	s := NewRPCServer(
		Registry(reg),
		Address("127.0.0.1:0"),
		RegisterInterval(1*time.Second),
		RegisterTTL(10*time.Second),
	)

	s2 := NewRPCServer(
		Registry(reg),
		Address("127.0.0.1:0"),
		Name("testsvc"),
	)

	if err := s.Start(); err != nil {
		t.Fatal(err)
	}

	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}

	_ = s2
}
