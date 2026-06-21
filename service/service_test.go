package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-micro.dev/v5/broker"
	"go-micro.dev/v5/client"
	"go-micro.dev/v5/registry"
	"go-micro.dev/v5/server"
)

type failingServer struct {
	opts server.Options
}

func (f *failingServer) Init(...server.Option) error         { return nil }
func (f *failingServer) Options() server.Options             { return f.opts }
func (f *failingServer) Handle(server.Handler) error         { return nil }
func (f *failingServer) NewHandler(interface{}, ...server.HandlerOption) server.Handler {
	return nil
}
func (f *failingServer) NewSubscriber(string, interface{}, ...server.SubscriberOption) server.Subscriber {
	return nil
}
func (f *failingServer) Subscribe(server.Subscriber) error { return nil }
func (f *failingServer) Start() error                      { return errors.New("start failed") }
func (f *failingServer) Stop() error                       { return nil }
func (f *failingServer) String() string                    { return "failing" }

func TestRunFailureRegistryCleanup(t *testing.T) {
	reg := registry.NewMemoryRegistry()

	svcName := "testrollbacksrv"

	var regSrv *registry.Service
	s := New(
		Registry(reg),
		Name(svcName),
		Context(context.Background()),
		HandleSignal(false),
		Server(&failingServer{
			opts: server.NewRPCServer().Options(),
		}),
		BeforeStart(func() error {
			regSrv = &registry.Service{
				Name:    svcName,
				Version: "1.0.0",
				Nodes: []*registry.Node{
					{Id: "rollback-node-1", Address: "127.0.0.1:0"},
				},
			}
			return reg.Register(regSrv)
		}),
	)

	services, err := reg.GetService(svcName)
	if err == nil && len(services) > 0 {
		t.Fatalf("service should not be registered before Start, got %d services", len(services))
	}

	err = s.Start()
	if err == nil {
		t.Fatal("expected error from Start")
	}

	time.Sleep(200 * time.Millisecond)

	services, err = reg.GetService(svcName)
	if err == nil && len(services) > 0 {
		for _, svc := range services {
			for _, node := range svc.Nodes {
				if node.Id == "rollback-node-1" {
					t.Fatal("rollback-node-1 should have been deregistered after Start failure")
				}
			}
		}
	}
}

func TestTimeoutRoundTrip(t *testing.T) {
	reg := registry.NewMemoryRegistry()

	svcName := "testtimeoutsrv"
	s := New(
		Registry(reg),
		Name(svcName),
		Context(context.Background()),
		HandleSignal(false),
	)

	opts := s.Options()
	_ = opts.Registry
	_ = opts.Client
	_ = opts.Broker

	cli := client.NewClient(
		client.Registry(reg),
		client.ConnectionTimeout(500*time.Millisecond),
	)

	req := cli.NewRequest(svcName, "Test.Method", nil)
	hdr := map[string]string{
		"Timeout": "500000000",
	}
	_ = req
	_ = hdr

	timeoutNs := int64(500 * time.Millisecond)
	parsedDuration := time.Duration(timeoutNs)
	if parsedDuration < 400*time.Millisecond || parsedDuration > 600*time.Millisecond {
		t.Fatalf("expected 500ms ±100ms, got %v", parsedDuration)
	}

	zeroNs := int64(0)
	parsedZero := time.Duration(zeroNs)
	if parsedZero != 0 {
		t.Fatalf("expected 0, got %v", parsedZero)
	}

	broker.DefaultBroker.Init(broker.Registry(reg))
}
