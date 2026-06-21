package registry

import (
	"fmt"
	"os"
	"testing"
	"time"
)

var (
	testData = map[string][]*Service{
		"foo": {
			{
				Name:    "foo",
				Version: "1.0.0",
				Nodes: []*Node{
					{
						Id:      "foo-1.0.0-123",
						Address: "localhost:9999",
					},
					{
						Id:      "foo-1.0.0-321",
						Address: "localhost:9999",
					},
				},
			},
			{
				Name:    "foo",
				Version: "1.0.1",
				Nodes: []*Node{
					{
						Id:      "foo-1.0.1-321",
						Address: "localhost:6666",
					},
				},
			},
			{
				Name:    "foo",
				Version: "1.0.3",
				Nodes: []*Node{
					{
						Id:      "foo-1.0.3-345",
						Address: "localhost:8888",
					},
				},
			},
		},
		"bar": {
			{
				Name:    "bar",
				Version: "default",
				Nodes: []*Node{
					{
						Id:      "bar-1.0.0-123",
						Address: "localhost:9999",
					},
					{
						Id:      "bar-1.0.0-321",
						Address: "localhost:9999",
					},
				},
			},
			{
				Name:    "bar",
				Version: "latest",
				Nodes: []*Node{
					{
						Id:      "bar-1.0.1-321",
						Address: "localhost:6666",
					},
				},
			},
		},
	}
)

func TestMemoryRegistry(t *testing.T) {
	m := NewMemoryRegistry()

	fn := func(k string, v []*Service) {
		services, err := m.GetService(k)
		if err != nil {
			t.Errorf("Unexpected error getting service %s: %v", k, err)
		}

		if len(services) != len(v) {
			t.Errorf("Expected %d services for %s, got %d", len(v), k, len(services))
		}

		for _, service := range v {
			var seen bool
			for _, s := range services {
				if s.Version == service.Version {
					seen = true
					break
				}
			}
			if !seen {
				t.Errorf("expected to find version %s", service.Version)
			}
		}
	}

	// register data
	for _, v := range testData {
		serviceCount := 0
		for _, service := range v {
			if err := m.Register(service); err != nil {
				t.Errorf("Unexpected register error: %v", err)
			}
			serviceCount++
			// after the service has been registered we should be able to query it
			services, err := m.GetService(service.Name)
			if err != nil {
				t.Errorf("Unexpected error getting service %s: %v", service.Name, err)
			}
			if len(services) != serviceCount {
				t.Errorf("Expected %d services for %s, got %d", serviceCount, service.Name, len(services))
			}
		}
	}

	// using test data
	for k, v := range testData {
		fn(k, v)
	}

	services, err := m.ListServices()
	if err != nil {
		t.Errorf("Unexpected error when listing services: %v", err)
	}

	totalServiceCount := 0
	for _, testSvc := range testData {
		for range testSvc {
			totalServiceCount++
		}
	}

	if len(services) != totalServiceCount {
		t.Errorf("Expected total service count: %d, got: %d", totalServiceCount, len(services))
	}

	// deregister
	for _, v := range testData {
		for _, service := range v {
			if err := m.Deregister(service); err != nil {
				t.Errorf("Unexpected deregister error: %v", err)
			}
		}
	}

	// after all the service nodes have been deregistered we should not get any results
	for _, v := range testData {
		for _, service := range v {
			services, err := m.GetService(service.Name)
			if err != ErrNotFound {
				t.Errorf("Expected error: %v, got: %v", ErrNotFound, err)
			}
			if len(services) != 0 {
				t.Errorf("Expected %d services for %s, got %d", 0, service.Name, len(services))
			}
		}
	}
}

func TestMemoryRegistryTTL(t *testing.T) {
	m := NewMemoryRegistry()

	for _, v := range testData {
		for _, service := range v {
			if err := m.Register(service, RegisterTTL(time.Millisecond)); err != nil {
				t.Fatal(err)
			}
		}
	}

	time.Sleep(ttlPruneTime * 2)

	for name := range testData {
		svcs, err := m.GetService(name)
		if err != nil {
			t.Fatal(err)
		}

		for _, svc := range svcs {
			if len(svc.Nodes) > 0 {
				t.Fatalf("Service %q still has nodes registered", name)
			}
		}
	}
}

func TestMemoryRegistryTTLConcurrent(t *testing.T) {
	concurrency := 1000
	waitTime := ttlPruneTime * 2
	m := NewMemoryRegistry()

	for _, v := range testData {
		for _, service := range v {
			if err := m.Register(service, RegisterTTL(waitTime/2)); err != nil {
				t.Fatal(err)
			}
		}
	}

	if len(os.Getenv("IN_TRAVIS_CI")) == 0 {
		t.Logf("test will wait %v, then check TTL timeouts", waitTime)
	}

	errChan := make(chan error, concurrency)
	syncChan := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		go func() {
			<-syncChan
			for name := range testData {
				svcs, err := m.GetService(name)
				if err != nil {
					errChan <- err
					return
				}

				for _, svc := range svcs {
					if len(svc.Nodes) > 0 {
						errChan <- fmt.Errorf("Service %q still has nodes registered", name)
						return
					}
				}
			}

			errChan <- nil
		}()
	}

	time.Sleep(waitTime)
	close(syncChan)

	for i := 0; i < concurrency; i++ {
		if err := <-errChan; err != nil {
			t.Fatal(err)
		}
	}
}

func TestMemoryRegistrySlowWatcherDoesNotBlock(t *testing.T) {
	m := NewMemoryRegistry().(*memRegistry)

	svcName := "test.svc.slow.watcher"
	svc := &Service{
		Name:    svcName,
		Version: "1.0.0",
		Nodes: []*Node{
			{Id: svcName + "-1", Address: "127.0.0.1:9001"},
		},
	}

	slowW, err := m.Watch()
	if err != nil {
		t.Fatal(err)
	}

	fastW, err := m.Watch()
	if err != nil {
		t.Fatal(err)
	}

	_ = slowW

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 5; i++ {
			_, err := fastW.Next()
			if err != nil {
				t.Errorf("fast watcher Next error: %v", err)
				return
			}
		}
	}()

	for i := 0; i < 5; i++ {
		if err := m.Register(svc); err != nil {
			t.Fatal(err)
		}
		if err := m.Deregister(svc); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case <-done:
	case <-time.After(time.Duration(5*(sendEventTime+time.Millisecond*50)) + time.Second*2):
		t.Fatal("fast watcher was blocked by slow watcher")
	}

	slowW.Stop()
	fastW.Stop()
}

func TestMemoryRegistryWatcherStopRemovesFromMap(t *testing.T) {
	m := NewMemoryRegistry().(*memRegistry)

	w1, err := m.Watch()
	if err != nil {
		t.Fatal(err)
	}
	w2, err := m.Watch()
	if err != nil {
		t.Fatal(err)
	}

	m.RLock()
	count := len(m.watchers)
	m.RUnlock()
	if count != 2 {
		t.Fatalf("expected 2 watchers, got %d", count)
	}

	w1.Stop()

	time.Sleep(time.Millisecond * 50)

	m.RLock()
	count = len(m.watchers)
	m.RUnlock()
	if count != 1 {
		t.Fatalf("expected 1 watcher after Stop, got %d", count)
	}

	w2.Stop()
	time.Sleep(time.Millisecond * 50)

	m.RLock()
	count = len(m.watchers)
	m.RUnlock()
	if count != 0 {
		t.Fatalf("expected 0 watchers after both Stop, got %d", count)
	}
}

func TestMemoryRegistryDeadWatcherCleanedOnSendEvent(t *testing.T) {
	m := NewMemoryRegistry().(*memRegistry)

	_, err := m.Watch()
	if err != nil {
		t.Fatal(err)
	}
	w2, err := m.Watch()
	if err != nil {
		t.Fatal(err)
	}

	m.RLock()
	count := len(m.watchers)
	m.RUnlock()
	if count != 2 {
		t.Fatalf("expected 2 watchers initially, got %d", count)
	}

	w2.Stop()
	time.Sleep(time.Millisecond * 50)

	svc := &Service{
		Name:    "dead.watcher.test",
		Version: "1.0.0",
		Nodes: []*Node{
			{Id: "n1", Address: "127.0.0.1:9002"},
		},
	}
	if err := m.Register(svc); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)

	m.RLock()
	count = len(m.watchers)
	m.RUnlock()
	if count != 1 {
		t.Fatalf("expected 1 watcher after sendEvent cleaned dead one, got %d", count)
	}
}
