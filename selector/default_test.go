package selector

import (
	"fmt"
	"os"
	"testing"

	"go-micro.dev/v5/registry"
)

func TestRegistrySelector(t *testing.T) {
	counts := map[string]int{}

	r := registry.NewMemoryRegistry(registry.Services(testData))
	cache := NewSelector(Registry(r))

	next, err := cache.Select("foo")
	if err != nil {
		t.Errorf("Unexpected error calling cache select: %v", err)
	}

	for i := 0; i < 100; i++ {
		node, err := next()
		if err != nil {
			t.Errorf("Expected node err, got err: %v", err)
		}
		counts[node.Id]++
	}

	if len(os.Getenv("IN_TRAVIS_CI")) == 0 {
		t.Logf("Selector Counts %v", counts)
	}
}

func TestRegistrySelectorMarkSkip(t *testing.T) {
	r := registry.NewMemoryRegistry(registry.Services(testData))
	sel := NewSelector(Registry(r))

	service := "foo"
	targetNode := &registry.Node{Id: "foo-1.0.0-123", Address: "localhost:9999"}

	for i := 0; i < 5; i++ {
		sel.Mark(service, targetNode, fmt.Errorf("simulated error %d", i))
	}

	next, err := sel.Select(service)
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}

	for i := 0; i < 200; i++ {
		node, err := next()
		if err != nil {
			t.Fatalf("next error: %v", err)
		}
		if node.Id == targetNode.Id {
			t.Fatalf("Expected node %s to be skipped after 5 failed marks, but was selected", targetNode.Id)
		}
	}
}

func TestRegistrySelectorReset(t *testing.T) {
	r := registry.NewMemoryRegistry(registry.Services(testData))
	sel := NewSelector(Registry(r))

	service := "foo"
	targetNode := &registry.Node{Id: "foo-1.0.0-123", Address: "localhost:9999"}

	for i := 0; i < 10; i++ {
		sel.Mark(service, targetNode, fmt.Errorf("simulated error %d", i))
	}

	next, err := sel.Select(service)
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}
	skipped := true
	for i := 0; i < 50; i++ {
		node, _ := next()
		if node.Id == targetNode.Id {
			skipped = false
			break
		}
	}
	if !skipped {
		t.Fatalf("Expected node to be skipped after 10 failures before Reset")
	}

	sel.Reset(service)

	next2, err := sel.Select(service)
	if err != nil {
		t.Fatalf("Select after reset error: %v", err)
	}

	found := false
	for i := 0; i < 200; i++ {
		node, _ := next2()
		if node.Id == targetNode.Id {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Expected node %s to be selectable after Reset", targetNode.Id)
	}
}

func TestRegistrySelectorSingleNodeNotSkipped(t *testing.T) {
	data := map[string][]*registry.Service{
		"single": {
			{
				Name:    "single",
				Version: "1.0.0",
				Nodes: []*registry.Node{
					{
						Id:      "single-1",
						Address: "localhost:1111",
					},
				},
			},
		},
	}
	r := registry.NewMemoryRegistry(registry.Services(data))
	sel := NewSelector(Registry(r))

	service := "single"
	node := &registry.Node{Id: "single-1", Address: "localhost:1111"}

	for i := 0; i < 10; i++ {
		sel.Mark(service, node, fmt.Errorf("boom %d", i))
	}

	next, err := sel.Select(service)
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}

	for i := 0; i < 50; i++ {
		n, err := next()
		if err != nil {
			t.Fatalf("next error: %v", err)
		}
		if n.Id != node.Id {
			t.Fatalf("Expected single node to still be selectable even with failures")
		}
	}
}
