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

func TestMarkResetBehavior(t *testing.T) {
	r := registry.NewMemoryRegistry(registry.Services(map[string][]*registry.Service{
		"svc": {
			{
				Name:    "svc",
				Version: "1.0.0",
				Nodes: []*registry.Node{
					{Id: "node1", Address: "localhost:9001"},
					{Id: "node2", Address: "localhost:9002"},
					{Id: "node3", Address: "localhost:9003"},
				},
			},
		},
	}))

	sel := NewSelector(Registry(r))
	defer sel.Close()

	next, err := sel.Select("svc")
	if err != nil {
		t.Fatal(err)
	}

	n, err := next()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < defaultMaxFailures; i++ {
		sel.Mark("svc", n, fmt.Errorf("fail %d", i))
	}

	next2, err := sel.Select("svc")
	if err != nil {
		t.Fatal(err)
	}

	selected := make(map[string]int)
	for i := 0; i < 30; i++ {
		node, err := next2()
		if err != nil {
			t.Fatal(err)
		}
		selected[node.Id]++
	}

	if selected[n.Id] > 0 {
		t.Fatalf("failed node %s should not be selected but was selected %d times", n.Id, selected[n.Id])
	}

	sel.Mark("svc", n, nil)

	next3, err := sel.Select("svc")
	if err != nil {
		t.Fatal(err)
	}

	foundRecovered := false
	for i := 0; i < 30; i++ {
		node, err := next3()
		if err != nil {
			t.Fatal(err)
		}
		if node.Id == n.Id {
			foundRecovered = true
			break
		}
	}

	if !foundRecovered {
		t.Fatalf("node %s should be selectable after Mark with nil error", n.Id)
	}
}

func TestResetClearsStatus(t *testing.T) {
	r := registry.NewMemoryRegistry(registry.Services(map[string][]*registry.Service{
		"svc": {
			{
				Name:    "svc",
				Version: "1.0.0",
				Nodes: []*registry.Node{
					{Id: "node1", Address: "localhost:9001"},
				},
			},
		},
	}))

	sel := NewSelector(Registry(r))
	defer sel.Close()

	node := &registry.Node{Id: "node1", Address: "localhost:9001"}
	for i := 0; i < defaultMaxFailures; i++ {
		sel.Mark("svc", node, fmt.Errorf("fail"))
	}

	sel.Reset("svc")

	next, err := sel.Select("svc")
	if err != nil {
		t.Fatal(err)
	}

	n, err := next()
	if err != nil {
		t.Fatal(err)
	}
	if n.Id != "node1" {
		t.Fatalf("expected node1 after Reset, got %s", n.Id)
	}
}
