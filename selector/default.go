package selector

import (
	"sync"
	"time"

	"github.com/pkg/errors"

	"go-micro.dev/v5/registry"
	"go-micro.dev/v5/registry/cache"
)

const (
	defaultMaxFailures = 3
)

type nodeStatus struct {
	failures int
	lastFail time.Time
}

type registrySelector struct {
	so       Options
	rc       cache.Cache
	mu       sync.RWMutex
	statuses map[string]map[string]*nodeStatus
}

func (c *registrySelector) newCache() cache.Cache {
	opts := make([]cache.Option, 0, 1)

	if c.so.Context != nil {
		if t, ok := c.so.Context.Value("selector_ttl").(time.Duration); ok {
			opts = append(opts, cache.WithTTL(t))
		}
	}

	return cache.New(c.so.Registry, opts...)
}

func (c *registrySelector) Init(opts ...Option) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, o := range opts {
		o(&c.so)
	}

	c.rc.Stop()
	c.rc = c.newCache()

	return nil
}

func (c *registrySelector) Options() Options {
	return c.so
}

func (c *registrySelector) Select(service string, opts ...SelectOption) (Next, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sopts := SelectOptions{
		Strategy: c.so.Strategy,
	}

	for _, opt := range opts {
		opt(&sopts)
	}

	services, err := c.rc.GetService(service)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, err
	}

	for _, filter := range sopts.Filters {
		services = filter(services)
	}

	if len(services) == 0 {
		return nil, ErrNoneAvailable
	}

	filtered := make([]*registry.Service, 0, len(services))
	now := time.Now()
	for _, svc := range services {
		nodes := make([]*registry.Node, 0, len(svc.Nodes))
		for _, node := range svc.Nodes {
			if st, ok := c.statuses[service]; ok {
				if ns, ok := st[node.Id]; ok {
					if ns.failures >= defaultMaxFailures {
						if now.Sub(ns.lastFail) < 30*time.Second {
							continue
						}
					}
				}
			}
			nodes = append(nodes, node)
		}
		if len(nodes) > 0 {
			newSvc := *svc
			newSvc.Nodes = nodes
			filtered = append(filtered, &newSvc)
		}
	}

	if len(filtered) == 0 {
		for _, svc := range services {
			if len(svc.Nodes) > 0 {
				filtered = append(filtered, svc)
			}
		}
	}

	if len(filtered) == 0 {
		return nil, ErrNoneAvailable
	}

	return sopts.Strategy(filtered), nil
}

func (c *registrySelector) Mark(service string, node *registry.Node, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.statuses == nil {
		c.statuses = make(map[string]map[string]*nodeStatus)
	}

	if _, ok := c.statuses[service]; !ok {
		c.statuses[service] = make(map[string]*nodeStatus)
	}

	if _, ok := c.statuses[service][node.Id]; !ok {
		c.statuses[service][node.Id] = &nodeStatus{}
	}

	ns := c.statuses[service][node.Id]

	if err != nil {
		ns.failures++
		ns.lastFail = time.Now()
	} else {
		ns.failures = 0
	}
}

func (c *registrySelector) Reset(service string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.statuses != nil {
		delete(c.statuses, service)
	}
}

// Close stops the watcher and destroys the cache.
func (c *registrySelector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.rc.Stop()
	c.statuses = nil

	return nil
}

func (c *registrySelector) String() string {
	return "registry"
}

// NewSelector creates a new default selector.
func NewSelector(opts ...Option) Selector {
	sopts := Options{
		Strategy: Random,
	}

	for _, opt := range opts {
		opt(&sopts)
	}

	if sopts.Registry == nil {
		sopts.Registry = registry.DefaultRegistry
	}

	s := &registrySelector{
		so:       sopts,
		statuses: make(map[string]map[string]*nodeStatus),
	}
	s.rc = s.newCache()

	return s
}
