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

type nodeStats struct {
	consecutiveFailures int
	lastUsed            time.Time
}

type registrySelector struct {
	so      Options
	rc      cache.Cache
	mu      sync.RWMutex
	nodeMap map[string]map[string]*nodeStats
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

	filtered := c.filterByStats(service, services)

	return sopts.Strategy(filtered), nil
}

func (c *registrySelector) filterByStats(service string, services []*registry.Service) []*registry.Service {
	if c.nodeMap == nil {
		return services
	}

	stats, ok := c.nodeMap[service]
	if !ok || len(stats) == 0 {
		return services
	}

	totalNodes := 0
	for _, svc := range services {
		totalNodes += len(svc.Nodes)
	}

	if totalNodes <= 1 {
		return services
	}

	filtered := make([]*registry.Service, 0, len(services))
	availableCount := 0

	for _, svc := range services {
		newSvc := &registry.Service{
			Name:      svc.Name,
			Version:   svc.Version,
			Metadata:  svc.Metadata,
			Endpoints: svc.Endpoints,
		}

		for _, node := range svc.Nodes {
			ns, exists := stats[node.Id]
			if exists && ns.consecutiveFailures >= defaultMaxFailures {
				continue
			}
			newSvc.Nodes = append(newSvc.Nodes, node)
			availableCount++
		}

		if len(newSvc.Nodes) > 0 {
			filtered = append(filtered, newSvc)
		}
	}

	if availableCount == 0 {
		return services
	}

	return filtered
}

func (c *registrySelector) Mark(service string, node *registry.Node, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nodeMap == nil {
		c.nodeMap = make(map[string]map[string]*nodeStats)
	}

	if _, ok := c.nodeMap[service]; !ok {
		c.nodeMap[service] = make(map[string]*nodeStats)
	}

	ns, ok := c.nodeMap[service][node.Id]
	if !ok {
		ns = &nodeStats{}
		c.nodeMap[service][node.Id] = ns
	}

	ns.lastUsed = time.Now()

	if err != nil {
		ns.consecutiveFailures++
	} else {
		ns.consecutiveFailures = 0
	}
}

func (c *registrySelector) Reset(service string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nodeMap != nil {
		delete(c.nodeMap, service)
	}
}

func (c *registrySelector) Close() error {
	c.rc.Stop()

	return nil
}

func (c *registrySelector) String() string {
	return "registry"
}

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
		so: sopts,
	}
	s.rc = s.newCache()

	return s
}
