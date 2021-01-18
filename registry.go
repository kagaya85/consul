package consul

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/hashicorp/consul/api"
)

var (
	_ registry.Discovery = &Registry{}
	_ registry.Registrar = &Registry{}
)

// Config is consul registry config
type Config struct {
	*api.Config
}

// Registry is consul registry
type Registry struct {
	cfg *Config
	cli *Client

	registry map[string]*serviceSet
	lock     sync.RWMutex
}

// New creates consul registry
func New(cfg *Config) (r *Registry, err error) {
	cli, err := NewClient(cfg.Config)
	if err != nil {
		return
	}
	r = &Registry{
		cfg:      cfg,
		cli:      cli,
		registry: make(map[string]*serviceSet),
	}
	return
}

// Register register service
func (r *Registry) Register(ctx context.Context, svc *registry.Service) error {
	return r.cli.Register(ctx, svc)
}

// Deregister deregister service
func (r *Registry) Deregister(ctx context.Context, svc *registry.Service) error {
	return r.cli.Deregister(ctx, svc.ID)
}

// GetService return service by name
func (r *Registry) GetService(name string) (services []*registry.Service, err error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	set := r.registry[name]
	if set == nil {
		return nil, errors.NotFound("service not resolved", "service %s not resolved in registry", name)
	}
	ss, _ := set.services.Load().([]*registry.Service)
	if ss == nil {
		return nil, errors.NotFound("service not found", "service %s not found in registry", name)
	}
	for _, s := range ss {
		services = append(services, s)
	}
	return
}

// ListService return service list
func (r *Registry) ListService() (allServices map[string][]*registry.Service, err error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	allServices = make(map[string][]*registry.Service)
	for name, set := range r.registry {
		var services []*registry.Service
		ss, _ := set.services.Load().([]*registry.Service)
		if ss == nil {
			continue
		}
		for _, s := range ss {
			services = append(services, s)
		}
		allServices[name] = services
	}
	return
}

// Resolve resolve service by name
func (r *Registry) Resolve(ctx context.Context, name string) (registry.Watcher, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	set, ok := r.registry[name]
	if !ok {
		set = &serviceSet{
			watcher:     make(map[*watcher]struct{}, 0),
			services:    &atomic.Value{},
			serviceName: name,
		}
		r.registry[name] = set
	}

	// 初始化watcher
	w := &watcher{
		event: make(chan struct{}, 1),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.set = set
	set.lock.Lock()
	set.watcher[w] = struct{}{}
	set.lock.Unlock()
	ss, _ := set.services.Load().([]*registry.Service)
	if len(ss) > 0 {
		// 如果services有值需要推送给watcher，否则watch的时候可能会永远阻塞拿不到初始的数据
		w.event <- struct{}{}
	}

	// 放在最后是为了防止漏推送
	if !ok {
		go r.resolve(set)
	}
	return w, nil
}

func (r *Registry) resolve(ss *serviceSet) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	services, idx, err := r.cli.Service(ctx, ss.serviceName, 0, true)
	cancel()
	if err == nil && len(services) > 0 {
		ss.broadcast(services)
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
		tmpService, tmpIdx, err := r.cli.Service(ctx, ss.serviceName, idx, true)
		cancel()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if len(tmpService) != 0 && tmpIdx != idx {
			services = tmpService
			ss.broadcast(services)
		}
		idx = tmpIdx
	}
}
