package consul

import (
	"sync"
	"sync/atomic"
)

// Service is consul service
type Service struct {
	id        string
	name      string
	version   string
	metadata  map[string]string
	endpoints []string
}

// ID return id
func (s *Service) ID() string {
	return s.id
}

// Name return name
func (s *Service) Name() string {
	return s.name
}

// Version return version
func (s *Service) Version() string {
	return s.version
}

// Metadata return metadata
func (s *Service) Metadata() map[string]string {
	return s.metadata
}

// Endpoints return endpoints
func (s *Service) Endpoints() []string {
	return s.endpoints
}

type serviceSet struct {
	serviceName string
	watcher     map[*watcher]struct{}
	services    *atomic.Value
	lock        sync.RWMutex
}

func (s *serviceSet) broadcast(ss []*Service) {
	s.services.Store(ss)
	s.lock.RLock()
	defer s.lock.RUnlock()
	for k := range s.watcher {
		select {
		case k.event <- struct{}{}:
		default:
		}
	}
}
