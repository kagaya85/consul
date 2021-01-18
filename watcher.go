package consul

import (
	"context"

	"github.com/go-kratos/kratos/v2/registry"
)

type watcher struct {
	event chan struct{}
	set   *serviceSet

	// for cancel
	ctx    context.Context
	cancel context.CancelFunc
}

func (w *watcher) Watch(ctx context.Context) (services []*registry.Service, err error) {
	select {
	case <-w.ctx.Done():
		err = ctx.Err()
	case <-ctx.Done():
		err = ctx.Err()
	case <-w.event:
	}
	ss, ok := w.set.services.Load().([]*registry.Service)
	if ok {
		for _, s := range ss {
			services = append(services, s)
		}
	}
	return
}

func (w *watcher) Close() {
	w.cancel()
	w.set.lock.Lock()
	defer w.set.lock.Unlock()
	delete(w.set.watcher, w)
	return
}
