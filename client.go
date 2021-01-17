package consul

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
)

// Client is consul client config
type Client struct {
	cfg *api.Config
	cli *api.Client
}

// NewClient creates consul client
func NewClient(cfg *api.Config) (*Client, error) {
	cli, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{cli: cli, cfg: cfg}, nil
}

// Service get services from consul
func (d *Client) Service(ctx context.Context, service string, index uint64, passingOnly bool) ([]*Service, uint64, error) {
	opts := &api.QueryOptions{
		WaitIndex: index,
		WaitTime:  time.Second * 55,
	}
	opts = opts.WithContext(ctx)
	entries, meta, err := d.cli.Health().Service(service, "", passingOnly, opts)
	if err != nil {
		return nil, 0, err
	}
	var services []*Service
	for _, entry := range entries {
		var version string
		for _, tag := range entry.Service.Tags {
			strs := strings.SplitN(tag, "=", 2)
			if len(strs) == 2 && strs[0] == "version" {
				version = strs[1]
			}
		}
		var endpoints []string
		for _, addr := range entry.Service.TaggedAddresses {
			endpoints = append(endpoints, addr.Address)
		}
		services = append(services, &Service{
			id:        entry.Service.ID,
			name:      entry.Service.Service,
			metadata:  entry.Service.Meta,
			version:   version,
			endpoints: endpoints,
		})
	}
	return services, meta.LastIndex, nil
}

// Register register service instacen to consul
func (d *Client) Register(ctx context.Context, svc *Service) error {
	addresses := make(map[string]api.ServiceAddress)
	var addr string
	var port uint64
	for _, endpoint := range svc.endpoints {
		raw, err := url.Parse(endpoint)
		if err != nil {
			return err
		}
		addr = raw.Hostname()
		port, _ = strconv.ParseUint(raw.Port(), 10, 16)
		addresses[raw.Scheme] = api.ServiceAddress{Address: endpoint, Port: int(port)}
	}
	asr := &api.AgentServiceRegistration{
		ID:              svc.id,
		Name:            svc.name,
		Meta:            svc.metadata,
		Tags:            []string{fmt.Sprintf("version=%s", svc.version)},
		TaggedAddresses: addresses,
		Address:         addr,
		Port:            int(port),
		Checks: []*api.AgentServiceCheck{
			{
				TCP:      fmt.Sprintf("%s:%d", addr, port),
				Interval: "10s",
			},
		},
	}

	ch := make(chan error)
	go func() {
		err := d.cli.Agent().ServiceRegister(asr)
		ch <- err
	}()
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-ch:
	}
	return err
}

// Deregister deregister service by service ID
func (d *Client) Deregister(ctx context.Context, serviceID string) error {
	ch := make(chan error)
	go func() {
		err := d.cli.Agent().ServiceDeregister(serviceID)
		ch <- err
	}()
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-ch:
	}
	return err
}
