package service

import (
	"consul-demo/proj1/config"
	"consul-demo/proj1/discover"
	"context"
	"errors"
)

type Service interface {
	// HealthCheck check service health status
	HealthCheck() bool

	// sayHelloService
	SayHello() string

	//  discovery service from consul by serviceName
	DiscoveryService(ctx context.Context, serviceName string) ([]interface{}, error)
}

var ErrNotServiceInstances = errors.New("instances are not existed")

type DiscoveryServiceImpl struct {
	discoveryClient discover.IDiscoveryClient
}

func NewDiscoveryServiceImpl(discoveryClient discover.IDiscoveryClient) Service {
	return &DiscoveryServiceImpl{
		discoveryClient: discoveryClient,
	}
}

func (*DiscoveryServiceImpl) SayHello() string {
	return "Hello World!"
}

func (service *DiscoveryServiceImpl) DiscoveryService(ctx context.Context, serviceName string) ([]interface{}, error) {
	instances := service.discoveryClient.DiscoverServices(serviceName, config.Logger)
	if instances == nil || len(instances) == 0 {
		return nil, ErrNotServiceInstances
	}
	return instances, nil
}

// HealthCheck implement Service method
// 用于检查服务的健康状态，这里仅仅返回true
func (*DiscoveryServiceImpl) HealthCheck() bool {
	return true
}
