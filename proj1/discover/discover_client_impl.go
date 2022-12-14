package discover

import (
	"github.com/go-kit/kit/sd/consul"
	consulAPI "github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"log"
	"strconv"
	"sync"
)

type DiscoveryClientImpl struct {
	Host         string            // Consul Host
	Port         int               // Consul Port
	client       consul.Client     // consul 客户端
	config       *consulAPI.Config // 连接 consul 的配置
	mutex        sync.Mutex        // 互斥
	instancesMap sync.Map          // 服务实例缓存字段
}

func NewDiscoveryClient(consulHost string, consulPort int) (IDiscoveryClient, error) {
	// 通过 Consul Host 和 Consul Port 创建一个 consul.Client
	consulConfig := consulAPI.DefaultConfig()
	consulConfig.Address = consulHost + ":" + strconv.Itoa(consulPort)
	apiClient, err := consulAPI.NewClient(consulConfig)
	if err != nil {
		return nil, err
	}

	client := consul.NewClient(apiClient)
	return &DiscoveryClientImpl{
		Host:   consulHost,
		Port:   consulPort,
		config: consulConfig,
		client: client,
	}, err
}

func (consulClient *DiscoveryClientImpl) Register(serviceName, instanceId, healthCheckUrl string, instanceHost string, instancePort int, meta map[string]string, logger *log.Logger) bool {
	// 1. 构建服务实例元数据
	serviceRegistration := &consulAPI.AgentServiceRegistration{
		ID:      instanceId,
		Name:    serviceName,
		Address: instanceHost,
		Port:    instancePort,
		Meta:    meta,
		Check: &consulAPI.AgentServiceCheck{
			DeregisterCriticalServiceAfter: "30s",
			HTTP:                           "http://" + instanceHost + ":" + strconv.Itoa(instancePort) + healthCheckUrl,
			Interval:                       "2s",
		},
	}

	// 2. 发送服务注册到 Consul 中
	err := consulClient.client.Register(serviceRegistration)
	if err != nil {
		log.Println("Register Service Error!")
		return false
	}

	log.Println("Register Service Success!")
	return true
}

func (consulClient *DiscoveryClientImpl) DeRegister(instanceId string, logger *log.Logger) bool {
	// 构建包含服务实例 ID 的元数据结构体
	serviceRegistration := &consulAPI.AgentServiceRegistration{
		ID: instanceId,
	}

	// 发送服务注销请求
	err := consulClient.client.Deregister(serviceRegistration)
	if err != nil {
		logger.Println("Deregister Service Error!")
		return false
	}

	log.Println("Deregister Service Success!")

	return true
}

func (consulClient *DiscoveryClientImpl) DiscoverServices(serviceName string, logger *log.Logger) []interface{} {
	//  该服务已监控并缓存
	instanceList, ok := consulClient.instancesMap.Load(serviceName)
	if ok {
		return instanceList.([]interface{})
	}

	// 申请锁
	consulClient.mutex.Lock()
	defer consulClient.mutex.Unlock()

	// 再次检查是否监控
	instanceList, ok = consulClient.instancesMap.Load(serviceName)
	if ok {
		return instanceList.([]interface{})
	} else {
		// 注册监控
		go func() {
			// 使用 consul 服务实例监控来监控某个服务名的服务实例列表变化
			params := make(map[string]interface{})
			params["type"] = "service"
			params["service"] = serviceName
			plan, _ := watch.Parse(params)
			plan.Handler = func(u uint64, i interface{}) {
				if i == nil {
					return
				}
				v, ok := i.([]*consulAPI.ServiceEntry)
				if !ok {
					return // 数据异常，忽略
				}
				// 没有服务实例在线
				if len(v) == 0 {
					consulClient.instancesMap.Store(serviceName, []interface{}{})
				}
				var healthServices []interface{}
				for _, service := range v {
					if service.Checks.AggregatedStatus() == consulAPI.HealthPassing {
						healthServices = append(healthServices, service.Service)
					}
				}
				consulClient.instancesMap.Store(serviceName, healthServices)
			}
			defer plan.Stop()
			plan.Run(consulClient.config.Address)
		}()
	}

	// 根据服务名请求服务实例列表
	entries, _, err := consulClient.client.Service(serviceName, "", false, nil)
	if err != nil {
		consulClient.instancesMap.Store(serviceName, []interface{}{})
		logger.Println("Discover Service Error!")
		return nil
	}
	instances := make([]interface{}, len(entries))
	for i := 0; i < len(instances); i++ {
		instances[i] = entries[i].Service
	}
	consulClient.instancesMap.Store(serviceName, instances)
	return instances
}
