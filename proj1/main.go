package main

import (
	"consul-demo/proj1/config"
	"consul-demo/proj1/discover"
	"consul-demo/proj1/endpoint"
	"consul-demo/proj1/service"
	"consul-demo/proj1/transport"
	"context"
	"flag"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	// 从命令行中读取相关参数，没有时使用默认值
	var (
		// 服务地址和服务名
		servicePort = flag.Int("service.port", 10086, "service port")
		serviceHost = flag.String("service.host", "127.0.0.1", "service host")
		serviceName = flag.String("service.name", "SayHello", "service name")
		// consul 地址
		consulPort = flag.Int("consul.port", 8500, "consul port")
		consulHost = flag.String("consul.host", "127.0.0.1", "consul host")
	)

	flag.Parse()

	ctx := context.Background()
	errChan := make(chan error)

	discoveryClient, err := discover.NewDiscoveryClient(*consulHost, *consulPort) // 创建服务发现客户端
	if err != nil {                                                               // 获取服务发现客户端失败，直接关闭服务
		config.Logger.Println("Get Consul Client failed")
		os.Exit(-1)
	}

	var svc = service.NewDiscoveryServiceImpl(discoveryClient) // 声明并初始化 Service
	sayHelloEndpoint := endpoint.MakeSayHelloEndpoint(svc)     // 创建打招呼的Endpoint
	discoveryEndpoint := endpoint.MakeDiscoveryEndpoint(svc)   // 创建服务发现的Endpoint
	healthEndpoint := endpoint.MakeHealthCheckEndpoint(svc)    // 创建健康检查的Endpoint

	endpts := endpoint.DiscoveryEndpoints{
		SayHelloEndpoint:    sayHelloEndpoint,
		DiscoveryEndpoint:   discoveryEndpoint,
		HealthCheckEndpoint: healthEndpoint,
	}

	r := transport.MakeHttpHandler(ctx, endpts, config.KitLogger) // 创建http.Handler
	instanceId := *serviceName + "-" + uuid.NewV4().String()      // 定义服务实例ID
	// 启动 http server
	go func() {
		config.Logger.Println("Http Server start at port:" + strconv.Itoa(*servicePort))
		ret := discoveryClient.Register(*serviceName, instanceId, "/health", *serviceHost, *servicePort, nil, config.Logger) // 启动前执行注册
		if !ret {                                                                                                            // 注册失败，服务启动失败
			config.Logger.Printf("string-service for service %s failed.", *serviceName)
			os.Exit(-1)
		}

		handler := r
		errChan <- http.ListenAndServe(":"+strconv.Itoa(*servicePort), handler) // 启动http服务
	}()

	go func() { // 监控系统信号，等待 ctrl + c 系统信号通知服务关闭
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errChan <- fmt.Errorf("%s", <-c)
	}()

	error := <-errChan
	// 服务退出取消注册
	discoveryClient.DeRegister(instanceId, config.Logger)
	config.Logger.Println(error)
}
