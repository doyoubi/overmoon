package main

import (
	"context"
	"fmt"

	"github.com/doyoubi/overmoon/src/broker"
	"github.com/doyoubi/overmoon/src/service"
)

func main() {
	cfg := broker.EtcdConfig{
		PathPrefix: "/undermoon",
		FailureTTL: 120,
	}
	endpoints := []string{"127.0.0.1:2380"}
	broker, err := broker.NewEtcdMetaBrokerFromEndpoints(&cfg, endpoints)
	if err != nil {
		fmt.Printf("Failed to create broker %s", err)
		return
	}

	ctx := context.Background()

	proxy := service.NewHttpBrokerProxy(ctx, broker, "127.0.0.1:7799")
	proxy.Serve()
}
