package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"

	"github.com/spf13/viper"

	"github.com/doyoubi/overmoon/src/broker"
	"github.com/doyoubi/overmoon/src/service"
)

type proxyConfig struct {
	pathPrefix string
	failureTTL int64
	etcdNodes  []string
}

func getConfig() (*proxyConfig, error) {
	data, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		return nil, err
	}

	viper.SetConfigType("YAML")
	viper.ReadConfig(bytes.NewBuffer(data))

	pathPrefix := viper.GetString("path_prefix")
	if pathPrefix == "" {
		pathPrefix = "/undermoon"
	}
	failureTTL := viper.GetInt64("failure_ttl")
	if failureTTL == 0 {
		failureTTL = 60
	}
	etcdNodes := viper.GetStringSlice("etcd_nodes")
	if etcdNodes == nil || len(etcdNodes) == 0 {
		etcdNodes = []string{"127.0.0.1:2380"}
	}

	return &proxyConfig{
		pathPrefix: pathPrefix,
		failureTTL: failureTTL,
		etcdNodes:  etcdNodes,
	}, nil
}

func main() {
	config, err := getConfig()
	if err != nil {
		fmt.Printf("Failed to read config file %s", err)
		return
	}
	fmt.Printf("config: %v\n", config)

	brokerCfg := broker.EtcdConfig{
		PathPrefix: config.pathPrefix,
		FailureTTL: config.failureTTL,
	}
	metaBroker, err := broker.NewEtcdMetaBrokerFromEndpoints(&brokerCfg, config.etcdNodes)
	if err != nil {
		fmt.Printf("Failed to create meta broker %s", err)
		return
	}
	maniBroker, err := broker.NewEtcdMetaManipulationBrokerFromEndpoints(&brokerCfg, config.etcdNodes)
	if err != nil {
		fmt.Printf("Failed to create manipulation broker %s", err)
		return
	}

	ctx := context.Background()

	proxy := service.NewHTTPBrokerProxy(ctx, metaBroker, maniBroker, "127.0.0.1:7799")
	proxy.Serve()
}
