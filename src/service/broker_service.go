package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	"github.com/doyoubi/overmoon/src/broker"
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

// RunBrokerService runs the broker service
func RunBrokerService() error {
	config, err := getConfig()
	if err != nil {
		errStr := fmt.Sprintf("Failed to read config file %s", err)
		log.Print(errStr)
		return errors.New(errStr)
	}
	fmt.Printf("config: %v\n", config)

	brokerCfg := &broker.EtcdConfig{
		PathPrefix: config.pathPrefix,
		FailureTTL: config.failureTTL,
	}
	metaBroker, err := broker.NewEtcdMetaBrokerFromEndpoints(brokerCfg, config.etcdNodes)
	if err != nil {
		errStr := fmt.Sprintf("Failed to create meta broker %s", err)
		log.Print(errStr)
		return errors.New(errStr)
	}
	maniBroker, err := broker.NewEtcdMetaManipulationBrokerFromEndpoints(brokerCfg, config.etcdNodes, metaBroker)
	if err != nil {
		errStr := fmt.Sprintf("Failed to create manipulation broker %s", err)
		log.Print(errStr)
		return errors.New(errStr)
	}

	ctx := context.Background()

	proxy := NewHTTPBrokerProxy(ctx, metaBroker, maniBroker, "127.0.0.1:7799")

	err = maniBroker.InitGlobalEpoch()
	if err != nil {
		log.Printf("failed to init global epoch %v", err)
		return err
	}

	group, ctx := errgroup.WithContext(context.Background())
	group.Go(func() error {
		err := proxy.Serve()
		if err != nil {
			log.Printf("HTTP server exited with %v", err)
			return err
		}
		<-ctx.Done()
		return errors.New("HTTP server exited")
	})
	group.Go(func() error {
		err := metaBroker.Serve(ctx)
		log.Printf("meta broker exited with %v", err)
		return errors.New("meta broker exited")
	})
	return group.Wait()
}
