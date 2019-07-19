package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	"github.com/doyoubi/overmoon/src/broker"
)

type proxyConfig struct {
	address        string
	pathPrefix     string
	failureTTL     int64
	etcdNodes      []string
	migrationLimit int64
}

func getConfig() (*proxyConfig, error) {
	data, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		return nil, err
	}

	viper.SetConfigType("YAML")
	viper.ReadConfig(bytes.NewBuffer(data))

	address := viper.GetString("address")
	if address == "" {
		address = "127.0.0.1:7799"
	}
	pathPrefix := viper.GetString("path_prefix")
	if pathPrefix == "" {
		pathPrefix = "/undermoon"
	}
	failureTTL := viper.GetInt64("failure_ttl")
	if failureTTL <= 0 {
		failureTTL = 60
	}
	etcdNodes := viper.GetStringSlice("etcd_nodes")
	if etcdNodes == nil || len(etcdNodes) == 0 {
		etcdNodes = []string{"127.0.0.1:2380"}
	}

	migrationLimit := viper.GetInt64("migration_limit")
	if migrationLimit <= 0 {
		migrationLimit = 2
	}

	return &proxyConfig{
		address:        address,
		pathPrefix:     pathPrefix,
		failureTTL:     failureTTL,
		etcdNodes:      etcdNodes,
		migrationLimit: migrationLimit,
	}, nil
}

// RunBrokerService runs the broker service
func RunBrokerService() error {
	config, err := getConfig()
	if err != nil {
		errStr := fmt.Sprintf("Failed to read config file %s", err)
		log.Info(errStr)
		return errors.New(errStr)
	}
	log.Infof("config: %+v\n", config)

	brokerCfg := &broker.EtcdConfig{
		PathPrefix:     config.pathPrefix,
		FailureTTL:     config.failureTTL,
		MigrationLimit: config.migrationLimit,
	}
	metaBroker, err := broker.NewEtcdMetaBrokerFromEndpoints(brokerCfg, config.etcdNodes)
	if err != nil {
		errStr := fmt.Sprintf("Failed to create meta broker %s", err)
		log.Info(errStr)
		return errors.New(errStr)
	}
	maniBroker, err := broker.NewEtcdMetaManipulationBrokerFromEndpoints(brokerCfg, config.etcdNodes, metaBroker)
	if err != nil {
		errStr := fmt.Sprintf("Failed to create manipulation broker %s", err)
		log.Info(errStr)
		return errors.New(errStr)
	}

	ctx := context.Background()

	proxy := NewHTTPBrokerProxy(ctx, metaBroker, maniBroker, config.address)

	err = maniBroker.InitGlobalEpoch()
	if err != nil {
		log.Errorf("failed to init global epoch %v", err)
		return err
	}

	group, ctx := errgroup.WithContext(context.Background())
	group.Go(func() error {
		err := proxy.Serve()
		if err != nil {
			log.Infof("HTTP server exited with %v", err)
			return err
		}
		<-ctx.Done()
		return errors.New("HTTP server exited")
	})
	group.Go(func() error {
		err := metaBroker.Serve(ctx)
		log.Infof("meta broker exited with %v", err)
		return errors.New("meta broker exited")
	})
	return group.Wait()
}
