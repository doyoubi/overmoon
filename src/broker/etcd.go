package broker

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/clientv3"
)

// EtcdMetaBroker implements the MetaDataBroker interface and serves as a broker backend.
type EtcdMetaBroker struct {
	config *EtcdConfig
	client *clientv3.Client
}

func NewEtcdMetaBrokerFromEndpoints(config *EtcdConfig, endpoints []string) (*EtcdMetaBroker, error) {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return NewEtcdMetaBroker(config, client)
}

// NewEtcdMetaBroker creates EtcdMetaBroker
func NewEtcdMetaBroker(config *EtcdConfig, client *clientv3.Client) (*EtcdMetaBroker, error) {
	return &EtcdMetaBroker{
		config: config,
		client: client,
	}, nil
}

// GetClusterNames retrieves all the cluster names from etcd.
func (broker *EtcdMetaBroker) GetClusterNames(ctx context.Context) ([]string, error) {
	hostKeyPrefix := fmt.Sprintf("%s/clusters/epoch/", broker.config.PathPrefix)
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
	}
	res, err := broker.client.Get(ctx, hostKeyPrefix, opts...)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(res.Kvs))
	for _, item := range res.Kvs {
		names = append(names, string(item.Key))
	}
	return names, nil
}

func (*EtcdMetaBroker) GetCluster(ctx context.Context, name string) (*Cluster, error) {
	return nil, nil
}

func (*EtcdMetaBroker) GetHostAddresses(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (*EtcdMetaBroker) GetHost(ctx context.Context, address string) (*Host, error) {
	return nil, nil
}

func (*EtcdMetaBroker) AddFailure(ctx context.Context, address string, reportID string) error {
	return nil
}

type EtcdConfig struct {
	PathPrefix string
}
