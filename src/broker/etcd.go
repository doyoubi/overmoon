package broker

import (
	"time"

	"go.etcd.io/etcd/clientv3"
)

// EtcdMetaBroker implements the MetaDataBroker interface and serves as a broker backend.
type EtcdMetaBroker struct {
	client *clientv3.Client
}

// NewEtcdMetaBroker creates EtcdMetaBroker
func NewEtcdMetaBroker(endpoints []string) (*EtcdMetaBroker, error) {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return &EtcdMetaBroker{
		client: client,
	}, nil
}

func (*EtcdMetaBroker) GetClusterNames() ([]string, error) {
	return nil, nil
}
func (*EtcdMetaBroker) GetCluster(name string) (*Cluster, error) {
	return nil, nil
}
func (*EtcdMetaBroker) GetHostAddresses() ([]string, error) {
	return nil, nil
}
func (*EtcdMetaBroker) GetHost(address string) (*Host, error) {
	return nil, nil
}
func (*EtcdMetaBroker) AddFailure(address string, reportID string) error {
	return nil
}
