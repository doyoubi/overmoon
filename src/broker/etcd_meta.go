package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/clientv3"
)

// TODO: Need to wrap each API into a transaction.

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
	clusterKeyPrefix := fmt.Sprintf("%s/clusters/epoch/", broker.config.PathPrefix)
	kvs, err := getRangeKeyPostfixAndValue(ctx, broker.client, clusterKeyPrefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	return keys, nil
}

// GetCluster queries a cluster by name
func (broker *EtcdMetaBroker) GetCluster(ctx context.Context, name string) (*Cluster, error) {
	epoch, err := broker.GetEpochByCluster(ctx, name)
	if err != nil {
		return nil, err
	}
	nodes, err := broker.GetNodesByCluster(ctx, name)
	if err != nil {
		return nil, err
	}
	cluster := &Cluster{
		Name:  name,
		Epoch: epoch,
		Nodes: nodes,
	}
	return cluster, nil
}

// GetHostAddresses queries all hosts.
func (broker *EtcdMetaBroker) GetHostAddresses(ctx context.Context) ([]string, error) {
	hostKeyPrefix := fmt.Sprintf("%s/hosts/epoch/", broker.config.PathPrefix)
	kvs, err := getRangeKeyPostfixAndValue(ctx, broker.client, hostKeyPrefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	return keys, nil
}

// GetHost query the host by address
func (broker *EtcdMetaBroker) GetHost(ctx context.Context, address string) (*Host, error) {
	epoch, err := broker.GetEpochByHost(ctx, address)
	if err != nil {
		return nil, err
	}
	nodes, err := broker.GetNodesByHost(ctx, address)
	if err != nil {
		return nil, err
	}
	return &Host{
		Address: address,
		Epoch:   epoch,
		Nodes:   nodes,
	}, nil
}

func (broker *EtcdMetaBroker) AddFailure(ctx context.Context, address string, reportID string) error {
	key := fmt.Sprintf("%s/failures/%s/%s", broker.config.PathPrefix, address, reportID)
	timestamp := time.Now().Unix()
	value := fmt.Sprintf("%v", timestamp)
	res, err := broker.client.Grant(ctx, broker.config.FailureTTL)
	if err != nil {
		return err
	}
	opts := []clientv3.OpOption{
		clientv3.WithLease(clientv3.LeaseID(res.ID)),
	}
	_, err = broker.client.Put(ctx, key, value, opts...)
	return err
}

func (broker *EtcdMetaBroker) GetFailures(ctx context.Context) ([]string, error) {
	prefix := fmt.Sprintf("%s/failures/", broker.config.PathPrefix)
	kv, err := getRangeKeyPostfixAndValue(ctx, broker.client, prefix)
	if err != nil {
		return nil, err
	}
	addresses := make([]string, 0, len(kv))
	for k := range kv {
		segs := strings.SplitN(string(k), "/", 2)
		if len(segs) != 2 {
			return nil, fmt.Errorf("invalid failure key %s", string(k))
		}
		nodeAddress := segs[0]
		addresses = append(addresses, nodeAddress)
	}
	return addresses, nil
}

func (broker *EtcdMetaBroker) GetEpochByCluster(ctx context.Context, name string) (int64, error) {
	key := fmt.Sprintf("%s/clusters/epoch/%s", broker.config.PathPrefix, name)
	return broker.GetEpoch(ctx, key)
}

func (broker *EtcdMetaBroker) GetEpochByHost(ctx context.Context, address string) (int64, error) {
	key := fmt.Sprintf("%s/hosts/epoch/%s", broker.config.PathPrefix, address)
	return broker.GetEpoch(ctx, key)
}

func (broker *EtcdMetaBroker) GetEpoch(ctx context.Context, key string) (int64, error) {
	// key := fmt.Sprintf("%s/hosts/epoch/%s", broker.config.PathPrefix, address)
	res, err := broker.client.Get(ctx, key)
	if err != nil {
		return 0, err
	}

	if len(res.Kvs) != 1 {
		return 0, NotExists
	}
	kv := res.Kvs[0]

	epoch, err := strconv.ParseInt(string(kv.Value), 10, 64)
	return epoch, err
}

// GetNodesByCluster queries all the nodes under a cluster.
func (broker *EtcdMetaBroker) GetNodesByCluster(ctx context.Context, name string) ([]*Node, error) {
	nodesKeyPrefix := fmt.Sprintf("%s/clusters/nodes/%s/", broker.config.PathPrefix, name)
	kvs, err := getRangeKeyPostfixAndValue(ctx, broker.client, nodesKeyPrefix)
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, 0, len(kvs))
	for _, v := range kvs {
		node := &Node{}
		err := json.Unmarshal(v, node)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// GetNodesByHost queries all the nodes under a host.
func (broker *EtcdMetaBroker) GetNodesByHost(ctx context.Context, address string) ([]*Node, error) {
	hostsKeyPrefix := fmt.Sprintf("%s/hosts/%s/nodes/", broker.config.PathPrefix, address)
	kvs, err := getRangeKeyPostfixAndValue(ctx, broker.client, hostsKeyPrefix)
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, 0, len(kvs))
	for _, v := range kvs {
		node := &Node{}
		err := json.Unmarshal(v, node)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func getRangeKeyPostfixAndValue(ctx context.Context, client *clientv3.Client, prefix string) (map[string][]byte, error) {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
	}
	res, err := client.Get(ctx, prefix, opts...)
	if err != nil {
		return nil, err
	}
	postfixes := make(map[string][]byte)
	for _, item := range res.Kvs {
		key := string(item.Key)
		if !strings.HasPrefix(key, prefix) {
			return nil, fmt.Errorf("Unexpected key %s", key)
		}
		postfix := strings.TrimPrefix(key, prefix)
		postfixes[postfix] = item.Value
	}
	return postfixes, nil
}

// EtcdConfig stores broker config.
type EtcdConfig struct {
	PathPrefix string
	FailureTTL int64
}
