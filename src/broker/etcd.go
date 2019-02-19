package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
		key := string(item.Key)
		if !strings.HasPrefix(key, hostKeyPrefix) {
			return nil, errors.New(fmt.Sprintf("Unexpected key %s", key))
		}
		name := strings.TrimPrefix(key, hostKeyPrefix)
		names = append(names, name)
	}
	return names, nil
}

func (broker *EtcdMetaBroker) GetCluster(ctx context.Context, name string) (*Cluster, error) {
	epoch, err := broker.GetEpoch(ctx, name)
	if err != nil {
		return nil, err
	}
	nodes, err := broker.GetNodes(ctx, name)
	cluster := &Cluster{
		Name:  name,
		Epoch: epoch,
		Nodes: nodes,
	}
	return cluster, nil
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

func (broker *EtcdMetaBroker) GetEpoch(ctx context.Context, name string) (int64, error) {
	key := fmt.Sprintf("%s/clusters/epoch/%s", broker.config.PathPrefix, name)
	res, err := broker.client.Get(ctx, key)
	if err != nil {
		return 0, err
	}

	if len(res.Kvs) != 1 {
		return 0, fmt.Errorf("Unexpected kv number %d", len(res.Kvs))
	}
	kv := res.Kvs[0]

	epoch, err := strconv.ParseInt(string(kv.Value), 10, 64)
	return epoch, err
}

type NodeValue struct {
	Slots [][]int `json:"slots"`
}

// GetRanges parses the json array to SlotRange
func (v *NodeValue) GetRanges() ([]SlotRange, error) {
	slotRanges := make([]SlotRange, 0, len(v.Slots))
	for _, slotRange := range v.Slots {
		if len(slotRange) == 1 || len(slotRange) == 2 {
			slotRanges = append(slotRanges, SlotRange{
				Start: slotRange[0],
				End:   slotRange[len(slotRange)-1],
			})
		} else {
			return nil, fmt.Errorf("Invalid slot range %v", slotRange)
		}
	}
	return slotRanges, nil
}

func (broker *EtcdMetaBroker) GetNodes(ctx context.Context, name string) ([]*Node, error) {
	nodesKeyPrefix := fmt.Sprintf("%s/clusters/nodes/%s/", broker.config.PathPrefix, name)
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
	}
	res, err := broker.client.Get(ctx, nodesKeyPrefix, opts...)
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, 0, len(res.Kvs))
	for _, item := range res.Kvs {
		key := string(item.Key)
		if !strings.HasPrefix(key, nodesKeyPrefix) {
			return nil, fmt.Errorf("Unexpected key %s", key)
		}
		address := strings.TrimPrefix(key, nodesKeyPrefix)
		nodeValue := NodeValue{}
		err := json.Unmarshal(item.Value, &nodeValue)
		if err != nil {
			return nil, err
		}
		slotRanges, err := nodeValue.GetRanges()
		if err != nil {
			return nil, err
		}
		node := Node{
			Address:     address,
			ClusterName: name,
			Slots:       slotRanges,
		}
		nodes = append(nodes, &node)
	}
	return nodes, nil
}

type EtcdConfig struct {
	PathPrefix string
}
