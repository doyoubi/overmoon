package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"go.etcd.io/etcd/clientv3"
	conc "go.etcd.io/etcd/clientv3/concurrency"
)

const halfChunkSize = 2
const chunkSize = halfChunkSize * 2

var ErrClusterEpochChanged = errors.New("cluster epoch changed")
var ErrClusterExists = errors.New("cluster already exists")
var ErrInvalidNodesNum = errors.New("invalid node number")
var ErrHostExists = errors.New("host already existed")
var ErrHostNotExist = errors.New("host not exist")
var ErrNodeNotAvailable = errors.New("node not available")
var ErrNoAvailableResource = errors.New("no available resource")

// EtcdMetaManipulationBroker is mainly for metadata modification
type EtcdMetaManipulationBroker struct {
	metaDataBroker *EtcdMetaBroker
	config         *EtcdConfig
	client         *clientv3.Client
}

// NewEtcdMetaManipulationBrokerFromEndpoints creates EtcdMetaManipulationBroker from endpoints
func NewEtcdMetaManipulationBrokerFromEndpoints(config *EtcdConfig, endpoints []string) (*EtcdMetaManipulationBroker, error) {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return NewEtcdMetaManipulationBroker(config, client)
}

// NewEtcdMetaManipulationBroker creates EtcdMetaManipulationBroker
func NewEtcdMetaManipulationBroker(config *EtcdConfig, client *clientv3.Client) (*EtcdMetaManipulationBroker, error) {
	metaDataBroker, err := NewEtcdMetaBroker(config, client)
	if err != nil {
		return nil, err
	}
	return &EtcdMetaManipulationBroker{
		metaDataBroker: metaDataBroker,
		config:         config,
		client:         client,
	}, nil
}

// AddHost adds new proxy and removes it from failed proxies
func (broker *EtcdMetaManipulationBroker) AddHost(ctx context.Context, address string, nodes []string) error {
	if len(nodes) != halfChunkSize {
		return ErrInvalidNodesNum
	}

	meta := &ProxyStore{
		ProxyIndex:    0,
		ClusterName:   "",
		NodeAddresses: nodes,
	}
	metaValue, err := meta.Encode()
	if err != nil {
		return err
	}

	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		proxyKey := fmt.Sprintf("%s/all_proxies/%s", broker.config.PathPrefix, address)
		failedProxyKey := fmt.Sprintf("%s/failed_proxies/%s", broker.config.PathPrefix, address)

		hostEpoch := s.Get(proxyKey)
		if hostEpoch != "" {
			return ErrHostExists
		}

		s.Put(proxyKey, string(metaValue))
		s.Del(failedProxyKey)
		return nil
	})
	log.Printf("response %v", response)
	return err
}

// CreateCluster creates a new cluster with specified node number
func (broker *EtcdMetaManipulationBroker) CreateCluster(ctx context.Context, clusterName string, nodeNum uint64) error {
	if nodeNum%chunkSize != 0 || nodeNum%halfChunkSize != 0 {
		return ErrInvalidNodesNum
	}

	possiblyAvailableProxies := broker.metaDataBroker.getAvailableProxyAddresses(ctx)

	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		txn := NewTxnBroker(broker.config, s)
		proxies, err := txn.consumeProxies(clusterName, nodeNum/2, possiblyAvailableProxies)
		if err != nil {
			return err
		}
		nodes, err := broker.genNodes(proxies)
		if err != nil {
			return err
		}
		txn.createCluster(clusterName, nodes)
		return nil
	})
	if err != nil {
		return err
	}
	log.Printf("response %v", response)

	return nil
}

func (broker *EtcdMetaManipulationBroker) genNodes(proxyMetadata map[string]*ProxyStore) ([]*NodeStore, error) {
	proxyNum := uint64(len(proxyMetadata))
	nodeNum := proxyNum * 2
	gap := (MaxSlotNumber + nodeNum - 1) / nodeNum
	nodes := make([]*NodeStore, 0, nodeNum)

	var index uint64
	for proxyAddress, meta := range proxyMetadata {
		if len(meta.NodeAddresses) != 2 {
			return nil, ErrInvalidNodesNum
		}
		end := (index+1)*gap - 1
		if MaxSlotNumber < end {
			end = MaxSlotNumber
		}
		slots := SlotRangeStore{
			Start: index * gap,
			End:   end,
			Tag:   SlotRangeTagStore{TagType: NoneTag},
		}
		master := &NodeStore{
			NodeAddress:  meta.NodeAddresses[0],
			ProxyAddress: proxyAddress,
			Slots:        []SlotRangeStore{slots},
		}
		replica := &NodeStore{
			NodeAddress:  meta.NodeAddresses[1],
			ProxyAddress: proxyAddress,
			Slots:        []SlotRangeStore{},
		}
		nodes = append(nodes, master)
		nodes = append(nodes, replica)

		index++
	}

	return nodes, nil
}

// ReplaceProxy changes the proxy and return the new one.
func (broker *EtcdMetaManipulationBroker) ReplaceProxy(ctx context.Context, address string) (*Host, error) {
	possiblyAvailableProxies := broker.metaDataBroker.getAvailableProxyAddresses(ctx)
	_, proxy, err := broker.metaDataBroker.getProxyMetaFromEtcd(ctx, address)
	if err != nil {
		return nil, err
	}
	clusterName := proxy.ClusterName
	if clusterName == "" {
		return nil, fmt.Errorf("%s not in use", address)
	}

	var newProxyAddress string

	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		txn := NewTxnBroker(broker.config, s)

		globalEpoch, _, cluster, err := txn.getCluster(clusterName)
		if err != nil {
			return err
		}

		proxies, err := txn.consumeProxies(clusterName, 1, possiblyAvailableProxies)
		if err != nil {
			return err
		}
		if len(proxies) != 1 {
			return fmt.Errorf("expected 1 proxy, got %d", len(proxies))
		}
		var newProxy *ProxyStore
		for k, v := range proxies {
			newProxyAddress = k
			newProxy = v
		}

		exists := false
		for i, node := range cluster.Nodes {
			if node.ProxyAddress == address && i%2 == 0 {
				node.ProxyAddress = newProxyAddress
				node.NodeAddress = newProxy.NodeAddresses[0]
			} else if node.ProxyAddress == address && i%2 == 1 {
				node.ProxyAddress = newProxyAddress
				node.NodeAddress = newProxy.NodeAddresses[0]
				exists = true
				break
			}
		}
		if !exists {
			return fmt.Errorf("cluster %s does not include %s", clusterName, address)
		}

		err = txn.setFailed(address)
		if err != nil {
			return err
		}
		return txn.updateCluster(clusterName, globalEpoch, cluster)
	})

	log.Printf("replace proxy response %v", response)
	if err != nil {
		return nil, err
	}

	return broker.metaDataBroker.GetHost(ctx, newProxyAddress)
}
