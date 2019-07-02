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

type EtcdMetaManipulationBroker struct {
	metaDataBroker *EtcdMetaBroker
	config         *EtcdConfig
	client         *clientv3.Client
}

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

	meta := &proxyMeta{
		ProxyIndex:    0,
		ClusterName:   "",
		NodeAddresses: nodes,
	}
	metaValue, err := meta.encode()
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

	proxyMetadata := broker.metaDataBroker.getAvailableProxies(ctx)
	possiblyAvailableProxies := make([]string, 0)
	for address := range proxyMetadata {
		possiblyAvailableProxies = append(possiblyAvailableProxies, address)
	}

	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		txn := NewTxnBroker(broker.config, s)
		proxies, err := txn.consumeProxies(clusterName, nodeNum/2, possiblyAvailableProxies)
		if err != nil {
			return err
		}
		nodes, err := broker.genNodes(clusterName, proxies)
		if err != nil {
			return err
		}
		txn.createCluster(clusterName, nodes)
		return nil
	})
	if err != nil {
		return err
	}
	log.Printf("response %s", response)

	return nil
}

func (broker *EtcdMetaManipulationBroker) genNodes(clusterName string, proxyMetadata map[string]*proxyMeta) ([]*Node, error) {
	proxyNum := uint64(len(proxyMetadata))
	nodeNum := proxyNum * 2
	gap := (MaxSlotNumber + nodeNum - 1) / nodeNum
	nodes := make([]*Node, 0, nodeNum)

	var index uint64
	for proxyAddress, meta := range proxyMetadata {
		if len(meta.NodeAddresses) != 2 {
			return nil, ErrInvalidNodesNum
		}
		end := (index+1)*gap - 1
		if MaxSlotNumber < end {
			end = MaxSlotNumber
		}
		slots := SlotRange{
			Start: index * gap,
			End:   end,
			Tag:   SlotRangeTag{TagType: NoneTag},
		}
		// TODO: add replication info
		master := &Node{
			Address:      meta.NodeAddresses[0],
			ProxyAddress: proxyAddress,
			ClusterName:  clusterName,
			Slots:        []SlotRange{slots},
			Role:         MasterRole,
		}
		replica := &Node{
			Address:      meta.NodeAddresses[1],
			ProxyAddress: proxyAddress,
			ClusterName:  clusterName,
			Slots:        []SlotRange{},
			Role:         ReplicaRole,
		}
		nodes = append(nodes, master)
		nodes = append(nodes, replica)

		index++
	}

	return nodes, nil
}

// // TODO: timeout
// func (broker *EtcdMetaManipulationBroker) createBasicClusterMeta(ctx context.Context, clusterName string, proxyNum uint64, nodes []*Node) error {
// 	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
// 		return nil
// 	})

// 	log.Printf("create cluster %v", response)

// 	return err
// }

func (broker *EtcdMetaManipulationBroker) CreateNode(ctx context.Context, clusterName string, currClusterEpoch int64, slotRanges []SlotRange, role Role) (*Node, error) {
	return broker.allocateNode(ctx, clusterName, currClusterEpoch, slotRanges, role, broker.addNode)
}

func (broker *EtcdMetaManipulationBroker) allocateNode(ctx context.Context, clusterName string, currClusterEpoch int64, slotRanges []SlotRange, role Role,
	commitFunc func(context.Context, int64, *Node) error) (*Node, error) {

	hostAddresses, err := broker.metaDataBroker.GetHostAddresses(ctx)
	if err != nil {
		return nil, err
	}
	log.Printf("Get hosts %v", hostAddresses)

	for _, hostAddress := range hostAddresses {
		nodes, err := broker.getAllNodesByHost(ctx, hostAddress)
		if err != nil {
			log.Printf("Failed to get nodes %s", err)
			continue
		}
		log.Printf("Get nodes %v", nodes)

		// TODO: need to check the number of existing nodes on this host.
		// TODO: shuffle the nodes to avoid collision

		for nodeAddress, cluster := range nodes {
			if cluster != "" {
				continue
			}
			node := &Node{
				Address:      nodeAddress,
				ProxyAddress: hostAddress,
				ClusterName:  clusterName,
				Slots:        slotRanges,
				Role:         role,
			}
			err := commitFunc(ctx, currClusterEpoch, node)
			if err == ErrClusterEpochChanged {
				return nil, err
			} else if err == ErrHostNotExist {
				break
			} else if err == ErrNodeNotAvailable {
				continue
			} else if err != nil {
				log.Printf("unexpected error: %s", err)
				return nil, err
			}
			return node, nil
		}
	}

	return nil, ErrNoAvailableResource
}

func (broker *EtcdMetaManipulationBroker) addNode(ctx context.Context, currClusterEpoch int64, node *Node) error {
	// TODO: timeout and isolation level
	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		return NewTxnBroker(broker.config, s).AddNode(node, currClusterEpoch)
	})

	log.Printf("resp %v", response.Succeeded)

	return err
}

func (broker *EtcdMetaManipulationBroker) getAllNodesByHost(ctx context.Context, hostAddress string) (map[string]string, error) {
	allNodesPrefix := fmt.Sprintf("%s/hosts/all_nodes/%s/", broker.config.PathPrefix, hostAddress)
	kvs, err := getRangeKeyPostfixAndValue(ctx, broker.client, allNodesPrefix)
	if err != nil {
		return nil, err
	}
	nodeAddresses := make(map[string]string)
	for k, v := range kvs {
		nodeAddresses[k] = string(v)
	}
	return nodeAddresses, nil
}

func (broker *EtcdMetaManipulationBroker) DeleteNode(ctx context.Context, currClusterEpoch int64, node *Node) error {
	// TODO: timeout and isolation level
	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		return NewTxnBroker(broker.config, s).RemoveNode(node, currClusterEpoch)
	})

	if response != nil {
		log.Printf("resp %v", response.Succeeded)
	}

	return err
}

func (broker *EtcdMetaManipulationBroker) ReplaceNode(ctx context.Context, currClusterEpoch int64, node *Node) (*Node, error) {
	return broker.allocateNode(ctx, node.ClusterName, currClusterEpoch, node.Slots, node.Role,
		func(ctx context.Context, currClusterEpoch int64, newNode *Node) error {
			return broker.swapNode(ctx, currClusterEpoch, node, newNode)
		})
}

func (broker *EtcdMetaManipulationBroker) swapNode(ctx context.Context, currClusterEpoch int64, oldNode, newNode *Node) error {
	// TODO: timeout and isolation level
	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		return NewTxnBroker(broker.config, s).ReplaceNode(oldNode, newNode, currClusterEpoch)
	})

	if response != nil {
		log.Printf("resp %v", response.Succeeded)
	}

	return err
}
