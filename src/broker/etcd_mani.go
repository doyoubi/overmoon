package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"go.etcd.io/etcd/clientv3"
	conc "go.etcd.io/etcd/clientv3/concurrency"
)

var ErrClusterEpochChanged = errors.New("cluster epoch changed")
var ErrClusterExists = errors.New("cluster already exists")
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

func (broker *EtcdMetaManipulationBroker) AddHost(ctx context.Context, address string, nodes []string) error {
	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		hostEpochKey := fmt.Sprintf("%s/hosts/epoch/%s", broker.config.PathPrefix, address)
		hostEpoch := s.Get(hostEpochKey)
		if hostEpoch != "" {
			return ErrHostExists
		}

		s.Put(hostEpochKey, "1")
		for _, nodeAddress := range nodes {
			nodeAddressKey := fmt.Sprintf("%s/hosts/all_nodes/%s/%s", broker.config.PathPrefix, address, nodeAddress)
			// empty string for not being used by any cluster
			s.Put(nodeAddressKey, "")
		}

		return nil
	})
	log.Printf("response %v", response)
	return err
}

func (broker *EtcdMetaManipulationBroker) CreateCluster(ctx context.Context, clusterName string, nodeNum, maxMemory int64) error {
	err := broker.CreateBasicClusterMeta(ctx, clusterName, nodeNum, maxMemory)
	if err != nil {
		return err
	}

	gap := MaxSlotNumber / nodeNum

	for i := int64(0); i != nodeNum; i++ {
		epoch, err := broker.metaDataBroker.GetEpochByCluster(ctx, clusterName)
		if err != nil {
			return err
		}
		slots := SlotRange{
			Start: i * gap,
			End:   (i+1)*gap - 1,
			Tag:   "",
		}
		_, err = broker.CreateNode(ctx, clusterName, epoch, []SlotRange{slots})
		if err != nil {
			return err
		}
	}

	return nil
}

// TODO: timeout
func (broker *EtcdMetaManipulationBroker) CreateBasicClusterMeta(ctx context.Context, clusterName string, nodeNum, maxMemory int64) error {
	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", broker.config.PathPrefix, clusterName)
		clusterEpoch := s.Get(clusterEpochKey)
		if clusterEpoch != "" {
			return ErrClusterExists
		}

		deletingClusterKey := fmt.Sprintf("%s/tasks/operation/remove_nodes/%s", broker.config.PathPrefix, clusterName)
		deletingCluster := s.Get(deletingClusterKey)
		if deletingCluster != "" {
			return ErrClusterExists
		}

		s.Put(clusterEpochKey, "1")
		nodeNumKey := fmt.Sprintf("%s/clusters/spec/node_number/%s", broker.config.PathPrefix, clusterName)
		nodeMaxMemKey := fmt.Sprintf("%s/clusters/spec/node_max_memory/%s", broker.config.PathPrefix, clusterName)
		s.Put(nodeNumKey, strconv.FormatInt(nodeNum, 10))
		s.Put(nodeMaxMemKey, strconv.FormatInt(maxMemory, 10))

		return nil
	})

	log.Printf("create cluster %v", response)

	return err
}

func (broker *EtcdMetaManipulationBroker) CreateNode(ctx context.Context, clusterName string, currClusterEpoch int64, slotRanges []SlotRange) (*Node, error) {
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
			}
			err := broker.addNode(ctx, currClusterEpoch, node)
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
	clusterName := node.ClusterName
	host := node.ProxyAddress
	nodeAddress := node.Address

	// TODO: timeout and isolation level
	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", broker.config.PathPrefix, clusterName)
		// When it's empty there're two cases:
		newClusterEpoch := s.Get(clusterEpochKey)
		if newClusterEpoch != strconv.FormatInt(currClusterEpoch, 10) {
			return ErrClusterEpochChanged
		}

		hostEpochKey := fmt.Sprintf("%s/hosts/epoch/%s", broker.config.PathPrefix, host)
		nodeOwnerCluster := fmt.Sprintf("%s/hosts/all_nodes/%s/%s", broker.config.PathPrefix, host, nodeAddress)
		hostEpoch := s.Get(hostEpochKey)
		cluster := s.Get(nodeOwnerCluster)
		if hostEpoch == "" {
			return ErrHostNotExist
		}
		if cluster != "" {
			return ErrNodeNotAvailable
		}

		s.Put(nodeOwnerCluster, clusterName)

		hostNode := fmt.Sprintf("%s/hosts/%s/nodes/%s", broker.config.PathPrefix, host, nodeAddress)
		jsPayload, err := json.Marshal(node)
		if err != nil {
			return err
		}
		s.Put(hostNode, string(jsPayload))

		clusterNode := fmt.Sprintf("%s/clusters/nodes/%s/%s", broker.config.PathPrefix, clusterName, nodeAddress)
		jsPayload, err = json.Marshal(node)
		if err != nil {
			return err
		}
		s.Put(clusterNode, string(jsPayload))

		s.Put(clusterEpochKey, strconv.FormatInt(currClusterEpoch+1, 10))
		hostEpochInt, err := strconv.ParseInt(hostEpoch, 10, 64)
		if err != nil {
			return err
		}
		s.Put(hostEpochKey, strconv.FormatInt(hostEpochInt+1, 10))

		return nil
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

func (broker *EtcdMetaManipulationBroker) ReplaceNode(ctx context.Context, node *Node) (*Node, error) {
	return nil, nil
}
