package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	conc "go.etcd.io/etcd/clientv3/concurrency"
)

const halfChunkSize = 2
const chunkSize = halfChunkSize * 2

// ErrClusterExists indicates the cluster has already existed.
var ErrClusterExists = errors.New("cluster already exists")

// ErrInvalidNodesNum indicates invalid node number.
var ErrInvalidNodesNum = errors.New("invalid node number")

// ErrInvalidRequestedNodesNum indicates invalid node number.
var ErrInvalidRequestedNodesNum = errors.New("invalid node number")

// ErrHostExists indicates the proxy has already existed.
var ErrHostExists = errors.New("host already existed")

// ErrNoAvailableResource indicates no available resource.
var ErrNoAvailableResource = errors.New("no available resource")

// ErrAllocatedProxyInUse tells the client to try again.
var ErrAllocatedProxyInUse = errors.New("allocated proxy is in use")

// ErrEmptyChunksExist indicates that the cluster still need to do
// the migration for the remaining empty chunks before adding
// more nodes.
var ErrEmptyChunksExist = errors.New("there're empty chunks")

// EtcdMetaManipulationBroker is mainly for metadata modification
type EtcdMetaManipulationBroker struct {
	metaDataBroker *EtcdMetaBroker
	config         *EtcdConfig
	client         *clientv3.Client
}

// NewEtcdMetaManipulationBrokerFromEndpoints creates EtcdMetaManipulationBroker from endpoints
func NewEtcdMetaManipulationBrokerFromEndpoints(config *EtcdConfig, endpoints []string, metaDataBroker *EtcdMetaBroker) (*EtcdMetaManipulationBroker, error) {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return NewEtcdMetaManipulationBroker(config, client, metaDataBroker)
}

// NewEtcdMetaManipulationBroker creates EtcdMetaManipulationBroker
func NewEtcdMetaManipulationBroker(config *EtcdConfig, client *clientv3.Client, metaDataBroker *EtcdMetaBroker) (*EtcdMetaManipulationBroker, error) {
	return &EtcdMetaManipulationBroker{
		metaDataBroker: metaDataBroker,
		config:         config,
		client:         client,
	}, nil
}

// GetMetaBroker returens the EtcdMetaBroker
func (broker *EtcdMetaManipulationBroker) GetMetaBroker() *EtcdMetaBroker {
	return broker.metaDataBroker
}

// InitGlobalEpoch initializes the global_epoch if it does not exist
func (broker *EtcdMetaManipulationBroker) InitGlobalEpoch() error {
	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		return NewTxnBroker(broker.config, s).initGlobalEpoch()
	})
	if err != nil {
		return errors.WithStack(err)
	}
	log.Infof("Successfully initialize global epoch. response %v", response)
	return nil
}

// AddProxy adds new proxy and removes it from failed proxies
func (broker *EtcdMetaManipulationBroker) AddProxy(ctx context.Context, address string, nodes []string) error {
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

		hostData := s.Get(proxyKey)
		if hostData != "" {
			return ErrHostExists
		}

		s.Put(proxyKey, string(metaValue))
		s.Del(failedProxyKey)
		return nil
	})
	if err != nil {
		log.Errorf("failed to add host. response: %v. error: %v", response, err)
		return err
	}
	return nil
}

// CreateCluster creates a new cluster with specified node number
func (broker *EtcdMetaManipulationBroker) CreateCluster(ctx context.Context, clusterName string, nodeNum uint64) error {
	if nodeNum%chunkSize != 0 || nodeNum%halfChunkSize != 0 || nodeNum == 0 {
		return ErrInvalidNodesNum
	}

	possiblyAvailableProxies, err := broker.metaDataBroker.getAvailableProxyAddresses(ctx)
	if err != nil {
		return err
	}
	if uint64(len(possiblyAvailableProxies)*halfChunkSize) < nodeNum {
		log.Infof("failed to create cluster, only found %d proxies, expected %d", len(possiblyAvailableProxies), nodeNum)
		return ErrNoAvailableResource
	}

	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		txn := NewTxnBroker(broker.config, s)
		_, _, cluster, err := txn.getCluster(clusterName)
		if err == nil && cluster != nil {
			return ErrClusterExists
		}
		if err != nil && err != ErrClusterNotFound {
			return err
		}

		existingChunks := make([]*NodeChunkStore, 0)
		chunks, err := txn.consumeChunks(clusterName, nodeNum/2, possiblyAvailableProxies, existingChunks)
		if err != nil {
			return err
		}
		chunks = initChunkSlots(chunks)
		cluster = &ClusterStore{Chunks: chunks}
		txn.createCluster(clusterName, cluster)
		return nil
	})
	if err != nil {
		log.Errorf("failed to create cluster. response: %v. error: %v", response, err)
		return err
	}
	return nil
}

// ReplaceProxy changes the proxy and return the new one.
func (broker *EtcdMetaManipulationBroker) ReplaceProxy(ctx context.Context, address string) (*Host, error) {
	_, proxy, err := broker.metaDataBroker.getProxyMetaFromEtcd(ctx, address)
	if err != nil {
		return nil, err
	}
	clusterName := proxy.ClusterName
	if clusterName == "" {
		log.Warnf("%s not in use", address)
		return nil, ErrProxyNotInUse
	}

	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		txn := NewTxnBroker(broker.config, s)

		globalEpoch, _, cluster, err := txn.getCluster(clusterName)
		if err != nil {
			return err
		}

		return txn.takeover(clusterName, address, globalEpoch, cluster)
	})

	if err != nil {
		log.Errorf("failed to takeover. response: %v. error: %v", response, err)
		return nil, err
	}

	possiblyAvailableProxies, err := broker.metaDataBroker.getAvailableProxyAddresses(ctx)
	if err != nil {
		return nil, err
	}

	var newProxyAddress string

	response, err = conc.NewSTM(broker.client, func(s conc.STM) error {
		txn := NewTxnBroker(broker.config, s)

		globalEpoch, _, cluster, err := txn.getCluster(clusterName)
		if err != nil {
			return err
		}

		newProxyAddress, err = txn.replaceProxy(clusterName, address, globalEpoch, cluster, possiblyAvailableProxies)
		return err
	})

	if err != nil {
		log.Errorf("failed to replace proxy. response: %+v. error: %v %s", response, err, err)
		return nil, err
	}

	// TODO: get proxy directly from cache or change api.
	return broker.metaDataBroker.GetProxy(ctx, newProxyAddress)
}

// AddNodesToCluster adds chunks to cluster.
func (broker *EtcdMetaManipulationBroker) AddNodesToCluster(ctx context.Context, clusterName string) error {
	possiblyAvailableProxies, err := broker.metaDataBroker.getAvailableProxyAddresses(ctx)
	if err != nil {
		return err
	}
	if len(possiblyAvailableProxies) == 0 {
		return ErrNoAvailableResource
	}

	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		txn := NewTxnBroker(broker.config, s)

		globalEpoch, _, cluster, err := txn.getCluster(clusterName)
		if err != nil {
			return err
		}
		if cluster.HasEmptyChunks() {
			return ErrEmptyChunksExist
		}
		log.Infof("start to add nodes to cluster %s", clusterName)
		expectedNodeNum := uint64(len(cluster.Chunks)*chunkSize) * 2
		return txn.addNodesToCluster(clusterName, expectedNodeNum, cluster, globalEpoch, possiblyAvailableProxies)
	})

	if err != nil {
		log.Errorf("failed to add nodes. response: %v. error: %v", response, err)
	}
	return err
}

// MigrateSlots splits the slots to another half cluster.
func (broker *EtcdMetaManipulationBroker) MigrateSlots(ctx context.Context, clusterName string) error {
	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		txn := NewTxnBroker(broker.config, s)
		return txn.migrateSlots(clusterName)
	})

	if err != nil {
		log.Errorf("failed to start migration. response: %v. error: %v", response, err)
	}
	return err
}

// CommitMigration finishes the migration.
func (broker *EtcdMetaManipulationBroker) CommitMigration(ctx context.Context, task MigrationTaskMeta) error {
	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		txn := NewTxnBroker(broker.config, s)
		return txn.commitMigration(task)
	})

	if err != nil {
		log.Errorf("failed to commit migration. response: %v. error: %v", response, err)
	}
	return err
}

// RemoveProxy remove a free proxy.
func (broker *EtcdMetaManipulationBroker) RemoveProxy(ctx context.Context, address string) error {
	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		txn := NewTxnBroker(broker.config, s)
		return txn.removeProxy(address)
	})

	if err != nil {
		log.Errorf("failed to remove proxy. response: %v. error: %v", response, err)
	}
	return err
}

// RemoveUnusedProxiesFromCluster free the unused proxies.
func (broker *EtcdMetaManipulationBroker) RemoveUnusedProxiesFromCluster(ctx context.Context, clusterName string) error {
	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		txn := NewTxnBroker(broker.config, s)
		return txn.removeUnusedProxiesFromCluster(clusterName)
	})

	if err != nil {
		log.Errorf("failed to remove proxy from cluster. response: %v. error: %v", response, err)
	}
	return err
}

// RemoveCluster removes the cluster.
func (broker *EtcdMetaManipulationBroker) RemoveCluster(ctx context.Context, clusterName string) error {
	response, err := conc.NewSTM(broker.client, func(s conc.STM) error {
		txn := NewTxnBroker(broker.config, s)
		return txn.removeCluster(clusterName)
	})

	if err != nil {
		log.Errorf("failed to remove cluster. response: %v. error: %v", response, err)
	}
	return err
}
