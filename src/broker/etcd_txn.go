package broker

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	conc "go.etcd.io/etcd/clientv3/concurrency"
)

// TxnBroker implements some building block for transaction.
type TxnBroker struct {
	config *EtcdConfig
	stm    conc.STM
}

// NewTxnBroker creates TxnBroker.
func NewTxnBroker(config *EtcdConfig, stm conc.STM) *TxnBroker {
	return &TxnBroker{
		config: config,
		stm:    stm,
	}
}

// TODO: exclude failed proxies
func (broker *TxnBroker) consumeProxies(clusterName string, proxyNum uint64, possiblyFreeProxies []string) (map[string]*ProxyStore, error) {
	// Etcd limits the operation number inside a transaction
	const tryNum uint64 = 100

	availableProxies := make(map[string]*ProxyStore)
	for i, address := range possiblyFreeProxies {
		if uint64(len(availableProxies)) == proxyNum {
			break
		}
		if uint64(i) >= tryNum {
			return nil, ErrNoAvailableResource
		}
		proxyKey := fmt.Sprintf("%s/all_proxies/%s", broker.config.PathPrefix, address)
		proxyData := broker.stm.Get(proxyKey)
		meta := &ProxyStore{}
		err := meta.Decode([]byte(proxyData))
		if err != nil {
			log.Errorf("invalid proxy meta format: %s", proxyData)
			continue
		}
		if meta.ClusterName != "" {
			continue
		}
		availableProxies[address] = meta
	}

	if uint64(len(availableProxies)) < proxyNum {
		return nil, ErrNoAvailableResource
	}

	var proxyIndex uint64
	for address, meta := range availableProxies {
		proxyKey := fmt.Sprintf("%s/all_proxies/%s", broker.config.PathPrefix, address)
		meta.ClusterName = clusterName
		meta.ProxyIndex = proxyIndex
		proxyIndex++
		metaStr, err := meta.Encode()
		if err != nil {
			return nil, nil
		}
		broker.stm.Put(proxyKey, string(metaStr))
	}

	return availableProxies, nil
}

func (broker *TxnBroker) createCluster(clusterName string, nodes []*NodeStore) error {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", broker.config.PathPrefix)
	globalEpochStr := broker.stm.Get(globalEpochKey)
	if globalEpochStr == "" {
		// TODO: make this safer by returning a error for the case that
		// this key might be lost.
		globalEpochStr = "0"
	}
	globalEpoch, err := strconv.ParseUint(globalEpochStr, 10, 64)
	if err != nil {
		return errors.WithStack(err)
	}
	newGlobalEpoch := globalEpoch + 1
	newGlobalEpochStr := strconv.FormatUint(newGlobalEpoch, 10)

	clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", broker.config.PathPrefix, clusterName)
	clusterEpoch := broker.stm.Get(clusterEpochKey)
	if clusterEpoch != "" {
		return ErrClusterExists
	}
	broker.stm.Put(clusterEpochKey, newGlobalEpochStr)
	broker.stm.Put(globalEpochKey, newGlobalEpochStr)

	clusterNodesKey := fmt.Sprintf("%s/clusters/nodes/%s", broker.config.PathPrefix, clusterName)
	cluster := &ClusterStore{Nodes: nodes}
	clusterData, err := cluster.Encode()
	if err != nil {
		return err
	}
	broker.stm.Put(clusterNodesKey, string(clusterData))

	return nil
}

func (broker *TxnBroker) getCluster(clusterName string) (uint64, uint64, *ClusterStore, error) {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", broker.config.PathPrefix)
	clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", broker.config.PathPrefix, clusterName)
	clusterNodesKey := fmt.Sprintf("%s/clusters/nodes/%s", broker.config.PathPrefix, clusterName)

	globalEpochStr := broker.stm.Get(globalEpochKey)
	clusterEpochStr := broker.stm.Get(clusterEpochKey)
	clusterNodesStr := broker.stm.Get(clusterNodesKey)

	globalEpoch, err := strconv.ParseUint(globalEpochStr, 10, 64)
	if err != nil {
		return 0, 0, nil, errors.WithStack(err)
	}
	clusterEpoch, err := strconv.ParseUint(clusterEpochStr, 10, 64)
	if err != nil {
		return 0, 0, nil, errors.WithStack(err)
	}

	cluster := &ClusterStore{}
	err = cluster.Decode([]byte(clusterNodesStr))
	if err != nil {
		return 0, 0, nil, err
	}

	return globalEpoch, clusterEpoch, cluster, nil
}

func (broker *TxnBroker) updateCluster(clusterName string, oldGlobalEpoch uint64, cluster *ClusterStore) error {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", broker.config.PathPrefix)
	clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", broker.config.PathPrefix, clusterName)
	clusterNodesKey := fmt.Sprintf("%s/clusters/nodes/%s/", broker.config.PathPrefix, clusterName)

	newGlobalEpoch := oldGlobalEpoch + 1
	newGlobalEpochStr := strconv.FormatUint(newGlobalEpoch, 10)
	broker.stm.Put(globalEpochKey, newGlobalEpochStr)
	broker.stm.Put(clusterEpochKey, newGlobalEpochStr)

	newClusterData, err := cluster.Encode()
	if err != nil {
		return err
	}
	broker.stm.Put(clusterNodesKey, string(newClusterData))
	return nil
}

func (broker *TxnBroker) setFailed(proxyAddress string) error {
	proxyKey := fmt.Sprintf("%s/all_proxies/%s", broker.config.PathPrefix, proxyAddress)
	failedProxyKey := fmt.Sprintf("%s/failed_proxies/%s", broker.config.PathPrefix, proxyAddress)

	proxyData := broker.stm.Get(proxyKey)
	proxy := &ProxyStore{}
	err := proxy.Decode([]byte(proxyData))
	if err != nil {
		return err
	}

	broker.stm.Del(proxyKey)

	failedProxy := FailedProxyStore{
		NodeAddresses: proxy.NodeAddresses,
	}
	data, err := failedProxy.Encode()
	if err != nil {
		return err
	}

	broker.stm.Put(failedProxyKey, string(data))
	return nil
}

func (broker *TxnBroker) initGlobalEpoch() error {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", broker.config.PathPrefix)
	globalEpochStr := broker.stm.Get(globalEpochKey)
	if globalEpochStr == "" {
		broker.stm.Put(globalEpochKey, "0")
	}
	return nil
}
