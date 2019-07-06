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
func (txn *TxnBroker) consumeChunks(clusterName string, proxyNum uint64, possiblyFreeProxies []string, cluster *ClusterStore) (map[string]*ProxyStore, error) {
	return nil, nil
}

// TODO: exclude failed proxies
func (txn *TxnBroker) consumeProxies(clusterName string, proxyNum uint64, possiblyFreeProxies []string) (map[string]*ProxyStore, error) {
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
		proxyKey := fmt.Sprintf("%s/all_proxies/%s", txn.config.PathPrefix, address)
		proxyData := txn.stm.Get(proxyKey)
		if len(proxyData) == 0 {
			continue
		}
		meta := &ProxyStore{}
		err := meta.Decode([]byte(proxyData))
		if err != nil {
			log.Errorf("invalid proxy meta format: '%s'", proxyData)
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
		proxyKey := fmt.Sprintf("%s/all_proxies/%s", txn.config.PathPrefix, address)
		meta.ClusterName = clusterName
		meta.ProxyIndex = proxyIndex
		proxyIndex++
		metaStr, err := meta.Encode()
		if err != nil {
			return nil, nil
		}
		txn.stm.Put(proxyKey, string(metaStr))
	}

	return availableProxies, nil
}

func (txn *TxnBroker) createCluster(clusterName string, cluster *ClusterStore) error {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", txn.config.PathPrefix)
	globalEpochStr := txn.stm.Get(globalEpochKey)
	if globalEpochStr == "" {
		return ErrGlobalEpochNotFound
	}
	globalEpoch, err := strconv.ParseUint(globalEpochStr, 10, 64)
	if err != nil {
		return errors.WithStack(err)
	}
	newGlobalEpoch := globalEpoch + 1
	newGlobalEpochStr := strconv.FormatUint(newGlobalEpoch, 10)

	clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", txn.config.PathPrefix, clusterName)
	clusterEpoch := txn.stm.Get(clusterEpochKey)
	if clusterEpoch != "" {
		return ErrClusterExists
	}
	txn.stm.Put(clusterEpochKey, newGlobalEpochStr)
	txn.stm.Put(globalEpochKey, newGlobalEpochStr)

	clusterNodesKey := fmt.Sprintf("%s/clusters/nodes/%s", txn.config.PathPrefix, clusterName)
	clusterData, err := cluster.Encode()
	if err != nil {
		return err
	}
	txn.stm.Put(clusterNodesKey, string(clusterData))

	return nil
}

func (txn *TxnBroker) getCluster(clusterName string) (uint64, uint64, *ClusterStore, error) {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", txn.config.PathPrefix)
	clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", txn.config.PathPrefix, clusterName)
	clusterNodesKey := fmt.Sprintf("%s/clusters/nodes/%s", txn.config.PathPrefix, clusterName)

	globalEpochStr := txn.stm.Get(globalEpochKey)
	clusterEpochStr := txn.stm.Get(clusterEpochKey)
	clusterNodesStr := txn.stm.Get(clusterNodesKey)

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

func (txn *TxnBroker) updateCluster(clusterName string, oldGlobalEpoch uint64, cluster *ClusterStore) error {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", txn.config.PathPrefix)
	clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", txn.config.PathPrefix, clusterName)
	clusterNodesKey := fmt.Sprintf("%s/clusters/nodes/%s", txn.config.PathPrefix, clusterName)

	newGlobalEpoch := oldGlobalEpoch + 1
	newGlobalEpochStr := strconv.FormatUint(newGlobalEpoch, 10)
	txn.stm.Put(globalEpochKey, newGlobalEpochStr)
	txn.stm.Put(clusterEpochKey, newGlobalEpochStr)

	newClusterData, err := cluster.Encode()
	if err != nil {
		return err
	}
	txn.stm.Put(clusterNodesKey, string(newClusterData))
	return nil
}

func (txn *TxnBroker) setFailed(proxyAddress string) error {
	proxyKey := fmt.Sprintf("%s/all_proxies/%s", txn.config.PathPrefix, proxyAddress)
	failedProxyKey := fmt.Sprintf("%s/failed_proxies/%s", txn.config.PathPrefix, proxyAddress)

	proxyData := txn.stm.Get(proxyKey)
	proxy := &ProxyStore{}
	err := proxy.Decode([]byte(proxyData))
	if err != nil {
		return err
	}

	txn.stm.Del(proxyKey)

	failedProxy := FailedProxyStore{
		NodeAddresses: proxy.NodeAddresses,
	}
	data, err := failedProxy.Encode()
	if err != nil {
		return err
	}

	txn.stm.Put(failedProxyKey, string(data))
	return nil
}

func (txn *TxnBroker) initGlobalEpoch() error {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", txn.config.PathPrefix)
	globalEpochStr := txn.stm.Get(globalEpochKey)
	if globalEpochStr == "" {
		txn.stm.Put(globalEpochKey, "0")
	}
	return nil
}

func (txn *TxnBroker) takeover(clusterName, failedProxyAddress string, globalEpoch uint64, cluster *ClusterStore) error {
	chunk, err := cluster.FindChunkByProxy(failedProxyAddress)
	if err != nil {
		return err
	}

	err = chunk.SwitchMaster(failedProxyAddress)
	if err != nil {
		return err
	}

	return txn.updateCluster(clusterName, globalEpoch, cluster)
}

func (txn *TxnBroker) replaceProxy(clusterName, failedProxyAddress string, globalEpoch uint64, cluster *ClusterStore, possiblyAvailableProxies []string) (string, error) {
	var newProxyAddress string

	proxies, err := txn.consumeProxies(clusterName, 1, possiblyAvailableProxies)
	if err != nil {
		return "", err
	}
	if len(proxies) != 1 {
		return "", errors.WithStack(fmt.Errorf("expected 1 proxy, got %d", len(proxies)))
	}
	var newProxy *ProxyStore
	for k, v := range proxies {
		newProxyAddress = k
		newProxy = v
	}

	exists := false
	for _, chunk := range cluster.Chunks {
		for i, node := range chunk.Nodes {
			if node.ProxyAddress == failedProxyAddress && i%2 == 0 {
				node.ProxyAddress = newProxyAddress
				node.NodeAddress = newProxy.NodeAddresses[0]
			} else if node.ProxyAddress == failedProxyAddress && i%2 == 1 {
				node.ProxyAddress = newProxyAddress
				node.NodeAddress = newProxy.NodeAddresses[1]
				exists = true
				break
			}
		}
		if exists {
			break
		}
	}
	if !exists {
		return "", fmt.Errorf("cluster %s does not include %s", clusterName, failedProxyAddress)
	}

	err = txn.setFailed(failedProxyAddress)
	if err != nil {
		return "", err
	}
	return newProxyAddress, txn.updateCluster(clusterName, globalEpoch, cluster)
}

func (txn *TxnBroker) addNodesToCluster(clusterName string, expectedNodeNum uint64, cluster *ClusterStore, globalEpoch uint64, possiblyAvailableProxies []string) error {
	if expectedNodeNum%chunkSize != 0 {
		return ErrInvalidRequestedNodesNum
	}

	nodeNum := uint64(len(cluster.Chunks)) * chunkSize
	if nodeNum >= expectedNodeNum {
		return nil
	}

	proxyNum := (expectedNodeNum - nodeNum) / halfChunkSize
	proxies, err := txn.consumeProxies(clusterName, proxyNum, possiblyAvailableProxies)
	if err != nil {
		return err
	}
	if uint64(len(proxies)) != proxyNum {
		return errors.WithStack(fmt.Errorf("expected %d proxy, got %d", proxyNum, len(proxies)))
	}

	return nil
}
