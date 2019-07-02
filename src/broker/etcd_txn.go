package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	conc "go.etcd.io/etcd/clientv3/concurrency"
)

type TxnBroker struct {
	config *EtcdConfig
	stm    conc.STM
}

func NewTxnBroker(config *EtcdConfig, stm conc.STM) *TxnBroker {
	return &TxnBroker{
		config: config,
		stm:    stm,
	}
}

func (broker *TxnBroker) ReplaceNode(oldNode, newNode *Node, currClusterEpoch int64) error {
	err := broker.AddNode(newNode, currClusterEpoch)
	if err != nil {
		return err
	}
	return broker.RemoveNode(oldNode, currClusterEpoch+1)
}

func (broker *TxnBroker) RemoveNode(node *Node, currClusterEpoch int64) error {
	err := broker.removeNodeWithoutBumping(node, currClusterEpoch)
	if err != nil {
		return err
	}

	broker.bumpClusterEpoch(node.ClusterName, currClusterEpoch)
	hostEpoch := broker.getHostEpoch(node.Address)
	if hostEpoch == "" {
		return nil
	}
	hostEpochInt, err := strconv.ParseInt(hostEpoch, 10, 64)
	if err != nil {
		return err
	}
	broker.bumpHostEpoch(node.Address, hostEpochInt)

	return nil
}

func (broker *TxnBroker) removeNodeWithoutBumping(node *Node, currClusterEpoch int64) error {
	clusterName := node.ClusterName
	host := node.ProxyAddress
	nodeAddress := node.Address

	// Also check existence
	if err := broker.checkClusterEpoch(clusterName, currClusterEpoch); err != nil {
		return err
	}

	hostNodeKey := fmt.Sprintf("%s/hosts/%s/nodes/%s", broker.config.PathPrefix, host, nodeAddress)
	broker.stm.Del(hostNodeKey)
	nodeClusterKey := fmt.Sprintf("%s/hosts/all_nodes/%s/%s", broker.config.PathPrefix, host, nodeAddress)
	broker.stm.Put(nodeClusterKey, "")

	clusterNodeKey := fmt.Sprintf("%s/clusters/nodes/%s/%s", broker.config.PathPrefix, clusterName, nodeAddress)
	broker.stm.Del(clusterNodeKey)
	return nil
}

func (broker *TxnBroker) AddNode(node *Node, currClusterEpoch int64) error {
	clusterName := node.ClusterName
	host := node.ProxyAddress
	nodeAddress := node.Address

	// Also check existence
	if err := broker.checkClusterEpoch(clusterName, currClusterEpoch); err != nil {
		return err
	}

	hostEpoch := broker.getHostEpoch(host)
	if hostEpoch == "" {
		return ErrHostNotExist
	}

	nodeOwnerCluster := fmt.Sprintf("%s/hosts/all_nodes/%s/%s", broker.config.PathPrefix, host, nodeAddress)
	cluster := broker.stm.Get(nodeOwnerCluster)
	if cluster != "" {
		return ErrNodeNotAvailable
	}

	broker.stm.Put(nodeOwnerCluster, clusterName)

	if err := broker.addNodeToHost(host, node); err != nil {
		return err
	}

	if err := broker.addNodeToCluster(clusterName, node); err != nil {
		return err
	}

	broker.bumpClusterEpoch(clusterName, currClusterEpoch)
	hostEpochInt, err := strconv.ParseInt(hostEpoch, 10, 64)
	if err != nil {
		return err
	}
	broker.bumpHostEpoch(host, hostEpochInt)

	return nil
}

func (broker *TxnBroker) checkClusterEpoch(clusterName string, currClusterEpoch int64) error {
	clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", broker.config.PathPrefix, clusterName)
	// When it's empty there're two cases:
	// - the cluster does not exist
	// - the cluster are deleting its node
	newClusterEpoch := broker.stm.Get(clusterEpochKey)
	if newClusterEpoch != strconv.FormatInt(currClusterEpoch, 10) {
		return ErrClusterEpochChanged
	}
	return nil
}

func (broker *TxnBroker) clusterExist(clusterName string) bool {
	clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", broker.config.PathPrefix, clusterName)
	epoch := broker.stm.Get(clusterEpochKey)
	return epoch != ""
}

func (broker *TxnBroker) getHostEpoch(address string) string {
	hostEpochKey := fmt.Sprintf("%s/hosts/epoch/%s", broker.config.PathPrefix, address)
	return broker.stm.Get(hostEpochKey)
}

func (broker *TxnBroker) bumpClusterEpoch(clusterName string, currEpoch int64) {
	clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", broker.config.PathPrefix, clusterName)
	broker.stm.Put(clusterEpochKey, strconv.FormatInt(currEpoch+1, 10))
}

func (broker *TxnBroker) bumpHostEpoch(address string, currEpoch int64) {
	hostEpochKey := fmt.Sprintf("%s/hosts/epoch/%s", broker.config.PathPrefix, address)
	broker.stm.Put(hostEpochKey, strconv.FormatInt(currEpoch+1, 10))
}

func (broker *TxnBroker) addNodeToHost(host string, node *Node) error {
	hostNode := fmt.Sprintf("%s/hosts/%s/nodes/%s", broker.config.PathPrefix, host, node.Address)
	jsPayload, err := json.Marshal(node)
	if err != nil {
		return err
	}
	broker.stm.Put(hostNode, string(jsPayload))
	return nil
}

func (broker *TxnBroker) addNodeToCluster(clusterName string, node *Node) error {
	clusterNodekey := fmt.Sprintf("%s/clusters/nodes/%s/%s", broker.config.PathPrefix, clusterName, node.Address)
	jsPayload, err := json.Marshal(node)
	if err != nil {
		return err
	}
	broker.stm.Put(clusterNodekey, string(jsPayload))
	return nil
}

func (broker *TxnBroker) getGlobalEpochV2() (uint64, error) {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", broker.config.PathPrefix)
	epochStr := broker.stm.Get(globalEpochKey)
	return strconv.ParseUint(epochStr, 10, 64)
}

func (broker *TxnBroker) bumpGlobalEpochV2() (uint64, error) {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", broker.config.PathPrefix)
	oldEpoch, err := broker.getGlobalEpochV2()
	if err != nil {
		return 0, err
	}

	newEpoch := oldEpoch + 1
	newEpochStr := strconv.FormatUint(newEpoch, 10)
	broker.stm.Put(globalEpochKey, newEpochStr)
	return newEpoch + 1, nil
}

func (broker *TxnBroker) consumeProxies(clusterName string, proxyNum uint64, possiblyFreeProxies []string) (map[string]*proxyMeta, error) {
	tryNum := proxyNum * 2

	availableProxies := make(map[string]*proxyMeta)
	for i, address := range possiblyFreeProxies {
		if uint64(len(availableProxies)) == proxyNum {
			break
		}
		if uint64(i) >= tryNum {
			return nil, ErrNodeNotAvailable
		}
		proxyKey := fmt.Sprintf("%s/all_proxies/%s", broker.config.PathPrefix, address)
		proxyData := broker.stm.Get(proxyKey)
		meta := &proxyMeta{}
		err := meta.decode([]byte(proxyData))
		if err != nil {
			log.Printf("invalid proxy meta format: %s", proxyData)
			continue
		}
		if meta.ClusterName != "" {
			continue
		}
		availableProxies[address] = meta
	}

	if uint64(len(availableProxies)) < proxyNum {
		return nil, ErrNodeNotAvailable
	}

	var proxyIndex uint64
	for address, meta := range availableProxies {
		proxyKey := fmt.Sprintf("%s/all_proxies/%s", broker.config.PathPrefix, address)
		meta.ClusterName = clusterName
		meta.ProxyIndex = proxyIndex
		proxyIndex++
		metaStr, err := meta.encode()
		if err != nil {
			return nil, nil
		}
		broker.stm.Put(proxyKey, string(metaStr))
	}

	return availableProxies, nil
}

func (broker *TxnBroker) createCluster(clusterName string, nodes []*nodeMeta) error {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", broker.config.PathPrefix)
	globalEpochStr := broker.stm.Get(globalEpochKey)
	if globalEpochStr == "" {
		// TODO: make this safer by returning a error for the case that
		// this key might be lost.
		globalEpochStr = "0"
	}
	globalEpoch, err := strconv.ParseUint(globalEpochStr, 10, 64)
	if err != nil {
		return err
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
	cluster := &clusterMeta{Nodes: nodes}
	clusterData, err := cluster.encode()
	if err != nil {
		return err
	}
	broker.stm.Put(clusterNodesKey, string(clusterData))

	return nil
}
