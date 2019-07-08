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
func (txn *TxnBroker) consumeChunks(clusterName string, proxyNum uint64, possiblyFreeProxies []string, existingChunks []*NodeChunkStore) ([]*NodeChunkStore, error) {
	alloc, err := newChunkAllocatorWithExistingChunks(possiblyFreeProxies, proxyNum, existingChunks, false)
	if err != nil {
		return nil, err
	}
	chunks, err := alloc.allocate()
	if err != nil {
		return nil, err
	}

	proxyAddresses := make([]string, 0, len(chunks)*2)
	for _, chunk := range chunks {
		proxyAddresses = append(proxyAddresses, chunk[0])
		proxyAddresses = append(proxyAddresses, chunk[1])
	}

	proxies, err := txn.consumeProxies(clusterName, uint64(len(proxyAddresses)), proxyAddresses)
	if err != nil {
		return nil, err
	}

	chunkStores := make([]*NodeChunkStore, 0, len(chunks))
	for _, chunk := range chunks {
		firstAddress := chunk[0]
		secondAddress := chunk[1]
		firstProxy := proxies[firstAddress]
		secondProxy := proxies[secondAddress]
		nodes := [4]*NodeStore{}
		nodes[0] = &NodeStore{
			NodeAddress:  firstProxy.NodeAddresses[0],
			ProxyAddress: firstAddress,
		}
		nodes[1] = &NodeStore{
			NodeAddress:  firstProxy.NodeAddresses[1],
			ProxyAddress: firstAddress,
		}
		nodes[2] = &NodeStore{
			NodeAddress:  secondProxy.NodeAddresses[0],
			ProxyAddress: secondAddress,
		}
		nodes[3] = &NodeStore{
			NodeAddress:  secondProxy.NodeAddresses[1],
			ProxyAddress: secondAddress,
		}
		chunk := &NodeChunkStore{
			RolePosition: ChunkRoleNormalPosition,
			Slots:        [2][]SlotRangeStore{[]SlotRangeStore{}, []SlotRangeStore{}},
			Nodes:        nodes,
		}
		chunkStores = append(chunkStores, chunk)
	}

	return chunkStores, nil
}

func (txn *TxnBroker) consumeHalfChunk(clusterName, failedProxyAddress, peerProxyAddress string, possiblyFreeProxies []string, existingChunks []*NodeChunkStore) (string, *ProxyStore, error) {
	alloc, err := newChunkReplaceProxyAllocator(possiblyFreeProxies, existingChunks)
	if err != nil {
		return "", nil, err
	}

	newProxyAddress, err := alloc.replaceProxy(failedProxyAddress, peerProxyAddress)
	if err != nil {
		return "", nil, err
	}

	proxies, err := txn.consumeProxies(clusterName, 1, []string{newProxyAddress})
	if err != nil {
		return "", nil, err
	}

	if len(proxies) == 0 {
		return "", nil, ErrAllocatedProxyInUse
	}
	if len(proxies) != 1 {
		return "", nil, errors.WithStack(fmt.Errorf("expected 1 proxy, got %d", len(proxies)))
	}
	var newProxy *ProxyStore
	for _, proxy := range proxies {
		newProxy = proxy
	}
	return newProxyAddress, newProxy, nil
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

	if clusterEpochStr == "" {
		return 0, 0, nil, ErrClusterNotFound
	}

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

func (txn *TxnBroker) updateCluster(clusterName string, newGlobalEpoch uint64, cluster *ClusterStore) error {
	log.Infof("update cluster %+v", cluster)
	globalEpochKey := fmt.Sprintf("%s/global_epoch", txn.config.PathPrefix)
	clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", txn.config.PathPrefix, clusterName)
	clusterNodesKey := fmt.Sprintf("%s/clusters/nodes/%s", txn.config.PathPrefix, clusterName)

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

	return txn.updateCluster(clusterName, globalEpoch+1, cluster)
}

func (txn *TxnBroker) replaceProxy(clusterName, failedProxyAddress string, globalEpoch uint64, cluster *ClusterStore, possiblyAvailableProxies []string) (string, error) {
	var newProxyAddress, peerProxyAddress string

	chunk, err := cluster.FindChunkByProxy(failedProxyAddress)
	if err != nil {
		return "", err
	}

	if chunk.Nodes[0].ProxyAddress == failedProxyAddress {
		peerProxyAddress = chunk.Nodes[halfChunkSize].ProxyAddress
	} else if chunk.Nodes[halfChunkSize].ProxyAddress == failedProxyAddress {
		peerProxyAddress = chunk.Nodes[0].ProxyAddress
	}
	if peerProxyAddress == "" {
		return "", errors.WithStack(fmt.Errorf("invalid state, can't found %s in chunk", failedProxyAddress))
	}

	newProxyAddress, newProxy, err := txn.consumeHalfChunk(clusterName, failedProxyAddress, peerProxyAddress, possiblyAvailableProxies, cluster.Chunks)
	if err != nil {
		return "", err
	}

	exists := false
	for i, node := range chunk.Nodes {
		if node.ProxyAddress == failedProxyAddress {
			node.ProxyAddress = newProxyAddress
			node.NodeAddress = newProxy.NodeAddresses[i%2]
			exists = true
		}
	}
	if !exists {
		return "", errors.WithStack(fmt.Errorf("cluster %s does not include %s", clusterName, failedProxyAddress))
	}

	err = txn.setFailed(failedProxyAddress)
	if err != nil {
		return "", err
	}
	log.Infof("after replacing, cluster %+v", cluster)
	return newProxyAddress, txn.updateCluster(clusterName, globalEpoch+1, cluster)
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
	log.Infof("try to consume chunks %s %d %v", clusterName, proxyNum, possiblyAvailableProxies)
	chunks, err := txn.consumeChunks(clusterName, proxyNum, possiblyAvailableProxies, cluster.Chunks)
	if err != nil {
		return err
	}
	log.Infof("get %d chunks", len(chunks))
	if uint64(len(chunks))*2 != proxyNum {
		return errors.WithStack(fmt.Errorf("expected %d proxy, got %d chunks", proxyNum, len(chunks)))
	}

	cluster.Chunks = append(cluster.Chunks, chunks...)
	return txn.updateCluster(clusterName, globalEpoch+1, cluster)
}

func initChunkSlots(chunks []*NodeChunkStore) []*NodeChunkStore {
	chunkNum := uint64(len(chunks))
	proxyNum := chunkNum * 2
	gap := (MaxSlotNumber + proxyNum - 1) / proxyNum
	chunkSlots := [2][]SlotRangeStore{}
	for index := uint64(0); index != proxyNum; index++ {
		end := (index+1)*gap - 1
		if MaxSlotNumber < end {
			end = MaxSlotNumber
		}
		slots := SlotRangeStore{
			Start: index * gap,
			End:   end,
			Tag:   SlotRangeTagStore{TagType: NoneTag},
		}
		chunkSlots[index%2] = []SlotRangeStore{slots}

		if index%2 == 1 {
			chunks[index/2].Slots = chunkSlots
			chunkSlots = [2][]SlotRangeStore{}
		}
	}
	return chunks
}

func (txn *TxnBroker) migrateSlots(clusterName string) error {
	globalEpoch, _, cluster, err := txn.getCluster(clusterName)
	if err != nil {
		return err
	}

	err = cluster.SplitSlots(globalEpoch + 1)
	if err != nil {
		return err
	}
	return txn.updateCluster(clusterName, globalEpoch+1, cluster)
}

func (txn *TxnBroker) commitMigration(task MigrationTaskMeta) error {
	globalEpoch, _, cluster, err := txn.getCluster(task.DBName)
	if err != nil {
		return err
	}

	err = cluster.CommitMigration(task.Slots)
	if err != nil {
		return err
	}
	return txn.updateCluster(task.DBName, globalEpoch+1, cluster)
}

func (txn *TxnBroker) removeProxy(address string) error {
	proxyKey := fmt.Sprintf("%s/all_proxies/%s", txn.config.PathPrefix, address)
	failedProxyKey := fmt.Sprintf("%s/failed_proxies/%s", txn.config.PathPrefix, address)

	proxyData := txn.stm.Get(proxyKey)
	if proxyData == "" {
		txn.stm.Del(failedProxyKey)
		return nil
	}

	proxy := &ProxyStore{}
	err := proxy.Decode([]byte(proxyData))
	if err != nil {
		return err
	}
	if proxy.ClusterName != "" {
		return ErrProxyInUse
	}

	txn.stm.Del(proxyKey)
	txn.stm.Del(failedProxyKey)
	return nil
}

func (txn *TxnBroker) freeProxiesFromCluster(proxyAddresses []string) error {
	for _, address := range proxyAddresses {
		proxyKey := fmt.Sprintf("%s/all_proxies/%s", txn.config.PathPrefix, address)
		proxyData := txn.stm.Get(proxyKey)
		if proxyData == "" {
			continue
		}
		proxy := &ProxyStore{}
		err := proxy.Decode([]byte(proxyData))
		if err != nil {
			return err
		}
		proxy.ProxyIndex = 0
		proxy.ClusterName = ""

		newProxyData, err := proxy.Encode()
		if err != nil {
			return err
		}
		txn.stm.Put(proxyKey, string(newProxyData))
	}
	return nil
}

func (txn *TxnBroker) removeUnusedProxiesFromCluster(clusterName string) error {
	globalEpoch, _, cluster, err := txn.getCluster(clusterName)
	if err != nil {
		return nil
	}

	free := []string{}
	var freeStartIndex int
	for i, chunk := range cluster.Chunks {
		if len(chunk.Slots[0]) == 0 && len(chunk.Slots[1]) == 0 {
			freeStartIndex = i
		}
		if freeStartIndex != 0 {
			if len(chunk.Slots[0]) != 0 || len(chunk.Slots[1]) != 0 {
				return errors.WithStack(errors.New("invalid state, unexpected chunk with slots"))
			}
			free = append(free, chunk.Nodes[0].ProxyAddress)
			free = append(free, chunk.Nodes[halfChunkSize].ProxyAddress)
		}
	}
	if freeStartIndex == 0 {
		return ErrProxyInUse
	}

	cluster.Chunks = cluster.Chunks[:freeStartIndex]
	err = txn.updateCluster(clusterName, globalEpoch+1, cluster)
	if err != nil {
		return nil
	}

	return txn.freeProxiesFromCluster(free)
}
