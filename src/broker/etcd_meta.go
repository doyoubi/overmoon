package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

var ErrTxnFailed = errors.New("txn failed")
var ErrInvalidKeyNum = errors.New("invalid key numbers")

const failureMinReporter = 2
const reportValidPeriod = 30

// EtcdMetaBroker implements the MetaDataBroker interface and serves as a broker backend.
type EtcdMetaBroker struct {
	config *EtcdConfig
	client *clientv3.Client
	cache  *metaCache
}

// NewEtcdMetaBrokerFromEndpoints creates EtcdMetaBroker from endpoints
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
		cache:  newMetaCache(),
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
	_, cluster, err := broker.getClusterFromCache(ctx, name)
	return cluster, err
}

func (broker *EtcdMetaBroker) getClusterFromCache(ctx context.Context, name string) (uint64, *Cluster, error) {
	cache := broker.cache.getCluster(name)
	if cache != nil {
		return 0, cache.cluster, nil
	}

	globalEpoch, cluster, err := broker.getClusterFromEtcd(ctx, name)
	if err != nil {
		return 0, nil, err
	}
	broker.cache.setCluster(name, &clusterCache{
		globalEpoch: globalEpoch,
		cluster:     cluster,
	})
	return globalEpoch, cluster, nil
}

func (broker *EtcdMetaBroker) getClusterFromEtcd(ctx context.Context, name string) (uint64, *Cluster, error) {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", broker.config.PathPrefix)
	clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", broker.config.PathPrefix, name)
	clusterNodesKey := fmt.Sprintf("%s/clusters/nodes/%s/", broker.config.PathPrefix, name)

	globalEpoch, epoch, nodes, err := broker.getEpochAndNodes(ctx, globalEpochKey, clusterEpochKey, clusterNodesKey)
	if err != nil {
		return 0, nil, err
	}
	if nodes == nil {
		return globalEpoch, nil, nil
	}

	for _, node := range nodes {
		node.ClusterName = name
	}

	cluster := &Cluster{
		Name:  name,
		Epoch: epoch,
		Nodes: nodes,
	}
	return globalEpoch, cluster, nil
}

func (broker *EtcdMetaBroker) getEpochAndNodes(ctx context.Context, globalEpochKey, epochKey, nodesKey string) (uint64, uint64, []*Node, error) {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
	}

	response, err := broker.client.Txn(ctx).Then(
		clientv3.OpGet(globalEpochKey),
		clientv3.OpGet(epochKey),
		clientv3.OpGet(nodesKey, opts...),
	).Commit()

	if err != nil {
		return 0, 0, nil, err
	}
	if !response.Succeeded {
		return 0, 0, nil, ErrTxnFailed
	}
	if len(response.Responses) != 4 {
		return 0, 0, nil, ErrInvalidKeyNum
	}

	globalEpochRes := response.Responses[0].GetResponseRange()
	globalEpoch, err := parseEpoch(globalEpochRes.Kvs)
	if err != nil {
		return 0, 0, nil, err
	}

	epochRes := response.Responses[1].GetResponseRange()
	epoch, err := parseEpoch(epochRes.Kvs)
	if err == ErrNotExists {
		return globalEpoch, 0, nil, nil
	}
	if err != nil {
		return 0, 0, nil, err
	}

	nodesRes := response.Responses[2].GetResponseRange()
	if len(nodesRes.Kvs) != 1 {
		return 0, 0, nil, ErrInvalidKeyNum
	}
	nodes, err := parseNodes(nodesRes.Kvs[0].Value)
	if err != nil {
		return 0, 0, nil, err
	}

	return globalEpoch, epoch, nodes, nil
}

// GetHostAddresses queries all proxies.
func (broker *EtcdMetaBroker) GetHostAddresses(ctx context.Context) ([]string, error) {
	hostKeyPrefix := fmt.Sprintf("%s/all_proxies/", broker.config.PathPrefix)
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
	return broker.getProxyFromEtcd(ctx, address)
}

func (broker *EtcdMetaBroker) getProxyFromEtcd(ctx context.Context, address string) (*Host, error) {
	globalEpoch, meta, err := broker.getProxyMetaFromCache(ctx, address)
	if err != nil {
		return nil, err
	}
	if meta.ClusterName == "" {
		host := &Host{
			Address: address,
			Epoch:   globalEpoch,
			Nodes:   []*Node{},
		}
		return host, nil
	}

	// Read them in two transaction is fine.
	_, cluster, err := broker.getClusterFromCache(ctx, meta.ClusterName)
	if err != nil {
		return nil, err
	}
	if cluster == nil {
		host := &Host{
			Address: address,
			Epoch:   globalEpoch,
			Nodes:   []*Node{},
		}
		return host, nil
	}

	nodes := make([]*Node, 0)
	for _, node := range cluster.Nodes {
		if node.ProxyAddress == address {
			nodes = append(nodes, node)
		}
	}

	host := &Host{
		Address: address,
		Epoch:   cluster.Epoch,
		Nodes:   nodes,
	}
	return host, nil
}

func (broker *EtcdMetaBroker) getProxyMetaFromCache(ctx context.Context, address string) (uint64, *proxyMeta, error) {
	cache := broker.cache.getProxy(address)
	if cache != nil {
		return cache.globalEpoch, cache.proxy, nil
	}

	globalEpoch, proxy, err := broker.getProxyMetaFromEtcd(ctx, address)
	if err != nil {
		return 0, nil, err
	}
	broker.cache.setProxy(address, &proxyCache{
		globalEpoch: globalEpoch,
		proxy:       proxy,
	})
	return globalEpoch, proxy, nil
}

func (broker *EtcdMetaBroker) getProxyMetaFromEtcd(ctx context.Context, address string) (uint64, *proxyMeta, error) {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", broker.config.PathPrefix)
	proxyKey := fmt.Sprintf("%s/all_proxies/%s", broker.config.PathPrefix, address)

	response, err := broker.client.Txn(ctx).Then(
		clientv3.OpGet(globalEpochKey),
		clientv3.OpGet(proxyKey),
	).Commit()

	if err != nil {
		return 0, nil, err
	}
	if !response.Succeeded {
		return 0, nil, ErrTxnFailed
	}
	if len(response.Responses) != 2 {
		return 0, nil, ErrInvalidKeyNum
	}

	globalEpochRes := response.Responses[0].GetResponseRange()
	globalEpoch, err := parseEpoch(globalEpochRes.Kvs)
	if err != nil {
		return 0, nil, err
	}

	proxyRes := response.Responses[0].GetResponseRange()
	if len(proxyRes.Kvs) != 1 {
		return 0, nil, ErrInvalidKeyNum
	}
	proxyData := proxyRes.Kvs[0].Value

	meta := &proxyMeta{}
	err = meta.decode(proxyData)
	if err != nil {
		return 0, nil, err
	}
	return globalEpoch, meta, nil
}

// AddFailure add failures reported by coordinators
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

// GetFailures retrieves the failures
func (broker *EtcdMetaBroker) GetFailures(ctx context.Context) ([]string, error) {
	prefix := fmt.Sprintf("%s/failures/", broker.config.PathPrefix)
	kv, err := getRangeKeyPostfixAndValue(ctx, broker.client, prefix)
	if err != nil {
		return nil, err
	}

	now := time.Now().Unix()
	// TODO: add min failure config
	addresses := make([]string, 0, len(kv))
	for k, v := range kv {
		segs := strings.SplitN(string(k), "/", 2)
		if len(segs) != 2 {
			return nil, fmt.Errorf("invalid failure key %s", string(k))
		}
		proxyAddress := segs[0]

		reportTimestamp, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			log.Printf("invalid report time %s", v)
			continue
		}
		if reportTimestamp-now > reportValidPeriod {
			continue
		}
		addresses = append(addresses, proxyAddress)
	}
	return addresses, nil
}

func (broker *EtcdMetaBroker) getAvailableProxies(ctx context.Context) map[string]*proxyMeta {
	allProxies := broker.cache.getAllProxy()
	proxies := make(map[string]*proxyMeta)
	for address, cache := range allProxies {
		if cache.proxy.ClusterName == "" {
			proxies[address] = cache.proxy
		}
	}
	return proxies
}

func (broker *EtcdMetaBroker) getAvailableProxyAddresses(ctx context.Context) []string {
	proxyMetadata := broker.getAvailableProxies(ctx)
	possiblyAvailableProxies := make([]string, 0)
	for address := range proxyMetadata {
		possiblyAvailableProxies = append(possiblyAvailableProxies, address)
	}
	return possiblyAvailableProxies
}

func (broker *EtcdMetaBroker) getEpoch(ctx context.Context, key string) (uint64, error) {
	res, err := broker.client.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	return parseEpoch(res.Kvs)
}

func parseEpoch(kvs []*mvccpb.KeyValue) (uint64, error) {
	if len(kvs) == 0 {
		return 0, ErrNotExists
	} else if len(kvs) > 1 {
		return 0, ErrInvalidKeyNum
	}
	kv := kvs[0]

	epoch, err := strconv.ParseUint(string(kv.Value), 10, 64)
	return epoch, err
}

func parseNodes(clusterData []byte) ([]*Node, error) {
	cluster := &clusterMeta{}
	err := cluster.decode(clusterData)
	if err != nil {
		return nil, err
	}

	nodes := make([]*Node, len(cluster.Nodes), len(cluster.Nodes))
	slots := make([][]slotRangeMeta, len(cluster.Nodes)/2)
	for nodeIndex, etcdNodeMeta := range cluster.Nodes {
		node := &Node{
			Address:      etcdNodeMeta.NodeAddress,
			ProxyAddress: etcdNodeMeta.ProxyAddress,
			ClusterName:  "",         // initialized later
			Slots:        nil,        // initialized later
			Repl:         ReplMeta{}, // initialized later
		}

		nodes = append(nodes, node)
		if nodeIndex%2 == 0 {
			slots = append(slots, etcdNodeMeta.Slots)
		}
	}
	nodes, err = setRepl(nodes)
	if err != nil {
		return nil, err
	}
	return addSlots(nodes, slots)
}

func setRepl(nodes []*Node) ([]*Node, error) {
	for nodeIndex, node := range nodes {
		role := MasterRole
		if nodeIndex%2 == 1 {
			role = ReplicaRole
		}
		var peerIndex int
		switch nodeIndex % 4 {
		case 0:
			peerIndex = nodeIndex + 3
		case 1:
			peerIndex = nodeIndex + 1
		case 2:
			peerIndex = nodeIndex - 1
		case 3:
			peerIndex = nodeIndex - 3
		}
		if peerIndex < 0 || peerIndex >= len(nodes) {
			return nil, errors.New("invalid node index when finding peer")
		}
		peer := nodes[peerIndex]
		node.Repl = ReplMeta{
			Role: role,
			Peers: []ReplPeer{ReplPeer{
				NodeAddress:  peer.Address,
				ProxyAddress: peer.ProxyAddress,
			}},
		}
	}
	return nodes, nil
}

func addSlots(nodes []*Node, slots [][]slotRangeMeta) ([]*Node, error) {
	if len(slots)*2 != len(nodes) {
		return nil, fmt.Errorf("mismatch slots and nodes number, nodes: %d, slots: %d", len(nodes), len(slots))
	}

	for i, slotRangeMeta := range slots {
		slotRanges := make([]SlotRange, 0, len(slotRangeMeta))
		for _, meta := range slotRangeMeta {
			srcMasterIndex := meta.Tag.Meta.SrcProxyIndex * 2
			dstMasterIndex := meta.Tag.Meta.DstProxyIndex * 2
			slotRanges = append(slotRanges, SlotRange{
				Start: meta.Start,
				End:   meta.End,
				Tag: SlotRangeTag{
					TagType: meta.Tag.TagType,
					Meta: &MigrationMeta{
						Epoch:           meta.Tag.Meta.Epoch,
						SrcNodeAddress:  nodes[srcMasterIndex].Address,
						SrcProxyAddress: nodes[srcMasterIndex].ProxyAddress,
						DstNodeAddress:  nodes[dstMasterIndex].Address,
						DstProxyAddress: nodes[dstMasterIndex].ProxyAddress,
					},
				},
			})
		}
		nodes[2*i].Slots = slotRanges
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
	return parseRangeResult(prefix, res.Kvs)
}

func parseRangeResult(prefix string, kvs []*mvccpb.KeyValue) (map[string][]byte, error) {
	postfixes := make(map[string][]byte)
	for _, item := range kvs {
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
