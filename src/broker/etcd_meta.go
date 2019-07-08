package broker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

// ErrTxnFailed indicates the transaction failed.
var ErrTxnFailed = errors.New("txn failed")

// ErrTryAgain indicates the cache is stall and client need to try again.
var ErrTryAgain = errors.New("try again")

// ErrClusterNotFound indicates that cluster does not exist.
var ErrClusterNotFound = errors.New("cluster not found")

// ErrProxyNotFound indicates that proxy does not exist.
var ErrProxyNotFound = errors.New("proxy not found")

// ErrProxyNotInUse indicates that key does not exist.
var ErrProxyNotInUse = errors.New("proxy not in use")

// ErrProxyInUse indicates that key does not exist.
var ErrProxyInUse = errors.New("proxy is in use")

// ErrGlobalEpochNotFound indicates that key does not exist.
var ErrGlobalEpochNotFound = errors.New("global epoch not found")

var errNotExists = errors.New("missing key")
var errInvalidKeyNum = errors.New("invalid key numbers")

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
		return nil, errors.WithStack(err)
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

// Serve runs the routine cleanup of cache
func (broker *EtcdMetaBroker) Serve(ctx context.Context) error {
	return broker.serveClearingCache(ctx)
}

func (broker *EtcdMetaBroker) serveClearingCache(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		broker.cache.clearAll()
		time.Sleep(time.Second * time.Duration(2))
	}
}

// ClearCache clears the cached metadata.
func (broker *EtcdMetaBroker) ClearCache() {
	broker.cache.clearAll()
}

// GetClusterNames retrieves all the cluster names from etcd.
func (broker *EtcdMetaBroker) GetClusterNames(ctx context.Context) ([]string, error) {
	clusterKeyPrefix := fmt.Sprintf("%s/clusters/epoch/", broker.config.PathPrefix)
	kvs, err := getRangeKeyPostfixAndValue(ctx, broker.client, clusterKeyPrefix)
	if err != nil {
		return nil, errors.WithStack(err)
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
	clusterNodesKey := fmt.Sprintf("%s/clusters/nodes/%s", broker.config.PathPrefix, name)

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
	response, err := broker.client.Txn(ctx).Then(
		clientv3.OpGet(globalEpochKey),
		clientv3.OpGet(epochKey),
		clientv3.OpGet(nodesKey),
	).Commit()

	if err != nil {
		return 0, 0, nil, errors.WithStack(err)
	}
	if !response.Succeeded {
		return 0, 0, nil, ErrTxnFailed
	}
	if len(response.Responses) != 3 {
		return 0, 0, nil, errors.WithStack(errInvalidKeyNum)
	}

	globalEpochRes := response.Responses[0].GetResponseRange()
	globalEpoch, err := parseEpoch(globalEpochRes.Kvs)
	if err != nil {
		if err == errNotExists {
			return 0, 0, nil, ErrGlobalEpochNotFound
		}
		return 0, 0, nil, err
	}

	epochRes := response.Responses[1].GetResponseRange()
	epoch, err := parseEpoch(epochRes.Kvs)
	if err == errNotExists {
		return globalEpoch, 0, nil, ErrClusterNotFound
	}
	if err != nil {
		return 0, 0, nil, err
	}

	nodesRes := response.Responses[2].GetResponseRange()
	if len(nodesRes.Kvs) != 1 {
		return 0, 0, nil, errors.WithStack(errInvalidKeyNum)
	}
	nodes, err := parseNodes(nodesRes.Kvs[0].Value)
	if err != nil {
		return 0, 0, nil, err
	}

	return globalEpoch, epoch, nodes, nil
}

// GetProxyAddresses queries all proxies.
func (broker *EtcdMetaBroker) GetProxyAddresses(ctx context.Context) ([]string, error) {
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

// GetProxy query the host by address
func (broker *EtcdMetaBroker) GetProxy(ctx context.Context, address string) (*Host, error) {
	return broker.getProxyFromCache(ctx, address)
}

func (broker *EtcdMetaBroker) getProxyFromCache(ctx context.Context, address string) (*Host, error) {
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
		return nil, ErrTryAgain
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

func (broker *EtcdMetaBroker) getProxyMetaFromCache(ctx context.Context, address string) (uint64, *ProxyStore, error) {
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

func (broker *EtcdMetaBroker) getProxyMetaFromEtcd(ctx context.Context, address string) (uint64, *ProxyStore, error) {
	globalEpochKey := fmt.Sprintf("%s/global_epoch", broker.config.PathPrefix)
	proxyKey := fmt.Sprintf("%s/all_proxies/%s", broker.config.PathPrefix, address)

	response, err := broker.client.Txn(ctx).Then(
		clientv3.OpGet(globalEpochKey),
		clientv3.OpGet(proxyKey),
	).Commit()

	if err != nil {
		return 0, nil, errors.WithStack(err)
	}
	if !response.Succeeded {
		return 0, nil, ErrTxnFailed
	}
	if len(response.Responses) != 2 {
		return 0, nil, errors.WithStack(errInvalidKeyNum)
	}

	globalEpochRes := response.Responses[0].GetResponseRange()
	globalEpoch, err := parseEpoch(globalEpochRes.Kvs)
	if err == errNotExists {
		return 0, nil, ErrProxyNotFound
	}
	if err != nil {
		return 0, nil, err
	}

	proxyRes := response.Responses[1].GetResponseRange()
	if len(proxyRes.Kvs) == 0 {
		return 0, nil, ErrProxyNotFound
	}
	if len(proxyRes.Kvs) != 1 {
		return 0, nil, errors.WithStack(errInvalidKeyNum)
	}
	proxyData := proxyRes.Kvs[0].Value

	meta := &ProxyStore{}
	err = meta.Decode(proxyData)
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
		return errors.WithStack(err)
	}

	opts := []clientv3.OpOption{
		clientv3.WithLease(clientv3.LeaseID(res.ID)),
	}

	// CreateRevision(key) == 0 means that key does not exist
	_, err = broker.client.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(key), "=", 0),
	).Then(
		clientv3.OpPut(key, value, opts...),
	).Commit()
	return errors.WithStack(err)
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
			err := fmt.Errorf("invalid failure key %s", string(k))
			return nil, errors.WithStack(err)
		}
		proxyAddress := segs[0]

		reportTimestamp, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			log.Errorf("invalid report time %s", v)
			continue
		}
		if reportTimestamp-now > reportValidPeriod {
			continue
		}
		addresses = append(addresses, proxyAddress)
	}
	return addresses, nil
}

func (broker *EtcdMetaBroker) getAvailableProxies(ctx context.Context) (map[string]*ProxyStore, error) {
	// load all the data to cache
	addresses, err := broker.GetProxyAddresses(ctx)
	if err != nil {
		return nil, err
	}
	for _, address := range addresses {
		_, err := broker.GetProxy(ctx, address)
		if err != nil {
			log.Errorf("failed to load proxy %s", address)
		}
	}

	allProxies := broker.cache.getAllProxy()
	proxies := make(map[string]*ProxyStore)
	for address, cache := range allProxies {
		if cache.proxy.ClusterName == "" {
			proxies[address] = cache.proxy
		}
	}
	return proxies, nil
}

func (broker *EtcdMetaBroker) getAvailableProxyAddresses(ctx context.Context) ([]string, error) {
	proxyMetadata, err := broker.getAvailableProxies(ctx)
	if err != nil {
		return nil, err
	}
	if len(proxyMetadata) == 0 {
		return nil, ErrNoAvailableResource
	}
	possiblyAvailableProxies := make([]string, 0)
	for address := range proxyMetadata {
		possiblyAvailableProxies = append(possiblyAvailableProxies, address)
	}
	return possiblyAvailableProxies, nil
}

func parseEpoch(kvs []*mvccpb.KeyValue) (uint64, error) {
	if len(kvs) == 0 {
		return 0, errNotExists
	} else if len(kvs) > 1 {
		return 0, errors.WithStack(errInvalidKeyNum)
	}
	kv := kvs[0]

	epoch, err := strconv.ParseUint(string(kv.Value), 10, 64)
	return epoch, errors.WithStack(err)
}

func getRangeKeyPostfixAndValue(ctx context.Context, client *clientv3.Client, prefix string) (map[string][]byte, error) {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
	}
	res, err := client.Get(ctx, prefix, opts...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return parseRangeResult(prefix, res.Kvs)
}

func parseRangeResult(prefix string, kvs []*mvccpb.KeyValue) (map[string][]byte, error) {
	postfixes := make(map[string][]byte)
	for _, item := range kvs {
		key := string(item.Key)
		if !strings.HasPrefix(key, prefix) {
			err := fmt.Errorf("Unexpected key %s", key)
			return nil, errors.WithStack(err)
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
