package broker

import "sync"

type clusterCache struct {
	globalEpoch uint64
	cluster     *Cluster
}

type proxyCache struct {
	globalEpoch uint64
	proxy       *proxyMeta
}

type metaCache struct {
	clustersLock sync.RWMutex
	clusters     map[string]*clusterCache
	proxiesLock  sync.RWMutex
	proxies      map[string]*proxyCache
}

func newMetaCache() *metaCache {
	return &metaCache{
		clustersLock: sync.RWMutex{},
		clusters:     make(map[string]*clusterCache),
		proxiesLock:  sync.RWMutex{},
		proxies:      make(map[string]*proxyCache),
	}
}

func (cache *metaCache) setCluster(clusterName string, cluster *clusterCache) {
	cache.clustersLock.Lock()
	defer cache.clustersLock.Unlock()

	cache.clusters[clusterName] = cluster
}

func (cache *metaCache) getCluster(clusterName string) *clusterCache {
	cache.clustersLock.RLock()
	defer cache.clustersLock.RUnlock()

	cluster, ok := cache.clusters[clusterName]
	if !ok {
		return nil
	}
	return cluster
}

func (cache *metaCache) setProxy(address string, proxy *proxyCache) {
	cache.proxiesLock.Lock()
	defer cache.proxiesLock.Unlock()

	cache.proxies[address] = proxy
}

func (cache *metaCache) getProxy(address string) *proxyCache {
	cache.proxiesLock.RLock()
	defer cache.proxiesLock.RUnlock()

	proxy, ok := cache.proxies[address]
	if !ok {
		return nil
	}
	return proxy
}

func (cache *metaCache) getAllProxy() map[string]*proxyCache {
	m := make(map[string]*proxyCache)

	cache.proxiesLock.RLock()
	defer cache.proxiesLock.RUnlock()

	for address, cache := range cache.proxies {
		nodeNum := len(cache.proxy.NodeAddresses)
		nodeAddresses := make([]string, nodeNum, nodeNum)
		copy(nodeAddresses, cache.proxy.NodeAddresses)
		proxy := &proxyMeta{
			ProxyIndex:    cache.proxy.ProxyIndex,
			ClusterName:   cache.proxy.ClusterName,
			NodeAddresses: nodeAddresses,
		}
		m[address] = &proxyCache{
			proxy:       proxy,
			globalEpoch: cache.globalEpoch,
		}
	}
	return m
}
