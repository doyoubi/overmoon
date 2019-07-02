package broker

import "sync"

type clusterCache struct {
	globalEpoch uint64
	cluster     *Cluster
}

type metaCache struct {
	lock     sync.RWMutex
	clusters map[string]*clusterCache
}

func newMetaCache() *metaCache {
	return &metaCache{
		lock:     sync.RWMutex{},
		clusters: make(map[string]*clusterCache),
	}
}

func (cache *metaCache) setCluster(clusterName string, cluster *clusterCache) {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	cache.clusters[clusterName] = cluster
}

func (cache *metaCache) getCluster(clusterName string) *clusterCache {
	cache.lock.RLock()
	defer cache.lock.RUnlock()

	cluster, ok := cache.clusters[clusterName]
	if !ok {
		return nil
	}
	return cluster
}
