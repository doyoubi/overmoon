package test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"go.etcd.io/etcd/clientv3"

	"github.com/stretchr/testify/assert"

	"github.com/doyoubi/overmoon/src/broker"
)

var endpoints = []string{"127.0.0.1:2380"}

func prepareData(assert *assert.Assertions) {
	ctx := context.Background()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints,
	})

	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
	}
	_, err = client.Delete(ctx, "/", opts...)
	assert.NoError(err)

	clusterName := "mycluster"

	node1 := &broker.NodeStore{
		NodeAddress:  "127.0.0.1:7001",
		ProxyAddress: "127.0.0.1:6001",
		Slots: []broker.SlotRangeStore{broker.SlotRangeStore{
			Start: 0, End: 10000, Tag: broker.SlotRangeTagStore{TagType: broker.NoneTag},
		}},
	}
	node2 := &broker.NodeStore{
		NodeAddress:  "127.0.0.1:7002",
		ProxyAddress: "127.0.0.1:6001",
		Slots:        []broker.SlotRangeStore{},
	}
	node3 := &broker.NodeStore{
		NodeAddress:  "127.0.0.2:7003",
		ProxyAddress: "127.0.0.2:6002",
		Slots: []broker.SlotRangeStore{
			broker.SlotRangeStore{Start: 10001, End: 15000, Tag: broker.SlotRangeTagStore{TagType: broker.NoneTag}},
			broker.SlotRangeStore{Start: 15001, End: 16382, Tag: broker.SlotRangeTagStore{TagType: broker.NoneTag}},
			broker.SlotRangeStore{Start: 16383, End: 16383, Tag: broker.SlotRangeTagStore{TagType: broker.NoneTag}},
		},
	}
	node4 := &broker.NodeStore{
		NodeAddress:  "127.0.0.2:7004",
		ProxyAddress: "127.0.0.2:6002",
		Slots:        []broker.SlotRangeStore{},
	}
	cluster := &broker.ClusterStore{
		Nodes: []*broker.NodeStore{node1, node2, node3, node4},
	}
	clusterStr, err := cluster.Encode()
	assert.NoError(err)

	proxy1 := &broker.ProxyStore{
		ClusterName:   clusterName,
		ProxyIndex:    0,
		NodeAddresses: []string{"127.0.0.1:7001", "127.0.0.1:7002"},
	}
	proxy2 := &broker.ProxyStore{
		ClusterName:   clusterName,
		ProxyIndex:    1,
		NodeAddresses: []string{"127.0.0.2:7003", "127.0.0.2:7004"},
	}
	proxy3 := &broker.ProxyStore{
		ClusterName:   "",
		ProxyIndex:    0,
		NodeAddresses: []string{"127.0.0.3:7005", "127.0.0.2:7006"},
	}
	proxy1Str, err := proxy1.Encode()
	assert.NoError(err)
	proxy2Str, err := proxy2.Encode()
	assert.NoError(err)
	proxy3Str, err := proxy3.Encode()
	assert.NoError(err)

	assert.Nil(err)
	assert.NotNil(client)
	client.Put(ctx, "/integration_test/global_epoch", "666")
	client.Put(ctx, fmt.Sprintf("/integration_test/clusters/epoch/%s", clusterName), "233")
	client.Put(ctx, fmt.Sprintf("/integration_test/clusters/nodes/%s", clusterName), string(clusterStr))
	client.Put(ctx, "/integration_test/all_proxies/127.0.0.1:6001", string(proxy1Str))
	client.Put(ctx, "/integration_test/all_proxies/127.0.0.2:6002", string(proxy2Str))
	client.Put(ctx, "/integration_test/all_proxies/127.0.0.3:6003", string(proxy3Str))
}

func genBroker(assert *assert.Assertions) *broker.EtcdMetaBroker {
	cfg := &broker.EtcdConfig{
		PathPrefix: "/integration_test",
		FailureTTL: 10,
	}
	etcdBroker, err := broker.NewEtcdMetaBrokerFromEndpoints(cfg, endpoints)
	assert.Nil(err)
	assert.NotNil(etcdBroker)
	return etcdBroker
}

func TestEtcdGetClusterNames(t *testing.T) {
	assert := assert.New(t)

	prepareData(assert)
	etcdBroker := genBroker(assert)

	ctx := context.Background()

	names, err := etcdBroker.GetClusterNames(ctx)
	assert.Nil(err)
	assert.NotNil(names)
	assert.Equal(1, len(names))
	sort.Strings(names)
	assert.Equal("mycluster", names[0])
}

func TestEtcdGetCluster(t *testing.T) {
	assert := assert.New(t)

	prepareData(assert)
	etcdBroker := genBroker(assert)

	ctx := context.Background()

	clusterName := "mycluster"

	cluster, err := etcdBroker.GetCluster(ctx, clusterName)
	assert.Nil(err)
	assert.NotNil(cluster)

	assert.Equal(cluster.Epoch, uint64(233))
	assert.Equal(cluster.Name, clusterName)

	noneTag := broker.SlotRangeTag{TagType: broker.NoneTag}

	nodes := cluster.Nodes
	assert.Equal(len(nodes), 4)
	assert.Equal(clusterName, nodes[0].ClusterName)
	assert.Equal(clusterName, nodes[1].ClusterName)
	assert.Equal(clusterName, nodes[2].ClusterName)
	assert.Equal(clusterName, nodes[3].ClusterName)
	assert.Equal("127.0.0.1:7001", nodes[0].Address)
	assert.Equal("127.0.0.1:7002", nodes[1].Address)
	assert.Equal("127.0.0.2:7003", nodes[2].Address)
	assert.Equal("127.0.0.2:7004", nodes[3].Address)
	assert.Equal([]broker.SlotRange{broker.SlotRange{Start: 0, End: 10000, Tag: noneTag}}, nodes[0].Slots)
	assert.Equal([]broker.SlotRange{}, nodes[1].Slots)
	assert.Equal([]broker.SlotRange{
		broker.SlotRange{Start: 10001, End: 15000, Tag: noneTag},
		broker.SlotRange{Start: 15001, End: 16382, Tag: noneTag},
		broker.SlotRange{Start: 16383, End: 16383, Tag: noneTag},
	}, nodes[2].Slots)
	assert.Equal([]broker.SlotRange{}, nodes[3].Slots)
}

func TestEtcdGetHosts(t *testing.T) {
	assert := assert.New(t)

	prepareData(assert)
	etcdBroker := genBroker(assert)

	ctx := context.Background()

	addresses, err := etcdBroker.GetProxyAddresses(ctx)
	assert.Nil(err)
	assert.NotNil(addresses)
	assert.Equal(3, len(addresses))
	sort.Strings(addresses)
	assert.Equal("127.0.0.1:6001", addresses[0])
	assert.Equal("127.0.0.2:6002", addresses[1])
	assert.Equal("127.0.0.3:6003", addresses[2])
}

func TestEtcdGetProxy(t *testing.T) {
	assert := assert.New(t)

	prepareData(assert)
	etcdBroker := genBroker(assert)

	ctx := context.Background()

	host, err := etcdBroker.GetProxy(ctx, "127.0.0.1:6001")
	assert.Nil(err)
	assert.NotNil(host)
	assert.Equal(uint64(233), host.Epoch)
	assert.Equal("127.0.0.1:6001", host.Address)
	assert.Equal(2, len(host.Nodes))

	host, err = etcdBroker.GetProxy(ctx, "127.0.0.3:6003")
	assert.Nil(err)
	assert.NotNil(host)
	assert.Equal(uint64(666), host.Epoch)
	assert.Equal("127.0.0.3:6003", host.Address)
	assert.Equal(0, len(host.Nodes))
}

func TestEtcdAddFailures(t *testing.T) {
	assert := assert.New(t)

	prepareData(assert)
	etcdBroker := genBroker(assert)

	ctx := context.Background()

	err := etcdBroker.AddFailure(ctx, "127.0.0.1:6001", "reporter_id1")
	assert.Nil(err)
	addresses, err := etcdBroker.GetFailures(ctx)
	assert.Equal(1, len(addresses))
	assert.Equal("127.0.0.1:6001", addresses[0])
}
