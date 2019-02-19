package test

import (
	"context"
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
	assert.Nil(err)
	assert.NotNil(client)
	client.Put(ctx, "/integration_test/clusters/epoch/clustername1", "233")
	client.Put(ctx, "/integration_test/clusters/epoch/clustername2", "666")
	client.Put(ctx, "/integration_test/clusters/nodes/clustername1/127.0.0.1:7001", "{\"slots\":[[0,5000]]}")
	client.Put(ctx, "/integration_test/clusters/nodes/clustername1/127.0.0.1:7002", "{\"slots\":[[5001,10000]]}")
	client.Put(ctx, "/integration_test/clusters/nodes/clustername1/127.0.0.1:7003", "{\"slots\":[[10001,15000], [15001,16383]]}")
}

func genBroker(assert *assert.Assertions) *broker.EtcdMetaBroker {
	cfg := &broker.EtcdConfig{PathPrefix: "/integration_test"}
	etcdBroker, err := broker.NewEtcdMetaBrokerFromEndpoints(cfg, endpoints)
	assert.Nil(err)
	assert.NotNil(etcdBroker)
	return etcdBroker
}

func sortNodes(nodes []*broker.Node) []*broker.Node {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Address < nodes[j].Address
	})
	return nodes
}

func TestEtcdGetClusterNames(t *testing.T) {
	assert := assert.New(t)

	prepareData(assert)
	etcdBroker := genBroker(assert)

	ctx := context.Background()

	names, err := etcdBroker.GetClusterNames(ctx)
	assert.Nil(err)
	assert.NotNil(names)
	assert.Equal(2, len(names))
	sort.Strings(names)
	assert.Equal("clustername1", names[0])
	assert.Equal("clustername2", names[1])
}

func TestEtcdGetCluster(t *testing.T) {
	assert := assert.New(t)

	prepareData(assert)
	etcdBroker := genBroker(assert)

	ctx := context.Background()

	cluster, err := etcdBroker.GetCluster(ctx, "clustername1")
	assert.Nil(err)
	assert.NotNil(cluster)

	assert.Equal(cluster.Epoch, int64(233))
	assert.Equal(cluster.Name, "clustername1")
	assert.Equal(len(cluster.Nodes), 3)

	nodes := sortNodes(cluster.Nodes)
	assert.Equal(len(nodes), 3)
	assert.Equal(nodes[0].ClusterName, "clustername1")
	assert.Equal(nodes[1].ClusterName, "clustername1")
	assert.Equal(nodes[2].ClusterName, "clustername1")
	assert.Equal(nodes[0].Address, "127.0.0.1:7001")
	assert.Equal(nodes[1].Address, "127.0.0.1:7002")
	assert.Equal(nodes[2].Address, "127.0.0.1:7003")
	assert.Equal(nodes[0].Slots, []broker.SlotRange{broker.SlotRange{Start: 0, End: 5000}})
	assert.Equal(nodes[1].Slots, []broker.SlotRange{broker.SlotRange{Start: 5001, End: 10000}})
	assert.Equal(nodes[2].Slots, []broker.SlotRange{
		broker.SlotRange{Start: 10001, End: 15000},
		broker.SlotRange{Start: 15001, End: 16383},
	})
}
