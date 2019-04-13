package test

import (
	"context"
	"encoding/json"
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

	node1 := &broker.Node{
		Address:      "127.0.0.1:7001",
		ProxyAddress: "127.0.0.1:5299",
		ClusterName:  "clustername1",
		Slots: []broker.SlotRange{broker.SlotRange{
			Start: 0, End: 5000, Tag: "",
		}},
		Role: "master",
	}
	node2 := &broker.Node{
		Address:      "127.0.0.1:7002",
		ProxyAddress: "127.0.0.1:5299",
		ClusterName:  "clustername1",
		Slots: []broker.SlotRange{broker.SlotRange{
			Start: 5001, End: 10000, Tag: "",
		}},
		Role: "master",
	}
	node3 := &broker.Node{
		Address:      "127.0.0.2:7003",
		ProxyAddress: "127.0.0.2:5299",
		ClusterName:  "clustername1",
		Slots: []broker.SlotRange{
			broker.SlotRange{Start: 10001, End: 15000, Tag: ""},
			broker.SlotRange{Start: 15001, End: 16382, Tag: ""},
			broker.SlotRange{Start: 16383, End: 16383, Tag: ""},
		},
		Role: "master",
	}
	node1Str, err := json.Marshal(node1)
	assert.NoError(err)
	node2Str, err := json.Marshal(node2)
	assert.NoError(err)
	node3Str, err := json.Marshal(node3)
	assert.NoError(err)

	assert.Nil(err)
	assert.NotNil(client)
	client.Put(ctx, "/integration_test/clusters/epoch/clustername1", "233")
	client.Put(ctx, "/integration_test/clusters/epoch/clustername2", "666")
	client.Put(ctx, "/integration_test/clusters/nodes/clustername1/127.0.0.1:7001", string(node1Str))
	client.Put(ctx, "/integration_test/clusters/nodes/clustername1/127.0.0.1:7002", string(node2Str))
	client.Put(ctx, "/integration_test/clusters/nodes/clustername1/127.0.0.2:7003", string(node3Str))
	client.Put(ctx, "/integration_test/hosts/epoch/127.0.0.1:5299", "2333")
	client.Put(ctx, "/integration_test/hosts/epoch/127.0.0.2:5299", "6666")
	client.Put(ctx, "/integration_test/hosts/127.0.0.1:5299/nodes/127.0.0.1:7001", string(node1Str))
	client.Put(ctx, "/integration_test/hosts/127.0.0.1:5299/nodes/127.0.0.1:7002", string(node2Str))
	client.Put(ctx, "/integration_test/hosts/127.0.0.2:5299/nodes/127.0.0.2:7003", string(node3Str))
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
	assert.Equal(nodes[2].Address, "127.0.0.2:7003")
	assert.Equal(nodes[0].Slots, []broker.SlotRange{broker.SlotRange{Start: 0, End: 5000}})
	assert.Equal(nodes[1].Slots, []broker.SlotRange{broker.SlotRange{Start: 5001, End: 10000}})
	assert.Equal(nodes[2].Slots, []broker.SlotRange{
		broker.SlotRange{Start: 10001, End: 15000},
		broker.SlotRange{Start: 15001, End: 16382},
		broker.SlotRange{Start: 16383, End: 16383},
	})
}

func TestEtcdGetHosts(t *testing.T) {
	assert := assert.New(t)

	prepareData(assert)
	etcdBroker := genBroker(assert)

	ctx := context.Background()

	addresses, err := etcdBroker.GetHostAddresses(ctx)
	assert.Nil(err)
	assert.NotNil(addresses)
	assert.Equal(2, len(addresses))
	sort.Strings(addresses)
	assert.Equal("127.0.0.1:5299", addresses[0])
	assert.Equal("127.0.0.2:5299", addresses[1])
}

func TestEtcdGetHost(t *testing.T) {
	assert := assert.New(t)

	prepareData(assert)
	etcdBroker := genBroker(assert)

	ctx := context.Background()

	host, err := etcdBroker.GetHost(ctx, "127.0.0.1:5299")
	assert.Nil(err)
	assert.NotNil(host)
	assert.Equal(int64(2333), host.Epoch)
	assert.Equal("127.0.0.1:5299", host.Address)
	assert.Equal(2, len(host.Nodes))
	nodes := sortNodes(host.Nodes)
	assert.Equal(2, len(nodes))
}

func TestEtcdAddFailures(t *testing.T) {
	assert := assert.New(t)

	prepareData(assert)
	etcdBroker := genBroker(assert)

	ctx := context.Background()

	err := etcdBroker.AddFailure(ctx, "127.0.0.1:5299", "report_id1")
	assert.Nil(err)
	addresses, err := etcdBroker.GetFailures(ctx)
	assert.Equal(1, len(addresses))
}
