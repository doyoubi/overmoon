package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"

	"github.com/doyoubi/overmoon/src/broker"
)

func initManiData(assert *assert.Assertions) {
	ctx := context.Background()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints,
	})

	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
	}
	_, err = client.Delete(ctx, "/", opts...)
	assert.NoError(err)
}

func genManiBroker(assert *assert.Assertions) *broker.EtcdMetaManipulationBroker {
	cfg := &broker.EtcdConfig{
		PathPrefix: "/integration_test",
		FailureTTL: 10,
	}
	etcdBroker, err := broker.NewEtcdMetaManipulationBrokerFromEndpoints(cfg, endpoints)
	assert.NoError(err)
	assert.NotNil(etcdBroker)
	return etcdBroker
}

// func TestCreateBasicClusterMeta(t *testing.T) {
// 	assert := assert.New(t)
// 	initManiData(assert)
// 	broker := genManiBroker(assert)
// 	meta_broker := genBroker(assert)
// 	ctx := context.Background()

// 	clusterName := "test_mani_create_basic_meta"
// 	err := broker.CreateBasicClusterMeta(ctx, clusterName, 1, 1024)
// 	assert.NoError(err)
// 	cluster, err := meta_broker.GetCluster(ctx, clusterName)
// 	assert.NoError(err)
// 	assert.NotNil(cluster)
// 	assert.Equal(int64(1), cluster.Epoch)
// 	assert.Equal(0, len(cluster.Nodes))
// 	assert.Equal(clusterName, cluster.Name)
// }

func TestAddProxies(t *testing.T) {
	assert := assert.New(t)
	initManiData(assert)
	maniBroker := genManiBroker(assert)
	metaBroker := maniBroker.GetMetaBroker()
	ctx := context.Background()

	err := maniBroker.InitGlobalEpoch()
	assert.NoError(err)

	nodes := []string{
		"127.0.0.1:7001",
		"127.0.0.1:7002",
	}
	err = maniBroker.AddHost(ctx, "127.0.0.1:6001", nodes)
	assert.NoError(err)
	host, err := metaBroker.GetHost(ctx, "127.0.0.1:6001")
	assert.NoError(err)
	assert.NotNil(host)
	assert.Equal(host.Address, "127.0.0.1:6001")
	assert.Equal(host.Epoch, uint64(0))
	assert.Equal(len(host.Nodes), 0)
}

// func TestCreateAndDeleteNode(t *testing.T) {
// 	assert := assert.New(t)
// 	initManiData(assert)
// mb := genBroker(assert)
// b := genManiBroker(assert)
// ctx := context.Background()

// nodes := []string{
// 	"127.0.0.1:7001",
// }
// clusterName := "test_create_node"

// err := b.AddHost(ctx, "127.0.0.1:5299", nodes)
// assert.NoError(err)
// err = b.CreateBasicClusterMeta(ctx, clusterName, 1, 1024)
// assert.NoError(err)
// slots := []broker.SlotRange{
// 	broker.SlotRange{
// 		Start: 0,
// 		End:   2333,
// 		Tag:   "",
// 	},
// }
// node, err := b.CreateNode(ctx, clusterName, 1, slots, broker.MasterRole)
// assert.NoError(err)
// assert.NotNil(node)
// assert.Equal(node.Address, "127.0.0.1:7001")
// assert.Equal(node.ClusterName, clusterName)
// assert.Equal(node.ProxyAddress, "127.0.0.1:5299")
// assert.Equal(1, len(node.Slots))
// assert.Equal(int64(0), node.Slots[0].Start)
// 	assert.Equal(int64(2333), node.Slots[0].End, 2333)
// 	assert.Equal("", node.Slots[0].Tag)

// 	cluster, err := mb.GetCluster(ctx, clusterName)
// 	assert.NoError(err)
// 	assert.NotNil(cluster)
// 	assert.Equal(1, len(cluster.Nodes))

// 	err = b.DeleteNode(ctx, 2, node)
// 	assert.NoError(err)

// 	cluster, err = mb.GetCluster(ctx, clusterName)
// 	assert.NoError(err)
// 	assert.NotNil(cluster)
// 	assert.Equal(0, len(cluster.Nodes))
// }

// func TestReplaceNode(t *testing.T) {
// 	assert := assert.New(t)
// 	initManiData(assert)
// 	mb := genBroker(assert)
// 	b := genManiBroker(assert)
// 	ctx := context.Background()

// 	nodes1 := []string{
// 		"127.0.0.1:7001",
// 	}
// 	clusterName := "test_replace_node"

// 	err := b.AddHost(ctx, "127.0.0.1:5299", nodes1)
// 	assert.NoError(err)
// 	err = b.CreateBasicClusterMeta(ctx, clusterName, 1, 1024)
// 	assert.NoError(err)
// 	slots := []broker.SlotRange{
// 		broker.SlotRange{
// 			Start: 0,
// 			End:   2333,
// 			Tag:   "",
// 		},
// 	}
// 	node, err := b.CreateNode(ctx, clusterName, 1, slots, broker.MasterRole)
// 	assert.NoError(err)
// 	assert.NotNil(node)

// 	nodes2 := []string{
// 		"127.0.0.2:7001",
// 	}
// 	err = b.AddHost(ctx, "127.0.0.2:5299", nodes2)

// 	newNode, err := b.ReplaceNode(ctx, 2, node)
// 	assert.NoError(err)
// 	assert.NotNil(newNode)
// 	assert.Equal("127.0.0.2:7001", newNode.Address)
// 	assert.Equal(clusterName, newNode.ClusterName)
// 	assert.Equal("127.0.0.2:5299", newNode.ProxyAddress)
// 	assert.Equal(1, len(newNode.Slots))

// 	cluster, err := mb.GetCluster(ctx, clusterName)
// 	assert.NoError(err)
// 	assert.NotNil(cluster)
// 	assert.Equal(1, len(cluster.Nodes))
// 	assert.Equal("127.0.0.2:7001", cluster.Nodes[0].Address)
// }

func TestCreateCluster(t *testing.T) {
	assert := assert.New(t)
	initManiData(assert)
	maniBroker := genManiBroker(assert)
	metaBroker := maniBroker.GetMetaBroker()
	ctx := context.Background()

	err := maniBroker.InitGlobalEpoch()
	assert.NoError(err)

	nodes1 := []string{
		"127.0.0.1:7001",
		"127.0.0.1:7002",
	}
	nodes2 := []string{
		"127.0.0.2:7001",
		"127.0.0.2:7002",
	}
	clusterName := "test_create_cluster"

	err = maniBroker.AddHost(ctx, "127.0.0.1:6001", nodes1)
	err = maniBroker.AddHost(ctx, "127.0.0.2:6002", nodes2)
	assert.NoError(err)
	err = maniBroker.CreateCluster(ctx, clusterName, 4)
	assert.NoError(err)

	cluster, err := metaBroker.GetCluster(ctx, clusterName)
	assert.NoError(err)
	assert.Equal(clusterName, cluster.Name)
	assert.True(cluster.Epoch > 0)
	assert.Equal(4, len(cluster.Nodes))

	nodes := cluster.Nodes
	assert.Equal(broker.MasterRole, nodes[0].Repl.Role)
	assert.Equal(broker.ReplicaRole, nodes[1].Repl.Role)
	assert.Equal(broker.MasterRole, nodes[2].Repl.Role)
	assert.Equal(broker.ReplicaRole, nodes[3].Repl.Role)

	for _, node := range nodes {
		assert.Equal(1, len(node.Repl.Peers))
	}

	assert.Equal(nodes[3].Address, nodes[0].Repl.Peers[0].NodeAddress)
	assert.Equal(nodes[3].ProxyAddress, nodes[0].Repl.Peers[0].ProxyAddress)
	assert.Equal(nodes[2].Address, nodes[1].Repl.Peers[0].NodeAddress)
	assert.Equal(nodes[2].ProxyAddress, nodes[1].Repl.Peers[0].ProxyAddress)
	assert.Equal(nodes[1].Address, nodes[2].Repl.Peers[0].NodeAddress)
	assert.Equal(nodes[1].ProxyAddress, nodes[2].Repl.Peers[0].ProxyAddress)
	assert.Equal(nodes[0].Address, nodes[3].Repl.Peers[0].NodeAddress)
	assert.Equal(nodes[0].ProxyAddress, nodes[3].Repl.Peers[0].ProxyAddress)
}
