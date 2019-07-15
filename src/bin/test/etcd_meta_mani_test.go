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
	metaBroker, err := broker.NewEtcdMetaBrokerFromEndpoints(cfg, endpoints)
	assert.Nil(err)
	assert.NotNil(metaBroker)
	maniBroker, err := broker.NewEtcdMetaManipulationBrokerFromEndpoints(cfg, endpoints, metaBroker)
	assert.NoError(err)
	assert.NotNil(maniBroker)
	return maniBroker
}

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
	err = maniBroker.AddProxy(ctx, "127.0.0.1:6001", nodes)
	assert.NoError(err)
	host, err := metaBroker.GetProxy(ctx, "127.0.0.1:6001")
	assert.NoError(err)
	assert.NotNil(host)
	assert.Equal(host.Address, "127.0.0.1:6001")
	assert.Equal(host.Epoch, uint64(0))
	assert.Equal(len(host.Nodes), 0)
}

func TestReplaceProxy(t *testing.T) {
	assert := assert.New(t)
	initManiData(assert)
	prepareData(assert)
	maniBroker := genManiBroker(assert)
	metaBroker := maniBroker.GetMetaBroker()
	ctx := context.Background()

	clusterName := "mycluster"
	cluster, err := metaBroker.GetCluster(ctx, clusterName)
	assert.NoError(err)
	assert.NotNil(cluster)

	assert.Equal("127.0.0.1:7001", cluster.Nodes[0].Address)
	assert.Equal("127.0.0.1:6001", cluster.Nodes[0].ProxyAddress)
	assert.Equal("127.0.0.1:7002", cluster.Nodes[1].Address)
	assert.Equal("127.0.0.1:6001", cluster.Nodes[1].ProxyAddress)

	assert.Equal(4, len(cluster.Nodes))
	assert.Equal(broker.MasterRole, cluster.Nodes[0].Repl.Role)
	assert.Equal(broker.ReplicaRole, cluster.Nodes[1].Repl.Role)
	assert.Equal(broker.MasterRole, cluster.Nodes[2].Repl.Role)
	assert.Equal(broker.ReplicaRole, cluster.Nodes[3].Repl.Role)

	proxy1, err := metaBroker.GetProxy(ctx, "127.0.0.1:6001")
	assert.NoError(err)
	assert.NotNil(proxy1)
	assert.Equal(2, len(proxy1.Nodes))
	proxy3, err := metaBroker.GetProxy(ctx, "127.0.0.3:6003")
	assert.NoError(err)
	assert.NotNil(proxy3)
	assert.Equal(0, len(proxy3.Nodes))

	_, err = maniBroker.ReplaceProxy(ctx, "127.0.0.1:6001")
	assert.NoError(err)
	metaBroker.ClearCache()

	cluster, err = metaBroker.GetCluster(ctx, clusterName)
	assert.NoError(err)
	assert.NotNil(cluster)

	assert.Equal("127.0.0.3:7005", cluster.Nodes[0].Address)
	assert.Equal("127.0.0.3:6003", cluster.Nodes[0].ProxyAddress)
	assert.Equal("127.0.0.3:7006", cluster.Nodes[1].Address)
	assert.Equal("127.0.0.3:6003", cluster.Nodes[1].ProxyAddress)

	assert.Equal(4, len(cluster.Nodes))
	assert.Equal(broker.ReplicaRole, cluster.Nodes[0].Repl.Role)
	assert.Equal(broker.ReplicaRole, cluster.Nodes[1].Repl.Role)
	assert.Equal(broker.MasterRole, cluster.Nodes[2].Repl.Role)
	assert.Equal(broker.MasterRole, cluster.Nodes[3].Repl.Role)

	proxy1, err = metaBroker.GetProxy(ctx, "127.0.0.1:6001")
	assert.Equal(broker.ErrProxyNotFound, err)
	assert.Nil(proxy1)
	proxy3, err = metaBroker.GetProxy(ctx, "127.0.0.3:6003")
	assert.NoError(err)
	assert.NotNil(proxy3)
	assert.Equal(2, len(proxy3.Nodes))
}

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

	err = maniBroker.AddProxy(ctx, "127.0.0.1:6001", nodes1)
	err = maniBroker.AddProxy(ctx, "127.0.0.2:6002", nodes2)
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

	assert.Equal(1, len(nodes[0].Slots))
	assert.Equal(0, len(nodes[1].Slots))
	assert.Equal(1, len(nodes[2].Slots))
	assert.Equal(0, len(nodes[3].Slots))
	assert.Equal(uint64(0), nodes[0].Slots[0].Start)
	assert.Equal(uint64(broker.MaxSlotNumber/2-1), nodes[0].Slots[0].End)
	assert.Equal(uint64(broker.MaxSlotNumber/2), nodes[2].Slots[0].Start)
	assert.Equal(uint64(broker.MaxSlotNumber-1), nodes[2].Slots[0].End)

	err = maniBroker.CreateCluster(ctx, clusterName, 4)
	assert.Equal(broker.ErrClusterExists, err)
}

func TestAddNodes(t *testing.T) {
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
	nodes3 := []string{
		"127.0.0.3:7001",
		"127.0.0.3:7002",
	}
	nodes4 := []string{
		"127.0.0.4:7001",
		"127.0.0.4:7002",
	}
	clusterName := "test_create_cluster"

	err = maniBroker.AddProxy(ctx, "127.0.0.1:6001", nodes1)
	assert.NoError(err)
	err = maniBroker.AddProxy(ctx, "127.0.0.2:6002", nodes2)
	assert.NoError(err)
	err = maniBroker.CreateCluster(ctx, clusterName, 4)
	assert.NoError(err)

	cluster, err := metaBroker.GetCluster(ctx, clusterName)
	assert.NoError(err)
	assert.Equal(4, len(cluster.Nodes))

	err = maniBroker.AddProxy(ctx, "127.0.0.3:6003", nodes3)
	assert.NoError(err)
	err = maniBroker.AddProxy(ctx, "127.0.0.4:6004", nodes4)
	assert.NoError(err)

	metaBroker.ClearCache()
	err = maniBroker.AddNodesToCluster(ctx, clusterName, 8)
	assert.NoError(err)

	metaBroker.ClearCache()
	cluster, err = metaBroker.GetCluster(ctx, clusterName)
	assert.NoError(err)
	assert.Equal(8, len(cluster.Nodes))

	// test migration
	err = maniBroker.MigrateSlots(ctx, clusterName)
	assert.NoError(err)
	metaBroker.ClearCache()

	cluster, err = metaBroker.GetCluster(ctx, clusterName)
	assert.NoError(err)
	assert.Equal(8, len(cluster.Nodes))
	for i, node := range cluster.Nodes {
		if node.Repl.Role == broker.MasterRole {
			if i < 4 {
				assert.Equal(2, len(node.Slots))
			} else {
				assert.Equal(1, len(node.Slots))
			}
		} else {
			assert.Equal(0, len(node.Slots))
		}
	}

	task := broker.MigrationTaskMeta{
		DBName: clusterName,
		Slots: broker.SlotRange{
			Start: 0,
			End:   4095,
			Tag: broker.SlotRangeTag{
				TagType: broker.NoneTag,
			},
		},
	}
	err = maniBroker.CommitMigration(ctx, task)
	assert.Equal(broker.ErrInvalidRequestedMigrationSlotRange, err)

	task = broker.MigrationTaskMeta{
		DBName: clusterName,
		Slots: broker.SlotRange{
			Start: 4096,
			End:   8191,
			Tag: broker.SlotRangeTag{
				TagType: broker.MigratingTag,
				Meta: &broker.MigrationMeta{
					Epoch:           cluster.Epoch,
					SrcNodeAddress:  cluster.Nodes[0].Address,
					SrcProxyAddress: cluster.Nodes[0].ProxyAddress,
					DstNodeAddress:  cluster.Nodes[4].Address,
					DstProxyAddress: cluster.Nodes[4].ProxyAddress,
				},
			},
		},
	}
	err = maniBroker.CommitMigration(ctx, task)
	assert.NoError(err)

	task.Slots = broker.SlotRange{
		Start: 12288,
		End:   16383,
		Tag: broker.SlotRangeTag{
			TagType: broker.MigratingTag,
			Meta: &broker.MigrationMeta{
				Epoch:           cluster.Epoch,
				SrcNodeAddress:  cluster.Nodes[2].Address,
				SrcProxyAddress: cluster.Nodes[2].ProxyAddress,
				DstNodeAddress:  cluster.Nodes[6].Address,
				DstProxyAddress: cluster.Nodes[6].ProxyAddress,
			},
		},
	}
	err = maniBroker.CommitMigration(ctx, task)
	assert.NoError(err)

	metaBroker.ClearCache()
	cluster, err = metaBroker.GetCluster(ctx, clusterName)
	assert.NoError(err)
	assert.Equal(8, len(cluster.Nodes))
	for _, node := range cluster.Nodes {
		if node.Repl.Role == broker.MasterRole {
			assert.Equal(1, len(node.Slots))
		} else {
			assert.Equal(0, len(node.Slots))
		}
	}
}

func TestRemoveProxies(t *testing.T) {
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
	nodes3 := []string{
		"127.0.0.3:7001",
		"127.0.0.3:7002",
	}
	nodes4 := []string{
		"127.0.0.4:7001",
		"127.0.0.4:7002",
	}
	clusterName := "test_remove_cluster"

	err = maniBroker.AddProxy(ctx, "127.0.0.1:6001", nodes1)
	assert.NoError(err)
	err = maniBroker.AddProxy(ctx, "127.0.0.2:6002", nodes2)
	assert.NoError(err)
	err = maniBroker.CreateCluster(ctx, clusterName, 4)
	assert.NoError(err)

	cluster, err := metaBroker.GetCluster(ctx, clusterName)
	assert.NoError(err)
	assert.Equal(4, len(cluster.Nodes))

	err = maniBroker.AddProxy(ctx, "127.0.0.3:6003", nodes3)
	assert.NoError(err)
	err = maniBroker.AddProxy(ctx, "127.0.0.4:6004", nodes4)
	assert.NoError(err)

	metaBroker.ClearCache()
	err = maniBroker.AddNodesToCluster(ctx, clusterName, 8)
	assert.NoError(err)

	metaBroker.ClearCache()
	cluster, err = metaBroker.GetCluster(ctx, clusterName)
	assert.NoError(err)
	assert.Equal(8, len(cluster.Nodes))

	proxy3, err := metaBroker.GetProxy(ctx, "127.0.0.3:6003")
	assert.NoError(err)
	assert.Equal(2, len(proxy3.Nodes))
	proxy4, err := metaBroker.GetProxy(ctx, "127.0.0.4:6004")
	assert.NoError(err)
	assert.Equal(2, len(proxy4.Nodes))

	err = maniBroker.RemoveUnusedProxiesFromCluster(ctx, clusterName)
	assert.NoError(err)

	metaBroker.ClearCache()
	cluster, err = metaBroker.GetCluster(ctx, clusterName)
	assert.NoError(err)
	assert.Equal(4, len(cluster.Nodes))

	proxy3, err = metaBroker.GetProxy(ctx, "127.0.0.3:6003")
	assert.NoError(err)
	assert.Equal(0, len(proxy3.Nodes))
	proxy4, err = metaBroker.GetProxy(ctx, "127.0.0.4:6004")
	assert.NoError(err)
	assert.Equal(0, len(proxy4.Nodes))

	err = maniBroker.RemoveProxy(ctx, "127.0.0.3:6003")
	assert.NoError(err)
	err = maniBroker.RemoveProxy(ctx, "127.0.0.4:6004")
	assert.NoError(err)

	metaBroker.ClearCache()
	_, err = metaBroker.GetProxy(ctx, "127.0.0.3:6003")
	assert.Equal(broker.ErrProxyNotFound, err)
	_, err = metaBroker.GetProxy(ctx, "127.0.0.4:6004")
	assert.Equal(broker.ErrProxyNotFound, err)

	err = maniBroker.RemoveCluster(ctx, clusterName)
	assert.NoError(err)

	metaBroker.ClearCache()
	_, err = metaBroker.GetCluster(ctx, clusterName)
	assert.Equal(broker.ErrClusterNotFound, err)

	proxy1, err := metaBroker.GetProxy(ctx, "127.0.0.1:6001")
	assert.NoError(err)
	assert.Equal(0, len(proxy1.Nodes))
	proxy2, err := metaBroker.GetProxy(ctx, "127.0.0.2:6002")
	assert.NoError(err)
	assert.Equal(0, len(proxy2.Nodes))
}
