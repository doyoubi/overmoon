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

func TestCreateBasicClusterMeta(t *testing.T) {
	assert := assert.New(t)
	initManiData(assert)
	broker := genManiBroker(assert)
	ctx := context.Background()

	err := broker.CreateBasicClusterMeta(ctx, "test_mani_create_basic_meta", 1, 1024)
	assert.NoError(err)
}
