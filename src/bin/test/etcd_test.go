package test

import (
	"context"
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
	client.Put(ctx, "/integration_test/clusters/epoch/clustername2", "233")
}

func TestEtcdGetClusterNames(t *testing.T) {
	assert := assert.New(t)

	prepareData(assert)

	cfg := &broker.EtcdConfig{PathPrefix: "/integration_test"}
	etcdBroker, err := broker.NewEtcdMetaBrokerFromEndpoints(cfg, endpoints)
	assert.Nil(err)
	assert.NotNil(etcdBroker)

	ctx := context.Background()

	names, err := etcdBroker.GetClusterNames(ctx)
	assert.Nil(err)
	assert.NotNil(names)
	assert.Equal(2, len(names))
}
