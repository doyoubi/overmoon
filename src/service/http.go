package service

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"github.com/doyoubi/overmoon/src/broker"
)

// HTTPBrokerProxy serves as a proxy
type HTTPBrokerProxy struct {
	broker     broker.MetaDataBroker
	maniBroker broker.MetaManipulationBroker
	address    string
	ctx        context.Context
}

// NewHTTPBrokerProxy creates the HttpBrokerProxy.
func NewHTTPBrokerProxy(ctx context.Context, broker broker.MetaDataBroker, maniBroker broker.MetaManipulationBroker, address string) *HTTPBrokerProxy {
	return &HTTPBrokerProxy{
		broker:     broker,
		maniBroker: maniBroker,
		address:    address,
		ctx:        ctx,
	}
}

// Serve start the http proxy server.
func (proxy *HTTPBrokerProxy) Serve() error {
	r := gin.Default()
	r.GET("/api/clusters/names", proxy.handleGetClusterNames)
	r.GET("/api/clusters/meta/:name", proxy.handleGetCluster)
	r.GET("/api/proxies/addresses", proxy.handleGetProxyAddresses)
	r.GET("/api/proxies/meta/:address", proxy.handleGetProxy)
	r.POST("/api/failures/:address/:reportID", proxy.handleAddFailure)
	r.GET("/api/failures", proxy.handleGetFailure)
	r.PUT("/api/clusters/migrations", proxy.handleCommitMigration)

	r.POST("/api/clusters", proxy.handleAddCluster)
	r.POST("/api/proxies/failover/:proxy_address", proxy.handleReplaceProxy)
	r.POST("/api/proxies/nodes", proxy.handleAddHost)
	r.PUT("/api/clusters/nodes/:clusterName", proxy.handleAddNodes)
	r.POST("/api/clusters/migrations/:clusterName", proxy.handleMigrateSlots)

	return r.Run(proxy.address)
}

// GET /api/clusters/names
func (proxy *HTTPBrokerProxy) handleGetClusterNames(c *gin.Context) {
	names, err := proxy.broker.GetClusterNames(proxy.ctx)
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	c.JSON(200, gin.H{
		"names": names,
	})
}

// GET /api/clusters/name/:name
func (proxy *HTTPBrokerProxy) handleGetCluster(c *gin.Context) {
	name := c.Param("name")
	cluster, err := proxy.broker.GetCluster(proxy.ctx, name)
	if err == broker.ErrClusterNotFound {
		c.JSON(200, gin.H{
			"cluster": nil,
		})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	c.JSON(200, gin.H{
		"cluster": cluster,
	})
}

// GET /api/hosts/addresses
func (proxy *HTTPBrokerProxy) handleGetProxyAddresses(c *gin.Context) {
	addresses, err := proxy.broker.GetProxyAddresses(proxy.ctx)
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	c.JSON(200, gin.H{
		"addresses": addresses,
	})
}

// GET /api/hosts/address/:address
func (proxy *HTTPBrokerProxy) handleGetProxy(c *gin.Context) {
	address := c.Param("address")
	host, err := proxy.broker.GetProxy(proxy.ctx, address)
	if err == broker.ErrProxyNotFound {
		c.JSON(200, gin.H{
			"host": nil,
		})
		return
	}
	if err == broker.ErrTryAgain {
		c.JSON(503, gin.H{
			"error": "try again",
		})
		return
	}
	if err != nil {
		log.Errorf("failed to get proxy %+v", err)
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	c.JSON(200, gin.H{
		"host": host,
	})
}

// POST /api/failures/:address/:reportID
func (proxy *HTTPBrokerProxy) handleAddFailure(c *gin.Context) {
	address := c.Param("address")
	reportID := c.Param("reportID")

	err := proxy.broker.AddFailure(proxy.ctx, address, reportID)
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	c.String(200, "")
}

// GET /api/failures
func (proxy *HTTPBrokerProxy) handleGetFailure(c *gin.Context) {
	addresses, err := proxy.broker.GetFailures(proxy.ctx)
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	c.JSON(200, gin.H{
		"addresses": addresses,
	})
}

type clusterPayload struct {
	ClusterName string `json:"cluster_name"`
	NodeNumber  uint64 `json:"node_number"`
}

// POST /api/clusters
func (proxy *HTTPBrokerProxy) handleAddCluster(c *gin.Context) {
	var cluster clusterPayload
	err := c.BindJSON(&cluster)
	if err != nil {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("failed to get json payload %s", err),
		})
		return
	}
	err = proxy.maniBroker.CreateCluster(
		proxy.ctx, cluster.ClusterName, cluster.NodeNumber)
	if err == broker.ErrNoAvailableResource {
		c.JSON(409, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	if err != nil {
		log.Errorf("failed to create cluster %+v", err)
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	c.String(200, "")
}

// PUT /api/clusters/nodes
func (proxy *HTTPBrokerProxy) handleReplaceProxy(c *gin.Context) {
	proxyAddress := c.Param("proxy_address")
	host, err := proxy.maniBroker.ReplaceProxy(proxy.ctx, proxyAddress)
	if err == broker.ErrNoAvailableResource {
		c.JSON(409, gin.H{
			"error": fmt.Sprintf("No available resource: %s", err),
		})
		return
	}
	if err == broker.ErrProxyNotInUse || err == broker.ErrProxyNotFound {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("can't replace proxy %s: %s", proxyAddress, err),
		})
		return
	}
	if err == broker.ErrTryAgain {
		c.JSON(503, gin.H{
			"error": "try again",
		})
		return
	}
	if err != nil {
		log.Errorf("failed to replace proxy: %+v", err)
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	c.JSON(200, host)
}

type addHostPayload struct {
	ProxyAddress string   `json:"proxy_address"`
	Nodes        []string `json:"nodes"`
}

func (proxy *HTTPBrokerProxy) handleAddHost(c *gin.Context) {
	var payload addHostPayload
	err := c.BindJSON(&payload)
	if err != nil {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("failed to get json payload %s", err),
		})
		return
	}
	err = proxy.maniBroker.AddHost(
		proxy.ctx, payload.ProxyAddress, payload.Nodes)
	if err == broker.ErrHostExists {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("%s already exists", payload.ProxyAddress),
		})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	c.String(200, "")
}

type addHNodesPayload struct {
	ExpectedNodeNumber uint64 `json:"expected_node_number"`
}

func (proxy *HTTPBrokerProxy) handleAddNodes(c *gin.Context) {
	clusterName := c.Param("clusterName")
	var payload addHNodesPayload
	err := c.BindJSON(&payload)
	if err != nil {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("failed to get json payload %s", err),
		})
		return
	}

	err = proxy.maniBroker.AddNodesToCluster(proxy.ctx, clusterName, payload.ExpectedNodeNumber)
	if err == broker.ErrNoAvailableResource {
		c.JSON(409, gin.H{
			"error": "no available resource",
		})
		return
	}
	if err == broker.ErrClusterNotFound {
		c.JSON(404, gin.H{
			"error": fmt.Sprintf("cluster %s not found", clusterName),
		})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("failed to add nodes to cluster %s: %s", clusterName, err),
		})
		return
	}
}

func (proxy *HTTPBrokerProxy) handleMigrateSlots(c *gin.Context) {
	clusterName := c.Param("clusterName")
	err := proxy.maniBroker.MigrateSlots(proxy.ctx, clusterName)
	if err == broker.ErrClusterNotFound {
		c.JSON(404, gin.H{
			"error": fmt.Sprintf("cluster %s not found", clusterName),
		})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s failed to migrate slots: %s", clusterName, err),
		})
		return
	}
}

func (proxy *HTTPBrokerProxy) handleCommitMigration(c *gin.Context) {
	var payload broker.MigrationTaskMeta
	err := c.BindJSON(&payload)
	if err != nil {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("failed to get json payload %s", err),
		})
		return
	}
	err = proxy.maniBroker.CommitMigration(proxy.ctx, payload)
	if err == broker.ErrClusterNotFound {
		c.JSON(404, gin.H{
			"error": fmt.Sprintf("cluster %s not found", payload.DBName),
		})
		return
	}
	if err == broker.ErrMigrationTaskNotFound {
		c.JSON(404, gin.H{
			"error": fmt.Sprintf("task %+v not found", payload),
		})
		return
	}
	if err == broker.ErrInvalidRequestedMigrationSlotRange {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("invalid migration task %+v", payload),
		})
		return
	}
	if err == broker.ErrAlreadyMigrating {
		c.JSON(400, gin.H{
			"error": "already migrating",
		})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s failed to migrate slots: %s", payload.DBName, err),
		})
		return
	}
}
