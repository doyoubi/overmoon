package service

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"

	"github.com/doyoubi/overmoon/src/broker"
)

// HttpBrokerProxy serves as a proxy
type HttpBrokerProxy struct {
	broker     broker.MetaDataBroker
	maniBroker broker.MetaManipulationBroker
	address    string
	ctx        context.Context
}

// NewHttpBrokerProxy creates the HttpBrokerProxy.
func NewHttpBrokerProxy(ctx context.Context, broker broker.MetaDataBroker, maniBroker broker.MetaManipulationBroker, address string) *HttpBrokerProxy {
	return &HttpBrokerProxy{
		broker:     broker,
		maniBroker: maniBroker,
		address:    address,
		ctx:        ctx,
	}
}

// Serve start the http proxy server.
func (proxy *HttpBrokerProxy) Serve() error {
	r := gin.Default()
	r.GET("/api/clusters/names", proxy.handleGetClusterNames)
	r.GET("/api/clusters/name/:name", proxy.handleGetCluster)
	r.GET("/api/hosts/addresses", proxy.handleGetHostAddresses)
	r.GET("/api/hosts/address/:address", proxy.handleGetHost)
	r.POST("/api/failures/:address/:reportID", proxy.handleAddFailure)
	r.GET("/api/failures", proxy.handleGetFailure)

	r.POST("/api/clusters", proxy.handleAddCluster)
	r.PUT("/api/clusters/nodes", proxy.handleReplaceNode)
	r.POST("/api/hosts", proxy.handleAddHost)

	return r.Run(proxy.address)
}

// GET /api/clusters/names
func (proxy *HttpBrokerProxy) handleGetClusterNames(c *gin.Context) {
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
func (proxy *HttpBrokerProxy) handleGetCluster(c *gin.Context) {
	name := c.Param("name")
	cluster, err := proxy.broker.GetCluster(proxy.ctx, name)
	if err == broker.NotExists {
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
func (proxy *HttpBrokerProxy) handleGetHostAddresses(c *gin.Context) {
	addresses, err := proxy.broker.GetHostAddresses(proxy.ctx)
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
func (proxy *HttpBrokerProxy) handleGetHost(c *gin.Context) {
	address := c.Param("address")
	host, err := proxy.broker.GetHost(proxy.ctx, address)
	if err == broker.NotExists {
		c.JSON(200, gin.H{
			"host": nil,
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
		"host": host,
	})
}

// POST /api/failures/:address/:reportID
func (proxy *HttpBrokerProxy) handleAddFailure(c *gin.Context) {
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
func (proxy *HttpBrokerProxy) handleGetFailure(c *gin.Context) {
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
	NodeNumber  int64  `json:"node_number"`
	MaxMemory   int64  `json:"max_memory"`
}

// POST /api/clusters
func (proxy *HttpBrokerProxy) handleAddCluster(c *gin.Context) {
	var cluster clusterPayload
	err := c.BindJSON(&cluster)
	if err != nil {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("failed to get json payload %s", err),
		})
		return
	}
	err = proxy.maniBroker.CreateCluster(
		proxy.ctx, cluster.ClusterName, cluster.NodeNumber, cluster.MaxMemory)
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	c.String(200, "")
}

type replaceNodePayload struct {
	ClusterEpoch int64        `json:"cluster_epoch"`
	Node         *broker.Node `json:"node"`
}

// PUT /api/clusters/nodes
func (proxy *HttpBrokerProxy) handleReplaceNode(c *gin.Context) {
	var payload replaceNodePayload
	err := c.BindJSON(&payload)
	if err != nil {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("failed to get json payload %s", err),
		})
		return
	}
	node, err := proxy.maniBroker.ReplaceNode(proxy.ctx, payload.ClusterEpoch, payload.Node)
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	c.JSON(200, node)
}

type addHostPayload struct {
	Address string   `json:"address"`
	Nodes   []string `json:"nodes"`
}

// POST /api/hosts
func (proxy *HttpBrokerProxy) handleAddHost(c *gin.Context) {
	var payload addHostPayload
	err := c.BindJSON(&payload)
	if err != nil {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("failed to get json payload %s", err),
		})
		return
	}
	err = proxy.maniBroker.AddHost(
		proxy.ctx, payload.Address, payload.Nodes)
	if err == broker.ErrHostExists {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("%s already exists", payload.Address),
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
