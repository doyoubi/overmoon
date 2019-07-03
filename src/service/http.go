package service

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"

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
	r.GET("/api/clusters/name/:name", proxy.handleGetCluster)
	r.GET("/api/hosts/addresses", proxy.handleGetHostAddresses)
	r.GET("/api/hosts/address/:address", proxy.handleGetHost)
	r.POST("/api/failures/:address/:reportID", proxy.handleAddFailure)
	r.GET("/api/failures", proxy.handleGetFailure)

	r.POST("/api/clusters", proxy.handleAddCluster)
	r.POST("/api/proxies/:proxy_address/failover", proxy.handleReplaceNode)
	r.POST("/api/hosts", proxy.handleAddHost)

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
	if err == broker.ErrNotExists {
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
func (proxy *HTTPBrokerProxy) handleGetHostAddresses(c *gin.Context) {
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
func (proxy *HTTPBrokerProxy) handleGetHost(c *gin.Context) {
	address := c.Param("address")
	host, err := proxy.broker.GetHost(proxy.ctx, address)
	if err == broker.ErrNotExists {
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
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	c.String(200, "")
}

// PUT /api/clusters/nodes
func (proxy *HTTPBrokerProxy) handleReplaceNode(c *gin.Context) {
	proxyAddress := c.Param("proxy_address")
	host, err := proxy.maniBroker.ReplaceProxy(proxy.ctx, proxyAddress)
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s", err),
		})
		return
	}
	c.JSON(200, host)
}

type addHostPayload struct {
	Address string   `json:"address"`
	Nodes   []string `json:"nodes"`
}

// POST /api/hosts
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
