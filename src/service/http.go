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

type httpResponse struct {
	statusCode int
	errorMsg   string
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
	r := gin.New()
	r.Use(gin.Recovery())

	freqGroup := r.Group("/api")
	logGroup := r.Group("/api")
	logGroup.Use(gin.Logger())

	freqGroup.GET("/clusters/names", proxy.handleGetClusterNames)
	freqGroup.GET("/clusters/meta/:clusterName", proxy.handleGetCluster)
	freqGroup.GET("/proxies/addresses", proxy.handleGetProxyAddresses)
	freqGroup.GET("/proxies/meta/:proxyAddress", proxy.handleGetProxy)
	freqGroup.POST("/failures/:proxyAddress/:reportID", proxy.handleAddFailure)
	freqGroup.GET("/failures", proxy.handleGetFailure)

	logGroup.PUT("/clusters/migrations", proxy.handleCommitMigration)

	logGroup.POST("/clusters", proxy.handleAddCluster)
	logGroup.POST("/proxies/failover/:proxyAddress", proxy.handleReplaceProxy)
	logGroup.POST("/proxies/nodes", proxy.handleAddProxy)
	logGroup.PUT("/clusters/nodes/:clusterName", proxy.handleAddNodes)
	logGroup.POST("/clusters/migrations/:clusterName", proxy.handleMigrateSlots)
	logGroup.DELETE("/proxies/nodes/:proxyAddress", proxy.handleRemoveProxy)
	logGroup.DELETE("/clusters/free_nodes/:clusterName", proxy.handleRemoveUnusedProxiesFromCluster)
	logGroup.DELETE("/clusters/meta/:clusterName", proxy.handleRemoveCluster)
	logGroup.GET("/version", proxy.handlerGetVersion)
	logGroup.PUT("clusters/config/:clusterName", proxy.handleSetClusterConfig)

	return r.Run(proxy.address)
}

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

func (proxy *HTTPBrokerProxy) handleGetCluster(c *gin.Context) {
	name := c.Param("clusterName")
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

func (proxy *HTTPBrokerProxy) handleGetProxy(c *gin.Context) {
	address := c.Param("proxyAddress")
	host, err := proxy.broker.GetProxy(proxy.ctx, address)
	if err == broker.ErrProxyNotFound {
		c.JSON(200, gin.H{
			"host": nil,
		})
		return
	}
	errMap := map[error]httpResponse{
		broker.ErrTryAgain: httpResponse{statusCode: 503, errorMsg: "try again"},
	}
	if response, ok := errMap[err]; ok {
		c.JSON(response.statusCode, gin.H{
			"error": response.errorMsg,
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

func (proxy *HTTPBrokerProxy) handleAddFailure(c *gin.Context) {
	address := c.Param("proxyAddress")
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
	errMap := map[error]httpResponse{
		broker.ErrNoAvailableResource: httpResponse{statusCode: 409, errorMsg: fmt.Sprintf("no available resource: %s", err)},
		broker.ErrInvalidNodesNum:     httpResponse{statusCode: 400, errorMsg: fmt.Sprintf("invalid nodes number: %s", err)},
		broker.ErrClusterExists:       httpResponse{statusCode: 400, errorMsg: fmt.Sprintf("cluster %s already exists", cluster.ClusterName)},
	}
	if response, ok := errMap[err]; ok {
		c.JSON(response.statusCode, gin.H{
			"error": response.errorMsg,
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

func (proxy *HTTPBrokerProxy) handleReplaceProxy(c *gin.Context) {
	proxyAddress := c.Param("proxyAddress")
	host, err := proxy.maniBroker.ReplaceProxy(proxy.ctx, proxyAddress)
	errMap := map[error]httpResponse{
		broker.ErrNoAvailableResource: httpResponse{statusCode: 409, errorMsg: fmt.Sprintf("no available resource: %s", err)},
		broker.ErrProxyNotFound:       httpResponse{statusCode: 400, errorMsg: fmt.Sprintf("proxy %s not found", proxyAddress)},
		broker.ErrProxyNotInUse:       httpResponse{statusCode: 400, errorMsg: fmt.Sprintf("proxy %s not in use", proxyAddress)},
		broker.ErrTryAgain:            httpResponse{statusCode: 503, errorMsg: "try again"},
		broker.ErrClusterNotFound:     httpResponse{statusCode: 400, errorMsg: "cluster not found"},
	}
	if response, ok := errMap[err]; ok {
		c.JSON(response.statusCode, gin.H{
			"error": response.errorMsg,
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

type addProxyPayload struct {
	ProxyAddress string   `json:"proxy_address"`
	Nodes        []string `json:"nodes"`
}

func (proxy *HTTPBrokerProxy) handleAddProxy(c *gin.Context) {
	var payload addProxyPayload
	err := c.BindJSON(&payload)
	if err != nil {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("failed to get json payload %s", err),
		})
		return
	}
	err = proxy.maniBroker.AddProxy(
		proxy.ctx, payload.ProxyAddress, payload.Nodes)
	errMap := map[error]httpResponse{
		broker.ErrHostExists:      httpResponse{statusCode: 400, errorMsg: fmt.Sprintf("%s already exists", payload.ProxyAddress)},
		broker.ErrInvalidNodesNum: httpResponse{statusCode: 400, errorMsg: fmt.Sprintf("invalid node number: %d", len(payload.Nodes))},
	}
	if response, ok := errMap[err]; ok {
		c.JSON(response.statusCode, gin.H{
			"error": response.errorMsg,
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

func (proxy *HTTPBrokerProxy) handleAddNodes(c *gin.Context) {
	clusterName := c.Param("clusterName")
	err := proxy.maniBroker.AddNodesToCluster(proxy.ctx, clusterName)
	errMap := map[error]httpResponse{
		broker.ErrNoAvailableResource: httpResponse{statusCode: 409, errorMsg: fmt.Sprintf("no available resource: %s", err)},
		broker.ErrClusterNotFound:     httpResponse{statusCode: 404, errorMsg: fmt.Sprintf("cluster %s not found", clusterName)},
		broker.ErrEmptyChunksExist:    httpResponse{statusCode: 400, errorMsg: fmt.Sprintf("empty chunks exist")},
	}
	if response, ok := errMap[err]; ok {
		c.JSON(response.statusCode, gin.H{
			"error": response.errorMsg,
		})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("failed to add nodes to cluster %s: %s", clusterName, err),
		})
		return
	}
	c.String(200, "")
}

func (proxy *HTTPBrokerProxy) handleMigrateSlots(c *gin.Context) {
	clusterName := c.Param("clusterName")
	err := proxy.maniBroker.MigrateSlots(proxy.ctx, clusterName)
	errMap := map[error]httpResponse{
		broker.ErrClusterNotFound:  httpResponse{statusCode: 404, errorMsg: fmt.Sprintf("cluster %s not found", clusterName)},
		broker.ErrCanNotMigrate:    httpResponse{statusCode: 400, errorMsg: "cannot migrate"},
		broker.ErrAlreadyMigrating: httpResponse{statusCode: 400, errorMsg: "already migrating"},
		broker.ErrNoAvailableNodes: httpResponse{statusCode: 400, errorMsg: "no available nodes with empty slots found"},
	}
	if response, ok := errMap[err]; ok {
		c.JSON(response.statusCode, gin.H{
			"error": response.errorMsg,
		})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s failed to migrate slots: %s", clusterName, err),
		})
		return
	}
	c.String(200, "")
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
	errMap := map[error]httpResponse{
		broker.ErrClusterNotFound:                    httpResponse{statusCode: 404, errorMsg: fmt.Sprintf("cluster %s not found", payload.DBName)},
		broker.ErrMigrationTaskNotFound:              httpResponse{statusCode: 404, errorMsg: fmt.Sprintf("task %+v not found", payload)},
		broker.ErrInvalidRequestedMigrationSlotRange: httpResponse{statusCode: 400, errorMsg: fmt.Sprintf("invalid migration task %+v", payload)},
		broker.ErrMigrationTaskNotMatch:              httpResponse{statusCode: 400, errorMsg: "migration task not match"},
	}
	if response, ok := errMap[err]; ok {
		c.JSON(response.statusCode, gin.H{
			"error": response.errorMsg,
		})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("%s failed to migrate slots: %+v", payload.DBName, err),
		})
		return
	}
	c.String(200, "")
}

func (proxy *HTTPBrokerProxy) handleRemoveUnusedProxiesFromCluster(c *gin.Context) {
	clusterName := c.Param("clusterName")
	err := proxy.maniBroker.RemoveUnusedProxiesFromCluster(proxy.ctx, clusterName)
	errMap := map[error]httpResponse{
		broker.ErrClusterNotFound: httpResponse{statusCode: 404, errorMsg: fmt.Sprintf("cluster %s not found", clusterName)},
		broker.ErrProxyInUse:      httpResponse{statusCode: 400, errorMsg: "proxy is in use"},
	}
	if response, ok := errMap[err]; ok {
		c.JSON(response.statusCode, gin.H{
			"error": response.errorMsg,
		})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("failed to free proxies from cluster %s: %+v", clusterName, err),
		})
		return
	}
	c.String(200, "")
}

func (proxy *HTTPBrokerProxy) handleRemoveProxy(c *gin.Context) {
	proxyAddress := c.Param("proxyAddress")
	err := proxy.maniBroker.RemoveProxy(proxy.ctx, proxyAddress)
	errMap := map[error]httpResponse{
		broker.ErrProxyInUse: httpResponse{statusCode: 400, errorMsg: "proxies are in use"},
	}
	if response, ok := errMap[err]; ok {
		c.JSON(response.statusCode, gin.H{
			"error": response.errorMsg,
		})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("failed to remove proxy %s: %+v", proxyAddress, err),
		})
		return
	}
	c.String(200, "")
}

func (proxy *HTTPBrokerProxy) handleRemoveCluster(c *gin.Context) {
	clusterName := c.Param("clusterName")
	err := proxy.maniBroker.RemoveCluster(proxy.ctx, clusterName)
	errMap := map[error]httpResponse{
		broker.ErrClusterNotFound: httpResponse{statusCode: 404, errorMsg: fmt.Sprintf("cluster %s not found", clusterName)},
	}
	if response, ok := errMap[err]; ok {
		c.JSON(response.statusCode, gin.H{
			"error": response.errorMsg,
		})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("failed to remove cluster %s: %+v", clusterName, err),
		})
		return
	}
	c.String(200, "")
}

func (proxy *HTTPBrokerProxy) handlerGetVersion(c *gin.Context) {
	c.String(200, OvermoonVersion)
}

func (proxy *HTTPBrokerProxy) handleSetClusterConfig(c *gin.Context) {
	clusterName := c.Param("clusterName")

	config := make(map[string]string, 0)
	err := c.BindJSON(&config)
	if err != nil {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("failed to get json payload %s", err),
		})
		return
	}

	err = proxy.maniBroker.SetConfig(proxy.ctx, clusterName, config)
	if err == broker.ErrInvalidClusterConfig {
		c.JSON(400, gin.H{
			"error": fmt.Sprintf("invalid config: %+v", err),
		})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{
			"error": fmt.Sprintf("failed to change cluster config %s: %+v", clusterName, err),
		})
		return
	}
	c.String(200, "")
}
