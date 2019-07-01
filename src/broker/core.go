package broker

import (
	"context"
	"errors"
)

var ErrNotExists = errors.New("Missing key")

// MetaDataBroker abstracts the ability to check meta data and detect failures.
type MetaDataBroker interface {
	GetClusterNames(ctx context.Context) ([]string, error)
	GetCluster(ctx context.Context, name string) (*Cluster, error)
	GetHostAddresses(ctx context.Context) ([]string, error)
	GetHost(ctx context.Context, address string) (*Host, error)
	AddFailure(ctx context.Context, address string, reportID string) error
	GetFailures(ctx context.Context) ([]string, error)
}

// MetaManipulationBroker abstracts the ability to manipulate clusters.
type MetaManipulationBroker interface {
	ReplaceNode(ctx context.Context, currClusterEpoch int64, node *Node) (*Node, error)
	CreateCluster(ctx context.Context, clusterName string, nodeNum, maxMaxmemory int64) error
	AddHost(ctx context.Context, address string, nodes []string) error
}

// SlotRange is the slot range of redis cluster. Start and End will be the same the single slot.
type SlotRange struct {
	Start int64  `json:"start"`
	End   int64  `json:"end"`
	Tag   string `json:"tag"`
}

// SlotRangeTag includes the migration type and migration metadata
type SlotRangeTag struct {
	TagType string
	Meta    MigrationMeta
}

// MigrationMeta includes the migration metadata
type MigrationMeta struct {
	Epoch           uint64 `json:"epoch"`
	SrcProxyAddress string `json:"src_proxy_address"`
	SrcNodeAddress  string `json:"src_node_address"`
	DstProxyAddress string `json:"dst_proxy_address"`
	DstNodeAddress  string `json:"dst_node_address"`
}

// Role could be 'master' or 'replica'.
type Role = string

// MasterRole represents master node.
const MasterRole = "master"

// ReplicaRole represents replcia node.
const ReplicaRole = "replica"

// Node is redis node.
type Node struct {
	Address      string      `json:"address"`
	ProxyAddress string      `json:"proxy_address"`
	ClusterName  string      `json:"cluster_name"`
	Slots        []SlotRange `json:"slots"`
	Role         Role        `json:"role"`
}

// Cluster is the redis cluster we implement.
type Cluster struct {
	Name  string  `json:"name"`
	Epoch int64   `json:"epoch"`
	Nodes []*Node `json:"nodes"`
}

// Host is the proxies on each physical machine.
type Host struct {
	Address string  `json:"address"`
	Epoch   int64   `json:"epoch"`
	Nodes   []*Node `json:"nodes"`
}

// MaxSlotNumber is specified by Redis Cluster
const MaxSlotNumber = 16384
