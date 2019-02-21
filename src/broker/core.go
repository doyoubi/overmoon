package broker

import "context"

// MetaDataBroker abstracts the real broker behind this proxy.
type MetaDataBroker interface {
	GetClusterNames(ctx context.Context) ([]string, error)
	GetCluster(ctx context.Context, name string) (*Cluster, error)
	GetHostAddresses(ctx context.Context) ([]string, error)
	GetHost(ctx context.Context, address string) (*Host, error)
	AddFailure(ctx context.Context, address string, reportID string) error
	GetFailures(ctx context.Context) ([]string, error)
}

// SlotRange is the slot range of redis cluster. Start and End will be the same the single slot.
type SlotRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// Node is redis node.
type Node struct {
	Address     string      `json:"address"`
	ClusterName string      `json:"cluster_name"`
	Slots       []SlotRange `json:"slots"`
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
