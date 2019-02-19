package broker

import "context"

type MetaDataBroker interface {
	GetClusterNames(ctx context.Context) ([]string, error)
	GetCluster(ctx context.Context, name string) (*Cluster, error)
	GetHostAddresses(ctx context.Context) ([]string, error)
	GetHost(ctx context.Context, address string) (*Host, error)
	AddFailure(ctx context.Context, address string, reportID string) error
}

type Node struct {
	address     string
	clusterName string
}

type Cluster struct {
	name  string
	epoch int64
	nodes []*Node
}

type Host struct {
	address string
	epoch   int64
	nodes   []*Node
}
