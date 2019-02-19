package broker

import "context"

type MetaDataBroker interface {
	GetClusterNames(ctx context.Context) ([]string, error)
	GetCluster(ctx context.Context, name string) (*Cluster, error)
	GetHostAddresses(ctx context.Context) ([]string, error)
	GetHost(ctx context.Context, address string) (*Host, error)
	AddFailure(ctx context.Context, address string, reportID string) error
}

type SlotRange struct {
	Start int
	End   int
}

type Node struct {
	Address     string
	ClusterName string
	Slots       []SlotRange
}

type Cluster struct {
	Name  string
	Epoch int64
	Nodes []*Node
}

type Host struct {
	Address string
	Epoch   int64
	Nodes   []*Node
}
