package broker

type MetaDataBroker interface {
	GetClusterNames() ([]string, error)
	GetCluster(name string) (*Cluster, error)
	GetHostAddresses() ([]string, error)
	GetHost(address string) (*Host, error)
	AddFailure(address string, reportID string) error
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
