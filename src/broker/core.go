package broker

import (
	"context"
	"encoding/json"
	"errors"
)

// MetaDataBroker abstracts the ability to check meta data and detect failures.
type MetaDataBroker interface {
	GetClusterNames(ctx context.Context) ([]string, error)
	GetCluster(ctx context.Context, name string) (*Cluster, error)
	GetProxyAddresses(ctx context.Context) ([]string, error)
	GetProxy(ctx context.Context, address string) (*Host, error)
	AddFailure(ctx context.Context, address string, reportID string) error
	GetFailures(ctx context.Context) ([]string, error)
}

// MetaManipulationBroker abstracts the ability to manipulate clusters.
type MetaManipulationBroker interface {
	// Basic API
	ReplaceProxy(ctx context.Context, proxyAddress string) (*Host, error)
	CommitMigration(ctx context.Context, task MigrationTaskMeta) error

	// Extended API
	InitGlobalEpoch() error
	CreateCluster(ctx context.Context, clusterName string, nodeNum uint64) error
	AddProxy(ctx context.Context, address string, nodes []string) error
	AddNodesToCluster(ctx context.Context, clusterName string) error
	MigrateSlots(ctx context.Context, clusterName string) error
	RemoveProxy(ctx context.Context, address string) error
	RemoveUnusedProxiesFromCluster(ctx context.Context, clusterName string) error
	RemoveCluster(ctx context.Context, clusterName string) error
	SetConfig(ctx context.Context, clusterName string, config map[string]string) error
}

// SlotRange is the slot range of redis cluster. Start and End will be the same the single slot.
type SlotRange struct {
	Start uint64       `json:"start"`
	End   uint64       `json:"end"`
	Tag   SlotRangeTag `json:"tag"`
}

// SlotRangeTag includes the migration type and migration metadata
type SlotRangeTag struct {
	TagType MigrationTagType
	Meta    *MigrationMeta
}

// MigrationTagType consists of "Migrating", "Importing", and "None"
type MigrationTagType string

// MigrationMeta includes the migration metadata
type MigrationMeta struct {
	Epoch           uint64 `json:"epoch"`
	SrcProxyAddress string `json:"src_proxy_address"`
	SrcNodeAddress  string `json:"src_node_address"`
	DstProxyAddress string `json:"dst_proxy_address"`
	DstNodeAddress  string `json:"dst_node_address"`
}

// MigrationTaskMeta denotes the migration task.
type MigrationTaskMeta struct {
	DBName string    `json:"db_name"`
	Slots  SlotRange `json:"slot_range"`
}

// Role could be 'master' or 'replica'.
type Role = string

// MasterRole represents master node.
const MasterRole = "master"

// ReplicaRole represents replcia node.
const ReplicaRole = "replica"

// ReplPeer stores the replication peer
type ReplPeer struct {
	NodeAddress  string `json:"node_address"`
	ProxyAddress string `json:"proxy_address"`
}

// ReplMeta stores the replication metadata
type ReplMeta struct {
	Role  Role       `json:"role"`
	Peers []ReplPeer `json:"peers"`
}

// Node is redis node.
type Node struct {
	Address      string      `json:"address"`
	ProxyAddress string      `json:"proxy_address"`
	ClusterName  string      `json:"cluster_name"`
	Slots        []SlotRange `json:"slots"`
	Repl         ReplMeta    `json:"repl"`
}

// Cluster is the redis cluster we implement.
type Cluster struct {
	Name   string        `json:"name"`
	Epoch  uint64        `json:"epoch"`
	Nodes  []*Node       `json:"nodes"`
	Config ClusterConfig `json:"config"`
}

// PeerProxy is used for server proxy to do the redirection.
type PeerProxy struct {
	ProxyAddress string      `json:"proxy_address"`
	ClusterName  string      `json:"cluster_name"`
	Slots        []SlotRange `json:"slots"`
}

// Host is the proxies on each physical machine.
type Host struct {
	Address        string                   `json:"address"`
	Epoch          uint64                   `json:"epoch"`
	Nodes          []*Node                  `json:"nodes"`
	FreeNodes      []string                 `json:"free_nodes"`
	Peers          []*PeerProxy             `json:"peers"`
	ClustersConfig map[string]ClusterConfig `json:"clusters_config"`
}

// MaxSlotNumber is specified by Redis Cluster
const MaxSlotNumber = 16384

const (
	// MigratingTag is for source node
	MigratingTag MigrationTagType = "Migrating"
	// ImportingTag is for destination node
	ImportingTag MigrationTagType = "Importing"
	// NoneTag is for stable slots
	NoneTag MigrationTagType = "None"
)

type migratingSlotRangeTag struct {
	Migrating *MigrationMeta `json:"Migrating"`
}

type importingSlotRangeTag struct {
	Importing *MigrationMeta `json:"Importing"`
}

const (
	// CompressionStrategyDisabled disables compression
	CompressionStrategyDisabled = "disabled"
	// CompressionStrategySetGetOnly enables compression and restrict the commands
	// to only SET family and GET for string data type.
	CompressionStrategySetGetOnly = "set_get_only"
	// CompressionStrategyAllowAll enables compression and allow all the commands,
	// even though they will get the wrong results.
	CompressionStrategyAllowAll = "allow_all"
)

// ClusterConfig is the config of each cluster.
type ClusterConfig struct {
	CompressionStrategy string `json:"compression_strategy"`
}

// NewClusterConfig creates ClusterConfig with default settings.
func NewClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		CompressionStrategy: CompressionStrategyDisabled,
	}
}

// MarshalJSON changes the json format of SlotRangeTag to the Rust Serde format.
func (slotRangeTag *SlotRangeTag) MarshalJSON() ([]byte, error) {
	switch slotRangeTag.TagType {
	case MigratingTag:
		return json.Marshal(&migratingSlotRangeTag{
			Migrating: slotRangeTag.Meta,
		})
	case ImportingTag:
		return json.Marshal(&importingSlotRangeTag{
			Importing: slotRangeTag.Meta,
		})
	default:
		return []byte(`"None"`), nil
	}
}

var errInvalidDataFormat = errors.New("invalid data format")

// UnmarshalJSON changes the json format of SlotRangeTag to the Rust Serde format.
func (slotRangeTag *SlotRangeTag) UnmarshalJSON(data []byte) error {
	migrating := &migratingSlotRangeTag{}
	err := json.Unmarshal(data, migrating)

	if err == nil && migrating.Migrating != nil {
		slotRangeTag.TagType = MigratingTag
		slotRangeTag.Meta = migrating.Migrating
		return nil
	}

	importing := &importingSlotRangeTag{}
	err = json.Unmarshal(data, importing)
	if err == nil && importing.Importing != nil {
		slotRangeTag.TagType = ImportingTag
		slotRangeTag.Meta = importing.Importing
		return nil
	}

	none := ""
	err = json.Unmarshal(data, &none)
	if err == nil && none == "None" {
		slotRangeTag.TagType = NoneTag
		slotRangeTag.Meta = nil
		return nil
	}

	return errInvalidDataFormat
}
