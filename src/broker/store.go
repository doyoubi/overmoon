package broker

import (
	"encoding/json"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var errMissingField = errors.New("missing field")

// ProxyStore stores the basic proxy metadata
type ProxyStore struct {
	ProxyIndex    uint64   `json:"proxy_index"`
	ClusterName   string   `json:"cluster_name"`
	NodeAddresses []string `json:"node_addresses"`
}

// Encode encodes json string
func (meta *ProxyStore) Encode() ([]byte, error) {
	data, err := json.Marshal(meta)
	return data, errors.WithStack(err)
}

// Decode decodes json string
func (meta *ProxyStore) Decode(data []byte) error {
	err := json.Unmarshal(data, meta)
	if err != nil {
		return errors.WithStack(err)
	}

	// For simplicity, just ignore the case that meta.ProxyIndex == 0
	if meta.NodeAddresses == nil {
		return errors.WithStack(errMissingField)
	}
	if meta.ClusterName == "" && meta.ProxyIndex > 0 {
		return errors.WithStack(errMissingField)
	}
	return nil
}

// FailedProxyStore stores
type FailedProxyStore struct {
	NodeAddresses []string `json:"node_addresses"`
}

// Encode encodes json string
func (meta *FailedProxyStore) Encode() ([]byte, error) {
	data, err := json.Marshal(meta)
	return data, errors.WithStack(err)
}

// Decode decodes json string
func (meta *FailedProxyStore) Decode(data []byte) error {
	err := json.Unmarshal(data, meta)
	if err != nil {
		return errors.WithStack(err)
	}

	if meta.NodeAddresses == nil {
		return errors.WithStack(errMissingField)
	}
	return nil
}

// NodeStore stores the metadata of node
type NodeStore struct {
	NodeAddress  string           `json:"node_address"`
	ProxyAddress string           `json:"proxy_address"`
	Slots        []SlotRangeStore `json:"slots"`
}

// ClusterStore stores the nodes
type ClusterStore struct {
	Nodes []*NodeStore `json:"nodes"`
}

// Encode encodes json string
func (meta *ClusterStore) Encode() ([]byte, error) {
	data, err := json.Marshal(meta)
	return data, errors.WithStack(err)
}

// Decode decodes json string
func (meta *ClusterStore) Decode(data []byte) error {
	err := json.Unmarshal(data, meta)
	if err != nil {
		log.Errorf("invalid cluster data '%v'", string(data))
		return errors.WithStack(err)
	}

	for _, node := range meta.Nodes {
		if node.NodeAddress == "" || node.ProxyAddress == "" {
			return errors.WithStack(errMissingField)
		}
		for _, slot := range node.Slots {
			if slot.Tag.TagType == "" {
				return errors.WithStack(errMissingField)
			}
			if slot.Tag.TagType != NoneTag && slot.Tag.Meta == nil {
				return errors.WithStack(errMissingField)
			}
		}
	}

	return nil
}

// SlotRangeStore stores the slot range
type SlotRangeStore struct {
	Start uint64            `json:"start"`
	End   uint64            `json:"end"`
	Tag   SlotRangeTagStore `json:"tag"`
}

// SlotRangeTagStore stores the tag and migration meta
type SlotRangeTagStore struct {
	TagType MigrationTagType    `json:"tag_type"`
	Meta    *MigrationMetaStore `json:"meta"`
}

// MigrationMetaStore stores the migration meta
type MigrationMetaStore struct {
	Epoch         uint64 `json:"epoch"`
	SrcProxyIndex uint64 `json:"src_proxy_index"`
	DstProxyIndex uint64 `json:"dst_proxy_index"`
}
