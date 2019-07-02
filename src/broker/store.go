package broker

import (
	"encoding/json"
	"errors"
)

var errMissingField = errors.New("missing field")

type proxyMeta struct {
	ProxyIndex    uint64   `json:"proxy_index"`
	ClusterName   string   `json:"cluster_name"`
	NodeAddresses []string `json:"node_addresses"`
}

func (meta *proxyMeta) encode() ([]byte, error) {
	return json.Marshal(meta)
}

func (meta *proxyMeta) decode(data []byte) error {
	err := json.Unmarshal(data, meta)
	if err != nil {
		return err
	}

	// For simplicity, just ignore the case that meta.ProxyIndex == 0
	if meta.ClusterName == "" || meta.NodeAddresses == nil {
		return errMissingField
	}
	return nil
}

type failedProxyMeta struct {
	NodeAddresses []string `json:"node_addresses"`
}

func (meta *failedProxyMeta) encode() ([]byte, error) {
	return json.Marshal(meta)
}

func (meta *failedProxyMeta) decode(data []byte) error {
	err := json.Unmarshal(data, meta)
	if err != nil {
		return err
	}

	if meta.NodeAddresses == nil {
		return errMissingField
	}
	return nil
}

type nodeMeta struct {
	NodeAddress  string          `json:"node_address"`
	ProxyAddress string          `json:"proxy_address"`
	Slots        []slotRangeMeta `json:"slots"`
}

type clusterMeta struct {
	Nodes []*nodeMeta `json:"nodes"`
}

func (meta *clusterMeta) encode() ([]byte, error) {
	return json.Marshal(meta)
}

func (meta *clusterMeta) decode(data []byte) error {
	err := json.Unmarshal(data, meta)
	if err != nil {
		return err
	}

	for _, node := range meta.Nodes {
		if node.NodeAddress == "" || node.ProxyAddress == "" {
			return errMissingField
		}
		for _, slot := range node.Slots {
			if slot.Tag.TagType == "" {
				return errMissingField
			}
			if slot.Tag.TagType != NoneTag && slot.Tag.Meta == nil {
				return errMissingField
			}
		}
	}

	return nil
}

type slotRangeMeta struct {
	Start uint64           `json:"start"`
	End   uint64           `json:"end"`
	Tag   slotRangeTagMeta `json:"tag"`
}

type slotRangeTagMeta struct {
	TagType MigrationTagType `json:"tag_type"`
	Meta    *migrationMeta   `json:"meta"`
}

type migrationMeta struct {
	Epoch         uint64 `json:"epoch"`
	SrcProxyIndex uint64 `json:"src_proxy_index"`
	DstProxyIndex uint64 `json:"dst_proxy_index"`
}