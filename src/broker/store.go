package broker

import (
	"encoding/json"
	"fmt"

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
func (store *ProxyStore) Encode() ([]byte, error) {
	data, err := json.Marshal(store)
	return data, errors.WithStack(err)
}

// Decode decodes json string
func (store *ProxyStore) Decode(data []byte) error {
	err := json.Unmarshal(data, store)
	if err != nil {
		return errors.WithStack(err)
	}

	// For simplicity, just ignore the case that meta.ProxyIndex == 0
	if store.NodeAddresses == nil {
		return errors.WithStack(errMissingField)
	}
	if store.ClusterName == "" && store.ProxyIndex > 0 {
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
	NodeAddress  string `json:"node_address"`
	ProxyAddress string `json:"proxy_address"`
}

// ClusterStore stores the nodes
type ClusterStore struct {
	Chunks []*NodeChunk `json:"chunks"`
}

// ChunkRolePosition indicates the roles in the chunk
type ChunkRolePosition int

const (
	// ChunkRoleNormalPosition indicates each proxy has one master.
	ChunkRoleNormalPosition ChunkRolePosition = 0
	// ChunkRoleFirstChunkMaster indicates all the masters are in the first proxy.
	ChunkRoleFirstChunkMaster ChunkRolePosition = 1
	// ChunkRoleSecondChunkMaster indicates all the masters are in the second proxy.
	ChunkRoleSecondChunkMaster ChunkRolePosition = 2
)

// NodeChunk stores 4 nodes as a group
type NodeChunk struct {
	RolePosition ChunkRolePosition
	Slots        [][]SlotRangeStore `json:"slots"`
	Nodes        []*NodeStore       `json:"nodes"`
}

// Encode encodes json string
func (store *ClusterStore) Encode() ([]byte, error) {
	data, err := json.Marshal(store)
	return data, errors.WithStack(err)
}

// Decode decodes json string
func (store *ClusterStore) Decode(data []byte) error {
	err := json.Unmarshal(data, store)
	if err != nil {
		log.Errorf("invalid cluster data '%v'", string(data))
		return errors.WithStack(err)
	}

	for _, chunk := range store.Chunks {
		for _, node := range chunk.Nodes {
			if node.NodeAddress == "" || node.ProxyAddress == "" {
				return errors.WithStack(errMissingField)
			}
		}
		for _, nodeSlots := range chunk.Slots {
			for _, slot := range nodeSlots {
				if slot.Tag.TagType == "" {
					return errors.WithStack(errMissingField)
				}
				if slot.Tag.TagType != NoneTag && slot.Tag.Meta == nil {
					return errors.WithStack(errMissingField)
				}
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

func parseNodes(clusterData []byte) ([]*Node, error) {
	cluster := &ClusterStore{}
	err := cluster.Decode(clusterData)
	if err != nil {
		return nil, err
	}

	nodes := make([]*Node, 0, len(cluster.Chunks)*chunkSize)
	slots := make([][]SlotRangeStore, 0, len(cluster.Chunks)/2)
	chunkRoles := make([]ChunkRolePosition, 0, len(cluster.Chunks))
	for _, chunk := range cluster.Chunks {
		if len(chunk.Nodes) != chunkSize {
			err = fmt.Errorf("invalid nodes of chunk %v", chunk.Nodes)
			return nil, errors.WithStack(err)
		}

		for _, etcdNodeMeta := range chunk.Nodes {
			node := &Node{
				Address:      etcdNodeMeta.NodeAddress,
				ProxyAddress: etcdNodeMeta.ProxyAddress,
				ClusterName:  "",         // initialized later
				Slots:        nil,        // initialized later
				Repl:         ReplMeta{}, // initialized later
			}
			nodes = append(nodes, node)
		}
		err = setRepl(chunk.RolePosition, nodes[len(nodes)-chunkSize:len(nodes)])
		if err != nil {
			return nil, err
		}
		if len(chunk.Slots) != chunkSize/halfChunkSize {
			err = fmt.Errorf("invalid slots of chunk %v", chunk.Slots)
			return nil, errors.WithStack(err)
		}
		slots = append(slots, chunk.Slots...)
		chunkRoles = append(chunkRoles, chunk.RolePosition)
	}

	err = setSlots(nodes, chunkRoles, slots)
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

func setRepl(rolePosition ChunkRolePosition, chunkNodes []*Node) error {
	for nodeIndex, node := range chunkNodes {
		role := MasterRole
		switch rolePosition {
		case ChunkRoleNormalPosition:
			if nodeIndex%2 == 1 {
				role = ReplicaRole
			}
		case ChunkRoleFirstChunkMaster:
			if nodeIndex >= halfChunkSize {
				role = ReplicaRole
			}
		case ChunkRoleSecondChunkMaster:
			if nodeIndex < halfChunkSize {
				role = ReplicaRole
			}
		}

		peerIndex := getPeerIndexInChunk(uint64(nodeIndex))
		if peerIndex < 0 || peerIndex >= uint64(len(chunkNodes)) {
			err := fmt.Errorf("invalid node index when finding peer %d", peerIndex)
			return errors.WithStack(err)
		}
		peer := chunkNodes[peerIndex]
		node.Repl = ReplMeta{
			Role: role,
			Peers: []ReplPeer{ReplPeer{
				NodeAddress:  peer.Address,
				ProxyAddress: peer.ProxyAddress,
			}},
		}
	}
	return nil
}

func getPeerIndexInChunk(indexInChunk uint64) uint64 {
	switch indexInChunk {
	case 0:
		return indexInChunk + 3
	case 1:
		return indexInChunk + 1
	case 2:
		return indexInChunk - 1
	case 3:
		return indexInChunk - 3
	}
	log.Errorf("invalid index %d", indexInChunk)
	return 0
}

func getRealPosition(rolePosition ChunkRolePosition, normalMasterIndex, chunkIndex uint64) (masterIndex uint64, replicaIndex uint64) {
	m, r := getRealPositionInChunk(rolePosition, normalMasterIndex%4)
	return chunkIndex*chunkSize + m, chunkIndex*chunkSize + r
}

func getRealPositionInChunk(rolePosition ChunkRolePosition, normalMasterIndexInChunk uint64) (masterIndexInChunk uint64, replicaIndexInChunk uint64) {
	if normalMasterIndexInChunk == 0 && rolePosition == ChunkRoleSecondChunkMaster {
		return getPeerIndexInChunk(normalMasterIndexInChunk), normalMasterIndexInChunk
	}
	if normalMasterIndexInChunk == 2 && rolePosition == ChunkRoleFirstChunkMaster {
		return getPeerIndexInChunk(normalMasterIndexInChunk), normalMasterIndexInChunk
	}
	return normalMasterIndexInChunk, getPeerIndexInChunk(normalMasterIndexInChunk)
}

func setSlots(nodes []*Node, chunkRoles []ChunkRolePosition, slots [][]SlotRangeStore) error {
	if len(slots)*halfChunkSize != len(nodes) || len(chunkRoles)*chunkSize != len(nodes) {
		err := fmt.Errorf("mismatch slots or nodes number, nodes: %d, chunks %d, slots: %d", len(nodes), len(chunkRoles), len(slots))
		return errors.WithStack(err)
	}

	for i, nodeSlots := range slots {
		slotRanges := make([]SlotRange, 0, len(nodeSlots))
		for _, slotRange := range nodeSlots {
			if slotRange.Tag.TagType == NoneTag {
				slotRanges = append(slotRanges, SlotRange{
					Start: slotRange.Start,
					End:   slotRange.End,
					Tag:   SlotRangeTag{TagType: slotRange.Tag.TagType},
				})
				continue
			}

			srcMasterIndex := slotRange.Tag.Meta.SrcProxyIndex * halfChunkSize
			dstMasterIndex := slotRange.Tag.Meta.DstProxyIndex * halfChunkSize
			srcChunkIndex := srcMasterIndex / chunkSize
			dstChunkIndex := dstMasterIndex / chunkSize
			srcMasterIndex, _ = getRealPosition(chunkRoles[srcChunkIndex], srcMasterIndex, srcChunkIndex)
			dstMasterIndex, _ = getRealPosition(chunkRoles[dstChunkIndex], dstMasterIndex, dstChunkIndex)

			slotRanges = append(slotRanges, SlotRange{
				Start: slotRange.Start,
				End:   slotRange.End,
				Tag: SlotRangeTag{
					TagType: slotRange.Tag.TagType,
					Meta: &MigrationMeta{
						Epoch:           slotRange.Tag.Meta.Epoch,
						SrcNodeAddress:  nodes[srcMasterIndex].Address,
						SrcProxyAddress: nodes[srcMasterIndex].ProxyAddress,
						DstNodeAddress:  nodes[dstMasterIndex].Address,
						DstProxyAddress: nodes[dstMasterIndex].ProxyAddress,
					},
				},
			})
		}

		chunkIndex := uint64(i / 2)
		masterIndex, replicaIndex := getRealPosition(chunkRoles[chunkIndex], uint64(2*i), chunkIndex)
		nodes[masterIndex].Slots = slotRanges
		nodes[replicaIndex].Slots = []SlotRange{}
	}
	return nil
}
