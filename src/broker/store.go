package broker

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var errMissingField = errors.New("missing field")

// ErrCanNotMigrate indicates that the cluster does not
// have enough empty chunks to do the migration.
var ErrCanNotMigrate = errors.New("cluster cannot migrate")

// ErrAlreadyMigrating indicates the cluster has already
// started migration.
var ErrAlreadyMigrating = errors.New("cluster is migrating")

// ProxyStore stores the basic proxy metadata
type ProxyStore struct {
	ProxyIndex    uint64   `json:"proxy_index"`
	ClusterName   string   `json:"cluster_name"`
	NodeAddresses []string `json:"node_addresses"`
}

// Encode encodes json string
func (proxy *ProxyStore) Encode() ([]byte, error) {
	data, err := json.Marshal(proxy)
	return data, errors.WithStack(err)
}

// Decode decodes json string
func (proxy *ProxyStore) Decode(data []byte) error {
	err := json.Unmarshal(data, proxy)
	if err != nil {
		return errors.WithStack(err)
	}

	// For simplicity, just ignore the case that meta.ProxyIndex == 0
	if proxy.NodeAddresses == nil {
		return errors.WithStack(errMissingField)
	}
	if proxy.ClusterName == "" && proxy.ProxyIndex > 0 {
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
	Chunks []*NodeChunkStore `json:"chunks"`
}

// FindChunkByProxy find chunk by proxy address.
func (cluster *ClusterStore) FindChunkByProxy(proxyAddress string) (*NodeChunkStore, error) {
	for _, chunk := range cluster.Chunks {
		if len(chunk.Nodes) != chunkSize {
			err := fmt.Errorf("invalid chunk size: %+v", chunk.Nodes)
			return nil, errors.WithStack(err)
		}
		if chunk.Nodes[0].ProxyAddress == proxyAddress || chunk.Nodes[halfChunkSize].ProxyAddress == proxyAddress {
			return chunk, nil
		}
	}
	return nil, ErrProxyNotFound
}

// SplitSlots splits the slots from the first half to the second half.
func (cluster *ClusterStore) SplitSlots(newEpoch uint64) error {
	if len(cluster.Chunks) < 2 {
		log.Info("Cannot migrate slots, number of chunks is not even.")
		return ErrCanNotMigrate
	}
	if len(cluster.Chunks)%2 != 0 {
		return errors.WithStack(fmt.Errorf("invalid chunk number %d", len(cluster.Chunks)))
	}
	halfChunkNum := uint64(len(cluster.Chunks) / 2)

	for i, srcChunk := range cluster.Chunks {
		srcChunkIndex := uint64(i)
		dstChunkIndex := srcChunkIndex + halfChunkNum
		if srcChunkIndex >= halfChunkNum {
			break
		}

		if len(srcChunk.Slots[0]) == 0 || len(srcChunk.Slots[1]) == 0 {
			log.Info("Cannot migrate slots, slot of source chunks is empty")
			return ErrCanNotMigrate
		}
		// For simplicity, we assume they just have only one slot range.
		if len(srcChunk.Slots[0]) != 1 || len(srcChunk.Slots[1]) != 1 {
			log.Info("Cannot migrate slots, number of slot ranges is not 1")
			return ErrCanNotMigrate
		}
		if srcChunk.Slots[0][0].Tag.TagType != NoneTag || srcChunk.Slots[1][0].Tag.TagType != NoneTag {
			return ErrAlreadyMigrating
		}

		dstChunk := cluster.Chunks[dstChunkIndex]
		log.Infof("srcChunk %+v, dstChunk %+v", srcChunk, dstChunk)

		if len(dstChunk.Slots[0]) > 0 || len(dstChunk.Slots[1]) > 0 {
			log.Info("Cannot migrate slots, slot of destination chunks is not empty")
			return ErrCanNotMigrate
		}

		for j := 0; j != 2; j++ {
			start := srcChunk.Slots[j][0].Start
			end := srcChunk.Slots[j][0].End
			if start == end {
				log.Infof("Cannot migrate slots, cluster reaches the maximum size.")
				return ErrCanNotMigrate
			}
			m := (start + end) / 2
			// we can prove that: start <= m < end and m + 1 <= end
			srcChunk.Slots[j][0].End = m
			srcChunk.Slots[j][0].Tag = SlotRangeTagStore{
				TagType: MigratingTag,
				Meta: &MigrationMetaStore{
					Epoch:         newEpoch,
					SrcProxyIndex: srcChunkIndex * halfChunkSize,
					DstProxyIndex: dstChunkIndex * halfChunkSize,
				},
			}

			dstChunk.Slots[j] = []SlotRangeStore{SlotRangeStore{
				Start: m + 1,
				End:   end,
				Tag: SlotRangeTagStore{
					TagType: ImportingTag,
					Meta: &MigrationMetaStore{
						Epoch:         newEpoch,
						SrcProxyIndex: srcChunkIndex * halfChunkSize,
						DstProxyIndex: dstChunkIndex * halfChunkSize,
					},
				},
			}}
		}
	}
	return nil
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

// NodeChunkStore stores 4 nodes as a group
type NodeChunkStore struct {
	RolePosition ChunkRolePosition
	Slots        [2][]SlotRangeStore `json:"slots"`
	Nodes        [4]*NodeStore       `json:"nodes"`
}

// SwitchMaster takes over the master role
func (chunk *NodeChunkStore) SwitchMaster(failedProxyAddress string) error {
	if chunk.Nodes[0].ProxyAddress == failedProxyAddress {
		chunk.RolePosition = ChunkRoleSecondChunkMaster
	} else if chunk.Nodes[halfChunkSize].ProxyAddress == failedProxyAddress {
		chunk.RolePosition = ChunkRoleFirstChunkMaster
	} else {
		return ErrProxyNotFound
	}
	return nil
}

// Encode encodes json string
func (cluster *ClusterStore) Encode() ([]byte, error) {
	data, err := json.Marshal(cluster)
	return data, errors.WithStack(err)
}

// Decode decodes json string
func (cluster *ClusterStore) Decode(data []byte) error {
	err := json.Unmarshal(data, cluster)
	if err != nil {
		log.Errorf("invalid cluster data '%v'", string(data))
		return errors.WithStack(err)
	}

	for _, chunk := range cluster.Chunks {
		if len(chunk.Nodes) != chunkSize {
			err := fmt.Errorf("invalid nodes size: %+v", chunk.Nodes)
			return errors.WithStack(err)
		}
		for _, node := range chunk.Nodes {
			if node.NodeAddress == "" || node.ProxyAddress == "" {
				return errors.WithStack(errMissingField)
			}
		}
		if len(chunk.Slots) != halfChunkSize {
			err := fmt.Errorf("invalid slots size: %+v", chunk.Nodes)
			return errors.WithStack(err)
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
		slots = append(slots, chunk.Slots[:]...)
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
