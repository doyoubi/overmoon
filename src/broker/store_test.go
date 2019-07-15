package broker

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func genTestingClusterStore(nodeNum int) *ClusterStore {
	chunks := make([]*NodeChunkStore, 0)
	nodes := [4]*NodeStore{}
	for i := 0; i != nodeNum; i++ {
		nodeAddress := fmt.Sprintf("node_address:%d", i)
		proxyAddress := fmt.Sprintf("node_address:%d", i/2)
		nodes[i%4] = &NodeStore{
			NodeAddress:  nodeAddress,
			ProxyAddress: proxyAddress,
		}
		if (i+1)%4 == 0 {
			var slots [2][]SlotRangeStore
			if i < nodeNum/2 {
				slots = [2][]SlotRangeStore{
					[]SlotRangeStore{
						SlotRangeStore{
							Start: uint64(4 * i),
							End:   uint64(4 * i),
							Tag: SlotRangeTagStore{
								TagType: NoneTag,
								Meta:    nil,
							},
						},
						SlotRangeStore{
							Start: uint64(4*i + 1),
							End:   uint64(4*i + 1),
							Tag: SlotRangeTagStore{
								TagType: MigratingTag,
								Meta: &MigrationMetaStore{
									Epoch:         0,
									SrcProxyIndex: uint64((i - 3) / 2),
									DstProxyIndex: uint64((i-3)/2 + nodeNum/4),
								},
							},
						},
					},
					[]SlotRangeStore{
						SlotRangeStore{
							Start: uint64(4*i + 2),
							End:   uint64(4*i + 2),
							Tag: SlotRangeTagStore{
								TagType: NoneTag,
								Meta:    nil,
							},
						},
						SlotRangeStore{
							Start: uint64(4*i + 3),
							End:   uint64(4*i + 3),
							Tag: SlotRangeTagStore{
								TagType: MigratingTag,
								Meta: &MigrationMetaStore{
									Epoch:         0,
									SrcProxyIndex: uint64((i-3)/2 + 1),
									DstProxyIndex: uint64((i-3)/2 + 1 + nodeNum/4),
								},
							},
						},
					},
				}
			} else {
				slots = [2][]SlotRangeStore{
					[]SlotRangeStore{
						SlotRangeStore{
							Start: uint64(4*i + 1),
							End:   uint64(4*i + 1),
							Tag: SlotRangeTagStore{
								TagType: ImportingTag,
								Meta: &MigrationMetaStore{
									Epoch:         0,
									SrcProxyIndex: uint64((i-3)/2 - nodeNum/4),
									DstProxyIndex: uint64((i - 3) / 2),
								},
							},
						},
					},
					[]SlotRangeStore{
						SlotRangeStore{
							Start: uint64(4*i + 3),
							End:   uint64(4*i + 3),
							Tag: SlotRangeTagStore{
								TagType: ImportingTag,
								Meta: &MigrationMetaStore{
									Epoch:         0,
									SrcProxyIndex: uint64(i/2 - nodeNum/4),
									DstProxyIndex: uint64((i-3)/2 + 1),
								},
							},
						},
					},
				}
			}
			chunks = append(chunks, &NodeChunkStore{
				RolePosition: ChunkRoleNormalPosition,
				Nodes:        nodes,
				Slots:        slots,
			})
			nodes = [4]*NodeStore{}
		}
	}
	return &ClusterStore{
		Chunks: chunks,
	}
}

func TestLimitMigration(t *testing.T) {
	assert := assert.New(t)

	for i := 1; i != 100; i++ {
		cluster := genTestingClusterStore(i * 8)
		newCluster, err := cluster.LimitMigration(1)
		assert.NoError(err)

		chunks := newCluster.Chunks

		assert.Equal(2*i, len(chunks))
		for j, chunk := range chunks {
			if j < len(chunks)/2 {
				assert.Equal(2, len(chunk.Slots[0]))
				assert.Equal(2, len(chunk.Slots[1]))

				if j == 0 {
					assert.Equal(NoneTag, chunk.Slots[0][0].Tag.TagType)
					assert.Equal(MigratingTag, chunk.Slots[0][1].Tag.TagType)
				} else {
					assert.Equal(NoneTag, chunk.Slots[1][0].Tag.TagType)
					assert.Equal(NoneTag, chunk.Slots[1][1].Tag.TagType)
				}
			} else {
				if j == len(chunks)/2 {
					assert.Equal(1, len(chunk.Slots[0]))
					assert.Equal(0, len(chunk.Slots[1]))
				} else {
					assert.Equal(0, len(chunk.Slots[0]))
					assert.Equal(0, len(chunk.Slots[1]))
				}
			}
		}

	}
}
