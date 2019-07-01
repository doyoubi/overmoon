package broker

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/kinbiko/jsonassert"
	"github.com/stretchr/testify/assert"
)

func TestEncodeSlotRangeTag(t *testing.T) {
	assert := assert.New(t)
	ja := jsonassert.New(t)

	slotRange := &SlotRangeTag{
		TagType: NoneTag,
		Meta:    nil,
	}
	output, err := json.Marshal(slotRange)
	assert.NoError(err)
	assert.NotNil(output)
	assert.Equal(`"None"`, string(output))

	for _, tag := range []MigrationTagType{MigratingTag, ImportingTag} {
		slotRangeTag := &SlotRangeTag{
			TagType: tag,
			Meta: &MigrationMeta{
				Epoch:           233,
				SrcProxyAddress: "127.0.0.1:1000",
				SrcNodeAddress:  "127.0.0.1:2000",
				DstProxyAddress: "127.0.0.1:3000",
				DstNodeAddress:  "127.0.0.1:4000",
			},
		}

		output, err := json.Marshal(slotRangeTag)
		assert.NoError(err)
		assert.NotNil(output)
		ja.Assertf(string(output), `{
			"%s": {
				"epoch": %d,
				"src_proxy_address": "%s",
				"src_node_address": "%s",
				"dst_proxy_address": "%s",
				"dst_node_address": "%s"
			}
		}`, tag, slotRangeTag.Meta.Epoch,
			slotRangeTag.Meta.SrcProxyAddress, slotRangeTag.Meta.SrcNodeAddress,
			slotRangeTag.Meta.DstProxyAddress, slotRangeTag.Meta.DstNodeAddress)
	}

}

func TestDecodeSlotRangeTag(t *testing.T) {
	assert := assert.New(t)

	srcProxyAddress := "127.0.0.1:1000"
	srcNodeAddress := "127.0.0.1:2000"
	dstProxyAddress := "127.0.0.1:3000"
	dstNodeAddress := "127.0.0.1:4000"

	input := `"None"`
	slotRangeTag := &SlotRangeTag{}
	err := json.Unmarshal([]byte(input), slotRangeTag)
	assert.NoError(err)
	assert.Equal(NoneTag, slotRangeTag.TagType)
	assert.Nil(slotRangeTag.Meta)

	for i, tag := range []MigrationTagType{MigratingTag, ImportingTag} {
		input := fmt.Sprintf(`{
			"%s": {
				"epoch": %d,
				"src_proxy_address": "%s",
				"src_node_address": "%s",
				"dst_proxy_address": "%s",
				"dst_node_address": "%s"
			}
		}`, tag, 233+i, srcProxyAddress, srcNodeAddress, dstProxyAddress, dstNodeAddress)

		slotRangeTag := &SlotRangeTag{}
		err := json.Unmarshal([]byte(input), slotRangeTag)
		assert.NoError(err)
		assert.Equal(tag, slotRangeTag.TagType)
		assert.Equal(uint64(233+i), slotRangeTag.Meta.Epoch)
		assert.Equal(srcProxyAddress, slotRangeTag.Meta.SrcProxyAddress)
		assert.Equal(srcNodeAddress, slotRangeTag.Meta.SrcNodeAddress)
		assert.Equal(dstProxyAddress, slotRangeTag.Meta.DstProxyAddress)
		assert.Equal(dstNodeAddress, slotRangeTag.Meta.DstNodeAddress)
	}
}
