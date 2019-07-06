package broker

import (
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestGetIP(t *testing.T) {
	assert := assert.New(t)

	ip, err := getIP("127.0.0.1:1000")
	assert.NoError(err)
	assert.Equal("127.0.0.1", ip)
}

func TestGenProxyMap(t *testing.T) {
	assert := assert.New(t)
	proxies := []string{
		"127.0.0.1:1000",
		"127.0.0.2:2000",
		"127.0.0.3:3000",
	}
	proxyMap := genProxyMap(proxies)
	assert.Equal(3, len(proxyMap))
}

func TestRemoveAdditionalProxies(t *testing.T) {
	assert := assert.New(t)
	proxyMap := map[string][]string{
		"127.0.0.1": []string{"127.0.0.1:1000"},
		"127.0.0.2": []string{"127.0.0.2:2000", "127.0.0.2:2001"},
		"127.0.0.3": []string{"127.0.0.3:3000"},
	}
	proxyMap = removeAdditionalProxies(proxyMap, 2)
	assert.Equal(2, getProxyMapSum(proxyMap))
}

func TestInitChunkTable(t *testing.T) {
	assert := assert.New(t)

	emptyChunks := make([]*NodeChunkStore, 0)

	proxies := []string{
		"127.0.0.1:1000",
		"127.0.0.2:2000",
		"127.0.0.3:3000",
	}
	_, _, _, err := initChunkTable(proxies, 4, emptyChunks, false)
	assert.Error(err)

	hostTable, linkTable, _, err := initChunkTable(proxies, 2, emptyChunks, false)
	assert.NoError(err)
	assert.Equal(2, len(hostTable))
	assert.Equal(1, len(hostTable[0]))
	assert.Equal(1, len(hostTable[1]))

	assert.Equal(len(hostTable), len(linkTable))
	for _, row := range linkTable {
		assert.Equal(len(hostTable), len(row))
		for _, value := range row {
			assert.Equal(0, value)
		}
	}

	proxies = []string{
		"127.0.0.1:1000",
		"127.0.0.2:2000",
		"127.0.0.2:2001",
		"127.0.0.3:3000",
	}
	hostTable, linkTable, _, err = initChunkTable(proxies, 2, emptyChunks, false)
	assert.NoError(err)
	assert.Equal(2, len(hostTable))
	assert.Equal(1, len(hostTable[0]))
	assert.Equal(1, len(hostTable[1]))

	assert.Equal(len(hostTable), len(linkTable))
	for _, row := range linkTable {
		assert.Equal(len(hostTable), len(row))
		for _, value := range row {
			assert.Equal(0, value)
		}
	}
}

func genExistingChunks() []*NodeChunkStore {
	return []*NodeChunkStore{
		&NodeChunkStore{
			Nodes: []*NodeStore{
				&NodeStore{
					NodeAddress:  "127.0.0.1:7001",
					ProxyAddress: "127.0.0.1:6001",
				},
				&NodeStore{
					NodeAddress:  "127.0.0.1:7002",
					ProxyAddress: "127.0.0.1:6001",
				},
				&NodeStore{
					NodeAddress:  "127.0.0.2:7003",
					ProxyAddress: "127.0.0.2:6002",
				},
				&NodeStore{
					NodeAddress:  "127.0.0.2:7004",
					ProxyAddress: "127.0.0.2:6002",
				},
			},
		},
	}
}

func TestInitChunkTableWithExistingChunks(t *testing.T) {
	assert := assert.New(t)

	existingChunks := genExistingChunks()

	proxies := []string{
		"127.0.0.1:1000",
		"127.0.0.2:2000",
		"127.0.0.3:3000",
		"127.0.0.4:4000",
	}
	_, _, _, err := initChunkTable(proxies, 6, existingChunks, false)
	assert.Error(err)

	hostTable, linkTable, _, err := initChunkTable(proxies, 4, existingChunks, false)
	assert.NoError(err)
	assert.Equal(4, len(hostTable))
	for _, proxies := range hostTable {
		if len(proxies) == 2 {
			ip, err := getIP(proxies[0])
			assert.NoError(err)
			assert.Equal("127.0.0.1", ip)
		}
		assert.NotZero(len(proxies))
	}

	assert.Equal(len(hostTable), len(linkTable))
	sum := 0
	for _, row := range linkTable {
		assert.Equal(len(hostTable), len(row))
		for _, value := range row {
			sum += value
		}
	}
	assert.Equal(2, sum)
}

func TestOneChunk(t *testing.T) {
	assert := assert.New(t)

	proxies := []string{
		"127.0.0.1:1000",
		"127.0.0.2:2000",
		"127.0.0.3:3000",
		"127.0.0.4:4000",
		"127.0.0.5:5000",
		"127.0.0.6:6000",
	}
	alloc, err := newChunkAllocator(proxies, 2)
	assert.NoError(err)
	assert.NotNil(alloc)
	chunks, err := alloc.allocate()
	assert.NoError(err)
	assert.Equal(1, len(chunks))
	assert.Equal(2, len(chunks[0]))
}

func TestMultiChunk(t *testing.T) {
	assert := assert.New(t)

	proxies := []string{
		"127.0.0.1:1000",
		"127.0.0.2:2000", "127.0.0.2:2001",
		"127.0.0.3:3000", "127.0.0.3:3001",
		"127.0.0.4:4000", "127.0.0.4:4001",
		"127.0.0.5:5000", "127.0.0.5:5001", "127.0.0.5:5002",
		"127.0.0.6:6000",
	}
	alloc, err := newChunkAllocator(proxies, 10)
	assert.NoError(err)
	assert.NotNil(alloc)
	chunks, err := alloc.allocate()
	assert.NoError(err)
	assert.Equal(5, len(chunks))
	for _, chunk := range chunks {
		assert.Equal(2, len(chunk))
	}
}

func TestAllocChunkWithExistingCluster(t *testing.T) {
	assert := assert.New(t)

	existingChunks := genExistingChunks()

	proxies := []string{
		"127.0.0.1:1000",
		"127.0.0.2:2000",
		"127.0.0.3:3000",
	}
	alloc, err := newChunkAllocatorWithExistingChunks(proxies, 2, existingChunks, false)
	assert.NoError(err)
	assert.NotNil(alloc)
	chunks, err := alloc.allocate()
	assert.NoError(err)
	assert.Equal(1, len(chunks))

	log.Errorf("chunks table %+v", chunks)
	chunk := chunks[0]
	assert.Equal(2, len(chunk))
}
