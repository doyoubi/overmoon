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
	hostTable := [][]string{
		[]string{"127.0.0.1:1000"},
		[]string{"127.0.0.2:2000"},
		[]string{"127.0.0.3:3000"},
	}
	hostTable = removeAdditionalProxies(hostTable, 2)
	assert.Equal(2, getHostSum(hostTable))
}

func TestInitHostTable(t *testing.T) {
	assert := assert.New(t)

	proxies := []string{
		"127.0.0.1:1000",
		"127.0.0.2:2000",
		"127.0.0.3:3000",
	}
	_, err := initHostTable(proxies, 4)
	assert.Error(err)

	hostTable, err := initHostTable(proxies, 2)
	log.Errorf("host table %+v", hostTable)
	assert.NoError(err)
	assert.Equal(2, len(hostTable))
	assert.Equal(1, len(hostTable[0]))
	assert.Equal(1, len(hostTable[1]))

	proxies = []string{
		"127.0.0.1:1000",
		"127.0.0.2:2000",
		"127.0.0.2:2001",
		"127.0.0.3:3000",
	}
	hostTable, err = initHostTable(proxies, 2)
	log.Errorf("host table %+v", hostTable)
	assert.NoError(err)
	assert.Equal(2, len(hostTable))
	assert.Equal(1, len(hostTable[0]))
	assert.Equal(1, len(hostTable[1]))
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
