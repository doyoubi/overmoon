package broker

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// ErrNoChunkResource indicates that the remaining resources can't produce enough chunks.
var ErrNoChunkResource = errors.New("no chunk resource")

// ErrNoChunkResource indicates that the remaining resources can't produce enough chunks.
var ErrInvalidRequestedProxyNum = errors.New("invalid proxy number")

type chunkError struct {
	msg string
}

func newChunkError(msg string) *chunkError {
	return &chunkError{msg: msg}
}

func (e *chunkError) Error() string {
	return e.msg
}

type chunkAllocator struct {
	linkTable [][]int
	hostTable [][]string
}

func newChunkAllocator(proxies []string, expectedProxyNum uint64) (*chunkAllocator, error) {
	if expectedProxyNum%2 != 0 {
		return nil, ErrInvalidRequestedProxyNum
	}
	hostTable, err := initHostTable(proxies, expectedProxyNum)
	if err != nil {
		return nil, err
	}
	linkTable := make([][]int, len(hostTable), len(hostTable))
	for i := range linkTable {
		linkTable[i] = make([]int, len(hostTable), len(hostTable))
	}
	return &chunkAllocator{
		linkTable: linkTable,
		hostTable: hostTable,
	}, nil
}

func (alloc *chunkAllocator) allocate() ([][2]string, error) {
	chunks := make([][2]string, 0)
	log.Errorf("host table %+v", alloc.hostTable)
	for getHostSum(alloc.hostTable) != 0 {
		maxNum, maxIndex := getMaxHost(alloc.hostTable)
		// check loop invariant
		hosSum := getHostSum(alloc.hostTable)
		if 2*maxNum > hosSum {
			log.Errorf("invalid state, host table %+v", alloc.hostTable)
			return nil, errors.WithStack(errors.New("invalid state, chunk algorithm failed to meet premise"))
		}

		maxHost := &alloc.hostTable[maxIndex]
		maxAddress := (*maxHost)[len(*maxHost)-1]
		*maxHost = (*maxHost)[:len(*maxHost)-1]

		leastLinkAddress, leastLinkIndex, err := alloc.consumeLeastLink(maxIndex)
		if err != nil {
			return nil, err
		}

		alloc.linkTable[maxIndex][leastLinkIndex]++
		alloc.linkTable[leastLinkIndex][maxIndex]++

		chunks = append(chunks, [2]string{maxAddress, leastLinkAddress})
	}
	return chunks, nil
}

func (alloc *chunkAllocator) consumeLeastLink(maxIndex int) (peerAddress string, peerIndex int, err error) {
	minLinkNum := -1
	for index, linkNum := range alloc.linkTable[maxIndex] {
		if index == maxIndex || len(alloc.hostTable[index]) == 0 {
			continue
		}
		if minLinkNum == -1 || linkNum < minLinkNum {
			minLinkNum = linkNum
			peerIndex = index
		}
	}

	if minLinkNum == -1 {
		return "", 0, errors.WithStack(errors.New("invalid state, can't get more peer"))
	}
	peerHost := &alloc.hostTable[peerIndex]
	peerAddress = (*peerHost)[len(*peerHost)-1]
	*peerHost = (*peerHost)[:len(*peerHost)-1]
	return peerAddress, peerIndex, nil
}

func initHostTable(proxies []string, expectedProxyNum uint64) ([][]string, error) {
	proxyMap := genProxyMap(proxies)

	hostTable := make([][]string, 0, len(proxyMap))
	for _, proxies := range proxyMap {
		hostTable = append(hostTable, proxies)
	}

	hostTable = removeAdditionalProxies(hostTable, expectedProxyNum)

	if uint64(getHostSum(hostTable)) != expectedProxyNum {
		return nil, ErrNoChunkResource
	}

	return hostTable, validateHostTablePrecondition(hostTable)
}

func genProxyMap(proxies []string) map[string][]string {
	proxyMap := make(map[string][]string, 0)
	for _, address := range proxies {
		ip, err := getIP(address)
		if err != nil {
			log.Errorf("invalid address format %s", address)
			continue
		}
		if _, ok := proxyMap[ip]; !ok {
			proxyMap[ip] = make([]string, 0)
		}
		proxyMap[ip] = append(proxyMap[ip], address)
	}
	return proxyMap
}

func validateHostTablePrecondition(hostTable [][]string) error {
	proxySum := getHostSum(hostTable)

	// premise 4
	maxNum, _ := getMaxHost(hostTable)
	if 2*maxNum > proxySum {
		return newChunkError("one of the host has too many proxy")
	}
	return nil
}

func removeAdditionalProxies(hostTable [][]string, expectedProxyNum uint64) [][]string {
	for proxySum := uint64(getHostSum(hostTable)); proxySum > expectedProxyNum; proxySum-- {
		_, maxIndex := getMaxHost(hostTable)
		maxHost := &hostTable[maxIndex]
		*maxHost = (*maxHost)[:len(*maxHost)-1]
	}
	newHostTable := make([][]string, 0)
	for _, proxies := range hostTable {
		if len(proxies) > 0 {
			newHostTable = append(newHostTable, proxies)
		}
	}
	return newHostTable
}

func getHostSum(hostTable [][]string) int {
	var s int
	for _, proxies := range hostTable {
		s += len(proxies)
	}
	return s
}

func getMaxHost(hostTable [][]string) (maxNum int, maxIndex int) {
	maxNum = 0
	maxIndex = 0
	for index, proxies := range hostTable {
		if len(proxies) > maxNum {
			maxNum = len(proxies)
			maxIndex = index
		}
	}
	return maxNum, maxIndex
}

func getIP(address string) (string, error) {
	segs := strings.Split(address, ":")
	if len(segs) != 2 {
		return "", errors.WithStack(fmt.Errorf("invalid address %s", address))
	}
	return segs[0], nil
}
