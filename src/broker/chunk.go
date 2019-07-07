package broker

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// ErrNoChunkResource indicates that the remaining resources can't produce enough chunks.
var ErrNoChunkResource = errors.New("no chunk resource")

// ErrInvalidRequestedProxyNum indicates that the remaining resources can't produce enough chunks.
var ErrInvalidRequestedProxyNum = errors.New("invalid requested proxy number")

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
	linkTable  [][]int
	hostTable  [][]string
	ipIndexMap map[string]int
}

func newChunkAllocator(proxies []string, expectedProxyNum uint64) (*chunkAllocator, error) {
	chunks := make([]*NodeChunkStore, 0)
	return newChunkAllocatorWithExistingChunks(proxies, expectedProxyNum, chunks, false)
}

func newChunkReplaceProxyAllocator(proxies []string, chunks []*NodeChunkStore) (*chunkAllocator, error) {
	return newChunkAllocatorWithExistingChunks(proxies, uint64(len(proxies)), chunks, true)
}

func newChunkAllocatorWithExistingChunks(proxies []string, expectedProxyNum uint64, chunks []*NodeChunkStore, replaceProxy bool) (*chunkAllocator, error) {
	if !replaceProxy && expectedProxyNum%2 != 0 {
		return nil, ErrInvalidRequestedProxyNum
	}
	hostTable, linkTable, ipIndexMap, err := initChunkTable(proxies, expectedProxyNum, chunks, replaceProxy)
	log.Infof("hostTable %+v, linkTable %+v", hostTable, linkTable)
	if err != nil {
		return nil, err
	}
	return &chunkAllocator{
		linkTable:  linkTable,
		hostTable:  hostTable,
		ipIndexMap: ipIndexMap,
	}, nil
}

func (alloc *chunkAllocator) allocate() ([][2]string, error) {
	chunks := make([][2]string, 0)
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

func (alloc *chunkAllocator) replaceProxy(failedProxyAddress, peerProxyAddress string) (string, error) {
	failedProxyIndex, err := alloc.getHostIndexByProxyAddress(failedProxyAddress)
	if err != nil {
		return "", errors.WithStack(err)
	}
	peerProxyIndex, err := alloc.getHostIndexByProxyAddress(peerProxyAddress)
	if err != nil {
		return "", errors.WithStack(err)
	}

	alloc.linkTable[failedProxyIndex][peerProxyIndex]--
	alloc.linkTable[peerProxyIndex][failedProxyIndex]--

	leastLinkAddress, _, err := alloc.consumeLeastLink(peerProxyIndex)
	if err != nil {
		return "", err
	}
	return leastLinkAddress, nil
}

func (alloc *chunkAllocator) getHostIndexByProxyAddress(proxyAddress string) (int, error) {
	ip, err := getIP(proxyAddress)
	if err != nil {
		return 0, err
	}
	index, ok := alloc.ipIndexMap[ip]
	if !ok {
		return 0, ErrProxyNotFound
	}
	return index, nil
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

func initChunkTable(proxies []string, expectedProxyNum uint64, chunks []*NodeChunkStore, replaceProxy bool) ([][]string, [][]int, map[string]int, error) {
	proxyMap := genProxyMap(proxies)
	proxyMap = removeAdditionalProxies(proxyMap, expectedProxyNum)

	for _, chunk := range chunks {
		for i, node := range chunk.Nodes {
			if i%2 == 1 {
				continue
			}
			ip, err := getIP(node.ProxyAddress)
			if err != nil {
				return nil, nil, nil, err
			}
			if _, ok := proxyMap[ip]; !ok {
				proxyMap[ip] = make([]string, 0)
			}
		}
	}

	ipIndexMap := make(map[string]int, 0)
	hostTable := make([][]string, 0, len(proxyMap))
	ipIndex := 0
	for ip, proxies := range proxyMap {
		hostTable = append(hostTable, proxies)
		ipIndexMap[ip] = ipIndex
		ipIndex++
	}

	if uint64(getHostSum(hostTable)) != expectedProxyNum {
		return nil, nil, nil, ErrNoChunkResource
	}

	linkTable := make([][]int, len(hostTable), len(hostTable))
	for i := range linkTable {
		linkTable[i] = make([]int, len(hostTable), len(hostTable))
	}

	for _, chunk := range chunks {
		firstIP, err := getIP(chunk.Nodes[0].ProxyAddress)
		if err != nil {
			return nil, nil, nil, err
		}
		secondIP, err := getIP(chunk.Nodes[halfChunkSize].ProxyAddress)
		if err != nil {
			return nil, nil, nil, err
		}
		firstIndex, firstOk := ipIndexMap[firstIP]
		secondIndex, secondOk := ipIndexMap[secondIP]
		if !firstOk || !secondOk {
			log.Errorf("invalid state, can't find ip in map %+v %s %s", ipIndexMap, firstIP, secondIP)
			return nil, nil, nil, err
		}
		linkTable[firstIndex][secondIndex]++
		linkTable[secondIndex][firstIndex]++
	}

	return hostTable, linkTable, ipIndexMap, validateHostTablePrecondition(hostTable, replaceProxy)
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

func validateHostTablePrecondition(hostTable [][]string, replaceProxy bool) error {
	proxySum := getHostSum(hostTable)
	// premise 1
	if !replaceProxy && proxySum%2 != 0 {
		return newChunkError("the number of proxy is not even")
	}

	// premise 4
	maxNum, _ := getMaxHost(hostTable)
	if !replaceProxy && 2*maxNum > proxySum {
		return newChunkError("one of the host has too many proxy")
	}
	return nil
}

func removeAdditionalProxies(proxyMap map[string][]string, expectedProxyNum uint64) map[string][]string {
	for proxySum := uint64(getProxyMapSum(proxyMap)); proxySum > expectedProxyNum; proxySum-- {
		_, maxIP := getProxyMapMaxHost(proxyMap)
		proxyMap[maxIP] = proxyMap[maxIP][:len(proxyMap[maxIP])-1]
	}
	newProxyMap := make(map[string][]string, 0)
	for ip, proxies := range proxyMap {
		if len(proxies) > 0 {
			newProxyMap[ip] = proxies
		}
	}
	return newProxyMap
}

func getHostSum(hostTable [][]string) int {
	var s int
	for _, proxies := range hostTable {
		s += len(proxies)
	}
	return s
}

func getProxyMapSum(proxyMap map[string][]string) int {
	var s int
	for _, proxies := range proxyMap {
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

func getProxyMapMaxHost(proxyMap map[string][]string) (maxNum int, maxIP string) {
	maxNum = 0
	maxIP = ""
	for ip, proxies := range proxyMap {
		if len(proxies) > maxNum {
			maxNum = len(proxies)
			maxIP = ip
		}
	}
	return maxNum, maxIP
}

func getIP(address string) (string, error) {
	segs := strings.Split(address, ":")
	if len(segs) != 2 {
		return "", errors.WithStack(fmt.Errorf("invalid address %s", address))
	}
	return segs[0], nil
}
