package broker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

var ErrTxnFailed = errors.New("txn failed")
var ErrInvalidKeyNum = errors.New("invalid key numbers")

// EtcdMetaBroker implements the MetaDataBroker interface and serves as a broker backend.
type EtcdMetaBroker struct {
	config *EtcdConfig
	client *clientv3.Client
}

func NewEtcdMetaBrokerFromEndpoints(config *EtcdConfig, endpoints []string) (*EtcdMetaBroker, error) {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return NewEtcdMetaBroker(config, client)
}

// NewEtcdMetaBroker creates EtcdMetaBroker
func NewEtcdMetaBroker(config *EtcdConfig, client *clientv3.Client) (*EtcdMetaBroker, error) {
	return &EtcdMetaBroker{
		config: config,
		client: client,
	}, nil
}

// GetClusterNames retrieves all the cluster names from etcd.
func (broker *EtcdMetaBroker) GetClusterNames(ctx context.Context) ([]string, error) {
	clusterKeyPrefix := fmt.Sprintf("%s/clusters/epoch/", broker.config.PathPrefix)
	kvs, err := getRangeKeyPostfixAndValue(ctx, broker.client, clusterKeyPrefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	return keys, nil
}

// GetCluster queries a cluster by name
func (broker *EtcdMetaBroker) GetCluster(ctx context.Context, name string) (*Cluster, error) {
	clusterEpochKey := fmt.Sprintf("%s/clusters/epoch/%s", broker.config.PathPrefix, name)
	clusterNodesKeyPrefix := fmt.Sprintf("%s/clusters/nodes/%s/", broker.config.PathPrefix, name)
	clusterSlotsKeyPrefix := fmt.Sprintf("%s/clusters/slots/%s/", broker.config.PathPrefix, name)

	epoch, nodes, err := broker.getEpochAndNodes(ctx, clusterEpochKey, clusterNodesKeyPrefix, clusterSlotsKeyPrefix)
	if err != nil {
		return nil, err
	}

	for _, node := range nodes {
		node.ClusterName = name
	}

	cluster := &Cluster{
		Name:  name,
		Epoch: epoch,
		Nodes: nodes,
	}
	return cluster, nil
}

func (broker *EtcdMetaBroker) getEpochAndNodes(ctx context.Context, epochKey, nodesKey, clusterSlotsKeyPrefix string) (int64, []*Node, error) {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
	}

	response, err := broker.client.Txn(ctx).Then(
		clientv3.OpGet(epochKey),
		clientv3.OpGet(nodesKey, opts...),
		clientv3.OpGet(clusterSlotsKeyPrefix, opts...),
	).Commit()

	if err != nil {
		return 0, nil, err
	}
	if !response.Succeeded {
		return 0, nil, ErrTxnFailed
	}
	if len(response.Responses) != 3 {
		return 0, nil, ErrInvalidKeyNum
	}

	epochRes := response.Responses[0].GetResponseRange()
	epoch, err := parseEpoch(epochRes.Kvs)
	if err != nil {
		return 0, nil, err
	}

	nodesRes := response.Responses[1].GetResponseRange()
	kvs, err := parseRangeResult(nodesKey, nodesRes.Kvs)
	if err != nil {
		return 0, nil, err
	}
	nodes, err := parseNodes(kvs)
	if err != nil {
		return 0, nil, err
	}

	slotsRes := response.Responses[1].GetResponseRange()
	kvs, err = parseRangeResult(nodesKey, nodesRes.Kvs)
	if err != nil {
		return 0, nil, err
	}
	slots, err := parseSlots(kvs)
	if err != nil {
		return 0, nil, err
	}

	nodes, err = addSlots(nodes, slots)
	if err != nil {
		return 0, nil, err
	}

	return epoch, nodes, nil
}

// GetHostAddresses queries all hosts.
func (broker *EtcdMetaBroker) GetHostAddresses(ctx context.Context) ([]string, error) {
	hostKeyPrefix := fmt.Sprintf("%s/all_proxies/", broker.config.PathPrefix)
	kvs, err := getRangeKeyPostfixAndValue(ctx, broker.client, hostKeyPrefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	return keys, nil
}

// GetHost query the host by address
func (broker *EtcdMetaBroker) GetHost(ctx context.Context, address string) (*Host, error) {
	hostEpochKey := fmt.Sprintf("%s/all_hosts/epoch/%s", broker.config.PathPrefix, address)
	hostNodesKeyPrefix := fmt.Sprintf("%s/hosts/%s/nodes/", broker.config.PathPrefix, address)
	epoch, nodes, err := broker.getEpochAndNodes(ctx, hostEpochKey, hostNodesKeyPrefix)
	if err != nil {
		return nil, err
	}
	return &Host{
		Address: address,
		Epoch:   epoch,
		Nodes:   nodes,
	}, nil
}

func (broker *EtcdMetaBroker) AddFailure(ctx context.Context, address string, reportID string) error {
	key := fmt.Sprintf("%s/failures/%s/%s", broker.config.PathPrefix, address, reportID)
	timestamp := time.Now().Unix()
	value := fmt.Sprintf("%v", timestamp)
	res, err := broker.client.Grant(ctx, broker.config.FailureTTL)
	if err != nil {
		return err
	}
	opts := []clientv3.OpOption{
		clientv3.WithLease(clientv3.LeaseID(res.ID)),
	}
	_, err = broker.client.Put(ctx, key, value, opts...)
	return err
}

func (broker *EtcdMetaBroker) GetFailures(ctx context.Context) ([]string, error) {
	prefix := fmt.Sprintf("%s/failures/", broker.config.PathPrefix)
	kv, err := getRangeKeyPostfixAndValue(ctx, broker.client, prefix)
	if err != nil {
		return nil, err
	}
	// TODO: add min failure config
	addresses := make([]string, 0, len(kv))
	for k := range kv {
		segs := strings.SplitN(string(k), "/", 2)
		if len(segs) != 2 {
			return nil, fmt.Errorf("invalid failure key %s", string(k))
		}
		nodeAddress := segs[0]
		addresses = append(addresses, nodeAddress)
	}
	return addresses, nil
}

func (broker *EtcdMetaBroker) GetEpochByCluster(ctx context.Context, name string) (int64, error) {
	key := fmt.Sprintf("%s/clusters/epoch/%s", broker.config.PathPrefix, name)
	return broker.GetEpoch(ctx, key)
}

func (broker *EtcdMetaBroker) GetEpochByHost(ctx context.Context, address string) (int64, error) {
	key := fmt.Sprintf("%s/hosts/epoch/%s", broker.config.PathPrefix, address)
	return broker.GetEpoch(ctx, key)
}

func (broker *EtcdMetaBroker) GetEpoch(ctx context.Context, key string) (int64, error) {
	res, err := broker.client.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	return parseEpoch(res.Kvs)
}

func parseEpoch(kvs []*mvccpb.KeyValue) (int64, error) {
	if len(kvs) == 0 {
		return 0, ErrNotExists
	} else if len(kvs) > 1 {
		return 0, ErrInvalidKeyNum
	}
	kv := kvs[0]

	epoch, err := strconv.ParseInt(string(kv.Value), 10, 64)
	return epoch, err
}

// GetNodesByCluster queries all the nodes under a cluster.
func (broker *EtcdMetaBroker) GetNodesByCluster(ctx context.Context, name string) ([]*Node, error) {
	nodesKeyPrefix := fmt.Sprintf("%s/clusters/nodes/%s/", broker.config.PathPrefix, name)
	kvs, err := getRangeKeyPostfixAndValue(ctx, broker.client, nodesKeyPrefix)
	if err != nil {
		return nil, err
	}
	return parseNodes(kvs)
}

func parseNodes(kvs map[string][]byte) ([]*Node, error) {
	nodes := make([]*Node, len(kvs), len(kvs))
	for k, v := range kvs {
		etcdNodeMeta := &nodeMeta{}
		err := etcdNodeMeta.decode(v)
		if err != nil {
			return nil, err
		}

		role := MasterRole
		if strings.HasPrefix(k, "master/") {
			k = strings.TrimPrefix(k, "master/")
		} else if strings.HasPrefix(k, "replica/") {
			role = ReplicaRole
			k = strings.TrimPrefix(k, "master/")
		} else {
			return nil, fmt.Errorf("invalid role type in path: %s", k)
		}

		proxyIndex, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			return nil, err
		}

		node := &Node{
			Address:      etcdNodeMeta.NodeAddress,
			ProxyAddress: etcdNodeMeta.ProxyAddress,
			ClusterName:  "",  // initialized later
			Slots:        nil, // initialized later
			Role:         role,
		}

		nodeIndex := proxyIndex * 2
		if role == ReplicaRole {
			nodeIndex++
		}

		if nodes[nodeIndex] != nil {
			return nil, fmt.Errorf("duplicated node index, proxy index: %d, node index: %d", nodeIndex, proxyIndex)
		}
		nodes[nodeIndex] = node
	}
	return nodes, nil
}

func parseSlots(kvs map[string][]byte) ([][]slotRangeMeta, error) {
	slots := make([][]slotRangeMeta, len(kvs), len(kvs))
	for k, v := range kvs {
		etcdSlotsMeta := &slotsMeta{}
		err := etcdSlotsMeta.decode(v)
		if err != nil {
			return nil, err
		}

		proxyIndex, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			return nil, err
		}

		if slots[proxyIndex] != nil {
			return nil, fmt.Errorf("duplicated proxy index, proxy index: %d", proxyIndex)
		}
		slots[proxyIndex] = etcdSlotsMeta.Slots
	}
	return slots, nil
}

func addSlots(nodes []*Node, slots [][]slotRangeMeta) ([]*Node, error) {
	if len(slots)*2 != len(nodes) {
		return nil, fmt.Errorf("mismatch slots and nodes number, nodes: %d, slots: %d", len(nodes), len(slots))
	}

	for i, slotRangeMeta := range slots {
		slotRanges := make([]SlotRange, 0, len(slotRangeMeta))
		for _, meta := range slotRangeMeta {
			srcMasterIndex := meta.Tag.Meta.SrcProxyIndex * 2
			dstMasterIndex := meta.Tag.Meta.DstProxyIndex * 2
			slotRanges = append(slotRanges, SlotRange{
				Start: meta.Start,
				End:   meta.End,
				Tag: SlotRangeTag{
					TagType: meta.Tag.TagType,
					Meta: &MigrationMeta{
						Epoch:           meta.Tag.Meta.Epoch,
						SrcNodeAddress:  nodes[srcMasterIndex].Address,
						SrcProxyAddress: nodes[srcMasterIndex].ProxyAddress,
						DstNodeAddress:  nodes[dstMasterIndex].Address,
						DstProxyAddress: nodes[dstMasterIndex].ProxyAddress,
					},
				},
			})
		}
		nodes[2*i].Slots = slotRanges
	}
	return nodes, nil
}

// GetNodesByHost queries all the nodes under a host.
func (broker *EtcdMetaBroker) GetNodesByHost(ctx context.Context, address string) ([]*Node, error) {
	hostsKeyPrefix := fmt.Sprintf("%s/hosts/%s/nodes/", broker.config.PathPrefix, address)
	kvs, err := getRangeKeyPostfixAndValue(ctx, broker.client, hostsKeyPrefix)
	if err != nil {
		return nil, err
	}
	return parseNodes(kvs)
}

func getRangeKeyPostfixAndValue(ctx context.Context, client *clientv3.Client, prefix string) (map[string][]byte, error) {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
	}
	res, err := client.Get(ctx, prefix, opts...)
	if err != nil {
		return nil, err
	}
	return parseRangeResult(prefix, res.Kvs)
}

func parseRangeResult(prefix string, kvs []*mvccpb.KeyValue) (map[string][]byte, error) {
	postfixes := make(map[string][]byte)
	for _, item := range kvs {
		key := string(item.Key)
		if !strings.HasPrefix(key, prefix) {
			return nil, fmt.Errorf("Unexpected key %s", key)
		}
		postfix := strings.TrimPrefix(key, prefix)
		postfixes[postfix] = item.Value
	}
	return postfixes, nil
}

// EtcdConfig stores broker config.
type EtcdConfig struct {
	PathPrefix string
	FailureTTL int64
}
