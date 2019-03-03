# Etcd Data Design

# Path
For MetaDataBroker
```
/hosts/<proxy_address>/nodes/<node address> => {
    "address": <string>,
    "proxy_address": <string>,
    "cluster_name": <string>,
    "slots": [{
        "start": <int>,
        "end": <int>,
        "tag": <string>,
    }, ...],
}

/hosts/epoch/<proxy_address> => <epoch>

// These nodes belong the a existing cluster if the corresponding config key exists.
// If not, these nodes belong the a deleting cluster.
/clusters/nodes/<name>/<node_address> => {
    "address": <string>,
    "proxy_address": <string>,
    "cluster_name": <string>,
    "slots": [{
        "start": <int>,
        "end": <int>,
        "tag": <string>,
    }, ...],
}

// Indicating whether the cluster exists
/clusters/epoch/<name> => <epoch>

/coordinators/<address>/report_id/<report_id> => <report_id>

/failures/<address>/<report_id> => <int64 timestamp>
```

For MetaManipulationBroker
```
/hosts/all_nodes/<proxy_address>/<node address> =>
    <cluster name>  // empty string when this node is not used

/tasks/epoch => <epoch>
/tasks/operation/bump_epoch_for_cluster_hosts/<cluster name> => <timestamp>
/tasks/operation/remove_nodes/<cluster name> => <cluster name>

/deleting/clusters/nodes/<cluster name> => <timestamp>

/clusters/spec/node_number/<cluster name> => <node number>
/clusters/spec/node_max_memory/<cluster name> => <node max memory>
```

## Operation

### Rules
- If one operation consists multiple transactions, from the second transaction there must have compensation, which also means the later transactions should be able to be safe to retry.
- Single Source Of Truth is based on
  * `/clusters/spec/*`
  * `/clusters/epoch/<name>`
  * `/hosts/all_nodes/<proxy_address>/<node address>`
  * `/hosts/epoch/<proxy_address>`
- Trigger bump_epoch_for_cluster_hosts when updates cluster.

### Operations List

#### Create Cluster
- Wrap them in a transaction
  * Check if the cluster name is still in the deleting list.
  * Check if the cluster name has already existed.
  * Set `/clusters/spec/*` and `/clusters/epoch/<name>` using transaction.
- Create node one by one. Need compensation.

#### Create Node
- Get some hosts and their epochs through `/hosts/all_nodes/<proxy_address>/<node address>` and `/hosts/epoch/<proxy_address>`.
For each available node in each host, try the following in a transaction.
- Check if the epoch of cluster has changed or if the cluster has been deleted.
- Check if the host has been deleted. No need to check epoch.
- Check if `/hosts/all_nodes/<proxy_address>/<node address>` is empty string.
- Set the host `/hosts/all_nodes/<proxy_address>/<node address>` to cluster name
- Set the `/hosts/<proxy_address>/nodes/<node address>`
- Bump the epoch of the host
- Set the `/clusters/nodes/<name>/<node_address>`
- Bump the epoch of the cluster
Trigger bump_epoch_for_cluster_hosts.

#### Replace Node
- Get the meta data of old node
- Same as `Create Node`, but to replace the old node from cluster with the new one.
- Remove `/hosts/<proxy_address>/nodes/<node address>`
- Change `/hosts/all_nodes/<proxy_address>/<node address>` to empty string.
- Bump the epoch of the two hosts
- Bump the epoch of cluster
Trigger bump_epoch_for_cluster_hosts.

#### Delete Node
Wrap them in a transaction
- Get the old node
- Remove `/hosts/<proxy_address>/nodes/<node address>`
- Change `/hosts/all_nodes/<proxy_address>/<node address>` to empty string.
- Remove node from cluster
- Bump the epoch of host
- Bump the epoch of cluster
Trigger bump_epoch_for_cluster_hosts.

#### Delete Cluster
Wrap them in a transaction.
- Check if the cluster has been deleted.
- Delete the cluster spec and epoch
- Delete the cluster
Add the task `/tasks/operation/remove_nodes/<cluster name>`, which should
- Delete the node from the host.
- Bump the epoch of the host.
- Delete the node from the cluster.

#### Migrate Slots
- Add node to cluster
- Set migrating tag for slots
- Keep checking the migration state of proxy.
- Clear the migration tag and change the slots.
Trigger bump_epoch_for_cluster_hosts.
(Need to think about node failure when put it into details)

#### Fix Inconsistent Data
- Fix conflict for the four key
- Fix other conflict and problems from these four keys.
