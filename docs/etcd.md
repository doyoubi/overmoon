# Etcd Data Design

# Path
```
/configurable_prefix/hosts/<host>/nodes/<node address> => {
    "cluster": <string>,
    "slots": [<string>],
}

/configurable_prefix/hosts/epoch/<host> => <epoch>

/configurable_prefix/clusters/nodes/<name>/<node_address> => {
    "slots": [[<int>, <int>], [<int>]],
    "proxy_address": <string>,
}

/configurable_prefix/clusters/epoch/<name> => <epoch>

/configurable_prefix/coordinators/<address>/report_id/<report_id> => <report_id>

/configurable_prefix/failures/<address>/<report_id> => <int64 timestamp>
```