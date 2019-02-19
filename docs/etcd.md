# Etcd Data Design

# Path
```
/configurable_prefix/hosts/<host>/port/<port> => {
    "cluster": <string>,
    "slots": [<string>],
}

/configurable_prefix/hosts/<host>/epoch => <epoch>

/configurable_prefix/clusters/nodes/<name>/<node_address> => {
    "slots": [<string>],
}

/configurable_prefix/clusters/epoch/<name> => <epoch>

/configurable_prefix/coordinators/<address>/report_id/<report_id> => <report_id>

/configurable_prefix/failures/<address>/<report_id>
```