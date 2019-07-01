# ETCD Data Design

# Path
```
/global_epoch => <int>

/all_proxies/<proxy_address> => {
    proxy_index: <int>,
    cluster_name: <string>,
    node_addresses: [<string>, ...],
}

/failed_proxies/<proxy_address> => {
    node_addresses: [<string>, ...],
}

/failures/<proxy_address>/<reporter_id> => {
    report_time: <timestamp>,
}

/clusters/epoch/<cluster_name> => <int>

/clusters/nodes/<master|replica>/<proxy_index> => {
    node_address: <string>,
    proxy_address: <string>,
    slots: [
        SlotRange{
            start: <int>,
            end: <int>,
            tag: SlotRangeTag{
                tag_type: "importing" | "migrating" | "None",
                meta: => MigrationMeta{
                    src_proxy_index: <int>,
                    dst_proxy_index: <int>,
                    epoch: <int>,
                },
            },
        }
    ]
}
```

