arg="$1"

if [ "$arg" = "proxy" ]; then

curl -XPOST -H 'Content-Type: application/json' localhost:7799/api/proxies/nodes -d'{"proxy_address":"127.0.0.1:6001","nodes":["127.0.0.1:7001","127.0.0.1:7002"]}'
curl -XPOST -H 'Content-Type: application/json' localhost:7799/api/proxies/nodes -d'{"proxy_address":"127.0.0.2:6002","nodes":["127.0.0.2:7003","127.0.0.2:7004"]}'
curl -XPOST -H 'Content-Type: application/json' localhost:7799/api/proxies/nodes -d'{"proxy_address":"127.0.0.3:6003","nodes":["127.0.0.3:7005","127.0.0.3:7006"]}'
curl -XPOST -H 'Content-Type: application/json' localhost:7799/api/proxies/nodes -d'{"proxy_address":"127.0.0.4:6004","nodes":["127.0.0.4:7007","127.0.0.4:7008"]}'
curl -XPOST -H 'Content-Type: application/json' localhost:7799/api/proxies/nodes -d'{"proxy_address":"127.0.0.5:6005","nodes":["127.0.0.5:7009","127.0.0.5:7010"]}'
curl -XPOST -H 'Content-Type: application/json' localhost:7799/api/proxies/nodes -d'{"proxy_address":"127.0.0.6:6006","nodes":["127.0.0.6:7011","127.0.0.6:7012"]}'
curl localhost:7799/api/proxies/addresses | python -m json.tool
curl localhost:7799/api/proxies/meta/127.0.0.1:6001 | python -m json.tool
curl localhost:7799/api/proxies/meta/127.0.0.2:6002 | python -m json.tool
curl localhost:7799/api/proxies/meta/127.0.0.3:6003 | python -m json.tool
curl localhost:7799/api/proxies/meta/127.0.0.4:6004 | python -m json.tool
curl localhost:7799/api/proxies/meta/127.0.0.5:6005 | python -m json.tool
curl localhost:7799/api/proxies/meta/127.0.0.6:6006 | python -m json.tool

elif [ "$arg" = "cluster" ]; then

curl -XPOST -H 'Content-Type: application/json' localhost:7799/api/clusters -d'{"cluster_name":"mydb","node_number":4}' -v
curl localhost:7799/api/clusters/meta/mydb | python -m json.tool
curl -v -XPUT -H 'Content-Type: application/json' localhost:7799/api/clusters/nodes/mydb
curl localhost:7799/api/clusters/meta/mydb | python -m json.tool

curl -v -XDELETE -H 'Content-Type: application/json' localhost:7799/api/clusters/free_nodes/mydb
curl localhost:7799/api/clusters/meta/mydb | python -m json.tool

curl -v -XDELETE localhost:7799/api/clusters/meta/mydb
curl localhost:7799/api/clusters/names

elif [ "$arg" = "config" ]; then
curl -XPUT -H 'Content-Type: application/json' localhost:7799/api/clusters/config/mydb -d'{"compression_strategy":"set_get_only"}' -v

fi

