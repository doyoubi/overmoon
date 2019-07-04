arg="$1"

if [ "$arg" = "proxy" ]; then

curl -XPOST -H 'Content-Type: application/json' localhost:7799/api/proxies/nodes -d'{"proxy_address":"127.0.0.1:6001","nodes":["127.0.0.1:7001","127.0.0.1:7002"]}'
curl -XPOST -H 'Content-Type: application/json' localhost:7799/api/proxies/nodes -d'{"proxy_address":"127.0.0.2:6002","nodes":["127.0.0.2:7003","127.0.0.2:7004"]}'
curl localhost:7799/api/proxies/addresses | python -m json.tool
curl localhost:7799/api/proxies/meta/127.0.0.1:6001 | python -m json.tool
curl localhost:7799/api/proxies/meta/127.0.0.2:6002 | python -m json.tool

elif [ "$arg" = "cluster" ]; then

curl -XPOST -H 'Content-Type: application/json' localhost:7799/api/clusters -d'{"cluster_name":"mydb","node_number":4}' | python -m json.tool
curl localhost:7799/api/clusters/meta/mydb | python -m json.tool

fi

