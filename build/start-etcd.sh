#!/bin/bash

set -x -e -o pipefail

docker pull quay.io/coreos/etcd:v2.2.4
etcd_id=$(docker run -d -p 2379:2379 --name etcd1 quay.io/coreos/etcd:v2.2.4 -advertise-client-urls=http://0.0.0.0:2379 -listen-client-urls=http://0.0.0.0:2379)
docker logs -f "$etcd_id" &

limit=$((SECONDS+5))
status=""

while [ $SECONDS -lt $limit ]; do
    echo "Remaining: " $((limit-SECONDS))
    status="$(curl -sv -o /dev/stderr -w '%{http_code}' http://localhost:2379/health || :)"
    if [ "$status" = "200" ]; then break; fi 
    sleep 1
done

if [ "$status" = "200" ]; then
	true;
else
	docker rm -f "$etcd_id" || true
fi

