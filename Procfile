etcd: etcd --listen-peer-urls http://localhost:2380 --data-dir=target/data-etcd
a: sleep 1; $TRACE target/debug/chain-repl -e http://127.0.0.1:2379/ 2>&1  -p 127.0.0.1:7000 -l 127.0.0.1:7100 -c 127.0.0.1:7110
b: sleep 2; $TRACE target/debug/chain-repl -e http://127.0.0.1:2379/ 2>&1  -p 127.0.0.1:7001 -l 127.0.0.1:7101 -c 127.0.0.1:7111
c: sleep 3; $TRACE target/debug/chain-repl -e http://127.0.0.1:2379/ 2>&1  -p 127.0.0.1:7002 -l 127.0.0.1:7102 -c 127.0.0.1:7112
