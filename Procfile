a: sleep 0; $TRACE target/debug/chain-repl-test -e http://127.0.0.1:2379/ 2>&1  -p 127.42.0.1:7000 -l 127.42.0.1:7100 > log-a.txt 2>&1
b: sleep 1; $TRACE target/debug/chain-repl-test -e http://127.0.0.1:2379/ 2>&1  -p 127.42.0.1:7001 -l 127.42.0.1:7101 > log-b.txt 2>&1
c: sleep 2; $TRACE target/debug/chain-repl-test -e http://127.0.0.1:2379/ 2>&1  -p 127.42.0.1:7002 -l 127.42.0.1:7102 > log-c.txt 2>&1
