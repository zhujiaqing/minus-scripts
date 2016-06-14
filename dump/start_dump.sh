#!/bin/bash

dump_20160614(){
for i in {1..10}
do
    echo "screen python meowchat_to_youja.py $(((i-1)*10000)) $((i*10000)) &" 
    echo
done

echo "screen python meowchat_to_youja.py 100000 200000000 &" 
echo
}

watch_20160614(){
    echo "redis-cli -h 10.179.67.118 -p 6379 -n 1 scard S:photo"
    echo
}

dump_20160614
