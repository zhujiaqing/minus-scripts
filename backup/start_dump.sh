#!/bin/bash

dump_20160614(){
for i in {20..100}
do
    echo "screen python meowchat_to_youja.py $(((i-1)*10000)) $((i*10000)) &" 
    echo
done

echo "screen python meowchat_to_youja.py 100000 200000000 &" 
echo
}

watch_20160614(){
    echo "redis-cli -n 1 scard S:photo"
    echo
}

dump_20160614

watch_20160614

