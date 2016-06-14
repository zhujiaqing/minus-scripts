#!/bin/bash

dump_20160614(){
for i in {1..9}
do
    screen python meowchat_to_youja.py $(((i-1)*10000)) $((i*10000)) &
sleep 3
done

screen python meowchat_to_youja.py 100000 200000000 &
}

dump_20160614
