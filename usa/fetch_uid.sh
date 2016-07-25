#!/bin/bash

fetch(){
    sfile="s3://youja-logs/nginx/$(date +%Y%m%d)/members.api.imyoujia.com_access.log-$(date +%Y%m%d%H).gz"
    tfile="/dev/shm/moplus-$(date +%s).gz"
    
    /usr/local/python/bin/aws s3 cp $sfile $tfile
    zcat $tfile | awk -F'`' '$17<100000000{print $17}' | sort -u > $tfile-uid
    /usr/local/redis/bin/redis-cli -p 6666 -n 26 sadd S:Task:relation $(paste -s -d' ')
}

fetch

exit 0
