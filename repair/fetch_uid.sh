#!/bin/bash

fetch(){
    tag=$1
    [ -z $tag ] && return
    sfile="s3://youja-logs/nginx/$tag/$(date +%Y%m%d)/moplus.api.imyoujia.com_access.log-$(date -d '-1 hours' +%Y%m%d%H).gz"
    tfile="/dev/shm/moplus-$(date +%s).gz"

    /usr/local/python/bin/aws s3 ls $sfile || return
    /usr/local/python/bin/aws s3 cp $sfile $tfile
    /bin/zcat $tfile | awk -F'`' '$17<100000000{print $17}' | sort -u > $tfile-uid
    /usr/local/redis/bin/redis-cli -p 6666 -n 26 sadd S:Task:relation $(paste -s -d' ' $tfile-uid )
    /usr/local/redis/bin/redis-cli -p 6666 -n 26 srem S:Task:relation -
    /bin/rm -f $tfile $tfile-uid
}

fetch nginx_10_13.youja.cn
fetch nginx_10_14.youja.cn

exit 0
