#!/bin/bash

# 用户头像
# 用户相册
# 用户动态

init_avator(){
mysql -h54.169.188.17 -uminus -pminus -Dminus \
-e"select uid from minus_user_status where avator=0" | \
while read uid
do
echo $uid

done

}

init_avator


