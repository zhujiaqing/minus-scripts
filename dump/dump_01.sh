#!/bin/bash

# 第1期，固定几个测试帐号
# 第2期，近2个月活动用户

uids=$(mysql -N -h10.231.129.198 -uroot -pcarlhu -Dminus \
-e'select id from minus_user where username in ("pepsi1016","8ops2016","atschx","yaweia","guangleiqiu","abataoa")' | \
paste -s -d',')

mysqldump -h10.231.129.198 -uroot -pcarlhu minus minus_user --where="id in ($uids)" | \
mysql -h54.169.188.17 -uminus -pminus -Dminus

mysqldump -h10.231.129.198 -uroot -pcarlhu minus minus_userbirthdate --where="user_id in ($uids)" | \
mysql -h54.169.188.17 -uminus -pminus -Dminus

mysqldump -h10.231.129.198 -uroot -pcarlhu minus minus_usergender --where="user_id in ($uids)" | \
mysql -h54.169.188.17 -uminus -pminus -Dminus


