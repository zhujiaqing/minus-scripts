#!/bin/bash

# 第1期，固定几个测试帐号
# 第2期，近2个月活动用户

part(){
uids=$(mysql -N -h10.231.129.198 -uroot -pcarlhu -Dminus \
-e'select id from minus_user where username in ("pepsi1016","8ops2016","atschx","yaweia","guangleiqiu","abataoa")' | \
paste -s -d',')

mysqldump -h10.231.129.198 -uroot -pcarlhu minus minus_user --where="id in ($uids)" | \
mysql -h54.169.234.201 -uminus -pminus -Dminus

mysqldump -h10.231.129.198 -uroot -pcarlhu minus minus_userbirthdate --where="user_id in ($uids)" | \
mysql -h54.169.234.201 -uminus -pminus -Dminus

mysqldump -h10.231.129.198 -uroot -pcarlhu minus minus_usergender --where="user_id in ($uids)" | \
mysql -h54.169.234.201 -uminus -pminus -Dminus

}

all(){
mysqldump -h10.231.129.198 -uroot -pcarlhu minus minus_user | mysql -h54.169.234.201 -uminus -pminus -Dminus

mysqldump -h10.231.129.198 -uroot -pcarlhu minus minus_userbirthdate | mysql -h54.169.234.201 -uminus -pminus -Dminus

mysqldump -h10.231.129.198 -uroot -pcarlhu minus minus_usergender | mysql -h54.169.234.201 -uminus -pminus -Dminus
}

local(){
mysqldump -h10.231.129.198 -uroot -pcarlhu minus minus_user | gzip -d > minus_user-$(date +%Y%m%d).gz

mysqldump -h10.231.129.198 -uroot -pcarlhu minus minus_userbirthdate | gzip -d > minus_userbirthdate-$(date +%Y%m%d).gz

mysqldump -h10.231.129.198 -uroot -pcarlhu minus minus_usergender | gzip -d > minus_usergender-$(date +%Y%m%d).gz

}

#part
#all
local

