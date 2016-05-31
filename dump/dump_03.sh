#!/bin/bash

# 用户头像 optype=1
# 用户相册 optype=0
# 用户动态

init_avator(){
mysql -N -h54.169.188.17 -uminus -pminus -Dminus \
-e"select minus_user.id,minus_user.avatar_item from minus_user inner join minus_user_status on minus_user.id=minus_user_status.uid where minus_user_status.avator=0;" | \
while read uid key
do
echo "dump avator $uid - $key"
url="https://d1uk5e10lg6nan.cloudfront.net/j$key.jpg"

curl -s -o /dev/shm/$key.jpg $url

curl -i -X PUT \
-H "Content-Type:image/jpeg;charset=UTF-8" \
--upload-file /dev/shm/$key.jpg \
"http://resource.api.imyoujia.com/uplusmain-file/resource_type/101?user_id=$uid&albumid=0&optype=1&user_type=3&client_ver=4.0.1-g&token=s00e330000010c43d8ef768417140ca20ce417ba75c41be1c304cdda55efd28791048199c2b99261a0a1149"

[ $? -eq 0 ] && mysql -N -h54.169.188.17 -uminus -pminus -Dminus \
-e"update minus_user_status set avator=1 where uid=$uid"

rm -f /dev/shm/$key.jpg

done

}

init_photo(){
mysql -N -h54.169.188.17 -uminus -pminus -Dminus \
-e"select minus_user_photo.uid,minus_user_photo.photo_key from minus_user_photo inner join minus_user_status on minus_user_photo.uid=minus_user_status.uid where photo=0;" | \
while read uid key
do
echo "dump avator $uid - $key"
url="https://d1uk5e10lg6nan.cloudfront.net/j$key.jpg"

curl -s -o /dev/shm/$key.jpg $url

curl -i -X PUT \
-H "Content-Type:image/jpeg;charset=UTF-8" \
--upload-file /dev/shm/$key.jpg \
"http://resource.api.imyoujia.com/uplusmain-file/resource_type/101?user_id=$uid&albumid=0&optype=0&user_type=3&client_ver=4.0.1-g&token=s00e330000010c43d8ef768417140ca20ce417ba75c41be1c304cdda55efd28791048199c2b99261a0a1149"

[ $? -eq 0 ] && mysql -N -h54.169.188.17 -uminus -pminus -Dminus \
-e"update minus_user_status set photo=1 where uid=$uid"

rm -f /dev/shm/$key.jpg
done
}

while true
do
init_avator
init_photo

echo "not status sleep 10 $(date)"
sleep 10
done






