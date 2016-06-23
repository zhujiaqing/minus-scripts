
====

# 导数据

```
海外版：meowchat --> imyoujia
初始化数据

1，导全表数据
    | minus_user          |
    | minus_userbirthdate |
    | minus_usergender    |
2，拉用户图像：minus_user_avator
    [mysql]select avatar_item from minus_user where id =16953316;
3，拉用户相册/feed：minus_user_feed
    [cass]SELECT * FROM items.userline WHERE uid = 16953316;
4，拉货币：minus_user_coins
    [cass] select * from users.score where uid=16953316;
5，拉关系：minus_user_relation_from/minus_user_reloation_to
    [cass]
    table：cb_er_dt(follwer关注的), cb_ee_dt(follwee被谁关注)
    select * from cb.cb_er_dt where follower_id=16953316;


上传图片
img=
uid=
curl -i \
    -X PUT \
    -H "Content-Type:image/jpeg;charset=UTF-8" \
    --upload-file 1458784364783-49700492-feed-1.jpg \
    "http://172.16.23.10:18080/uplusmain-file/resource_type/101?user_id=17172928&albumid=0&optype=1&user_type=3&client_ver=4.0.1-g&token=s00e330000010c43d8ef768417140ca20ce417ba75c41be1c304cdda55efd28791048199c2b99261a0a1149"

"""

```

usa
mysql -h10.169.235.151 -uroot -pcarlhu -Dminus -e"select id from minus_user" -N
redis-cli -h 10.154.148.158 -n 10 SADD S:youja $(paste -s -d' ' $fff)

sg
mysql -h172.16.121.20 -uminus -pminus -Duplusmain -e"select id from user;" -N
redis-cli -h 10.154.148.158 -n 10 SADD S:meow $(paste -s -d' ' $fff)

sdiff
redis-cli -h 10.154.148.158 -n 10 SDIFFSTORE S:diff S:meow S:youja


