#!/usr/bin/env python
# -*-coding:UTF8-*-

import time

import MySQLdb
import redis

from meowchat_user import DumpUser
from meowchat_photo import DumpPhoto

def loading_increment_user():
    usa_redis_10 = redis.Redis(host="10.154.148.158", port=6666, db=10)

    usa_mysql = MySQLdb.connect(host='10.231.129.198', user='root', passwd='carlhu', charset='utf8', db='minus', port=3306)
    cur = usa_mysql.cursor()
    user_sql = 'select id from minus_user where id>18160302 limit 2' 
    size = cur.execute(user_sql)
    
    print 'num: %s' % format(size, ',')
    users = cur.fetchall()
    
    for user in users:
        usa_redis_10.sadd('S:diff:user', user[0])
        usa_redis_10.sadd('S:diff:photo', user[0])
        
    cur.close()
    usa_mysql.close()
    
if __name__ == '__main__':
#     loading_increment_user()
#     
#     dumpUser = DumpUser()
#     dumpUser.repair_increment('S:diff:user')
#     dumpUser.close_all()
    
    dumpPhoto = DumpPhoto()
    dumpPhoto.repair_increment('S:diff:photo')
    dumpPhoto.close_all()
    
    print '\n[%s] Completed \n' % (time.strftime('%Y-%m-%d %H:%M:%S'))
    


