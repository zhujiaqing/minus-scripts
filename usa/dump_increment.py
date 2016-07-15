#!/usr/bin/env python
# -*-coding:UTF8-*-

import time

import MySQLdb
import redis

from meowchat_user import DumpUser
from meowchat_photo import DumpPhoto

KEY_SADD_DIFF_USER = 'S:diff:user'
KEY_SADD_DIFF_PHOTO = 'S:diff:photo'

def loading_increment_user():
    usa_redis_10 = redis.Redis(host="10.154.148.158", port=6666, db=10)    
    
    dumpUser = DumpUser()
    dumpPhoto = DumpPhoto()

    usa_mysql = MySQLdb.connect(host='10.231.129.198', user='root', passwd='carlhu', charset='utf8', db='minus', port=3306)
    cur = usa_mysql.cursor()
    while True:
        user_sql = 'select id from minus_user where id>%s limit 100' % usa_redis_10.get('meow:max:uid')
        size = cur.execute(user_sql)
        
        if 0 == size:break
        
        print 'num: %s' % format(size, ',')
        users = cur.fetchall()
        
        usa_redis_10.set('meow:max:uid', users[-1][0])
        
        for user in users:
            usa_redis_10.sadd(KEY_SADD_DIFF_USER, user[0])
            usa_redis_10.sadd(KEY_SADD_DIFF_PHOTO, user[0])
        
        dumpUser.repair_increment(KEY_SADD_DIFF_USER)
        dumpPhoto.repair_increment(KEY_SADD_DIFF_PHOTO)

    cur.close()
    usa_mysql.close()

    dumpUser.close_all()
    dumpPhoto.close_all()
    
if __name__ == '__main__':
    loading_increment_user()
     
    
    print '\n[%s] Completed \n' % (time.strftime('%Y-%m-%d %H:%M:%S'))
    


