#!/usr/bin/env python
# -*-coding:UTF8-*-

import time

import MySQLdb
import redis

from meowchat_user import DumpUser
from meowchat_photo import DumpPhoto

KEY_SADD_DIFF_USER = 'S:diff:user:repair'
KEY_SADD_DIFF_PHOTO = 'S:diff:photo:repair'

def loading_increment_user():
    usa_redis_10 = redis.Redis(host="10.154.148.158", port=6666, db=10)

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
        
    cur.close()
    usa_mysql.close()
    
def get_uids_by_photouri_is_null():
    usa_redis_10 = redis.Redis(host="10.154.148.158", port=6666, db=10)
    usa_redis_10.sadd(KEY_SADD_DIFF_USER, 17172928)
    
    return 
    
    sg_mysql_10 = MySQLdb.connect(host='54.169.234.201', user='uplus', passwd='q1w2e3r4t5', charset='utf8', db='uplus_resource', port=3306)
    cur = sg_mysql_10.cursor()
    
    max_id = 0
    
    while True:
        user_sql = 'select id,user_id from photos where photouri = "" and id>%s limit 2' % max_id
        size = cur.execute(user_sql)
        
        if 0 == size:break
    
        print 'num: %s' % format(size, ',')
        photos = cur.fetchall()
        
        max_id = photos[-1][0]
        
        for photo in photos:
            usa_redis_10.sadd(KEY_SADD_DIFF_USER, photo[1])
            usa_redis_10.sadd(KEY_SADD_DIFF_PHOTO, photo[1])
        
        break
    cur.close()
    sg_mysql_10.close()

if __name__ == '__main__':
    get_uids_by_photouri_is_null()
     
    dumpUser = DumpUser()
    dumpUser.repair_increment(KEY_SADD_DIFF_USER)
    dumpUser.close_all()
    
    dumpPhoto = DumpPhoto()
    dumpPhoto.repair_increment(KEY_SADD_DIFF_PHOTO)
    dumpPhoto.close_all()
    
    print '\n[%s] Completed \n' % (time.strftime('%Y-%m-%d %H:%M:%S'))
    


