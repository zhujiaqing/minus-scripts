#!/usr/bin/env python
# -*-coding:UTF8-*-

'''

    # atschx 16953316
    # 8ops2016 17172928
#     usa_redis_10.sadd(KEY_SADD_DIFF_USER, 16953316)
#     usa_redis_10.sadd(KEY_SADD_DIFF_PHOTO, 16953316)
#     return 
    
'''
import time

import MySQLdb
import redis

from meowchat_user import DumpUser
from meowchat_photo import DumpPhoto

timestamp = time.strftime('%Y%m%d%H%M%S')
KEY_SADD_DIFF_USER = 'S:diff:user:%s' % timestamp
KEY_SADD_DIFF_PHOTO = 'S:diff:photo:%s' % timestamp

def get_uids_by_photouri_is_null():
    usa_redis_10 = redis.Redis(host="10.154.148.158", port=6666, db=10)
    
    
#     max_id = 0
    
    while True:
        dumpUser = DumpUser()
        dumpPhoto = DumpPhoto()
        
        sg_mysql_10 = MySQLdb.connect(host='54.169.234.201', user='uplus', passwd='q1w2e3r4t5', charset='utf8', db='uplus_resource', port=3306)
        cur = sg_mysql_10.cursor()

#         user_sql = 'select id,user_id from photos where photouri = "" and id>%s limit 100' % max_id
        user_sql = 'select id,user_id from photos where photouri = "" order by id desc limit 100'
        size = cur.execute(user_sql)
        
        if 0 == size:break
    
        photos = cur.fetchall()
        print 'num: %s' % format(size, ','), photos
        
#         max_id = photos[-1][0]
        
        for photo in photos:
            usa_redis_10.sadd(KEY_SADD_DIFF_USER, photo[1])
            usa_redis_10.sadd(KEY_SADD_DIFF_PHOTO, photo[1])
        
        cur.close()
        sg_mysql_10.close()

        dumpUser.repair_photo(KEY_SADD_DIFF_USER)
        dumpPhoto.repair_increment(KEY_SADD_DIFF_PHOTO)
        
        dumpUser.close_all()
        dumpPhoto.close_all()

if __name__ == '__main__':
    get_uids_by_photouri_is_null()
     
    
    print '\n[%s] Completed \n' % (time.strftime('%Y-%m-%d %H:%M:%S'))
    


