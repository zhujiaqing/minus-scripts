#!/usr/bin/env python
# -*-coding:UTF8-*-

import time

import MySQLdb
import redis


def loading_s3():
    usa_redis_1 = redis.Redis(host="10.154.148.158", port=6379, db=1)
    usa_redis_2 = redis.Redis(host="10.154.148.158", port=6379, db=2)  # 头像
    usa_redis_3 = redis.Redis(host="10.154.148.158", port=6379, db=3)  # 相册
    usa_redis_10 = redis.Redis(host="10.154.148.158", port=6379, db=10)  # 任务

    usa_mysql = MySQLdb.connect(host='10.231.129.198', user='root', passwd='carlhu', charset='utf8', db='minus', port=3306)
    cur = usa_mysql.cursor()
    
    while True:
        uid = usa_redis_10.spop('S:photo')
        if uid is None:break
            
        try:
            views = usa_redis_1.hgetall('H:%s' % uid)
            for key in views.keys():
                s3_file_sql = 'select filename_s3 from minus_item where view_id="%s"' % key
                size = cur.execute(s3_file_sql)
                s3_file_list = cur.fetchall()
                if 0 == size:continue
                s3_file = s3_file_list[0]
                
                if '1' == views[key]: usa_redis_2.sadd('S:%s' % uid, s3_file[0])  # 头像
                else: usa_redis_3.sadd('S:%s' % uid, s3_file[0])  # 相册
            
            usa_redis_10.sadd('S:s3file', uid)
        
        except:
            usa_redis_10.sadd('S:error', uid)
            
    cur.close()
    usa_mysql.close()

if __name__ == '__main__':
    loading_s3()

    print '\n[%s] Completed \n' % (time.strftime('%Y-%m-%d %H:%M:%S'))
    


