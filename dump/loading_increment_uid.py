#!/usr/bin/env python
# -*-coding:UTF8-*-

import time

import MySQLdb
import redis


def loading_increment():
    usa_redis_10 = redis.Redis(host="10.154.148.158", port=6666, db=10)

    usa_mysql = MySQLdb.connect(host='10.231.129.198', user='root', passwd='carlhu', charset='utf8', db='minus', port=3306)
    cur = usa_mysql.cursor()
    user_sql = 'select id from minus_user where id>18160302 limit 10' 
    size = cur.execute(user_sql)
    
    print 'num: %s' % format(size, ',')
    users = cur.fetchall()
    
    for user in users:
        usa_redis_10.sadd('S:diff', user[0])
        
    cur.close()
    usa_mysql.close()

if __name__ == '__main__':
    loading_increment()

    print '\n[%s] Completed \n' % (time.strftime('%Y-%m-%d %H:%M:%S'))
    


