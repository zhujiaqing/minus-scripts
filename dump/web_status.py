#!/usr/bin/env python
# -*-coding:UTF8-*-

import sys
import time

import MySQLdb
import redis
import web


reload(sys)
sys.setdefaultencoding('UTF-8')  # @UndefinedVariable

usa_redis = redis.Redis(host="10.154.148.158", port=6379, db=5)
sg_mysql = MySQLdb.connect(host='54.169.234.201', user='minus', passwd='minus', charset='utf8', db='uplusmain', port=3306)

def get_meow():
    b_timestamp = usa_redis.hget('H:scale', 'meow_timestamp')
    b_num = usa_redis.hget('H:scale', 'meow_num')
    b_timestamp = 0 if not b_timestamp else int(b_timestamp)
    b_num = 0 if not b_num else int(b_num)
    
    info = usa_redis.info('Keyspace')
    num = info['db1']['keys']
    
    timestamp = int(time.time())
    usa_redis.hset('H:scale', 'meow_num', num)
    usa_redis.hset('H:scale', 'meow_timestamp', timestamp)
    
    if timestamp == b_timestamp:timestamp = 1
        
    return num, (num - b_num) / (timestamp - b_timestamp)

def get_youja():
    b_timestamp = usa_redis.hget('H:scale', 'youja_timestamp')
    b_num = usa_redis.hget('H:scale', 'youja_num')
    b_timestamp = 0 if not b_timestamp else int(b_timestamp)
    b_num = 0 if not b_num else int(b_num)
    
    cur = sg_mysql.cursor()
    cur.execute('select count(*) from user_status')
    num = int(cur.fetchone()[0])
    cur.close()
    
    timestamp = int(time.time())
    usa_redis.hset('H:scale', 'youja_num', num)
    usa_redis.hset('H:scale', 'youja_timestamp', timestamp)
    
    if timestamp == b_timestamp:timestamp = 1
    
    return num, (num - b_num) / (timestamp - b_timestamp)

urls = (
    "/status", "status",
    "/youja", "youja",
    "/meow", "meow",
    "/.*", "meow"
    )
app = web.application(urls, globals())

class status:
    def GET(self):
        web.header('Content-Type', 'text/html; charset=utf-8', unique=True) 
        web.header('Cache-Control', 'no-Cache') 
        meow_num, meow_speed = get_meow()
        youja_num, youja_speed = get_youja()
        return 'meow num is %s, speed is %s /s<br/>' \
            'youja num is %s, speed is %s /s' % (
                                              format(meow_num, ','),
                                              format(meow_speed, ','),
                                              format(youja_num, ','),
                                              format(youja_speed, ',')
                                              )
    
class youja:
    def GET(self):
        web.header('Content-Type', 'text/html; charset=utf-8', unique=True) 
        web.header('Cache-Control', 'no-Cache') 
        youja_num, youja_speed = get_youja()
        return 'meow num is %s, speed is %s /s' % (
                                              format(youja_num, ','),
                                              format(youja_speed, ',')
                                              )
    
class meow:
    def GET(self):
        web.header('Content-Type', 'text/html; charset=utf-8', unique=True) 
        web.header('Cache-Control', 'no-Cache') 
        web.header('"Refresh', 5)
        meow_num, meow_speed = get_meow()
        return 'meow num is %s, speed is %s /s' % (
                                              format(meow_num, ','),
                                              format(meow_speed, ',')
                                              )

if __name__ == "__main__":
    app.run()
