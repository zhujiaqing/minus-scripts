#!/usr/bin/env python
# -*-coding:UTF8-*-

import sys
import time

import MySQLdb
import redis
import web


reload(sys)
sys.setdefaultencoding('UTF-8')  # @UndefinedVariable

usa_redis_5 = redis.Redis(host="10.154.148.158", port=6379, db=5)
usa_redis_10 = redis.Redis(host="10.154.148.158", port=6379, db=10)

def get_meow():
    b_timestamp = usa_redis_5.hget('H:scale', 'meow_timestamp')
    b_num = usa_redis_5.hget('H:scale', 'meow_num')
    b_timestamp = 0 if not b_timestamp else int(b_timestamp)
    b_num = 0 if not b_num else int(b_num)
    
    info = usa_redis_5.info('Keyspace')
    num = info['db1']['keys']
    
    timestamp = int(time.time())
    usa_redis_5.hset('H:scale', 'meow_num', num)
    usa_redis_5.hset('H:scale', 'meow_timestamp', timestamp)
    
    if timestamp == b_timestamp:timestamp = 1
        
    return num, (num - b_num) / (timestamp - b_timestamp)

def get_youja():
    sg_mysql = MySQLdb.connect(host='54.169.234.201', user='minus', passwd='minus', charset='utf8', db='uplusmain', port=3306)

    b_timestamp = usa_redis_5.hget('H:scale', 'youja_timestamp')
    b_num = usa_redis_5.hget('H:scale', 'youja_num')
    b_timestamp = 0 if not b_timestamp else int(b_timestamp)
    b_num = 0 if not b_num else int(b_num)
    
    cur = sg_mysql.cursor()
    cur.execute('select count(*) from user_status')
    num = int(cur.fetchone()[0])
    cur.close()
    
    timestamp = int(time.time())
    usa_redis_5.hset('H:scale', 'youja_num', num)
    usa_redis_5.hset('H:scale', 'youja_timestamp', timestamp)
    
    if timestamp == b_timestamp:timestamp = 1
    
    return num, (num - b_num) / (timestamp - b_timestamp)

urls = (
    "/favicon.ico", "favicon",
    "/status", "status",
    "/youja", "youja",
    "/meow", "meow",
    "/.*", "meow"
    )
app = web.application(urls, globals())

class favicon:
    def GET(self):
        return ""
    
class status:
    def GET(self):
        web.header('Content-Type', 'text/html; charset=utf-8', unique=True) 
        web.header('Cache-Control', 'no-Cache') 
        meow_num, meow_speed = get_meow()
        youja_num, youja_speed = get_youja()
        return '<H1>[%s] meow num is %s, speed is %s /s<br/>' \
            'youja num is %s, speed is %s /s</H1>' % (
                                              time.strftime('%Y-%m-%d %H:%M:%S'),
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
        return '<H1>[%s] youja num is %s, speed is %s /s</H1>' % (
                                              time.strftime('%Y-%m-%d %H:%M:%S'),
                                              format(youja_num, ','),
                                              format(youja_speed, ',')
                                              )
    
class meow:
    def GET(self):
        web.header('Content-Type', 'text/html; charset=utf-8', unique=True) 
        web.header('Cache-Control', 'no-Cache') 
        web.header('Refresh', 15)
        meow_num, meow_speed = get_meow()
        return '<H1>[%s] ' \
            '<BR/>meow num is %s' \
            '<BR/>   speed is %s /s' \
            '<BR/>task num is %s' \
            '</H1>' % (
                                              time.strftime('%Y-%m-%d %H:%M:%S'),
                                              format(meow_num, ','),
                                              format(meow_speed, ','),
                                              format(usa_redis_10.scard('S:diff') if usa_redis_10.exists('S:diff') else 0, ',')
                                              )

if __name__ == "__main__":
    app.run()
