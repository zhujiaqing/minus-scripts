#!/usr/bin/env python
# -*-coding:UTF8-*-

import time

import MySQLdb
import redis
import web


usa_redis = redis.Redis(host="10.154.148.158", port=6379, db=5)
sg_mysql = MySQLdb.connect(host='54.169.234.201', user='minus', passwd='minus', charset='utf8', db='uplusmain', port=3306)

def get_meow():
    b_timestamp = usa_redis.hget('H:scale', 'meow_timestamp')
    b_num = usa_redis.hget('H:scale', 'meow_num')
    b_timestamp = 0 if b_timestamp is None else int(b_timestamp)
    b_num = 0 if b_num is None else int(b_num)
    
    info = usa_redis.info('Keyspace')
    num = info['db1']['keys']
    timestamp = int(time.time())
    usa_redis.hset('H:scale', 'meow_num', num)
    usa_redis.hset('H:scale', 'meow_timestamp', timestamp)
    
    return num, (num - b_num) / (timestamp - b_timestamp)

def get_youja():
    cur = sg_mysql.cursor()
    cur.execute('select count(*) from user_status')
    num = cur.fetchone()
    cur.close()
    
    
    
    return num

urls = (
    "/status", "status",
    "/youja", "youja",
    "/meow", "meow",
    "/.*", "meow"
    )
app = web.application(urls, globals())

class status:
    def GET(self):
        meow_num, meow_speed = get_meow()
        youja_num = get_youja()
        return meow_num, meow_speed, youja_num
    
class youja:
    def GET(self):
        youja_num = get_youja()
        return youja_num
    
class meow:
    def GET(self):
        meow_num, meow_speed = get_meow()
        return meow_num, meow_speed

if __name__ == "__main__":
    app.run()
