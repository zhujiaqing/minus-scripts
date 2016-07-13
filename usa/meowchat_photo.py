#!/usr/bin/env python
# -*- coding:utf8 -*-

import logging  # @UnusedImport
import logging.handlers
import sys
import time

import MySQLdb
import redis


reload(sys)
sys.setdefaultencoding("UTF-8")  # @UndefinedVariable

class DumpPhoto:
    
    def __init__(self):
        handler = logging.handlers.TimedRotatingFileHandler("/data/logs/meow-photo.out", when='D', interval=1)
        fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
        
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        self.logger = logging.getLogger('photo')
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        
        self.logger.info('============= init task =============')
        
        self.usa_redis_10 = redis.Redis(host="10.154.148.158", port=6666, db=10)  # 任务
        self.usa_redis_11 = redis.Redis(host="10.154.148.158", port=6666, db=11)  # 相册
    
        self.sg_mysql_10 = MySQLdb.connect(host='54.169.234.201', user='uplus', passwd='q1w2e3r4t5', charset='utf8', db='uplus_resource', port=3306)
        self.sg_mysql_20 = MySQLdb.connect(host='54.169.234.201', user='uplus', passwd='q1w2e3r4t5', charset='utf8', db='uplusmain', port=3307)
    
        self.fw = open('/data/file/aws-s3.file-%s' % time.strftime('%Y%m%d%H%M%S'), 'aw')
    
    def repair_increment(self, key):
        sg_cur_20 = self.sg_mysql_20.cursor()
        sg_cur_10 = self.sg_mysql_10.cursor()
        
        while True:
            
            uid = self.sg_redis_10.spop(key)
            if uid is None: break
            
            try:
                # 头像
                uri = self.usa_redis_11.spop('S:a1:%s' % uid)
                if uri is not None:
                    sg_cur_20.execute('insert into photo_user_index(user_id) values(%s)' % uid)
                    index_id = sg_cur_20.lastrowid  # self.sg_mysql_20.insert_id()
                    sg_cur_10.execute('insert into photos(id,album_id,user_id,photouri,size_type,create_time,status) values(%s,1,%s,"%s",2,"%s",3)' % (
                                                                                                              index_id,
                                                                                                              uid,
                                                                                                              uri,
                                                                                                              time.strftime('%Y-%m-%d %H:%M:%S')))
                    self.fw.write('aws s3 ls --region ap-southeast-1 s3://minus-item/%s || aws s3 cp --region us-east-1 s3://minus_items/%s s3://minus-item/%s\n' % (uri, uri, uri))
                    sg_cur_20.execute('update user_info set avatarid=%s where user_id=%s' % (index_id, uid))
                    
                    self.sg_mysql_20.commit()
                    self.sg_mysql_10.commit()
                
                # 相册
                while True:
                    uri = self.usa_redis_10.spop('S:a0:%s' % uid)
                    if uri is None: break
                    
                    sg_cur_20.execute('insert into photo_user_index(user_id) values(%s)' % uid)
                    index_id = sg_cur_20.lastrowid  # self.sg_mysql_20.insert_id()
                    sg_cur_10.execute('insert into photos(id,user_id,photouri,size_type,create_time,status) values(%s,%s,"%s",2,"%s",3)' % (
                                                                                                              index_id,
                                                                                                              uid,
                                                                                                              uri,
                                                                                                              time.strftime('%Y-%m-%d %H:%M:%S')))
                    self.fw.write('aws s3 ls --region ap-southeast-1 s3://minus-item/%s || aws s3 cp --region us-east-1 s3://minus_items/%s s3://minus-item/%s\n' % (uri, uri, uri))
                    
                    self.sg_mysql_20.commit()
                    self.sg_mysql_10.commit()
                
            except Exception as ex:
                self.sg_mysql_20.rollback()
                self.sg_mysql_10.rollback()
                self.sg_redis_10.sadd('S:diff:error:sg', uid)

                self.logger.warn('Exception %s' % str(ex))
            
        else:
            sg_cur_20.close()
            sg_cur_10.close()
    
    def close_all(self):
        self.sg_mysql_20.close()
        self.sg_mysql_10.close()
        self.fw.close()

    


