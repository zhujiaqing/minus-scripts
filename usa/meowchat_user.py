#!/usr/bin/env python
# -*- coding:utf8 -*-

import httplib
import logging  # @UnusedImport
import logging.handlers
import sys
import time

import MySQLdb
from cassandra.cluster import Cluster  # @UnresolvedImport
import redis
import simplejson


reload(sys)
sys.setdefaultencoding("UTF-8")  # @UndefinedVariable

class DumpUser:
    
    def __init__(self):
        handler = logging.handlers.TimedRotatingFileHandler("/data/logs/meow-user.out", when='D', interval=1)
        fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
        
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        self.logger = logging.getLogger('user')
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        
        self.logger.info('============= init task =============')
        
        self.usa_mysql = MySQLdb.connect(host='10.231.129.198', user='root', passwd='carlhu', charset='utf8', db='minus', port=3306)
        self.cur = self.usa_mysql.cursor()
        
        self.usa_cluster = Cluster(
                           ['10.140.244.182',
                            '10.137.127.31',
                            '10.183.33.73',
                            '10.35.179.200',
                            '10.71.191.186',
                            '10.187.62.106'],
                           protocol_version=3)
        self.usa_session = self.usa_cluster.connect()
    
        self.usa_redis_10 = redis.Redis(host="10.154.148.158", port=6666, db=10)
        self.usa_redis_11 = redis.Redis(host="10.154.148.158", port=6666, db=11)
    
    def api_request(self, host='info_ex.api.imyoujia.com', port=80, method='POST', uri=None, body=None):
        start_time = time.time()
        if uri is None or body is None: 
            self.logger.info('Not api request')
            return 
        try:
            headers = {'Content-Type': 'application/json;charset=UTF-8'}
            conn = httplib.HTTPConnection(host, port, timeout=2000)
            conn.request(method, uri, body=body, headers=headers)
            response = conn.getresponse()
            status = response.status
            self.logger.info(status)
            self.logger.info(response.read())
        except Exception as ex:
            self.logger.warn('Exception %s' % str(ex))
        finally:
            self.logger.info('api request cost time: %ss' % int(time.time() - start_time))
    
    def user_account(self, user):
        self.logger.info('========> account')
        try:
            uri = '/uplus-api/meow/import/useraccount'
            payload = {
                        "nick_name": str(user[24]),
                        "username": str(user[1]),
                        "password": str(user[3]),
                        "email": str(user[2]),
                        "sign_type": "20",
                        "user_id": str(user[0]),
                        "au_id": "20",
                        "security_token": "20",
                        "access_token": "20"
                        }
            self.logger.info(payload)
            self.api_request(uri=uri, body=simplejson.dumps(payload))
        except Exception as ex:
            self.logger.warn('Exception %s' % str(ex))
        
        # facebook
        if user[16] != '':
            self.logger.info('========> facebook')
            try:
                payload = {
                            "nick_name": str(user[24]),
                            "username": str(user[1]),
                            "password": str(user[3]),
                            "email": str(user[2]),
                            "sign_type": "16",
                            "user_id": str(user[0]),
                            "au_id": str(user[16]),
                            "security_token": "",
                            "access_token": str(user[19])
                        }
                self.logger.info(simplejson.dumps(payload))
                self.api_request(uri=uri, body=simplejson.dumps(payload))
            except Exception as ex:
                self.logger.warn('Exception %s' % str(ex))
        
        # twitter
        if user[15] != '':
            self.logger.info('========> twitter')
            try:
                payload = {
                            "nick_name": str(user[7]if '' == user[24] or None == user[24] else user[24]),
                            "username": str(user[1]),
                            "password": str(user[3]),
                            "email": str(user[2]),
                            "sign_type": "17",
                            "user_id": str(user[0]),
                            "au_id": str(user[15]),
                            "security_token": str(user[13]),
                            "access_token": str(user[12])
                        }
                self.logger.info(simplejson.dumps(payload))
                self.api_request(uri=uri, body=simplejson.dumps(payload))
            except Exception as ex:
                self.logger.warn('Exception %s' % str(ex))
    
    def user_profile(self, user):
        self.logger.info('========> profile')
        try:
            # birthdate
            birthdate_sql = 'select * from minus_userbirthdate where user_id=%s' % user[0]
            birthdate_size = self.cur.execute(birthdate_sql)
            birthdates = self.cur.fetchall()
            birthdate = None if 0 == birthdate_size else birthdates[0] 
            
            # gender
            gender_sql = 'select * from minus_usergender where user_id=%s' % user[0]
            gender_size = self.cur.execute(gender_sql)
            genders = self.cur.fetchall()
            gender = None if 0 == gender_size else genders[0]
            
            # balance
            coins = score = 0
            for balance in  self.usa_session.execute('SELECT coins,score FROM users.score WHERE uid=%s;' % user[0]):
                coins = balance.coins if balance.coins is not None else 0
                score = balance.score if balance.score is not None else 0
                
            uri = '/uplus-api/meow/import/userprofile'
            payload = {
                        "birthday": str('1995-01-01' if birthdate is None else birthdate[1]),
                        "fans_count": "0",
                        "sign_type": "20",
                        "gift_count": "0",
                        "avatarid": "0",
                        "oauth_bind": "20",
                        "nick_name": str(user[7]if '' == user[24] or None == user[24] else user[24]),
                        "first_client_version": "5.1.0-test",
                        "balance": str(coins),
                        "reg_finish_datetime": str(user[5]),
                        "glamour_count": str(score),
                        "client_type": "8",
                        "intruduction": str(user[8]),
                        "avatar_status": "2",
                        "name": str(user[7]),
                        "ua": "meow",
                        "gender": str('2' if gender is None else gender[1]),
                        "user_id": str(user[0]),
                        "id": str(user[0]),
                        "login_count": "0",
                        "client_version":"5.1.0-test"
                     }
            self.logger.info(simplejson.dumps(payload))
            self.api_request(uri=uri, body=simplejson.dumps(payload))
        except Exception as ex:
            self.logger.warn('Exception %s' % str(ex))
        
    def user_relation(self, user):
        self.logger.info('========> relatioin')
        try:
            uri = '/uplus-api/meow/import/relation'
            er_list = []
            for er in self.usa_session.execute('SELECT * FROM cb.cb_er_dt WHERE follower_id=%s;' % user[0]):
                er_list.append({
                                "fromUserId":str(user[0]),
                                "toUserId":str(er.followee_id),
                                "isLiked":"1",
                                "createTime": time.mktime(time.strptime(str(er.dt)[0:18], '%Y-%m-%d %H:%M:%S'))
                         })
            if 0 < len(er_list):  # 当没有关系时不用请求
                payload = {
                           "list":er_list,
                           "uid":str(user[0]),
                           "type":"0"
                       }
                self.logger.info(simplejson.dumps(payload))
                self.api_request(uri=uri, body=simplejson.dumps(payload))
        except Exception as ex:
            self.logger.warn('Exception %s' % str(ex))
        
        try:
            uri = '/uplus-api/meow/import/relation'
            ee_list = []
            for ee in self.usa_session.execute('SELECT * FROM cb.cb_ee_dt WHERE followee_id=%s;' % user[0]):
                ee_list.append({
                                "fromUserId":str(ee.follower_id),
                                "toUserId":str(user[0]),
                                "isLiked":"1",
                                "createTime": time.mktime(time.strptime(str(ee.dt)[0:18], '%Y-%m-%d %H:%M:%S'))
                         })
            
            if 0 < len(ee_list):  # 当没有关系时不用请求
                payload = {
                           "list":ee_list,
                           "uid":str(user[0]),
                           "type":"1"
                       }
                self.logger.info(simplejson.dumps(payload))
                self.api_request(uri=uri, body=simplejson.dumps(payload))
        except Exception as ex:
            self.logger.warn('Exception %s' % str(ex))

    def get_s3_by_view(self, view_id):
        s3_file = None
        try:
            s3_file_sql = 'select filename_s3 from minus_item where view_id="%s"' % view_id
            size = self.cur.execute(s3_file_sql)
            s3_file_list = self.cur.fetchall()
            if 0 < size:
                s3_file = s3_file_list[0][0]
        except Exception as ex:
            self.logger.warn('Exception %s' % str(ex))
        finally:
            return s3_file
        
    def user_photo(self, user):
        self.logger.info('========> photo')
        try:
            # avator
            s3_file_avator = self.get_s3_by_view(user[26])
            if s3_file_avator is not None:
                self.usa_redis_11.sadd('S:a1:%s' % user[0], s3_file_avator)
                self.logger.info(s3_file_avator)
            
            # photo
            for item in self.usa_session.execute('SELECT item_id,dt FROM items.userline WHERE uid=%s;' % user[0]):
                for ic in self.usa_session.execute('SELECT view_id FROM items.dict WHERE item_id=%s;' % item.item_id):
                    s3_file_photo = self.get_s3_by_view(ic.view_id)
                    if s3_file_photo is not None:
                        self.usa_redis_11.sadd('S:a0:%s' % user[0], s3_file_photo)
                    self.logger.info(self.usa_redis_11.smembers('S:a0:%s' % user[0]))
                    
        except Exception as ex:
            self.logger.warn('Exception %s' % str(ex))
        
    def repair_increment(self, key):
        while True:
            start_time = time.time()

            uid = self.usa_redis_10.spop(key)
            if uid is None:break
            
            size = self.cur.execute('select * from minus_user where id = %s' % uid)
            users = self.cur.fetchall()
            if 0 == size:  # 没有结果集
                self.logger.info('uid: %s, not exists with mius_user' % uid)
                continue
            user = users[0]
            if '' == user[1] or user[1] is None:  # 若用户名为空直接丢弃
                self.logger.info('uid: %s, not object' % uid)
                continue
                
            # convert storage
            self.logger.info('##############>>> [repair start conver storage] %s' % uid) 
            self.user_account(user)
            self.user_profile(user)
            self.user_relation(user)
            self.user_photo(user)
            self.logger.info('##############>>> [repair end conver storage] %s, cost time %ss' % 
                                 (uid,
                                  int(time.time() - start_time)))
            
    def close_all(self):
        self.cur.close()
        self.usa_mysql.close()
