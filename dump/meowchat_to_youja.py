#!/usr/bin/env python
# -*- coding:utf8 -*-

import time
import httplib
import httplib2
import simplejson
import MySQLdb
from cassandra.cluster import Cluster  # @UnresolvedImport
import redis

import sys
reload(sys)
sys.setdefaultencoding("UTF-8")  # @UndefinedVariable

import logging  # @UnusedImport
import logging.handlers

class Dump:
    
    def __init__(self):
        self.usa_mysql = MySQLdb.connect(host='10.231.129.198', user='root', passwd='carlhu', charset='utf8', db='minus', port=3306)
        
        self.usa_cluster = Cluster(['10.140.244.182', '10.137.127.31'], protocol_version=3)
        self.usa_session = self.usa_cluster.connect()
    
        self.usa_redis = redis.Redis(host="10.179.67.118", port=6379, db=1)
    
        handler = logging.handlers.TimedRotatingFileHandler("/data/logs/meow.out", when='D', interval=1)
        fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
        
        formatter = logging.Formatter(fmt)  # 实例化formatter
        handler.setFormatter(formatter)  # 为handler添加formatter
        self.logger = logging.getLogger('meow')  # 获取名为tst的logger
        self.logger.addHandler(handler)  # 为logger添加handler
        self.logger.setLevel(logging.DEBUG)
    
    def api_request(self, host='info_ex.api.imyoujia.com', port=80, method='POST', uri=None, body=None):
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
    
    def photo_upload(self, host='resource.api.imyoujia.com', port=80, method='PUT', uri=None, key=None):
        if uri is None or key is None: 
            self.logger.info('Not file upload')
            return 
        try:
            url = 'https://d1uk5e10lg6nan.cloudfront.net/j%s.jpg' % key
            url = 'http://medical.8ops.com/images/2016/04/14/1a.jpg'
            conn = httplib2.Http()
            resp, body = conn.request(url, 'GET')
            self.logger.info('Download {"status":%s}' % resp['status'])
            
            headers = {'Content-Type': 'image/jpeg;charset=UTF-8', 'Content-Length':0}
            conn = httplib.HTTPConnection(host, port, timeout=2000)
            conn.request(method, uri, body=body, headers=headers)
            response = conn.getresponse()
            status = response.status
            self.logger.info(status)
            self.logger.info(response.read())
        except Exception as ex:
            self.logger.warn('Exception %s' % str(ex))
    
    def test(self):
        cur = self.usa_mysql.cursor()
        m_sql = 'select * from minus_user where username="8ops2016"'
        cur.execute(m_sql)
        rows = cur.fetchall()
        user = rows[0]
        
        uid = user[0]
        
        m_sql = 'select * from minus_userbirthdate where user_id=%s' % uid
        cur.execute(m_sql)
        rows = cur.fetchall()
        birthdate = rows[0]
        
        m_sql = 'select * from minus_usergender where user_id=%s' % uid
        cur.execute(m_sql)
        rows = cur.fetchall()
        gender = rows[0]
        
        cur.close()
        
        print uid, user, birthdate, gender
        
        #### ====>  cass
        
        # balance
        for balance in  self.usa_session.execute('SELECT coins,score FROM users.score WHERE uid=%s;' % uid):
            coins = balance.coins if balance.coins is not None else 0
            score = balance.score if balance.score is not None else 0
            print coins, score
        
        # photo
        for item in self.usa_session.execute('SELECT item_id,dt FROM items.userline WHERE uid=%s;' % uid):
            for key in self.usa_session.execute('SELECT view_id FROM items.dict WHERE item_id=%s;' % item.item_id):
                print key.view_id, item.dt
        
        # relation
        for er in self.usa_session.execute('SELECT * FROM cb.cb_er_dt WHERE follower_id=%s;' % uid):
            print er.follower_id, er.followee_id, er.dt
    
        for ee in self.usa_session.execute('SELECT * FROM cb.cb_ee_dt WHERE followee_id=%s;' % uid):
            print ee.follower_id, ee.followee_id, ee.dt
            
            
        #### ====> request
        
        # account
        print '========> account'
        uri = '/moplus-service/meow/import/useraccount'
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
        print payload
        self.api_request(uri=uri, body=simplejson.dumps(payload))
        
        # facebook
        if user[19] != '':
            print '========> facebook'
            payload = {
                        "nick_name": str(user[24]),
                        "username": str(user[1]),
                        "password": str(user[3]),
                        "email": str(user[2]),
                        "sign_type": "16",
                        "user_id": str(user[0]),
                        "au_id": "20",
                        "security_token": str(user[19]),
                        "access_token": ""
                    }
            print payload
            self.api_request(uri=uri, body=simplejson.dumps(payload))
        
        # twitter
        if user[16] != '':
            print '========> twitter'
            payload = {
                        "nick_name": str(user[24]),
                        "username": str(user[1]),
                        "password": str(user[3]),
                        "email": str(user[2]),
                        "sign_type": "17",
                        "user_id": str(user[0]),
                        "au_id": "20",
                        "security_token": str(user[16]),
                        "access_token": str(user[17])
                    }
            print payload
            self.api_request(uri=uri, body=simplejson.dumps(payload))
        
        # profile
        print '========> profile'
        uri = '/moplus-service/meow/import/userprofile'
        payload = {
                    "birthday": str(birthdate[1]),
                    "fans_count": "0",
                    "sign_type": "20",
                    "gift_count": "0",
                    "avatarid": "0",
                    "oauth_bind": "20",
                    "nick_name": str(user[24]),
                    "first_client_version": "5.1.0-test",
                    "balance": str(coins),
                    "reg_finish_datetime": str(user[5]),
                    "glamour_count": str(score),
                    "client_type": "8",
                    "intruduction": str(user[8]),
                    "avatar_status": "2",
                    "name": str(user[7]),
                    "ua": "meow",
                    "gender": str(gender[1]),
                    "user_id": str(uid),
                    "id": str(uid),
                    "login_count": "0",
                    "client_version":"5.1.0-test"
                 }
        print payload
        self.api_request(uri=uri, body=simplejson.dumps(payload))
        
        # reloation
        print '========> relatioin'
        uri = '/moplus-service/meow/import/relation'
        er_list = []
        for er in self.usa_session.execute('SELECT * FROM cb.cb_er_dt WHERE follower_id=%s;' % uid):
            print er, str(er.dt)
            er_list.append({
                            "fromUserId":str(uid),
                            "toUserId":str(er.followee_id),
                            "isLiked":"1",
                            "createTime": time.mktime(time.strptime(str(er.dt)[0:18], '%Y-%m-%d %H:%M:%S'))
                     })
        payload = {
                   "list":er_list,
                   "uid":str(uid),
                   "type":"0"
               }
        print payload
        self.api_request(uri=uri, body=simplejson.dumps(payload))

        print '========> avator'
        # avator
        uri = '/uplusmain-file/resource_type/101?user_id=%s&albumid=0&optype=1&user_type=3&client_ver=4.0.1-g&token=s00e330000010c43d8ef768417140ca20ce417ba75c41be1c304cdda55efd28791048199c2b99261a0a1149' % uid
        key = user[26]
        print key, uri
        self.photo_upload(uri=uri, key=key)
 
        print '========> photo'
        # photo
        uri = '/uplusmain-file/resource_type/101?user_id=%s&albumid=0&optype=0&user_type=3&client_ver=4.0.1-g&token=s00e330000010c43d8ef768417140ca20ce417ba75c41be1c304cdda55efd28791048199c2b99261a0a1149' % uid
        key = 'mJYT32il9pwe'
        print key, uri
        self.photo_upload(uri=uri, key=key)

    def user_account(self, user):
        self.logger.info('========> account')
        try:
            uri = '/moplus-service/meow/import/useraccount'
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
        except Exception as ex:self.logger.warn('Exception %s' % str(ex))
        
        # facebook
        if user[19] != '':
            self.logger.info('========> facebook')
            try:
                payload = {
                            "nick_name": str(user[24]),
                            "username": str(user[1]),
                            "password": str(user[3]),
                            "email": str(user[2]),
                            "sign_type": "16",
                            "user_id": str(user[0]),
                            "au_id": "20",
                            "security_token": str(user[19]),
                            "access_token": ""
                        }
                self.logger.info(simplejson.dumps(payload))
                self.api_request(uri=uri, body=simplejson.dumps(payload))
            except Exception as ex:print 'Exception %s' % str(ex)
        
        # twitter
        if user[16] != '':
            self.logger.info('========> twitter')
            try:
                payload = {
                            "nick_name": str(user[7]if '' == user[24] or None == user[24] else user[24]),
                            "username": str(user[1]),
                            "password": str(user[3]),
                            "email": str(user[2]),
                            "sign_type": "17",
                            "user_id": str(user[0]),
                            "au_id": "20",
                            "security_token": str(user[16]),
                            "access_token": str(user[17])
                        }
                self.logger.info(simplejson.dumps(payload))
                self.api_request(uri=uri, body=simplejson.dumps(payload))
            except Exception as ex:self.logger.warn('Exception %s' % str(ex))
    
    def user_profile(self, user, cur):
        self.logger.info('========> profile')
        try:
            # birthdate
            birthdate_sql = 'select * from minus_userbirthdate where user_id=%s' % user[0]
            birthdate_size = cur.execute(birthdate_sql)
            birthdates = cur.fetchall()
            birthdate = None if 0 == birthdate_size else birthdates[0] 
            
            # gender
            gender_sql = 'select * from minus_usergender where user_id=%s' % user[0]
            gender_size = cur.execute(gender_sql)
            genders = cur.fetchall()
            gender = None if 0 == gender_size else genders[0]
            
            # balance
            coins = score = 0
            for balance in  self.usa_session.execute('SELECT coins,score FROM users.score WHERE uid=%s;' % user[0]):
                coins = balance.coins if balance.coins is not None else 0
                score = balance.score if balance.score is not None else 0
                
            uri = '/moplus-service/meow/import/userprofile'
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
        except Exception as ex:self.logger.warn('Exception %s' % str(ex))
        
    def user_relation(self, user):
        self.logger.info('========> relatioin')
        try:
            uri = '/moplus-service/meow/import/relation'
            er_list = []
            for er in self.usa_session.execute('SELECT * FROM cb.cb_er_dt WHERE follower_id=%s;' % user[0]):
                er_list.append({
                                "fromUserId":str(user[0]),
                                "toUserId":str(er.followee_id),
                                "isLiked":"1",
                                "createTime": time.mktime(time.strptime(str(er.dt)[0:18], '%Y-%m-%d %H:%M:%S'))
                         })
            payload = {
                       "list":er_list,
                       "uid":str(user[0]),
                       "type":"0"
                   }
            self.logger.info(simplejson.dumps(payload))
            self.api_request(uri=uri, body=simplejson.dumps(payload))
        except Exception as ex:self.logger.warn('Exception %s' % str(ex))
        
        try:
            uri = '/moplus-service/meow/import/relation'
            ee_list = []
            for ee in self.usa_session.execute('SELECT * FROM cb.cb_ee_dt WHERE followee_id=%s;' % user[0]):
                ee_list.append({
                                "fromUserId":str(ee.follower_id),
                                "toUserId":str(user[0]),
                                "isLiked":"1",
                                "createTime": time.mktime(time.strptime(str(ee.dt)[0:18], '%Y-%m-%d %H:%M:%S'))
                         })
            payload = {
                       "list":ee_list,
                       "uid":str(user[0]),
                       "type":"1"
                   }
            self.logger.info(simplejson.dumps(payload))
            self.api_request(uri=uri, body=simplejson.dumps(payload))
        except Exception as ex:self.logger.warn('Exception %s' % str(ex))

    def upload_photo(self, user):
        self.logger.info('========> relatioin')
        try:
            self.usa_redis.sadd('S:photo', user[0])
            self.usa_redis.hset('H:%s' % user[0], user[26], 1)  # avator
            
            # photo
            for item in self.usa_session.execute('SELECT item_id,dt FROM items.userline WHERE uid=%s;' % user[0]):
                for ic in self.usa_session.execute('SELECT view_id FROM items.dict WHERE item_id=%s;' % item.item_id):
                    self.usa_redis.hset('H:%s' % user[0], ic.view_id, 0)
                    
            self.logger.info(self.usa_redis.hgetall('H:%s' % user[0]))
        except Exception as ex:self.logger.warn('Exception %s' % str(ex))
        
    def more_user(self, start_uid=0, limit=100):
        cur = self.usa_mysql.cursor()
        while True:
            user_sql = 'select * from minus_user where id>%s limit %d' % (start_uid, limit)
            user_size = cur.execute(user_sql)
            users = cur.fetchall()
            if 0 == user_size : break
            start_uid = users[-1][0]
            
            # convert storage
            for user in users:
                self.logger.info('############## [conver storage] %s ##############' % user[0])
                self.user_account(user)
                self.user_profile(user, cur)
                self.user_relation(user)
                self.upload_photo(user)

            self.usa_redis.bgsave()
            if limit > user_size:break
        
        cur.close()
    
if __name__ == '__main__':

    dump = Dump()
    dump.more_user()
    

    print '\n[%s] Dump over\n' % time.strftime('%Y-%m-%d %H:%M:%S')


