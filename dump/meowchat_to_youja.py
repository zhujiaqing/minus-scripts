#!/usr/bin/env python
# -*- coding:utf8 -*-

import time
import httplib
import httplib2
import simplejson
import MySQLdb
from cassandra.cluster import Cluster  # @UnresolvedImport

import sys
reload(sys)
sys.setdefaultencoding("UTF-8")  # @UndefinedVariable

class Dump:
    
    def __init__(self):
        self.usa_mysql = MySQLdb.connect(host='10.231.129.198', user='root', passwd='carlhu', charset='utf8', db='minus', port=3306)
        self.usa_cluster = Cluster(['10.140.244.182', '10.137.127.31'], protocol_version=3)
        self.usa_session = self.usa_cluster.connect()
    
    def api_request(self, host='info_ex.api.imyoujia.com', port=80, method='POST', uri=None, body=None):
        if uri is None or body is None: 
            print 'Not api request'
            return 
        try:
            headers = {'Content-Type': 'application/json;charset=UTF-8'}
            conn = httplib.HTTPConnection(host, port, timeout=2000)
            conn.request(method, uri, body=body, headers=headers)
            response = conn.getresponse()
            status = response.status
            print status, response.read()
        except Exception as ex:
            print 'Exception %s' % str(ex)
    
    def photo_upload(self, host='resource.api.imyoujia.com', port=80, method='PUT', uri=None, key=None):
        if uri is None or key is None: 
            print 'Not file upload'
            return 
        try:
            url = 'https://d1uk5e10lg6nan.cloudfront.net/j%s.jpg' % key
            conn = httplib2.Http()
            resp, body = conn.request(url, 'GET')
            print 'Download {"status":%s}' % resp['status']
            
            headers = {'Content-Type': 'image/jpeg;charset=UTF-8', 'Content-Length':0}
            conn = httplib.HTTPConnection(host, port, timeout=2000)
            conn.request(method, uri, body=body, headers=headers)
            response = conn.getresponse()
            status = response.status
            print status, response.read()
        except Exception as ex:
            print 'Exception %s' % str(ex)
    
    def more_user(self, start_uid=0):
        cur = self.usa_mysql.cursor()
        m_sql = 'select * from minus_user where username="8ops2016" or username="atschx"'
        cur.execute(m_sql)
        rows = cur.fetchall()
    
    def user_account(self):
        cur = self.usa_mysql.cursor()
        m_sql = 'select * from minus_user where username="8ops2016" or username="atschx"'
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
        uri = '/moplus-service/meow/import/useraccount'
        payload = {
                    "nick_name": user[24],
                    "username": user[1],
                    "password": user[3],
                    "email": user[2],
                    "sign_type": "20",
                    "user_id": user[0],
                    "au_id": "20",
                    "security_token": "20",
                    "access_token": "20"
                    }
        print payload
        self.api_request(uri=uri, body=simplejson.dumps(payload))
        
        # facebook
        if user[19] != '':
            payload = {
                        "nick_name": user[24],
                        "username": user[1],
                        "password": user[3],
                        "email": user[2],
                        "sign_type": "16",
                        "user_id": user[0],
                        "au_id": "20",
                        "security_token": user[19],
                        "access_token": ""
                    }
            print payload
            self.api_request(uri=uri, body=simplejson.dumps(payload))
        
        # twitter
        if user[16] != '':
            payload = {
                        "nick_name": user[24],
                        "username": user[1],
                        "password": user[3],
                        "email": user[2],
                        "sign_type": "17",
                        "user_id": user[0],
                        "au_id": "20",
                        "security_token": user[16],
                        "access_token": user[17]
                    }
            print payload
            self.api_request(uri=uri, body=simplejson.dumps(payload))
        
        uri = '/moplus-service/meow/import/userprofile'
        payload = {
                    "birthday": gender[1],
                    "fans_count": "0",
                    "sign_type": "20",
                    "gift_count": "0",
                    "avatarid": "0",
                    "oauth_bind": "20",
                    "nick_name": user[24],
                    "first_client_version": "5.1.0-test",
                    "balance": coins,
                    "reg_finish_datetime": str(user[5]),
                    "glamour_count": score,
                    "client_type": "8",
                    "intruduction": user[8],
                    "avatar_status": "2",
                    "name": user[7],
                    "ua": "meow",
                    "gender": "1",
                    "user_id": uid,
                    "id": uid,
                    "login_count": "0",
                    "client_version":"5.1.0-test"
                 }
        print payload
        self.api_request(uri=uri, body=simplejson.dumps(payload))
        
        # TODO reloation list
        uri = '/moplus-service/meow/import/relation'
        payload = {
                   "list":[
                           {"fromUserId":uid,
                            "toUserId":"10000",
                            "isLiked":"1",
                            "createTime":"1465363249529"
                            }
                           ],
                   "uid":"1",
                   "type":"0"
               }
        print payload
        self.api_request(uri=uri, body=simplejson.dumps(payload))

        # avator
        uri = '/uplusmain-file/resource_type/101?user_id=%s&albumid=0&optype=1&user_type=3&client_ver=4.0.1-g&token=s00e330000010c43d8ef768417140ca20ce417ba75c41be1c304cdda55efd28791048199c2b99261a0a1149' % uid
        self.photo_upload(uri=uri, key=user[26])

        # photo
        uri = '/uplusmain-file/resource_type/101?user_id=%s&albumid=0&optype=0&user_type=3&client_ver=4.0.1-g&token=s00e330000010c43d8ef768417140ca20ce417ba75c41be1c304cdda55efd28791048199c2b99261a0a1149' % uid
        self.photo_upload(uri=uri, key='mJYT32il9pwe')

    def user_profile(self):
        pass
    
    def user_relation(self):
        pass

    def upload_photo(self):
        pass

if __name__ == '__main__':

    dump = Dump()
    dump.user_account()
    

    print '\n[%s] Dump over\n' % time.strftime('%Y-%m-%d %H:%M:%S')


