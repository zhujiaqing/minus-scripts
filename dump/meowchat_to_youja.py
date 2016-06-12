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
    
    def _request(self, host='info_ex.api.imyoujia.com', port=80, method='POST', uri, body=None):
        headers = {'Content-Type': 'application/json;charset=UTF-8'}
        conn = httplib.HTTPConnection(host, port, timeout=2000)
        conn.request(method, uri, body=body, headers=headers)
        response = conn.getresponse()
        print response.read()
        status = response.status
        print status
    
    def user_account(self):
        cur = self.usa_mysql.cursor()
        m_sql = 'select * from minus_user where username="8ops2016"'
        cur.execute(m_sql)
        rows = cur.fetchall()
        
        user = rows[0]
        
        print rows
        cur.close()
        
        uid = user[0]
        
        #### ====>  cass
        
        # balance
        for balance in  self.usa_session.execute('SELECT coins,score FROM users.score WHERE uid=%s;' % uid):
            coins = balance.coins if balance.coins is not None else 0
            score = balance.score if balance.score is not None else 0
            print coins, score
        
        # photo
        for item in self.usa_session.execute('SELECT item_id,dt FROM items.userline WHERE uid=%s;' % uid):
            for key in self.usa_session.execute('SELECT view_id FROM items.dict WHERE item_id=%s;' % item.item_id):
                print key.view_id
        
        # relation
        for er in self.usa_session.execute('SELECT * FROM cb.cb_er_dt WHERE follower_id=%s;' % uid):
            print er.follower_id, er.followee_id
    
        for ee in self.usa_session.execute('SELECT * FROM cb.cb_ee_dt WHERE followee_id=%s;' % uid):
            print ee.follower_id, ee.followee_id
            
            
        #### ====> request
        uri = '/moplus-service/meow/import/useraccount'
        payload = {"nick_name": user[24],
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
        self._request(host='info_ex.api.imyoujia.com', port=80, method='POST', uri=uri, body=payload)

    
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


