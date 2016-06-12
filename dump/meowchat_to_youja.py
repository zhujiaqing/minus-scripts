#!/usr/bin/env python
# -*- coding:utf8 -*-

import time
import MySQLdb
from cassandra.cluster import Cluster  # @UnresolvedImport

class Dump:
    
    def __init__(self):
        self.usa_mysql = MySQLdb.connect(host='10.231.129.198', user='root', passwd='carlhu', charset='utf8', db='minus', port=3306)
        self.usa_cluster = Cluster(['10.140.244.182', '10.137.127.31'], protocol_version=3)
        self.usa_session = self.usa_cluster.connect()
    
    def user_account(self):
        cur = self.usa_mysql.cursor()
        m_sql = 'select * from minus_user where username="8ops2016"'
        cur.execute(m_sql)
        rows = cur.fetchall()
        
        user = rows[0]
        
        print rows
        cur.close()
        
        uid = user[0]
        
        #### cass
        for coin in  self.usa_session.execute('SELECT coins,score FROM users.score WHERE uid=%s;' % uid):
            print coin.score,coin.coins
        
        for item in self.usa_session.execute('SELECT item_id,dt FROM items.userline WHERE uid=%s;' % uid):
            for key in self.usa_session.execute('SELECT view_id FROM items.dict WHERE item_id=%s;' % item.item_id):
                print key.view_id
        
        for er in self.usa_session.execute('SELECT * FROM cb.cb_er_dt WHERE follower_id=%s;' % uid):
            print er.follower_id, er.followee_id
    
        for ee in self.usa_session.execute('SELECT * FROM cb.cb_ee_dt WHERE followee_id=%s;' % uid):
            print ee.follower_id, ee.followee_id
    
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


