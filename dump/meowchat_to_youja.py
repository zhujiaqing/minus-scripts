#!/usr/bin/env python
# -*- coding:utf8 -*-

import time
import MySQLdb
from cassandra.cluster import Cluster  # @UnresolvedImport
from StdSuites.Table_Suite import rows

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
        
        print rows
        cur.close()
    
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


