w#!/usr/bin/env python
# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')  # @UndefinedVariable

import time
import MySQLdb
from cassandra.cluster import Cluster  # @UnresolvedImport

cluster = Cluster(['10.10.10.184'])
session = cluster.connect('uplus_im')

def batch_insert_to_cstar(cur,data,size):
    for group in data:
        session.execute("""insert into group (id,owner_id,name,create_time) values (%s,%s,%s,%s);""",(group[0],group[1],group[2],group[3]))
    print "-----%s" % time.time()
    if(len(data)==size):
        batch_insert_to_cstar(cur,cur.fetchmany(size),size)

def migrate_group_to_cstar(size=2000):
    mysql_conn_group = MySQLdb.connect(host='10.10.10.123',user='moplus',passwd='Wd36sRpt182jENTTGxVf',charset='utf8',db='uplusim',port=3306)
    cur = mysql_conn_group.cursor()
    cur.execute("select group_id,owner_id,group_name,create_time from `group`" )
    batch_insert_to_cstar(cur,cur.fetchmany(size),size)
    cur.close()
    mysql_conn_group.close()

if __name__ == '__main__':
    migrate_group_to_cstar()

