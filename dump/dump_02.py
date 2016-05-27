#!/usr/bin/env python
# -*- codding:utf8 -*-

import MySQLdb
mysql = MySQLdb.connect(host='54.169.188.17', user='minus', passwd='minus', charset='utf8', db='minus', port=3306)

from cassandra.cluster import Cluster
cluster = Cluster(['10.140.244.182'], protocol_version=3)
session = cluster.connect('items')

def init_uids():
    cur = mysql.cursor()
    cur.execute('SELECT id FROM minus_user')
    data = cur.fetchall()
    cur.close()
    return [int(item[0]) for item in data]

def dump_score(uids):
    for uid in uids:
        for item in session.execute('SELECT * FROM items.userline WHERE uid = %d;' % uid):
            print item

def dump_relation(uids):
    pass

def dump_photo(uids):
    pass

if __name__ == '__main__':
    uids = init_uids()
    
    dump_score(uids)
    dump_relation(uids)
    dump_photo(uids)

    print '\nDump over\n'


