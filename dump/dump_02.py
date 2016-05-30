#!/usr/bin/env python
# -*- codding:utf8 -*-

import MySQLdb
sg_mysql = MySQLdb.connect(host='54.169.188.17', user='minus', passwd='minus', charset='utf8', db='minus', port=3306)

from cassandra.cluster import Cluster
usa_cluster = Cluster(['10.140.244.182', '10.137.127.31'], protocol_version=3)
usa_session = usa_cluster.connect('items')

def init_uids():
    cur = sg_mysql.cursor()
    cur.execute('SELECT id FROM minus_user')
    data = cur.fetchall()
    cur.close()
    return [int(item[0]) for item in data]

def dump_score(uids):
    cur = sg_mysql.cursor()

    for uid in uids:
        for item in usa_session.execute('SELECT * FROM users.score WHERE uid=%d;' % uid):
            num = cur.execute('select * from minus_user_coins where uid=%s' % item.uid if item.uid is not None else 0)
            print num
            print cur.fetchone()
            
            cur.execute('INSERT INTO minus_user_coins(user_id,coins,score) VALUES(%s,%s,%s)',
                        (item.uid if item.uid is not None else 0,
                         item.coins if item.coins is not None else 0,
                         item.score if item.score is not None else 0))
            sg_mysql.commit()
            
    cur.close()

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


