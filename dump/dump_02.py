#!/usr/bin/env python
# -*- codding:utf8 -*-

import MySQLdb
sg_mysql = MySQLdb.connect(host='54.169.188.17', user='minus', passwd='minus', charset='utf8', db='minus', port=3306)

from cassandra.cluster import Cluster
usa_cluster = Cluster(['10.140.244.182', '10.137.127.31'], protocol_version=3)
usa_session = usa_cluster.connect()

def init_uids():
    cur = sg_mysql.cursor()
    cur.execute('SELECT id FROM minus_user')
    data = cur.fetchall()
    cur.close()
    return [item[0] for item in data]

def dump_score(uids):
    cur = sg_mysql.cursor()

    for uid in uids:
        for item in usa_session.execute('SELECT coins,score FROM users.score WHERE uid=%s;' % uid):
            coins = item.coins if item.coins is not None else 0
            score = item.score if item.score is not None else 0
            
            cur.execute('SELECT * FROM minus_user_score WHERE uid=%s' % uid)
            row = cur.fetchone()
            
            if row is None:
                cur.execute('INSERT INTO minus_user_score(uid,coins,item) VALUES(%s,%s,%s)' % (uid, coins, score))
            else:
                cur.execute('UPDATE minus_user_score set coins=%s,item=%s where uid=%s' % (coins, score, uid))
            sg_mysql.commit()
            
    cur.close()

def dump_relation(uids):
    pass

def dump_photo(uids):
    cur = sg_mysql.cursor()
    
    for uid in uids:
        for item in usa_session.execute('SELECT item_id,dt FROM items.userline WHERE uid=%s;' % uid):
            item_id = item.view_id
            create_time = item.dt
            
            for ic in usa_session.execute('SELECT view_id FROM dict WHERE item_id=%s;' % item_id):
                photo_key = ic.view_id
                
                cur.execute('SELECT * FROM minus_user_photo WHERE uid=%s and photo_key=%s' % (uid, photo_key))
                row = cur.fetchone()
                
                if row is None:
                    cur.execute('INSERT INTO minus_user_photo(uid,photo_key,create_time) VALUES(%s,%s,%s)' % (uid, photo_key, create_time))
                else:
                    cur.execute('UPDATE minus_user_photo set photo_key="%s",create_time="%s" where uid=%s' % (photo_key, create_time, uid))
            sg_mysql.commit()
    
    cur.close()

if __name__ == '__main__':
    uids = init_uids()
    
    dump_score(uids)
    dump_relation(uids)
    dump_photo(uids)

    print '\nDump over\n'


