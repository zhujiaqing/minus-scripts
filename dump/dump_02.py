#!/usr/bin/env python
# -*- coding:utf8 -*-

import MySQLdb
sg_mysql = MySQLdb.connect(host='54.169.188.17', user='minus', passwd='minus', charset='utf8', db='minus', port=3306)

from cassandra.cluster import Cluster
usa_cluster = Cluster(['10.140.244.182', '10.137.127.31'], protocol_version=3)
usa_session = usa_cluster.connect()

def init_uids():
    """
    以现有初始化用户的表为准
    """
    uids = []
    
    try:
        cur = sg_mysql.cursor()
        cur.execute('SELECT id FROM minus_user')
        data = cur.fetchall()
        cur.close()
        
        uids = [item[0] for item in data]
    except Exception as ex:
        print 'uids', str(ex)
        
    return uids


def dump_score(uids):
    cur = sg_mysql.cursor()

    for uid in uids:
        for item in usa_session.execute('SELECT coins,score FROM users.score WHERE uid=%s;' % uid):
            try:
                coins = item.coins if item.coins is not None else 0
                score = item.score if item.score is not None else 0
                
                cur.execute('SELECT * FROM minus_user_score WHERE uid=%s' % uid)
                row = cur.fetchone()
                
                if row is None:
                    cur.execute('INSERT INTO minus_user_score(uid,coins,score) VALUES(%s,%s,%s)' % (uid, coins, score))
                else:
                    cur.execute('UPDATE minus_user_score set coins=%s,score=%s where uid=%s' % (coins, score, uid))
                sg_mysql.commit()
            except Exception as ex:
                print 'score', uid, str(ex)
            
    cur.close()

def dump_photo(uids):
    cur = sg_mysql.cursor()
    
    for uid in uids:
        for item in usa_session.execute('SELECT item_id,dt FROM items.userline WHERE uid=%s;' % uid):
            try:
                item_id = item.item_id
                create_time = item.dt
                
                for ic in usa_session.execute('SELECT view_id FROM items.dict WHERE item_id=%s;' % item_id):
                    photo_key = ic.view_id
                    
                    cur.execute('SELECT * FROM minus_user_photo WHERE uid=%s and photo_key="%s"' % (uid, photo_key))
                    row = cur.fetchone()
                    
                    if row is None:
                        cur.execute('INSERT INTO minus_user_photo(uid,photo_key,create_time) VALUES(%s,"%s","%s")' % (uid, photo_key, create_time))
                sg_mysql.commit()
            except Exception as ex:
                print 'photo', uid, str(ex)
                
    cur.close()

def dump_relation(uids):
    cur = sg_mysql.cursor()

    for uid in uids:
        for item in usa_session.execute('SELECT * FROM cb.cb_er_dt WHERE follower_id=%s;' % uid):
            try:
                follower_id = item.follower_id
                followee_id = item.followee_id
                create_time = item.dt
                
                cur.execute('SELECT * FROM minus_user_follower WHERE follower_id=%s and followee_id=%s' % (follower_id, followee_id))
                row = cur.fetchone()
                
                if row is None:
                    cur.execute('INSERT INTO minus_user_follower(follower_id,followee_id,create_time) VALUES(%s,%s,"%s")' % (follower_id, followee_id, create_time))
                sg_mysql.commit()
            except Exception as ex:
                print 'follwer', uid, str(ex)
        
        for item in usa_session.execute('SELECT * FROM cb.cb_ee_dt WHERE followee_id=%s;' % uid):
            try:
                follower_id = item.follower_id
                followee_id = item.followee_id
                create_time = item.dt
                
                cur.execute('SELECT * FROM minus_user_followee WHERE follower_id=%s and followee_id=%s' % (follower_id, followee_id))
                row = cur.fetchone()
                
                if row is None:
                    cur.execute('INSERT INTO minus_user_followee(follower_id,followee_id,create_time) VALUES(%s,%s,"%s")' % (follower_id, followee_id, create_time))
                sg_mysql.commit()
            except Exception as ex:
                print 'follwee', uid, str(ex)
        
    cur.close()

if __name__ == '__main__':
    uids = init_uids()
     
    dump_score(uids)
    dump_photo(uids)
    dump_relation(uids)

    print '\nDump over\n'


