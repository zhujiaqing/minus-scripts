#!/usr/bin/env python
# -*- coding:utf8 -*-

from cassandra.cluster import Cluster  # @UnresolvedImport
import httplib
import logging  # @UnusedImport
import logging.handlers
import sys
import time

import MySQLdb
import httplib2
import redis
import simplejson

reload(sys)
sys.setdefaultencoding("UTF-8")  # @UndefinedVariable

class Dump:
    
    def __init__(self, start_uid=0, stop_uid=1000):
        handler = logging.handlers.TimedRotatingFileHandler("/data/logs/meow_%s-%s.out" % (start_uid, stop_uid), when='D', interval=1)
        fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
        
        formatter = logging.Formatter(fmt)  # 实例化formatter
        handler.setFormatter(formatter)  # 为handler添加formatter
        self.logger = logging.getLogger('meow')  # 获取名为tst的logger
        self.logger.addHandler(handler)  # 为logger添加handler
        self.logger.setLevel(logging.DEBUG)
        
        self.start_uid = start_uid
        self.stop_uid = stop_uid
        self.logger.info('[%s] ============= init task: %s ~ %s =============' % (time.strftime('%Y:%m:%d %H:%M:%S'), start_uid, stop_uid))
        
        self.usa_mysql = MySQLdb.connect(host='10.231.129.198', user='root', passwd='carlhu', charset='utf8', db='minus', port=3306)
        self.usa_cur = self.usa_mysql.cursor()
        
#         self.sg_mysql = MySQLdb.connect(host='54.169.234.201', user='minus', passwd='minus', charset='utf8', db='minus', port=3306)
#         self.sg_cur = self.sg_mysql.cursor()
        
        self.usa_cluster = Cluster(['10.140.244.182', '10.137.127.31'], protocol_version=3)
        self.usa_session = self.usa_cluster.connect()
    
        self.usa_redis = redis.Redis(host="10.154.148.158", port=6379, db=1)
        
#         self.sg_redis = redis.Redis(host="54.169.234.201", port=6379, db=1)
    
    def user_account(self, user):
        self.logger.info('========> account')

        # account_auth
        print 'insert into account_auth(username,email,password,user_id,create_time) values("%s","%s","%s",%s,"%s")' % (
                                                                                                            str(user[1]),
                                                                                                            str(user[2]),
                                                                                                            str(user[3]),
                                                                                                            str(user[0]),
                                                                                                            str(user[5]))
        # user_bind
        print 'insert into user_bind(user_id,bind_type,bind_id) values(%s,%s,%s)' % (user[0], 20, 1)  # TODO cur.lastrowid or con.insert_id()
        
        # facebook_auth
        if user[16] != '':
            print 'insert into facebook_auth(username,nick_name,user_id,access_token,create_time) values("%s","%s","%s",%s,"%s")' % (
                                                                                                           str(user[16]),
                                                                                                           str(user[1]),
                                                                                                           user[0],
                                                                                                           str(user[19]),
                                                                                                           str(user[5]))
            # user_bind
            print 'insert into user_bind(user_id,bind_type,bind_id) values(%s,%s,%s)' % (user[0], 16, 1)  # TODO cur.lastrowid or con.insert_id()
        
        # twitter_auth
        if user[15] != '':
            print 'insert into twitter_auth(username,nick_name,user_id,access_token,create_time) values("%s","%s","%s",%s,"%s")' % (
                                                                                                           str(user[15]),
                                                                                                           str(user[1]),
                                                                                                           user[0],
                                                                                                           str(user[12]),
                                                                                                           str(user[5]))
            # user_bind
            print 'insert into user_bind(user_id,bind_type,bind_id) values(%s,%s,%s)' % (user[0], 17, 1)  # TODO cur.lastrowid or con.insert_id()
    
    def user_profile(self, user):
        self.logger.info('========> profile')
        
        # device_record
        print 'insert into device_record(device_record,user_id,ua,create_time) values("%s",%s,"meow","%s")' % ("deviceid", user[0], str(user[5]))  # TODO create deviceid 
        
        # dvbf47ded24d0f42058c76bfc864ddc7a80000101523342a4a
        
        try:
            # birthdate
            birthdate_sql = 'select * from minus_userbirthdate where user_id=%s' % user[0]
            birthdate_size = self.cur.execute(birthdate_sql)
            birthdates = self.cur.fetchall()
            birthdate = None if 0 == birthdate_size else birthdates[0] 
            
            # gender
            gender_sql = 'select * from minus_usergender where user_id=%s' % user[0]
            gender_size = self.cur.execute(gender_sql)
            genders = self.cur.fetchall()
            gender = None if 0 == gender_size else genders[0]
            
            # user
            print 'insert into user(id,udid,available,gender) values(%s,"%s",1,)' % (
                                                                           user[0],
                                                                           'udid',
                                                                           str('2' if gender is None else gender[1]))
            # 985fcd7c-d6ea-41ce-9c0c-5de5115f237bdv0da6651800a84b00945889886d9f00b30000105567ccade7
            
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
        
    def more_user_with_mutli(self, limit=100):
        while True:
            user_sql = 'select * from minus_user where id>%s and id<%s limit %d' % (self.start_uid, self.stop_uid, limit)
            user_size = self.cur.execute(user_sql)
            users = self.cur.fetchall()
            if 0 == user_size : break
            self.start_uid = users[-1][0]
            
            # convert storage
            for user in users:
                start_time = time.time()
                if self.usa_redis.sismember('S:photo', user[0]):continue  # 避免重复转存
                
                self.logger.info('##############>>> [start conver storage] %s - [%s]' % 
                                 (user[0],
                                  time.strftime('%Y-%m-%d %H:%M:%S')))
                self.user_account(user)
                self.user_profile(user)
                self.user_relation(user)
                self.upload_photo(user)
                self.logger.info('##############>>> [end conver storage] %s - [%s], cost time %ss' % 
                                 (user[0],
                                  time.strftime('%Y-%m-%d %H:%M:%S'),
                                  time.time() - start_time))

            if limit > user_size:break
        else:
            self.cur.close()
            

def manual_start(arg):
    dump = Dump(arg[0], arg[1])
    dump.more_user_with_mutli(arg[2])

def mutliprocess_start_02(salt=0.0):
    max_uid = 20000000
    arg = []
    num = 100000
    limit = 100
    step = int(num * salt)
    for i in range(max_uid / num, max_uid / num):
        arg.append((i * num + step, (i + 1) * num, limit))
    
    from multiprocessing import Pool as JPool  # 多进程
    from multiprocessing import cpu_count
    pool = JPool(10 * cpu_count())
    pool.map(manual_start, arg)
    pool.close()
    pool.join()
    
if __name__ == '__main__':
    print '\n[%s] Dump start\n' % time.strftime('%Y-%m-%d %H:%M:%S')
    args = sys.argv
    
    
    print '\n[%s] Dump over\n' % time.strftime('%Y-%m-%d %H:%M:%S')


