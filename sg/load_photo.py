#!/usr/bin/env python
# -*-coding:UTF8-*-

################################################################################
#  execute env
#    host: 172.16.141.10
#
################################################################################
import time

import MySQLdb
import redis


class loading():
    """
        1, 121.10 uplusmian photo_user_index 插入user_id获取ID；
        2, 121.10 uplus-resource photos 通过上面的ID插入一条记录；
        3, 121.20 uplusmain user_info 更新avator对应上面的ID。
    """
    
    def __init__(self):
        self.sg_redis_2 = redis.Redis(host="172.16.141.10", port=6666, db=2)  # 头像
        self.sg_redis_3 = redis.Redis(host="172.16.141.10", port=6666, db=3)  # 相册
        self.sg_redis_10 = redis.Redis(host="172.16.141.10", port=6666, db=10)  # 任务
    
        self.sg_mysql_10 = MySQLdb.connect(host='172.16.121.10', user='uplus', passwd='q1w2e3r4t5', charset='utf8', db='uplusmain', port=3306)
        self.sg_mysql_20 = MySQLdb.connect(host='172.16.121.20', user='uplus', passwd='q1w2e3r4t5', charset='utf8', db='uplusmain', port=3306)
        
        self.sg_mysql_10_resource = MySQLdb.connect(host='172.16.121.10', user='uplus', passwd='q1w2e3r4t5', charset='utf8', db='uplus_resource', port=3306)
    
    def pop(self):
        sg_cur_10 = self.sg_mysql_10.cursor()
        sg_cur_20 = self.sg_mysql_20.cursor()
        sg_cur_10_resource = self.sg_mysql_10_resource.cursor()
        
        while True:
            uid = self.sg_redis_10.spop('S:s3file')
            if uid is None: break
            
            print uid
            
            try:
                
                sg_cur_10.execute('insert into photo_user_index(user_id) values(%s)' % uid)
                
                print sg_cur_10.lastrowid
                print self.sg_mysql_10.insert_id()
            
                self.sg_mysql_10.commit()
            
            
            
                self.sg_mysql_10.sadd('S:s3file:after')
            except:
                self.sg_redis_10.sadd('S:error:after', uid)
            
            break
        else:
            sg_cur_10.close()
            sg_cur_20.close()
            sg_cur_10_resource.close()
    
    def close_all(self):
        self.sg_mysql_10.close()
        self.sg_mysql_20.close()
        self.sg_mysql_10_resource.close()

if __name__ == '__main__':
    load = loading()
    load.pop()

    print '\n[%s] Completed \n' % (time.strftime('%Y-%m-%d %H:%M:%S'))
    

