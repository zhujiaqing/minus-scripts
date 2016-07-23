#!/usr/bin/env python
# -*- coding:utf8 -*-

import os

import redis


def check_by_file(uid_file):
    if uid_file is None or not os.path.exists(uid_file):return
    
    r_rf = redis.Redis(host='user_relation.redis.youja.cn', port=6802, db=5)
    r_rt = redis.Redis(host='user_relation.redis.youja.cn', port=6802, db=8)

    with open(uid_file, 'r') as uf:
        for uid in uf.readlines():
            print '%12s - rf - %04d' % (uid[:-2], r_rf.zcard('U:rf:%s' % uid))
            print '%12s - rf - %4s' % (uid[:-1], r_rt.zcard('U:rt:%s' % uid))
            break
        
        uf.close()
        
if __name__ == '__main__':
    check_by_file(uid_file='/home/jesse/backup/uid-20160723')
    
    print '\nCompleted\n'

