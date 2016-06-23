#!/usr/bin/env python
# -*-coding:UTF8-*-

import redis
from dump.test import repair

def repair(process_num=20):
    from multiprocessing import Pool as JPool  # 多进程
    from multiprocessing import cpu_count
    
    from meowchat_to_youja import Dump
    dump = Dump()
    
    base_redis = redis.Redis(host="10.154.148.158", port=6379, db=10)
    
    pool = JPool(process_num * cpu_count())
    pool.map(dump.repair, [base_redis.rpop('L:diff') for i in range(1000)])  # @UnusedVariable
    pool.close()
    pool.join()

if __name__ == '__main__':
    repair()

    print '\nCompleted\n'
