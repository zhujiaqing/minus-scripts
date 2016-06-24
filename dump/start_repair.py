#!/usr/bin/env python
# -*-coding:UTF8-*-

import redis

from meowchat_to_youja import Dump


def manual_start(x):
    dump = Dump(start_uid=x)
    
    usa_redis_10 = redis.Redis(host="10.154.148.158", port=6379, db=10)
    uids = [usa_redis_10.spop('S:diff') for i in range(100)]  # @UnusedVariable
    dump.repair(uids)
    dump.close_all()

def manual_start_with_pop(x):
    dump = Dump(start_uid=x)
    
    usa_redis_10 = redis.Redis(host="10.154.148.158", port=6379, db=10)
    dump.repair_with_pop(usa_redis_10)
    dump.close_all()

def repair(process_num=10):
    from multiprocessing import Pool as JPool  # 多进程
    from multiprocessing import cpu_count
    
    pool = JPool(process_num * cpu_count())
#     pool.map(manual_start, (i for i in range(100))) 
    pool.map(manual_start_with_pop, (i for i in range(process_num * cpu_count()))) 
    pool.close()
    pool.join()

if __name__ == '__main__':
    repair()

    print '\nCompleted\n'
