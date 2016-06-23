#!/usr/bin/env python
# -*-coding:UTF8-*-

import redis

from meowchat_to_youja import Dump


def manual_start(x):
    dump = Dump()
    
    base_redis = redis.Redis(host="10.154.148.158", port=6379, db=10)
    uids = [16876896, 16876897, 16876898, 16876899]
    dump.repair(uids)
    dump.close_all()

def repair(process_num=20):
    from multiprocessing import Pool as JPool  # 多进程
    from multiprocessing import cpu_count
    
    
#     pool = JPool(process_num * cpu_count())
    pool = JPool(2)
    pool.map(manual_start, (i for i in range(3))) 
    pool.close()
    pool.join()

if __name__ == '__main__':
    repair()

    print '\nCompleted\n'
