#!/usr/bin/env python
# -*-coding:UTF8-*-

import redis

def repair(process_num=20):
    from multiprocessing import Pool as JPool  # 多进程
    from multiprocessing import cpu_count
    
    from meowchat_to_youja import Dump
    dump = Dump()
    
    base_redis = redis.Redis(host="10.154.148.158", port=6379, db=10)
    
#     pool = JPool(process_num * cpu_count())
    pool = JPool(2)
#     pool.map(dump.repair, [base_redis.spop('S:diff') for i in range(10)])  # @UnusedVariable

    pool.map(dump.repair, set([[16876896, 16876897, 16876898], [16876899, 16876900, 16876901], [16876902, 16876903, 16876904, 17171928]])) 
    pool.close()
    pool.join()

if __name__ == '__main__':
    repair()

    print '\nCompleted\n'
