#!/usr/bin/env python
# -*-coding:UTF8-*-

import os
import time

import redis

def task(l_min=0, l_max=1, num=10000):
    print 'screen python ~/minus-scripts/dump/meowchat_to_youja.py 0 %d &\n' % (l_min * num)
    print 'screen python ~/minus-scripts/dump/meowchat_to_youja.py %d 200000000 &\n' % (l_max * num)
    print '\n\n'
    
    for n in range(l_min, l_max, 1):
        print 'screen python ~/minus-scripts/dump/meowchat_to_youja.py %d %d &\n' % (n * num , (n + 1) * num)
   
    print 'Init task over.\n'
    print '''
    May be: 
         
        uptime
         
        redis-cli -h 10.154.148.158 -p 6379 -n 1 scard S:photo
         
        tree /data
         
        ls -lth /data/logs
         
    '''

def func(arg):
    print time.strftime('%H:%M:%S'), arg[0], arg[1]
    time.sleep(2)
    return arg[0] * arg[1]

def mutli_process():
    arr = [(i, i + 1) for i in range(10)]
    print arr

#     from multiprocessing.dummy import Pool as JPool   # 多线程
    from multiprocessing import Pool as JPool  # 多进程
    from multiprocessing import cpu_count
    pool = JPool(cpu_count())
    results = pool.map(func, arr)
    print results
    pool.close()
    pool.join()

def test():
    from meowchat_to_youja import Dump
    dump = Dump()
#     uids = (17172928, 12011768, 15253309)
    uids = (3407830, 3417163, 3517440, 3826975, 3617153, 4224119, 3007666, 2908330, 3707875)
    dump.repair(uids)
    dump.close_all()

def thread_func(thread_name, delay):
    while True:
        print time.strftime('%y-%m-%d %H:%M:%S')
        time.sleep(delay)

def test_thread():
    import thread
    thread.start_new_thread(thread_func, ('Thread_test', 2,))
    while True:
        print 'Main'
        time.sleep(10)

def watch():
    try:
        base_redis = redis.Redis(host="10.154.148.158", port=6379, db=5)
        delay = 5
        while True:
            b_timestamp = base_redis.hget('H:scale', 'meow_timestamp')
            b_num = base_redis.hget('H:scale', 'meow_num')
            b_timestamp = 0 if b_timestamp is None else int(b_timestamp)
            b_num = 0 if b_num is None else int(b_num)
            
            os.system('clear')
            
            info = base_redis.info('Keyspace')
            num = info['db1']['keys']
            timestamp = int(time.time())
            base_redis.hset('H:scale', 'meow_num', num)
            base_redis.hset('H:scale', 'meow_timestamp', timestamp)
            
            print 'meow: 已经导入 %s ，%d 内秒速 %s 个/s' % (
                                      format(num, ','),
                                      timestamp - b_timestamp,
                                      format((num - b_num) / (timestamp - b_timestamp), ',')
                                      )
            
            time.sleep(delay)

    except: pass

def repair(process_num=15):
    from multiprocessing import Pool as JPool  # 多进程
    from multiprocessing import cpu_count
    
    from meowchat_to_youja import Dump
    dump = Dump()
    
    base_redis = redis.Redis(host="10.154.148.158", port=6379, db=10)
    
    pool = JPool(process_num * cpu_count())
    pool.map(dump.repair, [base_redis.rpop('L:diff') for i in range(100)])  # @UnusedVariable
    pool.close()
    pool.join()

if __name__ == '__main__':
#     test()
    watch()

    print '\nCompleted\n'