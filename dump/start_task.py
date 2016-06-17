#!/usr/bin/env python
# -*-coding:UTF8-*-

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

import time
def func(arg):
    print time.strftime('%H:%M:%S'), arg[0], arg[1]
    time.sleep(2)
    return arg[0] * arg[1]

def mutli_process():
    arr = [(i, i + 1) for i in range(10)]
    print arr
#     import multiprocessing
#     pool = multiprocessing.Pool(multiprocessing.cpu_count)
    
#     from multiprocessing.dummy import Pool as JPool   # 多线程
    from multiprocessing import Pool as JPool  # 多进程
    pool = JPool(2)
    results = pool.map(func, arr)
    print results
    pool.close()
    pool.join()

def test():
    from meowchat_to_youja import Dump
    dump = Dump()
    dump.temp_20160617()

if __name__ == '__main__':
#     task(l_min=10, l_max=200, num=100000)

    mutli_process()
    
#     test()
    
    print '\nCompleted\n'
