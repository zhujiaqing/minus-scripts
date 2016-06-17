#!/usr/bin/env python

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

def mutli_process():
    import time
    def func(x):
        print x, time.strftime('%H:%M:%S')
        time.sleep(5)
        return x * x
    arr = [i for i in range(10)]
    
    from multiprocessing import Pool as ThreadPool
#     from multiprocessing.dummy import Pool as ThreadPool
    pool = ThreadPool(2)
    results = pool.map(func, arr)
    print results
    pool.close()
    pool.join()


def mutli_thread():
    pass

def test():
    from meowchat_to_youja import Dump
    dump = Dump()
    dump.temp_20160617()

if __name__ == '__main__':
#     task(l_min=10, l_max=200, num=100000)

    mutli_process()
#     mutli_thread()
    
#     test()
    
    print '\nCompleted\n'
