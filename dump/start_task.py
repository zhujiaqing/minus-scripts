#!/usr/bin/env python

num = 10000

def task_20160614():
    start = stop = 0
    for i in range(20, 100):
        start = (i - 1) * num
        stop = i * num
        print 'screen python meowchat_to_youja.py %d %d &\n' % (start, stop)
        

    print 'screen python meowchat_to_youja.py %d 200000000 &\n' % stop

if __name__ == '__main__':
    task_20160614()
    
    print 'Init task over.'
    print '''
    May be: 
        redis-cli -h 10.154.148.158 -p 6379 -n 1 scard S:photo
        
        tree /data
        
        ls -lth /data/logs
        
    '''
    
    
