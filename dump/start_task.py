#!/usr/bin/env python

num = 10000

def task(l_min=0, l_max=100):
    start = stop = 0
    for i in range(l_min, l_max):
        start = (i - 1) * num
        stop = i * num
        print 'screen python ~/minus-scripts/dump/meowchat_to_youja.py %d %d &\n' % (start, stop)
    else:
        print 'screen python ~/minus-scripts/dump/meowchat_to_youja.py 0 %d &\n' % l_min * num
        print 'screen python ~/minus-scripts/dump/meowchat_to_youja.py %d 200000000 &\n' % l_max * num

if __name__ == '__main__':
    task(l_min=20, l_max=21)
    
    print 'Init task over.\n'
    print '''
    May be: 
        
        uptime
        
        redis-cli -h 10.154.148.158 -p 6379 -n 1 scard S:photo
        
        tree /data
        
        ls -lth /data/logs
        
    '''
    
    
