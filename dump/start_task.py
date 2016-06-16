#!/usr/bin/env python

def task(l_min=0, l_max=1, num=10000):
    print 'screen python ~/minus-scripts/dump/meowchat_to_youja.py 0 %d &\n' % (l_min * num)
    print 'screen python ~/minus-scripts/dump/meowchat_to_youja.py %d 200000000 &\n' % (l_max * num)
    print '\n\n'
    
    for n in range(l_min, l_max, 1):
        print 'screen python ~/minus-scripts/dump/meowchat_to_youja.py %d %d &\n' % (n * num + num / 2, (n + 1) * num)


if __name__ == '__main__':
    task(l_min=10, l_max=20, num=1000000)
    
    print 'Init task over.\n'
    print '''
    May be: 
        
        uptime
        
        redis-cli -h 10.154.148.158 -p 6379 -n 1 scard S:photo
        
        tree /data
        
        ls -lth /data/logs
        
    '''
    
    
