#!/usr/bin/env python
# -*- coding:utf8 -*-

import redis


rinfo = redis.Redis(host='jedisbuilderinfo.redis.youja.cn', port=6379, db=6)
rexp = redis.Redis(host='userexp.redis.youja.cn', port=6801, db=6)

glevearr = [0,
         250, 500,
         1250, 2000,
         3000, 4000,
         5000, 10000,
         35000, 65000,
         100000, 200000,
         350000, 4750000,
         5000000, 10000000,
         15000000, 20000000,
         25000000, 30000000,
         35000000, 40000000,
         45000000, 50000000,
         55000000, 60000000,
         65000000, 70000000,
         75000000, 80000000
        ]

keys = rinfo.keys('hExp:%s' % '11111*')
print keys

for key in keys:
    g = int(rinfo.hget(key, 'G'))
    gl = gnv = gnp = 0
    if g > glevearr[-1]:
        gl = len(glevearr) - 1
    else:
        for level in range(len(glevearr)):
            if g < glevearr[level + 1]:
                gl = level
                break
        gnv = glevearr[gl + 1] - g
        gnp = int((g - glevearr[gl]) * 100.00 / (glevearr[gl + 1] - glevearr[gl]))
    
    mset_val = {'GL':gl, 'GNV':gnv, 'GNP':gnp}
    
    rexp.hmget(key, rinfo.hgetall(key))
    rexp.mset(key, mset_val)
#     rinfo.delete(key)
    print key, mset_val
    
    
    
