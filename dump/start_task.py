#!/usr/bin/env python
# -*-coding:UTF8-*-

import random
import time

import redis

from meowchat_to_youja import Dump

################################### Execute #################################### 
BASE_REDIS = redis.Redis(host="10.154.148.158", port=6379, db=5)
KEY_TASK = 'L:task'
MAX_UID = 20000000
SINGLE_TASK_SIZE = 10000
MAX_TASK_NUMBER = MAX_UID / SINGLE_TASK_SIZE

def manual_start(x):
    task_id = BASE_REDIS.rpop(KEY_TASK)
    if task_id is None:return
    
    dump = Dump(int(task_id) * SINGLE_TASK_SIZE, (int(task_id) + 1) * SINGLE_TASK_SIZE)
    dump.more_user_with_mutli()
    dump.close_all()

def mutliprocess_start(process_num=20, limit=1000):
    from multiprocessing import Pool as JPool  # 多进程
    from multiprocessing import cpu_count
    pool = JPool(process_num * cpu_count())
    pool.map(manual_start, (i for i in range(MAX_TASK_NUMBER / 5)))
    pool.close()
    pool.join()
    
def init_task():
    """
    初始化任务
    若任务池存在就跳过，其它机器其它进程共用
    """
    time.sleep(random.randint(1, 3))  # 避免多台机器都在创建任务
#     BASE_REDIS.delete(KEY_TASK)
    
    if not BASE_REDIS.exists(KEY_TASK):
        for i in range(10, MAX_TASK_NUMBER):BASE_REDIS.lpush(KEY_TASK, i)

    print 'task size: %s' % BASE_REDIS.llen(KEY_TASK)
    
if __name__ == '__main__':
    print '\n[%s] Dump start\n' % time.strftime('%Y-%m-%d %H:%M:%S')
    
    init_task()
    mutliprocess_start()
    
    print '\n[%s] Dump over\n' % time.strftime('%Y-%m-%d %H:%M:%S')
    
    while True:
        print 'manual stop program'
        time.sleep(60)


