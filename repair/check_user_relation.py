#!/usr/bin/env python
# -*- coding:utf8 -*-

import os
from meowchat_user import DumpUser
import redis
import logging  # @UnusedImport
import logging.handlers
import sys
import time
import traceback


handler = logging.handlers.TimedRotatingFileHandler("/data/logs/repair-user-relation.out", when='D', interval=1)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'

formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)
logger = logging.getLogger('repair-user-relation')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


def check_relation_from_redis():
    usa_redis_26 = redis.Redis(host="10.154.148.158", port=6666, db=26)
    sg_redis_frela_5 = redis.Redis(host="52.220.41.121", port=6666, db=5)
    sg_redis_trela_8 = redis.Redis(host="52.220.41.121", port=6666, db=8)
    f_rela_key = "U:rf:{id}"
    t_rela_key = "U:rt:{id}"


    du = DumpUser()
    while True:
        uid = usa_redis_26.spop("S:Task:relation")
        if not uid:
            logger.info("============to sleep... =====")
            time.sleep(5*60)
        elif uid >=100000000:
            continue

        try:
            uid = int(uid)
            sg_rf = sg_redis_frela_5.zcard(f_rela_key.format(id=uid))
            sg_rf = int(sg_rf) if sg_rf else 0
            sg_rt = sg_redis_trela_8.zcard(t_rela_key.format(id=uid))
            sg_rt = int(sg_rt) if sg_rt else 0
            rs = du.get_relation_size_by_uid(uid)


            if (sg_rf < rs[0]) or (sg_rt < rs[1]) :
                logger.info("=================process uid=%s -sg_rf=%6d - rf=%6d - sg_rt =%6d -  rt=%6d===============" % (uid, sg_rf, rs[0], sg_rt, rs[1]))
                du.user_relation([uid])



        except Exception as ex:
            logger.error(traceback.format_exc())


if __name__ == '__main__':
    logger.info("============begin to repair relation... =====")
    check_relation_from_redis()
    logger.info("============ repair relation end =====")

    print '\nCompleted\n'

