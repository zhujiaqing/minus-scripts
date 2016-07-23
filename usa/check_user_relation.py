#!/usr/bin/env python
# -*- coding:utf8 -*-

import os
from meowchat_user import DumpUser

def check_by_file(uid_file):
    if uid_file is None or not os.path.exists(uid_file):return
    
    du = DumpUser()
    with open(uid_file, 'r') as uf:
        wf = open('%s-relation' % uid_file, 'w')
        for uid in uf.readlines():
            try:uid = int(uid)
            except:continue
            if uid > 100000000:continue  # 自己产生的uid，无须check
            
            rs = du.get_relation_size_by_uid(uid)
            wf.write('%12s - rf - %4d - rt - %4d\n' % (uid, rs[0], rs[1]))

        wf.close()
        uf.close()
        
if __name__ == '__main__':
    check_by_file(uid_file='/home/jesse/backup/uid-20160723')
    
    print '\nCompleted\n'

