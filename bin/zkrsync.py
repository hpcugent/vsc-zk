#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright 2013-2013 Ghent University
#
# This file is part of vsc-zk,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
#
"""
vsc-zk zkrsync

@author: Kenneth Waegeman (Ghent University)
"""

import time
from kazoo.security import make_digest_acl
from kazoo.recipe.lock import Lock
from kazoo.recipe.queue import LockingQueue
from kazoo.recipe.watchers import DataWatch
from vsc.utils import fancylogger
from vsc.utils.generaloption import simple_option
from vsc.zk.rsync.source import RsyncSource
from vsc.zk.rsync.destination import RsyncDestination
from vsc.zk.parser import get_rootinfo, parse_zkconfig, parse_acls



        
def main():
    options = {
        'servers'     : ('list of zk servers', 'strlist', 'store', None), 
        'source'      : ('rsync source',None , 'store_true', False, 'S'),
        'destination' : ('rsync destination',None , 'store_true', False, 'D'),
        'path'        : ('rsync basepath',None , 'store', None, 'p'),
        'session'     : ('session name',None , 'store', 'default', 'N'),
        'depth'       : ('queue depth',None , 'store', 4),
        'credentials' : ('credentials for user with create rights on rsync path',None , 'store', 'root:admin','c'),
    }
    go = simple_option(options)
    print go.options
    
    if go.options.servers == None:
        logger.error("No servers given!")
        exit(1)
    if go.options.path == None:
        logger.error("Path is mandatory!")
        exit(1)
    acreds = [('digest', go.options.credentials)] 
    user, passw = tuple(go.options.credentials.split(':'))
    admin_acl = make_digest_acl(user,passw, all=True)
    
    if go.options.source and go.options.destination:
        logger.error("I can not be the alpha AND the Omega!")
        exit(1)
    
    session = go.options.session
    
    kwargs = {
        'session'     : session,
        'default_acl' : [admin_acl],
        'auth_data'   : acreds,
        }
    if go.options.destination:
        # Start zookeeper connection and rsync daemon
        rsyncD = RsyncDestination(go.options.servers,**kwargs)
        
           
        # Add myself to dest_Q
        dest_Q = LockingQueue(rsyncD, rsyncD.znode_path(session + '/destQ'))
        #rsyncD.daemon_info().encode()
        dest_Q.put('bla')
        # Wait for closing signal
        #1
        
        rsyncD.exit()
        
    elif go.options.source:
        
        # Start zookeeper connections
        rsyncS = RsyncSource(go.options.servers,**kwargs)

        # Try to retrieve session lock
        depth = go.options.depth
        rsyncpath = go.options.path # Parse this
        lockpath = rsyncS.znode_path(session + '/lock')
        watchpath = rsyncS.znode_path(session + '/watch')
        print watchpath
        lock = Lock(rsyncS, lockpath, rsyncpath + ':' + str(depth)) # Info about contender..
        locked = lock.acquire(False)

        if locked:
            rsyncS.make_znode(watchpath, False)
            # sleep
            time.sleep(40)
            # Sanity checks: same path, depth,..
            # Build path_Q
            # Wait till Q is empty
            # Send stop signal to all hosts
        # Else
            # If available, Take path out of Q's
            # Check for stop signal
            # Repeat
            rsyncS.set(watchpath, True)
            lock.release()
            rsyncS.exit() 
        else:    
            @rsyncS.DataWatch(watchpath)
            def my_func(data, stat):
                print("Data is %s" % data)
                if data:
                    rsyncS.exit()
                    exit()
                    
            while true:
                pass
        
             
    
if __name__ == '__main__':
    logger = fancylogger.getLogger()
    main()