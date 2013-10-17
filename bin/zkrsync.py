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
from kazoo.recipe.lock import Lock
from kazoo.recipe.queue import LockingQueue
from kazoo.recipe.watchers import DataWatch
from kazoo.security import make_digest_acl
from vsc.utils import fancylogger
from vsc.utils.generaloption import simple_option
from vsc.zk.parser import get_rootinfo, parse_zkconfig, parse_acls
from vsc.zk.rsync.source import RsyncSource
from vsc.zk.rsync.destination import RsyncDestination
        
def main():
    options = {
        'servers'     : ('list of zk servers', 'strlist', 'store', None), 
        'source'      : ('rsync source', None, 'store_true', False, 'S'),
        'destination' : ('rsync destination', None, 'store_true', False, 'D'),
        'path'        : ('rsync basepath', None, 'store', None, 'p'),
        'session'     : ('session name', None, 'store', 'default', 'N'),
        'depth'       : ('queue depth', None, 'store', 4),
        'credentials' : ('credentials for user with create rights on rsync path', None, 'store', 'root:admin', 'c'),
    }
    go = simple_option(options)
    print go.options
    # Parsing options
    if not go.options.servers:
        logger.error("No servers given!")
        exit(1)
    if not go.options.path:
        logger.error("Path is mandatory!")
        exit(1)
    depth = go.options.depth
    rsyncpath = go.options.path
    session = go.options.session
    
    acreds = [('digest', go.options.credentials)] 
    user, passw = tuple(go.options.credentials.split(':'))
    admin_acl = make_digest_acl(user, passw, all=True)
    
    if go.options.source and go.options.destination:
        logger.error("I can not be the source AND the destination")
        exit(1)
    
    kwargs = {
        'session'     : session,
        'default_acl' : [admin_acl],
        'auth_data'   : acreds,
        }
    
    if go.options.destination:
        # Start zookeeper connection and rsync daemon
        rsyncD = RsyncDestination(go.options.servers, **kwargs)
        # Add myself to dest_Q
        watchpath = rsyncD.znode_path(session + '/watch')
        @rsyncD.DataWatch(watchpath)
        def ready_watcher(data, stat):
            logger.debug("Watch status is %s" % data)
            if data == 'end':
                logger.debug('End node received, exit')
                rsyncD.set_ready()        
        dest_Q = LockingQueue(rsyncD, rsyncD.znode_path(session + '/destQ'))
        dest_Q.put(rsyncD.daemon_info())
        
        while not rsyncD.is_ready():
            # Wait for closing signal
            time.sleep(15)
            
        logger.debug('%s Ready' % rsyncD.get_whoami())
        #exit daemon
        rsyncD.exit()
        
    elif go.options.source:
        # Start zookeeper connections
        rsyncS = RsyncSource(go.options.servers, **kwargs)
        # Try to retrieve session lock
        lockpath = rsyncS.znode_path(session + '/lock')
        watchpath = rsyncS.znode_path(session + '/watch')
        paramstr = rsyncpath + ':' + str(depth)
        lock = Lock(rsyncS, lockpath, paramstr) # Info about contender..
        locked = lock.acquire(False)
        dest_Q = LockingQueue(rsyncS, rsyncS.znode_path(session + '/destQ'))
        
        if locked:
            logger.debug('lock acquired')
            if rsyncS.exists(watchpath):
                logger.error('watchnode already exists!')
                lock.release()
                rsyncS.exit()
                exit(1)
            rsyncS.make_znode(watchpath, 'start')

            logger.debug('building tree, then waiting till empty')
            time.sleep(20)
            
            # Build path_Q
            # Wait till Q is empty
            # if Q is completely empty, so syncing is done:
            logger.debug("clients connected: %s" % rsyncS.get_all_hosts())
            rsyncS.set(watchpath, 'end')
            lock.release()
            logger.debug('watch set to end, lock released')
            # Check if other sources are disconnected
            while len(rsyncS.get_all_hosts()) > 1:
                logger.debug("clients still connected: %s" % rsyncS.get_all_hosts())
                time.sleep(5)
            
            rsyncS.delete(dest_Q.path, recursive=True)
            rsyncS.delete(watchpath)
            logger.debug('Queues and watch removed')
            rsyncS.exit() 
            exit()
        else:
            @rsyncS.DataWatch(watchpath)
            def ready_watcher(data, stat):
                logger.debug("Watch status is %s" % data)
                if data == 'end':
                    logger.debug('End node received, exit')
                    rsyncS.set_ready()
   
            params = lock.contenders()
            if not params or params.count(params[0]) != len(params):
                logger.error('central params not found or invalid')
                exit(1)
            elif paramstr != params[0]:
                logger.error('params of this client (%s) are not the same as master(%s)' % (paramstr, params[0]))
                exit(1)
            logger.debug(params[0])
            logger.debug('ready to do some rsyncing')    
            while not rsyncS.is_ready():
                # get on Q with timeout
                # check element , ok or continue with next iteration
                logger.debug('rsyncing')
                time.sleep(12)
                
            logger.debug('%s Ready' % rsyncS.get_whoami())
            rsyncS.exit()    
             
if __name__ == '__main__':
    logger = fancylogger.getLogger()
    main()