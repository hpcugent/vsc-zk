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

import sys
import time

from kazoo.recipe.lock import Lock
from kazoo.recipe.queue import LockingQueue
from kazoo.recipe.watchers import DataWatch
from kazoo.security import make_digest_acl
from vsc.utils import fancylogger
from vsc.utils.generaloption import simple_option
from vsc.zk.configparser import get_rootinfo, parse_zkconfig, parse_acls
from vsc.zk.rsync.destination import RsyncDestination
from vsc.zk.rsync.source import RsyncSource

SLEEP_TIME = 5
TIME_OUT = 5
logger = fancylogger.getLogger()

def zkrsync_parse(options):
    """Takes options of simple_option and returns all the parameters after checks"""
    if not options.servers:
        logger.error("No servers given!")
        sys.exit(1)
    if not options.rsyncpath:
        logger.error("Path is mandatory!")
        sys.exit(1)
    if options.depth <= 0:
        logger.error("Invalid depth!")
        sys.exit(1)

    rootcreds = [('digest', options.user + ':' + options.passwd)]
    admin_acl = make_digest_acl(options.user, options.passwd, all=True)

    if options.source and options.destination:
        logger.error("Client can not be the source AND the destination")
        sys.exit(1)
    if options.source:
        type = "source"
    elif options.destination:
        type = "destination"

    return rootcreds, admin_acl, type

def main():
    """ Start a new rsync client (destination or source) in a specified session """
    options = {
        'servers'     : ('list of zk servers', 'strlist', 'store', None),
        'domain'      : ('substitute domain', None, 'store', None),
        'source'      : ('rsync source', None, 'store_true', False, 'S'),
        'destination' : ('rsync destination', None, 'store_true', False, 'D'),
        'rsyncpath'   : ('rsync basepath', None, 'store', None, 'r'),
        'session'     : ('session name', None, 'store', 'default', 'N'),
        'depth'       : ('queue depth', "int", 'store', 4),
        'user'        : ('user with create rights on zookeeper', None, 'store', 'root', 'u'),
        'passwd'      : ('password for user with create rights', None, 'store', 'admin', 'p'),
        'rsyncport'   : ('port on wich rsync binds', "int", 'store', 4444),
        'netcat'      : ('run netcat test instead of rsync', None, 'store_true', False),
        'dryrun'      : ('run rsync in dry run mode', None, 'store_true', False, 'n'),
        'delete'      : ('run rsync with --delete', None, 'store_true', False),
        'pathsonly'   : ('Do a test run of the pathlist building', None, 'store_true', False),
        'logfile'     : ('Output to logfile', None, 'store', None),
        'state'       : ('Show only the state', None, 'store_true', False),
    }

    go = simple_option(options)
    acreds, admin_acl, type = zkrsync_parse(go.options)

    if go.options.logfile:
        fancylogger.logToFile(go.options.logfile)
        logger.debug('Logging to file %s:' % go.options.logfile)


    kwargs = {
        'session'     : go.options.session,
        'default_acl' : [admin_acl],
        'auth_data'   : acreds,
        'rsyncpath'   : go.options.rsyncpath,
        'netcat'      : go.options.netcat,
        }
    if go.options.state:
        rsyncP = RsyncSource(go.options.servers, **kwargs)
        logger.info('There are still %s of %s paths to do' % (rsyncP.len_paths(), rsyncP.paths_total))
        rsyncP.exit()
        sys.exit(0)

    elif go.options.pathsonly:
        kwargs['rsyncdepth'] = go.options.depth
        rsyncP = RsyncSource(go.options.servers, **kwargs)
        locked = rsyncP.acq_lock()
        if locked:
            starttime = time.time()
            rsyncP.build_pathqueue()
            endtime = time.time()
            timing = endtime - starttime
            pathqueue = rsyncP.path_queue
            logger.info('Building with depth %i took %f seconds walltime. there are %i paths in the Queue'
                         % (go.options.depth, timing, len(pathqueue)))
            rsyncP.delete(pathqueue.path, recursive=True)
            rsyncP.release_lock()
        else:
            logger.error('There is already a lock on the pathtree of this session')

        rsyncP.exit()
        sys.exit(0)

    elif type == "destination":
        # Start zookeeper connection and rsync daemon
        kwargs['rsyncport'] = go.options.rsyncport
        kwargs['domain'] = go.options.domain
        rsyncD = RsyncDestination(go.options.servers, **kwargs)
        rsyncD.run()

        logger.debug('%s Ready' % rsyncD.get_whoami())
        rsyncD.exit()
        sys.exit(0)

    elif type == "source":
        # Start zookeeper connections
        kwargs['rsyncdepth'] = go.options.depth
        kwargs['dryrun'] = go.options.dryrun
        kwargs['delete'] = go.options.delete
        rsyncS = RsyncSource(go.options.servers, **kwargs)
        # Try to retrieve session lock
        locked = rsyncS.acq_lock()

        if locked:
            logger.debug('lock acquired')
            watchnode = rsyncS.start_ready_rwatch()
            if not watchnode:
                sys.exit(1)
            paths_total = rsyncS.build_pathqueue()
            todo_paths = paths_total
            while not rsyncS.isempty_pathqueue():
                if todo_paths != rsyncS.len_paths():  # Output progress state
                    todo_paths = rsyncS.len_paths()
                    logger.info('There are still %s of %s paths to do' % (todo_paths, paths_total))
                time.sleep(SLEEP_TIME)
            rsyncS.shutdown_all()
            rsyncS.exit()
            sys.exit(0)
        else:
            rsyncS.ready_with_stop_watch()
            logger.debug('ready to do some rsyncing')
            while not rsyncS.is_ready():
                logger.debug('start rsyncing')
                rsyncS.rsync(TIME_OUT)

            logger.debug('%s Ready' % rsyncS.get_whoami())
            rsyncS.exit()
            sys.exit(0)

if __name__ == '__main__':
    main()
