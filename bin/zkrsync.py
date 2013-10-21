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

import time, sys
from kazoo.recipe.lock import Lock
from kazoo.recipe.queue import LockingQueue
from kazoo.recipe.watchers import DataWatch
from kazoo.security import make_digest_acl
from vsc.utils import fancylogger
from vsc.utils.generaloption import simple_option
from vsc.zk.configparser import get_rootinfo, parse_zkconfig, parse_acls
from vsc.zk.rsync.source import RsyncSource
from vsc.zk.rsync.destination import RsyncDestination

SLEEP_TIME = 15
TIME_OUT = 15
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

    return options.servers, options.rsyncpath, options.depth, options.session, rootcreds, admin_acl, type

def main():
    """ Start a new rsync client (destination or source) in a specified session """
    options = {
        'servers'     : ('list of zk servers', 'strlist', 'store', None),
        'source'      : ('rsync source', None, 'store_true', False, 'S'),
        'destination' : ('rsync destination', None, 'store_true', False, 'D'),
        'rsyncpath'   : ('rsync basepath', None, 'store', None, 'r'),
        'session'     : ('session name', None, 'store', 'default', 'N'),
        'depth'       : ('queue depth', None, 'store', 4),
        'user'        : ('user with create rights on zookeeper', None, 'store', 'root', 'u'),
        'passwd'      : ('password for user with create rights', None, 'store', 'admin', 'p'),
    }

    go = simple_option(options)
    servers, rsyncpath, depth, session, acreds, admin_acl, type = zkrsync_parse(go.options)

    kwargs = {
        'session'     : session,
        'default_acl' : [admin_acl],
        'auth_data'   : acreds,
        'rsyncpath'   : rsyncpath
        }

    if type == "destination":
        # Start zookeeper connection and rsync daemon
        rsyncD = RsyncDestination(servers, **kwargs)
        rsyncD.run()

        while not rsyncD.is_ready():  # Wait until receiving signal
            time.sleep(SLEEP_TIME)

        logger.debug('%s Ready' % rsyncD.get_whoami())
        rsyncD.exit()

    elif type == "source":
        # Start zookeeper connections
        rsyncS = RsyncSource(servers, **kwargs)
        # Try to retrieve session lock
        locked = rsyncS.acq_lock()

        if locked:
            logger.debug('lock acquired')
            watchnode = rsyncS.start_watch()
            if not watchnode:
                sys.exit(1)
            rsyncS.build_pathqueue()
            # TODO Wait till pathqueue is empty
            # TODO if pathqueue is completely empty, so syncing is done:
            rsyncS.shutdown_all()
            rsyncS.exit()
            sys.exit(0)
        else:
            rsyncS.stop_with_watch()
            logger.debug('ready to do some rsyncing')
            while not rsyncS.is_ready():
                # get on pathqueue with timeout
                # check element , ok or continue with next iteration
                logger.debug('rsyncing')
                time.sleep(TIME_OUT)

            logger.debug('%s Ready' % rsyncS.get_whoami())
            rsyncS.exit()
            sys.exit(0)

if __name__ == '__main__':
    main()
