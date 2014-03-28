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
# http://github.com/hpcugent/vsc-zk
#
# vsc-zk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Library General Public License as
# published by the Free Software Foundation, either version 2 of
# the License, or (at your option) any later version.
#
# vsc-zk is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public License
# along with vsc-zk. If not, see <http://www.gnu.org/licenses/>.
#
"""
vsc-zk zkrsync

@author: Kenneth Waegeman (Ghent University)
"""

import os
import re
import stat
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
CL_DEST = "dest"
CL_SOURCE = "source"
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
    elif options.source:
        rstype = CL_SOURCE
    elif options.destination:
        rstype = CL_DEST
    else:
        rstype = None

    return rootcreds, admin_acl, rstype

def main():
    """ Start a new rsync client (destination or source) in a specified session """
    options = {
        # Zookeeper connection options:
        'servers'     : ('list of zk servers', 'strlist', 'store', None),
        'user'        : ('user with creation rights on zookeeper', None, 'store', 'root', 'u'),
        'passwd'      : ('password for user with creation rights', None, 'store', 'admin', 'p'),
        # Role options, define exactly one of these:
        'source'      : ('rsync source', None, 'store_true', False, 'S'),
        'destination' : ('rsync destination', None, 'store_true', False, 'D'),
        'pathsonly'   : ('Only do a test run of the pathlist building', None, 'store_true', False),
        'state'       : ('Only do the state', None, 'store_true', False),
        # Session options; should be the same on all clients of the session!
        'session'     : ('session name', None, 'store', 'default', 'N'),
        'netcat'      : ('run netcat test instead of rsync', None, 'store_true', False),
        'dryrun'      : ('run rsync in dry run mode', None, 'store_true', False, 'n'),
        'rsyncpath'   : ('rsync basepath', None, 'store', None, 'r'),  # May differ between sources and dests
        # Pathbuilding (Source clients and pathsonly ) specific options:
        'excludere'   : ('Exclude from pathbuilding', None, 'regex', re.compile('/\.snapshots(/.*|$)')),
        'depth'       : ('queue depth', "int", 'store', 4),
        # Source clients options; should be the same on all clients of the session!:
        'delete'      : ('run rsync with --delete', None, 'store_true', False),
        # Individual client options
        'domain'      : ('substitute domain', None, 'store', None),
        'logfile'     : ('Output to logfile', None, 'store', '/tmp/zkrsync/%(session)s-%(rstype)s-%(pid)s.log'),
        # Individual Destination client specific options
        'rsyncport'   : ('force port on which rsyncd binds', "int", 'store', None),
        'startport'   : ('offset to look for rsyncd ports', "int", 'store', 4444)
    }

    go = simple_option(options)
    acreds, admin_acl, rstype = zkrsync_parse(go.options)

    if go.options.logfile:
        logfile = go.options.logfile % {
            'session': go.options.session,
            'rstype': rstype,
            'pid': str(os.getpid())
        }
        logdir = os.path.dirname(logfile)
        if logdir:
            if not os.path.exists(logdir):
                os.makedirs(logdir)
            os.chmod(logdir, stat.S_IRWXU)

        fancylogger.logToFile(logfile)
        logger.debug('Logging to file %s:' % logfile)

    kwargs = {
        'session'     : go.options.session,
        'default_acl' : [admin_acl],
        'auth_data'   : acreds,
        'rsyncpath'   : go.options.rsyncpath,
        'netcat'      : go.options.netcat,
        }
    if go.options.state:
        rsyncP = RsyncSource(go.options.servers, **kwargs)
        logger.info('Progress: %s of %s paths remaining' % (rsyncP.len_paths(), rsyncP.paths_total))
        rsyncP.exit()
        sys.exit(0)

    elif go.options.pathsonly:
        kwargs['rsyncdepth'] = go.options.depth
        kwargs['excludere'] = go.options.excludere
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

    elif rstype == CL_DEST:
        # Start zookeeper connection and rsync daemon
        kwargs['rsyncport'] = go.options.rsyncport
        kwargs['startport'] = go.options.startport
        kwargs['domain'] = go.options.domain
        rsyncD = RsyncDestination(go.options.servers, **kwargs)
        rsyncD.run()

        logger.debug('%s Ready' % rsyncD.get_whoami())
        rsyncD.exit()
        sys.exit(0)

    elif rstype == CL_SOURCE:
        # Start zookeeper connections
        kwargs['rsyncdepth'] = go.options.depth
        kwargs['dryrun'] = go.options.dryrun
        kwargs['delete'] = go.options.delete
        kwargs['excludere'] = go.options.excludere
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
                    logger.info('Progress: %s of %s paths remaining' % (todo_paths, paths_total))
                time.sleep(SLEEP_TIME)
            rsyncS.shutdown_all()
            rsyncS.exit()
            sys.exit(0)
        else:
            rsyncS.ready_with_stop_watch()
            logger.debug('ready to process paths')
            while not rsyncS.is_ready():
                logger.debug('trying to get a path out of Queue')
                rsyncS.rsync(TIME_OUT)

            logger.debug('%s Ready' % rsyncS.get_whoami())
            rsyncS.exit()
            sys.exit(0)

if __name__ == '__main__':
    main()
