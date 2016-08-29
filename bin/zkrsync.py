#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright 2013-2016 Ghent University
#
# This file is part of vsc-zk,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-zk
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

from kazoo.security import make_digest_acl
from vsc.utils import fancylogger
from vsc.utils.daemon import Daemon
from vsc.utils.generaloption import simple_option
from vsc.zk.rsync.destination import RsyncDestination
from vsc.zk.rsync.source import RsyncSource

TIME_OUT = 5
CL_DEST = "dest"
CL_SOURCE = "source"
logger = fancylogger.getLogger()


class ZkrsDaemon(Daemon):
    def __init__(self, pidfile, zkrs_type, zkrs_options, zkrs_kwargs):
        self.zkrs_type = zkrs_type
        self.zkrs_options = zkrs_options
        self.zkrs_kwargs = zkrs_kwargs
        Daemon.__init__(self, pidfile)

    def run(self):
        start_zkrs(self.zkrs_type, self.zkrs_options, self.zkrs_kwargs)


def init_logging(logfile, session, rstype):
    """Initiates the logfile"""
    logfile = logfile % {
        'session': session,
        'rstype': rstype,
        'pid': str(os.getpid())
    }
    logdir = os.path.dirname(logfile)
    if logdir:
        if not os.path.exists(logdir):
            os.makedirs(logdir)
        os.chmod(logdir, stat.S_IRWXU)

    fancylogger.logToFile(logfile)
    logger.info('Logging to file %s:' % logfile)

def init_pidfile(pidfile, session, rstype):
    """ Prepare pidfile """
    if not pidfile:
        logger.error('No PID file given!')
        sys.exit(1)
    else:
        pidfile = pidfile % {
            'session': session,
            'rstype': rstype,
            'pid': str(os.getpid())
        }
        piddir = os.path.dirname(pidfile)
        if piddir:
            if not os.path.exists(piddir):
                os.makedirs(piddir)
            os.chmod(piddir, stat.S_IRWXU)
        return pidfile

def get_state(servers, kwargs):
    """Get the state of a running session"""
    kwargs['verifypath'] = False # Not needed for state info
    rsyncP = RsyncSource(servers, rsyncdepth=0, **kwargs)
    code = rsyncP.get_state()
    rsyncP.exit()
    sys.exit(code)

def do_pathsonly(options, kwargs):
    """Only build the pathqueue and return timings"""
    kwargs['rsyncdepth'] = options.depth
    kwargs['excludere'] = options.excludere
    kwargs['excl_usr'] = options.excl_usr
    kwargs['rsubpaths'] = options.rsubpaths

    rsyncP = RsyncSource(options.servers, **kwargs)
    locked = rsyncP.acq_lock()
    if locked:
        starttime = time.time()
        rsyncP.build_pathqueue()
        endtime = time.time()
        timing = endtime - starttime
        pathqueue = rsyncP.path_queue
        logger.info('Building with depth %i took %f seconds walltime. there are %i paths in the Queue'
                     % (options.depth, timing, len(pathqueue)))
        rsyncP.delete(pathqueue.path, recursive=True)
        rsyncP.release_lock()
    else:
        logger.error('There is already a lock on the pathtree of this session')

    rsyncP.exit()
    sys.exit(0)

def start_destination(options, kwargs):
    """Starts a destination: Start zookeeper connection and rsync daemon"""
    kwargs['rsyncport'] = options.rsyncport
    kwargs['startport'] = options.startport
    kwargs['domain'] = options.domain
    rsyncD = RsyncDestination(options.servers, **kwargs)
    rsyncD.run()

    logger.debug('%s Ready' % rsyncD.get_whoami())
    rsyncD.exit()
    sys.exit(0)

def start_source(options, kwargs):
    """ Start a rsync source"""
    kwargs['rsyncdepth'] = options.depth
    kwargs['rsubpaths'] = options.rsubpaths
    kwargs['arbitopts'] = options.arbitopts
    kwargs['checksum'] = options.checksum
    kwargs['done_file'] = options.done_file
    kwargs['dryrun'] = options.dryrun
    kwargs['delete'] = options.delete
    kwargs['excludere'] = options.excludere
    kwargs['excl_usr'] = options.excl_usr
    kwargs['hardlinks'] = options.hardlinks
    kwargs['timeout'] = options.timeout
    kwargs['verbose'] = options.verbose
    # Start zookeeper connections
    rsyncS = RsyncSource(options.servers, **kwargs)
    # Try to retrieve session lock
    locked = rsyncS.acq_lock()

    if locked:
        logger.debug('lock acquired')
        watchnode = rsyncS.start_ready_rwatch()
        if not watchnode:
            sys.exit(1)
        rsyncS.build_pathqueue()
        rsyncS.wait_and_keep_progress()
        rsyncS.shutdown_all()

    else:
        rsyncS.ready_with_stop_watch()
        logger.debug('ready to process paths')
        while not rsyncS.is_ready():
            logger.debug('trying to get a path out of Queue')
            rsyncS.rsync(TIME_OUT)

        logger.debug('%s Ready' % rsyncS.get_whoami())

    rsyncS.exit()
    sys.exit(0)

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

def start_zkrs(rstype, options, kwargs):
    """ Start a run of zkrs"""
    if options.state:
        get_state(options.servers, kwargs)
    elif options.pathsonly:
        do_pathsonly(options, kwargs)
    elif rstype == CL_DEST:
        start_destination(options, kwargs)
    elif rstype == CL_SOURCE:
        start_source(options, kwargs)

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
        'rsubpaths'   : ('rsync subpaths, specified as <depth>_<path>, with deepest paths last', 'strlist', 'store', None),
        'excludere'   : ('Exclude from pathbuilding', None, 'regex', re.compile('/\.snapshots(/.*|$)')),
        'excl_usr'    : ('If set, exclude paths for this user only when using excludere', None, 'store', 'root'),
        'depth'       : ('queue depth', "int", 'store', 3),
        # Source clients options; should be the same on all clients of the session!:
        'delete'      : ('run rsync with --delete', None, 'store_true', False),
        'checksum'    : ('run rsync with --checksum', None, 'store_true', False),
        'hardlinks'   : ('run rsync with --hard-links', None, 'store_true', False),
        # Individual client options
        'verifypath'  : ('Check basepath exists while running', None, 'store_false', True),
        'daemon'      : ('daemonize client', None, 'store_true', False),
        'domain'      : ('substitute domain', None, 'store', None),
        'done-file'   : ('cachefile to write state to when done', None, 'store', None),
        'dropcache'   : ('run rsync with --drop-cache', None, 'store_true', False),
        'logfile'     : ('Output to logfile', None, 'store', '/tmp/zkrsync/%(session)s-%(rstype)s-%(pid)s.log'),
        'pidfile'     : ('Pidfile template', None, 'store', '/tmp/zkrsync/%(session)s-%(rstype)s-%(pid)s.pid'),
        'timeout'     : ('run rsync with --timeout TIMEOUT',  "int", 'store', 0),
        'verbose'     : ('run rsync with --verbose', None, 'store_true', False),
        # Individual Destination client specific options
        'rsyncport'   : ('force port on which rsyncd binds', "int", 'store', None),
        'startport'   : ('offset to look for rsyncd ports', "int", 'store', 4444),
        # Arbitrary rsync options: comma seperate list. Use a colon to seperate key and values
        'arbitopts'   : ('Arbitrary rsync source client long name options: comma seperate list. ' +
                             'Use a colon to seperate key and values. Beware these keys/values are not checked.',
                             'strlist', 'store', None),

    }

    go = simple_option(options)
    acreds, admin_acl, rstype = zkrsync_parse(go.options)
    if go.options.logfile:
        init_logging(go.options.logfile, go.options.session, rstype)

    kwargs = {
        'session'     : go.options.session,
        'default_acl' : [admin_acl],
        'auth_data'   : acreds,
        'rsyncpath'   : go.options.rsyncpath,
        'netcat'      : go.options.netcat,
        'verifypath'  : go.options.verifypath,
        'dropcache'   : go.options.dropcache,
        }

    if go.options.daemon:
        pidfile = init_pidfile(go.options.pidfile, go.options.session, rstype)
        zkrsdaemon = ZkrsDaemon(pidfile, rstype, go.options, kwargs)
        zkrsdaemon.start()
    else:
        start_zkrs(rstype, go.options, kwargs)

if __name__ == '__main__':
    main()
