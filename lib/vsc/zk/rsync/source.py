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
zk.rsync source

@author: Stijn De Weirdt (Ghent University)
@author: Kenneth Waegeman (Ghent University)
"""

import os
import tempfile
import time

from kazoo.recipe.lock import Lock
from kazoo.recipe.queue import LockingQueue
from vsc.utils.run import RunAsyncLoopLog
from vsc.zk.depthwalk import get_pathlist, encode_paths, decode_path
from vsc.zk.rsync.controller import RsyncController

class RsyncSource(RsyncController):
    """
    Class for controlling rsync with Zookeeper. 
    Builds a tree of paths to devide, and effectively rsyncs the subpaths.
    Stops when ready
    """

    BASE_PARTIES = RsyncController.BASE_PARTIES + ['sources']
    NC_RANGE = 15
    SLEEPTIME = 1  # For netcat stub
    TIME_OUT = 5  # waiting for destination
    WAITTIME = 5  # check interval of closure of other clients

    def __init__(self, hosts, session=None, name=None, default_acl=None,
                 auth_data=None, rsyncpath=None, rsyncdepth=-1,
                 netcat=False, dryrun=False, delete=False, excludere=None):

        kwargs = {
            'hosts'       : hosts,
            'session'     : session,
            'name'        : name,
            'default_acl' : default_acl,
            'auth_data'   : auth_data,
            'rsyncpath'   : rsyncpath,
            'netcat'      : netcat
        }
        super(RsyncSource, self).__init__(**kwargs)

        self.lockpath = self.znode_path(self.session + '/lock')
        self.lock = None
        self.path_queue = LockingQueue(self, self.znode_path(self.session + '/pathQueue'))
        self.completed_queue = LockingQueue(self, self.znode_path(self.session + '/completedQueue'))
        self.failed_queue = LockingQueue(self, self.znode_path(self.session + '/failedQueue'))
        self.output_queue = LockingQueue(self, self.znode_path(self.session + '/outputQueue'))
        if rsyncdepth < 0:
            self.log.raiseException('Invalid rsync depth: %i' % rsyncdepth)
        else:
            self.rsyncdepth = rsyncdepth
        self.rsync_delete = delete
        self.rsync_dry = dryrun
        self.excludere = excludere

    def get_sources(self):
        """ Get all zookeeper clients in this session registered as clients """
        hosts = []
        for host in self.parties['sources']:
            hosts.append(host)
        return hosts

    def acq_lock(self):
        """ Try to acquire lock. Returns true if lock is acquired """
        self.lock = self.Lock(self.lockpath, "")
        return self.lock.acquire(False)

    def release_lock(self):
        """ Release the acquired lock """
        return self.lock.release()

    def start_ready_rwatch(self):
        """ Start a watch other clients can register to, but release lock and exit on error """
        watch = self.start_ready_watch()
        if not watch:
            if len(self.get_all_hosts()) == 1:  # Fix previous unclean shutdown
                self.cleanup()
            self.release_lock()
            self.exit()
            return False
        else:
            return watch

    def build_pathqueue(self):
        """ Build a queue of paths that needs to be rsynced """
        self.log.debug('removing old queue and building new queue')
        if self.exists(self.path_queue.path):
            self.delete(self.path_queue.path, recursive=True)
        if self.netcat:
            paths = [str(i) for i in range(self.NC_RANGE)]
            time.sleep(self.SLEEPTIME)
        else:
            tuplpaths = get_pathlist(self.rsyncpath, self.rsyncdepth, exclude_re=self.excludere,
                                     exclude_usr='root')  # Don't exclude user files
            paths = encode_paths(tuplpaths)
        self.paths_total = len(paths)
        for path in paths:
            self.path_queue.put(path)  # Put_all can issue a zookeeper connection error with big lists
        self.log.debug('pathqueue building finished')
        return self.paths_total

    def isempty_pathqueue(self):
        """ Returns true if all paths in pathqueue are done """
        return len(self.path_queue) == 0

    def len_paths(self):
        """ Returns how many elements still in pathQueue """
        return len(self.path_queue)

    def shutdown_all(self):
        """ Send end signal and release lock 
        Make sure other clients are disconnected, clean up afterwards."""
        self.stop_ready_watch()
        self.log.debug('watch set to stop')

        while len(self.get_all_hosts()) > 1:
        #    self.log.debug("clients still connected: %s" % self.get_all_hosts())
            time.sleep(self.WAITTIME)
        self.cleanup()

    def cleanup(self):
        """ Remove all session nodes in zookeeper after first logging the output queues """
        self.delete(self.dest_queue.path, recursive=True)
        self.delete(self.path_queue.path, recursive=True)

        while (len(self.failed_queue) > 0):
            self.log.error('Failed Path %s' % self.failed_queue.get())
            self.failed_queue.consume()

        while (len(self.completed_queue) > 0):
            self.log.info('Completed Path %s' % self.completed_queue.get())
            self.completed_queue.consume()

        self.log.info('Output:')
        while (len(self.output_queue) > 0):
            self.log.info(self.output_queue.get())
            self.output_queue.consume()

        self.delete(self.completed_queue.path, recursive=True)
        self.delete(self.failed_queue.path, recursive=True)
        self.delete(self.output_queue.path, recursive=True)
        self.remove_ready_watch()
        self.release_lock()
        self.log.debug('Lock, Queues and watch removed')

    def generate_file(self, path):
        """
        Writes the relative path used for the rsync of this path, 
        for use by --files-from. 
        """
        if not path.startswith(self.rsyncpath):
            self.log.raiseException('Invalid path! %s is not a subpath of %s!' % (path, self.rsyncpath))
        else:
            subpath = path[len(self.rsyncpath):]
            subpath = subpath.strip(os.path.sep)

            fd, name = tempfile.mkstemp(dir=self.RSDIR, text=True)
            file = os.fdopen(fd, "w")
            file.write('%s/' % subpath)
            return name

    def attempt_run(self, path, attempts=3):
        """ Try to run a command x times, on failure add to failed queue """
        attempt = 1
        while (attempt <= attempts):
            dest = self.get_a_dest(self.TIME_OUT)  # Keeps it if not consuming
            if dest:  # We locked a rsync daemon
                self.log.debug('Got destination %s' % dest)
                port, host, other = tuple(dest.split(':', 2))
                if self.netcat:
                    code, output = self.run_netcat(path, host, port)
                else:
                    code, output = self.run_rsync(path, host, port)
                if code == 0:
                    self.completed_queue.put(path)
                    return code, output
            attempt += 1
            time.sleep(self.WAITTIME)  # Wait before new attempt

        if dest:
            self.log.error('There were issues with path %s!' % path)
            self.failed_queue.put(path)
            return 0, output  # otherwise client get stuck
        else:
            self.log.debug('Still no destination after %s tries' % attempts)
            self.path_queue.put(path, priority=50)  # Keep path in queue
            self.path_queue.consume()  # But stop locking it
            time.sleep(self.TIME_OUT)  # Wait before new attempt
            return 1, None

    def run_rsync(self, encpath, host, port):
        """
        Runs the rsync command with or without recursion, delete or dry-run option.
        It uses the destination module linked with this session.
        """
        path, recursive = decode_path(encpath)
        file = self.generate_file(path)
        # Start rsync recursive or non recursive; archive mode (a) is equivalent to  -rlptgoD (see man rsync)
        flags = ['--stats', '--numeric-ids', '-lptgoD', '--files-from=%s' % file]
        if recursive:
            flags.append('-r')
        if self.rsync_delete:
            flags.append('--delete')
        if self.rsync_dry:
            flags.append('-n')
        self.log.debug('echo %s is sending %s to %s %s' % (self.whoami, path, host, port))

        command = 'rsync %s %s/ rsync://%s:%s/%s' % (' '.join(flags), self.rsyncpath,
                                                     host, port, self.module)
        code, output = RunAsyncLoopLog.run(command)
        os.remove(file)
        return code, output

    def run_netcat(self, path, host, port):
        """ Test run with netcat """
        time.sleep(self.SLEEPTIME)
        command = 'echo %s is sending %s | nc %s %s' % (self.whoami, path, host, port)

        return RunAsyncLoopLog.run(command)

    def rsync_path(self, path):
        """ start rsync session for given path and destination, returns true if successful """
        if not path:
            self.log.raiseException('Empty path given!')
        elif not isinstance(path, basestring):
            self.log.raiseException('Invalid path: %s !' % path)
        else:
            code, output = self.attempt_run(path)
            if output:
                self.output_queue.put(output)
            return (code == 0)

    def get_a_dest(self, timeout):
        """ 
        Try to get a destination.
        check if destination is still running, otherwise remove
        """
        if len(self.dest_queue) == 0:
            self.log.debug('Destinations not yet available')
        dest = self.dest_queue.get(timeout)
        if dest:
            port, whoami = tuple(dest.split(':', 1))
            if not self.member_of_party(whoami, 'allsd'):
                self.log.debug('destination is not found in party')
                self.dest_queue.consume()
                return None
            else:
                portmap = '%s/portmap/%s' % (self.session, whoami)
                lport, stat = self.get_znode(portmap)
                if port != lport:
                    self.log.error('destination port not valid')  # Should not happen
                    self.dest_queue.consume()
                    return None
        return dest

    def rsync(self, timeout=None):
        """ Get a destination, a path and call a new rsync iteration """
        path = self.path_queue.get(timeout)
        if path:
            if self.rsync_path(path):
                    self.path_queue.consume()
