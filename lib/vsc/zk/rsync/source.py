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
import time
"""
zk.rsync source

@author: Stijn De Weirdt (Ghent University)
@author: Kenneth Waegeman (Ghent University)
"""

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
    SLEEPTIME = 1  # For netcat stub
    WAITTIME = 5  # check interval of closure of other clients

    def __init__(self, hosts, session=None, name=None, default_acl=None,
                 auth_data=None, rsyncpath=None, rsyncdepth=-1,
                 netcat=False, dryrun=False, delete=False):

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
        if rsyncdepth < 0:
            self.log.raiseException('Invalid rsync depth: %i' % rsyncdepth)
        else:
            self.rsyncdepth = rsyncdepth
        self.rsync_delete = delete
        self.rsync_dry = dryrun

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
            paths = [str(i) for i in range(5)]
            time.sleep(self.SLEEPTIME)
        else:
            tuplpaths = get_pathlist(self.rsyncpath, self.rsyncdepth)
            paths = encode_paths(tuplpaths)

        self.path_queue.put_all(paths)
        self.log.debug('pathqueue building finished')

    def isempty_pathqueue(self):
        """ Returns true if all paths in pathqueue are done """
        return len(self.path_queue) == 0

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
        self.delete(self.dest_queue.path, recursive=True)
        self.delete(self.path_queue.path, recursive=True)
        self.remove_ready_watch()
        self.release_lock()
        self.log.debug('Lock, Queues and watch removed')

    def path_depth(self):
        """ Returns the depth of the path relatively to base path """
        pass  # TODO

    def run_rsync(self, encpath, host, port):
        """ Runs the rsync command with or without recursion """
        path, recursive = decode_path(encpath)
        if not path.startswith(self.rsyncpath):
            self.log.raiseException('Invalid path! %s is not a subpath of %s!' % (path, self.rsyncpath))
        # Start rsync recursive or non recursive; archive mode (a) is equivalent to  -rlptgoD (see man rsync)
        flags = "-n --progress --stats -lptgoD"
        if recursive:
            flags = '%s -r' % flags
        if self.rsync_delete:
            flags = '%s --delete' % flags
        if self.rsync_dry:
            flags = '%s -n' % flags
        self.log.debug('echo %s is sending %s to %s %s' % (self.whoami, path, host, port))
        return RunAsyncLoopLog.run('rsync %s %s rsync://%s:%s ' % (flags, path, host, port))

    def run_netcat(self, path, host, port):
        """ Test run with netcat """
        time.sleep(self.SLEEPTIME)
        return RunAsyncLoopLog.run('echo %s is sending %s | nc %s %s' % (self.whoami, path, host, port))

    def rsync_path(self, path, destination):
        """ start rsync session for given path and destination, returns true if successful """
        if not path:
            self.log.raiseException('Empty path given!')
        elif not isinstance(path, basestring):
            self.log.raiseException('Invalid path: %s !' % path)
        else:
            host, port = tuple(destination.split(':'))
            if self.netcat:
                code, output = self.run_netcat(path, host, port)
            else:
                code, output = self.run_rsync(path, host, port)
            # TODO: add output to output queue
            return (code == 0)

    def rsync(self, timeout=None):
        if len(self.dest_queue) == 0:
            self.log.debug('Destinations not yet available')
        dest = self.dest_queue.get(timeout)  # Keeps it if not consuming
        if dest:  # We locked a rsync daemon
            path = self.path_queue.get(timeout)
            if path and self.dest_queue.holds_lock():  # Nothing wrong happened in timeout
                if self.rsync_path(path, dest):
                    self.path_queue.consume()
                    # If connection to daemon fails, consume it?


