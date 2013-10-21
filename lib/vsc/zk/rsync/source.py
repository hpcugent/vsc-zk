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
from vsc.zk.rsync.controller import RsyncController

class RsyncSource(RsyncController):
    """
    Class for controlling rsync with Zookeeper. 
    Builds a tree of paths to devide, and effectively rsyncs the subpaths.
    Stops when ready
    """

    BASE_PARTIES = RsyncController.BASE_PARTIES + ['sources']

    def __init__(self, hosts, session=None, name=None, default_acl=None,
                 auth_data=None, rsyncpath=None):

        kwargs = {
            'hosts'       : hosts,
            'session'     : session,
            'name'        : name,
            'default_acl' : default_acl,
            'auth_data'   : auth_data,
            'rsyncpath'   : rsyncpath,
        }
        self.lock = None
        super(RsyncSource, self).__init__(**kwargs)

        self.lockpath = self.znode_path(self.session + '/lock')

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

    def start_watch(self):
        """ Start a watch other clients can register to """
        if self.exists(self.watchpath):
            self.log.error('watchnode already exists!')
            self.release_lock()
            self.exit()
            return False
        return self.make_znode(self.watchpath, 'start')

    def end_watch(self):
        """ Send end signal by setting watch to 'end' """
        self.set(self.watchpath, 'end')

    def build_pathqueue(self):
        """ Build a queue of paths that needs to be rsynced """
        self.log.debug('building tree, then waiting till empty')
        time.sleep(20)  # stub

    def shutdown_all(self):
        """ Send end signal and release lock 
        Make sure other clients are disconnected, clean up afterwards."""
        self.end_watch()
        self.release_lock()
        self.log.debug('watch set to end, lock released')

        while len(self.get_all_hosts()) > 1:
            self.log.debug("clients still connected: %s" % self.get_all_hosts())
            time.sleep(5)
        self.delete(self.dest_queue.path, recursive=True)
        self.delete(self.watchpath)
        self.log.debug('Queues and watch removed')


