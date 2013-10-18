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

    BASE_PARTIES = RsyncController.BASE_PARTIES + ['sources']

    def __init__(self, hosts, session=None, name=None, default_acl=None, auth_data=None, rsyncpath=None):

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

    def get_sources(self):
        hosts = []
        for host in self.parties['sources']:
            hosts.append(host)
        return hosts

    def watchpath(self):
        return self.znode_path(self.session + '/watch')

    def lockpath(self):
        return self.znode_path(self.session + '/lock')


    def acq_lock(self):
        self.lock = self.Lock(self.lockpath(), "")
        return self.lock.acquire(False)

    def release_lock(self):
        return self.lock.release()

    def start_watch(self):
        if self.exists(self.watchpath()):
            self.log.error('watchnode already exists!')
            self.release_lock()
            self.exit()
            return False
        return self.make_znode(self.watchpath(), 'start')

    def end_watch(self):
        self.set(self.watchpath(), 'end')

    def build_pathQ(self):
        self.log.debug('building tree, then waiting till empty')
        time.sleep(20)  # stub

    def shutdown_all(self):
        self.end_watch()
        self.release_lock()
        self.log.debug('watch set to end, lock released')

        while len(self.get_all_hosts()) > 1:
            self.log.debug("clients still connected: %s" % self.get_all_hosts())
            time.sleep(5)
        self.delete(self.dest_Q().path, recursive=True)
        self.delete(self.watchpath())
        self.log.debug('Queues and watch removed')


